#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <poll.h>

#define BUFFER_SIZE 8192
#define MAX_NACK_RETRIES 4   // NACKs are cheap (4 bytes), so we aggressively retry

struct JitterFrame {
    uint32_t seq;
    unsigned char payload[160];
    int valid;
};

struct MissingPacket {
    uint32_t seq;
    double last_nack_at;
    int retry_count;
    int active;
};

static struct JitterFrame jitter_buffer[BUFFER_SIZE];
static struct MissingPacket missing_list[BUFFER_SIZE];

static int out_fd;
static struct sockaddr_in player_addr;
static int feedback_fd;
static struct sockaddr_in relay_feedback;

static double t0 = 0.0;
static double delay_ms = 40.0;

// High-precision monotonic clock reader
double get_current_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

// Calculate the absolute playout deadline for a given sequence number
double get_deadline_sec(uint32_t s, double t0, double delay_ms) {
    return t0 + (delay_ms / 1000.0) + (s * 0.020);
}

void send_nack(uint32_t seq) {
    uint32_t raw_seq = htonl(seq);
    sendto(feedback_fd, &raw_seq, 4, 0, 
           (struct sockaddr *)&relay_feedback, sizeof relay_feedback);
}

// Force-skip frames whose playout deadline is EXPIRED
void flush_expired_frames(double now, uint32_t *next_expected_seq, double t0, double delay_ms, int out_fd, struct sockaddr_in *player_addr) {
    while (1) {
        uint32_t seq = *next_expected_seq;
        double deadline = get_deadline_sec(seq, t0, delay_ms);
        
        // Zero margin. We hold the buffer until the last possible moment.
        if (now >= deadline) {
            uint32_t idx = seq % BUFFER_SIZE;
            if (jitter_buffer[idx].valid && jitter_buffer[idx].seq == seq) {
                unsigned char out_buf[164];
                uint32_t raw_seq = htonl(seq);
                memcpy(out_buf, &raw_seq, 4);
                memcpy(out_buf + 4, jitter_buffer[idx].payload, 160);
                sendto(out_fd, out_buf, 164, 0, (struct sockaddr *)player_addr, sizeof(*player_addr));
                jitter_buffer[idx].valid = 0; // Clear slot
            }
            (*next_expected_seq)++;
        } else {
            break;
        }
    }
}

// Deliver sequentially complete frames to the player immediately
void deliver_ready_frames(uint32_t *next_expected_seq, int out_fd, struct sockaddr_in *player_addr) {
    while (1) {
        uint32_t seq = *next_expected_seq;
        uint32_t idx = seq % BUFFER_SIZE;
        if (jitter_buffer[idx].valid && jitter_buffer[idx].seq == seq) {
            unsigned char out_buf[164];
            uint32_t raw_seq = htonl(seq);
            memcpy(out_buf, &raw_seq, 4);
            memcpy(out_buf + 4, jitter_buffer[idx].payload, 160);
            sendto(out_fd, out_buf, 164, 0, (struct sockaddr *)player_addr, sizeof(*player_addr));
            jitter_buffer[idx].valid = 0; // Clear slot
            (*next_expected_seq)++;
        } else {
            break;
        }
    }
}

// Highly Aggressive Multi-Try ARQ Engine
void process_pending_nacks(double now) {
    for (int i = 0; i < BUFFER_SIZE; i++) {
        if (missing_list[i].active) {
            uint32_t s = missing_list[i].seq;
            uint32_t idx = s % BUFFER_SIZE;
            
            // Clean up immediately if the packet has arrived
            if (jitter_buffer[idx].valid && jitter_buffer[idx].seq == s) {
                missing_list[i].active = 0;
                continue;
            }
            
            // 1. Fire the FIRST NACK the absolute microsecond we notice a gap!
            if (missing_list[i].retry_count == 0) {
                send_nack(s);
                missing_list[i].last_nack_at = now;
                missing_list[i].retry_count = 1;
            } 
            // 2. Aggressive Rapid-Fire Retry (every 12ms)
            else if (missing_list[i].retry_count < MAX_NACK_RETRIES) {
                if (now - missing_list[i].last_nack_at >= 0.012) {
                    send_nack(s);
                    missing_list[i].last_nack_at = now;
                    missing_list[i].retry_count++;
                }
            }
        }
    }
}

int main(void) {
    if (getenv("T0")) t0 = atof(getenv("T0"));
    if (getenv("DELAY_MS")) delay_ms = atof(getenv("DELAY_MS"));

    int in_fd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in in_addr = {0};
    in_addr.sin_family = AF_INET;
    in_addr.sin_port = htons(47002);
    in_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (bind(in_fd, (struct sockaddr *)&in_addr, sizeof in_addr) < 0) {
        perror("bind 47002");
        return 1;
    }

    out_fd = socket(AF_INET, SOCK_DGRAM, 0);
    player_addr.sin_family = AF_INET;
    player_addr.sin_port = htons(47020);
    player_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    feedback_fd = socket(AF_INET, SOCK_DGRAM, 0);
    relay_feedback.sin_family = AF_INET;
    relay_feedback.sin_port = htons(47003);
    relay_feedback.sin_addr.s_addr = inet_addr("127.0.0.1");

    memset(jitter_buffer, 0, sizeof(jitter_buffer));
    memset(missing_list, 0, sizeof(missing_list));

    struct pollfd fds[1];
    fds[0].fd = in_fd;
    fds[0].events = POLLIN;

    unsigned char buf[2048];
    uint32_t next_expected_seq = 0;
    uint32_t max_seen_seq = 0;
    int has_seen_any = 0;

    for (;;) {
        int ret = poll(fds, 1, 1);
        if (ret < 0) {
            perror("poll");
            continue;
        }

        if (ret > 0 && (fds[0].revents & POLLIN)) {
            ssize_t n = recvfrom(in_fd, buf, sizeof buf, 0, NULL, NULL);
            if (n >= 164) {
                uint32_t raw_seq;
                memcpy(&raw_seq, buf, 4);
                uint32_t seq = ntohl(raw_seq);

                uint32_t idx = seq % BUFFER_SIZE;
                jitter_buffer[idx].seq = seq;
                memcpy(jitter_buffer[idx].payload, buf + 4, 160);
                jitter_buffer[idx].valid = 1;

                if (n == 324) {
                    uint32_t prev_seq = seq - 1;
                    uint32_t prev_idx = prev_seq % BUFFER_SIZE;
                    if (!jitter_buffer[prev_idx].valid || jitter_buffer[prev_idx].seq != prev_seq) {
                        jitter_buffer[prev_idx].seq = prev_seq;
                        memcpy(jitter_buffer[prev_idx].payload, buf + 164, 160);
                        jitter_buffer[prev_idx].valid = 1;
                    }
                }

                if (!has_seen_any) {
                    if (seq > 0) {
                        for (uint32_t s = 0; s < seq; s++) {
                            uint32_t m_idx = s % BUFFER_SIZE;
                            missing_list[m_idx].seq = s;
                            missing_list[m_idx].retry_count = 0;
                            missing_list[m_idx].active = 1;
                        }
                    }
                    max_seen_seq = seq;
                    has_seen_any = 1;
                } else if (seq > max_seen_seq) {
                    for (uint32_t s = max_seen_seq + 1; s < seq; s++) {
                        if (s == seq - 1 && n == 324) continue;
                        
                        uint32_t m_idx = s % BUFFER_SIZE;
                        missing_list[m_idx].seq = s;
                        missing_list[m_idx].retry_count = 0;
                        missing_list[m_idx].active = 1;
                    }
                    max_seen_seq = seq;
                }
            }
        }

        double now = get_current_time_sec();
        process_pending_nacks(now);
        flush_expired_frames(now, &next_expected_seq, t0, delay_ms, out_fd, &player_addr);
        deliver_ready_frames(&next_expected_seq, out_fd, &player_addr);
    }
    return 0;
}