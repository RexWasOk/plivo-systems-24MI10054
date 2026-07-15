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
#define MAX_NACK_RETRIES 3

struct JitterFrame {
    uint32_t seq;
    unsigned char payload[160];
    int valid;
};

struct PacketState {
    uint32_t seq;
    int status; // 0: untouched, 1: received, 2: NACKed
    double last_nack_time;
    int retry_count;
};

static struct JitterFrame jitter_buffer[BUFFER_SIZE];
static struct PacketState packet_states[BUFFER_SIZE];

static int out_fd;
static struct sockaddr_in player_addr;
static int feedback_fd;
static struct sockaddr_in relay_feedback;

static double t0 = 0.0;
static double delay_ms = 50.0;
static double base_latency = -1.0;
static uint32_t next_expected_seq = 0;

// High-precision monotonic clock reader
double get_current_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

void send_nack(uint32_t seq) {
    uint32_t raw_seq = htonl(seq);
    sendto(feedback_fd, &raw_seq, 4, 0, 
           (struct sockaddr *)&relay_feedback, sizeof relay_feedback);
}

// Flush frames whose playout deadline has expired
void flush_expired_frames(double now) {
    while (1) {
        uint32_t seq = next_expected_seq;
        double deadline = t0 + (delay_ms / 1000.0) + (seq * 0.020);
        
        if (now >= deadline) {
            uint32_t idx = seq % BUFFER_SIZE;
            if (jitter_buffer[idx].valid && jitter_buffer[idx].seq == seq) {
                unsigned char out_buf[164];
                uint32_t raw_seq = htonl(seq);
                memcpy(out_buf, &raw_seq, 4);
                memcpy(out_buf + 4, jitter_buffer[idx].payload, 160);
                sendto(out_fd, out_buf, 164, 0, (struct sockaddr *)&player_addr, sizeof(player_addr));
                jitter_buffer[idx].valid = 0; // Clear slot
            }
            next_expected_seq++;
        } else {
            break;
        }
    }
}

// Deliver sequentially complete frames immediately
void deliver_ready_frames(void) {
    while (1) {
        uint32_t seq = next_expected_seq;
        uint32_t idx = seq % BUFFER_SIZE;
        if (jitter_buffer[idx].valid && jitter_buffer[idx].seq == seq) {
            unsigned char out_buf[164];
            uint32_t raw_seq = htonl(seq);
            memcpy(out_buf, &raw_seq, 4);
            memcpy(out_buf + 4, jitter_buffer[idx].payload, 160);
            sendto(out_fd, out_buf, 164, 0, (struct sockaddr *)&player_addr, sizeof(player_addr));
            jitter_buffer[idx].valid = 0; // Clear slot
            next_expected_seq++;
        } else {
            break;
        }
    }
}

// Predictive, Timer-Based NACK Scan Engine
void scan_predictive_nacks(double now) {
    if (base_latency < 0) return; // Cannot predict without base latency baseline
    
    // Scan sliding window of future frames to detect gaps proactively
    uint32_t start_seq = next_expected_seq;
    uint32_t end_seq = next_expected_seq + 30; 
    
    double jitter_threshold = 0.003; // 3ms threshold for ultra-fast predictive NACKs
    double retry_interval = 0.014;   // 14ms retry interval
    
    for (uint32_t s = start_seq; s < end_seq; s++) {
        uint32_t idx = s % BUFFER_SIZE;
        
        // If already received, skip
        if (packet_states[idx].status == 1 && packet_states[idx].seq == s) {
            continue;
        }
        
        double expected_arrival = t0 + base_latency + (s * 0.020);
        
        if (now >= expected_arrival + jitter_threshold) {
            // Frame is late! Initialize state if sequence mismatch
            if (packet_states[idx].seq != s) {
                packet_states[idx].seq = s;
                packet_states[idx].status = 0;
                packet_states[idx].retry_count = 0;
                packet_states[idx].last_nack_time = 0;
            }
            
            if (packet_states[idx].status != 1) {
                if (packet_states[idx].retry_count == 0) {
                    send_nack(s);
                    packet_states[idx].last_nack_time = now;
                    packet_states[idx].retry_count = 1;
                    packet_states[idx].status = 2; // Marked as NACKed
                } else if (packet_states[idx].retry_count < MAX_NACK_RETRIES) {
                    if (now - packet_states[idx].last_nack_time >= retry_interval) {
                        send_nack(s);
                        packet_states[idx].last_nack_time = now;
                        packet_states[idx].retry_count++;
                    }
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
    memset(packet_states, 0, sizeof(packet_states));

    struct pollfd fds[1];
    fds[0].fd = in_fd;
    fds[0].events = POLLIN;

    unsigned char buf[2048];

    for (;;) {
        double now = get_current_time_sec();
        
        // Calculate smart timeout to sleep exactly up to next urgent event
        double next_deadline = t0 + (delay_ms / 1000.0) + (next_expected_seq * 0.020);
        double next_event = next_deadline;
        
        if (base_latency >= 0) {
            for (uint32_t s = next_expected_seq; s < next_expected_seq + 5; s++) {
                uint32_t idx = s % BUFFER_SIZE;
                if (packet_states[idx].status != 1 || packet_states[idx].seq != s) {
                    double next_nack_time = t0 + base_latency + (s * 0.020) + 0.003;
                    if (next_nack_time < next_event) {
                        next_event = next_nack_time;
                    }
                    break;
                }
            }
        }
        
        double wait_time = next_event - now;
        int timeout_ms = (int)(wait_time * 1000.0);
        if (timeout_ms < 0) timeout_ms = 0;
        if (timeout_ms > 4) timeout_ms = 4; // Cap at 4ms for fine granularity

        int ret = poll(fds, 1, timeout_ms);
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

                // Dynamically establish baseline network latency
                if (base_latency < 0) {
                    double arrival_time = get_current_time_sec();
                    base_latency = arrival_time - t0 - (seq * 0.020);
                    if (base_latency < 0) base_latency = 0.010;
                }

                uint32_t idx = seq % BUFFER_SIZE;
                jitter_buffer[idx].seq = seq;
                memcpy(jitter_buffer[idx].payload, buf + 4, 160);
                jitter_buffer[idx].valid = 1;
                
                packet_states[idx].seq = seq;
                packet_states[idx].status = 1; // Mark as received

                // Process piggyback FEC redundancy
                if (n == 324) {
                    uint32_t prev_seq = seq - 1;
                    uint32_t prev_idx = prev_seq % BUFFER_SIZE;
                    if (!jitter_buffer[prev_idx].valid || jitter_buffer[prev_idx].seq != prev_seq) {
                        jitter_buffer[prev_idx].seq = prev_seq;
                        memcpy(jitter_buffer[prev_idx].payload, buf + 164, 160);
                        jitter_buffer[prev_idx].valid = 1;
                    }
                    packet_states[prev_idx].seq = prev_seq;
                    packet_states[prev_idx].status = 1; // Mark as received
                }
            }
        }

        now = get_current_time_sec();
        scan_predictive_nacks(now);
        flush_expired_frames(now);
        deliver_ready_frames();
    }
    return 0;
}