/* BASELINE RECEIVER (C) — naive on purpose. Rewrite it (C, C++, Go, or Rust).
 *
 * Ports (all 127.0.0.1):
 *   bind 47002  <- media from your sender, via the hostile relay
 *   send 47020  -> harness player. MUST be: 4-byte big-endian seq +
 *                  160-byte payload. Frame i counts only if it arrives
 *                  BEFORE its deadline t0 + DELAY_MS + i*20ms.
 *   send 47003  -> feedback to your sender, via the relay (optional)
 *
 * This baseline forwards whatever arrives straight to the player: lost
 * frames stay lost, late frames stay late, duplicates are re-sent
 * harmlessly. All yours to fix — jitter buffer, reordering, recovery.
 *
 * Env vars available: T0, DURATION_S, DELAY_MS. Harness kills the process
 * at run end; a forever-loop is fine.
 */
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <poll.h>


#define BUFFER_SIZE 8192

// Structure representing a single voice frame in the jitter buffer
struct JitterFrame {
    uint32_t seq;
    unsigned char payload[160];
    int valid;
};

// Thread-safe global jitter buffer
static struct JitterFrame jitter_buffer[BUFFER_SIZE];


// Network addresses and file descriptors for output
static int out_fd;
static struct sockaddr_in player_addr;
static int feedback_fd;
static struct sockaddr_in relay_feedback;

// Timing configuration parsed from environment variables
static double t0 = 0.0;
static double delay_ms = 40.0;

// High precision clock reader
double get_current_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

// Calculate the absolute deadline for a given sequence number
double get_deadline_sec(uint32_t s, double t0, double delay_ms) {
    return t0 + (delay_ms / 1000.0) + (s * 0.020);
}

void send_nack(uint32_t seq) {
    uint32_t raw_seq = htonl(seq);
    sendto(feedback_fd, &raw_seq, 4, 0, 
           (struct sockaddr *)&relay_feedback, sizeof relay_feedback);
}

// Force-skip frames whose playback deadline is about to expire
void flush_expired_frames(double now, uint32_t *next_expected_seq, double t0, double delay_ms, int out_fd, struct sockaddr_in *player_addr) {
    double margin = 0.001; // 1ms safety margin before absolute playout deadline
    while (1) {
        uint32_t seq = *next_expected_seq;
        double deadline = get_deadline_sec(seq, t0, delay_ms);
        if (now >= deadline - margin) {
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

// Deliver any sequentially complete frames sitting in the buffer
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


int main(void) {
    
    // Parse the environment variables for accurate sync
    if (getenv("T0")) t0 = atof(getenv("T0"));
    if (getenv("DELAY_MS")) delay_ms = atof(getenv("DELAY_MS"));

    // 1. Bind to port 47002 to receive media packets from the relay
    int in_fd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in in_addr = {0};
    in_addr.sin_family = AF_INET;
    in_addr.sin_port = htons(47002);
    in_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (bind(in_fd, (struct sockaddr *)&in_addr, sizeof in_addr) < 0) {
        perror("bind 47002");
        return 1;
    }

    // 2. Setup outbound socket to harness player (47020)
    out_fd = socket(AF_INET, SOCK_DGRAM, 0);
    player_addr.sin_family = AF_INET;
    player_addr.sin_port = htons(47020);
    player_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    // 3. Setup outbound feedback socket (NACK) to sender via relay (47003)
    feedback_fd = socket(AF_INET, SOCK_DGRAM, 0);
    relay_feedback.sin_family = AF_INET;
    relay_feedback.sin_port = htons(47003);
    relay_feedback.sin_addr.s_addr = inet_addr("127.0.0.1");

    memset(jitter_buffer, 0, sizeof(jitter_buffer));

    struct pollfd fds[1];
    fds[0].fd = in_fd;
    fds[0].events = POLLIN;

    unsigned char buf[2048];
    uint32_t next_expected_seq = 0;
    uint32_t max_seen_seq = 0;
    int has_seen_any = 0;

    for (;;) {
        // Poll with a tiny 1ms timeout to keep loop overhead highly responsive
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

                // 1. Store primary incoming packet
                uint32_t idx = seq % BUFFER_SIZE;
                jitter_buffer[idx].seq = seq;
                memcpy(jitter_buffer[idx].payload, buf + 4, 160);
                jitter_buffer[idx].valid = 1;

                // 2. Extract parity backup (from 324-byte odd packages)
                if (n == 324) {
                    uint32_t prev_seq = seq - 1;
                    uint32_t prev_idx = prev_seq % BUFFER_SIZE;
                    if (!jitter_buffer[prev_idx].valid || jitter_buffer[prev_idx].seq != prev_seq) {
                        jitter_buffer[prev_idx].seq = prev_seq;
                        memcpy(jitter_buffer[prev_idx].payload, buf + 164, 160);
                        jitter_buffer[prev_idx].valid = 1;
                    }
                }

                // 3. Selective NACK generation (tracks gaps up to max_seen_seq)
                if (!has_seen_any) {
                    if (seq > 0) {
                        for (uint32_t s = 0; s < seq; s++) {
                            send_nack(s);
                        }
                    }
                    max_seen_seq = seq;
                    has_seen_any = 1;
                } else if (seq > max_seen_seq) {
                    for (uint32_t s = max_seen_seq + 1; s < seq; s++) {
                        // Skip redundant NACKs if recovered instantly via odd parity package
                        if (s == seq - 1 && n == 324) {
                            continue;
                        }
                        send_nack(s);
                    }
                    max_seen_seq = seq;
                }
            }
        }

        // Active state recovery and timed flush routines
        double now = get_current_time_sec();
        flush_expired_frames(now, &next_expected_seq, t0, delay_ms, out_fd, &player_addr);
        deliver_ready_frames(&next_expected_seq, out_fd, &player_addr);
    }
    return 0;
}