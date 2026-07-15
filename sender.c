/* BASELINE SENDER (C) — naive on purpose. Rewrite it (C, C++, Go, or Rust).
 *
 * Ports (all 127.0.0.1):
 *   bind 47010  <- harness source delivers frame i here at t0 + i*20ms
 *                  (format: 4-byte big-endian seq + 160-byte payload)
 *   send 47001  -> relay uplink toward the receiver (YOUR wire format)
 *   bind 47004  <- feedback from your receiver, via the relay (optional)
 *
 * This baseline forwards each frame once, unchanged, and ignores feedback.
 * No redundancy, no retransmission. It cannot pass. That is the point.
 *
 * Env vars available if you want them: T0 (epoch seconds, float),
 * DURATION_S, DELAY_MS. The harness kills this process when the run ends,
 * so a forever-loop is fine.
 *
 * build: make        run: python3 run.py --delay_ms 60
 */
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <poll.h>

#define CACHE_SIZE 4096

// Simple ring buffer to cache previous frames for potential retransmission
struct CachedFrame {
    uint32_t seq;
    unsigned char payload[160];
    int valid;
};

static struct CachedFrame frame_cache[CACHE_SIZE];

int main(void) {
    int in_fd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in in_addr = {0};
    in_addr.sin_family = AF_INET;
    in_addr.sin_port = htons(47010);
    in_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (bind(in_fd, (struct sockaddr *)&in_addr, sizeof in_addr) < 0) {
        perror("bind 47010");
        return 1;
    }

    //Bind to port 47004 to receive NACKs/feedback from the receiver (new change by me)

    int feedback_fd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in feedback_addr = {0};

    feedback_addr.sin_family = AF_INET;
    feedback_addr.sin_port = htons(47004);
    feedback_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (bind(feedback_fd, (struct sockaddr *)&feedback_addr, sizeof feedback_addr) < 0) {
        perror("bind 47004");
        return 1;
    }

    int out_fd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in relay = {0};
    relay.sin_family = AF_INET;
    relay.sin_port = htons(47001);
    relay.sin_addr.s_addr = inet_addr("127.0.0.1");

    // Initialize the frame cache structure (by me)
    memset(frame_cache, 0, sizeof(frame_cache));

    // Setup poll to monitor both the harness input and feedback sockets (by me)
    struct pollfd fds[2];
    fds[0].fd = in_fd;
    fds[0].events = POLLIN;
    fds[1].fd = feedback_fd;
    fds[1].events = POLLIN;

    unsigned char buf[2048];

    for (;;) {

        // Block until either socket has data ready
        int ret = poll(fds, 2, -1);

        if (ret < 0) {
            perror("poll error");
            continue;
        }

        // PATH A: incoming frame from the harness (47010)

        if (fds[0].revents & POLLIN) {
            ssize_t n = recvfrom(in_fd, buf, sizeof buf, 0, NULL, NULL);
            if (n == 164) { // Valid structure: 4B seq + 160B payload
                uint32_t raw_seq;
                memcpy(&raw_seq, buf, 4);
                uint32_t seq = ntohl(raw_seq);

                // Cache the frame payload for future NACK retransmissions
                uint32_t cache_idx = seq % CACHE_SIZE;
                frame_cache[cache_idx].seq = seq;
                memcpy(frame_cache[cache_idx].payload, buf + 4, 160);
                frame_cache[cache_idx].valid = 1;

                if (seq % 2 == 0) {
                    // EVEN sequence numbers: send only the current packet (164 bytes)
                    sendto(out_fd, buf, 164, 0, (struct sockaddr *)&relay, sizeof relay);
                } else {
                    // ODD sequence numbers: piggyback the previous frame (324 bytes)
                    unsigned char odd_packet[324];
                    memcpy(odd_packet, buf, 164); // Copy current frame (seq + payload)

                    uint32_t prev_seq = seq - 1;
                    uint32_t prev_idx = prev_seq % CACHE_SIZE;

                    if (frame_cache[prev_idx].valid && frame_cache[prev_idx].seq == prev_seq) {
                        memcpy(odd_packet + 164, frame_cache[prev_idx].payload, 160);
                    } else {
                        memset(odd_packet + 164, 0, 160); // Zero fill if missing
                    }

                    sendto(out_fd, odd_packet, 324, 0, (struct sockaddr *)&relay, sizeof relay);
                }
            }
        }

        // PATH B: retransmission feedback request (47004)

        if (fds[1].revents & POLLIN) {
            unsigned char feedback_buf[4];
            ssize_t n = recvfrom(feedback_fd, feedback_buf, sizeof feedback_buf, 0, NULL, NULL);
            if (n == 4) {
                uint32_t raw_req_seq;
                memcpy(&raw_req_seq, feedback_buf, 4);
                uint32_t req_seq = ntohl(raw_req_seq);

                uint32_t req_idx = req_seq % CACHE_SIZE;
                if (frame_cache[req_idx].valid && frame_cache[req_idx].seq == req_seq) {
                    // Build retransmission payload: seq (4B) + cached payload (160B)
                    unsigned char retx_packet[164];
                    uint32_t raw_seq_be = htonl(req_seq);
                    memcpy(retx_packet, &raw_seq_be, 4);
                    memcpy(retx_packet + 4, frame_cache[req_idx].payload, 160);

                    // Send plain 164 bytes to stay within strict bandwidth bounds
                    sendto(out_fd, retx_packet, 164, 0, (struct sockaddr *)&relay, sizeof relay);
                }
            }
        }
    }

    return 0;
}
