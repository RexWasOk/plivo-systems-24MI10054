# Project Runlog: Stream Optimizer

**Project Context:** Optimization of UDP-based packet delivery.
**Goal:** Achieve < 1.00% deadline misses and < 2.00x bandwidth overhead.

## Summary Table

| Run | Date | Strategy | Delay | Miss Rate | Overhead | Result |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 01 | 2026-07-15 | Baseline Forwarding | 50ms | 1.67% | - | INVALID |
| 02 | 2026-07-15 | Predictive NACK Logic | 50ms | 22.00% | - | INVALID |
| 03 | 2026-07-15 | Multi-Retry ARQ Engine | 80ms | 0.67% | 1.70x | VALID |

---

## Detailed Log

### Run 01: Baseline Establishment
* **Parameters:** Default, 50ms delay.
* **Outcome:** The initial code functioned as a standard forwarder. While close to the target, it failed to provide the necessary reliability for the 1% miss threshold.
* **Learnings:** Baseline performance is insufficient for bursty network conditions.

### Run 02: Predictive Logic Attempt (Regression)
* **Parameters:** 50ms delay, dynamic latency prediction.
* **Outcome:** Failed significantly (22.00% miss rate).
* **Rationale:** The predictive algorithm attempted to estimate network jitter based on early packets. This caused the event loop to miscalculate, triggering unnecessary NACKs and saturating the processing queue.
* **Learnings:** Avoid complex latency guessing; stick to deterministic, absolute-time ARQ models.

### Run 03: Multi-Retry ARQ Engine (Final Success)
* **Parameters:** 80ms delay.
* **Code Changes:**
    * Implemented aggressive `#define MAX_NACK_RETRIES 10`.
    * Tightened feedback loop with `#define NACK_RETRY_INTERVAL_SEC 0.003` (3ms).
    * Integrated a "Double-NACK" burst on initial loss detection.
* **Outcome:** **VALID** status achieved.
* **Observations:** Increasing the playout delay to 80ms provided enough buffer headroom for the retransmission loop to stabilize without exceeding the 2.00x bandwidth overhead cap.