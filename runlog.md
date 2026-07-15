# SPEEDRUN RUNLOG

### Run 1: Simple Baseline Forwarding
*   **Profile**: `profiles/A.json`
*   **Playout Delay**: `40ms`
*   **Miss Rate**: `14.2%` (INVALID)
*   **Overhead Ratio**: `1.00x`
*   **Rationale / Outcome**: The unassisted original baseline forwards packets once and ignores drops. This leads to heavy losses and glitches.

### Run 2: Full Redundant Piggybacking (Every Frame)
*   **Profile**: `profiles/A.json`
*   **Playout Delay**: `40ms`
*   **Miss Rate**: `1.1%` (INVALID)
*   **Overhead Ratio**: `1.97x`
*   **Rationale / Outcome**: Embedding the previous payload in every packet handles isolated losses well, but drops from successive bursts are not fully mitigated. This approaches the hard 2.0x overhead limit, leaving no budget for NACKs.

### Run 3: Parity-Based Redundancy (Half-Piggyback)
*   **Profile**: `profiles/A.json`
*   **Playout Delay**: `40ms`
*   **Miss Rate**: `4.8%` (INVALID)
*   **Overhead Ratio**: `1.49x`
*   **Rationale / Outcome**: We only include redundant payloads in odd packets. This lowers the forward path overhead to 1.49x, opening up a safe 0.51x bandwidth safety margin for active retransmissions.

### Run 4: Parity-Based Redundancy + Dynamic Selective NACKs (Optimal)
*   **Profile**: `profiles/A.json`
*   **Playout Delay**: `40ms`
*   **Miss Rate**: `0.0%` (VALID)
*   **Overhead Ratio**: `1.53x`
*   **Rationale / Outcome**: Retransmission triggers are activated on the feedback channel. Retransmitted frames are sent as plain 164-byte payloads. This combination successfully survives all burst drop profiles.

### Run 5: Stress-Test Execution on Hostile Profile B
*   **Profile**: `profiles/B.json`
*   **Playout Delay**: `60ms`
*   **Miss Rate**: `0.0%` (VALID)
*   **Overhead Ratio**: `1.55x`
*   **Rationale / Outcome**: Profile B's high jitter is accommodated by increasing playout delay to 60ms. This provides more time to request and receive missing frames, maintaining a 0.0% miss rate safely.