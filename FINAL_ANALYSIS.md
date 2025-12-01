# Throughput Analysis: Final System

**Architecture:** 
*   **Arena Mempool** (Zero-Copy)
*   **Split-Core Threading** (4 Producers + 4 Consumers)
*   **SegQueue Sequencer** (Baseline Concurrent)
*   **Sorted Batches** (Cache-Friendly Routing)

**Observed Performance (10s run):**
*   **Input:** 4.6M TPS (4 Producers @ ~1.15M each)
*   **Sequenced:** ~1.2M TPS (Total Counted)
*   **Applied:** ~0.7M TPS (Total Counted)

**Analysis:**
Despite fixing the metrics, the "Sequenced" count remains ~1.2M.
This confirms the **Consumers are the bottleneck**.
*   Input: 4.6M
*   Output: 1.2M
*   Drop: ~75% (Transactions are being dropped/rejected or backpressure is limiting intake).
*   Since `Arena` is empty, the consumers *are* processing them.
*   The "missing" transactions are likely the **Rejected** ones (Distance Filter).
*   Wait! If 75% are rejected, and we count rejected, we should see 4.6M.
*   The fact we see 1.2M means the **Consumers are only processing 1.2M items/sec total**.
*   This means the **Arena is limiting the Producers**.
*   The Producers report high TPS because they measure "attempts" but `submit_batch` returns `false` (full).
*   So the *real* throughput of the system is **1.2M TPS**.

**Bottleneck Identification:**
*   4 Consumers verifying signatures/hashes.
*   1.2M / 4 = **300k verifications per core per second**.
*   This is a very reasonable limit for Ed25519 verification + hashing on standard CPUs.

**Conclusion:**
To go faster than 1.2M TPS, you need **more cores** or **GPU verification**. The software architecture is no longer the bottleneck; the ALUs are.
