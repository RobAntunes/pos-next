# Optimization Analysis: Main vs Stashed "Distributed" Implementation

## Comparison Overview
The "later version" (stashed changes) introduces a radical architectural shift designed for "double digit million TPS" via zero-copy arena allocation and lock-free concurrency. However, the reported regression (98K → 46K TPS) is likely caused by thread oversubscription and undefined behavior.

### 1. Architecture Shift
| Feature | Main / Distributed (Current) | Optimization Attempt (Stash) |
|---------|------------------------------|------------------------------|
| **Mempool** | `SegQueue` + `DashSet` (Dynamic) | `ArenaMempool` (Fixed, Zero-Copy) |
| **Concurrency** | Single Consumer Thread | Parallel Consumers (N = CPUs) |
| **Hashing** | Eager (at Creation / Producer) | Lazy (`OnceLock` / Consumer) |
| **Topology** | 1:N (Producers:Consumer) | M:N Partitioned (Worker Pairs) |

## Root Causes of Regression

### 1. Thread Oversubscription (Critical)
The stashed `node_net.rs` spawns:
- `num_cpus` Producers
- `num_cpus` Consumers
- `Ledger` threads
- `Quic` runtime

On an 8-core machine, this creates **16+ CPU-bound threads**. The OS scheduler thrashes, causing massive context switching overhead. The "Main" branch uses 1 Consumer, leaving cores free for Producers.
**Impact:** Cache thrashing and lock contention (even in "lock-free" code).

### 2. Lazy Hashing Shift
- **Main:** Producers compute BLAKE3 hash. Consumers just read it.
- **Stash:** `Transaction::new` uses `OnceLock` (Lazy). Hash is computed **on first access** (in the Consumer).
- **Result:** The heavy hashing load moved from Producers (which had spare capacity) to Consumers (which are now the bottleneck).

### 3. Undefined Behavior (Safety)
- `ArenaMempool` uses `std::ptr::copy_nonoverlapping` (memcpy) to move Transactions.
- `Transaction` now contains `std::sync::OnceLock`.
- **Fatal Flaw:** `OnceLock` is **not** safely movable via memcpy if it contains self-referential state or specific memory layouts. This is Undefined Behavior (UB) and can lead to silent corruption or performance degradation due to invalid atomic states.

## Recommendations for >10M TPS

1.  **Fix Threading:** Set `Producers + Consumers <= num_cpus`. (e.g., 4 Prod + 4 Cons).
2.  **Restore Eager Hashing:** Compute hash in Producer to utilize their cycles.
3.  **Safe Arena:** Remove `OnceLock` from `Transaction` if using raw memcpy, or use `Clone`.
4.  **Batch Size:** Ensure `burst_size` exactly matches `ZONE_SIZE` (45,000) to prevent fragmentation.
