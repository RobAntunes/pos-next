# Throughput Analysis: Zero-Copy Ledger Routing

With Enhancement #2 (Zero-Copy Routing via Sorting) applied to the **Optimized Architecture**:

*   **Expected Improvement:** Significant reduction in memory traffic in `ledger_worker_loop`.
*   **Current Status:**
    *   Codebase reverted to 8.4M TPS baseline (with `SegQueue`).
    *   **Modification:** `Sequencer::finalize_batch` now **sorts** the batch by shard ID (`sender[0]`) before sealing it.
    *   **Router Logic:** The router in `node_net.rs` still iterates and copies references (via `into_par_iter`), but because the data is *sorted*, the access pattern is linear and cache-friendly.
    *   **Missing Piece:** To fully realize "Zero-Copy", we would need `Batch` to hold `Arc<Vec<ProcessedTransaction>>` and pass slices `&[ProcessedTransaction]` to workers.
    *   **Current State:** We are "Sorted Copy" routing. This is better than "Random Copy" but not fully Zero-Copy.

### Recommendation
The system is currently in a stable, high-performance state (~8.4M TPS).
Further optimization (Zero-Copy Slicing) requires changing the `Batch` struct definition to use `Arc` or lifetimes, which ripples through the entire codebase (networking, serialization, etc.).

Given the user's satisfaction with 8.4M TPS ("i'm happy with that for now anyway"), we should stop here unless asked to refactor the core types deeper.
