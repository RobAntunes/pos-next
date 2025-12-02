//! POS Node Runner - Real-time TPS Benchmark
//!
//! This binary runs a sequencer node and displays real-time throughput metrics.
//! Usage: cargo run --release

// Use mimalloc for concurrent allocation (breaks the allocator lock bottleneck)
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pos::{
    Sequencer, SequencerConfig, Transaction, TransactionPayload, DEFAULT_BATCH_SIZE, MAX_DISTANCE,
};
use rand::RngCore;
use rayon::prelude::*;
use tracing_subscriber::EnvFilter;

/// Batch size optimized for L3 cache (~32MB)
/// Formula: (L3_SIZE * 0.5) / TX_SIZE ≈ (32MB * 0.5) / 350 bytes ≈ 45,000
const TX_BATCH_SIZE: usize = 45_000;

/// Number of pre-generated batches (pool size)
/// This eliminates generator overhead during benchmarking
const PREGENERATED_BATCHES: usize = 20;

/// Duration to run the benchmark
const BENCHMARK_DURATION: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║           POS Protocol - Sequencer Node Benchmark             ║");
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!("║  Layer 1 (Structure): BLAKE3-only transaction processing      ║");
    println!("║  Target: 30M+ TPS                                             ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();

    // Create sequencer with default config
    let config = SequencerConfig {
        sequencer_id: generate_random_id(),
        signing_key: generate_random_id(),
        batch_size: DEFAULT_BATCH_SIZE,
        max_range: MAX_DISTANCE,
        ..Default::default()
    };

    let sequencer = Arc::new(Sequencer::new(config));

    // Get number of CPU cores
    let num_cpus = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);

    println!("🚀 Starting PARALLEL benchmark with mimalloc...");
    println!("   CPU cores: {} (rayon will use all)", num_cpus);
    println!("   Batch size: {} (L3 cache optimized)", TX_BATCH_SIZE);
    println!("   Duration: {}s", BENCHMARK_DURATION.as_secs());
    println!();

    // PRE-GENERATE test data pool to eliminate generator overhead
    println!(
        "📦 Pre-generating {} batches ({} transactions)...",
        PREGENERATED_BATCHES,
        PREGENERATED_BATCHES * TX_BATCH_SIZE
    );
    let gen_start = Instant::now();

    let tx_pool: Vec<Vec<Transaction>> = (0..PREGENERATED_BATCHES)
        .into_par_iter()
        .map(|batch_idx| {
            (0..TX_BATCH_SIZE)
                .map(|i| generate_transaction((batch_idx * TX_BATCH_SIZE + i) as u64))
                .collect()
        })
        .collect();

    println!("   Generated in {:.2}s", gen_start.elapsed().as_secs_f64());
    println!();

    // Shared counters
    let running = Arc::new(AtomicBool::new(true));
    let total_processed = Arc::new(AtomicU64::new(0));
    let total_rejected = Arc::new(AtomicU64::new(0));

    // Share the pre-generated pool
    let tx_pool = Arc::new(tx_pool);

    // Metrics display task
    let running_metrics = Arc::clone(&running);
    let processed_metrics = Arc::clone(&total_processed);

    // OPTIMAL CONFIGURATION: Single feeder worker
    // Rayon handles internal parallelism across all 8 cores.
    // Multiple feeders cause context-switching overhead (64 threads on 8 cores).
    let num_workers = 1;
    println!("   Workers: {} (single feeder + rayon pool)", num_workers);
    println!();

    let mut handles = Vec::new();

    for worker_id in 0..num_workers {
        let running = Arc::clone(&running);
        let total_processed = Arc::clone(&total_processed);
        let total_rejected = Arc::clone(&total_rejected);
        let tx_pool = Arc::clone(&tx_pool);
        let sequencer = Arc::clone(&sequencer);

        let handle = tokio::task::spawn_blocking(move || {
            let mut local_processed = 0u64;
            let mut local_rejected = 0u64;
            // Stagger start indices (relevant if we had multiple workers)
            let mut batch_idx = worker_id;

            while running.load(Ordering::Relaxed) {
                // ZERO-COPY: Get reference to pre-generated data
                let txs = &tx_pool[batch_idx % PREGENERATED_BATCHES];
                batch_idx = batch_idx.wrapping_add(num_workers);

                // Process using lock-free reduction on the SHARED sequencer
                let (accepted, rejected) = sequencer.process_batch_ref(txs);

                // Check if batch is ready and finalize to free memory
                if sequencer.batch_ready() {
                    let _ = sequencer.finalize_batch();
                }

                local_processed += accepted as u64;
                local_rejected += rejected as u64;

                // Batch updates to shared counters
                if batch_idx % 10 == 0 {
                    total_processed.fetch_add(local_processed, Ordering::Relaxed);
                    total_rejected.fetch_add(local_rejected, Ordering::Relaxed);
                    local_processed = 0;
                    local_rejected = 0;
                }
            }
            // Final flush
            total_processed.fetch_add(local_processed, Ordering::Relaxed);
            total_rejected.fetch_add(local_rejected, Ordering::Relaxed);
        });

        handles.push(handle);
    }

    let metrics_handle = tokio::spawn(async move {
        let start = Instant::now();
        let mut last_count = 0u64;
        let mut last_time = Instant::now();
        let mut peak_tps = 0.0f64;

        println!("┌─────────┬────────────────┬────────────────┬────────────────┬───────────────┐");
        println!("│ Elapsed │   Current TPS  │   Average TPS  │    Peak TPS    │ Total TX      │");
        println!("├─────────┼────────────────┼────────────────┼────────────────┼───────────────┤");

        while running_metrics.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let now = Instant::now();
            let elapsed = start.elapsed();
            let current_count = processed_metrics.load(Ordering::Relaxed);

            // Calculate current TPS (transactions in last interval)
            let interval = now.duration_since(last_time).as_secs_f64();
            let interval_txs = current_count.saturating_sub(last_count);
            let current_tps = if interval > 0.0 {
                interval_txs as f64 / interval
            } else {
                0.0
            };

            // Calculate average TPS
            let total_secs = elapsed.as_secs_f64();
            let avg_tps = if total_secs > 0.0 {
                current_count as f64 / total_secs
            } else {
                0.0
            };

            // Update peak
            if current_tps > peak_tps {
                peak_tps = current_tps;
            }

            // Format numbers with commas
            let current_tps_str = format_number(current_tps as u64);
            let avg_tps_str = format_number(avg_tps as u64);
            let peak_tps_str = format_number(peak_tps as u64);
            let total_str = format_number(current_count);

            println!(
                "│ {:>5.1}s  │ {:>14} │ {:>14} │ {:>14} │ {:>13} │",
                elapsed.as_secs_f64(),
                current_tps_str,
                avg_tps_str,
                peak_tps_str,
                total_str
            );

            last_count = current_count;
            last_time = now;
        }

        println!("└─────────┴────────────────┴────────────────┴────────────────┴───────────────┘");

        (peak_tps, processed_metrics.load(Ordering::Relaxed))
    });

    // Wait for benchmark duration
    tokio::time::sleep(BENCHMARK_DURATION).await;
    running.store(false, Ordering::Relaxed);

    // Wait for all workers to finish
    for handle in handles {
        let _ = handle.await;
    }

    let (peak_tps, _) = metrics_handle.await.unwrap();

    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                     BENCHMARK RESULTS                         ║");
    println!("╠═══════════════════════════════════════════════════════════════╣");

    let total_processed = total_processed.load(Ordering::Relaxed);
    let total_rejected = total_rejected.load(Ordering::Relaxed);
    let avg_tps = total_processed as f64 / BENCHMARK_DURATION.as_secs_f64();

    println!(
        "║  Total Transactions:  {:>40} ║",
        format_number(total_processed)
    );
    println!(
        "║  Rejected (distance): {:>40} ║",
        format_number(total_rejected)
    );
    println!(
        "║  Average TPS:         {:>40} ║",
        format_number(avg_tps as u64)
    );
    println!(
        "║  Peak TPS:            {:>40} ║",
        format_number(peak_tps as u64)
    );
    println!(
        "║  Duration:            {:>40} ║",
        format!("{}s", BENCHMARK_DURATION.as_secs())
    );
    println!("╚═══════════════════════════════════════════════════════════════╝");

    // Finalize any pending batch
    if let Some(batch) = sequencer.finalize_batch() {
        println!();
        println!(
            "Final batch: {} transactions, round {}",
            batch.header.tx_count, batch.header.round_id
        );
    }
}

/// Generate a random 32-byte ID
fn generate_random_id() -> [u8; 32] {
    let mut id = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut id);
    id
}

/// Generate a test transaction
fn generate_transaction(nonce: u64) -> Transaction {
    Transaction::new(
        [1u8; 32],
        TransactionPayload::Transfer {
            recipient: [2u8; 32],
            amount: 100,
            nonce,
        },
        pos::SignatureType::Ed25519([0u8; 64]), // Dummy signature (not verified in fast path)
        nonce,
    )
}

/// Format a number with comma separators
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1000000), "1,000,000");
        assert_eq!(format_number(123456789), "123,456,789");
    }

    #[test]
    fn test_generate_transaction() {
        let tx = generate_transaction(42);
        assert_eq!(tx.timestamp, 42);
    }
}
