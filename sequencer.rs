//! Sequencer module - Layer 1 (Structure Layer)
//!
//! High-throughput transaction processing using BLAKE3 only.
//! Target: 30M+ TPS
//!
//! Responsibilities:
//! 1. Ingest raw transaction bytes
//! 2. Assign GeometricPosition and TemporalBinding (O(1) BLAKE3)
//! 3. Filter by geometric constraints
//! 4. Aggregate into Batches
//! 5. Broadcast Batch Header + Data Blob
//!
//! NOTE: Does NOT verify Ed25519 signatures (that's the Verifier's job)

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use blake3::{Hash, Hasher};
use parking_lot::RwLock;
use rayon::prelude::*;

use crate::metrics::NodeMetrics;
use crate::ring::calculate_ring_position;
use crate::types::{
    Batch, BatchHeader, ProcessedTransaction, RingInfo, RingPosition, Transaction,
    DEFAULT_BATCH_SIZE, MAX_DISTANCE,
};

/// Sequencer configuration
#[derive(Debug, Clone)]
pub struct SequencerConfig {
    /// Sequencer's public key
    pub sequencer_id: [u8; 32],
    /// Sequencer's signing key (for batch headers)
    pub signing_key: [u8; 32],
    /// Maximum transactions per batch
    pub batch_size: usize,
    /// Maximum ring range for acceptance (distance on the ring)
    pub max_range: u64,
    /// Node position on the ring
    pub node_position: RingPosition,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        // Generate deterministic node position on the ring
        let node_hash = blake3::hash(b"default-sequencer-node");
        let node_position = calculate_ring_position(&node_hash);

        Self {
            sequencer_id: [0u8; 32],
            signing_key: [0u8; 32],
            batch_size: DEFAULT_BATCH_SIZE,
            max_range: MAX_DISTANCE,
            node_position,
        }
    }
}

/// High-throughput sequencer for Layer 1
pub struct Sequencer {
    config: SequencerConfig,
    /// Current round ID
    round_id: RwLock<u64>,
    /// OPTIMIZATION: Lock-free pending queue eliminates write lock contention
    /// Multiple threads can push concurrently without blocking
    pending: crossbeam::queue::SegQueue<ProcessedTransaction>,
    /// Pending count for batch size checks
    pending_count: AtomicUsize,
    /// Metrics tracker
    metrics: RwLock<NodeMetrics>,
}

impl Sequencer {
    /// Create a new sequencer
    pub fn new(config: SequencerConfig) -> Self {
        let node_id = hex::encode(&config.sequencer_id[..8]);
        Self {
            config,
            round_id: RwLock::new(0),
            pending: crossbeam::queue::SegQueue::new(),
            pending_count: AtomicUsize::new(0),
            metrics: RwLock::new(NodeMetrics::new(node_id, 60)),
        }
    }

    /// Process a single transaction (fast path - BLAKE3 only)
    /// Returns true if accepted, false if rejected due to ring range constraints
    #[inline]
    pub fn process_transaction(&self, tx: Transaction) -> bool {
        let start = Instant::now();

        // 1. Compute transaction hash (BLAKE3 - fast)
        let tx_hash = tx.hash();

        // 2. Calculate ring position (BLAKE3 - O(1))
        let ring_info = self.calculate_ring_info(&tx_hash);

        // 3. Filter by ring range constraint
        if !ring_info.is_within_range(self.config.max_range) {
            return false; // Rejected - outside acceptance range
        }

        // 4. Add to pending batch (LOCK-FREE!)
        let processed = ProcessedTransaction {
            tx,
            ring_info,
            tx_hash,
        };

        // OPTIMIZATION: SegQueue push is lock-free - no contention!
        self.pending.push(processed);
        self.pending_count.fetch_add(1, Ordering::Relaxed);

        // Record metrics REMOVED
        // self.metrics.write().record_batch(1, start.elapsed());

        true
    }

    /// Process a batch of transactions (consumes the Vec)
    pub fn process_batch(&self, txs: Vec<Transaction>) -> (usize, usize) {
        self.process_batch_parallel_reduce(txs)
    }

    /// Process a batch of transactions (optimized version - no cloning)
    /// Uses SEQUENTIAL aggregation (Consumer is already parallel)
    pub fn process_batch_ref(&self, txs: &[Transaction]) -> (usize, usize) {
        // let start = Instant::now();
        let total = txs.len();
        let node_pos = self.config.node_position;
        let max_range = self.config.max_range;

        // Map-Reduce: Just count accepted/rejected, don't store transactions
        let accepted_count = txs.iter()
            .filter(|tx| {
                let tx_hash = tx.hash();
                let ring_info = Self::calculate_ring_info_stateless(&tx_hash, node_pos);
                ring_info.is_within_range(max_range)
            })
            .count();

        // Record metrics REMOVED (Lock contention)
        // self.metrics.write().record_batch(accepted_count as u64, start.elapsed());

        (accepted_count, total - accepted_count)
    }

    /// SEQUENTIAL batch processing with sequential XOR reduction
    pub fn process_batch_parallel_reduce(&self, txs: Vec<Transaction>) -> (usize, usize) {
        // let start = Instant::now();
        let total = txs.len();
        let node_pos = self.config.node_position;
        let max_range = self.config.max_range;

        // SEQUENTIAL: Process all transactions
        // Since consumers are already parallel (N threads), adding Rayon here would oversubscribe
        let new_pending: Vec<ProcessedTransaction> = txs
            .into_iter()
            .filter_map(|tx| {
                let tx_hash = tx.hash();
                let ring_info = Self::calculate_ring_info_stateless(&tx_hash, node_pos);

                if ring_info.is_within_range(max_range) {
                    Some(ProcessedTransaction { tx, ring_info, tx_hash })
                } else {
                    None
                }
            })
            .collect();

        let accepted = new_pending.len();

        // Push to lock-free queue (no write lock needed!)
        for ptx in new_pending {
            self.pending.push(ptx);
        }
        self.pending_count.fetch_add(accepted, Ordering::Relaxed);

        // self.metrics.write().record_batch(accepted as u64, start.elapsed());

        (accepted, total - accepted)
    }

    /// Calculate ring info using BLAKE3 (O(1)) - instance method
    #[inline]
    fn calculate_ring_info(&self, tx_hash: &Hash) -> RingInfo {
        Self::calculate_ring_info_stateless(tx_hash, self.config.node_position)
    }

    /// Calculate ring info - STATELESS pure function for parallel execution
    #[inline]
    fn calculate_ring_info_stateless(tx_hash: &Hash, node_pos: RingPosition) -> RingInfo {
        // Get transaction position on the ring
        let tx_position = calculate_ring_position(tx_hash);

        // Calculate clockwise distance on the ring from node to transaction
        let distance = if tx_position >= node_pos {
            tx_position - node_pos
        } else {
            // Wrap around: distance = (MAX - node_pos) + tx_pos + 1
            (u64::MAX - node_pos) + tx_position + 1
        };

        RingInfo {
            tx_position,
            node_position: Some(node_pos),
            distance: Some(distance),
        }
    }

    /// Finalize current batch and return it
    pub fn finalize_batch(&self) -> Option<Batch> {
        // Drain lock-free queue
        let mut transactions = Vec::new();
        let mut xor = [0u8; 32];

        // Drain all pending transactions
        while let Some(ptx) = self.pending.pop() {
            // Update XOR as we drain
            for (i, byte) in ptx.tx_hash.as_bytes().iter().enumerate() {
                xor[i] ^= byte;
            }
            transactions.push(ptx);
        }

        // Reset count
        self.pending_count.store(0, Ordering::Relaxed);

        if transactions.is_empty() {
            return None;
        }

        // OPTIMIZATION (Enhancement #2): Sort by shard ID (sender[0])
        // This allows Zero-Copy Routing in the ledger worker
        transactions.par_sort_unstable_by_key(|ptx| ptx.tx.sender[0]);

        // Build structure root (Merkle root of positions)
        let structure_root = self.compute_structure_root(&transactions);

        // Build XOR hash
        let set_xor = blake3::Hash::from_bytes(xor);

        // Get round ID and increment
        let round_id = {
            let mut rid = self.round_id.write();
            let current = *rid;
            *rid += 1;
            current
        };

        let tx_count = transactions.len() as u32;

        // Create header (signature would be added by actual signing key)
        let header = BatchHeader {
            sequencer_id: self.config.sequencer_id,
            round_id,
            structure_root,
            set_xor,
            tx_count,
            signature: [0u8; 64], // Placeholder - would use ed25519
        };

        Some(Batch {
            header,
            transactions,
        })
    }

    /// Compute Merkle root of ring positions
    fn compute_structure_root(&self, txs: &[ProcessedTransaction]) -> Hash {
        if txs.is_empty() {
            return blake3::hash(b"empty");
        }

        // Simple approach: hash all ring positions together
        // A real implementation would build a proper Merkle tree
        let mut hasher = Hasher::new();
        for ptx in txs {
            let ring_info = &ptx.ring_info;
            hasher.update(&ring_info.tx_position.to_le_bytes());
            if let Some(node_pos) = ring_info.node_position {
                hasher.update(&node_pos.to_le_bytes());
            }
        }
        hasher.finalize()
    }

    /// Get current pending transaction count
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Relaxed)
    }

    /// Get current round ID
    pub fn current_round(&self) -> u64 {
        *self.round_id.read()
    }

    /// Get metrics snapshot
    pub fn get_metrics(&self) -> NodeMetrics {
        self.metrics.read().clone()
    }

    /// Complete warmup period for accurate metrics
    pub fn complete_warmup(&self) {
        self.metrics.write().complete_warmup();
    }

    /// Check if batch is ready to finalize
    pub fn batch_ready(&self) -> bool {
        self.pending_count.load(Ordering::Relaxed) >= self.config.batch_size
    }
}

/// XOR-based set validation (O(1) verification) - PARALLEL version
pub fn calculate_set_xor(hashes: &[Hash]) -> [u8; 32] {
    if hashes.len() < 1000 {
        // Serial for small batches (overhead not worth it)
        let mut xor = [0u8; 32];
        for hash in hashes {
            for (i, byte) in hash.as_bytes().iter().enumerate() {
                xor[i] ^= byte;
            }
        }
        xor
    } else {
        // Parallel reduction for large batches
        hashes
            .par_iter()
            .fold(
                || [0u8; 32],
                |mut acc, hash| {
                    for (i, byte) in hash.as_bytes().iter().enumerate() {
                        acc[i] ^= byte;
                    }
                    acc
                },
            )
            .reduce(
                || [0u8; 32],
                |a, b| {
                    let mut result = [0u8; 32];
                    for i in 0..32 {
                        result[i] = a[i] ^ b[i];
                    }
                    result
                },
            )
    }
}

/// Verify set XOR matches declared value
pub fn verify_set_xor(hashes: &[Hash], declared: &[u8; 32]) -> bool {
    let computed = calculate_set_xor(hashes);
    computed == *declared
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TransactionPayload;

    fn make_test_tx(nonce: u64) -> Transaction {
        Transaction::new(
            [1u8; 32],
            TransactionPayload::Transfer {
                recipient: [2u8; 32],
                amount: 100,
                nonce,
            },
            crate::types::SignatureType::Ed25519([0u8; 64]),
            nonce,
        )
    }

    #[test]
    fn test_process_single_transaction() {
        let config = SequencerConfig {
            max_range: u64::MAX, // Accept all
            ..Default::default()
        };
        let sequencer = Sequencer::new(config);

        let tx = make_test_tx(0);
        assert!(sequencer.process_transaction(tx));
        assert_eq!(sequencer.pending_count(), 1);
    }

    #[test]
    fn test_process_batch() {
        let config = SequencerConfig {
            max_range: u64::MAX,
            ..Default::default()
        };
        let sequencer = Sequencer::new(config);

        let txs: Vec<_> = (0..100).map(make_test_tx).collect();
        let (accepted, rejected) = sequencer.process_batch(txs);

        assert_eq!(accepted, 100);
        assert_eq!(rejected, 0);
        assert_eq!(sequencer.pending_count(), 100);
    }

    #[test]
    fn test_finalize_batch() {
        let config = SequencerConfig {
            max_range: u64::MAX,
            ..Default::default()
        };
        let sequencer = Sequencer::new(config);

        let txs: Vec<_> = (0..50).map(make_test_tx).collect();
        sequencer.process_batch(txs);

        let batch = sequencer.finalize_batch().unwrap();
        assert_eq!(batch.header.tx_count, 50);
        assert_eq!(batch.transactions.len(), 50);
        assert_eq!(sequencer.pending_count(), 0);
    }

    #[test]
    fn test_xor_validation() {
        let hashes: Vec<_> = (0..10)
            .map(|i| blake3::hash(&[i as u8]))
            .collect();

        let xor = calculate_set_xor(&hashes);
        assert!(verify_set_xor(&hashes, &xor));

        // Tampered XOR should fail
        let mut bad_xor = xor;
        bad_xor[0] ^= 1;
        assert!(!verify_set_xor(&hashes, &bad_xor));
    }

    #[test]
    fn test_ring_distance_no_overflow() {
        let config = SequencerConfig {
            node_position: u64::MAX,
            max_range: u64::MAX,
            ..Default::default()
        };
        let sequencer = Sequencer::new(config);

        // This should not panic due to overflow
        let tx = make_test_tx(0);
        let _ = sequencer.process_transaction(tx);
    }
}
