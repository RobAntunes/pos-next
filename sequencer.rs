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

use std::convert::TryInto;
use std::time::Instant;

use blake3::{Hash, Hasher};
use parking_lot::RwLock;
use rayon::prelude::*;

use crate::metrics::NodeMetrics;
use crate::types::{
    Batch, BatchHeader, GeometricPosition, ProcessedTransaction, Transaction,
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
    /// Maximum geometric distance for acceptance
    pub max_distance: u64,
    /// Node position for geometric filtering
    pub node_position: (u64, u64),
}

impl Default for SequencerConfig {
    fn default() -> Self {
        // Generate deterministic node position from a default seed
        let node_hash = blake3::hash(b"default-sequencer-node");
        let bytes = node_hash.as_bytes();
        let x = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let y = u64::from_le_bytes(bytes[8..16].try_into().unwrap());

        Self {
            sequencer_id: [0u8; 32],
            signing_key: [0u8; 32],
            batch_size: DEFAULT_BATCH_SIZE,
            max_distance: MAX_DISTANCE,
            node_position: (x, y),
        }
    }
}

/// High-throughput sequencer for Layer 1
pub struct Sequencer {
    config: SequencerConfig,
    /// Current round ID
    round_id: RwLock<u64>,
    /// Pending transactions for current batch
    pending: RwLock<Vec<ProcessedTransaction>>,
    /// Running XOR of transaction hashes
    current_xor: RwLock<[u8; 32]>,
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
            pending: RwLock::new(Vec::with_capacity(DEFAULT_BATCH_SIZE)),
            current_xor: RwLock::new([0u8; 32]),
            metrics: RwLock::new(NodeMetrics::new(node_id, 60)),
        }
    }

    /// Process a single transaction (fast path - BLAKE3 only)
    /// Returns true if accepted, false if rejected due to geometric constraints
    #[inline]
    pub fn process_transaction(&self, tx: Transaction) -> bool {
        let start = Instant::now();

        // 1. Compute transaction hash (BLAKE3 - fast)
        let tx_hash = tx.hash();

        // 2. Calculate geometric position (BLAKE3 - O(1))
        let position = self.calculate_position(&tx_hash);

        // 3. Filter by distance constraint
        if let Some(distance) = position.distance {
            if distance > self.config.max_distance {
                return false; // Rejected - outside acceptance zone
            }
        }

        // 4. Add to pending batch
        let processed = ProcessedTransaction {
            tx,
            position,
            tx_hash,
        };

        {
            let mut pending = self.pending.write();
            pending.push(processed);

            // Update running XOR
            let mut xor = self.current_xor.write();
            for (i, byte) in tx_hash.as_bytes().iter().enumerate() {
                xor[i] ^= byte;
            }
        }

        // Record metrics
        self.metrics.write().record_batch(1, start.elapsed());

        true
    }

    /// Process a batch of transactions (Reference version for Zero-Copy)
    /// Uses LOCK-FREE aggregation via Rayon Map-Reduce
    pub fn process_batch_ref(&self, txs: &[Transaction]) -> (usize, usize) {
        let start = Instant::now();
        let total = txs.len();
        let node_pos = self.config.node_position;
        let max_dist = self.config.max_distance;

        // 1. Map-Reduce: Compute everything in parallel and aggregate without locks
        let (accepted_txs, batch_xor) = txs.par_iter()
            .map(|tx| {
                let tx_hash = tx.hash();
                let position = Self::calculate_position_stateless(&tx_hash, node_pos);
                
                // Geometric filter
                let accept = position.distance
                    .map(|d| d <= max_dist)
                    .unwrap_or(true);

                if accept {
                    let mut xor_part = [0u8; 32];
                    let hash_bytes = tx_hash.as_bytes();
                    for i in 0..32 {
                        xor_part[i] = hash_bytes[i];
                    }
                    
                    (vec![ProcessedTransaction {
                        tx: tx.clone(), // unavoidable if we want to store it
                        position,
                        tx_hash,
                    }], xor_part)
                } else {
                    (Vec::new(), [0u8; 32])
                }
            })
            .reduce(
                || (Vec::new(), [0u8; 32]), // Identity
                |mut a, b| {
                    // Merge vectors
                    a.0.extend(b.0);
                    // XOR merge
                    for i in 0..32 {
                        a.1[i] ^= b.1[i];
                    }
                    a
                }
            );

        let accepted_count = accepted_txs.len();

        // 2. Single critical section to update state
        if accepted_count > 0 {
            let mut pending = self.pending.write();
            let mut current_xor = self.current_xor.write();
            
            pending.extend(accepted_txs);
            for i in 0..32 {
                current_xor[i] ^= batch_xor[i];
            }
        }

        // Record metrics
        self.metrics.write().record_batch(accepted_count as u64, start.elapsed());

        (accepted_count, total - accepted_count)
    }

    /// PARALLEL batch processing with parallel XOR reduction
    pub fn process_batch_parallel_reduce(&self, txs: Vec<Transaction>) -> (usize, usize) {
        let start = Instant::now();
        let total = txs.len();
        let node_pos = self.config.node_position;
        let max_dist = self.config.max_distance;

        // PARALLEL: Process all transactions and reduce XOR in parallel
        let (new_pending, batch_xor): (Vec<ProcessedTransaction>, [u8; 32]) = txs
            .into_par_iter()
            .filter_map(|tx| {
                let tx_hash = tx.hash();
                let position = Self::calculate_position_stateless(&tx_hash, node_pos);

                if position.distance.map(|d| d <= max_dist).unwrap_or(true) {
                    Some((ProcessedTransaction { tx, position, tx_hash }, *tx_hash.as_bytes()))
                } else {
                    None
                }
            })
            .fold(
                || (Vec::new(), [0u8; 32]),
                |(mut txs, mut xor), (ptx, hash_bytes)| {
                    for i in 0..32 {
                        xor[i] ^= hash_bytes[i];
                    }
                    txs.push(ptx);
                    (txs, xor)
                },
            )
            .reduce(
                || (Vec::new(), [0u8; 32]),
                |(mut txs1, xor1), (txs2, xor2)| {
                    txs1.extend(txs2);
                    let mut combined_xor = [0u8; 32];
                    for i in 0..32 {
                        combined_xor[i] = xor1[i] ^ xor2[i];
                    }
                    (txs1, combined_xor)
                },
            );

        let accepted = new_pending.len();

        // Merge into pending (short critical section)
        {
            let mut pending = self.pending.write();
            let mut xor = self.current_xor.write();

            pending.extend(new_pending);
            for i in 0..32 {
                xor[i] ^= batch_xor[i];
            }
        }

        self.metrics.write().record_batch(accepted as u64, start.elapsed());

        (accepted, total - accepted)
    }

    /// Calculate geometric position using BLAKE3 (O(1)) - instance method
    #[inline]
    fn calculate_position(&self, tx_hash: &Hash) -> GeometricPosition {
        Self::calculate_position_stateless(tx_hash, self.config.node_position)
    }

    /// Calculate geometric position - STATELESS pure function for parallel execution
    #[inline]
    fn calculate_position_stateless(tx_hash: &Hash, node_pos: (u64, u64)) -> GeometricPosition {
        let mut hasher = Hasher::new();
        hasher.update(tx_hash.as_bytes());
        let position_hash = hasher.finalize();

        // Extract position from hash bytes
        let bytes = position_hash.as_bytes();
        let tx_x = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let tx_y = u64::from_le_bytes(bytes[8..16].try_into().unwrap());

        let (node_x, node_y) = node_pos;

        // Calculate distance using saturating arithmetic to prevent overflow
        let dx = if tx_x > node_x { tx_x - node_x } else { node_x - tx_x };
        let dy = if tx_y > node_y { tx_y - node_y } else { node_y - tx_y };

        // Use u128 for intermediate calculation to avoid overflow
        let dx_sq = (dx as u128).saturating_mul(dx as u128);
        let dy_sq = (dy as u128).saturating_mul(dy as u128);
        let sum = dx_sq.saturating_add(dy_sq);
        let distance = (sum as f64).sqrt() as u64;

        // Calculate theta (angle) - dy is always non-negative
        let theta = if dx == 0 {
            90
        } else {
            let angle = (dy as f64 / dx as f64).atan();
            ((angle * 180.0 / std::f64::consts::PI) as u64) % 360
        };

        GeometricPosition {
            tx_position: (tx_x, tx_y),
            node_position: Some((node_x, node_y)),
            distance: Some(distance),
            theta: Some(theta),
        }
    }

    /// Finalize current batch and return it
    pub fn finalize_batch(&self) -> Option<Batch> {
        let mut pending = self.pending.write();
        let mut xor = self.current_xor.write();

        if pending.is_empty() {
            return None;
        }

        // Build structure root (Merkle root of positions)
        let structure_root = self.compute_structure_root(&pending);

        // Build XOR hash
        let set_xor = blake3::Hash::from_bytes(*xor);

        // Get round ID and increment
        let round_id = {
            let mut rid = self.round_id.write();
            let current = *rid;
            *rid += 1;
            current
        };

        // Create header (signature would be added by actual signing key)
        let header = BatchHeader {
            sequencer_id: self.config.sequencer_id,
            round_id,
            structure_root,
            set_xor,
            tx_count: pending.len() as u32,
            signature: [0u8; 64], // Placeholder - would use ed25519
        };

        let transactions = std::mem::take(&mut *pending);
        *xor = [0u8; 32]; // Reset XOR

        Some(Batch {
            header,
            transactions,
        })
    }

    /// Compute Merkle root of geometric positions
    fn compute_structure_root(&self, txs: &[ProcessedTransaction]) -> Hash {
        if txs.is_empty() {
            return blake3::hash(b"empty");
        }

        // Simple approach: hash all positions together
        // A real implementation would build a proper Merkle tree
        let mut hasher = Hasher::new();
        for ptx in txs {
            let pos = &ptx.position;
            hasher.update(&pos.tx_position.0.to_le_bytes());
            hasher.update(&pos.tx_position.1.to_le_bytes());
            if let Some((nx, ny)) = pos.node_position {
                hasher.update(&nx.to_le_bytes());
                hasher.update(&ny.to_le_bytes());
            }
        }
        hasher.finalize()
    }

    /// Get current pending transaction count
    pub fn pending_count(&self) -> usize {
        self.pending.read().len()
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
        self.pending.read().len() >= self.config.batch_size
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
        Transaction {
            sender: [1u8; 32],
            payload: TransactionPayload::Transfer {
                recipient: [2u8; 32],
                amount: 100,
                nonce,
            },
            signature: [0u8; 64],
            timestamp: nonce,
        }
    }

    #[test]
    fn test_process_single_transaction() {
        let config = SequencerConfig {
            max_distance: u64::MAX, // Accept all
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
            max_distance: u64::MAX,
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
            max_distance: u64::MAX,
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
    fn test_geometric_distance_no_overflow() {
        let config = SequencerConfig {
            node_position: (u64::MAX, u64::MAX),
            max_distance: u64::MAX,
            ..Default::default()
        };
        let sequencer = Sequencer::new(config);

        // This should not panic due to overflow
        let tx = make_test_tx(0);
        let _ = sequencer.process_transaction(tx);
    }
}
