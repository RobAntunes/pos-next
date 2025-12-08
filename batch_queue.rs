//! Lock-Free Batch Queue
//!
//! Decouples the Sequencer (consensus) from the Network Distributor (DHT routing).
//! Sequencer produces batches → BatchQueue → NetworkDistributor consumes and routes.
//!
//! Design:
//! - Lock-free MPMC queue using crossbeam
//! - Bounded capacity with backpressure signaling
//! - Zero-copy batch passing via Arc<Batch>

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam::queue::ArrayQueue;

use crate::types::Batch;

/// Default capacity: 1024 batches in flight
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Lock-free queue for finalized batches
/// 
/// This is the handoff point between:
/// - Layer 1 (Sequencer): Processes transactions, creates batches
/// - Layer 2 (NetworkDistributor): Routes batches via DHT
pub struct BatchQueue {
    /// Lock-free bounded queue
    queue: ArrayQueue<Arc<Batch>>,
    /// Total batches pushed (for stats)
    total_pushed: AtomicU64,
    /// Total batches pulled (for stats)
    total_pulled: AtomicU64,
    /// Total batches dropped due to backpressure
    total_dropped: AtomicU64,
}

impl BatchQueue {
    /// Create a new batch queue with specified capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
            total_pushed: AtomicU64::new(0),
            total_pulled: AtomicU64::new(0),
            total_dropped: AtomicU64::new(0),
        }
    }

    /// Push a batch into the queue
    /// Returns true if successful, false if queue is full (backpressure)
    pub fn push(&self, batch: Arc<Batch>) -> bool {
        match self.queue.push(batch) {
            Ok(()) => {
                self.total_pushed.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(_) => {
                self.total_dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Pull a batch from the queue
    /// Returns None if queue is empty
    pub fn pull(&self) -> Option<Arc<Batch>> {
        self.queue.pop().map(|batch| {
            self.total_pulled.fetch_add(1, Ordering::Relaxed);
            batch
        })
    }

    /// Pull up to `max` batches from the queue
    /// More efficient than calling pull() in a loop
    pub fn pull_many(&self, max: usize) -> Vec<Arc<Batch>> {
        let mut batches = Vec::with_capacity(max);
        for _ in 0..max {
            match self.queue.pop() {
                Some(batch) => batches.push(batch),
                None => break,
            }
        }
        if !batches.is_empty() {
            self.total_pulled.fetch_add(batches.len() as u64, Ordering::Relaxed);
        }
        batches
    }

    /// Check if queue is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Check if queue is full (backpressure signal)
    #[inline]
    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }

    /// Get current queue length
    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Get queue capacity
    #[inline]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Get queue statistics
    pub fn stats(&self) -> BatchQueueStats {
        BatchQueueStats {
            current_len: self.queue.len(),
            capacity: self.queue.capacity(),
            total_pushed: self.total_pushed.load(Ordering::Relaxed),
            total_pulled: self.total_pulled.load(Ordering::Relaxed),
            total_dropped: self.total_dropped.load(Ordering::Relaxed),
        }
    }
}

impl Default for BatchQueue {
    fn default() -> Self {
        Self::new(DEFAULT_QUEUE_CAPACITY)
    }
}

/// Statistics for the batch queue
#[derive(Debug, Clone)]
pub struct BatchQueueStats {
    pub current_len: usize,
    pub capacity: usize,
    pub total_pushed: u64,
    pub total_pulled: u64,
    pub total_dropped: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BatchHeader, ProcessedTransaction, Transaction, TransactionPayload, RingInfo, SignatureType};

    fn make_test_batch(tx_count: usize) -> Batch {
        let transactions: Vec<ProcessedTransaction> = (0..tx_count)
            .map(|i| {
                let tx = Transaction::new(
                    [1u8; 32],
                    TransactionPayload::Transfer {
                        recipient: [2u8; 32],
                        amount: 100,
                        nonce: i as u64,
                    },
                    SignatureType::Ed25519([0u8; 64]),
                    i as u64,
                );
                let tx_hash = tx.hash();
                ProcessedTransaction {
                    tx,
                    ring_info: RingInfo {
                        tx_position: i as u64,
                        node_position: Some(0),
                        distance: None,
                    },
                    tx_hash,
                }
            })
            .collect();

        Batch {
            header: BatchHeader {
                sequencer_id: [0u8; 32],
                round_id: 0,
                structure_root: blake3::hash(b"test"),
                set_xor: blake3::hash(b"xor"),
                tx_count: tx_count as u32,
                signature: [0u8; 64],
            },
            transactions,
        }
    }

    #[test]
    fn test_push_pull() {
        let queue = BatchQueue::new(10);
        let batch = Arc::new(make_test_batch(5));

        assert!(queue.push(batch.clone()));
        assert_eq!(queue.len(), 1);

        let pulled = queue.pull().unwrap();
        assert_eq!(pulled.transactions.len(), 5);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_backpressure() {
        let queue = BatchQueue::new(2);

        // Fill the queue
        assert!(queue.push(Arc::new(make_test_batch(1))));
        assert!(queue.push(Arc::new(make_test_batch(1))));

        // Should fail due to backpressure
        assert!(!queue.push(Arc::new(make_test_batch(1))));
        assert!(queue.is_full());

        let stats = queue.stats();
        assert_eq!(stats.total_dropped, 1);
    }

    #[test]
    fn test_pull_many() {
        let queue = BatchQueue::new(10);

        for i in 0..5 {
            queue.push(Arc::new(make_test_batch(i + 1)));
        }

        let batches = queue.pull_many(3);
        assert_eq!(batches.len(), 3);
        assert_eq!(queue.len(), 2);
    }
}
