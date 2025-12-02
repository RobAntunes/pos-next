//! Spent Set - Lock-free dedup gate for double-spend prevention
//!
//! Uses a DashMap for O(1) concurrent lookups of (sender, nonce) pairs.
//! Sits at ingestion layer, before transactions enter the arena.
//! Sequencer never sees this - it just rips through clean txs.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Key for spent-set: hash of (sender || nonce)
/// Using blake3 for speed, 8 bytes is enough for collision resistance at scale
type SpentKey = u64;

/// Lock-free spent-set for dedup at ingestion
/// 
/// Design principles:
/// - O(1) lookup and insert
/// - Lock-free (DashMap uses fine-grained sharding)
/// - Memory efficient (8 bytes per entry)
/// - No impact on sequencer hot path
pub struct SpentSet {
    /// Map of spent (sender, nonce) pairs
    /// Key: first 8 bytes of blake3(sender || nonce)
    /// Value: timestamp of when it was marked spent (for potential pruning)
    spent: DashMap<SpentKey, u64>,
    
    /// Rolling root for state commitment (XOR of all keys)
    /// Can be used for Merkle proofs if needed later
    rolling_xor: AtomicU64,
    
    /// Stats
    total_checked: AtomicU64,
    total_rejected: AtomicU64,
}

impl SpentSet {
    pub fn new() -> Self {
        Self {
            spent: DashMap::with_capacity(10_000_000), // Pre-allocate for 10M entries
            rolling_xor: AtomicU64::new(0),
            total_checked: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
        }
    }
    
    /// Compute spent-key from sender and nonce
    #[inline(always)]
    fn compute_key(sender: &[u8; 32], nonce: u64) -> SpentKey {
        let mut hasher = blake3::Hasher::new();
        hasher.update(sender);
        hasher.update(&nonce.to_le_bytes());
        let hash = hasher.finalize();
        // Take first 8 bytes as u64
        u64::from_le_bytes(hash.as_bytes()[0..8].try_into().unwrap())
    }
    
    /// Check if a (sender, nonce) pair has been spent
    #[inline(always)]
    pub fn is_spent(&self, sender: &[u8; 32], nonce: u64) -> bool {
        let key = Self::compute_key(sender, nonce);
        self.spent.contains_key(&key)
    }
    
    /// Mark a (sender, nonce) pair as spent
    /// Returns false if already spent (double-spend attempt)
    #[inline(always)]
    pub fn mark_spent(&self, sender: &[u8; 32], nonce: u64) -> bool {
        let key = Self::compute_key(sender, nonce);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Try to insert - returns None if key didn't exist (success)
        if self.spent.insert(key, timestamp).is_none() {
            // Update rolling XOR
            self.rolling_xor.fetch_xor(key, Ordering::Relaxed);
            true
        } else {
            false
        }
    }
    
    /// Check and mark in one atomic operation
    /// Returns true if tx is NEW (not spent), false if duplicate
    #[inline(always)]
    pub fn check_and_mark(&self, sender: &[u8; 32], nonce: u64) -> bool {
        self.total_checked.fetch_add(1, Ordering::Relaxed);
        
        let key = Self::compute_key(sender, nonce);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Use entry API: if vacant, insert and return true; if occupied, return false
        use dashmap::mapref::entry::Entry;
        match self.spent.entry(key) {
            Entry::Vacant(vacant) => {
                vacant.insert(timestamp);
                self.rolling_xor.fetch_xor(key, Ordering::Relaxed);
                true
            }
            Entry::Occupied(_) => {
                self.total_rejected.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }
    
    /// Filter a batch, returning only non-spent transactions
    /// Also marks accepted txs as spent in one pass
    pub fn filter_batch<T, F>(&self, txs: Vec<T>, get_sender_nonce: F) -> (Vec<T>, usize)
    where
        F: Fn(&T) -> (&[u8; 32], u64),
    {
        let mut accepted = Vec::with_capacity(txs.len());
        let mut rejected_count = 0;
        
        for tx in txs {
            let (sender, nonce) = get_sender_nonce(&tx);
            if self.check_and_mark(sender, nonce) {
                accepted.push(tx);
            } else {
                rejected_count += 1;
            }
        }
        
        (accepted, rejected_count)
    }
    
    /// Get the rolling XOR root (state commitment)
    pub fn rolling_root(&self) -> u64 {
        self.rolling_xor.load(Ordering::Relaxed)
    }
    
    /// Get stats
    pub fn stats(&self) -> SpentSetStats {
        SpentSetStats {
            total_entries: self.spent.len(),
            total_checked: self.total_checked.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
            rolling_root: self.rolling_root(),
        }
    }
    
    /// Prune entries older than given age (for memory management)
    /// Call periodically if memory is a concern
    pub fn prune_older_than(&self, max_age_secs: u64) -> usize {
        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(max_age_secs);
        
        let mut pruned = 0;
        self.spent.retain(|_k, v| {
            if *v < cutoff {
                pruned += 1;
                false
            } else {
                true
            }
        });
        pruned
    }
}

impl Default for SpentSet {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct SpentSetStats {
    pub total_entries: usize,
    pub total_checked: u64,
    pub total_rejected: u64,
    pub rolling_root: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_dedup() {
        let set = SpentSet::new();
        let sender = [1u8; 32];
        
        // First submission should succeed
        assert!(set.check_and_mark(&sender, 1));
        
        // Second submission of same (sender, nonce) should fail
        assert!(!set.check_and_mark(&sender, 1));
        
        // Different nonce should succeed
        assert!(set.check_and_mark(&sender, 2));
        
        // Different sender, same nonce should succeed
        let sender2 = [2u8; 32];
        assert!(set.check_and_mark(&sender2, 1));
    }
    
    #[test]
    fn test_filter_batch() {
        let set = SpentSet::new();
        
        // Pre-spend one tx
        let sender1 = [1u8; 32];
        set.mark_spent(&sender1, 100);
        
        // Create batch with mix of new and duplicate
        let txs = vec![
            (sender1, 100),  // Duplicate - should be filtered
            (sender1, 101),  // New
            ([2u8; 32], 1),  // New
        ];
        
        let (accepted, rejected) = set.filter_batch(txs, |tx| (&tx.0, tx.1));
        
        assert_eq!(rejected, 1);
        assert_eq!(accepted.len(), 2);
    }
    
    #[test]
    fn test_rolling_root() {
        let set = SpentSet::new();
        
        let root1 = set.rolling_root();
        set.mark_spent(&[1u8; 32], 1);
        let root2 = set.rolling_root();
        
        // Root should change
        assert_ne!(root1, root2);
        
        // Adding same tx again shouldn't change root (idempotent)
        set.mark_spent(&[1u8; 32], 1);
        assert_eq!(root2, set.rolling_root());
    }
}
