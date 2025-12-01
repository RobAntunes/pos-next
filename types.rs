//! Core data structures for the POS protocol
//! 
//! Layer 1 (Structure): Orders transactions by Geometry and Time
//! Layer 2 (Settlement): Validates Signatures and State Transitions

use blake3::Hash;
use serde::{Deserialize, Serialize};

/// Maximum geometric distance for transaction acceptance
pub const MAX_DISTANCE: u64 = u64::MAX / 4;

/// Default batch size for the sequencer
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Transaction payload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionPayload {
    /// Standard token transfer
    Transfer {
        /// Recipient's public key hash
        recipient: [u8; 32],
        /// Amount to transfer
        amount: u64,
        /// Transaction nonce for replay protection
        nonce: u64,
    },
}

/// Raw transaction before processing
#[derive(Debug, Clone)]
pub struct Transaction {
    /// Sender's public key
    pub sender: [u8; 32],
    /// Transaction payload
    pub payload: TransactionPayload,
    /// Ed25519 signature over (sender || payload_hash)
    pub signature: [u8; 64],
    /// Timestamp in milliseconds
    pub timestamp: u64,
    /// Cached BLAKE3 hash (computed once at creation)
    hash: Hash,
}

impl Transaction {
    /// Create a new transaction with all fields and compute its hash
    pub fn new(
        sender: [u8; 32],
        payload: TransactionPayload,
        signature: [u8; 64],
        timestamp: u64,
    ) -> Self {
        // Compute hash once at creation
        let hash = Self::compute_hash(&sender, &payload, &signature, timestamp);

        Self {
            sender,
            payload,
            signature,
            timestamp,
            hash,
        }
    }

    /// Compute the transaction hash using BLAKE3 (internal helper)
    fn compute_hash(
        sender: &[u8; 32],
        payload: &TransactionPayload,
        signature: &[u8; 64],
        timestamp: u64,
    ) -> Hash {
        let payload_bytes = Self::serialize_payload(payload);
        let mut hasher = blake3::Hasher::new();
        hasher.update(sender);
        hasher.update(&payload_bytes);
        hasher.update(signature);
        hasher.update(&timestamp.to_le_bytes());
        hasher.finalize()
    }

    /// Get the cached transaction hash (O(1) - no recomputation!)
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// Serialize the payload for hashing/signing (static helper)
    fn serialize_payload(payload: &TransactionPayload) -> Vec<u8> {
        match payload {
            TransactionPayload::Transfer { recipient, amount, nonce } => {
                let mut bytes = vec![0u8]; // type discriminant
                bytes.extend_from_slice(recipient);
                bytes.extend_from_slice(&amount.to_le_bytes());
                bytes.extend_from_slice(&nonce.to_le_bytes());
                bytes
            }
        }
    }

    /// Serialize the payload for hashing/signing (public interface)
    pub fn payload_bytes(&self) -> Vec<u8> {
        Self::serialize_payload(&self.payload)
    }

    /// Get the message that should be signed
    pub fn signing_message(&self) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(&self.sender);
        msg.extend_from_slice(&self.payload_bytes());
        msg.extend_from_slice(&self.timestamp.to_le_bytes());
        msg
    }
}

/// Processed transaction with ring position
#[derive(Debug, Clone)]
pub struct ProcessedTransaction {
    /// Original transaction
    pub tx: Transaction,
    /// Ring position derived from BLAKE3
    pub ring_info: RingInfo,
    /// Transaction hash
    pub tx_hash: Hash,
}

/// Position on the consistent hash ring (0 to u64::MAX)
pub type RingPosition = u64;

/// Ring-based position for transaction placement
#[derive(Debug, Clone, Copy)]
pub struct RingInfo {
    /// Transaction's position on the ring (0 to u64::MAX)
    pub tx_position: RingPosition,
    /// Node's position on the ring (if applicable)
    pub node_position: Option<RingPosition>,
    /// Clockwise distance on the ring from node to transaction
    pub distance: Option<u64>,
}

impl RingInfo {
    /// Check if transaction is within acceptance range
    pub fn is_within_range(&self, max_range: u64) -> bool {
        self.distance.map(|d| d <= max_range).unwrap_or(true)
    }
}

/// Batch header for rollup commitment
#[derive(Debug, Clone)]
pub struct BatchHeader {
    /// Public key of the sequencer creating this batch
    pub sequencer_id: [u8; 32],
    /// Current time epoch / round ID
    pub round_id: u64,
    /// Merkle root of all GeometricPosition assignments
    pub structure_root: Hash,
    /// XOR sum of all transaction hashes (O(1) set validation)
    pub set_xor: Hash,
    /// Number of transactions in the batch
    pub tx_count: u32,
    /// Ed25519 signature over the header fields
    pub signature: [u8; 64],
}

impl BatchHeader {
    /// Compute bytes for signing (excludes signature field)
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.sequencer_id);
        bytes.extend_from_slice(&self.round_id.to_le_bytes());
        bytes.extend_from_slice(self.structure_root.as_bytes());
        bytes.extend_from_slice(self.set_xor.as_bytes());
        bytes.extend_from_slice(&self.tx_count.to_le_bytes());
        bytes
    }

    /// Compute header hash
    pub fn hash(&self) -> Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.signing_bytes());
        hasher.update(&self.signature);
        hasher.finalize()
    }
}

/// Complete batch with header and transaction data
#[derive(Debug, Clone)]
pub struct Batch {
    /// Batch header with sequencer commitment
    pub header: BatchHeader,
    /// Processed transactions with positions
    pub transactions: Vec<ProcessedTransaction>,
}

impl Batch {
    /// Get the batch ID (header hash)
    pub fn id(&self) -> Hash {
        self.header.hash()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_hash() {
        let tx = Transaction::new(
            [1u8; 32],
            TransactionPayload::Transfer {
                recipient: [2u8; 32],
                amount: 1000,
                nonce: 1,
            },
            [0u8; 64],
            12345,
        );

        let hash1 = tx.hash();
        let hash2 = tx.hash();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_payload_serialization() {
        let tx1 = Transaction::new(
            [0u8; 32],
            TransactionPayload::Transfer {
                recipient: [1u8; 32],
                amount: 100,
                nonce: 0,
            },
            [0u8; 64],
            0,
        );
        
        let tx2 = Transaction::new(
            [0u8; 32],
            TransactionPayload::Transfer {
                recipient: [2u8; 32],
                amount: 200,
                nonce: 1,
            },
            [0u8; 64],
            0,
        );

        // Different payloads should produce different bytes
        assert_ne!(tx1.payload_bytes(), tx2.payload_bytes());
    }
}
