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

/// Authentication signature type for high-frequency security
/// 
/// Implements Winternitz-Lite hash chain authentication:
/// - Ed25519: Slow path (~50,000ns) for account init and "refueling"
/// - HashReveal: Fast path (~15ns BLAKE3) for high-frequency transactions
#[derive(Debug, Clone)]
pub enum SignatureType {
    /// Slow path: Full Ed25519 signature (for high value / init / refuel)
    /// Used when creating account or refreshing the hash chain
    Ed25519([u8; 64]),
    
    /// Fast path: Reveal of the next pre-image in the hash chain
    /// Verifies against Account.auth_root in O(1) using BLAKE3
    /// After verification: auth_root = revealed_secret
    HashReveal([u8; 32]),
}

impl SignatureType {
    /// Check if this is the fast-path hash reveal
    #[inline]
    pub fn is_hash_reveal(&self) -> bool {
        matches!(self, SignatureType::HashReveal(_))
    }
    
    /// Get the signature bytes for hashing
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            SignatureType::Ed25519(sig) => sig.to_vec(),
            SignatureType::HashReveal(reveal) => reveal.to_vec(),
        }
    }
}

/// Transaction payload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionPayload {
    /// Standard token transfer
    Transfer {
        /// Recipient's public key hash
        recipient: [u8; 32],
        /// Amount to transfer
        amount: u64,
        /// Transaction nonce for replay protection (legacy, use tx.nonce)
        nonce: u64,
    },
    
    /// Set a new auth_root for fast-path authentication ("refueling")
    /// Must be signed with Ed25519 (slow path)
    /// This commits to a new hash chain: H^n(secret) where new_auth_root = H^0
    SetAuthRoot {
        /// The new auth_root (tip of the hash chain commitment)
        /// User keeps the secret S_n and reveals S_{n-1}, S_{n-2}, ... per transaction
        new_auth_root: [u8; 32],
    },
}

/// Raw transaction before processing
#[derive(Debug, Clone)]
pub struct Transaction {
    /// Sender's public key
    pub sender: [u8; 32],
    /// Transaction payload
    pub payload: TransactionPayload,
    
    // CONSENSUS FIELDS
    /// Transaction nonce for replay protection (must be account.nonce + 1)
    pub nonce: u64,
    /// Timestamp in milliseconds (temporal binding)
    pub timestamp: u64,
    
    // AUTHENTICATION
    /// Signature type: Ed25519 (slow) or HashReveal (fast)
    pub signature: SignatureType,
    
    /// Cached BLAKE3 hash (computed once at creation)
    hash: Hash,
}

impl Transaction {
    /// Create a new transaction with all fields and compute its hash
    pub fn new(
        sender: [u8; 32],
        payload: TransactionPayload,
        nonce: u64,
        timestamp: u64,
        signature: SignatureType,
    ) -> Self {
        // Compute hash once at creation
        let hash = Self::compute_hash(&sender, &payload, nonce, timestamp, &signature);

        Self {
            sender,
            payload,
            nonce,
            timestamp,
            signature,
            hash,
        }
    }
    
    /// Create a new transaction using the fast-path hash reveal authentication
    #[inline]
    pub fn new_fast(
        sender: [u8; 32],
        payload: TransactionPayload,
        nonce: u64,
        timestamp: u64,
        auth_secret: [u8; 32],
    ) -> Self {
        Self::new(sender, payload, nonce, timestamp, SignatureType::HashReveal(auth_secret))
    }
    
    /// Create a new transaction using the slow-path Ed25519 authentication
    #[inline]
    pub fn new_slow(
        sender: [u8; 32],
        payload: TransactionPayload,
        nonce: u64,
        timestamp: u64,
        ed25519_sig: [u8; 64],
    ) -> Self {
        Self::new(sender, payload, nonce, timestamp, SignatureType::Ed25519(ed25519_sig))
    }

    /// Compute the transaction hash using BLAKE3 (internal helper)
    fn compute_hash(
        sender: &[u8; 32],
        payload: &TransactionPayload,
        nonce: u64,
        timestamp: u64,
        signature: &SignatureType,
    ) -> Hash {
        let payload_bytes = Self::serialize_payload(payload);
        let mut hasher = blake3::Hasher::new();
        hasher.update(sender);
        hasher.update(&payload_bytes);
        hasher.update(&nonce.to_le_bytes());
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&signature.to_bytes());
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
            TransactionPayload::SetAuthRoot { new_auth_root } => {
                let mut bytes = vec![1u8]; // type discriminant
                bytes.extend_from_slice(new_auth_root);
                bytes
            }
        }
    }

    /// Serialize the payload for hashing/signing (public interface)
    pub fn payload_bytes(&self) -> Vec<u8> {
        Self::serialize_payload(&self.payload)
    }

    /// Get the message that should be signed (for Ed25519 slow path)
    pub fn signing_message(&self) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(&self.sender);
        msg.extend_from_slice(&self.payload_bytes());
        msg.extend_from_slice(&self.nonce.to_le_bytes());
        msg.extend_from_slice(&self.timestamp.to_le_bytes());
        msg
    }
    
    /// Check if this transaction uses fast-path authentication
    #[inline]
    pub fn is_fast_path(&self) -> bool {
        self.signature.is_hash_reveal()
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
        // Test with fast-path HashReveal
        let tx = Transaction::new_fast(
            [1u8; 32],
            TransactionPayload::Transfer {
                recipient: [2u8; 32],
                amount: 1000,
                nonce: 1,
            },
            1,  // nonce
            12345,  // timestamp
            [0u8; 32],  // auth_secret
        );

        let hash1 = tx.hash();
        let hash2 = tx.hash();
        assert_eq!(hash1, hash2);
        assert!(tx.is_fast_path());
    }
    
    #[test]
    fn test_transaction_slow_path() {
        // Test with slow-path Ed25519
        let tx = Transaction::new_slow(
            [1u8; 32],
            TransactionPayload::Transfer {
                recipient: [2u8; 32],
                amount: 1000,
                nonce: 1,
            },
            1,  // nonce
            12345,  // timestamp
            [0u8; 64],  // ed25519_sig
        );

        assert!(!tx.is_fast_path());
    }

    #[test]
    fn test_payload_serialization() {
        let tx1 = Transaction::new_fast(
            [0u8; 32],
            TransactionPayload::Transfer {
                recipient: [1u8; 32],
                amount: 100,
                nonce: 0,
            },
            0, 0, [0u8; 32],
        );
        
        let tx2 = Transaction::new_fast(
            [0u8; 32],
            TransactionPayload::Transfer {
                recipient: [2u8; 32],
                amount: 200,
                nonce: 1,
            },
            1, 0, [0u8; 32],
        );

        // Different payloads should produce different bytes
        assert_ne!(tx1.payload_bytes(), tx2.payload_bytes());
    }
}
