//! Core data structures for the POS protocol
//! 
//! Layer 1 (Structure): Orders transactions by Geometry and Time
//! Layer 2 (Settlement): Validates Signatures and State Transitions

use blake3::Hash;
use serde::{Deserialize, Serialize};

/// Minimum stake required to become a sequencer (1,000,000 tokens)
pub const MIN_STAKE: u64 = 1_000_000;

/// Lock-up period for staking in seconds (7 days)
pub const STAKE_LOCK_PERIOD: u64 = 7 * 24 * 60 * 60;

/// Maximum geometric distance for transaction acceptance
pub const MAX_DISTANCE: u64 = u64::MAX / 4;

/// Default batch size for the sequencer
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Transaction payload types supporting economy and fraud proofs
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
    /// Stake tokens for validator eligibility
    Stake {
        /// Amount to lock for staking
        amount: u64,
    },
    /// Unstake request (begins lock-up countdown)
    Unstake {
        /// Amount to unstake
        amount: u64,
    },
    /// Fraud proof submission
    FraudProof {
        /// Public key of the offending sequencer
        offender_id: [u8; 32],
        /// ID of the batch containing invalid transaction
        batch_id: [u8; 32],
        /// Index of the invalid transaction in the batch
        invalid_tx_index: u32,
        /// Merkle path + signature data proving invalidity
        proof_data: Vec<u8>,
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
}

impl Transaction {
    /// Compute the transaction hash using BLAKE3
    pub fn hash(&self) -> Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.sender);
        hasher.update(&self.payload_bytes());
        hasher.update(&self.signature);
        hasher.update(&self.timestamp.to_le_bytes());
        hasher.finalize()
    }

    /// Serialize the payload for hashing/signing
    pub fn payload_bytes(&self) -> Vec<u8> {
        match &self.payload {
            TransactionPayload::Transfer { recipient, amount, nonce } => {
                let mut bytes = vec![0u8]; // type discriminant
                bytes.extend_from_slice(recipient);
                bytes.extend_from_slice(&amount.to_le_bytes());
                bytes.extend_from_slice(&nonce.to_le_bytes());
                bytes
            }
            TransactionPayload::Stake { amount } => {
                let mut bytes = vec![1u8];
                bytes.extend_from_slice(&amount.to_le_bytes());
                bytes
            }
            TransactionPayload::Unstake { amount } => {
                let mut bytes = vec![2u8];
                bytes.extend_from_slice(&amount.to_le_bytes());
                bytes
            }
            TransactionPayload::FraudProof { offender_id, batch_id, invalid_tx_index, proof_data } => {
                let mut bytes = vec![3u8];
                bytes.extend_from_slice(offender_id);
                bytes.extend_from_slice(batch_id);
                bytes.extend_from_slice(&invalid_tx_index.to_le_bytes());
                bytes.extend_from_slice(proof_data);
                bytes
            }
        }
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

/// Processed transaction with geometric position
#[derive(Debug, Clone)]
pub struct ProcessedTransaction {
    /// Original transaction
    pub tx: Transaction,
    /// Geometric position derived from BLAKE3
    pub position: GeometricPosition,
    /// Transaction hash
    pub tx_hash: Hash,
}

/// Geometric position for transaction placement
#[derive(Debug, Clone, Copy)]
pub struct GeometricPosition {
    /// Transaction position (x, y) derived from commitment hash
    pub tx_position: (u64, u64),
    /// Assigning node's position (x, y)
    pub node_position: Option<(u64, u64)>,
    /// Euclidean distance between tx and node (using saturating math)
    pub distance: Option<u64>,
    /// Angular position (theta mod 2^256, stored as u64)
    pub theta: Option<u64>,
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

/// Slashing condition types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlashingReason {
    /// Transaction outside geometric acceptance zone
    InvalidStructure,
    /// Transaction signature doesn't match derived public key
    InvalidSignature,
    /// Transaction spends non-existent funds (post-settlement)
    InvalidState,
}

/// Result of fraud proof verification
#[derive(Debug, Clone)]
pub struct FraudProofResult {
    /// Whether the fraud proof is valid
    pub is_valid: bool,
    /// The slashing reason if valid
    pub reason: Option<SlashingReason>,
    /// Amount to slash from offender
    pub slash_amount: u64,
    /// Batch that should be rolled back
    pub batch_id: Option<Hash>,
}

/// Verification status for async validation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationStatus {
    /// Not yet verified
    Pending,
    /// Optimistically accepted (signature not checked)
    OptimisticAccept,
    /// Fully verified (signature validated)
    Verified,
    /// Invalid (fraud proof submitted)
    Invalid,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_hash() {
        let tx = Transaction {
            sender: [1u8; 32],
            payload: TransactionPayload::Transfer {
                recipient: [2u8; 32],
                amount: 1000,
                nonce: 1,
            },
            signature: [0u8; 64],
            timestamp: 12345,
        };

        let hash1 = tx.hash();
        let hash2 = tx.hash();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_payload_serialization() {
        let transfer = TransactionPayload::Transfer {
            recipient: [1u8; 32],
            amount: 100,
            nonce: 0,
        };
        
        let stake = TransactionPayload::Stake { amount: MIN_STAKE };
        
        let tx1 = Transaction {
            sender: [0u8; 32],
            payload: transfer,
            signature: [0u8; 64],
            timestamp: 0,
        };
        
        let tx2 = Transaction {
            sender: [0u8; 32],
            payload: stake,
            signature: [0u8; 64],
            timestamp: 0,
        };

        // Different payloads should produce different bytes
        assert_ne!(tx1.payload_bytes(), tx2.payload_bytes());
    }
}
