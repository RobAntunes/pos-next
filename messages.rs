//! Wire Protocol Message Types
//!
//! Defines binary message format for node-to-node communication using bincode.
//! Messages are exchanged over QUIC streams:
//! - Stream 0 (Control): Headers, handshakes, requests
//! - Stream 1 (Data): Bulk transaction data

use blake3::Hash;
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

use crate::types::{BatchHeader, Transaction, TransactionPayload};

/// Network protocol version
pub const PROTOCOL_VERSION: u32 = 1;

/// Wire message types for node communication
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WireMessage {
    /// Initial handshake when establishing connection
    Handshake {
        /// Protocol version for compatibility checking
        version: u32,
        /// Node's geometric ID (derived from public key)
        geometric_id: [u8; 32],
        /// Node's QUIC listen port
        listen_port: u16,
    },

    /// Broadcast new batch header to network (Stream 0)
    BatchProposal {
        /// The batch header with structure proof
        header: SerializableBatchHeader,
    },

    /// Request full batch data by hash (Stream 1)
    RequestBatch {
        /// Hash of the batch to retrieve
        batch_id: [u8; 32],
        /// Optional: specific transaction indices to fetch
        tx_indices: Option<Vec<u32>>,
    },

    /// Response with batch transaction data (Stream 1)
    BatchData {
        /// ID of the batch
        batch_id: [u8; 32],
        /// Serialized transactions (can be 45KB+ for full batches)
        transactions: Vec<SerializableTransaction>,
    },

    /// Submit individual transaction to peer's mempool
    TransactionSubmission {
        /// The transaction to add to mempool
        tx: SerializableTransaction,
    },

    /// Request peer's current mempool size (for load balancing)
    MempoolStatus,

    /// Response with mempool metrics
    MempoolStatusResponse {
        /// Number of pending transactions
        pending_count: u64,
        /// Estimated TPS capacity
        capacity_tps: u64,
    },

    /// Acknowledge receipt of data
    Ack {
        /// Message ID being acknowledged
        msg_id: [u8; 32],
    },
}

/// Serializable version of BatchHeader (bincode-compatible)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableBatchHeader {
    pub sequencer_id: [u8; 32],
    pub round_id: u64,
    pub structure_root: [u8; 32],
    pub set_xor: [u8; 32],
    pub tx_count: u32,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

impl From<BatchHeader> for SerializableBatchHeader {
    fn from(header: BatchHeader) -> Self {
        Self {
            sequencer_id: header.sequencer_id,
            round_id: header.round_id,
            structure_root: *header.structure_root.as_bytes(),
            set_xor: *header.set_xor.as_bytes(),
            tx_count: header.tx_count,
            signature: header.signature,
        }
    }
}

impl From<SerializableBatchHeader> for BatchHeader {
    fn from(header: SerializableBatchHeader) -> Self {
        Self {
            sequencer_id: header.sequencer_id,
            round_id: header.round_id,
            structure_root: Hash::from_bytes(header.structure_root),
            set_xor: Hash::from_bytes(header.set_xor),
            tx_count: header.tx_count,
            signature: header.signature,
        }
    }
}

/// Serializable version of Transaction (bincode-compatible)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableTransaction {
    pub sender: [u8; 32],
    pub payload: TransactionPayload,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
    pub timestamp: u64,
}

impl From<Transaction> for SerializableTransaction {
    fn from(tx: Transaction) -> Self {
        Self {
            sender: tx.sender,
            payload: tx.payload,
            signature: tx.signature,
            timestamp: tx.timestamp,
        }
    }
}

impl From<SerializableTransaction> for Transaction {
    fn from(tx: SerializableTransaction) -> Self {
        Self {
            sender: tx.sender,
            payload: tx.payload,
            signature: tx.signature,
            timestamp: tx.timestamp,
        }
    }
}

/// Serialize message to bytes using bincode (fast, compact)
pub fn to_bytes<T: Serialize>(msg: &T) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(msg)
}

/// Deserialize bytes to message using bincode
pub fn from_bytes<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, bincode::Error> {
    bincode::deserialize(bytes)
}

/// Helper to serialize WireMessage
pub fn serialize_message(msg: &WireMessage) -> Result<Vec<u8>, bincode::Error> {
    to_bytes(msg)
}

/// Helper to deserialize WireMessage
pub fn deserialize_message(bytes: &[u8]) -> Result<WireMessage, bincode::Error> {
    from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TransactionPayload;

    #[test]
    fn test_handshake_serialization() {
        let msg = WireMessage::Handshake {
            version: PROTOCOL_VERSION,
            geometric_id: [42u8; 32],
            listen_port: 9000,
        };

        let bytes = serialize_message(&msg).unwrap();
        let deserialized: WireMessage = deserialize_message(&bytes).unwrap();

        match deserialized {
            WireMessage::Handshake { version, geometric_id, listen_port } => {
                assert_eq!(version, PROTOCOL_VERSION);
                assert_eq!(geometric_id, [42u8; 32]);
                assert_eq!(listen_port, 9000);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_transaction_roundtrip() {
        let tx = SerializableTransaction {
            sender: [1u8; 32],
            payload: TransactionPayload::Transfer {
                recipient: [2u8; 32],
                amount: 1000,
                nonce: 1,
            },
            signature: [0u8; 64],
            timestamp: 12345,
        };

        let msg = WireMessage::TransactionSubmission { tx: tx.clone() };
        let bytes = serialize_message(&msg).unwrap();
        let deserialized: WireMessage = deserialize_message(&bytes).unwrap();

        match deserialized {
            WireMessage::TransactionSubmission { tx: tx2 } => {
                assert_eq!(tx.sender, tx2.sender);
                assert_eq!(tx.timestamp, tx2.timestamp);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_batch_proposal_size() {
        let header = SerializableBatchHeader {
            sequencer_id: [1u8; 32],
            round_id: 100,
            structure_root: [2u8; 32],
            set_xor: [3u8; 32],
            tx_count: 10000,
            signature: [0u8; 64],
        };

        let msg = WireMessage::BatchProposal { header };
        let bytes = serialize_message(&msg).unwrap();
        
        // Should be compact (< 200 bytes for header)
        assert!(bytes.len() < 200, "Header too large: {} bytes", bytes.len());
    }
}
