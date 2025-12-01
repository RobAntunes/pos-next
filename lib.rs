pub mod protocol;
pub mod metrics;
pub mod types;
// pub mod ledger;  // Old RocksDB-based ledger - replaced with geometric_ledger
pub mod geometric_ledger;
pub mod sequencer;
// pub mod verifier;  // Depends on old ledger - not used by node-net
pub mod round;
pub mod network;
pub mod messages;
pub mod mempool;
pub mod ring;
pub mod tui;

pub use protocol::{TangleProtocol, Commitment};
pub use metrics::NodeMetrics;
pub use types::{
    Transaction, TransactionPayload, ProcessedTransaction, RingInfo, RingPosition,
    Batch, BatchHeader, MIN_STAKE, MAX_DISTANCE, DEFAULT_BATCH_SIZE,
    SlashingReason, FraudProofResult, VerificationStatus,
};
// pub use ledger::{Ledger, LedgerError};  // Using GeometricLedger instead
pub use geometric_ledger::{GeometricLedger, Account};
pub use sequencer::{Sequencer, SequencerConfig};
// pub use verifier::{Verifier, VerifierConfig};  // Commented out - depends on old ledger
pub use round::{RoundManager, RoundConfig, RoundMetrics};
pub use messages::{WireMessage, SerializableBatchHeader, SerializableTransaction, PROTOCOL_VERSION};
pub use mempool::{Mempool, MempoolConfig, MempoolStats};
pub use ring::{RingRoutingTable, VirtualNodeConfig, calculate_ring_position};
