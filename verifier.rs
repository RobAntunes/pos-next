//! Verifier module - Layer 2 (Settlement Layer)
//!
//! Asynchronous validation of signatures and state transitions.
//! Guarantees: Correctness and Finality
//!
//! Responsibilities:
//! 1. Lazy validation - accept batches optimistically
//! 2. Random sampling of transactions for signature verification
//! 3. Fraud proof construction and verification
//! 4. Slashing coordination

use std::collections::HashMap;
use std::sync::Arc;

use ed25519_dalek::{Signature, VerifyingKey};
use parking_lot::RwLock;
use rand::Rng;

use crate::ledger::Ledger;
use crate::sequencer::verify_set_xor;
use crate::types::{
    Batch, FraudProofResult, SlashingReason, Transaction,
    TransactionPayload, VerificationStatus, MAX_DISTANCE,
};

/// Verifier configuration
#[derive(Debug, Clone)]
pub struct VerifierConfig {
    /// Percentage of transactions to sample for verification (0.0 - 1.0)
    pub sample_rate: f64,
    /// Whether to verify all fraud proofs immediately
    pub immediate_fraud_check: bool,
}

impl Default for VerifierConfig {
    fn default() -> Self {
        Self {
            sample_rate: 0.01, // 1% sampling rate
            immediate_fraud_check: true,
        }
    }
}

/// Tracked batch state
#[derive(Debug, Clone)]
pub struct TrackedBatch {
    pub batch: Batch,
    pub status: VerificationStatus,
    pub verified_count: usize,
    pub invalid_indices: Vec<u32>,
}

/// Asynchronous verifier for Layer 2
pub struct Verifier {
    config: VerifierConfig,
    /// Ledger for balance/state checking
    ledger: Arc<Ledger>,
    /// Tracked batches by ID
    batches: RwLock<HashMap<[u8; 32], TrackedBatch>>,
    /// Pending fraud proofs to process
    pending_fraud_proofs: RwLock<Vec<FraudProofData>>,
}

/// Internal fraud proof data
#[derive(Debug, Clone)]
struct FraudProofData {
    offender_id: [u8; 32],
    batch_id: [u8; 32],
    invalid_tx_index: u32,
    reason: SlashingReason,
}

impl Verifier {
    /// Create a new verifier
    pub fn new(config: VerifierConfig, ledger: Arc<Ledger>) -> Self {
        Self {
            config,
            ledger,
            batches: RwLock::new(HashMap::new()),
            pending_fraud_proofs: RwLock::new(Vec::new()),
        }
    }

    /// Accept a batch optimistically (lazy validation)
    pub fn accept_batch_optimistic(&self, batch: Batch) {
        let batch_id = *batch.header.hash().as_bytes();

        // Verify sequencer is active
        if !self.ledger.is_active_sequencer(&batch.header.sequencer_id) {
            tracing::warn!(
                sequencer = hex::encode(batch.header.sequencer_id),
                "Rejecting batch from non-sequencer"
            );
            return;
        }

        // Verify set XOR (O(1) check)
        let tx_hashes: Vec<_> = batch.transactions.iter().map(|t| t.tx_hash).collect();
        if !verify_set_xor(&tx_hashes, batch.header.set_xor.as_bytes()) {
            tracing::warn!(batch_id = hex::encode(batch_id), "Set XOR mismatch");
            self.queue_fraud_proof(
                batch.header.sequencer_id,
                batch_id,
                0,
                SlashingReason::InvalidStructure,
            );
            return;
        }

        // Apply transactions optimistically to ledger
        for ptx in &batch.transactions {
            if let Err(e) = self.ledger.apply_transaction(&ptx.tx) {
                tracing::debug!(
                    tx_hash = hex::encode(ptx.tx_hash.as_bytes()),
                    error = %e,
                    "Transaction failed ledger application (optimistic)"
                );
            }
        }

        // Track the batch
        let tracked = TrackedBatch {
            batch,
            status: VerificationStatus::OptimisticAccept,
            verified_count: 0,
            invalid_indices: Vec::new(),
        };

        self.batches.write().insert(batch_id, tracked);

        tracing::debug!(
            batch_id = hex::encode(batch_id),
            tx_count = tx_hashes.len(),
            "Batch accepted optimistically"
        );
    }

    /// Perform random sampling verification on a batch
    pub async fn verify_batch_sampled(&self, batch_id: [u8; 32]) -> VerificationStatus {
        let mut batches = self.batches.write();
        let tracked = match batches.get_mut(&batch_id) {
            Some(t) => t,
            None => return VerificationStatus::Pending,
        };

        let tx_count = tracked.batch.transactions.len();
        let sample_count = ((tx_count as f64) * self.config.sample_rate).ceil() as usize;
        let sample_count = sample_count.max(1).min(tx_count);

        let mut rng = rand::thread_rng();
        let mut sampled_indices: Vec<usize> = Vec::with_capacity(sample_count);

        // Random sampling without replacement
        while sampled_indices.len() < sample_count {
            let idx = rng.gen_range(0..tx_count);
            if !sampled_indices.contains(&idx) {
                sampled_indices.push(idx);
            }
        }

        let mut invalid_found = false;

        for idx in sampled_indices {
            let ptx = &tracked.batch.transactions[idx];

            // Verify signature (expensive Ed25519 operation)
            if !self.verify_signature(&ptx.tx) {
                tracked.invalid_indices.push(idx as u32);
                invalid_found = true;

                // Queue fraud proof
                self.queue_fraud_proof(
                    tracked.batch.header.sequencer_id,
                    batch_id,
                    idx as u32,
                    SlashingReason::InvalidSignature,
                );
            }

            // Verify ring position is within bounds
            if let Some(distance) = ptx.ring_info.distance {
                if distance > MAX_DISTANCE {
                    tracked.invalid_indices.push(idx as u32);
                    invalid_found = true;

                    self.queue_fraud_proof(
                        tracked.batch.header.sequencer_id,
                        batch_id,
                        idx as u32,
                        SlashingReason::InvalidStructure,
                    );
                }
            }

            tracked.verified_count += 1;
        }

        if invalid_found {
            tracked.status = VerificationStatus::Invalid;
        } else {
            tracked.status = VerificationStatus::Verified;
        }

        tracked.status
    }

    /// Verify a transaction's Ed25519 signature
    fn verify_signature(&self, tx: &Transaction) -> bool {
        // Get the message that was signed
        let message = tx.signing_message();

        // Try to construct the verifying key from sender pubkey
        let verifying_key = match VerifyingKey::from_bytes(&tx.sender) {
            Ok(k) => k,
            Err(_) => return false,
        };

        // Construct the signature (from_bytes returns Signature directly in ed25519-dalek 2.x)
        let signature = Signature::from_bytes(&tx.signature);

        // Verify
        verifying_key.verify_strict(&message, &signature).is_ok()
    }

    /// Queue a fraud proof for processing
    fn queue_fraud_proof(
        &self,
        offender_id: [u8; 32],
        batch_id: [u8; 32],
        invalid_tx_index: u32,
        reason: SlashingReason,
    ) {
        let proof = FraudProofData {
            offender_id,
            batch_id,
            invalid_tx_index,
            reason,
        };

        self.pending_fraud_proofs.write().push(proof);

        if self.config.immediate_fraud_check {
            // Process immediately
            let _ = self.process_fraud_proofs();
        }
    }

    /// Process all pending fraud proofs
    pub fn process_fraud_proofs(&self) -> Vec<FraudProofResult> {
        let proofs = std::mem::take(&mut *self.pending_fraud_proofs.write());
        let mut results = Vec::with_capacity(proofs.len());

        for proof in proofs {
            let result = self.verify_and_execute_fraud_proof(proof);
            results.push(result);
        }

        results
    }

    /// Verify a fraud proof and execute slashing if valid
    fn verify_and_execute_fraud_proof(&self, proof: FraudProofData) -> FraudProofResult {
        // Get the batch
        let batches = self.batches.read();
        let tracked = match batches.get(&proof.batch_id) {
            Some(t) => t,
            None => {
                return FraudProofResult {
                    is_valid: false,
                    reason: None,
                    slash_amount: 0,
                    batch_id: None,
                }
            }
        };

        // Verify the invalid transaction exists
        if proof.invalid_tx_index as usize >= tracked.batch.transactions.len() {
            return FraudProofResult {
                is_valid: false,
                reason: None,
                slash_amount: 0,
                batch_id: None,
            };
        }

        let ptx = &tracked.batch.transactions[proof.invalid_tx_index as usize];

        // Re-verify the claimed invalidity
        let is_actually_invalid = match proof.reason {
            SlashingReason::InvalidSignature => !self.verify_signature(&ptx.tx),
            SlashingReason::InvalidStructure => {
                ptx.ring_info.distance.map(|d| d > MAX_DISTANCE).unwrap_or(false)
            }
            SlashingReason::InvalidState => {
                // Would need to check against historical state
                // For now, trust the claim
                true
            }
        };

        if !is_actually_invalid {
            return FraudProofResult {
                is_valid: false,
                reason: None,
                slash_amount: 0,
                batch_id: None,
            };
        }

        // Execute slashing
        let batch_hash = blake3::Hash::from_bytes(proof.batch_id);
        match self.ledger.slash_sequencer(proof.offender_id, proof.reason, batch_hash) {
            Ok(amount) => FraudProofResult {
                is_valid: true,
                reason: Some(proof.reason),
                slash_amount: amount,
                batch_id: Some(batch_hash),
            },
            Err(e) => {
                tracing::error!(error = %e, "Failed to slash sequencer");
                FraudProofResult {
                    is_valid: true,
                    reason: Some(proof.reason),
                    slash_amount: 0,
                    batch_id: Some(batch_hash),
                }
            }
        }
    }

    /// Handle incoming fraud proof transaction
    pub fn handle_fraud_proof_tx(&self, tx: &Transaction) -> Option<FraudProofResult> {
        if let TransactionPayload::FraudProof {
            offender_id,
            batch_id,
            invalid_tx_index,
            proof_data: _,
        } = &tx.payload
        {
            // For now, assume InvalidSignature - real impl would decode proof_data
            let proof = FraudProofData {
                offender_id: *offender_id,
                batch_id: *batch_id,
                invalid_tx_index: *invalid_tx_index,
                reason: SlashingReason::InvalidSignature,
            };

            Some(self.verify_and_execute_fraud_proof(proof))
        } else {
            None
        }
    }

    /// Get batch status
    pub fn get_batch_status(&self, batch_id: &[u8; 32]) -> Option<VerificationStatus> {
        self.batches.read().get(batch_id).map(|t| t.status)
    }

    /// Get number of tracked batches
    pub fn tracked_batch_count(&self) -> usize {
        self.batches.read().len()
    }

    /// Get pending fraud proof count
    pub fn pending_fraud_proof_count(&self) -> usize {
        self.pending_fraud_proofs.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MIN_STAKE;

    fn make_test_batch(tx_count: usize) -> Batch {
        use crate::types::{BatchHeader, ProcessedTransaction, RingInfo};

        let transactions: Vec<_> = (0..tx_count)
            .map(|i| {
                let tx = Transaction::new(
                    [1u8; 32],
                    TransactionPayload::Transfer {
                        recipient: [2u8; 32],
                        amount: 100,
                        nonce: i as u64,
                    },
                    [0u8; 64],
                    i as u64,
                );
                let tx_hash = tx.hash();
                ProcessedTransaction {
                    tx,
                    ring_info: RingInfo {
                        tx_position: 0,
                        node_position: Some(0),
                        distance: Some(0),
                    },
                    tx_hash,
                }
            })
            .collect();

        // Calculate XOR
        let mut xor = [0u8; 32];
        for ptx in &transactions {
            for (i, b) in ptx.tx_hash.as_bytes().iter().enumerate() {
                xor[i] ^= b;
            }
        }

        Batch {
            header: BatchHeader {
                sequencer_id: [1u8; 32],
                round_id: 0,
                structure_root: blake3::hash(b"test"),
                set_xor: blake3::Hash::from_bytes(xor),
                tx_count: tx_count as u32,
                signature: [0u8; 64],
            },
            transactions,
        }
    }

    #[test]
    fn test_optimistic_accept() {
        let temp_dir = format!("/tmp/pos_test_{}", rand::random::<u64>());
        let ledger = Arc::new(Ledger::new(&temp_dir));
        // Make sequencer active
        ledger.set_balance([1u8; 32], MIN_STAKE);
        ledger.process_stake([1u8; 32], MIN_STAKE).unwrap();

        let verifier = Verifier::new(VerifierConfig::default(), ledger);
        let batch = make_test_batch(10);

        verifier.accept_batch_optimistic(batch.clone());

        let batch_id = *batch.header.hash().as_bytes();
        assert_eq!(
            verifier.get_batch_status(&batch_id),
            Some(VerificationStatus::OptimisticAccept)
        );
    }

    #[test]
    fn test_reject_non_sequencer() {
        let temp_dir = format!("/tmp/pos_test_{}", rand::random::<u64>());
        let ledger = Arc::new(Ledger::new(&temp_dir));
        // Don't stake - not a sequencer

        let verifier = Verifier::new(VerifierConfig::default(), ledger);
        let batch = make_test_batch(10);

        verifier.accept_batch_optimistic(batch.clone());

        // Should not be tracked (rejected)
        let batch_id = *batch.header.hash().as_bytes();
        assert_eq!(verifier.get_batch_status(&batch_id), None);
    }

    #[tokio::test]
    async fn test_sampled_verification() {
        let temp_dir = format!("/tmp/pos_test_{}", rand::random::<u64>());
        let ledger = Arc::new(Ledger::new(&temp_dir));
        ledger.set_balance([1u8; 32], MIN_STAKE);
        ledger.process_stake([1u8; 32], MIN_STAKE).unwrap();

        let config = VerifierConfig {
            sample_rate: 1.0, // Verify all for test
            immediate_fraud_check: false,
        };
        let verifier = Verifier::new(config, ledger);
        let batch = make_test_batch(10);
        let batch_id = *batch.header.hash().as_bytes();

        verifier.accept_batch_optimistic(batch);

        // Signatures are invalid (all zeros), so this should find issues
        let status = verifier.verify_batch_sampled(batch_id).await;
        assert_eq!(status, VerificationStatus::Invalid);
    }
}
