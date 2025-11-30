//! Ledger module for tracking balances, stakes, and slashing
//!
//! Implements the economic model from the POS spec:
//! - Minimum stake: 1,000,000 tokens
//! - Lock-up period: 7 days

use std::collections::HashMap;
use std::time::{Duration, Instant};
use blake3::Hash;
use parking_lot::RwLock;

use crate::types::{MIN_STAKE, STAKE_LOCK_PERIOD, SlashingReason, TransactionPayload, Transaction};

/// Account state in the ledger
#[derive(Debug, Clone)]
pub struct Account {
    /// Available balance (not staked)
    pub balance: u64,
    /// Amount currently staked
    pub staked: u64,
    /// Pending unstake amount
    pub pending_unstake: u64,
    /// Timestamp when unstake was initiated (for lock-up calculation)
    pub unstake_initiated: Option<Instant>,
    /// Transaction nonce for replay protection
    pub nonce: u64,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            balance: 0,
            staked: 0,
            pending_unstake: 0,
            unstake_initiated: None,
            nonce: 0,
        }
    }
}

impl Account {
    /// Create account with initial balance
    pub fn with_balance(balance: u64) -> Self {
        Self {
            balance,
            ..Default::default()
        }
    }

    /// Check if account can be a sequencer
    pub fn can_sequence(&self) -> bool {
        self.staked >= MIN_STAKE
    }

    /// Check if unstake lock period has passed
    pub fn can_complete_unstake(&self) -> bool {
        if let Some(initiated) = self.unstake_initiated {
            initiated.elapsed() >= Duration::from_secs(STAKE_LOCK_PERIOD)
        } else {
            false
        }
    }
}

/// Slashing event record
#[derive(Debug, Clone)]
pub struct SlashingEvent {
    /// Sequencer that was slashed
    pub sequencer_id: [u8; 32],
    /// Amount slashed
    pub amount: u64,
    /// Reason for slashing
    pub reason: SlashingReason,
    /// Batch that was invalidated
    pub batch_id: Hash,
    /// When the slashing occurred
    pub timestamp: Instant,
}

/// Global ledger state
pub struct Ledger {
    /// Account balances indexed by public key
    accounts: RwLock<HashMap<[u8; 32], Account>>,
    /// Set of active sequencers (staked >= MIN_STAKE)
    active_sequencers: RwLock<Vec<[u8; 32]>>,
    /// Slashing history
    slashing_events: RwLock<Vec<SlashingEvent>>,
    /// Batches pending rollback
    pending_rollbacks: RwLock<Vec<Hash>>,
}

impl Ledger {
    /// Create a new empty ledger
    pub fn new() -> Self {
        Self {
            accounts: RwLock::new(HashMap::new()),
            active_sequencers: RwLock::new(Vec::new()),
            slashing_events: RwLock::new(Vec::new()),
            pending_rollbacks: RwLock::new(Vec::new()),
        }
    }

    /// Get or create an account
    pub fn get_account(&self, pubkey: &[u8; 32]) -> Account {
        self.accounts
            .read()
            .get(pubkey)
            .cloned()
            .unwrap_or_default()
    }

    /// Set account balance (for testing/genesis)
    pub fn set_balance(&self, pubkey: [u8; 32], balance: u64) {
        let mut accounts = self.accounts.write();
        accounts
            .entry(pubkey)
            .or_insert_with(Account::default)
            .balance = balance;
    }

    /// Check if an address is an active sequencer
    pub fn is_active_sequencer(&self, pubkey: &[u8; 32]) -> bool {
        self.active_sequencers.read().contains(pubkey)
    }

    /// Process a stake transaction
    pub fn process_stake(&self, sender: [u8; 32], amount: u64) -> Result<(), LedgerError> {
        let mut accounts = self.accounts.write();
        let account = accounts.entry(sender).or_insert_with(Account::default);

        if account.balance < amount {
            return Err(LedgerError::InsufficientBalance {
                available: account.balance,
                required: amount,
            });
        }

        account.balance -= amount;
        account.staked += amount;

        // Update active sequencers if they now qualify
        if account.staked >= MIN_STAKE {
            let mut sequencers = self.active_sequencers.write();
            if !sequencers.contains(&sender) {
                sequencers.push(sender);
                tracing::info!(
                    sequencer = hex::encode(sender),
                    staked = account.staked,
                    "New sequencer registered"
                );
            }
        }

        Ok(())
    }

    /// Process an unstake request (starts lock-up period)
    pub fn process_unstake(&self, sender: [u8; 32], amount: u64) -> Result<(), LedgerError> {
        let mut accounts = self.accounts.write();
        let account = accounts.entry(sender).or_insert_with(Account::default);

        if account.staked < amount {
            return Err(LedgerError::InsufficientStake {
                staked: account.staked,
                requested: amount,
            });
        }

        account.staked -= amount;
        account.pending_unstake += amount;
        account.unstake_initiated = Some(Instant::now());

        // Remove from active sequencers if stake drops below minimum
        if account.staked < MIN_STAKE {
            let mut sequencers = self.active_sequencers.write();
            sequencers.retain(|s| s != &sender);
            tracing::info!(
                sequencer = hex::encode(sender),
                remaining_stake = account.staked,
                "Sequencer deactivated due to unstake"
            );
        }

        Ok(())
    }

    /// Complete unstake after lock-up period
    pub fn complete_unstake(&self, sender: [u8; 32]) -> Result<u64, LedgerError> {
        let mut accounts = self.accounts.write();
        let account = accounts
            .get_mut(&sender)
            .ok_or(LedgerError::AccountNotFound)?;

        if !account.can_complete_unstake() {
            return Err(LedgerError::UnstakeLocked);
        }

        let amount = account.pending_unstake;
        account.pending_unstake = 0;
        account.unstake_initiated = None;
        account.balance += amount;

        Ok(amount)
    }

    /// Process a transfer transaction
    pub fn process_transfer(
        &self,
        sender: [u8; 32],
        recipient: [u8; 32],
        amount: u64,
        nonce: u64,
    ) -> Result<(), LedgerError> {
        let mut accounts = self.accounts.write();

        // Check sender
        let sender_account = accounts.entry(sender).or_insert_with(Account::default);
        
        if sender_account.nonce != nonce {
            return Err(LedgerError::InvalidNonce {
                expected: sender_account.nonce,
                got: nonce,
            });
        }

        if sender_account.balance < amount {
            return Err(LedgerError::InsufficientBalance {
                available: sender_account.balance,
                required: amount,
            });
        }

        sender_account.balance -= amount;
        sender_account.nonce += 1;

        // Credit recipient
        let recipient_account = accounts.entry(recipient).or_insert_with(Account::default);
        recipient_account.balance += amount;

        Ok(())
    }

    /// Slash a sequencer for invalid behavior
    pub fn slash_sequencer(
        &self,
        sequencer_id: [u8; 32],
        reason: SlashingReason,
        batch_id: Hash,
    ) -> Result<u64, LedgerError> {
        let mut accounts = self.accounts.write();
        let account = accounts
            .get_mut(&sequencer_id)
            .ok_or(LedgerError::AccountNotFound)?;

        // Slash entire stake
        let slash_amount = account.staked;
        account.staked = 0;

        // Remove from active sequencers
        {
            let mut sequencers = self.active_sequencers.write();
            sequencers.retain(|s| s != &sequencer_id);
        }

        // Record slashing event
        {
            let mut events = self.slashing_events.write();
            events.push(SlashingEvent {
                sequencer_id,
                amount: slash_amount,
                reason,
                batch_id,
                timestamp: Instant::now(),
            });
        }

        // Mark batch for rollback
        {
            let mut rollbacks = self.pending_rollbacks.write();
            rollbacks.push(batch_id);
        }

        tracing::warn!(
            sequencer = hex::encode(sequencer_id),
            amount = slash_amount,
            reason = ?reason,
            "Sequencer slashed"
        );

        Ok(slash_amount)
    }

    /// Apply a transaction to the ledger (tentative/optimistic)
    pub fn apply_transaction(&self, tx: &Transaction) -> Result<(), LedgerError> {
        match &tx.payload {
            TransactionPayload::Transfer { recipient, amount, nonce } => {
                self.process_transfer(tx.sender, *recipient, *amount, *nonce)
            }
            TransactionPayload::Stake { amount } => {
                self.process_stake(tx.sender, *amount)
            }
            TransactionPayload::Unstake { amount } => {
                self.process_unstake(tx.sender, *amount)
            }
            TransactionPayload::FraudProof { .. } => {
                // Fraud proofs are handled separately by the verifier
                Ok(())
            }
        }
    }

    /// Get list of active sequencers
    pub fn get_active_sequencers(&self) -> Vec<[u8; 32]> {
        self.active_sequencers.read().clone()
    }

    /// Get slashing history
    pub fn get_slashing_events(&self) -> Vec<SlashingEvent> {
        self.slashing_events.read().clone()
    }

    /// Get total staked amount across all accounts
    pub fn total_staked(&self) -> u64 {
        self.accounts
            .read()
            .values()
            .map(|a| a.staked)
            .sum()
    }
}

impl Default for Ledger {
    fn default() -> Self {
        Self::new()
    }
}

/// Ledger errors
#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error("Insufficient balance: available {available}, required {required}")]
    InsufficientBalance { available: u64, required: u64 },

    #[error("Insufficient stake: staked {staked}, requested {requested}")]
    InsufficientStake { staked: u64, requested: u64 },

    #[error("Invalid nonce: expected {expected}, got {got}")]
    InvalidNonce { expected: u64, got: u64 },

    #[error("Account not found")]
    AccountNotFound,

    #[error("Unstake still locked (7 day lock-up period)")]
    UnstakeLocked,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stake_and_become_sequencer() {
        let ledger = Ledger::new();
        let pubkey = [1u8; 32];

        // Set initial balance
        ledger.set_balance(pubkey, MIN_STAKE * 2);

        // Stake minimum amount
        ledger.process_stake(pubkey, MIN_STAKE).unwrap();

        // Should now be an active sequencer
        assert!(ledger.is_active_sequencer(&pubkey));

        let account = ledger.get_account(&pubkey);
        assert_eq!(account.staked, MIN_STAKE);
        assert_eq!(account.balance, MIN_STAKE);
    }

    #[test]
    fn test_insufficient_balance() {
        let ledger = Ledger::new();
        let pubkey = [1u8; 32];

        ledger.set_balance(pubkey, 100);

        let result = ledger.process_stake(pubkey, 1000);
        assert!(matches!(result, Err(LedgerError::InsufficientBalance { .. })));
    }

    #[test]
    fn test_transfer() {
        let ledger = Ledger::new();
        let sender = [1u8; 32];
        let recipient = [2u8; 32];

        ledger.set_balance(sender, 1000);

        ledger.process_transfer(sender, recipient, 500, 0).unwrap();

        assert_eq!(ledger.get_account(&sender).balance, 500);
        assert_eq!(ledger.get_account(&recipient).balance, 500);
    }

    #[test]
    fn test_slashing() {
        let ledger = Ledger::new();
        let sequencer = [1u8; 32];

        ledger.set_balance(sequencer, MIN_STAKE);
        ledger.process_stake(sequencer, MIN_STAKE).unwrap();
        assert!(ledger.is_active_sequencer(&sequencer));

        let batch_id = blake3::hash(b"test batch");
        let slashed = ledger
            .slash_sequencer(sequencer, SlashingReason::InvalidSignature, batch_id)
            .unwrap();

        assert_eq!(slashed, MIN_STAKE);
        assert!(!ledger.is_active_sequencer(&sequencer));
        assert_eq!(ledger.get_account(&sequencer).staked, 0);
    }
}
