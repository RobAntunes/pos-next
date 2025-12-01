//! Simple persistence test to verify RocksDB integration
//!
//! This test:
//! 1. Creates a ledger and processes some transactions
//! 2. Closes the ledger
//! 3. Reopens the ledger to verify state was persisted

use pos::{Ledger, Transaction, TransactionPayload};
use std::sync::Arc;

fn main() {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         POS Persistence Test (RocksDB Integration)          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    let db_path = "./data/persistence_test_db";

    // Clean up any existing test database
    let _ = std::fs::remove_dir_all(db_path);

    println!("1️⃣  Creating new ledger and processing transactions...");
    {
        let ledger = Arc::new(Ledger::new(db_path));

        // Create test accounts
        let alice = [1u8; 32];
        let bob = [2u8; 32];
        let charlie = [3u8; 32];

        // Give Alice initial balance
        ledger.set_balance(alice, 10_000);

        // Create transactions
        let transactions = vec![
            Transaction::new(
                alice,
                TransactionPayload::Transfer {
                    recipient: bob,
                    amount: 3_000,
                    nonce: 0,
                },
                [0u8; 64],
                1234567890,
            ),
            Transaction::new(
                alice,
                TransactionPayload::Transfer {
                    recipient: charlie,
                    amount: 2_000,
                    nonce: 1,
                },
                [0u8; 64],
                1234567891,
            ),
        ];

        // Apply batch (this will persist to RocksDB)
        ledger.apply_batch(&transactions);

        // Verify in-memory state
        let alice_balance = ledger.get_account(&alice).balance;
        let bob_balance = ledger.get_account(&bob).balance;
        let charlie_balance = ledger.get_account(&charlie).balance;

        println!("   Alice balance: {}", alice_balance);
        println!("   Bob balance: {}", bob_balance);
        println!("   Charlie balance: {}", charlie_balance);
        println!("   Account count: {}", ledger.account_count());
        println!();

        assert_eq!(alice_balance, 5_000, "Alice should have 5,000 after transfers");
        assert_eq!(bob_balance, 3_000, "Bob should have 3,000");
        assert_eq!(charlie_balance, 2_000, "Charlie should have 2,000");
    } // Ledger is dropped here, forcing a close

    println!("2️⃣  Ledger closed. Reopening to verify persistence...");
    {
        let ledger = Arc::new(Ledger::new(db_path));

        let alice = [1u8; 32];
        let bob = [2u8; 32];
        let charlie = [3u8; 32];

        // Verify persisted state
        let alice_balance = ledger.get_account(&alice).balance;
        let bob_balance = ledger.get_account(&bob).balance;
        let charlie_balance = ledger.get_account(&charlie).balance;

        println!("   Alice balance (restored): {}", alice_balance);
        println!("   Bob balance (restored): {}", bob_balance);
        println!("   Charlie balance (restored): {}", charlie_balance);
        println!("   Account count: {}", ledger.account_count());
        println!();

        assert_eq!(alice_balance, 5_000, "Alice balance should be restored");
        assert_eq!(bob_balance, 3_000, "Bob balance should be restored");
        assert_eq!(charlie_balance, 2_000, "Charlie balance should be restored");
    }

    println!("✅ SUCCESS! State was correctly persisted and restored.");
    println!();
    println!("📝 Summary:");
    println!("   • Write-through cache: Instant RAM updates + disk persistence");
    println!("   • RocksDB integration: Successful crash recovery");
    println!("   • All balances restored correctly after restart");

    // Clean up test database
    let _ = std::fs::remove_dir_all(db_path);
}
