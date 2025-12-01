//! Simple persistence test to verify GeometricLedger integration
//!
//! This test:
//! 1. Creates a ledger and processes some transfers
//! 2. Closes the ledger
//! 3. Reopens the ledger to verify state was persisted

use pos::{GeometricLedger, Account};
use std::sync::Arc;

fn main() {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║       POS Persistence Test (GeometricLedger mmap)            ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    let db_path = "./data/persistence_test_db";

    // Clean up any existing test database
    let _ = std::fs::remove_dir_all(db_path);

    println!("1️⃣  Creating new ledger and processing transfers...");
    {
        let ledger = Arc::new(GeometricLedger::new(db_path).expect("Failed to create ledger"));

        // Create test accounts
        let alice = [1u8; 32];
        let bob = [2u8; 32];
        let charlie = [3u8; 32];

        // Give Alice initial balance
        ledger.mint(alice, 10_000);

        // Apply transfers using batch update
        let mut alice_acc = ledger.get(&alice).unwrap();
        alice_acc.balance -= 5_000;
        
        let bob_acc = Account { pubkey: bob, balance: 3_000, ..Default::default() };
        let charlie_acc = Account { pubkey: charlie, balance: 2_000, ..Default::default() };

        ledger.update_batch(&[
            (alice, alice_acc),
            (bob, bob_acc),
            (charlie, charlie_acc),
        ]).expect("Batch update failed");

        // Force flush to disk
        ledger.snapshot().expect("Snapshot failed");

        // Verify in-memory state
        let alice_balance = ledger.get(&alice).map(|a| a.balance).unwrap_or(0);
        let bob_balance = ledger.get(&bob).map(|a| a.balance).unwrap_or(0);
        let charlie_balance = ledger.get(&charlie).map(|a| a.balance).unwrap_or(0);

        println!("   Alice balance: {}", alice_balance);
        println!("   Bob balance: {}", bob_balance);
        println!("   Charlie balance: {}", charlie_balance);
        println!();

        assert_eq!(alice_balance, 5_000, "Alice should have 5,000 after transfers");
        assert_eq!(bob_balance, 3_000, "Bob should have 3,000");
        assert_eq!(charlie_balance, 2_000, "Charlie should have 2,000");
    } // Ledger is dropped here

    println!("2️⃣  Ledger closed. Reopening to verify persistence...");
    {
        let ledger = Arc::new(GeometricLedger::new(db_path).expect("Failed to reopen ledger"));

        let alice = [1u8; 32];
        let bob = [2u8; 32];
        let charlie = [3u8; 32];

        // Verify persisted state
        let alice_balance = ledger.get(&alice).map(|a| a.balance).unwrap_or(0);
        let bob_balance = ledger.get(&bob).map(|a| a.balance).unwrap_or(0);
        let charlie_balance = ledger.get(&charlie).map(|a| a.balance).unwrap_or(0);

        println!("   Alice balance (restored): {}", alice_balance);
        println!("   Bob balance (restored): {}", bob_balance);
        println!("   Charlie balance (restored): {}", charlie_balance);
        println!();

        assert_eq!(alice_balance, 5_000, "Alice balance should be restored");
        assert_eq!(bob_balance, 3_000, "Bob balance should be restored");
        assert_eq!(charlie_balance, 2_000, "Charlie balance should be restored");
    }

    println!("✅ SUCCESS! State was correctly persisted and restored.");
    println!();
    println!("📝 Summary:");
    println!("   • Memory-mapped files: Instant updates + OS-managed persistence");
    println!("   • GeometricLedger: 256 shards, 256M account capacity");
    println!("   • All balances restored correctly after restart");

    // Clean up test database
    let _ = std::fs::remove_dir_all(db_path);
}
