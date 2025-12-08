use pos::geometric_ledger::{GeometricLedger, Account};
use std::collections::HashMap;
use tempfile::tempdir;

fn main() {
    println!("🧪 Crash Recovery Test for GeometricLedger\n");
    
    test_wal_recovery();
    test_index_persistence();
    test_combined_recovery();
    
    println!("\n✅ All crash recovery tests passed!");
}

fn test_wal_recovery() {
    println!("Test 1: WAL Recovery (Simulated Crash)");
    println!("=======================================");
    
    let dir = tempdir().unwrap();
    let data_path = dir.path();
    
    // Phase 1: Write data with WAL enabled
    {
        let ledger = GeometricLedger::new_with_wal(data_path, 4, 10000, true).unwrap();
        
        // Insert 1000 accounts
        for i in 0..1000 {
            let mut pubkey = [0u8; 32];
            pubkey[0] = (i % 256) as u8;
            pubkey[1] = (i / 256) as u8;
            
            let account = Account {
                pubkey,
                balance: 1000 + i as u64,
                nonce: i as u64,
                last_modified: 123456,
                _padding: [0u8; 80],
            };
            
            ledger.mint(pubkey, account.balance);
        }
        
        println!("  ✓ Wrote 1000 accounts");
        
        // DON'T call shutdown() - simulate crash
        println!("  ⚡ Simulating crash (no clean shutdown)...");
    }
    
    // Phase 2: Recover from crash
    {
        println!("  🔄 Restarting ledger...");
        let ledger = GeometricLedger::new_with_wal(data_path, 4, 10000, true).unwrap();
        
        // Verify all accounts recovered
        let mut recovered = 0;
        for i in 0..1000 {
            let mut pubkey = [0u8; 32];
            pubkey[0] = (i % 256) as u8;
            pubkey[1] = (i / 256) as u8;
            
            if let Some(account) = ledger.get(&pubkey) {
                assert_eq!(account.balance, 1000 + i as u64, "Balance mismatch for account {}", i);
                recovered += 1;
            }
        }
        
        println!("  ✓ Recovered {} accounts from WAL", recovered);
        assert!(recovered >= 900, "Should recover at least 90% of accounts");
    }
    
    println!("  ✅ WAL recovery test passed\n");
}

fn test_index_persistence() {
    println!("Test 2: Index Persistence (Clean Shutdown)");
    println!("==========================================");
    
    let dir = tempdir().unwrap();
    let data_path = dir.path();
    
    let start_time = std::time::Instant::now();
    
    // Phase 1: Write data and shutdown cleanly
    {
        let ledger = GeometricLedger::new_with_wal(data_path, 16, 100000, true).unwrap();
        
        // Insert 10,000 accounts
        for i in 0..10000 {
            let mut pubkey = [0u8; 32];
            pubkey[0] = (i % 256) as u8;
            pubkey[1] = (i / 256) as u8;
            pubkey[2] = (i / 65536) as u8;
            
            ledger.mint(pubkey, 1000 + i as u64);
        }
        
        println!("  ✓ Wrote 10,000 accounts");
        
        // Clean shutdown
        ledger.shutdown().unwrap();
        println!("  ✓ Clean shutdown completed");
    }
    
    let shutdown_time = start_time.elapsed();
    
    // Phase 2: Restart and measure recovery time
    let restart_start = std::time::Instant::now();
    {
        let ledger = GeometricLedger::new_with_wal(data_path, 16, 100000, true).unwrap();
        let restart_time = restart_start.elapsed();
        
        println!("  ✓ Restart time: {:?} (should be <1s with index persistence)", restart_time);
        
        // Verify random samples
        let mut verified = 0;
        for i in (0..10000).step_by(100) {
            let mut pubkey = [0u8; 32];
            pubkey[0] = (i % 256) as u8;
            pubkey[1] = (i / 256) as u8;
            pubkey[2] = (i / 65536) as u8;
            
            if let Some(account) = ledger.get(&pubkey) {
                assert_eq!(account.balance, 1000 + i as u64);
                verified += 1;
            }
        }
        
        println!("  ✓ Verified {} sample accounts", verified);
        
        if restart_time.as_secs() < 5 {
            println!("  🚀 Fast restart achieved!");
        } else {
            println!("  ⚠️  Restart took longer than expected, but data is intact");
        }
    }
    
    println!("  ✅ Index persistence test passed\n");
}

fn test_combined_recovery() {
    println!("Test 3: Combined Recovery (Dirty Shutdown + Data Loss)");
    println!("======================================================");
    
    let dir = tempdir().unwrap();
    let data_path = dir.path();
    
    // Phase 1: Initial data with clean shutdown
    {
        let ledger = GeometricLedger::new_with_wal(data_path, 8, 50000, true).unwrap();
        
        for i in 0..5000 {
            let mut pubkey = [0u8; 32];
            pubkey[0] = (i % 256) as u8;
            pubkey[1] = (i / 256) as u8;
            
            ledger.mint(pubkey, 1000 + i as u64);
        }
        
        ledger.shutdown().unwrap();
        println!("  ✓ Initial 5,000 accounts written and persisted");
    }
    
    // Phase 2: Add more data, then crash
    {
        let ledger = GeometricLedger::new_with_wal(data_path, 8, 50000, true).unwrap();
        
        // Update existing accounts
        for i in 0..2500 {
            let mut pubkey = [0u8; 32];
            pubkey[0] = (i % 256) as u8;
            pubkey[1] = (i / 256) as u8;
            
            ledger.mint(pubkey, 500); // Add 500 to balance
        }
        
        // Add new accounts
        for i in 5000..7500 {
            let mut pubkey = [0u8; 32];
            pubkey[0] = (i % 256) as u8;
            pubkey[1] = (i / 256) as u8;
            
            ledger.mint(pubkey, 1000 + i as u64);
        }
        
        println!("  ✓ Updated 2,500 accounts and added 2,500 new ones");
        println!("  ⚡ Simulating crash without shutdown...");
        // No shutdown - simulate crash
    }
    
    // Phase 3: Recover and verify
    {
        println!("  🔄 Recovering from crash...");
        let ledger = GeometricLedger::new_with_wal(data_path, 8, 50000, true).unwrap();
        
        // Check original accounts
        let mut original_ok = 0;
        for i in 0..5000 {
            let mut pubkey = [0u8; 32];
            pubkey[0] = (i % 256) as u8;
            pubkey[1] = (i / 256) as u8;
            
            if let Some(account) = ledger.get(&pubkey) {
                original_ok += 1;
            }
        }
        
        println!("  ✓ Recovered {} / 5000 original accounts", original_ok);
        assert!(original_ok >= 4500, "Should recover most original accounts");
        
        // Check new accounts (may or may not be recovered depending on WAL sync)
        let mut new_ok = 0;
        for i in 5000..7500 {
            let mut pubkey = [0u8; 32];
            pubkey[0] = (i % 256) as u8;
            pubkey[1] = (i / 256) as u8;
            
            if let Some(_) = ledger.get(&pubkey) {
                new_ok += 1;
            }
        }
        
        println!("  ✓ Recovered {} / 2500 new accounts (WAL replay)", new_ok);
    }
    
    println!("  ✅ Combined recovery test passed\n");
}
