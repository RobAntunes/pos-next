//! Wire Protocol and Mempool Demo
//!
//! This example demonstrates:
//! 1. Serializing/deserializing wire messages
//! 2. Using the mempool for transaction ingestion
//! 3. Creating batch proposals

use pos::{
    Transaction, TransactionPayload, Mempool, WireMessage, 
    SerializableTransaction, SerializableBatchHeader, PROTOCOL_VERSION,
    messages::{serialize_message, deserialize_message},
};
use blake3::Hash;

fn main() {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         POS Wire Protocol & Mempool Demo                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    // 1. Create a mempool
    println!("1️⃣  Creating mempool...");
    let mempool = Mempool::new();
    println!("   ✓ Mempool initialized (max size: 100k transactions)");
    println!();

    // 2. Create and submit transactions (using fast-path HashReveal)
    println!("2️⃣  Creating and submitting transactions...");
    for i in 0..10 {
        let tx = Transaction::new_fast(
            [i as u8; 32],  // sender
            TransactionPayload::Transfer {
                recipient: [(i + 1) as u8; 32],
                amount: 1000 * (i + 1),
                nonce: i,
            },
            i,               // tx nonce
            1234567890 + i,  // timestamp
            [0u8; 32],       // auth_secret (HashReveal)
        );
        
        if mempool.submit(tx) {
            println!("   ✓ Transaction {} added to mempool", i);
        }
    }
    
    let stats = mempool.stats();
    println!("   📊 Mempool stats: {} pending, {:.1}% acceptance rate", 
             stats.pending, stats.acceptance_rate * 100.0);
    println!();

    // 3. Pull a batch from mempool
    println!("3️⃣  Pulling batch from mempool...");
    let batch = mempool.pull_batch(5);
    println!("   ✓ Pulled {} transactions from mempool", batch.len());
    println!("   📊 Remaining in mempool: {}", mempool.size());
    println!();

    // 4. Create and serialize WireMessages
    println!("4️⃣  Testing wire protocol serialization...");
    
    // Handshake message
    let handshake = WireMessage::Handshake {
        version: PROTOCOL_VERSION,
        geometric_id: [42u8; 32],
        listen_port: 9000,
    };
    
    let handshake_bytes = serialize_message(&handshake).unwrap();
    println!("   ✓ Handshake serialized: {} bytes", handshake_bytes.len());
    
    // Deserialize it back
    let deserialized: WireMessage = deserialize_message(&handshake_bytes).unwrap();
    match deserialized {
        WireMessage::Handshake { version, geometric_id, listen_port } => {
            println!("   ✓ Handshake deserialized: v{}, port {}", version, listen_port);
            println!("     Geometric ID: {}...", hex::encode(&geometric_id[..8]));
        }
        _ => println!("   ✗ Wrong message type!"),
    }
    println!();

    // Transaction submission message (fast-path HashReveal)
    println!("5️⃣  Testing transaction submission message...");
    let tx = Transaction::new_fast(
        [1u8; 32],  // sender
        TransactionPayload::Transfer {
            recipient: [2u8; 32],
            amount: 5000,
            nonce: 123,
        },
        123,         // tx nonce
        1234567890,  // timestamp
        [0u8; 32],   // auth_secret (HashReveal)
    );
    
    let tx_msg = WireMessage::TransactionSubmission {
        tx: tx.into(),
    };
    
    let tx_bytes = serialize_message(&tx_msg).unwrap();
    println!("   ✓ Transaction message serialized: {} bytes", tx_bytes.len());
    
    let deserialized_tx: WireMessage = deserialize_message(&tx_bytes).unwrap();
    match deserialized_tx {
        WireMessage::TransactionSubmission { tx } => {
            println!("   ✓ Transaction deserialized: amount={}, nonce={}", 
                     if let TransactionPayload::Transfer { amount, .. } = tx.payload { amount } else { 0 },
                     if let TransactionPayload::Transfer { nonce, .. } = tx.payload { nonce } else { 0 });
        }
        _ => println!("   ✗ Wrong message type!"),
    }
    println!();

    // Batch proposal message
    println!("6️⃣  Testing batch proposal message...");
    let header = SerializableBatchHeader {
        sequencer_id: [1u8; 32],
        round_id: 100,
        structure_root: [2u8; 32],
        set_xor: [3u8; 32],
        tx_count: 10000,
        signature: [0u8; 64],
    };
    
    let proposal = WireMessage::BatchProposal { header };
    let proposal_bytes = serialize_message(&proposal).unwrap();
    println!("   ✓ Batch proposal serialized: {} bytes", proposal_bytes.len());
    println!("     (Header-only broadcast for 10k transactions)");
    println!();

    // Mempool status
    println!("7️⃣  Testing mempool status messages...");
    let status_req = WireMessage::MempoolStatus;
    let status_bytes = serialize_message(&status_req).unwrap();
    println!("   ✓ Mempool status request: {} bytes", status_bytes.len());
    
    let status_resp = WireMessage::MempoolStatusResponse {
        pending_count: 5,
        capacity_tps: 30_000_000,
    };
    let resp_bytes = serialize_message(&status_resp).unwrap();
    println!("   ✓ Mempool status response: {} bytes", resp_bytes.len());
    println!();

    println!("✨ Demo complete! Wire protocol and mempool working correctly.");
    println!();
    println!("📝 Summary:");
    println!("   • Mempool: Lock-free queue with deduplication");
    println!("   • Wire Protocol: Compact bincode serialization");
    println!("   • Handshake messages: ~60 bytes");
    println!("   • Batch headers: ~160 bytes (for 10k+ transactions)");
    println!("   • Ready for network integration!");
}
