# Shard-Aware Routing Implementation Guide

Due to the size of node_net.rs, here are the key changes needed:

## 1. Add TUI flag to Args
```rust
#[arg(long, default_value_t = false)]
tui: bool,
```

## 2. Determine Shard ID
```rust
// After args parsing
let my_shard_id = if args.port % 2 == 0 { 0 } else { 1 };
```

## 3. Initialize TUI State
```rust
let ui_state = Arc::new(Mutex::new(pos::tui::AppState::new(&node_name, my_shard_id)));
```

## 4. Spawn TUI Thread (if enabled)
```rust
if args.tui {
    let ui_clone = ui_state.clone();
    tokio::spawn(async move {
        if let Err(e) = pos::tui::run_tui(ui_clone) {
            eprintln!("TUI error: {}", e);
        }
    });
}
```

## 5. Update Transaction Handling
In the `handle_quic_connections` function, replace the TransactionSubmission handler:

```rust
WireMessage::TransactionSubmission { tx } => {
    let tx: Transaction = tx.into();

    // Calculate ring position
    let tx_hash = tx.hash();
    let tx_pos = calculate_ring_position(&tx_hash);
    let midpoint = u64::MAX / 2;

    // Check ownership
    let is_mine = if my_shard_id == 0 {
        tx_pos < midpoint
    } else {
        tx_pos >= midpoint
    };

    if is_mine {
        // MINE: Process locally
        if mempool.submit(tx) {
            total_received.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut state) = ui_state.try_lock() {
                state.accepted_count += 1;
                state.log(format!("✅ Accepted tx at ring pos {}", tx_pos));
            }
        }
    } else {
        // NOT MINE: Forward to peer
        // (For demo, we'll just log it - full P2P forwarding needs peer discovery)
        if let Ok(mut state) = ui_state.try_lock() {
            state.forwarded_count += 1;
            state.log(format!("🔄 Would forward tx at ring pos {}", tx_pos));
        }
    }

    // Send ACK
    let ack = WireMessage::Ack { msg_id: [0u8; 32] };
    if let Ok(ack_bytes) = serialize_message(&ack) {
        let _ = send.write_all(&ack_bytes).await;
    }
    let _ = send.finish().await;
}
```

## 6. Add ForwardedTx Handler
```rust
WireMessage::ForwardedTx { tx } => {
    let tx: Transaction = tx.into();
    if mempool.submit(tx) {
        total_received.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut state) = ui_state.try_lock() {
            state.received_count += 1;
            state.accepted_count += 1;
            state.log("📥 Received bridged transaction".to_string());
        }
    }

    let ack = WireMessage::Ack { msg_id: [0u8; 32] };
    if let Ok(ack_bytes) = serialize_message(&ack) {
        let _ = send.write_all(&ack_bytes).await;
    }
    let _ = send.finish().await;
}
```

## 7. Update Stats Loop
```rust
_ = tokio::time::sleep(Duration::from_secs(1)) => {
    let received = total_received.load(Ordering::Relaxed);
    let processed = total_processed.load(Ordering::Relaxed);
    let pending = mempool.size();

    // Update TUI state
    if let Ok(mut state) = ui_state.try_lock() {
        state.mempool_size = pending;
        state.tps = processed.saturating_sub(state.accepted_count.min(processed));
        state.peer_count = 1; // Update when peer discovery is implemented
    }

    if !args.tui {
        info!("📊 Stats | Mempool: {} | Received: {} | Processed: {}",
              pending, received, processed);
    }
}
```

This implementation shows the ring-based routing in action without requiring full P2P mesh.
For a complete demo, you would connect two nodes and implement actual forwarding.
