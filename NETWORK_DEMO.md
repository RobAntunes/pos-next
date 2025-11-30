# POS Network Demo - Auto-Discovery

This demo shows how to run multiple POS nodes that automatically discover each other using mDNS (on localhost/LAN) and communicate via QUIC.

## Architecture

### Layer 0 (Discovery) - mDNS
- Nodes broadcast their presence on the local network
- Uses libp2p's mDNS for automatic peer discovery
- Works on localhost and LAN without manual configuration

### Layer 2 (Transport) - QUIC
- High-speed UDP-based transport with 0-RTT
- Self-signed certificates for demo (production would use proper PKI)
- Multiplexed streams for control and data

### Handover Process
1. Node A broadcasts mDNS announcement
2. Node B discovers Node A via mDNS
3. Node B extracts IP address from multiaddr
4. Node B initiates QUIC connection to Node A's data port
5. Bidirectional streams established for transaction batching

## Running the Demo

### Terminal 1 - Node A
```bash
cargo run --release --bin node-net -- --port 9000 --node-id "alice"
```

Expected output:
```
╔═══════════════════════════════════════════════════════════════╗
║           POS Protocol - Networked Node                       ║
╠═══════════════════════════════════════════════════════════════╣
║  L0 (Discovery): mDNS-based peer discovery                    ║
║  L2 (Transport): QUIC high-speed data plane                   ║
╚═══════════════════════════════════════════════════════════════╝

🚀 Starting alice on QUIC port 9000

✅ L2 (QUIC) listening on 0.0.0.0:9000
✅ L0 (mDNS) discovery service started

📡 Scanning for peers on the local network...
```

### Terminal 2 - Node B
```bash
cargo run --release --bin node-net -- --port 9001 --node-id "bob"
```

### What Happens

Within a few seconds, you'll see:

**Node A (alice) output:**
```
📡 L0 listening on /ip4/192.168.1.10/tcp/52341
🔎 NEW PEER DISCOVERED via mDNS!
   Peer ID: 12D3KooW...
   Address: /ip4/192.168.1.10/tcp/52342
   -> Attempting QUIC connection to 192.168.1.10:9001
✅ SUCCESSFULLY CONNECTED TO PEER at 192.168.1.10:9001
📬 Received response: "ACK"
```

**Node B (bob) output:**
```
📡 L0 listening on /ip4/192.168.1.10/tcp/52342
🔎 NEW PEER DISCOVERED via mDNS!
   Peer ID: 12D3KooW...
   Address: /ip4/192.168.1.10/tcp/52341
⚡ L2 CONNECTION ESTABLISHED from 192.168.1.10:52343
```

## Running on Different Machines

To test on different machines on the same LAN:

**Machine 1:**
```bash
cargo run --release --bin node-net -- --port 9000 --node-id "node1"
```

**Machine 2:**
```bash
cargo run --release --bin node-net -- --port 9000 --node-id "node2"
```

Note: Both can use the same port since they're on different machines. mDNS will discover them automatically.

## Production Deployment

For production use, you would need:

1. **Proper Certificate Management**
   - Replace self-signed certs with CA-signed certificates
   - Implement certificate rotation

2. **Port Advertisement Protocol**
   - Currently uses hardcoded port inference (9000 -> 9001)
   - Should use libp2p Identify or Kademlia DHT to advertise QUIC port
   - Add to peer metadata: `{"quic_port": 9000}`

3. **WAN Discovery**
   - Add Kademlia DHT for global peer discovery
   - Implement relay nodes for NAT traversal
   - Use libp2p's AutoNAT and hole-punching

4. **Security**
   - Add peer authentication beyond mDNS
   - Implement stake verification before accepting connections
   - Rate limiting and DOS protection

5. **Monitoring**
   - Add Prometheus metrics export
   - Connection health checks
   - Bandwidth monitoring per peer

## Architecture Notes

This implements the "Hybrid L0/L2" design from the spec:

- **L0 (libp2p)**: Control plane only - discovery, identity, metadata
- **L2 (QUIC)**: Data plane - high-throughput batch streaming

This avoids the performance overhead of routing all data through libp2p while still benefiting from its mature discovery and identity layers.

## Troubleshooting

### "No peers discovered"
- Check firewall settings (mDNS uses UDP port 5353)
- Verify both nodes are on the same subnet
- On macOS, ensure "Network" has local network permissions

### "QUIC connection failed"
- Ports might be blocked by firewall
- Try explicitly allowing ports 9000-9001 in firewall
- Check if another process is using the port: `lsof -i :9000`

### "Certificate verification failed" 
- This shouldn't happen with the demo (uses insecure mode)
- If it does, the rustls configuration may need adjustment

## Next Steps

To integrate with the sequencer:

1. Modify `handle_quic_connections` to deserialize incoming transaction batches
2. Call `sequencer.process_batch_ref()` on received data
3. Implement batch broadcasting in the main loop
4. Add peer scoring based on latency and throughput
5. Implement the perigee optimization (Core/Bridge/Random slots)
