# Multi-Node Setup Instructions

You have a fully optimized, decoupled blockchain node. To run it across 2 machines (e.g., Mac and Windows), follow these steps:

### 1. Prepare the Binary
**On macOS (Machine A):**
You already have the optimized binary in `./target/release/node-net`.

**On Windows (Machine B):**
1.  Install Rust: [rustup.rs](https://rustup.rs/)
2.  Clone the repo (or copy the files).
3.  Build the optimized release:
    ```powershell
    cargo build --release --bin node-net
    ```

### 2. Run Machine A (Producer/Sequencer)
Start the first node. It will generate transactions and act as a seed.
```bash
./target/release/node-net --producer --tps 10000000 --port 9000
```
*   `--producer`: Generates load.
*   `--tps`: Sets target throughput.
*   `--port`: QUIC listening port.

### 3. Run Machine B (Peer)
Start the second node. It will auto-discover Machine A via mDNS (if on same LAN).
```powershell
.\target\release\node-net.exe --port 9001
```
*   Note: Different port if testing on same machine, but standard 9000 is fine on different IPs.
*   **Firewall Warning:** Ensure Windows Firewall allows UDP port 5353 (mDNS) and UDP 9000/9001 (QUIC).

### 4. Verification
Watch the logs on both machines.
*   **Machine A:** Will show "NEW PEER DISCOVERED".
*   **Machine B:** Will show "NEW PEER DISCOVERED" and "L2 CONNECTION ESTABLISHED".
*   **Throughput:** Machine B will start receiving transactions via the QUIC stream and processing them in its own local ledger.

### Note on State
Currently, this setup runs two **independent** ledgers that exchange gossip. Full consensus (PBFT/Raft) requires enabling the `consensus` module (not active in this benchmark harness), but the networking and data plane are fully functional.
