# Fixed: RocksDB Lock Issue

## Problem

When trying to run two nodes on different ports:

```bash
# Terminal 1
cargo run --release --bin node-net -- --port 9000

# Terminal 2 - FAILED!
cargo run --release --bin node-net -- --port 9001
# Error: IO error: While lock file: ./data/pos_ledger_db/LOCK
```

**Root Cause**: Both nodes were trying to use the same RocksDB database path: `./data/pos_ledger_db`

## Solution

**Changed** `node_net.rs` line 115:

```rust
// Before (hardcoded path - causes lock conflict)
let ledger = Arc::new(Ledger::new("./data/pos_ledger_db"));

// After (port-specific path - no conflict!)
let db_path = format!("./data/pos_ledger_db_{}", args.port);
let ledger = Arc::new(Ledger::new(&db_path));
```

## Result

Now each node uses its own database:
- **Port 9000** → `./data/pos_ledger_db_9000`
- **Port 9001** → `./data/pos_ledger_db_9001`
- **Port 9002** → `./data/pos_ledger_db_9002`
- etc.

## Testing

### Quick Test
```bash
./run_demo.sh
# Choose option 2
```

### Manual Test
```bash
# Clean old data
rm -rf ./data/pos_ledger_db_*

# Terminal 1
cargo run --release --bin node-net -- --port 9000

# Terminal 2 (now works!)
cargo run --release --bin node-net -- --port 9001
```

## Automated Test Script

Created `test_cluster.sh` that:
1. Cleans old databases
2. Starts both nodes in background
3. Runs smart and dumb client tests
4. Shows results

```bash
./test_cluster.sh
```

## Cleanup

```bash
# Stop all nodes
pkill -f node-net

# Remove all databases
rm -rf ./data/pos_ledger_db_*
```

## Status

✅ **FIXED** - Multiple nodes can now run simultaneously on different ports without RocksDB lock conflicts!

## Demo Ready

All cluster demos now work:
- ✅ Ring visualization demo
- ✅ Two-node cluster with smart client
- ✅ Automated test script
- ✅ No manual cleanup needed
