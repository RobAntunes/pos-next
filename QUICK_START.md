# Quick Start - POS Hash Ring Cluster Demo

## Problem Solved

**Before**: RocksDB lock prevented running multiple nodes
**After**: Each node uses port-specific database directory

## One-Command Demo

```bash
./run_demo.sh
```

Choose:
- **Option 1**: Ring visualization (no nodes needed)
- **Option 2**: Full cluster test (auto-starts 2 nodes)

## What Option 2 Does

1. **Cleans old data** - Removes previous RocksDB databases
2. **Starts Shard 0** - Port 9000 with unique DB: `./data/pos_ledger_db_9000`
3. **Starts Shard 1** - Port 9001 with unique DB: `./data/pos_ledger_db_9001`
4. **Runs Smart Client** - 20k transactions, routes to correct shard
5. **Runs Dumb Client** - 20k transactions, all to shard 0 (tests bridging)

## Manual Testing

### Start Two Nodes

```bash
# Terminal 1: Shard 0
cargo run --release --bin node-net -- --port 9000 --producer --tps 5000

# Terminal 2: Shard 1
cargo run --release --bin node-net -- --port 9001 --producer --tps 5000
```

**Note**: Each node uses its own database:
- Shard 0: `./data/pos_ledger_db_9000`
- Shard 1: `./data/pos_ledger_db_9001`

### Test Smart Routing

```bash
# Terminal 3: Smart client
cargo run --release --bin client -- \
    --shard-0 127.0.0.1:9000 \
    --shard-1 127.0.0.1:9001 \
    --count 100000
```

**Expected Output:**
```
✅ Done!
   Sent to Shard 0: 50123 (50.1%)
   Sent to Shard 1: 49877 (49.9%)

✨ SMART MODE: Transactions routed to correct shards
   Minimal bridging required
```

### Test Dumb Mode (Force Bridging)

```bash
cargo run --release --bin client -- \
    --shard-0 127.0.0.1:9000 \
    --shard-1 127.0.0.1:9001 \
    --count 100000 \
    --dumb
```

**Expected Output:**
```
✅ Done!
   Sent to Shard 0: 100000 (100.0%)
   Sent to Shard 1: 0 (0.0%)

⚠️  DUMB MODE: All transactions sent to Shard 0
   Shard 0 will bridge ~50% to Shard 1
```

## Clean Up

```bash
# Stop all nodes
pkill -f node-net

# Remove databases
rm -rf ./data/pos_ledger_db_*
```

## Automated Test Script

For convenience, use the automated test:

```bash
./test_cluster.sh
```

This script:
- ✅ Cleans old data automatically
- ✅ Starts both nodes in background
- ✅ Runs both smart and dumb mode tests
- ✅ Provides PIDs for easy cleanup
- ✅ Shows logs location

## Troubleshooting

### RocksDB Lock Error

**Error**: `IO error: While lock file: ./data/pos_ledger_db/LOCK`

**Solution**: Each node now uses port-specific DB, this is fixed!

### Port Already in Use

```bash
# Find and kill process on port 9000
lsof -ti:9000 | xargs kill -9

# Or kill all node-net processes
pkill -f node-net
```

### Check Node Logs

```bash
# If using test_cluster.sh
tail -f shard0.log
tail -f shard1.log

# If running manually, check terminal output
```

## Key Points

1. **No More Lock Conflicts**: Each node has unique DB path
2. **Smart Client**: Knows ring topology, routes directly
3. **Dumb Client**: Demonstrates bridging fallback
4. **Easy Cleanup**: Just kill processes and delete data/

## Next Steps

See `CLUSTER_DEMO.md` for full documentation and TUI integration guide.
