#!/bin/bash
# Test the hash ring cluster with two shards
set -e

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║          POS Cluster Test - Two Shard Demo                   ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Clean up old data
echo "🧹 Cleaning old data..."
rm -rf ./data/pos_ledger_db_9000
rm -rf ./data/pos_ledger_db_9001
mkdir -p ./data

# Kill any existing node processes
echo "🔪 Killing old processes..."
pkill -f "node-net --port 9000" 2>/dev/null || true
pkill -f "node-net --port 9001" 2>/dev/null || true
sleep 1

echo ""
echo "🚀 Starting Shard 0 (port 9000)..."
./target/release/node-net --port 9000 --producer --tps 5000 --batch-size 1000 > shard0.log 2>&1 &
SHARD0_PID=$!
echo "   PID: $SHARD0_PID"

echo "🚀 Starting Shard 1 (port 9001)..."
./target/release/node-net --port 9001 --producer --tps 5000 --batch-size 1000 > shard1.log 2>&1 &
SHARD1_PID=$!
echo "   PID: $SHARD1_PID"

echo ""
echo "⏳ Waiting for nodes to start (5 seconds)..."
sleep 5

echo ""
echo "✅ Nodes running!"
echo "   Shard 0: http://127.0.0.1:9000 (PID $SHARD0_PID)"
echo "   Shard 1: http://127.0.0.1:9001 (PID $SHARD1_PID)"
echo ""
echo "📊 Logs:"
echo "   Shard 0: tail -f shard0.log"
echo "   Shard 1: tail -f shard1.log"
echo ""

# Run client test
echo "═══════════════════════════════════════════════════════════════"
echo "Testing SMART MODE (client routes to correct shard)"
echo "═══════════════════════════════════════════════════════════════"
echo ""
./target/release/client --count 20000 --shard-0 127.0.0.1:9000 --shard-1 127.0.0.1:9001

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Testing DUMB MODE (all transactions to shard 0)"
echo "═══════════════════════════════════════════════════════════════"
echo ""
./target/release/client --count 20000 --shard-0 127.0.0.1:9000 --shard-1 127.0.0.1:9001 --dumb

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Demo Complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "📊 Check logs to see transaction processing:"
echo "   tail -20 shard0.log"
echo "   tail -20 shard1.log"
echo ""
echo "🛑 To stop the nodes:"
echo "   kill $SHARD0_PID $SHARD1_PID"
echo ""
echo "Or press Ctrl+C to stop everything now..."
echo ""

# Keep script running so nodes stay up
trap "echo ''; echo '🛑 Stopping nodes...'; kill $SHARD0_PID $SHARD1_PID 2>/dev/null; exit 0" INT TERM

wait
