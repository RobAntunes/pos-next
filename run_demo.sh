#!/bin/bash
# Gemini Cluster Demo - Ring-Based Shard Routing
set -e

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║          POS Cluster Demo - Hash Ring Routing                ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Build everything
echo "🔨 Building binaries..."
cargo build --release --bin client --bin node-net 2>&1 | grep -E "(Finished|Compiling pos)" || true

echo ""
echo "✅ Build complete!"
echo ""

# Run the demo
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    Demo Scenarios                             ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

echo "Choose a demo mode:"
echo "  1) Ring Demo - Show virtual node distribution"
echo "  2) Cluster Test - Full two-shard demo with smart client"
echo ""
read -p "Enter choice [1-2]: " choice

case $choice in
    1)
        echo ""
        echo "🎲 Running Ring Demo..."
        cargo run --release --example ring_demo
        ;;
    2)
        echo ""
        echo "🚀 Running Full Cluster Test..."
        echo "   (This will start 2 nodes and run smart client tests)"
        echo ""
        ./test_cluster.sh
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "✨ Demo complete!"
