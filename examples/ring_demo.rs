//! Hash Ring Demo
//!
//! Demonstrates the consistent hash ring routing with virtual nodes

use pos::{RingRoutingTable, VirtualNodeConfig, calculate_ring_position};

fn main() {
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║              Consistent Hash Ring Demo                       ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();

    // Create 3 nodes
    let node1 = blake3::hash(b"alice").into();
    let node2 = blake3::hash(b"bob").into();
    let node3 = blake3::hash(b"charlie").into();

    // Create routing table with 10 virtual nodes per physical node
    let mut table = RingRoutingTable::with_config(VirtualNodeConfig { replicas: 10 });

    println!("📋 Adding 3 nodes to the ring (10 virtual nodes each):");
    println!("   • Alice");
    println!("   • Bob");
    println!("   • Charlie");
    println!();

    table.add_node(node1);
    table.add_node(node2);
    table.add_node(node3);

    println!("✅ Ring initialized:");
    println!("   Physical nodes: {}", table.node_count());
    println!("   Virtual nodes:  {}", table.size());
    println!();

    // Simulate 1000 transactions and count distribution
    println!("🎲 Simulating 1000 transactions...");
    println!();

    let mut node1_count = 0;
    let mut node2_count = 0;
    let mut node3_count = 0;

    for i in 0u64..1000 {
        let tx_hash = blake3::hash(&i.to_le_bytes());
        let tx_pos = calculate_ring_position(&tx_hash);

        if let Some(owner) = table.route(tx_pos) {
            if owner == node1 {
                node1_count += 1;
            } else if owner == node2 {
                node2_count += 1;
            } else if owner == node3 {
                node3_count += 1;
            }
        }
    }

    println!("📊 Load Distribution:");
    println!("   Alice:   {} tx ({:.1}%)", node1_count, node1_count as f64 / 10.0);
    println!("   Bob:     {} tx ({:.1}%)", node2_count, node2_count as f64 / 10.0);
    println!("   Charlie: {} tx ({:.1}%)", node3_count, node3_count as f64 / 10.0);
    println!();

    // Calculate standard deviation to show uniformity
    let mean = 1000.0 / 3.0;
    let variance = ((node1_count as f64 - mean).powi(2) +
                   (node2_count as f64 - mean).powi(2) +
                   (node3_count as f64 - mean).powi(2)) / 3.0;
    let std_dev = variance.sqrt();

    println!("📈 Statistics:");
    println!("   Expected per node: {:.1} tx", mean);
    println!("   Standard deviation: {:.2} tx", std_dev);
    println!("   Coefficient of variation: {:.2}%", (std_dev / mean) * 100.0);
    println!();

    // Test node removal
    println!("🔧 Removing Bob from the ring...");
    table.remove_node(&node2);

    println!("   Physical nodes: {}", table.node_count());
    println!("   Virtual nodes:  {}", table.size());
    println!();

    // Show how transactions redistribute
    let mut node1_after = 0;
    let mut node3_after = 0;

    for i in 0u64..1000 {
        let tx_hash = blake3::hash(&i.to_le_bytes());
        let tx_pos = calculate_ring_position(&tx_hash);

        if let Some(owner) = table.route(tx_pos) {
            if owner == node1 {
                node1_after += 1;
            } else if owner == node3 {
                node3_after += 1;
            }
        }
    }

    println!("📊 Load after Bob leaves:");
    println!("   Alice:   {} tx ({:.1}%)", node1_after, node1_after as f64 / 10.0);
    println!("   Charlie: {} tx ({:.1}%)", node3_after, node3_after as f64 / 10.0);
    println!();

    println!("✨ Key Insights:");
    println!("   • Virtual nodes ensure even distribution");
    println!("   • Adding/removing nodes is O(1) per vnode");
    println!("   • Load automatically rebalances when topology changes");
    println!("   • No manual partitioning or resharding needed");
    println!();
}
