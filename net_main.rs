use std::net::SocketAddr;
use pos::network::{GeometricTurbine, NetworkManager};
use pos::types::{BatchHeader, Batch};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    println!("🌐 Starting Geometric Turbine Network Test...");

    // Initialize Network Manager
    let bind_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let net = NetworkManager::new(bind_addr);

    // 1. Layer 0 Discovery
    println!("🔍 [L0] Discovering sector peers...");
    let peers = net.discover_sector(42).await;
    println!("   Found {} candidates: {:?}", peers.len(), peers);

    // 2. Layer 1 Topology
    println!("⚡ [L1] Optimizing topology (Perigee)...");
    net.optimize_topology().await;

    // 3. Layer 2 Transport
    println!("🚀 [L2] Broadcasting mock header...");
    let dummy_header = BatchHeader {
        sequencer_id: [0u8; 32],
        round_id: 1,
        structure_root: blake3::hash(b"root"),
        set_xor: blake3::hash(b"xor"),
        tx_count: 1000,
        signature: [0u8; 64],
    };
    net.broadcast_header(dummy_header).await;

    println!("✅ Network stack initialized successfully.");
}
