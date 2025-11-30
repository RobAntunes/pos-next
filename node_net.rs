//! Networked POS Node with Auto-Discovery
//!
//! Features:
//! - L0 (Discovery): mDNS-based peer discovery on LAN
//! - L2 (Transport): QUIC-based high-speed data transport
//! - Automatic handover from discovery to data plane

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use clap::Parser;
use futures::stream::StreamExt;
use libp2p::{
    core::upgrade,
    mdns,
    noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, SwarmBuilder, Transport,
};
use quinn::{ClientConfig, Endpoint, ServerConfig};
use std::convert::TryInto;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

use pos::{
    Sequencer, SequencerConfig, Transaction, Mempool, MempoolConfig, TransactionPayload, Ledger,
    WireMessage, SerializableBatchHeader, SerializableTransaction, PROTOCOL_VERSION,
    messages::{serialize_message, deserialize_message},
};

#[derive(Parser, Debug)]
#[command(author, version, about = "Networked POS Node with Auto-Discovery")]
struct Args {
    /// QUIC port for high-speed data transport (L2)
    #[arg(short, long, default_value_t = 9000)]
    port: u16,

    /// TCP port for mDNS discovery (L0) - 0 means OS picks random port
    #[arg(short, long, default_value_t = 0)]
    discovery_port: u16,

    /// Node ID (for debugging)
    #[arg(short, long)]
    node_id: Option<String>,
    
    /// Enable producer mode (generate test transactions)
    #[arg(long, default_value_t = false)]
    producer: bool,
    
    /// Transactions per second to generate (producer mode)
    #[arg(long, default_value_t = 1000)]
    tps: u64,
    
    /// Batch size for sequencer
    #[arg(long, default_value_t = 1000)]
    batch_size: usize,
    
    /// Generate valid geometric positions (100% acceptance rate)
    /// Without this flag, random positions are generated (~20% acceptance)
    #[arg(long, default_value_t = false)]
    smart_gen: bool,
    
    /// Maximum mempool size (default: 10M transactions)
    #[arg(long, default_value_t = 10_000_000)]
    mempool_size: usize,
}

#[derive(Debug, Clone)]
struct DiscoveredPeer {
    peer_id: PeerId,
    multiaddr: Multiaddr,
    quic_port: u16,
}

// Custom behaviour: just mDNS for local discovery
#[derive(NetworkBehaviour)]
struct DiscoveryBehaviour {
    mdns: mdns::tokio::Behaviour,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let args = Args::parse();
    let node_name = args
        .node_id
        .clone()
        .unwrap_or_else(|| format!("node-{}", args.port));

    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║           POS Protocol - Networked Node                       ║");
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!("║  L0 (Discovery): mDNS-based peer discovery                    ║");
    println!("║  L2 (Transport): QUIC high-speed data plane                   ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("🚀 Starting {} on QUIC port {}", node_name, args.port);
    println!();

    // Initialize ledger with persistence (unique per port to avoid RocksDB lock)
    let db_path = format!("./data/pos_ledger_db_{}", args.port);
    let ledger = Arc::new(Ledger::new(&db_path));
    
    // Initialize sequencer
    let config = SequencerConfig {
        sequencer_id: generate_node_id(&node_name),
        batch_size: args.batch_size,
        ..Default::default()
    };
    let sequencer = Arc::new(Sequencer::new(config));
    
    // Initialize mempool with configured size
    let mempool_config = MempoolConfig {
        max_size: args.mempool_size,
        ..Default::default()
    };
    let mempool = Arc::new(Mempool::with_config(mempool_config));
    info!("📦 Mempool initialized (max size: {})", args.mempool_size);

    // Channel for discovered peers
    let (peer_tx, mut peer_rx) = mpsc::channel::<DiscoveredPeer>(100);

    // Start QUIC transport (L2)
    let quic_endpoint = start_quic_server(args.port).await?;
    let quic_endpoint_clone = quic_endpoint.clone();
    
    let total_received = Arc::new(AtomicU64::new(0));
    let total_received_clone = total_received.clone();
    let sequencer_clone = sequencer.clone();
    let mempool_clone = mempool.clone();
    let node_id = generate_node_id(&node_name);

    // Spawn QUIC listener task
    tokio::spawn(async move {
        handle_quic_connections(
            quic_endpoint_clone,
            sequencer_clone,
            mempool_clone,
            total_received_clone,
            node_id,
            args.port,
        ).await;
    });

    info!("✅ L2 (QUIC) listening on 0.0.0.0:{}", args.port);

    // Start mDNS discovery (L0)
    let quic_port = args.port;
    tokio::spawn(async move {
        if let Err(e) = start_mdns_discovery(args.discovery_port, quic_port, peer_tx).await {
            warn!("mDNS discovery error: {}", e);
        }
    });

    info!("✅ L0 (mDNS) discovery service started");
    
    // Start The Heartbeat loops
    let batch_size = args.batch_size;
    
    // Producer Loop: Generate test transactions and submit to mempool
    if args.producer {
        let mempool_producer = mempool.clone();
        let tps = args.tps;
        let node_id_producer = node_id;
        let smart_gen = args.smart_gen;
        
        tokio::spawn(async move {
            producer_loop(mempool_producer, tps, node_id_producer, smart_gen).await;
        });
        
        info!("✅ Producer loop started (target: {} TPS, smart_gen: {})", args.tps, args.smart_gen);
    }
    
    // Consumer Loop: Pull from mempool, create batches, broadcast
    let sequencer_consumer = sequencer.clone();
    let mempool_consumer = mempool.clone();
    let ledger_consumer = ledger.clone();
    let total_processed = Arc::new(AtomicU64::new(0));
    let total_processed_clone = total_processed.clone();
    
    tokio::spawn(async move {
        consumer_loop(
            sequencer_consumer,
            mempool_consumer,
            ledger_consumer,
            total_processed_clone,
            batch_size,
        ).await;
    });
    
    info!("✅ Consumer loop started (batch size: {})", batch_size);
    
    println!();
    println!("📡 Scanning for peers on the local network...");
    println!();

    // Main event loop
    loop {
        tokio::select! {
            Some(peer_info) = peer_rx.recv() => {
                info!("🔎 NEW PEER DISCOVERED via mDNS!");
                info!("   Peer ID: {}", peer_info.peer_id);
                info!("   Address: {}", peer_info.multiaddr);
                
                // Extract IP from multiaddr and attempt to connect
                if let Some(ip) = extract_ip_from_multiaddr(&peer_info.multiaddr) {
                    // Try to infer peer's QUIC port (for demo: assume sequential ports)
                    let target_port = if args.port == 9000 { 9001 } else { 9000 };
                    info!("   -> Attempting QUIC connection to {}:{}", ip, target_port);
                    
                    let endpoint = quic_endpoint.clone();
                    tokio::spawn(async move {
                        if let Err(e) = connect_to_peer(endpoint, ip, target_port).await {
                            warn!("Failed to connect to peer: {}", e);
                        }
                    });
                } else {
                    warn!("Could not extract IP from multiaddr");
                }
            }
            
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                let received = total_received.load(Ordering::Relaxed);
                let processed = total_processed.load(Ordering::Relaxed);
                let pending = mempool.size();
                
                info!("📊 Heartbeat | Mempool: {} pending | Received: {} | Processed: {}", 
                      pending, received, processed);
            }
        }
    }
}

/// Start mDNS discovery service
async fn start_mdns_discovery(
    listen_port: u16,
    quic_port: u16,
    peer_tx: mpsc::Sender<DiscoveredPeer>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Generate libp2p identity
    let id_keys = libp2p::identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(id_keys.public());

    // Setup mDNS behaviour
    let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;
    let behaviour = DiscoveryBehaviour { mdns };

    // Build swarm with the new 0.53 API
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|_key| Ok(behaviour))?  
        .build();

    // Listen on all interfaces
    let listen_addr = if listen_port == 0 {
        "/ip4/0.0.0.0/tcp/0".parse()?
    } else {
        format!("/ip4/0.0.0.0/tcp/{}", listen_port).parse()?
    };
    swarm.listen_on(listen_addr)?;

    info!("📡 L0 Discovery service started (advertising QUIC port {})", quic_port);

    // Event loop
    loop {
        if let Some(event) = swarm.next().await {match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("📡 L0 listening on {}", address);
            }
            SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Mdns(mdns::Event::Discovered(
                list,
            ))) => {
                for (peer_id, multiaddr) in list {
                    let _ = peer_tx
                        .send(DiscoveredPeer {
                            peer_id,
                            multiaddr: multiaddr.clone(),
                            quic_port,
                        })
                        .await;
                }
            }
            SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                for (peer_id, _) in list {
                    info!("📉 Peer expired: {}", peer_id);
                }
            }
            _ => {}
        }
        }
    }
}

/// Start QUIC server
async fn start_quic_server(port: u16) -> Result<Endpoint, Box<dyn std::error::Error>> {
    let (cert, key) = generate_self_signed_cert()?;
    
    let server_config = ServerConfig::with_single_cert(vec![cert], key)?;
    
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    let mut endpoint = Endpoint::server(server_config, addr)?;
    
    // Configure client side to accept ANY certificate (for demo with self-signed certs)
    let mut crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    
    // Disable certificate verification for demo (DO NOT USE IN PRODUCTION)
    crypto.dangerous().set_certificate_verifier(Arc::new(NoCertificateVerification));
    
    let mut client_config = ClientConfig::new(Arc::new(crypto));
    client_config.transport_config(Arc::new(create_transport_config()));
    endpoint.set_default_client_config(client_config);
    
    Ok(endpoint)
}

/// Handle incoming QUIC connections
async fn handle_quic_connections(
    endpoint: Endpoint,
    _sequencer: Arc<Sequencer>,
    mempool: Arc<Mempool>,
    total_received: Arc<AtomicU64>,
    node_id: [u8; 32],
    listen_port: u16,
) {
    while let Some(connecting) = endpoint.accept().await {
        let mempool = mempool.clone();
        let total_received = total_received.clone();
        
        tokio::spawn(async move {
            match connecting.await {
                Ok(connection) => {
                    info!("⚡ L2 CONNECTION ESTABLISHED from {}", connection.remote_address());
                    
                    // Accept bidirectional streams
                    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                        let mempool = mempool.clone();
                        let total_received = total_received.clone();
                        
                        tokio::spawn(async move {
                            // Read wire message
                            match recv.read_to_end(10 * 1024 * 1024).await {
                                Ok(data) => {
                                    if let Ok(msg) = deserialize_message(&data) {
                                        match msg {
                                            WireMessage::Handshake { version, geometric_id, listen_port: peer_port } => {
                                                info!("🤝 Handshake from {} (port {})", 
                                                      hex::encode(&geometric_id[..8]), peer_port);
                                                
                                                // Send handshake response
                                                let response = WireMessage::Handshake {
                                                    version: PROTOCOL_VERSION,
                                                    geometric_id: node_id,
                                                    listen_port,
                                                };
                                                
                                                if let Ok(response_bytes) = serialize_message(&response) {
                                                    let _ = send.write_all(&response_bytes).await;
                                                }
                                                let _ = send.finish().await;
                                            }
                                            WireMessage::TransactionSubmission { tx } => {
                                                let tx: Transaction = tx.into();
                                                if mempool.submit(tx) {
                                                    total_received.fetch_add(1, Ordering::Relaxed);
                                                    info!("✅ Transaction added to mempool (size: {})", mempool.size());
                                                }
                                                
                                                // Send ACK
                                                let ack = WireMessage::Ack { msg_id: [0u8; 32] };
                                                if let Ok(ack_bytes) = serialize_message(&ack) {
                                                    let _ = send.write_all(&ack_bytes).await;
                                                }
                                                let _ = send.finish().await;
                                            }
                                            WireMessage::MempoolStatus => {
                                                let stats = mempool.stats();
                                                let response = WireMessage::MempoolStatusResponse {
                                                    pending_count: stats.pending as u64,
                                                    capacity_tps: 1_000_000, // Mock value
                                                };
                                                
                                                if let Ok(response_bytes) = serialize_message(&response) {
                                                    let _ = send.write_all(&response_bytes).await;
                                                }
                                                let _ = send.finish().await;
                                            }
                                            _ => {
                                                warn!("Received unhandled message type");
                                            }
                                        }
                                    } else {
                                        warn!("Failed to deserialize message");
                                    }
                                }
                                Err(e) => {
                                    warn!("Error reading stream: {}", e);
                                }
                            }
                        });
                    }
                }
                Err(e) => {
                    warn!("Connection failed: {}", e);
                }
            }
        });
    }
}

/// Connect to a discovered peer
async fn connect_to_peer(
    endpoint: Endpoint,
    ip: IpAddr,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::new(ip, port);
    
    let connection = endpoint
        .connect(addr, "localhost")?
        .await?;
    
    info!("✅ SUCCESSFULLY CONNECTED TO PEER at {}", addr);
    
    // Open a stream and send handshake
    let (mut send, mut recv) = connection.open_bi().await?;
    let handshake = WireMessage::Handshake {
        version: PROTOCOL_VERSION,
        geometric_id: [0u8; 32], // Would be actual node ID
        listen_port: port,
    };
    
    let msg_bytes = serialize_message(&handshake)?;
    send.write_all(&msg_bytes).await?;
    send.finish().await?;
    
    // Wait for response
    let response_bytes = recv.read_to_end(1024).await?;
    if let Ok(response) = deserialize_message(&response_bytes) {
        match response {
            WireMessage::Handshake { version, geometric_id, listen_port: peer_port } => {
                info!("📬 Received handshake response: v{}, peer={}, port={}",
                      version, hex::encode(&geometric_id[..8]), peer_port);
            }
            _ => {
                info!("📬 Received unexpected response type");
            }
        }
    }
    
    Ok(())
}

/// Generate self-signed certificate for QUIC
fn generate_self_signed_cert() -> Result<(rustls::Certificate, rustls::PrivateKey), Box<dyn std::error::Error>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.serialize_der()?;
    let key_der = cert.serialize_private_key_der();
    Ok((rustls::Certificate(cert_der), rustls::PrivateKey(key_der)))
}

/// Create optimized QUIC transport config
fn create_transport_config() -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    config.max_concurrent_bidi_streams(100u32.into());
    config.max_concurrent_uni_streams(100u32.into());
    config
}


/// Extract IP address from libp2p multiaddr
fn extract_ip_from_multiaddr(multiaddr: &Multiaddr) -> Option<IpAddr> {
    for proto in multiaddr.iter() {
        match proto {
            libp2p::multiaddr::Protocol::Ip4(ip) => return Some(IpAddr::V4(ip)),
            libp2p::multiaddr::Protocol::Ip6(ip) => return Some(IpAddr::V6(ip)),
            _ => {}
        }
    }
    None
}

/// Generate deterministic node ID from name
fn generate_node_id(name: &str) -> [u8; 32] {
    let hash = blake3::hash(name.as_bytes());
    *hash.as_bytes()
}

/// Certificate verifier that accepts all certificates (for demo with self-signed certs)
/// DO NOT USE IN PRODUCTION - this skips all TLS security
struct NoCertificateVerification;

impl rustls::client::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        // WARNING: Accepts any certificate without verification
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

// ============================================================================
// THE HEARTBEAT: Producer & Consumer Loops
// ============================================================================

/// Producer Loop: Generate test transactions and submit to mempool
/// 
/// This simulates users submitting transactions to the network.
/// In production, transactions would come from RPC endpoints or P2P gossip.
///
/// SMART GENERATION MODE:
/// When `smart_gen` is true, we generate transactions that will hash to
/// positions within MAX_DISTANCE of the node position. This achieves 100%
/// acceptance rate vs ~20% with random generation.
async fn producer_loop(
    mempool: Arc<Mempool>,
    target_tps: u64,
    sender_id: [u8; 32],
    smart_gen: bool,
) {
    use std::time::Instant;
    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;
    use pos::MAX_DISTANCE;
    
    let mut nonce: u64 = 0;
    let mut last_report = Instant::now();
    let mut produced_since_report: u64 = 0;
    
    // Use StdRng which is Send-safe
    let mut rng = StdRng::from_entropy();
    
    // Calculate burst size and interval for efficient batching
    // Produce in larger bursts with longer sleeps (more cache-friendly)
    let burst_size = (target_tps / 10).max(100).min(10_000) as usize; // 100ms worth at a time
    let interval_ms = (burst_size as u64 * 1000) / target_tps.max(1);
    
    // For smart generation: compute the node's ring position
    // This is the same calculation the sequencer uses
    let node_hash = blake3::hash(b"default-sequencer-node");
    let node_position = pos::calculate_ring_position(&node_hash);

    info!("🏭 Producer starting: {} TPS target (bursts of {}, {}ms interval, smart_gen: {})",
          target_tps, burst_size, interval_ms, smart_gen);

    if smart_gen {
        info!("🎯 Smart generation enabled - targeting node ring position {}", node_position);
    }
    
    loop {
        let burst_start = Instant::now();
        
        // Generate transactions in a burst
        // Use spawn_blocking to avoid blocking the async runtime
        let mempool_clone = mempool.clone();
        let mut rng_clone = StdRng::from_rng(&mut rng).unwrap();
        let current_nonce = nonce;
        
        let submitted = tokio::task::spawn_blocking(move || {
            let mut submitted = 0u64;
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            
            // MAX_RANGE for ring filtering
            let max_range = pos::MAX_DISTANCE;

            let mut i = 0usize;
            let mut attempts = 0u64;
            while i < burst_size {
                let mut recipient = [0u8; 32];
                rng_clone.fill(&mut recipient);

                let tx = Transaction {
                    sender: sender_id,
                    payload: TransactionPayload::Transfer {
                        recipient,
                        amount: rng_clone.gen_range(1..10000),
                        nonce: current_nonce + i as u64,
                    },
                    signature: [0u8; 64],
                    timestamp: timestamp + i as u64,
                };

                attempts += 1;

                if smart_gen {
                    // SMART GENERATION: Pre-filter like a real client would
                    // Only submit transactions that pass the ring range check
                    let tx_hash = tx.hash();
                    let tx_position = pos::calculate_ring_position(&tx_hash);

                    // Calculate clockwise distance on the ring
                    let distance = if tx_position >= node_position {
                        tx_position - node_position
                    } else {
                        (u64::MAX - node_position) + tx_position + 1
                    };

                    if distance <= max_range {
                        // Valid! Submit it
                        if mempool_clone.submit(tx) {
                            submitted += 1;
                            i += 1;
                        }
                    }
                    // else: rejected by ring filter, try again with new random data
                } else {
                    // RANDOM GENERATION: ~25% acceptance rate at sequencer
                    if mempool_clone.submit(tx) {
                        submitted += 1;
                    }
                    i += 1;
                }
            }
            submitted
        }).await.unwrap_or(0);
        
        nonce += burst_size as u64;
        produced_since_report += submitted;
        
        // Report every 5 seconds
        if last_report.elapsed() >= Duration::from_secs(5) {
            let actual_tps = produced_since_report as f64 / last_report.elapsed().as_secs_f64();
            info!("🏭 Producer: {} tx/s (target: {}), mempool: {} pending", 
                  actual_tps as u64, target_tps, mempool.size());
            produced_since_report = 0;
            last_report = Instant::now();
        }
        
        // Sleep to maintain target TPS, accounting for processing time
        let elapsed = burst_start.elapsed().as_millis() as u64;
        if elapsed < interval_ms {
            tokio::time::sleep(Duration::from_millis(interval_ms - elapsed)).await;
        } else {
            // We're behind - just yield to let other tasks run
            tokio::task::yield_now().await;
        }
    }
}

/// Consumer Loop: Pull from mempool, process through sequencer, apply to ledger
/// 
/// This is the core sequencing loop - the "heartbeat" of the blockchain.
/// It pulls transactions from the mempool, creates batches, and applies state changes.
///
/// KEY OPTIMIZATIONS:
/// 1. Batch Buffering: Wait for minimum batch size to avoid micro-batch overhead
/// 2. spawn_blocking: Offload CPU-heavy BLAKE3 work to dedicated thread pool
async fn consumer_loop(
    sequencer: Arc<Sequencer>,
    mempool: Arc<Mempool>,
    ledger: Arc<Ledger>,
    total_processed: Arc<AtomicU64>,
    batch_size: usize,
) {
    use std::time::Instant;
    
    let mut last_report = Instant::now();
    let mut batches_since_report: u64 = 0;
    let mut processed_since_report: u64 = 0;
    
    // Minimum batch threshold to avoid micro-batch starvation
    // Process when we have at least 20% of target batch size OR timeout
    let min_batch_threshold = (batch_size / 5).max(100);
    let max_wait_ms = 50; // Don't wait more than 50ms for a full batch
    
    info!("🔄 Consumer starting: batch size {}, min threshold {}", batch_size, min_batch_threshold);
    
    loop {
        // FIX #1: Batch Buffering - Wait for enough transactions
        let mut pending = mempool.size();
        let mut waited_ms = 0u64;
        
        // Wait until we have enough transactions OR timeout
        while pending < min_batch_threshold && waited_ms < max_wait_ms {
            tokio::time::sleep(Duration::from_millis(1)).await;
            waited_ms += 1;
            pending = mempool.size();
        }
        
        // Only process if we have transactions
        if pending > 0 {
            // Pull up to batch_size transactions
            let pull_size = batch_size.min(pending);
            let txs = mempool.pull_batch(pull_size);
            
            if !txs.is_empty() {
                let batch_start = Instant::now();
                let tx_count = txs.len();
                
                // FIX #2: Offload CPU-heavy work to blocking thread pool
                // This prevents the BLAKE3 hashing from blocking the Tokio runtime
                // which would starve network I/O (mDNS, QUIC handshakes)
                let sequencer_clone = sequencer.clone();
                let ledger_clone = ledger.clone();
                
                let result = tokio::task::spawn_blocking(move || {
                    // 1. Process through sequencer (geometric filtering + BLAKE3)
                    let (accepted, rejected) = sequencer_clone.process_batch(txs);
                    
                    // 2. Finalize batch
                    let batch_opt = sequencer_clone.finalize_batch();
                    
                    // 3. Apply to ledger (optimistic - no signature verification)
                    //    Uses fast-path that skips nonce checks for benchmarking
                    let mut applied = 0;
                    if let Some(ref batch) = batch_opt {
                        for ptx in &batch.transactions {
                            // Extract transfer details
                            if let pos::TransactionPayload::Transfer { recipient, amount, .. } = &ptx.tx.payload {
                                // Ensure sender has balance (mint on first use for benchmarks)
                                ledger_clone.mint(ptx.tx.sender, u64::MAX / 2);
                                
                                // Apply transfer using fast path (no nonce check)
                                if ledger_clone.apply_transfer_unchecked(ptx.tx.sender, *recipient, *amount) {
                                    applied += 1;
                                }
                            }
                        }
                    }
                    
                    (accepted, rejected, applied, batch_opt)
                }).await;
                
                if let Ok((accepted, rejected, applied, batch_opt)) = result {
                    if let Some(batch) = batch_opt {
                        let batch_id = hex::encode(&batch.header.hash().as_bytes()[..8]);
                        
                        let elapsed_us = batch_start.elapsed().as_micros();
                        let tps = if elapsed_us > 0 {
                            (accepted as u128 * 1_000_000) / elapsed_us
                        } else {
                            0
                        };
                        
                        // Only log large batches or every Nth small batch to reduce noise
                        if tx_count >= 1000 || batches_since_report % 10 == 0 {
                            info!("📦 Batch {} | {} accepted, {} rejected | {} applied | {}μs ({} TPS)",
                                  batch_id, accepted, rejected, applied, elapsed_us, tps);
                        }
                        
                        total_processed.fetch_add(accepted as u64, Ordering::Relaxed);
                        batches_since_report += 1;
                        processed_since_report += accepted as u64;
                    }
                }
            }
        } else {
            // No transactions - sleep longer to avoid busy-waiting
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        
        // Report every 5 seconds
        if last_report.elapsed() >= Duration::from_secs(5) {
            let actual_tps = processed_since_report as f64 / last_report.elapsed().as_secs_f64();
            info!("🔄 Consumer: {} batches, {} tx/s avg, {} total processed",
                  batches_since_report, actual_tps as u64, total_processed.load(Ordering::Relaxed));
            batches_since_report = 0;
            processed_since_report = 0;
            last_report = Instant::now();
        }
    }
}
