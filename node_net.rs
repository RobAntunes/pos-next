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
    Sequencer, SequencerConfig, Transaction, Mempool, MempoolConfig, TransactionPayload,
    GeometricLedger,
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
    #[arg(long, default_value_t = 2_000_000)]
    tps: u64,

    /// Batch size for sequencer (optimal: 45k for L3 cache)
    #[arg(long, default_value_t = 45_000)]
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

    // Initialize Geometric Ledger (custom high-performance database)
    let db_path = format!("./data/geometric_ledger_{}", args.port);
    let ledger = Arc::new(GeometricLedger::new(&db_path).expect("Failed to initialize GeometricLedger"));
    
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
        // Pre-mint balance for hash-derived sender accounts (uniform distribution)
        // Hash-derived senders will have their first byte uniformly distributed 0-255
        for i in 0..10000u64 {
            let sender_seed = i.to_le_bytes();
            let sender_hash = blake3::hash(&sender_seed);
            let sender_addr = *sender_hash.as_bytes();
            ledger.mint(sender_addr, u64::MAX / 10000);
        }
        info!("💰 Pre-minted balance for 10K hash-derived senders (uniform shard distribution)");

        // Spawn multiple parallel producers for maximum throughput
        let num_producers = num_cpus::get();
        let tps_per_producer = args.tps / num_producers as u64;

        for producer_id in 0..num_producers {
            let mempool_producer = mempool.clone();
            let node_id_producer = node_id;
            let smart_gen = args.smart_gen;

            tokio::spawn(async move {
                producer_loop(mempool_producer, tps_per_producer, node_id_producer, smart_gen, producer_id).await;
            });
        }

        info!("✅ {} Producer loops started (target: {} TPS total, {} TPS each, smart_gen: {})",
              num_producers, args.tps, tps_per_producer, args.smart_gen);
    }
    
    // Consumer Loop: Pull from mempool, create batches, broadcast
    // Run in dedicated blocking thread for maximum throughput
    let sequencer_consumer = sequencer.clone();
    let mempool_consumer = mempool.clone();
    let total_processed = Arc::new(AtomicU64::new(0));
    let total_processed_clone = total_processed.clone();

    // Channel for signed batches (sequencer → ledger worker)
    // OPTIMIZATION: Increased from 100 to 10000 to prevent sequencer blocking
    // The sequencer (fast) shouldn't wait for ledger (slow) - decouple throughput
    let (batch_tx, batch_rx) = std::sync::mpsc::sync_channel::<pos::Batch>(10000);

    std::thread::spawn(move || {
        consumer_loop_blocking(
            sequencer_consumer,
            mempool_consumer,
            total_processed_clone,
            batch_size,
            batch_tx,
        );
    });

    // Async Ledger Worker: Applies batches to state (eventual consistency)
    let ledger_worker = ledger.clone();
    let total_applied = Arc::new(AtomicU64::new(0));
    let total_applied_clone = total_applied.clone();

    std::thread::spawn(move || {
        ledger_worker_loop(ledger_worker, batch_rx, total_applied_clone);
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
                let sequenced = total_processed.load(Ordering::Relaxed);
                let applied = total_applied.load(Ordering::Relaxed);
                let pending = mempool.size();
                let lag = sequenced.saturating_sub(applied);

                info!("📊 Heartbeat | Mempool: {} | Sequenced: {} | Applied: {} | Lag: {} tx",
                      pending, sequenced, applied, lag);
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
    // Disable certificate verification for demo (DO NOT USE IN PRODUCTION)
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
        .with_no_client_auth();

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
                                                    // No per-tx logging - check periodic metrics instead
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
    producer_id: usize,
) {
    use std::time::Instant;
    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;
    use pos::MAX_DISTANCE;

    // Offset nonce by producer_id to ensure each producer generates different transactions
    let mut nonce: u64 = (producer_id as u64) * 100_000_000;
    let mut last_report = Instant::now();
    let mut produced_since_report: u64 = 0;
    
    // Use StdRng which is Send-safe
    let mut rng = StdRng::from_entropy();
    
    // Calculate burst size and interval for efficient batching
    // For maximum throughput: generate massive bursts to keep mempool full
    // Consumer pulls 45k at a time, so keep 2-3 batches buffered
    let burst_size = 135_000usize; // 3x 45k batches
    let interval_ms = (burst_size as u64 * 1000) / target_tps.max(1);
    
    // For smart generation: compute the node's ring position
    // This is the same calculation the sequencer uses
    let node_hash = blake3::hash(b"default-sequencer-node");
    let node_position = pos::calculate_ring_position(&node_hash);

    info!("🏭 Producer #{} starting: {} TPS target (bursts of {}, {}ms interval, smart_gen: {})",
          producer_id, target_tps, burst_size, interval_ms, smart_gen);

    if smart_gen && producer_id == 0 {
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
                // Generate sender address by hashing - cycle through 10K pre-minted senders
                // This ensures uniform distribution while reusing accounts with balance
                let sender_id = (current_nonce + i as u64) % 10000;
                let sender_seed = sender_id.to_le_bytes();
                let sender_hash = blake3::hash(&sender_seed);
                let sender = *sender_hash.as_bytes();

                let mut recipient = [0u8; 32];
                rng_clone.fill(&mut recipient);

                // Calculate per-sender nonce: since we cycle through 10K senders,
                // each sender's nonce is the number of times we've used it
                let sender_nonce = (current_nonce + i as u64) / 10000;

                let tx = Transaction::new(
                    sender,
                    TransactionPayload::Transfer {
                        recipient,
                        amount: rng_clone.gen_range(1..10000),
                        nonce: sender_nonce,
                    },
                    [0u8; 64],
                    timestamp + i as u64,
                );

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
        
        // Report every 5 seconds (only from producer 0 to avoid log spam)
        if producer_id == 0 && last_report.elapsed() >= Duration::from_secs(5) {
            let actual_tps = produced_since_report as f64 / last_report.elapsed().as_secs_f64();
            info!("🏭 Producer #{}: {} tx/s (target: {}), mempool: {} pending",
                  producer_id, actual_tps as u64, target_tps, mempool.size());
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

/// HIGH-PERFORMANCE Consumer Loop - Blocking Thread Version
///
/// This runs in a dedicated OS thread (not tokio) for maximum throughput.
/// Zero async overhead - just tight CPU loop pulling from mempool and hashing.
///
/// ARCHITECTURE: Sequencing is decoupled from state application
/// - This loop: FAST PATH (1.8M TPS sequencing)
/// - Ledger worker: SLOW PATH (eventual state materialization)
fn consumer_loop_blocking(
    sequencer: Arc<Sequencer>,
    mempool: Arc<Mempool>,
    total_processed: Arc<AtomicU64>,
    batch_size: usize,
    batch_sender: std::sync::mpsc::SyncSender<pos::Batch>,
) {
    use std::time::Instant;

    let mut last_report = Instant::now();
    let mut batches_since_report: u64 = 0;
    let mut processed_since_report: u64 = 0;

    tracing::info!("🔄 Consumer starting: batch size {} (BLOCKING MODE - MAX PERFORMANCE)", batch_size);

    loop {
        // Tight loop - no async overhead
        let pending = mempool.size();

        // Pull when we have enough transactions for efficient processing
        // Don't wait for exactly batch_size - pull whenever we have a reasonable amount
        let min_batch = batch_size / 10;  // Pull at 10% threshold (4.5k for 45k batch)

        if pending >= min_batch {
            // Pull up to batch_size for maximum efficiency
            let pull_size = pending.min(batch_size);
            let txs = mempool.pull_batch(pull_size);

            if !txs.is_empty() {
                let batch_start = Instant::now();

                // 1. Process through sequencer (geometric filtering + BLAKE3)
                let (accepted, rejected) = sequencer.process_batch(txs);

                // 2. Finalize batch
                if let Some(batch) = sequencer.finalize_batch() {
                    // 3. Send signed batch to async ledger worker
                    // This is the FAST PATH - just sequencing, no state updates
                    let batch_id = hex::encode(&batch.header.hash().as_bytes()[..8]);

                    let elapsed_us = batch_start.elapsed().as_micros();
                    let tps = if elapsed_us > 0 {
                        (accepted as u128 * 1_000_000) / elapsed_us
                    } else {
                        0
                    };

                    // Send batch to async ledger worker
                    // This will block if channel is full (back-pressure)
                    if let Err(e) = batch_sender.send(batch) {
                        tracing::error!("Failed to send batch to ledger worker: {}", e);
                    }

                    // Only log every 100th batch to reduce I/O overhead
                    if batches_since_report % 100 == 0 {
                        tracing::info!("📦 Batch {} | {} accepted, {} rejected | {}μs ({} TPS)",
                                      batch_id, accepted, rejected, elapsed_us, tps);
                    }

                    total_processed.fetch_add(accepted as u64, Ordering::Relaxed);
                    batches_since_report += 1;
                    processed_since_report += accepted as u64;
                }
            }
        } else if pending > 0 {
            // Small number of transactions - yield briefly
            std::thread::sleep(Duration::from_micros(10));
        } else {
            // Empty mempool - yield to avoid spinning
            std::thread::sleep(Duration::from_micros(100));
        }

        // Report every 5 seconds
        if last_report.elapsed() >= Duration::from_secs(5) {
            let actual_tps = processed_since_report as f64 / last_report.elapsed().as_secs_f64();
            tracing::info!("⚡ Sequencer: {} batches, {} tx/s avg, {} total sequenced",
                          batches_since_report, actual_tps as u64, total_processed.load(Ordering::Relaxed));
            batches_since_report = 0;
            processed_since_report = 0;
            last_report = Instant::now();
        }
    }
}

/// Async Ledger Worker - Materializes state from signed batches
///
/// Uses 256 shard-specific workers for horizontal scaling.
/// Batches arrive in FIFO order (the "trick") and are routed to shard workers.
/// Each shard processes its updates in arrival order, while different shards run in parallel.
///
/// KEY: The sequencer's signed batches are the source of truth.
/// This worker just materializes that truth into queryable state.
fn ledger_worker_loop(
    ledger: Arc<GeometricLedger>,
    batch_receiver: std::sync::mpsc::Receiver<pos::Batch>,
    total_applied: Arc<AtomicU64>,
) {
    use std::time::Instant;

    tracing::info!("💾 Starting 256 shard workers (horizontal scaling)");

    // Create 256 shard-specific channels (one per shard)
    let mut shard_senders = Vec::new();
    let mut shard_receivers = Vec::new();

    for _ in 0..256 {
        // OPTIMIZATION: Increased from 100 to 1000 per shard (256 shards × 1000 = 256K total buffer)
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<pos::Batch>>(1000);
        shard_senders.push(tx);
        shard_receivers.push(rx);
    }

    // Spawn 256 shard workers (one per shard)
    for shard_id in 0..256 {
        let rx = shard_receivers.remove(0);
        let ledger_clone = ledger.clone();
        let total_applied_clone = total_applied.clone();

        std::thread::spawn(move || {
            shard_worker_loop(shard_id, ledger_clone, rx, total_applied_clone);
        });
    }

    // Batch Router: Split batches by SENDER shard (for balance validation)
    let mut last_report = Instant::now();
    let mut batches_routed: u64 = 0;

    loop {
        match batch_receiver.recv() {
            Ok(batch) => {
                // OPTIMIZATION: Parallel routing - use rayon to partition 45K txs across 256 shards
                // This eliminates the single-threaded bottleneck of iterating 45K transactions
                use rayon::prelude::*;

                let shard_batches: Vec<Vec<pos::ProcessedTransaction>> = batch.transactions
                    .into_par_iter()
                    .fold(
                        || {
                            // Thread-local accumulator: 256 empty vectors
                            (0..256).map(|_| Vec::new()).collect::<Vec<Vec<_>>>()
                        },
                        |mut acc, ptx| {
                            // Route transaction to shard based on sender's first byte
                            let shard_id = ptx.tx.sender[0] as usize;
                            acc[shard_id].push(ptx);
                            acc
                        }
                    )
                    .reduce(
                        || (0..256).map(|_| Vec::new()).collect::<Vec<Vec<_>>>(),
                        |mut a, b| {
                            // Merge thread-local results
                            for (shard_id, mut txs) in b.into_iter().enumerate() {
                                a[shard_id].append(&mut txs);
                            }
                            a
                        }
                    );

                // Send to appropriate shard workers
                for (shard_id, txs) in shard_batches.into_iter().enumerate() {
                    if !txs.is_empty() {
                        let shard_batch = pos::Batch {
                            header: batch.header.clone(),
                            transactions: txs,
                        };
                        if let Err(e) = shard_senders[shard_id].send(vec![shard_batch]) {
                            tracing::error!("Failed to send to shard {}: {}", shard_id, e);
                        }
                    }
                }

                batches_routed += 1;

                // Report every 5 seconds
                if last_report.elapsed() >= Duration::from_secs(5) {
                    tracing::info!("📨 Router: {} batches routed to sender shards", batches_routed);
                    batches_routed = 0;
                    last_report = Instant::now();
                }
            }
            Err(_) => {
                tracing::info!("Ledger router: channel closed, exiting");
                break;
            }
        }
    }
}

/// Pending transaction waiting for balance
#[derive(Clone)]
struct PendingTransfer {
    sender: [u8; 32],
    recipient: [u8; 32],
    amount: u64,
    retry_count: u32,
    batch_number: u64,
}

/// Individual shard worker - processes batches for one sender shard in FIFO order
///
/// Maintains a pending queue for transactions that fail due to insufficient balance.
/// Retries pending transactions after each batch, with timeout after 100 retries (~5 seconds).
fn shard_worker_loop(
    shard_id: usize,
    ledger: Arc<GeometricLedger>,
    batch_receiver: std::sync::mpsc::Receiver<Vec<pos::Batch>>,
    total_applied: Arc<AtomicU64>,
) {
    use std::time::Instant;
    use std::collections::{VecDeque, HashMap};
    use pos::Account;

    const MAX_RETRIES: u32 = 100;  // ~5 seconds at 50 batches/sec

    let mut pending_queue: VecDeque<PendingTransfer> = VecDeque::new();
    let mut last_report = Instant::now();
    let mut batches_processed: u64 = 0;
    let mut transfers_applied: u64 = 0;
    let mut transfers_rejected: u64 = 0;
    let mut transfers_pending: u64 = 0;

    loop {
        match batch_receiver.recv() {
            Ok(batches) => {
                for batch in batches {
                    batches_processed += 1;

                    // OPTIMIZATION: Batch all ledger updates together
                    // Instead of individual apply_transfer calls, accumulate all changes
                    // and do a single update_batch at the end.

                    let mut account_cache: HashMap<[u8; 32], Account> = HashMap::new();
                    let mut successful_transfers = 0u64;

                    // 1. Process new transactions from this batch
                    for ptx in batch.transactions {
                        if let pos::TransactionPayload::Transfer {
                            recipient,
                            amount,
                            ..
                        } = ptx.tx.payload
                        {
                            let sender = ptx.tx.sender;

                            // Get current sender balance (from cache or ledger)
                            let mut sender_acc = if let Some(acc) = account_cache.get(&sender) {
                                acc.clone()
                            } else {
                                ledger.get(&sender).unwrap_or_else(|| Account {
                                    pubkey: sender,
                                    ..Default::default()
                                })
                            };

                            if sender_acc.balance >= amount {
                                // Sufficient balance - process transfer
                                sender_acc.balance -= amount;
                                account_cache.insert(sender, sender_acc);

                                // Credit recipient
                                let mut recipient_acc = if let Some(acc) = account_cache.get(&recipient) {
                                    acc.clone()
                                } else {
                                    ledger.get(&recipient).unwrap_or_else(|| Account {
                                        pubkey: recipient,
                                        ..Default::default()
                                    })
                                };
                                recipient_acc.balance += amount;
                                account_cache.insert(recipient, recipient_acc);

                                successful_transfers += 1;
                            } else {
                                // Insufficient balance - add to pending queue
                                pending_queue.push_back(PendingTransfer {
                                    sender,
                                    recipient,
                                    amount,
                                    retry_count: 0,
                                    batch_number: batches_processed,
                                });
                                transfers_pending += 1;
                            }
                        }
                    }

                    // 2. Retry pending transactions (balance might have arrived in this batch)
                    let pending_count = pending_queue.len();
                    for _ in 0..pending_count {
                        if let Some(mut pending) = pending_queue.pop_front() {
                            let mut sender_acc = if let Some(acc) = account_cache.get(&pending.sender) {
                                acc.clone()
                            } else {
                                ledger.get(&pending.sender).unwrap_or_else(|| Account {
                                    pubkey: pending.sender,
                                    ..Default::default()
                                })
                            };

                            if sender_acc.balance >= pending.amount {
                                // Success! Balance arrived
                                sender_acc.balance -= pending.amount;
                                account_cache.insert(pending.sender, sender_acc);

                                let mut recipient_acc = if let Some(acc) = account_cache.get(&pending.recipient) {
                                    acc.clone()
                                } else {
                                    ledger.get(&pending.recipient).unwrap_or_else(|| Account {
                                        pubkey: pending.recipient,
                                        ..Default::default()
                                    })
                                };
                                recipient_acc.balance += pending.amount;
                                account_cache.insert(pending.recipient, recipient_acc);

                                successful_transfers += 1;
                                transfers_pending -= 1;
                            } else {
                                // Still insufficient - retry later
                                pending.retry_count += 1;
                                if pending.retry_count < MAX_RETRIES {
                                    pending_queue.push_back(pending);
                                } else {
                                    // Timeout - reject permanently
                                    transfers_rejected += 1;
                                    transfers_pending -= 1;
                                    if transfers_rejected % 100 == 0 {
                                        tracing::warn!("💸 Shard {:03}: Transaction timed out after {} retries (batch {})",
                                                      shard_id, MAX_RETRIES, pending.batch_number);
                                    }
                                }
                            }
                        }
                    }

                    // 3. SINGLE BATCH WRITE - This is the key optimization!
                    // Write all modified accounts in one shot instead of individual writes
                    if !account_cache.is_empty() {
                        let updates: Vec<([u8; 32], Account)> = account_cache.into_iter().collect();
                        if let Err(e) = ledger.update_batch(&updates) {
                            tracing::error!("Shard {:03}: Batch update failed: {}", shard_id, e);
                        } else {
                            transfers_applied += successful_transfers;
                        }
                    }
                }

                total_applied.fetch_add(transfers_applied, Ordering::Relaxed);

                // Report every 10 seconds per shard
                if last_report.elapsed() >= Duration::from_secs(10) {
                    let tps = transfers_applied as f64 / last_report.elapsed().as_secs_f64();
                    tracing::debug!("💾 Shard {:03}: {} batches, {} applied, {} pending, {} rejected, {:.0} TPS",
                                   shard_id, batches_processed, transfers_applied,
                                   pending_queue.len(), transfers_rejected, tps);
                    batches_processed = 0;
                    transfers_applied = 0;
                    transfers_rejected = 0;
                    last_report = Instant::now();
                }
            }
            Err(_) => {
                tracing::info!("Shard {} worker: channel closed, {} pending transactions dropped",
                              shard_id, pending_queue.len());
                break;
            }
        }
    }
}
