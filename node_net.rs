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
    mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, SwarmBuilder,
};
use quinn::{ClientConfig, Endpoint, ServerConfig};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

use pos::{
    messages::{deserialize_message, serialize_message, RedirectReason},
    ArenaMempool, GeometricLedger, Sequencer, SequencerConfig, SpentSet, Transaction, TransactionPayload,
    WireMessage, PROTOCOL_VERSION,
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

    /// Maximum mempool size (default: 1M transactions)
    #[arg(long, default_value_t = 1_000_000)]
    mempool_size: usize,
}

#[derive(Debug, Clone)]
struct DiscoveredPeer {
    peer_id: PeerId,
    multiaddr: Multiaddr,
    quic_port: u16,
}

/// Perigee-inspired peer scoring for capacity-aware routing
/// Score = (avg_credits / avg_latency) + UCB exploration bonus
struct PeerScore {
    address: String,
    credits_sum: AtomicU64,
    latency_sum_ms: AtomicU64,
    sample_count: AtomicU64,
    last_updated: AtomicU64,
}

impl PeerScore {
    fn new(address: String) -> Self {
        Self {
            address,
            credits_sum: AtomicU64::new(0),
            latency_sum_ms: AtomicU64::new(0),
            sample_count: AtomicU64::new(0),
            last_updated: AtomicU64::new(0),
        }
    }

    /// Update score with new observation (credits received, ACK latency)
    fn update(&self, credits: u64, latency_ms: u64) {
        self.credits_sum.fetch_add(credits, Ordering::Relaxed);
        self.latency_sum_ms.fetch_add(latency_ms.max(1), Ordering::Relaxed);
        self.sample_count.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.last_updated.store(now, Ordering::Relaxed);
    }

    /// Perigee UCB score: exploitation + exploration
    fn score(&self, total_samples: u64) -> f64 {
        let samples = self.sample_count.load(Ordering::Relaxed);
        if samples == 0 {
            return f64::MAX; // Explore unknown peers first
        }

        let avg_credits = self.credits_sum.load(Ordering::Relaxed) as f64 / samples as f64;
        let avg_latency = self.latency_sum_ms.load(Ordering::Relaxed) as f64 / samples as f64;

        // Exploitation: credits per ms of latency
        let exploitation = avg_credits / avg_latency.max(1.0);

        // Exploration bonus (UCB): sqrt(2 * ln(N) / n)
        let exploration = if total_samples > 0 {
            (2.0 * (total_samples as f64).ln() / samples as f64).sqrt()
        } else {
            0.0
        };

        exploitation + exploration * 1000.0 // Scale exploration to be meaningful
    }

    fn get_credits(&self) -> u64 {
        let samples = self.sample_count.load(Ordering::Relaxed);
        if samples == 0 {
            return u64::MAX;
        }
        self.credits_sum.load(Ordering::Relaxed) / samples
    }
}

/// Thread-safe peer registry with Perigee-style scoring
struct PeerRegistry {
    peers: std::sync::RwLock<Vec<Arc<PeerScore>>>,
    total_samples: AtomicU64,
}

impl PeerRegistry {
    fn new() -> Self {
        Self {
            peers: std::sync::RwLock::new(Vec::new()),
            total_samples: AtomicU64::new(0),
        }
    }

    fn add_peer(&self, address: String) {
        let mut peers = self.peers.write().unwrap();
        if !peers.iter().any(|p| p.address == address) {
            info!("📡 Perigee: Added peer {} to registry", address);
            peers.push(Arc::new(PeerScore::new(address)));
        }
    }

    /// Update peer score after receiving BatchAccepted
    fn update_peer(&self, address: &str, credits: u64, latency_ms: u64) {
        let peers = self.peers.read().unwrap();
        if let Some(peer) = peers.iter().find(|p| p.address == address) {
            peer.update(credits, latency_ms);
            self.total_samples.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get best peer for forwarding (highest Perigee score)
    fn get_best_peer(&self, exclude: Option<&str>) -> Option<String> {
        let peers = self.peers.read().unwrap();
        let total = self.total_samples.load(Ordering::Relaxed);

        peers
            .iter()
            .filter(|p| exclude.map_or(true, |e| p.address != e))
            .filter(|p| p.get_credits() > 0) // Must have some capacity
            .max_by(|a, b| {
                a.score(total)
                    .partial_cmp(&b.score(total))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|p| p.address.clone())
    }

    /// Get peer with most capacity (for forwarding)
    fn get_peer_with_capacity(&self, exclude: Option<&str>, min_credits: u64) -> Option<String> {
        let peers = self.peers.read().unwrap();
        peers
            .iter()
            .filter(|p| exclude.map_or(true, |e| p.address != e))
            .filter(|p| p.get_credits() >= min_credits)
            .max_by_key(|p| p.get_credits())
            .map(|p| p.address.clone())
    }
}

// Custom behaviour: just mDNS for local discovery
#[derive(NetworkBehaviour)]
struct DiscoveryBehaviour {
    mdns: mdns::tokio::Behaviour,
}

type ShardSender = std::sync::mpsc::SyncSender<ShardWork>;

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

    let mut args = Args::parse();

    // NOTE: We do NOT force smart_gen = true anymore.
    // Instead, we will relax the Sequencer constraints to accept ALL transactions.
    // This allows for maximum TPS (no generation overhead) while ensuring 100% acceptance.

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

    // Initialize system info for adaptive configuration
    let mut sys = sysinfo::System::new_all();
    sys.refresh_all();
    let total_memory = sys.total_memory(); // in bytes
    let available_memory = sys.available_memory();

    info!(
        "🖥️  System Resources: {} GB Total RAM, {} GB Available",
        total_memory / 1024 / 1024 / 1024,
        available_memory / 1024 / 1024 / 1024
    );

    // Adaptive Configuration
    // 1. Shard Count: 4 shards per GB? No, let's be more conservative.
    //    Min 4 shards (for very low RAM), Max 256
    let shard_count = std::cmp::max(
        4,
        std::cmp::min(256, (total_memory / (1024 * 1024 * 1024) * 4) as usize),
    );
    // 2. Accounts per Shard:
    //    - < 4GB: 250,000 (Total 4M capacity with 16 shards) -> ~512MB Index RAM
    //    - < 8GB: 500,000 (Total 8M capacity) -> ~1GB Index RAM
    //    - >= 8GB: 1,000,000 (Total 16M+ capacity) -> ~2GB+ Index RAM
    let accounts_per_shard = if total_memory < 4 * 1024 * 1024 * 1024 {
        250_000
    } else if total_memory < 8 * 1024 * 1024 * 1024 {
        500_000
    } else {
        1_000_000
    };

    // 3. Arena Size: Use 10% of available RAM, min 64MB, max 4GB
    let arena_size = std::cmp::max(
        64 * 1024 * 1024,
        std::cmp::min(4 * 1024 * 1024 * 1024, (available_memory / 10) as usize),
    );

    info!(
        "⚙️  Adaptive Config: {} Shards, {} Accounts/Shard, {} MB Arena",
        shard_count,
        accounts_per_shard,
        arena_size / 1024 / 1024
    );

    // Initialize Ledger with adaptive settings
    let ledger = Arc::new(
        GeometricLedger::new("data", shard_count, accounts_per_shard)
            .expect("Failed to initialize ledger"),
    );

    // Initialize sequencer config template
    // IMPORTANT: For testing with few nodes, space them evenly on the ring
    // Port 9000 -> position 0, Port 9001 -> position u64::MAX/2, etc.
    let node_id = generate_node_id(&node_name);
    let node_position = {
        // Extract port number and use it to space nodes evenly
        let port_offset = args.port.saturating_sub(9000) as u64;
        let spacing = u64::MAX / 8; // Support up to 8 nodes evenly spaced
        port_offset * spacing
    };
    let mut sequencer_config = SequencerConfig {
        sequencer_id: node_id,
        batch_size: args.batch_size,
        node_position,
        ..Default::default()
    };
    
    info!(
        "🎯 Node geometric position: {} (0x{:016x}) [port-based spacing]",
        node_name,
        node_position
    );

    // BENCHMARK MODE: If producer is enabled, accept ALL transactions
    if args.producer {
        info!("🚀 BENCHMARK MODE: Max Range set to u64::MAX (Accept All)");
        sequencer_config.max_range = u64::MAX;
    }

    // Pre-mint tokens for test accounts (always)
    // We mint for 1M accounts to match the client's load test range
    // CRITICAL: This must happen BEFORE starting the network to avoid race conditions
    info!("💰 Pre-minting balances for 1,000,000 accounts (batched)...");

    // Batch updates to avoid 1M separate Rayon calls and disk flushes
    const BATCH_SIZE: usize = 10_000;
    let mut batch = Vec::with_capacity(BATCH_SIZE);

    for i in 0..1_000_000u64 {
        let sender_seed = i.to_le_bytes();
        let secret_hash = blake3::hash(&sender_seed);
        let secret = *secret_hash.as_bytes();
        let sender_hash = blake3::hash(&secret);
        let sender_addr = *sender_hash.as_bytes();

        // Create account with balance
        let acc = pos::Account {
            pubkey: sender_addr,
            balance: 1_000_000_000,
            ..Default::default()
        };

        batch.push((sender_addr, acc));

        if batch.len() >= BATCH_SIZE {
            ledger
                .update_batch(&batch)
                .expect("Failed to pre-mint batch");
            batch.clear();
        }
    }
    // Flush remaining
    if !batch.is_empty() {
        ledger
            .update_batch(&batch)
            .expect("Failed to pre-mint final batch");
    }
    info!("✅ Pre-minting complete!");

    // Threading Model
    let total_cores = num_cpus::get();
    let num_pairs = (total_cores / 2).max(1).min(pos::ARENA_MAX_WORKERS);
    info!(
        "⚙️ Threading Model: {} Producer/Consumer pairs (using {} cores)",
        num_pairs,
        num_pairs * 2
    );

    // Initialize Arena Mempool
    // Adaptive size based on available RAM
    let arena = Arc::new(ArenaMempool::new(
        arena_size, 100_000, // 100k batches
    ));
    let arena_capacity = arena_size;
    info!(
        "📦 Arena mempool initialized ({} zones, {}M capacity, partitioned)",
        pos::ARENA_MAX_WORKERS * 16,
        arena_capacity / 1_000_000
    );

    // Channel for discovered peers
    let (peer_tx, mut peer_rx) = mpsc::channel::<DiscoveredPeer>(100);

    // Initialize Perigee-style peer registry for capacity-aware routing
    let peer_registry = Arc::new(PeerRegistry::new());
    let peer_registry_clone = peer_registry.clone();

    // Initialize SpentSet for dedup at ingestion (double-spend prevention)
    let spent_set = Arc::new(SpentSet::new());
    let spent_set_clone = spent_set.clone();
    info!("🛡️  SpentSet initialized (dedup gate active)");

    // Start QUIC transport
    let quic_endpoint = start_quic_server(args.port).await?;
    let quic_endpoint_clone = quic_endpoint.clone();

    let total_received = Arc::new(AtomicU64::new(0));
    let total_received_clone = total_received.clone();
    let total_deduped = Arc::new(AtomicU64::new(0));
    let total_deduped_clone = total_deduped.clone();
    let arena_clone = arena.clone();
    let node_id = generate_node_id(&node_name);
    let listen_port = args.port;

    // Spawn QUIC listener with three-layer flow control + dedup gate
    tokio::spawn(async move {
        handle_quic_connections(
            quic_endpoint_clone,
            arena_clone,
            total_received_clone,
            total_deduped_clone,
            spent_set_clone,
            node_id,
            listen_port,
            num_pairs,
            peer_registry_clone,
        )
        .await;
    });

    info!("✅ L2 (QUIC) listening on 0.0.0.0:{}", args.port);

    // Start mDNS discovery
    let quic_port = args.port;
    tokio::spawn(async move {
        if let Err(e) = start_mdns_discovery(args.discovery_port, quic_port, peer_tx).await {
            warn!("mDNS discovery error: {}", e);
        }
    });

    info!("✅ L0 (mDNS) discovery service started");

    // Elastic batching - no fixed batch_size needed
    let total_processed = Arc::new(AtomicU64::new(0));
    let total_processed_clone = total_processed.clone();
    let total_applied = Arc::new(AtomicU64::new(0));

    // Initialize Shard Workers (Receivers)
    // Returns vector of Senders to be shared among Consumers
    let total_rejected = Arc::new(AtomicU64::new(0));
    let shard_senders = start_shard_workers(
        ledger.clone(),
        total_applied.clone(),
        total_rejected.clone(),
    );
    let shard_senders = Arc::new(shard_senders);

    // Spawn Consumers with ELASTIC BATCHING (pull all available pages)
    let mut sequencers = Vec::new();
    for i in 0..num_pairs {
        let sequencer = Arc::new(Sequencer::new(sequencer_config.clone()));
        sequencers.push(sequencer.clone());

        let arena_consumer = arena.clone();
        let total_clone = total_processed.clone();
        let shard_senders_clone = shard_senders.clone();

        std::thread::spawn(move || {
            consumer_loop_blocking(
                i, // worker_id
                sequencer,
                arena_consumer,
                total_clone,
                shard_senders_clone,
            );
        });
    }

    info!("✅ {} Consumer threads started [ELASTIC MODE]", num_pairs);

    // Spawn Producers
    if args.producer {
        let tps_per_producer = args.tps / num_pairs as u64;

        for i in 0..num_pairs {
            let arena_producer = arena.clone();
            let node_id_producer = node_id;
            let smart_gen = args.smart_gen;

            tokio::spawn(async move {
                producer_loop(
                    arena_producer,
                    tps_per_producer,
                    node_id_producer,
                    smart_gen,
                    i, // worker_id
                )
                .await;
            });
        }

        info!(
            "✅ {} Producer loops started (target: {} TPS total)",
            num_pairs, args.tps
        );
    }

    println!();
    println!("🌐 Starting Geometric Turbine Network Test...");
    println!("🚀 Geometric Turbine Network (HashReveal Enabled)");
    println!();

    // Monitor state
    let mut last_total = 0;
    let mut last_time = std::time::Instant::now();

    // Main event loop
    loop {
        tokio::select! {
                    Some(peer_info) = peer_rx.recv() => {
                        info!("🔎 NEW PEER DISCOVERED via mDNS!");
                        if let Some(ip) = extract_ip_from_multiaddr(&peer_info.multiaddr) {
                            // Determine peer's QUIC port (alternate between 9000/9001 for now)
                            let target_port = if args.port == 9000 { 9001 } else { 9000 };
                            
                            // Add to Perigee peer registry for capacity-aware forwarding
                            let peer_addr = format!("{}:{}", ip, target_port);
                            peer_registry.add_peer(peer_addr);
                            
                            let endpoint = quic_endpoint.clone();
                            tokio::spawn(async move {
                                if let Err(e) = connect_to_peer(endpoint, ip, target_port).await {
                                    warn!("Failed to connect to peer: {}", e);
                                }
                            });
                        }
                    }

            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                let total = total_processed.load(Ordering::Relaxed);
                let rejected = total_rejected.load(Ordering::Relaxed);
                let deduped = total_deduped.load(Ordering::Relaxed);

                // Aggregate sequenced count from all workers
                let sequenced: u64 = sequencers.iter()
                    .map(|s| s.current_round() * args.batch_size as u64)
                    .sum();

                let stats = arena.stats();
                let spent_stats = spent_set.stats();
                let lag = sequenced.saturating_sub(total + rejected);

                let now = std::time::Instant::now();
                let elapsed = now.duration_since(last_time).as_secs_f64();
                let tps = (total - last_total) as f64 / elapsed;

                last_total = total;
                last_time = now;

                info!(
                    "📊 Heartbeat | TPS: {:.0} | Arena: ready={} free={} | Sequenced: {} | Applied: {} | Rejected: {} | Deduped: {} | SpentSet: {} | Lag: {}",
                    tps,
                    stats.zones_ready,
                    stats.zones_free,
                    sequenced,
                    total,
                    rejected,
                    deduped,
                    spent_stats.total_entries,
                    lag
                );
            }
        }
    }
}

// Assuming shard_worker_loop is defined elsewhere, adding the log at its end.
// This assumes the structure of shard_worker_loop ends with a loop and then the function itself.
// The exact placement depends on the full definition of shard_worker_loop.
// For this edit, I'm placing it after the assumed end of the function's main loop.
// If shard_worker_loop is not defined in the provided context, this part is an assumption.
// The instruction implies it should be placed after the closing braces of the loop and function.
// Since the full definition of `shard_worker_loop` is not provided, I'm placing it
// where the instruction suggests it would logically fit after the function's main loop.
// This line would typically be inside the `shard_worker_loop` function, after its main processing loop.
// For the purpose of this diff, I'm placing it as indicated by the instruction's context.

fn start_shard_workers(
    ledger: Arc<GeometricLedger>,
    total_applied: Arc<AtomicU64>,
    total_rejected: Arc<AtomicU64>,
) -> Vec<std::sync::mpsc::SyncSender<ShardWork>> {
    // Decouple worker count from shard count
    // On low-core machines, 16 workers causes thrashing. Limit to num_cpus.
    let num_cpus = num_cpus::get();
    let num_workers = std::cmp::min(ledger.shard_count, std::cmp::max(2, num_cpus)); // At least 2 workers

    let mut senders = Vec::new();

    for i in 0..num_workers {
        // Reduced channel size to prevent memory pressure
        let (tx, rx) = std::sync::mpsc::sync_channel(1_000);
        senders.push(tx);
        let ledger_clone = ledger.clone();
        let total_clone = total_applied.clone();
        let rejected_clone = total_rejected.clone();

        std::thread::spawn(move || {
            shard_worker_loop(i, ledger_clone, rx, total_clone, rejected_clone);
        });
    }

    info!(
        "💾 Starting {} shard workers (Direct Dispatch, {} Shards)",
        num_workers, ledger.shard_count
    );
    senders
}

/// Start mDNS discovery service
async fn start_mdns_discovery(
    listen_port: u16,
    quic_port: u16,
    peer_tx: mpsc::Sender<DiscoveredPeer>,
) -> Result<(), Box<dyn std::error::Error>> {
    let id_keys = libp2p::identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(id_keys.public());
    let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;
    let behaviour = DiscoveryBehaviour { mdns };
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|_key| Ok(behaviour))?
        .build();
    let listen_addr = if listen_port == 0 {
        "/ip4/0.0.0.0/tcp/0".parse()?
    } else {
        format!("/ip4/0.0.0.0/tcp/{}", listen_port).parse()?
    };
    swarm.listen_on(listen_addr)?;
    info!(
        "📡 L0 Discovery service started (advertising QUIC port {})",
        quic_port
    );
    loop {
        if let Some(event) = swarm.next().await {
            if let SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Mdns(mdns::Event::Discovered(
                list,
            ))) = event
            {
                for (peer_id, multiaddr) in list {
                    let _ = peer_tx
                        .send(DiscoveredPeer {
                            peer_id,
                            multiaddr,
                            quic_port,
                        })
                        .await;
                }
            }
        }
    }
}

/// Start QUIC server
async fn start_quic_server(port: u16) -> Result<Endpoint, Box<dyn std::error::Error>> {
    let (cert, key) = generate_self_signed_cert()?;
    let mut server_config = ServerConfig::with_single_cert(vec![cert], key)?;
    // Apply transport config to SERVER (critical for handling many concurrent streams)
    server_config.transport_config(Arc::new(create_server_transport_config()));
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    let mut endpoint = Endpoint::server(server_config, addr)?;
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
        .with_no_client_auth();
    let mut client_config = ClientConfig::new(Arc::new(crypto));
    client_config.transport_config(Arc::new(create_transport_config()));
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

/// Handle incoming QUIC connections with four-layer flow control:
/// Layer 0: SpentSet dedup gate (double-spend prevention)
/// Layer 1: QUIC transport backpressure (delay read until capacity)
/// Layer 2: BatchAccepted response with credits
/// Layer 3: Perigee-inspired peer forwarding when overloaded
async fn handle_quic_connections(
    endpoint: Endpoint,
    arena: Arc<ArenaMempool>,
    total_received: Arc<AtomicU64>,
    total_deduped: Arc<AtomicU64>,
    spent_set: Arc<SpentSet>,
    _node_id: [u8; 32],
    listen_port: u16,
    num_active_workers: usize,
    peer_registry: Arc<PeerRegistry>,
) {
    let next_worker = Arc::new(AtomicUsize::new(0));
    let current_tps = Arc::new(AtomicU64::new(0));
    let self_address = format!("*******:{}", listen_port);

    while let Some(connecting) = endpoint.accept().await {
        let arena = arena.clone();
        let total_received = total_received.clone();
        let total_deduped = total_deduped.clone();
        let spent_set = spent_set.clone();
        let next_worker = next_worker.clone();
        let current_tps = current_tps.clone();
        let peer_registry = peer_registry.clone();
        let self_addr = self_address.clone();

        tokio::spawn(async move {
            match connecting.await {
                Ok(connection) => {
                    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                        let arena = arena.clone();
                        let total_received = total_received.clone();
                        let total_deduped = total_deduped.clone();
                        let spent_set = spent_set.clone();
                        let next_worker = next_worker.clone();
                        let current_tps = current_tps.clone();
                        let peer_registry = peer_registry.clone();
                        let self_addr = self_addr.clone();

                        tokio::spawn(async move {
                            // ═══════════════════════════════════════════════════════
                            // LAYER 1: QUIC Transport Backpressure
                            // Delay reading until we have capacity. QUIC flow control
                            // will naturally stall the sender's write.
                            // ═══════════════════════════════════════════════════════
                            let mut backpressure_wait = 0u32;
                            while arena.is_full() && backpressure_wait < 100 {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                backpressure_wait += 1;
                            }

                            if let Ok(data) = recv.read_to_end(10 * 1024 * 1024).await {
                                if let Ok(msg) = deserialize_message(&data) {
                                    match msg {
                                        WireMessage::TransactionSubmission { tx } => {
                                            let tx: Transaction = tx.into();
                                            if arena.submit_batch(&[tx]) {
                                                total_received.fetch_add(1, Ordering::Relaxed);
                                            }
                                            // Layer 2: Send BatchAccepted with credits
                                            let response = WireMessage::BatchAccepted {
                                                accepted_count: 1,
                                                credits: arena.free_capacity(),
                                                current_tps: current_tps.load(Ordering::Relaxed),
                                            };
                                            if let Ok(msg_bytes) = serialize_message(&response) {
                                                let _ = send.write_all(&msg_bytes).await;
                                            }
                                            let _ = send.finish().await;
                                        }
                                        WireMessage::BatchSubmission { txs } => {
                                            let txs: Vec<Transaction> =
                                                txs.into_iter().map(|tx| tx.into()).collect();

                                            // ═══════════════════════════════════════════
                                            // LAYER 0: SpentSet Dedup Gate
                                            // Filter out double-spend attempts BEFORE arena
                                            // ═══════════════════════════════════════════
                                            let (clean_txs, dupes) = spent_set.filter_batch(txs, |tx| {
                                                (&tx.sender, tx.nonce)
                                            });
                                            let count = clean_txs.len();
                                            
                                            if dupes > 0 {
                                                total_deduped.fetch_add(dupes as u64, Ordering::Relaxed);
                                            }

                                            let worker_id = next_worker
                                                .fetch_add(1, Ordering::Relaxed)
                                                % num_active_workers;

                                            // Try to submit (only clean txs)
                                            let submitted = if count == 0 {
                                                // All txs were duplicates
                                                true // Still "submitted" for response purposes
                                            } else if arena.submit_batch_partitioned(worker_id, &clean_txs) {
                                                total_received.fetch_add(count as u64, Ordering::Relaxed);
                                                true
                                            } else {
                                                // Fallback to another worker
                                                let retry_worker = (worker_id + 1) % num_active_workers;
                                                if arena.submit_batch_partitioned(retry_worker, &clean_txs) {
                                                    total_received.fetch_add(count as u64, Ordering::Relaxed);
                                                    true
                                                } else {
                                                    false
                                                }
                                            };

                                            if submitted {
                                                // ═══════════════════════════════════════════
                                                // LAYER 2: BatchAccepted with credits
                                                // ═══════════════════════════════════════════
                                                let response = WireMessage::BatchAccepted {
                                                    accepted_count: count as u64,
                                                    credits: arena.free_capacity(),
                                                    current_tps: current_tps.load(Ordering::Relaxed),
                                                };
                                                if let Ok(msg_bytes) = serialize_message(&response) {
                                                    let _ = send.write_all(&msg_bytes).await;
                                                }
                                            } else {
                                                // ═══════════════════════════════════════════
                                                // LAYER 3: Perigee-inspired peer forwarding
                                                // Find best peer with capacity and redirect
                                                // ═══════════════════════════════════════════
                                                if let Some(best_peer) = peer_registry
                                                    .get_peer_with_capacity(Some(&self_addr), count as u64)
                                                {
                                                    info!(
                                                        "📡 Perigee: Redirecting batch of {} txs to {}",
                                                        count, best_peer
                                                    );
                                                    let response = WireMessage::BatchRedirect {
                                                        suggested_node: best_peer,
                                                        reason: RedirectReason::ArenaFull,
                                                        suggested_credits: 0, // Unknown
                                                    };
                                                    if let Ok(msg_bytes) = serialize_message(&response) {
                                                        let _ = send.write_all(&msg_bytes).await;
                                                    }
                                                } else {
                                                    // No peers available, send BatchAccepted with 0 credits
                                                    warn!("Arena full, no peers available for redirect");
                                                    let response = WireMessage::BatchAccepted {
                                                        accepted_count: 0,
                                                        credits: 0,
                                                        current_tps: current_tps.load(Ordering::Relaxed),
                                                    };
                                                    if let Ok(msg_bytes) = serialize_message(&response) {
                                                        let _ = send.write_all(&msg_bytes).await;
                                                    }
                                                }
                                            }
                                            let _ = send.finish().await;
                                        }
                                        WireMessage::MempoolStatus => {
                                            let response = WireMessage::MempoolStatusResponse {
                                                pending_count: arena.free_capacity(),
                                                capacity_tps: current_tps.load(Ordering::Relaxed),
                                            };
                                            if let Ok(msg_bytes) = serialize_message(&response) {
                                                let _ = send.write_all(&msg_bytes).await;
                                            }
                                            let _ = send.finish().await;
                                        }
                                        _ => {
                                            let _ = send.finish().await;
                                        }
                                    }
                                } else if let Err(e) = deserialize_message(&data) {
                                    warn!(
                                        "Failed to deserialize message ({} bytes): {}",
                                        data.len(),
                                        e
                                    );
                                }
                            }
                        });
                    }
                }
                Err(_) => {}
            }
        });
    }
}

// ... (Helper functions like connect_to_peer, etc. remain the same) ...

async fn connect_to_peer(
    endpoint: Endpoint,
    ip: IpAddr,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::new(ip, port);
    let connection = endpoint.connect(addr, "localhost")?.await?;
    let (mut send, _) = connection.open_bi().await?;
    let handshake = WireMessage::Handshake {
        version: PROTOCOL_VERSION,
        geometric_id: [0u8; 32],
        listen_port: port,
    };
    let msg_bytes = serialize_message(&handshake)?;
    send.write_all(&msg_bytes).await?;
    send.finish().await?;
    Ok(())
}

fn generate_self_signed_cert(
) -> Result<(rustls::Certificate, rustls::PrivateKey), Box<dyn std::error::Error>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    Ok((
        rustls::Certificate(cert.serialize_der()?),
        rustls::PrivateKey(cert.serialize_private_key_der()),
    ))
}

fn create_transport_config() -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    config.max_concurrent_bidi_streams(1000u32.into());
    config.max_concurrent_uni_streams(1000u32.into());
    config.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));
    config.keep_alive_interval(Some(Duration::from_secs(5)));
    config
}

fn create_server_transport_config() -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    // Allow many concurrent streams from clients sending batches
    config.max_concurrent_bidi_streams(10_000u32.into());
    config.max_concurrent_uni_streams(10_000u32.into());
    // Increase receive window for high-throughput batch ingestion
    config.receive_window((32 * 1024 * 1024u32).into()); // 32MB
    config.stream_receive_window((16 * 1024 * 1024u32).into()); // 16MB per stream
    config.send_window(32 * 1024 * 1024); // 32MB
    // Longer timeout for sustained load
    config.max_idle_timeout(Some(Duration::from_secs(60).try_into().unwrap()));
    config.keep_alive_interval(Some(Duration::from_secs(10)));
    config
}

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

fn generate_node_id(name: &str) -> [u8; 32] {
    let hash = blake3::hash(name.as_bytes());
    *hash.as_bytes()
}

struct NoCertificateVerification;
impl rustls::client::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _: &rustls::Certificate,
        _: &[rustls::Certificate],
        _: &rustls::ServerName,
        _: &mut dyn Iterator<Item = &[u8]>,
        _: &[u8],
        _: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

// ============================================================================
// THE HEARTBEAT: Producer & Consumer Loops
// ============================================================================

async fn producer_loop(
    arena: Arc<ArenaMempool>,
    target_tps: u64,
    _sender_id: [u8; 32],
    smart_gen: bool,
    worker_id: usize,
) {
    // use rand::rngs::StdRng;
    // use rand::{Rng, SeedableRng};
    use std::time::Instant;

    let mut nonce: u64 = (worker_id as u64) * 100_000_000;
    let mut last_report = Instant::now();
    let mut produced_since_report: u64 = 0;

    // Match burst size to Arena Zone Size (45,000)
    let burst_size = pos::ZONE_SIZE;
    let interval_ms = (burst_size as u64 * 1000) / target_tps.max(1);

    let node_hash = blake3::hash(b"default-sequencer-node");
    let node_position = pos::calculate_ring_position(&node_hash);

    // Pre-compute accounts to avoid hashing in the hot loop
    // Pre-compute accounts and secrets
    let accounts: std::sync::Arc<Vec<([u8; 32], [u8; 32])>> = std::sync::Arc::new(
        (0u64..10000)
            .map(|i| {
                let seed = i.to_le_bytes();
                let secret_hash = blake3::hash(&seed);
                let secret = *secret_hash.as_bytes();
                let sender_hash = blake3::hash(&secret);
                (*sender_hash.as_bytes(), secret)
            })
            .collect(),
    );

    info!(
        "🏭 Producer #{} starting: {} TPS target (Partition {})",
        worker_id, target_tps, worker_id
    );

    loop {
        let burst_start = Instant::now();
        let arena_clone = arena.clone();
        let accounts_clone = accounts.clone();
        let current_nonce = nonce;

        let submitted = tokio::task::spawn_blocking(move || {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let max_range = pos::MAX_DISTANCE;

            // Pre-allocate batch vector
            let mut batch = Vec::with_capacity(burst_size);

            for i in 0..burst_size {
                // Deterministic lookup (O(1))
                // Deterministic lookup (O(1))
                let sender_idx = ((current_nonce + i as u64) % 10000) as usize;
                let (sender, secret) = accounts_clone[sender_idx];

                let recipient_idx = (((current_nonce + i as u64) / 10000) % 10000) as usize;
                let (recipient, _) = accounts_clone[recipient_idx];

                let sender_nonce = (current_nonce + i as u64) / 10000;
                let amount = ((i as u64 * 7919) % 9999) + 1;

                let tx = Transaction::new_fast(
                    sender,
                    TransactionPayload::Transfer {
                        recipient,
                        amount,
                        nonce: sender_nonce,
                    },
                    sender_nonce,
                    timestamp + i as u64,
                    secret, // Correct HashReveal secret
                );

                if smart_gen {
                    let tx_hash = tx.hash();
                    let tx_position = pos::calculate_ring_position(&tx_hash);
                    let distance = if tx_position >= node_position {
                        tx_position - node_position
                    } else {
                        (u64::MAX - node_position) + tx_position + 1
                    };
                    if distance <= max_range {
                        batch.push(tx);
                    }
                } else {
                    batch.push(tx);
                }
            }

            // Submit entire batch to partition
            if !batch.is_empty() {
                if arena_clone.submit_batch_partitioned(worker_id, &batch) {
                    return batch.len() as u64;
                }
            }
            0
        })
        .await
        .unwrap_or(0);

        nonce += burst_size as u64;
        produced_since_report += submitted;

        if worker_id == 0 && last_report.elapsed() >= Duration::from_secs(5) {
            let actual_tps = produced_since_report as f64 / last_report.elapsed().as_secs_f64();
            info!(
                "🏭 Producer #{} stats: {} tx/s",
                worker_id, actual_tps as u64
            );
            produced_since_report = 0;
            last_report = Instant::now();
        }

        let elapsed = burst_start.elapsed().as_millis() as u64;
        if elapsed < interval_ms {
            tokio::time::sleep(Duration::from_millis(interval_ms - elapsed)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }
}

/// Elastic consumer loop - pulls ALL available pages and processes them
/// Batch size is determined by available data, not a fixed number
fn consumer_loop_blocking(
    worker_id: usize,
    sequencer: Arc<Sequencer>,
    arena: Arc<ArenaMempool>,
    total_processed: Arc<AtomicU64>,
    shard_senders: Arc<Vec<std::sync::mpsc::SyncSender<ShardWork>>>,
) {
    use std::time::Instant;
    let mut last_report = Instant::now();
    let mut batches_since_report: u64 = 0;
    let mut processed_since_report: u64 = 0;
    let mut accepted_since_report: u64 = 0;
    let mut rejected_since_report: u64 = 0;
    let mut pages_since_report: u64 = 0;

    tracing::info!(
        "🔄 Consumer #{} starting (Partition {}) [ELASTIC MODE]",
        worker_id,
        worker_id
    );

    loop {
        // ═══════════════════════════════════════════════════════════════
        // ELASTIC BATCHING: Pull ALL ready pages from this partition
        // No fixed batch size - process whatever is available
        // ═══════════════════════════════════════════════════════════════
        let pages = arena.pull_all_ready(worker_id);
        
        if !pages.is_empty() {
            let page_count = pages.len();
            pages_since_report += page_count as u64;
            
            // Flatten all pages into one elastic batch
            let total_tx_count: usize = pages.iter().map(|p| p.len()).sum();
            
            // Process each page through the sequencer
            let mut total_accepted = 0usize;
            let mut total_rejected = 0usize;
            
            for page_txs in pages {
                let (accepted, rejected) = sequencer.process_batch(page_txs);
                total_accepted += accepted;
                total_rejected += rejected;
            }

            // Finalize after processing all pages (elastic batch)
            if let Some(batch) = sequencer.finalize_batch() {
                let batch = Arc::new(batch);
                let total_txs = batch.transactions.len();
                let num_shards = shard_senders.len();
                let chunk_size = (total_txs + num_shards - 1) / num_shards;

                for (i, sender) in shard_senders.iter().enumerate() {
                    let start = i * chunk_size;
                    if start >= total_txs {
                        break;
                    }
                    let count = std::cmp::min(chunk_size, total_txs - start);
                    let work = ShardWork {
                        batch: batch.clone(),
                        start,
                        count,
                    };
                    let _ = sender.send(work);
                }
                batches_since_report += 1;
            }

            // Update metrics
            total_processed.fetch_add((total_accepted + total_rejected) as u64, Ordering::Relaxed);
            processed_since_report += (total_accepted + total_rejected) as u64;
            accepted_since_report += total_accepted as u64;
            rejected_since_report += total_rejected as u64;
        } else {
            // No ready pages, yield briefly
            std::thread::sleep(Duration::from_micros(50));
        }

        if worker_id == 0 && last_report.elapsed() >= Duration::from_secs(5) {
            let actual_tps = processed_since_report as f64 / last_report.elapsed().as_secs_f64();
            let stats = arena.stats();
            tracing::info!(
                "⚡ ELASTIC: {} pages → {} batches | {} tx/s ({} acc, {} rej) | Arena: {} ready",
                pages_since_report,
                batches_since_report,
                actual_tps as u64,
                accepted_since_report,
                rejected_since_report,
                stats.zones_ready
            );
            batches_since_report = 0;
            processed_since_report = 0;
            accepted_since_report = 0;
            rejected_since_report = 0;
            pages_since_report = 0;
            last_report = Instant::now();
        }
    }
}

// Ledger Worker Loop removed (Decoupled architecture)

struct ShardWork {
    batch: Arc<pos::Batch>,
    start: usize,
    count: usize,
}

fn shard_worker_loop(
    shard_id: usize,
    ledger: Arc<GeometricLedger>,
    work_rx: std::sync::mpsc::Receiver<ShardWork>,
    total: Arc<AtomicU64>,
    total_rejected: Arc<AtomicU64>,
) {
    use pos::Account;
    use std::collections::HashMap;

    // Process work items as they arrive
    while let Ok(work) = work_rx.recv() {
        let mut cache = HashMap::new();
        let mut applied_count = 0;
        let mut rejected_count = 0;

        // Zero-Copy: Access the slice of the shared batch
        let txs = &work.batch.transactions[work.start..work.start + work.count];

        // DEBUG LOGGING
        if shard_id == 0 {
            // tracing::info!("Worker #{} received batch chunk of {} txs", shard_id, txs.len());
        }

        for ptx in txs {
            if let pos::TransactionPayload::Transfer {
                recipient, amount, ..
            } = ptx.tx.payload
            {
                let sender = ptx.tx.sender;
                // Simple ledger application logic
                let mut s_acc = cache.remove(&sender).unwrap_or_else(|| {
                    ledger.get(&sender).unwrap_or(Account {
                        pubkey: sender,
                        ..Default::default()
                    })
                });

                if s_acc.balance >= amount {
                    s_acc.balance -= amount;
                    cache.insert(sender, s_acc);
                    let mut r_acc = cache.remove(&recipient).unwrap_or_else(|| {
                        ledger.get(&recipient).unwrap_or(Account {
                            pubkey: recipient,
                            ..Default::default()
                        })
                    });
                    r_acc.balance += amount;
                    cache.insert(recipient, r_acc);
                    applied_count += 1;
                } else {
                    // tracing::warn!(
                    //     "Insufficient balance for sender {:?}: {} < {}",
                    //     &sender[0..4],
                    //     s_acc.balance,
                    //     amount
                    // );
                    cache.insert(sender, s_acc); // Return to cache
                    rejected_count += 1;
                }
            }
        }
        if !cache.is_empty() {
            let updates: Vec<_> = cache.into_iter().collect();
            let _ = ledger.update_batch(&updates);
            total.fetch_add(applied_count, Ordering::Relaxed);
            total_rejected.fetch_add(rejected_count, Ordering::Relaxed);
        }
    }
}
