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
    Sequencer, SequencerConfig, Transaction, ArenaMempool, MempoolConfig, TransactionPayload,
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
    
    // Initialize sequencer config template
    let sequencer_config = SequencerConfig {
        sequencer_id: generate_node_id(&node_name),
        batch_size: args.batch_size,
        ..Default::default()
    };
    
    // Initialize Arena Mempool
    // Partitioned per worker pair (Producer N <-> Consumer N)
    let arena = Arc::new(ArenaMempool::new());
    let arena_capacity = pos::ARENA_MAX_WORKERS * 16 * pos::ZONE_SIZE;
    info!("📦 Arena mempool initialized ({} zones, {}M capacity, partitioned)", 
          pos::ARENA_MAX_WORKERS * 16, arena_capacity / 1_000_000);

    // Channel for discovered peers
    let (peer_tx, mut peer_rx) = mpsc::channel::<DiscoveredPeer>(100);

    // Start QUIC transport (L2)
    let quic_endpoint = start_quic_server(args.port).await?;
    let quic_endpoint_clone = quic_endpoint.clone();
    
    let total_received = Arc::new(AtomicU64::new(0));
    let total_received_clone = total_received.clone();
    let arena_clone = arena.clone();
    let node_id = generate_node_id(&node_name);

    // Spawn QUIC listener task
    tokio::spawn(async move {
        handle_quic_connections(
            quic_endpoint_clone,
            arena_clone,
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
    
    // Determine Threading Model
    let total_cores = num_cpus::get();
    // Split cores: Half for producers, half for consumers
    // For 8 cores: 4 Prod, 4 Cons.
    let num_pairs = (total_cores / 2).max(1).min(pos::ARENA_MAX_WORKERS);
    
    info!("⚙️ Threading Model: {} Producer/Consumer pairs (using {} cores)", num_pairs, num_pairs * 2);
    
    let batch_size = args.batch_size;
    let total_processed = Arc::new(AtomicU64::new(0));
    let total_processed_clone = total_processed.clone();
    
    // Channel for signed batches (sequencer → ledger worker)
    // Unbounded to prevent sequencer blocking
    let (batch_tx, batch_rx) = std::sync::mpsc::channel::<pos::Batch>();

    // Spawn Consumers (Consumers pull from their partition)
    for i in 0..num_pairs {
        let arena_consumer = arena.clone();
        let total_clone = total_processed_clone.clone();
        let batch_tx_clone = batch_tx.clone();
        // dedicated sequencer instance per consumer (no contention)
        let sequencer = Sequencer::new(sequencer_config.clone());
        
        std::thread::spawn(move || {
            consumer_loop_blocking(
                i, // worker_id
                sequencer,
                arena_consumer,
                total_clone,
                batch_size,
                batch_tx_clone,
            );
        });
    }
    
    info!("✅ {} Consumer threads started", num_pairs);
    
    // Spawn Producers (Producers write to their partition)
    if args.producer {
        // Pre-mint balance
        for i in 0..10000u64 {
            let sender_seed = i.to_le_bytes();
            let sender_hash = blake3::hash(&sender_seed);
            let sender_addr = *sender_hash.as_bytes();
            ledger.mint(sender_addr, u64::MAX / 10000);
        }
        info!("💰 Pre-minted balance for 10K senders");

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
                    i // worker_id
                ).await;
            });
        }

        info!("✅ {} Producer loops started (target: {} TPS total)", num_pairs, args.tps);
    }

    // Async Ledger Worker: Applies batches to state (eventual consistency)
    let ledger_worker = ledger.clone();
    let total_applied = Arc::new(AtomicU64::new(0));
    let total_applied_clone = total_applied.clone();

    std::thread::spawn(move || {
        ledger_worker_loop(ledger_worker, batch_rx, total_applied_clone);
    });
    
    println!();
    println!("📡 Scanning for peers on the local network...");
    println!();

    // Main event loop
    loop {
        tokio::select! {
            Some(peer_info) = peer_rx.recv() => {
                info!("🔎 NEW PEER DISCOVERED via mDNS!");
                if let Some(ip) = extract_ip_from_multiaddr(&peer_info.multiaddr) {
                    let target_port = if args.port == 9000 { 9001 } else { 9000 };
                    let endpoint = quic_endpoint.clone();
                    tokio::spawn(async move {
                        if let Err(e) = connect_to_peer(endpoint, ip, target_port).await {
                            warn!("Failed to connect to peer: {}", e);
                        }
                    });
                }
            }
            
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                let sequenced = total_processed.load(Ordering::Relaxed);
                let applied = total_applied.load(Ordering::Relaxed);
                let stats = arena.stats();
                let lag = sequenced.saturating_sub(applied);

                info!("📊 Heartbeat | Arena: ready={} free={} | Sequenced: {} | Applied: {} | Lag: {}",
                      stats.zones_ready, stats.zones_free, sequenced, applied, lag);
            }
        }
    }
}

// ... (mDNS and QUIC setup code remains similar, see below for changes) ...

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
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
        .with_behaviour(|_key| Ok(behaviour))?  
        .build();
    let listen_addr = if listen_port == 0 { "/ip4/0.0.0.0/tcp/0".parse()? } else { format!("/ip4/0.0.0.0/tcp/{}", listen_port).parse()? };
    swarm.listen_on(listen_addr)?;
    info!("📡 L0 Discovery service started (advertising QUIC port {})", quic_port);
    loop {
        if let Some(event) = swarm.next().await {
            if let SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Mdns(mdns::Event::Discovered(list))) = event {
                for (peer_id, multiaddr) in list {
                    let _ = peer_tx.send(DiscoveredPeer { peer_id, multiaddr, quic_port }).await;
                }
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
    let crypto = rustls::ClientConfig::builder().with_safe_defaults().with_custom_certificate_verifier(Arc::new(NoCertificateVerification)).with_no_client_auth();
    let mut client_config = ClientConfig::new(Arc::new(crypto));
    client_config.transport_config(Arc::new(create_transport_config()));
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

/// Handle incoming QUIC connections
async fn handle_quic_connections(
    endpoint: Endpoint,
    arena: Arc<ArenaMempool>, // Changed from Mempool
    total_received: Arc<AtomicU64>,
    node_id: [u8; 32],
    listen_port: u16,
) {
    while let Some(connecting) = endpoint.accept().await {
        let arena = arena.clone();
        let total_received = total_received.clone();
        
        tokio::spawn(async move {
            match connecting.await {
                Ok(connection) => {
                    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                        let arena = arena.clone();
                        let total_received = total_received.clone();
                        tokio::spawn(async move {
                            if let Ok(data) = recv.read_to_end(10 * 1024 * 1024).await {
                                if let Ok(msg) = deserialize_message(&data) {
                                    match msg {
                                        WireMessage::TransactionSubmission { tx } => {
                                            let tx: Transaction = tx.into();
                                            // Use legacy submission for random individual txs
                                            if arena.submit_batch(&[tx]) {
                                                total_received.fetch_add(1, Ordering::Relaxed);
                                            }
                                            let _ = send.finish().await;
                                        }
                                        _ => { let _ = send.finish().await; }
                                    }
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

async fn connect_to_peer(endpoint: Endpoint, ip: IpAddr, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::new(ip, port);
    let connection = endpoint.connect(addr, "localhost")?.await?;
    let (mut send, _) = connection.open_bi().await?;
    let handshake = WireMessage::Handshake { version: PROTOCOL_VERSION, geometric_id: [0u8; 32], listen_port: port };
    let msg_bytes = serialize_message(&handshake)?;
    send.write_all(&msg_bytes).await?;
    send.finish().await?;
    Ok(())
}

fn generate_self_signed_cert() -> Result<(rustls::Certificate, rustls::PrivateKey), Box<dyn std::error::Error>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    Ok((rustls::Certificate(cert.serialize_der()?), rustls::PrivateKey(cert.serialize_private_key_der())))
}

fn create_transport_config() -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    config.max_concurrent_bidi_streams(100u32.into());
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
    fn verify_server_cert(&self, _: &rustls::Certificate, _: &[rustls::Certificate], _: &rustls::ServerName, _: &mut dyn Iterator<Item = &[u8]>, _: &[u8], _: std::time::SystemTime) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

// ============================================================================
// THE HEARTBEAT: Producer & Consumer Loops
// ============================================================================

async fn producer_loop(
    arena: Arc<ArenaMempool>,
    target_tps: u64,
    sender_id: [u8; 32],
    smart_gen: bool,
    worker_id: usize,
) {
    use std::time::Instant;
    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;
    
    let mut nonce: u64 = (worker_id as u64) * 100_000_000;
    let mut last_report = Instant::now();
    let mut produced_since_report: u64 = 0;
    
    // Match burst size to Arena Zone Size (45,000)
    let burst_size = pos::ZONE_SIZE; 
    let interval_ms = (burst_size as u64 * 1000) / target_tps.max(1);
    
    let node_hash = blake3::hash(b"default-sequencer-node");
    let node_position = pos::calculate_ring_position(&node_hash);

    // Pre-compute accounts to avoid hashing in the hot loop
    let accounts: std::sync::Arc<Vec<[u8; 32]>> = std::sync::Arc::new((0u64..10000).map(|i| {
        let seed = i.to_le_bytes();
        let hash = blake3::hash(&seed);
        *hash.as_bytes()
    }).collect());

    info!("🏭 Producer #{} starting: {} TPS target (Partition {})", worker_id, target_tps, worker_id);

    loop {
        let burst_start = Instant::now();
        let arena_clone = arena.clone();
        let accounts_clone = accounts.clone();
        let current_nonce = nonce;
        
        let submitted = tokio::task::spawn_blocking(move || {
            let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
            let max_range = pos::MAX_DISTANCE;
            
            // Pre-allocate batch vector
            let mut batch = Vec::with_capacity(burst_size);
            
            for i in 0..burst_size {
                // Deterministic lookup (O(1))
                let sender_idx = ((current_nonce + i as u64) % 10000) as usize;
                let sender = accounts_clone[sender_idx];
                
                let recipient_idx = (((current_nonce + i as u64) / 10000) % 10000) as usize;
                let recipient = accounts_clone[recipient_idx];

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
                    [0u8; 32], // HashReveal secret
                );

                if smart_gen {
                    let tx_hash = tx.hash();
                    let tx_position = pos::calculate_ring_position(&tx_hash);
                    let distance = if tx_position >= node_position { tx_position - node_position } else { (u64::MAX - node_position) + tx_position + 1 };
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
        }).await.unwrap_or(0);
        
        nonce += burst_size as u64;
        produced_since_report += submitted;
        
        if worker_id == 0 && last_report.elapsed() >= Duration::from_secs(5) {
            let actual_tps = produced_since_report as f64 / last_report.elapsed().as_secs_f64();
            info!("🏭 Producer #{} stats: {} tx/s", worker_id, actual_tps as u64);
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

fn consumer_loop_blocking(
    worker_id: usize,
    sequencer: Sequencer,
    arena: Arc<ArenaMempool>,
    total_processed: Arc<AtomicU64>,
    _batch_size: usize, // unused, we take full zones
    batch_sender: std::sync::mpsc::Sender<pos::Batch>,
) {
    use std::time::Instant;
    let mut last_report = Instant::now();
    let mut batches_since_report: u64 = 0;
    let mut processed_since_report: u64 = 0;

    tracing::info!("🔄 Consumer #{} starting (Partition {})", worker_id, worker_id);

    loop {
        // Pull from partition
        if let Some(txs) = arena.pull_batch_partitioned(worker_id) {
            if !txs.is_empty() {
                let batch_start = Instant::now();
                let (accepted, rejected) = sequencer.process_batch(txs);

                if let Some(batch) = sequencer.finalize_batch() {
                    if let Err(e) = batch_sender.send(batch) {
                        tracing::error!("Failed to send batch: {}", e);
                    }
                    batches_since_report += 1;
                }
                
                // Update metrics for ALL processed transactions (accepted or rejected)
                total_processed.fetch_add((accepted + rejected) as u64, Ordering::Relaxed);
                processed_since_report += (accepted + rejected) as u64;
            }
        } else {
            // No ready zones in partition, yield
            std::thread::sleep(Duration::from_micros(50));
        }

        if worker_id == 0 && last_report.elapsed() >= Duration::from_secs(5) {
            let actual_tps = processed_since_report as f64 / last_report.elapsed().as_secs_f64();
            let stats = arena.stats();
            tracing::info!("⚡ Consumer Stats: {} batches, {} tx/s (Sequenced) | Arena: {} ready",
                          batches_since_report, actual_tps as u64, stats.zones_ready);
            batches_since_report = 0;
            processed_since_report = 0;
            last_report = Instant::now();
        }
    }
}

// Ledger Worker Loop (Same as before)
fn ledger_worker_loop(
    ledger: Arc<GeometricLedger>,
    batch_receiver: std::sync::mpsc::Receiver<pos::Batch>,
    total_applied: Arc<AtomicU64>,
) {
    use std::time::Instant;
    tracing::info!("💾 Starting 256 shard workers");
    let mut shard_senders = Vec::new();
    let mut shard_receivers = Vec::new();
    for _ in 0..256 {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<pos::Batch>>(1000);
        shard_senders.push(tx);
        shard_receivers.push(rx);
    }
    for shard_id in 0..256 {
        let rx = shard_receivers.remove(0);
        let ledger_clone = ledger.clone();
        let total_applied_clone = total_applied.clone();
        std::thread::spawn(move || {
            shard_worker_loop(shard_id, ledger_clone, rx, total_applied_clone);
        });
    }
    loop {
        if let Ok(batch) = batch_receiver.recv() {
             use rayon::prelude::*;
             let shard_batches: Vec<Vec<pos::ProcessedTransaction>> = batch.transactions
                .into_par_iter()
                .fold(|| (0..256).map(|_| Vec::new()).collect::<Vec<Vec<_>>>(), |mut acc, ptx| {
                    let shard_id = ptx.tx.sender[0] as usize;
                    acc[shard_id].push(ptx);
                    acc
                })
                .reduce(|| (0..256).map(|_| Vec::new()).collect::<Vec<Vec<_>>>(), |mut a, b| {
                    for (i, mut txs) in b.into_iter().enumerate() { a[i].append(&mut txs); }
                    a
                });
            for (shard_id, txs) in shard_batches.into_iter().enumerate() {
                if !txs.is_empty() {
                    let _ = shard_senders[shard_id].send(vec![pos::Batch { header: batch.header.clone(), transactions: txs }]);
                }
            }
        }
    }
}

fn shard_worker_loop(shard_id: usize, ledger: Arc<GeometricLedger>, batch_rx: std::sync::mpsc::Receiver<Vec<pos::Batch>>, total: Arc<AtomicU64>) {
    use std::collections::HashMap;
    use pos::Account;
    while let Ok(batches) = batch_rx.recv() {
        let mut cache = HashMap::new();
        let mut count = 0;
        for batch in batches {
            for ptx in batch.transactions {
                if let pos::TransactionPayload::Transfer { recipient, amount, .. } = ptx.tx.payload {
                    let sender = ptx.tx.sender;
                    let mut s_acc = cache.remove(&sender).unwrap_or_else(|| ledger.get(&sender).unwrap_or(Account{pubkey:sender, ..Default::default()}));
                    if s_acc.balance >= amount {
                        s_acc.balance -= amount;
                        cache.insert(sender, s_acc);
                        let mut r_acc = cache.remove(&recipient).unwrap_or_else(|| ledger.get(&recipient).unwrap_or(Account{pubkey:recipient, ..Default::default()}));
                        r_acc.balance += amount;
                        cache.insert(recipient, r_acc);
                        count += 1;
                    } else {
                        cache.insert(sender, s_acc); // Return to cache
                    }
                }
            }
        }
        if !cache.is_empty() {
            let updates: Vec<_> = cache.into_iter().collect();
            let _ = ledger.update_batch(&updates);
            total.fetch_add(count, Ordering::Relaxed);
        }
    }
}