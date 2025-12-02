//! Smart Client - Ring-Aware Load Tester
//!
//! This client demonstrates client-side routing by calculating which shard
//! owns each transaction and sending it directly to the correct node.
//!
//! Modes:
//! - Smart Mode (default): Routes transactions to the correct shard
//! - Dumb Mode (--dumb): Sends all transactions to Shard 0 (forces bridging)

use clap::Parser;
use pos::{
    calculate_ring_position,
    messages::{deserialize_message, serialize_message, SerializableTransaction, WireMessage, RedirectReason},
    SignatureType, Transaction, TransactionPayload,
};
use quinn::{ClientConfig, Endpoint};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};

#[derive(Parser)]
#[command(author, version, about = "Smart Client - Ring-Aware Load Tester")]
struct Args {
    /// Shard 0 address
    #[arg(long, default_value = "127.0.0.1:9000")]
    shard_0: String,

    /// Shard 1 address
    #[arg(long, default_value = "127.0.0.1:9001")]
    shard_1: String,

    /// Number of transactions to send
    #[arg(long, default_value_t = 100_000)]
    count: u64,

    /// Dumb mode: send everything to Shard 0 (forces bridging)
    #[arg(long)]
    dumb: bool,

    /// Transactions per second limit (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    tps: u64,

    /// Start index offset (to avoid nonce collisions on persistent ledger)
    #[arg(long, default_value_t = 0)]
    offset: u64,

    /// Transactions per batch (per stream)
    #[arg(long, default_value_t = 45_000)]
    batch_size: usize,

    /// Max in-flight QUIC streams per connection
    #[arg(long, default_value_t = 8)]
    inflight: usize,

    /// Optional coordinated start time (unix epoch seconds)
    #[arg(long)]
    start_at: Option<u64>,

    /// Enable elastic per-machine scaling based on server mempool feedback
    #[arg(long, default_value_t = false)]
    elastic: bool,

    /// Elastic feedback polling interval (ms)
    #[arg(long, default_value_t = 500)]
    elastic_interval_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║           Smart Client - Ring-Aware Load Tester              ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();

    // Setup QUIC client
    let endpoint = create_client_endpoint()?;

    // Connect to both shards
    println!("🔗 Connecting to shards...");
    let addr0: SocketAddr = args
        .shard_0
        .to_socket_addrs()?
        .next()
        .ok_or("Failed to resolve shard_0 address")?;
    let addr1: SocketAddr = args
        .shard_1
        .to_socket_addrs()?
        .next()
        .ok_or("Failed to resolve shard_1 address")?;

    let conn0 = endpoint.connect(addr0, "localhost")?.await?;
    let conn1 = endpoint.connect(addr1, "localhost")?.await?;

    println!("✅ Connected to Shard 0: {}", addr0);
    println!("✅ Connected to Shard 1: {}", addr1);
    println!();

    // Optional coordinated start (for multi-host runs)
    if let Some(start_at) = args.start_at {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if start_at > now {
            let wait_s = start_at - now;
            println!("⏳ Waiting {}s for coordinated start...", wait_s);
            tokio::time::sleep(Duration::from_secs(wait_s)).await;
        }
    }

    // Multi-threaded load generation
    let num_threads = num_cpus::get();
    let tx_per_thread = args.count / num_threads as u64;

    println!(
        "🚀 Blasting {} transactions using {} threads...",
        args.count, num_threads
    );
    println!();

    let start = Instant::now();
    let delay_micros = Arc::new(AtomicU64::new(if args.tps > 0 {
        1_000_000 / args.tps
    } else {
        0
    }));

    // Start background poller for backpressure (optional elastic)
    let delay_clone = delay_micros.clone();
    let conn0_clone = conn0.clone();
    let conn1_clone = conn1.clone();
    if args.elastic {
        let poll_interval = Duration::from_millis(args.elastic_interval_ms);
        tokio::spawn(async move {
            // Simple feedback loop: AIMD based on pending queue growth
            let mut last_pending: u64 = 0;
            loop {
                tokio::time::sleep(poll_interval).await;
                // Query both shards; if one fails, continue
                let mut pending_sum = 0u64;
                let mut capacity_sum = 0u64;
                if let Ok((p, c)) = mempool_status(&conn0_clone).await { pending_sum += p; capacity_sum += c; }
                if let Ok((p, c)) = mempool_status(&conn1_clone).await { pending_sum += p; capacity_sum += c; }

                // If capacity is zero (shouldn't), skip
                if capacity_sum == 0 { continue; }

                // If pending is growing, slow down; if shrinking, speed up
                if pending_sum > last_pending {
                    // back off aggressively when queues grow
                    let current = delay_clone.load(Ordering::Relaxed);
                    let new_delay = if current == 0 { 50 } else { (current as f64 * 1.5) as u64 };
                    delay_clone.store(new_delay.min(20_000), Ordering::Relaxed);
                } else if pending_sum < last_pending.saturating_sub(10_000) {
                    // queues draining -> speed up
                    let current = delay_clone.load(Ordering::Relaxed);
                    if current > 0 {
                        delay_clone.store(current / 2, Ordering::Relaxed);
                    }
                }
                last_pending = pending_sum;
            }
        });
    }

    let total_sent = Arc::new(AtomicU64::new(0));
    let sent_0 = Arc::new(AtomicU64::new(0));
    let sent_1 = Arc::new(AtomicU64::new(0));

    // Per-connection inflight stream limit
    let sem0 = Arc::new(Semaphore::new(args.inflight));
    let sem1 = Arc::new(Semaphore::new(args.inflight));

    // Spawn monitor task
    let total_sent_monitor = total_sent.clone();
    let count = args.count;
    let start_monitor = start;
    tokio::spawn(async move {
        let mut last_reported = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let current = total_sent_monitor.load(Ordering::Relaxed);
            if current >= count {
                break;
            }

            let elapsed = start_monitor.elapsed().as_secs_f64();
            let tps = (current - last_reported) as f64 / 1.0;
            let total_tps = current as f64 / elapsed;

            println!(
                "  Progress: {}/{} ({:.1}% | {:.0} TPS | Avg: {:.0} TPS)",
                current,
                count,
                (current as f64 / count as f64) * 100.0,
                tps,
                total_tps
            );
            last_reported = current;
        }
    });

    let mut handles = Vec::new();

    for t in 0..num_threads {
        let conn0 = conn0.clone();
        let conn1 = conn1.clone();
        let delay_micros = delay_micros.clone();
        let total_sent = total_sent.clone();
        let sent_0 = sent_0.clone();
        let sent_1 = sent_1.clone();
        let sem0 = sem0.clone();
        let sem1 = sem1.clone();
        let dumb_mode = args.dumb;
        let offset = args.offset;
        let start_idx = t as u64 * tx_per_thread;
        let end_idx = if t == num_threads - 1 {
            args.count
        } else {
            start_idx + tx_per_thread
        };
        let tps_limit = args.tps / num_threads as u64;

        handles.push(tokio::spawn(async move {
            let batch_size = args.batch_size;
            let mut batch_0 = Vec::with_capacity(batch_size);
            let mut batch_1 = Vec::with_capacity(batch_size);

            // Local delay for this thread
            let thread_delay = if tps_limit > 0 {
                1_000_000 / tps_limit
            } else {
                0
            };

            for i in start_idx..end_idx {
                let global_i = i + offset;
                // Generate valid HashReveal credentials
                let secret_hash = blake3::hash(&global_i.to_le_bytes());
                let secret: [u8; 32] = *secret_hash.as_bytes();
                let sender_hash = blake3::hash(&secret);
                let sender: [u8; 32] = *sender_hash.as_bytes();

                let tx = Transaction::new_fast(
                    sender,
                    TransactionPayload::Transfer {
                        recipient: [(global_i % 256) as u8; 32],
                        amount: 1,
                        nonce: global_i,
                    },
                    global_i,
                    global_i, // timestamp
                    secret,
                );

                let tx_pos = calculate_ring_position(&tx.hash());
                let midpoint = u64::MAX / 2;

                if dumb_mode {
                    batch_0.push(tx);
                    sent_0.fetch_add(1, Ordering::Relaxed);
                } else if tx_pos < midpoint {
                    batch_0.push(tx);
                    sent_0.fetch_add(1, Ordering::Relaxed);
                } else {
                    batch_1.push(tx);
                    sent_1.fetch_add(1, Ordering::Relaxed);
                }

                if batch_0.len() >= batch_size {
                    if let Ok(permit) = sem0.clone().acquire_owned().await {
                        if let Err(e) = send_batch(&conn0, &batch_0).await {
                            eprintln!("Error sending batch to Shard 0: {}", e);
                        }
                        drop(permit);
                    }
                    batch_0.clear();
                }
                if batch_1.len() >= batch_size {
                    if let Ok(permit) = sem1.clone().acquire_owned().await {
                        if let Err(e) = send_batch(&conn1, &batch_1).await {
                            eprintln!("Error sending batch to Shard 1: {}", e);
                        }
                        drop(permit);
                    }
                    batch_1.clear();
                }

                total_sent.fetch_add(1, Ordering::Relaxed);

                // Rate limiting
                let global_delay = delay_micros.load(Ordering::Relaxed);
                let effective_delay = std::cmp::max(thread_delay, global_delay);

                if effective_delay > 0 && i % batch_size as u64 == 0 {
                    tokio::time::sleep(Duration::from_micros(effective_delay * batch_size as u64))
                        .await;
                }
            }

            // Flush remaining
            if !batch_0.is_empty() {
                if let Ok(permit) = sem0.clone().acquire_owned().await {
                    let _ = send_batch(&conn0, &batch_0).await;
                    drop(permit);
                }
            }
            if !batch_1.is_empty() {
                if let Ok(permit) = sem1.clone().acquire_owned().await {
                    let _ = send_batch(&conn1, &batch_1).await;
                    drop(permit);
                }
            }
        }));
    }

    // Wait for all threads
    for handle in handles {
        handle.await?;
    }

    let elapsed = start.elapsed();
    let total = total_sent.load(Ordering::Relaxed);
    let s0 = sent_0.load(Ordering::Relaxed);
    let s1 = sent_1.load(Ordering::Relaxed);
    let tps = total as f64 / elapsed.as_secs_f64();

    println!();
    println!("✅ Done!");
    println!(
        "   Sent to Shard 0: {} ({:.1}%)",
        s0,
        (s0 as f64 / total as f64) * 100.0
    );
    println!(
        "   Sent to Shard 1: {} ({:.1}%)",
        s1,
        (s1 as f64 / total as f64) * 100.0
    );
    println!("   Time: {:.2}s", elapsed.as_secs_f64());
    println!("   Average TPS: {:.0}", tps);
    println!();

    if args.dumb {
        println!("⚠️  DUMB MODE: All transactions sent to Shard 0");
    } else {
        println!("✨ SMART MODE: Transactions routed to correct shards");
    }

    Ok(())
}

/// Result of sending a batch, includes flow control info
#[derive(Debug)]
enum BatchResult {
    /// Batch accepted, with remaining credits
    Accepted { credits: u64, current_tps: u64 },
    /// Redirected to another node
    Redirected { suggested_node: String, reason: RedirectReason },
    /// No response (legacy compatibility)
    NoResponse,
}

async fn send_batch(
    conn: &quinn::Connection,
    txs: &[Transaction],
) -> Result<BatchResult, Box<dyn std::error::Error>> {
    let serializable_txs: Vec<SerializableTransaction> = txs
        .iter()
        .map(|tx| SerializableTransaction::from(tx.clone()))
        .collect();

    let msg = WireMessage::BatchSubmission {
        txs: serializable_txs,
    };
    let msg_bytes = serialize_message(&msg)?;

    // Open a single stream for the whole batch
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&msg_bytes).await?;
    send.finish().await?;

    // Read server response (BatchAccepted or BatchRedirect)
    match recv.read_to_end(64 * 1024).await {
        Ok(resp) if !resp.is_empty() => {
            match deserialize_message(&resp) {
                Ok(WireMessage::BatchAccepted { accepted_count: _, credits, current_tps }) => {
                    Ok(BatchResult::Accepted { credits, current_tps })
                }
                Ok(WireMessage::BatchRedirect { suggested_node, reason, .. }) => {
                    eprintln!("📡 Server redirected us to {} (reason: {:?})", suggested_node, reason);
                    Ok(BatchResult::Redirected { suggested_node, reason })
                }
                _ => Ok(BatchResult::NoResponse),
            }
        }
        _ => Ok(BatchResult::NoResponse),
    }
}

// Query mempool status from a connected shard
async fn mempool_status(conn: &quinn::Connection) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let msg = WireMessage::MempoolStatus;
    let bytes = serialize_message(&msg)?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&bytes).await?;
    send.finish().await?;

    let resp = recv.read_to_end(64 * 1024).await?;
    if let WireMessage::MempoolStatusResponse { pending_count, capacity_tps } = deserialize_message(&resp)? {
        Ok((pending_count, capacity_tps))
    } else {
        Err("unexpected response".into())
    }
}

fn create_client_endpoint() -> Result<Endpoint, Box<dyn std::error::Error>> {
    // Disable certificate verification for demo (DO NOT USE IN PRODUCTION)
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
        .with_no_client_auth();

    let mut client_config = ClientConfig::new(Arc::new(crypto));
    client_config.transport_config(Arc::new(create_transport_config()));

    let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}

fn create_transport_config() -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    config.max_concurrent_bidi_streams(1000u32.into());
    config.max_concurrent_uni_streams(1000u32.into());

    // Connection timeout settings
    config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
    config.keep_alive_interval(Some(Duration::from_secs(2)));

    config
}

/// Certificate verifier that accepts all certificates (for demo)
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
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

fn adjust_rate(delay_micros: &Arc<AtomicU64>, pending: u64, capacity: u64) {
    let current = delay_micros.load(Ordering::Relaxed);

    // Simple AIMD (Additive Increase, Multiplicative Decrease) style control
    // If pending > 50% capacity, slow down
    // If pending < 10% capacity, speed up

    let threshold_high = capacity / 2;
    let threshold_low = capacity / 10;

    if pending > threshold_high {
        // Congestion detected: Increase delay (slow down)
        let new_delay = if current == 0 { 10 } else { current * 2 };
        let capped_delay = new_delay.min(10_000); // Cap at 10ms (100 TPS)
        if capped_delay != current {
            delay_micros.store(capped_delay, Ordering::Relaxed);
            // println!("⚠️  Backpressure: Slowing down (Pending: {}, Delay: {}us)", pending, capped_delay);
        }
    } else if pending < threshold_low {
        // Capacity available: Decrease delay (speed up)
        if current > 0 {
            let new_delay = current / 2;
            delay_micros.store(new_delay, Ordering::Relaxed);
            // println!("🚀 Capacity available: Speeding up (Pending: {}, Delay: {}us)", pending, new_delay);
        }
    }
}
