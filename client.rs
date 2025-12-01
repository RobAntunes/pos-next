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
    calculate_ring_position, Transaction, TransactionPayload, SignatureType,
    messages::{serialize_message, WireMessage, SerializableTransaction},
};
use quinn::{ClientConfig, Endpoint};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    let addr0: SocketAddr = args.shard_0
        .to_socket_addrs()?
        .next()
        .ok_or("Failed to resolve shard_0 address")?;
    let addr1: SocketAddr = args.shard_1
        .to_socket_addrs()?
        .next()
        .ok_or("Failed to resolve shard_1 address")?;

    let conn0 = endpoint.connect(addr0, "localhost")?.await?;
    let conn1 = endpoint.connect(addr1, "localhost")?.await?;

    println!("✅ Connected to Shard 0: {}", addr0);
    println!("✅ Connected to Shard 1: {}", addr1);
    println!();

    // Run load test
    let mode = if args.dumb { "DUMB" } else { "SMART" };
    println!("🚀 Blasting {} transactions in {} mode...", args.count, mode);
    println!();

    let start = Instant::now();
    let mut sent_0 = 0u64;
    let mut sent_1 = 0u64;

    let delay_micros = if args.tps > 0 {
        1_000_000 / args.tps
    } else {
        0
    };

    for i in 0..args.count {
        // Create random transaction
        let tx = Transaction::new(
            [1u8; 32],
            TransactionPayload::Transfer {
                recipient: [(i % 256) as u8; 32],
                amount: 100,
                nonce: i,
            },
            SignatureType::Ed25519([0u8; 64]),
            i,
        );

        // Calculate ring position
        let tx_hash = tx.hash();
        let tx_pos = calculate_ring_position(&tx_hash);
        let midpoint = u64::MAX / 2;

        // Route to correct shard
        let target = if args.dumb {
            // Dumb mode: always send to Shard 0
            sent_0 += 1;
            &conn0
        } else if tx_pos < midpoint {
            // Smart mode: send to Shard 0
            sent_0 += 1;
            &conn0
        } else {
            // Smart mode: send to Shard 1
            sent_1 += 1;
            &conn1
        };

        // Send transaction
        let msg = WireMessage::TransactionSubmission {
            tx: SerializableTransaction::from(tx),
        };
        let msg_bytes = serialize_message(&msg)?;

        let (mut send, _recv) = target.open_bi().await?;
        send.write_all(&msg_bytes).await?;
        send.finish().await?;

        // Rate limit if requested
        if delay_micros > 0 {
            tokio::time::sleep(Duration::from_micros(delay_micros)).await;
        }

        // Progress report every 10k
        if (i + 1) % 10_000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let current_tps = (i + 1) as f64 / elapsed;
            println!(
                "  Progress: {}/{} ({:.1}% | {:.0} TPS)",
                i + 1,
                args.count,
                ((i + 1) as f64 / args.count as f64) * 100.0,
                current_tps
            );
        }
    }

    let elapsed = start.elapsed();
    let tps = args.count as f64 / elapsed.as_secs_f64();

    println!();
    println!("✅ Done!");
    println!("   Sent to Shard 0: {} ({:.1}%)", sent_0, (sent_0 as f64 / args.count as f64) * 100.0);
    println!("   Sent to Shard 1: {} ({:.1}%)", sent_1, (sent_1 as f64 / args.count as f64) * 100.0);
    println!("   Time: {:.2}s", elapsed.as_secs_f64());
    println!("   Average TPS: {:.0}", tps);
    println!();

    if args.dumb {
        println!("⚠️  DUMB MODE: All transactions sent to Shard 0");
        println!("   Shard 0 will bridge ~50% to Shard 1");
    } else {
        println!("✨ SMART MODE: Transactions routed to correct shards");
        println!("   Minimal bridging required");
    }

    Ok(())
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
