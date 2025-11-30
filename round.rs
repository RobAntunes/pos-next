use std::time::{Duration, Instant};
use parking_lot::RwLock;
use dashmap::DashMap;
use blake3::Hash;
use std::sync::Arc;
use tokio::time;

#[derive(Debug, Clone)]
pub struct RoundConfig {
    /// Target transactions per second
    pub target_tps: u32,
    /// Number of transactions required to complete a round
    pub transactions_per_round: u32,
    /// Maximum time a round can last (in seconds)
    pub max_round_duration: u32,
    /// Minimum time a round must last (in seconds)
    pub min_round_duration: u32,
}

impl Default for RoundConfig {
    fn default() -> Self {
        Self {
            target_tps: 10_000,
            transactions_per_round: 50_000,
            max_round_duration: 30,
            min_round_duration: 10,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RoundMetrics {
    pub start_time: Instant,
    pub transaction_count: u32,
    pub current_tps: f64,
    pub peak_tps: f64,
    pub average_tps: f64,
    pub round_number: u64,
}

pub struct RoundManager {
    config: RoundConfig,
    current_round: Arc<RwLock<RoundMetrics>>,
    transactions: Arc<DashMap<Hash, Instant>>,
    round_seed: Arc<RwLock<Hash>>,
}

impl RoundManager {
    pub fn new(config: RoundConfig) -> Self {
        let metrics = RoundMetrics {
            start_time: Instant::now(),
            transaction_count: 0,
            current_tps: 0.0,
            peak_tps: 0.0,
            average_tps: 0.0,
            round_number: 0,
        };

        let seed = blake3::hash(b"initial-round");

        Self {
            config,
            current_round: Arc::new(RwLock::new(metrics)),
            transactions: Arc::new(DashMap::new()),
            round_seed: Arc::new(RwLock::new(seed)),
        }
    }

    pub fn get_current_metrics(&self) -> RoundMetrics {
        *self.current_round.read()
    }

    pub fn get_round_seed(&self) -> Hash {
        *self.round_seed.read()
    }

    pub fn record_transaction(&self, tx_hash: Hash) -> bool {
        let now = Instant::now();
        self.transactions.insert(tx_hash, now);

        let mut round = self.current_round.write();
        round.transaction_count += 1;

        let elapsed = now.duration_since(round.start_time).as_secs_f64();
        round.current_tps = round.transaction_count as f64 / elapsed;
        round.peak_tps = round.peak_tps.max(round.current_tps);
        round.average_tps = round.transaction_count as f64 / elapsed;

        round.transaction_count >= self.config.transactions_per_round
    }

    pub async fn start_round_monitor(self: Arc<Self>) {
        let metrics_interval = Duration::from_millis(100);
        let mut interval = time::interval(metrics_interval);

        loop {
            interval.tick().await;
            
            let now = Instant::now();
            let round = self.current_round.read();
            let elapsed = now.duration_since(round.start_time);

            // Check if round should end
            if round.transaction_count >= self.config.transactions_per_round 
                || elapsed.as_secs() >= self.config.max_round_duration as u64 {
                if elapsed.as_secs() >= self.config.min_round_duration as u64 {
                    drop(round);
                    self.finalize_round().await;
                }
            }

            // Clean up old transactions
            self.transactions.retain(|_, timestamp| {
                now.duration_since(*timestamp) < Duration::from_secs(self.config.max_round_duration as u64)
            });
        }
    }

    async fn finalize_round(&self) {
        let mut round = self.current_round.write();
        let elapsed = Instant::now().duration_since(round.start_time);

        // Log round statistics
        tracing::info!(
            round = round.round_number,
            transactions = round.transaction_count,
            duration_secs = elapsed.as_secs(),
            average_tps = round.average_tps,
            peak_tps = round.peak_tps,
            "Round completed"
        );

        // Generate new round seed
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.round_seed.read().as_bytes());
        hasher.update(&round.round_number.to_le_bytes());
        *self.round_seed.write() = hasher.finalize();

        // Reset metrics for new round
        *round = RoundMetrics {
            start_time: Instant::now(),
            transaction_count: 0,
            current_tps: 0.0,
            peak_tps: 0.0,
            average_tps: 0.0,
            round_number: round.round_number + 1,
        };

        // Clear old transactions
        self.transactions.clear();
    }
}
