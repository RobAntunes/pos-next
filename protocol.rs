use blake3::{Hash, Hasher};
use curve25519_dalek::{
    constants::ED25519_BASEPOINT_POINT,
    scalar::Scalar,
};
use rand::RngCore;
use std::convert::TryInto;
use std::time::Instant;
use uuid::Uuid;

use crate::metrics::NodeMetrics;

/// Core implementation of the Tangle Protocol
pub struct TangleProtocol {
    node_id: String,
    master_seed: [u8; 32],
    current_salt: [u8; 32],
    current_index: u64,
    previous_tx: Option<Hash>,
    previous_round: Option<Hash>,
    metrics: NodeMetrics,
}

#[derive(Debug)]
pub struct Commitment {
    pub commitment: Hash,
    pub data_binding: Hash,
    pub address_binding: Hash,
    pub state_reference: Hash,
    pub temporal_binding: Hash,
}

#[derive(Debug)]
pub struct GeometricPosition {
    pub tx_position: (u64, u64),
    pub node_position: Option<(u64, u64)>,
    pub distance: Option<u64>,
    pub theta: Option<u64>,
}

impl TangleProtocol {
    /// Create a new instance with optional master seed and initial salt
    pub fn new(master_seed: Option<[u8; 32]>, initial_salt: Option<[u8; 32]>) -> Self {
        let mut rng = rand::thread_rng();
        let master_seed = master_seed.unwrap_or_else(|| {
            let mut seed = [0u8; 32];
            rng.fill_bytes(&mut seed);
            seed
        });

        let current_salt = initial_salt.unwrap_or_else(|| {
            let mut salt = [0u8; 32];
            rng.fill_bytes(&mut salt);
            salt
        });

        let node_id = Uuid::new_v4().to_string();
        let metrics = NodeMetrics::new(&node_id, 60);

        Self {
            node_id,
            master_seed,
            current_salt,
            current_index: 0,
            previous_tx: None,
            previous_round: None,
            metrics,
        }
    }

    /// Get the node's ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get the node's metrics
    pub fn metrics(&self) -> NodeMetrics {
        self.metrics.clone()
    }

    /// Get a reference to the node's metrics
    pub fn metrics_ref(&mut self) -> &mut NodeMetrics {
        &mut self.metrics
    }

    /// Generate the root address (A₀) and its corresponding public key (P₀)
    pub fn generate_root_address(&self) -> (Hash, curve25519_dalek::EdwardsPoint) {
        let mut hasher = Hasher::new();
        hasher.update(&self.master_seed);
        hasher.update(&self.current_salt);
        let root_hash = hasher.finalize();

        let scalar_bytes = root_hash.as_bytes();
        let scalar = Scalar::from_bytes_mod_order(*scalar_bytes);
        let public_key = ED25519_BASEPOINT_POINT * scalar;

        (root_hash, public_key)
    }

    /// Generate the next evolutionary address (Aᵢ) and public key (Pᵢ)
    pub fn evolve_address(
        &mut self,
        prev_tx_hash: Hash,
        prev_round_hash: Hash,
    ) -> (Hash, curve25519_dalek::EdwardsPoint) {
        // Update state
        self.previous_tx = Some(prev_tx_hash);
        self.previous_round = Some(prev_round_hash);
        self.current_index += 1;

        // Generate next address
        let mut hasher = Hasher::new();
        hasher.update(&self.master_seed);
        hasher.update(&self.current_salt);
        hasher.update(&self.current_index.to_le_bytes());
        
        if let Some(prev_tx) = self.previous_tx {
            hasher.update(prev_tx.as_bytes());
        }
        
        if let Some(prev_round) = self.previous_round {
            hasher.update(prev_round.as_bytes());
        }

        let address_hash = hasher.finalize();

        let scalar_bytes = address_hash.as_bytes();
        let scalar = Scalar::from_bytes_mod_order(*scalar_bytes);
        let public_key = ED25519_BASEPOINT_POINT * scalar;

        (address_hash, public_key)
    }

    /// Create a transaction commitment with all required bindings
    pub fn create_commitment(&mut self, transaction_data: &[u8], current_round_seed: &[u8]) -> Commitment {
        let start = Instant::now();

        // Generate address binding
        let (address, _) = if self.previous_tx.is_none() {
            self.generate_root_address()
        } else {
            self.evolve_address(
                self.previous_tx.unwrap(),
                self.previous_round.unwrap(),
            )
        };

        // Create data binding
        let mut hasher = Hasher::new();
        hasher.update(transaction_data);
        let data_binding = hasher.finalize();

        // Create state reference
        let mut hasher = Hasher::new();
        if let Some(prev_tx) = self.previous_tx {
            hasher.update(prev_tx.as_bytes());
        }
        let state_reference = hasher.finalize();

        // Create temporal binding
        let mut hasher = Hasher::new();
        hasher.update(current_round_seed);
        let temporal_binding = hasher.finalize();

        // Create final commitment
        let mut hasher = Hasher::new();
        hasher.update(address.as_bytes());
        hasher.update(data_binding.as_bytes());
        hasher.update(state_reference.as_bytes());
        hasher.update(temporal_binding.as_bytes());
        let commitment = hasher.finalize();

        // Record metrics
        let elapsed = start.elapsed();
        self.metrics.record_batch(1, elapsed);

        Commitment {
            commitment,
            data_binding,
            address_binding: address,
            state_reference,
            temporal_binding,
        }
    }

    /// Create multiple transaction commitments in a batch (reuses create_commitment for DRY)
    pub fn create_commitment_batch(&mut self, transaction_data: &[u8], current_round_seed: &[u8], batch_size: usize) -> Vec<Commitment> {
        let start = Instant::now();
        let mut commitments = Vec::with_capacity(batch_size);
        
        // Temporarily disable per-commitment metrics recording
        for _ in 0..batch_size {
            commitments.push(self.create_commitment_internal(transaction_data, current_round_seed));
        }

        // Record metrics for the entire batch
        let elapsed = start.elapsed();
        self.metrics.record_batch(batch_size as u64, elapsed);

        commitments
    }

    /// Internal commitment creation without metrics (for batch use)
    fn create_commitment_internal(&mut self, transaction_data: &[u8], current_round_seed: &[u8]) -> Commitment {
        // Generate address binding
        let (address, _) = if self.previous_tx.is_none() {
            self.generate_root_address()
        } else {
            self.evolve_address(
                self.previous_tx.unwrap(),
                self.previous_round.unwrap(),
            )
        };

        // Create data binding
        let mut hasher = Hasher::new();
        hasher.update(transaction_data);
        let data_binding = hasher.finalize();

        // Create state reference
        let mut hasher = Hasher::new();
        if let Some(prev_tx) = self.previous_tx {
            hasher.update(prev_tx.as_bytes());
        }
        let state_reference = hasher.finalize();

        // Create temporal binding
        let mut hasher = Hasher::new();
        hasher.update(current_round_seed);
        let temporal_binding = hasher.finalize();

        // Create final commitment
        let mut hasher = Hasher::new();
        hasher.update(address.as_bytes());
        hasher.update(data_binding.as_bytes());
        hasher.update(state_reference.as_bytes());
        hasher.update(temporal_binding.as_bytes());
        let commitment = hasher.finalize();

        Commitment {
            commitment,
            data_binding,
            address_binding: address,
            state_reference,
            temporal_binding,
        }
    }

    /// Calculate geometric positions for transactions and nodes
    pub fn calculate_geometric_position(
        &self,
        commitment: &Hash,
        node_id: Option<&[u8]>,
    ) -> GeometricPosition {
        let mut hasher = Hasher::new();
        hasher.update(commitment.as_bytes());
        let position_hash = hasher.finalize();

        // Use first 16 bytes for transaction position
        let tx_bytes: [u8; 16] = position_hash.as_bytes()[0..16].try_into().unwrap();
        let tx_x = u64::from_le_bytes(tx_bytes[0..8].try_into().unwrap());
        let tx_y = u64::from_le_bytes(tx_bytes[8..16].try_into().unwrap());
        let tx_position = (tx_x, tx_y);

        let (node_position, distance, theta) = if let Some(node_id) = node_id {
            // Calculate node position
            let mut hasher = Hasher::new();
            hasher.update(node_id);
            let node_hash = hasher.finalize();
            
            let node_bytes: [u8; 16] = node_hash.as_bytes()[0..16].try_into().unwrap();
            let node_x = u64::from_le_bytes(node_bytes[0..8].try_into().unwrap());
            let node_y = u64::from_le_bytes(node_bytes[8..16].try_into().unwrap());
            
            // Calculate distance and angle using saturating arithmetic to prevent overflow
            let dx = if tx_x > node_x { tx_x - node_x } else { node_x - tx_x };
            let dy = if tx_y > node_y { tx_y - node_y } else { node_y - tx_y };
            // Use u128 for intermediate calculation to avoid overflow in squaring
            let dx_sq = (dx as u128).saturating_mul(dx as u128);
            let dy_sq = (dy as u128).saturating_mul(dy as u128);
            let sum = dx_sq.saturating_add(dy_sq);
            let distance = (sum as f64).sqrt() as u64;
            
            let theta = if dx == 0 {
                90 // dy is always non-negative after abs-style subtraction
            } else {
                ((dy as f64 / dx as f64).atan() * 180.0 / std::f64::consts::PI) as u64
            };

            (Some((node_x, node_y)), Some(distance), Some(theta))
        } else {
            (None, None, None)
        };

        GeometricPosition {
            tx_position,
            node_position,
            distance,
            theta,
        }
    }

    /// Validate a set of commitments using XOR aggregation (O(1) set validation)
    pub fn validate_commitment_set(&mut self, commitments: &[Hash], declared_xor: Option<&[u8; 32]>) -> bool {
        let start = Instant::now();
        
        // Compute XOR of all commitment hashes
        let mut computed_xor = [0u8; 32];
        for commitment in commitments {
            // Hash each commitment for set membership (per spec 5.2)
            let mut hasher = Hasher::new();
            hasher.update(b"tangle-set-hash");
            hasher.update(commitment.as_bytes());
            let h = hasher.finalize();
            
            // XOR into accumulator
            for (i, byte) in h.as_bytes().iter().enumerate() {
                computed_xor[i] ^= byte;
            }
        }

        let elapsed = start.elapsed();
        self.metrics.record_batch(commitments.len() as u64, elapsed);

        // If declared XOR provided, verify it matches
        if let Some(declared) = declared_xor {
            computed_xor == *declared
        } else {
            // Just verify commitments are non-zero
            commitments.iter().all(|c| !c.as_bytes().iter().all(|&x| x == 0))
        }
    }

    /// Compute XOR hash of a commitment set (for batch headers)
    pub fn compute_set_xor(commitments: &[Hash]) -> [u8; 32] {
        let mut xor = [0u8; 32];
        for commitment in commitments {
            let mut hasher = Hasher::new();
            hasher.update(b"tangle-set-hash");
            hasher.update(commitment.as_bytes());
            let h = hasher.finalize();
            
            for (i, byte) in h.as_bytes().iter().enumerate() {
                xor[i] ^= byte;
            }
        }
        xor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_address_generation() {
        let protocol = TangleProtocol::new(None, None);
        let (address, public_key) = protocol.generate_root_address();
        assert!(!address.as_bytes().iter().all(|&x| x == 0));
        assert!(public_key.is_torsion_free());
    }

    #[test]
    fn test_address_evolution() {
        let mut protocol = TangleProtocol::new(None, None);
        let prev_tx = blake3::hash(b"previous tx");
        let prev_round = blake3::hash(b"previous round");
        
        let (address, public_key) = protocol.evolve_address(prev_tx, prev_round);
        assert!(!address.as_bytes().iter().all(|&x| x == 0));
        assert!(public_key.is_torsion_free());
    }

    #[test]
    fn test_commitment_creation() {
        let mut protocol = TangleProtocol::new(None, None);
        let tx_data = b"test transaction";
        let round_seed = b"test round";
        
        let commitment = protocol.create_commitment(tx_data, round_seed);
        assert!(!commitment.commitment.as_bytes().iter().all(|&x| x == 0));
    }

    #[test]
    fn test_geometric_position() {
        let protocol = TangleProtocol::new(None, None);
        let commitment = blake3::hash(b"test commitment");
        let node_id = b"test node";
        
        let position = protocol.calculate_geometric_position(&commitment, Some(node_id));
        assert!(position.distance.is_some());
        assert!(position.theta.is_some());
    }

    #[test]
    fn test_commitment_set_validation() {
        let mut protocol = TangleProtocol::new(None, None);
        let commitments = vec![
            blake3::hash(b"commitment 1"),
            blake3::hash(b"commitment 2"),
            blake3::hash(b"commitment 3"),
        ];
        
        // Compute expected XOR
        let expected_xor = TangleProtocol::compute_set_xor(&commitments);
        
        // Validation should pass with correct XOR
        assert!(protocol.validate_commitment_set(&commitments, Some(&expected_xor)));
        
        // Validation should fail with wrong XOR
        let mut bad_xor = expected_xor;
        bad_xor[0] ^= 1;
        assert!(!protocol.validate_commitment_set(&commitments, Some(&bad_xor)));
    }

    #[test]
    fn test_geometric_distance_no_overflow() {
        let protocol = TangleProtocol::new(None, None);
        // Use a commitment that would produce extreme coordinates
        let commitment = blake3::hash(b"extreme position test");
        // This should not panic even with extreme node positions
        let _position = protocol.calculate_geometric_position(&commitment, Some(&[0xff; 32]));
    }
}
