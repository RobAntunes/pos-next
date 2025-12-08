//! Network Distributor - Layer 2 DHT Routing
//!
//! Completely decoupled from the consensus/sequencer layer.
//! Owns the RingRoutingTable and handles batch distribution.
//!
//! Flow:
//! 1. Pull batches from BatchQueue
//! 2. For each transaction in batch, determine routing via DHT
//! 3. Local transactions → Shard Workers
//! 4. Remote transactions → Forward via QUIC to target node
//!
//! Key Design:
//! - DHT updates (peer join/leave) happen here, NOT in sequencer
//! - Non-blocking: uses read-copy-update pattern for ring table
//! - Batches are processed atomically (all-local or needs-forwarding)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::batch_queue::BatchQueue;
use crate::ring::{calculate_ring_position, RingRoutingTable};
use crate::types::Batch;

/// Network Distributor configuration
#[derive(Clone, Debug)]
pub struct DistributorConfig {
    /// This node's ID (for DHT responsibility checks)
    pub node_id: [u8; 32],
    /// Number of virtual nodes per physical node
    pub virtual_nodes: usize,
}

impl Default for DistributorConfig {
    fn default() -> Self {
        Self {
            node_id: [0u8; 32],
            virtual_nodes: 200,
        }
    }
}

/// Routing decision for a batch
#[derive(Debug, Clone)]
pub enum RoutingDecision {
    /// All transactions belong to this node - process locally
    Local,
    /// All transactions belong to a single remote node
    RemoteSingle {
        target_node: [u8; 32],
    },
    /// Transactions need to be split across multiple nodes
    Split {
        /// (node_id, transaction indices)
        routing_map: Vec<([u8; 32], Vec<usize>)>,
    },
}

/// Network Distributor - owns DHT routing logic
/// 
/// This is the ONLY component that accesses RingRoutingTable.
/// Sequencer and other consensus components have no knowledge of DHT.
pub struct NetworkDistributor {
    config: DistributorConfig,
    /// DHT ring routing table (owned here, not in sequencer)
    ring_table: RwLock<RingRoutingTable>,
    /// Batch queue to pull from
    batch_queue: Arc<BatchQueue>,
    /// Statistics
    batches_routed_local: AtomicU64,
    batches_routed_remote: AtomicU64,
    batches_split: AtomicU64,
}

impl NetworkDistributor {
    /// Create a new network distributor
    pub fn new(config: DistributorConfig, batch_queue: Arc<BatchQueue>) -> Self {
        let mut ring_table = RingRoutingTable::new();
        // Add ourselves to the ring
        ring_table.add_node(config.node_id);

        Self {
            config,
            ring_table: RwLock::new(ring_table),
            batch_queue,
            batches_routed_local: AtomicU64::new(0),
            batches_routed_remote: AtomicU64::new(0),
            batches_split: AtomicU64::new(0),
        }
    }

    /// Add a peer node to the DHT ring
    /// Called when a new peer is discovered via mDNS or other discovery
    pub fn add_peer(&self, node_id: [u8; 32]) {
        let mut table = self.ring_table.write();
        table.add_node(node_id);
    }

    /// Remove a peer from the DHT ring
    /// Called when a peer disconnects or times out
    pub fn remove_peer(&self, node_id: &[u8; 32]) {
        let mut table = self.ring_table.write();
        table.remove_node(node_id);
    }

    /// Get the number of nodes in the ring
    pub fn node_count(&self) -> usize {
        self.ring_table.read().node_count()
    }

    /// Get the total virtual node count
    pub fn ring_size(&self) -> usize {
        self.ring_table.read().size()
    }

    /// Pull and route a batch from the queue
    /// Returns None if queue is empty, otherwise returns the batch and routing decision
    pub fn pull_and_route(&self) -> Option<(Arc<Batch>, RoutingDecision)> {
        let batch = self.batch_queue.pull()?;
        let decision = self.route_batch(&batch);
        
        // Update stats
        match &decision {
            RoutingDecision::Local => {
                self.batches_routed_local.fetch_add(1, Ordering::Relaxed);
            }
            RoutingDecision::RemoteSingle { .. } => {
                self.batches_routed_remote.fetch_add(1, Ordering::Relaxed);
            }
            RoutingDecision::Split { .. } => {
                self.batches_split.fetch_add(1, Ordering::Relaxed);
            }
        }

        Some((batch, decision))
    }

    /// Determine routing for a batch
    /// 
    /// In single-node mode or when all txs hash to our range: Local
    /// In multi-node mode: checks each transaction's position
    pub fn route_batch(&self, batch: &Batch) -> RoutingDecision {
        let table = self.ring_table.read();
        
        // Fast path: single node (just us) - everything is local
        if table.node_count() <= 1 {
            return RoutingDecision::Local;
        }

        let my_node_id = self.config.node_id;
        let mut all_local = true;
        let mut single_remote: Option<[u8; 32]> = None;
        let mut needs_split = false;

        // Check each transaction's routing
        for (idx, ptx) in batch.transactions.iter().enumerate() {
            let tx_position = ptx.ring_info.tx_position;
            let owner = table.route(tx_position).unwrap_or(my_node_id);

            if owner == my_node_id {
                // Local
                if single_remote.is_some() {
                    needs_split = true;
                    break;
                }
            } else {
                // Remote
                all_local = false;
                match single_remote {
                    None => single_remote = Some(owner),
                    Some(existing) if existing != owner => {
                        needs_split = true;
                        break;
                    }
                    _ => {}
                }
            }
        }

        if all_local {
            return RoutingDecision::Local;
        }

        if !needs_split {
            if let Some(target) = single_remote {
                if all_local {
                    return RoutingDecision::Local;
                }
                // Check if ANY are local
                let has_local = batch.transactions.iter().any(|ptx| {
                    let owner = table.route(ptx.ring_info.tx_position).unwrap_or(my_node_id);
                    owner == my_node_id
                });
                if !has_local {
                    return RoutingDecision::RemoteSingle { target_node: target };
                }
            }
        }

        // Need to build full routing map
        self.build_routing_map(batch, &table)
    }

    /// Build detailed routing map for a batch that needs splitting
    fn build_routing_map(&self, batch: &Batch, table: &RingRoutingTable) -> RoutingDecision {
        use std::collections::HashMap;
        
        let my_node_id = self.config.node_id;
        let mut map: HashMap<[u8; 32], Vec<usize>> = HashMap::new();

        for (idx, ptx) in batch.transactions.iter().enumerate() {
            let tx_position = ptx.ring_info.tx_position;
            let owner = table.route(tx_position).unwrap_or(my_node_id);
            map.entry(owner).or_default().push(idx);
        }

        // Convert to Vec for easier use
        let routing_map: Vec<_> = map.into_iter().collect();

        // If everything ended up going to one node, simplify
        if routing_map.len() == 1 {
            let (node, _) = &routing_map[0];
            if *node == my_node_id {
                return RoutingDecision::Local;
            } else {
                return RoutingDecision::RemoteSingle { target_node: *node };
            }
        }

        RoutingDecision::Split { routing_map }
    }

    /// Check if a specific transaction position belongs to this node
    pub fn is_responsible(&self, tx_position: u64) -> bool {
        let table = self.ring_table.read();
        table.is_responsible(&self.config.node_id, tx_position)
    }

    /// Get statistics
    pub fn stats(&self) -> DistributorStats {
        DistributorStats {
            batches_routed_local: self.batches_routed_local.load(Ordering::Relaxed),
            batches_routed_remote: self.batches_routed_remote.load(Ordering::Relaxed),
            batches_split: self.batches_split.load(Ordering::Relaxed),
            ring_node_count: self.ring_table.read().node_count(),
            ring_vnode_count: self.ring_table.read().size(),
        }
    }

    /// Get the batch queue reference (for external monitoring)
    pub fn batch_queue(&self) -> &Arc<BatchQueue> {
        &self.batch_queue
    }
}

/// Statistics for the network distributor
#[derive(Debug, Clone)]
pub struct DistributorStats {
    pub batches_routed_local: u64,
    pub batches_routed_remote: u64,
    pub batches_split: u64,
    pub ring_node_count: usize,
    pub ring_vnode_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BatchHeader, ProcessedTransaction, Transaction, TransactionPayload, RingInfo, SignatureType};
    use crate::ring::calculate_ring_position;

    fn make_test_batch(tx_count: usize) -> Batch {
        let transactions: Vec<ProcessedTransaction> = (0..tx_count)
            .map(|i| {
                let tx = Transaction::new(
                    [i as u8; 32],
                    TransactionPayload::Transfer {
                        recipient: [2u8; 32],
                        amount: 100,
                        nonce: i as u64,
                    },
                    SignatureType::Ed25519([0u8; 64]),
                    i as u64,
                );
                let tx_hash = tx.hash();
                let tx_position = calculate_ring_position(&tx_hash);
                ProcessedTransaction {
                    tx,
                    ring_info: RingInfo {
                        tx_position,
                        node_position: Some(0),
                        distance: None,
                    },
                    tx_hash,
                }
            })
            .collect();

        Batch {
            header: BatchHeader {
                sequencer_id: [0u8; 32],
                round_id: 0,
                structure_root: blake3::hash(b"test"),
                set_xor: blake3::hash(b"xor"),
                tx_count: tx_count as u32,
                signature: [0u8; 64],
            },
            transactions,
        }
    }

    #[test]
    fn test_single_node_always_local() {
        let batch_queue = Arc::new(BatchQueue::new(10));
        let config = DistributorConfig {
            node_id: [1u8; 32],
            ..Default::default()
        };
        let distributor = NetworkDistributor::new(config, batch_queue.clone());

        let batch = make_test_batch(10);
        let decision = distributor.route_batch(&batch);

        assert!(matches!(decision, RoutingDecision::Local));
    }

    #[test]
    fn test_multi_node_routing() {
        let batch_queue = Arc::new(BatchQueue::new(10));
        let config = DistributorConfig {
            node_id: [1u8; 32],
            ..Default::default()
        };
        let distributor = NetworkDistributor::new(config, batch_queue.clone());

        // Add another node
        let other_node = [2u8; 32];
        distributor.add_peer(other_node);

        assert_eq!(distributor.node_count(), 2);

        // With 2 nodes and virtual nodes, some txs should route elsewhere
        let batch = make_test_batch(100);
        let decision = distributor.route_batch(&batch);

        // Should either be Local, RemoteSingle, or Split depending on hash distribution
        match decision {
            RoutingDecision::Local => println!("All local"),
            RoutingDecision::RemoteSingle { target_node } => {
                println!("All remote to {:?}", &target_node[..4]);
            }
            RoutingDecision::Split { routing_map } => {
                println!("Split across {} nodes", routing_map.len());
            }
        }
    }

    #[test]
    fn test_add_remove_peer() {
        let batch_queue = Arc::new(BatchQueue::new(10));
        let config = DistributorConfig::default();
        let distributor = NetworkDistributor::new(config, batch_queue);

        assert_eq!(distributor.node_count(), 1);

        distributor.add_peer([2u8; 32]);
        assert_eq!(distributor.node_count(), 2);

        distributor.remove_peer(&[2u8; 32]);
        assert_eq!(distributor.node_count(), 1);
    }
}
