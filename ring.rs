//! Consistent Hash Ring Implementation
//!
//! Replaces 2D geometric routing with a 1D consistent hash ring.
//! Benefits:
//! - Simpler math (u64 comparison vs sqrt calculations)
//! - Better load balancing with virtual nodes
//! - Faster routing decisions
//! - Easier network resizing

use blake3::Hash;
use std::collections::BTreeMap;
use std::convert::TryInto;

/// Position on the hash ring (0 to u64::MAX)
pub type RingPosition = u64;

/// Node identifier in the network
pub type NodeId = [u8; 32];

/// Calculate ring position from transaction hash
/// Uses first 8 bytes of BLAKE3 hash as u64
#[inline]
pub fn calculate_ring_position(tx_hash: &Hash) -> RingPosition {
    let bytes = tx_hash.as_bytes();
    u64::from_le_bytes(bytes[0..8].try_into().unwrap())
}

/// Calculate ring position from arbitrary data
#[inline]
pub fn calculate_position_from_bytes(data: &[u8; 32]) -> RingPosition {
    u64::from_le_bytes(data[0..8].try_into().unwrap())
}

/// Virtual node configuration for load balancing
#[derive(Debug, Clone)]
pub struct VirtualNodeConfig {
    /// Number of virtual nodes per physical node
    pub replicas: usize,
}

impl Default for VirtualNodeConfig {
    fn default() -> Self {
        Self {
            replicas: 200, // Default to 200 virtual nodes for good distribution
        }
    }
}

/// Consistent hash ring routing table
#[derive(Debug, Clone)]
pub struct RingRoutingTable {
    /// Sorted ring: position -> node_id
    /// BTreeMap maintains sorted order for efficient range queries
    ring: BTreeMap<RingPosition, NodeId>,
    /// Virtual node configuration
    vnode_config: VirtualNodeConfig,
}

impl RingRoutingTable {
    /// Create a new empty routing table
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
            vnode_config: VirtualNodeConfig::default(),
        }
    }

    /// Create with custom virtual node configuration
    pub fn with_config(vnode_config: VirtualNodeConfig) -> Self {
        Self {
            ring: BTreeMap::new(),
            vnode_config,
        }
    }

    /// Add a node to the ring with virtual nodes
    pub fn add_node(&mut self, node_id: NodeId) {
        for replica in 0..self.vnode_config.replicas {
            let vnode_position = self.calculate_vnode_position(&node_id, replica);
            self.ring.insert(vnode_position, node_id);
        }
    }

    /// Remove a node from the ring (all its virtual nodes)
    pub fn remove_node(&mut self, node_id: &NodeId) {
        // Remove all virtual nodes for this physical node
        self.ring.retain(|_, nid| nid != node_id);
    }

    /// Find the node responsible for a given ring position
    /// Returns None if ring is empty
    pub fn route(&self, position: RingPosition) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }

        // Find the first node whose position >= transaction position
        // If none exists (we're past the end), wrap around to the first node
        self.ring
            .range(position..)
            .next()
            .map(|(_, node_id)| *node_id)
            .or_else(|| {
                // Wrap around: take the first node in the ring
                self.ring.values().next().copied()
            })
    }

    /// Check if this node is responsible for a given position
    /// A node owns the range from its predecessor to itself
    pub fn is_responsible(&self, my_node_id: &NodeId, position: RingPosition) -> bool {
        if let Some(owner) = self.route(position) {
            owner == *my_node_id
        } else {
            false
        }
    }

    /// Get the range owned by a node (start, end)
    /// Returns None if node not found
    pub fn get_node_range(&self, node_id: &NodeId) -> Option<(RingPosition, RingPosition)> {
        // Find the first virtual node for this physical node
        let mut node_positions: Vec<RingPosition> = self.ring
            .iter()
            .filter(|(_, nid)| *nid == node_id)
            .map(|(pos, _)| *pos)
            .collect();

        if node_positions.is_empty() {
            return None;
        }

        node_positions.sort();

        // For consistent hashing, each vnode owns from predecessor to itself
        // We return the aggregate range (smallest to largest vnode position)
        let start = node_positions.first().copied()?;
        let end = node_positions.last().copied()?;

        Some((start, end))
    }

    /// Calculate virtual node position
    /// Uses BLAKE3(node_id || replica_index)
    fn calculate_vnode_position(&self, node_id: &NodeId, replica: usize) -> RingPosition {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"vnode:");
        hasher.update(node_id);
        hasher.update(&replica.to_le_bytes());
        let hash = hasher.finalize();
        calculate_ring_position(&hash)
    }

    /// Get all nodes in the ring (deduplicated)
    pub fn get_all_nodes(&self) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self.ring.values().copied().collect();
        nodes.sort();
        nodes.dedup();
        nodes
    }

    /// Get ring size (number of virtual nodes)
    pub fn size(&self) -> usize {
        self.ring.len()
    }

    /// Get number of physical nodes
    pub fn node_count(&self) -> usize {
        self.get_all_nodes().len()
    }

    /// Get the successor node positions for Chord-style routing
    /// Returns up to N successor positions for fault tolerance
    pub fn get_successors(&self, position: RingPosition, count: usize) -> Vec<(RingPosition, NodeId)> {
        let mut successors = Vec::new();

        // Get positions starting from the given position
        for (pos, node_id) in self.ring.range(position..).take(count) {
            successors.push((*pos, *node_id));
        }

        // If we didn't get enough, wrap around
        if successors.len() < count {
            let remaining = count - successors.len();
            for (pos, node_id) in self.ring.iter().take(remaining) {
                successors.push((*pos, *node_id));
            }
        }

        successors
    }
}

impl Default for RingRoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

// RingInfo is defined in types.rs to avoid duplication

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node_id(seed: u8) -> NodeId {
        let mut id = [0u8; 32];
        id[0] = seed;
        id
    }

    #[test]
    fn test_ring_position_calculation() {
        let hash = blake3::hash(b"test transaction");
        let pos = calculate_ring_position(&hash);

        // Position should be deterministic
        let pos2 = calculate_ring_position(&hash);
        assert_eq!(pos, pos2);
    }

    #[test]
    fn test_routing_table_basic() {
        let mut table = RingRoutingTable::new();
        let node1 = make_node_id(1);
        let node2 = make_node_id(2);

        table.add_node(node1);
        table.add_node(node2);

        // Should have virtual nodes
        assert!(table.size() > 2);

        // Should have 2 physical nodes
        assert_eq!(table.node_count(), 2);
    }

    #[test]
    fn test_routing() {
        let mut table = RingRoutingTable::new();
        let node1 = make_node_id(1);
        let node2 = make_node_id(2);

        table.add_node(node1);
        table.add_node(node2);

        // Any position should route to one of the nodes
        let position = 12345u64;
        let routed_node = table.route(position);
        assert!(routed_node.is_some());

        // Should be one of our nodes
        let routed = routed_node.unwrap();
        assert!(routed == node1 || routed == node2);
    }

    #[test]
    fn test_node_removal() {
        let mut table = RingRoutingTable::new();
        let node1 = make_node_id(1);
        let node2 = make_node_id(2);

        table.add_node(node1);
        table.add_node(node2);
        assert_eq!(table.node_count(), 2);

        table.remove_node(&node1);
        assert_eq!(table.node_count(), 1);

        // All positions should now route to node2
        let position = 12345u64;
        assert_eq!(table.route(position).unwrap(), node2);
    }

    #[test]
    fn test_ring_distance_calculation() {
        // Direct distance (no wraparound)
        let tx_pos = 1000u64;
        let node_pos = 500u64;

        let distance = if tx_pos >= node_pos {
            tx_pos - node_pos
        } else {
            (u64::MAX - node_pos) + tx_pos + 1
        };

        assert_eq!(distance, 500);
    }

    #[test]
    fn test_ring_wraparound_distance() {
        // Wraparound case
        let tx_pos = 100u64;
        let node_pos = 200u64;

        let distance = if tx_pos >= node_pos {
            tx_pos - node_pos
        } else {
            (u64::MAX - node_pos) + tx_pos + 1
        };

        // Distance should wrap around
        assert!(distance > tx_pos);
    }

    #[test]
    fn test_virtual_nodes_distribution() {
        let mut table = RingRoutingTable::with_config(VirtualNodeConfig { replicas: 10 });
        let node1 = make_node_id(1);

        table.add_node(node1);

        // Should have 10 virtual nodes
        assert_eq!(table.size(), 10);
    }

    #[test]
    fn test_successors() {
        let mut table = RingRoutingTable::with_config(VirtualNodeConfig { replicas: 5 });
        let node1 = make_node_id(1);
        let node2 = make_node_id(2);

        table.add_node(node1);
        table.add_node(node2);

        let successors = table.get_successors(0, 3);
        assert_eq!(successors.len(), 3);
    }
}
