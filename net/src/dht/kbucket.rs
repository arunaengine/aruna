// net/src/dht/kbucket.rs
use aruna_core::id::{NodeId, NodeIdExt};
use aruna_core::util::unix_timestamp_secs;
use std::collections::VecDeque;

/// XOR distance between two 32-byte values
#[inline]
fn xor_distance(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut result = [0u8; 32];
    for (i, byte) in result.iter_mut().enumerate() {
        *byte = a[i] ^ b[i];
    }
    result
}

/// Maximum entries per bucket (standard Kademlia k value)
pub const K: usize = 20;

/// Maximum buckets (256 for 256-bit keys)
pub const NUM_BUCKETS: usize = 256;

/// Information about a known peer in the routing table.
/// Connection info is looked up via iroh discovery when needed.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: NodeId,
    pub last_seen: u64,
}

impl PeerInfo {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            last_seen: unix_timestamp_secs(),
        }
    }

    pub fn touch(&mut self) {
        self.last_seen = unix_timestamp_secs();
    }
}

/// A single k-bucket holding up to K peers
#[derive(Debug, Default)]
pub struct KBucket {
    /// Peers ordered by last-seen (oldest first, newest last)
    peers: VecDeque<PeerInfo>,
}

impl KBucket {
    pub fn new() -> Self {
        Self {
            peers: VecDeque::with_capacity(K),
        }
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.peers.len() >= K
    }

    /// Try to insert or update a peer. Returns the evicted peer if bucket was full
    /// and the oldest peer should be evicted (caller should ping it first).
    pub fn insert(&mut self, peer: PeerInfo) -> InsertResult {
        // Check if peer already exists
        if let Some(pos) = self.peers.iter().position(|p| p.node_id == peer.node_id) {
            // Move to back (most recently seen) and update
            let mut existing = self.peers.remove(pos).expect("position valid");
            existing.touch();
            self.peers.push_back(existing);
            return InsertResult::Updated;
        }

        // New peer
        if self.is_full() {
            // Return oldest for ping check
            InsertResult::BucketFull {
                oldest: self.peers.front().cloned(),
                pending: peer,
            }
        } else {
            self.peers.push_back(peer);
            InsertResult::Inserted
        }
    }

    /// Remove the oldest peer (used after ping failure)
    pub fn evict_oldest(&mut self) -> Option<PeerInfo> {
        self.peers.pop_front()
    }

    /// Get all peers in this bucket
    pub fn peers(&self) -> impl Iterator<Item = &PeerInfo> {
        self.peers.iter()
    }

    /// Check if a peer exists in this bucket
    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.peers.iter().any(|p| &p.node_id == node_id)
    }

    /// Remove a specific peer
    pub fn remove(&mut self, node_id: &NodeId) -> Option<PeerInfo> {
        if let Some(pos) = self.peers.iter().position(|p| &p.node_id == node_id) {
            self.peers.remove(pos)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum InsertResult {
    /// Peer was inserted
    Inserted,
    /// Existing peer was updated
    Updated,
    /// Bucket is full, need to ping oldest peer first
    BucketFull {
        oldest: Option<PeerInfo>,
        pending: PeerInfo,
    },
}

/// The full Kademlia routing table
#[derive(Debug)]
pub struct RoutingTable {
    local_id: NodeId,
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            local_id,
            buckets: (0..NUM_BUCKETS).map(|_| KBucket::new()).collect(),
        }
    }

    pub fn local_id(&self) -> &NodeId {
        &self.local_id
    }

    /// Get the bucket index for a given node
    fn bucket_index(&self, node_id: &NodeId) -> Option<usize> {
        let idx = self.local_id.bucket_index(node_id);
        if idx >= NUM_BUCKETS {
            None // Same as local node
        } else {
            Some(idx)
        }
    }

    /// Try to insert or update a peer in the routing table
    pub fn insert(&mut self, peer: PeerInfo) -> InsertResult {
        if peer.node_id == self.local_id {
            return InsertResult::Updated; // Ignore self
        }

        let Some(idx) = self.bucket_index(&peer.node_id) else {
            return InsertResult::Updated;
        };

        self.buckets[idx].insert(peer)
    }

    /// Remove a peer from the routing table
    pub fn remove(&mut self, node_id: &NodeId) -> Option<PeerInfo> {
        let idx = self.bucket_index(node_id)?;
        self.buckets[idx].remove(node_id)
    }

    /// Evict the oldest peer from a specific bucket
    pub fn evict_oldest(&mut self, bucket_idx: usize) -> Option<PeerInfo> {
        self.buckets
            .get_mut(bucket_idx)
            .and_then(|b| b.evict_oldest())
    }

    /// Find the k closest nodes to a target (raw bytes for XOR distance)
    pub fn closest(&self, target: &[u8; 32], count: usize) -> Vec<PeerInfo> {
        let mut all_peers: Vec<_> = self
            .buckets
            .iter()
            .flat_map(|b| b.peers())
            .cloned()
            .collect();

        // Sort by XOR distance to target
        all_peers.sort_by(|a, b| {
            let dist_a = xor_distance(target, a.node_id.as_bytes());
            let dist_b = xor_distance(target, b.node_id.as_bytes());
            dist_a.cmp(&dist_b)
        });

        all_peers.truncate(count);
        all_peers
    }

    /// Total number of peers in routing table
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(|b| b.is_empty())
    }

    /// Get all known peers
    pub fn all_peers(&self) -> Vec<PeerInfo> {
        self.buckets
            .iter()
            .flat_map(|b| b.peers())
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(seed: u8) -> NodeId {
        // Generate deterministic keys from seed
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        let secret = iroh::SecretKey::from_bytes(&seed_bytes);
        secret.public()
    }

    #[test]
    fn test_kbucket_insert() {
        let mut bucket = KBucket::new();
        let peer = PeerInfo::new(make_node(1));

        assert!(matches!(
            bucket.insert(peer.clone()),
            InsertResult::Inserted
        ));
        assert_eq!(bucket.len(), 1);

        // Update same peer
        assert!(matches!(bucket.insert(peer), InsertResult::Updated));
        assert_eq!(bucket.len(), 1);
    }

    #[test]
    fn test_kbucket_full() {
        let mut bucket = KBucket::new();

        // Fill the bucket
        for i in 0..K {
            let peer = PeerInfo::new(make_node(i as u8 + 1));
            bucket.insert(peer);
        }

        assert!(bucket.is_full());

        // Try to insert one more
        let new_peer = PeerInfo::new(make_node(100));
        let result = bucket.insert(new_peer);
        assert!(matches!(result, InsertResult::BucketFull { .. }));
    }

    #[test]
    fn test_routing_table_closest() {
        let local = make_node(0);
        let mut table = RoutingTable::new(local);

        // Insert some peers and save target node
        let target = make_node(5);
        for i in 1..10 {
            let peer = PeerInfo::new(make_node(i));
            table.insert(peer);
        }

        let closest = table.closest(target.as_bytes(), 3);

        assert_eq!(closest.len(), 3);
        // First should be closest to target (5)
        assert_eq!(closest[0].node_id, target);
    }
}
