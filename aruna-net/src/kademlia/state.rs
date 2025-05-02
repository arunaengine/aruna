use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};
use parking_lot::RwLock;

use iroh::{NodeAddr, NodeId};
use tracing::warn;

use crate::K_BUCKET_SIZE;

use super::{
    k_bucket::KBucket,
    kademlia::KEY_TTL,
    node_info::NodeInfo,
    time_handler::TimeHandler,
    utils::{calculate_distance, get_bucket_index},
};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct KademliaValue {
    pub node_id: NodeId,
    signature: Option<Vec<u8>>,
}

/// Internal mutable state of Kademlia
#[derive(Debug)]
struct KademliaState {
    // Our node address, should be set at startup
    node_addr: NodeAddr,
    // Kademlia routing table with K buckets
    k_buckets: [KBucket; 256],
    // Resources stored by us and the nodes that have them
    resources: HashMap<[u8; 32], HashSet<KademliaValue>>,
    // Locally known node addresses
    node_addresses: HashMap<NodeId, NodeAddr>,
    local_resources: TimeHandler, // Tracking stored resources by us
    store_timer: TimeHandler,     // Tracking all stored resources
}

impl KademliaState {
    fn new(node_id: NodeId) -> Self {
        let initial_addr = NodeAddr::from(node_id);

        Self {
            node_addr: initial_addr,
            k_buckets: std::array::from_fn(|_| KBucket::new()),
            resources: HashMap::new(),
            node_addresses: HashMap::new(),
            local_resources: TimeHandler::new(),
            store_timer: TimeHandler::new(),
        }
    }

    // Helper method to clean up expired resources
    fn prune_expired_resources(&mut self) -> usize {
        let Some(old) = SystemTime::now().checked_sub(KEY_TTL) else {
            return 0;
        };
        let all = self.store_timer.remove_older_than(old);
        for key in all.iter() {
            // Remove the key from the resources map
            if let Some(entries) = self.resources.get_mut(&key.key()) {
                let Some(node_id) = key.node_id() else {
                    continue;
                };
                entries.remove(&KademliaValue { node_id, signature: None });
                if entries.is_empty() {
                    self.resources.remove(&key.key());
                }
            }
        }
        all.len()
    }
}

#[derive(Clone, Debug)]
pub struct KademliaStateHandler {
    state: Arc<RwLock<KademliaState>>,
}

impl KademliaStateHandler {
    pub fn new(node_id: NodeId) -> Self {
        let state = KademliaState::new(node_id);
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub fn set_node_addr(&self, node_addr: NodeAddr) {
        let mut state = self.state.write();
        state.node_addr = node_addr.clone();
        state
            .node_addresses
            .insert(node_addr.node_id, node_addr.clone());
    }

    pub fn get_node_addr(&self) -> NodeAddr {
        self.state.read().node_addr.clone()
    }

    pub fn _get_k_buckets(&self) -> [KBucket; 256] {
        self.state.read().k_buckets.clone()
    }

    pub fn _get_resources(&self) -> HashMap<[u8; 32], HashSet<KademliaValue>> {
        self.state.read().resources.clone()
    }

    pub fn _get_node_addresses(&self) -> HashMap<NodeId, NodeAddr> {
        self.state
            .read()
            .node_addresses
            .clone()
    }

    pub fn get_stale_nodes(&self) -> Vec<Vec<NodeAddr>> {
        let state = self.state.read();
        let mut stale_nodes = Vec::new();
        // Remove stale nodes from all buckets
        for bucket in state.k_buckets.iter() {
            let potential_stale = bucket.get_stale_nodes();
            stale_nodes.push(potential_stale)
        }
        stale_nodes
    }

    pub fn insert_node_addr(&self, node_addr: NodeAddr) {
        let mut state = self.state.write();
        state
            .node_addresses
            .insert(node_addr.node_id, node_addr.clone());
    }

    pub fn update_bucket(
        &self,
        bucket_idx: usize,
        node_info: NodeInfo,
    ) -> Option<(NodeAddr, usize)> {
        let mut state = self.state.write();
        state.k_buckets[bucket_idx].update(node_info.clone())
    }

    pub fn refresh_node(&self, bucket_idx: usize, node_idx: usize) {
        let mut state = self.state.write();
        state.k_buckets[bucket_idx].refresh_node(node_idx);
    }

    pub fn replace_node(&self, bucket_idx: usize, node_idx: usize, node_info: NodeInfo) {
        let mut state = self.state.write();
        state.k_buckets[bucket_idx].replace_node(node_idx, node_info);
    }

    pub fn remove_node(&self, bucket_idx: usize, node_id: &NodeId) {
        let mut state = self.state.write();
        state.k_buckets[bucket_idx].remove_node(node_id);
        state.node_addresses.remove(node_id);
    }

    pub fn find_node_k_idx(&self, bucket_idx: usize, node_id: &NodeId) -> Option<usize> {
        let state = self.state.read();
        state.k_buckets[bucket_idx].find_node(node_id)
    }

    pub fn prune_expired_resources(&self) -> usize {
        let mut state = self.state.write();
        state.prune_expired_resources()
    }

    pub fn get_republish_sources(
        &self,
        interval: Duration,
    ) -> Vec<([u8; 32], Option<Vec<u8>>)> {
        let mut state = self.state.write();
        let Some(republish_threshold) = SystemTime::now().checked_sub(interval) else {
            warn!("Failed to calculate republish threshold");
            return vec![];
        };

        state
            .local_resources
            .remove_older_than(republish_threshold)
            .into_iter()
            .map(|key| (key.key(), key.signature()))
            .collect::<Vec<_>>()
    }

    pub fn find_local_addr(&self, key: &[u8; 32]) -> Option<Vec<NodeAddr>> {
        let state = self.state.read();
        let mut values = Vec::new();

        if let Some(entries) = state.resources.get(key) {
            for KademliaValue { node_id, .. } in entries {
                if let Some(addr) = state.node_addresses.get(node_id) {
                    values.push(addr.clone());
                }
            }
        }

        if values.is_empty() {
            if let Some(addr) = state.node_addresses.get(key) {
                values.push(addr.clone());
                Some(values)
            } else {
                None
            }
        } else {
            Some(values)
        }
    }

    pub fn store(&self, key: [u8; 32], node_addr: &NodeAddr, signature: Option<Vec<u8>>) {
        let mut state = self.state.write();

        let value = KademliaValue {
            node_id: node_addr.node_id,
            signature: signature.clone(),
        };

        // Get or create entry for this key
        let entries = state.resources.entry(key).or_default();

        // Update the entry with new TTL
        entries.insert(value);

        // Insert the key into the local expiration timer
        state.store_timer.insert(key, Some(node_addr.node_id), signature);

        // Always update the node address mapping
        state
            .node_addresses
            .insert(node_addr.node_id, node_addr.clone());
    }

    /// Find the closest nodes to a target from our routing table
    pub fn find_closest_nodes(&self, target: &[u8; 32]) -> Vec<NodeAddr> {
        let state = self.state.read();
        let addr = &state.node_addr;
        let self_node_id_bytes = addr.node_id.as_bytes();

        let distance_to_target = calculate_distance(self_node_id_bytes, target);
        let bucket_idx = get_bucket_index(&distance_to_target);

        // Start with the exact bucket
        let mut candidates = Vec::new();
        candidates.extend(state.k_buckets[bucket_idx].get_nodes());

        // If we need more nodes, check adjacent buckets
        let mut i = 1;
        while candidates.len() < K_BUCKET_SIZE && i < 256 {
            // Check bucket before
            if bucket_idx >= i {
                candidates.extend(state.k_buckets[bucket_idx - i].get_nodes());
            }

            // Check bucket after
            if bucket_idx + i < state.k_buckets.len() {
                candidates.extend(state.k_buckets[bucket_idx + i].get_nodes());
            }

            i += 1;

            // Stop if we've checked all buckets or found enough nodes
            if candidates.len() >= K_BUCKET_SIZE
                || (i > bucket_idx && bucket_idx + i >= state.k_buckets.len())
            {
                break;
            }
        }

        // Calculate actual distances and sort
        if candidates.len() > K_BUCKET_SIZE {
            let mut with_distance: Vec<_> = candidates
                .into_iter()
                .map(|addr| {
                    let id_bytes = addr.node_id.as_bytes();
                    let dist = calculate_distance(id_bytes, target);
                    (addr, dist)
                })
                .collect();

            with_distance.sort_by(|a, b| a.1.cmp(&b.1));

            with_distance
                .into_iter()
                .take(K_BUCKET_SIZE)
                .map(|(addr, _)| addr)
                .collect()
        } else {
            candidates
        }
    }

    pub fn copy_addr_and_resources(
        &self,
    ) -> (
        HashMap<NodeId, NodeAddr>,
        HashMap<[u8; 32], HashSet<KademliaValue>>,
    ) {
        let state = self.state.read();
        (state.node_addresses.clone(), state.resources.clone())
    }
}
