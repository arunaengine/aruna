// net/src/dht/lookup.rs
use aruna_core::id::{DhtKeyId, NodeId};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

use super::client::DhtClient;
use super::kbucket::{K, PeerInfo};
use super::rpc::StoredValue;

/// Parallelism factor for lookups (alpha in Kademlia paper)
const ALPHA: usize = 3;

/// Maximum lookup iterations
const MAX_ITERATIONS: usize = 20;

/// XOR distance between two 32-byte values
#[inline]
fn xor_distance(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut result = [0u8; 32];
    for (i, byte) in result.iter_mut().enumerate() {
        *byte = a[i] ^ b[i];
    }
    result
}

/// A node with its distance to target for priority queue
#[derive(Clone)]
struct NodeWithDistance {
    node_id: NodeId,
    distance: [u8; 32],
}

impl Eq for NodeWithDistance {}

impl PartialEq for NodeWithDistance {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl Ord for NodeWithDistance {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap (closest first)
        other.distance.cmp(&self.distance)
    }
}

impl PartialOrd for NodeWithDistance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Perform iterative node lookup (FindNode) - target is raw bytes for XOR distance
pub async fn find_node(
    client: &DhtClient,
    initial_nodes: Vec<PeerInfo>,
    target: &[u8; 32],
) -> Vec<NodeId> {
    let mut queried: HashSet<[u8; 32]> = HashSet::new();
    let mut heap: BinaryHeap<NodeWithDistance> = BinaryHeap::new();

    // Initialize with closest known nodes
    for peer in initial_nodes {
        let distance = xor_distance(target, peer.node_id.as_bytes());
        heap.push(NodeWithDistance {
            node_id: peer.node_id,
            distance,
        });
    }

    for _ in 0..MAX_ITERATIONS {
        // Get up to ALPHA closest unqueried nodes
        let mut to_query = Vec::with_capacity(ALPHA);
        let mut temp = Vec::new();

        while let Some(nwd) = heap.pop() {
            if queried.contains(nwd.node_id.as_bytes()) {
                continue;
            }
            if to_query.len() < ALPHA {
                to_query.push(nwd);
            } else {
                temp.push(nwd);
                break;
            }
        }

        // Put remaining back
        for nwd in temp {
            heap.push(nwd);
        }

        if to_query.is_empty() {
            break; // No more nodes to query
        }

        // Query in parallel
        let mut handles = Vec::new();
        for nwd in &to_query {
            queried.insert(*nwd.node_id.as_bytes());
            let node_id = nwd.node_id;
            let target = *target;
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                client.find_node(&node_id, &target).await
            }));
        }

        // Collect results
        for handle in handles {
            if let Ok(Ok(nodes)) = handle.await {
                for node_id in nodes {
                    if !queried.contains(node_id.as_bytes()) {
                        let distance = xor_distance(target, node_id.as_bytes());
                        heap.push(NodeWithDistance { node_id, distance });
                    }
                }
            }
        }
    }

    // Return k closest
    let mut result: Vec<NodeWithDistance> = heap.into_iter().collect();
    result.sort();
    result.truncate(K);
    result.into_iter().map(|nwd| nwd.node_id).collect()
}

/// Perform iterative value lookup (GetValue)
pub async fn get_value(
    client: &DhtClient,
    initial_nodes: Vec<PeerInfo>,
    key: &DhtKeyId,
) -> (Vec<StoredValue>, Vec<NodeId>) {
    let mut queried: HashSet<[u8; 32]> = HashSet::new();
    let mut heap: BinaryHeap<NodeWithDistance> = BinaryHeap::new();
    let mut all_values: Vec<StoredValue> = Vec::new();

    // Target is key bytes for distance calculation
    let target = key.as_bytes();

    // Initialize
    for peer in initial_nodes {
        let distance = xor_distance(target, peer.node_id.as_bytes());
        heap.push(NodeWithDistance {
            node_id: peer.node_id,
            distance,
        });
    }

    for _ in 0..MAX_ITERATIONS {
        let mut to_query = Vec::with_capacity(ALPHA);
        let mut temp = Vec::new();

        while let Some(nwd) = heap.pop() {
            if queried.contains(nwd.node_id.as_bytes()) {
                continue;
            }
            if to_query.len() < ALPHA {
                to_query.push(nwd);
            } else {
                temp.push(nwd);
                break;
            }
        }

        for nwd in temp {
            heap.push(nwd);
        }

        if to_query.is_empty() {
            break;
        }

        // Query in parallel
        let mut handles = Vec::new();
        for nwd in &to_query {
            queried.insert(*nwd.node_id.as_bytes());
            let node_id = nwd.node_id;
            let key = *key;
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                client.get_value(&node_id, &key).await
            }));
        }

        for handle in handles {
            if let Ok(Ok((values, closer_nodes))) = handle.await {
                all_values.extend(values);

                for node_id in closer_nodes {
                    if !queried.contains(node_id.as_bytes()) {
                        let distance = xor_distance(target, node_id.as_bytes());
                        heap.push(NodeWithDistance { node_id, distance });
                    }
                }
            }
        }

        // If we found values, we can stop early
        if !all_values.is_empty() {
            break;
        }
    }

    let mut closest: Vec<NodeWithDistance> = heap.into_iter().collect();
    closest.sort();
    closest.truncate(K);

    (
        all_values,
        closest.into_iter().map(|nwd| nwd.node_id).collect(),
    )
}

/// Store value on k closest nodes
pub async fn put_value(
    client: &DhtClient,
    closest_nodes: Vec<NodeId>,
    key: &DhtKeyId,
    value: Vec<u8>,
    ttl_secs: u64,
    publisher: &NodeId,
    signature: Option<[u8; 64]>,
) -> usize {
    let mut handles = Vec::new();

    for node_id in closest_nodes.into_iter().take(K) {
        let key = *key;
        let value = value.clone();
        let publisher = *publisher;
        let client = client.clone();

        handles.push(tokio::spawn(async move {
            client
                .put_value(&node_id, &key, value, ttl_secs, &publisher, signature)
                .await
        }));
    }

    let mut success_count = 0;
    for handle in handles {
        if let Ok(Ok(())) = handle.await {
            success_count += 1;
        }
    }

    success_count
}
