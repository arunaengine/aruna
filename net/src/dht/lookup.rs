// net/src/dht/lookup.rs
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::util::xor_distance_32;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use super::client::DhtClient;
use super::kbucket::{K, PeerInfo};
use super::rpc::StoredValue;

/// Parallelism factor for lookups (alpha in Kademlia paper)
const ALPHA: usize = 3;

/// Maximum lookup iterations
const MAX_ITERATIONS: usize = 20;

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
    let mut candidates: HashMap<[u8; 32], NodeWithDistance> = HashMap::new();

    // Initialize with closest known nodes
    for peer in initial_nodes {
        let node = NodeWithDistance {
            node_id: peer.node_id,
            distance: xor_distance_32(target, peer.node_id.as_bytes()),
        };
        upsert_candidate(&mut candidates, &mut heap, node);
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
                    let node = NodeWithDistance {
                        node_id,
                        distance: xor_distance_32(target, node_id.as_bytes()),
                    };
                    upsert_candidate(&mut candidates, &mut heap, node);
                }
            }
        }
    }

    // Return k closest
    let mut result: Vec<NodeWithDistance> = candidates.into_values().collect();
    sort_by_closest(&mut result);
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
    let mut candidates: HashMap<[u8; 32], NodeWithDistance> = HashMap::new();
    let mut all_values: Vec<StoredValue> = Vec::new();

    // Target is key bytes for distance calculation
    let target = key.as_bytes();

    // Initialize
    for peer in initial_nodes {
        let node = NodeWithDistance {
            node_id: peer.node_id,
            distance: xor_distance_32(target, peer.node_id.as_bytes()),
        };
        upsert_candidate(&mut candidates, &mut heap, node);
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
                    let node = NodeWithDistance {
                        node_id,
                        distance: xor_distance_32(target, node_id.as_bytes()),
                    };
                    upsert_candidate(&mut candidates, &mut heap, node);
                }
            }
        }

        // If we found values, we can stop early
        if !all_values.is_empty() {
            break;
        }
    }

    let mut closest: Vec<NodeWithDistance> = candidates.into_values().collect();
    sort_by_closest(&mut closest);
    closest.truncate(K);

    (
        all_values,
        closest.into_iter().map(|nwd| nwd.node_id).collect(),
    )
}

fn upsert_candidate(
    candidates: &mut HashMap<[u8; 32], NodeWithDistance>,
    heap: &mut BinaryHeap<NodeWithDistance>,
    node: NodeWithDistance,
) {
    let key = *node.node_id.as_bytes();
    if let std::collections::hash_map::Entry::Vacant(entry) = candidates.entry(key) {
        entry.insert(node.clone());
        heap.push(node);
    }
}

#[inline]
fn sort_by_closest(nodes: &mut [NodeWithDistance]) {
    nodes.sort_by(|a, b| a.distance.cmp(&b.distance));
}

/// Store value on k closest nodes
pub async fn put_value(
    client: &DhtClient,
    closest_nodes: Vec<NodeId>,
    key: &DhtKeyId,
    value: Vec<u8>,
    ttl_secs: u64,
    publisher: &NodeId,
    signature: Option<iroh::Signature>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    #[test]
    fn test_binary_heap_pops_closest_first() {
        let target = [0u8; 32];
        let mut heap = BinaryHeap::new();
        let near = make_node(1);
        let far = make_node(200);

        heap.push(NodeWithDistance {
            node_id: far,
            distance: xor_distance_32(&target, far.as_bytes()),
        });
        heap.push(NodeWithDistance {
            node_id: near,
            distance: xor_distance_32(&target, near.as_bytes()),
        });

        let first = heap.pop().expect("heap has first");
        let second = heap.pop().expect("heap has second");
        assert!(first.distance <= second.distance);
    }

    #[test]
    fn test_sort_by_closest_uses_ascending_distance() {
        let mut nodes = vec![
            NodeWithDistance {
                node_id: make_node(1),
                distance: [3u8; 32],
            },
            NodeWithDistance {
                node_id: make_node(2),
                distance: [1u8; 32],
            },
            NodeWithDistance {
                node_id: make_node(3),
                distance: [2u8; 32],
            },
        ];

        sort_by_closest(&mut nodes);

        assert_eq!(nodes[0].distance, [1u8; 32]);
        assert_eq!(nodes[1].distance, [2u8; 32]);
        assert_eq!(nodes[2].distance, [3u8; 32]);
    }
}
