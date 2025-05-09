use std::collections::BTreeMap;

use iroh::{NodeAddr, NodeId};

/// XOR distance calculation for Kademlia
///
/// Calculates the XOR distance between two 32-byte IDs
pub fn calculate_distance(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut result = [0u8; 32];
    for i in 0..32 {
        result[i] = a[i] ^ b[i]; // XOR for distance in Kademlia
    }
    result
}

/// Find the position of the first set bit in a distance
///
/// Returns the index of the first non-zero bit (0-255)
pub fn get_bucket_index(distance: &[u8; 32]) -> usize {
    // Find the index of the first non-zero bit in the distance
    for (i, _) in distance.iter().enumerate() {
        let byte = distance[i];
        if byte != 0 {
            // Find the position of the first set bit in this byte
            for j in 0..8 {
                if (byte & (1 << (7 - j))) != 0 {
                    return i * 8 + j;
                }
            }
        }
    }

    // If distance is 0 (same node), use last bucket
    255
}

// // Create a unified visualization enum for all types
// pub enum AllTypes {
//     NodeId(iroh::NodeId),
//     NodeAddr(iroh::NodeAddr),
//     Key([u8; 32]),
// }

// pub fn viz(input: impl Into<[u8; 32]>) -> String {
//     let input = input.into();
//     let mut result = String::new();
//     for byte in input.iter() {
//         result.push_str(&format!("{:02x}", byte));
//     }
//     result
// }

type Distance = [u8; 32];

pub struct Closest<const N: usize> {
    target: [u8; 32],
    nodes: Vec<(Distance, NodeAddr)>,
}

impl<const N: usize> Closest<N> {
    pub fn new(target: [u8; 32]) -> Self {
        Self {
            target,
            nodes: Vec::with_capacity(N),
        }
    }

    /// Adds a node to the closest set if it's one of the N closest nodes.
    /// Returns true if the node was added, false otherwise.
    pub fn add(&mut self, node: NodeAddr) -> bool {
        // Check if the node already exists
        for i in 0..self.nodes.len() {
            if self.nodes[i].1.node_id == node.node_id {
                // Node with same ID already exists, don't update it
                return false;
            }
        }

        let distance = calculate_distance(&self.target, node.node_id.as_bytes());

        // If we haven't filled up our capacity yet, just add the node
        if self.nodes.len() < N {
            self.nodes.push((distance, node));
            // Sort by distance - using built-in Ord for [u8; 32]
            self.nodes.sort_by(|a, b| a.0.cmp(&b.0));
            return true;
        }

        // If the list is full, check if this node is closer than the furthest one
        if let Some(furthest) = self.nodes.last() {
            if distance < furthest.0 {
                // Replace the furthest node with this closer one
                self.nodes.pop();
                self.nodes.push((distance, node));
                // Re-sort the list
                self.nodes.sort_by(|a, b| a.0.cmp(&b.0));
                return true;
            }
        }

        // This node wasn't close enough to be included
        false
    }

    pub fn get_closest(&self) -> Vec<NodeAddr> {
        self.nodes.iter().map(|(_, node)| node.clone()).collect()
    }

    pub fn is_full(&self) -> bool {
        self.nodes.len() >= N
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn get(&self, index: usize) -> Option<&NodeAddr> {
        self.nodes.get(index).map(|(_, node)| node)
    }

    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.nodes.iter().any(|(_, node)| node.node_id == *node_id)
    }
}
