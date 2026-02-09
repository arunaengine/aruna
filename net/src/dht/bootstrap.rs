// net/src/dht/bootstrap.rs
use aruna_core::NodeId;
use parking_lot::RwLock;
use std::sync::Arc;

use super::client::DhtClient;
use super::kbucket::{K, PeerInfo, RoutingTable};
use super::lookup::find_node;
use crate::error::{NetError, Result};

/// Bootstrap the DHT by connecting to known peers
pub async fn bootstrap(
    client: &DhtClient,
    routing_table: Arc<RwLock<RoutingTable>>,
    bootstrap_nodes: &[NodeId],
) -> Result<()> {
    if bootstrap_nodes.is_empty() {
        return Ok(()); // No bootstrap nodes configured
    }

    let local_id = *routing_table.read().local_id();

    // First, ping bootstrap nodes and add responsive ones to routing table
    let mut active_nodes = Vec::new();

    for node_id in bootstrap_nodes {
        // Try to ping the bootstrap node
        if client.ping(node_id).await.unwrap_or(false) {
            let peer = PeerInfo::new(*node_id);
            routing_table.write().insert(peer.clone());
            active_nodes.push(peer);
        }
    }

    if active_nodes.is_empty() {
        return Err(NetError::Bootstrap(
            "no bootstrap nodes reachable".to_string(),
        ));
    }

    // Perform iterative lookup for our own ID to populate routing table
    let found_nodes = find_node(client, active_nodes.clone(), local_id.as_bytes()).await;

    // Add discovered nodes to routing table
    for node_id in found_nodes {
        let peer = PeerInfo::new(node_id);
        routing_table.write().insert(peer);
    }

    // Optionally: refresh all buckets by looking up a random ID in each
    // This helps ensure good coverage of the network
    refresh_buckets(client, routing_table).await;

    Ok(())
}

/// Refresh routing table by performing lookups in each bucket
async fn refresh_buckets(client: &DhtClient, routing_table: Arc<RwLock<RoutingTable>>) {
    let local_id = *routing_table.read().local_id();

    // For each bucket, generate a random ID in that bucket's range and look it up
    for bucket_idx in 0..256 {
        // Create a target ID that would fall into this bucket
        let mut target_bytes = *local_id.as_bytes();

        // Flip the bit at bucket_idx to create a target in that bucket
        let byte_idx = bucket_idx / 8;
        let bit_idx = 7 - (bucket_idx % 8);
        target_bytes[byte_idx] ^= 1 << bit_idx;

        // Add some randomness to the lower bits
        for byte in target_bytes.iter_mut().skip(byte_idx + 1) {
            *byte = rand::random();
        }

        // Use raw bytes directly for XOR distance (not a valid NodeId)
        let initial = routing_table.read().closest(&target_bytes, K);
        if initial.is_empty() {
            continue;
        }

        // Perform lookup
        let found = find_node(client, initial, &target_bytes).await;

        // Add to routing table
        for node_id in found {
            let peer = PeerInfo::new(node_id);
            routing_table.write().insert(peer);
        }
    }
}
