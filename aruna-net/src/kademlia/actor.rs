use anyhow::{Result, anyhow};
use iroh::{NodeAddr, NodeId, PublicKey};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, trace, warn};
use ulid::Ulid;

use crate::actor::CHANNEL_SIZE;
use crate::actor_handle::{NetworkActorHandle, ReceiveStreams};
use crate::kademlia::k_bucket::KBucket;
use crate::kademlia::messages::{KademliaMessage, MessageType};
use crate::kademlia::node_info::NodeInfo;
use crate::kademlia::utils::{calculate_distance, get_bucket_index, viz};
use crate::{ALPHA, K_BUCKET_SIZE, REQUEST_TIMEOUT};

use super::actor_handle::{KademliaActorHandle, KademliaRequest};
use super::messages::FindResult;
use super::time_handler::TimeHandler;

pub const KADEMLIA_PROTOCOL_ID: u32 = 1;
pub const KEY_TTL: Duration = Duration::from_secs(86400); // 1d
pub const REPUBLISH_INTERVAL: Duration = Duration::from_secs(79200); // 22h

/// Internal mutable state of Kademlia
#[derive(Debug)]
struct KademliaState {
    // Our node address, should be set at startup
    node_addr: NodeAddr,
    // Kademlia routing table with K buckets
    k_buckets: [KBucket; 256],
    // Resources stored by us and the nodes that have them
    resources: HashMap<[u8; 32], HashSet<NodeId>>,
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
                entries.remove(&node_id);
                if entries.is_empty() {
                    self.resources.remove(&key.key());
                }
            }
        }
        all.len()
    }
}

/// Kademlia distributed hash table implementation with interior mutability
pub struct KademliaActor {
    network: NetworkActorHandle,
    state: KademliaState,
    receiver: async_channel::Receiver<KademliaRequest>,
}

impl KademliaActor {
    /// Create a new Kademlia instance
    pub async fn new(node_id: NodeId, con: NetworkActorHandle) -> KademliaActorHandle {
        let (sender, receiver) = async_channel::bounded(CHANNEL_SIZE);
        let actor = Self {
            network: con,
            state: KademliaState::new(node_id),
            receiver,
        };
        actor.run().await;
        KademliaActorHandle::new(sender)
    }

    pub async fn run(mut self) {
        tokio::spawn(async move {
            let mut republish_timer = tokio::time::interval(REPUBLISH_INTERVAL);
            let mut maintenance_timer = tokio::time::interval(Duration::from_secs(3600));

            loop {
                tokio::select! {
                    // Maintenance timer
                    _ = maintenance_timer.tick() => {
                        self.run_maintenance().await;
                        info!("Maintenance run for node {}", self.node_id());
                    }

                    // Republish timer
                    _ = republish_timer.tick() => {
                        self.republish_resources().await;
                        info!("Resources republished for node {}", self.node_id());
                    }

                    // Handle incoming requests
                    Ok(message) = self.receiver.recv() => {
                        self.handle_actor_request(message).await;
                    }

                    // Handle incoming streams
                    Ok(inc_stream) = self.network.receive() => {
                        match self.handle_incoming_stream(inc_stream).await {
                            Ok(_) => {
                                trace!("Handled incoming stream successfully");
                            }
                            Err(e) => {
                                error!("Failed to handle incoming stream: {e:#}");
                            }
                        }
                    }

                    // Check for shutdown signal
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                }
            }
        });
    }

    pub async fn handle_incoming_stream(
        &mut self,
        ReceiveStreams {
            sender,
            mut send_stream,
            mut recv_stream,
        }: ReceiveStreams,
    ) -> Result<()> {
        trace!("Received stream from: {:?}", sender);

        let len = recv_stream.read_u32().await?;
        let mut buf = vec![0; len as usize];
        recv_stream.read_exact(&mut buf).await?;

        let message = postcard::from_bytes::<KademliaMessage>(&buf)?;
        let Some(response) = self.handle_message(message).await else {
            error!("Failed to handle message");
            return Err(anyhow!("Failed to handle message"));
        };
        let buf = postcard::to_allocvec(&response)?;
        send_stream.write_u32(buf.len() as u32).await?;
        send_stream.write_all(&buf).await?;
        send_stream.flush().await?;
        Ok(())
    }

    pub fn set_node_addr(&mut self, addr: NodeAddr) {
        self.state.node_addr = addr.clone();
        self.state.node_addresses.insert(addr.node_id, addr.clone());
    }

    fn get_node_addr(&self) -> &NodeAddr {
        &self.state.node_addr
    }

    /// Get our node ID
    fn node_id(&self) -> &NodeId {
        &self.get_node_addr().node_id
    }

    /// Get node ID bytes as owned array
    fn node_id_bytes(&self) -> [u8; 32] {
        self.node_id().as_bytes().clone()
    }

    async fn handle_actor_request(&mut self, request: KademliaRequest) {
        match request {
            KademliaRequest::Request {
                request,
                response: response_channel,
            } => {
                let response = self.run_request(request).await;
                if let Ok(response) = response {
                    let _ = response_channel.send(response);
                }
            }
            KademliaRequest::SetNodeAddr { node_addr } => {
                self.set_node_addr(node_addr);
            }
            KademliaRequest::Bootstrap { node_addrs } => {
                if let Err(e) = self.bootstrap(node_addrs).await {
                    error!("Failed to bootstrap: {e:#}");
                }
            }
        }
    }

    async fn run_request(&mut self, request: KademliaMessage) -> Result<KademliaMessage> {
        match &request.msg_type {
            MessageType::PingRequest { node_addr } => {
                match self.spawn_ping(node_addr.clone()).await? {
                    true => Ok(request.create_response(
                        Some(self.get_node_addr().clone()),
                        MessageType::PingResponse,
                    )),
                    false => Err(anyhow!("Ping failed")),
                }
            }
            MessageType::FindRequest { target } => {
                let find_result = self.find(*target, true).await?;
                Ok(request.create_response(
                    Some(self.get_node_addr().clone()),
                    MessageType::FindResponse {
                        value: find_result.value,
                        nodes: find_result.nodes,
                    },
                ))
            }
            MessageType::StoreRequest { key, value } => {
                // Store the key-value pair locally
                self.store(*key, value.clone()).await?;
                Ok(request.create_response(
                    Some(self.get_node_addr().clone()),
                    MessageType::StoreResponse,
                ))
            }
            _ => {
                error!("Invalid request type: Response: {:?}", request.msg_type);
                Err(anyhow!("Invalid request type"))
            }
        }
    }

    /// Run maintenance to clean up stale nodes and expired keys
    pub async fn run_maintenance(&mut self) {
        // Use a write lock to update the state
        let mut stale_nodes = Vec::new();
        // Remove stale nodes from all buckets
        for bucket in self.state.k_buckets.iter() {
            let potential_stale = bucket.get_stale_nodes();
            stale_nodes.push(potential_stale)
        }

        // Remove the node from routing and ping it, if it's still alive
        for (bucket_idx, stale_node) in stale_nodes.into_iter().enumerate() {
            for stale in stale_node {
                match self.spawn_ping(stale.clone()).await {
                    Ok(true) => {
                        // Node is alive, update its last seen time
                        let Some(idx) = self.state.k_buckets[bucket_idx].find_node(&stale.node_id)
                        else {
                            warn!(
                                "Node {} not found in bucket {}, this should not happen",
                                stale.node_id, bucket_idx
                            );
                            continue;
                        };
                        self.state.k_buckets[bucket_idx].refresh_node(idx);
                    }
                    Ok(false) => {
                        // Node is dead, remove it from the bucket
                        self.state.k_buckets[bucket_idx].remove_node(&stale.node_id);
                        // Also remove from the node addresses
                        self.state.node_addresses.remove(&stale.node_id);
                    }
                    Err(e) => {
                        warn!("Failed to ping node {}: {e:#}", stale.node_id);
                        // Node is stale, remove it from the bucket
                        self.state.k_buckets[bucket_idx].remove_node(&stale.node_id);
                        // Also remove from the node addresses
                        self.state.node_addresses.remove(&stale.node_id);
                    }
                }
            }
        }

        // Clean up expired resources
        let pruned_count = self.state.prune_expired_resources();

        // Log maintenance results if significant
        if pruned_count > 0 {
            println!(
                "Maintenance: pruned {} expired resource entries",
                pruned_count
            );
        }
    }

    /// Periodically republish all stored key-value pairs
    async fn republish_resources(&mut self) {
        // Get all resources to republish
        let node_addr = self.get_node_addr().clone();
        let resources_to_republish = {
            let Some(republish_threshold) = SystemTime::now().checked_sub(REPUBLISH_INTERVAL)
            else {
                warn!("Failed to calculate republish threshold");
                return;
            };
            self.state
                .local_resources
                .remove_older_than(republish_threshold)
                .into_iter()
                .map(|key| (key.key(), node_addr.clone()))
                .collect::<Vec<_>>()
        };

        // Republish each returned local resource
        for (key, value) in resources_to_republish {
            let _ = self.store(key, value).await;
        }
    }

    /// Handle an incoming Kademlia message
    pub async fn handle_message(&mut self, message: KademliaMessage) -> Option<KademliaMessage> {
        trace!("Received message: {:?} @ node: {}", message, self.node_id());

        // Update routing table with sender
        if let Some(addr) = message.sender.clone() {
            self.update_node_seen(addr).await;
        }

        // Route message based on whether it's a request or response
        if message.is_response() {
            // Handle response (no reply needed)
            self.handle_response(message).await;
            None
        } else {
            // Handle request and generate response
            self.handle_request(message).await
        }
    }

    /// Handle a request message and generate a response
    async fn handle_request(&mut self, request: KademliaMessage) -> Option<KademliaMessage> {
        match request.msg_type {
            MessageType::PingRequest { .. } => {
                // Simple ping response
                Some(request.create_response(
                    Some(self.get_node_addr().clone()),
                    MessageType::PingResponse,
                ))
            }

            MessageType::FindRequest { target } => {
                // Read lock for finding nodes
                // Get all values for this target
                let mut values = Vec::new();

                // First check resource values
                if let Some(entries) = self.state.resources.get(&target) {
                    for node_id in entries {
                        if let Some(addr) = self.state.node_addresses.get(node_id) {
                            values.push(addr.clone());
                        }
                    }
                }

                // Also check if we have an exact match for node ID
                if values.is_empty() {
                    if let Some(addr) = self.state.node_addresses.get(&target) {
                        values.push(addr.clone());
                    }
                }

                // Find closest nodes to target
                let closest_nodes = self.find_closest_nodes(&target).await;

                // Create response with values and closest nodes
                Some(request.create_response(
                    Some(self.get_node_addr().clone()),
                    MessageType::FindResponse {
                        value: values,
                        nodes: closest_nodes,
                    },
                ))
            }

            MessageType::StoreRequest { key, ref value } => {
                // Get or create entry for this key
                let entries = self.state.resources.entry(key).or_insert_with(HashSet::new);

                // Update the entry with new TTL
                entries.insert(value.node_id);

                // Insert the key into the local expiration timer
                self.state.store_timer.insert(key, Some(value.node_id));

                // Always update the node address mapping
                self.state
                    .node_addresses
                    .insert(value.node_id, value.clone());

                // Create response
                Some(request.create_response(
                    Some(self.get_node_addr().clone()),
                    MessageType::StoreResponse,
                ))
            }

            // Ignore response messages in request handler
            _ => None,
        }
    }

    /// Handle a response message
    async fn handle_response(&mut self, response: KademliaMessage) {
        // Just update our routing table with received information
        if let MessageType::FindResponse {
            ref value,
            ref nodes,
        } = response.msg_type
        {
            // Add all value entries if present
            for val in value {
                self.state.node_addresses.insert(val.node_id, val.clone());
            }

            // Add all returned nodes
            for node in nodes {
                if &node.node_id != self.node_id() {
                    self.update_node_seen(node.clone()).await;
                }
            }
        }
    }

    /// Send a message to a node
    async fn send_message(
        &self,
        message: KademliaMessage,
        target_addr: NodeAddr,
    ) -> Result<KademliaMessage> {
        info!(
            "Sending message: {:?} from node {} to node: {}",
            message,
            self.node_id(),
            target_addr.node_id
        );

        let (mut sx, mut rx) = self.network.create_stream(target_addr.node_id).await?;

        // Serialize the message
        let buf = postcard::to_allocvec(&message)
            .map_err(|e| anyhow!("Failed to serialize message: {e:#}"))?;
        // Send the message
        sx.write_u32(buf.len() as u32).await?;
        sx.write_all(&buf).await?;
        sx.flush().await?;

        // Read the response
        let len = rx.read_u32().await?;
        let mut buf = vec![0; len as usize];
        rx.read_exact(&mut buf).await?;
        let response = postcard::from_bytes::<KademliaMessage>(&buf)
            .map_err(|e| anyhow!("Failed to deserialize response: {e:#}"))?;

        info!(
            "Received response: {:?} from node {} at node: {}",
            response,
            self.node_id(),
            target_addr.node_id
        );

        Ok(response)
    }

    /// Find the closest nodes to a target from our routing table
    async fn find_closest_nodes(&self, target: &[u8; 32]) -> Vec<NodeAddr> {
        let distance_to_target = calculate_distance(&self.node_id_bytes(), target);
        let bucket_idx = get_bucket_index(&distance_to_target);

        // Start with the exact bucket
        let mut candidates = Vec::new();
        candidates.extend(self.state.k_buckets[bucket_idx].get_nodes());

        // If we need more nodes, check adjacent buckets
        let mut i = 1;
        while candidates.len() < K_BUCKET_SIZE && i < 256 {
            // Check bucket before
            if bucket_idx >= i {
                candidates.extend(self.state.k_buckets[bucket_idx - i].get_nodes());
            }

            // Check bucket after
            if bucket_idx + i < self.state.k_buckets.len() {
                candidates.extend(self.state.k_buckets[bucket_idx + i].get_nodes());
            }

            i += 1;

            // Stop if we've checked all buckets or found enough nodes
            if candidates.len() >= K_BUCKET_SIZE
                || (i > bucket_idx && bucket_idx + i >= self.state.k_buckets.len())
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

    /// Update a node's info in the appropriate k-bucket
    async fn update_node_seen(&mut self, addr: NodeAddr) {
        let node_id = addr.node_id;

        // Don't track ourselves
        if &node_id == self.node_id() {
            return;
        }

        // Store in our quick lookup map
        self.state.node_addresses.insert(node_id, addr.clone());

        // Calculate bucket index
        let distance = calculate_distance(&self.node_id_bytes(), node_id.as_bytes());
        let bucket_idx = get_bucket_index(&distance);

        // Create new node info
        let node_info = NodeInfo::new(addr.clone());

        // Try to update the bucket
        if let Some((least_recent_addr, idx)) =
            self.state.k_buckets[bucket_idx].update(node_info.clone())
        {
            // Only ping if it's not us
            if &least_recent_addr.node_id != self.node_id() {
                if let Ok(true) = self.spawn_ping(least_recent_addr).await {
                    self.state.k_buckets[bucket_idx].refresh_node(idx);
                } else {
                    self.state.k_buckets[bucket_idx].replace_node(idx, node_info);
                }
            }
        }
    }

    /// Ping a node to check if it's still alive
    async fn spawn_ping(&self, target: NodeAddr) -> Result<bool> {
        // Create ping message
        let id = Ulid::new();
        let message = KademliaMessage::new_request(
            id,
            Some(self.get_node_addr().clone()),
            MessageType::PingRequest {
                node_addr: target.clone(),
            },
        );

        // Send and wait for response
        match self.send_message(message, target).await?.msg_type {
            MessageType::PingResponse => Ok(true),
            _ => Ok(false),
        }
    }

    /// External API: Find operation with choice of mode
    pub async fn find(&self, target: [u8; 32], shortcircuit: bool) -> Result<FindResult> {
        info!(
            "Searching key: {:?} @ {} ",
            PublicKey::from_bytes(&target).unwrap(),
            self.node_id()
        );

        // First check if we have the value locally
        let mut local_values = Vec::new();

        if shortcircuit {
            trace!(
                "local nodes: {:?} @ node: {}",
                self.state.node_addresses,
                self.node_id()
            );
            trace!(
                "local resources: {:?} @ node: {}",
                self.state.resources,
                self.node_id()
            );

            // Check for values in resources
            if let Some(entries) = self.state.resources.get(&target) {
                for node_id in entries {
                    if let Some(addr) = self.state.node_addresses.get(node_id) {
                        local_values.push(addr.clone());
                    }
                }
            }

            // Check for exact node ID match (if we didn't find resource values)
            if local_values.is_empty() {
                if let Some(value) = self.state.node_addresses.get(&target) {
                    local_values.push(value.clone());
                }
            }
        }

        if !local_values.is_empty() {
            return Ok(FindResult {
                value: local_values,
                nodes: Vec::new(),
            });
        }

        // Find closest nodes
        let closest_nodes = self.find_closest_nodes(&target).await;

        // If no nodes found, return empty result
        if closest_nodes.is_empty() {
            //trace!(
            //    "No nodes found for target: {:?}",
            //    PublicKey::from_bytes(&target).unwrap()
            //);

            return Ok(FindResult::empty());
        }

        // Now implement the actual find operation
        self.find_with_nodes(target, closest_nodes, shortcircuit)
            .await
    }

    /// Implementation of find that takes initial nodes
    async fn find_with_nodes(
        &self,
        target: [u8; 32],
        initial_nodes: Vec<NodeAddr>,
        shortcircuit: bool,
    ) -> Result<FindResult> {
        // Set up tracking for the find operation
        let mut visited_nodes = HashSet::new();
        let mut closest_nodes = BTreeMap::new();
        let mut found_values = Vec::new();
        let mut pending_nodes = Vec::new();

        // Initialize with our closest nodes, tracking distance
        for node in initial_nodes {
            if &node.node_id != self.node_id() {
                let distance = calculate_distance(&target, node.node_id.as_bytes());
                closest_nodes.insert(distance, node.clone());
                pending_nodes.push(node);
            }
        }

        // Create ID for all requests in this operation
        let command_id = Ulid::new();

        // Track start time for timeout
        let started_at = Instant::now();

        // Main search loop
        while !pending_nodes.is_empty() {
            // Check for timeout
            if started_at.elapsed() > REQUEST_TIMEOUT {
                break;
            }

            // Get next batch of nodes to query (up to ALPHA)
            let batch = pending_nodes
                .drain(..std::cmp::min(ALPHA, pending_nodes.len()))
                .collect::<Vec<_>>();

            // Send FindRequest to each node in parallel
            let mut responses = Vec::new();
            for node in batch {
                let message = KademliaMessage::new_request(
                    command_id,
                    Some(self.get_node_addr().clone()),
                    MessageType::FindRequest { target },
                );

                match self.send_message(message, node.clone()).await {
                    Ok(response) => responses.push(response),
                    Err(err) => error!("{err}"),
                }

                visited_nodes.insert(node.node_id);
            }

            trace!("Responses: {:?} @ node: {}", responses, self.node_id());

            // Process all responses
            let mut found_value_in_batch = false;

            for response in responses {
                if let MessageType::FindResponse { value, nodes } = response.msg_type {
                    // Add all found values
                    if !value.is_empty() {
                        found_values.extend(value.clone());
                        found_value_in_batch = true;
                    }

                    // Process returned nodes for further lookup
                    for node in nodes {
                        if &node.node_id != self.node_id() && !visited_nodes.contains(&node.node_id)
                        {
                            let distance = calculate_distance(&target, node.node_id.as_bytes());
                            closest_nodes.insert(distance, node.clone());

                            // Keep only the K closest nodes
                            while closest_nodes.len() > K_BUCKET_SIZE {
                                if let Some(max_distance) = closest_nodes.keys().last().cloned() {
                                    closest_nodes.remove(&max_distance);
                                }
                            }

                            // Add to pending if we still want to search more
                            pending_nodes.push(node);
                        }
                    }
                }
            }

            // Check if we should short-circuit
            if found_value_in_batch && shortcircuit {
                break;
            }
        }

        // If values found, return those
        if !found_values.is_empty() {
            return Ok(FindResult {
                value: found_values,
                nodes: Vec::new(),
            });
        }

        // Otherwise return closest nodes
        let closest = closest_nodes.values().cloned().collect();
        Ok(FindResult {
            value: Vec::new(),
            nodes: closest,
        })
    }

    /// External API: Store operation (simplified)
    pub async fn store(&mut self, key: [u8; 32], value: NodeAddr) -> Result<()> {
        info!(
            "Storing key: {:?} @ {} ",
            PublicKey::from_bytes(&key).unwrap(),
            self.node_id()
        );

        // Store locally first with TTL
        {
            // Only store if the key is a "key" and not the node ID
            if value.node_id.as_bytes() != &key {
                // Get or create entry for this key
                let entries = self.state.resources.entry(key).or_insert_with(HashSet::new);
                entries.insert(value.node_id);
                // Track the key as local resource to ensure Republish
                self.state.local_resources.insert(key, None);
            }

            // Update node address mapping
            self.state
                .node_addresses
                .insert(value.node_id, value.clone());
        }

        // Find nodes closest to the key
        let find_result = self.find(key, false).await?;

        trace!("Closest nodes to store: {:?}", find_result.nodes);

        // Send store message to all closest nodes
        let command_id = Ulid::new();
        let mut store_requests = Vec::new();

        for node in find_result.nodes {
            if &node.node_id != self.node_id() {
                let message = KademliaMessage::new_request(
                    command_id,
                    Some(self.get_node_addr().clone()),
                    MessageType::StoreRequest {
                        key,
                        value: value.clone(),
                    },
                );

                store_requests.push(self.send_message(message, node));
            }
        }

        // Wait for all store requests to complete
        for request in futures_util::future::join_all(store_requests).await {
            if let Err(err) = request {
                error!("Failed to store on node: {err:#}");
            }
        }

        Ok(())
    }

    /// Bootstrap the node by adding it to an existing Kademlia network (private)
    async fn bootstrap(&mut self, bootstrap_nodes: Vec<NodeAddr>) -> Result<()> {
        info!("Bootstrapping with nodes: {:?}", bootstrap_nodes);

        if bootstrap_nodes.is_empty() {
            return Err(anyhow!("No bootstrap nodes provided"));
        }

        let node_addr = self.get_node_addr().clone();
        // Add bootstrap nodes to our routing table

        self.state
            .node_addresses
            .insert(node_addr.node_id, node_addr);
        for addr in bootstrap_nodes.clone() {
            self.update_node_seen(addr).await;
        }

        // Find our own node ID in the network to discover closest nodes
        let target = self.node_id_bytes();

        // Store our node in the network
        self.store(target, self.get_node_addr().clone()).await?;

        Ok(())
    }
}
