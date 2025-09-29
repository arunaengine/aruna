use super::messages::FindResult;
use super::state::KademliaStateHandler;
use super::utils::Closest;
use crate::ALPHA;
use crate::actor_handle::{NetworkActorHandle, ReceiveStreams};
use crate::kademlia::messages::{KademliaMessage, MaybeSignedAddr, MessageType};
use crate::kademlia::node_info::NodeInfo;
use crate::kademlia::utils::{calculate_distance, get_bucket_index};
use anyhow::{Result, anyhow};
use iroh::{NodeAddr, NodeId, PublicKey};
use n0_future::{FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{Instrument, debug, error, info, trace, warn};
use ulid::Ulid;

pub const KADEMLIA_PROTOCOL_ID: u32 = 1;
pub const KEY_TTL: Duration = Duration::from_secs(86400); // 1d
pub const REPUBLISH_INTERVAL: Duration = Duration::from_secs(79200); // 22h

/// Kademlia distributed hash table implementation
#[derive(Clone, Debug)]
pub struct Kademlia {
    network: NetworkActorHandle,
    state: KademliaStateHandler,
}

impl Kademlia {
    /// Create a new Kademlia instance
    #[tracing::instrument(level = "trace", skip(con))]
    pub async fn new(node_id: NodeId, con: NetworkActorHandle) -> Self {
        let kademlia = Self {
            network: con,
            state: KademliaStateHandler::new(node_id),
        };
        kademlia.clone().run().await;
        kademlia
    }

    #[tracing::instrument(level = "trace", skip(self, send_stream, recv_stream))]
    pub async fn handle_incoming_stream(
        &self,
        ReceiveStreams {
            sender,
            mut send_stream,
            mut recv_stream,
        }: ReceiveStreams,
    ) -> Result<()> {
        trace!("Received stream from: {:?}", sender);

        let len = recv_stream.read_u32().await?;
        trace!("Message len {len}");
        let mut buf = vec![0; len as usize];
        recv_stream.read_exact(&mut buf).await?;
        trace!("Read message");

        let message = postcard::from_bytes::<KademliaMessage>(&buf)?;
        trace!("Serialized message {:?} from {:?}", message, sender);

        let Some(response) = self.handle_message(message).await else {
            error!("Failed to handle message");
            return Err(anyhow!("Failed to handle message"));
        };
        let buf = postcard::to_allocvec(&response)?;
        send_stream.write_u32(buf.len() as u32).await?;
        send_stream.write_all(&buf).await?;
        send_stream.flush().await?;
        send_stream.finish()?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn run(self) {
        tokio::spawn(
            async move {
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

                        // Handle incoming streams
                        Ok(inc_stream) = self.network.receive() => {
                            let self_clone = self.clone();

                            trace!("Received stream from: {:?}", inc_stream.sender);
                            tokio::spawn(
                                async move {
                                    if let Err(e) = self_clone.handle_incoming_stream(
                                        inc_stream,
                                    ).await{
                                        error!("Failed to handle incoming stream: {e:#}");
                                    }
                                }.in_current_span()
                            );
                        }

                        // Check for shutdown signal
                        _ = tokio::signal::ctrl_c() => {
                            break;
                        }
                    }
                }
            }
            .in_current_span(),
        );
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn set_node_addr(&self, addr: NodeAddr) {
        self.state.set_node_addr(addr);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn get_node_addr(&self) -> NodeAddr {
        self.state.get_node_addr()
    }

    /// Get our node ID
    #[tracing::instrument(level = "trace", skip(self))]
    fn node_id(&self) -> NodeId {
        self.get_node_addr().node_id
    }

    /// Get node ID bytes as owned array
    #[tracing::instrument(level = "trace", skip(self))]
    fn _node_id_bytes(&self) -> [u8; 32] {
        *self.node_id().as_bytes()
    }

    /// Run maintenance to clean up stale nodes and expired keys
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn run_maintenance(&self) {
        // Remove the node from routing and ping it, if it's still alive
        for (bucket_idx, stale_node) in self.state.get_stale_nodes().into_iter().enumerate() {
            for stale in stale_node {
                match self.ping(stale.clone()).await {
                    Ok(true) => {
                        // Node is alive, update its last seen time
                        let Some(idx) = self.state.find_node_k_idx(bucket_idx, &stale.node_id)
                        else {
                            warn!(
                                "Node {} not found in bucket {}, this should not happen",
                                stale.node_id, bucket_idx
                            );
                            continue;
                        };
                        self.state.refresh_node(bucket_idx, idx);
                    }
                    Ok(false) => {
                        // Node is dead, remove it from the bucket
                        self.state.remove_node(bucket_idx, &stale.node_id);
                    }
                    Err(e) => {
                        warn!("Failed to ping node {}: {e:#}", stale.node_id);
                        // Node is stale, remove it from the bucket
                        self.state.remove_node(bucket_idx, &stale.node_id);
                    }
                }
            }
        }

        // Clean up expired resources
        let pruned_count = self.state.prune_expired_resources();

        // Log maintenance results if significant
        if pruned_count > 0 {
            debug!("Maintenance: pruned {pruned_count} expired resource entries");
        }
    }

    /// Periodically republish all stored key-value pairs
    #[tracing::instrument(level = "trace", skip(self))]
    async fn republish_resources(&self) {
        // Get all resources to republish
        let node_addr = self.get_node_addr().clone();
        let resources_to_republish = self.state.get_republish_sources(REPUBLISH_INTERVAL);

        // Republish each returned local resource
        for (key, signature) in resources_to_republish {
            let _ = self.store(key, node_addr.clone(), signature).await;
        }
    }

    /// Handle an incoming Kademlia message
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_message(&self, message: KademliaMessage) -> Option<KademliaMessage> {
        trace!("Received message: {:?}", message);

        // Update routing table with sender
        if let Some(addr) = message.sender.clone() {
            self.update_node_seen(addr).await;
            trace!("Updated node");
        }

        // Route message based on whether it's a request or response
        if message.is_response() {
            // Handle response (no reply needed)
            self.handle_response(message).await;
            trace!("Handled response");
            None
        } else {
            // Handle request and generate response
            let res = self.handle_request(message).await;
            trace!("Handled quest");
            res
        }
    }

    /// Handle a request message and generate a response
    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_request(&self, request: KademliaMessage) -> Option<KademliaMessage> {
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
                let values = self.state.find_local_addr(&target).unwrap_or_default();

                // Find closest nodes to target
                let closest_nodes = self.state.find_closest_nodes(&target);

                // Create response with values and closest nodes
                Some(request.create_response(
                    Some(self.get_node_addr().clone()),
                    MessageType::FindResponse {
                        value: values,
                        nodes: closest_nodes,
                    },
                ))
            }

            MessageType::StoreRequest {
                key,
                ref value,
                ref signature,
            } => {
                self.state.store(key, value, signature.clone());
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
    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_response(&self, response: KademliaMessage) {
        // Just update our routing table with received information
        if let MessageType::FindResponse {
            ref value,
            ref nodes,
        } = response.msg_type
        {
            // Add all value entries if present
            for val in value {
                self.state.insert_node_addr(val.addr().clone());
            }
            let self_id = self.node_id();

            // Add all returned nodes
            for node in nodes {
                if node.node_id != self_id {
                    self.update_node_seen(node.clone()).await;
                }
            }
        }
    }

    /// Send a message to a node
    #[tracing::instrument(level = "trace", skip(self))]
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
        trace!(
            "Created stream to node {} from {}",
            self.node_id(),
            target_addr.node_id
        );

        // Serialize the message
        let buf = postcard::to_allocvec(&message)
            .map_err(|e| anyhow!("Failed to serialize message: {e:#}"))?;
        // Send the message
        sx.write_u32(buf.len() as u32).await?;
        sx.write_all(&buf).await?;
        sx.flush().await?;
        sx.finish()?;

        trace!("Waiting for response from node: {}", target_addr.node_id);
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

    /// Update a node's info in the appropriate k-bucket
    #[tracing::instrument(level = "trace", skip(self))]
    async fn update_node_seen(&self, addr: NodeAddr) {
        let node_id = addr.node_id;
        trace!("update node");
        let self_id = self.node_id();

        // Don't track ourselves
        if node_id == self_id {
            return;
        }
        // Store in our quick lookup map
        self.state.insert_node_addr(addr.clone());
        trace!("insert node addr");

        // Calculate bucket index
        let distance = calculate_distance(self_id.as_bytes(), node_id.as_bytes());
        let bucket_idx = get_bucket_index(&distance);

        // Create new node info
        let node_info = NodeInfo::new(addr.clone());

        // Try to update the bucket
        if let Some((least_recent_addr, idx)) =
            self.state.update_bucket(bucket_idx, node_info.clone())
        {
            // Only ping if it's not us
            if least_recent_addr.node_id != self_id {
                if let Ok(true) = self.ping(least_recent_addr).await {
                    self.state.refresh_node(bucket_idx, idx);
                } else {
                    self.state.replace_node(bucket_idx, idx, node_info);
                }
            }
        }
    }

    /// Ping a node to check if it's still alive
    #[tracing::instrument(level = "trace", skip(self))]
    async fn ping(&self, target: NodeAddr) -> Result<bool> {
        let self_addr = self.state.get_node_addr();
        // Create ping message
        let id = Ulid::new();
        let message = KademliaMessage::new_request(
            id,
            Some(self_addr.clone()),
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

    /// External API: Find value for key
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn find_value(&self, target: [u8; 32]) -> Result<Vec<MaybeSignedAddr>> {
        info!("Searching key: {:?} @ {} ", &target, self.node_id());

        // Check if we have the value locally (and shortcut)
        if let Some(entries) = self.state.find_local_addr(&target) {
            return Ok(entries);
        };

        // Find closest nodes
        let closest_nodes = self.state.find_closest_nodes(&target);

        // If no nodes found, return empty result
        if closest_nodes.is_empty() {
            trace!("No nodes found for target: {:?}", &target);
            return Ok(vec![]);
        }

        // Now use the actual find operation
        let result = self.find_with_nodes(target, closest_nodes, true).await?;

        // Convert the result to a vector of MaybeSignedAddr
        Ok(result.get_values())
    }

    /// External API: Find operation
    /// Returns a list of nodes closest to the target key
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn find_nodes(&self, target: [u8; 32]) -> Result<Vec<NodeAddr>> {
        info!(
            "Searching nodes for key: {:?} @ {} ",
            &target,
            self.node_id()
        );

        // Find closest nodes
        let closest_nodes = self.state.find_closest_nodes(&target);

        // If no nodes found, return empty result
        if closest_nodes.is_empty() {
            trace!("No nodes found for target: {:?}", &target);
            return Ok(Vec::new());
        }

        // Now use the actual find operation
        let result = self.find_with_nodes(target, closest_nodes, false).await?;

        // Return the closest nodes
        Ok(result.nodes)
    }

    /// External API: Find at closest nodes
    /// Returns the value only at the closest nodes
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn find_at_closest_nodes(&self, target: [u8; 32]) -> Result<Vec<MaybeSignedAddr>> {
        info!(
            "Searching nodes for key: {:?} @ {} ",
            &target,
            self.node_id()
        );

        // Find closest nodes
        let closest_nodes = self.state.find_closest_nodes(&target);

        // If no nodes found, return empty result
        if closest_nodes.is_empty() {
            trace!("No nodes found for target: {:?}", &target);
            return Ok(Vec::new());
        }

        // Now use the actual find operation
        let result = self.find_with_nodes(target, closest_nodes, false).await?;

        // Return the closest nodes
        Ok(result.values_at_closest())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn find_message(
        &self,
        self_addr: NodeAddr,
        addr: NodeAddr,
        command_id: Ulid,
        target: [u8; 32],
    ) -> Result<KademliaMessage> {
        let message = KademliaMessage::new_request(
            command_id,
            Some(self_addr.clone()),
            MessageType::FindRequest { target },
        );
        self.send_message(message, addr).await
    }

    /// Implementation of find that takes initial nodes
    #[tracing::instrument(level = "trace", skip(self))]
    async fn find_with_nodes(
        &self,
        target: [u8; 32],
        initial_nodes: Vec<NodeAddr>,
        shortcircuit: bool, // If true, stop searching after first value found
    ) -> Result<FindResult> {
        // Set up tracking for the find operation
        let mut visited_nodes = HashSet::new();
        let mut closest_nodes = Closest::<ALPHA>::new(target);
        closest_nodes.add(self.get_node_addr());
        let mut found_values_at = HashMap::new();
        if let Some(value) = self.state.find_local_addr(&target) {
            found_values_at.insert(self.node_id(), value);
        };

        let mut futures = FuturesUnordered::new();

        let self_addr = self.get_node_addr();
        let command_id = Ulid::new();

        // Initialize with our closest nodes, tracking distance
        for node in initial_nodes {
            if node.node_id != self.node_id() {
                visited_nodes.insert(node.node_id.clone());
                let _ = closest_nodes.add(node.clone());
                futures.push(tokio::time::timeout(
                    Duration::from_secs(5),
                    self.find_message(self_addr.clone(), node, command_id.clone(), target.clone()),
                ));
            }
        }

        while let Some(result) = futures.next().await {
            match result {
                Ok(Ok(response)) => {
                    if let MessageType::FindResponse { value, nodes } = response.msg_type {
                        // Add all found values
                        if !value.is_empty() {
                            let Some(sender) = response.sender else {
                                error!("No sender in response");
                                continue;
                            };

                            found_values_at.insert(sender.node_id, value.clone());
                            if shortcircuit {
                                // If we found a value and want to stop searching
                                return Ok(FindResult {
                                    value_at_nodes: found_values_at,
                                    nodes: closest_nodes.get_closest(),
                                });
                            }
                        }

                        // Process returned nodes for further lookup
                        for node in nodes {
                            if node.node_id != self_addr.node_id
                                && !visited_nodes.contains(&node.node_id)
                            {
                                // Check if the node is closer
                                if closest_nodes.add(node.clone()) {
                                    // Add to pending if we still want to search more
                                    futures.push(tokio::time::timeout(
                                        Duration::from_secs(5),
                                        self.find_message(
                                            self_addr.clone(),
                                            node,
                                            command_id.clone(),
                                            target.clone(),
                                        ),
                                    ));
                                }
                            }
                        }
                    }
                }
                Ok(Err(err)) => error!("Failed to send message: {err:#}"),
                Err(_) => error!("Timeout while waiting for response"),
            }
        }
        Ok(FindResult {
            value_at_nodes: found_values_at,
            nodes: closest_nodes.get_closest(),
        })
    }

    /// External API: Store operation (simplified)
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn store(
        &self,
        key: [u8; 32],
        value: NodeAddr,
        signature: Option<Vec<u8>>,
    ) -> Result<()> {
        let self_addr = self.get_node_addr();

        trace!(
            "Storing key: {:?} @ {} ",
            PublicKey::from_bytes(&key),
            self_addr.node_id
        );

        // Store locally first with TTL
        self.state.store(key, &self_addr, signature.clone());

        // Find nodes closest to the key
        let find_result = self.find_nodes(key).await?;

        trace!("Closest nodes to store: {:?}", find_result);

        // Send store message to all closest nodes
        let command_id = Ulid::new();
        let mut store_requests = Vec::new();

        for node in find_result {
            if node.node_id != self_addr.node_id {
                let message = KademliaMessage::new_request(
                    command_id,
                    Some(self.get_node_addr().clone()),
                    MessageType::StoreRequest {
                        key,
                        value: value.clone(),
                        signature: signature.clone(),
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
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn bootstrap(&self, bootstrap_nodes: Vec<NodeAddr>) -> Result<()> {
        info!("Bootstrapping with nodes: {:?}", bootstrap_nodes);

        if bootstrap_nodes.is_empty() {
            return Err(anyhow!("No bootstrap nodes provided"));
        }

        let node_addr = self.get_node_addr().clone();
        // Add bootstrap nodes to our routing table

        self.state.insert_node_addr(node_addr.clone());

        for addr in bootstrap_nodes.clone() {
            self.update_node_seen(addr).await;
        }

        // Find our own node ID in the network to discover closest nodes
        let target = *node_addr.clone().node_id.as_bytes();

        // Store our node in the network
        self.store(target, node_addr, None).await?;

        Ok(())
    }
}
