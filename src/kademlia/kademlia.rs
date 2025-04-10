use anyhow::{Result, anyhow};
use futures_util::FutureExt;
use iroh::{Endpoint, NodeAddr, NodeId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::connection_handler::ConnectionHandler;
use crate::kademlia::k_bucket::KBucket;
use crate::kademlia::messages::{FindResult, KademliaMessage, MessageType};
use crate::kademlia::node_info::NodeInfo;
use crate::kademlia::utils::{calculate_distance, get_bucket_index};
use crate::{ALPHA, K_BUCKET_SIZE, REQUEST_TIMEOUT};

/// Internal mutable state of Kademlia
#[derive(Debug)]
struct KademliaState {
    k_buckets: [KBucket; 256],
    resources: HashMap<[u8; 32], NodeId>,
    node_addresses: HashMap<NodeId, NodeAddr>,
    pending_commands: HashMap<Ulid, CommandState>,
}

impl KademliaState {
    fn new() -> Self {
        Self {
            k_buckets: std::array::from_fn(|_| KBucket::new()),
            resources: HashMap::new(),
            node_addresses: HashMap::new(),
            pending_commands: HashMap::new(),
        }
    }
}

/// Simplified command type enum with better type safety
#[derive(Debug)]
enum CommandType {
    Find(Option<oneshot::Sender<Result<FindResult>>>),
    Store(Option<oneshot::Sender<Result<()>>>),
    Ping(Option<oneshot::Sender<Result<bool>>>),
}

/// Combined command state for tracking progress
#[derive(Debug)]
struct CommandState {
    started_at: Instant,
    visited_nodes: HashSet<NodeId>,
    closest_nodes: BTreeMap<[u8; 32], NodeAddr>, // Using BTreeMap with distance as key
    found_value: Option<NodeAddr>,
    target: [u8; 32],
    pending_count: usize,
    command_type: CommandType,
}

impl CommandState {
    fn new(target: [u8; 32], command_type: CommandType) -> Self {
        Self {
            started_at: Instant::now(),
            visited_nodes: HashSet::new(),
            closest_nodes: BTreeMap::new(),
            found_value: None,
            target,
            pending_count: 0,
            command_type,
        }
    }

    fn is_timed_out(&self) -> bool {
        self.started_at.elapsed() > REQUEST_TIMEOUT
    }

    fn add_closest_node(&mut self, node: NodeAddr) {
        let distance = calculate_distance(&self.target, node.node_id.as_bytes());

        // Insert this node with its distance
        self.closest_nodes.insert(distance, node);

        // Keep only the K closest nodes
        // Since BTreeMap is sorted by key (distance), we remove the largest distances
        while self.closest_nodes.len() > K_BUCKET_SIZE {
            if let Some(max_distance) = self.closest_nodes.keys().last().cloned() {
                self.closest_nodes.remove(&max_distance);
            }
        }
    }

    fn get_closest_nodes(&self) -> Vec<NodeAddr> {
        // Return the values (NodeAddr) from closest to farthest
        self.closest_nodes.values().cloned().collect()
    }

    fn get_found_value(&self) -> Option<NodeAddr> {
        self.found_value.clone()
    }
}

/// Kademlia distributed hash table implementation with interior mutability
#[derive(Debug)]
pub struct Kademlia {
    node_addr: NodeAddr,
    chandler: Arc<ConnectionHandler>,
    pub state: RwLock<KademliaState>,
    maintenance_handle: Mutex<Option<JoinHandle<()>>>,
    self_arc: Mutex<Option<Arc<Self>>>,
}

impl Kademlia {
    /// Create a new Kademlia instance for the given node address
    pub async fn new(chandler: Arc<ConnectionHandler>) -> Result<Arc<Self>> {
        let node_addr = endpoint.node_addr().await?;
        let kademlia = Arc::new(Self {
            node_addr,
            chandler,
            state: RwLock::new(KademliaState::new()),
            maintenance_handle: Mutex::new(None),
            self_arc: Mutex::new(None),
        });

        // Start the maintenance task
        kademlia.clone().start_maintenance_task();

        kademlia
            .self_arc
            .lock()
            .unwrap()
            .replace(kademlia.clone());

        Ok(kademlia)
    }

    /// Get our node ID
    fn node_id(&self) -> NodeId {
        self.node_addr.node_id
    }

    /// Get node ID bytes as owned array
    fn node_id_bytes(&self) -> [u8; 32] {
        *self.node_addr.node_id.as_bytes()
    }

    pub fn get_self(&self) -> Arc<Self> {
        self.self_arc
            .lock()
            .unwrap()
            .as_ref()
            .expect("Self reference not set")
            .clone()
    }

    /// Start periodic maintenance task
    fn start_maintenance_task(self: Arc<Self>) {
        let kademlia_clone = Arc::clone(&self);

        // Spawn the background task with simple sleep-based scheduling
        let handle = tokio::spawn(async move {
            loop {
                // Run maintenance every 60 seconds
                tokio::time::sleep(Duration::from_secs(60)).await;

                // Run maintenance
                kademlia_clone.run_maintenance();
                println!("Maintenance run for node {}", kademlia_clone.node_id());

                // Check for shutdown signal
                if tokio::signal::ctrl_c().now_or_never().is_some() {
                    break;
                }
            }
        });

        // Store the handle
        *self.maintenance_handle.lock().unwrap() = Some(handle);
    }

    /// Run maintenance to clean up stale nodes and timed-out requests
    pub fn run_maintenance(&self) {
        // Use a write lock to update the state
        let mut state = self.state.write().unwrap();

        // Remove stale nodes from all buckets
        for bucket in state.k_buckets.iter_mut() {
            let _ = bucket.prune_stale_nodes();
        }

        // Clean up timed-out commands
        let timed_out: Vec<Ulid> = state
            .pending_commands
            .iter()
            .filter(|(_, cmd_state)| cmd_state.is_timed_out())
            .map(|(id, _)| *id)
            .collect();

        // Simply abort all timed-out commands with error
        for id in timed_out {
            if let Some(cmd_state) = state.pending_commands.remove(&id) {
                match cmd_state.command_type {
                    CommandType::Find(mut sender) => {
                        if let Some(s) = sender.take() {
                            let _ = s.send(Err(anyhow!("Find operation timed out")));
                        }
                    }
                    CommandType::Store(mut sender) => {
                        if let Some(s) = sender.take() {
                            let _ = s.send(Err(anyhow!("Store operation timed out")));
                        }
                    }
                    CommandType::Ping(mut sender) => {
                        if let Some(tx) = sender.take() {
                            let _ = tx.send(Err(anyhow!("Ping operation timed out")));
                        }
                    }
                }
            }
        }
    }

    /// Handle an incoming Kademlia message
    pub fn handle_message(&self, message: KademliaMessage) -> Option<KademliaMessage> {
        // Update routing table with sender
        {
            let mut state = self.state.write().unwrap();
            self.update_node_seen(&mut state, message.sender.clone());
        }

        // Route message based on whether it's a request or response
        if message.is_response() {
            // Handle response (no reply needed)
            self.handle_response(message);
            None
        } else {
            // Handle request and generate response
            self.handle_request(message)
        }
    }

    /// Handle a request message and generate a response
    fn handle_request(&self, request: KademliaMessage) -> Option<KademliaMessage> {
        match request.msg_type {
            MessageType::PingRequest => {
                // Simple ping response
                Some(request.create_response(self.node_addr.clone(), MessageType::PingResponse))
            }

            MessageType::FindRequest { target } => {
                // Read lock for finding nodes
                let state = self.state.read().unwrap();

                // Check if we have the value for this target
                let value = state
                    .resources
                    .get(&target)
                    .and_then(|node_id| state.node_addresses.get(node_id).cloned());

                // Find closest nodes to target
                let nodes = self.find_closest_nodes(&target);

                // Create response
                Some(request.create_response(
                    self.node_addr.clone(),
                    MessageType::FindResponse { value, nodes },
                ))
            }

            MessageType::StoreRequest { key, ref value } => {
                // Write lock for storing
                let mut state = self.state.write().unwrap();

                // Store the key-value pair
                state.resources.insert(key, value.node_id);
                state.node_addresses.insert(value.node_id, value.clone());

                // Create response
                Some(request.create_response(self.node_addr.clone(), MessageType::StoreResponse))
            }

            // Ignore response messages in request handler
            _ => None,
        }
    }

    /// Handle a response message
    fn handle_response(&self, response: KademliaMessage) {
        let command_id = response.id;

        // Process the response in the context of the pending command
        let mut state = self.state.write().expect("Poisoned lock");

        if let Some(cmd_state) = state.pending_commands.get_mut(&command_id) {
            match response.msg_type {
                MessageType::PingResponse => {
                    // Extract and complete the ping command
                    if let CommandType::Ping(sender) =
                        std::mem::replace(&mut cmd_state.command_type, CommandType::Ping(None))
                    {
                        if let Some(tx) = sender {
                            let _ = tx.send(Ok(true));
                        }
                        // Remove this command
                        state.pending_commands.remove(&command_id);
                    }
                }

                MessageType::FindResponse { value, nodes } => {
                    // Process find response
                    // If we found a value, store it and complete
                    if let Some(val) = value {
                        cmd_state.found_value = Some(val);
                        self.complete_command(&mut state, command_id);
                        return;
                    }

                    cmd_state.pending_count -= 1;

                    // Process new nodes
                    self.process_find_nodes(&mut state, command_id, nodes);
                }

                MessageType::StoreResponse => {
                    // Simply decrement pending count and check completion
                    cmd_state.pending_count -= 1;
                    self.check_command_completion(&mut state, command_id);
                }

                // Ignore request messages in response handler
                _ => {}
            }
        }
    }

    /// Process nodes from a find response
    fn process_find_nodes(
        &self,
        state: &mut KademliaState,
        command_id: Ulid,
        nodes: Vec<NodeAddr>,
    ) {
        // Add new nodes to our routing table and track as closest
        for node_addr in &nodes {
            if node_addr.node_id != self.node_addr.node_id {
                self.update_node_seen(state, node_addr.clone());
                let cmd_state = state
                    .pending_commands
                    .get_mut(&command_id)
                    .expect("Command state not found");
                cmd_state.add_closest_node(node_addr.clone());
            }
        }
        let cmd_state = state
            .pending_commands
            .get_mut(&command_id)
            .expect("Command state not found");

        // Find new nodes to query
        let new_nodes: Vec<_> = nodes
            .into_iter()
            .filter(|addr| {
                !cmd_state.visited_nodes.contains(&addr.node_id)
                    && addr.node_id != self.node_addr.node_id
            })
            .collect();

        if new_nodes.is_empty() || cmd_state.pending_count == 0 {
            // No more nodes to query or no pending requests, check completion
            self.check_command_completion(state, command_id);
            return;
        }

        // Copy target for query_nodes
        let target = cmd_state.target;

        // Find closest new nodes and query them
        self.query_nodes(state, command_id, new_nodes, target);
    }

    /// Check if a command is complete
    fn check_command_completion(&self, state: &mut KademliaState, command_id: Ulid) {
        if let Some(cmd_state) = state.pending_commands.get(&command_id) {
            if cmd_state.pending_count == 0 || cmd_state.found_value.is_some() {
                self.complete_command(state, command_id);
            }
        }
    }

    /// Complete a command and send the result
    fn complete_command(&self, state: &mut KademliaState, command_id: Ulid) {
        if let Some(mut cmd_state) = state.pending_commands.remove(&command_id) {
            match &mut cmd_state.command_type {
                CommandType::Find(sender) => {
                    if let Some(s) = sender.take() {
                        let _ = s.send(Ok(FindResult {
                            value: cmd_state.get_found_value(),
                            nodes: cmd_state.get_closest_nodes(),
                        }));
                    }
                }
                CommandType::Store(sender) => {
                    if let Some(s) = sender.take() {
                        let _ = s.send(Ok(()));
                    }
                }
                CommandType::Ping(sender) => {
                    if let Some(s) = sender.take() {
                        let _ = s.send(Ok(true));
                    }
                }
            }
        }
    }

    /// Query nodes for a target
    fn query_nodes(
        &self,
        state: &mut KademliaState,
        command_id: Ulid,
        nodes: Vec<NodeAddr>,
        target: [u8; 32],
    ) {
        if let Some(cmd_state) = state.pending_commands.get_mut(&command_id) {
            // Sort nodes by distance to target
            let mut with_distance: Vec<_> = nodes
                .into_iter()
                .map(|addr| {
                    let distance = calculate_distance(&target, addr.node_id.as_bytes());
                    (addr, distance)
                })
                .collect();

            with_distance.sort_by(|a, b| a.1.cmp(&b.1));

            // Take up to ALPHA closest nodes
            let closest = with_distance
                .into_iter()
                .take(ALPHA)
                .map(|(addr, _)| addr)
                .collect::<Vec<_>>();

            if closest.is_empty() {
                self.check_command_completion(state, command_id);
                return;
            }

            // Send Find messages to closest nodes
            for addr in closest {
                cmd_state.visited_nodes.insert(addr.node_id);
                cmd_state.pending_count += 1;

                // Send message
                let message = KademliaMessage::new_request(
                    command_id,
                    self.node_addr.clone(),
                    MessageType::FindRequest { target },
                );

                self.send_message(message, addr);
            }
        }
    }

    /// Update a node's info in the appropriate k-bucket
    fn update_node_seen(&self, state: &mut KademliaState, addr: NodeAddr) {
        let node_id = addr.node_id;

        // Don't track ourselves
        if node_id == self.node_addr.node_id {
            return;
        }

        // Store in our quick lookup map
        state.node_addresses.insert(node_id, addr.clone());

        // Calculate bucket index
        let distance = calculate_distance(&self.node_id_bytes(), node_id.as_bytes());
        let bucket_idx = get_bucket_index(&distance);

        // Create new node info
        let node_info = NodeInfo::new(addr.clone());

        // Try to update the bucket
        if let Some(least_recent_addr) = state.k_buckets[bucket_idx].update(node_info) {
            // Only ping if it's not us
            if least_recent_addr.node_id != self.node_addr.node_id {
                self.spawn_ping(state, least_recent_addr);
            }
        }
    }

    /// Send a message to a node
    fn send_message(&self, message: KademliaMessage, target_addr: NodeAddr) {






    }

    /// Find the closest nodes to a target from our routing table
    fn find_closest_nodes(&self, target: &[u8; 32]) -> Vec<NodeAddr> {
        let state = self.state.read().unwrap();
        self.find_closest_nodes_inner(&state, target)
    }

    /// Inner implementation of find_closest_nodes that takes a state reference
    fn find_closest_nodes_inner(&self, state: &KademliaState, target: &[u8; 32]) -> Vec<NodeAddr> {
        let distance_to_target = calculate_distance(&self.node_id_bytes(), target);
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

    /// Spawn a ping command
    fn spawn_ping(
        &self,
        state: &mut KademliaState,
        target: NodeAddr,
    ) -> oneshot::Receiver<Result<bool>> {
        let (sender, receiver) = oneshot::channel();

        // Create ping command
        let command_type = CommandType::Ping(Some(sender));

        // Create command state
        let mut cmd_state = CommandState::new([0u8; 32], command_type); // Target doesn't matter for ping
        cmd_state.pending_count = 1;

        // Generate ID and store command
        let id = Ulid::new();
        state.pending_commands.insert(id, cmd_state);

        // Send ping message
        let message =
            KademliaMessage::new_request(id, self.node_addr.clone(), MessageType::PingRequest);

        self.send_message(message, target);

        receiver
    }

    /// External API: Find operation
    pub async fn find(&self, target: [u8; 32]) -> Result<FindResult> {
        // First check if we have the value locally
        let local_value = {
            let state = self.state.read().unwrap();
            state
                .resources
                .get(&target)
                .and_then(|node_id| state.node_addresses.get(node_id).cloned())
                .map(|addr| FindResult {
                    value: Some(addr),
                    nodes: Vec::new(),
                })
        };

        if let Some(result) = local_value {
            return Ok(result);
        }

        // Find closest nodes without holding a lock for too long
        let closest_nodes = self.find_closest_nodes(&target);

        // If no nodes found, return empty result
        if closest_nodes.is_empty() {
            return Ok(FindResult {
                value: None,
                nodes: Vec::new(),
            });
        }

        // Create oneshot channel
        let (sender, receiver) = oneshot::channel();
        let command_id = Ulid::new();
        let command_type = CommandType::Find(Some(sender));

        // Setup the find operation
        {
            let mut state = self.state.write().unwrap();

            // Initialize state
            let mut cmd_state = CommandState::new(target, command_type);

            // Send find messages to closest nodes
            for addr in &closest_nodes {
                if addr.node_id != self.node_addr.node_id {
                    cmd_state.visited_nodes.insert(addr.node_id);
                    cmd_state.pending_count += 1;

                    // Prepare message
                    let message = KademliaMessage::new_request(
                        command_id,
                        self.node_addr.clone(),
                        MessageType::FindRequest { target },
                    );

                    // Send message
                    self.send_message(message, addr.clone());
                }
            }

            // Store command
            state.pending_commands.insert(command_id, cmd_state);
        }

        // Wait for result
        receiver
            .await
            .map_err(|_| anyhow!("Find operation failed"))?
    }

    /// External API: Store operation
    pub async fn store(&self, key: [u8; 32], value: NodeAddr) -> Result<()> {
        // Store locally first
        {
            let mut state = self.state.write().unwrap();
            state.resources.insert(key, value.node_id);
            state.node_addresses.insert(value.node_id, value.clone());
        }

        // Find nodes closest to the key without holding the lock
        let closest_nodes = self.find_closest_nodes(&key);

        // If no nodes, we're done
        if closest_nodes.is_empty() {
            return Ok(());
        }

        // Create oneshot channel
        let (sender, receiver) = oneshot::channel();
        let command_id = Ulid::new();

        // Setup the store operation
        {
            let mut state = self.state.write().unwrap();

            // Create command type
            let command_type = CommandType::Store(Some(sender));

            // Initialize state
            let mut cmd_state = CommandState::new(key, command_type);

            // Send store messages to closest nodes
            for addr in closest_nodes {
                if addr.node_id != self.node_addr.node_id {
                    // Track this node
                    cmd_state.visited_nodes.insert(addr.node_id);
                    cmd_state.pending_count += 1;

                    // Send message
                    let message = KademliaMessage::new_request(
                        command_id,
                        self.node_addr.clone(),
                        MessageType::StoreRequest {
                            key,
                            value: value.clone(),
                        },
                    );

                    self.send_message(message, addr);
                }
            }

            if cmd_state.pending_count > 0 {
                // Store command if we sent any messages
                state.pending_commands.insert(command_id, cmd_state);
            } else {
                // No messages sent, complete immediately
                return Ok(());
            }
        }

        // Wait for result
        receiver.await?
    }

    /// Bootstrap the node by adding it to an existing Kademlia network
    pub async fn bootstrap(&self, bootstrap_nodes: Vec<NodeAddr>) -> Result<Vec<NodeAddr>> {
        if bootstrap_nodes.is_empty() {
            return Err(anyhow!("No bootstrap nodes provided"));
        }

        // Add bootstrap nodes to our routing table
        {
            let mut state = self.state.write().unwrap();
            for addr in bootstrap_nodes.clone() {
                self.update_node_seen(&mut state, addr);
            }
        }

        // Find our own node ID in the network to discover closest nodes
        let target = self.node_id_bytes();
        let find_result = self.find(target).await?;

        // Store our node in the network
        self.store(target, self.node_addr.clone()).await?;

        Ok(find_result.nodes)
    }

    // Utility functions for direct access to resources
    pub fn store_local(&self, key: [u8; 32], value: NodeId) {
        let mut state = self.state.write().unwrap();
        state.resources.insert(key, value);
    }

    pub fn lookup_local(&self, key: &[u8; 32]) -> Option<NodeId> {
        let state = self.state.read().unwrap();
        state.resources.get(key).cloned()
    }

    pub fn lookup_local_addr(&self, key: &[u8; 32]) -> Option<NodeAddr> {
        let state = self.state.read().unwrap();
        state
            .resources
            .get(key)
            .and_then(|node_id| state.node_addresses.get(node_id).cloned())
    }

    pub fn get_all_nodes(&self) -> Vec<NodeAddr> {
        let state = self.state.read().unwrap();
        state.node_addresses.values().cloned().collect()
    }
}
