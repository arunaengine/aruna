use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use aruna_core::events::DhtEntry;
use aruna_core::id::{DhtKeyId, NodeId, NodeIdExt};
use aruna_core::util::xor_distance_32;
use smallvec::SmallVec;

use super::constants::{DRIVER_TICK_INTERVAL, LOOKUP_ALPHA, LOOKUP_MAX_QUERIES, RPC_TIMEOUT_TICKS};
use super::kbucket::{InsertResult, K, PeerInfo, RoutingTable};
use super::protocol::{
    CLEANUP_OP_ID, DhtCmd, DhtEffect, DhtInput, DhtIo, DhtIoError, DhtIoRequest, DhtOutput,
    DhtOutputValue, INTERNAL_OP_START, InboundId, OpId, RpcPhase, StorageStage,
};
use super::rpc::{DhtRequest, DhtResponse, ErrorCode, StoredValue};
use super::storage::{CLEANUP_PAGE_SIZE, StoredEntry, live_entries, merge_entry};

type PendingMap = HashMap<PendingKey, PendingMeta>;

#[derive(Debug, Clone, Copy)]
struct PendingMeta {
    deadline_tick: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PendingKey {
    Rpc { phase: RpcPhase, peer: NodeId },
    Storage { stage: StorageStage },
}

pub struct DhtStateMachine {
    local_id: NodeId,
    secret_key: iroh::SecretKey,
    routing_table: RoutingTable,
    next_internal_op_id: OpId,
    ops: HashMap<OpId, OpState>,
    cleanup_cursor: Option<Vec<u8>>,
    cleanup_inflight: bool,
    current_tick: u64,
    now_secs: u64,
}

impl std::fmt::Debug for DhtStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtStateMachine")
            .field("local_id", &self.local_id)
            .field("routing_table_size", &self.routing_table.len())
            .field("ops", &self.ops.len())
            .field("cleanup_cursor", &self.cleanup_cursor)
            .field("cleanup_inflight", &self.cleanup_inflight)
            .field("current_tick", &self.current_tick)
            .field("now_secs", &self.now_secs)
            .finish()
    }
}

#[derive(Debug)]
enum OpState {
    Put(PutOp),
    Get(GetOp),
    Bootstrap(BootstrapOp),
    EvictionPing(EvictionPingOp),
    MaintenancePing(MaintenancePingOp),
    InboundGet(InboundGetOp),
    InboundPut(InboundPutOp),
}

impl OpState {
    fn pending(&self) -> &PendingMap {
        match self {
            Self::Put(op) => &op.pending,
            Self::Get(op) => &op.pending,
            Self::Bootstrap(op) => &op.pending,
            Self::EvictionPing(op) => &op.pending,
            Self::MaintenancePing(op) => &op.pending,
            Self::InboundGet(op) => &op.pending,
            Self::InboundPut(op) => &op.pending,
        }
    }

    fn pending_mut(&mut self) -> &mut PendingMap {
        match self {
            Self::Put(op) => &mut op.pending,
            Self::Get(op) => &mut op.pending,
            Self::Bootstrap(op) => &mut op.pending,
            Self::EvictionPing(op) => &mut op.pending,
            Self::MaintenancePing(op) => &mut op.pending,
            Self::InboundGet(op) => &mut op.pending,
            Self::InboundPut(op) => &mut op.pending,
        }
    }
}

#[derive(Debug)]
struct PutOp {
    key: DhtKeyId,
    value: Vec<u8>,
    ttl_secs: u64,
    signature: iroh::Signature,
    frontier: LookupFrontier,
    lookup_finished: bool,
    pending: PendingMap,
}

#[derive(Debug)]
struct GetOp {
    key: DhtKeyId,
    values: Vec<DhtEntry>,
    seen_publishers: HashSet<NodeId>,
    frontier: LookupFrontier,
    pending: PendingMap,
}

#[derive(Debug)]
struct BootstrapOp {
    active_nodes: usize,
    pending: PendingMap,
}

#[derive(Debug)]
struct EvictionPingOp {
    oldest_node: NodeId,
    bucket_idx: usize,
    pending_peer: PeerInfo,
    pending: PendingMap,
}

#[derive(Debug)]
struct MaintenancePingOp {
    peer: NodeId,
    pending: PendingMap,
}

#[derive(Debug)]
struct InboundGetOp {
    inbound_id: InboundId,
    key: DhtKeyId,
    pending: PendingMap,
}

#[derive(Debug)]
struct InboundPutOp {
    inbound_id: InboundId,
    key: DhtKeyId,
    new_entry: StoredEntry,
    pending: PendingMap,
}

#[derive(Debug, Default)]
struct LookupFrontier {
    discovered: HashSet<NodeId>,
    queried: HashSet<NodeId>,
    pending: Vec<NodeId>,
    sent_queries: usize,
}

impl LookupFrontier {
    fn add_candidate(&mut self, node_id: NodeId, local_id: NodeId) {
        if node_id == local_id {
            return;
        }

        if !self.discovered.insert(node_id) {
            return;
        }

        if !self.queried.contains(&node_id) {
            self.pending.push(node_id);
        }
    }

    fn add_candidates<I: IntoIterator<Item = NodeId>>(&mut self, nodes: I, local_id: NodeId) {
        for node_id in nodes {
            self.add_candidate(node_id, local_id);
        }
    }

    fn pop_next(&mut self, target: &[u8; 32]) -> Option<NodeId> {
        if self.pending.is_empty() {
            return None;
        }

        self.pending
            .sort_unstable_by(|a, b| compare_distance(target, a, b));
        let next = self.pending.remove(0);
        self.queried.insert(next);
        Some(next)
    }

    fn closest_discovered(&self, target: &[u8; 32], count: usize) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self.discovered.iter().copied().collect();
        nodes.sort_unstable_by(|a, b| compare_distance(target, a, b));
        nodes.truncate(count);
        nodes
    }

    fn pending_exhausted(&self) -> bool {
        self.pending.is_empty() || self.sent_queries >= LOOKUP_MAX_QUERIES
    }
}

impl DhtStateMachine {
    pub fn new(local_id: NodeId, secret_key: iroh::SecretKey, start_unix_secs: u64) -> Self {
        Self {
            local_id,
            secret_key,
            routing_table: RoutingTable::new(local_id),
            next_internal_op_id: INTERNAL_OP_START,
            ops: HashMap::new(),
            cleanup_cursor: None,
            cleanup_inflight: false,
            current_tick: 0,
            now_secs: start_unix_secs,
        }
    }

    pub fn step(&mut self, input: DhtInput) -> SmallVec<[DhtEffect; 4]> {
        let mut out = SmallVec::<[DhtEffect; 4]>::new();

        match input {
            DhtInput::Cmd(cmd) => self.handle_cmd(cmd, &mut out),
            DhtInput::Io(io) => self.handle_io(io, &mut out),
            DhtInput::Tick { now_tick } => self.handle_tick(now_tick, &mut out),
        }

        out
    }

    fn handle_cmd(&mut self, cmd: DhtCmd, out: &mut SmallVec<[DhtEffect; 4]>) {
        match cmd {
            DhtCmd::Put {
                op_id,
                key,
                value,
                ttl,
            } => self.handle_cmd_put(op_id, key, value, ttl, out),
            DhtCmd::Get { op_id, key } => self.handle_cmd_get(op_id, key, out),
            DhtCmd::Bootstrap { op_id, nodes } => self.handle_cmd_bootstrap(op_id, nodes, out),
            DhtCmd::RoutingTableSize { op_id } => {
                out.push(DhtEffect::Output(DhtOutput::Completed {
                    op_id,
                    result: DhtOutputValue::RoutingTableSize(self.routing_table.len()),
                }));
            }
            DhtCmd::AddPeer { node_id } => self.insert_peer(node_id, out),
        }
    }

    fn handle_cmd_put(
        &mut self,
        op_id: OpId,
        key: DhtKeyId,
        value: Vec<u8>,
        ttl: std::time::Duration,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if self.ops.contains_key(&op_id) {
            out.push(DhtEffect::Output(DhtOutput::Failed {
                op_id,
                error: DhtIoError::Network("duplicate op id".to_string()),
            }));
            return;
        }

        let ttl_secs = ttl.as_secs();
        let mut signed_data = Vec::with_capacity(32 + value.len() + 8);
        signed_data.extend_from_slice(key.as_bytes());
        signed_data.extend_from_slice(&value);
        signed_data.extend_from_slice(&ttl_secs.to_le_bytes());
        let signature = self.secret_key.sign(&signed_data);

        let mut op = OpState::Put(PutOp {
            key,
            value,
            ttl_secs,
            signature,
            frontier: LookupFrontier::default(),
            lookup_finished: false,
            pending: HashMap::new(),
        });
        self.queue_storage_read(op_id, &mut op, StorageStage::PutLocalRead, key, out);
        self.ops.insert(op_id, op);
    }

    fn handle_cmd_get(&mut self, op_id: OpId, key: DhtKeyId, out: &mut SmallVec<[DhtEffect; 4]>) {
        if self.ops.contains_key(&op_id) {
            out.push(DhtEffect::Output(DhtOutput::Failed {
                op_id,
                error: DhtIoError::Network("duplicate op id".to_string()),
            }));
            return;
        }

        let mut op = OpState::Get(GetOp {
            key,
            values: Vec::new(),
            seen_publishers: HashSet::new(),
            frontier: LookupFrontier::default(),
            pending: HashMap::new(),
        });
        self.queue_storage_read(op_id, &mut op, StorageStage::GetLocalRead, key, out);
        self.ops.insert(op_id, op);
    }

    fn handle_cmd_bootstrap(
        &mut self,
        op_id: OpId,
        nodes: Vec<NodeId>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if self.ops.contains_key(&op_id) {
            out.push(DhtEffect::Output(DhtOutput::Failed {
                op_id,
                error: DhtIoError::Network("duplicate op id".to_string()),
            }));
            return;
        }

        let mut unique: Vec<NodeId> = nodes
            .into_iter()
            .filter(|node| *node != self.local_id)
            .collect();
        unique.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        unique.dedup();

        if unique.is_empty() {
            out.push(DhtEffect::Output(DhtOutput::Completed {
                op_id,
                result: DhtOutputValue::Unit,
            }));
            return;
        }

        let mut op = OpState::Bootstrap(BootstrapOp {
            active_nodes: 0,
            pending: HashMap::new(),
        });
        for node in unique {
            self.queue_rpc(
                op_id,
                &mut op,
                RpcPhase::Bootstrap,
                node,
                DhtRequest::Ping,
                out,
            );
        }
        self.ops.insert(op_id, op);
    }

    fn handle_io(&mut self, io: DhtIo, out: &mut SmallVec<[DhtEffect; 4]>) {
        match io {
            DhtIo::RpcResponse {
                op_id,
                phase,
                peer,
                response,
            } => self.handle_rpc_response(op_id, phase, peer, response, out),
            DhtIo::RpcError {
                op_id,
                phase,
                peer,
                error,
            } => self.handle_rpc_error(op_id, phase, peer, error, out),
            DhtIo::InboundRequest {
                inbound_id,
                peer,
                request,
            } => {
                self.insert_peer(peer, out);
                self.handle_inbound_request(inbound_id, request, out);
            }
            DhtIo::InboundDropped { .. } => {}
            DhtIo::StorageReadResult {
                op_id,
                stage,
                entries,
            } => self.handle_storage_read(op_id, stage, entries, out),
            DhtIo::StorageWriteResult { op_id, stage } => {
                self.handle_storage_write(op_id, stage, out)
            }
            DhtIo::StorageDeleteResult { op_id, stage } => {
                self.handle_storage_delete(op_id, stage, out)
            }
            DhtIo::StorageIterResult {
                op_id,
                stage,
                values,
                next_start_after,
            } => self.handle_storage_iter(op_id, stage, values, next_start_after, out),
            DhtIo::StorageError {
                op_id,
                stage,
                error,
            } => self.handle_storage_error(op_id, stage, error, out),
            DhtIo::PeerSeen { peer } => self.insert_peer(peer, out),
        }
    }

    fn handle_tick(&mut self, now_tick: u64, out: &mut SmallVec<[DhtEffect; 4]>) {
        let previous_tick = self.current_tick;
        self.current_tick = now_tick;
        if now_tick > previous_tick {
            let delta = now_tick.saturating_sub(previous_tick);
            self.now_secs = self
                .now_secs
                .saturating_add(delta.saturating_mul(DRIVER_TICK_INTERVAL.as_secs()));
        }

        let mut timed_out = self.collect_timed_out_rpc();
        timed_out.sort_unstable_by(|(op_a, phase_a, peer_a), (op_b, phase_b, peer_b)| {
            op_a.cmp(op_b)
                .then_with(|| rpc_phase_order(*phase_a).cmp(&rpc_phase_order(*phase_b)))
                .then_with(|| peer_a.as_bytes().cmp(peer_b.as_bytes()))
        });

        for (op_id, phase, peer) in timed_out {
            self.handle_rpc_error(op_id, phase, peer, DhtIoError::Timeout, out);
        }

        self.schedule_maintenance_ping(now_tick, out);
        self.schedule_cleanup(out);
    }

    fn handle_rpc_response(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        response: DhtResponse,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let Some(mut op_state) = self.ops.remove(&op_id) else {
            return;
        };
        if !take_pending_rpc(op_state.pending_mut(), phase, peer) {
            self.ops.insert(op_id, op_state);
            return;
        }

        match op_state {
            OpState::Put(mut op) => {
                if phase == RpcPhase::PutLookup {
                    match response {
                        DhtResponse::Nodes { nodes } => {
                            op.frontier.add_candidates(nodes, self.local_id);
                        }
                        DhtResponse::Value { closer_nodes, .. } => {
                            op.frontier.add_candidates(closer_nodes, self.local_id);
                        }
                        DhtResponse::Pong | DhtResponse::Stored | DhtResponse::Error { .. } => {}
                    }

                    self.dispatch_put_lookup_requests(op_id, &mut op, out);
                    if self.put_lookup_exhausted(&op) {
                        self.begin_put_store(op_id, &mut op, out);
                    }
                }

                self.maybe_complete_put(op_id, op, out);
            }
            OpState::Get(mut op) => {
                if phase != RpcPhase::GetLookup {
                    self.ops.insert(op_id, OpState::Get(op));
                    return;
                }

                match response {
                    DhtResponse::Value {
                        entries,
                        closer_nodes,
                    } => {
                        let now = self.now_secs;
                        for entry in entries {
                            if entry.expires_at <= now {
                                continue;
                            }
                            if op.seen_publishers.insert(entry.publisher) {
                                op.values.push(DhtEntry {
                                    node_id: entry.publisher,
                                    value: entry.value,
                                    expires_at: entry.expires_at,
                                });
                            }
                        }

                        for node_id in &closer_nodes {
                            self.routing_table.insert(PeerInfo::new(*node_id));
                        }
                        op.frontier.add_candidates(closer_nodes, self.local_id);
                    }
                    DhtResponse::Nodes { nodes } => {
                        for node_id in &nodes {
                            self.routing_table.insert(PeerInfo::new(*node_id));
                        }
                        op.frontier.add_candidates(nodes, self.local_id);
                    }
                    DhtResponse::Pong | DhtResponse::Stored | DhtResponse::Error { .. } => {}
                }

                self.dispatch_get_requests(op_id, &mut op, out);
                self.maybe_complete_get(op_id, op, out);
            }
            OpState::Bootstrap(mut op) => {
                if phase != RpcPhase::Bootstrap {
                    self.ops.insert(op_id, OpState::Bootstrap(op));
                    return;
                }
                if matches!(response, DhtResponse::Pong) {
                    self.routing_table.insert(PeerInfo::new(peer));
                    op.active_nodes = op.active_nodes.saturating_add(1);
                }
                self.finish_bootstrap(
                    op_id,
                    op,
                    DhtIoError::Network("no bootstrap nodes reachable".to_string()),
                    out,
                );
            }
            OpState::EvictionPing(op) => {
                if phase != RpcPhase::EvictionPing {
                    self.ops.insert(op_id, OpState::EvictionPing(op));
                    return;
                }

                let peer_alive = matches!(response, DhtResponse::Pong);
                if !peer_alive {
                    let _ = self.routing_table.remove(&op.oldest_node);
                    self.routing_table.insert(op.pending_peer.clone());
                }

                if pending_rpc_count(&op.pending, RpcPhase::EvictionPing) > 0 {
                    self.ops.insert(op_id, OpState::EvictionPing(op));
                }
            }
            OpState::MaintenancePing(op) => {
                if phase != RpcPhase::MaintenancePing {
                    self.ops.insert(op_id, OpState::MaintenancePing(op));
                    return;
                }

                if matches!(response, DhtResponse::Pong) {
                    self.routing_table.insert(PeerInfo::new(peer));
                }

                if pending_rpc_count(&op.pending, RpcPhase::MaintenancePing) > 0 {
                    self.ops.insert(op_id, OpState::MaintenancePing(op));
                }
            }
            OpState::InboundGet(op) => {
                self.ops.insert(op_id, OpState::InboundGet(op));
            }
            OpState::InboundPut(op) => {
                self.ops.insert(op_id, OpState::InboundPut(op));
            }
        }
    }

    fn handle_rpc_error(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        error: DhtIoError,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let Some(mut op_state) = self.ops.remove(&op_id) else {
            return;
        };
        if !take_pending_rpc(op_state.pending_mut(), phase, peer) {
            self.ops.insert(op_id, op_state);
            return;
        }

        match op_state {
            OpState::Put(mut op) => {
                if phase == RpcPhase::PutLookup {
                    self.dispatch_put_lookup_requests(op_id, &mut op, out);
                    if self.put_lookup_exhausted(&op) {
                        self.begin_put_store(op_id, &mut op, out);
                    }
                }

                self.maybe_complete_put(op_id, op, out);
            }
            OpState::Get(mut op) => {
                if phase != RpcPhase::GetLookup {
                    self.ops.insert(op_id, OpState::Get(op));
                    return;
                }

                self.dispatch_get_requests(op_id, &mut op, out);
                self.maybe_complete_get(op_id, op, out);
            }
            OpState::Bootstrap(op) => {
                if phase != RpcPhase::Bootstrap {
                    self.ops.insert(op_id, OpState::Bootstrap(op));
                    return;
                }
                self.finish_bootstrap(op_id, op, error, out);
            }
            OpState::EvictionPing(op) => {
                if phase != RpcPhase::EvictionPing {
                    self.ops.insert(op_id, OpState::EvictionPing(op));
                    return;
                }

                let _ = self.routing_table.evict_oldest(op.bucket_idx);
                self.routing_table.insert(op.pending_peer.clone());
            }
            OpState::MaintenancePing(op) => {
                if phase != RpcPhase::MaintenancePing {
                    self.ops.insert(op_id, OpState::MaintenancePing(op));
                    return;
                }

                let _ = self.routing_table.remove(&op.peer);
            }
            OpState::InboundGet(op) => {
                self.ops.insert(op_id, OpState::InboundGet(op));
            }
            OpState::InboundPut(op) => {
                self.ops.insert(op_id, OpState::InboundPut(op));
            }
        }
    }

    fn handle_storage_read(
        &mut self,
        op_id: OpId,
        stage: StorageStage,
        entries: Vec<StoredEntry>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if op_id == CLEANUP_OP_ID {
            return;
        }

        let Some(mut op_state) = self.ops.remove(&op_id) else {
            return;
        };
        if !take_pending_storage(op_state.pending_mut(), stage) {
            self.ops.insert(op_id, op_state);
            return;
        }

        match op_state {
            OpState::Put(mut op) => {
                if stage != StorageStage::PutLocalRead {
                    self.ops.insert(op_id, OpState::Put(op));
                    return;
                }

                let new_entry = StoredEntry {
                    publisher: self.local_id,
                    value: op.value.clone(),
                    expires_at: self.now_secs.saturating_add(op.ttl_secs),
                    signature: Some(op.signature),
                };
                let merged = merge_entry(entries, new_entry, self.now_secs);
                self.queue_storage_write_pending(
                    op_id,
                    &mut op.pending,
                    StorageStage::PutLocalWrite,
                    op.key,
                    merged,
                    out,
                );
                self.ops.insert(op_id, OpState::Put(op));
            }
            OpState::Get(mut op) => {
                if stage != StorageStage::GetLocalRead {
                    self.ops.insert(op_id, OpState::Get(op));
                    return;
                }

                let now = self.now_secs;
                for entry in live_entries(entries, now) {
                    if op.seen_publishers.insert(entry.publisher) {
                        op.values.push(DhtEntry {
                            node_id: entry.publisher,
                            value: entry.value,
                            expires_at: entry.expires_at,
                        });
                    }
                }

                op.frontier.add_candidates(
                    self.routing_table
                        .closest(op.key.as_bytes(), K)
                        .into_iter()
                        .map(|peer| peer.node_id),
                    self.local_id,
                );

                self.dispatch_get_requests(op_id, &mut op, out);
                self.maybe_complete_get(op_id, op, out);
            }
            OpState::InboundGet(op) => {
                if stage != StorageStage::InboundGetRead {
                    self.ops.insert(op_id, OpState::InboundGet(op));
                    return;
                }

                let now = self.now_secs;
                let values: Vec<StoredValue> = live_entries(entries, now)
                    .into_iter()
                    .map(|entry| StoredValue {
                        publisher: entry.publisher,
                        value: entry.value,
                        expires_at: entry.expires_at,
                        signature: entry.signature,
                    })
                    .collect();
                let closer_nodes = self
                    .routing_table
                    .closest(op.key.as_bytes(), K)
                    .into_iter()
                    .map(|peer| peer.node_id)
                    .collect();

                out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                    inbound_id: op.inbound_id,
                    response: DhtResponse::Value {
                        entries: values,
                        closer_nodes,
                    },
                }));
            }
            OpState::InboundPut(mut op) => {
                if stage != StorageStage::InboundPutRead {
                    self.ops.insert(op_id, OpState::InboundPut(op));
                    return;
                }

                let merged = merge_entry(entries, op.new_entry.clone(), self.now_secs);
                self.queue_storage_write_pending(
                    op_id,
                    &mut op.pending,
                    StorageStage::InboundPutWrite,
                    op.key,
                    merged,
                    out,
                );
                self.ops.insert(op_id, OpState::InboundPut(op));
            }
            OpState::Bootstrap(op) => {
                self.ops.insert(op_id, OpState::Bootstrap(op));
            }
            OpState::EvictionPing(op) => {
                self.ops.insert(op_id, OpState::EvictionPing(op));
            }
            OpState::MaintenancePing(op) => {
                self.ops.insert(op_id, OpState::MaintenancePing(op));
            }
        }
    }

    fn handle_storage_write(
        &mut self,
        op_id: OpId,
        stage: StorageStage,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if op_id == CLEANUP_OP_ID {
            return;
        }

        let Some(mut op_state) = self.ops.remove(&op_id) else {
            return;
        };
        if !take_pending_storage(op_state.pending_mut(), stage) {
            self.ops.insert(op_id, op_state);
            return;
        }

        match op_state {
            OpState::Put(mut op) => {
                if stage != StorageStage::PutLocalWrite {
                    self.ops.insert(op_id, OpState::Put(op));
                    return;
                }

                op.frontier.add_candidates(
                    self.routing_table
                        .closest(op.key.as_bytes(), K)
                        .into_iter()
                        .map(|peer| peer.node_id),
                    self.local_id,
                );

                self.dispatch_put_lookup_requests(op_id, &mut op, out);
                if self.put_lookup_exhausted(&op) {
                    self.begin_put_store(op_id, &mut op, out);
                }
                self.maybe_complete_put(op_id, op, out);
            }
            OpState::InboundPut(op) => {
                if stage != StorageStage::InboundPutWrite {
                    self.ops.insert(op_id, OpState::InboundPut(op));
                    return;
                }

                out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                    inbound_id: op.inbound_id,
                    response: DhtResponse::Stored,
                }));
            }
            OpState::Get(op) => {
                self.ops.insert(op_id, OpState::Get(op));
            }
            OpState::Bootstrap(op) => {
                self.ops.insert(op_id, OpState::Bootstrap(op));
            }
            OpState::EvictionPing(op) => {
                self.ops.insert(op_id, OpState::EvictionPing(op));
            }
            OpState::MaintenancePing(op) => {
                self.ops.insert(op_id, OpState::MaintenancePing(op));
            }
            OpState::InboundGet(op) => {
                self.ops.insert(op_id, OpState::InboundGet(op));
            }
        }
    }

    fn handle_storage_delete(
        &mut self,
        op_id: OpId,
        stage: StorageStage,
        _out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if op_id == CLEANUP_OP_ID {
            return;
        }

        let Some(mut op_state) = self.ops.remove(&op_id) else {
            return;
        };
        let _ = take_pending_storage(op_state.pending_mut(), stage);
        self.ops.insert(op_id, op_state);
    }

    fn handle_storage_iter(
        &mut self,
        op_id: OpId,
        stage: StorageStage,
        values: Vec<(Vec<u8>, Vec<StoredEntry>)>,
        next_start_after: Option<Vec<u8>>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if op_id != CLEANUP_OP_ID || stage != StorageStage::CleanupIter {
            return;
        }

        for (raw_key, entries) in values {
            let Ok(key_bytes): Result<[u8; 32], _> = raw_key.as_slice().try_into() else {
                continue;
            };
            let key = DhtKeyId::from_bytes(key_bytes);

            let original_len = entries.len();
            let live = live_entries(entries, self.now_secs);
            if live.is_empty() {
                out.push(DhtEffect::IoRequest(DhtIoRequest::StorageDelete {
                    op_id: CLEANUP_OP_ID,
                    stage: StorageStage::CleanupDelete,
                    key,
                }));
            } else if live.len() < original_len {
                out.push(DhtEffect::IoRequest(DhtIoRequest::StorageWrite {
                    op_id: CLEANUP_OP_ID,
                    stage: StorageStage::CleanupWrite,
                    key,
                    entries: live,
                }));
            }
        }

        self.cleanup_cursor = next_start_after;
        self.cleanup_inflight = false;
    }

    fn handle_storage_error(
        &mut self,
        op_id: OpId,
        stage: StorageStage,
        error: DhtIoError,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if op_id == CLEANUP_OP_ID {
            if stage == StorageStage::CleanupIter {
                self.cleanup_inflight = false;
            }
            return;
        }

        let Some(mut op_state) = self.ops.remove(&op_id) else {
            return;
        };
        if !take_pending_storage(op_state.pending_mut(), stage) {
            self.ops.insert(op_id, op_state);
            return;
        }

        match op_state {
            OpState::Put(_) | OpState::Get(_) | OpState::Bootstrap(_) => {
                out.push(DhtEffect::Output(DhtOutput::Failed { op_id, error }));
            }
            OpState::InboundGet(op) => {
                out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                    inbound_id: op.inbound_id,
                    response: DhtResponse::Error {
                        code: ErrorCode::Internal,
                        message: "storage error".to_string(),
                    },
                }));
            }
            OpState::InboundPut(op) => {
                out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                    inbound_id: op.inbound_id,
                    response: DhtResponse::Error {
                        code: ErrorCode::Internal,
                        message: "storage error".to_string(),
                    },
                }));
            }
            OpState::EvictionPing(_) | OpState::MaintenancePing(_) => {}
        }
    }

    fn handle_inbound_request(
        &mut self,
        inbound_id: InboundId,
        request: DhtRequest,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        match request {
            DhtRequest::Ping => {
                out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                    inbound_id,
                    response: DhtResponse::Pong,
                }));
            }
            DhtRequest::FindNode { target } => {
                let nodes = self
                    .routing_table
                    .closest(&target, K)
                    .into_iter()
                    .map(|peer| peer.node_id)
                    .collect();

                out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                    inbound_id,
                    response: DhtResponse::Nodes { nodes },
                }));
            }
            DhtRequest::GetValue { key } => {
                let op_id = self.alloc_internal_op_id();
                let mut op = OpState::InboundGet(InboundGetOp {
                    inbound_id,
                    key,
                    pending: HashMap::new(),
                });
                self.queue_storage_read(op_id, &mut op, StorageStage::InboundGetRead, key, out);
                self.ops.insert(op_id, op);
            }
            DhtRequest::PutValue {
                key,
                value,
                ttl_secs,
                publisher,
                signature,
            } => {
                let Some(signature) = signature else {
                    out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                        inbound_id,
                        response: DhtResponse::Error {
                            code: ErrorCode::InvalidSignature,
                            message: "Missing publisher signature".to_string(),
                        },
                    }));
                    return;
                };

                let mut signed_data = Vec::with_capacity(32 + value.len() + 8);
                signed_data.extend_from_slice(key.as_bytes());
                signed_data.extend_from_slice(&value);
                signed_data.extend_from_slice(&ttl_secs.to_le_bytes());

                if publisher.verify(&signed_data, &signature).is_err() {
                    out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                        inbound_id,
                        response: DhtResponse::Error {
                            code: ErrorCode::InvalidSignature,
                            message: "Invalid publisher signature".to_string(),
                        },
                    }));
                    return;
                }

                let new_entry = StoredEntry {
                    publisher,
                    value,
                    expires_at: self.now_secs.saturating_add(ttl_secs),
                    signature: Some(signature),
                };

                let op_id = self.alloc_internal_op_id();
                let mut op = OpState::InboundPut(InboundPutOp {
                    inbound_id,
                    key,
                    new_entry,
                    pending: HashMap::new(),
                });
                self.queue_storage_read(op_id, &mut op, StorageStage::InboundPutRead, key, out);
                self.ops.insert(op_id, op);
            }
        }
    }

    fn dispatch_get_requests(
        &mut self,
        op_id: OpId,
        op: &mut GetOp,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        while pending_rpc_count(&op.pending, RpcPhase::GetLookup) < LOOKUP_ALPHA
            && op.frontier.sent_queries < LOOKUP_MAX_QUERIES
        {
            let Some(peer) = op.frontier.pop_next(op.key.as_bytes()) else {
                break;
            };
            self.queue_rpc_pending(
                op_id,
                &mut op.pending,
                RpcPhase::GetLookup,
                peer,
                DhtRequest::GetValue { key: op.key },
                out,
            );
            op.frontier.sent_queries = op.frontier.sent_queries.saturating_add(1);
        }
    }

    fn dispatch_put_lookup_requests(
        &mut self,
        op_id: OpId,
        op: &mut PutOp,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        while pending_rpc_count(&op.pending, RpcPhase::PutLookup) < LOOKUP_ALPHA
            && op.frontier.sent_queries < LOOKUP_MAX_QUERIES
        {
            let Some(peer) = op.frontier.pop_next(op.key.as_bytes()) else {
                break;
            };
            self.queue_rpc_pending(
                op_id,
                &mut op.pending,
                RpcPhase::PutLookup,
                peer,
                DhtRequest::FindNode {
                    target: *op.key.as_bytes(),
                },
                out,
            );
            op.frontier.sent_queries = op.frontier.sent_queries.saturating_add(1);
        }
    }

    fn put_lookup_exhausted(&self, op: &PutOp) -> bool {
        !op.lookup_finished
            && pending_rpc_count(&op.pending, RpcPhase::PutLookup) == 0
            && op.frontier.pending_exhausted()
    }

    fn begin_put_store(&mut self, op_id: OpId, op: &mut PutOp, out: &mut SmallVec<[DhtEffect; 4]>) {
        if op.lookup_finished {
            return;
        }
        op.lookup_finished = true;

        let targets = op.frontier.closest_discovered(op.key.as_bytes(), K);
        for peer in targets {
            self.queue_rpc_pending(
                op_id,
                &mut op.pending,
                RpcPhase::PutStore,
                peer,
                DhtRequest::PutValue {
                    key: op.key,
                    value: op.value.clone(),
                    ttl_secs: op.ttl_secs,
                    publisher: self.local_id,
                    signature: Some(op.signature),
                },
                out,
            );
        }
    }

    fn maybe_complete_get(&mut self, op_id: OpId, op: GetOp, out: &mut SmallVec<[DhtEffect; 4]>) {
        if pending_rpc_count(&op.pending, RpcPhase::GetLookup) == 0
            && op.frontier.pending_exhausted()
        {
            out.push(DhtEffect::Output(DhtOutput::Completed {
                op_id,
                result: DhtOutputValue::GetValues(op.values),
            }));
        } else {
            self.ops.insert(op_id, OpState::Get(op));
        }
    }

    fn maybe_complete_put(&mut self, op_id: OpId, op: PutOp, out: &mut SmallVec<[DhtEffect; 4]>) {
        if op.lookup_finished && pending_rpc_count(&op.pending, RpcPhase::PutStore) == 0 {
            out.push(DhtEffect::Output(DhtOutput::Completed {
                op_id,
                result: DhtOutputValue::Unit,
            }));
        } else {
            self.ops.insert(op_id, OpState::Put(op));
        }
    }

    fn finish_bootstrap(
        &mut self,
        op_id: OpId,
        op: BootstrapOp,
        error: DhtIoError,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if pending_rpc_count(&op.pending, RpcPhase::Bootstrap) == 0 {
            if op.active_nodes == 0 {
                out.push(DhtEffect::Output(DhtOutput::Failed { op_id, error }));
            } else {
                out.push(DhtEffect::Output(DhtOutput::Completed {
                    op_id,
                    result: DhtOutputValue::Unit,
                }));
            }
        } else {
            self.ops.insert(op_id, OpState::Bootstrap(op));
        }
    }

    fn schedule_maintenance_ping(&mut self, now_tick: u64, out: &mut SmallVec<[DhtEffect; 4]>) {
        let mut peers = self.routing_table.all_peers();
        if peers.is_empty() {
            return;
        }

        peers.sort_unstable_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
        let idx = (now_tick as usize) % peers.len();
        let peer = peers[idx].node_id;

        let op_id = self.alloc_internal_op_id();
        let mut op = OpState::MaintenancePing(MaintenancePingOp {
            peer,
            pending: HashMap::new(),
        });
        self.queue_rpc(
            op_id,
            &mut op,
            RpcPhase::MaintenancePing,
            peer,
            DhtRequest::Ping,
            out,
        );
        self.ops.insert(op_id, op);
    }

    fn schedule_cleanup(&mut self, out: &mut SmallVec<[DhtEffect; 4]>) {
        if self.cleanup_inflight {
            return;
        }

        self.cleanup_inflight = true;
        out.push(DhtEffect::IoRequest(DhtIoRequest::StorageIter {
            op_id: CLEANUP_OP_ID,
            stage: StorageStage::CleanupIter,
            start_after: self.cleanup_cursor.clone(),
            limit: CLEANUP_PAGE_SIZE,
        }));
    }

    fn insert_peer(&mut self, node_id: NodeId, out: &mut SmallVec<[DhtEffect; 4]>) {
        if node_id == self.local_id {
            return;
        }

        let result = self.routing_table.insert(PeerInfo::new(node_id));
        let InsertResult::BucketFull { oldest, pending } = result else {
            return;
        };

        let Some(oldest_peer) = oldest else {
            return;
        };

        let bucket_idx = self.local_id.bucket_index(&oldest_peer.node_id);
        if bucket_idx >= 256 {
            return;
        }

        let op_id = self.alloc_internal_op_id();
        let mut op = OpState::EvictionPing(EvictionPingOp {
            oldest_node: oldest_peer.node_id,
            bucket_idx,
            pending_peer: pending,
            pending: HashMap::new(),
        });
        self.queue_rpc(
            op_id,
            &mut op,
            RpcPhase::EvictionPing,
            oldest_peer.node_id,
            DhtRequest::Ping,
            out,
        );
        self.ops.insert(op_id, op);
    }

    fn queue_rpc(
        &self,
        op_id: OpId,
        op: &mut OpState,
        phase: RpcPhase,
        peer: NodeId,
        request: DhtRequest,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        self.queue_rpc_pending(op_id, op.pending_mut(), phase, peer, request, out);
    }

    fn queue_rpc_pending(
        &self,
        op_id: OpId,
        pending: &mut PendingMap,
        phase: RpcPhase,
        peer: NodeId,
        request: DhtRequest,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let deadline_tick = self.current_tick.saturating_add(RPC_TIMEOUT_TICKS);
        pending.insert(
            PendingKey::Rpc { phase, peer },
            PendingMeta {
                deadline_tick: Some(deadline_tick),
            },
        );

        out.push(DhtEffect::IoRequest(DhtIoRequest::RpcRequest {
            op_id,
            phase,
            peer,
            request,
        }));
    }

    fn queue_storage_read(
        &self,
        op_id: OpId,
        op: &mut OpState,
        stage: StorageStage,
        key: DhtKeyId,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        op.pending_mut().insert(
            PendingKey::Storage { stage },
            PendingMeta {
                deadline_tick: None,
            },
        );

        out.push(DhtEffect::IoRequest(DhtIoRequest::StorageRead {
            op_id,
            stage,
            key,
        }));
    }

    fn queue_storage_write_pending(
        &self,
        op_id: OpId,
        pending: &mut PendingMap,
        stage: StorageStage,
        key: DhtKeyId,
        entries: Vec<StoredEntry>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        pending.insert(
            PendingKey::Storage { stage },
            PendingMeta {
                deadline_tick: None,
            },
        );

        out.push(DhtEffect::IoRequest(DhtIoRequest::StorageWrite {
            op_id,
            stage,
            key,
            entries,
        }));
    }

    fn collect_timed_out_rpc(&self) -> Vec<(OpId, RpcPhase, NodeId)> {
        let mut timed_out = Vec::new();

        for (op_id, op) in &self.ops {
            for (key, meta) in op.pending() {
                let PendingKey::Rpc { phase, peer } = key else {
                    continue;
                };
                if let Some(deadline_tick) = meta.deadline_tick {
                    if deadline_tick <= self.current_tick {
                        timed_out.push((*op_id, *phase, *peer));
                    }
                }
            }
        }

        timed_out
    }

    fn alloc_internal_op_id(&mut self) -> OpId {
        let op_id = self.next_internal_op_id;
        self.next_internal_op_id = self.next_internal_op_id.saturating_add(1);
        op_id
    }
}

fn pending_rpc_count(pending: &PendingMap, phase: RpcPhase) -> usize {
    pending
        .keys()
        .filter(|key| matches!(key, PendingKey::Rpc { phase: p, .. } if *p == phase))
        .count()
}

fn take_pending_rpc(pending: &mut PendingMap, phase: RpcPhase, peer: NodeId) -> bool {
    pending.remove(&PendingKey::Rpc { phase, peer }).is_some()
}

fn take_pending_storage(pending: &mut PendingMap, stage: StorageStage) -> bool {
    pending.remove(&PendingKey::Storage { stage }).is_some()
}

fn compare_distance(target: &[u8; 32], a: &NodeId, b: &NodeId) -> Ordering {
    let dist_a = xor_distance_32(target, a.as_bytes());
    let dist_b = xor_distance_32(target, b.as_bytes());
    dist_a
        .cmp(&dist_b)
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

fn rpc_phase_order(phase: RpcPhase) -> u8 {
    match phase {
        RpcPhase::PutLookup => 0,
        RpcPhase::PutStore => 1,
        RpcPhase::GetLookup => 2,
        RpcPhase::Bootstrap => 3,
        RpcPhase::EvictionPing => 4,
        RpcPhase::MaintenancePing => 5,
    }
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
    fn step_returns_smallvec_capacity_four() {
        let local_secret = iroh::SecretKey::from_bytes(&[7u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let effects = state.step(DhtInput::Cmd(DhtCmd::RoutingTableSize { op_id: 1 }));

        assert!(matches!(
            effects.as_slice(),
            [DhtEffect::Output(DhtOutput::Completed {
                op_id: 1,
                result: DhtOutputValue::RoutingTableSize(0)
            })]
        ));
    }

    #[test]
    fn add_peer_is_sync_and_emits_no_effect_when_not_full() {
        let local_secret = iroh::SecretKey::from_bytes(&[11u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let peer = make_node(1);
        let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
        assert!(effects.is_empty());
    }

    #[test]
    fn deterministic_replay_produces_identical_effects() {
        let local_secret_a = iroh::SecretKey::from_bytes(&[21u8; 32]);
        let local_id_a = local_secret_a.public();
        let local_secret_b = iroh::SecretKey::from_bytes(&[21u8; 32]);
        let local_id_b = local_secret_b.public();

        let mut state_a = DhtStateMachine::new(local_id_a, local_secret_a, 100);
        let mut state_b = DhtStateMachine::new(local_id_b, local_secret_b, 100);

        let key = DhtKeyId::from_data(b"deterministic-key");
        let trace = vec![
            DhtInput::Cmd(DhtCmd::Put {
                op_id: 1,
                key,
                value: b"hello".to_vec(),
                ttl: std::time::Duration::from_secs(30),
            }),
            DhtInput::Io(DhtIo::StorageReadResult {
                op_id: 1,
                stage: StorageStage::PutLocalRead,
                entries: Vec::new(),
            }),
            DhtInput::Io(DhtIo::StorageWriteResult {
                op_id: 1,
                stage: StorageStage::PutLocalWrite,
            }),
        ];

        let outputs_a: Vec<String> = trace
            .iter()
            .map(|input| format!("{:?}", state_a.step(input.clone())))
            .collect();
        let outputs_b: Vec<String> = trace
            .iter()
            .map(|input| format!("{:?}", state_b.step(input.clone())))
            .collect();

        assert_eq!(outputs_a, outputs_b);
    }

    #[test]
    fn late_rpc_response_is_ignored_after_timeout() {
        let local_secret = iroh::SecretKey::from_bytes(&[23u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let peer = make_node(42);
        let effects = state.step(DhtInput::Cmd(DhtCmd::Bootstrap {
            op_id: 5,
            nodes: vec![peer],
        }));

        assert!(effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::IoRequest(DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::Bootstrap,
                    peer: req_peer,
                    ..
                }) if *op_id == 5 && *req_peer == peer
            )
        }));

        let timeout_effects = state.step(DhtInput::Tick {
            now_tick: RPC_TIMEOUT_TICKS,
        });
        assert!(timeout_effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Failed {
                    op_id: 5,
                    error: DhtIoError::Timeout
                })
            )
        }));

        let late_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 5,
            phase: RpcPhase::Bootstrap,
            peer,
            response: DhtResponse::Pong,
        }));
        assert!(late_effects.is_empty());
    }

    #[test]
    fn get_lookup_continues_with_closer_nodes() {
        let local_secret = iroh::SecretKey::from_bytes(&[31u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let first_peer = make_node(1);
        let second_peer = make_node(2);
        let key = DhtKeyId::from_data(b"iterative-get");

        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
            node_id: first_peer,
        }));
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get { op_id: 9, key }));

        let local_read_effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: 9,
            stage: StorageStage::GetLocalRead,
            entries: Vec::new(),
        }));

        assert!(local_read_effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::IoRequest(DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::GetLookup,
                    peer,
                    request: DhtRequest::GetValue { .. },
                }) if *op_id == 9 && *peer == first_peer
            )
        }));

        let follow_up = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 9,
            phase: RpcPhase::GetLookup,
            peer: first_peer,
            response: DhtResponse::Value {
                entries: Vec::new(),
                closer_nodes: vec![second_peer],
            },
        }));

        assert!(follow_up.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::IoRequest(DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::GetLookup,
                    peer,
                    request: DhtRequest::GetValue { .. },
                }) if *op_id == 9 && *peer == second_peer
            )
        }));
    }
}
