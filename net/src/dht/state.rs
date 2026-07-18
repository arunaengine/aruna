use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use aruna_core::DistributedTraceContext;
use aruna_core::events::DhtEntry;
use aruna_core::id::{DhtKeyId, NodeId, NodeIdExt};
use aruna_core::structs::RealmId;
use aruna_core::util::xor_distance_32;
use smallvec::SmallVec;

use super::constants::{
    LOOKUP_ALPHA, LOOKUP_MAX_QUERIES, MAX_CLOCK_SKEW_SECS, MAX_TTL_SECS, MAX_VALUE_SIZE,
    RPC_TIMEOUT_TICKS,
};
use super::kbucket::{InsertResult, K, PeerInfo, RoutingTable};
use super::protocol::{
    CLEANUP_OP_ID, DhtCmd, DhtEffect, DhtGetCompletedReason, DhtGetStats, DhtInput, DhtIo,
    DhtIoError, DhtIoRequest, DhtOutput, DhtOutputValue, DhtPeerError, DhtPutStats,
    INTERNAL_OP_START, InboundId, OpId, RpcPhase, StorageStage,
};
use super::rpc::{
    DhtRequest, DhtResponse, ErrorCode, StoredValue, signed_record_bytes, verify_stored_value,
};
use super::storage::{
    CLEANUP_PAGE_SIZE, StoredEntry, entry_is_fresh, live_entries, retained_entries,
};

const MIN_TTL_SECS: u64 = 1;

type PendingMap = HashMap<PendingKey, PendingMeta>;

const LOOKUP_LOG_PEER_LIMIT: usize = 16;
const LOOKUP_LOG_ERROR_LIMIT: usize = 16;

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
    eviction_buckets: HashSet<usize>,
    peer_epoch: u64,
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

    fn trace_context(&self) -> Option<DistributedTraceContext> {
        match self {
            Self::Put(op) => op.trace_context.clone(),
            Self::Get(op) => op.trace_context.clone(),
            Self::Bootstrap(op) => op.trace_context.clone(),
            Self::EvictionPing(_)
            | Self::MaintenancePing(_)
            | Self::InboundGet(_)
            | Self::InboundPut(_) => None,
        }
    }
}

#[derive(Debug)]
struct PutOp {
    key: DhtKeyId,
    realm_id: RealmId,
    value: Vec<u8>,
    expires_at: u64,
    revision: Option<u64>,
    signature: Option<iroh::Signature>,
    frontier: LookupFrontier,
    local_write_complete: bool,
    lookup_finished: bool,
    store_candidates: Vec<NodeId>,
    store_attempt_count: usize,
    store_count: usize,
    trace_context: Option<DistributedTraceContext>,
    pending: PendingMap,
}

#[derive(Debug)]
struct GetOp {
    key: DhtKeyId,
    realm_filter: Option<RealmId>,
    values: Vec<DhtEntry>,
    value_indices: HashMap<(NodeId, RealmId), usize>,
    value_versions: HashMap<(NodeId, RealmId), u64>,
    frontier: LookupFrontier,
    local_value_count: usize,
    remote_value_count: usize,
    peer_error_count: usize,
    peer_errors: Vec<DhtPeerError>,
    successful_responses: usize,
    trace_context: Option<DistributedTraceContext>,
    pending: PendingMap,
}

#[derive(Debug)]
struct BootstrapOp {
    active_nodes: usize,
    trace_context: Option<DistributedTraceContext>,
    pending: PendingMap,
}

#[derive(Debug)]
struct EvictionPingOp {
    oldest_node: NodeId,
    oldest_seen: u64,
    bucket_idx: usize,
    pending_peer: PeerInfo,
    pending: PendingMap,
}

#[derive(Debug)]
struct MaintenancePingOp {
    peer: NodeId,
    peer_seen: u64,
    pending: PendingMap,
}

#[derive(Debug)]
struct InboundGetOp {
    inbound_id: InboundId,
    key: DhtKeyId,
    realm_filter: Option<RealmId>,
    pending: PendingMap,
}

#[derive(Debug)]
struct InboundPutOp {
    inbound_id: InboundId,
    pending: PendingMap,
}

#[derive(Debug, Default)]
struct LookupFrontier {
    discovered: HashSet<NodeId>,
    queried: HashSet<NodeId>,
    responsive: HashSet<NodeId>,
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
        if self.lookup_complete(target) {
            return None;
        }
        let next = self.pending.remove(0);
        self.queried.insert(next);
        Some(next)
    }

    fn closest_responsive(&self, target: &[u8; 32]) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self.responsive.iter().copied().collect();
        nodes.sort_unstable_by(|a, b| compare_distance(target, a, b));
        nodes
    }

    fn lookup_complete(&self, target: &[u8; 32]) -> bool {
        if self.pending.is_empty() || self.sent_queries >= LOOKUP_MAX_QUERIES {
            return true;
        }
        if self.responsive.len() < K {
            return false;
        }

        let mut responsive: Vec<NodeId> = self.responsive.iter().copied().collect();
        responsive.sort_unstable_by(|a, b| compare_distance(target, a, b));
        let farthest = responsive[K - 1];
        !self
            .pending
            .iter()
            .any(|peer| compare_distance(target, peer, &farthest).is_lt())
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
            eviction_buckets: HashSet::new(),
            peer_epoch: 0,
            current_tick: 0,
            now_secs: start_unix_secs,
        }
    }

    #[tracing::instrument(
        name = "dht.state.step",
        level = "debug",
        skip(self, input),
        fields(input = dht_input_kind(&input))
    )]
    pub fn step(&mut self, input: DhtInput) -> SmallVec<[DhtEffect; 4]> {
        let mut out = SmallVec::<[DhtEffect; 4]>::new();

        match input {
            DhtInput::Cmd(cmd) => self.handle_cmd(cmd, &mut out),
            DhtInput::Io(io) => self.handle_io(io, &mut out),
            DhtInput::Clock { now_secs } => self.update_clock(now_secs),
            DhtInput::Tick { now_tick, now_secs } => self.handle_tick(now_tick, now_secs, &mut out),
        }

        out
    }

    pub(crate) fn contains_op(&self, op_id: OpId) -> bool {
        self.ops.contains_key(&op_id)
    }

    #[tracing::instrument(
        name = "dht.state.cmd",
        level = "debug",
        skip(self, cmd, out),
        fields(command = dht_cmd_kind(&cmd))
    )]
    fn handle_cmd(&mut self, cmd: DhtCmd, out: &mut SmallVec<[DhtEffect; 4]>) {
        match cmd {
            DhtCmd::Put {
                op_id,
                key,
                realm_id,
                value,
                ttl,
                trace_context,
            } => self.handle_cmd_put(op_id, key, realm_id, value, ttl, trace_context, out),
            DhtCmd::Get {
                op_id,
                key,
                realm_filter,
                trace_context,
            } => self.handle_cmd_get(op_id, key, realm_filter, trace_context, out),
            DhtCmd::Bootstrap {
                op_id,
                nodes,
                trace_context,
            } => self.handle_cmd_bootstrap(op_id, nodes, trace_context, out),
            DhtCmd::RoutingTableSize { op_id } => {
                out.push(DhtEffect::Output(DhtOutput::Completed {
                    op_id,
                    result: DhtOutputValue::RoutingTableSize(self.routing_table.len()),
                }));
            }
            DhtCmd::AddPeer { node_id } => self.insert_peer(node_id, out),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        name = "dht.state.cmd_put",
        level = "debug",
        skip(self, value, trace_context, out),
        fields(op_id, key = %key, realm_id = %realm_id, value_len = value.len(), ttl_secs = ttl.as_secs())
    )]
    fn handle_cmd_put(
        &mut self,
        op_id: OpId,
        key: DhtKeyId,
        realm_id: RealmId,
        value: Vec<u8>,
        ttl: std::time::Duration,
        trace_context: Option<DistributedTraceContext>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if self.ops.contains_key(&op_id) {
            out.push(DhtEffect::Output(DhtOutput::Failed {
                op_id,
                error: DhtIoError::Network("duplicate op id".to_string()),
            }));
            return;
        }

        if value.len() > MAX_VALUE_SIZE {
            out.push(DhtEffect::Output(DhtOutput::Failed {
                op_id,
                error: DhtIoError::invalid_request("DHT value exceeds the maximum size"),
            }));
            return;
        }

        let ttl_secs = ttl.as_secs().max(MIN_TTL_SECS);
        if ttl_secs > MAX_TTL_SECS {
            out.push(DhtEffect::Output(DhtOutput::Failed {
                op_id,
                error: DhtIoError::invalid_request("DHT TTL exceeds the maximum"),
            }));
            return;
        }

        let Some(expires_at) = self.now_secs.checked_add(ttl_secs) else {
            out.push(DhtEffect::Output(DhtOutput::Failed {
                op_id,
                error: DhtIoError::invalid_request("DHT expiry overflows Unix time"),
            }));
            return;
        };
        let mut op = OpState::Put(PutOp {
            key,
            realm_id,
            value,
            expires_at,
            revision: None,
            signature: None,
            frontier: LookupFrontier::default(),
            local_write_complete: false,
            lookup_finished: false,
            store_candidates: Vec::new(),
            store_attempt_count: 0,
            store_count: 0,
            trace_context,
            pending: HashMap::new(),
        });
        self.queue_storage_revision(op_id, &mut op, out);
        self.ops.insert(op_id, op);
    }

    #[tracing::instrument(
        name = "dht.state.cmd_get",
        level = "debug",
        skip(self, trace_context, out),
        fields(op_id, key = %key, realm_id = ?realm_filter)
    )]
    fn handle_cmd_get(
        &mut self,
        op_id: OpId,
        key: DhtKeyId,
        realm_filter: Option<RealmId>,
        trace_context: Option<DistributedTraceContext>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if self.ops.contains_key(&op_id) {
            out.push(DhtEffect::Output(DhtOutput::Failed {
                op_id,
                error: DhtIoError::Network("duplicate op id".to_string()),
            }));
            return;
        }

        let mut op = OpState::Get(GetOp {
            key,
            realm_filter,
            values: Vec::new(),
            value_indices: HashMap::new(),
            value_versions: HashMap::new(),
            frontier: LookupFrontier::default(),
            local_value_count: 0,
            remote_value_count: 0,
            peer_error_count: 0,
            peer_errors: Vec::new(),
            successful_responses: 0,
            trace_context,
            pending: HashMap::new(),
        });
        self.queue_storage_read(
            op_id,
            &mut op,
            StorageStage::GetLocalRead,
            key,
            realm_filter,
            out,
        );
        self.ops.insert(op_id, op);
    }

    #[tracing::instrument(
        name = "dht.state.cmd_bootstrap",
        level = "debug",
        skip(self, nodes, trace_context, out),
        fields(op_id, node_count = nodes.len())
    )]
    fn handle_cmd_bootstrap(
        &mut self,
        op_id: OpId,
        nodes: Vec<NodeId>,
        trace_context: Option<DistributedTraceContext>,
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
            trace_context,
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

    #[tracing::instrument(
        name = "dht.state.io",
        level = "debug",
        skip(self, io, out),
        fields(io = dht_io_kind(&io), op_id = ?dht_io_op_id(&io), inbound_id = ?dht_io_inbound_id(&io))
    )]
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
                trace_context: _,
            } => {
                self.insert_peer(peer, out);
                self.handle_inbound_request(inbound_id, request, out);
            }
            DhtIo::InboundReadError { .. } => {}
            DhtIo::InboundDropped { .. } => {}
            DhtIo::StorageReadResult {
                op_id,
                stage,
                entries,
            } => self.handle_storage_read(op_id, stage, entries, out),
            DhtIo::StorageRevisionResult {
                op_id,
                stage,
                revision,
            } => self.handle_storage_revision(op_id, stage, revision, out),
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

    #[tracing::instrument(
        name = "dht.state.tick",
        level = "debug",
        skip(self, out),
        fields(now_tick, now_secs)
    )]
    fn handle_tick(&mut self, now_tick: u64, now_secs: u64, out: &mut SmallVec<[DhtEffect; 4]>) {
        self.current_tick = now_tick;
        self.update_clock(now_secs);

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

    fn update_clock(&mut self, now_secs: u64) {
        self.now_secs = self.now_secs.max(now_secs);
    }

    #[tracing::instrument(
        name = "dht.state.rpc_response",
        level = "debug",
        skip(self, response, out),
        fields(op_id, phase = ?phase, peer = %peer, response = ?response)
    )]
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

        self.handle_rpc_response_state(op_id, phase, peer, response, op_state, out);
    }

    #[tracing::instrument(
        name = "dht.state.rpc_error",
        level = "debug",
        skip(self, error, out),
        fields(op_id, phase = ?phase, peer = %peer, error = %error)
    )]
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

        self.handle_rpc_error_state(op_id, phase, peer, error, op_state, out);
    }

    #[tracing::instrument(
        name = "dht.state.rpc_response_state",
        level = "trace",
        skip(self, response, op_state, out),
        fields(op_id, phase = ?phase, peer = %peer, response = ?response)
    )]
    fn handle_rpc_response_state(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        response: DhtResponse,
        op_state: OpState,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        match op_state {
            OpState::Put(op) => self.handle_rpc_response_put(op_id, phase, peer, response, op, out),
            OpState::Get(op) => self.handle_rpc_response_get(op_id, phase, peer, response, op, out),
            OpState::Bootstrap(op) => {
                self.handle_rpc_response_bootstrap(op_id, phase, peer, response, op, out)
            }
            OpState::EvictionPing(op) => {
                self.handle_rpc_response_eviction_ping(op_id, phase, response, op, out)
            }
            OpState::MaintenancePing(op) => {
                self.handle_rpc_response_maintenance_ping(op_id, phase, peer, response, op, out)
            }
            OpState::InboundGet(op) => {
                self.ops.insert(op_id, OpState::InboundGet(op));
            }
            OpState::InboundPut(op) => {
                self.ops.insert(op_id, OpState::InboundPut(op));
            }
        }
    }

    fn handle_rpc_response_put(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        response: DhtResponse,
        mut op: PutOp,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        match phase {
            RpcPhase::PutLookup => {
                match response {
                    DhtResponse::Nodes { nodes } => {
                        op.frontier.responsive.insert(peer);
                        self.insert_peer(peer, out);
                        op.frontier.add_candidates(nodes, self.local_id);
                    }
                    DhtResponse::Pong
                    | DhtResponse::Value { .. }
                    | DhtResponse::Stored
                    | DhtResponse::Error { .. } => {}
                }

                self.dispatch_put_lookup_requests(op_id, &mut op, out);
                if self.put_lookup_exhausted(&op) {
                    self.begin_put_store(op_id, &mut op, out);
                }
            }
            RpcPhase::PutStore => {
                if matches!(response, DhtResponse::Stored) {
                    op.store_count = op.store_count.saturating_add(1);
                }
                self.queue_put_stores(op_id, &mut op, out);
            }
            _ => {
                self.ops.insert(op_id, OpState::Put(op));
                return;
            }
        }

        self.maybe_complete_put(op_id, op, out);
    }

    fn handle_rpc_response_get(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        response: DhtResponse,
        mut op: GetOp,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if phase != RpcPhase::GetLookup {
            self.ops.insert(op_id, OpState::Get(op));
            return;
        }

        match response {
            DhtResponse::Value {
                entries,
                closer_nodes,
            } => {
                let mut valid_response = entries.is_empty();
                for entry in entries {
                    if !stored_value_valid(&op.key, &entry, self.now_secs)
                        || !realm_matches_filter(op.realm_filter.as_ref(), &entry.realm_id)
                    {
                        record_get_peer_error(&mut op, peer, "invalid_record");
                        continue;
                    }
                    valid_response = true;
                    if insert_get_value(&mut op, entry) {
                        op.remote_value_count = op.remote_value_count.saturating_add(1);
                    }
                }
                if valid_response {
                    op.successful_responses = op.successful_responses.saturating_add(1);
                    op.frontier.responsive.insert(peer);
                    self.insert_peer(peer, out);
                }
                op.frontier.add_candidates(closer_nodes, self.local_id);
            }
            DhtResponse::Error { code, .. } => {
                record_get_peer_error(&mut op, peer, format!("remote_error:{code:?}"));
            }
            DhtResponse::Pong | DhtResponse::Nodes { .. } | DhtResponse::Stored => {
                record_get_peer_error(&mut op, peer, "unexpected_response");
            }
        }

        self.dispatch_get_requests(op_id, &mut op, out);
        self.maybe_complete_get(op_id, op, out);
    }

    fn handle_rpc_response_bootstrap(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        response: DhtResponse,
        mut op: BootstrapOp,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if phase != RpcPhase::Bootstrap {
            self.ops.insert(op_id, OpState::Bootstrap(op));
            return;
        }

        if matches!(response, DhtResponse::Pong) {
            self.insert_peer(peer, out);
            op.active_nodes = op.active_nodes.saturating_add(1);
        }

        self.finish_bootstrap(
            op_id,
            op,
            DhtIoError::network("no bootstrap nodes reachable"),
            out,
        );
    }

    fn handle_rpc_response_eviction_ping(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        response: DhtResponse,
        op: EvictionPingOp,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if phase != RpcPhase::EvictionPing {
            self.ops.insert(op_id, OpState::EvictionPing(op));
            return;
        }

        self.eviction_buckets.remove(&op.bucket_idx);
        let peer_alive = matches!(response, DhtResponse::Pong);
        if peer_alive {
            self.insert_peer(op.oldest_node, out);
        } else if self
            .routing_table
            .remove_seen(&op.oldest_node, op.oldest_seen)
            .is_some()
        {
            self.routing_table.insert(op.pending_peer.clone());
        }

        if pending_rpc_count(&op.pending, RpcPhase::EvictionPing) > 0 {
            self.ops.insert(op_id, OpState::EvictionPing(op));
        }
    }

    fn handle_rpc_response_maintenance_ping(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        response: DhtResponse,
        op: MaintenancePingOp,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if phase != RpcPhase::MaintenancePing {
            self.ops.insert(op_id, OpState::MaintenancePing(op));
            return;
        }

        if matches!(response, DhtResponse::Pong) {
            self.insert_peer(peer, out);
        }

        if pending_rpc_count(&op.pending, RpcPhase::MaintenancePing) > 0 {
            self.ops.insert(op_id, OpState::MaintenancePing(op));
        }
    }

    #[tracing::instrument(
        name = "dht.state.rpc_error_state",
        level = "trace",
        skip(self, error, op_state, out),
        fields(op_id, phase = ?phase, error = %error)
    )]
    fn handle_rpc_error_state(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        error: DhtIoError,
        op_state: OpState,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        match op_state {
            OpState::Put(op) => self.handle_rpc_error_put(op_id, phase, error, op, out),
            OpState::Get(op) => self.handle_rpc_error_get(op_id, phase, peer, error, op, out),
            OpState::Bootstrap(op) => self.handle_rpc_error_bootstrap(op_id, phase, error, op, out),
            OpState::EvictionPing(op) => self.handle_rpc_error_eviction_ping(op_id, phase, op),
            OpState::MaintenancePing(op) => {
                self.handle_rpc_error_maintenance_ping(op_id, phase, op)
            }
            OpState::InboundGet(op) => {
                self.ops.insert(op_id, OpState::InboundGet(op));
            }
            OpState::InboundPut(op) => {
                self.ops.insert(op_id, OpState::InboundPut(op));
            }
        }
    }

    fn handle_rpc_error_put(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        _error: DhtIoError,
        mut op: PutOp,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        match phase {
            RpcPhase::PutLookup => {
                self.dispatch_put_lookup_requests(op_id, &mut op, out);
                if self.put_lookup_exhausted(&op) {
                    self.begin_put_store(op_id, &mut op, out);
                }
            }
            RpcPhase::PutStore => self.queue_put_stores(op_id, &mut op, out),
            _ => {
                self.ops.insert(op_id, OpState::Put(op));
                return;
            }
        }

        self.maybe_complete_put(op_id, op, out);
    }

    fn handle_rpc_error_get(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        error: DhtIoError,
        mut op: GetOp,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if phase != RpcPhase::GetLookup {
            self.ops.insert(op_id, OpState::Get(op));
            return;
        }

        record_get_peer_error(&mut op, peer, dht_io_error_label(&error));
        self.dispatch_get_requests(op_id, &mut op, out);
        self.maybe_complete_get(op_id, op, out);
    }

    fn handle_rpc_error_bootstrap(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        error: DhtIoError,
        op: BootstrapOp,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        if phase != RpcPhase::Bootstrap {
            self.ops.insert(op_id, OpState::Bootstrap(op));
            return;
        }

        self.finish_bootstrap(op_id, op, error, out);
    }

    fn handle_rpc_error_eviction_ping(&mut self, op_id: OpId, phase: RpcPhase, op: EvictionPingOp) {
        if phase != RpcPhase::EvictionPing {
            self.ops.insert(op_id, OpState::EvictionPing(op));
            return;
        }

        self.eviction_buckets.remove(&op.bucket_idx);
        if self
            .routing_table
            .remove_seen(&op.oldest_node, op.oldest_seen)
            .is_some()
        {
            self.routing_table.insert(op.pending_peer.clone());
        }
    }

    fn handle_rpc_error_maintenance_ping(
        &mut self,
        op_id: OpId,
        phase: RpcPhase,
        op: MaintenancePingOp,
    ) {
        if phase != RpcPhase::MaintenancePing {
            self.ops.insert(op_id, OpState::MaintenancePing(op));
            return;
        }

        let _ = self.routing_table.remove_seen(&op.peer, op.peer_seen);
    }

    fn handle_storage_revision(
        &mut self,
        op_id: OpId,
        stage: StorageStage,
        revision: u64,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let Some(mut op_state) = self.ops.remove(&op_id) else {
            return;
        };
        if stage != StorageStage::PutRevision
            || !take_pending_storage(op_state.pending_mut(), stage)
        {
            self.ops.insert(op_id, op_state);
            return;
        }
        if revision == 0 {
            out.push(DhtEffect::Output(DhtOutput::Failed {
                op_id,
                error: DhtIoError::storage("DHT revision must be non-zero"),
            }));
            return;
        }

        let OpState::Put(mut op) = op_state else {
            self.ops.insert(op_id, op_state);
            return;
        };
        let signed_data = signed_record_bytes(
            &op.key,
            &self.local_id,
            &op.realm_id,
            &op.value,
            op.expires_at,
            revision,
        );
        let signature = self.secret_key.sign(&signed_data);
        let entry = StoredEntry {
            publisher: self.local_id,
            realm_id: op.realm_id,
            value: op.value.clone(),
            expires_at: op.expires_at,
            revision,
            signature,
            retain_until: op.expires_at,
        };
        op.revision = Some(revision);
        op.signature = Some(signature);
        let key = op.key;
        let mut op_state = OpState::Put(op);
        self.queue_storage_merge(
            op_id,
            &mut op_state,
            StorageStage::PutLocalMerge,
            key,
            entry,
            out,
        );
        self.ops.insert(op_id, op_state);
    }

    #[tracing::instrument(
        name = "dht.state.storage_read",
        level = "debug",
        skip(self, entries, out),
        fields(op_id, stage = ?stage, entry_count = entries.len())
    )]
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
            OpState::Put(op) => {
                self.ops.insert(op_id, OpState::Put(op));
            }
            OpState::Get(mut op) => {
                if stage != StorageStage::GetLocalRead {
                    self.ops.insert(op_id, OpState::Get(op));
                    return;
                }

                for entry in retained_entries(entries, self.now_secs) {
                    if !realm_matches_filter(op.realm_filter.as_ref(), &entry.realm_id) {
                        continue;
                    }
                    let value = stored_entry_value(entry);
                    if !stored_value_bounded(&op.key, &value, self.now_secs) {
                        continue;
                    }
                    if entry_is_fresh(value.expires_at, self.now_secs) {
                        if insert_get_value(&mut op, value) {
                            op.local_value_count = op.local_value_count.saturating_add(1);
                        }
                    } else {
                        op.value_versions
                            .entry((value.publisher, value.realm_id))
                            .and_modify(|revision| *revision = (*revision).max(value.revision))
                            .or_insert(value.revision);
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
                    .map(stored_entry_value)
                    .filter(|entry| {
                        realm_matches_filter(op.realm_filter.as_ref(), &entry.realm_id)
                            && stored_value_valid(&op.key, entry, now)
                    })
                    .collect();
                let closer_nodes = self
                    .routing_table
                    .closest(op.key.as_bytes(), K)
                    .into_iter()
                    .map(|peer| peer.node_id)
                    .collect();

                out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcResponse {
                    inbound_id: op.inbound_id,
                    response: DhtResponse::Value {
                        entries: values,
                        closer_nodes,
                    },
                })));
            }
            OpState::InboundPut(op) => {
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

    #[tracing::instrument(
        name = "dht.state.storage_write",
        level = "debug",
        skip(self, out),
        fields(op_id, stage = ?stage)
    )]
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
                if stage != StorageStage::PutLocalMerge {
                    self.ops.insert(op_id, OpState::Put(op));
                    return;
                }

                op.local_write_complete = true;

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
                if stage != StorageStage::InboundPutMerge {
                    self.ops.insert(op_id, OpState::InboundPut(op));
                    return;
                }

                out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcResponse {
                    inbound_id: op.inbound_id,
                    response: DhtResponse::Stored,
                })));
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

    #[tracing::instrument(
        name = "dht.state.storage_delete",
        level = "debug",
        skip(self, _out),
        fields(op_id, stage = ?stage)
    )]
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

    #[tracing::instrument(
        name = "dht.state.storage_iter",
        level = "debug",
        skip(self, values, next_start_after, out),
        fields(op_id, stage = ?stage, page_len = values.len(), has_next = next_start_after.is_some())
    )]
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
            let retained = retained_entries(entries, self.now_secs);
            if retained.len() < original_len {
                out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::StoragePrune {
                    op_id: CLEANUP_OP_ID,
                    stage: StorageStage::CleanupPrune,
                    key,
                    now_secs: self.now_secs,
                })));
            }
        }

        self.cleanup_cursor = next_start_after;
        self.cleanup_inflight = false;
    }

    #[tracing::instrument(
        name = "dht.state.storage_error",
        level = "debug",
        skip(self, error, out),
        fields(op_id, stage = ?stage, error = %error)
    )]
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
                out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcResponse {
                    inbound_id: op.inbound_id,
                    response: storage_error_response(&error),
                })));
            }
            OpState::InboundPut(op) => {
                out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcResponse {
                    inbound_id: op.inbound_id,
                    response: storage_error_response(&error),
                })));
            }
            OpState::EvictionPing(_) | OpState::MaintenancePing(_) => {}
        }
    }

    #[tracing::instrument(
        name = "dht.state.inbound_request",
        level = "debug",
        skip(self, request, out),
        fields(inbound_id, request = ?request)
    )]
    fn handle_inbound_request(
        &mut self,
        inbound_id: InboundId,
        request: DhtRequest,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        match request {
            DhtRequest::Ping => {
                out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcResponse {
                    inbound_id,
                    response: DhtResponse::Pong,
                })));
            }
            DhtRequest::FindNode { target } => {
                let nodes = self
                    .routing_table
                    .closest(&target, K)
                    .into_iter()
                    .map(|peer| peer.node_id)
                    .collect();

                out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcResponse {
                    inbound_id,
                    response: DhtResponse::Nodes { nodes },
                })));
            }
            DhtRequest::GetValue { key, realm_filter } => {
                let op_id = self.alloc_internal_op_id();
                let mut op = OpState::InboundGet(InboundGetOp {
                    inbound_id,
                    key,
                    realm_filter,
                    pending: HashMap::new(),
                });
                self.queue_storage_read(
                    op_id,
                    &mut op,
                    StorageStage::InboundGetRead,
                    key,
                    realm_filter,
                    out,
                );
                self.ops.insert(op_id, op);
            }
            DhtRequest::PutValue {
                key,
                realm_id,
                value,
                expires_at,
                revision,
                publisher,
                signature,
            } => {
                let Some(signature) = signature else {
                    out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcResponse {
                        inbound_id,
                        response: DhtResponse::Error {
                            code: ErrorCode::InvalidSignature,
                            message: "Missing publisher signature".to_string(),
                        },
                    })));
                    return;
                };

                if value.len() > MAX_VALUE_SIZE {
                    out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcResponse {
                        inbound_id,
                        response: DhtResponse::Error {
                            code: ErrorCode::InvalidRequest,
                            message: "DHT value exceeds the maximum size".to_string(),
                        },
                    })));
                    return;
                }

                if revision == 0
                    || !entry_is_fresh(expires_at, self.now_secs)
                    || expires_at.saturating_sub(self.now_secs)
                        > MAX_TTL_SECS.saturating_add(MAX_CLOCK_SKEW_SECS)
                {
                    out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcResponse {
                        inbound_id,
                        response: DhtResponse::Error {
                            code: ErrorCode::InvalidRequest,
                            message: "DHT expiry is invalid".to_string(),
                        },
                    })));
                    return;
                }

                let signed_data =
                    signed_record_bytes(&key, &publisher, &realm_id, &value, expires_at, revision);

                if publisher.verify(&signed_data, &signature).is_err() {
                    out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcResponse {
                        inbound_id,
                        response: DhtResponse::Error {
                            code: ErrorCode::InvalidSignature,
                            message: "Invalid publisher signature".to_string(),
                        },
                    })));
                    return;
                }

                let new_entry = StoredEntry {
                    publisher,
                    realm_id,
                    value,
                    expires_at,
                    revision,
                    signature,
                    retain_until: expires_at,
                };

                let op_id = self.alloc_internal_op_id();
                let mut op = OpState::InboundPut(InboundPutOp {
                    inbound_id,
                    pending: HashMap::new(),
                });
                self.queue_storage_merge(
                    op_id,
                    &mut op,
                    StorageStage::InboundPutMerge,
                    key,
                    new_entry,
                    out,
                );
                self.ops.insert(op_id, op);
            }
        }
    }

    #[tracing::instrument(
        name = "dht.state.dispatch_get_requests",
        level = "debug",
        skip(self, op, out),
        fields(op_id, key = %op.key)
    )]
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
                DhtRequest::GetValue {
                    key: op.key,
                    realm_filter: op.realm_filter,
                },
                op.trace_context.clone(),
                out,
            );
            op.frontier.sent_queries = op.frontier.sent_queries.saturating_add(1);
        }
    }

    #[tracing::instrument(
        name = "dht.state.dispatch_put_lookup_requests",
        level = "debug",
        skip(self, op, out),
        fields(op_id, key = %op.key)
    )]
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
                op.trace_context.clone(),
                out,
            );
            op.frontier.sent_queries = op.frontier.sent_queries.saturating_add(1);
        }
    }

    fn put_lookup_exhausted(&self, op: &PutOp) -> bool {
        !op.lookup_finished
            && pending_rpc_count(&op.pending, RpcPhase::PutLookup) == 0
            && op.frontier.lookup_complete(op.key.as_bytes())
    }

    #[tracing::instrument(
        name = "dht.state.begin_put_store",
        level = "debug",
        skip(self, op, out),
        fields(op_id, key = %op.key)
    )]
    fn begin_put_store(&mut self, op_id: OpId, op: &mut PutOp, out: &mut SmallVec<[DhtEffect; 4]>) {
        if op.lookup_finished {
            return;
        }
        op.lookup_finished = true;
        op.store_candidates = op.frontier.closest_responsive(op.key.as_bytes());
        self.queue_put_stores(op_id, op, out);
    }

    fn queue_put_stores(&self, op_id: OpId, op: &mut PutOp, out: &mut SmallVec<[DhtEffect; 4]>) {
        let (Some(revision), Some(signature)) = (op.revision, op.signature) else {
            return;
        };
        while op.store_count + pending_rpc_count(&op.pending, RpcPhase::PutStore) < K
            && !op.store_candidates.is_empty()
        {
            let peer = op.store_candidates.remove(0);
            op.store_attempt_count = op.store_attempt_count.saturating_add(1);
            self.queue_rpc_pending(
                op_id,
                &mut op.pending,
                RpcPhase::PutStore,
                peer,
                DhtRequest::PutValue {
                    key: op.key,
                    realm_id: op.realm_id,
                    value: op.value.clone(),
                    expires_at: op.expires_at,
                    revision,
                    publisher: self.local_id,
                    signature: Some(signature),
                },
                op.trace_context.clone(),
                out,
            );
        }
    }

    #[tracing::instrument(
        name = "dht.state.maybe_complete_get",
        level = "debug",
        skip(self, op, out),
        fields(op_id, key = %op.key, value_count = op.values.len())
    )]
    fn maybe_complete_get(&mut self, op_id: OpId, op: GetOp, out: &mut SmallVec<[DhtEffect; 4]>) {
        if pending_rpc_count(&op.pending, RpcPhase::GetLookup) > 0
            || !op.frontier.lookup_complete(op.key.as_bytes())
        {
            self.ops.insert(op_id, OpState::Get(op));
            return;
        }

        if op.values.is_empty() && op.successful_responses == 0 {
            out.push(DhtEffect::Output(DhtOutput::Failed {
                op_id,
                error: DhtIoError::network("all DHT lookup peers were unavailable"),
            }));
            return;
        }

        let completed_reason = if op.remote_value_count > 0 {
            DhtGetCompletedReason::RemoteValue
        } else if op.local_value_count > 0 {
            DhtGetCompletedReason::LocalValue
        } else {
            DhtGetCompletedReason::LookupExhausted
        };
        complete_get(op_id, op, completed_reason, out);
    }

    #[tracing::instrument(
        name = "dht.state.maybe_complete_put",
        level = "debug",
        skip(self, op, out),
        fields(op_id, key = %op.key)
    )]
    fn maybe_complete_put(&mut self, op_id: OpId, op: PutOp, out: &mut SmallVec<[DhtEffect; 4]>) {
        if op.local_write_complete
            && op.lookup_finished
            && pending_rpc_count(&op.pending, RpcPhase::PutStore) == 0
            && (op.store_count >= K || op.store_candidates.is_empty())
        {
            out.push(DhtEffect::Output(DhtOutput::Completed {
                op_id,
                result: DhtOutputValue::PutStored {
                    stats: DhtPutStats {
                        remote_attempt_count: op.store_attempt_count,
                        remote_store_count: op.store_count,
                    },
                },
            }));
        } else {
            self.ops.insert(op_id, OpState::Put(op));
        }
    }

    #[tracing::instrument(
        name = "dht.state.finish_bootstrap",
        level = "debug",
        skip(self, op, error, out),
        fields(op_id, active_nodes = op.active_nodes, error = %error)
    )]
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

    #[tracing::instrument(
        name = "dht.state.schedule_maintenance_ping",
        level = "trace",
        skip(self, out),
        fields(now_tick)
    )]
    fn schedule_maintenance_ping(&mut self, now_tick: u64, out: &mut SmallVec<[DhtEffect; 4]>) {
        let mut peers = self.routing_table.all_peers();
        if peers.is_empty() {
            return;
        }

        peers.sort_unstable_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
        let idx = (now_tick as usize) % peers.len();
        let peer = peers[idx].clone();

        let op_id = self.alloc_internal_op_id();
        let mut op = OpState::MaintenancePing(MaintenancePingOp {
            peer: peer.node_id,
            peer_seen: peer.last_seen,
            pending: HashMap::new(),
        });
        self.queue_rpc(
            op_id,
            &mut op,
            RpcPhase::MaintenancePing,
            peer.node_id,
            DhtRequest::Ping,
            out,
        );
        self.ops.insert(op_id, op);
    }

    #[tracing::instrument(name = "dht.state.schedule_cleanup", level = "trace", skip(self, out))]
    fn schedule_cleanup(&mut self, out: &mut SmallVec<[DhtEffect; 4]>) {
        if self.cleanup_inflight {
            return;
        }

        self.cleanup_inflight = true;
        out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::StorageIter {
            op_id: CLEANUP_OP_ID,
            stage: StorageStage::CleanupIter,
            start_after: self.cleanup_cursor.clone(),
            limit: CLEANUP_PAGE_SIZE,
            now_secs: self.now_secs,
        })));
    }

    #[tracing::instrument(
        name = "dht.state.insert_peer",
        level = "debug",
        skip(self, out),
        fields(node_id = %node_id)
    )]
    fn insert_peer(&mut self, node_id: NodeId, out: &mut SmallVec<[DhtEffect; 4]>) {
        if node_id == self.local_id {
            return;
        }

        self.peer_epoch = self.peer_epoch.saturating_add(1);
        let result = self
            .routing_table
            .insert(PeerInfo::seen(node_id, self.peer_epoch));
        let InsertResult::BucketFull { oldest, pending } = result else {
            return;
        };

        let Some(oldest_peer) = oldest else {
            return;
        };

        let bucket_idx = self.local_id.bucket_index(&oldest_peer.node_id);
        if bucket_idx >= 256 || !self.eviction_buckets.insert(bucket_idx) {
            return;
        }

        let op_id = self.alloc_internal_op_id();
        let mut op = OpState::EvictionPing(EvictionPingOp {
            oldest_node: oldest_peer.node_id,
            oldest_seen: oldest_peer.last_seen,
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

    #[tracing::instrument(
        name = "dht.state.queue_rpc",
        level = "debug",
        skip(self, op, request, out),
        fields(op_id, phase = ?phase, peer = %peer, request = ?request)
    )]
    fn queue_rpc(
        &self,
        op_id: OpId,
        op: &mut OpState,
        phase: RpcPhase,
        peer: NodeId,
        request: DhtRequest,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let trace_context = op.trace_context();
        self.queue_rpc_pending(
            op_id,
            op.pending_mut(),
            phase,
            peer,
            request,
            trace_context,
            out,
        );
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        name = "dht.state.queue_rpc_pending",
        level = "debug",
        skip(self, pending, request, trace_context, out),
        fields(op_id, phase = ?phase, peer = %peer, request = ?request)
    )]
    fn queue_rpc_pending(
        &self,
        op_id: OpId,
        pending: &mut PendingMap,
        phase: RpcPhase,
        peer: NodeId,
        request: DhtRequest,
        trace_context: Option<DistributedTraceContext>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let deadline_tick = self.current_tick.saturating_add(RPC_TIMEOUT_TICKS);
        pending.insert(
            PendingKey::Rpc { phase, peer },
            PendingMeta {
                deadline_tick: Some(deadline_tick),
            },
        );

        out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::RpcRequest {
            op_id,
            phase,
            peer,
            request,
            trace_context,
        })));
    }

    #[tracing::instrument(
        name = "dht.state.queue_storage_read",
        level = "debug",
        skip(self, op, out),
        fields(op_id, stage = ?stage, key = %key, realm_id = ?realm_filter)
    )]
    fn queue_storage_read(
        &self,
        op_id: OpId,
        op: &mut OpState,
        stage: StorageStage,
        key: DhtKeyId,
        realm_filter: Option<RealmId>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        op.pending_mut().insert(
            PendingKey::Storage { stage },
            PendingMeta {
                deadline_tick: None,
            },
        );

        out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::StorageRead {
            op_id,
            stage,
            key,
            realm_filter,
            now_secs: self.now_secs,
        })));
    }

    fn queue_storage_revision(
        &self,
        op_id: OpId,
        op: &mut OpState,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let stage = StorageStage::PutRevision;
        op.pending_mut().insert(
            PendingKey::Storage { stage },
            PendingMeta {
                deadline_tick: None,
            },
        );
        out.push(DhtEffect::IoRequest(Box::new(
            DhtIoRequest::StorageRevision { op_id, stage },
        )));
    }

    #[allow(clippy::too_many_arguments)]
    fn queue_storage_merge(
        &self,
        op_id: OpId,
        op: &mut OpState,
        stage: StorageStage,
        key: DhtKeyId,
        entry: StoredEntry,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        op.pending_mut().insert(
            PendingKey::Storage { stage },
            PendingMeta {
                deadline_tick: None,
            },
        );

        out.push(DhtEffect::IoRequest(Box::new(DhtIoRequest::StorageMerge {
            op_id,
            stage,
            key,
            entry,
            now_secs: self.now_secs,
        })));
    }

    #[tracing::instrument(name = "dht.state.collect_timeouts", level = "trace", skip(self))]
    fn collect_timed_out_rpc(&self) -> Vec<(OpId, RpcPhase, NodeId)> {
        let mut timed_out = Vec::new();

        for (op_id, op) in &self.ops {
            for (key, meta) in op.pending() {
                let PendingKey::Rpc { phase, peer } = key else {
                    continue;
                };
                if let Some(deadline_tick) = meta.deadline_tick
                    && deadline_tick <= self.current_tick
                {
                    timed_out.push((*op_id, *phase, *peer));
                }
            }
        }

        timed_out
    }

    #[tracing::instrument(name = "dht.state.alloc_internal_op", level = "trace", skip(self))]
    fn alloc_internal_op_id(&mut self) -> OpId {
        let op_id = self.next_internal_op_id;
        self.next_internal_op_id = self.next_internal_op_id.saturating_add(1);
        op_id
    }
}

fn complete_get(
    op_id: OpId,
    mut op: GetOp,
    completed_reason: DhtGetCompletedReason,
    out: &mut SmallVec<[DhtEffect; 4]>,
) {
    let stats = get_stats(&op, completed_reason);
    op.values.sort_unstable_by(|left, right| {
        left.node_id
            .as_bytes()
            .cmp(right.node_id.as_bytes())
            .then_with(|| left.realm_id.as_bytes().cmp(right.realm_id.as_bytes()))
    });
    out.push(DhtEffect::Output(DhtOutput::Completed {
        op_id,
        result: DhtOutputValue::GetValues {
            values: op.values,
            stats,
        },
    }));
}

fn stored_entry_value(entry: StoredEntry) -> StoredValue {
    StoredValue {
        publisher: entry.publisher,
        realm_id: entry.realm_id,
        value: entry.value,
        expires_at: entry.expires_at,
        revision: entry.revision,
        signature: entry.signature,
    }
}

fn stored_value_valid(key: &DhtKeyId, entry: &StoredValue, now_secs: u64) -> bool {
    entry_is_fresh(entry.expires_at, now_secs) && stored_value_bounded(key, entry, now_secs)
}

fn stored_value_bounded(key: &DhtKeyId, entry: &StoredValue, now_secs: u64) -> bool {
    entry.value.len() <= MAX_VALUE_SIZE
        && entry.revision > 0
        && entry.expires_at.saturating_sub(now_secs)
            <= MAX_TTL_SECS.saturating_add(MAX_CLOCK_SKEW_SECS)
        && verify_stored_value(key, entry)
}

fn insert_get_value(op: &mut GetOp, entry: StoredValue) -> bool {
    let record_key = (entry.publisher, entry.realm_id);
    let revision = entry.revision;
    let value = DhtEntry {
        node_id: entry.publisher,
        realm_id: entry.realm_id,
        value: entry.value,
        expires_at: entry.expires_at,
    };

    if let Some(index) = op.value_indices.get(&record_key).copied() {
        let existing = &op.values[index];
        let existing_revision = op.value_versions[&record_key];
        if revision
            .cmp(&existing_revision)
            .then_with(|| value.expires_at.cmp(&existing.expires_at))
            .then_with(|| value.value.cmp(&existing.value))
            .is_gt()
        {
            op.values[index] = value;
            op.value_versions.insert(record_key, revision);
            return true;
        }
        return false;
    }

    if op
        .value_versions
        .get(&record_key)
        .is_some_and(|current| revision <= *current)
    {
        return false;
    }

    op.value_indices.insert(record_key, op.values.len());
    op.value_versions.insert(record_key, revision);
    op.values.push(value);
    true
}

fn storage_error_response(error: &DhtIoError) -> DhtResponse {
    let (code, message) = match error {
        DhtIoError::StorageFull => (ErrorCode::StorageFull, "DHT key capacity exceeded"),
        DhtIoError::InvalidRequest(_) => (ErrorCode::InvalidRequest, "stale DHT record"),
        _ => (ErrorCode::Internal, "storage error"),
    };
    DhtResponse::Error {
        code,
        message: message.to_string(),
    }
}

fn get_stats(op: &GetOp, completed_reason: DhtGetCompletedReason) -> DhtGetStats {
    let queried_peers = limited_sorted_peers(&op.frontier.queried, LOOKUP_LOG_PEER_LIMIT);
    let queried_peer_count = op.frontier.queried.len();

    DhtGetStats {
        completed_reason,
        local_value_count: op.local_value_count,
        remote_value_count: op.remote_value_count,
        queried_peer_count,
        queried_peers,
        queried_peers_truncated: queried_peer_count > LOOKUP_LOG_PEER_LIMIT,
        peer_error_count: op.peer_error_count,
        peer_errors: op.peer_errors.clone(),
        peer_errors_truncated: op.peer_error_count > op.peer_errors.len(),
    }
}

fn limited_sorted_peers(peers: &HashSet<NodeId>, limit: usize) -> Vec<NodeId> {
    let mut peers: Vec<_> = peers.iter().copied().collect();
    peers.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    peers.truncate(limit);
    peers
}

fn record_get_peer_error(op: &mut GetOp, peer: NodeId, error: impl Into<String>) {
    op.peer_error_count = op.peer_error_count.saturating_add(1);
    if op.peer_errors.len() >= LOOKUP_LOG_ERROR_LIMIT {
        return;
    }

    op.peer_errors.push(DhtPeerError {
        peer,
        error: error.into(),
    });
}

fn dht_io_error_label(error: &DhtIoError) -> &'static str {
    match error {
        DhtIoError::QueueFull => "queue_full",
        DhtIoError::Shutdown => "shutdown",
        DhtIoError::Timeout => "timeout",
        DhtIoError::Network(_) => "network",
        DhtIoError::Storage(_) => "storage",
        DhtIoError::InvalidResponse(_) => "invalid_response",
        DhtIoError::InvalidRequest(_) => "invalid_request",
        DhtIoError::StorageFull => "storage_full",
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

fn realm_matches_filter(filter: Option<&RealmId>, realm_id: &RealmId) -> bool {
    match filter {
        Some(filter) => filter == realm_id,
        None => true,
    }
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

fn dht_input_kind(input: &DhtInput) -> &'static str {
    match input {
        DhtInput::Cmd(cmd) => dht_cmd_kind(cmd),
        DhtInput::Io(io) => dht_io_kind(io),
        DhtInput::Clock { .. } => "clock",
        DhtInput::Tick { .. } => "tick",
    }
}

fn dht_cmd_kind(cmd: &DhtCmd) -> &'static str {
    match cmd {
        DhtCmd::Put { .. } => "put",
        DhtCmd::Get { .. } => "get",
        DhtCmd::Bootstrap { .. } => "bootstrap",
        DhtCmd::RoutingTableSize { .. } => "routing_table_size",
        DhtCmd::AddPeer { .. } => "add_peer",
    }
}

fn dht_io_kind(io: &DhtIo) -> &'static str {
    match io {
        DhtIo::RpcResponse { .. } => "rpc_response",
        DhtIo::RpcError { .. } => "rpc_error",
        DhtIo::InboundRequest { .. } => "inbound_request",
        DhtIo::InboundReadError { .. } => "inbound_read_error",
        DhtIo::InboundDropped { .. } => "inbound_dropped",
        DhtIo::StorageReadResult { .. } => "storage_read_result",
        DhtIo::StorageRevisionResult { .. } => "storage_revision_result",
        DhtIo::StorageWriteResult { .. } => "storage_write_result",
        DhtIo::StorageDeleteResult { .. } => "storage_delete_result",
        DhtIo::StorageIterResult { .. } => "storage_iter_result",
        DhtIo::StorageError { .. } => "storage_error",
        DhtIo::PeerSeen { .. } => "peer_seen",
    }
}

fn dht_io_op_id(io: &DhtIo) -> Option<OpId> {
    match io {
        DhtIo::RpcResponse { op_id, .. }
        | DhtIo::RpcError { op_id, .. }
        | DhtIo::StorageReadResult { op_id, .. }
        | DhtIo::StorageRevisionResult { op_id, .. }
        | DhtIo::StorageWriteResult { op_id, .. }
        | DhtIo::StorageDeleteResult { op_id, .. }
        | DhtIo::StorageIterResult { op_id, .. }
        | DhtIo::StorageError { op_id, .. } => Some(*op_id),
        DhtIo::InboundRequest { .. }
        | DhtIo::InboundReadError { .. }
        | DhtIo::InboundDropped { .. }
        | DhtIo::PeerSeen { .. } => None,
    }
}

fn dht_io_inbound_id(io: &DhtIo) -> Option<InboundId> {
    match io {
        DhtIo::InboundRequest { inbound_id, .. }
        | DhtIo::InboundReadError { inbound_id, .. }
        | DhtIo::InboundDropped { inbound_id } => Some(*inbound_id),
        DhtIo::RpcResponse { .. }
        | DhtIo::RpcError { .. }
        | DhtIo::StorageReadResult { .. }
        | DhtIo::StorageRevisionResult { .. }
        | DhtIo::StorageWriteResult { .. }
        | DhtIo::StorageDeleteResult { .. }
        | DhtIo::StorageIterResult { .. }
        | DhtIo::StorageError { .. }
        | DhtIo::PeerSeen { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(seed: u8) -> NodeId {
        make_secret(seed).public()
    }

    fn make_secret(seed: u8) -> iroh::SecretKey {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes)
    }

    fn make_realm(seed: u8) -> RealmId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        RealmId::from_bytes(bytes)
    }

    fn make_trace_context(seed: u8) -> DistributedTraceContext {
        DistributedTraceContext::new(
            format!("00-{seed:032x}-{seed:016x}-01"),
            Some(format!("test={seed}")),
        )
    }

    fn make_value(
        seed: u8,
        key: DhtKeyId,
        realm_id: RealmId,
        value: &[u8],
        expires_at: u64,
    ) -> StoredValue {
        let secret = make_secret(seed);
        let publisher = secret.public();
        let revision = 1;
        let signed_data =
            signed_record_bytes(&key, &publisher, &realm_id, value, expires_at, revision);
        StoredValue {
            publisher,
            realm_id,
            value: value.to_vec(),
            expires_at,
            revision,
            signature: secret.sign(&signed_data),
        }
    }

    fn make_entry(
        seed: u8,
        key: DhtKeyId,
        realm_id: RealmId,
        value: &[u8],
        expires_at: u64,
    ) -> StoredEntry {
        let value = make_value(seed, key, realm_id, value, expires_at);
        StoredEntry {
            publisher: value.publisher,
            realm_id: value.realm_id,
            value: value.value,
            expires_at: value.expires_at,
            revision: value.revision,
            signature: value.signature,
            retain_until: value.expires_at,
        }
    }

    fn start_get(state: &mut DhtStateMachine, op_id: OpId, key: DhtKeyId, peer: NodeId) {
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id,
            key,
            realm_filter: None,
            trace_context: None,
        }));
        let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id,
            stage: StorageStage::GetLocalRead,
            entries: Vec::new(),
        }));
        assert!(effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::IoRequest(inner)
                    if matches!(
                        &**inner,
                        DhtIoRequest::RpcRequest {
                            phase: RpcPhase::GetLookup,
                            peer: request_peer,
                            ..
                        } if *request_peer == peer
                    )
            )
        }));
    }

    fn start_put(
        state: &mut DhtStateMachine,
        op_id: OpId,
        key: DhtKeyId,
    ) -> SmallVec<[DhtEffect; 4]> {
        let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
            op_id,
            key,
            realm_id: make_realm(1),
            value: b"value".to_vec(),
            ttl: std::time::Duration::from_secs(60),
            trace_context: None,
        }));
        let _ = reserve_put(state, op_id);
        state.step(DhtInput::Io(DhtIo::StorageWriteResult {
            op_id,
            stage: StorageStage::PutLocalMerge,
        }))
    }

    fn reserve_put(state: &mut DhtStateMachine, op_id: OpId) -> SmallVec<[DhtEffect; 4]> {
        state.step(DhtInput::Io(DhtIo::StorageRevisionResult {
            op_id,
            stage: StorageStage::PutRevision,
            revision: op_id.max(1),
        }))
    }

    fn bucket_nodes(local_id: NodeId, count: usize) -> Vec<NodeId> {
        let mut buckets: HashMap<usize, Vec<NodeId>> = HashMap::new();
        for seed in 1u64..100_000 {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&seed.to_le_bytes());
            let node = iroh::SecretKey::from_bytes(&bytes).public();
            let bucket = local_id.bucket_index(&node);
            if bucket >= 256 {
                continue;
            }
            let nodes = buckets.entry(bucket).or_default();
            nodes.push(node);
            if nodes.len() == count {
                return nodes.clone();
            }
        }
        panic!("failed to generate peers for one bucket");
    }

    #[test]
    fn forged_value_rejected() {
        let local_secret = make_secret(70);
        let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
        let key = DhtKeyId::from_data(b"forged-value");
        let peer = make_node(71);
        start_get(&mut state, 70, key, peer);

        let mut forged = make_value(72, key, make_realm(1), b"authentic", 2_000);
        forged.value = b"forged".to_vec();
        let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 70,
            phase: RpcPhase::GetLookup,
            peer,
            response: DhtResponse::Value {
                entries: vec![forged],
                closer_nodes: Vec::new(),
            },
        }));

        assert!(effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Failed { op_id: 70, .. })
            )
        }));
    }

    #[test]
    fn expiry_tamper_rejected() {
        let local_secret = make_secret(73);
        let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
        let key = DhtKeyId::from_data(b"expiry-tamper");
        let peer = make_node(74);
        start_get(&mut state, 71, key, peer);

        let mut forged = make_value(75, key, make_realm(1), b"value", 2_000);
        forged.expires_at = 3_000;
        let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 71,
            phase: RpcPhase::GetLookup,
            peer,
            response: DhtResponse::Value {
                entries: vec![forged],
                closer_nodes: Vec::new(),
            },
        }));

        assert!(effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Failed { op_id: 71, .. })
            )
        }));
    }

    #[test]
    fn revision_tamper_rejected() {
        let local_secret = make_secret(91);
        let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
        let key = DhtKeyId::from_data(b"revision-tamper");
        let peer = make_node(92);
        start_get(&mut state, 79, key, peer);

        let mut forged = make_value(93, key, make_realm(1), b"value", 2_000);
        forged.revision = forged.revision.saturating_add(1);
        let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 79,
            phase: RpcPhase::GetLookup,
            peer,
            response: DhtResponse::Value {
                entries: vec![forged],
                closer_nodes: Vec::new(),
            },
        }));

        assert!(matches!(
            effects.as_slice(),
            [DhtEffect::Output(DhtOutput::Failed { op_id: 79, .. })]
        ));
    }

    #[test]
    fn get_fails_unavailable() {
        let local_secret = make_secret(76);
        let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
        let key = DhtKeyId::from_data(b"unavailable-get");
        let peer = make_node(77);
        start_get(&mut state, 72, key, peer);

        let effects = state.step(DhtInput::Io(DhtIo::RpcError {
            op_id: 72,
            phase: RpcPhase::GetLookup,
            peer,
            error: DhtIoError::Timeout,
        }));
        assert!(matches!(
            effects.as_slice(),
            [DhtEffect::Output(DhtOutput::Failed {
                op_id: 72,
                error: DhtIoError::Network(_)
            })]
        ));
    }

    #[test]
    fn get_empty_succeeds() {
        let local_secret = make_secret(78);
        let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
        let key = DhtKeyId::from_data(b"empty-get");
        let peer = make_node(79);
        start_get(&mut state, 73, key, peer);

        let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 73,
            phase: RpcPhase::GetLookup,
            peer,
            response: DhtResponse::Value {
                entries: Vec::new(),
                closer_nodes: Vec::new(),
            },
        }));
        assert!(matches!(
            effects.as_slice(),
            [DhtEffect::Output(DhtOutput::Completed {
                result: DhtOutputValue::GetValues { values, .. },
                ..
            })] if values.is_empty()
        ));
    }

    #[test]
    fn tick_sets_expiry() {
        let local_secret = make_secret(80);
        let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
        let _ = state.step(DhtInput::Tick {
            now_tick: 1,
            now_secs: 10_000,
        });

        let key = DhtKeyId::from_data(b"sampled-time");
        let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
            op_id: 74,
            key,
            realm_id: make_realm(1),
            value: b"value".to_vec(),
            ttl: std::time::Duration::from_secs(60),
            trace_context: None,
        }));
        let effects = reserve_put(&mut state, 74);
        assert!(effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::IoRequest(inner)
                    if matches!(
                        &**inner,
                        DhtIoRequest::StorageMerge { entry, .. }
                            if entry.expires_at == 10_060
                    )
            )
        }));
    }

    #[test]
    fn oversized_put_rejected() {
        let local_secret = make_secret(81);
        let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
        let effects = state.step(DhtInput::Cmd(DhtCmd::Put {
            op_id: 75,
            key: DhtKeyId::from_data(b"oversized-put"),
            realm_id: make_realm(1),
            value: vec![0; MAX_VALUE_SIZE + 1],
            ttl: std::time::Duration::from_secs(60),
            trace_context: None,
        }));

        assert!(matches!(
            effects.as_slice(),
            [DhtEffect::Output(DhtOutput::Failed {
                op_id: 75,
                error: DhtIoError::InvalidRequest(_)
            })]
        ));
    }

    #[test]
    fn long_ttl_rejected() {
        let local_secret = make_secret(82);
        let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
        let effects = state.step(DhtInput::Cmd(DhtCmd::Put {
            op_id: 76,
            key: DhtKeyId::from_data(b"long-ttl"),
            realm_id: make_realm(1),
            value: b"value".to_vec(),
            ttl: std::time::Duration::from_secs(MAX_TTL_SECS + 1),
            trace_context: None,
        }));

        assert!(matches!(
            effects.as_slice(),
            [DhtEffect::Output(DhtOutput::Failed {
                op_id: 76,
                error: DhtIoError::InvalidRequest(_)
            })]
        ));
    }

    #[test]
    fn eviction_probe_serialized() {
        let local_secret = make_secret(83);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
        let nodes = bucket_nodes(local_id, K + 2);
        for node_id in &nodes[..K] {
            assert!(
                state
                    .step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: *node_id }))
                    .is_empty()
            );
        }

        let first = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[K] }));
        assert_eq!(
            first
                .iter()
                .filter(|effect| matches!(
                    effect,
                    DhtEffect::IoRequest(inner)
                        if matches!(&**inner, DhtIoRequest::RpcRequest {
                            phase: RpcPhase::EvictionPing,
                            ..
                        })
                ))
                .count(),
            1
        );

        let duplicate = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
            node_id: nodes[K + 1],
        }));
        assert!(duplicate.is_empty());
    }

    #[test]
    fn pong_refreshes_lru() {
        let local_secret = make_secret(84);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
        let nodes = bucket_nodes(local_id, K + 2);
        for node_id in &nodes[..K] {
            let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: *node_id }));
        }

        let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[K] }));
        let op_id = effects
            .iter()
            .find_map(|effect| {
                if let DhtEffect::IoRequest(inner) = effect
                    && let DhtIoRequest::RpcRequest {
                        op_id,
                        phase: RpcPhase::EvictionPing,
                        peer,
                        ..
                    } = &**inner
                    && *peer == nodes[0]
                {
                    Some(*op_id)
                } else {
                    None
                }
            })
            .expect("eviction probe");
        let _ = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id,
            phase: RpcPhase::EvictionPing,
            peer: nodes[0],
            response: DhtResponse::Pong,
        }));

        let next = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
            node_id: nodes[K + 1],
        }));
        assert!(next.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::IoRequest(inner)
                    if matches!(&**inner, DhtIoRequest::RpcRequest {
                        phase: RpcPhase::EvictionPing,
                        peer,
                        ..
                    } if *peer == nodes[1])
            )
        }));
    }

    #[test]
    fn eviction_removes_probed() {
        let local_secret = make_secret(85);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
        let nodes = bucket_nodes(local_id, K + 1);
        for node_id in &nodes[..K] {
            let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: *node_id }));
        }

        let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[K] }));
        let op_id = effects
            .iter()
            .find_map(|effect| {
                if let DhtEffect::IoRequest(inner) = effect
                    && let DhtIoRequest::RpcRequest {
                        op_id,
                        phase: RpcPhase::EvictionPing,
                        ..
                    } = &**inner
                {
                    Some(*op_id)
                } else {
                    None
                }
            })
            .expect("eviction probe");
        let _ = state.step(DhtInput::Io(DhtIo::RpcError {
            op_id,
            phase: RpcPhase::EvictionPing,
            peer: nodes[0],
            error: DhtIoError::Timeout,
        }));

        let peers: HashSet<NodeId> = state
            .routing_table
            .all_peers()
            .into_iter()
            .map(|peer| peer.node_id)
            .collect();
        assert_eq!(peers.len(), K);
        assert!(!peers.contains(&nodes[0]));
        assert!(peers.contains(&nodes[1]));
        assert!(peers.contains(&nodes[K]));
    }

    #[test]
    fn stale_probe_preserves() {
        let local_secret = make_secret(90);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
        let nodes = bucket_nodes(local_id, K + 1);
        for node_id in &nodes[..K] {
            let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: *node_id }));
        }

        let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[K] }));
        let op_id = effects
            .iter()
            .find_map(|effect| {
                if let DhtEffect::IoRequest(inner) = effect
                    && let DhtIoRequest::RpcRequest {
                        op_id,
                        phase: RpcPhase::EvictionPing,
                        ..
                    } = &**inner
                {
                    Some(*op_id)
                } else {
                    None
                }
            })
            .expect("eviction probe");
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[0] }));
        let _ = state.step(DhtInput::Io(DhtIo::RpcError {
            op_id,
            phase: RpcPhase::EvictionPing,
            peer: nodes[0],
            error: DhtIoError::Timeout,
        }));

        let peers: HashSet<NodeId> = state
            .routing_table
            .all_peers()
            .into_iter()
            .map(|peer| peer.node_id)
            .collect();
        assert_eq!(peers.len(), K);
        assert!(peers.contains(&nodes[0]));
        assert!(!peers.contains(&nodes[K]));
    }

    #[test]
    fn eviction_aba_preserves() {
        let local_secret = make_secret(91);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
        let nodes = bucket_nodes(local_id, K + 1);
        for node_id in &nodes[..K] {
            let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: *node_id }));
        }

        let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[K] }));
        let op_id = effects
            .iter()
            .find_map(|effect| {
                if let DhtEffect::IoRequest(inner) = effect
                    && let DhtIoRequest::RpcRequest {
                        op_id,
                        phase: RpcPhase::EvictionPing,
                        ..
                    } = &**inner
                {
                    Some(*op_id)
                } else {
                    None
                }
            })
            .expect("eviction probe");

        let _ = state.routing_table.remove(&nodes[0]);
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[0] }));
        let _ = state.step(DhtInput::Io(DhtIo::RpcError {
            op_id,
            phase: RpcPhase::EvictionPing,
            peer: nodes[0],
            error: DhtIoError::Timeout,
        }));

        let peers = state.routing_table.all_peers();
        assert!(peers.iter().any(|peer| peer.node_id == nodes[0]));
        assert!(!peers.iter().any(|peer| peer.node_id == nodes[K]));
    }

    #[test]
    fn expiry_breaks_ties() {
        let local_secret = make_secret(92);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
        let key = DhtKeyId::from_data(b"expiry-tie-break");
        let realm_id = make_realm(1);
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id: 91,
            key,
            realm_filter: None,
            trace_context: None,
        }));
        let OpState::Get(mut op) = state.ops.remove(&91).expect("get operation") else {
            panic!("expected get operation");
        };

        let shorter = make_value(93, key, realm_id, b"same", 1_500);
        let longer = make_value(93, key, realm_id, b"same", 1_600);
        assert!(insert_get_value(&mut op, shorter.clone()));
        assert!(insert_get_value(&mut op, longer));
        assert!(!insert_get_value(&mut op, shorter));
        assert_eq!(op.values[0].expires_at, 1_600);
    }

    #[test]
    fn put_uses_responders() {
        let local_secret = make_secret(86);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
        let first = make_node(87);
        let second = make_node(88);
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: first }));
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: second }));
        let key = DhtKeyId::from_data(b"responsive-targets");
        let lookup = start_put(&mut state, 77, key);
        let lookup_peers: Vec<NodeId> = lookup
            .iter()
            .filter_map(|effect| {
                if let DhtEffect::IoRequest(inner) = effect
                    && let DhtIoRequest::RpcRequest {
                        phase: RpcPhase::PutLookup,
                        peer,
                        ..
                    } = &**inner
                {
                    Some(*peer)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(lookup_peers.len(), 2);

        let responsive = lookup_peers[0];
        let failed = lookup_peers[1];
        let _ = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 77,
            phase: RpcPhase::PutLookup,
            peer: responsive,
            response: DhtResponse::Nodes { nodes: Vec::new() },
        }));
        let store = state.step(DhtInput::Io(DhtIo::RpcError {
            op_id: 77,
            phase: RpcPhase::PutLookup,
            peer: failed,
            error: DhtIoError::Timeout,
        }));

        let store_peers: Vec<NodeId> = store
            .iter()
            .filter_map(|effect| {
                if let DhtEffect::IoRequest(inner) = effect
                    && let DhtIoRequest::RpcRequest {
                        phase: RpcPhase::PutStore,
                        peer,
                        ..
                    } = &**inner
                {
                    Some(*peer)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(store_peers, vec![responsive]);
    }

    #[test]
    fn put_uses_fallbacks() {
        let local_secret = make_secret(89);
        let local_id = local_secret.public();
        let state = DhtStateMachine::new(local_id, local_secret.clone(), 1_000);
        let key = DhtKeyId::from_data(b"store-fallbacks");
        let realm_id = make_realm(1);
        let value = b"value".to_vec();
        let expires_at = 2_000;
        let revision = 1;
        let signed_data =
            signed_record_bytes(&key, &local_id, &realm_id, &value, expires_at, revision);
        let mut op = PutOp {
            key,
            realm_id,
            value,
            expires_at,
            revision: Some(revision),
            signature: Some(local_secret.sign(&signed_data)),
            frontier: LookupFrontier::default(),
            local_write_complete: true,
            lookup_finished: true,
            store_candidates: (1..=K + 1).map(|seed| make_node(seed as u8)).collect(),
            store_attempt_count: 0,
            store_count: 0,
            trace_context: None,
            pending: HashMap::new(),
        };

        let mut initial = SmallVec::new();
        state.queue_put_stores(78, &mut op, &mut initial);
        assert_eq!(pending_rpc_count(&op.pending, RpcPhase::PutStore), K);
        let failed = initial
            .iter()
            .find_map(|effect| {
                if let DhtEffect::IoRequest(inner) = effect
                    && let DhtIoRequest::RpcRequest { peer, .. } = &**inner
                {
                    Some(*peer)
                } else {
                    None
                }
            })
            .expect("initial store target");
        assert!(take_pending_rpc(
            &mut op.pending,
            RpcPhase::PutStore,
            failed
        ));

        let mut fallback = SmallVec::new();
        state.queue_put_stores(78, &mut op, &mut fallback);
        assert_eq!(fallback.len(), 1);
        assert_eq!(op.store_attempt_count, K + 1);
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
        let trace = [
            DhtInput::Cmd(DhtCmd::Put {
                op_id: 1,
                key,
                realm_id: make_realm(1),
                value: b"hello".to_vec(),
                ttl: std::time::Duration::from_secs(30),
                trace_context: None,
            }),
            DhtInput::Io(DhtIo::StorageRevisionResult {
                op_id: 1,
                stage: StorageStage::PutRevision,
                revision: 1,
            }),
            DhtInput::Io(DhtIo::StorageWriteResult {
                op_id: 1,
                stage: StorageStage::PutLocalMerge,
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
            trace_context: None,
        }));

        assert!(effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                    **inner,
                    DhtIoRequest::RpcRequest {
                        op_id,
                        phase: RpcPhase::Bootstrap,
                        peer: req_peer,
                        ..
                    }
                 if op_id == 5 && req_peer == peer),
                _ => false,
            }
        }));

        let timeout_effects = state.step(DhtInput::Tick {
            now_tick: RPC_TIMEOUT_TICKS,
            now_secs: 0,
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
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id: 9,
            key,
            realm_filter: None,
            trace_context: None,
        }));

        let local_read_effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: 9,
            stage: StorageStage::GetLocalRead,
            entries: Vec::new(),
        }));

        assert!(local_read_effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                    **inner,
                    DhtIoRequest::RpcRequest {
                        op_id,
                        phase: RpcPhase::GetLookup,
                        peer,
                        request: DhtRequest::GetValue { .. },
                        ..
                    } if op_id == 9 && peer == first_peer),
                _ => false,
            }
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
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                    **inner,
                    DhtIoRequest::RpcRequest {
                        op_id,
                        phase: RpcPhase::GetLookup,
                        peer,
                        request: DhtRequest::GetValue { .. },
                        ..
                } if op_id == 9 && peer == second_peer),
                _ => false,
            }
        }));
    }

    #[test]
    fn get_trace_context_propagates_to_remote_lookup() {
        let local_secret = iroh::SecretKey::from_bytes(&[32u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let peer = make_node(3);
        let key = DhtKeyId::from_data(b"trace-context-get");
        let trace_context = make_trace_context(1);

        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id: 10,
            key,
            realm_filter: None,
            trace_context: Some(trace_context.clone()),
        }));

        let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: 10,
            stage: StorageStage::GetLocalRead,
            entries: Vec::new(),
        }));

        let propagated = effects.iter().find_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    phase: RpcPhase::GetLookup,
                    trace_context,
                    ..
                } = &(**inner)
            {
                trace_context.clone()
            } else {
                None
            }
        });

        assert_eq!(propagated, Some(trace_context));
    }

    #[test]
    fn bootstrap_trace_context_propagates_to_ping() {
        let local_secret = iroh::SecretKey::from_bytes(&[33u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let peer = make_node(4);
        let trace_context = make_trace_context(2);
        let effects = state.step(DhtInput::Cmd(DhtCmd::Bootstrap {
            op_id: 10,
            nodes: vec![peer],
            trace_context: Some(trace_context.clone()),
        }));

        let propagated = effects.iter().find_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    phase: RpcPhase::Bootstrap,
                    trace_context,
                    ..
                } = &(**inner)
            {
                trace_context.clone()
            } else {
                None
            }
        });

        assert_eq!(propagated, Some(trace_context));
    }

    #[test]
    fn put_reports_local() {
        let local_secret = iroh::SecretKey::from_bytes(&[41u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

        let key = DhtKeyId::from_data(b"strict-put-no-targets");
        let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
            op_id: 11,
            key,
            realm_id: make_realm(1),
            value: b"value".to_vec(),
            ttl: std::time::Duration::from_secs(60),
            trace_context: None,
        }));
        let _ = reserve_put(&mut state, 11);
        let effects = state.step(DhtInput::Io(DhtIo::StorageWriteResult {
            op_id: 11,
            stage: StorageStage::PutLocalMerge,
        }));

        assert!(effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Completed {
                    op_id: 11,
                    result: DhtOutputValue::PutStored { stats }
                }) if stats.remote_attempt_count == 0 && stats.remote_store_count == 0
            )
        }));
    }

    #[test]
    fn zero_ttl_clamped() {
        let local_secret = iroh::SecretKey::from_bytes(&[61u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

        let key = DhtKeyId::from_data(b"zero-ttl-local-put");
        let realm_id = make_realm(1);
        let value = b"value".to_vec();
        let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
            op_id: 24,
            key,
            realm_id,
            value: value.clone(),
            ttl: std::time::Duration::ZERO,
            trace_context: None,
        }));
        let effects = reserve_put(&mut state, 24);

        let entry = effects
            .iter()
            .find_map(|effect| {
                if let DhtEffect::IoRequest(inner) = effect
                    && let DhtIoRequest::StorageMerge {
                        stage: StorageStage::PutLocalMerge,
                        entry,
                        ..
                    } = &(**inner)
                {
                    Some(entry)
                } else {
                    None
                }
            })
            .expect("put should enqueue atomic local storage merge");
        assert_eq!(entry.expires_at, 1_000 + MIN_TTL_SECS);

        let signed_data = signed_record_bytes(
            &key,
            &local_id,
            &realm_id,
            &value,
            1_000 + MIN_TTL_SECS,
            entry.revision,
        );
        assert!(local_id.verify(&signed_data, &entry.signature).is_ok());
    }

    #[test]
    fn put_reports_timeout() {
        let local_secret = iroh::SecretKey::from_bytes(&[42u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 100);

        let peer = make_node(4);
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));

        let key = DhtKeyId::from_data(b"strict-put-timeout");
        let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
            op_id: 12,
            key,
            realm_id: make_realm(1),
            value: b"value".to_vec(),
            ttl: std::time::Duration::from_secs(60),
            trace_context: None,
        }));
        let _ = reserve_put(&mut state, 12);
        let write_effects = state.step(DhtInput::Io(DhtIo::StorageWriteResult {
            op_id: 12,
            stage: StorageStage::PutLocalMerge,
        }));

        assert!(
            write_effects
                .iter()
                .all(|effect| !matches!(effect, DhtEffect::Output(_)))
        );

        assert!(write_effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                    **inner,
                    DhtIoRequest::RpcRequest {
                        op_id: 12,
                        phase: RpcPhase::PutLookup,
                        peer: req_peer,
                        ..
                    } if req_peer == peer),
                _ => false,
            }
        }));

        let store_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 12,
            phase: RpcPhase::PutLookup,
            peer,
            response: DhtResponse::Nodes { nodes: Vec::new() },
        }));

        assert!(store_effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                    **inner,
                    DhtIoRequest::RpcRequest {
                        op_id: 12,
                        phase: RpcPhase::PutStore,
                        peer: req_peer,
                        ..
                    } if req_peer == peer),
                _ => false,
            }
        }));

        let fail_effects = state.step(DhtInput::Io(DhtIo::RpcError {
            op_id: 12,
            phase: RpcPhase::PutStore,
            peer,
            error: DhtIoError::Timeout,
        }));

        assert!(fail_effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Completed {
                    op_id: 12,
                    result: DhtOutputValue::PutStored { stats }
                }) if stats.remote_attempt_count == 1 && stats.remote_store_count == 0
            )
        }));
    }

    #[test]
    fn put_reports_rejection() {
        let local_secret = iroh::SecretKey::from_bytes(&[43u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 100);

        let peer = make_node(5);
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));

        let key = DhtKeyId::from_data(b"strict-put-non-stored");
        let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
            op_id: 13,
            key,
            realm_id: make_realm(1),
            value: b"value".to_vec(),
            ttl: std::time::Duration::from_secs(60),
            trace_context: None,
        }));
        let _ = reserve_put(&mut state, 13);
        let write_effects = state.step(DhtInput::Io(DhtIo::StorageWriteResult {
            op_id: 13,
            stage: StorageStage::PutLocalMerge,
        }));

        assert!(
            write_effects
                .iter()
                .all(|effect| !matches!(effect, DhtEffect::Output(_)))
        );

        let _ = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 13,
            phase: RpcPhase::PutLookup,
            peer,
            response: DhtResponse::Nodes { nodes: Vec::new() },
        }));

        let fail_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 13,
            phase: RpcPhase::PutStore,
            peer,
            response: DhtResponse::Error {
                code: ErrorCode::Internal,
                message: "store failed".to_string(),
            },
        }));

        assert!(fail_effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Completed {
                    op_id: 13,
                    result: DhtOutputValue::PutStored { stats }
                }) if stats.remote_attempt_count == 1 && stats.remote_store_count == 0
            )
        }));
    }

    #[test]
    fn put_reports_replication() {
        let local_secret = iroh::SecretKey::from_bytes(&[44u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 100);

        let peer_a = make_node(6);
        let peer_b = make_node(7);
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_a }));
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_b }));

        let key = DhtKeyId::from_data(b"strict-put-first-ack");
        let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
            op_id: 14,
            key,
            realm_id: make_realm(1),
            value: b"value".to_vec(),
            ttl: std::time::Duration::from_secs(60),
            trace_context: None,
        }));
        let _ = reserve_put(&mut state, 14);
        let write_effects = state.step(DhtInput::Io(DhtIo::StorageWriteResult {
            op_id: 14,
            stage: StorageStage::PutLocalMerge,
        }));

        let mut lookup_peers = Vec::new();
        for effect in &write_effects {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    op_id: 14,
                    phase: RpcPhase::PutLookup,
                    peer,
                    ..
                } = **inner
            {
                lookup_peers.push(peer);
            }
        }
        assert!(
            lookup_peers.len() >= 2,
            "expected lookup fanout to both peers"
        );
        assert!(
            write_effects
                .iter()
                .all(|effect| !matches!(effect, DhtEffect::Output(_)))
        );

        let _ = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 14,
            phase: RpcPhase::PutLookup,
            peer: lookup_peers[0],
            response: DhtResponse::Nodes { nodes: Vec::new() },
        }));
        let store_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 14,
            phase: RpcPhase::PutLookup,
            peer: lookup_peers[1],
            response: DhtResponse::Nodes { nodes: Vec::new() },
        }));

        let mut store_peers = Vec::new();
        for effect in &store_effects {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    op_id: 14,
                    phase: RpcPhase::PutStore,
                    peer,
                    ..
                } = **inner
            {
                store_peers.push(peer);
            }
        }
        assert!(
            store_peers.len() >= 2,
            "expected store fanout to both peers"
        );

        let ack_peer = store_peers[0];
        let late_peer = *store_peers
            .iter()
            .find(|peer| **peer != ack_peer)
            .expect("expected second pending store peer");

        let complete_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 14,
            phase: RpcPhase::PutStore,
            peer: ack_peer,
            response: DhtResponse::Stored,
        }));
        assert!(complete_effects.is_empty());

        let late_effects = state.step(DhtInput::Io(DhtIo::RpcError {
            op_id: 14,
            phase: RpcPhase::PutStore,
            peer: late_peer,
            error: DhtIoError::Timeout,
        }));
        assert!(late_effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Completed {
                    op_id: 14,
                    result: DhtOutputValue::PutStored { stats }
                }) if stats.remote_attempt_count == 2 && stats.remote_store_count == 1
            )
        }));
    }

    #[test]
    fn get_merges_local() {
        let local_secret = iroh::SecretKey::from_bytes(&[45u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

        let peer = make_node(8);
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));

        let key = DhtKeyId::from_data(b"get-short-circuit-local");
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id: 15,
            key,
            realm_filter: None,
            trace_context: None,
        }));

        let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: 15,
            stage: StorageStage::GetLocalRead,
            entries: vec![make_entry(9, key, make_realm(1), b"cached", 2_000)],
        }));

        assert!(effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                    **inner,
                    DhtIoRequest::RpcRequest {
                        phase: RpcPhase::GetLookup,
                        peer: req_peer,
                        ..
                    } if req_peer == peer
                ),
                _ => false,
            }
        }));

        let complete = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 15,
            phase: RpcPhase::GetLookup,
            peer,
            response: DhtResponse::Value {
                entries: Vec::new(),
                closer_nodes: Vec::new(),
            },
        }));
        assert!(complete.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Completed {
                    op_id: 15,
                    result: DhtOutputValue::GetValues { values, stats }
                }) if values.iter().any(|entry| entry.value == b"cached".to_vec())
                    && stats.completed_reason == DhtGetCompletedReason::LocalValue
                    && stats.local_value_count == 1
                    && stats.remote_value_count == 0
                    && stats.queried_peer_count == 1
                    && stats.peer_error_count == 0
            )
        }));
    }

    #[test]
    fn highwater_blocks_get() {
        let local_secret = make_secret(96);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 801);
        let peer = make_node(97);
        let key = DhtKeyId::from_data(b"get-highwater");
        let realm_id = make_realm(1);
        let publisher_secret = make_secret(98);
        let publisher = publisher_secret.public();
        let value = b"newer".to_vec();
        let expires_at = 500;
        let revision = 2;
        let signed = signed_record_bytes(&key, &publisher, &realm_id, &value, expires_at, revision);
        let highwater = StoredEntry {
            publisher,
            realm_id,
            value,
            expires_at,
            revision,
            signature: publisher_secret.sign(&signed),
            retain_until: 86_800,
        };

        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id: 92,
            key,
            realm_filter: None,
            trace_context: None,
        }));
        let lookup = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: 92,
            stage: StorageStage::GetLocalRead,
            entries: vec![highwater],
        }));
        assert!(lookup.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::IoRequest(inner)
                    if matches!(&**inner, DhtIoRequest::RpcRequest {
                        phase: RpcPhase::GetLookup,
                        peer: request_peer,
                        ..
                    } if *request_peer == peer)
            )
        }));

        let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 92,
            phase: RpcPhase::GetLookup,
            peer,
            response: DhtResponse::Value {
                entries: vec![make_value(98, key, realm_id, b"older", 1_000)],
                closer_nodes: Vec::new(),
            },
        }));
        assert!(matches!(
            effects.as_slice(),
            [DhtEffect::Output(DhtOutput::Completed {
                result: DhtOutputValue::GetValues { values, .. },
                ..
            })] if values.is_empty()
        ));
    }

    #[test]
    fn local_realm_filter() {
        let local_secret = iroh::SecretKey::from_bytes(&[58u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

        let key = DhtKeyId::from_data(b"get-realm-filter-local");
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id: 22,
            key,
            realm_filter: Some(make_realm(1)),
            trace_context: None,
        }));

        let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: 22,
            stage: StorageStage::GetLocalRead,
            entries: vec![make_entry(23, key, make_realm(2), b"foreign", 2_000)],
        }));

        assert!(effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Failed { op_id: 22, .. })
            )
        }));
    }

    #[test]
    fn get_forwards_realm_filter_to_remote_lookups() {
        let local_secret = iroh::SecretKey::from_bytes(&[59u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

        let peer = make_node(24);
        let realm_filter = make_realm(3);
        let key = DhtKeyId::from_data(b"get-realm-filter-remote");

        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id: 23,
            key,
            realm_filter: Some(realm_filter),
            trace_context: None,
        }));

        let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: 23,
            stage: StorageStage::GetLocalRead,
            entries: Vec::new(),
        }));

        assert!(effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                &(**inner),
                DhtIoRequest::RpcRequest {
                    op_id: 23,
                    phase: RpcPhase::GetLookup,
                    peer: req_peer,
                    request: DhtRequest::GetValue {
                        key: req_key,
                        realm_filter: Some(req_realm_filter),
                    },
                    ..
                } if *req_peer == peer && *req_key == key && *req_realm_filter == realm_filter),
                _ => false,
            }
        }));
    }

    #[test]
    fn get_collects_publishers() {
        let local_secret = iroh::SecretKey::from_bytes(&[46u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

        let peer_a = make_node(10);
        let peer_b = make_node(11);
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_a }));
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_b }));

        let key = DhtKeyId::from_data(b"get-short-circuit-remote");
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id: 16,
            key,
            realm_filter: None,
            trace_context: None,
        }));

        let local_effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: 16,
            stage: StorageStage::GetLocalRead,
            entries: Vec::new(),
        }));

        let mut lookup_peers = Vec::new();
        for effect in &local_effects {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    op_id: 16,
                    phase: RpcPhase::GetLookup,
                    peer,
                    ..
                } = **inner
            {
                lookup_peers.push(peer);
            }
        }
        assert!(
            lookup_peers.len() >= 2,
            "expected lookup fanout to both peers"
        );

        let response_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 16,
            phase: RpcPhase::GetLookup,
            peer: lookup_peers[0],
            response: DhtResponse::Value {
                entries: vec![make_value(12, key, make_realm(1), b"first", 2_000)],
                closer_nodes: Vec::new(),
            },
        }));

        assert!(
            response_effects
                .iter()
                .all(|effect| !matches!(effect, DhtEffect::Output(_)))
        );

        let complete = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 16,
            phase: RpcPhase::GetLookup,
            peer: lookup_peers[1],
            response: DhtResponse::Value {
                entries: vec![make_value(13, key, make_realm(1), b"second", 2_000)],
                closer_nodes: Vec::new(),
            },
        }));
        assert!(complete.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Completed {
                    op_id: 16,
                    result: DhtOutputValue::GetValues { values, stats }
                }) if values.iter().any(|entry| entry.value == b"first".to_vec())
                    && values.iter().any(|entry| entry.value == b"second".to_vec())
                    && stats.completed_reason == DhtGetCompletedReason::RemoteValue
                    && stats.local_value_count == 0
                    && stats.remote_value_count == 2
                    && stats.queried_peer_count == lookup_peers.len()
            )
        }));
    }

    #[test]
    fn get_stats_include_peer_errors_until_remote_value() {
        let local_secret = iroh::SecretKey::from_bytes(&[61u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

        let peer_a = make_node(28);
        let peer_b = make_node(29);
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_a }));
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_b }));

        let op_id = 24;
        let key = DhtKeyId::from_data(b"get-stats-peer-errors");
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id,
            key,
            realm_filter: None,
            trace_context: None,
        }));

        let local_effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id,
            stage: StorageStage::GetLocalRead,
            entries: Vec::new(),
        }));

        let mut lookup_peers = Vec::new();
        for effect in &local_effects {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    op_id: request_op_id,
                    phase: RpcPhase::GetLookup,
                    peer,
                    ..
                } = **inner
                && request_op_id == op_id
            {
                lookup_peers.push(peer);
            }
        }
        assert_eq!(lookup_peers.len(), 2);

        let failed_peer = lookup_peers[0];
        let value_peer = lookup_peers[1];
        let error_effects = state.step(DhtInput::Io(DhtIo::RpcError {
            op_id,
            phase: RpcPhase::GetLookup,
            peer: failed_peer,
            error: DhtIoError::Timeout,
        }));
        assert!(
            error_effects
                .iter()
                .all(|effect| !matches!(effect, DhtEffect::Output(_)))
        );

        let response_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id,
            phase: RpcPhase::GetLookup,
            peer: value_peer,
            response: DhtResponse::Value {
                entries: vec![make_value(30, key, make_realm(1), b"after-error", 2_000)],
                closer_nodes: Vec::new(),
            },
        }));

        let (values, stats) = response_effects
            .iter()
            .find_map(|effect| {
                if let DhtEffect::Output(DhtOutput::Completed {
                    op_id: completed_op_id,
                    result: DhtOutputValue::GetValues { values, stats },
                }) = effect
                    && *completed_op_id == op_id
                {
                    Some((values, stats))
                } else {
                    None
                }
            })
            .expect("get should complete after remote value");

        assert!(
            values
                .iter()
                .any(|entry| entry.value == b"after-error".to_vec())
        );
        assert_eq!(stats.completed_reason, DhtGetCompletedReason::RemoteValue);
        assert_eq!(stats.local_value_count, 0);
        assert_eq!(stats.remote_value_count, 1);
        assert_eq!(stats.queried_peer_count, lookup_peers.len());
        assert!(
            lookup_peers
                .iter()
                .all(|peer| stats.queried_peers.contains(peer))
        );
        assert_eq!(stats.peer_error_count, 1);
        assert_eq!(stats.peer_errors.len(), 1);
        assert_eq!(stats.peer_errors[0].peer, failed_peer);
        assert_eq!(stats.peer_errors[0].error, "timeout");
    }

    #[test]
    fn bootstrap_partial_success_completes_when_other_peer_times_out() {
        let local_secret = iroh::SecretKey::from_bytes(&[47u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let peer_a = make_node(14);
        let peer_b = make_node(15);
        let _ = state.step(DhtInput::Cmd(DhtCmd::Bootstrap {
            op_id: 17,
            nodes: vec![peer_a, peer_b],
            trace_context: None,
        }));

        let first = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 17,
            phase: RpcPhase::Bootstrap,
            peer: peer_a,
            response: DhtResponse::Pong,
        }));
        assert!(
            first
                .iter()
                .all(|effect| !matches!(effect, DhtEffect::Output(_)))
        );

        let second = state.step(DhtInput::Io(DhtIo::RpcError {
            op_id: 17,
            phase: RpcPhase::Bootstrap,
            peer: peer_b,
            error: DhtIoError::Timeout,
        }));
        assert!(second.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Completed {
                    op_id: 17,
                    result: DhtOutputValue::Unit
                })
            )
        }));
    }

    #[test]
    fn wrong_rpc_phase_is_ignored_and_operation_continues() {
        let local_secret = iroh::SecretKey::from_bytes(&[48u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let peer = make_node(16);
        let _ = state.step(DhtInput::Cmd(DhtCmd::Bootstrap {
            op_id: 18,
            nodes: vec![peer],
            trace_context: None,
        }));

        let wrong_phase = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 18,
            phase: RpcPhase::GetLookup,
            peer,
            response: DhtResponse::Pong,
        }));
        assert!(wrong_phase.is_empty());

        let complete = state.step(DhtInput::Io(DhtIo::RpcResponse {
            op_id: 18,
            phase: RpcPhase::Bootstrap,
            peer,
            response: DhtResponse::Pong,
        }));
        assert!(complete.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Completed {
                    op_id: 18,
                    result: DhtOutputValue::Unit
                })
            )
        }));
    }

    #[test]
    fn wrong_stage_ignored() {
        let local_secret = iroh::SecretKey::from_bytes(&[49u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let key = DhtKeyId::from_data(b"wrong-stage");
        let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
            op_id: 19,
            key,
            realm_filter: None,
            trace_context: None,
        }));

        let wrong_stage = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: 19,
            stage: StorageStage::PutLocalRead,
            entries: Vec::new(),
        }));
        assert!(wrong_stage.is_empty());

        let complete = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: 19,
            stage: StorageStage::GetLocalRead,
            entries: Vec::new(),
        }));
        assert!(complete.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Failed { op_id: 19, .. })
            )
        }));
    }

    #[test]
    fn inbound_put_missing_signature_returns_invalid_signature_error() {
        let local_secret = iroh::SecretKey::from_bytes(&[50u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let publisher = make_node(17);
        let key = DhtKeyId::from_data(b"missing-signature");
        let realm_id = make_realm(1);
        let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
            inbound_id: 7,
            peer: make_node(18),
            request: DhtRequest::PutValue {
                key,
                realm_id,
                value: b"value".to_vec(),
                expires_at: 30,
                revision: 1,
                publisher,
                signature: None,
            },
            trace_context: None,
        }));

        assert!(effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                    **inner,
                    DhtIoRequest::RpcResponse {
                        inbound_id: 7,
                        response: DhtResponse::Error {
                            code: ErrorCode::InvalidSignature,
                            ..
                        }
                    }
                ),
                _ => false,
            }
        }));
    }

    #[test]
    fn expired_put_rejected() {
        let local_secret = iroh::SecretKey::from_bytes(&[62u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let publisher_secret = iroh::SecretKey::from_bytes(&[63u8; 32]);
        let key = DhtKeyId::from_data(b"zero-ttl-inbound-put");
        let realm_id = make_realm(1);
        let value = b"value".to_vec();
        let publisher = publisher_secret.public();
        let expires_at = 0;
        let revision = 1;
        let signed_data =
            signed_record_bytes(&key, &publisher, &realm_id, &value, expires_at, revision);
        let signature = publisher_secret.sign(&signed_data);

        let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
            inbound_id: 11,
            peer: make_node(28),
            request: DhtRequest::PutValue {
                key,
                realm_id,
                value,
                expires_at,
                revision,
                publisher,
                signature: Some(signature),
            },
            trace_context: None,
        }));

        assert!(effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                    &(**inner),
                    DhtIoRequest::RpcResponse {
                        inbound_id: 11,
                        response: DhtResponse::Error {
                            code: ErrorCode::InvalidRequest,
                            message,
                        }
                    } if message.contains("expiry")
                ),
                _ => false,
            }
        }));
        assert!(!effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::IoRequest(inner)
                    if matches!(&**inner, DhtIoRequest::StorageMerge { .. })
            )
        }));
    }

    #[test]
    fn inbound_get_filters_response_entries_by_realm() {
        let local_secret = iroh::SecretKey::from_bytes(&[60u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

        let key = DhtKeyId::from_data(b"inbound-get-realm-filter");
        let realm_filter = make_realm(4);

        let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
            inbound_id: 10,
            peer: make_node(25),
            request: DhtRequest::GetValue {
                key,
                realm_filter: Some(realm_filter),
            },
            trace_context: None,
        }));

        let op_id = effects
            .iter()
            .find_map(|effect| {
                if let DhtEffect::IoRequest(inner) = effect
                    && let DhtIoRequest::StorageRead {
                        op_id,
                        stage: StorageStage::InboundGetRead,
                        key: req_key,
                        realm_filter: Some(req_realm_filter),
                        now_secs,
                    } = &(**inner)
                {
                    if *req_key == key && *req_realm_filter == realm_filter && *now_secs == 1_000 {
                        Some(op_id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .expect("inbound get should enqueue filtered storage read");

        let response_effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
            op_id: *op_id,
            stage: StorageStage::InboundGetRead,
            entries: vec![
                make_entry(26, key, realm_filter, b"allowed", 2_000),
                make_entry(27, key, make_realm(5), b"foreign", 2_000),
            ],
        }));

        assert!(response_effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                    &(**inner),
                    DhtIoRequest::RpcResponse {
                        inbound_id: 10,
                        response: DhtResponse::Value {
                            entries,
                            closer_nodes: _,
                        },
                } if entries.len() == 1
                    && entries[0].realm_id == realm_filter
                    && entries[0].value == b"allowed".to_vec()),
                _ => false,
            }
        }));
    }

    #[test]
    fn inbound_put_invalid_signature_returns_invalid_signature_error() {
        let local_secret = iroh::SecretKey::from_bytes(&[51u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let publisher_secret = iroh::SecretKey::from_bytes(&[52u8; 32]);
        let attacker_secret = iroh::SecretKey::from_bytes(&[53u8; 32]);

        let key = DhtKeyId::from_data(b"invalid-signature");
        let realm_id = make_realm(1);
        let value = b"value".to_vec();
        let expires_at: u64 = 45;
        let revision = 1;
        let publisher = publisher_secret.public();
        let signed_data =
            signed_record_bytes(&key, &publisher, &realm_id, &value, expires_at, revision);
        let invalid_signature = attacker_secret.sign(&signed_data);

        let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
            inbound_id: 8,
            peer: make_node(19),
            request: DhtRequest::PutValue {
                key,
                realm_id,
                value,
                expires_at,
                revision,
                publisher,
                signature: Some(invalid_signature),
            },
            trace_context: None,
        }));

        assert!(effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                    **inner,
                    DhtIoRequest::RpcResponse {
                        inbound_id: 8,
                        response: DhtResponse::Error {
                            code: ErrorCode::InvalidSignature,
                            ..
                        }
                    }
                ),
                _ => false,
            }
        }));
    }

    #[test]
    fn valid_put_merges() {
        let local_secret = iroh::SecretKey::from_bytes(&[56u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let publisher_secret = iroh::SecretKey::from_bytes(&[57u8; 32]);
        let key = DhtKeyId::from_data(b"valid-signature");
        let realm_id = make_realm(1);
        let value = b"value".to_vec();
        let expires_at: u64 = 45;
        let revision = 1;
        let publisher = publisher_secret.public();

        let signed_data =
            signed_record_bytes(&key, &publisher, &realm_id, &value, expires_at, revision);
        let signature = publisher_secret.sign(&signed_data);

        let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
            inbound_id: 9,
            peer: make_node(22),
            request: DhtRequest::PutValue {
                key,
                realm_id,
                value,
                expires_at,
                revision,
                publisher,
                signature: Some(signature),
            },
            trace_context: None,
        }));

        assert!(effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::StorageMerge {
                    stage: StorageStage::InboundPutMerge,
                    key: req_key,
                    ..
                } if req_key == key),
                _ => false,
            }
        }));
    }

    #[test]
    fn maintenance_ping_timeout_removes_peer_from_routing_table() {
        let local_secret = iroh::SecretKey::from_bytes(&[54u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let peer = make_node(20);
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));

        let before = state.step(DhtInput::Cmd(DhtCmd::RoutingTableSize { op_id: 20 }));
        assert!(matches!(
            before.as_slice(),
            [DhtEffect::Output(DhtOutput::Completed {
                op_id: 20,
                result: DhtOutputValue::RoutingTableSize(1)
            })]
        ));

        let tick_effects = state.step(DhtInput::Tick {
            now_tick: 1,
            now_secs: 0,
        });
        assert!(tick_effects.iter().any(|effect| {
            match effect {
                DhtEffect::IoRequest(inner) => matches!(
                &(**inner),
                DhtIoRequest::RpcRequest {
                    phase: RpcPhase::MaintenancePing,
                    peer: req_peer,
                    trace_context: None,
                    ..
                } if *req_peer == peer),
                _ => false,
            }
        }));

        let _ = state.step(DhtInput::Tick {
            now_tick: 1 + RPC_TIMEOUT_TICKS,
            now_secs: 0,
        });

        let after = state.step(DhtInput::Cmd(DhtCmd::RoutingTableSize { op_id: 21 }));
        assert!(matches!(
            after.as_slice(),
            [DhtEffect::Output(DhtOutput::Completed {
                op_id: 21,
                result: DhtOutputValue::RoutingTableSize(0)
            })]
        ));
    }

    #[test]
    fn stale_maintenance_preserves() {
        let local_secret = make_secret(94);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);
        let peer = make_node(95);
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
        let _ = state.step(DhtInput::Tick {
            now_tick: 1,
            now_secs: 0,
        });
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
        let _ = state.step(DhtInput::Tick {
            now_tick: 1 + RPC_TIMEOUT_TICKS,
            now_secs: 0,
        });

        assert!(
            state
                .routing_table
                .all_peers()
                .iter()
                .any(|entry| entry.node_id == peer)
        );
    }

    #[test]
    fn unknown_operation_io_is_ignored() {
        let local_secret = iroh::SecretKey::from_bytes(&[55u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let effects = state.step(DhtInput::Io(DhtIo::RpcError {
            op_id: 999,
            phase: RpcPhase::Bootstrap,
            peer: make_node(21),
            error: DhtIoError::Timeout,
        }));
        assert!(effects.is_empty());
    }
}
