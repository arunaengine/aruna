use std::collections::{BTreeMap, HashMap, HashSet};

use aruna_core::events::DhtEntry;
use aruna_core::id::{DhtKeyId, NodeId, NodeIdExt};
use smallvec::SmallVec;

use super::constants::{DRIVER_TICK_INTERVAL, RPC_TIMEOUT_TICKS};
use super::kbucket::{InsertResult, K, PeerInfo, RoutingTable};
use super::protocol::{
    CallerId, DhtCmd, DhtEffect, DhtInput, DhtIo, DhtIoError, DhtIoRequest, DhtOutput,
    DhtOutputValue, InboundId, OpId, RequestId, StorageId,
};
use super::rpc::{DhtRequest, DhtResponse, ErrorCode, StoredValue};
use super::storage::{CLEANUP_PAGE_SIZE, StoredEntry, live_entries, merge_entry};

pub struct DhtStateMachine {
    local_id: NodeId,
    secret_key: iroh::SecretKey,
    routing_table: RoutingTable,
    next_request_id: u64,
    next_storage_id: u64,
    next_op_id: u64,
    inflight_rpc: HashMap<RequestId, RpcRoute>,
    inflight_storage: HashMap<StorageId, StorageRoute>,
    pending_ops: HashMap<OpId, PendingOp>,
    deadlines: BTreeMap<u64, Vec<RequestId>>,
    cleanup_cursor: Option<Vec<u8>>,
    cleanup_inflight: bool,
    shutdown_requested: bool,
    current_tick: u64,
    now_secs: u64,
}

impl std::fmt::Debug for DhtStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtStateMachine")
            .field("local_id", &self.local_id)
            .field("routing_table_size", &self.routing_table.len())
            .field("pending_ops", &self.pending_ops.len())
            .field("inflight_rpc", &self.inflight_rpc.len())
            .field("inflight_storage", &self.inflight_storage.len())
            .field("cleanup_cursor", &self.cleanup_cursor)
            .field("cleanup_inflight", &self.cleanup_inflight)
            .field("shutdown_requested", &self.shutdown_requested)
            .field("current_tick", &self.current_tick)
            .field("now_secs", &self.now_secs)
            .finish()
    }
}

#[derive(Debug)]
enum PendingOp {
    Put(PutOp),
    Get(GetOp),
    Bootstrap(BootstrapOp),
    EvictionPing(EvictionPingOp),
    MaintenancePing(MaintenancePingOp),
}

#[derive(Debug)]
struct PutOp {
    caller_id: Option<CallerId>,
    key: DhtKeyId,
    value: Vec<u8>,
    ttl_secs: u64,
    signature: iroh::Signature,
    outstanding_rpc: usize,
}

#[derive(Debug)]
struct GetOp {
    caller_id: Option<CallerId>,
    key: DhtKeyId,
    values: Vec<DhtEntry>,
    seen_publishers: HashSet<NodeId>,
    outstanding_rpc: usize,
}

#[derive(Debug)]
struct BootstrapOp {
    caller_id: Option<CallerId>,
    outstanding_rpc: usize,
    active_nodes: usize,
}

#[derive(Debug)]
struct EvictionPingOp {
    oldest_node: NodeId,
    bucket_idx: usize,
    pending_peer: PeerInfo,
    outstanding_rpc: usize,
}

#[derive(Debug)]
struct MaintenancePingOp {
    peer: NodeId,
    outstanding_rpc: usize,
}

#[derive(Debug)]
enum RpcRoute {
    Op { op_id: OpId },
}

#[derive(Debug)]
enum StorageRoute {
    PutLocalRead {
        op_id: OpId,
        key: DhtKeyId,
        new_entry: StoredEntry,
    },
    PutLocalWrite {
        op_id: OpId,
    },
    GetLocalRead {
        op_id: OpId,
    },
    InboundGetValue {
        inbound_id: InboundId,
        key: DhtKeyId,
    },
    InboundPutRead {
        inbound_id: InboundId,
        key: DhtKeyId,
        new_entry: StoredEntry,
    },
    InboundPutWrite {
        inbound_id: InboundId,
    },
    CleanupIter,
    CleanupWrite,
    CleanupDelete,
}

impl DhtStateMachine {
    pub fn new(local_id: NodeId, secret_key: iroh::SecretKey, start_unix_secs: u64) -> Self {
        Self {
            local_id,
            secret_key,
            routing_table: RoutingTable::new(local_id),
            next_request_id: 1,
            next_storage_id: 1,
            next_op_id: 1,
            inflight_rpc: HashMap::new(),
            inflight_storage: HashMap::new(),
            pending_ops: HashMap::new(),
            deadlines: BTreeMap::new(),
            cleanup_cursor: None,
            cleanup_inflight: false,
            shutdown_requested: false,
            current_tick: 0,
            now_secs: start_unix_secs,
        }
    }

    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    pub fn step(&mut self, input: DhtInput) -> SmallVec<[DhtEffect; 4]> {
        let mut out = SmallVec::<[DhtEffect; 4]>::new();

        match input {
            DhtInput::Cmd(cmd) => self.handle_cmd(cmd, &mut out),
            DhtInput::Io(io) => self.handle_io(io, &mut out),
            DhtInput::Tick { now_tick } => self.handle_tick(now_tick, &mut out),
            DhtInput::ShutdownRequested => self.handle_shutdown_requested(&mut out),
            DhtInput::ContinueOp { op_id } => self.handle_continue(op_id, &mut out),
        }

        out
    }

    fn handle_cmd(&mut self, cmd: DhtCmd, out: &mut SmallVec<[DhtEffect; 4]>) {
        if self.shutdown_requested {
            let caller_id = match cmd {
                DhtCmd::Put { caller_id, .. }
                | DhtCmd::Get { caller_id, .. }
                | DhtCmd::Bootstrap { caller_id, .. }
                | DhtCmd::RoutingTableSize { caller_id }
                | DhtCmd::Shutdown { caller_id } => caller_id,
                DhtCmd::AddPeer { .. } => None,
            };

            if let Some(caller_id) = caller_id {
                out.push(DhtEffect::Output(DhtOutput::Failed {
                    caller_id,
                    error: DhtIoError::Shutdown,
                }));
            }
            return;
        }

        match cmd {
            DhtCmd::Put {
                key,
                value,
                ttl,
                caller_id,
            } => self.handle_cmd_put(key, value, ttl, caller_id, out),
            DhtCmd::Get { key, caller_id } => self.handle_cmd_get(key, caller_id, out),
            DhtCmd::Bootstrap { nodes, caller_id } => {
                self.handle_cmd_bootstrap(nodes, caller_id, out)
            }
            DhtCmd::RoutingTableSize { caller_id } => {
                if let Some(caller_id) = caller_id {
                    out.push(DhtEffect::Output(DhtOutput::Completed {
                        caller_id,
                        result: DhtOutputValue::RoutingTableSize(self.routing_table.len()),
                    }));
                }
            }
            DhtCmd::AddPeer { node_id } => self.insert_peer(node_id, out),
            DhtCmd::Shutdown { caller_id } => {
                self.shutdown_requested = true;
                self.fail_all_pending(DhtIoError::Shutdown, out);
                if let Some(caller_id) = caller_id {
                    out.push(DhtEffect::Output(DhtOutput::Completed {
                        caller_id,
                        result: DhtOutputValue::Unit,
                    }));
                }
                out.push(DhtEffect::Output(DhtOutput::ShutdownComplete));
            }
        }
    }

    fn handle_cmd_put(
        &mut self,
        key: DhtKeyId,
        value: Vec<u8>,
        ttl: std::time::Duration,
        caller_id: Option<CallerId>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let ttl_secs = ttl.as_secs();
        let expires_at = self.now_secs.saturating_add(ttl_secs);

        let mut signed_data = Vec::with_capacity(32 + value.len() + 8);
        signed_data.extend_from_slice(key.as_bytes());
        signed_data.extend_from_slice(&value);
        signed_data.extend_from_slice(&ttl_secs.to_le_bytes());
        let signature = self.secret_key.sign(&signed_data);

        let new_entry = StoredEntry {
            publisher: self.local_id,
            value: value.clone(),
            expires_at,
            signature: Some(signature),
        };

        let op_id = self.alloc_op_id();
        self.pending_ops.insert(
            op_id,
            PendingOp::Put(PutOp {
                caller_id,
                key,
                value,
                ttl_secs,
                signature,
                outstanding_rpc: 0,
            }),
        );

        let storage_id = self.alloc_storage_id();
        self.inflight_storage.insert(
            storage_id,
            StorageRoute::PutLocalRead {
                op_id,
                key,
                new_entry,
            },
        );
        out.push(DhtEffect::IoRequest(DhtIoRequest::StorageRead {
            storage_id,
            key,
        }));
    }

    fn handle_cmd_get(
        &mut self,
        key: DhtKeyId,
        caller_id: Option<CallerId>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let op_id = self.alloc_op_id();
        self.pending_ops.insert(
            op_id,
            PendingOp::Get(GetOp {
                caller_id,
                key,
                values: Vec::new(),
                seen_publishers: HashSet::new(),
                outstanding_rpc: 0,
            }),
        );

        let storage_id = self.alloc_storage_id();
        self.inflight_storage
            .insert(storage_id, StorageRoute::GetLocalRead { op_id });
        out.push(DhtEffect::IoRequest(DhtIoRequest::StorageRead {
            storage_id,
            key,
        }));
    }

    fn handle_cmd_bootstrap(
        &mut self,
        nodes: Vec<NodeId>,
        caller_id: Option<CallerId>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let mut unique: Vec<NodeId> = nodes
            .into_iter()
            .filter(|node| *node != self.local_id)
            .collect();
        unique.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        unique.dedup();

        if unique.is_empty() {
            if let Some(caller_id) = caller_id {
                out.push(DhtEffect::Output(DhtOutput::Completed {
                    caller_id,
                    result: DhtOutputValue::Unit,
                }));
            }
            return;
        }

        let op_id = self.alloc_op_id();
        self.pending_ops.insert(
            op_id,
            PendingOp::Bootstrap(BootstrapOp {
                caller_id,
                outstanding_rpc: 0,
                active_nodes: 0,
            }),
        );

        let mut sent = 0usize;
        for node in unique {
            if self
                .queue_rpc_request(op_id, node, DhtRequest::Ping, out)
                .is_some()
            {
                sent = sent.saturating_add(1);
            }
        }

        if let Some(PendingOp::Bootstrap(op)) = self.pending_ops.get_mut(&op_id) {
            op.outstanding_rpc = sent;
            if sent == 0 {
                let caller_id = op.caller_id;
                self.pending_ops.remove(&op_id);
                if let Some(caller_id) = caller_id {
                    out.push(DhtEffect::Output(DhtOutput::Failed {
                        caller_id,
                        error: DhtIoError::Network("no bootstrap nodes reachable".to_string()),
                    }));
                }
            }
        }
    }

    fn handle_io(&mut self, io: DhtIo, out: &mut SmallVec<[DhtEffect; 4]>) {
        match io {
            DhtIo::RpcResponse {
                request_id,
                peer,
                response,
            } => {
                let Some(route) = self.inflight_rpc.remove(&request_id) else {
                    return;
                };
                match route {
                    RpcRoute::Op { op_id } => {
                        self.handle_op_rpc_response(op_id, peer, response, out);
                    }
                }
            }
            DhtIo::RpcError {
                request_id,
                peer,
                error,
            } => {
                let Some(route) = self.inflight_rpc.remove(&request_id) else {
                    return;
                };
                match route {
                    RpcRoute::Op { op_id } => self.handle_op_rpc_error(op_id, peer, error, out),
                }
            }
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
                storage_id,
                entries,
            } => {
                let Some(route) = self.inflight_storage.remove(&storage_id) else {
                    return;
                };
                self.handle_storage_read(route, entries, out);
            }
            DhtIo::StorageWriteResult { storage_id } => {
                let Some(route) = self.inflight_storage.remove(&storage_id) else {
                    return;
                };
                self.handle_storage_write(route, out);
            }
            DhtIo::StorageDeleteResult { storage_id } => {
                let Some(route) = self.inflight_storage.remove(&storage_id) else {
                    return;
                };
                match route {
                    StorageRoute::InboundPutWrite { inbound_id } => {
                        out.push(DhtEffect::IoRequest(DhtIoRequest::DropInbound {
                            inbound_id,
                        }));
                    }
                    StorageRoute::CleanupDelete => {}
                    StorageRoute::PutLocalRead { .. }
                    | StorageRoute::PutLocalWrite { .. }
                    | StorageRoute::GetLocalRead { .. }
                    | StorageRoute::InboundGetValue { .. }
                    | StorageRoute::InboundPutRead { .. }
                    | StorageRoute::CleanupIter
                    | StorageRoute::CleanupWrite => {}
                }
            }
            DhtIo::StorageIterResult {
                storage_id,
                values,
                next_start_after,
            } => {
                let Some(route) = self.inflight_storage.remove(&storage_id) else {
                    return;
                };
                self.handle_storage_iter(route, values, next_start_after, out);
            }
            DhtIo::StorageError { storage_id, error } => {
                let Some(route) = self.inflight_storage.remove(&storage_id) else {
                    return;
                };
                self.handle_storage_error(route, error, out);
            }
            DhtIo::PeerSeen { peer } => self.insert_peer(peer, out),
            DhtIo::DispatcherClosed => {
                self.fail_all_pending(DhtIoError::DispatcherClosed, out);
                self.shutdown_requested = true;
            }
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

        let mut expired = Vec::new();
        while let Some((deadline, _)) = self.deadlines.first_key_value() {
            if *deadline > now_tick {
                break;
            }
            if let Some((_, request_ids)) = self.deadlines.pop_first() {
                expired.extend(request_ids);
            }
        }

        for request_id in expired {
            let Some(route) = self.inflight_rpc.remove(&request_id) else {
                continue;
            };
            match route {
                RpcRoute::Op { op_id } => {
                    self.handle_op_rpc_error(op_id, self.local_id, DhtIoError::Timeout, out)
                }
            }
        }

        self.schedule_maintenance_ping(now_tick, out);

        if !self.cleanup_inflight {
            let storage_id = self.alloc_storage_id();
            self.cleanup_inflight = true;
            self.inflight_storage
                .insert(storage_id, StorageRoute::CleanupIter);
            out.push(DhtEffect::IoRequest(DhtIoRequest::StorageIter {
                storage_id,
                start_after: self.cleanup_cursor.clone(),
                limit: CLEANUP_PAGE_SIZE,
            }));
        }
    }

    fn handle_shutdown_requested(&mut self, out: &mut SmallVec<[DhtEffect; 4]>) {
        if self.shutdown_requested {
            return;
        }

        self.shutdown_requested = true;
        self.fail_all_pending(DhtIoError::Shutdown, out);
        out.push(DhtEffect::Output(DhtOutput::ShutdownComplete));
    }

    fn handle_continue(&mut self, _op_id: OpId, _out: &mut SmallVec<[DhtEffect; 4]>) {}

    fn handle_storage_read(
        &mut self,
        route: StorageRoute,
        entries: Vec<StoredEntry>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        match route {
            StorageRoute::PutLocalRead {
                op_id,
                key,
                new_entry,
            } => {
                let merged = merge_entry(entries, new_entry, self.now_secs);
                let storage_id = self.alloc_storage_id();
                self.inflight_storage
                    .insert(storage_id, StorageRoute::PutLocalWrite { op_id });
                out.push(DhtEffect::IoRequest(DhtIoRequest::StorageWrite {
                    storage_id,
                    key,
                    entries: merged,
                }));
            }
            StorageRoute::GetLocalRead { op_id } => {
                let Some(PendingOp::Get(mut op)) = self.pending_ops.remove(&op_id) else {
                    return;
                };

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

                let peers = self.routing_table.closest(op.key.as_bytes(), K);
                let mut sent = 0usize;
                for peer in peers {
                    if self
                        .queue_rpc_request(
                            op_id,
                            peer.node_id,
                            DhtRequest::GetValue { key: op.key },
                            out,
                        )
                        .is_some()
                    {
                        sent = sent.saturating_add(1);
                    }
                }

                op.outstanding_rpc = sent;
                if op.outstanding_rpc == 0 {
                    if let Some(caller_id) = op.caller_id {
                        out.push(DhtEffect::Output(DhtOutput::Completed {
                            caller_id,
                            result: DhtOutputValue::GetValues(op.values),
                        }));
                    }
                } else {
                    self.pending_ops.insert(op_id, PendingOp::Get(op));
                }
            }
            StorageRoute::InboundGetValue { inbound_id, key } => {
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
                    .closest(key.as_bytes(), K)
                    .into_iter()
                    .map(|peer| peer.node_id)
                    .collect();

                out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                    inbound_id,
                    response: DhtResponse::Value {
                        entries: values,
                        closer_nodes,
                    },
                }));
            }
            StorageRoute::InboundPutRead {
                inbound_id,
                key,
                new_entry,
            } => {
                let merged = merge_entry(entries, new_entry, self.now_secs);
                let storage_id = self.alloc_storage_id();
                self.inflight_storage
                    .insert(storage_id, StorageRoute::InboundPutWrite { inbound_id });
                out.push(DhtEffect::IoRequest(DhtIoRequest::StorageWrite {
                    storage_id,
                    key,
                    entries: merged,
                }));
            }
            StorageRoute::PutLocalWrite { .. } => {}
            StorageRoute::InboundPutWrite { inbound_id } => {
                out.push(DhtEffect::IoRequest(DhtIoRequest::DropInbound {
                    inbound_id,
                }));
            }
            StorageRoute::CleanupIter
            | StorageRoute::CleanupWrite
            | StorageRoute::CleanupDelete => {}
        }
    }

    fn handle_storage_iter(
        &mut self,
        route: StorageRoute,
        values: Vec<(Vec<u8>, Vec<StoredEntry>)>,
        next_start_after: Option<Vec<u8>>,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let StorageRoute::CleanupIter = route else {
            return;
        };

        for (raw_key, entries) in values {
            let Ok(key_bytes): Result<[u8; 32], _> = raw_key.as_slice().try_into() else {
                continue;
            };
            let key = DhtKeyId::from_bytes(key_bytes);

            let original_len = entries.len();
            let live = live_entries(entries, self.now_secs);
            if live.is_empty() {
                let storage_id = self.alloc_storage_id();
                self.inflight_storage
                    .insert(storage_id, StorageRoute::CleanupDelete);
                out.push(DhtEffect::IoRequest(DhtIoRequest::StorageDelete {
                    storage_id,
                    key,
                }));
            } else if live.len() < original_len {
                let storage_id = self.alloc_storage_id();
                self.inflight_storage
                    .insert(storage_id, StorageRoute::CleanupWrite);
                out.push(DhtEffect::IoRequest(DhtIoRequest::StorageWrite {
                    storage_id,
                    key,
                    entries: live,
                }));
            }
        }

        self.cleanup_cursor = next_start_after;
        self.cleanup_inflight = false;
    }

    fn handle_storage_write(&mut self, route: StorageRoute, out: &mut SmallVec<[DhtEffect; 4]>) {
        match route {
            StorageRoute::PutLocalWrite { op_id } => {
                let Some(PendingOp::Put(mut op)) = self.pending_ops.remove(&op_id) else {
                    return;
                };

                let peers = self.routing_table.closest(op.key.as_bytes(), K);
                let mut sent = 0usize;
                for peer in peers {
                    if self
                        .queue_rpc_request(
                            op_id,
                            peer.node_id,
                            DhtRequest::PutValue {
                                key: op.key,
                                value: op.value.clone(),
                                ttl_secs: op.ttl_secs,
                                publisher: self.local_id,
                                signature: Some(op.signature),
                            },
                            out,
                        )
                        .is_some()
                    {
                        sent = sent.saturating_add(1);
                    }
                }

                op.outstanding_rpc = sent;
                if op.outstanding_rpc == 0 {
                    if let Some(caller_id) = op.caller_id {
                        out.push(DhtEffect::Output(DhtOutput::Completed {
                            caller_id,
                            result: DhtOutputValue::Unit,
                        }));
                    }
                } else {
                    self.pending_ops.insert(op_id, PendingOp::Put(op));
                }
            }
            StorageRoute::InboundPutWrite { inbound_id } => {
                out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                    inbound_id,
                    response: DhtResponse::Stored,
                }));
            }
            StorageRoute::CleanupWrite => {}
            StorageRoute::PutLocalRead { .. }
            | StorageRoute::GetLocalRead { .. }
            | StorageRoute::InboundGetValue { .. }
            | StorageRoute::InboundPutRead { .. }
            | StorageRoute::CleanupIter
            | StorageRoute::CleanupDelete => {}
        }
    }

    fn handle_storage_error(
        &mut self,
        route: StorageRoute,
        error: DhtIoError,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        match route {
            StorageRoute::PutLocalRead { op_id, .. }
            | StorageRoute::PutLocalWrite { op_id }
            | StorageRoute::GetLocalRead { op_id } => {
                self.fail_op(op_id, error, out);
            }
            StorageRoute::CleanupIter => {
                self.cleanup_inflight = false;
            }
            StorageRoute::CleanupWrite | StorageRoute::CleanupDelete => {}
            StorageRoute::InboundGetValue { inbound_id, .. }
            | StorageRoute::InboundPutRead { inbound_id, .. }
            | StorageRoute::InboundPutWrite { inbound_id } => {
                out.push(DhtEffect::IoRequest(DhtIoRequest::RpcResponse {
                    inbound_id,
                    response: DhtResponse::Error {
                        code: ErrorCode::Internal,
                        message: "storage error".to_string(),
                    },
                }));
            }
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
                let storage_id = self.alloc_storage_id();
                self.inflight_storage.insert(
                    storage_id,
                    StorageRoute::InboundGetValue { inbound_id, key },
                );
                out.push(DhtEffect::IoRequest(DhtIoRequest::StorageRead {
                    storage_id,
                    key,
                }));
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

                let storage_id = self.alloc_storage_id();
                self.inflight_storage.insert(
                    storage_id,
                    StorageRoute::InboundPutRead {
                        inbound_id,
                        key,
                        new_entry,
                    },
                );
                out.push(DhtEffect::IoRequest(DhtIoRequest::StorageRead {
                    storage_id,
                    key,
                }));
            }
        }
    }

    fn handle_op_rpc_response(
        &mut self,
        op_id: OpId,
        peer: NodeId,
        response: DhtResponse,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let Some(op) = self.pending_ops.remove(&op_id) else {
            return;
        };

        match op {
            PendingOp::Put(mut put_op) => {
                put_op.outstanding_rpc = put_op.outstanding_rpc.saturating_sub(1);
                if put_op.outstanding_rpc == 0 {
                    if let Some(caller_id) = put_op.caller_id {
                        out.push(DhtEffect::Output(DhtOutput::Completed {
                            caller_id,
                            result: DhtOutputValue::Unit,
                        }));
                    }
                } else {
                    self.pending_ops.insert(op_id, PendingOp::Put(put_op));
                }
            }
            PendingOp::Get(mut get_op) => {
                if let DhtResponse::Value {
                    entries,
                    closer_nodes,
                } = response
                {
                    let now = self.now_secs;
                    for entry in entries {
                        if entry.expires_at <= now {
                            continue;
                        }
                        if get_op.seen_publishers.insert(entry.publisher) {
                            get_op.values.push(DhtEntry {
                                node_id: entry.publisher,
                                value: entry.value,
                                expires_at: entry.expires_at,
                            });
                        }
                    }

                    for node_id in closer_nodes {
                        self.routing_table.insert(PeerInfo::new(node_id));
                    }
                }

                get_op.outstanding_rpc = get_op.outstanding_rpc.saturating_sub(1);
                if get_op.outstanding_rpc == 0 {
                    if let Some(caller_id) = get_op.caller_id {
                        out.push(DhtEffect::Output(DhtOutput::Completed {
                            caller_id,
                            result: DhtOutputValue::GetValues(get_op.values),
                        }));
                    }
                } else {
                    self.pending_ops.insert(op_id, PendingOp::Get(get_op));
                }
            }
            PendingOp::Bootstrap(mut bootstrap_op) => {
                if matches!(response, DhtResponse::Pong) {
                    self.routing_table.insert(PeerInfo::new(peer));
                    bootstrap_op.active_nodes = bootstrap_op.active_nodes.saturating_add(1);
                }

                bootstrap_op.outstanding_rpc = bootstrap_op.outstanding_rpc.saturating_sub(1);
                if bootstrap_op.outstanding_rpc == 0 {
                    if let Some(caller_id) = bootstrap_op.caller_id {
                        if bootstrap_op.active_nodes == 0 {
                            out.push(DhtEffect::Output(DhtOutput::Failed {
                                caller_id,
                                error: DhtIoError::Network(
                                    "no bootstrap nodes reachable".to_string(),
                                ),
                            }));
                        } else {
                            out.push(DhtEffect::Output(DhtOutput::Completed {
                                caller_id,
                                result: DhtOutputValue::Unit,
                            }));
                        }
                    }
                } else {
                    self.pending_ops
                        .insert(op_id, PendingOp::Bootstrap(bootstrap_op));
                }
            }
            PendingOp::EvictionPing(mut eviction_op) => {
                let peer_alive = matches!(response, DhtResponse::Pong);
                eviction_op.outstanding_rpc = eviction_op.outstanding_rpc.saturating_sub(1);
                if !peer_alive {
                    let _ = self.routing_table.remove(&eviction_op.oldest_node);
                    self.routing_table.insert(eviction_op.pending_peer.clone());
                }

                if eviction_op.outstanding_rpc > 0 {
                    self.pending_ops
                        .insert(op_id, PendingOp::EvictionPing(eviction_op));
                }
            }
            PendingOp::MaintenancePing(mut ping_op) => {
                if matches!(response, DhtResponse::Pong) {
                    self.routing_table.insert(PeerInfo::new(peer));
                }
                ping_op.outstanding_rpc = ping_op.outstanding_rpc.saturating_sub(1);
                if ping_op.outstanding_rpc > 0 {
                    self.pending_ops
                        .insert(op_id, PendingOp::MaintenancePing(ping_op));
                }
            }
        }
    }

    fn handle_op_rpc_error(
        &mut self,
        op_id: OpId,
        _peer: NodeId,
        error: DhtIoError,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) {
        let Some(op) = self.pending_ops.remove(&op_id) else {
            return;
        };

        match op {
            PendingOp::Put(mut put_op) => {
                put_op.outstanding_rpc = put_op.outstanding_rpc.saturating_sub(1);
                if put_op.outstanding_rpc == 0 {
                    if let Some(caller_id) = put_op.caller_id {
                        out.push(DhtEffect::Output(DhtOutput::Completed {
                            caller_id,
                            result: DhtOutputValue::Unit,
                        }));
                    }
                } else {
                    self.pending_ops.insert(op_id, PendingOp::Put(put_op));
                }
            }
            PendingOp::Get(mut get_op) => {
                get_op.outstanding_rpc = get_op.outstanding_rpc.saturating_sub(1);
                if get_op.outstanding_rpc == 0 {
                    if let Some(caller_id) = get_op.caller_id {
                        out.push(DhtEffect::Output(DhtOutput::Completed {
                            caller_id,
                            result: DhtOutputValue::GetValues(get_op.values),
                        }));
                    }
                } else {
                    self.pending_ops.insert(op_id, PendingOp::Get(get_op));
                }
            }
            PendingOp::Bootstrap(mut bootstrap_op) => {
                bootstrap_op.outstanding_rpc = bootstrap_op.outstanding_rpc.saturating_sub(1);
                if bootstrap_op.outstanding_rpc == 0 {
                    if let Some(caller_id) = bootstrap_op.caller_id {
                        if bootstrap_op.active_nodes == 0 {
                            out.push(DhtEffect::Output(DhtOutput::Failed { caller_id, error }));
                        } else {
                            out.push(DhtEffect::Output(DhtOutput::Completed {
                                caller_id,
                                result: DhtOutputValue::Unit,
                            }));
                        }
                    }
                } else {
                    self.pending_ops
                        .insert(op_id, PendingOp::Bootstrap(bootstrap_op));
                }
            }
            PendingOp::EvictionPing(mut eviction_op) => {
                eviction_op.outstanding_rpc = eviction_op.outstanding_rpc.saturating_sub(1);
                let _ = self.routing_table.evict_oldest(eviction_op.bucket_idx);
                self.routing_table.insert(eviction_op.pending_peer.clone());
                if eviction_op.outstanding_rpc > 0 {
                    self.pending_ops
                        .insert(op_id, PendingOp::EvictionPing(eviction_op));
                }
            }
            PendingOp::MaintenancePing(mut ping_op) => {
                ping_op.outstanding_rpc = ping_op.outstanding_rpc.saturating_sub(1);
                let _ = self.routing_table.remove(&ping_op.peer);
                if ping_op.outstanding_rpc > 0 {
                    self.pending_ops
                        .insert(op_id, PendingOp::MaintenancePing(ping_op));
                }
            }
        }
    }

    fn queue_rpc_request(
        &mut self,
        op_id: OpId,
        peer: NodeId,
        request: DhtRequest,
        out: &mut SmallVec<[DhtEffect; 4]>,
    ) -> Option<RequestId> {
        if self.shutdown_requested {
            return None;
        }

        let request_id = self.alloc_request_id();
        self.inflight_rpc.insert(request_id, RpcRoute::Op { op_id });
        let deadline_tick = self.current_tick.saturating_add(RPC_TIMEOUT_TICKS);
        self.deadlines
            .entry(deadline_tick)
            .or_default()
            .push(request_id);
        out.push(DhtEffect::IoRequest(DhtIoRequest::RpcRequest {
            request_id,
            peer,
            request,
            deadline_tick,
        }));
        Some(request_id)
    }

    fn schedule_maintenance_ping(&mut self, now_tick: u64, out: &mut SmallVec<[DhtEffect; 4]>) {
        if self.shutdown_requested {
            return;
        }

        let mut peers = self.routing_table.all_peers();
        if peers.is_empty() {
            return;
        }
        peers.sort_unstable_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
        let idx = (now_tick as usize) % peers.len();
        let peer = peers[idx].node_id;

        let op_id = self.alloc_op_id();
        self.pending_ops.insert(
            op_id,
            PendingOp::MaintenancePing(MaintenancePingOp {
                peer,
                outstanding_rpc: 1,
            }),
        );

        if self
            .queue_rpc_request(op_id, peer, DhtRequest::Ping, out)
            .is_none()
        {
            let _ = self.pending_ops.remove(&op_id);
        }
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

        let op_id = self.alloc_op_id();
        self.pending_ops.insert(
            op_id,
            PendingOp::EvictionPing(EvictionPingOp {
                oldest_node: oldest_peer.node_id,
                bucket_idx,
                pending_peer: pending,
                outstanding_rpc: 1,
            }),
        );

        let _ = self.queue_rpc_request(op_id, oldest_peer.node_id, DhtRequest::Ping, out);
    }

    fn fail_op(&mut self, op_id: OpId, error: DhtIoError, out: &mut SmallVec<[DhtEffect; 4]>) {
        let Some(op) = self.pending_ops.remove(&op_id) else {
            return;
        };
        self.drop_routes_for_op(op_id);

        let caller_id = match op {
            PendingOp::Put(op) => op.caller_id,
            PendingOp::Get(op) => op.caller_id,
            PendingOp::Bootstrap(op) => op.caller_id,
            PendingOp::EvictionPing(_) => None,
            PendingOp::MaintenancePing(_) => None,
        };

        if let Some(caller_id) = caller_id {
            out.push(DhtEffect::Output(DhtOutput::Failed { caller_id, error }));
        }
    }

    fn fail_all_pending(&mut self, error: DhtIoError, out: &mut SmallVec<[DhtEffect; 4]>) {
        let op_ids: Vec<OpId> = self.pending_ops.keys().copied().collect();
        for op_id in op_ids {
            self.fail_op(op_id, error.clone(), out);
        }
    }

    fn drop_routes_for_op(&mut self, op_id: OpId) {
        self.inflight_rpc.retain(|_, route| match route {
            RpcRoute::Op { op_id: route_op } => *route_op != op_id,
        });
        self.inflight_storage.retain(|_, route| match route {
            StorageRoute::PutLocalRead {
                op_id: route_op, ..
            }
            | StorageRoute::PutLocalWrite { op_id: route_op }
            | StorageRoute::GetLocalRead { op_id: route_op } => *route_op != op_id,
            StorageRoute::InboundGetValue { .. }
            | StorageRoute::InboundPutRead { .. }
            | StorageRoute::InboundPutWrite { .. }
            | StorageRoute::CleanupIter
            | StorageRoute::CleanupWrite
            | StorageRoute::CleanupDelete => true,
        });
    }

    fn alloc_request_id(&mut self) -> RequestId {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        id
    }

    fn alloc_storage_id(&mut self) -> StorageId {
        let id = self.next_storage_id;
        self.next_storage_id = self.next_storage_id.saturating_add(1);
        id
    }

    fn alloc_op_id(&mut self) -> OpId {
        let id = self.next_op_id;
        self.next_op_id = self.next_op_id.saturating_add(1);
        id
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

        let effects = state.step(DhtInput::Cmd(DhtCmd::RoutingTableSize {
            caller_id: Some(1),
        }));

        assert!(matches!(
            effects.as_slice(),
            [DhtEffect::Output(DhtOutput::Completed {
                caller_id: 1,
                result: DhtOutputValue::RoutingTableSize(0)
            })]
        ));
    }

    #[test]
    fn add_peer_is_sync_and_emits_eviction_ping_when_full() {
        let local_secret = iroh::SecretKey::from_bytes(&[11u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let peer = make_node(1);
        let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
        assert!(effects.is_empty());
    }

    #[test]
    fn shutdown_fails_new_commands() {
        let local_secret = iroh::SecretKey::from_bytes(&[19u8; 32]);
        let local_id = local_secret.public();
        let mut state = DhtStateMachine::new(local_id, local_secret, 0);

        let _ = state.step(DhtInput::ShutdownRequested);
        let effects = state.step(DhtInput::Cmd(DhtCmd::Get {
            key: DhtKeyId::from_data(b"k"),
            caller_id: Some(9),
        }));

        assert!(matches!(
            effects.as_slice(),
            [DhtEffect::Output(DhtOutput::Failed {
                caller_id: 9,
                error: DhtIoError::Shutdown
            })]
        ));
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
                key,
                value: b"hello".to_vec(),
                ttl: std::time::Duration::from_secs(30),
                caller_id: Some(1),
            }),
            DhtInput::Io(DhtIo::StorageReadResult {
                storage_id: 1,
                entries: Vec::new(),
            }),
            DhtInput::Io(DhtIo::StorageWriteResult { storage_id: 2 }),
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
            nodes: vec![peer],
            caller_id: Some(5),
        }));

        let request_id = effects
            .iter()
            .find_map(|effect| match effect {
                DhtEffect::IoRequest(DhtIoRequest::RpcRequest { request_id, .. }) => {
                    Some(*request_id)
                }
                _ => None,
            })
            .expect("bootstrap should emit rpc request");

        let timeout_effects = state.step(DhtInput::Tick {
            now_tick: RPC_TIMEOUT_TICKS,
        });
        assert!(timeout_effects.iter().any(|effect| {
            matches!(
                effect,
                DhtEffect::Output(DhtOutput::Failed {
                    caller_id: 5,
                    error: DhtIoError::Timeout
                })
            )
        }));

        let late_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
            request_id,
            peer,
            response: DhtResponse::Pong,
        }));
        assert!(late_effects.is_empty());
    }
}
