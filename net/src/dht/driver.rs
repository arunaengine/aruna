use std::collections::HashMap;
use std::time::{Duration, Instant};

use aruna_core::DistributedTraceContext;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::keyspaces::DHT_KEYSPACE;
use aruna_core::structs::RealmId;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use crossfire::{AsyncRx, MAsyncTx, MTx, mpsc};
use iroh::Endpoint;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug_span, field, info_span, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::constants::{
    DRIVER_IO_EVENT_CAPACITY, DRIVER_TICK_INTERVAL, MAX_MESSAGE_SIZE, RPC_TIMEOUT, STORAGE_TIMEOUT,
};
use super::protocol::{
    CLEANUP_OP_ID, DhtCmd, DhtEffect, DhtInput, DhtIo, DhtIoError, DhtIoRequest, DhtOutput,
    DhtOutputValue, InboundId, OpId, RpcPhase, StorageStage,
};
use super::rpc::{
    DHT_ALPN, DhtRequest, DhtResponse, ErrorCode, decode_request_with_trace_context,
    decode_response, encode_request_with_trace_context, encode_response,
};
use super::state::DhtStateMachine;
use super::storage::{CLEANUP_PAGE_SIZE, StoredEntry, decode_entries, encode_entries};
use crate::telemetry::{
    current_trace_context, duration_ms, extract_trace_context, record_duration_ms,
    warn_if_slow_iroh_phase, warn_if_slow_iroh_request,
};

pub type CallerOutcome = std::result::Result<DhtOutputValue, DhtIoError>;
pub type InboundDhtStream = (Connection, SendStream, RecvStream, NodeId);

pub type DriverCmdSender = MTx<mpsc::Array<DriverCmd>>;
pub type DriverCmdReceiver = AsyncRx<mpsc::Array<DriverCmd>>;
pub type InboundSender = MTx<mpsc::Array<InboundDhtStream>>;
pub type InboundReceiver = AsyncRx<mpsc::Array<InboundDhtStream>>;

type IoSender = MAsyncTx<mpsc::Array<DhtIo>>;
type IoReceiver = AsyncRx<mpsc::Array<DhtIo>>;

pub enum DriverCmd {
    Put {
        key: DhtKeyId,
        realm_id: RealmId,
        value: Vec<u8>,
        ttl: Duration,
        trace_context: Option<DistributedTraceContext>,
        reply: oneshot::Sender<CallerOutcome>,
    },
    Get {
        key: DhtKeyId,
        realm_filter: Option<RealmId>,
        trace_context: Option<DistributedTraceContext>,
        reply: oneshot::Sender<CallerOutcome>,
    },
    Bootstrap {
        nodes: Vec<NodeId>,
        trace_context: Option<DistributedTraceContext>,
        reply: oneshot::Sender<CallerOutcome>,
    },
    RoutingTableSize {
        trace_context: Option<DistributedTraceContext>,
        reply: oneshot::Sender<CallerOutcome>,
    },
    AddPeer {
        node_id: NodeId,
    },
}

impl std::fmt::Debug for DriverCmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Put {
                key,
                realm_id,
                value,
                ttl,
                ..
            } => f
                .debug_struct("DriverCmd::Put")
                .field("key", key)
                .field("realm_id", realm_id)
                .field("value_len", &value.len())
                .field("ttl", ttl)
                .finish(),
            Self::Get {
                key, realm_filter, ..
            } => f
                .debug_struct("DriverCmd::Get")
                .field("key", key)
                .field("realm_filter", realm_filter)
                .finish(),
            Self::Bootstrap { nodes, .. } => f
                .debug_struct("DriverCmd::Bootstrap")
                .field("nodes", &nodes.len())
                .finish(),
            Self::RoutingTableSize { .. } => f.debug_struct("DriverCmd::RoutingTableSize").finish(),
            Self::AddPeer { node_id } => f
                .debug_struct("DriverCmd::AddPeer")
                .field("node_id", node_id)
                .finish(),
        }
    }
}

pub struct DhtDriver {
    state: DhtStateMachine,
    endpoint: Endpoint,
    storage: StorageHandle,
    cmd_rx: DriverCmdReceiver,
    inbound_rx: InboundReceiver,
    io_tx: IoSender,
    io_rx: IoReceiver,
    shutdown: CancellationToken,
    now_tick: u64,
    pending_callers: HashMap<OpId, oneshot::Sender<CallerOutcome>>,
    op_spans: HashMap<OpId, Span>,
    next_op_id: OpId,
    inbound_contexts: HashMap<InboundId, (Connection, SendStream)>,
    inbound_spans: HashMap<InboundId, Span>,
    next_inbound_id: InboundId,
}

impl std::fmt::Debug for DhtDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtDriver")
            .field("now_tick", &self.now_tick)
            .field("pending_callers", &self.pending_callers.len())
            .field("inbound_contexts", &self.inbound_contexts.len())
            .finish()
    }
}

impl DhtDriver {
    pub fn new(
        state: DhtStateMachine,
        endpoint: Endpoint,
        storage: StorageHandle,
        cmd_rx: DriverCmdReceiver,
        inbound_rx: InboundReceiver,
        shutdown: CancellationToken,
    ) -> Self {
        let (io_tx, io_rx) = mpsc::bounded_async(DRIVER_IO_EVENT_CAPACITY);

        Self {
            state,
            endpoint,
            storage,
            cmd_rx,
            inbound_rx,
            io_tx,
            io_rx,
            shutdown,
            now_tick: 0,
            pending_callers: HashMap::new(),
            op_spans: HashMap::new(),
            next_op_id: 1,
            inbound_contexts: HashMap::new(),
            inbound_spans: HashMap::new(),
            next_inbound_id: 1,
        }
    }

    #[tracing::instrument(name = "dht.driver.run", level = "debug", skip(self))]
    pub async fn run(mut self) {
        let mut ticker = tokio::time::interval(DRIVER_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let _ = ticker.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    break;
                }
                maybe_cmd = self.cmd_rx.recv() => {
                    let Ok(cmd) = maybe_cmd else {
                        break;
                    };
                    self.handle_driver_cmd(cmd);
                }
                maybe_inbound = self.inbound_rx.recv() => {
                    let Ok(inbound) = maybe_inbound else {
                        break;
                    };
                    self.handle_inbound_stream(inbound);
                }
                maybe_io = self.io_rx.recv() => {
                    let Ok(io) = maybe_io else {
                        break;
                    };
                    self.handle_worker_io(io);
                }
                _ = ticker.tick() => {
                    self.now_tick = self.now_tick.saturating_add(1);
                    self.process_input(DhtInput::Tick { now_tick: self.now_tick });
                }
            }
        }

        self.fail_pending_callers(DhtIoError::Shutdown);
        self.inbound_contexts.clear();
    }

    #[tracing::instrument(
        name = "dht.driver.process_input",
        level = "debug",
        skip(self),
        fields(input = dht_input_kind(&input))
    )]
    fn process_input(&mut self, input: DhtInput) {
        let effects = self.state.step(input);
        for effect in effects {
            self.handle_effect(effect);
        }
        self.cleanup_finished_spans();
    }

    #[tracing::instrument(
        name = "dht.driver.process_input_for_op",
        level = "debug",
        skip(self),
        fields(op_id, input = dht_input_kind(&input))
    )]
    fn process_input_for_op(&mut self, op_id: OpId, input: DhtInput) {
        if let Some(span) = self.op_spans.get(&op_id).cloned() {
            let _guard = span.enter();
            self.process_input(input);
        } else {
            self.process_input(input);
        }
    }

    #[tracing::instrument(
        name = "dht.driver.process_input_for_io",
        level = "debug",
        skip(self, io),
        fields(io = dht_io_kind(&io), op_id = ?dht_io_op_id(&io), inbound_id = ?dht_io_inbound_id(&io))
    )]
    fn process_input_for_io(&mut self, io: DhtIo) {
        if let Some(span) = self.span_for_io(&io) {
            let _guard = span.enter();
            self.process_input(DhtInput::Io(io));
        } else {
            self.process_input(DhtInput::Io(io));
        }
    }

    #[tracing::instrument(
        name = "dht.driver.command",
        level = "debug",
        skip(self),
        fields(command = ?cmd)
    )]
    fn handle_driver_cmd(&mut self, cmd: DriverCmd) {
        match cmd {
            DriverCmd::Put {
                key,
                realm_id,
                value,
                ttl,
                trace_context,
                reply,
            } => {
                let op_id = self.register_caller(reply);
                let span = info_span!(
                    "dht.operation",
                    "otel.kind" = "internal",
                    "otel.status_code" = field::Empty,
                    "otel.status_description" = field::Empty,
                    op_id,
                    operation = "put",
                    key = %key,
                    realm_id = %realm_id,
                    value_len = value.len(),
                    ttl_secs = ttl.as_secs(),
                );
                if let Some(trace_context) = trace_context.as_ref() {
                    let _ = span.set_parent(extract_trace_context(trace_context));
                }
                self.op_spans.insert(op_id, span);
                self.process_input_for_op(
                    op_id,
                    DhtInput::Cmd(DhtCmd::Put {
                        op_id,
                        key,
                        realm_id,
                        value,
                        ttl,
                        trace_context,
                    }),
                );
            }
            DriverCmd::Get {
                key,
                realm_filter,
                trace_context,
                reply,
            } => {
                let op_id = self.register_caller(reply);
                let span = info_span!(
                    "dht.operation",
                    "otel.kind" = "internal",
                    "otel.status_code" = field::Empty,
                    "otel.status_description" = field::Empty,
                    op_id,
                    operation = "get",
                    key = %key,
                    realm_id = ?realm_filter,
                );
                if let Some(trace_context) = trace_context.as_ref() {
                    let _ = span.set_parent(extract_trace_context(trace_context));
                }
                self.op_spans.insert(op_id, span);
                self.process_input_for_op(
                    op_id,
                    DhtInput::Cmd(DhtCmd::Get {
                        op_id,
                        key,
                        realm_filter,
                        trace_context,
                    }),
                );
            }
            DriverCmd::Bootstrap {
                nodes,
                trace_context,
                reply,
            } => {
                let op_id = self.register_caller(reply);
                let span = info_span!(
                    "dht.operation",
                    "otel.kind" = "internal",
                    "otel.status_code" = field::Empty,
                    "otel.status_description" = field::Empty,
                    op_id,
                    operation = "bootstrap",
                    node_count = nodes.len(),
                );
                if let Some(trace_context) = trace_context.as_ref() {
                    let _ = span.set_parent(extract_trace_context(trace_context));
                }
                self.op_spans.insert(op_id, span);
                self.process_input_for_op(
                    op_id,
                    DhtInput::Cmd(DhtCmd::Bootstrap {
                        op_id,
                        nodes,
                        trace_context,
                    }),
                );
            }
            DriverCmd::RoutingTableSize {
                trace_context,
                reply,
            } => {
                let op_id = self.register_caller(reply);
                let span = info_span!(
                    "dht.operation",
                    "otel.kind" = "internal",
                    "otel.status_code" = field::Empty,
                    "otel.status_description" = field::Empty,
                    op_id,
                    operation = "routing_table_size",
                );
                if let Some(trace_context) = trace_context.as_ref() {
                    let _ = span.set_parent(extract_trace_context(trace_context));
                }
                self.op_spans.insert(op_id, span);
                self.process_input_for_op(op_id, DhtInput::Cmd(DhtCmd::RoutingTableSize { op_id }));
            }
            DriverCmd::AddPeer { node_id } => {
                self.process_input(DhtInput::Cmd(DhtCmd::AddPeer { node_id }));
            }
        }
    }

    #[tracing::instrument(
        name = "dht.driver.register_caller",
        level = "trace",
        skip(self, reply)
    )]
    fn register_caller(&mut self, reply: oneshot::Sender<CallerOutcome>) -> OpId {
        let op_id = self.next_op_id;
        self.next_op_id = self.next_op_id.saturating_add(1);
        self.pending_callers.insert(op_id, reply);
        op_id
    }

    #[tracing::instrument(
        name = "dht.driver.effect",
        level = "debug",
        skip(self, effect),
        fields(effect = dht_effect_kind(&effect))
    )]
    fn handle_effect(&mut self, effect: DhtEffect) {
        match effect {
            DhtEffect::IoRequest(request) => {
                let span = self.span_for_io_request(&request);
                if let Some(span) = span {
                    let _guard = span.enter();
                    self.dispatch_io_request(*request);
                } else {
                    self.dispatch_io_request(*request);
                }
            }
            DhtEffect::Output(output) => {
                let span = self.op_spans.get(&output_op_id(&output)).cloned();
                if let Some(span) = span {
                    let _guard = span.enter();
                    self.handle_output(output);
                } else {
                    self.handle_output(output);
                }
            }
        }
    }

    #[tracing::instrument(
        name = "dht.driver.output",
        level = "debug",
        skip(self, output),
        fields(op_id = output_op_id(&output), output = dht_output_kind(&output))
    )]
    fn handle_output(&mut self, output: DhtOutput) {
        match output {
            DhtOutput::Completed { op_id, result } => {
                if let Some(span) = self.op_spans.get(&op_id) {
                    span.record("otel.status_code", "OK");
                }
                if let Some(reply) = self.pending_callers.remove(&op_id) {
                    let _ = reply.send(Ok(result));
                }
            }
            DhtOutput::Failed { op_id, error } => {
                if let Some(span) = self.op_spans.get(&op_id) {
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", field::display(error.to_string()));
                }
                if let Some(reply) = self.pending_callers.remove(&op_id) {
                    let _ = reply.send(Err(error));
                }
            }
        }
    }

    #[tracing::instrument(name = "dht.driver.fail_pending", level = "debug", skip(self))]
    fn fail_pending_callers(&mut self, error: DhtIoError) {
        let waiting: Vec<_> = self.pending_callers.drain().collect();
        for (_, sender) in waiting {
            let _ = sender.send(Err(error.clone()));
        }
        self.op_spans.clear();
    }

    #[tracing::instrument(name = "dht.driver.cleanup_spans", level = "trace", skip(self))]
    fn cleanup_finished_spans(&mut self) {
        let finished: Vec<_> = self
            .op_spans
            .keys()
            .copied()
            .filter(|op_id| {
                *op_id != CLEANUP_OP_ID
                    && !self.state.contains_op(*op_id)
                    && !self.pending_callers.contains_key(op_id)
            })
            .collect();
        for op_id in finished {
            self.op_spans.remove(&op_id);
        }
    }

    fn span_for_io(&self, io: &DhtIo) -> Option<Span> {
        match io {
            DhtIo::RpcResponse { op_id, .. }
            | DhtIo::RpcError { op_id, .. }
            | DhtIo::StorageReadResult { op_id, .. }
            | DhtIo::StorageWriteResult { op_id, .. }
            | DhtIo::StorageDeleteResult { op_id, .. }
            | DhtIo::StorageIterResult { op_id, .. }
            | DhtIo::StorageError { op_id, .. } => self.op_spans.get(op_id).cloned(),
            DhtIo::InboundRequest { inbound_id, .. }
            | DhtIo::InboundReadError { inbound_id, .. }
            | DhtIo::InboundDropped { inbound_id } => self.inbound_spans.get(inbound_id).cloned(),
            DhtIo::PeerSeen { .. } => None,
        }
    }

    fn span_for_io_request(&mut self, request: &DhtIoRequest) -> Option<Span> {
        match request {
            DhtIoRequest::RpcRequest { op_id, .. }
            | DhtIoRequest::StorageRead { op_id, .. }
            | DhtIoRequest::StorageWrite { op_id, .. }
            | DhtIoRequest::StorageDelete { op_id, .. }
            | DhtIoRequest::StorageIter { op_id, .. } => {
                if *op_id == CLEANUP_OP_ID {
                    return None;
                }
                if !self.op_spans.contains_key(op_id) {
                    let span = debug_span!(
                        "dht.operation",
                        "otel.kind" = "internal",
                        "otel.status_code" = field::Empty,
                        "otel.status_description" = field::Empty,
                        op_id = *op_id,
                        operation = internal_operation_kind(request),
                    );
                    self.op_spans.insert(*op_id, span);
                }
                self.op_spans.get(op_id).cloned()
            }
            DhtIoRequest::RpcResponse { inbound_id, .. }
            | DhtIoRequest::DropInbound { inbound_id } => {
                self.inbound_spans.get(inbound_id).cloned()
            }
        }
    }

    #[tracing::instrument(
        name = "dht.driver.inbound_stream",
        level = "debug",
        skip(self, inbound),
        fields(peer = %inbound.3)
    )]
    fn handle_inbound_stream(&mut self, inbound: InboundDhtStream) {
        let (conn, send, mut recv, peer) = inbound;
        let inbound_id = self.next_inbound_id;
        self.next_inbound_id = self.next_inbound_id.saturating_add(1);

        self.inbound_contexts.insert(inbound_id, (conn, send));
        self.process_input_for_io(DhtIo::PeerSeen { peer });

        let io_tx = self.io_tx.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            match tokio::time::timeout(RPC_TIMEOUT, read_request_from_stream(&mut recv)).await {
                Ok(Ok((trace_context, request))) => {
                    let _ = io_tx
                        .send(DhtIo::InboundRequest {
                            inbound_id,
                            peer,
                            request,
                            trace_context,
                        })
                        .await;
                }
                Ok(Err(error)) => {
                    warn!(
                        event = "dht.rpc.inbound_read_failed",
                        inbound_id,
                        peer = %peer,
                        duration_ms = duration_ms(started.elapsed()),
                        error = %error,
                        "Failed to read inbound DHT RPC request"
                    );
                    let _ = io_tx
                        .send(DhtIo::InboundReadError { inbound_id, error })
                        .await;
                }
                Err(error) => {
                    warn!(
                        event = "dht.rpc.inbound_read_timeout",
                        inbound_id,
                        peer = %peer,
                        duration_ms = duration_ms(started.elapsed()),
                        timeout_ms = duration_ms(RPC_TIMEOUT),
                        error = %error,
                        "Timed out reading inbound DHT RPC request"
                    );
                    let _ = io_tx
                        .send(DhtIo::InboundReadError {
                            inbound_id,
                            error: error.into(),
                        })
                        .await;
                }
            }
        });
    }

    #[tracing::instrument(
        name = "dht.driver.worker_io",
        level = "debug",
        skip(self, io),
        fields(io = dht_io_kind(&io), op_id = ?dht_io_op_id(&io), inbound_id = ?dht_io_inbound_id(&io))
    )]
    fn handle_worker_io(&mut self, io: DhtIo) {
        if let DhtIo::InboundRequest {
            inbound_id,
            peer,
            request,
            trace_context,
        } = io
        {
            let span = info_span!(
                "dht.rpc.receive",
                "otel.kind" = "server",
                peer = %peer,
                request = ?request,
            );
            if let Some(trace_context) = trace_context.as_ref() {
                let _ = span.set_parent(extract_trace_context(trace_context));
            }
            self.inbound_spans.insert(inbound_id, span.clone());
            {
                let _guard = span.enter();
                trace!(
                    event = "dht.rpc.received",
                    peer = %peer,
                    request = ?request,
                    "Received inbound DHT RPC"
                );
            }
            self.process_input_for_io(DhtIo::InboundRequest {
                inbound_id,
                peer,
                request,
                trace_context,
            });
            return;
        }

        if let DhtIo::InboundReadError { inbound_id, error } = io {
            let maybe_send = self.inbound_contexts.remove(&inbound_id);
            let io_tx = self.io_tx.clone();
            let span = self.inbound_spans.get(&inbound_id).cloned();
            let future = async move {
                if let Some((_conn, mut send)) = maybe_send {
                    let _ = write_response_to_stream(
                        &mut send,
                        &DhtResponse::Error {
                            code: ErrorCode::InvalidRequest,
                            message: error.to_string(),
                        },
                    )
                    .await;
                }

                let _ = io_tx.send(DhtIo::InboundDropped { inbound_id }).await;
            };
            if let Some(span) = span {
                tokio::spawn(future.instrument(span));
            } else {
                tokio::spawn(future);
            }
            return;
        }

        if let DhtIo::InboundDropped { inbound_id } = io {
            self.inbound_contexts.remove(&inbound_id);
            self.process_input_for_io(DhtIo::InboundDropped { inbound_id });
            self.inbound_spans.remove(&inbound_id);
            return;
        }

        self.process_input_for_io(io);
    }

    #[tracing::instrument(
        name = "dht.driver.io_request",
        level = "debug",
        skip(self, request),
        fields(request = dht_io_request_kind(&request), op_id = ?dht_io_request_op_id(&request), inbound_id = ?dht_io_request_inbound_id(&request))
    )]
    fn dispatch_io_request(&mut self, request: DhtIoRequest) {
        match request {
            DhtIoRequest::RpcRequest {
                op_id,
                phase,
                peer,
                request,
                trace_context,
            } => self.dispatch_rpc_request(op_id, phase, peer, request, trace_context),
            DhtIoRequest::RpcResponse {
                inbound_id,
                response,
            } => self.dispatch_rpc_response(inbound_id, response),
            DhtIoRequest::DropInbound { inbound_id } => self.dispatch_drop_inbound(inbound_id),
            DhtIoRequest::StorageRead {
                op_id,
                stage,
                key,
                realm_filter,
            } => self.dispatch_storage_read(op_id, stage, key, realm_filter),
            DhtIoRequest::StorageWrite {
                op_id,
                stage,
                key,
                entries,
            } => self.dispatch_storage_write(op_id, stage, key, entries),
            DhtIoRequest::StorageDelete { op_id, stage, key } => {
                self.dispatch_storage_delete(op_id, stage, key)
            }
            DhtIoRequest::StorageIter {
                op_id,
                stage,
                start_after,
                limit,
            } => self.dispatch_storage_iter(op_id, stage, start_after, limit),
        }
    }

    #[tracing::instrument(
        name = "dht.driver.rpc_request.dispatch",
        level = "debug",
        skip(self, request, trace_context),
        fields(op_id, phase = ?phase, peer = %peer, request = ?request)
    )]
    fn dispatch_rpc_request(
        &self,
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        request: DhtRequest,
        trace_context: Option<DistributedTraceContext>,
    ) {
        let endpoint = self.endpoint.clone();
        let io_tx = self.io_tx.clone();
        trace!(
            event = "dht.rpc.dispatch",
            op_id,
            phase = ?phase,
            peer = %peer,
            request = ?request,
            "Dispatching outbound DHT RPC"
        );
        let current_parent = Span::current();
        let span = info_span!(
            "dht.rpc.request",
            "otel.kind" = "client",
            "otel.status_code" = field::Empty,
            "otel.status_description" = field::Empty,
            "network.transport" = "quic",
            "rpc.system" = "aruna-dht",
            "iroh.alpn" = "aruna/dht/1",
            "iroh.connect_ms" = field::Empty,
            "iroh.open_bi_ms" = field::Empty,
            "iroh.write_request_ms" = field::Empty,
            "iroh.wait_response_header_ms" = field::Empty,
            "iroh.read_response_body_ms" = field::Empty,
            "iroh.total_ms" = field::Empty,
            "iroh.request_bytes" = field::Empty,
            "iroh.response_bytes" = field::Empty,
            "iroh.selected_address" = field::Empty,
            "iroh.rtt_ms" = field::Empty,
            op_id,
            phase = ?phase,
            peer = %peer,
            request = ?request,
        );
        if current_parent.is_disabled()
            && let Some(trace_context) = trace_context.as_ref()
        {
            let _ = span.set_parent(extract_trace_context(trace_context));
        }
        tokio::spawn(
            async move {
                let outbound_trace_context = current_trace_context().or(trace_context);
                match rpc_request(
                    endpoint.clone(),
                    op_id,
                    phase,
                    peer,
                    request,
                    outbound_trace_context,
                )
                .await
                {
                    Ok(response) => {
                        Span::current().record("otel.status_code", "OK");
                        let _ = io_tx
                            .send(DhtIo::RpcResponse {
                                op_id,
                                phase,
                                peer,
                                response,
                            })
                            .await;
                    }
                    Err(error) => {
                        let span = Span::current();
                        span.record("otel.status_code", "ERROR");
                        span.record("otel.status_description", field::display(error.to_string()));
                        let remote_info = endpoint.remote_info(peer).await.map(|info| {
                            info.addrs()
                                .map(|addr| format!("{:?} ({:?})", addr.addr(), addr.usage()))
                                .collect::<Vec<_>>()
                        });
                        warn!(
                            op_id,
                            phase = ?phase,
                            peer = %peer,
                            error = %error,
                            remote_info = ?remote_info,
                            "Outbound DHT RPC failed"
                        );
                        let _ = io_tx
                            .send(DhtIo::RpcError {
                                op_id,
                                phase,
                                peer,
                                error,
                            })
                            .await;
                    }
                }
            }
            .instrument(span),
        );
    }

    #[tracing::instrument(
        name = "dht.driver.rpc_response.dispatch",
        level = "debug",
        skip(self, response),
        fields(inbound_id, response = ?response)
    )]
    fn dispatch_rpc_response(&mut self, inbound_id: InboundId, response: DhtResponse) {
        let maybe_send = self.inbound_contexts.remove(&inbound_id);
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                if let Some((_conn, mut send)) = maybe_send {
                    let _ = write_response_to_stream(&mut send, &response).await;
                }
                let _ = io_tx.send(DhtIo::InboundDropped { inbound_id }).await;
            }
            .instrument(Span::current()),
        );
    }

    #[tracing::instrument(
        name = "dht.driver.inbound_drop.dispatch",
        level = "debug",
        skip(self),
        fields(inbound_id)
    )]
    fn dispatch_drop_inbound(&mut self, inbound_id: InboundId) {
        self.inbound_contexts.remove(&inbound_id);
        self.process_input_for_io(DhtIo::InboundDropped { inbound_id });
        self.inbound_spans.remove(&inbound_id);
    }

    #[tracing::instrument(
        name = "dht.driver.storage_read.dispatch",
        level = "debug",
        skip(self),
        fields(op_id, stage = ?stage, key = %key, realm_id = ?realm_filter)
    )]
    fn dispatch_storage_read(
        &self,
        op_id: OpId,
        stage: StorageStage,
        key: DhtKeyId,
        realm_filter: Option<RealmId>,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                let effect = Effect::Storage(StorageEffect::Read {
                    key_space: DHT_KEYSPACE.to_string(),
                    key: ByteView::from(key.as_bytes().as_slice()),
                    txn_id: None,
                });

                match send_storage_effect_with_timeout(&storage, effect, op_id, stage, "read").await
                {
                    Ok(Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                        let mut entries =
                            value.map(|data| decode_entries(&data)).unwrap_or_default();
                        if let Some(realm_filter) = realm_filter {
                            entries.retain(|entry| entry.realm_id == realm_filter);
                        }

                        let _ = io_tx
                            .send(DhtIo::StorageReadResult {
                                op_id,
                                stage,
                                entries,
                            })
                            .await;
                    }
                    Ok(Event::Storage(StorageEvent::Error { error })) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error: DhtIoError::storage(error),
                            })
                            .await;
                    }
                    Ok(other) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error: DhtIoError::storage(format!(
                                    "unexpected storage read event: {other:?}"
                                )),
                            })
                            .await;
                    }
                    Err(error) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error,
                            })
                            .await;
                    }
                }
            }
            .instrument(Span::current()),
        );
    }

    #[tracing::instrument(
        name = "dht.driver.storage_write.dispatch",
        level = "debug",
        skip(self, entries),
        fields(op_id, stage = ?stage, key = %key, entry_count = entries.len())
    )]
    fn dispatch_storage_write(
        &self,
        op_id: OpId,
        stage: StorageStage,
        key: DhtKeyId,
        entries: Vec<StoredEntry>,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                let Some(bytes) = encode_entries(&entries) else {
                    let _ = io_tx
                        .send(DhtIo::StorageError {
                            op_id,
                            stage,
                            error: DhtIoError::storage("serialize dht entries failed"),
                        })
                        .await;
                    return;
                };

                let effect = Effect::Storage(StorageEffect::Write {
                    key_space: DHT_KEYSPACE.to_string(),
                    key: ByteView::from(key.as_bytes().as_slice()),
                    value: ByteView::from(bytes),
                    txn_id: None,
                });

                match send_storage_effect_with_timeout(&storage, effect, op_id, stage, "write")
                    .await
                {
                    Ok(Event::Storage(StorageEvent::WriteResult { .. })) => {
                        let _ = io_tx.send(DhtIo::StorageWriteResult { op_id, stage }).await;
                    }
                    Ok(Event::Storage(StorageEvent::Error { error })) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error: DhtIoError::storage(error),
                            })
                            .await;
                    }
                    Ok(other) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error: DhtIoError::storage(format!(
                                    "unexpected storage write event: {other:?}"
                                )),
                            })
                            .await;
                    }
                    Err(error) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error,
                            })
                            .await;
                    }
                }
            }
            .instrument(Span::current()),
        );
    }

    #[tracing::instrument(
        name = "dht.driver.storage_delete.dispatch",
        level = "debug",
        skip(self),
        fields(op_id, stage = ?stage, key = %key)
    )]
    fn dispatch_storage_delete(&self, op_id: OpId, stage: StorageStage, key: DhtKeyId) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                let effect = Effect::Storage(StorageEffect::Delete {
                    key_space: DHT_KEYSPACE.to_string(),
                    key: ByteView::from(key.as_bytes().as_slice()),
                    txn_id: None,
                });

                match send_storage_effect_with_timeout(&storage, effect, op_id, stage, "delete")
                    .await
                {
                    Ok(Event::Storage(StorageEvent::DeleteResult { .. })) => {
                        let _ = io_tx
                            .send(DhtIo::StorageDeleteResult { op_id, stage })
                            .await;
                    }
                    Ok(Event::Storage(StorageEvent::Error { error })) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error: DhtIoError::storage(error),
                            })
                            .await;
                    }
                    Ok(other) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error: DhtIoError::storage(format!(
                                    "unexpected storage delete event: {other:?}"
                                )),
                            })
                            .await;
                    }
                    Err(error) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error,
                            })
                            .await;
                    }
                }
            }
            .instrument(Span::current()),
        );
    }

    #[tracing::instrument(
        name = "dht.driver.storage_iter.dispatch",
        level = "debug",
        skip(self, start_after),
        fields(op_id, stage = ?stage, limit, has_cursor = start_after.is_some())
    )]
    fn dispatch_storage_iter(
        &self,
        op_id: OpId,
        stage: StorageStage,
        start_after: Option<Vec<u8>>,
        limit: usize,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                let effect = Effect::Storage(StorageEffect::Iter {
                    key_space: DHT_KEYSPACE.to_string(),
                    prefix: None,
                    start_after: start_after.map(ByteView::from),
                    limit: if limit == 0 { CLEANUP_PAGE_SIZE } else { limit },
                    txn_id: None,
                });

                match send_storage_effect_with_timeout(&storage, effect, op_id, stage, "iter").await
                {
                    Ok(Event::Storage(StorageEvent::IterResult {
                        values,
                        next_start_after,
                    })) => {
                        let decoded_values = values
                            .into_iter()
                            .map(|(key, value)| (key.as_ref().to_vec(), decode_entries(&value)))
                            .collect();

                        let _ = io_tx
                            .send(DhtIo::StorageIterResult {
                                op_id,
                                stage,
                                values: decoded_values,
                                next_start_after: next_start_after.map(|k| k.as_ref().to_vec()),
                            })
                            .await;
                    }
                    Ok(Event::Storage(StorageEvent::Error { error })) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error: DhtIoError::storage(error),
                            })
                            .await;
                    }
                    Ok(other) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error: DhtIoError::storage(format!(
                                    "unexpected storage iter event: {other:?}"
                                )),
                            })
                            .await;
                    }
                    Err(error) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error,
                            })
                            .await;
                    }
                }
            }
            .instrument(Span::current()),
        );
    }
}

async fn send_storage_effect_with_timeout(
    storage: &StorageHandle,
    effect: Effect,
    op_id: OpId,
    stage: StorageStage,
    operation: &'static str,
) -> Result<Event, DhtIoError> {
    let started = Instant::now();
    match tokio::time::timeout(STORAGE_TIMEOUT, storage.send_effect(effect)).await {
        Ok(event) => Ok(event),
        Err(error) => {
            warn!(
                event = "dht.storage.timeout",
                op_id,
                stage = ?stage,
                operation,
                duration_ms = duration_ms(started.elapsed()),
                timeout_ms = duration_ms(STORAGE_TIMEOUT),
                error = %error,
                "DHT storage operation timed out"
            );
            Err(DhtIoError::Timeout)
        }
    }
}

fn output_op_id(output: &DhtOutput) -> OpId {
    match output {
        DhtOutput::Completed { op_id, .. } | DhtOutput::Failed { op_id, .. } => *op_id,
    }
}

fn dht_input_kind(input: &DhtInput) -> &'static str {
    match input {
        DhtInput::Cmd(cmd) => dht_cmd_kind(cmd),
        DhtInput::Io(io) => dht_io_kind(io),
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

fn dht_effect_kind(effect: &DhtEffect) -> &'static str {
    match effect {
        DhtEffect::IoRequest(request) => dht_io_request_kind(request),
        DhtEffect::Output(output) => dht_output_kind(output),
    }
}

fn dht_output_kind(output: &DhtOutput) -> &'static str {
    match output {
        DhtOutput::Completed { .. } => "completed",
        DhtOutput::Failed { .. } => "failed",
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
        | DhtIo::StorageWriteResult { .. }
        | DhtIo::StorageDeleteResult { .. }
        | DhtIo::StorageIterResult { .. }
        | DhtIo::StorageError { .. }
        | DhtIo::PeerSeen { .. } => None,
    }
}

fn dht_io_request_kind(request: &DhtIoRequest) -> &'static str {
    match request {
        DhtIoRequest::RpcRequest { .. } => "rpc_request",
        DhtIoRequest::RpcResponse { .. } => "rpc_response",
        DhtIoRequest::DropInbound { .. } => "drop_inbound",
        DhtIoRequest::StorageRead { .. } => "storage_read",
        DhtIoRequest::StorageWrite { .. } => "storage_write",
        DhtIoRequest::StorageDelete { .. } => "storage_delete",
        DhtIoRequest::StorageIter { .. } => "storage_iter",
    }
}

fn dht_io_request_op_id(request: &DhtIoRequest) -> Option<OpId> {
    match request {
        DhtIoRequest::RpcRequest { op_id, .. }
        | DhtIoRequest::StorageRead { op_id, .. }
        | DhtIoRequest::StorageWrite { op_id, .. }
        | DhtIoRequest::StorageDelete { op_id, .. }
        | DhtIoRequest::StorageIter { op_id, .. } => Some(*op_id),
        DhtIoRequest::RpcResponse { .. } | DhtIoRequest::DropInbound { .. } => None,
    }
}

fn dht_io_request_inbound_id(request: &DhtIoRequest) -> Option<InboundId> {
    match request {
        DhtIoRequest::RpcResponse { inbound_id, .. } | DhtIoRequest::DropInbound { inbound_id } => {
            Some(*inbound_id)
        }
        DhtIoRequest::RpcRequest { .. }
        | DhtIoRequest::StorageRead { .. }
        | DhtIoRequest::StorageWrite { .. }
        | DhtIoRequest::StorageDelete { .. }
        | DhtIoRequest::StorageIter { .. } => None,
    }
}

fn internal_operation_kind(request: &DhtIoRequest) -> &'static str {
    match request {
        DhtIoRequest::RpcRequest { phase, .. } => match phase {
            RpcPhase::PutLookup | RpcPhase::PutStore => "put",
            RpcPhase::GetLookup => "get",
            RpcPhase::Bootstrap => "bootstrap",
            RpcPhase::EvictionPing => "eviction_ping",
            RpcPhase::MaintenancePing => "maintenance_ping",
        },
        DhtIoRequest::StorageRead { stage, .. }
        | DhtIoRequest::StorageWrite { stage, .. }
        | DhtIoRequest::StorageDelete { stage, .. }
        | DhtIoRequest::StorageIter { stage, .. } => match stage {
            StorageStage::PutLocalRead | StorageStage::PutLocalWrite => "put",
            StorageStage::GetLocalRead => "get",
            StorageStage::InboundGetRead => "inbound_get",
            StorageStage::InboundPutRead | StorageStage::InboundPutWrite => "inbound_put",
            StorageStage::CleanupIter
            | StorageStage::CleanupWrite
            | StorageStage::CleanupDelete => "cleanup",
        },
        DhtIoRequest::RpcResponse { .. } | DhtIoRequest::DropInbound { .. } => "inbound",
    }
}

#[tracing::instrument(
    name = "dht.rpc.request.io",
    level = "debug",
    skip(endpoint, request, trace_context),
    fields(op_id, phase = ?phase, peer = %peer, request = ?request)
)]
async fn rpc_request(
    endpoint: Endpoint,
    op_id: OpId,
    phase: RpcPhase,
    peer: NodeId,
    request: DhtRequest,
    trace_context: Option<DistributedTraceContext>,
) -> Result<DhtResponse, DhtIoError> {
    let span = Span::current();
    let total_started = Instant::now();

    let connect_started = Instant::now();
    let conn = match tokio::time::timeout(RPC_TIMEOUT, endpoint.connect(peer, DHT_ALPN)).await {
        Ok(Ok(conn)) => {
            let elapsed = connect_started.elapsed();
            record_duration_ms(&span, "iroh.connect_ms", elapsed);
            warn_if_slow_iroh_phase("dht.rpc", "connect", elapsed);
            trace!(
                event = "dht.rpc.iroh_phase",
                op_id,
                phase = ?phase,
                peer = %peer,
                iroh_phase = "connect",
                duration_ms = duration_ms(elapsed),
                "Completed Iroh DHT RPC phase"
            );
            conn
        }
        Ok(Err(error)) => {
            let elapsed = connect_started.elapsed();
            record_duration_ms(&span, "iroh.connect_ms", elapsed);
            warn!(
                event = "dht.rpc.iroh_connect_failed",
                op_id,
                phase = ?phase,
                peer = %peer,
                duration_ms = duration_ms(elapsed),
                error = %error,
                "Iroh DHT RPC connect failed"
            );
            return Err(DhtIoError::network(error));
        }
        Err(error) => {
            let elapsed = connect_started.elapsed();
            record_duration_ms(&span, "iroh.connect_ms", elapsed);
            warn!(
                event = "dht.rpc.iroh_connect_failed",
                op_id,
                phase = ?phase,
                peer = %peer,
                duration_ms = duration_ms(elapsed),
                error = %error,
                timed_out = true,
                "Iroh DHT RPC connect timed out"
            );
            return Err(error.into());
        }
    };

    record_selected_path(&span, &conn);

    let open_started = Instant::now();
    let (mut send, mut recv) = match tokio::time::timeout(RPC_TIMEOUT, conn.open_bi()).await {
        Ok(Ok(streams)) => {
            let elapsed = open_started.elapsed();
            record_duration_ms(&span, "iroh.open_bi_ms", elapsed);
            warn_if_slow_iroh_phase("dht.rpc", "open_bi", elapsed);
            trace!(
                event = "dht.rpc.iroh_phase",
                op_id,
                phase = ?phase,
                peer = %peer,
                iroh_phase = "open_bi",
                duration_ms = duration_ms(elapsed),
                "Completed Iroh DHT RPC phase"
            );
            streams
        }
        Ok(Err(error)) => {
            let elapsed = open_started.elapsed();
            record_duration_ms(&span, "iroh.open_bi_ms", elapsed);
            warn!(
                event = "dht.rpc.iroh_open_bi_failed",
                op_id,
                phase = ?phase,
                peer = %peer,
                duration_ms = duration_ms(elapsed),
                error = %error,
                "Iroh DHT RPC bidirectional stream open failed"
            );
            return Err(DhtIoError::network(error));
        }
        Err(error) => {
            let elapsed = open_started.elapsed();
            record_duration_ms(&span, "iroh.open_bi_ms", elapsed);
            warn!(
                event = "dht.rpc.iroh_open_bi_timeout",
                op_id,
                phase = ?phase,
                peer = %peer,
                duration_ms = duration_ms(elapsed),
                timeout_ms = duration_ms(RPC_TIMEOUT),
                error = %error,
                "Iroh DHT RPC bidirectional stream open timed out"
            );
            return Err(error.into());
        }
    };

    let request_bytes = encode_request_with_trace_context(&request, trace_context)?;
    span.record("iroh.request_bytes", request_bytes.len() as u64);
    let len = (request_bytes.len() as u32).to_be_bytes();

    let write_started = Instant::now();
    match tokio::time::timeout(RPC_TIMEOUT, async {
        send.write_all(&len).await.map_err(DhtIoError::network)?;
        send.write_all(&request_bytes)
            .await
            .map_err(DhtIoError::network)?;
        send.finish().map_err(DhtIoError::network)?;
        Ok::<(), DhtIoError>(())
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let elapsed = write_started.elapsed();
            record_duration_ms(&span, "iroh.write_request_ms", elapsed);
            warn!(
                event = "dht.rpc.write_request_failed",
                op_id,
                phase = ?phase,
                peer = %peer,
                duration_ms = duration_ms(elapsed),
                error = %error,
                "Failed to write Iroh DHT RPC request"
            );
            return Err(error);
        }
        Err(error) => {
            let elapsed = write_started.elapsed();
            record_duration_ms(&span, "iroh.write_request_ms", elapsed);
            warn!(
                event = "dht.rpc.write_request_timeout",
                op_id,
                phase = ?phase,
                peer = %peer,
                duration_ms = duration_ms(elapsed),
                timeout_ms = duration_ms(RPC_TIMEOUT),
                error = %error,
                "Timed out writing Iroh DHT RPC request"
            );
            return Err(error.into());
        }
    }
    let elapsed = write_started.elapsed();
    record_duration_ms(&span, "iroh.write_request_ms", elapsed);
    warn_if_slow_iroh_phase("dht.rpc", "write_request", elapsed);
    trace!(
        event = "dht.rpc.iroh_phase",
        op_id,
        phase = ?phase,
        peer = %peer,
        iroh_phase = "write_request",
        duration_ms = duration_ms(elapsed),
        request_bytes = request_bytes.len(),
        "Completed Iroh DHT RPC phase"
    );

    let mut len_buf = [0u8; 4];
    let wait_response_started = Instant::now();
    match tokio::time::timeout(RPC_TIMEOUT, recv.read_exact(&mut len_buf)).await {
        Ok(Ok(_)) => {
            let elapsed = wait_response_started.elapsed();
            record_duration_ms(&span, "iroh.wait_response_header_ms", elapsed);
            warn_if_slow_iroh_phase("dht.rpc", "wait_response_header", elapsed);
            trace!(
                event = "dht.rpc.iroh_phase",
                op_id,
                phase = ?phase,
                peer = %peer,
                iroh_phase = "wait_response_header",
                duration_ms = duration_ms(elapsed),
                "Completed Iroh DHT RPC phase"
            );
        }
        Ok(Err(error)) => {
            let elapsed = wait_response_started.elapsed();
            record_duration_ms(&span, "iroh.wait_response_header_ms", elapsed);
            return Err(DhtIoError::network(error));
        }
        Err(error) => {
            let elapsed = wait_response_started.elapsed();
            record_duration_ms(&span, "iroh.wait_response_header_ms", elapsed);
            warn!(
                event = "dht.rpc.iroh_response_timeout",
                op_id,
                phase = ?phase,
                peer = %peer,
                duration_ms = duration_ms(elapsed),
                error = %error,
                "Timed out waiting for Iroh DHT RPC response header"
            );
            return Err(error.into());
        }
    }

    let response_len = u32::from_be_bytes(len_buf) as usize;
    span.record("iroh.response_bytes", response_len as u64);
    if response_len > MAX_MESSAGE_SIZE {
        return Err(DhtIoError::invalid_response("response too large"));
    }

    let mut response_bytes = vec![0u8; response_len];
    let read_body_started = Instant::now();
    match tokio::time::timeout(RPC_TIMEOUT, recv.read_exact(&mut response_bytes)).await {
        Ok(Ok(_)) => {
            let elapsed = read_body_started.elapsed();
            record_duration_ms(&span, "iroh.read_response_body_ms", elapsed);
            warn_if_slow_iroh_phase("dht.rpc", "read_response_body", elapsed);
            trace!(
                event = "dht.rpc.iroh_phase",
                op_id,
                phase = ?phase,
                peer = %peer,
                iroh_phase = "read_response_body",
                duration_ms = duration_ms(elapsed),
                response_bytes = response_len,
                "Completed Iroh DHT RPC phase"
            );
        }
        Ok(Err(error)) => {
            let elapsed = read_body_started.elapsed();
            record_duration_ms(&span, "iroh.read_response_body_ms", elapsed);
            return Err(DhtIoError::network(error));
        }
        Err(error) => {
            let elapsed = read_body_started.elapsed();
            record_duration_ms(&span, "iroh.read_response_body_ms", elapsed);
            warn!(
                event = "dht.rpc.iroh_response_timeout",
                op_id,
                phase = ?phase,
                peer = %peer,
                duration_ms = duration_ms(elapsed),
                error = %error,
                "Timed out reading Iroh DHT RPC response body"
            );
            return Err(error.into());
        }
    }

    let total_elapsed = total_started.elapsed();
    record_duration_ms(&span, "iroh.total_ms", total_elapsed);
    warn_if_slow_iroh_request("dht.rpc", total_elapsed);
    trace!(
        event = "dht.rpc.iroh_completed",
        op_id,
        phase = ?phase,
        peer = %peer,
        duration_ms = duration_ms(total_elapsed),
        request_bytes = request_bytes.len(),
        response_bytes = response_len,
        "Completed Iroh DHT RPC"
    );

    Ok(decode_response(&response_bytes)?)
}

fn record_selected_path(span: &Span, conn: &Connection) {
    let paths = conn.paths();
    let Some(path) = paths.iter().find(|path| path.is_selected()) else {
        return;
    };

    span.record(
        "iroh.selected_address",
        field::display(format!("{:?}", path.remote_addr())),
    );
    span.record("iroh.rtt_ms", duration_ms(path.rtt()));
}

async fn read_request_from_stream(
    recv: &mut RecvStream,
) -> Result<(Option<DistributedTraceContext>, DhtRequest), DhtIoError> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(DhtIoError::network)?;

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err(DhtIoError::invalid_response("request too large"));
    }

    let mut req_bytes = vec![0u8; len];
    recv.read_exact(&mut req_bytes)
        .await
        .map_err(DhtIoError::network)?;

    match recv.read_chunk(1).await {
        Ok(None) => {}
        Ok(Some(_)) => {
            return Err(DhtIoError::invalid_response("request framing mismatch"));
        }
        Err(err) => return Err(DhtIoError::network(err)),
    }

    Ok(decode_request_with_trace_context(&req_bytes)?)
}

async fn write_response_to_stream(
    send: &mut SendStream,
    response: &DhtResponse,
) -> Result<(), DhtIoError> {
    let response_bytes = encode_response(response)?;

    if response_bytes.len() > MAX_MESSAGE_SIZE {
        return Err(DhtIoError::invalid_response("response too large"));
    }

    let len = (response_bytes.len() as u32).to_be_bytes();
    let started = Instant::now();
    match tokio::time::timeout(RPC_TIMEOUT, async {
        send.write_all(&len).await.map_err(DhtIoError::network)?;
        send.write_all(&response_bytes)
            .await
            .map_err(DhtIoError::network)?;
        send.finish().map_err(DhtIoError::network)?;
        Ok::<(), DhtIoError>(())
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            warn!(
                event = "dht.rpc.write_response_failed",
                duration_ms = duration_ms(started.elapsed()),
                error = %error,
                "Failed to write inbound DHT RPC response"
            );
            return Err(error);
        }
        Err(error) => {
            warn!(
                event = "dht.rpc.write_response_timeout",
                duration_ms = duration_ms(started.elapsed()),
                timeout_ms = duration_ms(RPC_TIMEOUT),
                error = %error,
                "Timed out writing inbound DHT RPC response"
            );
            return Err(error.into());
        }
    }

    let _ = tokio::time::timeout(Duration::from_millis(100), send.stopped()).await;

    Ok(())
}
