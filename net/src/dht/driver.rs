use std::collections::HashMap;
use std::time::{Duration, Instant};

use aruna_core::DistributedTraceContext;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::keyspaces::DHT_KEYSPACE;
use aruna_core::structs::RealmId;
use aruna_core::types::TxnId;
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
    DHT_KEY_COUNT_KEY, DHT_META_KEYSPACE, DHT_REVISION_KEY, DRIVER_IO_EVENT_CAPACITY,
    DRIVER_TICK_INTERVAL, MAX_MESSAGE_SIZE, MAX_STORED_KEYS, RPC_TIMEOUT, STORAGE_MUTATION_RETRIES,
    STORAGE_TIMEOUT,
};
use super::protocol::{
    CLEANUP_OP_ID, DhtCmd, DhtEffect, DhtInput, DhtIo, DhtIoError, DhtIoRequest, DhtOutput,
    DhtOutputValue, InboundId, OpId, RpcPhase, StorageStage,
};
use super::rpc::{
    DhtRequest, DhtResponse, ErrorCode, decode_request_with_trace_context, decode_response,
    encode_request_with_trace_context, encode_response,
};
use super::state::DhtStateMachine;
use super::storage::{
    CLEANUP_PAGE_SIZE, MergeError, StoredEntry, decode_entries, encode_entries, merge_entry,
    now_unix_secs, retained_entries, validate_entries,
};
use crate::connection_pool::{ConnectionPool, PoolConnectError};
use crate::telemetry::{
    current_trace_context, duration_ms, extract_trace_context, record_duration_ms,
    warn_if_slow_iroh_phase, warn_if_slow_iroh_request,
};

pub type CallerOutcome = std::result::Result<DhtOutputValue, DhtIoError>;
pub type InboundDhtStream = (SendStream, RecvStream, NodeId);

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

#[derive(Debug, Clone, Copy)]
enum DriverLane {
    Command,
    Inbound,
    Io,
}

enum DriverEvent {
    Shutdown,
    Tick,
    Command(DriverCmd),
    Inbound(InboundDhtStream),
    Io(DhtIo),
    Closed,
}

pub struct DhtDriver {
    state: DhtStateMachine,
    endpoint: Endpoint,
    connection_pool: ConnectionPool,
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
    inbound_contexts: HashMap<InboundId, SendStream>,
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

async fn next_driver_event(
    lane: DriverLane,
    shutdown: &CancellationToken,
    ticker: &mut tokio::time::Interval,
    cmd_rx: &mut DriverCmdReceiver,
    inbound_rx: &mut InboundReceiver,
    io_rx: &mut IoReceiver,
) -> (DriverEvent, DriverLane) {
    match lane {
        DriverLane::Command => {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => (DriverEvent::Shutdown, lane),
                _ = ticker.tick() => (DriverEvent::Tick, lane),
                result = cmd_rx.recv() => (
                    result.map_or(DriverEvent::Closed, DriverEvent::Command),
                    DriverLane::Inbound,
                ),
                result = inbound_rx.recv() => (
                    result.map_or(DriverEvent::Closed, DriverEvent::Inbound),
                    DriverLane::Io,
                ),
                result = io_rx.recv() => (
                    result.map_or(DriverEvent::Closed, DriverEvent::Io),
                    DriverLane::Command,
                ),
            }
        }
        DriverLane::Inbound => {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => (DriverEvent::Shutdown, lane),
                _ = ticker.tick() => (DriverEvent::Tick, lane),
                result = inbound_rx.recv() => (
                    result.map_or(DriverEvent::Closed, DriverEvent::Inbound),
                    DriverLane::Io,
                ),
                result = io_rx.recv() => (
                    result.map_or(DriverEvent::Closed, DriverEvent::Io),
                    DriverLane::Command,
                ),
                result = cmd_rx.recv() => (
                    result.map_or(DriverEvent::Closed, DriverEvent::Command),
                    DriverLane::Inbound,
                ),
            }
        }
        DriverLane::Io => {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => (DriverEvent::Shutdown, lane),
                _ = ticker.tick() => (DriverEvent::Tick, lane),
                result = io_rx.recv() => (
                    result.map_or(DriverEvent::Closed, DriverEvent::Io),
                    DriverLane::Command,
                ),
                result = cmd_rx.recv() => (
                    result.map_or(DriverEvent::Closed, DriverEvent::Command),
                    DriverLane::Inbound,
                ),
                result = inbound_rx.recv() => (
                    result.map_or(DriverEvent::Closed, DriverEvent::Inbound),
                    DriverLane::Io,
                ),
            }
        }
    }
}

impl DhtDriver {
    pub fn new(
        state: DhtStateMachine,
        endpoint: Endpoint,
        storage: StorageHandle,
        connection_pool: ConnectionPool,
        cmd_rx: DriverCmdReceiver,
        inbound_rx: InboundReceiver,
        shutdown: CancellationToken,
    ) -> Self {
        let (io_tx, io_rx) = mpsc::bounded_async(DRIVER_IO_EVENT_CAPACITY);

        Self {
            state,
            endpoint,
            connection_pool,
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
        let mut next_lane = DriverLane::Command;

        loop {
            let (event, following_lane) = next_driver_event(
                next_lane,
                &self.shutdown,
                &mut ticker,
                &mut self.cmd_rx,
                &mut self.inbound_rx,
                &mut self.io_rx,
            )
            .await;
            next_lane = following_lane;

            match event {
                DriverEvent::Shutdown | DriverEvent::Closed => break,
                DriverEvent::Command(cmd) => {
                    self.process_input(DhtInput::Clock {
                        now_secs: now_unix_secs(),
                    });
                    self.handle_driver_cmd(cmd);
                }
                DriverEvent::Inbound(inbound) => self.handle_inbound_stream(inbound),
                DriverEvent::Io(io) => {
                    self.process_input(DhtInput::Clock {
                        now_secs: now_unix_secs(),
                    });
                    self.handle_worker_io(io);
                }
                DriverEvent::Tick => {
                    self.now_tick = self.now_tick.saturating_add(1);
                    self.process_input(DhtInput::Tick {
                        now_tick: self.now_tick,
                        now_secs: now_unix_secs(),
                    });
                }
            }
        }

        self.fail_pending_callers(DhtIoError::Shutdown);
        self.inbound_contexts.clear();
    }

    #[tracing::instrument(
        name = "dht.driver.process_input",
        level = "trace",
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
        level = "trace",
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
            | DhtIo::StorageRevisionResult { op_id, .. }
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
            | DhtIoRequest::StorageRevision { op_id, .. }
            | DhtIoRequest::StorageWrite { op_id, .. }
            | DhtIoRequest::StorageMerge { op_id, .. }
            | DhtIoRequest::StorageDelete { op_id, .. }
            | DhtIoRequest::StorageIter { op_id, .. }
            | DhtIoRequest::StoragePrune { op_id, .. } => {
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
        fields(peer = %inbound.2)
    )]
    fn handle_inbound_stream(&mut self, inbound: InboundDhtStream) {
        let (send, mut recv, peer) = inbound;
        let inbound_id = self.next_inbound_id;
        self.next_inbound_id = self.next_inbound_id.saturating_add(1);

        self.inbound_contexts.insert(inbound_id, send);
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
                if let Some(mut send) = maybe_send
                    && let Err(error) = write_response_to_stream(
                        &mut send,
                        &DhtResponse::Error {
                            code: ErrorCode::InvalidRequest,
                            message: error.to_string(),
                        },
                    )
                    .await
                {
                    warn!(
                        inbound_id,
                        error = %error,
                        "Failed to dispatch inbound DHT read-error response"
                    );
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
                now_secs,
            } => self.dispatch_storage_read(op_id, stage, key, realm_filter, now_secs),
            DhtIoRequest::StorageRevision { op_id, stage } => {
                self.dispatch_storage_revision(op_id, stage)
            }
            DhtIoRequest::StorageWrite {
                op_id,
                stage,
                key,
                entries,
            } => self.dispatch_storage_write(op_id, stage, key, entries),
            DhtIoRequest::StorageMerge {
                op_id,
                stage,
                key,
                entry,
                now_secs,
            } => self.dispatch_storage_merge(op_id, stage, key, entry, now_secs),
            DhtIoRequest::StorageDelete { op_id, stage, key } => {
                self.dispatch_storage_delete(op_id, stage, key)
            }
            DhtIoRequest::StorageIter {
                op_id,
                stage,
                start_after,
                limit,
                now_secs,
            } => self.dispatch_storage_iter(op_id, stage, start_after, limit, now_secs),
            DhtIoRequest::StoragePrune {
                op_id,
                stage,
                key,
                now_secs,
            } => self.dispatch_storage_prune(op_id, stage, key, now_secs),
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
        let connection_pool = self.connection_pool.clone();
        let current_parent = Span::current();
        let span = info_span!(
            "dht.rpc.request",
            "otel.kind" = "client",
            "otel.status_code" = field::Empty,
            "otel.status_description" = field::Empty,
            "network.transport" = "quic",
            "rpc.system" = "aruna-dht",
            "iroh.alpn" = "aruna/dht/2",
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
                    connection_pool,
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
                if let Some(mut send) = maybe_send
                    && let Err(error) = write_response_to_stream(&mut send, &response).await
                {
                    warn!(
                        inbound_id,
                        error = %error,
                        "Failed to dispatch inbound DHT RPC response"
                    );
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
        now_secs: u64,
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
                        let mut entries = match value {
                            Some(data) => match decode_entries(&data) {
                                Ok(entries) => entries,
                                Err(error) => {
                                    warn!(op_id, stage = ?stage, key = %key, error = %error, "Failed to decode stored DHT entries");
                                    let _ = io_tx
                                        .send(DhtIo::StorageError {
                                            op_id,
                                            stage,
                                            error: DhtIoError::storage(error),
                                        })
                                        .await;
                                    return;
                                }
                            },
                            None => Vec::new(),
                        };
                        if let Err(error) = validate_entries(&key, &entries, now_secs) {
                            warn!(op_id, stage = ?stage, key = %key, error = %error, "Invalid stored DHT entries");
                            let _ = io_tx
                                .send(DhtIo::StorageError {
                                    op_id,
                                    stage,
                                    error: DhtIoError::storage(error),
                                })
                                .await;
                            return;
                        }
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

    fn dispatch_storage_revision(&self, op_id: OpId, stage: StorageStage) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                match reserve_revision(&storage, op_id, stage).await {
                    Ok(revision) => {
                        let _ = io_tx
                            .send(DhtIo::StorageRevisionResult {
                                op_id,
                                stage,
                                revision,
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
                let bytes = match encode_entries(&entries) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error: DhtIoError::storage(error),
                            })
                            .await;
                        return;
                    }
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
        name = "dht.driver.storage_merge.dispatch",
        level = "debug",
        skip(self, entry),
        fields(op_id, stage = ?stage, key = %key)
    )]
    fn dispatch_storage_merge(
        &self,
        op_id: OpId,
        stage: StorageStage,
        key: DhtKeyId,
        entry: StoredEntry,
        now_secs: u64,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                let mutation = StorageMutation::Merge { entry, now_secs };
                match mutate_storage_entries(&storage, op_id, stage, key, &mutation).await {
                    Ok(()) => {
                        let _ = io_tx.send(DhtIo::StorageWriteResult { op_id, stage }).await;
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
        name = "dht.driver.storage_prune.dispatch",
        level = "debug",
        skip(self),
        fields(op_id, stage = ?stage, key = %key)
    )]
    fn dispatch_storage_prune(
        &self,
        op_id: OpId,
        stage: StorageStage,
        key: DhtKeyId,
        now_secs: u64,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                let mutation = StorageMutation::Prune { now_secs };
                match mutate_storage_entries(&storage, op_id, stage, key, &mutation).await {
                    Ok(()) => {
                        let _ = io_tx.send(DhtIo::StorageWriteResult { op_id, stage }).await;
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
        now_secs: u64,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                let effect = Effect::Storage(StorageEffect::Iter {
                    key_space: DHT_KEYSPACE.to_string(),
                    prefix: None,
                    start: start_after.map(ByteView::from).map(IterStart::After),
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
                            .filter_map(|(key, value)| {
                                let raw_key = key.as_ref();
                                let Ok(key_bytes): Result<[u8; 32], _> = raw_key.try_into() else {
                                    warn!(key = %hex::encode(raw_key), "Preserving invalid DHT key during cleanup");
                                    return None;
                                };
                                let dht_key = DhtKeyId::from_bytes(key_bytes);
                                match decode_entries(&value) {
                                    Ok(entries)
                                        if validate_entries(&dht_key, &entries, now_secs).is_ok() =>
                                    {
                                        Some((raw_key.to_vec(), entries))
                                    }
                                    Ok(_) => {
                                        warn!(key = %hex::encode(raw_key), "Preserving invalid DHT row during cleanup");
                                        None
                                    }
                                    Err(error) => {
                                        warn!(key = %hex::encode(raw_key), error = %error, "Preserving corrupt DHT row during cleanup");
                                        None
                                    }
                                }
                            })
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

enum StorageMutation {
    Merge { entry: StoredEntry, now_secs: u64 },
    Prune { now_secs: u64 },
}

enum TransactionFailure {
    Conflict,
    Failed(DhtIoError),
}

async fn start_storage_txn(
    storage: &StorageHandle,
    op_id: OpId,
    stage: StorageStage,
) -> Result<TxnId, TransactionFailure> {
    let start = Effect::Storage(StorageEffect::StartTransaction { read: false });
    match send_storage_effect_with_timeout(storage, start, op_id, stage, "start").await {
        Ok(Event::Storage(StorageEvent::TransactionStarted { txn_id })) => Ok(txn_id),
        Ok(Event::Storage(StorageEvent::Error { error })) => Err(storage_transaction_error(error)),
        Ok(other) => Err(TransactionFailure::Failed(DhtIoError::storage(format!(
            "unexpected transaction start event: {other:?}"
        )))),
        Err(error) => Err(TransactionFailure::Failed(error)),
    }
}

async fn reserve_revision(
    storage: &StorageHandle,
    op_id: OpId,
    stage: StorageStage,
) -> Result<u64, DhtIoError> {
    for _ in 0..STORAGE_MUTATION_RETRIES {
        let txn_id = match start_storage_txn(storage, op_id, stage).await {
            Ok(txn_id) => txn_id,
            Err(TransactionFailure::Conflict) => continue,
            Err(TransactionFailure::Failed(error)) => return Err(error),
        };
        match reserve_revision_txn(storage, op_id, stage, txn_id).await {
            Ok(revision) => return Ok(revision),
            Err(TransactionFailure::Conflict) => continue,
            Err(TransactionFailure::Failed(error)) => return Err(error),
        }
    }
    Err(DhtIoError::storage(
        "DHT revision reservation exceeded conflict retry limit",
    ))
}

async fn reserve_revision_txn(
    storage: &StorageHandle,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
) -> Result<u64, TransactionFailure> {
    let read = Effect::Storage(StorageEffect::Read {
        key_space: DHT_META_KEYSPACE.to_string(),
        key: ByteView::from(DHT_REVISION_KEY),
        txn_id: Some(txn_id),
    });
    let current = match send_storage_effect_with_timeout(
        storage,
        read,
        op_id,
        stage,
        "revision_read",
    )
    .await
    {
        Ok(Event::Storage(StorageEvent::ReadResult { value, .. })) => match decode_counter(value) {
            Ok(current) => current,
            Err(error) => {
                abort_storage_txn(storage, op_id, stage, txn_id).await;
                return Err(error);
            }
        },
        Ok(Event::Storage(StorageEvent::Error { error })) => {
            abort_storage_txn(storage, op_id, stage, txn_id).await;
            return Err(storage_transaction_error(error));
        }
        Ok(other) => {
            abort_storage_txn(storage, op_id, stage, txn_id).await;
            return Err(TransactionFailure::Failed(DhtIoError::storage(format!(
                "unexpected revision read event: {other:?}"
            ))));
        }
        Err(error) => {
            abort_storage_txn(storage, op_id, stage, txn_id).await;
            return Err(TransactionFailure::Failed(error));
        }
    };
    let Some(revision) = current.checked_add(1) else {
        abort_storage_txn(storage, op_id, stage, txn_id).await;
        return Err(TransactionFailure::Failed(DhtIoError::storage(
            "DHT revision counter exhausted",
        )));
    };
    if let Err(error) = write_counter(
        storage,
        op_id,
        stage,
        txn_id,
        DHT_REVISION_KEY,
        revision,
        "revision_write",
    )
    .await
    {
        abort_storage_txn(storage, op_id, stage, txn_id).await;
        return Err(error);
    }
    commit_storage_txn(storage, op_id, stage, txn_id).await?;
    Ok(revision)
}

async fn mutate_storage_entries(
    storage: &StorageHandle,
    op_id: OpId,
    stage: StorageStage,
    key: DhtKeyId,
    mutation: &StorageMutation,
) -> Result<(), DhtIoError> {
    for _ in 0..STORAGE_MUTATION_RETRIES {
        let txn_id = match start_storage_txn(storage, op_id, stage).await {
            Ok(txn_id) => txn_id,
            Err(TransactionFailure::Conflict) => continue,
            Err(TransactionFailure::Failed(error)) => return Err(error),
        };

        match run_storage_txn(storage, op_id, stage, key, txn_id, mutation).await {
            Ok(()) => return Ok(()),
            Err(TransactionFailure::Conflict) => continue,
            Err(TransactionFailure::Failed(error)) => return Err(error),
        }
    }

    Err(DhtIoError::storage(
        "DHT storage mutation exceeded conflict retry limit",
    ))
}

async fn run_storage_txn(
    storage: &StorageHandle,
    op_id: OpId,
    stage: StorageStage,
    key: DhtKeyId,
    txn_id: TxnId,
    mutation: &StorageMutation,
) -> Result<(), TransactionFailure> {
    let read = Effect::Storage(StorageEffect::Read {
        key_space: DHT_KEYSPACE.to_string(),
        key: ByteView::from(key.as_bytes().as_slice()),
        txn_id: Some(txn_id),
    });
    let value = match send_storage_effect_with_timeout(storage, read, op_id, stage, "read").await {
        Ok(Event::Storage(StorageEvent::ReadResult { value, .. })) => value,
        Ok(Event::Storage(StorageEvent::Error { error })) => {
            abort_storage_txn(storage, op_id, stage, txn_id).await;
            return Err(storage_transaction_error(error));
        }
        Ok(other) => {
            abort_storage_txn(storage, op_id, stage, txn_id).await;
            return Err(TransactionFailure::Failed(DhtIoError::storage(format!(
                "unexpected transactional read event: {other:?}"
            ))));
        }
        Err(error) => {
            abort_storage_txn(storage, op_id, stage, txn_id).await;
            return Err(TransactionFailure::Failed(error));
        }
    };

    let had_value = value.is_some();
    let entries = match value {
        Some(value) => match decode_entries(&value) {
            Ok(entries) => entries,
            Err(error) => {
                abort_storage_txn(storage, op_id, stage, txn_id).await;
                return Err(TransactionFailure::Failed(DhtIoError::storage(error)));
            }
        },
        None => Vec::new(),
    };
    let entries = match mutation {
        StorageMutation::Merge { entry, now_secs } => {
            match merge_entry(&key, entries, entry.clone(), *now_secs) {
                Ok(entries) => entries,
                Err(MergeError::Stale) => {
                    abort_storage_txn(storage, op_id, stage, txn_id).await;
                    return Err(TransactionFailure::Failed(DhtIoError::invalid_request(
                        "record version is stale or conflicting",
                    )));
                }
                Err(MergeError::Capacity) => {
                    abort_storage_txn(storage, op_id, stage, txn_id).await;
                    return Err(TransactionFailure::Failed(DhtIoError::StorageFull));
                }
                Err(MergeError::Encoding) => {
                    abort_storage_txn(storage, op_id, stage, txn_id).await;
                    return Err(TransactionFailure::Failed(DhtIoError::storage(
                        "serialize DHT entries failed",
                    )));
                }
                Err(MergeError::Invalid) => {
                    abort_storage_txn(storage, op_id, stage, txn_id).await;
                    return Err(TransactionFailure::Failed(DhtIoError::storage(
                        "stored DHT record set is invalid",
                    )));
                }
            }
        }
        StorageMutation::Prune { now_secs } => {
            if let Err(error) = validate_entries(&key, &entries, *now_secs) {
                abort_storage_txn(storage, op_id, stage, txn_id).await;
                return Err(TransactionFailure::Failed(DhtIoError::storage(error)));
            }
            retained_entries(entries, *now_secs)
        }
    };

    let count_result = if !had_value && !entries.is_empty() {
        update_key_count(storage, op_id, stage, txn_id, true).await
    } else if had_value && entries.is_empty() {
        update_key_count(storage, op_id, stage, txn_id, false).await
    } else {
        Ok(())
    };
    if let Err(error) = count_result {
        abort_storage_txn(storage, op_id, stage, txn_id).await;
        return Err(error);
    }

    let mutation_effect = if entries.is_empty() {
        StorageEffect::Delete {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            txn_id: Some(txn_id),
        }
    } else {
        let bytes = match encode_entries(&entries) {
            Ok(bytes) => bytes,
            Err(error) => {
                abort_storage_txn(storage, op_id, stage, txn_id).await;
                return Err(TransactionFailure::Failed(DhtIoError::storage(error)));
            }
        };
        StorageEffect::Write {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            value: ByteView::from(bytes),
            txn_id: Some(txn_id),
        }
    };

    match send_storage_effect_with_timeout(
        storage,
        Effect::Storage(mutation_effect),
        op_id,
        stage,
        "mutate",
    )
    .await
    {
        Ok(Event::Storage(StorageEvent::WriteResult { .. }))
        | Ok(Event::Storage(StorageEvent::DeleteResult { .. })) => {}
        Ok(Event::Storage(StorageEvent::Error { error })) => {
            abort_storage_txn(storage, op_id, stage, txn_id).await;
            return Err(storage_transaction_error(error));
        }
        Ok(other) => {
            abort_storage_txn(storage, op_id, stage, txn_id).await;
            return Err(TransactionFailure::Failed(DhtIoError::storage(format!(
                "unexpected transactional mutation event: {other:?}"
            ))));
        }
        Err(error) => {
            abort_storage_txn(storage, op_id, stage, txn_id).await;
            return Err(TransactionFailure::Failed(error));
        }
    }

    commit_storage_txn(storage, op_id, stage, txn_id).await
}

async fn update_key_count(
    storage: &StorageHandle,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
    increment: bool,
) -> Result<(), TransactionFailure> {
    let read = Effect::Storage(StorageEffect::Read {
        key_space: DHT_META_KEYSPACE.to_string(),
        key: ByteView::from(DHT_KEY_COUNT_KEY),
        txn_id: Some(txn_id),
    });
    let count =
        match send_storage_effect_with_timeout(storage, read, op_id, stage, "count_read").await {
            Ok(Event::Storage(StorageEvent::ReadResult { value, .. })) => decode_counter(value)?,
            Ok(Event::Storage(StorageEvent::Error { error })) => {
                return Err(storage_transaction_error(error));
            }
            Ok(other) => {
                return Err(TransactionFailure::Failed(DhtIoError::storage(format!(
                    "unexpected DHT key-count read event: {other:?}"
                ))));
            }
            Err(error) => return Err(TransactionFailure::Failed(error)),
        };
    let next = if increment {
        if count >= MAX_STORED_KEYS {
            return Err(TransactionFailure::Failed(DhtIoError::StorageFull));
        }
        count + 1
    } else {
        count.checked_sub(1).ok_or_else(|| {
            TransactionFailure::Failed(DhtIoError::storage("DHT key-count metadata is invalid"))
        })?
    };
    write_counter(
        storage,
        op_id,
        stage,
        txn_id,
        DHT_KEY_COUNT_KEY,
        next,
        "count_write",
    )
    .await
}

fn decode_counter(value: Option<ByteView>) -> Result<u64, TransactionFailure> {
    let Some(value) = value else {
        return Ok(0);
    };
    let bytes: [u8; 8] = value.as_ref().try_into().map_err(|_| {
        TransactionFailure::Failed(DhtIoError::storage("invalid DHT metadata counter"))
    })?;
    Ok(u64::from_le_bytes(bytes))
}

async fn write_counter(
    storage: &StorageHandle,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
    key: &'static [u8],
    value: u64,
    operation: &'static str,
) -> Result<(), TransactionFailure> {
    let write = Effect::Storage(StorageEffect::Write {
        key_space: DHT_META_KEYSPACE.to_string(),
        key: ByteView::from(key),
        value: ByteView::from(value.to_le_bytes().as_slice()),
        txn_id: Some(txn_id),
    });
    match send_storage_effect_with_timeout(storage, write, op_id, stage, operation).await {
        Ok(Event::Storage(StorageEvent::WriteResult { .. })) => Ok(()),
        Ok(Event::Storage(StorageEvent::Error { error })) => Err(storage_transaction_error(error)),
        Ok(other) => Err(TransactionFailure::Failed(DhtIoError::storage(format!(
            "unexpected DHT metadata write event: {other:?}"
        )))),
        Err(error) => Err(TransactionFailure::Failed(error)),
    }
}

async fn commit_storage_txn(
    storage: &StorageHandle,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
) -> Result<(), TransactionFailure> {
    let commit = Effect::Storage(StorageEffect::CommitTransaction { txn_id });
    match send_storage_effect_with_timeout(storage, commit, op_id, stage, "commit").await {
        Ok(Event::Storage(StorageEvent::TransactionCommitted { .. })) => Ok(()),
        Ok(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        })) => Err(TransactionFailure::Conflict),
        Ok(Event::Storage(StorageEvent::Error { error })) => {
            Err(TransactionFailure::Failed(DhtIoError::storage(error)))
        }
        Ok(other) => Err(TransactionFailure::Failed(DhtIoError::storage(format!(
            "unexpected transaction commit event: {other:?}"
        )))),
        Err(error) => Err(TransactionFailure::Failed(error)),
    }
}

async fn abort_storage_txn(
    storage: &StorageHandle,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
) {
    let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id });
    let _ = send_storage_effect_with_timeout(storage, abort, op_id, stage, "abort").await;
}

fn storage_transaction_error(error: StorageError) -> TransactionFailure {
    if error == StorageError::TransactionConflict {
        TransactionFailure::Conflict
    } else {
        TransactionFailure::Failed(DhtIoError::storage(error))
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

fn pool_connect_error(error: PoolConnectError) -> DhtIoError {
    match error {
        PoolConnectError::Timeout => DhtIoError::Timeout,
        PoolConnectError::Shutdown => DhtIoError::Shutdown,
        other => DhtIoError::network(other),
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

fn dht_io_request_kind(request: &DhtIoRequest) -> &'static str {
    match request {
        DhtIoRequest::RpcRequest { .. } => "rpc_request",
        DhtIoRequest::RpcResponse { .. } => "rpc_response",
        DhtIoRequest::DropInbound { .. } => "drop_inbound",
        DhtIoRequest::StorageRead { .. } => "storage_read",
        DhtIoRequest::StorageRevision { .. } => "storage_revision",
        DhtIoRequest::StorageWrite { .. } => "storage_write",
        DhtIoRequest::StorageMerge { .. } => "storage_merge",
        DhtIoRequest::StorageDelete { .. } => "storage_delete",
        DhtIoRequest::StorageIter { .. } => "storage_iter",
        DhtIoRequest::StoragePrune { .. } => "storage_prune",
    }
}

fn dht_io_request_op_id(request: &DhtIoRequest) -> Option<OpId> {
    match request {
        DhtIoRequest::RpcRequest { op_id, .. }
        | DhtIoRequest::StorageRead { op_id, .. }
        | DhtIoRequest::StorageRevision { op_id, .. }
        | DhtIoRequest::StorageWrite { op_id, .. }
        | DhtIoRequest::StorageMerge { op_id, .. }
        | DhtIoRequest::StorageDelete { op_id, .. }
        | DhtIoRequest::StorageIter { op_id, .. }
        | DhtIoRequest::StoragePrune { op_id, .. } => Some(*op_id),
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
        | DhtIoRequest::StorageRevision { .. }
        | DhtIoRequest::StorageWrite { .. }
        | DhtIoRequest::StorageMerge { .. }
        | DhtIoRequest::StorageDelete { .. }
        | DhtIoRequest::StorageIter { .. }
        | DhtIoRequest::StoragePrune { .. } => None,
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
        | DhtIoRequest::StorageRevision { stage, .. }
        | DhtIoRequest::StorageWrite { stage, .. }
        | DhtIoRequest::StorageMerge { stage, .. }
        | DhtIoRequest::StorageDelete { stage, .. }
        | DhtIoRequest::StorageIter { stage, .. }
        | DhtIoRequest::StoragePrune { stage, .. } => match stage {
            StorageStage::PutRevision
            | StorageStage::PutLocalRead
            | StorageStage::PutLocalWrite
            | StorageStage::PutLocalMerge => "put",
            StorageStage::GetLocalRead => "get",
            StorageStage::InboundGetRead => "inbound_get",
            StorageStage::InboundPutRead
            | StorageStage::InboundPutWrite
            | StorageStage::InboundPutMerge => "inbound_put",
            StorageStage::CleanupIter
            | StorageStage::CleanupWrite
            | StorageStage::CleanupDelete
            | StorageStage::CleanupPrune => "cleanup",
        },
        DhtIoRequest::RpcResponse { .. } | DhtIoRequest::DropInbound { .. } => "inbound",
    }
}

fn encode_request_frame(
    request: &DhtRequest,
    trace_context: Option<DistributedTraceContext>,
) -> Result<Vec<u8>, DhtIoError> {
    let bytes = encode_request_with_trace_context(request, trace_context)?;
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err(DhtIoError::invalid_request("request too large"));
    }
    Ok(bytes)
}

#[tracing::instrument(
    name = "dht.rpc.request.io",
    level = "debug",
    skip(connection_pool, request, trace_context),
    fields(op_id, phase = ?phase, peer = %peer, request = ?request)
)]
async fn rpc_request(
    connection_pool: ConnectionPool,
    op_id: OpId,
    phase: RpcPhase,
    peer: NodeId,
    request: DhtRequest,
    trace_context: Option<DistributedTraceContext>,
) -> Result<DhtResponse, DhtIoError> {
    let span = Span::current();
    let total_started = Instant::now();
    let request_bytes = encode_request_frame(&request, trace_context)?;
    span.record("iroh.request_bytes", request_bytes.len() as u64);

    let connect_started = Instant::now();
    let conn = match connection_pool.get_or_connect(peer, Alpn::Dht).await {
        Ok(conn) => {
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
                "Iroh DHT RPC connect failed"
            );
            return Err(pool_connect_error(error));
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::RealmId;
    use aruna_storage::FjallStorage;
    use tempfile::{TempDir, tempdir};

    fn make_node(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn make_entry(seed: u8, key: DhtKeyId, expires_at: u64) -> StoredEntry {
        let mut secret = [0u8; 32];
        secret[0] = seed;
        let secret = iroh::SecretKey::from_bytes(&secret);
        let publisher = secret.public();
        let mut realm = [0u8; 32];
        realm[0] = seed;
        let realm_id = RealmId::from_bytes(realm);
        let value = vec![seed];
        let revision = 1;
        let signed = super::super::rpc::signed_record_bytes(
            &key, &publisher, &realm_id, &value, expires_at, revision,
        );
        StoredEntry {
            publisher,
            realm_id,
            value,
            expires_at,
            revision,
            signature: secret.sign(&signed),
            retain_until: expires_at,
        }
    }

    fn open_storage() -> (TempDir, StorageHandle) {
        let directory = tempdir().expect("create storage directory");
        let storage = FjallStorage::open(directory.path().to_str().expect("utf-8 storage path"))
            .expect("open storage");
        (directory, storage)
    }

    async fn read_entries(storage: &StorageHandle, key: DhtKeyId) -> Vec<StoredEntry> {
        let event = storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: DHT_KEYSPACE.to_string(),
                key: ByteView::from(key.as_bytes().as_slice()),
                txn_id: None,
            }))
            .await;
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = event
        else {
            panic!("expected stored DHT entries: {event:?}");
        };
        decode_entries(&value).expect("decode stored entries")
    }

    #[tokio::test]
    async fn ready_lanes_rotate() {
        let (cmd_tx, mut cmd_rx) = mpsc::bounded_blocking_async(4);
        let (_inbound_tx, mut inbound_rx) = mpsc::bounded_blocking_async(1);
        let (io_tx, mut io_rx) = mpsc::bounded_async(4);
        cmd_tx
            .try_send(DriverCmd::AddPeer {
                node_id: make_node(1),
            })
            .expect("queue first command");
        cmd_tx
            .try_send(DriverCmd::AddPeer {
                node_id: make_node(2),
            })
            .expect("queue second command");
        io_tx
            .send(DhtIo::PeerSeen { peer: make_node(3) })
            .await
            .expect("queue IO event");

        let shutdown = CancellationToken::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(3_600));
        let _ = ticker.tick().await;
        let (first, next_lane) = next_driver_event(
            DriverLane::Command,
            &shutdown,
            &mut ticker,
            &mut cmd_rx,
            &mut inbound_rx,
            &mut io_rx,
        )
        .await;
        assert!(matches!(first, DriverEvent::Command(_)));

        let (second, _) = next_driver_event(
            next_lane,
            &shutdown,
            &mut ticker,
            &mut cmd_rx,
            &mut inbound_rx,
            &mut io_rx,
        )
        .await;
        assert!(matches!(second, DriverEvent::Io(_)));
    }

    #[test]
    fn request_size_bound() {
        let request = DhtRequest::PutValue {
            key: DhtKeyId::from_data(b"oversized-request"),
            realm_id: RealmId::from_bytes([1u8; 32]),
            value: vec![0; MAX_MESSAGE_SIZE],
            expires_at: 1,
            revision: 1,
            publisher: make_node(1),
            signature: None,
        };

        assert!(matches!(
            encode_request_frame(&request, None),
            Err(DhtIoError::InvalidRequest(message)) if message == "request too large"
        ));
    }

    #[tokio::test]
    async fn revision_is_monotonic() {
        let (_directory, storage) = open_storage();
        let first = reserve_revision(&storage, 10, StorageStage::PutRevision)
            .await
            .expect("reserve first revision");
        let second = reserve_revision(&storage, 11, StorageStage::PutRevision)
            .await
            .expect("reserve second revision");

        assert_eq!((first, second), (1, 2));
    }

    #[tokio::test]
    async fn key_limit_enforced() {
        let (_directory, storage) = open_storage();
        let event = storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: DHT_META_KEYSPACE.to_string(),
                key: ByteView::from(DHT_KEY_COUNT_KEY),
                value: ByteView::from(MAX_STORED_KEYS.to_le_bytes().as_slice()),
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let key = DhtKeyId::from_data(b"full-keyspace");
        let mutation = StorageMutation::Merge {
            entry: make_entry(1, key, 200),
            now_secs: 100,
        };
        assert!(matches!(
            mutate_storage_entries(&storage, 12, StorageStage::InboundPutMerge, key, &mutation,)
                .await,
            Err(DhtIoError::StorageFull)
        ));

        let event = storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: DHT_KEYSPACE.to_string(),
                key: ByteView::from(key.as_bytes().as_slice()),
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::ReadResult { value: None, .. })
        ));
    }

    #[tokio::test]
    async fn concurrent_merges_preserve() {
        let (_directory, storage) = open_storage();
        let key = DhtKeyId::from_data(b"concurrent-merges");
        let first = StorageMutation::Merge {
            entry: make_entry(1, key, 200),
            now_secs: 100,
        };
        let second = StorageMutation::Merge {
            entry: make_entry(2, key, 200),
            now_secs: 100,
        };

        let (first_result, second_result) = tokio::join!(
            mutate_storage_entries(&storage, 1, StorageStage::PutLocalMerge, key, &first),
            mutate_storage_entries(&storage, 2, StorageStage::InboundPutMerge, key, &second),
        );
        first_result.expect("first merge succeeds");
        second_result.expect("second merge retries and succeeds");

        let entries = read_entries(&storage, key).await;
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn cleanup_preserves_fresh() {
        let (_directory, storage) = open_storage();
        let key = DhtKeyId::from_data(b"cleanup-race");
        let old = StorageMutation::Merge {
            entry: make_entry(1, key, 50),
            now_secs: 0,
        };
        mutate_storage_entries(&storage, 3, StorageStage::InboundPutMerge, key, &old)
            .await
            .expect("store old entry");

        let prune = StorageMutation::Prune { now_secs: 400 };
        let merge = StorageMutation::Merge {
            entry: make_entry(2, key, 500),
            now_secs: 400,
        };
        let (prune_result, merge_result) = tokio::join!(
            mutate_storage_entries(&storage, 3, StorageStage::CleanupPrune, key, &prune),
            mutate_storage_entries(&storage, 4, StorageStage::InboundPutMerge, key, &merge),
        );
        prune_result.expect("cleanup retries and succeeds");
        merge_result.expect("fresh merge succeeds");

        let entries = read_entries(&storage, key).await;
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| entry.value == vec![2]));
        assert!(entries.iter().any(|entry| entry.value == vec![1]));
    }

    #[tokio::test]
    async fn corrupt_row_preserved() {
        let (_directory, storage) = open_storage();
        let key = DhtKeyId::from_data(b"corrupt-row");
        let corrupt = ByteView::from(b"not-postcard".as_slice());
        let event = storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: DHT_KEYSPACE.to_string(),
                key: ByteView::from(key.as_bytes().as_slice()),
                value: corrupt.clone(),
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let mutation = StorageMutation::Merge {
            entry: make_entry(1, key, 200),
            now_secs: 100,
        };
        assert!(
            mutate_storage_entries(&storage, 5, StorageStage::InboundPutMerge, key, &mutation,)
                .await
                .is_err()
        );

        let event = storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: DHT_KEYSPACE.to_string(),
                key: ByteView::from(key.as_bytes().as_slice()),
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::ReadResult { value: Some(value), .. }) if value == corrupt
        ));
    }
}
