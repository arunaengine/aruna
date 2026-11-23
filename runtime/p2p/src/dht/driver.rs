//! Runs the DHT driver loop that drives the state machine and its RPC and storage work.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::alpn::Alpn;
use aruna_core::effects::DhtGetOptions;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::{StoragePort, TransactionOwner};
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::keyspaces::DHT_KEYSPACE;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::trace_context::DistributedTraceContext;
use aruna_core::types::TxnId;
use byteview::ByteView;
use crossfire::{AsyncRx, MAsyncTx, MTx, mpsc};
use iroh::Endpoint;
use iroh::endpoint::{RecvStream, SendStream};
use tokio::sync::oneshot;
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, debug_span, field, info_span, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::constants::{
    CMD_CHANNEL_CAPACITY, DHT_META_KEYSPACE, DHT_REVISION_KEY, DRIVER_TICK_INTERVAL,
    ENTRIES_PER_KEY, INBOUND_STORAGE_BUDGET, IO_EVENT_CAPACITY, KEY_COUNT_KEY, MAX_INBOUND_RPCS,
    MAX_MESSAGE_SIZE, MAX_STORED_KEYS, RPC_TIMEOUT, STORAGE_MUTATION_RETRIES, STORAGE_TIMEOUT,
};
use super::kbucket::K;
use super::protocol::{
    CLEANUP_OP_ID, DhtCmd, DhtEffect, DhtInput, DhtIo, DhtIoError, DhtIoRequest, DhtOutput,
    DhtOutputValue, InboundId, OpId, RpcPhase, StorageStage, dht_input_kind, dht_io_kind,
    io_inbound_id, io_op_id,
};
use super::rpc::{
    DhtRequest, DhtResponse, ErrorCode, decode_response, decode_traced_request, encode_response,
    encode_traced_request, request_kind, response_kind,
};
use super::state::DhtStateMachine;
use super::storage::{
    CLEANUP_PAGE_SIZE, DeadlineEntry, DeadlineIndex, DhtClock, DhtClockError, MergeError,
    StoredEntry, active_deadline, deadline_key, decode_entries, encode_entries, floor_deadline,
    merge_entry, parse_deadline, retained_entries, row_deadline, validate_entries,
};
use crate::connection_pool::{ConnectionPool, PoolConnectError};
use crate::telemetry::{record_selected_path, warn_iroh_phase, warn_iroh_request};
use aruna_clock::unix_timestamp_secs;
use aruna_core::telemetry::{
    current_trace_context, duration_ms, extract_trace_context, record_duration_ms,
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
        options: DhtGetOptions,
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
                key,
                realm_filter,
                options,
                ..
            } => f
                .debug_struct("DriverCmd::Get")
                .field("key", key)
                .field("realm_filter", realm_filter)
                .field("options", options)
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

fn reject_driver_cmd(cmd: DriverCmd, error: DhtIoError) {
    match cmd {
        DriverCmd::Put { reply, .. }
        | DriverCmd::Get { reply, .. }
        | DriverCmd::Bootstrap { reply, .. }
        | DriverCmd::RoutingTableSize { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        DriverCmd::AddPeer { .. } => {}
    }
}

fn clock_error(error: DhtClockError) -> DhtIoError {
    DhtIoError::storage(format!("DHT clock unhealthy: {error}"))
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
    Deadline,
    Command(DriverCmd),
    Inbound(InboundDhtStream),
    Io(DhtIo),
    Closed,
}

/// Wall-clock deadlines of the operations a caller is waiting on. The driver
/// owns them so an expired lookup is released here instead of running on after
/// its caller stopped waiting.
#[derive(Debug, Default)]
struct OpDeadlines {
    by_op: HashMap<OpId, TokioInstant>,
    order: BTreeSet<(TokioInstant, OpId)>,
}

impl OpDeadlines {
    fn insert(&mut self, op_id: OpId, at: TokioInstant) {
        self.remove(op_id);
        self.by_op.insert(op_id, at);
        self.order.insert((at, op_id));
    }

    fn remove(&mut self, op_id: OpId) {
        if let Some(at) = self.by_op.remove(&op_id) {
            self.order.remove(&(at, op_id));
        }
    }

    fn next(&self) -> Option<TokioInstant> {
        self.order.first().map(|(at, _)| *at)
    }

    fn expired(&mut self, now: TokioInstant) -> Vec<OpId> {
        let mut expired = Vec::new();
        while let Some((at, op_id)) = self.order.first().copied() {
            if at > now {
                break;
            }
            self.order.remove(&(at, op_id));
            self.by_op.remove(&op_id);
            expired.push(op_id);
        }
        expired
    }

    fn contains(&self, op_id: OpId) -> bool {
        self.by_op.contains_key(&op_id)
    }

    fn len(&self) -> usize {
        self.by_op.len()
    }
}

// Committed blocks amortize revision I/O; unused values become harmless crash gaps.
const REVISION_BLOCK_SIZE: u64 = 64;

struct RevisionRequest {
    op_id: OpId,
    stage: StorageStage,
    span: Span,
}

pub struct DhtDriver {
    state: DhtStateMachine,
    clock: DhtClock,
    clock_diverged: bool,
    endpoint: Endpoint,
    connection_pool: ConnectionPool,
    storage: Arc<dyn StoragePort>,
    cmd_rx: DriverCmdReceiver,
    inbound_rx: InboundReceiver,
    io_tx: IoSender,
    io_rx: IoReceiver,
    shutdown: CancellationToken,
    now_tick: u64,
    pending_callers: HashMap<OpId, oneshot::Sender<CallerOutcome>>,
    deadlines: OpDeadlines,
    op_spans: HashMap<OpId, Span>,
    next_op_id: OpId,
    inbound_contexts: HashMap<InboundId, Option<SendStream>>,
    inbound_spans: HashMap<InboundId, Span>,
    next_inbound_id: InboundId,
    revision_tx: tokio::sync::mpsc::Sender<RevisionRequest>,
    revision_task: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for DhtDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtDriver")
            .field("now_tick", &self.now_tick)
            .field("pending_callers", &self.pending_callers.len())
            .field("deadlines", &self.deadlines.len())
            .field("inbound_contexts", &self.inbound_contexts.len())
            .finish()
    }
}

#[allow(clippy::too_many_arguments)]
async fn next_driver_event(
    lane: DriverLane,
    shutdown: &CancellationToken,
    ticker: &mut tokio::time::Interval,
    deadline: Option<TokioInstant>,
    cmd_rx: &mut DriverCmdReceiver,
    inbound_rx: &mut InboundReceiver,
    io_rx: &mut IoReceiver,
) -> (DriverEvent, DriverLane) {
    let expiry = async move {
        match deadline {
            Some(at) => tokio::time::sleep_until(at).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(expiry);

    match lane {
        DriverLane::Command => {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => (DriverEvent::Shutdown, lane),
                _ = &mut expiry => (DriverEvent::Deadline, lane),
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
                _ = &mut expiry => (DriverEvent::Deadline, lane),
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
                _ = &mut expiry => (DriverEvent::Deadline, lane),
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
        storage: impl Into<Arc<dyn StoragePort>>,
        connection_pool: ConnectionPool,
        cmd_rx: DriverCmdReceiver,
        inbound_rx: InboundReceiver,
        shutdown: CancellationToken,
    ) -> Self {
        let clock = DhtClock::from_secs(state.clock_secs());
        Self::with_clock(
            state,
            clock,
            endpoint,
            storage,
            connection_pool,
            cmd_rx,
            inbound_rx,
            shutdown,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_clock(
        state: DhtStateMachine,
        clock: DhtClock,
        endpoint: Endpoint,
        storage: impl Into<Arc<dyn StoragePort>>,
        connection_pool: ConnectionPool,
        cmd_rx: DriverCmdReceiver,
        inbound_rx: InboundReceiver,
        shutdown: CancellationToken,
    ) -> Self {
        let storage: Arc<dyn StoragePort> = storage.into();
        let (io_tx, io_rx) = mpsc::bounded_async(IO_EVENT_CAPACITY);
        let (revision_tx, revision_rx) = tokio::sync::mpsc::channel(CMD_CHANNEL_CAPACITY);
        let revision_task = tokio::spawn(run_revision_allocator(
            storage.clone(),
            io_tx.clone(),
            revision_rx,
            shutdown.clone(),
        ));

        Self {
            state,
            clock,
            clock_diverged: false,
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
            deadlines: OpDeadlines::default(),
            op_spans: HashMap::new(),
            next_op_id: 1,
            inbound_contexts: HashMap::new(),
            inbound_spans: HashMap::new(),
            next_inbound_id: 1,
            revision_tx,
            revision_task,
        }
    }

    #[tracing::instrument(name = "dht.driver.run", level = "debug", skip(self))]
    pub async fn run(mut self) {
        let mut ticker = tokio::time::interval(DRIVER_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let _ = ticker.tick().await;
        let mut next_lane = DriverLane::Command;

        let terminal_error = loop {
            let (event, following_lane) = next_driver_event(
                next_lane,
                &self.shutdown,
                &mut ticker,
                self.deadlines.next(),
                &mut self.cmd_rx,
                &mut self.inbound_rx,
                &mut self.io_rx,
            )
            .await;
            next_lane = following_lane;

            if matches!(&event, DriverEvent::Shutdown | DriverEvent::Closed) {
                break DhtIoError::Shutdown;
            }
            self.release_expired();
            self.release_abandoned();
            let now_secs = match self.clock.now_secs() {
                Ok(now_secs) => {
                    self.clock_diverged = false;
                    now_secs
                }
                Err(error) => {
                    if !self.clock_diverged {
                        warn!(error = %error, "DHT clock diverged; failing new operations until it re-anchors");
                        self.clock_diverged = true;
                    }
                    if let DriverEvent::Command(cmd) = event {
                        // A mutation must not be stamped under an unstable clock;
                        // fail loudly but keep the driver alive to recover.
                        reject_driver_cmd(cmd, clock_error(error));
                        continue;
                    }
                    // In-flight work still resolves on the last ratcheted time.
                    self.clock.current_secs()
                }
            };
            match event {
                DriverEvent::Shutdown | DriverEvent::Closed => break DhtIoError::Shutdown,
                DriverEvent::Command(cmd) => {
                    self.process_input(DhtInput::Clock { now_secs });
                    self.handle_driver_cmd(cmd);
                }
                DriverEvent::Inbound(inbound) => self.handle_inbound_stream(inbound),
                DriverEvent::Io(io) => {
                    self.process_input(DhtInput::Clock { now_secs });
                    self.handle_worker_io(io);
                }
                DriverEvent::Tick => {
                    self.now_tick = self.now_tick.saturating_add(1);
                    self.process_input(DhtInput::Tick {
                        now_tick: self.now_tick,
                        now_secs,
                    });
                }
                DriverEvent::Deadline => {}
            }
        };

        self.revision_task.abort();
        let _ = (&mut self.revision_task).await;
        self.fail_pending_callers(terminal_error.clone());
        let buffered = self.cmd_rx.len();
        for _ in 0..buffered {
            let Ok(cmd) = self.cmd_rx.try_recv() else {
                break;
            };
            reject_driver_cmd(cmd, terminal_error.clone());
        }
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
    fn process_op_input(&mut self, op_id: OpId, input: DhtInput) {
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
        fields(io = dht_io_kind(&io), op_id = ?io_op_id(&io), inbound_id = ?io_inbound_id(&io))
    )]
    fn process_io_input(&mut self, io: DhtIo) {
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
                self.process_op_input(
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
                options,
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
                    deadline_ms = duration_ms(options.deadline),
                );
                if let Some(trace_context) = trace_context.as_ref() {
                    let _ = span.set_parent(extract_trace_context(trace_context));
                }
                self.op_spans.insert(op_id, span);
                self.deadlines
                    .insert(op_id, TokioInstant::now() + options.deadline);
                self.process_op_input(
                    op_id,
                    DhtInput::Cmd(DhtCmd::Get {
                        op_id,
                        key,
                        realm_filter,
                        completion: options.completion,
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
                self.process_op_input(
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
                self.process_op_input(op_id, DhtInput::Cmd(DhtCmd::RoutingTableSize { op_id }));
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
                let span = self.span_for_request(&request);
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
        self.deadlines.remove(output_op_id(&output));
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
        self.deadlines = OpDeadlines::default();
        self.op_spans.clear();
    }

    /// Releases every operation whose wall-clock budget elapsed. Completed cache
    /// write-backs are already durable; only the live lookup is dropped.
    fn release_expired(&mut self) {
        for op_id in self.deadlines.expired(TokioInstant::now()) {
            debug!(op_id, outcome = "deadline", "releasing DHT operation");
            self.cancel_op(op_id, DhtIoError::Timeout);
        }
    }

    /// Releases reads whose caller stopped waiting, so a dropped request cannot
    /// leave a lookup consuming connection attempts. Only deadline-carrying
    /// reads qualify; an accepted put still finishes storing.
    fn release_abandoned(&mut self) {
        let abandoned: Vec<OpId> = self
            .pending_callers
            .iter()
            .filter(|(op_id, reply)| reply.is_closed() && self.deadlines.contains(**op_id))
            .map(|(op_id, _)| *op_id)
            .collect();
        for op_id in abandoned {
            debug!(op_id, outcome = "caller_gone", "releasing DHT operation");
            self.deadlines.remove(op_id);
            self.pending_callers.remove(&op_id);
            self.process_input(DhtInput::Cmd(DhtCmd::Cancel {
                op_id,
                error: DhtIoError::Timeout,
            }));
        }
    }

    #[tracing::instrument(name = "dht.driver.cancel_op", level = "debug", skip(self))]
    fn cancel_op(&mut self, op_id: OpId, error: DhtIoError) {
        self.deadlines.remove(op_id);
        self.process_input(DhtInput::Cmd(DhtCmd::Cancel {
            op_id,
            error: error.clone(),
        }));
        if let Some(reply) = self.pending_callers.remove(&op_id) {
            let _ = reply.send(Err(error));
            self.op_spans.remove(&op_id);
        }
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
            | DhtIo::StorageIterResult { op_id, .. }
            | DhtIo::StorageError { op_id, .. } => self.op_spans.get(op_id).cloned(),
            DhtIo::InboundRequest { inbound_id, .. }
            | DhtIo::InboundReadError { inbound_id, .. }
            | DhtIo::InboundDropped { inbound_id } => self.inbound_spans.get(inbound_id).cloned(),
            DhtIo::PeerSeen { .. } => None,
        }
    }

    fn span_for_request(&mut self, request: &DhtIoRequest) -> Option<Span> {
        match request {
            DhtIoRequest::RpcRequest { op_id, .. }
            | DhtIoRequest::StorageRead { op_id, .. }
            | DhtIoRequest::StorageRevision { op_id, .. }
            | DhtIoRequest::StorageMerge { op_id, .. }
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
        let (mut send, mut recv, peer) = inbound;
        if self.inbound_contexts.len() >= MAX_INBOUND_RPCS {
            warn!(peer = %peer, "Dropping inbound DHT stream: active RPC limit reached");
            let _ = send.finish();
            let _ = recv.stop(0u32.into());
            return;
        }
        let inbound_id = self.next_inbound_id;
        self.next_inbound_id = self.next_inbound_id.saturating_add(1);

        self.inbound_contexts.insert(inbound_id, Some(send));

        let io_tx = self.io_tx.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            match tokio::time::timeout(RPC_TIMEOUT, read_stream_request(&mut recv)).await {
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
        fields(io = dht_io_kind(&io), op_id = ?io_op_id(&io), inbound_id = ?io_inbound_id(&io))
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
                request = request_kind(&request),
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
                    request = request_kind(&request),
                    "Received inbound DHT RPC"
                );
            }
            self.process_io_input(DhtIo::InboundRequest {
                inbound_id,
                peer,
                request,
                trace_context,
            });
            return;
        }

        if let DhtIo::InboundReadError { inbound_id, error } = io {
            let maybe_send = self
                .inbound_contexts
                .get_mut(&inbound_id)
                .and_then(Option::take);
            let io_tx = self.io_tx.clone();
            let span = self.inbound_spans.get(&inbound_id).cloned();
            let future = async move {
                if let Some(mut send) = maybe_send
                    && let Err(error) = write_stream_response(
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
            self.process_io_input(DhtIo::InboundDropped { inbound_id });
            self.inbound_spans.remove(&inbound_id);
            return;
        }

        self.process_io_input(io);
    }

    #[tracing::instrument(
        name = "dht.driver.io_request",
        level = "debug",
        skip(self, request),
        fields(request = io_request_kind(&request), op_id = ?request_op_id(&request), inbound_id = ?request_inbound_id(&request))
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
            DhtIoRequest::StorageMerge {
                op_id,
                stage,
                key,
                entries,
            } => self.dispatch_storage_merge(op_id, stage, key, entries),
            DhtIoRequest::StorageIter {
                op_id,
                stage,
                index,
                limit,
            } => self.dispatch_storage_iter(op_id, stage, index, limit),
            DhtIoRequest::StoragePrune {
                op_id,
                stage,
                index,
                entry,
                now_secs,
            } => self.dispatch_storage_prune(op_id, stage, index, entry, now_secs),
        }
    }

    #[tracing::instrument(
        name = "dht.driver.rpc_request.dispatch",
        level = "debug",
        skip(self, request, trace_context),
        fields(op_id, phase = ?phase, peer = %peer, request = request_kind(&request))
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
            request = request_kind(&request),
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
            request = request_kind(&request),
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
        fields(inbound_id, response = response_kind(&response))
    )]
    fn dispatch_rpc_response(&mut self, inbound_id: InboundId, response: DhtResponse) {
        let maybe_send = self
            .inbound_contexts
            .get_mut(&inbound_id)
            .and_then(Option::take);
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                if let Some(mut send) = maybe_send
                    && let Err(error) = write_stream_response(&mut send, &response).await
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
        self.process_io_input(DhtIo::InboundDropped { inbound_id });
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

                match run_storage_io(
                    stage,
                    send_storage_effect(&storage, effect, op_id, stage, "read"),
                )
                .await
                {
                    Ok(Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                        let entries = match value {
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

    fn dispatch_storage_revision(&mut self, op_id: OpId, stage: StorageStage) {
        let request = RevisionRequest {
            op_id,
            stage,
            span: Span::current(),
        };
        let error = match self.revision_tx.try_send(request) {
            Ok(()) => return,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => DhtIoError::QueueFull,
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => DhtIoError::Shutdown,
        };
        self.process_io_input(DhtIo::StorageError {
            op_id,
            stage,
            error,
        });
    }

    #[tracing::instrument(
        name = "dht.driver.storage_merge.dispatch",
        level = "debug",
        skip(self, entries),
        fields(op_id, stage = ?stage, key = %key, entry_count = entries.len())
    )]
    fn dispatch_storage_merge(
        &self,
        op_id: OpId,
        stage: StorageStage,
        key: DhtKeyId,
        entries: Vec<StoredEntry>,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        let clock = self.clock.clone();
        tokio::spawn(
            async move {
                let mutation = StorageMutation::Merge { entries };
                match run_storage_io(
                    stage,
                    mutate_storage_checked(&storage, op_id, stage, key, &mutation, &clock),
                )
                .await
                {
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
        fields(op_id, stage = ?stage, ?index, key = %entry.key)
    )]
    fn dispatch_storage_prune(
        &self,
        op_id: OpId,
        stage: StorageStage,
        index: DeadlineIndex,
        entry: DeadlineEntry,
        now_secs: u64,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        let clock = self.clock.clone();
        tokio::spawn(
            async move {
                let mutation = StorageMutation::Prune {
                    index,
                    entry,
                    now_secs,
                };
                match mutate_storage_checked(&storage, op_id, stage, entry.key, &mutation, &clock)
                    .await
                {
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
        name = "dht.driver.storage_iter.dispatch",
        level = "debug",
        skip(self),
        fields(op_id, stage = ?stage, ?index, limit)
    )]
    fn dispatch_storage_iter(
        &self,
        op_id: OpId,
        stage: StorageStage,
        index: DeadlineIndex,
        limit: usize,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(
            async move {
                match iter_deadlines(&storage, op_id, stage, index, limit, None).await {
                    Ok(entries) => {
                        let _ = io_tx
                            .send(DhtIo::StorageIterResult {
                                op_id,
                                stage,
                                index,
                                entries,
                            })
                            .await;
                    }
                    Err(TransactionFailure::Failed(error)) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error,
                            })
                            .await;
                    }
                    Err(TransactionFailure::Conflict) => {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                op_id,
                                stage,
                                error: DhtIoError::storage("DHT deadline iteration conflicted"),
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
    Merge {
        entries: Vec<StoredEntry>,
    },
    Prune {
        index: DeadlineIndex,
        entry: DeadlineEntry,
        now_secs: u64,
    },
}

enum TransactionFailure {
    Conflict,
    Failed(DhtIoError),
}

struct ReclaimRow {
    index: DeadlineIndex,
    entry: DeadlineEntry,
    entries: Vec<StoredEntry>,
}

async fn start_storage_txn(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
) -> Result<TransactionOwner, TransactionFailure> {
    let started = Instant::now();
    match tokio::time::timeout(STORAGE_TIMEOUT, storage.start_transaction(false)).await {
        Ok(Ok(owner)) => Ok(owner),
        Ok(Err(error)) => Err(storage_transaction_error(error)),
        Err(error) => {
            warn!(
                event = "dht.storage.timeout",
                op_id,
                stage = ?stage,
                operation = "start",
                duration_ms = duration_ms(started.elapsed()),
                timeout_ms = duration_ms(STORAGE_TIMEOUT),
                error = %error,
                "DHT storage operation timed out"
            );
            Err(TransactionFailure::Failed(DhtIoError::Timeout))
        }
    }
}

async fn run_revision_allocator(
    storage: impl Into<Arc<dyn StoragePort>>,
    io_tx: IoSender,
    mut revision_rx: tokio::sync::mpsc::Receiver<RevisionRequest>,
    shutdown: CancellationToken,
) {
    let storage: Arc<dyn StoragePort> = storage.into();
    let mut revisions = None;
    loop {
        let request = tokio::select! {
            _ = shutdown.cancelled() => return,
            request = revision_rx.recv() => {
                let Some(request) = request else {
                    return;
                };
                request
            }
        };
        let RevisionRequest { op_id, stage, span } = request;
        handle_revision_request(&storage, &io_tx, &mut revisions, op_id, stage)
            .instrument(span)
            .await;
    }
}

async fn handle_revision_request(
    storage: &dyn StoragePort,
    io_tx: &IoSender,
    revisions: &mut Option<std::ops::RangeInclusive<u64>>,
    op_id: OpId,
    stage: StorageStage,
) {
    let revision = match revisions.as_mut().and_then(Iterator::next) {
        Some(revision) => Ok(revision),
        None => match reserve_revision_block(storage, op_id, stage).await {
            Ok(block) => {
                *revisions = Some(block);
                revisions.as_mut().and_then(Iterator::next).ok_or_else(|| {
                    DhtIoError::storage("DHT revision allocator returned an empty block")
                })
            }
            Err(error) => Err(error),
        },
    };
    let io = match revision {
        Ok(revision) => DhtIo::StorageRevisionResult {
            op_id,
            stage,
            revision,
        },
        Err(error) => DhtIo::StorageError {
            op_id,
            stage,
            error,
        },
    };
    let _ = io_tx.send(io).await;
}

async fn reserve_revision_block(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
) -> Result<std::ops::RangeInclusive<u64>, DhtIoError> {
    for _ in 0..STORAGE_MUTATION_RETRIES {
        let mut owner = match start_storage_txn(storage, op_id, stage).await {
            Ok(owner) => owner,
            Err(TransactionFailure::Conflict) => continue,
            Err(TransactionFailure::Failed(error)) => return Err(error),
        };
        match reserve_revision_txn(storage, op_id, stage, &mut owner).await {
            Ok(revisions) => return Ok(revisions),
            Err(TransactionFailure::Conflict) => continue,
            Err(TransactionFailure::Failed(error)) => return Err(error),
        }
    }
    Err(DhtIoError::storage(
        "DHT revision block reservation exceeded conflict retry limit",
    ))
}

async fn reserve_revision_txn(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    owner: &mut TransactionOwner,
) -> Result<std::ops::RangeInclusive<u64>, TransactionFailure> {
    let Some(txn_id) = owner.id() else {
        return Err(TransactionFailure::Failed(DhtIoError::storage(
            "DHT transaction owner missing transaction",
        )));
    };
    let read = Effect::Storage(StorageEffect::Read {
        key_space: DHT_META_KEYSPACE.to_string(),
        key: ByteView::from(DHT_REVISION_KEY),
        txn_id: Some(txn_id),
    });
    let current = match send_storage_effect(storage, read, op_id, stage, "revision_read").await {
        Ok(Event::Storage(StorageEvent::ReadResult { value, .. })) => match decode_counter(value) {
            Ok(current) => current,
            Err(error) => {
                abort_storage_txn(storage, op_id, stage, owner).await;
                return Err(error);
            }
        },
        Ok(Event::Storage(StorageEvent::Error { error })) => {
            abort_storage_txn(storage, op_id, stage, owner).await;
            return Err(storage_transaction_error(error));
        }
        Ok(other) => {
            abort_storage_txn(storage, op_id, stage, owner).await;
            return Err(TransactionFailure::Failed(DhtIoError::storage(format!(
                "unexpected revision read event: {other:?}"
            ))));
        }
        Err(error) => {
            abort_storage_txn(storage, op_id, stage, owner).await;
            return Err(TransactionFailure::Failed(error));
        }
    };
    let Some(first) = current.checked_add(1) else {
        abort_storage_txn(storage, op_id, stage, owner).await;
        return Err(TransactionFailure::Failed(DhtIoError::storage(
            "DHT revision counter exhausted",
        )));
    };
    let last = current.saturating_add(REVISION_BLOCK_SIZE);
    if let Err(error) = write_counter(
        storage,
        op_id,
        stage,
        txn_id,
        DHT_REVISION_KEY,
        last,
        "revision_write",
    )
    .await
    {
        abort_storage_txn(storage, op_id, stage, owner).await;
        return Err(error);
    }
    commit_storage_txn(storage, op_id, stage, owner).await?;
    Ok(first..=last)
}

#[cfg(test)]
async fn mutate_storage_entries(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    key: DhtKeyId,
    mutation: &StorageMutation,
) -> Result<(), DhtIoError> {
    mutate_storage_with(storage, op_id, stage, key, mutation, None).await
}

async fn mutate_storage_checked(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    key: DhtKeyId,
    mutation: &StorageMutation,
    clock: &DhtClock,
) -> Result<(), DhtIoError> {
    mutate_storage_with(storage, op_id, stage, key, mutation, Some(clock)).await
}

async fn mutate_storage_with(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    key: DhtKeyId,
    mutation: &StorageMutation,
    clock: Option<&DhtClock>,
) -> Result<(), DhtIoError> {
    for _ in 0..STORAGE_MUTATION_RETRIES {
        let mut owner = match start_storage_txn(storage, op_id, stage).await {
            Ok(owner) => owner,
            Err(TransactionFailure::Conflict) => continue,
            Err(TransactionFailure::Failed(error)) => return Err(error),
        };
        let Some(txn_id) = owner.id() else {
            return Err(DhtIoError::storage(
                "DHT transaction owner missing transaction",
            ));
        };

        match run_storage_txn(
            storage, op_id, stage, key, txn_id, &mut owner, mutation, clock,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(TransactionFailure::Conflict) => continue,
            Err(TransactionFailure::Failed(error)) => return Err(error),
        }
    }

    Err(DhtIoError::storage(
        "DHT storage mutation exceeded conflict retry limit",
    ))
}

async fn iter_deadlines(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    index: DeadlineIndex,
    limit: usize,
    txn_id: Option<TxnId>,
) -> Result<Vec<DeadlineEntry>, TransactionFailure> {
    let limit = if limit == 0 { CLEANUP_PAGE_SIZE } else { limit };
    loop {
        let effect = Effect::Storage(StorageEffect::Iter {
            key_space: DHT_META_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(index.prefix())),
            start: None,
            limit,
            txn_id,
        });
        let values = match send_storage_effect(storage, effect, op_id, stage, "deadline_iter").await
        {
            Ok(Event::Storage(StorageEvent::IterResult { values, .. })) => values,
            Ok(Event::Storage(StorageEvent::Error { error })) => {
                return Err(storage_transaction_error(error));
            }
            Ok(other) => {
                return Err(TransactionFailure::Failed(DhtIoError::storage(format!(
                    "unexpected deadline iteration event: {other:?}"
                ))));
            }
            Err(error) => return Err(TransactionFailure::Failed(error)),
        };
        let mut entries = Vec::with_capacity(values.len());
        let mut invalid = Vec::new();
        for (key, _) in values {
            if let Some(entry) = parse_deadline(index, &key) {
                entries.push(entry);
            } else {
                invalid.push((DHT_META_KEYSPACE.to_string(), key));
            }
        }
        if invalid.is_empty() {
            return Ok(entries);
        }

        warn!(
            index = ?index,
            key_count = invalid.len(),
            "Removing invalid DHT deadline index keys"
        );
        let effect = Effect::Storage(StorageEffect::BatchDelete {
            deletes: invalid,
            txn_id,
        });
        match send_storage_effect(storage, effect, op_id, stage, "deadline_repair").await {
            Ok(Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {}
            Ok(Event::Storage(StorageEvent::Error { error })) => {
                return Err(storage_transaction_error(error));
            }
            Ok(other) => {
                return Err(TransactionFailure::Failed(DhtIoError::storage(format!(
                    "unexpected deadline repair event: {other:?}"
                ))));
            }
            Err(error) => return Err(TransactionFailure::Failed(error)),
        }
        if !entries.is_empty() {
            return Ok(entries);
        }
    }
}

async fn delete_deadline(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
    index: DeadlineIndex,
    entry: DeadlineEntry,
) -> Result<(), TransactionFailure> {
    apply_batch_delete(
        storage,
        op_id,
        stage,
        txn_id,
        vec![(
            DHT_META_KEYSPACE.to_string(),
            ByteView::from(deadline_key(index, entry)),
        )],
    )
    .await
}

async fn find_reclaimable(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
    now_secs: u64,
) -> Result<Option<ReclaimRow>, TransactionFailure> {
    let mut remaining = CLEANUP_PAGE_SIZE;
    for index in [DeadlineIndex::Floor, DeadlineIndex::Active] {
        loop {
            if remaining == 0 {
                return Ok(None);
            }
            let Some(entry) = iter_deadlines(storage, op_id, stage, index, 1, Some(txn_id))
                .await?
                .into_iter()
                .next()
            else {
                break;
            };
            if entry.deadline > now_secs {
                break;
            }
            remaining -= 1;

            let read = Effect::Storage(StorageEffect::Read {
                key_space: DHT_KEYSPACE.to_string(),
                key: ByteView::from(entry.key.as_bytes().as_slice()),
                txn_id: Some(txn_id),
            });
            let value = match send_storage_effect(storage, read, op_id, stage, "reclaim_read").await
            {
                Ok(Event::Storage(StorageEvent::ReadResult { value, .. })) => value,
                Ok(Event::Storage(StorageEvent::Error { error })) => {
                    return Err(storage_transaction_error(error));
                }
                Ok(other) => {
                    return Err(TransactionFailure::Failed(DhtIoError::storage(format!(
                        "unexpected reclaim read event: {other:?}"
                    ))));
                }
                Err(error) => return Err(TransactionFailure::Failed(error)),
            };
            let Some(value) = value else {
                delete_deadline(storage, op_id, stage, txn_id, index, entry).await?;
                continue;
            };
            let entries = match decode_entries(&value) {
                Ok(entries) => entries,
                Err(error) => {
                    warn!(key = %entry.key, error = %error, "Preserving corrupt DHT row during capacity reclaim");
                    delete_deadline(storage, op_id, stage, txn_id, index, entry).await?;
                    continue;
                }
            };
            if let Err(error) = validate_entries(&entry.key, &entries, now_secs) {
                warn!(key = %entry.key, error = %error, "Preserving invalid DHT row during capacity reclaim");
                delete_deadline(storage, op_id, stage, txn_id, index, entry).await?;
                continue;
            }
            let Some((current_index, current_entry)) = row_deadline(entry.key, &entries, now_secs)
            else {
                return Ok(Some(ReclaimRow {
                    index,
                    entry,
                    entries,
                }));
            };
            if current_index == DeadlineIndex::Floor && current_entry.deadline <= now_secs {
                return Ok(Some(ReclaimRow {
                    index,
                    entry,
                    entries,
                }));
            }
            delete_deadline(storage, op_id, stage, txn_id, index, entry).await?;
            apply_batch_write(
                storage,
                op_id,
                stage,
                txn_id,
                vec![(
                    DHT_META_KEYSPACE.to_string(),
                    ByteView::from(deadline_key(current_index, current_entry)),
                    ByteView::from(Vec::new()),
                )],
            )
            .await?;
        }
    }
    Ok(None)
}

async fn reserve_key_slot(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
    index: DeadlineIndex,
    now_secs: u64,
) -> Result<Option<ReclaimRow>, TransactionFailure> {
    let count = read_key_count(storage, op_id, stage, txn_id).await?;
    if count < MAX_STORED_KEYS {
        write_counter(
            storage,
            op_id,
            stage,
            txn_id,
            KEY_COUNT_KEY,
            count + 1,
            "count_write",
        )
        .await?;
        return Ok(None);
    }
    if index == DeadlineIndex::Floor {
        return Err(TransactionFailure::Failed(DhtIoError::StorageFull));
    }

    find_reclaimable(storage, op_id, stage, txn_id, now_secs)
        .await?
        .ok_or(TransactionFailure::Failed(DhtIoError::StorageFull))
        .map(Some)
}

#[allow(clippy::too_many_arguments)]
async fn run_storage_txn(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    key: DhtKeyId,
    txn_id: TxnId,
    owner: &mut TransactionOwner,
    mutation: &StorageMutation,
    clock: Option<&DhtClock>,
) -> Result<(), TransactionFailure> {
    let read = Effect::Storage(StorageEffect::Read {
        key_space: DHT_KEYSPACE.to_string(),
        key: ByteView::from(key.as_bytes().as_slice()),
        txn_id: Some(txn_id),
    });
    let value = match send_storage_effect(storage, read, op_id, stage, "read").await {
        Ok(Event::Storage(StorageEvent::ReadResult { value, .. })) => value,
        Ok(Event::Storage(StorageEvent::Error { error })) => {
            abort_storage_txn(storage, op_id, stage, owner).await;
            return Err(storage_transaction_error(error));
        }
        Ok(other) => {
            abort_storage_txn(storage, op_id, stage, owner).await;
            return Err(TransactionFailure::Failed(DhtIoError::storage(format!(
                "unexpected transactional read event: {other:?}"
            ))));
        }
        Err(error) => {
            abort_storage_txn(storage, op_id, stage, owner).await;
            return Err(TransactionFailure::Failed(error));
        }
    };

    let had_value = value.is_some();
    let (entries, mut preserve_row) = match value {
        Some(value) => match decode_entries(&value) {
            Ok(entries) => (entries, false),
            Err(error) if matches!(mutation, StorageMutation::Prune { .. }) => {
                warn!(key = %key, error = %error, "Preserving corrupt DHT row during cleanup");
                (Vec::new(), true)
            }
            Err(error) => {
                abort_storage_txn(storage, op_id, stage, owner).await;
                return Err(TransactionFailure::Failed(DhtIoError::storage(error)));
            }
        },
        None => (Vec::new(), false),
    };
    let original_entries = entries;
    let now_secs = match clock {
        Some(clock) => match clock.now_secs() {
            Ok(now_secs) => now_secs,
            Err(error) => {
                abort_storage_txn(storage, op_id, stage, owner).await;
                return Err(TransactionFailure::Failed(clock_error(error)));
            }
        },
        None => match mutation {
            StorageMutation::Merge { .. } => unix_timestamp_secs(),
            StorageMutation::Prune { now_secs, .. } => *now_secs,
        },
    };
    let entries = match mutation {
        StorageMutation::Merge { entries, .. } => {
            let merged = if stage == StorageStage::GetRemoteMerge {
                merge_fresh_entries(&key, original_entries.clone(), entries, now_secs)
            } else {
                merge_storage_entries(&key, original_entries.clone(), entries, now_secs)
            };
            match merged {
                Ok(entries) => entries,
                Err(MergeError::Stale) => {
                    abort_storage_txn(storage, op_id, stage, owner).await;
                    return Err(TransactionFailure::Failed(DhtIoError::invalid_request(
                        "record version is stale or conflicting",
                    )));
                }
                Err(MergeError::Capacity) => {
                    abort_storage_txn(storage, op_id, stage, owner).await;
                    return Err(TransactionFailure::Failed(DhtIoError::StorageFull));
                }
                Err(MergeError::Encoding) => {
                    abort_storage_txn(storage, op_id, stage, owner).await;
                    return Err(TransactionFailure::Failed(DhtIoError::storage(
                        "serialize DHT entries failed",
                    )));
                }
                Err(MergeError::Invalid) => {
                    abort_storage_txn(storage, op_id, stage, owner).await;
                    return Err(TransactionFailure::Failed(DhtIoError::storage(
                        "stored DHT record set is invalid",
                    )));
                }
            }
        }
        StorageMutation::Prune { .. } => {
            if let Err(error) = validate_entries(&key, &original_entries, now_secs) {
                warn!(key = %key, error = %error, "Preserving invalid DHT row during cleanup");
                preserve_row = true;
                Vec::new()
            } else {
                retained_entries(original_entries.clone(), now_secs)
            }
        }
    };

    if matches!(mutation, StorageMutation::Merge { .. })
        && had_value
        && !entries.is_empty()
        && entries == original_entries
    {
        abort_storage_txn(storage, op_id, stage, owner).await;
        return Ok(());
    }

    let mut delete_keys = Vec::with_capacity(3);
    if let StorageMutation::Prune { index, entry, .. } = mutation {
        delete_keys.push(deadline_key(*index, *entry));
    }
    if had_value {
        if let Some(entry) = active_deadline(key, &original_entries) {
            delete_keys.push(deadline_key(DeadlineIndex::Active, entry));
        }
        if let Some(entry) = floor_deadline(key, &original_entries) {
            delete_keys.push(deadline_key(DeadlineIndex::Floor, entry));
        }
    }
    let mut writes = Vec::with_capacity(2);
    let deadline = if entries.is_empty() {
        None
    } else {
        let Some((index, entry)) = row_deadline(key, &entries, now_secs) else {
            abort_storage_txn(storage, op_id, stage, owner).await;
            return Err(TransactionFailure::Failed(DhtIoError::storage(
                "DHT row has no retention deadline",
            )));
        };
        Some((index, entry))
    };
    if let Some((index, entry)) = deadline {
        writes.push((
            DHT_META_KEYSPACE.to_string(),
            ByteView::from(deadline_key(index, entry)),
            ByteView::from(Vec::new()),
        ));
        if !had_value || entries != original_entries {
            let bytes = match encode_entries(&entries) {
                Ok(bytes) => bytes,
                Err(error) => {
                    abort_storage_txn(storage, op_id, stage, owner).await;
                    return Err(TransactionFailure::Failed(DhtIoError::storage(error)));
                }
            };
            writes.push((
                DHT_KEYSPACE.to_string(),
                ByteView::from(key.as_bytes().as_slice()),
                ByteView::from(bytes),
            ));
        }
    }

    let reclaimed = if let (false, Some((index, _))) = (had_value, deadline) {
        match reserve_key_slot(storage, op_id, stage, txn_id, index, now_secs).await {
            Ok(reclaimed) => reclaimed,
            Err(error) => {
                abort_storage_txn(storage, op_id, stage, owner).await;
                return Err(error);
            }
        }
    } else {
        None
    };
    if had_value
        && entries.is_empty()
        && !preserve_row
        && let Err(error) = decrement_key_count(storage, op_id, stage, txn_id).await
    {
        abort_storage_txn(storage, op_id, stage, owner).await;
        return Err(error);
    }

    if let Some(victim) = &reclaimed {
        delete_keys.push(deadline_key(victim.index, victim.entry));
        if let Some(entry) = active_deadline(victim.entry.key, &victim.entries) {
            delete_keys.push(deadline_key(DeadlineIndex::Active, entry));
        }
        if let Some(entry) = floor_deadline(victim.entry.key, &victim.entries) {
            delete_keys.push(deadline_key(DeadlineIndex::Floor, entry));
        }
    }
    delete_keys.sort_unstable();
    delete_keys.dedup();

    let mut deletes = delete_keys
        .into_iter()
        .map(|key| (DHT_META_KEYSPACE.to_string(), ByteView::from(key)))
        .collect::<Vec<_>>();
    if had_value && entries.is_empty() && !preserve_row {
        deletes.push((
            DHT_KEYSPACE.to_string(),
            ByteView::from(key.as_bytes().as_slice()),
        ));
    }
    if let Some(victim) = reclaimed {
        deletes.push((
            DHT_KEYSPACE.to_string(),
            ByteView::from(victim.entry.key.as_bytes().as_slice()),
        ));
    }

    if let Err(error) = apply_batch_delete(storage, op_id, stage, txn_id, deletes).await {
        abort_storage_txn(storage, op_id, stage, owner).await;
        return Err(error);
    }
    if let Err(error) = apply_batch_write(storage, op_id, stage, txn_id, writes).await {
        abort_storage_txn(storage, op_id, stage, owner).await;
        return Err(error);
    }

    commit_storage_txn(storage, op_id, stage, owner).await
}

async fn apply_batch_delete(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
    deletes: Vec<(String, ByteView)>,
) -> Result<(), TransactionFailure> {
    if deletes.is_empty() {
        return Ok(());
    }
    let effect = Effect::Storage(StorageEffect::BatchDelete {
        deletes,
        txn_id: Some(txn_id),
    });
    match send_storage_effect(storage, effect, op_id, stage, "index_delete").await {
        Ok(Event::Storage(StorageEvent::BatchDeleteResult { .. })) => Ok(()),
        Ok(Event::Storage(StorageEvent::Error { error })) => Err(storage_transaction_error(error)),
        Ok(other) => Err(TransactionFailure::Failed(DhtIoError::storage(format!(
            "unexpected DHT batch-delete event: {other:?}"
        )))),
        Err(error) => Err(TransactionFailure::Failed(error)),
    }
}

async fn apply_batch_write(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
    writes: Vec<(String, ByteView, ByteView)>,
) -> Result<(), TransactionFailure> {
    if writes.is_empty() {
        return Ok(());
    }
    let effect = Effect::Storage(StorageEffect::BatchWrite {
        writes,
        txn_id: Some(txn_id),
    });
    match send_storage_effect(storage, effect, op_id, stage, "index_write").await {
        Ok(Event::Storage(StorageEvent::BatchWriteResult { .. })) => Ok(()),
        Ok(Event::Storage(StorageEvent::Error { error })) => Err(storage_transaction_error(error)),
        Ok(other) => Err(TransactionFailure::Failed(DhtIoError::storage(format!(
            "unexpected DHT batch-write event: {other:?}"
        )))),
        Err(error) => Err(TransactionFailure::Failed(error)),
    }
}

fn merge_storage_entries(
    key: &DhtKeyId,
    original: Vec<StoredEntry>,
    entries: &[StoredEntry],
    now_secs: u64,
) -> Result<Vec<StoredEntry>, MergeError> {
    entries.iter().try_fold(original, |current, entry| {
        merge_entry(key, current, entry.clone(), now_secs)
    })
}

fn merge_fresh_entries(
    key: &DhtKeyId,
    original: Vec<StoredEntry>,
    entries: &[StoredEntry],
    now_secs: u64,
) -> Result<Vec<StoredEntry>, MergeError> {
    let mut current = original;
    for entry in entries {
        match merge_entry(key, current.clone(), entry.clone(), now_secs) {
            Ok(merged) => current = merged,
            // Read-repair drops an entry that lost a concurrent revision race rather
            // than failing the whole write-back; the winning entries still persist.
            Err(MergeError::Stale) => {}
            Err(other) => return Err(other),
        }
    }
    Ok(current)
}

async fn read_key_count(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
) -> Result<u64, TransactionFailure> {
    let read = Effect::Storage(StorageEffect::Read {
        key_space: DHT_META_KEYSPACE.to_string(),
        key: ByteView::from(KEY_COUNT_KEY),
        txn_id: Some(txn_id),
    });
    match send_storage_effect(storage, read, op_id, stage, "count_read").await {
        Ok(Event::Storage(StorageEvent::ReadResult { value, .. })) => decode_counter(value),
        Ok(Event::Storage(StorageEvent::Error { error })) => Err(storage_transaction_error(error)),
        Ok(other) => Err(TransactionFailure::Failed(DhtIoError::storage(format!(
            "unexpected DHT key-count read event: {other:?}"
        )))),
        Err(error) => Err(TransactionFailure::Failed(error)),
    }
}

async fn decrement_key_count(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    txn_id: TxnId,
) -> Result<(), TransactionFailure> {
    let count = read_key_count(storage, op_id, stage, txn_id).await?;
    let next = count.checked_sub(1).ok_or_else(|| {
        TransactionFailure::Failed(DhtIoError::storage("DHT key-count metadata is invalid"))
    })?;
    write_counter(
        storage,
        op_id,
        stage,
        txn_id,
        KEY_COUNT_KEY,
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
    storage: &dyn StoragePort,
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
    match send_storage_effect(storage, write, op_id, stage, operation).await {
        Ok(Event::Storage(StorageEvent::WriteResult { .. })) => Ok(()),
        Ok(Event::Storage(StorageEvent::Error { error })) => Err(storage_transaction_error(error)),
        Ok(other) => Err(TransactionFailure::Failed(DhtIoError::storage(format!(
            "unexpected DHT metadata write event: {other:?}"
        )))),
        Err(error) => Err(TransactionFailure::Failed(error)),
    }
}

async fn commit_storage_txn(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    owner: &mut TransactionOwner,
) -> Result<(), TransactionFailure> {
    let Some(txn_id) = owner.id() else {
        return Err(TransactionFailure::Failed(DhtIoError::storage(
            "DHT transaction owner missing transaction",
        )));
    };
    let commit = Effect::Storage(StorageEffect::CommitTransaction { txn_id });
    match send_storage_effect(storage, commit, op_id, stage, "commit").await {
        Ok(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: committed, ..
        })) if committed == txn_id => {
            owner.finish();
            Ok(())
        }
        Ok(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        })) => {
            owner.finish();
            Err(TransactionFailure::Conflict)
        }
        Ok(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionNotFound,
        })) => {
            owner.finish();
            Err(TransactionFailure::Failed(DhtIoError::storage(
                StorageError::TransactionNotFound,
            )))
        }
        Ok(Event::Storage(StorageEvent::Error {
            error: StorageError::QueueFull,
        })) => Err(TransactionFailure::Failed(DhtIoError::storage(
            StorageError::QueueFull,
        ))),
        Ok(Event::Storage(StorageEvent::Error { error })) => {
            owner.unknown();
            Err(TransactionFailure::Failed(DhtIoError::storage(error)))
        }
        Ok(other) => {
            owner.unknown();
            Err(TransactionFailure::Failed(DhtIoError::storage(format!(
                "unexpected transaction commit event: {other:?}"
            ))))
        }
        Err(error) => {
            owner.unknown();
            Err(TransactionFailure::Failed(error))
        }
    }
}

async fn abort_storage_txn(
    storage: &dyn StoragePort,
    op_id: OpId,
    stage: StorageStage,
    owner: &mut TransactionOwner,
) {
    let Some(txn_id) = owner.id() else {
        return;
    };
    let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id });
    match send_storage_effect(storage, abort, op_id, stage, "abort").await {
        Ok(Event::Storage(StorageEvent::TransactionAborted { txn_id: aborted }))
            if aborted == txn_id =>
        {
            owner.finish()
        }
        Ok(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionNotFound,
        })) => owner.finish(),
        _ => {}
    }
}

fn storage_transaction_error(error: StorageError) -> TransactionFailure {
    if error == StorageError::TransactionConflict {
        TransactionFailure::Conflict
    } else {
        TransactionFailure::Failed(DhtIoError::storage(error))
    }
}

async fn send_storage_effect(
    storage: &dyn StoragePort,
    effect: Effect,
    op_id: OpId,
    stage: StorageStage,
    operation: &'static str,
) -> Result<Event, DhtIoError> {
    let Effect::Storage(effect) = effect else {
        return Ok(Event::Storage(StorageEvent::Error {
            error: StorageError::InvalidEffect,
        }));
    };
    let started = Instant::now();
    match tokio::time::timeout(STORAGE_TIMEOUT, storage.send_storage_effect(effect)).await {
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

async fn run_storage_io<T, F>(stage: StorageStage, future: F) -> Result<T, DhtIoError>
where
    F: std::future::Future<Output = Result<T, DhtIoError>>,
{
    if matches!(
        stage,
        StorageStage::InboundGetRead | StorageStage::InboundPutMerge
    ) {
        tokio::time::timeout(INBOUND_STORAGE_BUDGET, future)
            .await
            .map_err(DhtIoError::from)?
    } else {
        future.await
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

fn dht_effect_kind(effect: &DhtEffect) -> &'static str {
    match effect {
        DhtEffect::IoRequest(request) => io_request_kind(request),
        DhtEffect::Output(output) => dht_output_kind(output),
    }
}

fn dht_output_kind(output: &DhtOutput) -> &'static str {
    match output {
        DhtOutput::Completed { .. } => "completed",
        DhtOutput::Failed { .. } => "failed",
    }
}

fn io_request_kind(request: &DhtIoRequest) -> &'static str {
    match request {
        DhtIoRequest::RpcRequest { .. } => "rpc_request",
        DhtIoRequest::RpcResponse { .. } => "rpc_response",
        DhtIoRequest::DropInbound { .. } => "drop_inbound",
        DhtIoRequest::StorageRead { .. } => "storage_read",
        DhtIoRequest::StorageRevision { .. } => "storage_revision",
        DhtIoRequest::StorageMerge { .. } => "storage_merge",
        DhtIoRequest::StorageIter { .. } => "storage_iter",
        DhtIoRequest::StoragePrune { .. } => "storage_prune",
    }
}

fn request_op_id(request: &DhtIoRequest) -> Option<OpId> {
    match request {
        DhtIoRequest::RpcRequest { op_id, .. }
        | DhtIoRequest::StorageRead { op_id, .. }
        | DhtIoRequest::StorageRevision { op_id, .. }
        | DhtIoRequest::StorageMerge { op_id, .. }
        | DhtIoRequest::StorageIter { op_id, .. }
        | DhtIoRequest::StoragePrune { op_id, .. } => Some(*op_id),
        DhtIoRequest::RpcResponse { .. } | DhtIoRequest::DropInbound { .. } => None,
    }
}

fn request_inbound_id(request: &DhtIoRequest) -> Option<InboundId> {
    match request {
        DhtIoRequest::RpcResponse { inbound_id, .. } | DhtIoRequest::DropInbound { inbound_id } => {
            Some(*inbound_id)
        }
        DhtIoRequest::RpcRequest { .. }
        | DhtIoRequest::StorageRead { .. }
        | DhtIoRequest::StorageRevision { .. }
        | DhtIoRequest::StorageMerge { .. }
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
        | DhtIoRequest::StorageMerge { stage, .. }
        | DhtIoRequest::StorageIter { stage, .. }
        | DhtIoRequest::StoragePrune { stage, .. } => match stage {
            StorageStage::PutRevision
            | StorageStage::PutLocalRead
            | StorageStage::PutLocalMerge => "put",
            StorageStage::GetLocalRead | StorageStage::GetRemoteMerge => "get",
            StorageStage::InboundGetRead => "inbound_get",
            StorageStage::InboundPutRead | StorageStage::InboundPutMerge => "inbound_put",
            StorageStage::CleanupActiveIter
            | StorageStage::CleanupFloorIter
            | StorageStage::CleanupActivePrune
            | StorageStage::CleanupFloorPrune => "cleanup",
        },
        DhtIoRequest::RpcResponse { .. } | DhtIoRequest::DropInbound { .. } => "inbound",
    }
}

fn encode_request_frame(
    request: &DhtRequest,
    trace_context: Option<DistributedTraceContext>,
) -> Result<Vec<u8>, DhtIoError> {
    let bytes = encode_traced_request(request, trace_context)?;
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err(DhtIoError::invalid_request("request too large"));
    }
    Ok(bytes)
}

#[tracing::instrument(
    name = "dht.rpc.request.io",
    level = "debug",
    skip(connection_pool, request, trace_context),
    fields(op_id, phase = ?phase, peer = %peer, request = request_kind(&request))
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
            warn_iroh_phase("dht.rpc", "connect", elapsed);
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
            debug!(
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
            warn_iroh_phase("dht.rpc", "open_bi", elapsed);
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
            debug!(
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
            debug!(
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
            debug!(
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
            debug!(
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
    warn_iroh_phase("dht.rpc", "write_request", elapsed);
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
            warn_iroh_phase("dht.rpc", "wait_response_header", elapsed);
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
            debug!(
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
            warn_iroh_phase("dht.rpc", "read_response_body", elapsed);
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
            debug!(
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
    warn_iroh_request("dht.rpc", total_elapsed);
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

    let response = decode_response(&response_bytes)?;
    validate_response(&response)?;
    Ok(response)
}

fn validate_response(response: &DhtResponse) -> Result<(), DhtIoError> {
    match response {
        DhtResponse::Nodes { nodes } if nodes.len() > K => {
            Err(DhtIoError::invalid_response("too many DHT nodes"))
        }
        DhtResponse::Value {
            entries,
            closer_nodes,
        } if entries.len() > ENTRIES_PER_KEY || closer_nodes.len() > K => {
            Err(DhtIoError::invalid_response("too many DHT values or nodes"))
        }
        _ => Ok(()),
    }
}

async fn read_stream_request(
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

    Ok(decode_traced_request(&req_bytes)?)
}

async fn write_stream_response(
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
            debug!(
                event = "dht.rpc.write_response_failed",
                duration_ms = duration_ms(started.elapsed()),
                error = %error,
                "Failed to write inbound DHT RPC response"
            );
            return Err(error);
        }
        Err(error) => {
            debug!(
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
#[path = "driver_tests.rs"]
mod tests;
