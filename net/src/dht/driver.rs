use std::collections::HashMap;
use std::time::Duration;

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
use tracing::{Instrument, info_span, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::constants::{
    DRIVER_IO_EVENT_CAPACITY, DRIVER_TICK_INTERVAL, MAX_MESSAGE_SIZE, RPC_TIMEOUT,
};
use super::protocol::{
    DhtCmd, DhtEffect, DhtInput, DhtIo, DhtIoError, DhtIoRequest, DhtOutput, DhtOutputValue,
    InboundId, OpId, RpcPhase, StorageStage,
};
use super::rpc::{
    DHT_ALPN, DhtRequest, DhtResponse, ErrorCode, decode_request_with_trace_context,
    decode_response, encode_request_with_trace_context, encode_response,
};
use super::state::DhtStateMachine;
use super::storage::{CLEANUP_PAGE_SIZE, StoredEntry, decode_entries, encode_entries};
use crate::telemetry::extract_trace_context;

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
    next_op_id: OpId,
    inbound_contexts: HashMap<InboundId, (Connection, SendStream)>,
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
            next_op_id: 1,
            inbound_contexts: HashMap::new(),
            next_inbound_id: 1,
        }
    }

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

    fn process_input(&mut self, input: DhtInput) {
        let effects = self.state.step(input);
        for effect in effects {
            self.handle_effect(effect);
        }
    }

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
                self.process_input(DhtInput::Cmd(DhtCmd::Put {
                    op_id,
                    key,
                    realm_id,
                    value,
                    ttl,
                    trace_context,
                }));
            }
            DriverCmd::Get {
                key,
                realm_filter,
                trace_context,
                reply,
            } => {
                let op_id = self.register_caller(reply);
                self.process_input(DhtInput::Cmd(DhtCmd::Get {
                    op_id,
                    key,
                    realm_filter,
                    trace_context,
                }));
            }
            DriverCmd::Bootstrap {
                nodes,
                trace_context,
                reply,
            } => {
                let op_id = self.register_caller(reply);
                self.process_input(DhtInput::Cmd(DhtCmd::Bootstrap {
                    op_id,
                    nodes,
                    trace_context,
                }));
            }
            DriverCmd::RoutingTableSize { reply } => {
                let op_id = self.register_caller(reply);
                self.process_input(DhtInput::Cmd(DhtCmd::RoutingTableSize { op_id }));
            }
            DriverCmd::AddPeer { node_id } => {
                self.process_input(DhtInput::Cmd(DhtCmd::AddPeer { node_id }));
            }
        }
    }

    fn register_caller(&mut self, reply: oneshot::Sender<CallerOutcome>) -> OpId {
        let op_id = self.next_op_id;
        self.next_op_id = self.next_op_id.saturating_add(1);
        self.pending_callers.insert(op_id, reply);
        op_id
    }

    fn handle_effect(&mut self, effect: DhtEffect) {
        match effect {
            DhtEffect::IoRequest(request) => self.dispatch_io_request(*request),
            DhtEffect::Output(output) => self.handle_output(output),
        }
    }

    fn handle_output(&mut self, output: DhtOutput) {
        match output {
            DhtOutput::Completed { op_id, result } => {
                if let Some(reply) = self.pending_callers.remove(&op_id) {
                    let _ = reply.send(Ok(result));
                }
            }
            DhtOutput::Failed { op_id, error } => {
                if let Some(reply) = self.pending_callers.remove(&op_id) {
                    let _ = reply.send(Err(error));
                }
            }
        }
    }

    fn fail_pending_callers(&mut self, error: DhtIoError) {
        let waiting: Vec<_> = self.pending_callers.drain().collect();
        for (_, sender) in waiting {
            let _ = sender.send(Err(error.clone()));
        }
    }

    fn handle_inbound_stream(&mut self, inbound: InboundDhtStream) {
        let (conn, send, mut recv, peer) = inbound;
        let inbound_id = self.next_inbound_id;
        self.next_inbound_id = self.next_inbound_id.saturating_add(1);

        self.inbound_contexts.insert(inbound_id, (conn, send));
        self.process_input(DhtInput::Io(DhtIo::PeerSeen { peer }));

        let io_tx = self.io_tx.clone();
        tokio::spawn(async move {
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
                    let _ = io_tx
                        .send(DhtIo::InboundReadError { inbound_id, error })
                        .await;
                }
                Err(error) => {
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
            let _guard = span.enter();
            trace!(
                event = "dht.rpc.received",
                peer = %peer,
                request = ?request,
                "Received inbound DHT RPC"
            );
            self.process_input(DhtInput::Io(DhtIo::InboundRequest {
                inbound_id,
                peer,
                request,
                trace_context,
            }));
            return;
        }

        if let DhtIo::InboundReadError { inbound_id, error } = io {
            let maybe_send = self.inbound_contexts.remove(&inbound_id);
            let io_tx = self.io_tx.clone();
            tokio::spawn(async move {
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
            });
            return;
        }

        if let DhtIo::InboundDropped { inbound_id } = io {
            self.inbound_contexts.remove(&inbound_id);
            self.process_input(DhtInput::Io(DhtIo::InboundDropped { inbound_id }));
            return;
        }

        self.process_input(DhtInput::Io(io));
    }

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
        let span = info_span!(
            "dht.rpc.request",
            "otel.kind" = "client",
            op_id,
            phase = ?phase,
            peer = %peer,
            request = ?request,
        );
        if let Some(trace_context) = trace_context.as_ref() {
            let _ = span.set_parent(extract_trace_context(trace_context));
        }
        tokio::spawn(
            async move {
                match rpc_request(endpoint.clone(), peer, request, trace_context).await {
                    Ok(response) => {
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

    fn dispatch_rpc_response(&mut self, inbound_id: InboundId, response: DhtResponse) {
        let maybe_send = self.inbound_contexts.remove(&inbound_id);
        let io_tx = self.io_tx.clone();
        tokio::spawn(async move {
            if let Some((_conn, mut send)) = maybe_send {
                let _ = write_response_to_stream(&mut send, &response).await;
            }
            let _ = io_tx.send(DhtIo::InboundDropped { inbound_id }).await;
        });
    }

    fn dispatch_drop_inbound(&mut self, inbound_id: InboundId) {
        self.inbound_contexts.remove(&inbound_id);
        self.process_input(DhtInput::Io(DhtIo::InboundDropped { inbound_id }));
    }

    fn dispatch_storage_read(
        &self,
        op_id: OpId,
        stage: StorageStage,
        key: DhtKeyId,
        realm_filter: Option<RealmId>,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(async move {
            let effect = Effect::Storage(StorageEffect::Read {
                key_space: DHT_KEYSPACE.to_string(),
                key: ByteView::from(key.as_bytes().as_slice()),
                txn_id: None,
            });

            match storage.send_effect(effect).await {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let mut entries = value.map(|data| decode_entries(&data)).unwrap_or_default();
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
                Event::Storage(StorageEvent::Error { error }) => {
                    let _ = io_tx
                        .send(DhtIo::StorageError {
                            op_id,
                            stage,
                            error: DhtIoError::storage(error),
                        })
                        .await;
                }
                other => {
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
            }
        });
    }

    fn dispatch_storage_write(
        &self,
        op_id: OpId,
        stage: StorageStage,
        key: DhtKeyId,
        entries: Vec<StoredEntry>,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(async move {
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

            match storage.send_effect(effect).await {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let _ = io_tx.send(DhtIo::StorageWriteResult { op_id, stage }).await;
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    let _ = io_tx
                        .send(DhtIo::StorageError {
                            op_id,
                            stage,
                            error: DhtIoError::storage(error),
                        })
                        .await;
                }
                other => {
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
            }
        });
    }

    fn dispatch_storage_delete(&self, op_id: OpId, stage: StorageStage, key: DhtKeyId) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(async move {
            let effect = Effect::Storage(StorageEffect::Delete {
                key_space: DHT_KEYSPACE.to_string(),
                key: ByteView::from(key.as_bytes().as_slice()),
                txn_id: None,
            });

            match storage.send_effect(effect).await {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    let _ = io_tx
                        .send(DhtIo::StorageDeleteResult { op_id, stage })
                        .await;
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    let _ = io_tx
                        .send(DhtIo::StorageError {
                            op_id,
                            stage,
                            error: DhtIoError::storage(error),
                        })
                        .await;
                }
                other => {
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
            }
        });
    }

    fn dispatch_storage_iter(
        &self,
        op_id: OpId,
        stage: StorageStage,
        start_after: Option<Vec<u8>>,
        limit: usize,
    ) {
        let storage = self.storage.clone();
        let io_tx = self.io_tx.clone();
        tokio::spawn(async move {
            let effect = Effect::Storage(StorageEffect::Iter {
                key_space: DHT_KEYSPACE.to_string(),
                prefix: None,
                start_after: start_after.map(ByteView::from),
                limit: if limit == 0 { CLEANUP_PAGE_SIZE } else { limit },
                txn_id: None,
            });

            match storage.send_effect(effect).await {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
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
                Event::Storage(StorageEvent::Error { error }) => {
                    let _ = io_tx
                        .send(DhtIo::StorageError {
                            op_id,
                            stage,
                            error: DhtIoError::storage(error),
                        })
                        .await;
                }
                other => {
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
            }
        });
    }
}

async fn rpc_request(
    endpoint: Endpoint,
    peer: NodeId,
    request: DhtRequest,
    trace_context: Option<DistributedTraceContext>,
) -> Result<DhtResponse, DhtIoError> {
    let conn = tokio::time::timeout(RPC_TIMEOUT, endpoint.connect(peer, DHT_ALPN))
        .await?
        .map_err(DhtIoError::network)?;

    let (mut send, mut recv) = conn.open_bi().await.map_err(DhtIoError::network)?;

    let request_bytes = encode_request_with_trace_context(&request, trace_context)?;
    let len = (request_bytes.len() as u32).to_be_bytes();
    send.write_all(&len).await.map_err(DhtIoError::network)?;
    send.write_all(&request_bytes)
        .await
        .map_err(DhtIoError::network)?;
    send.finish().map_err(DhtIoError::network)?;

    let mut len_buf = [0u8; 4];
    tokio::time::timeout(RPC_TIMEOUT, recv.read_exact(&mut len_buf))
        .await?
        .map_err(DhtIoError::network)?;

    let response_len = u32::from_be_bytes(len_buf) as usize;
    if response_len > MAX_MESSAGE_SIZE {
        return Err(DhtIoError::invalid_response("response too large"));
    }

    let mut response_bytes = vec![0u8; response_len];
    tokio::time::timeout(RPC_TIMEOUT, recv.read_exact(&mut response_bytes))
        .await?
        .map_err(DhtIoError::network)?;

    Ok(decode_response(&response_bytes)?)
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
    send.write_all(&len).await.map_err(DhtIoError::network)?;
    send.write_all(&response_bytes)
        .await
        .map_err(DhtIoError::network)?;
    send.finish().map_err(DhtIoError::network)?;

    let _ = tokio::time::timeout(Duration::from_millis(100), send.stopped()).await;

    Ok(())
}
