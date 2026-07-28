use super::{BlobHandle, BlobHandler};
use crate::error::BlobLibError;
use crate::opendal::init_operator;
use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::handle::Handle;
use aruna_core::structs::{BackendConfig, BlobState, Status};
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::storage::StorageHandle;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::{Duration, Instant, interval, timeout};
use ulid::Ulid;

// Bounds concurrent transfers so overload queues instead of exhausting fds.
pub(super) const TRANSFER_SLOTS: usize = 256;
const QUEUE_WAIT_WARN: Duration = Duration::from_millis(100);
const SLOW_LOCAL_EFFECT: Duration = Duration::from_secs(1);
const SLOW_CONTROL_EFFECT: Duration = Duration::from_secs(120);
const SLOW_TRANSFER_EFFECT: Duration = Duration::from_secs(600);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EffectClass {
    Local,
    Control,
    Transfer,
}

impl EffectClass {
    fn slow_threshold(self) -> Duration {
        match self {
            EffectClass::Local => SLOW_LOCAL_EFFECT,
            EffectClass::Control => SLOW_CONTROL_EFFECT,
            EffectClass::Transfer => SLOW_TRANSFER_EFFECT,
        }
    }
}

fn classify_effect(effect: &BlobEffect) -> (EffectClass, &'static str) {
    match effect {
        BlobEffect::Write { .. } => (EffectClass::Transfer, "write"),
        BlobEffect::WritePart { .. } => (EffectClass::Transfer, "write_part"),
        BlobEffect::Compose { .. } => (EffectClass::Transfer, "compose"),
        BlobEffect::SpoolHidden { .. } => (EffectClass::Transfer, "spool_hidden"),
        BlobEffect::Replicate { .. } => (EffectClass::Transfer, "replicate"),
        BlobEffect::HandleReplication { .. } => (EffectClass::Transfer, "handle_replication"),
        BlobEffect::ServeRead { .. } => (EffectClass::Transfer, "serve_read"),
        BlobEffect::ReceiveRead { .. } => (EffectClass::Transfer, "receive_read"),
        BlobEffect::OpenConnection { .. } => (EffectClass::Control, "open_connection"),
        BlobEffect::SendMessage { .. } => (EffectClass::Control, "send_message"),
        BlobEffect::ReadMessage { .. } => (EffectClass::Control, "read_message"),
        BlobEffect::CloseConnection { .. } => (EffectClass::Control, "close_connection"),
        BlobEffect::Read { .. } => (EffectClass::Local, "read"),
        BlobEffect::ReadRange { .. } => (EffectClass::Local, "read_range"),
        BlobEffect::ReadHiddenRange { .. } => (EffectClass::Local, "read_hidden_range"),
        BlobEffect::Delete { .. } => (EffectClass::Local, "delete"),
        BlobEffect::DeleteHidden { .. } => (EffectClass::Local, "delete_hidden"),
        BlobEffect::ListHidden { .. } => (EffectClass::Local, "list_hidden"),
    }
}

#[async_trait]
impl Handle for BlobHandle {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Blob(blob_effect) => self.send_blob_effect(blob_effect).await,
            Effect::StagingSource(staging_source_effect) => {
                self.send_staging_source_effect(staging_source_effect).await
            }
            _ => Event::Blob(BlobEvent::Error(BlobError::InvalidEffect)),
        }
    }
}

impl BlobHandle {
    pub fn new(handler: BlobHandler) -> Self {
        BlobHandle { handler }
    }

    pub async fn send_blob_effect(&self, effect: BlobEffect) -> Event {
        let (class, kind) = classify_effect(&effect);
        let _permit = if class == EffectClass::Transfer {
            let queued = Instant::now();
            let permit = self.handler.transfer_slots.clone().acquire_owned().await;
            let queue_wait = queued.elapsed();
            if queue_wait >= QUEUE_WAIT_WARN {
                tracing::warn!(
                    event = "blob.queue.lag",
                    effect = kind,
                    queue_wait_ms = queue_wait.as_millis() as u64,
                    in_flight = self.handler.inflight.load(Ordering::Relaxed),
                    "Blob transfer slots saturated"
                );
            }
            permit.ok()
        } else {
            None
        };

        let in_flight = self.handler.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        let started = Instant::now();
        let blob_event = self.handler.execute_effect(effect).await;
        self.handler.inflight.fetch_sub(1, Ordering::Relaxed);
        let service = started.elapsed();
        if service >= class.slow_threshold() {
            tracing::warn!(
                event = "blob.effect.slow",
                effect = kind,
                service_ms = service.as_millis() as u64,
                in_flight,
                "Slow blob effect"
            );
        }
        Event::Blob(blob_event)
    }

    pub async fn send_staging_source_effect(&self, effect: StagingSourceEffect) -> Event {
        let staging_source_event = match effect {
            StagingSourceEffect::Check { access } => {
                self.handler.check_staging_source(access).await
            }
            StagingSourceEffect::Head { access } => self.handler.head_staging_source(access).await,
            StagingSourceEffect::List {
                access,
                offset,
                limit,
                recursive,
                files_only,
            } => {
                self.handler
                    .list_staging_source(access, offset, limit, recursive, files_only)
                    .await
            }
            StagingSourceEffect::Read { access, range } => {
                self.handler.read_staging_source(access, range).await
            }
        };

        Event::StagingSource(staging_source_event)
    }

    pub async fn store_connection(
        &self,
        peer: NodeId,
        stream: BiStream,
    ) -> Result<Ulid, BlobError> {
        self.handler.add_connection(None, peer, stream).await
    }

    pub async fn get_status(&self) -> BlobState {
        let backend_type = self.handler.backend_config.backend_type.clone();
        let status = *self.handler.operator_status.read().await;

        BlobState {
            backend_type,
            max_bucket_size: self.handler.backend_config.max_bucket_size,
            multipart_bucket: self.handler.backend_config.multipart_bucket.clone(),
            timeouts: self.handler.backend_config.timeouts,
            status,
        }
    }
}

impl BlobHandler {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        config: BackendConfig,
        storage: StorageHandle,
        net: NetHandle,
    ) -> Result<BlobHandle, BlobLibError> {
        let blob_handler = BlobHandler {
            backend_config: config,
            storage,
            net,
            connections: Arc::new(Mutex::new(HashMap::new())),
            operator_status: Arc::new(RwLock::new(Status::Unavailable)),
            transfer_slots: Arc::new(Semaphore::new(TRANSFER_SLOTS)),
            inflight: Arc::new(AtomicUsize::new(0)),
        };
        let initial_status = blob_handler.probe_operator_status().await;
        *blob_handler.operator_status.write().await = initial_status;
        blob_handler.ensure_multipart_bucket().await?;
        *blob_handler.operator_status.write().await = Status::Available;
        let status_handler = blob_handler.clone();
        tokio::spawn(async move {
            status_handler.monitor_operator_status().await;
        });

        Ok(BlobHandle::new(blob_handler))
    }

    // Effects run concurrently on their caller's task; per-operation ordering
    // is preserved by the driver awaiting each effect before the next.
    pub(super) async fn execute_effect(&self, effect: BlobEffect) -> BlobEvent {
        match effect {
            BlobEffect::Write {
                bucket,
                key,
                created_by,
                blob,
            } => self.write_blob(&bucket, &key, created_by, blob).await,
            BlobEffect::WritePart {
                upload_id,
                part_number,
                created_by,
                compressed,
                encrypted,
                blob,
            } => {
                self.write_blob_part(
                    upload_id,
                    part_number,
                    created_by,
                    compressed,
                    encrypted,
                    blob,
                )
                .await
            }
            BlobEffect::Compose {
                bucket,
                key,
                created_by,
                parts,
            } => self.compose_blob(&bucket, &key, created_by, parts).await,
            BlobEffect::Read { location } => self.read_blob(location).await,
            BlobEffect::ReadRange { location, range } => {
                self.read_blob_range(location, range).await
            }
            BlobEffect::Delete { location } => self.delete_blob(location).await,
            BlobEffect::SpoolHidden {
                namespace,
                name,
                created_by,
                max_bytes,
                blob,
            } => {
                self.spool_hidden_blob(namespace, &name, created_by, max_bytes, blob)
                    .await
            }
            BlobEffect::ReadHiddenRange { location, range } => {
                self.read_hidden_range(location, range).await
            }
            BlobEffect::DeleteHidden { key } => self.delete_hidden_blob(key).await,
            BlobEffect::ListHidden { namespace } => self.list_hidden_blobs(namespace).await,
            BlobEffect::OpenConnection { node_id } => self.open_connection(node_id).await,
            BlobEffect::SendMessage { stream_id, payload } => {
                self.send_message(stream_id, payload).await
            }
            BlobEffect::ReadMessage { stream_id } => self.read_message(stream_id).await,
            BlobEffect::CloseConnection { stream_id } => self.close_connection(stream_id).await,
            BlobEffect::Replicate {
                replication_id,
                stream_id,
                location,
                keep_alive,
            } => {
                self.replicate_blob(replication_id, stream_id, location, keep_alive)
                    .await
            }
            BlobEffect::HandleReplication {
                replication_id,
                stream_id,
                keep_alive,
            } => {
                self.handle_incoming_replication(replication_id, stream_id, keep_alive)
                    .await
            }
            BlobEffect::ServeRead {
                stream_id,
                location,
                expected_blake3,
            } => self.serve_read(stream_id, location, expected_blake3).await,
            BlobEffect::ReceiveRead {
                stream_id,
                size,
                expected_blake3,
            } => self.receive_read(stream_id, size, expected_blake3).await,
        }
    }

    pub async fn open_connection(&self, node_id: NodeId) -> BlobEvent {
        match super::control_plane::with_control_plane_timeout(
            self.net.open_stream(node_id, Alpn::Bao),
            self.control_plane_connect_timeout(),
            super::ControlPlaneTimeoutKind::Connection,
            "opening bao replication stream",
        )
        .await
        {
            Ok(Ok(stream)) => match self.add_connection(None, node_id, stream).await {
                Ok(stream_id) => BlobEvent::ConnectionEstablished { stream_id },
                Err(err) => BlobEvent::Error(err),
            },
            Ok(Err(err)) => BlobEvent::Error(BlobError::ConnectionFailed(err.to_string())),
            Err(event) => event,
        }
    }

    pub async fn send_message(&self, stream_id: Ulid, payload: Vec<u8>) -> BlobEvent {
        let stream = match self.connection_handle(stream_id).await {
            Ok(stream) => stream,
            Err(event) => return event,
        };
        let mut stream = stream.lock().await;
        let sx = &mut stream.0;

        if let Err(event) = super::control_plane::send_framed_message_with_timeout(
            sx,
            &payload,
            self.control_plane_io_timeout(),
            "sending control-plane message",
        )
        .await
        {
            return event;
        }

        BlobEvent::MessageSent { stream_id }
    }

    pub async fn read_message(&self, stream_id: Ulid) -> BlobEvent {
        let stream = match self.connection_handle(stream_id).await {
            Ok(stream) => stream,
            Err(event) => return event,
        };
        let mut stream = stream.lock().await;
        let rx = &mut stream.1;

        let buf = match super::control_plane::read_framed_message_with_timeout(
            rx,
            self.control_plane_io_timeout(),
            "reading control-plane message",
        )
        .await
        {
            Ok(payload) => payload,
            Err(event) => return event,
        };

        BlobEvent::MessageReceived {
            stream_id,
            payload: buf,
        }
    }

    pub async fn add_connection(
        &self,
        stream_id: Option<Ulid>,
        peer: NodeId,
        stream: BiStream,
    ) -> Result<Ulid, BlobError> {
        let mut connections = self.connections.lock().await;
        let stream_id = match stream_id {
            Some(stream_id) => {
                if stream_id.is_nil() {
                    return Err(BlobError::ConnectionFailed(
                        "refusing to register a nil stream id".to_string(),
                    ));
                }
                if connections.contains_key(&stream_id) {
                    return Err(BlobError::ConnectionFailed(format!(
                        "stream id already registered: {stream_id}"
                    )));
                }
                stream_id
            }
            None => {
                let mut candidate = Ulid::generate();
                while candidate.is_nil() || connections.contains_key(&candidate) {
                    candidate = Ulid::generate();
                }
                candidate
            }
        };
        connections.insert(
            stream_id,
            super::Connection {
                peer,
                stream: Arc::new(Mutex::new(stream)),
            },
        );
        Ok(stream_id)
    }

    pub async fn close_connection(&self, stream_id: Ulid) -> BlobEvent {
        let connection = self.connections.lock().await.remove(&stream_id);
        let Some(connection) = connection else {
            return BlobEvent::ConnectionClosed { stream_id };
        };
        tracing::debug!(stream_id = %stream_id, peer = %connection.peer, "closing blob connection");
        let mut stream = connection.stream.lock().await;

        _ = stream.0.finish();
        _ = stream.1.stop(0u32.into());
        BlobEvent::ConnectionClosed { stream_id }
    }

    pub(super) async fn connection_handle(
        &self,
        stream_id: Ulid,
    ) -> Result<Arc<Mutex<BiStream>>, BlobEvent> {
        self.connections
            .lock()
            .await
            .get(&stream_id)
            .map(|connection| connection.stream.clone())
            .ok_or_else(|| {
                BlobEvent::Error(BlobError::ReplicationRejected(
                    "Stream not available".to_string(),
                ))
            })
    }

    async fn monitor_operator_status(&self) {
        let mut interval = interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let status = self.probe_operator_status().await;
            *self.operator_status.write().await = status;
            self.report_pressure();
        }
    }

    fn report_pressure(&self) {
        let in_flight = self.inflight.load(Ordering::Relaxed);
        let free_slots = self.transfer_slots.available_permits();
        if free_slots == 0 {
            tracing::warn!(
                event = "blob.slots.exhausted",
                in_flight,
                "All blob transfer slots are busy"
            );
        } else if in_flight > 0 {
            tracing::debug!(
                event = "blob.pressure",
                in_flight,
                free_slots,
                "Blob effects in flight"
            );
        }
    }

    async fn probe_operator_status(&self) -> Status {
        let backend_type = self.backend_config.backend_type.clone();
        let mut config = self.backend_config.service_config.clone();
        if !self.backend_config.root.trim().is_empty() {
            config.insert("root".to_string(), self.backend_config.root.clone());
        }

        match init_operator(backend_type, config) {
            Ok(operator) => {
                let probe_timeout = self.handler_probe_timeout();
                match timeout(probe_timeout, operator.check()).await {
                    Ok(Ok(_)) => Status::Available,
                    Ok(Err(_)) | Err(_) => Status::Unavailable,
                }
            }
            Err(BlobError::OperatorCreationFailed(_)) => Status::NotConfigured,
            Err(_) => Status::Unavailable,
        }
    }

    fn handler_probe_timeout(&self) -> Duration {
        self.backend_config
            .timeouts
            .control_plane_io_timeout
            .min(Duration::from_secs(5))
    }
}
