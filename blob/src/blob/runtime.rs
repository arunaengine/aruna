use super::{BackendRegistry, BlobHandle, BlobHandler};
use crate::egress::EgressGuard;
use crate::error::BlobLibError;
use crate::opendal::init_operator;
use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect};
use aruna_core::egress::EgressPolicy;
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::handle::Handle;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{BackendConfig, BackendState, BlobState, MultipartUploadPartKey, Status};
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::storage::StorageHandle;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, Instant, interval, timeout};
use ulid::Ulid;

// Bounds concurrent transfers so overload queues instead of exhausting fds.
pub(super) const TRANSFER_SLOTS: usize = 256;
// Reads hold a backend connection for as long as the returned stream lives.
pub(super) const READ_SLOTS: usize = 256;
// Spools take their own pool because their input stream may issue nested
// transfer and read effects; sharing a pool with those would deadlock.
pub(super) const SPOOL_SLOTS: usize = 64;
const QUEUE_WAIT_WARN: Duration = Duration::from_millis(100);
const SLOW_LOCAL_EFFECT: Duration = Duration::from_secs(1);
const SLOW_CONTROL_EFFECT: Duration = Duration::from_secs(120);
const SLOW_TRANSFER_EFFECT: Duration = Duration::from_secs(600);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EffectClass {
    Local,
    Control,
    Transfer,
    Read,
    Spool,
}

impl EffectClass {
    fn slow_threshold(self) -> Duration {
        match self {
            EffectClass::Local | EffectClass::Read => SLOW_LOCAL_EFFECT,
            EffectClass::Control => SLOW_CONTROL_EFFECT,
            EffectClass::Transfer | EffectClass::Spool => SLOW_TRANSFER_EFFECT,
        }
    }
}

fn classify_effect(effect: &BlobEffect) -> (EffectClass, &'static str) {
    match effect {
        BlobEffect::Write { .. } => (EffectClass::Transfer, "write"),
        BlobEffect::WritePart { .. } => (EffectClass::Transfer, "write_part"),
        BlobEffect::Compose { .. } => (EffectClass::Transfer, "compose"),
        BlobEffect::SpoolHidden { .. } => (EffectClass::Spool, "spool_hidden"),
        BlobEffect::Replicate { .. } => (EffectClass::Transfer, "replicate"),
        BlobEffect::HandleReplication { .. } => (EffectClass::Transfer, "handle_replication"),
        BlobEffect::ServeRead { .. } => (EffectClass::Transfer, "serve_read"),
        BlobEffect::ReceiveRead { .. } => (EffectClass::Transfer, "receive_read"),
        BlobEffect::OpenConnection { .. } => (EffectClass::Control, "open_connection"),
        BlobEffect::SendMessage { .. } => (EffectClass::Control, "send_message"),
        BlobEffect::ReadMessage { .. } => (EffectClass::Control, "read_message"),
        BlobEffect::CloseConnection { .. } => (EffectClass::Control, "close_connection"),
        BlobEffect::Read { .. } => (EffectClass::Read, "read"),
        BlobEffect::ReadRange { .. } => (EffectClass::Read, "read_range"),
        BlobEffect::ReadHiddenRange { .. } => (EffectClass::Read, "read_hidden_range"),
        BlobEffect::Delete { .. } => (EffectClass::Local, "delete"),
        BlobEffect::DeleteHidden { .. } => (EffectClass::Local, "delete_hidden"),
        BlobEffect::ListHidden { .. } => (EffectClass::Local, "list_hidden"),
        BlobEffect::CheckGroupBackend { .. } => (EffectClass::Control, "check_group_backend"),
    }
}

// Holds a read slot until the lazily consumed stream is dropped.
struct PermitStream {
    inner: BackendStream<Result<Bytes, StreamError>>,
    _permit: OwnedSemaphorePermit,
}

impl Stream for PermitStream {
    type Item = Result<Bytes, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.0.as_mut().poll_next(cx)
    }
}

fn hold_permit(
    blob: BackendStream<Result<Bytes, StreamError>>,
    permit: OwnedSemaphorePermit,
) -> BackendStream<Result<Bytes, StreamError>> {
    BackendStream(Box::pin(PermitStream {
        inner: blob,
        _permit: permit,
    }))
}

// Error events drop the permit here; only a live stream keeps a slot.
fn attach_permit(event: BlobEvent, permit: Option<OwnedSemaphorePermit>) -> BlobEvent {
    let Some(permit) = permit else {
        return event;
    };
    match event {
        BlobEvent::ReadFinished { blob, stream_size } => BlobEvent::ReadFinished {
            blob: hold_permit(blob, permit),
            stream_size,
        },
        BlobEvent::HiddenRead { blob, stream_size } => BlobEvent::HiddenRead {
            blob: hold_permit(blob, permit),
            stream_size,
        },
        other => other,
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
        let slots = match class {
            EffectClass::Transfer => Some(self.handler.transfer_slots.clone()),
            EffectClass::Read => Some(self.handler.read_slots.clone()),
            EffectClass::Spool => Some(self.handler.spool_slots.clone()),
            EffectClass::Local | EffectClass::Control => None,
        };
        let permit = match slots {
            Some(slots) => {
                let queued = Instant::now();
                let permit = slots.acquire_owned().await;
                let queue_wait = queued.elapsed();
                if queue_wait >= QUEUE_WAIT_WARN {
                    tracing::warn!(
                        event = "blob.queue.lag",
                        effect = kind,
                        queue_wait_ms = queue_wait.as_millis() as u64,
                        in_flight = self.handler.inflight.load(Ordering::Relaxed),
                        "Blob slots saturated"
                    );
                }
                permit.ok()
            }
            None => None,
        };

        let in_flight = self.handler.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        let started = Instant::now();
        // Boxed so the large per-effect future never inflates caller stacks.
        let blob_event = Box::pin(self.handler.execute_effect(effect)).await;
        self.handler.inflight.fetch_sub(1, Ordering::Relaxed);
        // Read events carry a lazy stream, so the slot follows the stream.
        let blob_event = if class == EffectClass::Read {
            attach_permit(blob_event, permit)
        } else {
            blob_event
        };
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

    /// Node-local routing inputs for callers assembling an operation config.
    pub fn routing(&self) -> aruna_core::structs::NodeRouting {
        self.handler.registry.routing()
    }

    /// Tenant backends with an effect running against them right now. Their
    /// credentials must outlive that effect or its rollback cannot reach the
    /// bytes it wrote.
    pub fn active_group_backends(&self) -> std::collections::BTreeSet<Ulid> {
        self.handler.active_group_backends()
    }

    /// Per-backend health for `/info`.
    pub async fn backend_states(&self) -> Vec<BackendState> {
        let mut states = Vec::new();
        for (name, backend) in self.handler.registry.entries() {
            states.push(BackendState {
                name: name.clone(),
                backend_type: backend.config.backend_type.clone(),
                class: backend.class.clone(),
                allow_tenants: backend.allow_tenants,
                quota_bytes: backend.quota_bytes,
                default: name == self.handler.registry.default_name(),
                status: *backend.status.read().await,
            });
        }
        states
    }

    pub async fn store_connection(
        &self,
        peer: NodeId,
        stream: BiStream,
    ) -> Result<Ulid, BlobError> {
        self.handler.add_connection(None, peer, stream).await
    }

    /// The headline signal stays the default backend's, so existing consumers
    /// keep one status while the registry tracks each backend separately.
    pub async fn get_status(&self) -> BlobState {
        let registry = &self.handler.registry;
        let config = registry.default_config().clone();
        let status = registry
            .backend(&registry.default_ref())
            .map(|backend| backend.status.clone());
        let status = match status {
            Ok(status) => *status.read().await,
            Err(_) => Status::NotConfigured,
        };

        BlobState {
            backend_type: config.backend_type,
            max_bucket_size: config.max_bucket_size,
            multipart_bucket: config.multipart_bucket,
            timeouts: config.timeouts,
            status,
            backends: self.backend_states().await,
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
        Self::with_egress(config, storage, net, EgressPolicy::strict()).await
    }

    /// Constructor seam for the egress policy. Production wiring calls `new`,
    /// which pins the strict policy; fixtures pass a loopback-permitting one.
    #[allow(clippy::new_ret_no_self)]
    pub async fn with_egress(
        config: BackendConfig,
        storage: StorageHandle,
        net: NetHandle,
        policy: EgressPolicy,
    ) -> Result<BlobHandle, BlobLibError> {
        Self::with_registry(BackendRegistry::single(config), storage, net, policy).await
    }

    #[allow(clippy::new_ret_no_self)]
    pub async fn with_registry(
        registry: BackendRegistry,
        storage: StorageHandle,
        net: NetHandle,
        policy: EgressPolicy,
    ) -> Result<BlobHandle, BlobLibError> {
        let blob_handler = BlobHandler {
            registry,
            egress: EgressGuard::new(policy)?,
            storage,
            net,
            connections: Arc::new(Mutex::new(HashMap::new())),
            transfer_slots: Arc::new(Semaphore::new(TRANSFER_SLOTS)),
            read_slots: Arc::new(Semaphore::new(READ_SLOTS)),
            spool_slots: Arc::new(Semaphore::new(SPOOL_SLOTS)),
            inflight: Arc::new(AtomicUsize::new(0)),
            group_effects: Arc::new(std::sync::Mutex::new(HashMap::new())),
        };
        blob_handler.ensure_multipart_bucket().await?;
        blob_handler.probe_all_backends().await;
        let status_handler = blob_handler.clone();
        tokio::spawn(async move {
            status_handler.monitor_backend_status().await;
        });

        Ok(BlobHandle::new(blob_handler))
    }

    // Effects run concurrently on their caller's task; per-operation ordering
    // is preserved by the driver awaiting each effect before the next.
    pub(super) async fn execute_effect(&self, effect: BlobEffect) -> BlobEvent {
        let _hold = self.hold_group_backends(&effect);
        let handler = match self.with_group_backends(&effect).await {
            Ok(handler) => handler,
            Err(error) => return BlobEvent::Error(error),
        };
        handler.dispatch_effect(effect).await
    }

    async fn dispatch_effect(&self, effect: BlobEffect) -> BlobEvent {
        match effect {
            BlobEffect::Write {
                bucket,
                key,
                resolved,
                created_by,
                blob,
            } => Box::pin(self.write_blob(&bucket, &key, resolved, created_by, blob)).await,
            BlobEffect::WritePart {
                upload_id,
                part_number,
                resolved,
                created_by,
                compressed,
                encrypted,
                blob,
            } => {
                Box::pin(self.write_blob_part(
                    MultipartUploadPartKey::new(upload_id, part_number),
                    resolved,
                    created_by,
                    compressed,
                    encrypted,
                    blob,
                ))
                .await
            }
            BlobEffect::Compose {
                bucket,
                key,
                resolved,
                created_by,
                parts,
            } => Box::pin(self.compose_blob(&bucket, &key, resolved, created_by, parts)).await,
            BlobEffect::Read { location } => Box::pin(self.read_blob(location)).await,
            BlobEffect::ReadRange { location, range } => {
                Box::pin(self.read_blob_range(location, range)).await
            }
            BlobEffect::Delete { location } => Box::pin(self.delete_blob(location)).await,
            BlobEffect::SpoolHidden {
                namespace,
                name,
                created_by,
                max_bytes,
                blob,
            } => {
                Box::pin(self.spool_hidden_blob(namespace, &name, created_by, max_bytes, blob))
                    .await
            }
            BlobEffect::ReadHiddenRange { location, range } => {
                Box::pin(self.read_hidden_range(location, range)).await
            }
            BlobEffect::DeleteHidden { key } => Box::pin(self.delete_hidden_blob(key)).await,
            BlobEffect::ListHidden { namespace } => {
                Box::pin(self.list_hidden_blobs(namespace)).await
            }
            BlobEffect::CheckGroupBackend { record, secret } => {
                Box::pin(self.check_group_backend(record, secret)).await
            }
            BlobEffect::OpenConnection { node_id } => Box::pin(self.open_connection(node_id)).await,
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
                Box::pin(self.replicate_blob(replication_id, stream_id, location, keep_alive)).await
            }
            BlobEffect::HandleReplication {
                replication_id,
                stream_id,
                resolved,
                keep_alive,
            } => {
                Box::pin(self.handle_incoming_replication(
                    replication_id,
                    stream_id,
                    resolved,
                    keep_alive,
                ))
                .await
            }
            BlobEffect::ServeRead {
                stream_id,
                location,
                expected_blake3,
            } => Box::pin(self.serve_read(stream_id, location, expected_blake3)).await,
            BlobEffect::ReceiveRead {
                stream_id,
                size,
                expected_blake3,
            } => Box::pin(self.receive_read(stream_id, size, expected_blake3)).await,
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

    async fn monitor_backend_status(&self) {
        let mut interval = interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            self.probe_all_backends().await;
            self.report_pressure();
        }
    }

    async fn probe_all_backends(&self) {
        for (_, backend) in self.registry.entries() {
            let status = self.probe_backend_status(&backend.config).await;
            *backend.status.write().await = status;
        }
    }

    fn report_pressure(&self) {
        let in_flight = self.inflight.load(Ordering::Relaxed);
        let free_slots = self.transfer_slots.available_permits();
        let free_reads = self.read_slots.available_permits();
        let free_spools = self.spool_slots.available_permits();
        if free_slots == 0 || free_reads == 0 || free_spools == 0 {
            tracing::warn!(
                event = "blob.slots.exhausted",
                in_flight,
                free_slots,
                free_reads,
                free_spools,
                "A blob slot pool is fully busy"
            );
        } else if in_flight > 0 {
            tracing::debug!(
                event = "blob.pressure",
                in_flight,
                free_slots,
                free_reads,
                free_spools,
                "Blob effects in flight"
            );
        }
    }

    async fn probe_backend_status(&self, backend: &BackendConfig) -> Status {
        let backend_type = backend.backend_type.clone();
        let mut config = backend.service_config.clone();
        if !backend.root.trim().is_empty() {
            config.insert("root".to_string(), backend.root.clone());
        }
        // S3 operators need a bucket; without a pinned one, probe the multipart
        // bucket that startup guarantees.
        if backend_type == aruna_core::structs::Backend::S3 && !config.contains_key("bucket") {
            match backend.multipart_bucket.as_deref() {
                Some(bucket) => {
                    config.insert("bucket".to_string(), bucket.to_string());
                }
                None => return Status::NotConfigured,
            }
        }

        match init_operator(backend_type, config, &self.egress) {
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
        self.registry
            .timeouts()
            .control_plane_io_timeout
            .min(Duration::from_secs(5))
    }
}
