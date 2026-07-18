use super::{BlobHandle, BlobHandler, EffectReceiver};
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
use crossfire::{mpsc, oneshot};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, interval, timeout};
use ulid::Ulid;

#[async_trait]
impl Handle for BlobHandle {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Blob(blob_effect) => {
                let (response_tx, response_rx) = oneshot::oneshot();
                if self
                    .write_channel
                    .send((blob_effect, response_tx))
                    .await
                    .is_err()
                {
                    return Event::Blob(BlobEvent::Error(BlobError::ChannelClosed));
                }
                match response_rx.await {
                    Ok(event) => Event::Blob(event),
                    Err(_) => Event::Blob(BlobEvent::Error(BlobError::ChannelClosed)),
                }
            }
            Effect::StagingSource(staging_source_effect) => {
                self.send_staging_source_effect(staging_source_effect).await
            }
            _ => Event::Blob(BlobEvent::Error(BlobError::InvalidEffect)),
        }
    }
}

impl BlobHandle {
    pub fn new(handler: BlobHandler) -> (Self, EffectReceiver) {
        let (sender, receiver) = mpsc::bounded_async(2048);
        (
            BlobHandle {
                handler,
                write_channel: sender,
            },
            receiver,
        )
    }

    pub async fn send_blob_effect(&self, effect: BlobEffect) -> Event {
        let blob_event = {
            let (response_tx, response_rx) = oneshot::oneshot();
            if self
                .write_channel
                .send((effect, response_tx))
                .await
                .is_err()
            {
                return Event::Blob(BlobEvent::Error(BlobError::ChannelClosed));
            }
            response_rx
                .await
                .unwrap_or_else(|_| BlobEvent::Error(BlobError::ChannelClosed))
        };
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
                limit,
                recursive,
                files_only,
            } => {
                self.handler
                    .list_staging_source(access, limit, recursive, files_only)
                    .await
            }
            StagingSourceEffect::Read { access, range } => {
                self.handler.read_staging_source(access, range).await
            }
        };

        Event::StagingSource(staging_source_event)
    }

    pub async fn store_connection(
        &mut self,
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
        let mut blob_handler = BlobHandler {
            backend_config: config,
            storage,
            net,
            connections: Arc::new(Mutex::new(HashMap::new())),
            operator_status: Arc::new(RwLock::new(Status::Unavailable)),
        };
        let initial_status = blob_handler.probe_operator_status().await;
        *blob_handler.operator_status.write().await = initial_status;
        blob_handler.ensure_multipart_bucket().await?;
        *blob_handler.operator_status.write().await = Status::Available;
        let (blob_handle, rx) = BlobHandle::new(blob_handler.clone());
        let status_handler = blob_handler.clone();
        tokio::spawn(async move {
            status_handler.monitor_operator_status().await;
        });
        tokio::spawn(async move {
            blob_handler.receive_loop(rx).await;
        });

        Ok(blob_handle)
    }

    pub async fn receive_loop(&mut self, receiver: EffectReceiver) -> ! {
        loop {
            match receiver.recv().await {
                Ok((effect, response_tx)) => {
                    let event = match effect {
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
                        BlobEffect::OpenConnection { node_id } => {
                            self.open_connection(node_id).await
                        }
                        BlobEffect::SendMessage { stream_id, payload } => {
                            self.send_message(stream_id, payload).await
                        }
                        BlobEffect::ReadMessage { stream_id } => self.read_message(stream_id).await,
                        BlobEffect::CloseConnection { stream_id } => {
                            self.close_connection(stream_id).await
                        }
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
                    };
                    response_tx.send(event);
                }
                Err(_) => {
                    tracing::warn!("Blob receiver channel closed, shutting down blob thread.");
                }
            }
        }
    }

    pub async fn open_connection(&mut self, node_id: NodeId) -> BlobEvent {
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

    pub async fn send_message(&mut self, stream_id: Ulid, payload: Vec<u8>) -> BlobEvent {
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

    pub async fn read_message(&mut self, stream_id: Ulid) -> BlobEvent {
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
        &mut self,
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
                let mut candidate = Ulid::r#gen();
                while candidate.is_nil() || connections.contains_key(&candidate) {
                    candidate = Ulid::r#gen();
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

    pub async fn close_connection(&mut self, stream_id: Ulid) -> BlobEvent {
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
