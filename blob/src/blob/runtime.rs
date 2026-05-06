use super::{BlobHandle, BlobHandler, EffectReceiver};
use crate::error::BlobLibError;
use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::handle::Handle;
use aruna_core::structs::BackendConfig;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::storage::StorageHandle;
use async_trait::async_trait;
use crossfire::{mpsc, oneshot};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
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
            StagingSourceEffect::Head { access } => self.handler.head_staging_source(access).await,
            StagingSourceEffect::Read { access, range } => {
                self.handler.read_staging_source(access, range).await
            }
        };

        Event::StagingSource(staging_source_event)
    }

    pub async fn store_connection(&mut self, stream: BiStream) -> Ulid {
        self.handler.add_connection(None, stream).await
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
        };
        blob_handler.ensure_multipart_bucket().await?;
        let (blob_handle, rx) = BlobHandle::new(blob_handler.clone());
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
            Ok(Ok(stream)) => BlobEvent::ConnectionEstablished {
                stream_id: self.add_connection(None, stream).await,
            },
            Ok(Err(err)) => BlobEvent::Error(BlobError::ConnectionFailed(err.to_string())),
            Err(event) => event,
        }
    }

    pub async fn send_message(&mut self, stream_id: Ulid, payload: Vec<u8>) -> BlobEvent {
        let mut connections = self.connections.lock().await;
        let Some((sx, _)) = connections.get_mut(&stream_id) else {
            return BlobEvent::Error(BlobError::ReplicationRejected(
                "Stream not available".to_string(),
            ));
        };

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
        let mut connections = self.connections.lock().await;
        let Some((_, rx)) = connections.get_mut(&stream_id) else {
            return BlobEvent::Error(BlobError::ReplicationRejected(
                "Stream not available".to_string(),
            ));
        };

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

    pub async fn add_connection(&mut self, stream_id: Option<Ulid>, stream: BiStream) -> Ulid {
        let stream_id = stream_id.unwrap_or_default();
        self.connections.lock().await.insert(stream_id, stream);
        stream_id
    }

    pub async fn close_connection(&mut self, stream_id: Ulid) -> BlobEvent {
        let connection = self.connections.lock().await.remove(&stream_id);
        let Some((mut sx, mut rx)) = connection else {
            return BlobEvent::ConnectionClosed { stream_id };
        };

        _ = sx.finish();
        _ = rx.stop(0u32.into());
        BlobEvent::ConnectionClosed { stream_id }
    }
}
