use crate::bao_tree::{OpenDalReader, OpenDalWriter, RecvStreamWrapper, SendStreamWrapper};
use crate::error::BlobLibError;
use crate::hash::Hasher;
use crate::messages::{MessageType, ReplicationMessage};
use crate::opendal::{init_backend_operator, init_operator};
use crate::s3::make_bucket;
use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::BUCKET_STATS_DB;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::NegotiationResult::{Accepted, Rejected};
use aruna_core::structs::{
    Backend, BackendBucket, BackendConfig, BackendLocation, UserIdentity,
};
use aruna_core::types::UserId;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::storage::StorageHandle;
use async_trait::async_trait;
use bao_tree::io::fsm::{CreateOutboard, decode_ranges, encode_ranges_validated};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::{BaoTree, BlockSize, ByteRanges};
use bytes::{Bytes, BytesMut};
use crossfire::{mpsc, oneshot};
use futures::{AsyncWriteExt, StreamExt, stream};
use opendal::Operator;
use std::collections::HashMap;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;
use tracing::debug;
use ulid::Ulid;

pub const BAO_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(16); // 2^16 bytes

pub type EffectHandle = (BlobEffect, oneshot::TxOneshot<BlobEvent>);
pub type EffectSender = crossfire::MAsyncTx<mpsc::Array<EffectHandle>>;
pub type EffectReceiver = crossfire::AsyncRx<mpsc::Array<EffectHandle>>;

#[derive(Clone, Debug)]
pub struct BlobHandler {
    backend_config: BackendConfig,
    //backend_operators: HashMap<String, Operator>, // Store operator for each bucket
    storage: StorageHandle, // For storage backend bucket evaluation
    net: NetHandle,         // To create replication connections
    connections: Arc<Mutex<HashMap<Ulid, BiStream>>>, // Open connections for replication
}

#[derive(Clone, Debug)]
pub struct BlobHandle {
    handler: BlobHandler,
    write_channel: EffectSender,
}

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
                        BlobEffect::NegotiateIncoming { stream_id } => {
                            self.negotiate_incoming(stream_id).await
                        }
                        BlobEffect::NegotiateOutgoing {
                            replication_id,
                            stream_id,
                            location,
                        } => {
                            self.negotiate_outgoing(replication_id, stream_id, location)
                                .await
                        }
                        BlobEffect::Replicate {
                            replication_id,
                            stream_id,
                            location,
                        } => {
                            self.replicate_blob(replication_id, stream_id, location)
                                .await
                        }
                        BlobEffect::HandleReplication {
                            replication_id,
                            stream_id,
                        } => {
                            self.handle_incoming_replication(replication_id, stream_id)
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

    // Connection handling
    pub async fn open_connection(&mut self, node_id: NodeId) -> BlobEvent {
        match self.net.open_stream(node_id, Alpn::Bao).await {
            Ok(stream) => BlobEvent::ConnectionEstablished {
                stream_id: self.add_connection(None, stream).await,
            },
            Err(err) => BlobEvent::Error(BlobError::ConnectionFailed(err.to_string())),
        }
    }

    pub async fn add_connection(&mut self, stream_id: Option<Ulid>, stream: BiStream) -> Ulid {
        let stream_id = stream_id.unwrap_or_default();
        self.connections.lock().await.insert(stream_id, stream);
        stream_id
    }

    async fn ensure_multipart_bucket(&self) -> Result<(), BlobLibError> {
        if self.backend_config.backend_type != Backend::S3 {
            return Ok(());
        }

        let Some(bucket) = self.backend_config.multipart_bucket.as_deref() else {
            return Ok(());
        };

        make_bucket(bucket, &self.backend_config.service_config)
            .await
            .map_err(|err| BlobLibError::IoError(std::io::Error::other(err.to_string())))
    }

    fn multipart_bucket(&self) -> Result<&str, BlobError> {
        self.backend_config
            .multipart_bucket
            .as_deref()
            .ok_or_else(|| {
                BlobError::OperatorCreationFailed("multipart bucket not configured".to_string())
            })
    }

    async fn write_stream_to_location(
        &self,
        mut location: BackendLocation,
        operator: Operator,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let Ok(mut writer) = operator.writer(&storage_path).await else {
            return BlobEvent::Error(BlobError::OperatorCreationFailed(
                "Failed to create writer from operator".to_string(),
            ));
        };

        let mut hasher = Hasher::new();
        let mut bytes_written = 0u64;
        while let Some(chunk) = blob.next().await {
            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(err) => return BlobEvent::Error(BlobError::WriteError(err.to_string())),
            };
            hasher.update(&bytes);
            if let Err(err) = writer.write(bytes.to_vec()).await {
                return BlobEvent::Error(BlobError::WriteError(err.to_string()));
            }
            bytes_written += bytes.len() as u64;
        }

        _ = writer.close().await;
        location.blob_size = bytes_written;
        location.hashes = hasher.to_map();
        BlobEvent::WriteFinished { location }
    }

    pub async fn write_blob(
        &self,
        request_bucket: &str,
        request_key: &str,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        // Evaluate backend bucket and init location
        let backend_bucket = match self.eval_backend_bucket().await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::new();
        let backend_path = match build_backend_path(request_bucket, request_key, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        let location = BackendLocation {
            root: self.backend_config.root.clone(),
            storage_bucket: backend_bucket.clone(),
            backend_path,
            ulid,
            compressed: false,
            encrypted: false,
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };

        // Init operator for backend bucket
        let operator = match init_backend_operator(self.backend_config.clone(), backend_bucket) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };
        match self
            .write_stream_to_location(location, operator, blob)
            .await
        {
            BlobEvent::WriteFinished { location } => {
                if let Err(err) = self.increment_bucket_load(&location.storage_bucket).await {
                    BlobEvent::Error(err)
                } else {
                    BlobEvent::WriteFinished { location }
                }
            }
            other => other,
        }
    }

    pub async fn write_blob_part(
        &self,
        upload_id: Ulid,
        part_number: u16,
        created_by: UserId,
        compressed: bool,
        encrypted: bool,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let multipart_bucket = match self.multipart_bucket() {
            Ok(bucket) => bucket.to_string(),
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::new();
        let location = BackendLocation {
            root: self.backend_config.root.clone(),
            storage_bucket: multipart_bucket.clone(),
            backend_path: build_multipart_part_path(upload_id, part_number, ulid),
            ulid,
            compressed,
            encrypted,
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };
        let operator = match init_backend_operator(self.backend_config.clone(), multipart_bucket) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };
        self.write_stream_to_location(location, operator, blob)
            .await
    }

    pub async fn compose_blob(
        &self,
        request_bucket: &str,
        request_key: &str,
        created_by: UserId,
        parts: Vec<BackendLocation>,
    ) -> BlobEvent {
        let backend_bucket = match self.eval_backend_bucket().await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::new();
        let backend_path = match build_backend_path(request_bucket, request_key, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        let location = BackendLocation {
            root: self.backend_config.root.clone(),
            storage_bucket: backend_bucket.clone(),
            backend_path,
            ulid,
            compressed: false,
            encrypted: false,
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let operator = match init_backend_operator(self.backend_config.clone(), backend_bucket) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };
        let Ok(mut writer) = operator.writer(&storage_path).await else {
            return BlobEvent::Error(BlobError::OperatorCreationFailed(
                "Failed to create writer from operator".to_string(),
            ));
        };

        let mut hasher = Hasher::new();
        let mut bytes_written = 0u64;
        for part in parts {
            let part_operator = match self.operator_from_location(&part) {
                Ok(op) => op,
                Err(err) => return BlobEvent::Error(err),
            };
            let part_storage_path = match part.get_storage_path() {
                Ok(storage_path) => storage_path,
                Err(err) => return BlobEvent::Error(err),
            };
            let reader = match part_operator.reader(&part_storage_path).await {
                Ok(reader) => match reader.into_bytes_stream(..).await {
                    Ok(stream) => stream,
                    Err(err) => return BlobEvent::Error(BlobError::ReadError(err.to_string())),
                },
                Err(err) => return BlobEvent::Error(BlobError::ReadError(err.to_string())),
            };

            let mut reader = BackendStream::new(reader);
            while let Some(chunk) = reader.next().await {
                let bytes = match chunk {
                    Ok(bytes) => bytes,
                    Err(err) => return BlobEvent::Error(BlobError::ReadError(err.to_string())),
                };
                hasher.update(&bytes);
                if let Err(err) = writer.write(bytes.to_vec()).await {
                    return BlobEvent::Error(BlobError::WriteError(err.to_string()));
                }
                bytes_written += bytes.len() as u64;
            }
        }

        _ = writer.close().await;
        let mut location = location;
        location.blob_size = bytes_written;
        location.hashes = hasher.to_map();
        if let Err(err) = self.increment_bucket_load(&location.storage_bucket).await {
            BlobEvent::Error(err)
        } else {
            BlobEvent::WriteFinished { location }
        }
    }

    pub async fn read_blob(&self, location: BackendLocation) -> BlobEvent {
        let expected_blake3: [u8; 32] = match location.get_blake3() {
            Some(hash) => match hash.try_into() {
                Ok(hash) => hash,
                Err(_) => {
                    return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                        "invalid stored blake3 hash".to_string(),
                    ));
                }
            },
            None => {
                return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                    "missing stored blake3 hash".to_string(),
                ));
            }
        };

        // Get operator for provided location
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };

        // Create reader stream
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let reader = match operator.reader(&storage_path).await {
            Ok(r) => match r.into_bytes_stream(..).await {
                Ok(stream) => stream,
                Err(e) => return BlobEvent::Error(BlobError::ReadError(e.to_string())),
            },
            Err(e) => return BlobEvent::Error(BlobError::ReadError(e.to_string())),
        };

        let expected_size = location.blob_size;
        let blob = BackendStream::new(stream::try_unfold(
            (BackendStream::new(reader), Hasher::new(), 0u64),
            move |(mut stream, mut hasher, bytes_read)| async move {
                match stream.next().await {
                    Some(Ok(bytes)) => {
                        hasher.update(&bytes);
                        let next_bytes_read = bytes_read + bytes.len() as u64;
                        Ok(Some((bytes, (stream, hasher, next_bytes_read))))
                    }
                    Some(Err(err)) => Err(BlobError::ReadError(err.to_string())),
                    None => {
                        if bytes_read != expected_size {
                            return Err(BlobError::IntegrityCheckFailed(format!(
                                "expected {} bytes but streamed {} bytes",
                                expected_size, bytes_read
                            )));
                        }

                        if hasher.finalize().blake3.as_bytes() != &expected_blake3 {
                            return Err(BlobError::IntegrityCheckFailed(
                                "blake3 hash mismatch".to_string(),
                            ));
                        }

                        Ok(None)
                    }
                }
            },
        ));

        BlobEvent::ReadFinished {
            blob,
            stream_size: expected_size,
        }
    }

    pub async fn read_blob_range(
        &self,
        location: BackendLocation,
        range: impl RangeBounds<u64>,
    ) -> BlobEvent {
        // Get operator for provided location
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };

        // Create reader stream
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let reader = match operator.reader(&storage_path).await {
            Ok(r) => match r.into_bytes_stream(range).await {
                Ok(stream) => stream,
                Err(e) => return BlobEvent::Error(BlobError::ReadError(e.to_string())),
            },
            Err(e) => return BlobEvent::Error(BlobError::ReadError(e.to_string())),
        };

        BlobEvent::ReadFinished {
            blob: BackendStream::new(reader),
            stream_size: 0,
        }
    }

    pub async fn delete_blob(&self, location: BackendLocation) -> BlobEvent {
        // Get operator for provided location
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };

        // Create reader stream
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };

        if let Err(e) = operator.delete(&storage_path).await {
            return BlobEvent::Error(BlobError::DeleteError(e.to_string()));
        }
        if let Err(err) = self.decrement_bucket_load(&location.storage_bucket).await {
            return BlobEvent::Error(err);
        }
        BlobEvent::DeleteFinished
    }

    pub async fn negotiate_outgoing(
        &mut self,
        replication_id: Ulid,
        stream_id: Ulid,
        location: BackendLocation,
    ) -> BlobEvent {
        // Send replication request and evaluate response
        if let Some((sx, rx)) = self.connections.lock().await.get_mut(&stream_id) {
            if let Err(err) = ReplicationMessage::new(
                replication_id,
                MessageType::NegotiationRequest {
                    user_id: UserIdentity {
                        //TODO
                        user_id: Default::default(),
                    },
                    group_id: Default::default(),
                    size: location.blob_size,
                },
            )
            .send(sx)
            .await
            {
                return BlobEvent::Error(BlobError::WriteError(err.to_string()));
            }

            // Evaluate response
            let response = ReplicationMessage::read(rx)
                .await
                .map_err(|err| BlobEvent::Error(BlobError::ReadError(err.to_string())));
            match response {
                Err(event) => event,
                Ok(response) => match response.msg_type {
                    MessageType::NegotiationResponse(result) => {
                        BlobEvent::NegotiationFinished(result)
                    }
                    _ => BlobEvent::NegotiationFinished(Rejected(
                        "Invalid negotiation response".to_string(),
                    )),
                },
            }
        } else {
            BlobEvent::NegotiationFinished(Rejected("Stream not available".to_string()))
        }
    }

    pub async fn negotiate_incoming(&mut self, stream_id: Ulid) -> BlobEvent {
        // Read the incoming replication request and run acceptance criteria checks
        if let Some((sx, rx)) = self.connections.lock().await.get_mut(&stream_id) {
            let message = match ReplicationMessage::read2(rx).await {
                Ok(msg) => msg,
                Err(err) => return err,
            };
            if let MessageType::NegotiationRequest { .. } = &message.msg_type {
                //TODO: Things to check in the future:
                // - Quotas (User/Group)
                // - Node Policies
                // - Storage space left
                // - ...
                // (- S3 Path already occupied)
                // (- ODRL Data Usage Agreements)

                // Send the negotiation response after acceptance criteria checks
                if let Err(err) = ReplicationMessage::new(
                    message.id,
                    MessageType::NegotiationResponse(Accepted(message.id)),
                )
                .send(sx)
                .await
                {
                    return BlobEvent::Error(BlobError::WriteError(err.to_string()));
                }

                BlobEvent::NegotiationFinished(Accepted(message.id))
            } else {
                BlobEvent::NegotiationFinished(Rejected("Invalid negotiating message".to_string()))
            }
        } else {
            BlobEvent::NegotiationFinished(Rejected("Stream not available".to_string()))
        }
    }

    pub async fn replicate_blob(
        &mut self,
        replication_id: Ulid,
        stream_id: Ulid,
        location: BackendLocation,
    ) -> BlobEvent {
        // Get operator for blob location
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };

        // Create replication Outboard
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };

        let mut reader =
            match OpenDalReader::new(&operator, &storage_path, location.blob_size).await {
                Ok(reader) => reader,
                Err(err) => {
                    return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
                }
            };
        let mut outboard =
            match PreOrderOutboard::<BytesMut>::create(&mut reader, BAO_BLOCK_SIZE).await {
                Ok(outboard) => outboard,
                Err(err) => {
                    return BlobEvent::Error(BlobError::OutboardCreationFailed(err.to_string()));
                }
            };

        let mut connections = self.connections.lock().await;
        let Some((sx, rx)) = connections.get_mut(&stream_id) else {
            return BlobEvent::Error(BlobError::ReplicationRejected(
                "Stream not available".to_string(),
            ));
        };

        // Send stuff to the recipient node for outboard creation and wait for ack
        let replication_init = ReplicationMessage {
            id: replication_id,
            msg_type: MessageType::BaoTreeInfo {
                location: location.clone(),
                root: outboard.root,
            },
        };
        if let Err(err) = replication_init.send(sx).await {
            return BlobEvent::Error(BlobError::WriteError(err.to_string()));
        }

        //TODO: Acknowledge necessary or just hose data into stream?
        match ReplicationMessage::read2(rx).await {
            Ok(msg) => {
                if let MessageType::BaoTreeInfoReceived = msg.msg_type {
                    return BlobEvent::Error(BlobError::ReplicationRejected(
                        "Recipient node did not acknowledge replication init".to_string(),
                    ));
                }
            }
            Err(err) => return err,
        }

        match ReplicationMessage::read(rx).await {
            Ok(msg) if msg.msg_type == MessageType::BaoTreeInfoReceived => {}
            Ok(_) => {
                return BlobEvent::Error(BlobError::ReplicationRejected(
                    "unexpected message type".to_string(),
                ));
            }
            Err(err) => return BlobEvent::Error(BlobError::ReplicationRejected(err.to_string())),
        }

        // Send bao_tree encoded chunks
        let mut sx_wrapper = SendStreamWrapper(sx);
        let ranges = ByteRanges::from(0..location.blob_size);
        let ranges = round_up_to_chunks(&ranges);
        debug!("Chunk Ranges: {:#?}", ranges.boundaries());

        // Actually send blob chunks
        if let Err(err) =
            encode_ranges_validated(reader, &mut outboard, &ranges, &mut sx_wrapper).await
        {
            return BlobEvent::Error(BlobError::ReplicationFailed(err.to_string()));
        }

        // Close connection and return
        _ = sx.finish();
        _ = rx.stop(0u32.into());
        BlobEvent::ReplicationFinished { location }
    }

    pub async fn handle_incoming_replication(
        &mut self,
        replication_id: Ulid,
        stream_id: Ulid,
    ) -> BlobEvent {
        let (root, mut location) = {
            let mut connections = self.connections.lock().await;
            let Some((sx, rx)) = connections.get_mut(&stream_id) else {
                return BlobEvent::Error(BlobError::ReplicationRejected(
                    "Stream not available".to_string(),
                ));
            };

            match ReplicationMessage::read(rx).await {
                Ok(msg) => {
                    let MessageType::BaoTreeInfo { root, location } = msg.msg_type else {
                        return BlobEvent::Error(BlobError::ReplicationRejected(
                            "Invalid BaoTreeInfo message".to_string(),
                        ));
                    };

                    if let Err(err) =
                        ReplicationMessage::new(replication_id, MessageType::BaoTreeInfoReceived)
                            .send(sx)
                            .await
                    {
                        return BlobEvent::Error(BlobError::WriteError(err.to_string()));
                    };
                    (root, location)
                }
                Err(e) => return BlobEvent::Error(BlobError::ReadError(e.to_string())),
            }
        };

        // Eval suitable backend bucket
        let backend_bucket = match self.eval_backend_bucket().await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::new();
        location.root = self.backend_config.root.clone();
        location.storage_bucket = backend_bucket.clone();
        location.backend_path = match rebuild_backend_path(&location.backend_path, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        location.ulid = ulid;

        // Get operator for blob location
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };

        let mut connections = self.connections.lock().await;
        let Some((_, rx)) = connections.get_mut(&stream_id) else {
            return BlobEvent::Error(BlobError::ReplicationRejected(
                "Stream not available".to_string(),
            ));
        };
        let rx_wrapper = RecvStreamWrapper(rx);
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let mut writer =
            match OpenDalWriter::new(&operator, &storage_path, location.blob_size).await {
                Ok(writer) => writer,
                Err(err) => {
                    return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
                }
            };
        let mut ob = PreOrderOutboard {
            tree: BaoTree::new(location.blob_size, BAO_BLOCK_SIZE),
            root,
            data: BytesMut::new(),
        };
        let byte_ranges = ByteRanges::from(0..location.blob_size);
        let chunk_ranges = round_up_to_chunks(&byte_ranges);

        debug!("Try to decode chunks received from bidi stream");
        if let Err(err) = decode_ranges(rx_wrapper, chunk_ranges, &mut writer, &mut ob).await {
            return BlobEvent::Error(BlobError::ReplicationFailed(err.to_string()));
        }
        _ = writer.writer.close().await; // Can only fail if already closed/aborted
        debug!("Decoded all chunks and wrote them into the backend");

        let hashes = writer.hasher.to_map();
        let actual_blake3 = writer.hasher.finalize().blake3;
        if actual_blake3 != root {
            let _ = operator.delete(&storage_path).await;
            return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                "replicated content hash mismatch".to_string(),
            ));
        }

        location.hashes = hashes;
        if let Err(err) = self.increment_bucket_load(&location.storage_bucket).await {
            BlobEvent::Error(err)
        } else {
            BlobEvent::ReplicationFinished { location }
        }
    }

    async fn eval_backend_bucket(&self) -> Result<String, BlobError> {
        // Short circuit if static bucket is set in config
        if let Some(bucket) = self.backend_config.service_config.get("bucket") {
            return Ok(bucket.clone());
        }

        // Fetch bucket stats from database and check if a suitable bucket exists
        let buckets = self.fetch_bucket_stats().await?;
        if let Some(bucket_max_size) = self.backend_config.max_bucket_size {
            // Find bucket with fewer objects than max bucket size
            for bucket in buckets {
                if bucket.load < bucket_max_size {
                    return Ok(bucket.name);
                }
            }
        } else if let Some(bucket) = buckets.into_iter().next() {
            // Take the first existing bucket if no size limit
            return Ok(bucket.name);
        }

        // No suitable bucket found -> make bucket
        let bucket_name = generate_bucket_name(self.backend_config.bucket_prefix.as_deref());

        if Backend::S3 == self.backend_config.backend_type {
            // Bucket only needs to be actively created with a S3 storage backend
            make_bucket(&bucket_name, &self.backend_config.service_config).await?;
        }

        Ok(bucket_name)
    }

    async fn fetch_bucket_stats(&self) -> Result<Vec<BackendBucket>, ConversionError> {
        let mut buckets = Vec::new();
        let mut start_after = None;

        loop {
            let event = self
                .storage
                .send_effect(Effect::Storage(StorageEffect::Iter {
                    key_space: BUCKET_STATS_DB.to_string(),
                    prefix: self
                        .backend_config
                        .bucket_prefix
                        .clone()
                        .map(|prefix| prefix.into()),
                    start_after: start_after.clone(),
                    limit: 1024,
                    txn_id: None,
                }))
                .await;

            let Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) = event
            else {
                return Ok(Vec::new());
            };

            buckets.extend(
                values
                    .into_iter()
                    .map(BackendBucket::try_from)
                    .collect::<Result<Vec<BackendBucket>, ConversionError>>()?,
            );

            if let Some(next_start_after) = next_start_after {
                start_after = Some(next_start_after);
            } else {
                break;
            }
        }

        Ok(buckets)
    }

    async fn bucket_load(&self, bucket: &str) -> Result<u64, BlobError> {
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: BUCKET_STATS_DB.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                txn_id: None,
            }))
            .await;

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return Err(BlobError::ReadError(
                "failed to read bucket stats".to_string(),
            ));
        };

        match value {
            Some(value) => Ok(u64::from_le_bytes(
                value.as_ref().try_into().map_err(ConversionError::from)?,
            )),
            None => Ok(0),
        }
    }

    async fn write_bucket_load(&self, bucket: &str, load: u64) -> Result<(), BlobError> {
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_STATS_DB.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                value: load.to_le_bytes().to_vec().into(),
                txn_id: None,
            }))
            .await;

        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(BlobError::ReadError(format!(
                "failed to write bucket stats: {error}"
            ))),
            _ => Err(BlobError::ReadError(
                "unexpected storage event while writing bucket stats".to_string(),
            )),
        }
    }

    fn is_stats_managed_bucket(&self, bucket: &str) -> bool {
        self.backend_config.multipart_bucket.as_deref() != Some(bucket)
    }

    async fn increment_bucket_load(&self, bucket: &str) -> Result<(), BlobError> {
        if !self.is_stats_managed_bucket(bucket) {
            return Ok(());
        }

        let load = self.bucket_load(bucket).await?;
        self.write_bucket_load(bucket, load.saturating_add(1)).await
    }

    async fn decrement_bucket_load(&self, bucket: &str) -> Result<(), BlobError> {
        if !self.is_stats_managed_bucket(bucket) {
            return Ok(());
        }

        let load = self.bucket_load(bucket).await?;
        self.write_bucket_load(bucket, load.saturating_sub(1)).await
    }

    fn operator_from_location(&self, location: &BackendLocation) -> Result<Operator, BlobError> {
        // Clone config for temporary only manipulation
        let mut config = self.backend_config.service_config.clone();

        // Insert root and bucket (in case of S3 storage) into service config
        config.insert("root".to_string(), location.root.clone());
        if Backend::S3 == self.backend_config.backend_type {
            config.insert("bucket".to_string(), location.storage_bucket.clone());
        }

        // Init operator
        init_operator(self.backend_config.backend_type.clone(), config)
    }
}

fn generate_bucket_name(prefix: Option<&str>) -> String {
    let prefix = prefix.unwrap_or("aruna-");
    format!("{}{}", prefix, Ulid::new().to_string().to_lowercase())
}

fn build_backend_path(bucket: &str, key: &str, ulid: Ulid) -> Result<String, ConversionError> {
    PathBuf::from(bucket)
        .join(format!("{}_{}", key, ulid))
        .into_os_string()
        .into_string()
        .map_err(|_| ConversionError::OsStringError)
}

fn build_multipart_part_path(upload_id: Ulid, part_number: u16, ulid: Ulid) -> String {
    PathBuf::from(upload_id.to_string())
        .join(format!("{:05}_{}", part_number, ulid))
        .into_os_string()
        .into_string()
        .expect("multipart part path must be valid utf-8")
}

fn rebuild_backend_path(original_path: &str, ulid: Ulid) -> Result<String, ConversionError> {
    let original = PathBuf::from(original_path);
    let parent = original.parent().map(PathBuf::from).unwrap_or_default();
    let file_name = original
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(ConversionError::OsStringError)?;
    let base_name = file_name
        .rsplit_once('_')
        .map_or(file_name, |(base, _)| base);

    parent
        .join(format!("{}_{}", base_name, ulid))
        .into_os_string()
        .into_string()
        .map_err(|_| ConversionError::OsStringError)
}

#[cfg(test)]
mod tests {
    use super::BlobHandler;
    use crate::blob::BlobHandle;
    use aruna_core::effects::{BlobEffect, StorageEffect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::keyspaces::BUCKET_STATS_DB;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{Backend, BackendConfig};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::tempdir;
    use ulid::Ulid;

    struct TestContext {
        _temp_dir: tempfile::TempDir,
        blob_handle: BlobHandle,
        storage_handle: aruna_storage::storage::StorageHandle,
    }

    async fn setup_blob_handle(max_bucket_size: u64) -> TestContext {
        let temp_dir = tempdir().unwrap();
        let temp_root = temp_dir.path().to_str().unwrap().to_string();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(&temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                root: blob_root,
                service_config: HashMap::new(),
                bucket_prefix: Some("aruna-test-".to_string()),
                max_bucket_size: Some(max_bucket_size),
                multipart_bucket: Some("uploaded-parts".to_string()),
            },
            storage_handle.clone(),
            net_handle,
        )
        .await
        .unwrap();

        TestContext {
            _temp_dir: temp_dir,
            blob_handle,
            storage_handle,
        }
    }

    fn stream_from_bytes(
        bytes: &[u8],
    ) -> BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>> {
        BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
            bytes.to_vec(),
        )))
    }

    async fn bucket_load(storage_handle: &storage::StorageHandle, bucket: &str) -> u64 {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BUCKET_STATS_DB.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage event")
        };

        value
            .map(|value| u64::from_le_bytes(value.as_ref().try_into().unwrap()))
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn reuses_bucket_until_max_object_count_is_reached() {
        let context = setup_blob_handle(2).await;

        let Event::Blob(BlobEvent::WriteFinished { location: first }) = context
            .blob_handle
                .send_blob_effect(BlobEffect::Write {
                    bucket: "bucket-a".to_string(),
                    key: "one.bin".to_string(),
                    created_by: Default::default(),
                    blob: stream_from_bytes(b"one"),
                })
            .await
        else {
            panic!("first write failed")
        };

        let Event::Blob(BlobEvent::WriteFinished { location: second }) = context
            .blob_handle
                .send_blob_effect(BlobEffect::Write {
                    bucket: "bucket-a".to_string(),
                    key: "two.bin".to_string(),
                    created_by: Default::default(),
                    blob: stream_from_bytes(b"two"),
                })
            .await
        else {
            panic!("second write failed")
        };

        assert_eq!(first.storage_bucket, second.storage_bucket);
        assert!(first.storage_bucket.starts_with("aruna-test-"));
        assert_eq!(
            bucket_load(&context.storage_handle, &first.storage_bucket).await,
            2
        );
    }

    #[tokio::test]
    async fn creates_new_bucket_after_reaching_max_object_count() {
        let context = setup_blob_handle(1).await;

        let Event::Blob(BlobEvent::WriteFinished { location: first }) = context
            .blob_handle
                .send_blob_effect(BlobEffect::Write {
                    bucket: "bucket-a".to_string(),
                    key: "one.bin".to_string(),
                    created_by: Default::default(),
                    blob: stream_from_bytes(b"one"),
                })
            .await
        else {
            panic!("first write failed")
        };

        let Event::Blob(BlobEvent::WriteFinished { location: second }) = context
            .blob_handle
                .send_blob_effect(BlobEffect::Write {
                    bucket: "bucket-a".to_string(),
                    key: "two.bin".to_string(),
                    created_by: Default::default(),
                    blob: stream_from_bytes(b"two"),
                })
            .await
        else {
            panic!("second write failed")
        };

        assert_ne!(first.storage_bucket, second.storage_bucket);
        assert_eq!(
            bucket_load(&context.storage_handle, &first.storage_bucket).await,
            1
        );
        assert_eq!(
            bucket_load(&context.storage_handle, &second.storage_bucket).await,
            1
        );
    }

    #[tokio::test]
    async fn deleting_last_object_keeps_bucket_stat_row_at_zero_for_reuse() {
        let context = setup_blob_handle(1).await;

        let Event::Blob(BlobEvent::WriteFinished { location: first }) = context
            .blob_handle
                .send_blob_effect(BlobEffect::Write {
                    bucket: "bucket-a".to_string(),
                    key: "one.bin".to_string(),
                    created_by: Default::default(),
                    blob: stream_from_bytes(b"one"),
                })
            .await
        else {
            panic!("write failed")
        };

        let Event::Blob(BlobEvent::DeleteFinished) = context
            .blob_handle
            .send_blob_effect(BlobEffect::Delete {
                location: first.clone(),
            })
            .await
        else {
            panic!("delete failed")
        };

        assert_eq!(
            bucket_load(&context.storage_handle, &first.storage_bucket).await,
            0
        );

        let Event::Blob(BlobEvent::WriteFinished { location: second }) = context
            .blob_handle
                .send_blob_effect(BlobEffect::Write {
                    bucket: "bucket-a".to_string(),
                    key: "two.bin".to_string(),
                    created_by: Default::default(),
                    blob: stream_from_bytes(b"two"),
                })
            .await
        else {
            panic!("second write failed")
        };

        assert_eq!(first.storage_bucket, second.storage_bucket);
        assert_eq!(
            bucket_load(&context.storage_handle, &first.storage_bucket).await,
            1
        );
    }

    #[tokio::test]
    async fn multipart_part_bucket_is_excluded_from_bucket_stats() {
        let context = setup_blob_handle(5).await;

        let Event::Blob(BlobEvent::WriteFinished { location }) = context
            .blob_handle
                .send_blob_effect(BlobEffect::WritePart {
                    upload_id: Ulid::new(),
                    part_number: 1,
                    created_by: Default::default(),
                    compressed: false,
                    encrypted: false,
                    blob: stream_from_bytes(b"part"),
            })
            .await
        else {
            panic!("multipart write failed")
        };

        assert_eq!(location.storage_bucket, "uploaded-parts");
        assert_eq!(
            bucket_load(&context.storage_handle, "uploaded-parts").await,
            0
        );
    }
}
