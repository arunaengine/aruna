use crate::bao_tree::{OpenDalReader, OpenDalWriter, RecvStreamWrapper, SendStreamWrapper};
use crate::error::BlobLibError;
use crate::hash::Hasher;
use crate::messages::{MessageType, ReplicationMessage};
use crate::opendal::{init_backend_operator, init_operator};
use crate::s3::make_bucket;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::NegotiationResult::{Accepted, Rejected};
use aruna_core::structs::{
    Backend, BackendBucket, BackendConfig, BackendLocation, BlobInfo, RealmId, UserIdentity,
};
use aruna_core::NodeId;
use aruna_net::streams::BiStream;
use aruna_net::NetHandle;
use aruna_storage::storage::StorageHandle;
use async_trait::async_trait;
use bao_tree::io::fsm::{decode_ranges, encode_ranges_validated, CreateOutboard};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::{BaoTree, BlockSize, ByteRanges};
use bytes::{Bytes, BytesMut};
use crossfire::{mpsc, oneshot};
use futures::{AsyncWriteExt, StreamExt};
use opendal::Operator;
use std::collections::HashMap;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;
use ulid::Ulid;

pub const BLOB_LOCATION_DB: &str = "locations";
pub const BLOB_PATH_DB: &str = "path_locations_mapping";
pub const BUCKET_STATS_DB: &str = "bucket_stats";
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
                        BlobEffect::Write { bucket, key, blob } => {
                            self.write_blob(&bucket, &key, blob).await
                        }
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
                            blob_info,
                        } => {
                            self.negotiate_outgoing(replication_id, stream_id, blob_info)
                                .await
                        }
                        BlobEffect::Replicate {
                            replication_id,
                            stream_id,
                            blob_info,
                        } => {
                            self.replicate_blob(replication_id, stream_id, blob_info)
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
        let stream_id = stream_id.unwrap_or_else(|| Ulid::new());
        self.connections.lock().await.insert(stream_id, stream);
        stream_id
    }

    pub async fn write_blob(
        &self,
        request_bucket: &str,
        request_key: &str,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        // Init stuff
        let mut hasher = Hasher::new();
        let mut bytes_written: u64 = 0;

        // Evaluate backend bucket and init location
        let backend_bucket = match self.eval_backend_bucket().await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let location = BackendLocation {
            root: self.backend_config.root.clone(),
            storage_bucket: backend_bucket.clone(),
            object_bucket: request_bucket.to_string(),
            object_key: request_key.to_string(),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
        };
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };

        // Init operator for backend bucket
        let operator = match init_backend_operator(self.backend_config.clone(), backend_bucket) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };
        let Ok(mut writer) = operator.writer(&storage_path).await else {
            return BlobEvent::Error(BlobError::OperatorCreationFailed(
                "Failed to create writer from operator".to_string(),
            ));
        };

        // Write blob to backend storage
        while let Some(Ok(bytes)) = blob.next().await {
            hasher.update(&bytes);
            if let Err(e) = writer.write(bytes.to_vec()).await {
                return BlobEvent::Error(BlobError::WriteError(e.to_string()));
            }
            bytes_written += bytes.len() as u64;
        }
        _ = writer.close().await; // Can only fail if already closed/aborted

        // Return write result
        BlobEvent::WriteFinished {
            location,
            bytes_written,
            hashes: hasher.to_map(),
            //checksums: Default::default(),
        }
    }

    pub async fn read_blob(&self, location: BackendLocation) -> BlobEvent {
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

        BlobEvent::ReadFinished {
            blob: BackendStream::new(reader),
            stream_size: 0, //TODO: Should this be queried in the state machine?
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
            stream_size: 0, //TODO: Should this be queried in the state machine?
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
        BlobEvent::DeleteFinished
    }

    pub async fn negotiate_outgoing(
        &mut self,
        replication_id: Ulid,
        stream_id: Ulid,
        blob_info: BlobInfo,
    ) -> BlobEvent {
        // Send replication request and evaluate response
        if let Some((sx, rx)) = self.connections.lock().await.get_mut(&stream_id) {
            if let Err(err) = ReplicationMessage::new(
                replication_id,
                MessageType::NegotiationRequest {
                    user_id: UserIdentity {
                        //TODO
                        user_id: Default::default(),
                        realm_key: RealmId([0u8; 32]),
                    },
                    group_id: Default::default(),
                    size: blob_info.blob_size,
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
        blob_info: BlobInfo,
    ) -> BlobEvent {
        // Get operator for blob location
        let operator = match self.operator_from_location(&blob_info.location) {
            Ok(op) => op,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };

        // Create replication Outboard
        let storage_path = match blob_info.location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };

        let mut reader =
            match OpenDalReader::new(&operator, &storage_path, blob_info.blob_size).await {
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
                blob_info: blob_info.clone(),
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
        let ranges = ByteRanges::from(0..blob_info.blob_size);
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
        BlobEvent::ReplicationFinished { blob_info }
    }

    pub async fn handle_incoming_replication(
        &mut self,
        replication_id: Ulid,
        stream_id: Ulid,
    ) -> BlobEvent {
        let (root, mut blob_info) = {
            let mut connections = self.connections.lock().await;
            let Some((sx, rx)) = connections.get_mut(&stream_id) else {
                return BlobEvent::Error(BlobError::ReplicationRejected(
                    "Stream not available".to_string(),
                ));
            };

            match ReplicationMessage::read(rx).await {
                Ok(msg) => {
                    let MessageType::BaoTreeInfo { root, blob_info } = msg.msg_type else {
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
                    (root, blob_info)
                }
                Err(e) => return BlobEvent::Error(BlobError::ReadError(e.to_string())),
            }
        };

        // Eval suitable backend bucket
        let backend_bucket = match self.eval_backend_bucket().await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        blob_info.location.root = self.backend_config.root.clone();
        blob_info.location.storage_bucket = backend_bucket.clone();
        blob_info.location.ulid = Ulid::new();

        // Get operator for blob location
        let operator = match self.operator_from_location(&blob_info.location) {
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
        let storage_path = match blob_info.location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let mut writer =
            match OpenDalWriter::new(&operator, &storage_path, blob_info.blob_size).await {
                Ok(writer) => writer,
                Err(err) => {
                    return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
                }
            };
        let mut ob = PreOrderOutboard {
            tree: BaoTree::new(blob_info.blob_size, BAO_BLOCK_SIZE),
            root,
            data: BytesMut::new(),
        };
        let byte_ranges = ByteRanges::from(0..blob_info.blob_size);
        let chunk_ranges = round_up_to_chunks(&byte_ranges);

        debug!("Try to decode chunks received from bidi stream");
        if let Err(err) = decode_ranges(rx_wrapper, chunk_ranges, &mut writer, &mut ob).await {
            return BlobEvent::Error(BlobError::ReplicationFailed(err.to_string()));
        }
        _ = writer.writer.close().await; // Can only fail if already closed/aborted
        debug!("Decoded all chunks and wrote them into the backend");

        BlobEvent::ReplicationFinished { blob_info }
    }

    async fn eval_backend_bucket(&self) -> Result<String, BlobError> {
        // Short circuit if static bucket is set in config
        if let Some(bucket) = self.backend_config.service_config.get("bucket") {
            return Ok(bucket.clone());
        }

        // Fetch bucket stats from database
        let buckets = self.fetch_bucket_stats().await?;

        // Check if a suitable bucket exists
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
        let bucket_name = generate_bucket_name();

        if Backend::S3 == self.backend_config.backend_type {
            // Bucket only needs to be actively created with a S3 storage backend
            make_bucket(&bucket_name, &self.backend_config.service_config).await?;
        }

        Ok(bucket_name)
    }

    async fn fetch_bucket_stats(&self) -> Result<Vec<BackendBucket>, ConversionError> {
        if let Event::Storage(StorageEvent::IterResult { values, .. }) = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Iter {
                key_space: BUCKET_STATS_DB.to_string(),
                prefix: self
                    .backend_config
                    .bucket_prefix
                    .clone()
                    .map(|prefix| prefix.into()),
                start_after: None,
                limit: 0,
                txn_id: None,
            }))
            .await
        {
            values
                .into_iter()
                .map(BackendBucket::try_from)
                .collect::<Result<Vec<BackendBucket>, ConversionError>>()
        } else {
            Ok(Vec::new())
        }
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

fn generate_bucket_name() -> String {
    format!("aruna-{}", Ulid::new().to_string().to_lowercase())
}
