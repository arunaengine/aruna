use crate::error::BlobLibError;
use crate::hash::Hasher;
use crate::opendal::{init_backend_operator, init_operator};
use crate::s3::make_bucket;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{Backend, BackendBucket, BackendConfig, BackendLocation, BlobInfo};
use aruna_net::streams::BiStream;
use aruna_storage::storage::StorageHandle;
use bytes::Bytes;
use crossfire::{mpsc, oneshot};
use futures::StreamExt;
use opendal::Operator;
use std::ops::RangeBounds;
use ulid::Ulid;

pub const BLOB_LOCATION_DB: &str = "locations";
pub const BLOB_PATH_DB: &str = "path_locations_mapping";
pub const BUCKET_STATS_DB: &str = "bucket_stats";

pub type EffectHandle = (BlobEffect, oneshot::TxOneshot<BlobEvent>);
pub type EffectSender = crossfire::MAsyncTx<mpsc::Array<EffectHandle>>;
pub type EffectReceiver = crossfire::AsyncRx<mpsc::Array<EffectHandle>>;

//TODO: BaoTree replication (immer getrieben vom Initiator)

pub struct BlobHandler {
    backend_config: BackendConfig,
    //backend_operators: HashMap<String, Operator>, // Store operator for each bucket !?
    storage: StorageHandle, // For storage backend bucket evaluation
}

#[derive(Debug)]
pub struct BlobHandle {
    write_channel: EffectSender,
}

impl BlobHandle {
    pub fn new() -> (Self, EffectReceiver) {
        let (sender, receiver) = mpsc::bounded_async(2048);
        (
            BlobHandle {
                write_channel: sender,
            },
            receiver,
        )
    }

    pub async fn send_effect(&self, effect: BlobEffect) -> Event {
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
}

impl BlobHandler {
    pub async fn new(
        config: BackendConfig,
        handle: StorageHandle,
    ) -> Result<BlobHandle, BlobLibError> {
        let (blob_handle, rx) = BlobHandle::new();

        tokio::spawn(async {
            let mut blob_handler = BlobHandler {
                backend_config: config,
                storage: handle,
            };
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
                    };
                    response_tx.send(event);
                }
                Err(_) => {
                    tracing::warn!("Blob receiver channel closed, shutting down blob thread.");
                }
            }
        }
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
        let mut writer = operator.writer(&storage_path).await.unwrap();

        // Write blob to backend storage
        while let Some(Ok(bytes)) = blob.next().await {
            hasher.update(&bytes);
            if let Err(e) = writer.write(bytes.to_vec()).await {
                return BlobEvent::Error(BlobError::WriteError(e.to_string()));
            }
            bytes_written += bytes.len() as u64;
        }
        writer.close().await.unwrap();

        // Return write result
        BlobEvent::WriteFinished {
            location,
            bytes_written,
            hashes: hasher.to_map(),
            //checksums: Default::default(),
        }
    }

    //pub async fn read_blob(&self, operator: Operator, path: &str) -> BlobEvent {
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

    pub async fn negotiate_replication(
        &self,
        (_sx, _rx): BiStream,
        _blob_info: BlobInfo,
    ) -> Result<(), BlobError> {
        //TODO: Handle replication init:
        // - User allowed to write to this node?
        // - Node has enough space?
        // - ...?
        todo!()
    }

    pub async fn replicate_blob(
        &self,
        (_sx, _rx): BiStream,
        _blob_location: BackendLocation,
    ) -> Result<(), BlobError> {
        todo!()
    }

    pub async fn handle_incoming_replication(&self, (_sx, _rx): BiStream) -> Result<(), BlobError> {
        todo!()
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
