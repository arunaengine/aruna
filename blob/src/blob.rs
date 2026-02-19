use crate::error::BlobLibError;
use crate::hash::Hasher;
use crate::opendal::{emit_backend_operator, get_backend_operator};
use crate::s3::make_bucket;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::stream::BackendStream;
use aruna_core::stream::BoxError;
use aruna_core::structs::{BackendBucket, BackendConfig};
use aruna_storage::storage::StorageHandle;
use bytes::Bytes;
use crossfire::{mpsc, oneshot};
use futures::StreamExt;
use opendal::Operator;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::RangeBounds;
use std::str::FromStr;
use std::thread;
use ulid::Ulid;

pub const BLOB_LOCATION_DB: &str = "locations";
pub const BLOB_PATH_DB: &str = "path_locations_mapping";

pub type EffectHandle = (BlobEffect, oneshot::TxOneshot<BlobEvent>);
pub type EffectSender = crossfire::MTx<mpsc::Array<EffectHandle>>;
pub type EffectReceiver = crossfire::Rx<mpsc::Array<EffectHandle>>;

//TODO: BaoTree replication (immer getrieben vom Initiator)
//TODO: Hold StorageHandle to manage backend "buckets"?

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Backend {
    #[default]
    S3,
    HTTP,
    Postgres,
    FileSystem,
}

impl FromStr for Backend {
    type Err = BlobLibError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3" => Ok(Backend::S3),
            "http" => Ok(Backend::HTTP),
            "postgres" => Ok(Backend::Postgres),
            "filesystem" => Ok(Backend::FileSystem),
            _ => Err(BlobLibError::ConversionError(format!(
                "unknown backend {}",
                s
            ))),
        }
    }
}

impl Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::S3 => write!(f, "s3"),
            Backend::HTTP => write!(f, "http"),
            Backend::Postgres => write!(f, "postgres"),
            Backend::FileSystem => write!(f, "filesystem"),
        }
    }
}

pub struct BlobHandler {
    backend_config: BackendConfig,
    storage: StorageHandle, // For storage backend bucket evaluation
}

#[derive(Debug)]
pub struct BlobHandle {
    write_channel: EffectSender,
}

impl BlobHandle {
    pub fn new() -> (Self, EffectReceiver) {
        let (sender, receiver) = mpsc::bounded_blocking(2048);
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
            if self.write_channel.send((effect, response_tx)).is_err() {
                return Event::Blob(BlobEvent::Error {
                    error: BlobError::ChannelClosed,
                });
            }
            response_rx.await.unwrap_or_else(|_| BlobEvent::Error {
                error: BlobError::ChannelClosed,
            })
        };
        Event::Blob(blob_event)
    }
}

impl BlobHandler {
    pub fn new(config: BackendConfig, handle: StorageHandle) -> Result<BlobHandle, BlobLibError> {
        let (blob_handle, rx) = BlobHandle::new();

        thread::spawn(async move || {
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
            match receiver.recv() {
                Ok((effect, response_tx)) => {
                    let event = match effect {
                        BlobEffect::GetOperator { bucket } => {
                            emit_backend_operator(bucket, self.backend_config.clone())
                        }
                        BlobEffect::Write {
                            operator,
                            path,
                            blob,
                        } => self.write_blob(operator, &path, blob).await,
                        BlobEffect::Read { operator, path } => {
                            self.read_blob(operator, &path).await
                        }
                        BlobEffect::ReadRange {
                            operator,
                            path,
                            range,
                        } => self.read_blob_range(operator, &path, range).await,
                        BlobEffect::Delete { operator, path } => {
                            self.delete_blob(operator, &path).await
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

    pub async fn write_blob(
        &self,
        operator: Operator,
        path: &str,
        mut blob: BackendStream<Result<Bytes, BoxError>>,
    ) -> BlobEvent {
        // Just write the StreamingBlob to the backend
        let mut hasher = Hasher::new();
        let mut bytes_written: u64 = 0;

        while let Some(Ok(bytes)) = blob.next().await {
            hasher.update(&bytes);
            if let Err(e) = operator.write(path, bytes.to_vec()).await {
                return BlobEvent::Error { error: e.into() };
            }
            bytes_written += bytes.len() as u64;
        }

        BlobEvent::WriteFinished {
            backend_path: path.to_string(),
            bytes_written,
            hashes: hasher.to_map(),
            //checksums: Default::default(),
        }
    }

    pub async fn read_blob(&self, operator: Operator, path: &str) -> BlobEvent {
        let reader = match operator.reader(path).await {
            Ok(r) => match r.into_bytes_stream(..).await {
                Ok(stream) => stream,
                Err(e) => return BlobEvent::Error { error: e.into() },
            },
            Err(e) => return BlobEvent::Error { error: e.into() },
        };

        BlobEvent::ReadFinished {
            blob: BackendStream::new(reader),
            stream_size: 0, //TODO
        }
    }

    pub async fn read_blob_range(
        &self,
        operator: Operator,
        path: &str,
        range: impl RangeBounds<u64>,
    ) -> BlobEvent {
        let reader = match operator.reader(path).await {
            Ok(r) => match r.into_bytes_stream(range).await {
                Ok(stream) => stream,
                Err(e) => return BlobEvent::Error { error: e.into() },
            },
            Err(e) => return BlobEvent::Error { error: e.into() },
        };

        BlobEvent::ReadFinished {
            blob: BackendStream::new(reader),
            stream_size: 0, //TODO
        }
    }

    pub async fn delete_blob(&self, operator: Operator, path: &str) -> BlobEvent {
        match operator.delete(path).await {
            Ok(_) => {}
            Err(e) => return BlobEvent::Error { error: e.into() },
        }
        BlobEvent::DeleteFinished
    }

    async fn evaluate_backend_bucket(&self) -> Result<String, BlobLibError> {
        let buckets: HashMap<String, u64> =
            if let Event::Storage(StorageEvent::IterResult { values, .. }) = self
                .storage
                .send_effect(Effect::Storage(StorageEffect::Iter {
                    key_space: "buckets".to_string(),
                    prefix: self
                        .backend_config
                        .bucket_prefix
                        .map(|prefix| prefix.into()),
                    start_after: None,
                    limit: 0,
                    txn_id: None,
                }))
                .await
            {
                values
                    .into_iter()
                    .filter_map(|(k, v)| {
                        let key = String::from_utf8(k.to_vec()).ok()?;
                        let value = String::from_utf8(v.to_vec()).ok()?.parse::<u64>().ok()?;
                        Some((key, value))
                    })
                    .collect()
            } else {
                HashMap::new()
            };

        if let Some(bucket_max_size) = self.backend_config.max_bucket_size {
            // Find bucket with fewer objects than max bucket size
            for (bucket, load) in buckets {
                if load < bucket_max_size {
                    return Ok(bucket.to_string());
                }
            }
        } else if let Some((bucket, _)) = buckets.into_iter().next() {
            // Take first bucket if no size limit
            return Ok(bucket);
        }

        // No suitable bucket found, create new one
        Ok(generate_bucket_name())
    }
}

fn generate_bucket_name() -> String {
    format!("aruna-{}", Ulid::new().to_string().to_lowercase())
}
