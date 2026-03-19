use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::S3_LOOKUP_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{Location, LookupKey, UserIdentity};
use aruna_core::types::Effects;
use bytes::Bytes;
use smallvec::{SmallVec, smallvec};
use std::ops::Range;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetObjectState {
    Init,
    StartTransaction,
    GetLookup,
    CommitTransaction,
    GetBlob,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: GetObjectState,
        expected: GetObjectState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: GetObjectState,
        expected: &'static str,
        received: Event,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("The specified key does not exist.")]
    NoSuchKey,
    #[error("GetObject failed (miserably)")]
    GetObjectFailed,
}

#[derive(Debug, PartialEq)]
pub struct GetObjectInput {
    pub bucket: String,
    pub key: String,
    pub range: Option<Range<u64>>,
    pub group_id: Ulid,
    pub user_identity: UserIdentity,
    //TODO: tbc
}

#[derive(Debug, PartialEq)]
pub struct GetObjectOperation {
    input: GetObjectInput,
    state: GetObjectState,
    txn_id: Option<Ulid>,
    output: Option<Result<BackendStream<Result<Bytes, StreamError>>, GetObjectError>>,
}

impl GetObjectOperation {
    pub fn new(input: GetObjectInput) -> Self {
        GetObjectOperation {
            input,
            state: GetObjectState::Init,
            txn_id: None,
            output: None,
        }
    }

    pub fn emit_error(&mut self, error: GetObjectError) -> Effects {
        self.state = GetObjectState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    pub fn handle_init(&mut self) -> Effects {
        if self.state != GetObjectState::Init {
            self.emit_error(GetObjectError::InvalidState {
                current: self.state.clone(),
                expected: GetObjectState::Init,
            })
        } else {
            self.state = GetObjectState::StartTransaction;
            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        }
    }

    pub fn handle_transaction_started(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event {
            self.txn_id = Some(txn_id);
            self.state = GetObjectState::GetLookup;

            let key = match LookupKey::object(&self.input.bucket, &self.input.key).to_bytes() {
                Ok(key) => key.into(),
                Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
            };
            smallvec![Effect::Storage(StorageEffect::Read {
                key_space: S3_LOOKUP_KEYSPACE.to_string(),
                key,
                txn_id: self.txn_id,
            })]
        } else {
            self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            })
        }
    }

    pub fn handle_received_lookup(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(GetObjectError::NoSuchKey);
        };

        let location = match Location::from_bytes(val.as_ref()) {
            Ok(Location::Real(location)) => location,
            Ok(Location::Deleted) => return self.emit_error(GetObjectError::NoSuchKey),
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(GetObjectError::NoTransactionFound);
        };

        self.state = GetObjectState::CommitTransaction;

        smallvec![
            Effect::Storage(StorageEffect::CommitTransaction { txn_id }),
            Effect::Blob(BlobEffect::Read { location })
        ]
    }

    pub fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event {
            self.state = GetObjectState::GetBlob;
            smallvec![]
        } else {
            self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            })
        }
    }

    pub fn handle_received_blob(&mut self, event: Event) -> Effects {
        if let Event::Blob(BlobEvent::ReadFinished { blob, .. }) = event {
            self.state = GetObjectState::Finish;
            self.output = Some(Ok(blob));
            smallvec![]
        } else {
            self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Blob(BlobEvent::ReadFinished)",
                received: event,
            })
        }
    }
}

impl Operation for GetObjectOperation {
    type Output = Option<Result<BackendStream<Result<Bytes, StreamError>>, GetObjectError>>;
    type Error = GetObjectError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match &self.state {
            GetObjectState::Init => self.handle_init(),
            GetObjectState::StartTransaction => self.handle_transaction_started(event),
            GetObjectState::GetLookup => self.handle_received_lookup(event),
            GetObjectState::CommitTransaction => self.handle_transaction_committed(event),
            GetObjectState::GetBlob => self.handle_received_blob(event),
            GetObjectState::Finish => smallvec![],
            GetObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetObjectState::Finish | GetObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if GetObjectState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(GetObjectError::GetObjectFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use crate::s3::get_object::{GetObjectInput, GetObjectOperation, GetObjectState};
    use aruna_blob::blob::BlobHandler;
    use aruna_blob::hash::Hasher;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::S3_LOOKUP_KEYSPACE;
    use aruna_core::structs::{
        Backend, BackendConfig, BackendLocation, Location, LookupKey, RealmId, UserIdentity,
    };
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use std::path::Path;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_get_object() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();
        let content = "Hello, World!";
        let hasher = Hasher::new_with_bytes(content.as_bytes());
        let hashes = hasher.finalize();

        let bucket = "s3test".to_string();
        let key = "test.txt".to_string();
        let blob_ulid = Ulid::new();
        let location = BackendLocation {
            root: temp_root.to_string(),
            storage_bucket: format!("aruna_{}", Ulid::new()),
            backend_path: format!("{bucket}/{key}_{blob_ulid}"),
            ulid: blob_ulid,
            compressed: false,
            encrypted: false,
            created_by: Default::default(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: content.len() as u64,
            hashes: hasher.to_map(),
        };

        // Write file + db entries
        std::fs::create_dir_all(
            Path::new(&location.get_full_path().unwrap())
                .parent()
                .unwrap(),
        )
        .unwrap();
        std::fs::write(location.get_full_path().unwrap(), content).unwrap();

        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            let hash_lookup_key = LookupKey::from_blake3_hash(hashes.blake3.as_slice())
                .unwrap()
                .to_bytes()
                .unwrap();
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_LOOKUP_KEYSPACE.to_string(),
                    key: hash_lookup_key.into(),
                    value: Location::Real(location.clone()).to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;

            let object_lookup_key = LookupKey::object(&bucket, &key).to_bytes().unwrap();
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_LOOKUP_KEYSPACE.to_string(),
                    key: object_lookup_key.into(),
                    value: Location::Real(location.clone()).to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;

            let _ = storage_handle
                .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
                .await;
        } else {
            panic!("Failed to start transaction");
        }

        // Read file with operation
        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            automerge_handle: None,
            task_handle: None,
        };
        let operation = GetObjectOperation {
            input: GetObjectInput {
                bucket,
                key,
                range: None,
                group_id: Ulid::new(),
                user_identity: UserIdentity {
                    user_id: Default::default(),
                    realm_key: RealmId([0u8; 32]),
                },
            },
            state: GetObjectState::Init,
            txn_id: None,
            output: None,
        };

        let mut blob_stream = drive(operation, &driver_ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut read_buffer = Vec::new();
        while let Some(Ok(bytes)) = blob_stream.next().await {
            read_buffer.extend_from_slice(&bytes);
        }
        assert_eq!(read_buffer, content.as_bytes());
    }
}
