use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    S3_LOOKUP_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE, S3_VERSION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendLocation, Location, LookupKey, MultipartChecksumType, MultipartObjectMetadataKey,
    MultipartObjectSummary, VersionKey, VersionMetadata,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeadObjectState {
    Init,
    StartTransaction,
    GetVersion,
    GetLookup,
    ResolveVersion,
    ReadMultipartSummary,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum HeadObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: HeadObjectState,
        expected: HeadObjectState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: HeadObjectState,
        expected: &'static str,
        received: Event,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("The specified key does not exist.")]
    NoSuchKey,
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("The specified version is a delete marker.")]
    DeleteMarker,
    #[error("HeadObject failed")]
    HeadObjectFailed,
}

#[derive(Debug, PartialEq)]
pub struct HeadObjectInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeadObjectResult {
    pub location: BackendLocation,
    pub version_id: Option<Ulid>,
    pub resolved_version_id: Option<Ulid>,
    pub checksum_type: MultipartChecksumType,
}

#[derive(Debug, PartialEq)]
pub struct HeadObjectOperation {
    input: HeadObjectInput,
    state: HeadObjectState,
    txn_id: Option<Ulid>,
    location: Option<BackendLocation>,
    resolved_version_id: Option<Ulid>,
    checksum_type: MultipartChecksumType,
    output: Option<Result<HeadObjectResult, HeadObjectError>>,
}

impl HeadObjectOperation {
    pub fn new(input: HeadObjectInput) -> Self {
        Self {
            input,
            state: HeadObjectState::Init,
            txn_id: None,
            location: None,
            resolved_version_id: None,
            checksum_type: MultipartChecksumType::FullObject,
            output: None,
        }
    }

    fn emit_error(&mut self, error: HeadObjectError) -> Effects {
        self.state = HeadObjectState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        if self.state != HeadObjectState::Init {
            self.emit_error(HeadObjectError::InvalidState {
                current: self.state.clone(),
                expected: HeadObjectState::Init,
            })
        } else {
            self.state = HeadObjectState::StartTransaction;
            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        }
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event {
            self.txn_id = Some(txn_id);
            if let Some(version_id) = self.input.version_id {
                self.state = HeadObjectState::GetVersion;
                let key = match VersionKey::new(&self.input.bucket, &self.input.key, version_id)
                    .to_bytes()
                {
                    Ok(key) => key.into(),
                    Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
                };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_VERSION_KEYSPACE.to_string(),
                    key,
                    txn_id: self.txn_id,
                })]
            } else {
                self.state = HeadObjectState::GetLookup;
                let key = match LookupKey::object(&self.input.bucket, &self.input.key).to_bytes() {
                    Ok(key) => key.into(),
                    Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
                };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_LOOKUP_KEYSPACE.to_string(),
                    key,
                    txn_id: self.txn_id,
                })]
            }
        } else {
            self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            })
        }
    }

    fn handle_received_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(HeadObjectError::NoSuchVersion);
        };

        let metadata = match VersionMetadata::from_bytes(val.as_ref()) {
            Ok(metadata) => metadata,
            Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
        };

        let location = match metadata.location {
            Location::Real(location) => location,
            Location::Deleted => return self.emit_error(HeadObjectError::DeleteMarker),
        };

        self.read_multipart_summary(location, Some(metadata.version_id))
    }

    fn handle_received_lookup(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(HeadObjectError::NoSuchKey);
        };

        let location = match Location::from_bytes(val.as_ref()) {
            Ok(Location::Real(location)) => location,
            Ok(Location::Deleted) => return self.emit_error(HeadObjectError::NoSuchKey),
            Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(HeadObjectError::NoTransactionFound);
        };
        let prefix = match VersionKey::object_prefix(&self.input.bucket, &self.input.key) {
            Ok(prefix) => prefix.into(),
            Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
        };

        self.location = Some(location);
        self.state = HeadObjectState::ResolveVersion;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_VERSION_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start_after: None,
            limit: 10_000,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_resolved_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let resolved_version_id = values
            .into_iter()
            .filter_map(|(key, value)| {
                let version_key = VersionKey::from_bytes(key.as_ref()).ok()?;
                match VersionMetadata::from_bytes(value.as_ref()).ok()?.location {
                    Location::Real(_) => Some(version_key.version_id),
                    Location::Deleted => None,
                }
            })
            .max();

        let Some(location) = self.location.clone() else {
            return self.emit_error(HeadObjectError::HeadObjectFailed);
        };

        self.read_multipart_summary(location, resolved_version_id)
    }

    fn read_multipart_summary(
        &mut self,
        location: BackendLocation,
        resolved_version_id: Option<Ulid>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(HeadObjectError::NoTransactionFound);
        };

        self.location = Some(location);
        self.resolved_version_id = resolved_version_id;

        let Some(version_id) = resolved_version_id else {
            return self.finish_lookup();
        };

        let key = match MultipartObjectMetadataKey::summary(version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
        };

        self.state = HeadObjectState::ReadMultipartSummary;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_multipart_summary_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        self.checksum_type = value
            .and_then(|value| MultipartObjectSummary::from_bytes(value.as_ref()).ok())
            .map(|summary| summary.checksum_type)
            .unwrap_or(MultipartChecksumType::FullObject);

        self.finish_lookup()
    }

    fn finish_lookup(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(HeadObjectError::NoTransactionFound);
        };
        let Some(location) = self.location.clone() else {
            return self.emit_error(HeadObjectError::HeadObjectFailed);
        };

        self.state = HeadObjectState::CommitTransaction;
        self.output = Some(Ok(HeadObjectResult {
            location,
            version_id: self.input.version_id,
            resolved_version_id: self.resolved_version_id,
            checksum_type: self.checksum_type,
        }));

        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event {
            self.state = HeadObjectState::Finish;
            smallvec![]
        } else {
            self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            })
        }
    }
}

impl Operation for HeadObjectOperation {
    type Output = Option<Result<HeadObjectResult, HeadObjectError>>;
    type Error = HeadObjectError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(HeadObjectError::StorageError(error));
        }

        match self.state {
            HeadObjectState::Init => self.handle_init(),
            HeadObjectState::StartTransaction => self.handle_transaction_started(event),
            HeadObjectState::GetVersion => self.handle_received_version(event),
            HeadObjectState::GetLookup => self.handle_received_lookup(event),
            HeadObjectState::ResolveVersion => self.handle_resolved_version(event),
            HeadObjectState::ReadMultipartSummary => self.handle_multipart_summary_read(event),
            HeadObjectState::CommitTransaction => self.handle_transaction_committed(event),
            HeadObjectState::Finish | HeadObjectState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, HeadObjectState::Finish | HeadObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if HeadObjectState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(HeadObjectError::HeadObjectFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::structs::BackendConfig;
    use aruna_core::structs::BackendLocation;
    use aruna_core::structs::checksum::HASH_MD5;
    use aruna_core::structs::{Backend, VersionMetadata};
    use aruna_net::{NetConfig, NetHandle};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn location_with_hash() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert(HASH_MD5.to_string(), vec![1; 16]);
        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "mybucket".to_string(),
            backend_path: "hello.txt".to_string(),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_at: SystemTime::now(),
            created_by: Default::default(),
            staging: false,
            partial: false,
            blob_size: 5,
            hashes,
        }
    }

    #[tokio::test]
    async fn head_object_reads_lookup_location() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let location = location_with_hash();
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };

        let key = LookupKey::object("mybucket", "hello.txt")
            .to_bytes()
            .unwrap();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_LOOKUP_KEYSPACE.to_string(),
                key: key.into(),
                value: Location::Real(location.clone()).to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let result = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.location, location);
        assert_eq!(result.version_id, None);
        assert_eq!(result.checksum_type, MultipartChecksumType::FullObject);
    }

    #[tokio::test]
    async fn head_object_reads_specific_version() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let location = location_with_hash();
        let version_id = Ulid::new();
        let metadata = VersionMetadata {
            version_id,
            location: Location::Real(location.clone()),
            created_at: SystemTime::now(),
            created_by: Default::default(),
        };

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };

        let key = VersionKey::new("mybucket", "hello.txt", version_id)
            .to_bytes()
            .unwrap();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                key: key.into(),
                value: metadata.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let result = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: Some(version_id),
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.location, location);
        assert_eq!(result.version_id, Some(version_id));
        assert_eq!(result.checksum_type, MultipartChecksumType::FullObject);
    }
}
