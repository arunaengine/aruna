use aruna_core::effects::{BlobEffect, DhtEffect, Effect, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::{S3_CURRENT_VERSION_KEYSPACE, S3_LOOKUP_KEYSPACE, S3_VERSION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::{
    BackendLocation, CurrentVersionPointer, Location, LookupKey, RealmId, VersionKey,
    VersionMetadata, VersionSourceBinding,
};
use aruna_core::types::{Effects, GroupId, NodeId, UserId};
use bytes::Bytes;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Eq, PartialEq)]
pub enum PutObjectState {
    Init,
    WriteBlob,
    CleanupFailedWrite,
    StartTransaction,
    CheckHashLookup,
    CreateHashLookup,
    ReadObjectLookup,
    CreateObjectLookup,
    CreateVersionRecord,
    CommitTransaction,
    RegisterBlobInDht,
    CleanupDuplicate,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("output is missing")]
    MissingOutput,
    #[error("hash missing: {0}")]
    MissingHash(String),
    #[error("request body missing")]
    MissingBody,
    #[error("body size did not match Content-Length header")]
    IncompleteBody,
    #[error("missing stored checksum for {0}")]
    MissingExpectedChecksum(&'static str),
    #[error("checksum mismatch for {0}")]
    ChecksumMismatch(&'static str),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Something went wrong ...")]
    PutObjectFailed,
}

#[derive(Debug, PartialEq)]
pub struct PutObjectInput {
    pub bucket: String,
    pub key: String,
    pub content_length: Option<u64>,
    pub body: Option<BackendStream<Result<Bytes, StreamError>>>,
}

#[derive(Debug, PartialEq)]
pub struct PutObjectConfig {
    pub user_id: UserId,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub request: PutObjectInput,
    pub expected_checksums: Vec<ExpectedChecksum>,
    pub checksum_type: Option<String>,
    pub exists: bool, //Note: For version shenanigans which will be implemented later
    pub version_source: Option<VersionSourceBinding>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PutObjectResult {
    pub location: BackendLocation,
    pub version_id: Ulid,
}

#[derive(Debug, PartialEq)]
pub struct PutObjectOperation {
    state: PutObjectState,
    config: PutObjectConfig,
    txn_id: Option<Ulid>,
    version_id: Option<Ulid>,
    written_location: Option<BackendLocation>,
    cleanup_location: Option<BackendLocation>,
    pending_error: Option<PutObjectError>,
    output: Option<Result<BackendLocation, PutObjectError>>,
}

impl PutObjectOperation {
    pub fn new(config: PutObjectConfig) -> Self {
        PutObjectOperation {
            state: PutObjectState::Init,
            config,
            txn_id: None,
            version_id: None,
            written_location: None,
            cleanup_location: None,
            pending_error: None,
            output: None,
        }
    }

    fn handle_init(&mut self) -> Effects {
        self.state = PutObjectState::WriteBlob;
        if let Some(blob) = self.config.request.body.take() {
            smallvec![Effect::Blob(BlobEffect::Write {
                bucket: self.config.request.bucket.clone(),
                key: self.config.request.key.clone(),
                created_by: self.config.user_id,
                blob
            })]
        } else {
            self.emit_error(PutObjectError::MissingBody)
        }
    }

    fn handle_write_finished(&mut self, event: Event) -> Effects {
        if let Event::Blob(BlobEvent::WriteFinished { location }) = event {
            self.written_location = Some(location.clone());

            // Check if the body was fully written
            if self
                .config
                .request
                .content_length
                .is_some_and(|expected| location.blob_size != expected)
            {
                return self.cleanup_failed_write(PutObjectError::IncompleteBody);
            }

            for expected in &self.config.expected_checksums {
                let Some(actual) = location.hashes.get(expected.algorithm.hash_key()) else {
                    return self.cleanup_failed_write(PutObjectError::MissingExpectedChecksum(
                        expected.algorithm.s3_name(),
                    ));
                };

                if actual != &expected.digest {
                    return self.cleanup_failed_write(PutObjectError::ChecksumMismatch(
                        expected.algorithm.s3_name(),
                    ));
                }
            }

            self.state = PutObjectState::StartTransaction;
            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event {
            self.txn_id = Some(txn_id);
            self.state = PutObjectState::CheckHashLookup;

            if let Some(written_location) = self.get_written_location() {
                if let Some(blake3_hash) = written_location.get_blake3() {
                    let key = match LookupKey::from_blake3_hash(blake3_hash)
                        .and_then(|key| key.to_bytes())
                    {
                        Ok(key) => key.into(),
                        Err(e) => return self.emit_error(PutObjectError::ConversionError(e)),
                    };
                    smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: S3_LOOKUP_KEYSPACE.to_string(),
                        key,
                        txn_id: self.txn_id,
                    })]
                } else {
                    self.emit_error(PutObjectError::MissingHash("blake3".to_string()))
                }
            } else {
                self.emit_error(PutObjectError::MissingOutput)
            }
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_hash_lookup_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };

        let Some(written_location) = self.get_written_location().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };

        match value {
            Some(value) => {
                let existing_location = match Location::from_bytes(value.as_ref()) {
                    Ok(Location::Real(location)) => location,
                    Ok(Location::Deleted) => {
                        self.output = Some(Ok(written_location.clone()));
                        return self.create_hash_lookup(written_location);
                    }
                    Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
                };

                if existing_location != written_location {
                    self.cleanup_location = Some(written_location);
                }
                self.output = Some(Ok(existing_location));
                self.create_object_lookup()
            }
            None => {
                self.output = Some(Ok(written_location.clone()));
                self.create_hash_lookup(written_location)
            }
        }
    }

    fn create_hash_lookup(&mut self, location: BackendLocation) -> Effects {
        self.state = PutObjectState::CreateHashLookup;
        let Some(blake3_hash) = location.get_blake3() else {
            return self.emit_error(PutObjectError::MissingHash("blake3".to_string()));
        };

        let key = match LookupKey::from_blake3_hash(blake3_hash).and_then(|key| key.to_bytes()) {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };
        let value = match Location::Real(location).to_bytes() {
            Ok(bytes) => bytes.into(),
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };

        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_LOOKUP_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })]
    }

    fn create_object_lookup(&mut self) -> Effects {
        let Some(_output) = self.get_output().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };

        self.state = PutObjectState::ReadObjectLookup;
        let key = match LookupKey::object(
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
        )
        .to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_object_lookup_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };

        let existing = match value
            .as_ref()
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(existing) => existing,
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };
        let version_id = *self.version_id.get_or_insert_with(Ulid::new);

        self.state = PutObjectState::CreateObjectLookup;
        let key = match LookupKey::object(
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
        )
        .to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };
        let value = match CurrentVersionPointer::next_for(existing.as_ref(), version_id).to_bytes()
        {
            Ok(bytes) => bytes.into(),
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };

        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })]
    }

    fn handle_hash_lookup_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.create_object_lookup()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_object_lookup_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            let Some(output) = self.get_output().cloned() else {
                return self.emit_error(PutObjectError::MissingOutput);
            };
            let Some(version_id) = self.version_id else {
                return self.emit_error(PutObjectError::PutObjectFailed);
            };
            self.state = PutObjectState::CreateVersionRecord;

            let key = match VersionKey::new(
                self.config.request.bucket.clone(),
                self.config.request.key.clone(),
                version_id,
            )
            .to_bytes()
            {
                Ok(key) => key.into(),
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
            };
            let value = match VersionMetadata::materialized(
                version_id,
                output.clone(),
                output.created_at,
                output.created_by,
                self.config.version_source.clone(),
            )
            .to_bytes()
            {
                Ok(bytes) => bytes.into(),
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
            };

            smallvec![Effect::Storage(StorageEffect::Write {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                key,
                value,
                txn_id: self.txn_id,
            })]
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_version_record_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            if let Some(txn_id) = self.txn_id {
                self.state = PutObjectState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            } else {
                self.emit_error(PutObjectError::NoTransactionFound)
            }
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event {
            self.txn_id = None;
            self.register_blob_in_dht_or_continue()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn register_blob_in_dht_or_continue(&mut self) -> Effects {
        let Some(location) = self.get_output() else {
            return self.continue_after_dht_registration();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.continue_after_dht_registration();
        };
        let key: [u8; 32] = match blake3_hash.try_into() {
            Ok(key) => key,
            Err(_) => return self.continue_after_dht_registration(),
        };

        self.state = PutObjectState::RegisterBlobInDht;
        smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Put {
            key,
            realm_id: self.config.realm_id,
            value: self.config.node_id.as_bytes().to_vec(),
            ttl: Default::default(),
        }))]
    }

    fn handle_blob_registered_in_dht(&mut self, event: Event) -> Effects {
        match event {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
            | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
            | Event::Net(NetEvent::Error(_)) => self.continue_after_dht_registration(),
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn continue_after_dht_registration(&mut self) -> Effects {
        if let Some(location) = self.cleanup_location.take() {
            self.state = PutObjectState::CleanupDuplicate;
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.state = PutObjectState::Finish;
            smallvec![]
        }
    }

    fn handle_duplicate_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.state = PutObjectState::Finish;
                smallvec![]
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn emit_finish(&mut self) -> Effects {
        self.state = PutObjectState::Finish;
        smallvec![]
    }

    fn cleanup_failed_write(&mut self, error: PutObjectError) -> Effects {
        self.pending_error = Some(error);
        self.state = PutObjectState::CleanupFailedWrite;

        self.get_written_location().cloned().map_or_else(
            || self.emit_pending_error(),
            |location| smallvec![Effect::Blob(BlobEffect::Delete { location })],
        )
    }

    fn handle_failed_write_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.emit_pending_error()
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn emit_pending_error(&mut self) -> Effects {
        let Some(error) = self.pending_error.take() else {
            return self.emit_error(PutObjectError::PutObjectFailed);
        };
        self.emit_error(error)
    }

    fn emit_error(&mut self, error: PutObjectError) -> Effects {
        self.state = PutObjectState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn get_output(&self) -> Option<&BackendLocation> {
        self.output.as_ref()?.as_ref().ok()
    }

    fn get_written_location(&self) -> Option<&BackendLocation> {
        self.written_location.as_ref()
    }
}

impl Operation for PutObjectOperation {
    type Output = Option<Result<PutObjectResult, PutObjectError>>;
    type Error = PutObjectError;

    fn start(&mut self) -> Effects {
        if self.state != PutObjectState::Init {
            self.emit_error(PutObjectError::InvalidOperationState)
        } else {
            self.handle_init()
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match &self.state {
            PutObjectState::Init => self.handle_init(),
            PutObjectState::WriteBlob => self.handle_write_finished(event),
            PutObjectState::CleanupFailedWrite => self.handle_failed_write_cleanup(event),
            PutObjectState::StartTransaction => self.handle_transaction_started(event),
            PutObjectState::CheckHashLookup => self.handle_hash_lookup_checked(event),
            PutObjectState::CreateHashLookup => self.handle_hash_lookup_created(event),
            PutObjectState::ReadObjectLookup => self.handle_object_lookup_read(event),
            PutObjectState::CreateObjectLookup => self.handle_object_lookup_created(event),
            PutObjectState::CreateVersionRecord => self.handle_version_record_created(event),
            PutObjectState::CommitTransaction => self.handle_transaction_committed(event),
            PutObjectState::RegisterBlobInDht => self.handle_blob_registered_in_dht(event),
            PutObjectState::CleanupDuplicate => self.handle_duplicate_cleanup(event),
            PutObjectState::Finish => self.emit_finish(),
            PutObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PutObjectState::Finish | PutObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if PutObjectState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(PutObjectError::PutObjectFailed);
        }
        Ok(self.output.map(|result| {
            result.and_then(|location| {
                self.version_id
                    .map(|version_id| PutObjectResult {
                        location,
                        version_id,
                    })
                    .ok_or(PutObjectError::PutObjectFailed)
            })
        }))
    }

    fn abort(&mut self) -> Effects {
        // Rollback blob io and transaction
        let mut actions = smallvec![];
        if let Some(output) = self.get_written_location() {
            actions.insert(
                0,
                Effect::Blob(BlobEffect::Delete {
                    location: output.clone(),
                }),
            )
        }
        if let Some(txn_id) = self.txn_id {
            actions.insert(
                1,
                Effect::Storage(StorageEffect::AbortTransaction { txn_id }),
            )
        }
        actions
    }
}

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        DHT_KEYSPACE, S3_CURRENT_VERSION_KEYSPACE, S3_LOOKUP_KEYSPACE, S3_VERSION_KEYSPACE,
    };
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
    use aruna_core::structs::{
        Backend, BackendConfig, BackendLocation, CurrentVersionPointer, Location, LookupKey,
        RealmId, VersionKey, VersionMetadata,
    };
    use aruna_net::dht::storage::decode_entries;
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::fs::{exists, read_to_string};
    use std::path::Path;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn count_files(path: &Path) -> usize {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .map(|path| if path.is_dir() { count_files(&path) } else { 1 })
            .sum()
    }

    #[tokio::test]
    pub async fn test_put_object() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root.clone(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let data = b"hello, world!";
        let stream = tokio_util::io::ReaderStream::new(&data[..]);
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let put_config = PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
            group_id: Ulid::new(),
            realm_id,
            node_id: net_handle.node_id(),
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: "some-file.txt".to_string(),
                content_length: Some(data.len() as u64),
                body: Some(BackendStream::new(stream)),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
        };
        let put_operation = PutObjectOperation::new(put_config);

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        // Jesus, Take the Wheel!
        let result = drive(put_operation, &context)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert!(exists(result.location.get_full_path().unwrap()).unwrap());
        assert_eq!(
            read_to_string(result.location.get_full_path().unwrap()).unwrap(),
            String::from_utf8_lossy(&data[..]).to_string()
        );

        let hash_lookup_key = LookupKey::from_blake3_hash(result.location.get_blake3().unwrap())
            .unwrap()
            .to_bytes()
            .unwrap();
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(hash_lookup_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_LOOKUP_KEYSPACE.to_string(),
                key: hash_lookup_key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing hash lookup entry");
        };
        assert_eq!(
            Location::from_bytes(hash_lookup_value.as_ref()).unwrap(),
            Location::Real(result.location.clone())
        );

        let object_lookup_key = LookupKey::object("mybucket", "some-file.txt")
            .to_bytes()
            .unwrap();
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(object_lookup_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                key: object_lookup_key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing object lookup entry");
        };
        assert_eq!(
            CurrentVersionPointer::from_bytes(object_lookup_value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(result.version_id, 1)
        );

        let version_prefix = VersionKey::object_prefix("mybucket", "some-file.txt").unwrap();
        let Event::Storage(StorageEvent::IterResult { values, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                prefix: Some(version_prefix.into()),
                start_after: None,
                limit: 10,
                txn_id: None,
            })
            .await
        else {
            panic!("missing version metadata entry");
        };
        assert_eq!(values.len(), 1);
        let version = VersionMetadata::from_bytes(values[0].1.as_ref()).unwrap();
        assert_eq!(version.version_id, result.version_id);
        assert_eq!(
            version.lookup_location(),
            Some(Location::Real(result.location.clone()))
        );

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(dht_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: DHT_KEYSPACE.to_string(),
                key: result.location.get_blake3().unwrap().to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing DHT blob registration");
        };
        let entries = decode_entries(dht_value.as_ref());
        assert!(entries.iter().any(|entry| {
            entry.realm_id == realm_id
                && entry.value
                    == context
                        .net_handle
                        .as_ref()
                        .unwrap()
                        .node_id()
                        .as_bytes()
                        .to_vec()
        }));
    }

    #[tokio::test]
    pub async fn test_put_object_dedup() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root.clone(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let data = b"hello, world!";

        let first = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::new(), RealmId::from_bytes([1u8; 32])),
                group_id: Ulid::new(),
                realm_id: RealmId::from_bytes([1u8; 32]),
                node_id: context.net_handle.as_ref().unwrap().node_id(),
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "first.txt".to_string(),
                    content_length: Some(data.len() as u64),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &data[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let second = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::new(), RealmId::from_bytes([1u8; 32])),
                group_id: Ulid::new(),
                realm_id: RealmId::from_bytes([1u8; 32]),
                node_id: context.net_handle.as_ref().unwrap().node_id(),
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "second.txt".to_string(),
                    content_length: Some(data.len() as u64),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &data[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(first.location, second.location);
        assert_eq!(count_files(Path::new(&blob_root)), 1);

        for key in ["first.txt", "second.txt"] {
            let object_lookup_key = LookupKey::object("mybucket", key).to_bytes().unwrap();
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(object_lookup_value),
                ..
            }) = context
                .storage_handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                    key: object_lookup_key.into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("missing object lookup entry for duplicate test");
            };

            let expected_version_id = if key == "first.txt" {
                first.version_id
            } else {
                second.version_id
            };

            assert_eq!(
                CurrentVersionPointer::from_bytes(object_lookup_value.as_ref()).unwrap(),
                CurrentVersionPointer::new_with_generation(expected_version_id, 1)
            );
        }
    }

    #[test]
    fn put_object_current_pointer_generation_increments_from_existing_pointer() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut op = PutObjectOperation::new(PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
            group_id: Ulid::new(),
            realm_id,
            node_id: iroh::SecretKey::generate(&mut rand::rng()).public(),
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: "some-file.txt".to_string(),
                content_length: None,
                body: None,
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
        });
        let version_id = Ulid::new();
        op.version_id = Some(version_id);
        op.output = Some(Ok(BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_by: op.config.user_id,
            created_at: std::time::SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: HashMap::new(),
        }));
        op.txn_id = Some(Ulid::new());
        let existing = CurrentVersionPointer::new_with_generation(Ulid::new(), 4);

        let effects = op.handle_object_lookup_read(Event::Storage(StorageEvent::ReadResult {
            key: vec![0].into(),
            value: Some(existing.to_bytes().unwrap().into()),
        }));

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected current pointer write")
        };
        assert_eq!(
            CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(version_id, 5)
        );
    }

    #[tokio::test]
    async fn test_put_object_checksum_mismatch_cleans_up_blob() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root.clone(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let data = b"hello, world!";
        let err = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::new(), RealmId::from_bytes([1u8; 32])),
                group_id: Ulid::new(),
                realm_id: RealmId::from_bytes([1u8; 32]),
                node_id: context.net_handle.as_ref().unwrap().node_id(),
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "bad.txt".to_string(),
                    content_length: Some(data.len() as u64),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &data[..],
                    ))),
                },
                expected_checksums: vec![ExpectedChecksum {
                    algorithm: ChecksumAlgorithm::Sha256,
                    digest: vec![0; 32],
                }],
                checksum_type: None,
                exists: false,
                version_source: None,
            }),
            &context,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            crate::s3::put_object::PutObjectError::ChecksumMismatch("SHA256")
        ));
        assert_eq!(count_files(Path::new(&blob_root)), 0);
    }
}
