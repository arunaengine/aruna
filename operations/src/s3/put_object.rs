use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, add_hash_path_index_effect, write_blob_head_effect,
    write_blob_location_effect, write_blob_version_effect,
};
use aruna_core::effects::{BlobEffect, DhtEffect, Effect, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::{
    BackendLocation, BlobHeadKey, BlobVersion, CurrentVersionPointer, RealmId, VersionKey,
    VersionSourceBinding,
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
    CreateBlobLocation,
    ReadObjectLookup,
    ReadPreviousVersion,
    WriteBlobHead,
    WriteHashPathIndex,
    CreateBlobVersionRecord,
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
    existing_pointer: Option<CurrentVersionPointer>,
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
            existing_pointer: None,
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
                    smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                        key: blake3_hash.to_vec().into(),
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
                let existing_location = match BackendLocation::from_bytes(value.as_ref()) {
                    Ok(location) => location,
                    Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
                };

                if existing_location != written_location {
                    self.cleanup_location = Some(written_location);
                }
                self.output = Some(Ok(existing_location));
                self.create_blob_location()
            }
            None => {
                self.output = Some(Ok(written_location.clone()));
                self.create_blob_location()
            }
        }
    }

    fn create_blob_location(&mut self) -> Effects {
        self.state = PutObjectState::CreateBlobLocation;
        let Some(location) = self.get_output().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.emit_error(PutObjectError::MissingHash("blake3".to_string()));
        };

        let effect = match write_blob_location_effect(
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err.into())),
            },
            location,
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };
        smallvec![effect]
    }

    fn alias_context(&self) -> HeadAliasContext {
        HeadAliasContext::new(
            self.config.realm_id,
            self.config.group_id,
            self.config.node_id,
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
        )
    }

    fn create_object_lookup(&mut self) -> Effects {
        let Some(_output) = self.get_output().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };

        self.state = PutObjectState::ReadObjectLookup;
        let key = match BlobHeadKey::new(
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
        )
        .to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
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
        self.existing_pointer = existing;
        if let Some(existing_pointer) = self.existing_pointer.as_ref() {
            let version_key = match VersionKey::new(
                self.config.request.bucket.clone(),
                self.config.request.key.clone(),
                existing_pointer.version_id,
            )
            .to_bytes()
            {
                Ok(key) => key,
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
            };

            let version_value = match self.txn_id {
                Some(txn_id) => smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: version_key.into(),
                    txn_id: Some(txn_id),
                })],
                None => return self.emit_error(PutObjectError::NoTransactionFound),
            };
            self.state = PutObjectState::ReadPreviousVersion;
            return version_value;
        }

        self.write_current_lookup(None)
    }

    fn handle_previous_version_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };

        let _ = value;

        let existing_pointer = self.existing_pointer.clone();
        self.write_current_lookup(existing_pointer.as_ref())
    }

    fn write_current_lookup(&mut self, existing: Option<&CurrentVersionPointer>) -> Effects {
        let version_id = *self.version_id.get_or_insert_with(Ulid::new);
        let pointer = CurrentVersionPointer::next_for(existing, version_id);
        let effect = match write_blob_head_effect(&self.alias_context(), pointer, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };

        self.state = PutObjectState::WriteBlobHead;
        smallvec![effect]
    }

    fn handle_blob_location_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.create_object_lookup()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_blob_head_written(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.write_hash_path_index()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn write_hash_path_index(&mut self) -> Effects {
        let Some(location) = self.get_output().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.emit_error(PutObjectError::MissingHash("blake3".to_string()));
        };
        let effect = match add_hash_path_index_effect(
            &self.alias_context(),
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err.into())),
            },
            match self.version_id {
                Some(version_id) => version_id,
                None => return self.emit_error(PutObjectError::PutObjectFailed),
            },
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };
        self.state = PutObjectState::WriteHashPathIndex;
        smallvec![effect]
    }

    fn handle_hash_path_index_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            let Some(version_id) = self.version_id else {
                return self.emit_error(PutObjectError::PutObjectFailed);
            };
            let Some(output) = self.get_output().cloned() else {
                return self.emit_error(PutObjectError::MissingOutput);
            };
            let Some(blake3_hash) = output.get_blake3() else {
                return self.emit_error(PutObjectError::MissingHash("blake3".to_string()));
            };
            let version = BlobVersion::materialized(
                match blake3_hash.try_into() {
                    Ok(hash) => hash,
                    Err(err) => {
                        return self.emit_error(PutObjectError::ConversionError(err.into()));
                    }
                },
                output.created_at,
                output.created_by,
                self.config.version_source.clone(),
            );
            let version_key = VersionKey::new(
                self.config.request.bucket.clone(),
                self.config.request.key.clone(),
                version_id,
            );
            let effect = match write_blob_version_effect(&version_key, &version, self.txn_id) {
                Ok(effect) => effect,
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
            };
            self.state = PutObjectState::CreateBlobVersionRecord;
            smallvec![effect]
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_blob_version_record_created(&mut self, event: Event) -> Effects {
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
            PutObjectState::CreateBlobLocation => self.handle_blob_location_created(event),
            PutObjectState::ReadObjectLookup => self.handle_object_lookup_read(event),
            PutObjectState::ReadPreviousVersion => self.handle_previous_version_read(event),
            PutObjectState::WriteBlobHead => self.handle_blob_head_written(event),
            PutObjectState::WriteHashPathIndex => self.handle_hash_path_index_created(event),
            PutObjectState::CreateBlobVersionRecord => {
                self.handle_blob_version_record_created(event)
            }
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
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DHT_KEYSPACE,
        HASH_PATHS_INDEX_KEYSPACE,
    };
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
    use aruna_core::structs::{
        Backend, BackendConfig, BackendLocation, BlobHeadKey, BlobVersion, CurrentVersionPointer,
        HashPathIndexKey, RealmId, VersionKey,
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

    async fn read_value(
        context: &DriverContext,
        key_space: &str,
        key: Vec<u8>,
    ) -> Option<aruna_core::types::Value> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };

        value
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
        let group_id = Ulid::new();
        let node_id = net_handle.node_id();
        let put_config = PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
            group_id,
            realm_id,
            node_id,
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

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(blob_location_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: result.location.get_blake3().unwrap().to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing blob location entry");
        };
        assert_eq!(
            BackendLocation::from_bytes(blob_location_value.as_ref()).unwrap(),
            result.location.clone()
        );

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(blob_head_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("mybucket", "some-file.txt")
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing blob head entry");
        };
        assert_eq!(
            CurrentVersionPointer::from_bytes(blob_head_value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(result.version_id, 1)
        );

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(blob_version_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("mybucket", "some-file.txt", result.version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing blob version entry");
        };
        let blob_version = BlobVersion::from_bytes(blob_version_value.as_ref()).unwrap();
        assert!(blob_version.is_materialized());
        assert_eq!(
            blob_version.blob_hash(),
            Some(&result.location.get_blake3().unwrap().try_into().unwrap())
        );

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(hash_path_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: HASH_PATHS_INDEX_KEYSPACE.to_string(),
                key: HashPathIndexKey::new(
                    result.location.get_blake3().unwrap().try_into().unwrap(),
                    result.version_id,
                    realm_id,
                    group_id,
                    node_id,
                    "mybucket",
                    "some-file.txt",
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing hash path index entry");
        };
        assert!(hash_path_value.is_empty());

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
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::new();
        let node_id = context.net_handle.as_ref().unwrap().node_id();

        let first = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
                group_id,
                realm_id,
                node_id,
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
                user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
                group_id,
                realm_id,
                node_id,
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
        let blob_hash: [u8; 32] = first.location.get_blake3().unwrap().try_into().unwrap();

        let blob_location_value = read_value(&context, BLOB_LOCATIONS_KEYSPACE, blob_hash.to_vec())
            .await
            .expect("missing blob location entry");
        assert_eq!(
            BackendLocation::from_bytes(blob_location_value.as_ref()).unwrap(),
            first.location.clone()
        );

        for key in ["first.txt", "second.txt"] {
            let expected_version_id = if key == "first.txt" {
                first.version_id
            } else {
                second.version_id
            };

            let blob_head_value = read_value(
                &context,
                BLOB_HEAD_KEYSPACE,
                BlobHeadKey::new("mybucket", key).to_bytes().unwrap(),
            )
            .await
            .expect("missing blob head entry");
            assert_eq!(
                CurrentVersionPointer::from_bytes(blob_head_value.as_ref()).unwrap(),
                CurrentVersionPointer::new_with_generation(expected_version_id, 1)
            );

            let blob_version_value = read_value(
                &context,
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new("mybucket", key, expected_version_id)
                    .to_bytes()
                    .unwrap(),
            )
            .await
            .expect("missing blob version entry");
            let blob_version = BlobVersion::from_bytes(blob_version_value.as_ref()).unwrap();
            assert!(blob_version.is_materialized());
            assert_eq!(blob_version.blob_hash(), Some(&blob_hash));

            let hash_path_value = read_value(
                &context,
                HASH_PATHS_INDEX_KEYSPACE,
                HashPathIndexKey::new(
                    blob_hash,
                    expected_version_id,
                    realm_id,
                    group_id,
                    node_id,
                    "mybucket",
                    key,
                )
                .to_bytes()
                .unwrap(),
            )
            .await
            .expect("missing hash path index entry");
            assert!(hash_path_value.is_empty());
        }
    }

    #[test]
    fn put_object_current_pointer_generation_increments_from_existing_pointer() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut op = PutObjectOperation::new(PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
            group_id: Ulid::new(),
            realm_id,
            node_id: iroh::SecretKey::generate().public(),
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
        let previous_version_id = existing.version_id;

        let effects = op.handle_object_lookup_read(Event::Storage(StorageEvent::ReadResult {
            key: vec![0].into(),
            value: Some(existing.to_bytes().unwrap().into()),
        }));

        let [Effect::Storage(StorageEffect::Read { key_space, key, .. })] = effects.as_slice()
        else {
            panic!("expected previous version read")
        };
        assert_eq!(key_space, BLOB_VERSIONS_KEYSPACE);
        assert_eq!(
            VersionKey::from_bytes(key.as_ref()).unwrap(),
            VersionKey::new("mybucket", "some-file.txt", previous_version_id)
        );

        let effects = op.handle_previous_version_read(Event::Storage(StorageEvent::ReadResult {
            key: vec![0].into(),
            value: Some(
                BlobVersion::deleted(std::time::SystemTime::now(), op.config.user_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
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
    pub async fn test_put_object_overwrite_retains_historical_hash_path_index() {
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

        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::new();
        let node_id = context.net_handle.as_ref().unwrap().node_id();

        let first = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "same-key.txt".to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"first"[..],
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
                user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "same-key.txt".to_string(),
                    content_length: Some(6),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"second"[..],
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

        assert_ne!(first.location, second.location);
        assert_eq!(count_files(Path::new(&blob_root)), 2);

        let first_hash: [u8; 32] = first.location.get_blake3().unwrap().try_into().unwrap();
        let second_hash: [u8; 32] = second.location.get_blake3().unwrap().try_into().unwrap();

        let current_blob_head = read_value(
            &context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("mybucket", "same-key.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob head entry");
        assert_eq!(
            CurrentVersionPointer::from_bytes(current_blob_head.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(second.version_id, 2)
        );

        let historical_hash_path = read_value(
            &context,
            HASH_PATHS_INDEX_KEYSPACE,
            HashPathIndexKey::new(
                first_hash,
                first.version_id,
                realm_id,
                group_id,
                node_id,
                "mybucket",
                "same-key.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .expect("missing historical hash path entry");
        assert!(historical_hash_path.is_empty());

        let new_hash_path = read_value(
            &context,
            HASH_PATHS_INDEX_KEYSPACE,
            HashPathIndexKey::new(
                second_hash,
                second.version_id,
                realm_id,
                group_id,
                node_id,
                "mybucket",
                "same-key.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .expect("missing replacement hash path entry");
        assert!(new_hash_path.is_empty());

        let first_blob_version = read_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "same-key.txt", first.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing first blob version");
        assert_eq!(
            BlobVersion::from_bytes(first_blob_version.as_ref())
                .unwrap()
                .blob_hash(),
            Some(&first_hash)
        );

        let second_blob_version = read_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "same-key.txt", second.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing second blob version");
        assert_eq!(
            BlobVersion::from_bytes(second_blob_version.as_ref())
                .unwrap()
                .blob_hash(),
            Some(&second_hash)
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
