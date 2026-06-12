use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer,
    SourceMetadata, VersionKey,
};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListObjectsV2State {
    Init,
    StartTransaction,
    ReadHeads,
    ReadVersion,
    ReadBlobLocation,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListObjectsV2Error {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListObjectsV2State,
        expected: &'static str,
        received: Event,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("ListObjectsV2 failed")]
    ListObjectsV2Failed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListObjectsV2Input {
    pub bucket: String,
    pub group_id: GroupId,
    pub continuation_token: Option<Vec<u8>>,
    pub max_keys: Option<usize>,
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListObjectsV2Object {
    pub head: BlobHeadKey,
    pub location: Option<BackendLocation>,
    pub source_metadata: Option<SourceMetadata>,
    pub last_refresh: Option<std::time::SystemTime>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListObjectsV2Result {
    pub objects: Vec<ListObjectsV2Object>,
    pub continuation_token: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub struct ListObjectsV2Operation {
    input: ListObjectsV2Input,
    state: ListObjectsV2State,
    txn_id: Option<Ulid>,
    pending: Vec<(BlobHeadKey, Ulid)>,
    objects: Vec<ListObjectsV2Object>,
    continuation_token: Option<Vec<u8>>,
    current_head: Option<BlobHeadKey>,
    output: Option<Result<ListObjectsV2Result, ListObjectsV2Error>>,
}

impl ListObjectsV2Operation {
    const DEFAULT_MAX_KEYS: usize = 1_000;

    pub fn new(input: ListObjectsV2Input) -> Self {
        Self {
            input,
            state: ListObjectsV2State::Init,
            txn_id: None,
            pending: Vec::new(),
            objects: Vec::new(),
            continuation_token: None,
            current_head: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListObjectsV2Error) -> Effects {
        self.state = ListObjectsV2State::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn max_keys(&self) -> usize {
        self.input.max_keys.unwrap_or(Self::DEFAULT_MAX_KEYS)
    }

    fn handle_init(&mut self) -> Effects {
        self.state = ListObjectsV2State::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(ListObjectsV2Error::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        let prefix = match BlobHeadKey::bucket_prefix(&self.input.bucket) {
            Ok(prefix) => prefix,
            Err(err) => return self.emit_error(err.into()),
        };
        let iter_start_key = if self.input.continuation_token.is_some() {
            self.input.continuation_token.clone()
        } else if let Some(prefix) = self
            .input
            .prefix
            .as_ref()
            .filter(|prefix| !prefix.is_empty())
        {
            match BlobHeadKey::object_prefix(&self.input.bucket, prefix) {
                Ok(prefix) => Some(prefix),
                Err(err) => return self.emit_error(err.into()),
            }
        } else {
            None
        };

        let limit = self.max_keys().saturating_add(1);

        self.state = ListObjectsV2State::ReadHeads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start_after: iter_start_key.map(Into::into),
            limit,
            txn_id: self.txn_id,
        })]
    }

    fn handle_heads_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(ListObjectsV2Error::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let max_keys = self.max_keys();
        self.pending.clear();
        self.continuation_token = None;

        if max_keys == 0 {
            return self.read_next_version_or_finish();
        }

        let has_prefix = self.input.prefix.as_ref().is_some_and(|p| !p.is_empty());
        let raw_count = values.len();

        if !has_prefix {
            // Track the key of the last pushed item as the continuation
            // token.  Since the Iter effect uses an *exclusive* start_after
            // bound, using the first excluded item (index == max_keys) would
            // skip it on the next page.
            //
            // Only set the token when the storage returned a full page
            // (more than max_keys items), which is our signal that more
            // items likely exist.  When fewer items are returned we are
            // at the end and there is nothing left to page.
            let mut continuation = None;
            for (index, (key, value)) in values.into_iter().enumerate() {
                if index >= max_keys {
                    break;
                }
                let head = match BlobHeadKey::from_bytes(key.as_ref()) {
                    Ok(head) => head,
                    Err(err) => return self.emit_error(err.into()),
                };
                let pointer = match CurrentVersionPointer::from_bytes(value.as_ref()) {
                    Ok(pointer) => pointer,
                    Err(err) => return self.emit_error(err.into()),
                };
                if head.bucket != self.input.bucket {
                    continue;
                }
                continuation = Some(key.to_vec());
                self.pending.push((head, pointer.version_id));
            }
            if raw_count > max_keys {
                self.continuation_token = continuation;
            }
        } else {
            // Prefix filter: we start scanning at the requested object prefix,
            // then stop at the first key outside that contiguous prefix range.
            let prefix = self.input.prefix.as_ref().unwrap();
            let mut continuation = None;
            let mut has_more_matches = false;
            for (key, value) in values {
                let head = match BlobHeadKey::from_bytes(key.as_ref()) {
                    Ok(head) => head,
                    Err(err) => return self.emit_error(err.into()),
                };
                let pointer = match CurrentVersionPointer::from_bytes(value.as_ref()) {
                    Ok(pointer) => pointer,
                    Err(err) => return self.emit_error(err.into()),
                };
                if head.bucket != self.input.bucket {
                    continue;
                }
                if !head.key.starts_with(prefix.as_str()) {
                    break;
                }
                if self.pending.len() < max_keys {
                    self.pending.push((head, pointer.version_id));
                    continuation = Some(key.to_vec());
                } else {
                    has_more_matches = true;
                    break;
                }
            }
            if has_more_matches {
                self.continuation_token = continuation;
            }
        }

        self.pending.reverse();

        self.read_next_version_or_finish()
    }

    fn read_next_version_or_finish(&mut self) -> Effects {
        let Some((head, version_id)) = self.pending.pop() else {
            let Some(txn_id) = self.txn_id else {
                return self.emit_error(ListObjectsV2Error::NoTransactionFound);
            };

            self.state = ListObjectsV2State::CommitTransaction;
            self.output = Some(Ok(ListObjectsV2Result {
                objects: std::mem::take(&mut self.objects),
                continuation_token: self.continuation_token.clone(),
            }));
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        };

        self.current_head = Some(head.clone());
        let key = match VersionKey::new(&head.bucket, &head.key, version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = ListObjectsV2State::ReadVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_version_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(ListObjectsV2Error::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(value) = value else {
            return self.read_next_version_or_finish();
        };

        let version = match BlobVersion::from_bytes(value.as_ref()) {
            Ok(version) => version,
            Err(err) => return self.emit_error(err.into()),
        };

        match version.state {
            BlobVersionState::Deleted => self.read_next_version_or_finish(),
            BlobVersionState::Reference {
                cached_metadata,
                last_refresh,
                ..
            } => {
                let Some(head) = self.current_head.clone() else {
                    return self.emit_error(ListObjectsV2Error::ListObjectsV2Failed);
                };

                self.objects.push(ListObjectsV2Object {
                    head,
                    location: None,
                    source_metadata: Some(cached_metadata),
                    last_refresh: Some(last_refresh),
                });
                self.read_next_version_or_finish()
            }
            BlobVersionState::Materialized { blob_hash, .. } => {
                self.state = ListObjectsV2State::ReadBlobLocation;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: blob_hash.to_vec().into(),
                    txn_id: self.txn_id,
                })]
            }
        }
    }

    fn handle_blob_location_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(ListObjectsV2Error::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(value) = value else {
            return self.read_next_version_or_finish();
        };

        let location = match BackendLocation::from_bytes(value.as_ref()) {
            Ok(location) => location,
            Err(err) => return self.emit_error(err.into()),
        };

        let Some(head) = self.current_head.clone() else {
            return self.emit_error(ListObjectsV2Error::ListObjectsV2Failed);
        };

        self.objects.push(ListObjectsV2Object {
            head,
            location: Some(location),
            source_metadata: None,
            last_refresh: None,
        });
        self.read_next_version_or_finish()
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(ListObjectsV2Error::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.state = ListObjectsV2State::Finish;
        smallvec![]
    }
}

impl Operation for ListObjectsV2Operation {
    type Output = Option<Result<ListObjectsV2Result, ListObjectsV2Error>>;
    type Error = ListObjectsV2Error;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(ListObjectsV2Error::StorageError(error));
        }

        match self.state {
            ListObjectsV2State::Init => self.handle_init(),
            ListObjectsV2State::StartTransaction => self.handle_transaction_started(event),
            ListObjectsV2State::ReadHeads => self.handle_heads_read(event),
            ListObjectsV2State::ReadVersion => self.handle_version_read(event),
            ListObjectsV2State::ReadBlobLocation => self.handle_blob_location_read(event),
            ListObjectsV2State::CommitTransaction => self.handle_transaction_committed(event),
            ListObjectsV2State::Finish | ListObjectsV2State::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListObjectsV2State::Finish | ListObjectsV2State::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ListObjectsV2State::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ListObjectsV2Error::ListObjectsV2Failed);
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
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    };
    use aruna_core::structs::{
        BlobVersion, CurrentVersionPointer, PortableSourceDescriptor, RealmId, SourceConnectorKind,
        StagingStrategy, VersionSourceBinding,
    };
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_list_objects_v2_skips_deleted_versions() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let group_id = Ulid::new();
        let realm_id = RealmId([7u8; 32]);
        let created_by = UserId::local(Ulid::new(), realm_id);
        let live_version_id = Ulid::new();
        let deleted_version_id = Ulid::new();
        let live_hash = [3u8; 32];
        let created_at = UNIX_EPOCH + Duration::from_secs(5);

        for (key, version_id, version) in [
            (
                "alpha",
                live_version_id,
                BlobVersion::materialized(live_hash, created_at, created_by, None),
            ),
            (
                "beta",
                deleted_version_id,
                BlobVersion::deleted(created_at, created_by),
            ),
        ] {
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new("bucket", key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new("bucket", key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: version.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
        }

        let location = BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_by,
            created_at,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes: HashMap::new(),
        };
        let event = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: live_hash.to_vec().into(),
                value: location.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id,
                continuation_token: None,
                max_keys: Some(10),
                prefix: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].head.key, "alpha");
        assert_eq!(result.objects[0].location, Some(location));
        assert_eq!(result.continuation_token, None);
    }

    #[tokio::test]
    async fn test_list_objects_v2_with_prefix() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let group_id = Ulid::new();
        let realm_id = RealmId([7u8; 32]);
        let created_by = UserId::local(Ulid::new(), realm_id);
        let created_at = UNIX_EPOCH + Duration::from_secs(5);

        let keys = ["common/a", "common/b", "rare/1", "rare/2", "rare/3"];
        for key in keys {
            let version_id = Ulid::new();
            let hash = [key.len() as u8; 32];
            let version = BlobVersion::materialized(hash, created_at, created_by, None);
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new("bucket", key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new("bucket", key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: version.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: hash.to_vec().into(),
                    value: BackendLocation {
                        root: "/tmp".to_string(),
                        storage_bucket: "objects".to_string(),
                        backend_path: format!("path/{key}"),
                        ulid: Ulid::new(),
                        compressed: false,
                        encrypted: false,
                        created_by,
                        created_at,
                        staging: false,
                        partial: false,
                        blob_size: 42,
                        hashes: HashMap::new(),
                    }
                    .to_bytes()
                    .unwrap()
                    .into(),
                    txn_id: None,
                })
                .await;
        }

        // Run with prefix filter; collect all results across pages
        let mut continuation_token = None;
        let mut all_keys = Vec::new();

        loop {
            let result = drive(
                ListObjectsV2Operation::new(ListObjectsV2Input {
                    bucket: "bucket".to_string(),
                    group_id,
                    continuation_token,
                    max_keys: Some(2),
                    prefix: Some("rare/".to_string()),
                }),
                &driver_ctx,
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();

            for obj in result.objects {
                all_keys.push(obj.head.key);
            }

            continuation_token = result.continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        // Verify prefix filtered correctly — all returned keys start with "rare/"
        assert!(all_keys.iter().all(|k| k.starts_with("rare/")));
        // And we got the right ones
        let mut sorted = all_keys.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["rare/1", "rare/2", "rare/3"]);
    }

    #[tokio::test]
    async fn test_list_objects_v2_prefix_scan_does_not_stop_on_prefix_miss() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let group_id = Ulid::new();
        let realm_id = RealmId([7u8; 32]);
        let created_by = UserId::local(Ulid::new(), realm_id);
        let created_at = UNIX_EPOCH + Duration::from_secs(5);

        let keys = [
            "common/01",
            "common/02",
            "common/03",
            "common/04",
            "common/05",
            "rare/01",
        ];
        for key in keys {
            let version_id = Ulid::new();
            let hash = [key.len() as u8; 32];
            let version = BlobVersion::materialized(hash, created_at, created_by, None);
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new("bucket", key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new("bucket", key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: version.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: hash.to_vec().into(),
                    value: BackendLocation {
                        root: "/tmp".to_string(),
                        storage_bucket: "objects".to_string(),
                        backend_path: format!("path/{key}"),
                        ulid: Ulid::new(),
                        compressed: false,
                        encrypted: false,
                        created_by,
                        created_at,
                        staging: false,
                        partial: false,
                        blob_size: 42,
                        hashes: HashMap::new(),
                    }
                    .to_bytes()
                    .unwrap()
                    .into(),
                    txn_id: None,
                })
                .await;
        }

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id,
                continuation_token: None,
                max_keys: Some(1),
                prefix: Some("rare/".to_string()),
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let keys: Vec<_> = result
            .objects
            .into_iter()
            .map(|object| object.head.key)
            .collect();
        assert_eq!(keys, vec!["rare/01"]);
        assert!(result.continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_list_objects_v2_honors_explicit_zero_max_keys() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let group_id = Ulid::new();
        let realm_id = RealmId([7u8; 32]);
        let created_by = UserId::local(Ulid::new(), realm_id);
        let created_at = UNIX_EPOCH + Duration::from_secs(5);
        let version_id = Ulid::new();
        let hash = [3u8; 32];

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("bucket", "alpha")
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("bucket", "alpha", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::materialized(hash, created_at, created_by, None)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id,
                continuation_token: None,
                max_keys: Some(0),
                prefix: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(result.objects.is_empty());
        assert!(result.continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_list_objects_v2_pagination_without_prefix() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let group_id = Ulid::new();
        let realm_id = RealmId([7u8; 32]);
        let created_by = UserId::local(Ulid::new(), realm_id);
        let created_at = UNIX_EPOCH + Duration::from_secs(5);

        let keys = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"];
        for key in keys {
            let version_id = Ulid::new();
            let hash = [key.len() as u8; 32];
            let version = BlobVersion::materialized(hash, created_at, created_by, None);
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new("bucket", key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new("bucket", key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: version.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: hash.to_vec().into(),
                    value: BackendLocation {
                        root: "/tmp".to_string(),
                        storage_bucket: "objects".to_string(),
                        backend_path: format!("path/{key}"),
                        ulid: Ulid::new(),
                        compressed: false,
                        encrypted: false,
                        created_by,
                        created_at,
                        staging: false,
                        partial: false,
                        blob_size: 42,
                        hashes: HashMap::new(),
                    }
                    .to_bytes()
                    .unwrap()
                    .into(),
                    txn_id: None,
                })
                .await;
        }

        // Paginate without prefix; collect all results across pages
        let mut continuation_token = None;
        let mut all_keys = Vec::new();

        loop {
            let result = drive(
                ListObjectsV2Operation::new(ListObjectsV2Input {
                    bucket: "bucket".to_string(),
                    group_id,
                    continuation_token,
                    max_keys: Some(3),
                    prefix: None,
                }),
                &driver_ctx,
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();

            for obj in result.objects {
                all_keys.push(obj.head.key);
            }

            continuation_token = result.continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        // Verify all 7 keys were returned
        let mut sorted = all_keys.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec!["alpha", "beta", "delta", "epsilon", "eta", "gamma", "zeta"]
        );
    }

    #[tokio::test]
    async fn test_list_objects_v2_empty_bucket() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let group_id = Ulid::new();

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "empty-bucket".to_string(),
                group_id,
                continuation_token: None,
                max_keys: Some(10),
                prefix: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(result.objects.is_empty());
        assert!(result.continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_list_objects_v2_reference_object() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let group_id = Ulid::new();
        let realm_id = RealmId([7u8; 32]);
        let created_by = UserId::local(Ulid::new(), realm_id);
        let version_id = Ulid::new();
        let created_at = UNIX_EPOCH + Duration::from_secs(5);
        let last_refresh = UNIX_EPOCH + Duration::from_secs(20);

        let source_metadata = SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("ref-etag-1".to_string()),
            last_modified: Some(UNIX_EPOCH + Duration::from_secs(10)),
            source_version: None,
        };

        let version = BlobVersion::reference(
            VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::Http,
                    public_config: HashMap::new(),
                    source_path: "source/path".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                },
                connector_id: None,
            },
            source_metadata.clone(),
            created_at,
            created_by,
            last_refresh,
        );

        // Write head entry
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("bucket", "ref-object")
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;

        // Write version entry (reference — no location)
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("bucket", "ref-object", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id,
                continuation_token: None,
                max_keys: Some(10),
                prefix: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].head.key, "ref-object");
        assert_eq!(result.objects[0].location, None);
        assert_eq!(result.objects[0].source_metadata, Some(source_metadata));
        assert_eq!(result.objects[0].last_refresh, Some(last_refresh));
        assert!(result.continuation_token.is_none());
    }
}
