//! Seals missing bucket key copies for a holder who just published a user key, in every
//! encrypted bucket on this node whose active key generation is unlocked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyError, BucketKeyRef};
use aruna_core::structs::storage::holders::KeyLookup;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;

use crate::s3::bucket::seal_missing::{SealMissingError, SealMissingInput, SealMissingOperation};

const PAGE: usize = 256;

#[derive(Debug, PartialEq)]
enum SealState {
    Init,
    ScanSettings,
    ReadBucket,
    Seal,
    Finish,
    Error,
}

/// Seals copies for the publisher of `record`; answers the buckets that sealed one. A locked
/// bucket is skipped: its next unlock seals the missing copies.
#[derive(Debug, PartialEq)]
pub struct SealPublishedOperation {
    realm_id: RealmId,
    node_id: NodeId,
    record: UserKeyRecord,
    state: SealState,
    page: VecDeque<(String, BucketKeyRef)>,
    current: Option<String>,
    next: Option<Key>,
    sealed: Vec<String>,
    error: Option<StorageError>,
}

impl SealPublishedOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId, record: UserKeyRecord) -> Self {
        Self {
            realm_id,
            node_id,
            record,
            state: SealState::Init,
            page: VecDeque::new(),
            current: None,
            next: None,
            sealed: Vec::new(),
            error: None,
        }
    }

    fn scan(&mut self, start: Option<Key>) -> Effects {
        self.state = SealState::ScanSettings;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: PAGE,
            txn_id: None,
        })]
    }

    fn page_read(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        // Rows that do not decode or name no active key are not this check's business.
        self.page = values
            .into_iter()
            .filter_map(|(key, value)| {
                let bucket = String::from_utf8(key.to_vec()).ok()?;
                let settings = BucketEncryption::from_bytes(&value).ok()?;
                Some((bucket, settings.active_key()?))
            })
            .collect();
        self.next = next;
        self.next_bucket()
    }

    fn next_bucket(&mut self) -> Effects {
        let Some((bucket, _)) = self.page.front() else {
            return match self.next.take() {
                Some(next) => self.scan(Some(next)),
                None => {
                    self.state = SealState::Finish;
                    smallvec![]
                }
            };
        };
        self.state = SealState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn bucket_read(&mut self, value: Option<Value>) -> Effects {
        let Some((bucket, key)) = self.page.pop_front() else {
            return self.fail(StorageError::ReadError("no bucket was read".to_string()));
        };
        let Some(info) = value.and_then(|value| BucketInfo::from_bytes(&value).ok()) else {
            return self.next_bucket();
        };
        let user_id = self.record.user_id;
        let input = SealMissingInput {
            bucket: bucket.clone(),
            group_id: info.group_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            key,
            lookups: BTreeMap::from([(user_id, KeyLookup::Keys(vec![self.record.clone()]))]),
        };
        self.state = SealState::Seal;
        self.current = Some(bucket.clone());
        let seal = SealMissingOperation::new(input);
        smallvec![Effect::SubOperation(boxed_suboperation(
            seal,
            move |result| {
                let sealed = match result {
                    Ok(users) => users.contains(&user_id),
                    Err(SealMissingError::Blob(BlobError::BucketKey(BucketKeyError::Locked(
                        _,
                    )))) => false,
                    Err(error) => {
                        tracing::warn!(%bucket, %error, "could not seal a copy for a new user key");
                        false
                    }
                };
                Event::SubOperation(SubOperationEvent::CopySealed { sealed })
            }
        ))]
    }

    fn fail(&mut self, error: StorageError) -> Effects {
        self.state = SealState::Error;
        self.error = Some(error);
        smallvec![]
    }
}

impl Operation for SealPublishedOperation {
    type Output = Vec<String>;
    type Error = StorageError;

    fn start(&mut self) -> Effects {
        self.scan(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (
                SealState::ScanSettings | SealState::ReadBucket,
                Event::Storage(StorageEvent::Error { error }),
            ) => self.fail(error),
            (
                SealState::ScanSettings,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.page_read(values, next_start_after),
            (SealState::ReadBucket, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.bucket_read(value)
            }
            (SealState::Seal, Event::SubOperation(SubOperationEvent::CopySealed { sealed })) => {
                if let Some(bucket) = self.current.take()
                    && sealed
                {
                    self.sealed.push(bucket);
                }
                self.next_bucket()
            }
            (_, event) => self.fail(StorageError::ReadError(format!(
                "unexpected event while sealing copies: {}",
                event.label()
            ))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SealState::Finish | SealState::Error)
    }

    fn finalize(self) -> Result<Vec<String>, StorageError> {
        match (self.state, self.error) {
            (SealState::Finish, _) => Ok(self.sealed),
            (_, Some(error)) => Err(error),
            _ => Err(StorageError::ReadError(
                "the copy seal did not finish".to_string(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::bucket::create::CreateBucketOperation;
    use crate::s3::bucket::encryption::{EnableEncryptionOperation, EnableInput};
    use crate::s3::bucket::key::install::{InstallInput, InstallKeyOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::UserId;
    use aruna_core::compute::SecretBytes;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, KEY_COPY_KEYSPACE};
    use aruna_core::node_vault::NodeVaultKey;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::blob::{Backend, BackendConfig};
    use aruna_core::structs::storage::encryption::{
        BlockCipher, BlockKeys, EncryptionMode, public_key_of,
    };
    use aruna_core::structs::storage::format::Compression;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn key_record(user_id: UserId, seed: u8, node_id: NodeId) -> UserKeyRecord {
        let public_key = public_key_of(&SecretBytes::new(vec![seed; 32])).unwrap();
        UserKeyRecord {
            realm_id: user_id.realm_id,
            user_id,
            record_id: Ulid::from_bytes([seed; 16]),
            key_id: format!("slot-{seed}"),
            public_key,
            fingerprint: aruna_core::vault_format::key_fingerprint(&public_key),
            has_recovery: true,
            node_id,
            placement: PlacementRef::NIL,
            created_at_ms: 1,
            signature: None,
        }
    }

    #[tokio::test]
    async fn seals_admin_key() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = aruna_storage::FjallStorage::open(root).unwrap();
        storage.open_vault(NodeVaultKey::random());
        let net = aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage.clone())
            .await
            .unwrap();
        let config = BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: format!("{root}/blobs"),
            service_config: HashMap::new(),
            timeouts: Default::default(),
        };
        let blob = BlobHandler::new(config, storage.clone(), net.clone())
            .await
            .unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: Some(blob),
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let (realm_id, node_id) = (RealmId::from_bytes([1; 32]), net.node_id());
        let creator = UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let admin = UserId::new(Ulid::from_bytes([6; 16]), realm_id);
        let group_id = Ulid::from_bytes([3; 16]);
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: creator,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let rows = crate::s3::bucket::key::rows::authority_rows(&info, None, &[admin]);
        let documents = [realm_id.as_bytes().to_vec(), group_id.to_bytes().to_vec()];
        for (key, (_, value)) in documents.into_iter().zip(rows.into_iter().skip(2)) {
            let write = StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: key.into(),
                value: value.unwrap(),
                txn_id: None,
            };
            storage.send_storage_effect(write).await;
        }
        let create = CreateBucketOperation::new("sealed".to_string(), info);
        drive(create, &context).await.unwrap();
        let lookups = BTreeMap::from([
            (
                creator,
                KeyLookup::Keys(vec![key_record(creator, 7, node_id)]),
            ),
            (admin, KeyLookup::Missing),
        ]);
        let enabled = drive(
            EnableEncryptionOperation::new(EnableInput {
                bucket: "sealed".to_string(),
                group_id,
                realm_id,
                node_id,
                caller: admin,
                mode: EncryptionMode::NodeManaged,
                cipher: BlockCipher::ChaCha20Poly1305,
                block_keys: BlockKeys::ContentDerived,
                max_unlock_ms: None,
                expected_generation: 0,
                lookups,
                now_ms: 1,
            }),
            &context,
        )
        .await
        .unwrap();
        let key = enabled.key.key;
        let install = InstallKeyOperation::new(InstallInput {
            key,
            public_key: enabled.key.public_key,
            private_key: enabled.private_key,
            duration: None,
            max: None,
        });
        drive(install, &context).await.unwrap();

        let published = key_record(admin, 8, node_id);
        let seal = SealPublishedOperation::new(realm_id, node_id, published.clone());
        let sealed = drive(seal, &context).await;
        assert_eq!(sealed, Ok(vec!["sealed".to_string()]));
        let copies = StorageEffect::Iter {
            key_space: KEY_COPY_KEYSPACE.to_string(),
            prefix: Some(key.key().into()),
            start: None,
            limit: 8,
            txn_id: None,
        };
        let Event::Storage(StorageEvent::IterResult { values, .. }) =
            storage.send_storage_effect(copies).await
        else {
            panic!("no copies");
        };
        assert_eq!(values.len(), 2);
        // The admin holds a copy now, so publishing again seals nothing.
        let seal = SealPublishedOperation::new(realm_id, node_id, published.clone());
        let again = drive(seal, &context).await;
        assert_eq!(again, Ok(Vec::new()));
    }

    fn operation() -> SealPublishedOperation {
        let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([3; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let user = UserId::new(Ulid::from_bytes([1; 16]), realm_id);
        SealPublishedOperation::new(realm_id, node_id, key_record(user, 9, node_id))
    }

    fn scan_effect(start: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: PAGE,
            txn_id: None,
        })
    }

    fn settings_page(rows: Vec<(&str, BucketEncryption)>, next: Option<&str>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: (rows.into_iter())
                .map(|(bucket, settings)| {
                    let value = settings.to_bytes().unwrap();
                    (bucket.as_bytes().to_vec().into(), value.into())
                })
                .collect(),
            next_start_after: next.map(|key| key.as_bytes().to_vec().into()),
        })
    }

    fn encrypted() -> BucketEncryption {
        BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(Ulid::from_bytes([5; 16])),
            key_generation: 1,
            ..Default::default()
        }
    }

    #[test]
    fn skips_unsealable_buckets() {
        // A plain bucket is never read; a bucket without a record is skipped.
        let mut operation = operation();
        let page = settings_page(
            vec![
                ("plain", BucketEncryption::default()),
                ("gone", encrypted()),
            ],
            Some("gone"),
        );
        let read_gone = Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: b"gone".to_vec().into(),
            txn_id: None,
        });
        let missing = Event::Storage(StorageEvent::ReadResult {
            key: b"gone".to_vec().into(),
            value: None,
        });
        let last = settings_page(Vec::new(), None);
        let steps = vec![
            (page, vec![read_gone]),
            (missing, vec![scan_effect(Some(b"gone".to_vec().into()))]),
            (last, vec![]),
        ];
        check_trace(&mut operation, &[scan_effect(None)], steps);
        assert_eq!(operation.finalize(), Ok(Vec::new()));
    }

    #[test]
    fn storage_errors_fail() {
        let timeout = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })
        };
        let mut scanning = operation();
        scanning.start();
        scanning.step(timeout());
        assert_eq!(scanning.finalize(), Err(StorageError::Timeout));
        let mut reading = operation();
        reading.start();
        reading.step(settings_page(vec![("b", encrypted())], None));
        reading.step(timeout());
        assert_eq!(reading.finalize(), Err(StorageError::Timeout));
    }

    #[test]
    fn rejects_unexpected_events() {
        let scanning = || {
            let mut operation = operation();
            operation.start();
            operation
        };
        let refused = rejects_events(scanning, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refused.is_empty());
        let reading = || {
            let mut operation = scanning();
            operation.step(settings_page(vec![("b", encrypted())], None));
            operation
        };
        let refused = rejects_events(reading, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refused.is_empty());
    }

    #[test]
    fn abort_and_finalize() {
        // Reads and sub-operations only, so no phase holds anything to clean up.
        let mut fresh = operation();
        check_abort(&mut fresh, &[]);
        rejects_finalize(fresh);
        let mut scanning = operation();
        scanning.start();
        check_abort(&mut scanning, &[]);
        rejects_finalize(scanning);
    }
}
