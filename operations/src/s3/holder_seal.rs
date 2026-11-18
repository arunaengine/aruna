//! Seals missing bucket key copies for a holder who just published a user key, in every
//! encrypted bucket on this node whose active key generation is unlocked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyError, BucketKeyRef};
use aruna_core::structs::storage::holders::KeyLookup;
use aruna_core::types::Value;
use aruna_core::{NodeId, UserId};
use thiserror::Error;

use crate::s3::bucket::seal_missing::{SealMissingError, SealMissingInput, SealMissingOperation};

const PAGE: usize = 256;

#[derive(Debug, Error, PartialEq)]
pub enum SealPublishedError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("could not seal copies for a new user key: {0}")]
    Io(IoError),
}

impl From<IoError> for SealPublishedError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

#[derive(Debug, PartialEq)]
enum SealState {
    ScanSettings,
    ReadBucket,
    Seal,
}

type Step = Result<Next<Vec<String>>, SealPublishedError>;

/// Seals copies for the publisher of `record`; answers the buckets that sealed one. A locked
/// bucket is skipped: its next unlock seals the missing copies.
#[derive(Debug, PartialEq)]
pub struct SealPublishedOperation {
    realm_id: RealmId,
    node_id: NodeId,
    record: UserKeyRecord,
    state: SealState,
    pages: PrefixPages,
    page: VecDeque<(String, BucketKeyRef)>,
    current: Option<String>,
    sealed: Vec<String>,
}

impl SealPublishedOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId, record: UserKeyRecord) -> Self {
        Self {
            realm_id,
            node_id,
            record,
            state: SealState::ScanSettings,
            pages: PrefixPages::new(BUCKET_ENCRYPTION_KEYSPACE, Vec::new(), PAGE),
            page: VecDeque::new(),
            current: None,
            sealed: Vec::new(),
        }
    }

    fn page_read(&mut self, result: IoResult) -> Step {
        // Rows that do not decode or name no active key are not this check's business.
        self.page = (self.pages.take(result)?.into_iter())
            .filter_map(|(key, value)| {
                let bucket = String::from_utf8(key.to_vec()).ok()?;
                let settings = BucketEncryption::from_bytes(&value).ok()?;
                Some((bucket, settings.active_key()?))
            })
            .collect();
        Ok(self.next_bucket())
    }

    fn next_bucket(&mut self) -> Next<Vec<String>> {
        let Some((bucket, _)) = self.page.front() else {
            if self.pages.done() {
                return Next::Done(std::mem::take(&mut self.sealed));
            }
            self.state = SealState::ScanSettings;
            return Next::Call(self.pages.call());
        };
        self.state = SealState::ReadBucket;
        Next::Call(read_row(S3_BUCKET_KEYSPACE, bucket.as_bytes()))
    }

    fn bucket_read(&mut self, value: Option<Value>) -> Step {
        let Some((bucket, key)) = self.page.pop_front() else {
            return Err(StorageError::ReadError("no bucket was read".to_string()).into());
        };
        let Some(info) = value.and_then(|value| BucketInfo::from_bytes(&value).ok()) else {
            return Ok(self.next_bucket());
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
        self.current = Some(bucket);
        Ok(Next::Call(IoCall::child(SealMissingOperation::new(input))))
    }

    /// Whether the seal made a copy for the publisher; a locked bucket is skipped quietly.
    fn copy_sealed(&self, bucket: &str, result: Result<Vec<UserId>, SealMissingError>) -> bool {
        match result {
            Ok(users) => users.contains(&self.record.user_id),
            Err(SealMissingError::Blob(BlobError::BucketKey(BucketKeyError::Locked(_)))) => false,
            Err(error) => {
                tracing::warn!(%bucket, %error, "could not seal a copy for a new user key");
                false
            }
        }
    }
}

impl Operation for SealPublishedOperation {
    type Output = Vec<String>;
    type Error = SealPublishedError;

    fn first(&mut self) -> Step {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            SealState::ScanSettings => self.page_read(result),
            SealState::ReadBucket => self.bucket_read(result.value()?),
            SealState::Seal => {
                let result = result.child::<Vec<UserId>, SealMissingError>()?;
                let bucket = self.current.take().ok_or(IoError::Unfinished)?;
                if self.copy_sealed(&bucket, result) {
                    self.sealed.push(bucket);
                }
                Ok(self.next_bucket())
            }
        }
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
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, KEY_COPY_KEYSPACE};
    use aruna_core::node_vault::NodeVaultKey;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::blob::{Backend, BackendConfig};
    use aruna_core::structs::storage::encryption::{
        BlockCipher, BlockKeys, EncryptionMode, public_key_of,
    };
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::types::Key;
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
        let net = aruna_net::NetHandle::new(crate::testing::local_net(), storage.clone())
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

    fn scan_call(after: Option<Key>) -> Step {
        Ok(Next::Call(IoCall::Page {
            space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: PAGE,
        }))
    }

    fn settings_page(rows: Vec<(&str, BucketEncryption)>, next: Option<&str>) -> IoResult {
        IoResult::Page {
            rows: (rows.into_iter())
                .map(|(bucket, settings)| {
                    let value = settings.to_bytes().unwrap();
                    (bucket.as_bytes().to_vec().into(), value.into())
                })
                .collect(),
            next: next.map(|key| key.as_bytes().to_vec().into()),
        }
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
        assert_eq!(operation.first(), scan_call(None));
        let page = settings_page(
            vec![
                ("plain", BucketEncryption::default()),
                ("gone", encrypted()),
            ],
            Some("gone"),
        );
        let read_gone = read_row(S3_BUCKET_KEYSPACE, b"gone".as_slice());
        assert_eq!(operation.next(page), Ok(Next::Call(read_gone)));
        let missing = IoResult::Values(vec![None]);
        let after = Some(Key::from(b"gone".to_vec()));
        assert_eq!(operation.next(missing), scan_call(after));
        let last = settings_page(Vec::new(), None);
        assert_eq!(operation.next(last), Ok(Next::Done(Vec::new())));
    }

    #[test]
    fn unexpected_results_fail() {
        let error = SealPublishedError::from(IoError::Storage(StorageError::Timeout));
        assert_eq!(error, SealPublishedError::Storage(StorageError::Timeout));
        let mut scanning = operation();
        let _ = scanning.first();
        assert!(matches!(
            scanning.next(IoResult::Done),
            Err(SealPublishedError::Io(IoError::Unexpected { .. }))
        ));
    }
}
