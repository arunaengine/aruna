//! Deletes a bucket only when it holds no objects, uploads or sync links, and fixes usage.
//! Its bucket keys, holders and node vault keys go with it, and its unlocked keys are locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::node::usage_stats::{UsageCounterUpdate, UsageUpdateError, schedule_snapshot_publish};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ABE_DUE_KEYSPACE, ABE_REISSUE_KEYSPACE, ABE_REKEY_KEYSPACE, BLOB_HEAD_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE,
    BUCKET_KEY_KEYSPACE, GROUP_ENCRYPTED_KEYSPACE, KEY_COPY_KEYSPACE, NODE_VAULT_KEYSPACE,
    RELATIONSHIP_IN_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE, S3_BUCKET_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{BlobHeadKey, BucketInfo, VersionKey};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyRecord};
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::structs::{SyncRelationship, sync_relationship_key, sync_relationship_prefix};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;

use crate::sync::mirror_repair::mirror_delete_entry;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteBucketState {
    Init,
    StartTransaction,
    ReadBucket,
    CheckCurrentObjects,
    CheckVersions,
    CheckMultipartUploads,
    ScanOutRelationships,
    ScanInRelationships,
    WriteRepairs,
    ReadEncryption,
    ScanKeyRows,
    DeleteBucket,
    UpdateUsage,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteBucketError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("Bucket is not empty")]
    NotEmpty,
    #[error("Transaction id missing")]
    TransactionMissing,
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: DeleteBucketState,
        expected: &'static str,
        received: Event,
    },
    #[error(transparent)]
    UsageUpdateError(#[from] UsageUpdateError),
    #[error("DeleteBucket failed")]
    DeleteBucketFailed,
    #[error("operation did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
pub struct DeleteBucketOperation {
    bucket: String,
    state: DeleteBucketState,
    txn_id: Option<TxnId>,
    group_id: Option<GroupId>,
    usage_update: Option<UsageCounterUpdate>,
    relationship_deletes: Vec<(String, Key)>,
    relationships: Vec<SyncRelationship>,
    /// Stable id of an encrypted bucket, whose key rows go with it.
    bucket_id: Option<ulid::Ulid>,
    key_spaces: Vec<&'static str>,
    key_deletes: Vec<(String, Key)>,
    output: Option<Result<(), DeleteBucketError>>,
}

impl DeleteBucketOperation {
    const SCAN_LIMIT: usize = 10_000;

    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: DeleteBucketState::Init,
            txn_id: None,
            group_id: None,
            usage_update: None,
            relationship_deletes: Vec::new(),
            relationships: Vec::new(),
            bucket_id: None,
            key_spaces: Vec::new(),
            key_deletes: Vec::new(),
            output: None,
        }
    }

    fn emit_error(&mut self, error: DeleteBucketError) -> Effects {
        self.state = DeleteBucketState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn handle_init(&mut self) -> Effects {
        self.state = DeleteBucketState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        self.state = DeleteBucketState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_bucket_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(value) = value else {
            return self.emit_error(DeleteBucketError::NotFound);
        };
        match BucketInfo::from_bytes(value.as_ref()) {
            Ok(info) => self.group_id = Some(info.group_id),
            Err(err) => return self.emit_error(err.into()),
        }

        let prefix = match BlobHeadKey::bucket_prefix(&self.bucket) {
            Ok(prefix) => prefix,
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = DeleteBucketState::CheckCurrentObjects;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: Self::SCAN_LIMIT,
            txn_id: self.txn_id,
        })]
    }

    fn objects_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        if !values.is_empty() {
            return self.emit_error(DeleteBucketError::NotEmpty);
        }

        let prefix = match VersionKey::bucket_prefix(&self.bucket) {
            Ok(prefix) => prefix,
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = DeleteBucketState::CheckVersions;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: Self::SCAN_LIMIT,
            txn_id: self.txn_id,
        })]
    }

    fn handle_versions_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        if !values.is_empty() {
            return self.emit_error(DeleteBucketError::NotEmpty);
        }

        self.state = DeleteBucketState::CheckMultipartUploads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: UPLOAD_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: u64::MAX as usize,
            txn_id: self.txn_id,
        })]
    }

    fn uploads_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        for (_, value) in values {
            let Ok(upload) = MultipartUpload::from_bytes(value.as_ref()) else {
                continue;
            };
            if upload.bucket == self.bucket {
                return self.emit_error(DeleteBucketError::NotEmpty);
            }
        }

        self.scan_relationships(false, None)
    }

    fn scan_relationships(&mut self, incoming: bool, start: Option<Key>) -> Effects {
        self.state = if incoming {
            DeleteBucketState::ScanInRelationships
        } else {
            DeleteBucketState::ScanOutRelationships
        };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: if incoming {
                RELATIONSHIP_IN_KEYSPACE.to_string()
            } else {
                RELATIONSHIP_OUT_KEYSPACE.to_string()
            },
            prefix: Some(sync_relationship_prefix(&self.bucket).into()),
            start: start.map(aruna_core::effects::IterStart::After),
            limit: Self::SCAN_LIMIT,
            txn_id: self.txn_id,
        })]
    }

    fn handle_relationship_scan(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };
        let incoming = self.state == DeleteBucketState::ScanInRelationships;
        let key_space = if incoming {
            RELATIONSHIP_IN_KEYSPACE
        } else {
            RELATIONSHIP_OUT_KEYSPACE
        };
        for (key, value) in values {
            let relationship = match SyncRelationship::from_bytes(&value) {
                Ok(relationship)
                    if key.as_ref()
                        == sync_relationship_key(&self.bucket, relationship.id).as_slice() =>
                {
                    relationship
                }
                Ok(_) => {
                    return self.emit_error(
                        ConversionError::FromStrError(
                            "sync relationship key does not match payload".to_string(),
                        )
                        .into(),
                    );
                }
                Err(error) => return self.emit_error(error.into()),
            };
            self.relationship_deletes.push((key_space.to_string(), key));
            if !self
                .relationships
                .iter()
                .any(|existing| existing.id == relationship.id)
            {
                self.relationships.push(relationship);
            }
        }
        if let Some(start) = next_start_after {
            return self.scan_relationships(incoming, Some(start));
        }
        if !incoming {
            return self.scan_relationships(true, None);
        }

        if !self.relationships.is_empty() {
            let writes = match self
                .relationships
                .iter()
                .map(mirror_delete_entry)
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(writes) => writes,
                Err(error) => return self.emit_error(error.into()),
            };
            self.state = DeleteBucketState::WriteRepairs;
            return smallvec![Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: self.txn_id,
            })];
        }

        self.read_encryption()
    }

    fn read_encryption(&mut self) -> Effects {
        self.state = DeleteBucketState::ReadEncryption;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().into(),
            txn_id: self.txn_id,
        })]
    }

    fn encryption_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        match BucketEncryption::from_row(value.as_deref()) {
            Ok(settings) => self.bucket_id = settings.bucket_id,
            Err(error) => return self.emit_error(error.into()),
        }
        self.key_spaces = vec![
            KEY_COPY_KEYSPACE,
            BUCKET_HOLDER_KEYSPACE,
            BUCKET_KEY_KEYSPACE,
        ];
        self.scan_key_rows(None)
    }

    fn scan_key_rows(&mut self, start: Option<Key>) -> Effects {
        let (Some(bucket_id), Some(key_space)) = (self.bucket_id, self.key_spaces.last()) else {
            return self.delete_bucket_records();
        };
        self.state = DeleteBucketState::ScanKeyRows;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(bucket_id.to_bytes().to_vec().into()),
            start: start.map(aruna_core::effects::IterStart::After),
            limit: Self::SCAN_LIMIT,
            txn_id: self.txn_id,
        })]
    }

    fn key_rows_scanned(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };
        let Some(&key_space) = self.key_spaces.last() else {
            return self.emit_error(DeleteBucketError::DeleteBucketFailed);
        };
        for (key, value) in values {
            // A node-managed key also leaves its node vault row.
            if key_space == BUCKET_KEY_KEYSPACE {
                match BucketKeyRecord::from_bytes(value.as_ref()) {
                    Ok(record) => {
                        let entries = record.vault_entry.into_iter();
                        let vault = entries.map(|id| VaultEntry::new(VaultPurpose::BucketKey, id));
                        self.key_deletes
                            .extend(vault.map(|entry| {
                                (NODE_VAULT_KEYSPACE.to_string(), entry.key().into())
                            }));
                    }
                    Err(error) => return self.emit_error(error.into()),
                }
            }
            self.key_deletes.push((key_space.to_string(), key));
        }
        if next_start_after.is_none() {
            self.key_spaces.pop();
        }
        self.scan_key_rows(next_start_after)
    }

    fn delete_bucket_records(&mut self) -> Effects {
        self.state = DeleteBucketState::DeleteBucket;
        let bucket = self.bucket.as_bytes().to_vec();
        let mut deletes = vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.clone().into()),
            // A bucket created again under this name starts without encryption.
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket.into()),
        ];
        if let Some(group_id) = self.group_id {
            let key = crate::s3::bucket::key::rows::group_bucket_key(group_id, &self.bucket);
            deletes.push((GROUP_ENCRYPTED_KEYSPACE.to_string(), key));
        }
        if let Some(bucket_id) = self.bucket_id {
            let id: Key = bucket_id.to_bytes().to_vec().into();
            deletes.push((ABE_DUE_KEYSPACE.to_string(), id.clone()));
            deletes.push((ABE_REISSUE_KEYSPACE.to_string(), id.clone()));
            deletes.push((ABE_REKEY_KEYSPACE.to_string(), id));
        }
        deletes.append(&mut self.relationship_deletes);
        deletes.append(&mut self.key_deletes);
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_repairs_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received: event,
            });
        };
        self.read_encryption()
    }

    fn handle_bucket_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received: event,
            });
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(DeleteBucketError::TransactionMissing);
        };
        let Some(group_id) = self.group_id else {
            return self.emit_error(DeleteBucketError::DeleteBucketFailed);
        };

        let mut update = UsageCounterUpdate::for_group(
            group_id,
            UsageDelta {
                buckets: -1,
                ..Default::default()
            },
        );
        self.state = DeleteBucketState::UpdateUsage;
        let effects = update.start(txn_id);
        self.usage_update = Some(update);
        effects
    }

    fn handle_usage_update(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(DeleteBucketError::TransactionMissing);
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self.emit_error(DeleteBucketError::DeleteBucketFailed);
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => {
                self.state = DeleteBucketState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Err(err) => self.emit_error(err.into()),
        }
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.txn_id = None;
        self.state = DeleteBucketState::Finish;
        self.output = Some(Ok(()));
        let mut effects = smallvec![schedule_snapshot_publish()];
        // The registry frees the slots of the deleted bucket id; nothing can read it any more.
        if let Some(bucket_id) = self.bucket_id {
            effects.push(Effect::Blob(BlobEffect::LockKey {
                bucket_id,
                session: None,
            }));
        }
        if !self.relationships.is_empty() {
            effects.push(Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::DrainMirrorRepair,
                after: std::time::Duration::ZERO,
            }));
        }
        effects
    }
}

impl Operation for DeleteBucketOperation {
    type Output = ();
    type Error = DeleteBucketError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.emit_error(error.clone().into());
        }
        match self.state {
            DeleteBucketState::Init => self.handle_init(),
            DeleteBucketState::StartTransaction => self.handle_transaction_started(event),
            DeleteBucketState::ReadBucket => self.handle_bucket_read(event),
            DeleteBucketState::CheckCurrentObjects => self.objects_checked(event),
            DeleteBucketState::CheckVersions => self.handle_versions_checked(event),
            DeleteBucketState::CheckMultipartUploads => self.uploads_checked(event),
            DeleteBucketState::ScanOutRelationships | DeleteBucketState::ScanInRelationships => {
                self.handle_relationship_scan(event)
            }
            DeleteBucketState::WriteRepairs => self.handle_repairs_written(event),
            DeleteBucketState::ReadEncryption => self.encryption_read(event),
            DeleteBucketState::ScanKeyRows => self.key_rows_scanned(event),
            DeleteBucketState::DeleteBucket => self.handle_bucket_deleted(event),
            DeleteBucketState::UpdateUsage => self.handle_usage_update(event),
            DeleteBucketState::CommitTransaction => self.handle_transaction_committed(event),
            DeleteBucketState::Finish => smallvec![],
            DeleteBucketState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteBucketState::Finish | DeleteBucketState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(DeleteBucketError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::bucket::create::CreateBucketOperation;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, MIRROR_REPAIR_KEYSPACE,
        RELATIONSHIP_IN_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{BlobVersion, CurrentVersionPointer};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::replication::ArunaArn;
    use aruna_core::structs::{SyncMode, SyncState, SyncStatusSnapshot, sync_relationship_key};
    use aruna_storage::storage;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_delete_bucket() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let bucket = "bucket-a".to_string();
        drive(
            CreateBucketOperation::new(
                bucket.clone(),
                BucketInfo {
                    group_id: Ulid::generate(),
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                },
            ),
            &driver_ctx,
        )
        .await
        .unwrap();

        drive(DeleteBucketOperation::new(bucket.clone()), &driver_ctx)
            .await
            .unwrap();

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing read result");
        };
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn removes_encryption_settings() {
        use aruna_core::structs::storage::encryption::{BucketKeyRef, EncryptionMode};
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let bucket = "bucket-a".to_string();
        let info = BucketInfo {
            group_id: Ulid::generate(),
            created_at: SystemTime::now(),
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
        };
        drive(
            CreateBucketOperation::new(bucket.clone(), info),
            &driver_ctx,
        )
        .await
        .unwrap();
        let (bucket_id, other_id, vault_id) =
            (Ulid::generate(), Ulid::generate(), Ulid::generate());
        let settings = BucketEncryption {
            mode: EncryptionMode::NodeManaged,
            bucket_id: Some(bucket_id),
            key_generation: 1,
            ..Default::default()
        };
        let mut record =
            BucketKeyRecord::new(BucketKeyRef::new(bucket_id, 1), vault_id, [5; 32], 1);
        record.vault_entry = Some(vault_id);
        let other = BucketKeyRecord::new(BucketKeyRef::new(other_id, 1), vault_id, [5; 32], 1);
        let vault_key = VaultEntry::new(VaultPurpose::BucketKey, vault_id).key();
        let suffixed = |id: Ulid| [&id.to_bytes()[..], b"row"].concat();
        let rows = [
            (
                BUCKET_ENCRYPTION_KEYSPACE,
                bucket.as_bytes().to_vec(),
                settings.to_bytes().unwrap(),
            ),
            (
                BUCKET_KEY_KEYSPACE,
                record.key.key(),
                record.to_bytes().unwrap(),
            ),
            (
                BUCKET_KEY_KEYSPACE,
                other.key.key(),
                other.to_bytes().unwrap(),
            ),
            (BUCKET_HOLDER_KEYSPACE, suffixed(bucket_id), vec![1]),
            (KEY_COPY_KEYSPACE, suffixed(bucket_id), vec![1]),
            (NODE_VAULT_KEYSPACE, vault_key.clone(), vec![1]),
            (ABE_DUE_KEYSPACE, bucket_id.to_bytes().to_vec(), vec![1]),
            (ABE_REISSUE_KEYSPACE, bucket_id.to_bytes().to_vec(), vec![1]),
            (ABE_REKEY_KEYSPACE, bucket_id.to_bytes().to_vec(), vec![1]),
        ];
        for (key_space, key, value) in rows.clone() {
            storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }

        drive(DeleteBucketOperation::new(bucket.clone()), &driver_ctx)
            .await
            .unwrap();

        // Every row of the bucket goes; the keys of another bucket stay.
        for (index, (key_space, key, _)) in rows.into_iter().enumerate() {
            let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("missing read result");
            };
            assert_eq!(value.is_some(), index == 2, "row {index} of {key_space}");
        }
    }

    #[test]
    fn commit_locks_keys() {
        let mut operation = DeleteBucketOperation::new("bucket".to_string());
        let bucket_id = Ulid::generate();
        operation.bucket_id = Some(bucket_id);
        operation.state = DeleteBucketState::CommitTransaction;
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::generate(),
        }));
        let lock = Effect::Blob(BlobEffect::LockKey {
            bucket_id,
            session: None,
        });
        assert!(effects.contains(&lock), "{effects:?}");
    }

    #[tokio::test]
    async fn removes_replication_config() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let bucket = "bucket-a".to_string();
        drive(
            CreateBucketOperation::new(
                bucket.clone(),
                BucketInfo {
                    group_id: Ulid::generate(),
                    created_at: SystemTime::now(),
                    created_by: aruna_core::UserId::nil(RealmId::from_bytes([0u8; 32])),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                },
            ),
            &driver_ctx,
        )
        .await
        .unwrap();

        drive(DeleteBucketOperation::new(bucket.clone()), &driver_ctx)
            .await
            .unwrap();

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.clone().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing bucket read result");
        };
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn deletes_sync_records() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let bucket = "ws-temporary".to_string();
        drive(
            CreateBucketOperation::new(
                bucket.clone(),
                BucketInfo {
                    group_id: Ulid::generate(),
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                },
            ),
            &driver_ctx,
        )
        .await
        .unwrap();
        let realm_id = RealmId::from_bytes([7; 32]);
        let local_node = iroh::SecretKey::from_bytes(&[8; 32]).public();
        let remote_node = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let creator = aruna_core::UserId::nil(realm_id);
        let relationships = [
            SyncRelationship {
                id: Ulid::from(1u128),
                source: ArunaArn::s3_bucket(realm_id, local_node, &bucket).unwrap(),
                target: ArunaArn::s3_bucket(realm_id, remote_node, "target").unwrap(),
                mode: SyncMode::Continuous,
                reference_handling: Default::default(),
                reference_serving: false,
                replicate_deletes: true,
                created_by: creator,
                created_at: SystemTime::UNIX_EPOCH,
                state: SyncState::Enabled,
                status: SyncStatusSnapshot::default(),
            },
            SyncRelationship {
                id: Ulid::from(2u128),
                source: ArunaArn::s3_bucket(realm_id, remote_node, "source").unwrap(),
                target: ArunaArn::s3_bucket(realm_id, local_node, &bucket).unwrap(),
                mode: SyncMode::Continuous,
                reference_handling: Default::default(),
                reference_serving: false,
                replicate_deletes: true,
                created_by: creator,
                created_at: SystemTime::UNIX_EPOCH,
                state: SyncState::Enabled,
                status: SyncStatusSnapshot::default(),
            },
        ];
        let keys = [
            (
                RELATIONSHIP_OUT_KEYSPACE,
                sync_relationship_key(&bucket, relationships[0].id),
                relationships[0].to_bytes().unwrap(),
            ),
            (
                RELATIONSHIP_IN_KEYSPACE,
                sync_relationship_key(&bucket, relationships[1].id),
                relationships[1].to_bytes().unwrap(),
            ),
        ];
        for (key_space, key, value) in &keys {
            storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: (*key_space).to_string(),
                    key: key.clone().into(),
                    value: value.clone().into(),
                    txn_id: None,
                })
                .await;
        }

        drive(DeleteBucketOperation::new(bucket), &driver_ctx)
            .await
            .unwrap();

        for (key_space, key, _) in keys {
            let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("missing sync relationship read result");
            };
            assert!(value.is_none());
        }
        for relationship in relationships {
            let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
                    key: relationship.id.to_bytes().to_vec().into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("missing sync mirror repair read result");
            };
            assert!(value.is_some());
        }
    }

    #[tokio::test]
    async fn nonempty_bucket_rejected() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let bucket = "bucket-a".to_string();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.clone().into(),
                value: BucketInfo {
                    group_id: Ulid::generate(),
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;

        let version_id = Ulid::generate();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(&bucket, "key").to_bytes().unwrap().into(),
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
                key: VersionKey::new(&bucket, "key", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::deleted(SystemTime::now(), Default::default())
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;

        let result = drive(DeleteBucketOperation::new(bucket), &driver_ctx).await;
        assert_eq!(result.unwrap_err(), DeleteBucketError::NotEmpty);
    }

    #[tokio::test]
    async fn versioned_bucket_rejected() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let bucket = "bucket-a".to_string();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.clone().into(),
                value: BucketInfo {
                    group_id: Ulid::generate(),
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&bucket, "deleted-key", Ulid::generate())
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::deleted(SystemTime::now(), Default::default())
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;

        let result = drive(DeleteBucketOperation::new(bucket), &driver_ctx).await;
        assert_eq!(result.unwrap_err(), DeleteBucketError::NotEmpty);
    }
}
