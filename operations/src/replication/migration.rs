use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    API_STATE_KEYSPACE, S3_BUCKET_KEYSPACE, S3_BUCKET_REPLICATION_KEYSPACE,
};
use aruna_core::structs::{
    ArunaArn, BucketInfo, BucketReplicationConfig, RealmId, SyncMode, SyncRelationship, SyncState,
    SyncStatusSnapshot,
};
use aruna_storage::StorageHandle;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::sync_mirror_repair::{
    SyncMirrorRepairError, SyncMirrorRepairIntent, clear_mirror_repair, delete_sync_mirror,
    ensure_sync_mirror, kick_mirror_repair, stage_mirror_delete, stage_mirror_reconcile,
};
use crate::sync_relationship::{
    ListSyncRelationshipsOperation, StoreSyncRelationshipOperation, SyncRelationshipDirection,
    SyncRelationshipError,
};

const MIGRATION_PAGE_SIZE: usize = 128;
const LEGACY_SYNC_MIGRATION_KEY: &[u8] = b"migration/s3-sync-relationships-v1";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegacySyncMigrationSummary {
    pub migrated: usize,
    pub skipped: usize,
    pub failed: usize,
    pub already_complete: bool,
}

#[derive(Debug, Error)]
pub enum LegacySyncMigrationError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Relationship(#[from] SyncRelationshipError),
    #[error(transparent)]
    Repair(#[from] SyncMirrorRepairError),
    #[error("unexpected storage event: {0:?}")]
    UnexpectedEvent(Event),
}

pub async fn migrate_legacy_sync(
    context: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
) -> Result<LegacySyncMigrationSummary, LegacySyncMigrationError> {
    if migration_complete(&context.storage_handle).await? {
        return Ok(LegacySyncMigrationSummary {
            already_complete: true,
            ..Default::default()
        });
    }

    let (configs, malformed) = read_legacy_configs(&context.storage_handle).await?;
    let mut summary = LegacySyncMigrationSummary {
        failed: malformed,
        ..Default::default()
    };
    for (bucket, mut config) in configs {
        let Some(bucket_info) = read_bucket_info(&context.storage_handle, &bucket).await? else {
            warn!(%bucket, "Skipping legacy replication config for missing bucket");
            summary.failed = summary.failed.saturating_add(config.targets.len());
            continue;
        };
        let source = match ArunaArn::s3_bucket(realm_id, node_id, bucket.clone()) {
            Ok(source) if !bucket.starts_with("ws-") => source,
            Ok(_) => {
                summary.failed = summary.failed.saturating_add(config.targets.len());
                continue;
            }
            Err(error) => {
                warn!(%bucket, %error, "Skipping invalid legacy replication source");
                summary.failed = summary.failed.saturating_add(config.targets.len());
                continue;
            }
        };
        let mut existing = drive(
            ListSyncRelationshipsOperation::new(
                SyncRelationshipDirection::Outgoing,
                Some(bucket.clone()),
            ),
            context,
        )
        .await?;

        for target in config.targets.clone() {
            let target_arn = match ArunaArn::s3_bucket(
                target.realm_id,
                target.node_id,
                target.bucket.clone(),
            ) {
                Ok(target_arn)
                    if target.realm_id == realm_id
                        && !target.bucket.starts_with("ws-")
                        && ArunaArn::parse(&target.arn).is_ok_and(|arn| arn == target_arn)
                        && target_arn != source =>
                {
                    target_arn
                }
                Ok(_) => {
                    warn!(bucket = %bucket, target = %target.arn, "Skipping invalid legacy replication target");
                    summary.failed = summary.failed.saturating_add(1);
                    continue;
                }
                Err(error) => {
                    warn!(bucket = %bucket, target = %target.arn, %error, "Skipping malformed legacy replication target");
                    summary.failed = summary.failed.saturating_add(1);
                    continue;
                }
            };
            let relationship_id = legacy_sync_id(&source, &target_arn);
            if existing.iter().any(|relationship| {
                (relationship.source == source
                    && relationship.target == target_arn
                    && relationship.mode == SyncMode::Continuous)
                    || relationship.id == relationship_id
            }) {
                prune_legacy_target(&context.storage_handle, &bucket, &mut config, &target).await?;
                summary.skipped = summary.skipped.saturating_add(1);
                continue;
            }

            let relationship = SyncRelationship {
                id: relationship_id,
                source: source.clone(),
                target: target_arn,
                mode: SyncMode::Continuous,
                replicate_deletes: target.replicate_delete_markers,
                created_by: bucket_info.created_by,
                created_at: bucket_info.created_at,
                state: SyncState::Enabled,
                status: SyncStatusSnapshot::default(),
            };
            stage_mirror_reconcile(context, &relationship).await?;
            if let Err(error) = ensure_sync_mirror(context, node_id, &relationship).await {
                kick_mirror_repair(context).await;
                warn!(
                    relationship_id = %relationship.id,
                    target = %relationship.target,
                    %error,
                    "Failed to create migrated sync mirror"
                );
                summary.failed = summary.failed.saturating_add(1);
                continue;
            }
            if let Err(error) = drive(
                StoreSyncRelationshipOperation::new(
                    relationship.clone(),
                    SyncRelationshipDirection::Outgoing,
                ),
                context,
            )
            .await
            {
                if let Err(stage_error) = stage_mirror_delete(context, &relationship).await {
                    warn!(
                        relationship_id = %relationship.id,
                        error = %stage_error,
                        "Failed to stage migrated sync mirror rollback"
                    );
                }
                kick_mirror_repair(context).await;
                if let Err(rollback_error) =
                    delete_sync_mirror(context, node_id, &relationship).await
                {
                    warn!(
                        relationship_id = %relationship.id,
                        error = %rollback_error,
                        "Failed to roll back migrated sync mirror"
                    );
                } else if let Err(clear_error) = clear_mirror_repair(
                    &context.storage_handle,
                    &relationship,
                    SyncMirrorRepairIntent::Delete,
                )
                .await
                {
                    warn!(relationship_id = %relationship.id, error = %clear_error, "Failed to clear migrated sync mirror repair");
                }
                return Err(error.into());
            }
            if let Err(error) = clear_mirror_repair(
                &context.storage_handle,
                &relationship,
                SyncMirrorRepairIntent::Reconcile,
            )
            .await
            {
                warn!(relationship_id = %relationship.id, %error, "Failed to clear migrated sync mirror repair");
                kick_mirror_repair(context).await;
            }
            prune_legacy_target(&context.storage_handle, &bucket, &mut config, &target).await?;
            existing.push(relationship);
            summary.migrated = summary.migrated.saturating_add(1);
        }
    }

    if summary.failed == 0 {
        mark_migration_complete(&context.storage_handle).await?;
    }
    Ok(summary)
}

fn legacy_sync_id(source: &ArunaArn, target: &ArunaArn) -> Ulid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna:legacy-s3-sync:v1\0");
    hasher.update(source.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(target.to_string().as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ulid::from_bytes(bytes)
}

async fn migration_complete(storage: &StorageHandle) -> Result<bool, LegacySyncMigrationError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: LEGACY_SYNC_MIGRATION_KEY.to_vec().into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LegacySyncMigrationError::UnexpectedEvent(other)),
    }
}

async fn mark_migration_complete(storage: &StorageHandle) -> Result<(), LegacySyncMigrationError> {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: LEGACY_SYNC_MIGRATION_KEY.to_vec().into(),
            value: vec![1].into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LegacySyncMigrationError::UnexpectedEvent(other)),
    }
}

async fn read_legacy_configs(
    storage: &StorageHandle,
) -> Result<(Vec<(String, BucketReplicationConfig)>, usize), LegacySyncMigrationError> {
    let mut configs = Vec::new();
    let mut malformed = 0usize;
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: MIGRATION_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => return Err(LegacySyncMigrationError::UnexpectedEvent(other)),
        };
        for (key, value) in values {
            match (
                String::from_utf8(key.to_vec()),
                BucketReplicationConfig::from_bytes(&value),
            ) {
                (Ok(bucket), Ok(config)) => configs.push((bucket, config)),
                (bucket, config) => {
                    warn!(
                        ?bucket,
                        ?config,
                        "Skipping malformed legacy replication config"
                    );
                    malformed = malformed.saturating_add(1);
                }
            }
        }
        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok((configs, malformed)),
        }
    }
}

async fn read_bucket_info(
    storage: &StorageHandle,
    bucket: &str,
) -> Result<Option<BucketInfo>, LegacySyncMigrationError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => BucketInfo::from_bytes(&value).map(Some).map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LegacySyncMigrationError::UnexpectedEvent(other)),
    }
}

async fn prune_legacy_target(
    storage: &StorageHandle,
    bucket: &str,
    config: &mut BucketReplicationConfig,
    target: &aruna_core::structs::BucketReplicationTarget,
) -> Result<(), LegacySyncMigrationError> {
    config.targets.retain(|candidate| candidate != target);
    let event = if config.targets.is_empty() {
        storage
            .send_storage_effect(StorageEffect::Delete {
                key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                txn_id: None,
            })
            .await
    } else {
        storage
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                value: config.to_bytes()?.into(),
                txn_id: None,
            })
            .await
    };
    match event {
        Event::Storage(StorageEvent::DeleteResult { .. } | StorageEvent::WriteResult { .. }) => {
            Ok(())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LegacySyncMigrationError::UnexpectedEvent(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, S3_BUCKET_REPLICATION_KEYSPACE, SYNC_RELATIONSHIP_OUT_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, BucketReplicationTarget, GroupAuthorizationDocument, RealmAuthorizationDocument,
    };
    use aruna_storage::storage;
    use std::time::SystemTime;
    use tempfile::TempDir;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn test_context() -> (TempDir, DriverContext) {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        (
            storage_dir,
            DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            },
        )
    }

    async fn write_value(storage: &StorageHandle, keyspace: &str, key: Vec<u8>, value: Vec<u8>) {
        assert!(matches!(
            storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: keyspace.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn seed_legacy(
        context: &DriverContext,
        realm_id: RealmId,
        source_node: NodeId,
        target_node: NodeId,
        owner: UserId,
    ) {
        let bucket = "source-bucket";
        let group_id = Ulid::from_bytes([8; 16]);
        let actor = Actor {
            node_id: source_node,
            user_id: owner,
            realm_id,
        };
        write_value(
            &context.storage_handle,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::new_default_realm_doc(realm_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        write_value(
            &context.storage_handle,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        let bucket_info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: owner,
            cors_configuration: None,
        };
        write_value(
            &context.storage_handle,
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec(),
            bucket_info.to_bytes().unwrap(),
        )
        .await;
        if target_node == source_node {
            write_value(
                &context.storage_handle,
                S3_BUCKET_KEYSPACE,
                b"target-bucket".to_vec(),
                bucket_info.to_bytes().unwrap(),
            )
            .await;
        }
        let target_arn = ArunaArn::s3_bucket(realm_id, target_node, "target-bucket").unwrap();
        write_value(
            &context.storage_handle,
            S3_BUCKET_REPLICATION_KEYSPACE,
            bucket.as_bytes().to_vec(),
            BucketReplicationConfig {
                targets: vec![BucketReplicationTarget {
                    node_id: target_node,
                    realm_id,
                    bucket: "target-bucket".to_string(),
                    arn: target_arn.to_string(),
                    replicate_delete_markers: true,
                }],
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
    }

    async fn read_legacy(storage: &StorageHandle, bucket: &str) -> Option<BucketReplicationConfig> {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                value.map(|value| BucketReplicationConfig::from_bytes(&value).unwrap())
            }
            event => panic!("unexpected event: {event:?}"),
        }
    }

    #[tokio::test]
    async fn migrates_legacy_once() {
        let (_storage_dir, context) = test_context();
        let realm_id = RealmId([4; 32]);
        let source_node = node(5);
        let owner = UserId::local(Ulid::from_bytes([7; 16]), realm_id);
        seed_legacy(&context, realm_id, source_node, source_node, owner).await;

        let first = migrate_legacy_sync(&context, source_node, realm_id)
            .await
            .unwrap();
        assert_eq!(first.migrated, 1);
        assert_eq!(first.failed, 0);
        let relationships = drive(
            ListSyncRelationshipsOperation::new(
                SyncRelationshipDirection::Outgoing,
                Some("source-bucket".to_string()),
            ),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].created_by, owner);
        assert_eq!(relationships[0].created_at, SystemTime::UNIX_EPOCH);
        assert_eq!(relationships[0].mode, SyncMode::Continuous);
        assert!(relationships[0].replicate_deletes);
        let incoming = drive(
            ListSyncRelationshipsOperation::new(
                SyncRelationshipDirection::Incoming,
                Some("target-bucket".to_string()),
            ),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(incoming, relationships);

        let second = migrate_legacy_sync(&context, source_node, realm_id)
            .await
            .unwrap();
        assert!(second.already_complete);
        assert!(
            read_legacy(&context.storage_handle, "source-bucket")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn skips_existing_relationship() {
        let (_storage_dir, context) = test_context();
        let realm_id = RealmId([11; 32]);
        let source_node = node(12);
        let target_node = node(13);
        let owner = UserId::local(Ulid::from_bytes([14; 16]), realm_id);
        seed_legacy(&context, realm_id, source_node, target_node, owner).await;
        let relationship = SyncRelationship {
            id: Ulid::from_bytes([15; 16]),
            source: ArunaArn::s3_bucket(realm_id, source_node, "source-bucket").unwrap(),
            target: ArunaArn::s3_bucket(realm_id, target_node, "target-bucket").unwrap(),
            mode: SyncMode::Continuous,
            replicate_deletes: false,
            created_by: owner,
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Paused,
            status: SyncStatusSnapshot::default(),
        };
        write_value(
            &context.storage_handle,
            SYNC_RELATIONSHIP_OUT_KEYSPACE,
            aruna_core::structs::sync_relationship_key("source-bucket", relationship.id),
            relationship.to_bytes().unwrap(),
        )
        .await;

        let summary = migrate_legacy_sync(&context, source_node, realm_id)
            .await
            .unwrap();
        assert_eq!(summary.migrated, 0);
        assert_eq!(summary.skipped, 1);
        let relationships = drive(
            ListSyncRelationshipsOperation::new(
                SyncRelationshipDirection::Outgoing,
                Some("source-bucket".to_string()),
            ),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(relationships, vec![relationship]);
        assert!(
            read_legacy(&context.storage_handle, "source-bucket")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn retries_failed_mirror() {
        let (_storage_dir, context) = test_context();
        let realm_id = RealmId([21; 32]);
        let source_node = node(22);
        let target_node = node(23);
        let owner = UserId::local(Ulid::from_bytes([24; 16]), realm_id);
        seed_legacy(&context, realm_id, source_node, target_node, owner).await;

        for _ in 0..2 {
            let summary = migrate_legacy_sync(&context, source_node, realm_id)
                .await
                .unwrap();
            assert_eq!(summary.migrated, 0);
            assert_eq!(summary.failed, 1);
            assert!(!summary.already_complete);
            assert!(!migration_complete(&context.storage_handle).await.unwrap());
            assert_eq!(
                read_legacy(&context.storage_handle, "source-bucket")
                    .await
                    .unwrap()
                    .targets
                    .len(),
                1
            );
        }
    }

    #[tokio::test]
    async fn keeps_failed_target() {
        let (_storage_dir, context) = test_context();
        let realm_id = RealmId([31; 32]);
        let source_node = node(32);
        let failed_node = node(33);
        let owner = UserId::local(Ulid::from_bytes([34; 16]), realm_id);
        seed_legacy(&context, realm_id, source_node, source_node, owner).await;
        let failed_arn = ArunaArn::s3_bucket(realm_id, failed_node, "failed-bucket").unwrap();
        let failed_target = BucketReplicationTarget {
            node_id: failed_node,
            realm_id,
            bucket: "failed-bucket".to_string(),
            arn: failed_arn.to_string(),
            replicate_delete_markers: false,
        };
        let mut config = read_legacy(&context.storage_handle, "source-bucket")
            .await
            .unwrap();
        config.targets.push(failed_target.clone());
        write_value(
            &context.storage_handle,
            S3_BUCKET_REPLICATION_KEYSPACE,
            b"source-bucket".to_vec(),
            config.to_bytes().unwrap(),
        )
        .await;

        let summary = migrate_legacy_sync(&context, source_node, realm_id)
            .await
            .unwrap();
        assert_eq!(summary.migrated, 1);
        assert_eq!(summary.failed, 1);
        assert_eq!(
            read_legacy(&context.storage_handle, "source-bucket")
                .await
                .unwrap()
                .targets,
            vec![failed_target]
        );
    }
}
