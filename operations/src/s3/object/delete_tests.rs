use super::*;
use crate::driver::{DriverContext, drive};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_blob::blob::BlobHandler;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PATHS_INDEX_KEYSPACE,
    NODE_STATS_KEYSPACE, USAGE_STATS_KEYSPACE,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BlobHeadKey, BlobVersion, CurrentVersionPointer, HashIndex, VersionKey,
};
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_net::{NetConfig, NetHandle};
use aruna_storage::storage;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::fs::exists;
use tempfile::tempdir;
use ulid::Ulid;

fn test_node_id() -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[7; 32]).public()
}

fn test_user_id() -> aruna_core::UserId {
    aruna_core::UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32]))
}

#[test]
fn obligation_keeps_restrictions() {
    // The durable repair record is what a lost enqueue replays, so a scoped
    // credential must stay scoped on it.
    let restrictions = vec![PathRestriction {
        pattern: "/realm/g/group/data/node/bucket/scoped/**".to_string(),
        permission: aruna_core::structs::identity::auth::Permission::WRITE,
    }];
    let mut operation = DeleteObjectOperation::new(DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "scoped/key".to_string(),
        version_id: None,
        group_id: Ulid::generate(),
        realm_id: RealmId::from_bytes([1u8; 32]),
        node_id: iroh::SecretKey::generate().public(),
        deleted_by: test_user_id(),
    })
    .with_restrictions(Some(restrictions.clone()));
    operation.version_id = Some(Ulid::generate());

    let effects = operation.write_obligation();

    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected one obligation write, got {effects:?}")
    };
    let record = crate::replication::queue::LiveObligationRecord::from_bytes(value.as_ref())
        .expect("obligation decodes");
    assert_eq!(record.auth_context.path_restrictions, Some(restrictions));
}

fn audit_op(version_id: Option<Ulid>) -> DeleteObjectOperation {
    let mut operation = DeleteObjectOperation::new(DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "reports/a.csv".to_string(),
        version_id,
        group_id: Ulid::from_bytes([2u8; 16]),
        realm_id: RealmId::from_bytes([1u8; 32]),
        node_id: test_node_id(),
        deleted_by: test_user_id(),
    });
    operation.txn_id = Some(Ulid::generate());
    operation
}

fn audit_record(effects: &[Effect]) -> aruna_core::structs::storage::delete_audit::BlobAuditRecord {
    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects
    else {
        panic!("expected one audit write, got {effects:?}")
    };
    assert_eq!(key_space, DELETE_AUDIT_KEYSPACE);
    aruna_core::structs::storage::delete_audit::BlobAuditRecord::from_bytes(value.as_ref()).expect("audit record decodes")
}

#[test]
fn audits_delete_marker() {
    let marker = Ulid::generate();
    let mut operation = audit_op(None);
    operation.version_id = Some(marker);

    let record = audit_record(&operation.write_delete_audit());
    assert_eq!(
        record.kind,
        aruna_core::structs::storage::delete_audit::BlobAuditKind::DeleteMarker
    );
    assert_eq!(record.version_id, Some(marker));
    assert_eq!(record.bucket, "bucket");
    assert_eq!(record.key, "reports/a.csv");
}

#[test]
fn audits_version_delete() {
    let version_id = Ulid::generate();
    let mut operation = audit_op(Some(version_id));

    let record = audit_record(&operation.write_delete_audit());
    assert_eq!(
        record.kind,
        aruna_core::structs::storage::delete_audit::BlobAuditKind::DeleteVersion
    );
    assert_eq!(record.version_id, Some(version_id));
}

#[test]
fn typed_storage_failure() {
    let mut operation = DeleteObjectOperation::new(DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "reports/a.csv".to_string(),
        version_id: None,
        group_id: Ulid::from_bytes([2u8; 16]),
        realm_id: RealmId::from_bytes([1u8; 32]),
        node_id: test_node_id(),
        deleted_by: test_user_id(),
    });
    operation.start();

    operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::CommitFailed,
    }));

    assert!(operation.is_complete());
    assert_eq!(
        operation.finalize(),
        Err(DeleteObjectError::StorageError(StorageError::CommitFailed))
    );
}

#[test]
fn begun_storage_failure() {
    // A failure after the transaction began must roll it back exactly once
    // and still report the typed storage error.
    let txn_id = Ulid::from_bytes([9u8; 16]);
    let mut operation = DeleteObjectOperation::new(DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "reports/a.csv".to_string(),
        version_id: None,
        group_id: Ulid::from_bytes([2u8; 16]),
        realm_id: RealmId::from_bytes([1u8; 32]),
        node_id: test_node_id(),
        deleted_by: test_user_id(),
    });
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));

    let effects = operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::CommitFailed,
    }));

    assert_eq!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    );
    assert!(operation.abort().is_empty());
    assert_eq!(
        operation.finalize(),
        Err(DeleteObjectError::StorageError(StorageError::CommitFailed))
    );
}

fn deleted_version_value(user_id: aruna_core::UserId) -> aruna_core::types::Value {
    BlobVersion::deleted(SystemTime::UNIX_EPOCH, user_id)
        .to_bytes()
        .unwrap()
        .into()
}

fn deleted_version_entry(
    bucket: &str,
    key: &str,
    version_id: Ulid,
    user_id: aruna_core::UserId,
) -> (aruna_core::types::Key, aruna_core::types::Value) {
    (
        VersionKey::new(bucket, key, version_id)
            .to_bytes()
            .unwrap()
            .into(),
        deleted_version_value(user_id),
    )
}

#[test]
fn reference_counts_content() {
    let version_id = Ulid::generate();
    let version = BlobVersion::reference(
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaNative,
                public_config: HashMap::new(),
                source_path: "source/object.bin".to_string(),
                version_selector: Some(format!("version:{version_id}")),
                capabilities: Vec::new(),
                origin_node_id: Some(iroh::SecretKey::from_bytes(&[4u8; 32]).public()),
            },
            connector_id: None,
        },
        SourceMetadata {
            content_length: 1_000_000,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
        },
        SystemTime::UNIX_EPOCH,
        Default::default(),
        SystemTime::UNIX_EPOCH,
    );
    let summary = VersionSummary::from_blob_version(version_id, &version);

    assert_eq!(summary.logical_size, Some(1_000_000));
    assert!(summary.referenced);
}

fn drifted_delete_op() -> DeleteObjectOperation {
    let version_id = Ulid::from_bytes([2u8; 16]);
    let mut op = DeleteObjectOperation::new(DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "key".to_string(),
        version_id: Some(version_id),
        group_id: Ulid::from_bytes([1u8; 16]),
        realm_id: RealmId::from_bytes([1u8; 32]),
        node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
        deleted_by: test_user_id(),
    });
    op.txn_id = Some(Ulid::from_bytes([7u8; 16]));
    op.target_version = Some(VersionSummary {
        version_id,
        materialized_hash: Some([4u8; 32]),
        logical_size: None,
        referenced: false,
        deleted: false,
    });
    op.existing_pointer = Some(CurrentVersionPointer::new(version_id));
    op.target_size = Some(34_788_022);
    op
}

#[test]
fn drifted_counter_commits() {
    // A counter below the deleted object's size must clamp, not abort the
    // transaction and strand the object.
    let mut op = drifted_delete_op();
    let effects = op.start_usage_update();
    let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice() else {
        panic!("expected the counter read, got {effects:?}")
    };
    let stored = aruna_core::structs::storage::usage::UsageCounters {
        objects: 1,
        logical_bytes: 1_096_145,
        ..Default::default()
    }
    .to_bytes()
    .unwrap();
    let values = reads
        .iter()
        .map(|(_, key)| {
            (
                key.clone(),
                Some(aruna_core::types::Value::from(stored.clone())),
            )
        })
        .collect();

    let effects = op.handle_usage_update(Event::Storage(StorageEvent::BatchReadResult { values }));

    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the clamped counter write, got {effects:?}")
    };
    assert_ne!(op.state, DeleteObjectState::Error);
    let counters: Vec<aruna_core::structs::storage::usage::UsageCounters> = writes
        .iter()
        .filter(|(key_space, ..)| key_space == USAGE_STATS_KEYSPACE)
        .map(|(_, _, value)| {
            aruna_core::structs::storage::usage::UsageCounters::from_bytes(value.as_ref()).unwrap()
        })
        .collect();
    assert_eq!(counters.len(), reads.len());
    assert!(counters.iter().all(|counters| counters.logical_bytes == 0));
    // The dirty markers still ride the same transaction.
    assert!(
        writes
            .iter()
            .any(|(key_space, ..)| key_space == NODE_STATS_KEYSPACE)
    );

    let effects = op.handle_usage_update(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert_eq!(op.state, DeleteObjectState::WriteDeleteAudit);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { .. })]
    ));
}

fn candidate_op(location: Option<BlobLocationKey>) -> DeleteObjectOperation {
    let mut op = DeleteObjectOperation::new(DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "key".to_string(),
        version_id: Some(Ulid::from_bytes([2u8; 16])),
        group_id: Ulid::from_bytes([1u8; 16]),
        realm_id: RealmId::from_bytes([1u8; 32]),
        node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
        deleted_by: test_user_id(),
    });
    op.txn_id = Some(Ulid::from_bytes([7u8; 16]));
    op.target_location = location;
    op
}

#[test]
fn queues_deleted_copy() {
    // The candidate rides the delete transaction, keyed backend first.
    let key = BlobLocationKey::new([4u8; 32], aruna_core::structs::storage::blob::BackendRef::node_default());
    let mut op = candidate_op(Some(key.clone()));

    let effects = op.write_reclaim_candidate();

    let [
        Effect::Storage(StorageEffect::Write {
            key_space,
            key: written,
            txn_id,
            ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected candidate write, got {effects:?}")
    };
    assert_eq!(key_space, BLOB_RECLAIM_KEYSPACE);
    assert_eq!(*txn_id, op.txn_id);
    assert_eq!(
        written.as_ref(),
        ReclaimCandidateKey::new(key.backend, key.blake3_hash)
            .to_bytes()
            .as_slice()
    );
}

#[test]
fn marker_queues_nothing() {
    // No reclaim candidate without a location; the delete trail still rides
    // the same transaction.
    let mut op = candidate_op(None);

    let effects = op.write_reclaim_candidate();

    let [
        Effect::Storage(StorageEffect::Write {
            key_space, txn_id, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected the audit write, got {effects:?}")
    };
    assert_eq!(key_space, DELETE_AUDIT_KEYSPACE);
    assert_eq!(*txn_id, op.txn_id);
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
        panic!("unexpected storage event");
    };

    value
}

fn delete_until_lookup(
    target_version_id: Ulid,
    all_version_ids: Vec<Ulid>,
    user_id: aruna_core::UserId,
) -> DeleteObjectOperation {
    let mut op = DeleteObjectOperation::new(DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "key".to_string(),
        version_id: Some(target_version_id),
        group_id: Ulid::generate(),
        realm_id: RealmId::from_bytes([1u8; 32]),
        node_id: iroh::SecretKey::generate().public(),
        deleted_by: user_id,
    });

    let effects = op.start();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    ));

    let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::generate(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE
    ));

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![1u8].into(),
        value: Some(deleted_version_value(user_id)),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { key_space, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE
    ));

    let effects = op.step(Event::Storage(StorageEvent::IterResult {
        values: all_version_ids
            .into_iter()
            .map(|version_id| deleted_version_entry("bucket", "key", version_id, user_id))
            .collect(),
        next_start_after: None,
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_HEAD_KEYSPACE
    ));

    op
}

#[test]
fn preserves_noncurrent_pointer() {
    let user_id = test_user_id();
    let current_version_id = Ulid::from_bytes([1u8; 16]);
    let target_version_id = Ulid::from_bytes([2u8; 16]);
    let newer_version_id = Ulid::from_bytes([9u8; 16]);
    let mut op = delete_until_lookup(
        target_version_id,
        vec![current_version_id, target_version_id, newer_version_id],
        user_id,
    );
    let current_pointer = CurrentVersionPointer::new_with_generation(current_version_id, 20);

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![2u8].into(),
        value: Some(current_pointer.to_bytes().unwrap().into()),
    }));

    assert_eq!(op.state, DeleteObjectState::DeleteTargetVersion);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Delete { key_space, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE
    ));
}

#[test]
fn rewrites_current_pointer() {
    let user_id = test_user_id();
    let target_version_id = Ulid::from_bytes([1u8; 16]);
    let fallback_version_id = Ulid::from_bytes([9u8; 16]);
    let mut op = delete_until_lookup(
        target_version_id,
        vec![target_version_id, fallback_version_id],
        user_id,
    );
    let current_pointer = CurrentVersionPointer::new_with_generation(target_version_id, 20);

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![2u8].into(),
        value: Some(current_pointer.to_bytes().unwrap().into()),
    }));

    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected blob head write");
    };
    assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
    assert_eq!(
        CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(fallback_version_id, 21)
    );
}

#[test]
fn removes_final_pointer() {
    let user_id = test_user_id();
    let target_version_id = Ulid::from_bytes([1u8; 16]);
    let mut op = delete_until_lookup(target_version_id, vec![target_version_id], user_id);
    let current_pointer = CurrentVersionPointer::new_with_generation(target_version_id, 20);

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![2u8].into(),
        value: Some(current_pointer.to_bytes().unwrap().into()),
    }));

    assert_eq!(op.state, DeleteObjectState::ApplyHeadTransition);
    assert_eq!(effects.len(), 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Delete { key_space, .. })]
            if key_space == BLOB_HEAD_KEYSPACE
    ));
}

#[tokio::test]
async fn missing_version_rejected() {
    let temp_handle = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let result = drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: "mybucket".to_string(),
            key: "missing.txt".to_string(),
            version_id: Some(Ulid::generate()),
            group_id: Ulid::generate(),
            realm_id: RealmId::from_bytes([1u8; 32]),
            node_id: iroh::SecretKey::generate().public(),
            deleted_by: test_user_id(),
        }),
        &context,
    )
    .await;

    assert!(matches!(result, Err(DeleteObjectError::NoSuchVersion)));
}

#[tokio::test]
async fn creates_tombstone() {
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
            root: blob_root,
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
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let user_id = aruna_core::UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32]));
    let group_id = Ulid::generate();
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let put_result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id,
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: "to-delete.txt".to_string(),
                content_length: Some(5),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                    &b"hello"[..],
                ))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        &context,
    )
    .await
    .unwrap();

    let delete_result = drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: "mybucket".to_string(),
            key: "to-delete.txt".to_string(),
            version_id: None,
            group_id,
            realm_id,
            node_id,
            deleted_by: user_id,
        }),
        &context,
    )
    .await
    .unwrap();

    assert!(exists(put_result.location.get_full_path().unwrap()).unwrap());

    let blob_head = read_value(
        &context,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new("mybucket", "to-delete.txt")
            .to_bytes()
            .unwrap(),
    )
    .await
    .expect("missing blob head entry");
    assert_eq!(
        CurrentVersionPointer::from_bytes(blob_head.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(delete_result.version_id, 2)
    );

    let blob_tombstone = read_value(
        &context,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new("mybucket", "to-delete.txt", delete_result.version_id)
            .to_bytes()
            .unwrap(),
    )
    .await
    .expect("missing blob tombstone version");
    let blob_tombstone = BlobVersion::from_bytes(blob_tombstone.as_ref()).unwrap();
    assert!(blob_tombstone.is_deleted());
    assert_eq!(blob_tombstone.created_by, user_id);

    let historical_hash_path = read_value(
        &context,
        PATHS_INDEX_KEYSPACE,
        HashIndex::new(
            put_result
                .location
                .get_blake3()
                .unwrap()
                .try_into()
                .unwrap(),
            put_result.version_id,
            realm_id,
            group_id,
            node_id,
            "mybucket",
            "to-delete.txt",
        )
        .to_bytes()
        .unwrap(),
    )
    .await
    .expect("missing historical materialized hash path entry");
    assert!(historical_hash_path.is_empty());

    let get_result = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: "mybucket".to_string(),
            key: "to-delete.txt".to_string(),
            version_id: None,
            range: None,
            group_id: Ulid::generate(),
            user_identity: user_id,
            node_id: test_node_id(),
        }),
        &context,
    )
    .await;
    assert!(matches!(get_result, Err(GetObjectError::NoSuchKey)));
}

#[tokio::test]
async fn deletes_version() {
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
            root: blob_root,
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
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let user_id = aruna_core::UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32]));
    let group_id = Ulid::generate();
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let put_result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id,
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                content_length: Some(5),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                    &b"hello"[..],
                ))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        &context,
    )
    .await
    .unwrap();

    let tombstone = drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: "mybucket".to_string(),
            key: "versioned.txt".to_string(),
            version_id: None,
            group_id,
            realm_id,
            node_id,
            deleted_by: user_id,
        }),
        &context,
    )
    .await
    .unwrap();
    assert!(tombstone.delete_marker);

    let delete_marker_result = drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: "mybucket".to_string(),
            key: "versioned.txt".to_string(),
            version_id: Some(tombstone.version_id),
            group_id,
            realm_id,
            node_id,
            deleted_by: user_id,
        }),
        &context,
    )
    .await
    .unwrap();
    assert!(delete_marker_result.delete_marker);

    let blob_head = read_value(
        &context,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new("mybucket", "versioned.txt")
            .to_bytes()
            .unwrap(),
    )
    .await
    .expect("missing blob head entry after deleting marker");
    assert_eq!(
        CurrentVersionPointer::from_bytes(blob_head.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(put_result.version_id, 3)
    );

    let restored_hash_path = read_value(
        &context,
        PATHS_INDEX_KEYSPACE,
        HashIndex::new(
            put_result
                .location
                .get_blake3()
                .unwrap()
                .try_into()
                .unwrap(),
            put_result.version_id,
            realm_id,
            group_id,
            node_id,
            "mybucket",
            "versioned.txt",
        )
        .to_bytes()
        .unwrap(),
    )
    .await
    .expect("missing restored hash path index");
    assert!(restored_hash_path.is_empty());

    assert!(
        read_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "versioned.txt", tombstone.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_none()
    );

    let mut restored_blob = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: "mybucket".to_string(),
            key: "versioned.txt".to_string(),
            version_id: None,
            range: None,
            group_id: Ulid::generate(),
            user_identity: user_id,
            node_id: test_node_id(),
        }),
        &context,
    )
    .await
    .unwrap()
    .blob;
    let restored = restored_blob.next().await.unwrap().unwrap();
    assert_eq!(restored.as_ref(), b"hello");

    let removed_object_version = drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: "mybucket".to_string(),
            key: "versioned.txt".to_string(),
            version_id: Some(put_result.version_id),
            group_id,
            realm_id,
            node_id,
            deleted_by: user_id,
        }),
        &context,
    )
    .await
    .unwrap();
    assert!(!removed_object_version.delete_marker);

    assert!(
        read_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "versioned.txt", put_result.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_none()
    );

    let get_result = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: "mybucket".to_string(),
            key: "versioned.txt".to_string(),
            version_id: None,
            range: None,
            group_id: Ulid::generate(),
            user_identity: user_id,
            node_id: test_node_id(),
        }),
        &context,
    )
    .await;
    assert!(matches!(get_result, Err(GetObjectError::NoSuchKey)));

    assert!(
        read_value(
            &context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("mybucket", "versioned.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_none()
    );

    assert!(
        read_value(
            &context,
            PATHS_INDEX_KEYSPACE,
            HashIndex::new(
                put_result
                    .location
                    .get_blake3()
                    .unwrap()
                    .try_into()
                    .unwrap(),
                put_result.version_id,
                realm_id,
                group_id,
                node_id,
                "mybucket",
                "versioned.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .is_none()
    );

    assert!(
        read_value(
            &context,
            PATHS_INDEX_KEYSPACE,
            HashIndex::new(
                put_result
                    .location
                    .get_blake3()
                    .unwrap()
                    .try_into()
                    .unwrap(),
                put_result.version_id,
                realm_id,
                group_id,
                node_id,
                "mybucket",
                "versioned.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .is_none()
    );
}
