//! Tests object delete: tombstones, audit records, pointer rewrites and queued copy cleanup.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_blob::blob::BlobHandler;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, NODE_STATS_KEYSPACE, PATHS_INDEX_KEYSPACE,
    PENDING_LOCATION_KEYSPACE,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BlobHeadKey, BlobVersion, CurrentVersionPointer, HashIndex, VersionKey,
};
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_net::NetHandle;
use aruna_storage::storage;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::fs::exists;
use tempfile::tempdir;
use ulid::Ulid;

type Stepper = crate::stepper::Stepper<DeleteObjectResult, DeleteObjectError>;

/// The call a step asks for.
fn call(turn: Turn) -> IoCall {
    match turn {
        Ok(Next::Call(call)) => call,
        other => panic!("expected a call, got {other:?}"),
    }
}

/// The key space of the first row a step reads, writes or deletes.
fn space(turn: &Turn) -> &str {
    match turn {
        Ok(Next::Call(IoCall::Read(rows) | IoCall::Delete(rows))) => &rows[0].0,
        Ok(Next::Call(IoCall::Write(rows))) => &rows[0].0,
        Ok(Next::Call(IoCall::Page { space, .. })) => space,
        other => panic!("expected a storage call, got {other:?}"),
    }
}

fn read(value: Option<aruna_core::types::Value>) -> IoResult {
    IoResult::Values(vec![value])
}

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

    let written = call(operation.write_obligation());

    let IoCall::Write(rows) = &written else {
        panic!("expected one obligation write, got {written:?}")
    };
    let [(_, _, value)] = rows.as_slice() else {
        panic!("expected one obligation row, got {rows:?}")
    };
    let record = aruna_core::structs::storage::replication_queue::LiveObligationRecord::from_bytes(
        value.as_ref(),
    )
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
    operation.incarnation = Some(INCARNATION);
    operation
}

const INCARNATION: u128 = 9;

fn audit_record(written: &IoCall) -> aruna_core::structs::storage::delete_audit::BlobAuditRecord {
    let IoCall::Write(writes) = written else {
        panic!("expected one audit write, got {written:?}")
    };
    let [(audit_space, _, value), (trail_space, trail, indexed)] = writes.as_slice() else {
        panic!("expected the audit row and its trail entry, got {writes:?}")
    };
    assert_eq!(audit_space, DELETE_AUDIT_KEYSPACE);
    // The same record is indexed by its object key in the same transaction.
    assert_eq!(trail_space, aruna_core::keyspaces::DELETE_TRAIL_KEYSPACE);
    assert_eq!(value, indexed);
    let record =
        aruna_core::structs::storage::delete_audit::BlobAuditRecord::from_bytes(value.as_ref())
            .expect("audit record decodes");
    let prefix = aruna_core::structs::storage::delete_audit::delete_prefix(
        record.group_id,
        &record.bucket,
        INCARNATION,
        &record.key,
    );
    assert!(trail.starts_with(&prefix));
    record
}

#[test]
fn reads_audit_bucket() {
    // Without a known incarnation the creation row is read in the delete transaction first.
    let mut operation = audit_op(None);
    operation.version_id = Some(Ulid::generate());
    operation.incarnation = None;
    let turn = operation.write_delete_audit();
    assert_eq!(
        space(&turn),
        aruna_core::keyspaces::BUCKET_CREATION_KEYSPACE
    );
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(operation.state, DeleteObjectState::ReadAuditBucket);
    let value = Some(INCARNATION.to_be_bytes().to_vec().into());
    audit_record(&call(operation.audit_bucket_read(value)));
}

#[test]
fn audits_delete_marker() {
    let marker = Ulid::generate();
    let mut operation = audit_op(None);
    operation.version_id = Some(marker);

    let record = audit_record(&call(operation.write_delete_audit()));
    assert_eq!(
        record.kind,
        aruna_core::structs::storage::delete_audit::BlobAuditKind::DeleteMarker
    );
    assert_eq!(record.version_id, Some(marker));
    assert_eq!(record.bucket, "bucket");
    assert_eq!(record.key, "reports/a.csv");
}

#[test]
fn fence_refuses_marker() {
    // A pulled delete whose receipt another commit already wrote rolls back instead of
    // stacking a second marker; a free receipt records the marker id before the commit.
    let receipt: aruna_core::types::Key = b"receipt".to_vec().into();
    let space = "receipts".to_string();
    let marker = Ulid::generate();
    let fenced = || {
        let mut operation = audit_op(None).with_fence(
            vec![(space.clone(), receipt.clone(), None)],
            vec![(space.clone(), receipt.clone())],
        );
        operation.version_id = Some(marker);
        operation.write_delete_audit().unwrap();
        operation
    };
    let mut free = fenced();
    let turn = free.next(IoResult::Done);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let written = call(free.next(read(None)));
    let IoCall::Write(writes) = &written else {
        panic!("expected the receipt write, got {written:?}")
    };
    assert_eq!(writes[0].2.as_ref(), marker.to_bytes());
    assert!(matches!(free.next(IoResult::Done), Ok(Next::Then { .. })));

    let mut taken = fenced();
    taken.next(IoResult::Done).unwrap();
    let winner: aruna_core::types::Value = Ulid::generate().to_bytes().to_vec().into();
    assert_eq!(
        taken.next(read(Some(winner))).err(),
        Some(DeleteObjectError::FenceTaken)
    );
}

#[test]
fn audits_version_delete() {
    let version_id = Ulid::generate();
    let mut operation = audit_op(Some(version_id));

    let record = audit_record(&call(operation.write_delete_audit()));
    assert_eq!(
        record.kind,
        aruna_core::structs::storage::delete_audit::BlobAuditKind::DeleteVersion
    );
    assert_eq!(record.version_id, Some(version_id));
}

fn marker_input() -> DeleteObjectInput {
    DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "reports/a.csv".to_string(),
        version_id: None,
        group_id: Ulid::from_bytes([2u8; 16]),
        realm_id: RealmId::from_bytes([1u8; 32]),
        node_id: test_node_id(),
        deleted_by: test_user_id(),
    }
}

#[test]
fn typed_storage_failure() {
    let (mut operation, _) = Stepper::new(DeleteObjectOperation::new(marker_input()));

    operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::CommitFailed,
    }));

    assert!(operation.is_complete());
    assert_eq!(
        operation.finish(),
        Err(DeleteObjectError::StorageError(StorageError::CommitFailed))
    );
}

#[test]
fn begun_storage_failure() {
    // A failure after the transaction began must roll it back exactly once
    // and still report the typed storage error.
    let txn_id = Ulid::from_bytes([9u8; 16]);
    let (mut operation, _) = Stepper::new(DeleteObjectOperation::new(marker_input()));
    operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));

    let effects = operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::CommitFailed,
    }));

    assert_eq!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    );
    assert_eq!(
        operation.finish(),
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
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        },
        SourceMetadata {
            content_length: 1_000_000,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        },
        SystemTime::UNIX_EPOCH,
        Default::default(),
        SystemTime::UNIX_EPOCH,
    );
    let key = VersionKey::new("bucket", "object.bin", version_id);
    let summary = VersionSummary::from_blob_version(&key, &version);

    assert_eq!(summary.logical_size, Some(1_000_000));
    assert!(summary.referenced);
}

fn sized_delete_op() -> DeleteObjectOperation {
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
    op.incarnation = Some(INCARNATION);
    op.target_version = Some(VersionSummary {
        version_id,
        materialized_hash: Some([4u8; 32]),
        logical_size: None,
        referenced: false,
        offered: false,
        deleted: false,
    });
    op.existing_pointer = Some(CurrentVersionPointer::new(version_id));
    op.target_size = Some(34_788_022);
    op
}

#[test]
fn delete_stages_debit() {
    // Storage applies the debit at commit and clamps a drifted counter there.
    let mut op = sized_delete_op();
    let staged = call(op.start_usage_update());
    let IoCall::AddUsage { deltas, .. } = &staged else {
        panic!("expected the counter deltas, got {staged:?}")
    };
    let debit = UsageDelta {
        objects: -1,
        logical_bytes: -34_788_022,
        ..Default::default()
    };
    assert_eq!(deltas.len(), 2);
    assert!(deltas.iter().all(|(_, delta)| *delta == debit));

    let marked = call(op.usage_updated(IoResult::Done));
    let IoCall::Write(writes) = &marked else {
        panic!("expected the dirty marker write, got {marked:?}")
    };
    assert!(
        writes
            .iter()
            .all(|(key_space, ..)| key_space == NODE_STATS_KEYSPACE)
    );

    let turn = op.usage_updated(IoResult::Done);
    assert_eq!(op.state, DeleteObjectState::WriteDeleteAudit);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
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
    op.incarnation = Some(INCARNATION);
    op.target_location = location;
    op
}

#[test]
fn queues_deleted_copy() {
    // The candidate rides the delete transaction, keyed backend first.
    let key = BlobLocationKey::new(
        [4u8; 32],
        EncodingClass::Raw,
        aruna_core::structs::storage::blob::BackendRef::node_default(),
    );
    let mut op = candidate_op(Some(key.clone()));

    let written = call(op.write_reclaim_candidate());

    let IoCall::Write(rows) = &written else {
        panic!("expected candidate write, got {written:?}")
    };
    let [(key_space, key_written, _)] = rows.as_slice() else {
        panic!("expected one candidate row, got {rows:?}")
    };
    assert_eq!(key_space, BLOB_RECLAIM_KEYSPACE);
    assert_eq!(
        key_written.as_ref(),
        ReclaimCandidateKey::new(key.backend, key.encoding, key.blake3_hash)
            .to_bytes()
            .as_slice()
    );
}

#[test]
fn marker_queues_nothing() {
    // No reclaim candidate without a location; the delete trail still rides
    // the same transaction.
    let mut op = candidate_op(None);

    let turn = op.write_reclaim_candidate();

    assert_eq!(space(&turn), DELETE_AUDIT_KEYSPACE);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
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
    assert_eq!(space(&op.first()), S3_BUCKET_KEYSPACE);
    // An ordinary bucket goes on to the version.
    assert_eq!(space(&op.next(read(None))), BLOB_VERSIONS_KEYSPACE);
    let turn = op.next(read(Some(deleted_version_value(user_id))));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
    assert_eq!(space(&turn), BLOB_VERSIONS_KEYSPACE);
    let rows = all_version_ids
        .into_iter()
        .map(|version_id| deleted_version_entry("bucket", "key", version_id, user_id))
        .collect();
    let turn = op.next(IoResult::Page { rows, next: None });
    assert_eq!(space(&turn), BLOB_HEAD_KEYSPACE);
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

    let turn = op.next(read(Some(current_pointer.to_bytes().unwrap().into())));

    assert_eq!(op.state, DeleteObjectState::DeleteTargetVersion);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
    assert_eq!(space(&turn), BLOB_VERSIONS_KEYSPACE);
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

    let written = call(op.next(read(Some(current_pointer.to_bytes().unwrap().into()))));

    let IoCall::Write(rows) = &written else {
        panic!("expected blob head write, got {written:?}");
    };
    let [(key_space, _, value)] = rows.as_slice() else {
        panic!("expected only the head row, got {rows:?}");
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

    let turn = op.next(read(Some(current_pointer.to_bytes().unwrap().into())));

    assert_eq!(op.state, DeleteObjectState::WriteHead);
    assert!(matches!(&turn, Ok(Next::Call(IoCall::Delete(rows))) if rows.len() == 1));
    assert_eq!(space(&turn), BLOB_HEAD_KEYSPACE);
}

#[test]
fn purge_skips_history() {
    // A purge reads the head directly instead of every version of the key.
    let user_id = test_user_id();
    let version_id = Ulid::from_bytes([1u8; 16]);
    let binding = DistributionBinding {
        set_id: Ulid::from_bytes([3u8; 16]),
        member_id: Ulid::from_bytes([4u8; 16]),
        incarnation: 5,
        revision: Default::default(),
    };
    let mut op = DeleteObjectOperation::new(DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "key".to_string(),
        version_id: Some(version_id),
        group_id: Ulid::generate(),
        realm_id: RealmId::from_bytes([1u8; 32]),
        node_id: test_node_id(),
        deleted_by: user_id,
    })
    .purging(binding.clone());
    op.first().unwrap();
    let info = BucketInfo {
        group_id: Ulid::generate(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution: Some(binding),
    };
    op.next(read(Some(info.to_bytes().unwrap().into())))
        .unwrap();
    let turn = op.next(read(Some(deleted_version_value(user_id))));
    assert_eq!(space(&turn), BLOB_HEAD_KEYSPACE);
    let pointer = CurrentVersionPointer::new_with_generation(version_id, 20);
    let turn = op.next(read(Some(pointer.to_bytes().unwrap().into())));
    assert!(
        matches!(turn, Ok(Next::Call(IoCall::Delete(_)))),
        "the purged head is removed, not moved to an older version"
    );
    assert_eq!(space(&turn), BLOB_HEAD_KEYSPACE);
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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
    let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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
            realm_id: user_id.realm_id,
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
    let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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
            realm_id: user_id.realm_id,
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
            realm_id: user_id.realm_id,
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

/// Seeds versions of `sealed` that share one pending archive, the last one current.
async fn seed_pending(storage: &storage::StorageHandle, versions: &[Ulid]) -> BackendLocation {
    use crate::blob::records::owner_write_effect;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::encryption::BucketKeyRef;
    use aruna_core::structs::storage::format::{PithosLayout, StoredFormat};

    let layout = PithosLayout {
        stored_size: 80,
        metadata_digest: [5u8; 32],
        storage_generation: 0,
    };
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "objects".to_string(),
        backend_path: "sealed".to_string(),
        ulid: Ulid::generate(),
        format: StoredFormat::pithos(layout, BucketKeyRef::new(Ulid::generate(), 1)),
        created_by: test_user_id(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 50,
        hashes: HashMap::new(),
    };
    let archive = ArchiveKey::of(&location);
    let version = BlobVersion::pending(
        archive.clone(),
        location.created_at,
        location.created_by,
        None,
    );
    let current = *versions.last().unwrap();
    let mut effects = vec![
        StorageEffect::Write {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new("bucket", "sealed")
                .to_bytes()
                .unwrap()
                .into(),
            value: CurrentVersionPointer::new(current)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        },
        StorageEffect::Write {
            key_space: PENDING_LOCATION_KEYSPACE.to_string(),
            key: archive.to_bytes().into(),
            value: location.to_bytes().unwrap().into(),
            txn_id: None,
        },
    ];
    for version_id in versions {
        let version_key = VersionKey::new("bucket", "sealed", *version_id);
        effects.push(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key.to_bytes().unwrap().into(),
            value: version.to_bytes().unwrap().into(),
            txn_id: None,
        });
        let owner = CopyOwner::new(archive.clone(), version_key);
        let Effect::Storage(effect) = owner_write_effect(&owner).unwrap() else {
            panic!("owner rows are storage writes");
        };
        effects.push(effect);
    }
    for effect in effects {
        storage.send_storage_effect(effect).await;
    }
    location
}

fn delete_pending(group_id: Ulid, version_id: Ulid) -> DeleteObjectOperation {
    DeleteObjectOperation::new(DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "sealed".to_string(),
        version_id: Some(version_id),
        group_id,
        realm_id: RealmId::from_bytes([1u8; 32]),
        node_id: test_node_id(),
        deleted_by: test_user_id(),
    })
}

/// Commits one counter update in its own transaction, as a writer's commit would.
#[derive(Debug, PartialEq)]
struct ApplyUsage(UsageCounterUpdate);

impl aruna_core::operation::Operation for ApplyUsage {
    type Output = ();
    type Error = crate::s3::usage::UsageUpdateError;

    fn first(&mut self) -> Result<aruna_core::calls::Next<()>, Self::Error> {
        let call = self.0.start_calls().into_call();
        Ok(aruna_core::calls::Next::Call(call))
    }

    fn next(
        &mut self,
        result: aruna_core::calls::IoResult,
    ) -> Result<aruna_core::calls::Next<()>, Self::Error> {
        Ok(match self.0.step_calls(result)? {
            Some(call) => aruna_core::calls::Next::Call(call.into_call()),
            None => aruna_core::calls::Next::Done(()),
        })
    }

    fn transaction(&self) -> aruna_core::operation::TransactionMode {
        aruna_core::operation::TransactionMode::Write
    }
}

/// Commits one counter update, as the publication of the seeded versions would.
async fn apply_usage(storage: &storage::StorageHandle, update: UsageCounterUpdate) {
    let context = crate::testing::test_context(storage.clone());
    drive(ApplyUsage(update), &context).await.unwrap();
}

async fn read_counters(
    context: &DriverContext,
    key: Vec<u8>,
) -> aruna_core::structs::storage::usage::UsageCounters {
    let value = read_value(context, aruna_core::keyspaces::USAGE_STATS_KEYSPACE, key).await;
    value.map_or_else(Default::default, |value| {
        aruna_core::structs::storage::usage::UsageCounters::from_bytes(value.as_ref()).unwrap()
    })
}

#[tokio::test]
async fn pending_archive_shared() {
    use crate::s3::usage::StoredDelta;
    use aruna_core::keyspaces::{BLOB_CLEANUP_KEYSPACE, COPY_OWNER_KEYSPACE};
    use aruna_core::structs::storage::blob::BlobCleanupWork;
    use aruna_core::structs::storage::usage::{
        global_shard_key, shard_for_archive, usage_group_key,
    };

    let temp_handle = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    // Two versions of one key share the archive, as a same-bucket copy does.
    let (first, second) = (Ulid::generate(), Ulid::generate());
    let location = seed_pending(&storage_handle, &[first, second]).await;
    let archive = ArchiveKey::of(&location);
    let group_id = Ulid::generate();
    let published = UsageDelta {
        objects: 1,
        logical_bytes: 100,
        ..Default::default()
    };
    let stored = StoredDelta::of_copy(&location, 1, 80).unwrap();
    apply_usage(
        &storage_handle,
        UsageCounterUpdate::with_stored(group_id, published, stored),
    )
    .await;
    let cleanup_rows = || {
        let storage_handle = storage_handle.clone();
        async move {
            let Event::Storage(StorageEvent::IterResult { values, .. }) = storage_handle
                .send_storage_effect(StorageEffect::Iter {
                    key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
                    prefix: None,
                    start: None,
                    limit: 16,
                    txn_id: None,
                })
                .await
            else {
                panic!("unexpected storage event");
            };
            values
                .iter()
                .map(|(_, value)| BlobCleanupWork::from_bytes(value).unwrap())
                .collect::<Vec<_>>()
        }
    };
    let shard = global_shard_key(shard_for_archive(location.ulid));

    drive(delete_pending(group_id, first), &context)
        .await
        .unwrap();
    let owner = CopyOwner::new(archive.clone(), VersionKey::new("bucket", "sealed", first));
    let owner_row = read_value(&context, COPY_OWNER_KEYSPACE, owner.key().unwrap()).await;
    assert!(owner_row.is_none());
    let pending = read_value(&context, PENDING_LOCATION_KEYSPACE, archive.to_bytes()).await;
    assert!(pending.is_some(), "the other owner still uses the archive");
    assert!(cleanup_rows().await.is_empty());
    let group = read_counters(&context, usage_group_key(group_id)).await;
    assert_eq!((group.objects, group.logical_bytes), (1, 50));
    let physical = read_counters(&context, shard.clone()).await;
    assert_eq!((physical.stored_blobs, physical.stored_bytes), (1, 80));

    drive(delete_pending(group_id, second), &context)
        .await
        .unwrap();
    // The last owner leaves the archive to the backend's Retain or reclaim grace: no
    // delete is queued and the physical charge stays until the archive is freed.
    let pending = read_value(&context, PENDING_LOCATION_KEYSPACE, archive.to_bytes()).await;
    assert!(pending.is_some());
    assert!(cleanup_rows().await.is_empty());
    let group = read_counters(&context, usage_group_key(group_id)).await;
    assert_eq!((group.objects, group.logical_bytes), (0, 0));
    let physical = read_counters(&context, shard).await;
    assert_eq!((physical.stored_blobs, physical.stored_bytes), (1, 80));
}

#[tokio::test]
async fn malformed_owner_aborts() {
    use aruna_core::keyspaces::COPY_OWNER_KEYSPACE;

    let temp_handle = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let version_id = Ulid::generate();
    let location = seed_pending(&storage_handle, &[version_id]).await;
    let archive = ArchiveKey::of(&location);
    let malformed = [CopyOwner::prefix(&archive), vec![0xFF; 3]].concat();
    storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: COPY_OWNER_KEYSPACE.to_string(),
            key: malformed.into(),
            value: Vec::new().into(),
            txn_id: None,
        })
        .await;

    let result = drive(delete_pending(Ulid::generate(), version_id), &context).await;

    assert!(matches!(result, Err(DeleteObjectError::ConversionError(_))));
    let pending = read_value(&context, PENDING_LOCATION_KEYSPACE, archive.to_bytes()).await;
    assert!(
        pending.is_some(),
        "an unreadable owner never frees the archive"
    );
    let version = VersionKey::new("bucket", "sealed", version_id);
    let kept = read_value(
        &context,
        BLOB_VERSIONS_KEYSPACE,
        version.to_bytes().unwrap(),
    )
    .await;
    assert!(kept.is_some(), "the delete transaction is aborted");
}

#[tokio::test]
async fn removes_envelope_rows() {
    use crate::s3::usage::StoredDelta;
    use aruna_core::keyspaces::{
        ABE_ARCHIVE_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE,
    };
    use aruna_core::structs::storage::usage::usage_group_key;

    let temp_handle = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let version_id = Ulid::generate();
    let location = seed_pending(&storage_handle, &[version_id]).await;
    let version = VersionKey::new("bucket", "sealed", version_id)
        .to_bytes()
        .unwrap();
    let id = Ulid::generate().to_bytes().to_vec();
    let rows = [
        (ABE_VERSION_KEYSPACE, version.clone(), id.clone()),
        (ABE_ENVELOPE_KEYSPACE, id.clone(), vec![1; 40]),
        (ABE_ARCHIVE_KEYSPACE, id.clone(), vec![2; 9]),
    ];
    for (key_space, key, value) in &rows {
        storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.clone().into(),
                value: value.clone().into(),
                txn_id: None,
            })
            .await;
    }
    let group_id = Ulid::generate();
    let published = UsageDelta {
        objects: 1,
        logical_bytes: 50 + 49,
        ..Default::default()
    };
    let stored = StoredDelta::of_copy(&location, 1, 80).unwrap();
    apply_usage(
        &storage_handle,
        UsageCounterUpdate::with_stored(group_id, published, stored),
    )
    .await;

    drive(delete_pending(group_id, version_id), &context)
        .await
        .unwrap();

    for (key_space, key, _) in rows {
        assert!(read_value(&context, key_space, key).await.is_none());
    }
    let group = read_counters(&context, usage_group_key(group_id)).await;
    assert_eq!((group.objects, group.logical_bytes), (0, 0));
}

#[tokio::test]
async fn removes_pending_copy() {
    // Deleting a copy that waits for its envelope deletes its pending row and its charge, also
    // after a key rotation or with encryption off.
    use crate::s3::usage::StoredDelta;
    use aruna_core::keyspaces::{ABE_COPY_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE};
    use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
    use aruna_core::structs::storage::usage::usage_group_key;

    for mode in [EncryptionMode::VaultLocked, EncryptionMode::Off] {
        let temp_handle = tempdir().unwrap();
        let path = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(path).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let version_id = Ulid::generate();
        let location = seed_pending(&storage_handle, &[version_id]).await;
        let version = VersionKey::new("bucket", "sealed", version_id);
        let bucket_id = location.format.bucket_key().unwrap().bucket_id;
        let version = crate::s3::abe_copies::copy_row(bucket_id, &version).unwrap();
        let settings = BucketEncryption {
            mode,
            bucket_id: Some(bucket_id),
            key_generation: 2,
            ..Default::default()
        };
        for (key_space, key, value) in [
            (ABE_COPY_KEYSPACE, version.clone(), vec![1; 8]),
            (
                BUCKET_ENCRYPTION_KEYSPACE,
                b"bucket".to_vec(),
                settings.to_bytes().unwrap(),
            ),
        ] {
            storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }
        let group_id = Ulid::generate();
        let published = UsageDelta {
            objects: 1,
            logical_bytes: 50 + 8,
            ..Default::default()
        };
        let stored = StoredDelta::of_copy(&location, 1, 80).unwrap();
        apply_usage(
            &storage_handle,
            UsageCounterUpdate::with_stored(group_id, published, stored),
        )
        .await;

        drive(delete_pending(group_id, version_id), &context)
            .await
            .unwrap();

        assert!(
            read_value(&context, ABE_COPY_KEYSPACE, version)
                .await
                .is_none()
        );
        let group = read_counters(&context, usage_group_key(group_id)).await;
        assert_eq!((group.objects, group.logical_bytes), (0, 0));
    }
}

/// Offers one file twice with changed content and returns the offer with the
/// stale first observation. `withdraw` then puts a delete marker over both.
async fn offered_history(
    context: &DriverContext,
    withdraw: bool,
) -> (crate::staging::offered_directory::OfferDirectoryInput, Ulid) {
    use crate::staging::offered_directory::{
        OfferDirectoryInput, WithdrawOfferInput, offer_directory, withdraw_offer,
    };

    let root = tempdir().unwrap();
    let file = root.path().join("data.bin");
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let offer = OfferDirectoryInput {
        bucket: "offered".to_string(),
        root: root.path().to_str().unwrap().to_string(),
        group_id: Ulid::generate(),
        realm_id,
        node_id: test_node_id(),
        user_id: aruna_core::UserId::new(Ulid::generate(), realm_id),
    };
    std::fs::write(&file, b"one").unwrap();
    let first = drive(offer_directory(offer.clone()), context)
        .await
        .unwrap();
    std::fs::write(&file, b"one-changed").unwrap();
    drive(offer_directory(offer.clone()), context)
        .await
        .unwrap();
    if withdraw {
        let input = WithdrawOfferInput {
            bucket: offer.bucket.clone(),
            realm_id,
            node_id: offer.node_id,
            user_id: offer.user_id,
        };
        drive(withdraw_offer(input), context).await.unwrap();
    }
    (offer, first.observed[0].version_id)
}

/// Runs one delete and requires the group counters to equal a recount.
async fn delete_then_recount(
    context: &DriverContext,
    offer: &crate::staging::offered_directory::OfferDirectoryInput,
    version_id: Option<Ulid>,
) -> (u64, u64) {
    use aruna_core::structs::storage::usage::usage_group_key;

    let input = DeleteObjectInput {
        bucket: offer.bucket.clone(),
        key: "data.bin".to_string(),
        version_id,
        group_id: offer.group_id,
        realm_id: offer.realm_id,
        node_id: offer.node_id,
        deleted_by: offer.user_id,
    };
    drive(DeleteObjectOperation::new(input), context)
        .await
        .unwrap();
    let counters = read_counters(context, usage_group_key(offer.group_id)).await;
    drive(
        crate::node::usage_rebuild::RebuildStatsOperation::new(),
        context,
    )
    .await
    .unwrap();
    assert_eq!(
        read_counters(context, usage_group_key(offer.group_id)).await,
        counters
    );
    (counters.objects, counters.referenced_bytes)
}

async fn head_id(context: &DriverContext, bucket: &str) -> Ulid {
    let key = BlobHeadKey::new(bucket, "data.bin").to_bytes().unwrap();
    let value = read_value(context, BLOB_HEAD_KEYSPACE, key).await.unwrap();
    CurrentVersionPointer::from_bytes(value.as_ref())
        .unwrap()
        .version_id
}

// A replaced observation was released when its successor became the head, so
// deleting its row must not release it a second time.
#[tokio::test]
async fn stale_observation_uncharged() {
    let fixture = crate::tests::staging::setup_driver_context().await;
    let context = &fixture.driver_context;
    let (offer, stale) = offered_history(context, false).await;

    assert_eq!(
        delete_then_recount(context, &offer, Some(stale)).await,
        (1, 11)
    );
}

// Deleting the delete marker makes the last observation the head again, so it
// is charged again.
#[tokio::test]
async fn promoted_observation_charged() {
    let fixture = crate::tests::staging::setup_driver_context().await;
    let context = &fixture.driver_context;
    let (offer, _) = offered_history(context, true).await;
    let marker = head_id(context, &offer.bucket).await;

    assert_eq!(
        delete_then_recount(context, &offer, Some(marker)).await,
        (1, 11)
    );
}

// A delete marker over a head observation releases it, as a withdrawal does.
#[tokio::test]
async fn marker_releases_observation() {
    let fixture = crate::tests::staging::setup_driver_context().await;
    let context = &fixture.driver_context;
    let (offer, _) = offered_history(context, true).await;
    let marker = head_id(context, &offer.bucket).await;
    delete_then_recount(context, &offer, Some(marker)).await;

    assert_eq!(delete_then_recount(context, &offer, None).await, (0, 0));
}

#[test]
fn changed_version_kept() {
    // A delete bound to the record a check saw refuses one that changed, in its transaction.
    let seen = aruna_core::structs::storage::blob::BlobVersion::materialized(
        [4u8; 32],
        aruna_core::structs::storage::blob::BackendRef::node_default(),
        aruna_core::structs::storage::format::EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        test_user_id(),
        None,
    );
    let mut changed = seen.clone();
    changed
        .metadata
        .insert("note".to_string(), "edited".to_string());
    let mut op = sized_delete_op().with_expected(seen);

    let turn = op.target_version_read(Some(changed.to_bytes().unwrap().into()));

    assert_eq!(turn.err(), Some(DeleteObjectError::VersionChanged));
}

#[test]
fn marker_needs_head() {
    let head = Ulid::from_parts(5, 5);
    let marked = |current: Ulid| {
        let mut operation = audit_op(None).with_head(head);
        operation.state = DeleteObjectState::ReadCurrentLookup;
        operation.pending_version_id = Some(Ulid::generate());
        let pointer = CurrentVersionPointer::new(current).to_bytes().unwrap();
        operation.next(read(Some(pointer.into())))
    };
    // A head written meanwhile is never covered by the marker.
    let moved = marked(Ulid::from_parts(6, 6));
    assert_eq!(moved.err(), Some(DeleteObjectError::VersionChanged));
    assert!(marked(head).is_ok());
}

/// Puts, an overwrite, a delete marker and a version delete in two buckets keep each bucket row
/// exact and the group row their sum, and a rebuild finds the same rows.
#[tokio::test]
async fn bucket_rows_sum() {
    use crate::node::usage_rebuild::RebuildStatsOperation;
    use crate::s3::bucket::create::CreateBucketOperation;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::usage::{BucketCounters, usage_bucket_key, usage_group_key};

    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
    let node_id = net_handle.node_id();
    let context = DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    for bucket in ["one", "two"] {
        let info = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::now(),
            created_by: user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        drive(
            CreateBucketOperation::new(bucket.to_string(), info),
            &context,
        )
        .await
        .unwrap();
    }
    let put = async |bucket: &str, key: &str, body: &'static [u8]| {
        let request = PutObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_length: Some(body.len() as u64),
            body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(body))),
        };
        let config = PutObjectConfig {
            user_id,
            group_id,
            realm_id,
            node_id,
            request,
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        };
        drive(PutObjectOperation::new(config), &context)
            .await
            .unwrap()
    };
    let delete = async |bucket: &str, key: &str, version_id: Option<Ulid>| {
        let input = DeleteObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            group_id,
            realm_id,
            node_id,
            deleted_by: user_id,
        };
        drive(DeleteObjectOperation::new(input), &context)
            .await
            .unwrap();
    };
    put("one", "x", b"hello").await;
    put("one", "x", b"hello!").await;
    delete("one", "x", None).await;
    put("two", "y", b"1234567").await;
    let z = put("two", "z", b"abc").await;
    delete("two", "z", Some(z.version_id)).await;

    let bucket = async |name: &str| {
        let row = read_value(
            &context,
            aruna_core::keyspaces::USAGE_STATS_KEYSPACE,
            usage_bucket_key(name),
        );
        BucketCounters::from_bytes(&row.await.expect("the bucket row")).unwrap()
    };
    let (one, two) = (bucket("one").await, bucket("two").await);
    // The marker hides x, whose two versions stay stored; z is gone.
    let expected = BucketCounters {
        versions: 2,
        delete_markers: 1,
        logical_bytes: 11,
        ..Default::default()
    };
    assert_eq!(one, expected);
    let expected = BucketCounters {
        objects: 1,
        versions: 1,
        logical_bytes: 7,
        ..Default::default()
    };
    assert_eq!(two, expected);
    // The group row is the sum of its bucket rows.
    let group = read_counters(&context, usage_group_key(group_id)).await;
    assert_eq!(group.buckets, 2);
    assert_eq!(group.objects, one.objects + two.objects);
    assert_eq!(group.logical_bytes, one.logical_bytes + two.logical_bytes);

    drive(RebuildStatsOperation::new(), &context).await.unwrap();
    assert_eq!((bucket("one").await, bucket("two").await), (one, two));
    assert_eq!(
        read_counters(&context, usage_group_key(group_id)).await,
        group
    );
}

#[test]
fn marker_carries_hint() {
    use aruna_core::structs::storage::distribution::{DistributionBinding, SetHint};
    use aruna_core::structs::storage::format::Compression;

    let marker = |binding: Option<DistributionBinding>, record: Option<Vec<u8>>| {
        let mut operation = audit_op(None);
        operation.state = DeleteObjectState::CheckPurgeFence;
        assert_eq!(space(&operation.next(read(None))), S3_BUCKET_KEYSPACE);
        let info = BucketInfo {
            group_id: Ulid::from_bytes([2u8; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: test_user_id(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: binding,
        };
        let mut turn = operation.next(read(Some(info.to_bytes().unwrap().into())));
        if let Some(record) = record {
            assert_eq!(operation.state, DeleteObjectState::ReadRecord);
            turn = operation.next(IoResult::Values(vec![Some(record.into()), None]));
        }
        assert!(turn.is_ok(), "{turn:?}");
        (
            operation.version_id.expect("marker id minted"),
            operation.set_hint,
        )
    };
    let (snapshot, bytes) = crate::distribution::test_record::record(true);
    let set_id = snapshot.set_id();
    // "reports/a.csv" lies above "m", in the range member 2 coordinates.
    let binding = crate::distribution::test_record::binding(&snapshot, 2, 5);
    let (hinted, hint) = marker(Some(binding), Some(bytes));
    assert_eq!(hint, Some(SetHint::of_set(set_id)));
    assert_eq!(SetHint::of_version(hinted), SetHint::of_set(set_id));
    assert_eq!(marker(None, None).1, None);
}

#[test]
fn member_marker_refused() {
    let (snapshot, bytes) = crate::distribution::test_record::record(true);
    let mut operation = audit_op(None);
    operation.binding = Some(crate::distribution::test_record::binding(&snapshot, 1, 5));
    operation.state = DeleteObjectState::ReadRecord;
    let turn = operation.next(IoResult::Values(vec![Some(bytes.into()), None]));
    assert!(matches!(
        turn,
        Err(DeleteObjectError::Distribution(
            crate::distribution::guard::GuardError::NotCoordinator { .. }
        ))
    ));
}

#[test]
fn distributed_version_refused() {
    use aruna_core::structs::storage::format::Compression;
    let (snapshot, _) = crate::distribution::test_record::record(true);
    let mut operation = audit_op(Some(Ulid::from_bytes([3u8; 16])));
    assert_eq!(space(&operation.first()), S3_BUCKET_KEYSPACE);
    let info = BucketInfo {
        group_id: Ulid::from_bytes([2u8; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: test_user_id(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: Some(crate::distribution::test_record::binding(&snapshot, 1, 5)),
    };
    let turn = operation.next(read(Some(info.to_bytes().unwrap().into())));
    assert_eq!(turn.err(), Some(DeleteObjectError::DistributedVersion));
}

#[test]
fn purge_bound_only() {
    use aruna_core::structs::storage::format::Compression;
    let (snapshot, _) = crate::distribution::test_record::record(true);
    let bound = crate::distribution::test_record::binding(&snapshot, 1, 5);
    let info = BucketInfo {
        group_id: Ulid::from_bytes([2u8; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: test_user_id(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: Some(bound.clone()),
    };
    let run = |purge: DistributionBinding| {
        let mut operation = audit_op(Some(Ulid::from_bytes([3u8; 16]))).purging(purge);
        operation.first().unwrap();
        let turn = operation.next(read(Some(info.to_bytes().unwrap().into())));
        (operation, turn)
    };
    // The purge of the bound member deletes the version like in an ordinary bucket.
    let (operation, turn) = run(bound.clone());
    assert_eq!(operation.state, DeleteObjectState::ReadTargetVersion);
    assert_eq!(space(&turn), BLOB_VERSIONS_KEYSPACE);
    // A purge recorded for an earlier incarnation of the name deletes nothing.
    let replaced = crate::distribution::test_record::binding(&snapshot, 1, 6);
    let (_, turn) = run(replaced);
    assert_eq!(turn.err(), Some(DeleteObjectError::NoSuchVersion));
}
