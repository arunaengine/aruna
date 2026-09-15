use crate::driver::{DriverContext, drive};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectState,
};

use crate::node::usage_stats::{QuotaGate, UsageCounterUpdate};
use aruna_blob::blob::BlobHandler;
use aruna_blob::blob::{BackendRegistry, NodeBackend};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::egress::EgressPolicy;
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DHT_KEYSPACE,
    HASH_PATHS_INDEX_KEYSPACE, S3_BUCKET_KEYSPACE, S3_PURGE_FENCE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
    BucketInfo, CurrentVersionPointer, HashIndex, VersionKey,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::structs::storage::routing::{
    BackendCatalog, NodeRoutingRule, RoutingSnapshot, RoutingTarget,
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

fn test_location(created_by: aruna_core::UserId) -> BackendLocation {
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "path".to_string(),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by,
        created_at: std::time::SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 1,
        hashes: HashMap::new(),
    }
}

fn put_config(realm_id: RealmId, group_id: Ulid, node_id: aruna_core::NodeId) -> PutObjectConfig {
    PutObjectConfig {
        user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
        group_id,
        realm_id,
        node_id,
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
        preassigned_version_id: None,
        quota_ceiling: Some(1),
        routing: RoutingSnapshot::single(group_id),
    }
}

fn fence_clear() -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: crate::s3::purge_fence::fence_key("mybucket"),
        value: None,
    })
}

#[test]
fn guard_allows_edit() {
    // A routing or CORS edit is prospective policy, not a different bucket:
    // it must not discard a write that already landed.
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let node_id = iroh::SecretKey::generate().public();
    let config = put_config(realm_id, group_id, node_id);
    let expected = BucketInfo {
        group_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: config.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    let edited = BucketInfo {
        cors_configuration: Some(aruna_core::structs::storage::blob::BucketCorsConfiguration {
            rules: Vec::new(),
        }),
        ..expected.clone()
    };
    let mut op = PutObjectOperation::new(config).with_bucket_guard(expected);
    op.state = PutObjectState::StartTransaction;
    op.written_location = Some(test_location(op.config.user_id));
    op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::generate(),
    }));
    op.step(fence_clear());

    let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                b"mybucket".to_vec().into(),
                Some(edited.to_bytes().unwrap().into()),
            ),
            (b"subject".to_vec().into(), None),
        ],
    }));

    // Past the guard the minimal fixture fails at the hash step; a recreate
    // would have ended here with a transaction conflict instead.
    let outcome = op.finalize();
    assert!(
        matches!(outcome, Err(PutObjectError::MissingHash(_))),
        "expected the write to pass the guard, got {outcome:?} after {effects:?}"
    );
}

#[test]
fn recreate_rejected() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let node_id = iroh::SecretKey::generate().public();
    let config = put_config(realm_id, group_id, node_id);
    let expected = BucketInfo {
        group_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: config.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    let recreated = BucketInfo {
        created_at: std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1),
        ..expected.clone()
    };
    let mut op = PutObjectOperation::new(config).with_bucket_guard(expected);
    op.state = PutObjectState::StartTransaction;
    op.written_location = Some(test_location(op.config.user_id));

    let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::generate(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == S3_PURGE_FENCE_KEYSPACE
    ));
    let effects = op.step(fence_clear());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads, .. })]
            if reads.first().map(|(space, _)| space.as_str()) == Some(S3_BUCKET_KEYSPACE)
    ));
    let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                b"mybucket".to_vec().into(),
                Some(recreated.to_bytes().unwrap().into()),
            ),
            (b"subject".to_vec().into(), None),
        ],
    }));

    // The terminal state is complete, so this is the only chance to release
    // the transaction the guard read joined.
    assert!(
        matches!(
            effects.as_slice(),
            [
                Effect::Blob(BlobEffect::Delete { .. }),
                Effect::Storage(StorageEffect::AbortTransaction { .. }),
            ]
        ),
        "expected a rollback, got {effects:?}"
    );
    assert!(op.is_complete());
    assert!(op.step(Event::Blob(BlobEvent::DeleteFinished)).is_empty());
    assert_eq!(
        op.finalize(),
        Err(PutObjectError::StorageError(
            StorageError::TransactionConflict
        ))
    );
}

#[test]
fn error_closes_transaction() {
    // Error is a complete state, so nothing else can release the transaction.
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut op = PutObjectOperation::new(put_config(
        realm_id,
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    ));
    let txn_id = Ulid::generate();
    op.state = PutObjectState::CheckHashLookup;
    op.txn_id = Some(txn_id);

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"unexpected".to_vec().into(),
    }));

    assert_eq!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    );
    assert!(op.txn_id.is_none());
    // Replaying the terminal state must not abort the same transaction twice.
    assert!(
        op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }))
            .is_empty()
    );
}

#[test]
fn rejects_write_error() {
    // A rejected body stream (e.g. trailer checksum mismatch) must
    // surface WriteFailed instead of InvalidOperationState.
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut op = PutObjectOperation::new(put_config(
        realm_id,
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    ));
    op.state = PutObjectState::WriteBlob;

    let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::StreamFailed(
        "checksum mismatch".to_string(),
    ))));

    assert!(effects.is_empty());
    assert!(op.is_complete());
    assert!(matches!(op.finalize(), Err(PutObjectError::WriteFailed(_))));
}

#[test]
fn rejects_server_write() {
    // A full or flapping disk must never be reported as a client bad digest.
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut op = PutObjectOperation::new(put_config(
        realm_id,
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    ));
    op.state = PutObjectState::WriteBlob;

    let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteError(
        "No space left on device".to_string(),
    ))));

    assert!(effects.is_empty());
    assert!(op.is_complete());
    assert!(matches!(
        op.finalize(),
        Err(PutObjectError::BlobWriteFailed(_))
    ));
}

#[test]
fn quota_error_cleans() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let node_id = iroh::SecretKey::generate().public();
    let mut op = PutObjectOperation::new(put_config(realm_id, group_id, node_id));
    let txn_id = Ulid::generate();
    let location = test_location(op.config.user_id);

    op.state = PutObjectState::EnforceQuota;
    op.txn_id = Some(txn_id);
    op.written_location = Some(location.clone());
    op.quota_gate = Some(QuotaGate::new(1, 1, group_id, node_id));

    let effects = op.handle_enforce_quota(Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    }));

    assert_eq!(effects.len(), 1);
    assert!(matches!(
        effects[0],
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: observed }) if observed == txn_id
    ));
    assert_eq!(op.txn_id, None);

    let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));

    let [Effect::Blob(BlobEffect::Delete { location: deleted })] = effects.as_slice() else {
        panic!("expected blob cleanup")
    };
    assert_eq!(deleted, &location);

    let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));

    assert!(effects.is_empty());
    assert!(op.is_complete());
    assert!(matches!(
        op.finalize(),
        Err(crate::s3::object::put::PutObjectError::QuotaGateError(_))
    ));
}

#[test]
fn usage_error_cleans() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let node_id = iroh::SecretKey::generate().public();
    let mut op = PutObjectOperation::new(put_config(realm_id, group_id, node_id));
    let txn_id = Ulid::generate();
    let location = test_location(op.config.user_id);

    op.state = PutObjectState::UpdateUsage;
    op.txn_id = Some(txn_id);
    op.written_location = Some(location.clone());
    op.usage_update = Some(UsageCounterUpdate::for_group(
        group_id,
        UsageDelta::default(),
    ));

    let effects = op.handle_usage_update(Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    }));

    assert_eq!(effects.len(), 1);
    assert!(matches!(
        effects[0],
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: observed }) if observed == txn_id
    ));
    assert_eq!(op.txn_id, None);

    let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));

    let [Effect::Blob(BlobEffect::Delete { location: deleted })] = effects.as_slice() else {
        panic!("expected blob cleanup")
    };
    assert_eq!(deleted, &location);

    let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));

    assert!(effects.is_empty());
    assert!(op.is_complete());
    assert!(matches!(
        op.finalize(),
        Err(crate::s3::object::put::PutObjectError::UsageUpdateError(_))
    ));
}

#[test]
fn retries_commit_conflict() {
    // Concurrent writes contend on the usage counters; the streamed blob
    // stays put and only the metadata transaction reopens.
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = iroh::SecretKey::generate().public();
    let mut op = PutObjectOperation::new(put_config(realm_id, Ulid::generate(), node_id));
    let location = test_location(op.config.user_id);
    let version_id = Ulid::generate();

    op.state = PutObjectState::CommitTransaction;
    op.txn_id = Some(Ulid::generate());
    op.version_id = Some(version_id);
    op.written_location = Some(location.clone());
    op.cleanup_location = Some(location.clone());
    op.new_blob = true;

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    ));
    assert_eq!(op.state, PutObjectState::StartTransaction);
    assert!(op.txn_id.is_none());
    assert_eq!(op.written_location, Some(location.clone()));
    assert_eq!(op.cleanup_location, None);
    assert!(!op.new_blob);
    // The retry must stay the same write, not mint a second version.
    assert_eq!(op.version_id, Some(version_id));

    let retry_txn = Ulid::generate();
    op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: retry_txn,
    }));
    assert_eq!(op.state, PutObjectState::CheckPurgeFence);
    assert_eq!(op.txn_id, Some(retry_txn));

    op.state = PutObjectState::CommitTransaction;
    let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: retry_txn,
    }));
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReleaseReservation {
            id: location.ulid
        })]
    );
}

#[test]
fn conflict_exhausts_retries() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let node_id = iroh::SecretKey::generate().public();
    let mut op = PutObjectOperation::new(put_config(realm_id, group_id, node_id));
    let txn_id = Ulid::generate();
    let location = test_location(op.config.user_id);

    op.state = PutObjectState::CommitTransaction;
    op.txn_id = Some(txn_id);
    op.written_location = Some(location.clone());

    let conflict = || {
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        })
    };
    for _ in 0..super::CONFLICT_RETRIES {
        assert!(matches!(
            op.step(conflict()).as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        op.state = PutObjectState::CommitTransaction;
    }

    let effects = op.step(conflict());

    let [Effect::Blob(BlobEffect::Delete { location: deleted })] = effects.as_slice() else {
        panic!("expected blob cleanup")
    };
    assert_eq!(deleted, &location);

    let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));

    assert!(effects.is_empty());
    assert!(op.is_complete());
    assert!(matches!(
        op.finalize(),
        Err(crate::s3::object::put::PutObjectError::StorageError(
            StorageError::TransactionConflict
        ))
    ));
}

#[test]
fn writes_before_commit() {
    // The reconciliation row must commit atomically with metadata ownership.
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = iroh::SecretKey::generate().public();
    let mut op = PutObjectOperation::new(put_config(realm_id, Ulid::generate(), node_id));
    let txn_id = Ulid::generate();
    let mut location = test_location(op.config.user_id);
    location.hashes.insert(
        aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
        vec![7u8; 32],
    );
    op.txn_id = Some(txn_id);
    op.written_location = Some(location.clone());

    let effects = op.write_cleanup_row(txn_id);
    let [
        Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            txn_id: observed,
            value,
            ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected a transactional reconciliation row, got {effects:?}")
    };
    assert_eq!(key_space, aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE);
    assert_eq!(key.as_ref(), location.ulid.to_bytes());
    assert_eq!(*observed, Some(txn_id));
    assert!(matches!(
        super::BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        super::BlobCleanupWork::ReconcileWrite {
            location: observed,
            owner: super::WriteOwner::Blob { blake3, .. },
        } if blake3 == [7u8; 32] && observed == location
    ));

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"cleanup".to_vec().into(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { txn_id: observed })]
            if *observed == txn_id
    ));
}

#[test]
fn release_after_commit() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut op = PutObjectOperation::new(put_config(
        realm_id,
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    ));
    let location = test_location(op.config.user_id);
    let id = location.ulid;
    op.state = PutObjectState::CommitTransaction;
    op.written_location = Some(location);

    let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::generate(),
    }));
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReleaseReservation { id })]
    );
    assert_eq!(op.state, PutObjectState::ReleaseReservation);

    let effects = op.step(Event::Blob(BlobEvent::ReservationReleased { id }));
    assert_eq!(op.state, PutObjectState::Finish);
    assert_eq!(effects.len(), 1);
    assert!(
        op.step(Event::Blob(BlobEvent::ReservationReleased { id }))
            .is_empty()
    );
}

#[test]
fn release_failure_succeeds() {
    // A refused release after a durable commit hands the reservation to the
    // cleanup queue; the client must not be told its committed write failed.
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut op = PutObjectOperation::new(put_config(
        realm_id,
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    ));
    let mut location = test_location(op.config.user_id);
    location.hashes.insert(
        aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
        vec![7u8; 32],
    );
    let id = location.ulid;
    op.version_id = Some(Ulid::generate());
    op.state = PutObjectState::CommitTransaction;
    op.written_location = Some(location.clone());
    op.output = Some(Ok(location.clone()));

    op.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::generate(),
    }));
    let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteError(
        "release refused".to_string(),
    ))));

    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected a reconciliation row, got {effects:?}")
    };
    assert_eq!(key_space, aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE);
    assert!(matches!(
        super::BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        super::BlobCleanupWork::ReconcileWrite { location: observed, .. }
            if observed.ulid == id
    ));

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"k".to_vec().into(),
    }));
    assert_eq!(effects.len(), 1);
    assert_eq!(op.state, PutObjectState::Finish);
    assert!(matches!(
        op.finalize(),
        Ok(result) if result.location == location
    ));
}

#[test]
fn release_on_failure() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut op = PutObjectOperation::new(put_config(
        realm_id,
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    ));
    let mut location = test_location(op.config.user_id);
    location.hashes.insert(
        aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
        vec![7u8; 32],
    );
    let id = location.ulid;
    op.cleanup
        .set_error(PutObjectError::StorageError(StorageError::CommitFailed));
    op.cleanup.set_release(id);
    op.state = PutObjectState::QueueCleanupRow;
    assert!(
        op.cleanup
            .queue(super::BlobCleanupWork::ReconcileWrite {
                location,
                owner: super::WriteOwner::Blob {
                    blake3: [7u8; 32],
                    realm_id,
                    ttl_ms: super::RoCrateLimits::default().holder_ttl_ms,
                },
            })
            .is_some()
    );

    // The row is retried until storage accepts it; only then is the
    // reservation released.
    for _ in 0..2 {
        assert!(matches!(
            op.step(Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            }))
            .as_slice(),
            [Effect::Storage(StorageEffect::Write { .. })]
        ));
    }
    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"k".to_vec().into(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReleaseReservation { id: observed })]
            if *observed == id
    ));
    assert!(
        op.step(Event::Blob(BlobEvent::ReservationReleased { id }))
            .is_empty()
    );
    assert!(op.is_complete());
}

#[test]
fn closed_keeps_hold() {
    // A closed storage channel leaves the durable reservation for restart reconciliation.
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut op = PutObjectOperation::new(put_config(
        realm_id,
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    ));
    let location = test_location(op.config.user_id);
    let id = location.ulid;
    op.cleanup
        .set_error(PutObjectError::StorageError(StorageError::ChannelClosed));
    op.cleanup.set_release(id);
    op.state = PutObjectState::QueueCleanupRow;
    assert!(
        op.cleanup
            .queue(super::BlobCleanupWork::ReconcileReservation { location })
            .is_some()
    );

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::ChannelClosed,
    }));
    assert!(effects.is_empty());
    assert_eq!(op.cleanup.release_id(), Some(id));
    assert!(op.cleanup.retry(&StorageError::Timeout).is_none());
    assert!(op.is_complete());
    assert!(matches!(
        op.finalize(),
        Err(PutObjectError::StorageError(StorageError::ChannelClosed))
    ));
}

#[test]
fn exhausted_keeps_hold() {
    // Exhausted cleanup retries must not release a reservation whose row was
    // never accepted; the pending error still fails the request.
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut op = PutObjectOperation::new(put_config(
        realm_id,
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    ));
    let location = test_location(op.config.user_id);
    let id = location.ulid;
    op.cleanup
        .set_error(PutObjectError::StorageError(StorageError::Timeout));
    op.cleanup.set_release(id);
    op.state = PutObjectState::QueueCleanupRow;
    assert!(
        op.cleanup
            .queue(super::BlobCleanupWork::ReconcileReservation { location })
            .is_some()
    );

    for _ in 0..3 {
        assert!(matches!(
            op.step(Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            }))
            .as_slice(),
            [Effect::Storage(StorageEffect::Write { .. })]
        ));
    }
    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    }));

    assert!(effects.is_empty());
    assert_eq!(op.cleanup.release_id(), Some(id));
    assert!(op.cleanup.retry(&StorageError::Timeout).is_none());
    assert!(op.is_complete());
    assert!(matches!(
        op.finalize(),
        Err(PutObjectError::StorageError(StorageError::Timeout))
    ));
}

#[test]
fn unknown_keeps_blob() {
    // Only a proven refusal rolls the blob back; any other commit failure
    // may already own the version, so the copy goes to reconciliation.
    for error in [
        StorageError::CommitFailed,
        StorageError::PersistError("journal".to_string()),
        StorageError::Timeout,
    ] {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let mut op = PutObjectOperation::new(put_config(realm_id, Ulid::generate(), node_id));
        op.state = PutObjectState::CommitTransaction;
        op.txn_id = Some(Ulid::generate());
        let mut location = test_location(op.config.user_id);
        location.hashes.insert(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            vec![7u8; 32],
        );
        let release_id = location.ulid;
        op.written_location = Some(location.clone());

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: error.clone(),
        }));

        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("{error} must queue reconciliation, got {effects:?}")
        };
        assert_eq!(key_space, aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE);
        assert_eq!(
            super::BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            super::BlobCleanupWork::ReconcileWrite {
                location,
                owner: super::WriteOwner::Blob {
                    blake3: [7u8; 32],
                    realm_id,
                    ttl_ms: super::RoCrateLimits::default().holder_ttl_ms,
                },
            }
        );

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"k".to_vec().into(),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation {
                id: release_id
            })]
        );
        let effects = op.step(Event::Blob(BlobEvent::ReservationReleased {
            id: release_id,
        }));
        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert!(matches!(
            op.finalize(),
            Err(PutObjectError::StorageError(observed)) if observed == error
        ));
    }
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
    let group_id = Ulid::generate();
    let node_id = net_handle.node_id();
    let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let preassigned_version_id = Ulid::generate();
    let put_config = PutObjectConfig {
        user_id,
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
        preassigned_version_id: Some(preassigned_version_id),
        quota_ceiling: None,
        routing: RoutingSnapshot::single(group_id),
    };
    let put_operation = PutObjectOperation::new(put_config);

    let context = DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    // Jesus, Take the Wheel!
    let result = drive(put_operation, &context).await.unwrap();

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
            key: BlobLocationKey::from_blake3(
                result.location.get_blake3().unwrap(),
                result.location.backend.clone(),
            )
            .unwrap()
            .to_bytes()
            .into(),
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
            key: HashIndex::new(
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
    let entries = decode_entries(dht_value.as_ref()).expect("decode DHT entries");
    assert!(entries.iter().any(|entry| {
        entry.realm_id == realm_id
            && entry.publisher == context.net_handle.as_ref().unwrap().node_id()
            && entry.value.is_empty()
    }));

    let retry_data = b"different content";
    let retry = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id,
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: "some-file.txt".to_string(),
                content_length: Some(retry_data.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                    &retry_data[..],
                ))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: Some(preassigned_version_id),
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(retry, result);
    assert_eq!(
        read_to_string(retry.location.get_full_path().unwrap()).unwrap(),
        String::from_utf8_lossy(&data[..]).to_string()
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
}

#[tokio::test]
pub async fn deduplicates_blob() {
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
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let data = b"hello, world!";
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();

    let first = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
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
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        &context,
    )
    .await
    .unwrap();

    let second = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
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
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        &context,
    )
    .await
    .unwrap();

    assert_eq!(first.location, second.location);
    assert_eq!(count_files(Path::new(&blob_root)), 1);
    let blob_hash: [u8; 32] = first.location.get_blake3().unwrap().try_into().unwrap();

    let location_key = BlobLocationKey::new(blob_hash, first.location.backend.clone()).to_bytes();
    let blob_location_value = read_value(&context, BLOB_LOCATIONS_KEYSPACE, location_key)
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
            HashIndex::new(
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

/// Two filesystem node backends with distinct roots: enough to prove that a
/// routed write never adopts a copy sitting on the other backend.
async fn setup_two_backends(temp_root: &str) -> (DriverContext, String, String) {
    let hot_root = format!("{temp_root}/hot");
    let cold_root = format!("{temp_root}/cold");
    std::fs::create_dir_all(&hot_root).unwrap();
    std::fs::create_dir_all(&cold_root).unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();

    let backend = |root: &str, prefix: &str, class: Option<String>| {
        std::sync::Arc::new(NodeBackend::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some(prefix.to_string()),
                max_bucket_size: Some(100_000),
                multipart_bucket: Some(format!("{prefix}parts")),
                root: root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            class,
        ))
    };
    let mut backends = std::collections::BTreeMap::new();
    backends.insert("default".to_string(), backend(&hot_root, "hot-", None));
    backends.insert(
        "cold".to_string(),
        backend(&cold_root, "cold-", Some("cold".to_string())),
    );
    let registry = BackendRegistry::new(backends, "default".to_string()).unwrap();
    let blob_handle = BlobHandler::with_registry(
        registry,
        storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::loopback(),
    )
    .await
    .unwrap();

    (
        DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        },
        hot_root,
        cold_root,
    )
}

fn archive_routing(group_id: Ulid) -> RoutingSnapshot {
    let catalog = BackendCatalog::new("default")
        .with_backend("default", None)
        .with_backend("cold", Some("cold".to_string()));
    RoutingSnapshot::new(group_id, catalog).with_node_rules(vec![NodeRoutingRule {
        group: None,
        bucket: None,
        key_prefix: Some("archive/".to_string()),
        target: RoutingTarget::Class("cold".to_string()),
    }])
}

async fn put_routed(
    context: &DriverContext,
    group_id: Ulid,
    realm_id: RealmId,
    key: &str,
    data: &'static [u8],
) -> super::PutObjectResult {
    drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
            group_id,
            realm_id,
            node_id: context.net_handle.as_ref().unwrap().node_id(),
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: key.to_string(),
                content_length: Some(data.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(data))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: archive_routing(group_id),
        }),
        context,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn dedup_per_backend() {
    // Identical bytes routed to two backends must keep one copy on each.
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let (context, hot_root, cold_root) = setup_two_backends(temp_root).await;

    let data = b"identical bytes";
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();

    let hot = put_routed(&context, group_id, realm_id, "hot.txt", data).await;
    let cold = put_routed(&context, group_id, realm_id, "archive/cold.txt", data).await;

    assert_eq!(hot.location.backend, BackendRef::node_default());
    assert_eq!(cold.location.backend, BackendRef::Node("cold".to_string()));
    assert_eq!(count_files(Path::new(&hot_root)), 1);
    assert_eq!(count_files(Path::new(&cold_root)), 1);

    let hash: [u8; 32] = hot.location.get_blake3().unwrap().try_into().unwrap();
    assert_eq!(cold.location.get_blake3().unwrap(), hash);

    for (key, result) in [("hot.txt", &hot), ("archive/cold.txt", &cold)] {
        let version_value = read_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", key, result.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob version entry");
        let version = BlobVersion::from_bytes(version_value.as_ref()).unwrap();
        assert_eq!(version.blob_backend(), Some(&result.location.backend));

        let location_value = read_value(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            version.location_key().unwrap().to_bytes(),
        )
        .await
        .expect("missing blob location entry");
        assert_eq!(
            BackendLocation::from_bytes(location_value.as_ref()).unwrap(),
            result.location
        );
        assert!(exists(result.location.get_full_path().unwrap()).unwrap());
    }
}

#[tokio::test]
async fn dedup_repeats_backend() {
    // A rewrite onto the same backend must still adopt the stored copy.
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let (context, _hot_root, cold_root) = setup_two_backends(temp_root).await;

    let data = b"identical bytes";
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();

    let first = put_routed(&context, group_id, realm_id, "archive/one.txt", data).await;
    let second = put_routed(&context, group_id, realm_id, "archive/two.txt", data).await;

    assert_eq!(first.location, second.location);
    assert_eq!(count_files(Path::new(&cold_root)), 1);
}

#[tokio::test]
async fn delete_keeps_copy() {
    // Deleting one object must leave the twin copy on the other backend.
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let (context, _hot_root, cold_root) = setup_two_backends(temp_root).await;

    let data = b"identical bytes";
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();

    let hot = put_routed(&context, group_id, realm_id, "hot.txt", data).await;
    let cold = put_routed(&context, group_id, realm_id, "archive/cold.txt", data).await;

    let deleted = drive(
        crate::s3::object::delete::DeleteObjectOperation::new(
            crate::s3::object::delete::DeleteObjectInput {
                bucket: "mybucket".to_string(),
                key: "hot.txt".to_string(),
                version_id: Some(hot.version_id),
                group_id,
                realm_id,
                node_id: context.net_handle.as_ref().unwrap().node_id(),
                deleted_by: aruna_core::UserId::local(Ulid::generate(), realm_id),
            },
        ),
        &context,
    )
    .await
    .unwrap();
    let _ = deleted;

    let location_value = read_value(
        &context,
        BLOB_LOCATIONS_KEYSPACE,
        BlobLocationKey::new(
            cold.location.get_blake3().unwrap().try_into().unwrap(),
            cold.location.backend.clone(),
        )
        .to_bytes(),
    )
    .await
    .expect("cold copy was removed with the hot object");
    assert_eq!(
        BackendLocation::from_bytes(location_value.as_ref()).unwrap(),
        cold.location
    );
    assert_eq!(count_files(Path::new(&cold_root)), 1);
}

#[test]
fn generation_increments() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut op = PutObjectOperation::new(PutObjectConfig {
        user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
        group_id: Ulid::generate(),
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
        preassigned_version_id: None,
        quota_ceiling: None,
        routing: RoutingSnapshot::single(Ulid::generate()),
    });
    let version_id = Ulid::generate();
    op.version_id = Some(version_id);
    op.output = Some(Ok(BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "path".to_string(),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by: op.config.user_id,
        created_at: std::time::SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 1,
        hashes: HashMap::new(),
    }));
    op.txn_id = Some(Ulid::generate());
    let existing = CurrentVersionPointer::new_with_generation(Ulid::generate(), 4);

    let effects = op.object_lookup_read(Event::Storage(StorageEvent::ReadResult {
        key: vec![0].into(),
        value: Some(existing.to_bytes().unwrap().into()),
    }));
    let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
        panic!("expected liveness version read")
    };
    assert_eq!(key_space, BLOB_VERSIONS_KEYSPACE);

    let effects = op.liveness_read(Event::Storage(StorageEvent::ReadResult {
        key: vec![0].into(),
        value: None,
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
pub async fn overwrite_retains_index() {
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
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();

    let first = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
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
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        &context,
    )
    .await
    .unwrap();

    let second = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
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
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        &context,
    )
    .await
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
        HashIndex::new(
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
        HashIndex::new(
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
async fn mismatch_cleans_blob() {
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
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let data = b"hello, world!";
    let err = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
            group_id: Ulid::generate(),
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
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(Ulid::generate()),
        }),
        &context,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        err,
        crate::s3::object::put::PutObjectError::ChecksumMismatch("SHA256")
    ));
    assert_eq!(count_files(Path::new(&blob_root)), 0);
}
