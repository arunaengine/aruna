use std::time::Duration;

use super::*;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::multipart::{COMPLETION_LEASE_MS, MultipartChecksumHint};
use aruna_core::task::{TaskEffect, TaskKey};

pub(super) const TEST_NOW_MS: u64 = 1_700_000_000_000;

fn finalize_input() -> CompleteUploadInput {
    let realm_id = RealmId::from_bytes([3u8; 32]);
    CompleteUploadInput {
        bucket: "bucket".to_string(),
        key: "object".to_string(),
        upload_id: Ulid::from_parts(1, 1),
        realm_id,
        node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
        completed_parts: vec![],
        expected_checksums: vec![],
        checksum_algorithm: None,
        checksum_type: MultipartChecksumType::FullObject,
        checksum_type_explicit: false,
        object_size: Some(10),
        created_by: UserId::local(Ulid::from_parts(2, 2), realm_id),
        quota_ceiling: Some(30),
        now_ms: TEST_NOW_MS,
    }
}

/// Answers a purge fence read with no fence held.
fn fence_clear() -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: crate::s3::purge_fence::fence_key("bucket"),
        value: None,
    })
}

#[test]
fn obligation_keeps_restrictions() {
    // The durable repair record is what a lost enqueue replays, so a scoped
    // credential must stay scoped on it.
    let restrictions = vec![PathRestriction {
        pattern: "/realm/g/group/data/node/bucket/scoped/**".to_string(),
        permission: aruna_core::structs::identity::auth::Permission::WRITE,
    }];
    let mut operation = CompleteUploadOperation::new(finalize_input())
        .with_restrictions(Some(restrictions.clone()));
    operation.version_id = Some(Ulid::from_parts(3, 3));

    let effects = operation.write_obligation();

    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected one obligation write, got {effects:?}")
    };
    let record = crate::replication::queue::LiveObligationRecord::from_bytes(value.as_ref())
        .expect("obligation decodes");
    assert_eq!(record.auth_context.path_restrictions, Some(restrictions));
}

fn open_upload_record(input: &CompleteUploadInput) -> MultipartUpload {
    MultipartUpload {
        backend: BackendRef::node_default(),
        storage_class: None,
        upload_id: input.upload_id,
        bucket: input.bucket.clone(),
        key: input.key.clone(),
        group_id: Ulid::from_parts(4, 4),
        created_by: input.created_by,
        created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1600000060),
        status: MultipartUploadStatus::Completing,
        checksum_hint: None,
        metadata: HashMap::new(),
        placement_policies: Vec::new(),
        subject_generation: 0,
        completing_since_ms: None,
    }
}

fn part_record(part_number: u16, blob_size: u64) -> MultipartPart {
    MultipartPart {
        part_number,
        location: BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "multipart".to_string(),
            backend_path: format!("part-{part_number}"),
            ulid: Ulid::from_parts(5, 5),
            compressed: false,
            encrypted: false,
            created_by: UserId::local(Ulid::from_parts(6, 6), RealmId::from_bytes([4u8; 32])),
            created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1600000120),
            staging: false,
            partial: true,
            blob_size,
            hashes: HashMap::new(),
        },
        created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1600000180),
    }
}

#[test]
fn rejects_foreign_part() {
    // A part stored elsewhere means routing was re-run; compose must fail.
    let mut input = finalize_input();
    input.completed_parts = vec![CompleteMultipartPart {
        part_number: 1,
        etag: None,
        expected_checksums: Vec::new(),
    }];
    let mut operation = CompleteUploadOperation::new(input);
    let record = open_upload_record(&operation.input);
    let upload_id = record.upload_id;
    operation.upload_record = Some(record);
    let mut part = part_record(1, 10);
    part.location.backend = BackendRef::Node("elsewhere".to_string());

    let result = operation.extract_requested_parts(part_values(upload_id, vec![part]));

    assert!(matches!(result, Err(CompleteUploadError::BackendMismatch)));
}

fn part_values(
    upload_id: Ulid,
    parts: Vec<MultipartPart>,
) -> Vec<(aruna_core::types::Key, aruna_core::types::Value)> {
    parts
        .into_iter()
        .map(|part| {
            (
                MultipartPartKey::new(upload_id, part.part_number)
                    .to_bytes()
                    .unwrap()
                    .into(),
                part.to_bytes().unwrap().into(),
            )
        })
        .collect()
}

#[test]
fn refuses_disabled_backend() {
    // Compose already ran on the pinned backend, so the finalize fence has
    // to abort the transaction and roll the composed object back.
    let backend_id = Ulid::from_bytes([5u8; 16]);
    let mut op = CompleteUploadOperation::new(finalize_input());
    op.upload_record = Some(open_upload_record(&op.input));
    op.composed_location = Some(composed_location(backend_id));
    op.state = CompleteUploadState::StartFinalizeTransaction;
    let txn_id = TxnId::generate();

    let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    assert_eq!(op.state, CompleteUploadState::CheckPurgeFinalize);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ));

    let effects = op.step(fence_clear());
    assert_eq!(op.state, CompleteUploadState::ReadBucketDefault);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { .. })]
    ));

    let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (b"bucket".to_vec().into(), None),
            (b"subject".to_vec().into(), None),
        ],
    }));
    assert_eq!(op.state, CompleteUploadState::FenceBackend);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ));

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"x".to_vec().into(),
        value: Some(disabled_record(backend_id).into()),
    }));

    assert!(
        matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
                if *aborted == txn_id
        ),
        "expected the finalize transaction to abort, got {effects:?}"
    );
    assert_eq!(
        op.cleanup.take_error(),
        Some(BackendFenceError::Unavailable.into())
    );
}

#[test]
fn fence_rejects_stray() {
    let mut op = CompleteUploadOperation::new(finalize_input());
    op.composed_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
    op.state = CompleteUploadState::FenceBackend;

    op.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));

    assert!(matches!(
        op.cleanup.take_error(),
        Some(CompleteUploadError::BackendFenceError(
            BackendFenceError::Read(_)
        ))
    ));
}

#[test]
fn rollback_queues_cleanup() {
    // A backend that refuses the rollback delete would otherwise leave a
    // composed object no location or cleanup row can find.
    let mut op = CompleteUploadOperation::new(finalize_input());
    op.composed_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
    op.state = CompleteUploadState::FenceBackend;

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"x".to_vec().into(),
        value: Some(disabled_record(Ulid::from_bytes([5u8; 16])).into()),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::Delete { .. })]
    ));

    let effects = op.step(Event::Blob(BlobEvent::Error(
        aruna_core::errors::BlobError::DeleteError("unreachable".to_string()),
    )));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { key_space, .. })]
            if key_space == BLOB_CLEANUP_KEYSPACE
    ));
    assert!(
        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"x".to_vec().into(),
        }))
        .is_empty()
    );
    assert!(op.is_complete());
}

#[test]
fn commit_keeps_composed() {
    // A possibly-landed finalize commit owns the composed object, so it
    // goes to reconciliation instead of being deleted here.
    let mut op = CompleteUploadOperation::new(finalize_input());
    let mut location = composed_location(Ulid::from_bytes([5u8; 16]));
    location.hashes.insert(
        aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
        vec![7u8; 32],
    );
    op.composed_location = Some(location.clone());
    op.state = CompleteUploadState::CommitFinalizeTransaction;

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::CommitFailed,
    }));

    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected reconciliation to be queued, got {effects:?}")
    };
    assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
    assert_eq!(
        BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        BlobCleanupWork::ReconcileWrite {
            location,
            owner: WriteOwner::Blob {
                blake3: [7u8; 32],
                realm_id: op.input.realm_id,
                ttl_ms: RoCrateLimits::default().holder_ttl_ms,
            },
        }
    );
    assert!(op.composed_location.is_none());

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"k".to_vec().into(),
    }));
    assert!(effects.is_empty());
    assert_eq!(op.state, CompleteUploadState::Error);
    assert!(op.is_complete());
    assert!(matches!(
        op.finalize(),
        Err(CompleteUploadError::StorageError(
            StorageError::CommitFailed
        ))
    ));
}

#[test]
fn release_on_failure() {
    let mut op = CompleteUploadOperation::new(finalize_input());
    let mut location = composed_location(Ulid::from_bytes([5u8; 16]));
    location.hashes.insert(
        aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
        vec![7u8; 32],
    );
    let id = location.ulid;
    op.cleanup.set_error(CompleteUploadError::StorageError(
        StorageError::CommitFailed,
    ));
    op.cleanup.set_release(id);
    op.state = CompleteUploadState::QueueCleanupRow;
    assert!(
        op.cleanup
            .queue(BlobCleanupWork::ReconcileWrite {
                location,
                owner: WriteOwner::Blob {
                    blake3: [7u8; 32],
                    realm_id: op.input.realm_id,
                    ttl_ms: op.rocrate_limits.holder_ttl_ms,
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
fn exhausted_releases_hold() {
    // Giving up on the cleanup row still releases the reservation before the
    // pending error is reported.
    let mut op = CompleteUploadOperation::new(finalize_input());
    let mut location = composed_location(Ulid::from_bytes([5u8; 16]));
    location.hashes.insert(
        aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
        vec![7u8; 32],
    );
    let id = location.ulid;
    op.cleanup
        .set_error(CompleteUploadError::StorageError(StorageError::Timeout));
    op.cleanup.set_release(id);
    op.state = CompleteUploadState::QueueCleanupRow;
    assert!(
        op.cleanup
            .queue(BlobCleanupWork::ReconcileWrite {
                location,
                owner: WriteOwner::Blob {
                    blake3: [7u8; 32],
                    realm_id: op.input.realm_id,
                    ttl_ms: op.rocrate_limits.holder_ttl_ms,
                },
            })
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

    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReleaseReservation { id: observed })] if *observed == id
    ));
    assert!(
        op.step(Event::Blob(BlobEvent::ReservationReleased { id }))
            .is_empty()
    );
    assert!(op.is_complete());
    assert!(matches!(
        op.finalize(),
        Err(CompleteUploadError::StorageError(StorageError::Timeout))
    ));
}

#[test]
fn cleanup_keeps_location() {
    let input = finalize_input();
    let mut op = CompleteUploadOperation::new(input);
    let location = composed_location(Ulid::from_bytes([5u8; 16]));
    let release_id = location.ulid;
    let record = open_upload_record(&op.input);
    op.upload_record = Some(record.clone());
    op.state = CompleteUploadState::ComposeBlob;

    let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteCleanup {
        location: location.clone(),
        message: "reservation finalization failed".to_string(),
    })));
    assert_eq!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    );
    assert_eq!(op.delete_location, Some(location.clone()));
    assert_eq!(op.reconcile_location, None);
    assert_eq!(op.cleanup.release_id(), Some(release_id));

    let reset_txn = TxnId::generate();
    op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: reset_txn,
    }));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: Some(record.to_bytes().unwrap().into()),
    }));
    op.step(Event::Storage(StorageEvent::WriteResult {
        key: Vec::new().into(),
    }));
    let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: reset_txn,
    }));
    let [
        Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id,
        }),
    ] = effects.as_slice()
    else {
        panic!("expected durable cleanup row, got {effects:?}");
    };
    assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
    assert_eq!(key.as_ref().len(), 16);
    assert_eq!(*txn_id, None);
    assert!(matches!(
        BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        BlobCleanupWork::DeleteBlob { location: observed } if observed == location
    ));

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: Vec::new().into(),
    }));
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReleaseReservation {
            id: release_id
        })]
    );
    assert!(
        op.step(Event::Blob(BlobEvent::ReservationReleased {
            id: release_id
        }))
        .is_empty()
    );
    assert_eq!(
        op.finalize(),
        Err(CompleteUploadError::CompleteUploadFailed)
    );
}

#[test]
fn cleanup_reset_error() {
    let input = finalize_input();
    let mut op = CompleteUploadOperation::new(input);
    let location = composed_location(Ulid::from_bytes([5u8; 16]));
    let release_id = location.ulid;
    op.upload_record = Some(open_upload_record(&op.input));
    op.state = CompleteUploadState::ComposeBlob;

    let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteCleanup {
        location: location.clone(),
        message: "reservation finalization failed".to_string(),
    })));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    ));

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    }));
    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected durable cleanup row, got {effects:?}");
    };
    assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
    assert_eq!(
        BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        BlobCleanupWork::DeleteBlob { location }
    );

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: Vec::new().into(),
    }));
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReleaseReservation {
            id: release_id
        })]
    );
}

#[test]
fn conflict_deletes_composed() {
    // A refused finalize proves no version names the composed object.
    let mut op = CompleteUploadOperation::new(finalize_input());
    op.composed_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
    op.state = CompleteUploadState::CommitFinalizeTransaction;

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::Delete { .. })]
    ));
}

#[test]
fn abort_deletes_composed() {
    // The reset transaction never commits, so nothing else can reach the
    // composed object once this operation ends.
    let mut op = CompleteUploadOperation::new(finalize_input());
    op.composed_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
    op.state = CompleteUploadState::CommitResetTransaction;

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::Delete { .. })]
    ));
    assert!(op.composed_location.is_none());
}

#[test]
fn unknown_reset_reconciles() {
    let mut op = CompleteUploadOperation::new(finalize_input());
    let location = composed_location(Ulid::from_bytes([5u8; 16]));
    op.composed_location = Some(location.clone());
    op.txn_id = Some(TxnId::from_bytes([3u8; 16]));
    op.cleanup
        .set_error(CompleteUploadError::CompleteUploadFailed);
    op.state = CompleteUploadState::CommitResetTransaction;

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::CommitFailed,
    }));

    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected reconciliation row, got {effects:?}");
    };
    assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
    assert!(matches!(
        BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        BlobCleanupWork::ReconcileReservation { location: observed }
            if observed == location
    ));
    assert_eq!(op.txn_id, None);
    assert_eq!(op.state, CompleteUploadState::QueueCleanupRow);
}

#[test]
fn abort_keeps_blob() {
    let mut op = CompleteUploadOperation::new(finalize_input());
    let location = composed_location(Ulid::from_bytes([5u8; 16]));
    op.composed_location = Some(location.clone());
    op.cleanup
        .set_error(CompleteUploadError::CompleteUploadFailed);
    op.state = CompleteUploadState::AbortFinalizeTransaction;

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));

    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected reconciliation row, got {effects:?}");
    };
    assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
    assert!(matches!(
        BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        BlobCleanupWork::ReconcileReservation { location: observed }
            if observed == location
    ));
    assert_eq!(op.cleanup.release_id(), None);
}

#[test]
fn cleanup_close_stops() {
    let mut op = CompleteUploadOperation::new(finalize_input());
    let location = composed_location(Ulid::from_bytes([5u8; 16]));
    op.cleanup
        .set_error(CompleteUploadError::CompleteUploadFailed);
    op.state = CompleteUploadState::QueueCleanupRow;
    assert!(
        op.cleanup
            .queue(BlobCleanupWork::ReconcileReservation { location })
            .is_some()
    );

    assert!(
        op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::ChannelClosed,
        }))
        .is_empty()
    );
    assert_eq!(op.state, CompleteUploadState::Error);
    assert!(op.abort().is_empty());
}

#[test]
fn abort_queues_cleanup() {
    let mut op = CompleteUploadOperation::new(finalize_input());
    op.delete_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
    op.state = CompleteUploadState::ComposeBlob;

    let effects = op.abort();

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { key_space, .. })]
            if key_space == BLOB_CLEANUP_KEYSPACE
    ));
    assert_eq!(op.state, CompleteUploadState::QueueCleanupRow);
    assert!(op.delete_location.is_none());
    assert!(matches!(
        op.finalize(),
        Err(CompleteUploadError::NotFinished)
    ));
}

fn composed_location(backend_id: Ulid) -> BackendLocation {
    let mut location = part_record(1, 10).location;
    location.backend = BackendRef::Group(backend_id);
    location.partial = false;
    location
}

fn disabled_record(backend_id: Ulid) -> Vec<u8> {
    aruna_core::structs::storage::group_backend::GroupStorage {
        backend_id,
        group_id: Ulid::from_bytes([7u8; 16]),
        name: "tenant".to_string(),
        kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
        public_config: HashMap::new(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        disabled: true,
        cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
    }
    .to_bytes()
    .unwrap()
}

// A fresh completion stamps the lease it will be judged by.
#[test]
fn marks_completion_lease() {
    let input = finalize_input();
    let mut record = open_upload_record(&input);
    record.status = MultipartUploadStatus::Open;
    let mut op = CompleteUploadOperation::new(input);
    op.txn_id = Some(TxnId::generate());
    op.state = CompleteUploadState::ReadUploadMark;

    let effects = op.mark_upload_read(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: Some(record.to_bytes().unwrap().into()),
    }));

    assert_eq!(op.state, CompleteUploadState::WriteUploadCompleting);
    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected the marking write")
    };
    let written = MultipartUpload::from_bytes(value.as_ref()).unwrap();
    assert_eq!(written.status, MultipartUploadStatus::Completing);
    assert_eq!(written.completing_since_ms, Some(TEST_NOW_MS));
}

// A completion whose request died left the record Completing; the next one
// takes it over once the lease lapsed, instead of failing forever.
#[test]
fn takes_stale_lease() {
    let input = finalize_input();
    let mut record = open_upload_record(&input);
    record.completing_since_ms = Some(TEST_NOW_MS - COMPLETION_LEASE_MS);
    let mut op = CompleteUploadOperation::new(input);
    op.txn_id = Some(TxnId::generate());
    op.state = CompleteUploadState::ReadUploadMark;

    let effects = op.mark_upload_read(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: Some(record.to_bytes().unwrap().into()),
    }));

    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected the take-over write")
    };
    let written = MultipartUpload::from_bytes(value.as_ref()).unwrap();
    assert_eq!(written.completing_since_ms, Some(TEST_NOW_MS));
}

// A live lease is a retryable refusal, never NoSuchUpload.
#[test]
fn refuses_live_lease() {
    let input = finalize_input();
    let mut record = open_upload_record(&input);
    record.completing_since_ms = Some(TEST_NOW_MS - 1);
    let mut op = CompleteUploadOperation::new(input);
    let txn_id = TxnId::generate();
    op.txn_id = Some(txn_id);
    op.state = CompleteUploadState::ReadUploadMark;

    let effects = op.mark_upload_read(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: Some(record.to_bytes().unwrap().into()),
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
            if *aborted == txn_id
    ));
    assert_eq!(
        op.cleanup.take_error(),
        Some(CompleteUploadError::CompletionInProgress)
    );
}

// A deadline between the mark and the finalize must reopen the record.
#[test]
fn abort_reopens_record() {
    let input = finalize_input();
    let record = open_upload_record(&input);
    let mut op = CompleteUploadOperation::new(input);
    op.upload_record = Some(record);
    op.state = CompleteUploadState::ComposeBlob;

    assert!(op.abort_after_commit());
    let effects = op.abort();

    assert_eq!(op.state, CompleteUploadState::ResetUploadTransaction);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    ));
}

// Only the attempt that owns the lease may reopen the record.
#[test]
fn reset_skips_foreign() {
    let input = finalize_input();
    let mut record = open_upload_record(&input);
    record.completing_since_ms = Some(TEST_NOW_MS + 1);
    let mut op = CompleteUploadOperation::new(input);
    op.txn_id = Some(TxnId::generate());
    op.state = CompleteUploadState::ReadUploadReset;

    let effects = op.reset_upload_read(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: Some(record.to_bytes().unwrap().into()),
    }));

    assert_eq!(op.state, CompleteUploadState::CleanupFailedCompose);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
}

#[test]
fn contract_failure_aborts() {
    let mut input = finalize_input();
    input.checksum_type = MultipartChecksumType::FullObject;
    input.checksum_type_explicit = true;
    let mut record = open_upload_record(&input);
    record.status = MultipartUploadStatus::Open;
    record.checksum_hint = Some(MultipartChecksumHint {
        algorithm: Some(ChecksumAlgorithm::Sha256),
        checksum_type: MultipartChecksumType::Composite,
    });
    let mut op = CompleteUploadOperation::new(input);
    let txn_id = TxnId::generate();
    op.txn_id = Some(txn_id);
    op.state = CompleteUploadState::ReadUploadMark;

    let effects = op.mark_upload_read(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: Some(record.to_bytes().unwrap().into()),
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
            if *aborted == txn_id
    ));
    assert_eq!(op.txn_id, None);
}

#[test]
fn composite_parts_only() {
    let digest = vec![7; ChecksumAlgorithm::Sha256.digest_len()];
    let mut input = finalize_input();
    input.completed_parts = vec![CompleteMultipartPart {
        part_number: 1,
        etag: None,
        expected_checksums: vec![ExpectedChecksum {
            algorithm: ChecksumAlgorithm::Sha256,
            digest: digest.clone(),
        }],
    }];
    input.object_size = Some(1);
    let mut upload = open_upload_record(&input);
    upload.checksum_hint = Some(MultipartChecksumHint {
        algorithm: Some(ChecksumAlgorithm::Sha256),
        checksum_type: MultipartChecksumType::Composite,
    });
    let mut part = part_record(1, 1);
    part.location
        .hashes
        .insert(ChecksumAlgorithm::Sha256.hash_key().to_string(), digest);
    let values = part_values(input.upload_id, vec![part]);
    let mut op = CompleteUploadOperation::new(input);

    assert_eq!(op.validate_checksum_contract(&upload), Ok(()));
    assert_eq!(op.input.checksum_type, MultipartChecksumType::Composite);
    op.upload_record = Some(upload);
    assert!(op.extract_requested_parts(values).is_ok());
}

#[test]
fn requires_part_checksum() {
    let mut input = finalize_input();
    input.completed_parts = vec![CompleteMultipartPart {
        part_number: 1,
        etag: None,
        expected_checksums: vec![],
    }];
    input.object_size = Some(1);
    let mut upload = open_upload_record(&input);
    upload.checksum_hint = Some(MultipartChecksumHint {
        algorithm: Some(ChecksumAlgorithm::Sha256),
        checksum_type: MultipartChecksumType::Composite,
    });
    let values = part_values(input.upload_id, vec![part_record(1, 1)]);
    let mut op = CompleteUploadOperation::new(input);
    op.upload_record = Some(upload);

    assert_eq!(
        op.extract_requested_parts(values),
        Err(CompleteUploadError::ChecksumContractMismatch)
    );
}

#[test]
fn undersized_middle_rejected() {
    let mut input = finalize_input();
    input.completed_parts = vec![
        CompleteMultipartPart {
            part_number: 1,
            etag: None,
            expected_checksums: vec![],
        },
        CompleteMultipartPart {
            part_number: 2,
            etag: None,
            expected_checksums: vec![],
        },
    ];
    input.object_size = None;
    let values = part_values(
        input.upload_id,
        vec![part_record(1, 5 * 1024 * 1024 - 1), part_record(2, 1)],
    );
    let op = CompleteUploadOperation::new(input);

    assert_eq!(
        op.extract_requested_parts(values),
        Err(CompleteUploadError::EntityTooSmall)
    );
}

#[test]
fn undersized_final_allowed() {
    let mut input = finalize_input();
    input.completed_parts = vec![
        CompleteMultipartPart {
            part_number: 1,
            etag: None,
            expected_checksums: vec![],
        },
        CompleteMultipartPart {
            part_number: 2,
            etag: None,
            expected_checksums: vec![],
        },
    ];
    input.object_size = None;
    let values = part_values(
        input.upload_id,
        vec![part_record(1, 5 * 1024 * 1024), part_record(2, 1)],
    );
    let op = CompleteUploadOperation::new(input);

    assert!(op.extract_requested_parts(values).is_ok());
}

#[test]
fn omitted_parts_deleted() {
    let input = finalize_input();
    let mut op = CompleteUploadOperation::new(input);
    op.upload_parts = vec![part_record(1, 10), part_record(2, 20)];
    op.resolved_parts = vec![op.upload_parts[0].clone()];

    let effects = op.delete_upload_records();

    let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = effects.as_slice() else {
        panic!("expected batch delete effect");
    };
    assert_eq!(deletes.len(), 3);
    let omitted_key = MultipartPartKey::new(op.input.upload_id, 2)
        .to_bytes()
        .unwrap();
    assert!(deletes.iter().any(|(_, key)| key.as_ref() == omitted_key));
}

#[test]
fn cleanup_covers_omitted() {
    // Deferred delete records must cover requested AND omitted parts.
    let input = finalize_input();
    let mut op = CompleteUploadOperation::new(input);
    let requested = part_record(1, 10);
    let omitted = part_record(2, 20);
    let mut final_location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "objects".to_string(),
        backend_path: "object".to_string(),
        ulid: Ulid::from_parts(7, 7),
        compressed: false,
        encrypted: false,
        created_by: UserId::local(Ulid::from_parts(8, 8), RealmId::from_bytes([4u8; 32])),
        created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1600000240),
        staging: false,
        partial: false,
        blob_size: 10,
        hashes: HashMap::new(),
    };
    final_location.hashes.insert(
        aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
        vec![7u8; 32],
    );
    op.final_location = Some(final_location.clone());
    op.composed_location = Some(final_location.clone());
    let txn_id = Ulid::from_parts(9, 9);
    op.txn_id = Some(txn_id);
    op.version_id = Some(Ulid::from_parts(10, 10));
    op.upload_parts = vec![requested.clone(), omitted.clone()];
    op.resolved_parts = vec![requested.clone()];

    let effects = op.write_cleanup_records();
    let [
        Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: observed,
        }),
    ] = effects.as_slice()
    else {
        panic!("expected cleanup batch write");
    };
    assert_eq!(*observed, Some(txn_id));
    assert!(writes.iter().any(|(key_space, key, value)| {
        key_space == BLOB_CLEANUP_KEYSPACE
            && key.as_ref() == final_location.ulid.to_bytes()
            && matches!(
                BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
                BlobCleanupWork::ReconcileWrite { .. }
            )
    }));
    let works: Vec<BlobCleanupWork> = writes
        .iter()
        .map(|(key_space, _, value)| {
            assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap()
        })
        .collect();
    for record in [&requested, &omitted] {
        assert!(works.iter().any(|work| matches!(
            work,
            BlobCleanupWork::DeleteBlob { location } if location == &record.location
        )));
    }
    assert!(works.iter().any(|work| matches!(
        work,
        BlobCleanupWork::ReconcileWrite {
            location,
            owner: WriteOwner::Blob {
                blake3,
                realm_id,
                ttl_ms,
            },
        } if *blake3 == [7u8; 32]
            && location.get_blake3() == Some(&[7u8; 32][..])
            && *realm_id == op.input.realm_id
            && *ttl_ms == op.rocrate_limits.holder_ttl_ms
    )));
}

#[test]
fn finish_after_commit() {
    // The response must be ready at finalize commit; housekeeping is deferred.
    let input = finalize_input();
    let mut op = CompleteUploadOperation::new(input);
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "objects".to_string(),
        backend_path: "object".to_string(),
        ulid: Ulid::from_parts(11, 11),
        compressed: false,
        encrypted: false,
        created_by: UserId::local(Ulid::from_parts(12, 12), RealmId::from_bytes([4u8; 32])),
        created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1600000300),
        staging: false,
        partial: false,
        blob_size: 10,
        hashes: HashMap::new(),
    };
    op.final_location = Some(location.clone());
    op.composed_location = Some(location.clone());
    op.version_id = Some(Ulid::from_parts(13, 13));
    op.state = CompleteUploadState::CommitFinalizeTransaction;
    op.txn_id = Some(TxnId::generate());

    let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: TxnId::generate(),
    }));

    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReleaseReservation {
            id: location.ulid
        })]
    );
    assert_eq!(op.state, CompleteUploadState::ReleaseReservation);
    let effects = op.step(Event::Blob(BlobEvent::ReservationReleased {
        id: location.ulid,
    }));
    assert!(op.is_complete());
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::PublishUsageSnapshots,
                ..
            }),
            Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::DrainCleanupQueue,
                ..
            }),
        ]
    ));
    let result = op.finalize().unwrap();
    assert_eq!(result.location, location);
}

// A quota rejection leaves the finalize transaction open; it must be
// aborted before reset or the actor pins an LSM snapshot.
#[test]
fn quota_aborts_finalize() {
    let input = finalize_input();
    let record = open_upload_record(&input);
    let mut op = CompleteUploadOperation::new(input);
    let finalize_txn = TxnId::generate();
    op.txn_id = Some(finalize_txn);
    op.upload_record = Some(record);
    op.state = CompleteUploadState::EnforceQuota;

    let effects = op.schedule_error(CompleteUploadError::QuotaExceeded {
        limit: 30,
        usage: 35,
    });

    assert_eq!(effects.len(), 1);
    assert!(matches!(
        effects[0],
        Effect::Storage(StorageEffect::AbortTransaction { txn_id }) if txn_id == finalize_txn
    ));
    assert_eq!(op.state, CompleteUploadState::AbortFinalizeTransaction);
    // Cleared so the reset StartTransaction can never overwrite (orphan) it.
    assert_eq!(op.txn_id, None);

    let effects = op.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: finalize_txn,
    }));

    assert_eq!(effects.len(), 1);
    assert!(matches!(
        effects[0],
        Effect::Storage(StorageEffect::StartTransaction { read: false })
    ));
    assert_eq!(op.state, CompleteUploadState::ResetUploadTransaction);
    assert_eq!(
        op.cleanup.take_error(),
        Some(CompleteUploadError::QuotaExceeded {
            limit: 30,
            usage: 35
        })
    );
}

// If the finalize-txn abort itself errors, the original quota error must still
// be surfaced rather than masked by the abort failure.
#[test]
fn abort_preserves_error() {
    let input = finalize_input();
    let mut op = CompleteUploadOperation::new(input);
    let finalize_txn = TxnId::generate();
    op.txn_id = Some(finalize_txn);
    op.state = CompleteUploadState::EnforceQuota;

    let effects = op.schedule_error(CompleteUploadError::QuotaExceeded {
        limit: 30,
        usage: 35,
    });
    assert_eq!(effects.len(), 1);
    assert!(matches!(
        effects[0],
        Effect::Storage(StorageEffect::AbortTransaction { txn_id }) if txn_id == finalize_txn
    ));

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    }));

    assert!(effects.is_empty());
    assert!(op.is_complete());
    assert_eq!(
        op.finalize(),
        Err(CompleteUploadError::QuotaExceeded {
            limit: 30,
            usage: 35
        })
    );
}

#[test]
fn unknown_mark_resets() {
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input);
    operation.upload_record = Some(open_upload_record(&operation.input));
    operation.txn_id = Some(TxnId::from_bytes([3u8; 16]));
    operation.state = CompleteUploadState::CommitMarkTransaction;

    let effects = operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::CommitFailed,
    }));

    assert_eq!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    );
    assert_eq!(operation.state, CompleteUploadState::ResetUploadTransaction);
    assert_eq!(operation.txn_id, None);
    assert_eq!(
        operation.cleanup.take_error(),
        Some(CompleteUploadError::StorageError(
            StorageError::CommitFailed
        ))
    );
}

#[test]
fn conflict_mark_aborts() {
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input);
    operation.txn_id = Some(TxnId::from_bytes([3u8; 16]));
    operation.state = CompleteUploadState::CommitMarkTransaction;
    let txn_id = operation.txn_id.unwrap();

    let effects = operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));

    assert_eq!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    );
    assert_eq!(operation.state, CompleteUploadState::Error);
    assert_eq!(operation.txn_id, None);
}

#[test]
fn committed_mark_continues() {
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input);
    operation.txn_id = Some(TxnId::from_bytes([3u8; 16]));
    operation.state = CompleteUploadState::CommitMarkTransaction;

    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: TxnId::from_bytes([3u8; 16]),
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { key_space, .. })]
            if key_space == UPLOAD_PART_KEYSPACE
    ));
    assert_eq!(operation.state, CompleteUploadState::ReadUploadParts);
    assert_eq!(operation.txn_id, None);
}
