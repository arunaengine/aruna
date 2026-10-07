//! Tests the multipart complete state machine: fences, rollbacks, cleanup and commits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::format::StoredFormat;
use std::time::Duration;

use super::*;
use aruna_core::structs::checksum::{HASH_CRC32, HASH_CRC32C, HASH_CRC64NVME, HASH_SHA256};
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::multipart::{
    BackendUpload, COMPLETION_LEASE_MS, MultipartChecksumHint,
};
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
        backend_upload: None,
        encryption: None,
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
            format: StoredFormat::default(),
            created_by: UserId::local(Ulid::from_parts(6, 6), RealmId::from_bytes([4u8; 32])),
            created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1600000120),
            staging: false,
            partial: true,
            blob_size,
            hashes: HashMap::new(),
        },
        created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1600000180),
        backend_etag: None,
        piece: None,
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

    op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (b"bucket".to_vec().into(), None),
            (b"subject".to_vec().into(), None),
        ],
    }));
    assert_eq!(op.state, CompleteUploadState::CheckSealSettings);
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: None,
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
fn stale_encoding_aborts() {
    // The setting changed while the parts were composed: the raw object must not
    // become a version a finished migration would never revisit.
    let mut op = CompleteUploadOperation::new(finalize_input());
    op.upload_record = Some(open_upload_record(&op.input));
    op.composed_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
    op.state = CompleteUploadState::StartFinalizeTransaction;
    let txn_id = TxnId::generate();
    op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    op.step(fence_clear());
    let bucket = BucketInfo {
        group_id: Ulid::from_bytes([6u8; 16]),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: op.input.created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Zstd { level: 3 },
    };

    let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                b"bucket".to_vec().into(),
                Some(bucket.to_bytes().unwrap().into()),
            ),
            (b"subject".to_vec().into(), None),
        ],
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
        Some(StorageError::TransactionConflict.into())
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

fn in_place_record(op: &CompleteUploadOperation, target: &BackendLocation) -> MultipartUpload {
    let mut record = open_upload_record(&op.input);
    record.backend_upload = Some(BackendUpload {
        location: target.clone(),
        upload_id: "provider".to_string(),
        record_id: Ulid::from_bytes([9u8; 16]),
    });
    record
}

#[test]
fn uncertain_keeps_object() {
    // A commit that may not have landed leaves the in-place object to the upload or its version.
    let mut op = CompleteUploadOperation::new(finalize_input());
    let mut target = composed_location(Ulid::from_bytes([5u8; 16]));
    target.hashes.insert(
        aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
        vec![7u8; 32],
    );
    op.upload_record = Some(in_place_record(&op, &target));
    op.reset_done = true;
    op.composed_location = Some(target.clone());
    op.state = CompleteUploadState::CommitFinalizeTransaction;

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::CommitFailed,
    }));

    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected reconciliation to be queued, got {effects:?}")
    };
    assert_eq!(
        BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        BlobCleanupWork::ReconcileWrite {
            location: target,
            owner: WriteOwner::CompletedUpload {
                upload_id: op.input.upload_id,
                blake3: [7u8; 32],
                realm_id: op.input.realm_id,
                ttl_ms: RoCrateLimits::default().holder_ttl_ms,
            },
        }
    );
}

#[test]
fn completes_at_provider() {
    // The provider assembles the parts; no compose reads them back.
    let mut op = CompleteUploadOperation::new(finalize_input());
    let target = composed_location(Ulid::from_bytes([5u8; 16]));
    op.upload_record = Some(in_place_record(&op, &target));
    op.resolved_parts = vec![part_record(1, 10)];

    let effects = op.compose_blob();

    let [
        Effect::Blob(BlobEffect::CompleteUpload {
            backend_upload,
            parts,
        }),
    ] = effects.as_slice()
    else {
        panic!("expected the provider completion, got {effects:?}")
    };
    assert_eq!(backend_upload.location, target);
    assert_eq!(parts, &op.resolved_parts);
}

#[test]
fn frames_provider_object() {
    // Compression was turned on after the upload opened: the raw provider object is composed
    // into frames, and only that copy is checked and published.
    let mut op = CompleteUploadOperation::new(finalize_input());
    let target = composed_location(Ulid::from_bytes([5u8; 16]));
    op.upload_record = Some(in_place_record(&op, &target));
    op.compression = Compression::Zstd { level: 3 };
    op.state = CompleteUploadState::ComposeBlob;

    let effects = op.step(Event::Blob(BlobEvent::WriteFinished {
        location: target.clone(),
    }));

    let [
        Effect::Blob(BlobEffect::Compose {
            resolved, parts, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected a framed compose, got {effects:?}")
    };
    assert_eq!(parts, &vec![target.clone()]);
    assert_eq!(resolved.compression, Compression::Zstd { level: 3 });
    assert_eq!(op.composed_location, None);

    let mut framed = composed_location(Ulid::from_bytes([5u8; 16]));
    framed.ulid = Ulid::from_bytes([8u8; 16]);
    framed.backend_path = "bucket/framed".to_string();
    let effects = op.step(Event::Blob(BlobEvent::WriteFinished {
        location: framed.clone(),
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction { .. })]
    ));
    assert_eq!(op.composed_location, Some(framed));
}

#[test]
fn raw_provider_object() {
    // Without compression the provider object is published as it is.
    let mut op = CompleteUploadOperation::new(finalize_input());
    let target = composed_location(Ulid::from_bytes([5u8; 16]));
    op.upload_record = Some(in_place_record(&op, &target));
    op.state = CompleteUploadState::ComposeBlob;

    let effects = op.step(Event::Blob(BlobEvent::WriteFinished {
        location: target.clone(),
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction { .. })]
    ));
    assert_eq!(op.composed_location, Some(target));
}

#[test]
fn conflict_keeps_object() {
    // The in-place object is the only copy of its parts, so a refused finalize keeps it.
    let mut op = CompleteUploadOperation::new(finalize_input());
    let target = composed_location(Ulid::from_bytes([5u8; 16]));
    op.upload_record = Some(in_place_record(&op, &target));
    op.reset_done = true;
    op.composed_location = Some(target.clone());
    op.state = CompleteUploadState::CommitFinalizeTransaction;

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));

    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected a kept cleanup row, got {effects:?}")
    };
    assert_eq!(
        BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        BlobCleanupWork::ReconcileWrite {
            location: target,
            owner: WriteOwner::Upload {
                upload_id: op.input.upload_id,
            },
        }
    );
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
    let mut op = CompleteUploadOperation::new(input);

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
    let mut op = CompleteUploadOperation::new(input);

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
    assert_eq!(deletes.len(), 4);
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
        format: StoredFormat::default(),
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
        format: StoredFormat::default(),
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
fn compose_keeps_cause() {
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input);
    operation.upload_record = Some(open_upload_record(&operation.input));
    operation.state = CompleteUploadState::ComposeBlob;

    let effects = operation.step(Event::Blob(BlobEvent::Error(BlobError::WriteError(
        "part limit exceeded".to_string(),
    ))));

    assert_eq!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    );
    assert_eq!(operation.state, CompleteUploadState::ResetUploadTransaction);
    assert_eq!(
        operation.cleanup.take_error(),
        Some(CompleteUploadError::BlobError(BlobError::WriteError(
            "part limit exceeded".to_string()
        )))
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

fn sealed_plan() -> aruna_core::structs::storage::encryption::SealPlan {
    use aruna_core::structs::storage::encryption::{BucketKeyRef, SealPlan};
    SealPlan {
        key: BucketKeyRef::new(Ulid::from_parts(8, 8), 1),
        public_key: [3; 32],
        cipher: Default::default(),
        block_keys: Default::default(),
        storage_generation: 2,
    }
}

/// An operation of an encrypted upload whose parts were sealed, with its composed archive.
fn sealed_operation(parts: &[&[u8]]) -> (CompleteUploadOperation, BackendLocation) {
    use aruna_core::structs::storage::format::PithosLayout;
    use aruna_core::structs::storage::multipart::{PartPiece, UploadEncryption};
    let input = finalize_input();
    let mut record = open_upload_record(&input);
    record.encryption = Some(UploadEncryption {
        plan: sealed_plan(),
        compression: Compression::Off,
    });
    let mut operation = CompleteUploadOperation::new(input);
    operation.upload_record = Some(record);
    operation.compose_share = Some(test_share());
    operation.resolved_parts = parts
        .iter()
        .zip(1u16..)
        .map(|(bytes, number)| {
            let mut part = part_record(number, bytes.len() as u64);
            part.location.hashes = Hasher::new_with_bytes(bytes).to_map();
            part.piece = Some(PartPiece {
                record: vec![number as u8],
                stored_len: bytes.len() as u64 + 40,
                content_offset: None,
            });
            part
        })
        .collect();
    let mut location = composed_location(Ulid::from_parts(9, 9));
    location.backend = BackendRef::node_default();
    let layout = PithosLayout {
        stored_size: 400,
        metadata_digest: [6; 32],
        storage_generation: 0,
    };
    location.format = StoredFormat::pithos(layout, sealed_plan().key);
    location.blob_size = parts.iter().map(|bytes| bytes.len() as u64).sum();
    (operation, location)
}

/// A reservation the tests hand to the completion in place of the blob adapter's.
fn test_share() -> aruna_core::structs::storage::multipart::WorkingShare {
    aruna_core::structs::storage::multipart::WorkingShare::new(1 << 30, std::sync::Arc::new(()))
}

#[test]
fn sealed_reserves_first() {
    // The composition's share is reserved before any piece record loads, and composition
    // receives that same share; a saturated budget therefore stops the completion early.
    use aruna_core::structs::storage::multipart::MAX_PART_SIZE;
    let (mut operation, _) = sealed_operation(&[b"first"]);
    operation.compose_share = None;
    operation.input.completed_parts = vec![CompleteMultipartPart {
        part_number: 1,
        etag: None,
        expected_checksums: Vec::new(),
    }];
    operation.state = CompleteUploadState::CommitMarkTransaction;
    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: TxnId::generate(),
    }));
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReserveCompose {
            content: MAX_PART_SIZE
        })]
    );
    let share = test_share();
    let effects = operation.step(Event::Blob(BlobEvent::ComposeReserved {
        share: share.clone(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { key_space, .. })] if key_space == UPLOAD_PART_KEYSPACE
    ));
    let effects = operation.compose_blob();
    let [Effect::Blob(BlobEffect::ComposePieces { share: kept, .. })] = effects.as_slice() else {
        panic!("expected a piece composition, got {effects:?}")
    };
    assert_eq!(*kept, share);
}

impl CompleteUploadOperation {
    fn step_composed(&mut self, location: BackendLocation) -> Effects {
        self.state = CompleteUploadState::ComposeBlob;
        self.step(Event::Blob(BlobEvent::WriteFinished { location }))
    }
}

#[test]
fn sealed_parts_compose() {
    // Completion composes the saved pieces with the captured plan, never the bucket's.
    let (mut operation, _) = sealed_operation(&[b"first", b"second"]);
    let effects = operation.compose_blob();
    let [
        Effect::Blob(BlobEffect::ComposePieces {
            resolved, parts, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected a piece composition, got {effects:?}")
    };
    assert_eq!(resolved.encryption, Some(sealed_plan()));
    assert_eq!(parts.len(), 2);

    let (mut operation, _) = sealed_operation(&[b"first"]);
    operation.resolved_parts[0].piece = None;
    operation.compose_blob();
    assert_eq!(
        operation.cleanup.take_error(),
        Some(CompleteUploadError::InvalidPart)
    );
}

#[test]
fn sealed_crcs_combine() {
    // Full-object CRCs come from the parts; a full-object SHA256 is never acknowledged.
    let parts: [&[u8]; 2] = [b"the first part", b"and the last"];
    let (mut operation, location) = sealed_operation(&parts);
    let effects = operation.step_composed(location.clone());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction { .. })]
    ));
    let composed = operation.composed_location.clone().unwrap();
    let whole = Hasher::new_with_bytes(&parts.concat()).to_map();
    for name in [HASH_CRC32, HASH_CRC32C, HASH_CRC64NVME] {
        assert_eq!(composed.hashes.get(name), whole.get(name), "{name}");
    }
    assert_eq!(composed.get_blake3(), None);

    let (mut operation, location) = sealed_operation(&parts);
    operation.input.expected_checksums = vec![ExpectedChecksum {
        algorithm: ChecksumAlgorithm::Sha256,
        digest: whole[HASH_SHA256].clone(),
    }];
    operation.step_composed(location);
    assert_eq!(
        operation.cleanup.take_error(),
        Some(CompleteUploadError::MissingExpectedChecksum("SHA256"))
    );
}

#[test]
fn pending_publishes_owner() {
    // Without a content hash the archive waits in pending_locations; its owner row and
    // version commit in the same transaction.
    let (mut operation, location) = sealed_operation(&[b"first"]);
    let txn_id = Ulid::from_parts(4, 4);
    operation.txn_id = Some(txn_id);
    operation.composed_location = Some(location.clone());
    let effects = operation.check_hash_lookup();
    let archive = ArchiveKey::of(&location);
    let [
        Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            txn_id: write_txn,
            ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected the pending location, got {effects:?}")
    };
    assert_eq!(key_space, PENDING_LOCATION_KEYSPACE);
    assert_eq!(key.as_ref(), archive.to_bytes());
    assert_eq!(*write_txn, Some(txn_id));
    assert!(operation.new_blob);

    operation.version_id = Some(Ulid::from_parts(5, 1));
    let effects = operation.write_version();
    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected the version, got {effects:?}")
    };
    let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
    assert_eq!(version.state.pending_archive(), Some(&archive));

    let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
        key: b"version".to_vec().into(),
    }));
    let version_key = VersionKey::new("bucket", "object", Ulid::from_parts(5, 1));
    let owner = CopyOwner::new(archive, version_key);
    let [Effect::Storage(StorageEffect::Write { key_space, key, .. })] = effects.as_slice() else {
        panic!("expected the owner row, got {effects:?}")
    };
    assert_eq!(key_space, aruna_core::keyspaces::COPY_OWNER_KEYSPACE);
    assert_eq!(key.as_ref(), owner.key().unwrap());
    // The stored credit is the new archive's, booked by its archive id.
    let credit = StoredDelta::for_location(&location, true).unwrap();
    assert_eq!(credit.bytes, 400);
}

#[test]
fn sealed_rotation_refused() {
    // A plan that is no longer current is never published.
    use aruna_core::structs::storage::encryption::EncryptionMode;
    let (mut operation, location) = sealed_operation(&[b"first"]);
    operation.txn_id = Some(Ulid::from_parts(4, 4));
    operation.composed_location = Some(location);
    let settings = BucketEncryption {
        mode: EncryptionMode::NodeManaged,
        bucket_id: Some(Ulid::from_parts(8, 8)),
        key_generation: 2,
        storage_generation: 2,
        ..Default::default()
    };
    operation.state = CompleteUploadState::CheckSealSettings;
    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: Some(settings.to_bytes().unwrap().into()),
    }));
    assert!(matches!(
        operation.cleanup.take_error(),
        Some(CompleteUploadError::BucketKey(_))
    ));
    assert!(operation.reclaim_pending);
}

/// Answers the envelope fence of a sealed completion; `pending` stores an epoch 1 envelope.
fn abe_fenced(pending: bool, epoch: u64) -> CompleteUploadOperation {
    use aruna_core::structs::storage::abe::{EnvelopePlan, create_envelope, create_parameters};
    let (mut operation, location) = sealed_operation(&[b"first"]);
    operation.txn_id = Some(Ulid::from_parts(4, 4));
    operation.composed_location = Some(location);
    let input = &operation.input;
    let secret = aruna_core::compute::SecretBytes::new(vec![9; 32]);
    let key = sealed_plan().key;
    let parameters = create_parameters(&secret, input.realm_id, input.node_id, key).unwrap();
    let (envelope, _) = create_envelope(EnvelopePlan {
        parameters: parameters.clone(),
        epoch: 1,
        write_id: Ulid::from_parts(6, 6),
        object_key: input.key.clone(),
        bucket_public: [3; 32],
    })
    .unwrap();
    let row = pending.then(|| envelope.to_bytes().unwrap().into());
    operation.state = CompleteUploadState::FenceAbe;
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (b"pending".to_vec().into(), row),
            (
                b"p".to_vec().into(),
                Some(parameters.to_bytes().unwrap().into()),
            ),
            (
                b"e".to_vec().into(),
                Some(epoch.to_be_bytes().to_vec().into()),
            ),
        ],
    }));
    operation
}

#[test]
fn stale_epoch_reclaims() {
    // A raised epoch refuses the completion; the record reset also deletes the pending envelope.
    let mut operation = abe_fenced(true, 2);
    assert!(operation.reclaim_pending);
    operation.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: Ulid::from_parts(4, 4),
    }));
    let txn_id = Ulid::from_parts(4, 5);
    operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    let mut record = operation.upload_record.clone().unwrap();
    record.status = MultipartUploadStatus::Completing;
    record.completing_since_ms = Some(TEST_NOW_MS);
    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: b"upload".to_vec().into(),
        value: Some(record.to_bytes().unwrap().into()),
    }));
    let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
        key: b"upload".to_vec().into(),
    }));
    let [Effect::Storage(StorageEffect::Delete { key_space, key, .. })] = effects.as_slice() else {
        panic!("expected the pending envelope delete, got {effects:?}")
    };
    assert_eq!(key_space, ABE_PENDING_KEYSPACE);
    assert_eq!(key.as_ref(), operation.input.upload_id.to_bytes());
    let effects = operation.step(Event::Storage(StorageEvent::DeleteResult {
        key: key.clone(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit })] if *commit == txn_id
    ));
    assert!(matches!(
        operation.cleanup.take_error(),
        Some(CompleteUploadError::BlobError(BlobError::Abe(
            AbeError::Epoch
        )))
    ));
}

/// Refuses the commit of a published envelope and answers the reset's recheck with `epoch`.
fn conflict_recheck(epoch: u64) -> (CompleteUploadOperation, Effects) {
    use aruna_core::structs::storage::abe::create_parameters;
    use aruna_core::structs::storage::encryption::EncryptionMode;
    let mut operation = abe_fenced(true, 1);
    let envelope = operation.envelope.take().unwrap();
    operation.envelope_bytes = 10;
    operation.state = CompleteUploadState::CommitFinalizeTransaction;
    operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));
    let txn_id = Ulid::from_parts(4, 5);
    operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    let mut record = operation.upload_record.clone().unwrap();
    record.status = MultipartUploadStatus::Completing;
    record.completing_since_ms = Some(TEST_NOW_MS);
    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: b"upload".to_vec().into(),
        value: Some(record.to_bytes().unwrap().into()),
    }));
    let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
        key: b"upload".to_vec().into(),
    }));
    let [
        Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: read,
        }),
    ] = effects.as_slice()
    else {
        panic!("expected the pending recheck, got {effects:?}")
    };
    assert_eq!((reads.len(), *read), (4, Some(txn_id)));
    let settings = BucketEncryption {
        mode: EncryptionMode::NodeManaged,
        bucket_id: Some(Ulid::from_parts(8, 8)),
        key_generation: 1,
        storage_generation: 2,
        ..Default::default()
    };
    let input = &operation.input;
    let secret = aruna_core::compute::SecretBytes::new(vec![9; 32]);
    let key = sealed_plan().key;
    let parameters = create_parameters(&secret, input.realm_id, input.node_id, key).unwrap();
    let values = vec![
        (
            reads[0].1.clone(),
            Some(settings.to_bytes().unwrap().into()),
        ),
        (
            reads[1].1.clone(),
            Some(envelope.to_bytes().unwrap().into()),
        ),
        (
            reads[2].1.clone(),
            Some(parameters.to_bytes().unwrap().into()),
        ),
        (
            reads[3].1.clone(),
            Some(epoch.to_be_bytes().to_vec().into()),
        ),
    ];
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    (operation, effects)
}

#[test]
fn conflict_reclaims_stale() {
    // An epoch raised between the fence read and the commit drops the pending envelope.
    let (_, effects) = conflict_recheck(2);
    assert!(
        matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { key_space, .. })] if key_space == ABE_PENDING_KEYSPACE
        ),
        "{effects:?}"
    );
    // An unrelated conflict keeps it, so the upload stays retryable.
    let (_, effects) = conflict_recheck(1);
    assert!(
        matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ),
        "{effects:?}"
    );
}

#[test]
fn bucket_only_publishes() {
    // A generation without admitted parameters completes without an envelope.
    let (mut operation, location) = sealed_operation(&[b"first"]);
    operation.txn_id = Some(Ulid::from_parts(4, 4));
    operation.composed_location = Some(location);
    operation.state = CompleteUploadState::FenceAbe;
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (b"pending".to_vec().into(), None),
            (b"p".to_vec().into(), None),
            (b"e".to_vec().into(), None),
        ],
    }));
    assert!(
        !matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ),
        "{effects:?}"
    );
    assert!(operation.envelope.is_none());
    assert!(operation.cleanup.take_error().is_none());
}

#[test]
fn missing_envelope_refused() {
    // An upload of a generation with admitted parameters never publishes without its envelope.
    let mut operation = abe_fenced(false, 1);
    assert!(!operation.reclaim_pending);
    assert!(matches!(
        operation.cleanup.take_error(),
        Some(CompleteUploadError::BlobError(BlobError::Abe(
            AbeError::Stale
        )))
    ));
}

#[test]
fn envelope_publishes_charged() {
    // A current envelope is published with the version and charged to the group.
    let mut operation = abe_fenced(true, 1);
    assert!(operation.envelope.is_some());
    let location = operation.composed_location.clone().unwrap();
    operation.final_location = Some(location);
    operation.version_id = Some(Ulid::from_parts(5, 1));
    operation.state = CompleteUploadState::WriteVersionRecord;
    let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
        key: b"version".to_vec().into(),
    }));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })] = effects.as_slice() else {
        panic!("expected the envelope rows, got {effects:?}")
    };
    assert_eq!(*txn_id, operation.txn_id);
    let spaces: Vec<_> = writes.iter().map(|(space, ..)| space.as_str()).collect();
    assert_eq!(
        spaces,
        [
            aruna_core::keyspaces::ABE_ENVELOPE_KEYSPACE,
            aruna_core::keyspaces::ABE_VERSION_KEYSPACE,
            aruna_core::keyspaces::ABE_ARCHIVE_KEYSPACE,
        ]
    );
    assert!(operation.envelope_bytes > 0);
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { key_space, .. })]
            if key_space == aruna_core::keyspaces::COPY_OWNER_KEYSPACE
    ));
}

/// Publishes the fenced pending envelope under a metadata limit, returning the step's effects.
fn publish_envelope(metadata_bytes: u64) -> (CompleteUploadOperation, Effects) {
    let limits = RoCrateLimits {
        metadata_bytes,
        ..Default::default()
    };
    let mut operation = abe_fenced(true, 1).with_rocrate_limits(limits);
    operation.final_location = operation.composed_location.clone();
    operation.version_id = Some(Ulid::from_parts(5, 1));
    operation.state = CompleteUploadState::WriteVersionRecord;
    let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
        key: b"version".to_vec().into(),
    }));
    (operation, effects)
}

#[test]
fn envelope_limit_boundary() {
    // The metadata limit and the charge count a pending mapping at its promoted size.
    use aruna_core::structs::storage::abe::{EnvelopeArchive, envelope_charge};
    let (operation, effects) = publish_envelope(u64::MAX);
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the envelope rows, got {effects:?}")
    };
    let (envelope, id, mapping) = (&writes[0].2, &writes[0].1, &writes[2].2);
    let location = operation.final_location.as_ref().unwrap();
    let mut archive: EnvelopeArchive = postcard::from_bytes(mapping).unwrap();
    assert!(archive.location_key.is_empty());
    let key = BlobLocationKey::new(
        [1; 32],
        location.format.encoding(),
        location.backend.clone(),
    );
    archive.location_key = key.to_bytes();
    let promoted = postcard::to_allocvec(&archive).unwrap();
    assert!(promoted.len() > mapping.len() + 1);
    assert_eq!(
        operation.envelope_bytes,
        envelope_charge(envelope, &promoted)
    );
    assert_eq!(operation.envelope_bytes, envelope_charge(envelope, mapping));
    let metadata = &operation.upload_record.as_ref().unwrap().metadata;
    let metadata = postcard::to_allocvec(metadata).unwrap();
    let limit = operation.envelope_bytes + (id.len() + metadata.len()) as u64;
    let (_, effects) = publish_envelope(limit);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchWrite { .. })]
    ));
    let (mut operation, _) = publish_envelope(limit - 1);
    assert!(matches!(
        operation.cleanup.take_error(),
        Some(CompleteUploadError::BlobError(BlobError::Abe(
            AbeError::Limit
        )))
    ));
}

#[test]
fn ceiling_covers_promotion() {
    // A completion exactly at the ceiling has paid for the mapping its keyed read promotes.
    use aruna_core::structs::storage::usage::UsageCounters;
    for (slack, exceeded) in [(0, false), (1, true)] {
        let (mut operation, _) = publish_envelope(u64::MAX);
        let size = operation.final_location.as_ref().unwrap().blob_size;
        let used = 7;
        operation.input.quota_ceiling = Some(used + size + operation.envelope_bytes - slack);
        operation.state = CompleteUploadState::WriteReplicationObligation;
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"obligation".to_vec().into(),
        }));
        assert!(matches!(operation.state, CompleteUploadState::EnforceQuota));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"realm".to_vec().into(),
            value: None,
        }));
        let counters = UsageCounters {
            logical_bytes: used,
            ..Default::default()
        };
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"group".to_vec().into(),
            value: Some(counters.to_bytes().unwrap().into()),
        }));
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        let error = operation.cleanup.take_error();
        let refused = matches!(error, Some(CompleteUploadError::QuotaExceeded { .. }));
        assert_eq!(refused, exceeded, "slack {slack}: {error:?}");
    }
}

#[test]
fn plain_completion_refused() {
    // A plain upload never publishes once its bucket encrypts, even if its record predates it.
    use aruna_core::structs::storage::encryption::EncryptionMode;
    let input = finalize_input();
    let record = open_upload_record(&input);
    let mut operation = CompleteUploadOperation::new(input);
    operation.upload_record = Some(record);
    operation.txn_id = Some(Ulid::from_parts(4, 4));
    operation.composed_location = Some(composed_location(Ulid::from_parts(9, 9)));
    let settings = BucketEncryption {
        mode: EncryptionMode::NodeManaged,
        bucket_id: Some(Ulid::from_parts(8, 8)),
        key_generation: 1,
        ..Default::default()
    };
    operation.state = CompleteUploadState::CheckSealSettings;
    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: Some(settings.to_bytes().unwrap().into()),
    }));
    assert!(matches!(
        operation.cleanup.take_error(),
        Some(CompleteUploadError::BucketKey(_))
    ));
}

#[test]
fn pending_schedules_promotion() {
    // A pending completion during an unlock session asks for promotion at once; the timer is
    // persisted, so it also runs after a restart. A known hash needs none.
    let (mut operation, location) = sealed_operation(&[b"first"]);
    let key = location.format.bucket_key().unwrap();
    operation.final_location = Some(location.clone());
    let effects = operation.finish_commit();
    let promote = Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::PromotePending {
            bucket_id: key.bucket_id,
            generation: key.generation,
        },
        after: Duration::ZERO,
    });
    assert!(effects.contains(&promote));

    let (mut operation, mut known) = sealed_operation(&[b"first"]);
    known.hashes.insert(
        aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
        vec![7; 32],
    );
    operation.final_location = Some(known);
    let effects = operation.finish_commit();
    assert!(!effects.contains(&promote));
}

#[test]
fn omitted_parts_paged() {
    // Many large omitted parts are read page by page and kept only as cleanup metadata; only
    // the selected part keeps its piece record.
    use aruna_core::structs::storage::multipart::PartPiece;
    let (mut operation, _) = sealed_operation(&[b"first"]);
    let upload_id = operation.input.upload_id;
    operation.input.completed_parts = vec![CompleteMultipartPart {
        part_number: 300,
        etag: None,
        expected_checksums: Vec::new(),
    }];
    operation.input.object_size = None;
    let parts: Vec<MultipartPart> = (1..=400u16)
        .map(|number| {
            let mut part = part_record(number, 6 << 20);
            part.location.hashes = Hasher::new_with_bytes(b"part").to_map();
            part.piece = Some(PartPiece {
                record: vec![7; 4096],
                stored_len: (6 << 20) + 40,
                content_offset: None,
            });
            part
        })
        .collect();
    let values = part_values(upload_id, parts);
    let (first, second) = values.split_at(PART_PAGE);
    let next = first.last().unwrap().0.clone();
    operation.state = CompleteUploadState::ReadUploadParts;
    let effects = operation.step(Event::Storage(StorageEvent::IterResult {
        values: first.to_vec(),
        next_start_after: Some(next.clone()),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { start: Some(IterStart::After(after)), limit, .. })]
            if *after == next && *limit == PART_PAGE
    ));
    // Omitted parts already read hold no piece record while the next page loads.
    assert!(
        operation
            .upload_parts
            .iter()
            .all(|part| part.piece.is_none())
    );
    assert_eq!(operation.selected_parts.len(), 0);

    operation.step(Event::Storage(StorageEvent::IterResult {
        values: second.to_vec(),
        next_start_after: None,
    }));
    assert_eq!(operation.upload_parts.len(), 400);
    assert!(
        operation
            .upload_parts
            .iter()
            .all(|part| part.piece.is_none())
    );
    assert_eq!(operation.resolved_parts.len(), 1);
    assert_eq!(operation.resolved_parts[0].part_number, 300);
    assert!(operation.resolved_parts[0].piece.is_some());
}

#[test]
fn oversized_selection_refused() {
    use aruna_core::structs::storage::multipart::PartPiece;
    let (mut operation, _) = sealed_operation(&[]);
    operation.input.object_size = None;
    operation.input.completed_parts = (1..=10_000)
        .map(|part_number| CompleteMultipartPart {
            part_number,
            etag: None,
            expected_checksums: Vec::new(),
        })
        .collect();
    operation.read_parts(None);
    let limit = aruna_blob::blob::pithos::MAX_SIZE;
    let mut effects = smallvec![];
    for start in (1..=10_000u16).step_by(PART_PAGE) {
        let end = (start as usize + PART_PAGE).min(10_001) as u16;
        let parts = (start..end)
            .map(|number| {
                let mut part = part_record(number, MAX_PART_SIZE);
                part.piece = Some(PartPiece {
                    record: number.to_le_bytes().to_vec(),
                    stored_len: MAX_PART_SIZE,
                    content_offset: None,
                });
                part
            })
            .collect();
        let values = part_values(operation.input.upload_id, parts);
        let next_start_after = values.last().map(|(key, _)| key.clone());
        effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Blob(BlobEffect::ComposePieces { .. })))
        );
        if operation.state != CompleteUploadState::ReadUploadParts {
            break;
        }
    }
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction { .. })]
    ));
    assert!(operation.selected_bytes <= limit);
    assert!(operation.resolved_parts.is_empty());
    assert_eq!(
        operation.cleanup.take_error(),
        Some(CompleteUploadError::BlobError(
            BlobError::SizeLimitExceeded { limit }
        ))
    );
}

#[test]
fn share_outlives_compose() {
    // The completion keeps its reservation after composition starts, while publication still
    // holds the selected piece records.
    let (mut operation, location) = sealed_operation(&[b"first"]);
    let share = operation.compose_share.clone().unwrap();
    let effects = operation.compose_blob();
    let [Effect::Blob(BlobEffect::ComposePieces { share: sent, .. })] = effects.as_slice() else {
        panic!("expected a piece composition, got {effects:?}")
    };
    assert_eq!(*sent, share);
    operation.step_composed(location);
    assert!(operation.resolved_parts[0].piece.is_some());
    assert_eq!(operation.compose_share, Some(share));
}
