//! Tests the multipart completion as calls and results: the mark, the compose, the finalize
//! child and the cleanup its failures leave to `abort`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::format::StoredFormat;
use std::time::Duration;

use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::checksum::{
    HASH_BLAKE3, HASH_CRC32, HASH_CRC32C, HASH_CRC64NVME, HASH_SHA256,
};
use aruna_core::structs::storage::multipart::{
    BackendUpload, COMPLETION_LEASE_MS, MultipartChecksumHint,
};

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

fn part_values(upload_id: Ulid, parts: Vec<MultipartPart>) -> Vec<(Key, Value)> {
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

fn composed_location(backend_id: Ulid) -> BackendLocation {
    let mut location = part_record(1, 10).location;
    location.backend = BackendRef::Group(backend_id);
    location.partial = false;
    location
}

fn hashed(mut location: BackendLocation) -> BackendLocation {
    location
        .hashes
        .insert(HASH_BLAKE3.to_string(), vec![7u8; 32]);
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

/// The finalize child of a completion holding `record` and its composed object.
fn finalizing(record: MultipartUpload, composed: BackendLocation) -> CompleteUploadOperation {
    let mut parent = CompleteUploadOperation::new(finalize_input());
    parent.upload_record = Some(record);
    parent.composed_location = Some(composed);
    parent.finalize_child()
}

fn values(values: Vec<Option<Value>>) -> IoResult {
    IoResult::Values(values)
}

fn event(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

/// The finalize child's answer to its parent.
fn finalize_answer(result: Result<CompleteUploadResult, CompleteUploadError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

fn written(turn: &Turn) -> &[(String, Key, Value)] {
    match turn {
        Ok(Next::Call(IoCall::Write(rows))) => rows,
        other => panic!("expected a write, got {other:?}"),
    }
}

fn read_spaces(turn: &Turn) -> Vec<&str> {
    match turn {
        Ok(Next::Call(IoCall::Read(reads))) => reads.iter().map(|(space, _)| &space[..]).collect(),
        other => panic!("expected a read, got {other:?}"),
    }
}

fn asked(turn: &Turn) -> &Effect {
    match turn {
        Ok(Next::Call(IoCall::Effect(effect))) => effect,
        other => panic!("expected an effect, got {other:?}"),
    }
}

fn starts_child(turn: &Turn) -> bool {
    matches!(turn, Ok(Next::Call(IoCall::Child(_))))
}

/// The cleanup rows among `calls`, in order.
fn cleanup_works(calls: &[IoCall]) -> Vec<BlobCleanupWork> {
    let rows = calls.iter().filter_map(|call| match call {
        IoCall::Write(rows) => Some(rows),
        _ => None,
    });
    rows.flatten()
        .map(|(space, _, value)| {
            assert_eq!(space, BLOB_CLEANUP_KEYSPACE);
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap()
        })
        .collect()
}

fn released(calls: &[IoCall]) -> Option<Ulid> {
    calls.iter().find_map(|call| match call {
        IoCall::Effect(effect) => match &**effect {
            Effect::Blob(BlobEffect::ReleaseReservation { id }) => Some(*id),
            _ => None,
        },
        _ => None,
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

    let turn = operation.write_obligation();

    let [(_, _, value)] = written(&turn) else {
        panic!("expected one obligation write, got {turn:?}")
    };
    let record = LiveObligationRecord::from_bytes(value.as_ref()).expect("obligation decodes");
    assert_eq!(record.auth_context.path_restrictions, Some(restrictions));
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

#[test]
fn refuses_disabled_backend() {
    // Compose already ran on the pinned backend, so the finalize fence has to fail the
    // transaction and leave the composed object to the rollback.
    let backend_id = Ulid::from_bytes([5u8; 16]);
    let input = finalize_input();
    let mut child = finalizing(open_upload_record(&input), composed_location(backend_id));
    assert_eq!(child.transaction(), TransactionMode::Write);

    let turn = child.first();
    assert_eq!(read_spaces(&turn), [PURGE_FENCE_KEYSPACE]);
    let turn = child.next(values(vec![None]));
    assert_eq!(
        read_spaces(&turn),
        [S3_BUCKET_KEYSPACE, NODE_SUBJECT_KEYSPACE]
    );
    let turn = child.next(values(vec![None, None]));
    assert_eq!(read_spaces(&turn), [BUCKET_ENCRYPTION_KEYSPACE]);
    let turn = child.next(values(vec![None]));
    assert_eq!(read_spaces(&turn), [STORAGE_BACKEND_KEYSPACE]);

    let turn = child.next(values(vec![Some(disabled_record(backend_id).into())]));

    assert_eq!(turn.err(), Some(BackendFenceError::Unavailable.into()));
    // The finalize leaves its cleanup to the parent.
    assert!(child.abort().is_empty());
}

#[test]
fn stale_encoding_aborts() {
    // The setting changed while the parts were composed: the raw object must not
    // become a version a finished migration would never revisit.
    let input = finalize_input();
    let composed = composed_location(Ulid::from_bytes([5u8; 16]));
    let mut child = finalizing(open_upload_record(&input), composed);
    child.first().unwrap();
    child.next(values(vec![None])).unwrap();
    let bucket = BucketInfo {
        group_id: Ulid::from_bytes([6u8; 16]),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: input.created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Zstd { level: 3 },
        distribution: None,
    };

    let turn = child.next(values(vec![Some(bucket.to_bytes().unwrap().into()), None]));

    assert_eq!(turn.err(), Some(StorageError::TransactionConflict.into()));
}

#[test]
fn fence_rejects_stray() {
    let input = finalize_input();
    let composed = composed_location(Ulid::from_bytes([5u8; 16]));
    let mut child = finalizing(open_upload_record(&input), composed);

    let turn = child.backend_fenced(Some(vec![1, 2, 3].into()));

    assert!(matches!(
        turn,
        Err(CompleteUploadError::BackendFenceError(
            BackendFenceError::Read(_)
        ))
    ));
}

/// A completion waiting for its finalize child, after a committed mark.
fn finalize_waiting(record: MultipartUpload, composed: BackendLocation) -> CompleteUploadOperation {
    let mut operation = CompleteUploadOperation::new(finalize_input());
    operation.upload_record = Some(record);
    operation.marked = true;
    operation.composed_location = Some(composed);
    operation.state = CompleteUploadState::Finalize;
    operation
}

#[test]
fn refused_rolls_back() {
    // A refused finalize proves no version names the composed object, so it is queued for
    // deletion and the record reopened.
    let input = finalize_input();
    let composed = composed_location(Ulid::from_bytes([5u8; 16]));
    let mut operation = finalize_waiting(open_upload_record(&input), composed.clone());

    let turn = operation.next(finalize_answer(Err(BackendFenceError::Unavailable.into())));
    assert_eq!(turn.err(), Some(BackendFenceError::Unavailable.into()));

    // The record reopens and a rollback child deletes the composed object.
    let calls = operation.abort();
    assert!(matches!(
        calls.as_slice(),
        [IoCall::Child(_), IoCall::Child(_)]
    ));
    assert!(cleanup_works(&calls).is_empty());
    assert!(operation.composed_location.is_none());
    // A second abort finds nothing left to do.
    assert!(operation.abort().is_empty());

    // A lost conflict is the same refusal.
    let composed = composed_location(Ulid::from_bytes([5u8; 16]));
    let mut operation = finalize_waiting(open_upload_record(&input), composed.clone());
    let conflict = StorageError::TransactionConflict;
    operation
        .next(finalize_answer(Err(conflict.into())))
        .unwrap_err();
    let calls = operation.abort();
    assert!(matches!(
        calls.as_slice(),
        [IoCall::Child(_), IoCall::Child(_)]
    ));
    assert_eq!(operation.composed_location, None);
    drop(composed);
}

#[test]
fn commit_keeps_composed() {
    // A possibly-landed finalize commit owns the composed object, so it goes to
    // reconciliation instead of being deleted here.
    let input = finalize_input();
    let location = hashed(composed_location(Ulid::from_bytes([5u8; 16])));
    let mut operation = finalize_waiting(open_upload_record(&input), location.clone());

    let failed = StorageError::CommitFailed;
    let turn = operation.next(finalize_answer(Err(failed.clone().into())));

    assert_eq!(turn.err(), Some(failed.into()));
    assert!(operation.composed_location.is_none());
    let calls = operation.abort();
    // The reopen reads the record first: a commit that applied deleted it.
    assert!(matches!(calls.first(), Some(IoCall::Child(_))));
    assert_eq!(
        cleanup_works(&calls),
        [BlobCleanupWork::ReconcileWrite {
            location,
            owner: WriteOwner::Blob {
                blake3: [7u8; 32],
                realm_id: input.realm_id,
                ttl_ms: RoCrateLimits::default().holder_ttl_ms,
                announce: true,
            },
        }]
    );
}

#[test]
fn cleanup_keeps_location() {
    // A compose that could not settle its reservation hands the object to the durable delete
    // queue and releases the reservation, after the record reopens.
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input.clone());
    let location = composed_location(Ulid::from_bytes([5u8; 16]));
    operation.upload_record = Some(open_upload_record(&input));
    operation.marked = true;
    operation.state = CompleteUploadState::ComposeBlob;

    let failed = BlobError::WriteCleanup {
        location: location.clone(),
        message: "reservation finalization failed".to_string(),
    };
    let turn = operation.next(event(Event::Blob(BlobEvent::Error(failed))));
    assert_eq!(turn.err(), Some(CompleteUploadError::CompleteUploadFailed));
    assert_eq!(operation.delete_location, Some(location.clone()));

    let calls = operation.abort();
    assert!(matches!(calls.first(), Some(IoCall::Child(_))));
    assert_eq!(
        cleanup_works(&calls),
        [BlobCleanupWork::DeleteBlob {
            location: location.clone()
        }]
    );
    assert_eq!(released(&calls), Some(location.ulid));
    assert!(matches!(calls.last(), Some(IoCall::Effect(_))));
}

fn in_place_record(input: &CompleteUploadInput, target: &BackendLocation) -> MultipartUpload {
    let mut record = open_upload_record(input);
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
    let input = finalize_input();
    let target = hashed(composed_location(Ulid::from_bytes([5u8; 16])));
    let record = in_place_record(&input, &target);
    let mut operation = finalize_waiting(record, target.clone());

    let failed = StorageError::CommitFailed;
    operation
        .next(finalize_answer(Err(failed.into())))
        .unwrap_err();

    assert_eq!(
        cleanup_works(&operation.abort()),
        [BlobCleanupWork::ReconcileWrite {
            location: target,
            owner: WriteOwner::CompletedUpload {
                upload_id: input.upload_id,
                blake3: [7u8; 32],
                realm_id: input.realm_id,
                ttl_ms: RoCrateLimits::default().holder_ttl_ms,
                announce: true,
            },
        }]
    );
}

#[test]
fn conflict_keeps_object() {
    // The in-place object is the only copy of its parts, so a refused finalize keeps it.
    let input = finalize_input();
    let target = composed_location(Ulid::from_bytes([5u8; 16]));
    let mut operation = finalize_waiting(in_place_record(&input, &target), target.clone());

    let conflict = StorageError::TransactionConflict;
    operation
        .next(finalize_answer(Err(conflict.into())))
        .unwrap_err();

    assert_eq!(
        cleanup_works(&operation.abort()),
        [BlobCleanupWork::ReconcileWrite {
            location: target,
            owner: WriteOwner::Upload {
                upload_id: input.upload_id,
            },
        }]
    );
}

#[test]
fn cut_finalize_reconciles() {
    // A run that ends while its finalize child runs may have committed: the record is not
    // reopened, and the composed object goes to reconciliation without a release.
    let input = finalize_input();
    let location = composed_location(Ulid::from_bytes([5u8; 16]));
    let mut operation = finalize_waiting(open_upload_record(&input), location.clone());
    operation.cleanup.set_release(location.ulid);

    let calls = operation.abort();

    assert!(!calls.iter().any(|call| matches!(call, IoCall::Child(_))));
    assert_eq!(
        cleanup_works(&calls),
        [BlobCleanupWork::ReconcileReservation { location }]
    );
    assert_eq!(released(&calls), None);
}

#[test]
fn abort_queues_cleanup() {
    let mut operation = CompleteUploadOperation::new(finalize_input());
    let location = composed_location(Ulid::from_bytes([5u8; 16]));
    operation.delete_location = Some(location.clone());
    operation.state = CompleteUploadState::ComposeBlob;

    let calls = operation.abort();

    assert_eq!(
        cleanup_works(&calls),
        [BlobCleanupWork::DeleteBlob { location }]
    );
    assert!(operation.delete_location.is_none());
}

#[test]
fn completes_at_provider() {
    // The provider assembles the parts; no compose reads them back.
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input.clone());
    let target = composed_location(Ulid::from_bytes([5u8; 16]));
    operation.upload_record = Some(in_place_record(&input, &target));
    operation.resolved_parts = vec![part_record(1, 10)];
    // Every streamed part is named, listed or not, so completion never writes over one.
    let mut staged = [part_record(1, 10), part_record(3, 10)];
    for part in &mut staged {
        part.location.partial = false;
    }
    let [first, last] = staged;
    operation.upload_parts = vec![first, part_record(2, 10), last];

    let turn = operation.compose_blob();

    let Effect::Blob(BlobEffect::CompleteUpload {
        backend_upload,
        parts,
        streamed,
        compression,
        if_not_exists,
    }) = asked(&turn)
    else {
        panic!("expected the provider completion, got {turn:?}")
    };
    assert_eq!(backend_upload.location, target);
    assert_eq!(parts, &operation.resolved_parts);
    assert_eq!(streamed, &vec![2]);
    assert_eq!(compression, &operation.compression);
    assert!(!if_not_exists);
}

fn composed_event(location: BackendLocation) -> IoResult {
    event(Event::Blob(BlobEvent::WriteFinished { location }))
}

#[test]
fn frames_provider_object() {
    // Compression was turned on after the upload opened: the raw provider object is composed
    // into frames, and only that copy is checked and published.
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input.clone());
    let target = composed_location(Ulid::from_bytes([5u8; 16]));
    operation.upload_record = Some(in_place_record(&input, &target));
    operation.compression = Compression::Zstd { level: 3 };
    operation.state = CompleteUploadState::ComposeBlob;

    let turn = operation.next(composed_event(target.clone()));

    let Effect::Blob(BlobEffect::Compose {
        resolved, parts, ..
    }) = asked(&turn)
    else {
        panic!("expected a framed compose, got {turn:?}")
    };
    assert_eq!(parts, &vec![target.clone()]);
    assert_eq!(resolved.compression, Compression::Zstd { level: 3 });
    assert_eq!(operation.composed_location, None);

    let mut framed = composed_location(Ulid::from_bytes([5u8; 16]));
    framed.ulid = Ulid::from_bytes([8u8; 16]);
    framed.backend_path = "bucket/framed".to_string();
    let turn = operation.next(composed_event(framed.clone()));

    assert!(starts_child(&turn));
    assert_eq!(operation.state, CompleteUploadState::Finalize);
    assert_eq!(operation.composed_location, Some(framed));
}

#[test]
fn framed_provider_object() {
    // A framed provider object is published as it is; another level composes its original bytes.
    use aruna_core::structs::storage::format::{FrameLayout, StoredLayout};
    let input = finalize_input();
    let mut target = composed_location(Ulid::from_bytes([5u8; 16]));
    target.format.layout = StoredLayout::Frames(Box::new(FrameLayout {
        level: 3,
        frames: 1,
        stored_size: 9,
        index_hash: [1; 32],
    }));
    for (level, composes) in [(3, false), (5, true)] {
        let mut operation = CompleteUploadOperation::new(input.clone());
        operation.upload_record = Some(in_place_record(&input, &target));
        operation.compression = Compression::Zstd { level };
        operation.state = CompleteUploadState::ComposeBlob;

        let turn = operation.next(composed_event(target.clone()));

        match composes {
            true => assert!(matches!(
                asked(&turn),
                Effect::Blob(BlobEffect::Compose { parts, .. }) if parts == &vec![target.clone()]
            )),
            false => {
                assert!(starts_child(&turn));
                assert_eq!(operation.composed_location.as_ref(), Some(&target));
            }
        }
    }
}

#[test]
fn raw_provider_object() {
    // Without compression the provider object is published as it is.
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input.clone());
    let target = composed_location(Ulid::from_bytes([5u8; 16]));
    operation.upload_record = Some(in_place_record(&input, &target));
    operation.state = CompleteUploadState::ComposeBlob;

    let turn = operation.next(composed_event(target.clone()));

    assert!(starts_child(&turn));
    assert_eq!(operation.composed_location, Some(target));
}

/// Runs the mark child on `record` up to its write.
fn mark(
    input: CompleteUploadInput,
    record: &MultipartUpload,
) -> (MarkCompleting, Result<Next<Marked>, CompleteUploadError>) {
    let mut mark = MarkCompleting {
        input,
        record: None,
    };
    assert_eq!(mark.transaction(), TransactionMode::Write);
    let Ok(Next::Call(IoCall::Read(reads))) = mark.first() else {
        panic!("expected the fence and record read")
    };
    assert_eq!(reads[0].0, PURGE_FENCE_KEYSPACE);
    assert_eq!(reads[1].0, UPLOAD_KEYSPACE);
    let row = Some(record.to_bytes().unwrap().into());
    let turn = mark.next(values(vec![None, row]));
    (mark, turn)
}

// A fresh completion stamps the lease it will be judged by.
#[test]
fn marks_completion_lease() {
    let input = finalize_input();
    let mut record = open_upload_record(&input);
    record.status = MultipartUploadStatus::Open;

    let (mut mark, turn) = mark(input, &record);

    let Ok(Next::Call(IoCall::Write(rows))) = turn else {
        panic!("expected the marking write, got {turn:?}")
    };
    let written = MultipartUpload::from_bytes(rows[0].2.as_ref()).unwrap();
    assert_eq!(written.status, MultipartUploadStatus::Completing);
    assert_eq!(written.completing_since_ms, Some(TEST_NOW_MS));
    let Ok(Next::Done((marked, checksum_type))) = mark.next(IoResult::Done) else {
        panic!("expected the marked record")
    };
    assert_eq!(marked, written);
    assert_eq!(checksum_type, MultipartChecksumType::FullObject);

    // A missing record is no upload.
    let mut mark = MarkCompleting {
        input: finalize_input(),
        record: None,
    };
    mark.first().unwrap();
    let turn = mark.next(values(vec![None, None]));
    assert_eq!(turn.err(), Some(CompleteUploadError::NoSuchUpload));
}

// A completion whose request died left the record Completing; the next one
// takes it over once the lease lapsed, instead of failing forever.
#[test]
fn takes_stale_lease() {
    let input = finalize_input();
    let mut record = open_upload_record(&input);
    record.completing_since_ms = Some(TEST_NOW_MS - COMPLETION_LEASE_MS);

    let (_, turn) = mark(input, &record);

    let Ok(Next::Call(IoCall::Write(rows))) = turn else {
        panic!("expected the take-over write, got {turn:?}")
    };
    let written = MultipartUpload::from_bytes(rows[0].2.as_ref()).unwrap();
    assert_eq!(written.completing_since_ms, Some(TEST_NOW_MS));
}

// A live lease is a retryable refusal, never NoSuchUpload.
#[test]
fn refuses_live_lease() {
    let input = finalize_input();
    let mut record = open_upload_record(&input);
    record.completing_since_ms = Some(TEST_NOW_MS - 1);

    let (_, turn) = mark(input, &record);

    assert_eq!(turn.err(), Some(CompleteUploadError::CompletionInProgress));
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

    let (_, turn) = mark(input, &record);

    assert_eq!(
        turn.err(),
        Some(CompleteUploadError::ChecksumContractMismatch)
    );
}

/// Runs the reopen child on `record`.
fn reopen(record: &MultipartUpload) -> (ReopenUpload, Reopened) {
    let mut reopen = ReopenUpload {
        input: finalize_input(),
        plan: None,
        reclaim_pending: false,
        recheck_pending: false,
        step: ReopenStep::ReadRecord,
    };
    assert_eq!(reopen.transaction(), TransactionMode::Write);
    assert!(matches!(reopen.first(), Ok(Next::Call(IoCall::Read(_)))));
    let turn = reopen.next(values(vec![Some(record.to_bytes().unwrap().into())]));
    (reopen, turn)
}

// A deadline between the mark and the finalize must reopen the record.
#[test]
fn abort_reopens_record() {
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input.clone());
    operation.upload_record = Some(open_upload_record(&input));
    operation.marked = true;
    operation.state = CompleteUploadState::ComposeBlob;

    let calls = operation.abort();

    assert!(matches!(calls.as_slice(), [IoCall::Child(_)]));
    assert!(operation.abort().is_empty());

    // The reopen writes the record open again.
    let mut record = open_upload_record(&input);
    record.completing_since_ms = Some(TEST_NOW_MS);
    let (mut reopen, turn) = reopen(&record);
    let Ok(Next::Call(IoCall::Write(rows))) = turn else {
        panic!("expected the reopen, got {turn:?}")
    };
    let written = MultipartUpload::from_bytes(rows[0].2.as_ref()).unwrap();
    assert_eq!(written.status, MultipartUploadStatus::Open);
    assert_eq!(written.completing_since_ms, None);
    assert_eq!(reopen.next(IoResult::Done), Ok(Next::Done(())));
}

// Only the attempt that owns the lease may reopen the record.
#[test]
fn reset_skips_foreign() {
    let input = finalize_input();
    let mut record = open_upload_record(&input);
    record.completing_since_ms = Some(TEST_NOW_MS + 1);

    let (_, turn) = reopen(&record);

    assert_eq!(turn, Ok(Next::Done(())));
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

    let checked = CompleteUploadOperation::validate_checksum_contract(&mut op.input, &upload);
    assert_eq!(checked, Ok(()));
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

fn two_parts(first: u64) -> Result<(Vec<MultipartPart>, Vec<MultipartPart>), CompleteUploadError> {
    let mut input = finalize_input();
    input.completed_parts = (1..=2)
        .map(|part_number| CompleteMultipartPart {
            part_number,
            etag: None,
            expected_checksums: vec![],
        })
        .collect();
    input.object_size = None;
    let values = part_values(
        input.upload_id,
        vec![part_record(1, first), part_record(2, 1)],
    );
    CompleteUploadOperation::new(input).extract_requested_parts(values)
}

#[test]
fn undersized_middle_rejected() {
    assert_eq!(
        two_parts(5 * 1024 * 1024 - 1),
        Err(CompleteUploadError::EntityTooSmall)
    );
    // The last part may be smaller.
    assert!(two_parts(5 * 1024 * 1024).is_ok());
}

#[test]
fn omitted_parts_deleted() {
    let mut op = CompleteUploadOperation::new(finalize_input());
    op.upload_parts = vec![part_record(1, 10), part_record(2, 20)];
    op.resolved_parts = vec![op.upload_parts[0].clone()];

    let turn = op.delete_records();

    let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
        panic!("expected the record delete, got {turn:?}");
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
    let mut op = CompleteUploadOperation::new(finalize_input());
    let requested = part_record(1, 10);
    let omitted = part_record(2, 20);
    let mut final_location = hashed(composed_location(Ulid::from_parts(7, 7)));
    final_location.backend = BackendRef::node_default();
    final_location.ulid = Ulid::from_parts(7, 7);
    op.final_location = Some(final_location.clone());
    op.composed_location = Some(final_location.clone());
    op.version_id = Some(Ulid::from_parts(10, 10));
    op.upload_parts = vec![requested.clone(), omitted.clone()];
    op.resolved_parts = vec![requested.clone()];

    let turn = op.write_cleanup_records(true);

    let writes = written(&turn);
    assert!(
        writes
            .iter()
            .any(|(_, key, _)| key.as_ref() == final_location.ulid.to_bytes())
    );
    let works: Vec<_> = writes
        .iter()
        .map(|(space, _, value)| {
            assert_eq!(space, BLOB_CLEANUP_KEYSPACE);
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
            owner: WriteOwner::Blob { blake3, realm_id, ttl_ms, .. },
        } if *blake3 == [7u8; 32]
            && location.get_blake3() == Some(&[7u8; 32][..])
            && *realm_id == op.input.realm_id
            && *ttl_ms == op.rocrate_limits.holder_ttl_ms
    )));
}

fn result_at(location: BackendLocation) -> CompleteUploadResult {
    CompleteUploadResult {
        location,
        version_id: Ulid::from_parts(13, 13),
        checksum_type: MultipartChecksumType::FullObject,
        response_hashes: HashMap::new(),
        part_count: 1,
    }
}

#[test]
fn finish_after_commit() {
    // The response is ready at the finalize commit; the release and housekeeping follow it.
    let input = finalize_input();
    let location = composed_location(Ulid::from_parts(11, 11));
    let mut operation = finalize_waiting(open_upload_record(&input), location.clone());
    let result = result_at(location.clone());

    let turn = operation.next(finalize_answer(Ok(result.clone())));

    let Ok(Next::Then { output, calls }) = turn else {
        panic!("expected the result with its follow-up calls, got {turn:?}")
    };
    assert_eq!(output, result);
    assert_eq!(released(&calls), Some(location.ulid));
    let timers: Vec<_> = (calls.iter())
        .filter_map(|call| match call {
            IoCall::Effect(effect) => match &**effect {
                Effect::Task(TaskEffect::ShortenTimer { key, .. }) => Some(key.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(
        timers,
        [TaskKey::PublishUsageSnapshots, TaskKey::DrainCleanupQueue]
    );
}

#[test]
fn quota_aborts_finalize() {
    // A quota refusal of the finalize reopens the record and rolls the composed object back,
    // and the refusal stays the answer.
    let input = finalize_input();
    let location = composed_location(Ulid::from_parts(11, 11));
    let mut operation = finalize_waiting(open_upload_record(&input), location.clone());
    let exceeded = CompleteUploadError::QuotaExceeded {
        limit: 30,
        usage: 35,
    };

    let turn = operation.next(finalize_answer(Err(exceeded)));

    assert_eq!(
        turn.err(),
        Some(CompleteUploadError::QuotaExceeded {
            limit: 30,
            usage: 35
        })
    );
    let calls = operation.abort();
    assert!(matches!(
        calls.as_slice(),
        [IoCall::Child(_), IoCall::Child(_)]
    ));
    assert!(cleanup_works(&calls).is_empty());
    drop(location);
}

fn marked_answer(result: Result<Marked, CompleteUploadError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

#[test]
fn unknown_mark_resets() {
    // A mark commit that may have applied leaves the record completing: it is reopened.
    let mut operation = CompleteUploadOperation::new(finalize_input());
    operation.first().unwrap();
    assert_eq!(operation.state, CompleteUploadState::MarkUpload);

    let failed = StorageError::CommitFailed;
    let turn = operation.next(marked_answer(Err(failed.clone().into())));

    assert_eq!(turn.err(), Some(failed.into()));
    assert!(matches!(operation.abort().as_slice(), [IoCall::Child(_)]));
}

#[test]
fn conflict_mark_aborts() {
    // A refused mark changed nothing, so nothing is reopened.
    let mut operation = CompleteUploadOperation::new(finalize_input());
    operation.first().unwrap();

    let conflict = StorageError::TransactionConflict;
    let turn = operation.next(marked_answer(Err(conflict.into())));

    assert!(turn.is_err());
    assert!(operation.abort().is_empty());
}

#[test]
fn committed_mark_continues() {
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input.clone());
    operation.first().unwrap();
    let record = open_upload_record(&input);

    let turn = operation.next(marked_answer(Ok((
        record,
        MultipartChecksumType::Composite,
    ))));

    assert!(matches!(
        turn,
        Ok(Next::Call(IoCall::Page { ref space, .. })) if space == UPLOAD_PART_KEYSPACE
    ));
    assert_eq!(operation.state, CompleteUploadState::ReadUploadParts);
    assert_eq!(
        operation.input.checksum_type,
        MultipartChecksumType::Composite
    );
}

#[test]
fn compose_keeps_cause() {
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input.clone());
    operation.upload_record = Some(open_upload_record(&input));
    operation.marked = true;
    operation.state = CompleteUploadState::ComposeBlob;

    let failed = || BlobError::WriteError("part limit exceeded".to_string());
    let turn = operation.next(event(Event::Blob(BlobEvent::Error(failed()))));

    assert_eq!(turn.err(), Some(failed().into()));
    assert!(matches!(operation.abort().as_slice(), [IoCall::Child(_)]));
}

fn sealed_plan() -> SealPlan {
    use aruna_core::structs::storage::encryption::BucketKeyRef;
    SealPlan {
        key: BucketKeyRef::new(Ulid::from_parts(8, 8), 1),
        public_key: [3; 32],
        cipher: Default::default(),
        block_keys: Default::default(),
        storage_generation: 2,
    }
}

fn sealed_record(input: &CompleteUploadInput) -> MultipartUpload {
    use aruna_core::structs::storage::multipart::UploadEncryption;
    let mut record = open_upload_record(input);
    record.encryption = Some(UploadEncryption {
        plan: sealed_plan(),
        compression: Compression::Off,
    });
    record
}

/// An operation of an encrypted upload whose parts were sealed, with its composed archive.
fn sealed_operation(parts: &[&[u8]]) -> (CompleteUploadOperation, BackendLocation) {
    use aruna_core::structs::storage::format::PithosLayout;
    use aruna_core::structs::storage::multipart::PartPiece;
    let input = finalize_input();
    let mut operation = CompleteUploadOperation::new(input.clone());
    operation.upload_record = Some(sealed_record(&input));
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

/// The finalize child of a sealed completion whose archive was composed.
fn sealed_child(parts: &[&[u8]]) -> (CompleteUploadOperation, BackendLocation) {
    let (mut operation, location) = sealed_operation(parts);
    operation.composed_location = Some(location.clone());
    (operation.finalize_child(), location)
}

/// A reservation the tests hand to the completion in place of the blob adapter's.
fn test_share() -> WorkingShare {
    WorkingShare::new(1 << 30, std::sync::Arc::new(()))
}

#[test]
fn sealed_reserves_first() {
    // The composition's share is reserved before any piece record loads, and composition
    // receives that same share; a saturated budget therefore stops the completion early.
    let (mut operation, _) = sealed_operation(&[b"first"]);
    let record = operation.upload_record.clone().unwrap();
    operation.compose_share = None;
    operation.input.completed_parts = vec![CompleteMultipartPart {
        part_number: 1,
        etag: None,
        expected_checksums: Vec::new(),
    }];
    operation.state = CompleteUploadState::MarkUpload;
    let turn = operation.next(marked_answer(Ok((
        record,
        MultipartChecksumType::FullObject,
    ))));
    assert_eq!(
        asked(&turn),
        &Effect::Blob(BlobEffect::ReserveCompose {
            content: MAX_PART_SIZE
        })
    );
    let share = test_share();
    let reserved = BlobEvent::ComposeReserved {
        share: share.clone(),
    };
    let turn = operation.next(event(Event::Blob(reserved)));
    assert!(matches!(
        turn,
        Ok(Next::Call(IoCall::Page { ref space, .. })) if space == UPLOAD_PART_KEYSPACE
    ));
    let turn = operation.compose_blob();
    let Effect::Blob(BlobEffect::ComposePieces { share: kept, .. }) = asked(&turn) else {
        panic!("expected a piece composition, got {turn:?}")
    };
    assert_eq!(*kept, share);
}

#[test]
fn sealed_parts_compose() {
    // Completion composes the saved pieces with the captured plan, never the bucket's.
    let (mut operation, _) = sealed_operation(&[b"first", b"second"]);
    let turn = operation.compose_blob();
    let Effect::Blob(BlobEffect::ComposePieces {
        resolved, parts, ..
    }) = asked(&turn)
    else {
        panic!("expected a piece composition, got {turn:?}")
    };
    assert_eq!(resolved.encryption, Some(sealed_plan()));
    assert_eq!(parts.len(), 2);

    let (mut operation, _) = sealed_operation(&[b"first"]);
    operation.resolved_parts[0].piece = None;
    assert_eq!(
        operation.compose_blob().err(),
        Some(CompleteUploadError::InvalidPart)
    );
}

#[test]
fn sealed_crcs_combine() {
    // Full-object CRCs come from the parts; a full-object SHA256 is never acknowledged.
    let parts: [&[u8]; 2] = [b"the first part", b"and the last"];
    let (mut operation, location) = sealed_operation(&parts);
    operation.state = CompleteUploadState::ComposeBlob;
    let turn = operation.next(composed_event(location.clone()));
    assert!(starts_child(&turn));
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
    operation.state = CompleteUploadState::ComposeBlob;
    let turn = operation.next(composed_event(location));
    assert_eq!(
        turn.err(),
        Some(CompleteUploadError::MissingExpectedChecksum("SHA256"))
    );
}

#[test]
fn pending_publishes_owner() {
    // Without a content hash the archive waits in pending_locations; its owner row and
    // version commit in the same transaction.
    let (mut child, location) = sealed_child(&[b"first"]);
    let turn = child.check_hash_lookup();
    let archive = ArchiveKey::of(&location);
    let [(space, key, _)] = written(&turn) else {
        panic!("expected the pending location, got {turn:?}")
    };
    assert_eq!(space, PENDING_LOCATION_KEYSPACE);
    assert_eq!(key.as_ref(), archive.to_bytes());
    assert!(child.new_blob);

    child.version_id = Some(Ulid::from_parts(5, 1));
    let turn = child.write_version();
    // The version commits with its journal rows in one write.
    let writes = written(&turn);
    let version = BlobVersion::from_bytes(writes[0].2.as_ref()).unwrap();
    assert!(
        writes[1..]
            .iter()
            .all(|(space, ..)| space == "event_journal")
    );
    assert_eq!(version.state.pending_archive(), Some(&archive));

    let turn = child.next(IoResult::Done);
    let version_key = VersionKey::new("bucket", "object", Ulid::from_parts(5, 1));
    let owner = CopyOwner::new(archive, version_key);
    let [(space, key, _)] = written(&turn) else {
        panic!("expected the owner row, got {turn:?}")
    };
    assert_eq!(space, COPY_OWNER_KEYSPACE);
    assert_eq!(key.as_ref(), owner.key().unwrap());
    // The stored credit is the new archive's, booked by its archive id.
    let credit = StoredDelta::for_location(&location, true).unwrap();
    assert_eq!(credit.bytes, 400);
}

#[test]
fn sealed_rotation_refused() {
    // A plan that is no longer current is never published, and its pending envelope goes
    // with the reopen.
    use aruna_core::structs::storage::encryption::EncryptionMode;
    let (mut child, location) = sealed_child(&[b"first"]);
    let settings = BucketEncryption {
        mode: EncryptionMode::NodeManaged,
        bucket_id: Some(Ulid::from_parts(8, 8)),
        key_generation: 2,
        storage_generation: 2,
        ..Default::default()
    };
    let turn = child.seal_settings_read(Some(settings.to_bytes().unwrap().into()));
    let error = turn.unwrap_err();
    assert!(matches!(error, CompleteUploadError::BucketKey(_)));

    let input = finalize_input();
    let mut operation = finalize_waiting(sealed_record(&input), location);
    operation.next(finalize_answer(Err(error))).unwrap_err();
    assert!(operation.reclaim_pending);
}

/// The answers to the envelope fence of a sealed completion; `pending` stores an epoch 1
/// envelope.
fn fence_values(
    input: &CompleteUploadInput,
    pending: bool,
    epoch: u64,
) -> (Vec<Option<Value>>, ObjectEnvelope) {
    use aruna_core::structs::storage::abe::{EnvelopePlan, create_envelope, create_parameters};
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
    let values = vec![
        row,
        Some(parameters.to_bytes().unwrap().into()),
        Some(epoch.to_be_bytes().to_vec().into()),
    ];
    (values, envelope)
}

/// A sealed finalize child after its envelope fence.
fn abe_fenced(pending: bool, epoch: u64) -> (CompleteUploadOperation, Turn) {
    let (mut child, _) = sealed_child(&[b"first"]);
    let (values, _) = fence_values(&child.input, pending, epoch);
    let turn = child.abe_fenced(values);
    (child, turn)
}

#[test]
fn stale_epoch_reclaims() {
    // A raised epoch refuses the completion; the reopen also deletes the pending envelope.
    let (_, turn) = abe_fenced(true, 2);
    let error = turn.unwrap_err();
    assert_eq!(error, BlobError::Abe(AbeError::Epoch).into());

    let input = finalize_input();
    let location = composed_location(Ulid::from_parts(9, 9));
    let mut operation = finalize_waiting(sealed_record(&input), location);
    operation.next(finalize_answer(Err(error))).unwrap_err();
    assert!(operation.reclaim_pending);

    let mut record = sealed_record(&input);
    record.completing_since_ms = Some(TEST_NOW_MS);
    let mut reopen = ReopenUpload {
        input: input.clone(),
        plan: Some(sealed_plan()),
        reclaim_pending: true,
        recheck_pending: false,
        step: ReopenStep::ReadRecord,
    };
    reopen.first().unwrap();
    reopen
        .next(values(vec![Some(record.to_bytes().unwrap().into())]))
        .unwrap();
    let turn = reopen.next(IoResult::Done);
    let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
        panic!("expected the pending envelope delete, got {turn:?}")
    };
    assert_eq!(deletes[0].0, ABE_PENDING_KEYSPACE);
    assert_eq!(deletes[0].1.as_ref(), input.upload_id.to_bytes());
    assert_eq!(reopen.next(IoResult::Done), Ok(Next::Done(())));
}

/// Reopens after a refused commit of a published envelope and answers the recheck with
/// `epoch`.
fn conflict_recheck(epoch: u64) -> Reopened {
    use aruna_core::structs::storage::encryption::EncryptionMode;
    let input = finalize_input();
    let location = composed_location(Ulid::from_parts(9, 9));
    let mut operation = finalize_waiting(sealed_record(&input), location);
    let conflict = StorageError::TransactionConflict;
    operation
        .next(finalize_answer(Err(conflict.into())))
        .unwrap_err();
    assert!(operation.recheck_pending);

    let mut record = sealed_record(&input);
    record.completing_since_ms = Some(TEST_NOW_MS);
    let mut reopen = ReopenUpload {
        input: input.clone(),
        plan: Some(sealed_plan()),
        reclaim_pending: false,
        recheck_pending: true,
        step: ReopenStep::ReadRecord,
    };
    reopen.first().unwrap();
    reopen
        .next(values(vec![Some(record.to_bytes().unwrap().into())]))
        .unwrap();
    let turn = reopen.next(IoResult::Done);
    let Ok(Next::Call(IoCall::Read(reads))) = turn else {
        panic!("expected the pending recheck, got {turn:?}")
    };
    assert_eq!(reads.len(), 4);
    let settings = BucketEncryption {
        mode: EncryptionMode::NodeManaged,
        bucket_id: Some(Ulid::from_parts(8, 8)),
        key_generation: 1,
        storage_generation: 2,
        ..Default::default()
    };
    let (fence, _) = fence_values(&input, true, epoch);
    let mut answers = vec![Some(settings.to_bytes().unwrap().into())];
    answers.extend(fence);
    reopen.next(values(answers))
}

#[test]
fn conflict_reclaims_stale() {
    // An epoch raised between the fence read and the commit drops the pending envelope.
    let turn = conflict_recheck(2);
    assert!(
        matches!(turn, Ok(Next::Call(IoCall::Delete(ref deletes))) if deletes[0].0 == ABE_PENDING_KEYSPACE),
        "{turn:?}"
    );
    // An unrelated conflict keeps it, so the upload stays retryable.
    assert_eq!(conflict_recheck(1), Ok(Next::Done(())));
}

#[test]
fn bucket_only_publishes() {
    // A generation without admitted parameters completes without an envelope.
    let (mut child, _) = sealed_child(&[b"first"]);
    let turn = child.abe_fenced(vec![None, None, None]);
    assert!(turn.is_ok(), "{turn:?}");
    assert!(child.envelope.is_none());
}

#[test]
fn missing_envelope_refused() {
    // An upload of a generation with admitted parameters never publishes without its envelope.
    let (_, turn) = abe_fenced(false, 1);
    let error = turn.unwrap_err();
    assert_eq!(error, BlobError::Abe(AbeError::Stale).into());

    let input = finalize_input();
    let location = composed_location(Ulid::from_parts(9, 9));
    let mut operation = finalize_waiting(sealed_record(&input), location);
    operation.next(finalize_answer(Err(error))).unwrap_err();
    assert!(!operation.reclaim_pending);
}

/// Publishes the fenced pending envelope under a metadata limit, returning the step's turn.
fn publish_envelope(metadata_bytes: u64) -> (CompleteUploadOperation, Turn) {
    let (mut child, turn) = abe_fenced(true, 1);
    turn.unwrap();
    child.rocrate_limits = RoCrateLimits {
        metadata_bytes,
        ..Default::default()
    };
    child.final_location = child.composed_location.clone();
    child.version_id = Some(Ulid::from_parts(5, 1));
    child.state = CompleteUploadState::WriteVersionRecord;
    let turn = child.next(IoResult::Done);
    (child, turn)
}

#[test]
fn envelope_publishes_charged() {
    // A current envelope is published with the version and charged to the group.
    let (mut child, turn) = publish_envelope(u64::MAX);
    let spaces: Vec<_> = written(&turn)
        .iter()
        .map(|(space, ..)| space.as_str())
        .collect();
    assert_eq!(
        spaces,
        [
            aruna_core::keyspaces::ABE_ENVELOPE_KEYSPACE,
            aruna_core::keyspaces::ABE_VERSION_KEYSPACE,
            aruna_core::keyspaces::ABE_ARCHIVE_KEYSPACE,
        ]
    );
    assert!(child.envelope_bytes > 0);
    let turn = child.next(IoResult::Done);
    assert_eq!(written(&turn)[0].0, COPY_OWNER_KEYSPACE);
}

#[test]
fn envelope_limit_boundary() {
    // The metadata limit and the charge count a pending mapping at its promoted size.
    use aruna_core::structs::storage::abe::{EnvelopeArchive, envelope_charge};
    let (child, turn) = publish_envelope(u64::MAX);
    let writes = written(&turn);
    let (envelope, id, mapping) = (&writes[0].2, &writes[0].1, &writes[2].2);
    let location = child.final_location.as_ref().unwrap();
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
    assert_eq!(child.envelope_bytes, envelope_charge(envelope, &promoted));
    assert_eq!(child.envelope_bytes, envelope_charge(envelope, mapping));
    let metadata = &child.upload_record.as_ref().unwrap().metadata;
    let metadata = postcard::to_allocvec(metadata).unwrap();
    let limit = child.envelope_bytes + (id.len() + metadata.len()) as u64;
    let (_, turn) = publish_envelope(limit);
    written(&turn);
    let (_, turn) = publish_envelope(limit - 1);
    assert_eq!(turn.err(), Some(BlobError::Abe(AbeError::Limit).into()));
}

#[test]
fn ceiling_covers_promotion() {
    // A completion exactly at the ceiling has paid for the mapping its keyed read promotes.
    use aruna_core::structs::storage::usage::UsageCounters;
    for (slack, exceeded) in [(0, false), (1, true)] {
        let (mut child, _) = publish_envelope(u64::MAX);
        let size = child.final_location.as_ref().unwrap().blob_size;
        let used = 7;
        child.input.quota_ceiling = Some(used + size + child.envelope_bytes - slack);
        child.state = CompleteUploadState::WriteReplicationObligation;
        let turn = child.next(IoResult::Done);
        assert!(
            matches!(child.state, CompleteUploadState::EnforceQuota),
            "{turn:?}"
        );
        // The rebuild marker, the realm config, the group counters, then the remote scan.
        child.next(values(vec![None])).unwrap();
        child.next(values(vec![None])).unwrap();
        let counters = UsageCounters {
            logical_bytes: used,
            ..Default::default()
        };
        child
            .next(values(vec![Some(counters.to_bytes().unwrap().into())]))
            .unwrap();
        let turn = child.next(IoResult::Page {
            rows: Vec::new(),
            next: None,
        });
        let refused = matches!(turn, Err(CompleteUploadError::QuotaExceeded { .. }));
        assert_eq!(refused, exceeded, "slack {slack}: {turn:?}");
    }
}

#[test]
fn plain_completion_refused() {
    // A plain upload never publishes once its bucket encrypts, even if its record predates it.
    use aruna_core::structs::storage::encryption::EncryptionMode;
    let input = finalize_input();
    let composed = composed_location(Ulid::from_parts(9, 9));
    let mut child = finalizing(open_upload_record(&input), composed);
    let settings = BucketEncryption {
        mode: EncryptionMode::NodeManaged,
        bucket_id: Some(Ulid::from_parts(8, 8)),
        key_generation: 1,
        ..Default::default()
    };

    let turn = child.seal_settings_read(Some(settings.to_bytes().unwrap().into()));

    assert!(matches!(turn, Err(CompleteUploadError::BucketKey(_))));
}

#[test]
fn pending_schedules_promotion() {
    // A pending completion during an unlock session asks for promotion at once; the timer is
    // persisted, so it also runs after a restart. A known hash needs none.
    let (mut operation, location) = sealed_operation(&[b"first"]);
    let key = location.format.bucket_key().unwrap();
    let promote = IoCall::from(Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::PromotePending {
            bucket_id: key.bucket_id,
            generation: key.generation,
        },
        after: Duration::ZERO,
    }));
    let Next::Then { calls, .. } = operation.finished(result_at(location.clone())) else {
        panic!("expected the follow-up calls")
    };
    assert!(calls.contains(&promote));

    let (mut operation, _) = sealed_operation(&[b"first"]);
    let Next::Then { calls, .. } = operation.finished(result_at(hashed(location))) else {
        panic!("expected the follow-up calls")
    };
    assert!(!calls.contains(&promote));
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
    let rows = part_values(upload_id, parts);
    let (first, second) = rows.split_at(PART_PAGE);
    let next = first.last().unwrap().0.clone();
    operation.state = CompleteUploadState::ReadUploadParts;
    let turn = operation.next(IoResult::Page {
        rows: first.to_vec(),
        next: Some(next.clone()),
    });
    assert!(matches!(
        turn,
        Ok(Next::Call(IoCall::Page { after: Some(ref after), limit, .. }))
            if *after == next && limit == PART_PAGE
    ));
    // Omitted parts already read hold no piece record while the next page loads.
    assert!(
        operation
            .upload_parts
            .iter()
            .all(|part| part.piece.is_none())
    );
    assert_eq!(operation.selected_parts.len(), 0);

    operation
        .next(IoResult::Page {
            rows: second.to_vec(),
            next: None,
        })
        .unwrap();
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
    operation.read_parts(None).unwrap();
    let limit = aruna_blob::blob::pithos::MAX_SIZE;
    let mut turn = Err(CompleteUploadError::InvalidOperationState);
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
        let rows = part_values(operation.input.upload_id, parts);
        let next = rows.last().map(|(key, _)| key.clone());
        turn = operation.next(IoResult::Page { rows, next });
        if !matches!(turn, Ok(Next::Call(IoCall::Page { .. }))) {
            break;
        }
    }
    assert_eq!(
        turn.err(),
        Some(BlobError::SizeLimitExceeded { limit }.into())
    );
    assert!(operation.selected_bytes <= limit);
    assert!(operation.resolved_parts.is_empty());
}

#[test]
fn share_outlives_compose() {
    // The completion keeps its reservation after composition starts, while the finalize child
    // holds the selected piece records.
    let (mut operation, location) = sealed_operation(&[b"first"]);
    let share = operation.compose_share.clone().unwrap();
    let turn = operation.compose_blob();
    let Effect::Blob(BlobEffect::ComposePieces { share: sent, .. }) = asked(&turn) else {
        panic!("expected a piece composition, got {turn:?}")
    };
    assert_eq!(*sent, share);
    operation.composed_location = Some(location);
    let child = operation.finalize_child();
    assert!(child.resolved_parts[0].piece.is_some());
    assert_eq!(operation.compose_share, Some(share));
}

/// The placement gate runs as a child before the compose and is rechecked in the finalize.
mod decisions {
    use super::*;
    use crate::placement::policy::PolicyCacheStats;
    use aruna_core::structs::placement::policy::{
        PlacementDecision, PlacementPolicy, PlacementSelector, PlacementSubject, VerifiedPolicy,
    };
    use std::collections::BTreeMap;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[9u8; 32]).public()
    }

    fn policy(location: &str) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([1u8; 16]),
            "residency".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some(location.to_string()),
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn gate(location: &str) -> GateContext {
        GateContext {
            realm_id: finalize_input().realm_id,
            subject: PlacementSubject {
                node_id: node(),
                generation: 1,
                location: location.to_string(),
                labels: BTreeMap::new(),
                executor_kind: None,
                local_to_controller: true,
            },
            now_ms: 1_000,
            admitting: true,
        }
    }

    fn bucket(refs: Vec<PlacementPolicyRef>, generation: u64) -> Option<Value> {
        let info = BucketInfo {
            group_id: Ulid::from_bytes([2u8; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: refs,
            placement_policy_generation: generation,
            compression: Compression::Off,
            distribution: None,
        };
        Some(info.to_bytes().expect("bucket encodes").into())
    }

    /// `location` of `None` leaves the node without a subject, which fails every governed
    /// completion closed.
    fn at_gate(location: Option<&str>) -> CompleteUploadOperation {
        let input = finalize_input();
        let mut record = open_upload_record(&input);
        record.status = MultipartUploadStatus::Open;
        let mut operation = CompleteUploadOperation::new(input);
        if let Some(location) = location {
            operation = operation.with_gate(gate(location));
        }
        operation.upload_record = Some(record);
        operation.state = CompleteUploadState::ReadGateBucket;
        operation
    }

    fn decided(decision: PlacementDecision) -> IoResult {
        let outcome = PolicyGateOutcome {
            decision,
            foreign_owner: None,
            stats: PolicyCacheStats::default(),
        };
        IoResult::Child(ChildOutcome::new(Ok::<_, ReadPolicyError>(outcome)))
    }

    #[test]
    fn denies_before_compose() {
        // The rule admits another location, so the gate runs before any part is composed and
        // its refusal ends the completion.
        let rule = policy("us-east");
        let mut operation = at_gate(Some("eu-west"));
        let turn = operation.next(values(vec![bucket(vec![rule.policy_ref()], 1)]));
        assert!(starts_child(&turn));
        assert_eq!(operation.state, CompleteUploadState::PolicyGate);

        let policy_ids = vec![rule.policy().policy_id];
        let denied = PlacementDecision::Denied {
            policy_ids: policy_ids.clone(),
        };
        let turn = operation.next(decided(denied));

        assert_eq!(
            turn.err(),
            Some(PolicyGateError::Denied { policy_ids }.into())
        );
    }

    #[test]
    fn missing_policy_blocks() {
        // A rule that cannot be obtained blocks; it is never read as a grant.
        let rule = policy("eu-west");
        let mut operation = at_gate(Some("eu-west"));
        operation
            .next(values(vec![bucket(vec![rule.policy_ref()], 1)]))
            .unwrap();
        let unavailable = PlacementDecision::Unavailable {
            policy_ids: vec![rule.policy().policy_id],
        };

        let turn = operation.next(decided(unavailable));

        assert!(matches!(
            turn,
            Err(CompleteUploadError::PolicyGate(
                PolicyGateError::Unavailable { .. }
            ))
        ));
    }

    #[test]
    fn missing_subject_blocks() {
        let mut operation = at_gate(None);
        let refs = vec![PlacementPolicyRef {
            policy_id: Ulid::from_bytes([1u8; 16]),
            digest: [4u8; 32],
        }];

        let turn = operation.next(values(vec![bucket(refs, 1)]));

        assert_eq!(turn.err(), Some(PolicyGateError::NoSubject.into()));
    }

    #[test]
    fn ungoverned_composes() {
        // An object with no refs reaches the compose with no policy round trip.
        let mut operation = at_gate(Some("eu-west"));
        let turn = operation.next(values(vec![bucket(Vec::new(), 0)]));
        assert!(matches!(
            asked(&turn),
            Effect::Blob(BlobEffect::Compose { .. })
        ));

        // An allowed object composes after the gate.
        let rule = policy("eu-west");
        let mut operation = at_gate(Some("eu-west"));
        operation
            .next(values(vec![bucket(vec![rule.policy_ref()], 1)]))
            .unwrap();
        let turn = operation.next(decided(PlacementDecision::Allowed));
        assert!(matches!(
            asked(&turn),
            Effect::Blob(BlobEffect::Compose { .. })
        ));
    }

    #[test]
    fn drift_aborts_finalize() {
        // The default changed while the parts composed, so the object must not commit refs
        // nothing evaluated.
        let mut operation = at_gate(Some("eu-west"));
        operation.next(values(vec![bucket(Vec::new(), 0)])).unwrap();
        operation.composed_location = Some(hashed(composed_location(Ulid::from_parts(9, 9))));
        let mut child = operation.finalize_child();
        child.state = CompleteUploadState::ReadBucketDefault;

        let turn = child.next(values(vec![
            bucket(vec![policy("us-east").policy_ref()], 1),
            None,
        ]));

        assert_eq!(turn.err(), Some(PolicyGateError::Drift.into()));
    }
}
