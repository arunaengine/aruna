//! Tests PutObject as calls and results: gates, the version transaction child, its conflict
//! retries, the cleanup a failure leaves to `abort`, and per backend dedup.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use aruna_blob::blob::BlobHandler;
use aruna_blob::blob::{BackendRegistry, NodeBackend};
use aruna_core::calls::ChildOutcome;
use aruna_core::effects::StorageEffect;
use aruna_core::egress::EgressPolicy;
use aruna_core::keyspaces::{DELETE_TRAIL_KEYSPACE, DHT_KEYSPACE};
use aruna_core::structs::checksum::{ChecksumAlgorithm, HASH_BLAKE3};
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BackendRef, HashIndex};
use aruna_core::structs::storage::format::{Compression, StoredFormat};
use aruna_core::structs::storage::routing::{BackendCatalog, NodeRoutingRule, RoutingTarget};
use aruna_net::dht::storage::decode_entries;
use aruna_net::{NetConfig, NetHandle};
use aruna_storage::storage;
use std::fs::{exists, read_to_string};
use std::path::Path;
use tempfile::tempdir;

fn count_files(path: &Path) -> usize {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| if path.is_dir() { count_files(&path) } else { 1 })
        .sum()
}

async fn read_value(context: &DriverContext, key_space: &str, key: Vec<u8>) -> Option<Value> {
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

fn test_location(created_by: UserId) -> BackendLocation {
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "path".to_string(),
        ulid: Ulid::generate(),
        format: StoredFormat::default(),
        created_by,
        created_at: std::time::SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 1,
        hashes: HashMap::new(),
    }
}

fn hashed(mut location: BackendLocation) -> BackendLocation {
    location
        .hashes
        .insert(HASH_BLAKE3.to_string(), vec![7u8; 32]);
    location
}

pub(super) fn put_config(realm_id: RealmId, group_id: Ulid, node_id: NodeId) -> PutObjectConfig {
    PutObjectConfig {
        user_id: UserId::local(Ulid::generate(), realm_id),
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

fn config() -> PutObjectConfig {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    put_config(
        realm_id,
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    )
}

pub(super) fn values(values: Vec<Option<Value>>) -> IoResult {
    IoResult::Values(values)
}

pub(super) fn event(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

/// The version transaction's answer to its parent.
pub(super) fn published(result: Result<PutObjectResult, PutObjectError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

pub(super) fn read_spaces(turn: &Turn) -> Vec<&str> {
    match turn {
        Ok(Next::Call(IoCall::Read(reads))) => reads.iter().map(|(space, _)| &space[..]).collect(),
        other => panic!("expected a read, got {other:?}"),
    }
}

pub(super) fn written(turn: &Turn) -> &[(String, Key, Value)] {
    match turn {
        Ok(Next::Call(IoCall::Write(rows))) => rows,
        other => panic!("expected a write, got {other:?}"),
    }
}

pub(super) fn asked(turn: &Turn) -> &Effect {
    match turn {
        Ok(Next::Call(IoCall::Effect(effect))) => effect,
        other => panic!("expected an effect, got {other:?}"),
    }
}

/// The cleanup rows among `calls`, in order.
pub(super) fn cleanup_works(calls: &[IoCall]) -> Vec<BlobCleanupWork> {
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

/// `calls` delete the written blob through a rollback child, and nothing else.
fn assert_rolls_back(calls: &[IoCall], location: &BackendLocation) {
    let rollback = IoCall::child(RollbackWrite::new(location.clone()));
    assert_eq!(calls, [rollback]);
}

/// The version transaction of a put that wrote `location`.
pub(super) fn publisher(
    mut operation: PutObjectOperation,
    location: BackendLocation,
) -> PutObjectOperation {
    operation.written_location = Some(location);
    operation.publish_child()
}

/// A put waiting for its version transaction after writing `location`.
fn waiting(location: BackendLocation) -> PutObjectOperation {
    let mut operation = PutObjectOperation::new(config());
    operation.written_location = Some(location);
    operation.version_id = Some(Ulid::generate());
    operation.state = PutObjectState::Publish;
    operation
}

fn bucket(group_id: Ulid, created_by: UserId, compression: Compression) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression,
        distribution: None,
    }
}

/// Runs the version transaction up to its bucket read and answers it with `bucket`.
fn bucket_answer(child: &mut PutObjectOperation, bucket: &BucketInfo) -> Turn {
    assert_eq!(child.transaction(), TransactionMode::Write);
    assert_eq!(read_spaces(&child.first()), [PURGE_FENCE_KEYSPACE]);
    let turn = child.next(values(vec![None]));
    assert_eq!(
        read_spaces(&turn),
        [S3_BUCKET_KEYSPACE, NODE_SUBJECT_KEYSPACE]
    );
    child.next(values(vec![Some(bucket.to_bytes().unwrap().into()), None]))
}

#[test]
fn guard_allows_edit() {
    // A routing or CORS edit is prospective policy, not a different bucket: it must not
    // discard a write that already landed.
    let config = config();
    let expected = bucket(config.group_id, config.user_id, Compression::Off);
    let edited = BucketInfo {
        cors_configuration: Some(
            aruna_core::structs::storage::blob::BucketCorsConfiguration { rules: Vec::new() },
        ),
        ..expected.clone()
    };
    let created_by = config.user_id;
    let operation = PutObjectOperation::new(config).with_bucket_guard(expected);
    let mut child = publisher(operation, test_location(created_by));

    let turn = bucket_answer(&mut child, &edited);
    assert_eq!(read_spaces(&turn), [BUCKET_ENCRYPTION_KEYSPACE]);

    // Past the guard the minimal fixture fails at the hash step; a recreate would have
    // ended with a transaction conflict instead.
    let turn = child.next(values(vec![None]));
    assert!(
        matches!(turn, Err(PutObjectError::MissingHash(_))),
        "expected the write to pass the guard, got {turn:?}"
    );
}

#[test]
fn stale_encoding_rejected() {
    // The setting changed while the bytes streamed: the raw copy must not become a version a
    // finished migration would never revisit.
    let config = config();
    let current = bucket(
        config.group_id,
        config.user_id,
        Compression::Zstd { level: 3 },
    );
    let created_by = config.user_id;
    let mut child = publisher(PutObjectOperation::new(config), test_location(created_by));

    let turn = bucket_answer(&mut child, &current);

    assert_eq!(turn.err(), Some(StorageError::TransactionConflict.into()));
}

#[test]
fn recreate_rejected() {
    let config = config();
    let expected = bucket(config.group_id, config.user_id, Compression::Off);
    let recreated = BucketInfo {
        created_at: std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1),
        ..expected.clone()
    };
    let created_by = config.user_id;
    let operation = PutObjectOperation::new(config).with_bucket_guard(expected);
    let mut child = publisher(operation, test_location(created_by));

    let turn = bucket_answer(&mut child, &recreated);

    assert_eq!(turn.err(), Some(StorageError::TransactionConflict.into()));
    // The version transaction leaves the cleanup to its parent.
    assert!(child.abort().is_empty());
}

#[test]
fn rejects_write_error() {
    // A rejected body stream (e.g. trailer checksum mismatch) surfaces WriteFailed.
    let mut operation = PutObjectOperation::new(config());
    operation.state = PutObjectState::WriteBlob;

    let failed = BlobError::StreamFailed("checksum mismatch".to_string());
    let turn = operation.next(event(Event::Blob(BlobEvent::Error(failed))));

    assert!(matches!(turn, Err(PutObjectError::WriteFailed(_))));
    assert!(operation.abort().is_empty());
}

#[test]
fn rejects_server_write() {
    // A full or flapping disk must never be reported as a client bad digest.
    let mut operation = PutObjectOperation::new(config());
    operation.state = PutObjectState::WriteBlob;

    let failed = BlobError::WriteError("No space left on device".to_string());
    let turn = operation.next(event(Event::Blob(BlobEvent::Error(failed))));

    assert!(matches!(turn, Err(PutObjectError::BlobWriteFailed(_))));
}

#[test]
fn quota_error_cleans() {
    // A quota or counter failure in the version transaction rolls the written blob back.
    let config = config();
    let (group_id, node_id) = (config.group_id, config.node_id);
    let location = test_location(config.user_id);
    let mut child = publisher(PutObjectOperation::new(config), location.clone());
    child.quota_gate = Some(QuotaGate::new(1, 1, group_id, node_id));
    let turn = child.handle_enforce_quota(IoResult::Done);
    let error = turn.unwrap_err();
    assert!(matches!(error, PutObjectError::QuotaGateError(_)));

    child.usage_update = Some(UsageCounterUpdate::for_group(
        group_id,
        UsageDelta::default(),
    ));
    child.usage_update.as_mut().unwrap().start_calls();
    let turn = child.handle_usage_update(values(vec![None]));
    assert!(matches!(turn, Err(PutObjectError::UsageUpdateError(_))));

    let mut operation = waiting(location.clone());
    let turn = operation.next(published(Err(error)));
    assert!(matches!(turn, Err(PutObjectError::QuotaGateError(_))));
    assert_rolls_back(&operation.abort(), &location);
}

#[test]
fn retries_commit_conflict() {
    // Concurrent writes contend on the usage counters; the written blob stays put and only
    // the version transaction runs again, with the same version id.
    let location = test_location(UserId::default());
    let mut operation = waiting(location.clone());
    let version_id = operation.version_id;

    let conflict = StorageError::TransactionConflict;
    let turn = operation.next(published(Err(conflict.into())));

    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.state, PutObjectState::Publish);
    assert_eq!(operation.written_location, Some(location.clone()));
    assert_eq!(operation.version_id, version_id);
    let child = operation.publish_child();
    assert_eq!(child.version_id, version_id);
    assert_eq!(child.written_location, Some(location.clone()));

    let result = PutObjectResult {
        location,
        version_id: version_id.unwrap(),
        replayed: false,
    };
    let turn = operation.next(published(Ok(result.clone())));
    assert_eq!(turn, Ok(Next::Done(result)));
}

#[test]
fn conflict_exhausts_retries() {
    let location = test_location(UserId::default());
    let mut operation = waiting(location.clone());
    let conflict = || published(Err(StorageError::TransactionConflict.into()));
    for _ in 0..CONFLICT_RETRIES {
        let turn = operation.next(conflict());
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    }

    let turn = operation.next(conflict());

    assert_eq!(turn.err(), Some(StorageError::TransactionConflict.into()));
    assert_rolls_back(&operation.abort(), &location);
}

#[test]
fn fence_refuses_replay() {
    // Two pulls of one device version race: the loser's commit conflicts, its rerun reads the
    // winner's receipt and drops its own bytes instead of minting a second version.
    let receipt: Key = b"receipt".to_vec().into();
    let space = "receipts".to_string();
    let operation = PutObjectOperation::new(config()).with_fence(
        vec![(space.clone(), receipt.clone(), None)],
        vec![(space, receipt)],
    );
    let location = hashed(test_location(UserId::default()));
    let mut parent = operation;
    parent.written_location = Some(location.clone());
    let mut child = parent.publish_child();
    let version_id = child.version_id.unwrap();

    // Both transactions find the receipt free, so both record their version id.
    let turn = child.write_cleanup_row();
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let turn = child.next(values(vec![None]));
    assert_eq!(written(&turn)[0].2.as_ref(), version_id.to_bytes());
    // The reconcile row follows in the same transaction.
    let turn = child.next(IoResult::Done);
    assert_eq!(written(&turn)[0].0, BLOB_CLEANUP_KEYSPACE);

    // The rerun reads the guard again and finds the other pull's receipt.
    let mut rerun = parent.publish_child();
    rerun.write_cleanup_row().unwrap();
    let winner: Value = Ulid::generate().to_bytes().to_vec().into();
    let turn = rerun.next(values(vec![Some(winner)]));
    assert_eq!(turn.err(), Some(PutObjectError::FenceTaken));

    parent.state = PutObjectState::Publish;
    parent
        .next(published(Err(PutObjectError::FenceTaken)))
        .unwrap_err();
    assert_rolls_back(&parent.abort(), &location);
}

#[test]
fn writes_before_commit() {
    // The reconciliation row commits atomically with metadata ownership.
    let location = hashed(test_location(UserId::default()));
    let mut child = publisher(PutObjectOperation::new(config()), location.clone());

    let turn = child.write_cleanup_row();

    let [(space, key, value)] = written(&turn) else {
        panic!("expected a reconciliation row, got {turn:?}")
    };
    assert_eq!(space, BLOB_CLEANUP_KEYSPACE);
    assert_eq!(key.as_ref(), location.ulid.to_bytes());
    assert!(matches!(
        BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        BlobCleanupWork::ReconcileWrite {
            location: observed,
            owner: WriteOwner::Blob { blake3, .. },
        } if blake3 == [7u8; 32] && observed == location
    ));
}

#[test]
fn release_after_commit() {
    // The reservation is released, the bytes announced and the snapshot published after the
    // version commits.
    let location = hashed(test_location(UserId::default()));
    let mut child = publisher(PutObjectOperation::new(config()), location.clone());
    child.output = Some(location.clone());

    let Ok(Next::Then { output, calls }) = child.finish_publish() else {
        panic!("expected the result with its follow-up calls")
    };

    assert_eq!(output.location, location);
    let release = IoCall::from(Effect::Blob(BlobEffect::ReleaseReservation {
        id: location.ulid,
    }));
    assert_eq!(calls.first(), Some(&release));
    assert!(calls.contains(&IoCall::from(schedule_snapshot_publish())));
    assert!(calls.iter().any(|call| matches!(
        call,
        IoCall::Effect(effect) if matches!(**effect, Effect::Net(_))
    )));
}

#[test]
fn unknown_keeps_blob() {
    // Only a proven refusal rolls the blob back; any other commit failure may already own
    // the version, so the copy goes to reconciliation and its reservation is released.
    for error in [
        StorageError::CommitFailed,
        StorageError::PersistError("journal".to_string()),
        StorageError::Timeout,
    ] {
        let location = hashed(test_location(UserId::default()));
        let mut operation = waiting(location.clone());
        let realm_id = operation.config.realm_id;

        let turn = operation.next(published(Err(error.clone().into())));

        assert_eq!(turn.err(), Some(error.into()));
        let calls = operation.abort();
        assert_eq!(
            cleanup_works(&calls),
            [BlobCleanupWork::ReconcileWrite {
                location: location.clone(),
                owner: WriteOwner::Blob {
                    blake3: [7u8; 32],
                    realm_id,
                    ttl_ms: RoCrateLimits::default().holder_ttl_ms,
                    announce: true,
                },
            }]
        );
        let release = IoCall::from(Effect::Blob(BlobEffect::ReleaseReservation {
            id: location.ulid,
        }));
        assert_eq!(calls.last(), Some(&release));
    }
}

#[test]
fn cut_publish_reconciles() {
    // A run that ends while its version transaction runs may have committed: the bytes go to
    // reconciliation, without a release.
    let location = hashed(test_location(UserId::default()));
    let mut operation = waiting(location.clone());

    let calls = operation.abort();

    assert!(matches!(
        cleanup_works(&calls).as_slice(),
        [BlobCleanupWork::ReconcileWrite { location: kept, .. }] if *kept == location
    ));
    assert_eq!(calls.len(), 1);
}

