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
use aruna_net::NetHandle;
use aruna_net::dht::storage::decode_entries;
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
    // The release runs as a child that keeps the reconcile row when the release is refused.
    let realm_id = child.config.realm_id;
    let reconcile = BlobCleanupWork::ReconcileWrite {
        location: location.clone(),
        owner: WriteOwner::Blob {
            blake3: [7u8; 32],
            realm_id,
            ttl_ms: RoCrateLimits::default().holder_ttl_ms,
            announce: true,
        },
    };
    let release = IoCall::child(ReleaseWrite::new(location.ulid, Some(reconcile)));
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

#[tokio::test]
pub async fn test_put_object() {
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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
                EncodingClass::Raw,
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
            key_space: PATHS_INDEX_KEYSPACE.to_string(),
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
    assert_eq!(retry.location, result.location);
    assert_eq!(retry.version_id, result.version_id);
    // Only the first put committed; the retry hands the same version back.
    assert!(!result.replayed && retry.replayed);
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

    // A create-only write replays its own version but never replaces a live head.
    let create = |version_id| {
        let body = tokio_util::io::ReaderStream::new(&retry_data[..]);
        PutObjectOperation::new(PutObjectConfig {
            user_id,
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: "some-file.txt".to_string(),
                content_length: Some(retry_data.len() as u64),
                body: Some(BackendStream::new(body)),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: Some(version_id),
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        })
        .create_only()
    };
    let replay = drive(create(preassigned_version_id), &context)
        .await
        .unwrap();
    assert_eq!(replay.location, result.location);
    assert_eq!(replay.version_id, result.version_id);
    assert!(replay.replayed);
    let other = drive(create(Ulid::generate()), &context).await;
    assert!(matches!(other, Err(PutObjectError::ObjectExists)));
}

#[tokio::test]
pub async fn deduplicates_blob() {
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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

    let location_key = BlobLocationKey::new(
        blob_hash,
        EncodingClass::Raw,
        first.location.backend.clone(),
    )
    .to_bytes();
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
            PATHS_INDEX_KEYSPACE,
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
    let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
        None,
    )
    .await
    .unwrap();

    (
        DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
async fn release_row_committed() {
    // A version tagged as a release commits its release row; an untagged one writes none.
    use aruna_core::keyspaces::BUCKET_RELEASE_KEYSPACE;
    use aruna_core::structs::storage::bucket_metadata::{
        RELEASE_PREFIX_TAG, RELEASE_TAG, ReleasePointer, release_key, release_marker,
    };
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let (context, _, _) = setup_two_backends(temp_root).await;
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let tags = HashMap::from([
        (RELEASE_TAG.to_string(), "6.0".to_string()),
        (RELEASE_PREFIX_TAG.to_string(), "db/".to_string()),
    ]);
    let put = |key: &str, metadata: HashMap<String, String>| {
        PutObjectOperation::new(PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
            group_id,
            realm_id,
            node_id: context.net_handle.as_ref().unwrap().node_id(),
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: key.to_string(),
                content_length: Some(2),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                    b"{}".as_slice(),
                ))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: archive_routing(group_id),
        })
        .with_metadata(metadata)
    };
    let tagged = drive(put("db/release.json", tags), &context).await.unwrap();
    let plain = drive(put("db/other.json", HashMap::new()), &context)
        .await
        .unwrap();
    let row = read_value(
        &context,
        BUCKET_RELEASE_KEYSPACE,
        release_key("mybucket", tagged.version_id),
    )
    .await
    .expect("the release row commits with the version");
    let pointer = ReleasePointer::from_bytes(row.as_ref()).unwrap();
    assert_eq!(pointer.key, "db/release.json");
    // The bucket's release marker names the newest release crate version.
    let marker = read_value(
        &context,
        BUCKET_RELEASE_KEYSPACE,
        release_marker("mybucket"),
    )
    .await;
    assert_eq!(
        marker.as_deref(),
        Some(tagged.version_id.to_bytes().as_slice())
    );
    assert_eq!(
        (pointer.label.as_str(), pointer.prefix.as_str()),
        ("6.0", "db/")
    );
    let untagged = read_value(
        &context,
        BUCKET_RELEASE_KEYSPACE,
        release_key("mybucket", plain.version_id),
    )
    .await;
    assert!(untagged.is_none());
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
            EncodingClass::Raw,
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

#[tokio::test]
pub async fn overwrite_retains_index() {
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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
        PATHS_INDEX_KEYSPACE,
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
        PATHS_INDEX_KEYSPACE,
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
    let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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

/// A version transaction with `output` as the location it publishes.
fn publishing(config: PutObjectConfig, output: BackendLocation) -> PutObjectOperation {
    let mut child = publisher(PutObjectOperation::new(config), output.clone());
    child.output = Some(output);
    child
}

#[test]
fn generation_increments() {
    let config = put_config(
        RealmId::from_bytes([1u8; 32]),
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    );
    let output = hashed(test_location(config.user_id));
    let mut child = publishing(config, output);
    let version_id = child.version_id.unwrap();
    let existing = CurrentVersionPointer::new_with_generation(Ulid::generate(), 4);

    let turn = child.object_lookup_read(Some(existing.to_bytes().unwrap().into()));
    assert_eq!(read_spaces(&turn), [BLOB_VERSIONS_KEYSPACE]);

    let turn = child.liveness_read(None);
    // The location row commits with the head.
    let [(location_space, ..), (head_space, _, head)] = written(&turn) else {
        panic!("expected the location and head rows, got {turn:?}")
    };
    assert_eq!(location_space, BLOB_LOCATIONS_KEYSPACE);
    assert_eq!(head_space, BLOB_HEAD_KEYSPACE);
    assert_eq!(
        CurrentVersionPointer::from_bytes(head.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(version_id, 5)
    );
}

#[test]
fn dedup_keeps_writer() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let (first, writer) = (
        UserId::local(Ulid::generate(), realm_id),
        UserId::local(Ulid::generate(), realm_id),
    );
    let mut config = put_config(
        realm_id,
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    );
    config.user_id = writer;
    // The deduplicated location keeps the earlier copy, written by someone else.
    let mut child = publisher(
        PutObjectOperation::new(config),
        hashed(test_location(writer)),
    );
    child.output = Some(hashed(test_location(first)));
    child.state = PutObjectState::WritePathIndex;

    let turn = child.next(IoResult::Done);

    let writes = written(&turn);
    let version = BlobVersion::from_bytes(&writes[0].2).unwrap();
    assert_eq!(version.created_by, writer);
    // The new version is journaled in the same write: a kept row, a pending row and its
    // source-id row.
    let journal: Vec<_> = (writes.iter())
        .filter(|(space, key, _)| space == "event_journal" && key[0] != b'i')
        .map(|(_, _, value)| SourceEvent::from_bytes(value).unwrap())
        .collect();
    assert_eq!(journal.len(), 2);
    assert_eq!(journal[0].revision, child.version_id.unwrap().to_string());
    assert!(journal[0].resource.ends_with("/mybucket/some-file.txt"));
}

#[test]
fn history_keeps_head() {
    let config = put_config(
        RealmId::from_bytes([1u8; 32]),
        Ulid::generate(),
        iroh::SecretKey::generate().public(),
    );
    let user_id = config.user_id;
    let mut output = hashed(test_location(user_id));
    output.blob_size = 7;
    let mut parent = PutObjectOperation::new(config).keep_head();
    parent.written_location = Some(output.clone());
    let mut child = parent.publish_child();
    child.output = Some(output);
    let head = CurrentVersionPointer::new_with_generation(Ulid::generate(), 4);
    child
        .object_lookup_read(Some(head.to_bytes().unwrap().into()))
        .unwrap();
    let live = BlobVersion::materialized(
        [9; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        user_id,
        None,
    );
    // The head is not written; the version itself is read inside the transaction.
    let turn = child.liveness_read(Some(live.to_bytes().unwrap().into()));
    assert_eq!(read_spaces(&turn), [BLOB_VERSIONS_KEYSPACE]);
    assert_eq!(child.state, PutObjectState::ReadHistory);
    let turn = child.history_read(None);
    let spaces: Vec<_> = written(&turn)
        .iter()
        .map(|(space, ..)| space.as_str())
        .collect();
    assert_eq!(spaces, [BLOB_LOCATIONS_KEYSPACE, PATHS_INDEX_KEYSPACE]);

    // No replication obligation, no new live object and the head keeps its charge.
    child.config.quota_ceiling = None;
    child.state = PutObjectState::RegisterManagedCopy;
    let turn = child.next(IoResult::Done);
    let Ok(Next::Call(IoCall::AddUsage { deltas, .. })) = turn else {
        panic!("expected the counter deltas, got {turn:?}")
    };
    let expected = UsageDelta {
        logical_bytes: 7,
        ..Default::default()
    };
    assert_eq!(deltas[0].1, expected);

    // A version that appeared meanwhile is never overwritten.
    let turn = child.history_read(Some(live.to_bytes().unwrap().into()));
    assert_eq!(turn.err(), Some(PutObjectError::VersionExists));
}

#[test]
fn registered_commits_nothing() {
    // A listed write an import registered with the same bytes ends with no row written, and
    // its bytes are still released and announced after the commit.
    let config = config();
    let output = hashed(test_location(config.user_id));
    let mut child = publishing(config, output.clone());
    let version = BlobVersion::materialized(
        [7; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        UserId::default(),
        None,
    );

    let turn = child.history_read(Some(version.to_bytes().unwrap().into()));

    let Ok(Next::Then {
        output: result,
        calls,
    }) = turn
    else {
        panic!("expected the registered version, got {turn:?}")
    };
    assert_eq!(result.location, output);
    assert!(child.registered);
    assert!(calls.iter().all(|call| !matches!(call, IoCall::Write(_))));
}

#[test]
fn writer_reads_trail() {
    use aruna_core::structs::storage::delete_audit::{BlobAuditKind, BlobAuditRecord};
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let writer = || {
        let config = put_config(realm_id, group_id, iroh::SecretKey::generate().public());
        let output = hashed(test_location(config.user_id));
        let mut child = publishing(config, output);
        // The version is not registered, so the trail is read before the head moves, in the
        // incarnation the bucket's creation row names.
        let turn = child.history_read(None);
        assert_eq!(
            read_spaces(&turn),
            [aruna_core::keyspaces::BUCKET_CREATION_KEYSPACE]
        );
        let turn = child.next(values(vec![Some(5u128.to_be_bytes().to_vec().into())]));
        let Ok(Next::Call(IoCall::Page {
            space,
            prefix,
            after,
            ..
        })) = turn
        else {
            panic!("expected the trail read, got {turn:?}")
        };
        assert_eq!(space, DELETE_TRAIL_KEYSPACE);
        // Only deletes since the version was created are read.
        let id = child.version_id.unwrap();
        let floor = Ulid::from_parts(id.timestamp_ms(), 0).0 - 1;
        let after = after.expect("a bounded start");
        assert_eq!(
            after.as_ref(),
            [prefix.as_ref(), &Ulid(floor).to_bytes()[..]].concat()
        );
        child
    };
    let page = |rows: Vec<(Key, Value)>| IoResult::Page { rows, next: None };
    // An import registered the version and a user deleted it: it is not published again.
    let mut child = writer();
    let record = BlobAuditRecord {
        realm_id,
        group_id,
        node_id: iroh::SecretKey::generate().public(),
        user_id: child.config.user_id,
        kind: BlobAuditKind::DeleteVersion,
        bucket: "mybucket".to_string(),
        key: "some-file.txt".to_string(),
        version_id: child.version_id,
        occurred_at_ms: 1,
    };
    let turn = child.next(page(vec![(
        vec![0].into(),
        record.to_bytes().unwrap().into(),
    )]));
    assert_eq!(turn.err(), Some(PutObjectError::HistoryDeleted));
    // A clean trail moves the head after both ranges; every purge is read, as a purge is
    // keyed by its job.
    let mut child = writer();
    let turn = child.next(page(Vec::new()));
    let Ok(Next::Call(IoCall::Page {
        prefix,
        after: None,
        ..
    })) = turn
    else {
        panic!("expected the purge read, got {turn:?}")
    };
    // The purges are read in the incarnation the creation row named.
    let purges = aruna_core::structs::storage::delete_audit::purge_prefix(group_id, "mybucket", 5);
    assert_eq!(prefix, Key::from(purges));
    let turn = child.next(page(Vec::new()));
    assert_eq!(written(&turn)[1].0, BLOB_HEAD_KEYSPACE);
}

#[tokio::test]
async fn plain_owns_copy() {
    use aruna_core::handle::Handle as _;
    use aruna_core::structs::storage::blob::{BlobVersionState, ResolvedBackend};
    use aruna_core::structs::storage::format::StoredLayout;
    use aruna_core::structs::storage::routing::PlainTarget;
    let temp = tempdir().unwrap();
    let storage = storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
    let backend = BackendRef::Group(Ulid::from_parts(2, 2));
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = iroh::SecretKey::generate().public();
    // Two provider buckets on one storage hold the same bytes, each at its own tenant path.
    let copy = |path: &str| {
        let ulid = Ulid::generate();
        BackendLocation {
            backend: backend.clone(),
            storage_bucket: String::new(),
            backend_path: path.to_string(),
            ulid,
            format: StoredFormat {
                layout: StoredLayout::Plain(ulid),
                ..StoredFormat::default()
            },
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), vec![5; 32])]),
            ..test_location(UserId::default())
        }
    };
    let mut committed = Vec::new();
    for path in ["team-a-y/a.csv", "team-a-x/data/a.csv"] {
        let written = copy(path);
        let mut op = PutObjectOperation::new(put_config(realm_id, Ulid::generate(), node_id));
        let target = PlainTarget {
            import_id: Ulid::generate(),
            backend: backend.clone(),
            path: written.backend_path.clone(),
            same_name: false,
            listed: true,
            shared: Vec::new(),
            mapped: false,
        };
        op.version_id = Some(Ulid::generate());
        let _ = op.start_plain(target, ResolvedBackend::new(backend.clone(), None));
        op.written_location = Some(written.clone());
        // The lookup and the location row go through the storage, as in the commit.
        let Ok(Next::Call(IoCall::Read(reads))) = op.start_hash_lookup() else {
            panic!("expected the location lookup")
        };
        let read = Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        });
        let Event::Storage(StorageEvent::BatchReadResult { values }) =
            storage.send_effect(read).await
        else {
            panic!("expected the lookup answer")
        };
        op.hash_checked(values.into_iter().next().and_then(|(_, value)| value))
            .unwrap();
        assert!(op.new_blob, "{path} must not adopt another copy");
        assert_eq!(op.output.as_ref(), Some(&written));
        let (space, key, value) = op.location_row().unwrap();
        let write = Effect::Storage(StorageEffect::Write {
            key_space: space,
            key,
            value,
            txn_id: None,
        });
        storage.send_effect(write).await;
        committed.push(written);
    }
    // Each version reads its own copy back; the other bucket's copy stays where it was.
    for written in &committed {
        let state = BlobVersionState::Materialized {
            blob_hash: [5; 32],
            backend: backend.clone(),
            encoding: written.format.encoding(),
            source: None,
        };
        let key = state.location_key().unwrap();
        let event = storage
            .send_effect(crate::blob::records::blob_location_read(&key, None))
            .await;
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = event
        else {
            panic!("missing location row: {event:?}");
        };
        assert_eq!(&BackendLocation::from_bytes(&value).unwrap(), written);
    }
}

/// Starts a put into a bucket bound as member `seed` of the two-shard test set and answers
/// the bucket and record reads; the key "some-file.txt" lies in member 2's range.
fn bound_put(seed: u8) -> (PutObjectOperation, Turn) {
    use crate::distribution::test_record::{binding, record};
    use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
    let (snapshot, bytes) = record(true);
    let config = config();
    let mut bucket = bucket(config.group_id, config.user_id, Compression::Off);
    bucket.distribution = Some(binding(&snapshot, seed, 5));
    let mut op = PutObjectOperation::new(config);
    op.first().unwrap();
    let turn = op.next(values(vec![Some(bucket.to_bytes().unwrap().into())]));
    assert_eq!(read_spaces(&turn)[0], DISTRIBUTION_KEYSPACE);
    let Event::Storage(StorageEvent::BatchReadResult { values: rows }) =
        crate::distribution::test_record::authority_rows(Some(bytes), None)
    else {
        panic!("expected the record rows")
    };
    let turn = op.next(values(rows.into_iter().map(|(_, value)| value).collect()));
    (op, turn)
}

#[test]
fn coordinator_stamps_put() {
    let (op, turn) = bound_put(2);
    assert_eq!(
        op.version_epoch,
        aruna_core::structs::storage::distribution::OriginalEpoch(1)
    );
    // The coordinator continues with the purge fence.
    assert_eq!(read_spaces(&turn), [PURGE_FENCE_KEYSPACE]);
}

#[test]
fn member_put_refused() {
    let (_, turn) = bound_put(1);
    assert!(matches!(
        turn,
        Err(PutObjectError::Distribution(
            GuardError::NotCoordinator { .. }
        ))
    ));
}

mod sealed {
    use super::*;
    use aruna_core::structs::storage::abe::{
        AbeError, AbeParameters, EnvelopeContext, ObjectEnvelope, create_parameters,
    };
    use aruna_core::structs::storage::encryption::{BucketKeyRef, EncryptionMode};
    use aruna_core::structs::storage::format::PithosLayout;

    fn settings() -> BucketEncryption {
        BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(Ulid::from_bytes([4; 16])),
            key_generation: 1,
            storage_generation: 2,
            ..Default::default()
        }
    }

    fn record() -> BucketKeyRecord {
        let key = settings().active_key().unwrap();
        BucketKeyRecord::new(key, Ulid::from_bytes([5; 16]), [6; 32], 1)
    }

    fn row(value: Option<Vec<u8>>) -> IoResult {
        values(vec![value.map(Into::into)])
    }

    fn operation() -> PutObjectOperation {
        let mut config = config();
        config.request.body = Some(BackendStream::new(futures_util::stream::iter([Ok::<
            _,
            std::io::Error,
        >(
            bytes::Bytes::from_static(b"data"),
        )])));
        PutObjectOperation::new(config)
    }

    /// Runs a put up to its encryption settings read: no bucket, no purge fence.
    fn at_settings(op: &mut PutObjectOperation) {
        op.first().unwrap();
        op.next(row(None)).unwrap();
        let turn = op.next(row(None));
        assert_eq!(read_spaces(&turn), [BUCKET_ENCRYPTION_KEYSPACE]);
    }

    #[test]
    fn captures_seal_plan() {
        let mut op = operation();
        at_settings(&mut op);
        let turn = op.next(row(Some(settings().to_bytes().unwrap())));
        let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
            panic!("expected the key record read, got {turn:?}")
        };
        assert_eq!(reads[0].0, BUCKET_KEY_KEYSPACE);
        assert_eq!(reads[0].1.as_ref(), record().key.key().as_slice());
        let turn = op.next(row(Some(record().to_bytes().unwrap())));
        assert_eq!(read_spaces(&turn).len(), 2);
        let secret = aruna_core::compute::SecretBytes::new(vec![9; 32]);
        let (realm, node) = (op.config.realm_id, op.config.node_id);
        let parameters = create_parameters(&secret, realm, node, record().key).unwrap();
        let answers = vec![
            Some(parameters.to_bytes().unwrap().into()),
            Some(1u64.to_be_bytes().to_vec().into()),
        ];
        let turn = op.next(values(answers));
        let Effect::Blob(BlobEffect::Abe(effect)) = asked(&turn) else {
            panic!("expected the write, got {turn:?}")
        };
        let aruna_core::structs::storage::abe::AbeEffect::Write { plan, resolved, .. } =
            effect.as_ref()
        else {
            panic!("expected the envelope write")
        };
        assert_eq!((plan.epoch, &plan.parameters), (1, &parameters));
        let seal = SealPlan::capture(&settings(), &record()).unwrap();
        assert_eq!(resolved.encryption, seal);
    }

    #[test]
    fn missing_key_refused() {
        let mut op = operation();
        at_settings(&mut op);
        op.next(row(Some(settings().to_bytes().unwrap()))).unwrap();
        let turn = op.next(row(None));
        assert_eq!(turn.err(), Some(seal_error(BucketKeyError::Unsupported)));
    }

    /// Runs the version transaction up to the seal fence with `current` as the settings row.
    fn fenced(plan: Option<SealPlan>, current: &BucketEncryption) -> (PutObjectOperation, Turn) {
        fenced_with(plan, current, |_| {})
    }

    fn fenced_with(
        plan: Option<SealPlan>,
        current: &BucketEncryption,
        prepare: impl FnOnce(&mut PutObjectOperation),
    ) -> (PutObjectOperation, Turn) {
        let mut op = PutObjectOperation::new(config());
        prepare(&mut op);
        let mut location = test_location(op.config.user_id);
        if let Some(plan) = plan {
            let layout = PithosLayout {
                stored_size: 10,
                metadata_digest: [1; 32],
                storage_generation: plan.storage_generation,
            };
            location.format = StoredFormat::pithos(layout, plan.key);
        }
        op.seal_plan = plan;
        let mut child = publisher(op, location);
        child.first().unwrap();
        child.next(row(None)).unwrap();
        child.next(values(vec![None, None])).unwrap();
        let turn = child.next(row(Some(current.to_bytes().unwrap())));
        (child, turn)
    }

    #[test]
    fn stale_seal_refused() {
        let plan = SealPlan::capture(&settings(), &record()).unwrap();
        // The bucket changed its stored format while the bytes streamed.
        let mut moved = settings();
        moved.storage_generation += 1;
        let (_, turn) = fenced(plan, &moved);
        assert!(matches!(
            turn,
            Err(PutObjectError::ConversionError(ConversionError::BucketKey(
                BucketKeyError::StaleGeneration { .. }
            )))
        ));
        // The current plan passes the fence and reaches the hash step.
        let (_, turn) = fenced(plan, &settings());
        assert!(
            matches!(turn, Err(PutObjectError::MissingHash(_))),
            "{turn:?}"
        );
    }

    #[test]
    fn plain_into_sealed() {
        let (_, turn) = fenced(None, &settings());
        assert!(matches!(
            turn,
            Err(PutObjectError::ConversionError(ConversionError::BucketKey(
                BucketKeyError::StaleGeneration { .. }
            )))
        ));
    }

    #[test]
    fn records_copy_owner() {
        let mut location = test_location(UserId::default());
        let key = BucketKeyRef::new(Ulid::from_bytes([4; 16]), 1);
        let layout = PithosLayout {
            stored_size: 10,
            metadata_digest: [1; 32],
            storage_generation: 0,
        };
        location.format = StoredFormat::pithos(layout, key);
        let mut child = publishing(config(), location);
        child.state = PutObjectState::CreateVersionRecord;

        let turn = child.next(IoResult::Done);

        assert_eq!(written(&turn)[0].0, COPY_OWNER_KEYSPACE);
    }

    fn parameters(op: &PutObjectOperation, secret: u8) -> AbeParameters {
        let secret = aruna_core::compute::SecretBytes::new(vec![secret; 32]);
        create_parameters(&secret, op.config.realm_id, op.config.node_id, record().key).unwrap()
    }

    fn envelope(parameters: AbeParameters) -> ObjectEnvelope {
        ObjectEnvelope {
            context: EnvelopeContext {
                parameters,
                epoch: 1,
                object_key: "some-file.txt".to_string(),
                write_id: Ulid::from_bytes([8; 16]),
                public_key: [3; 32],
            },
            abe: vec![1; 16],
            recovery_enc: [2; 32],
            recovery_ciphertext: vec![4; 32],
        }
    }

    /// Answers the publication fence with substituted parameters or another epoch.
    fn abe_fence(substituted: bool, epoch: u64) -> Turn {
        let plan = SealPlan::capture(&settings(), &record()).unwrap();
        let (mut child, _) = fenced_with(plan, &settings(), |op| {
            op.envelope = Some(envelope(parameters(op, 9)));
        });
        assert_eq!(child.state, PutObjectState::FenceAbe);
        let current = parameters(&child, if substituted { 10 } else { 9 });
        child.next(values(vec![
            Some(current.to_bytes().unwrap().into()),
            Some(epoch.to_be_bytes().to_vec().into()),
        ]))
    }

    #[test]
    fn envelope_fence_refused() {
        let refused = |turn: Turn| match turn {
            Err(PutObjectError::BlobWriteFailed(BlobError::Abe(error))) => error,
            other => panic!("expected an envelope refusal, got {other:?}"),
        };
        assert_eq!(refused(abe_fence(false, 2)), AbeError::Epoch);
        assert_eq!(refused(abe_fence(true, 1)), AbeError::Parameters);
        // The admitted parameters and epoch pass the fence and reach the hash step.
        let turn = abe_fence(false, 1);
        assert!(
            matches!(turn, Err(PutObjectError::MissingHash(_))),
            "{turn:?}"
        );
    }

    #[test]
    fn plain_skips_envelope() {
        let mut op = operation();
        at_settings(&mut op);
        let turn = op.next(row(None));
        let Effect::Blob(BlobEffect::Write { resolved, .. }) = asked(&turn) else {
            panic!("expected the plain write, got {turn:?}")
        };
        assert_eq!(resolved.encryption, None);
    }

    #[test]
    fn disabled_skips_envelope() {
        let mut op = operation().without_envelope();
        at_settings(&mut op);
        op.next(row(Some(settings().to_bytes().unwrap()))).unwrap();
        let turn = op.next(row(Some(record().to_bytes().unwrap())));
        let Effect::Blob(BlobEffect::Write { resolved, .. }) = asked(&turn) else {
            panic!("expected the sealed write without envelope, got {turn:?}")
        };
        assert_eq!(
            resolved.encryption,
            SealPlan::capture(&settings(), &record()).unwrap()
        );
    }

    fn sealed_output(blob_size: u64) -> BackendLocation {
        let mut location = hashed(test_location(UserId::default()));
        location.blob_size = blob_size;
        location.format = StoredFormat::pithos(
            PithosLayout {
                stored_size: 10,
                metadata_digest: [1; 32],
                storage_generation: 2,
            },
            record().key,
        );
        location
    }

    #[test]
    fn charges_envelope_bytes() {
        let mut config = config();
        config.quota_ceiling = None;
        let mut child = publishing(config, sealed_output(1));
        child.envelope = Some(envelope(parameters(&child, 9)));
        let turn = child.store_envelope();
        let writes = written(&turn);
        let charge = writes[0].2.len() + writes[2].2.len();
        assert_eq!(child.envelope_bytes, charge as u64);
        child.state = PutObjectState::WriteReplicationObligation;
        let turn = child.next(IoResult::Done);
        let Ok(Next::Call(IoCall::AddUsage { deltas, .. })) = turn else {
            panic!("expected the counter deltas, got {turn:?}")
        };
        assert!(
            deltas
                .iter()
                .any(|(_, delta)| delta.logical_bytes == 1 + charge as i128)
        );
    }

    /// Runs the quota gate for a write with a 2 byte ceiling and no prior usage.
    fn quota_charge(payload: u64, envelope_bytes: u64) -> Turn {
        let mut config = config();
        config.quota_ceiling = Some(2);
        let mut output = hashed(test_location(UserId::default()));
        output.blob_size = payload;
        let mut child = publishing(config, output);
        child.envelope_bytes = envelope_bytes;
        child.state = PutObjectState::WriteReplicationObligation;
        let mut turn = child.next(IoResult::Done);
        if child.state == PutObjectState::EnforceQuota {
            // The rebuild marker, the realm config and the local counter are absent.
            for _ in 0..3 {
                child.next(row(None)).unwrap();
            }
            turn = child.next(IoResult::Page {
                rows: Vec::new(),
                next: None,
            });
        }
        turn
    }

    #[test]
    fn quota_counts_envelope() {
        let refused = |turn: Turn| matches!(turn, Err(PutObjectError::QuotaExceeded { .. }));
        // The payload alone fits, but the envelope pushes the charge over the ceiling.
        assert!(refused(quota_charge(2, 1)));
        assert!(refused(quota_charge(0, 3)));
        assert!(!refused(quota_charge(0, 2)));
        assert!(!refused(quota_charge(1, 1)));
    }

    /// While usage is rebuilt a write for a group with a quota is refused as retryable; a
    /// group without a quota never reads the marker and books its usage.
    #[test]
    fn rebuild_refuses_quota() {
        use aruna_core::structs::storage::usage::{USAGE_COMPLETE_KEY, USAGE_REBUILD_KEY};
        for ceiling in [Some(u64::MAX), None] {
            let mut config = config();
            config.quota_ceiling = ceiling;
            let mut child = publishing(config, hashed(test_location(UserId::default())));
            child.state = PutObjectState::WriteReplicationObligation;
            let turn = child.next(IoResult::Done);
            if ceiling.is_none() {
                assert!(matches!(turn, Ok(Next::Call(IoCall::AddUsage { .. }))));
                continue;
            }
            assert!(matches!(
                turn,
                Ok(Next::Call(IoCall::Read(ref reads))) if reads[0].1.as_ref() == USAGE_REBUILD_KEY
            ));
            let turn = child.next(row(Some(Vec::new())));
            // No earlier rebuild completed the counters, so nothing backs the quota.
            assert!(matches!(
                turn,
                Ok(Next::Call(IoCall::Read(ref reads))) if reads[0].1.as_ref() == USAGE_COMPLETE_KEY
            ));
            let turn = child.next(row(None));
            assert_eq!(turn.err(), Some(PutObjectError::UsageRebuilding));
        }
    }
}

/// Routing resolves the backend before any byte moves; the version transaction fences it.
mod routing {
    use super::*;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use aruna_core::structs::storage::routing::{GroupRoutingInputs, StorageRoutingRule};
    use std::collections::BTreeSet;

    fn config(snapshot: RoutingSnapshot) -> PutObjectConfig {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        PutObjectConfig {
            user_id: UserId::local(Ulid::from_parts(1, 1), realm_id),
            group_id: snapshot.group_id,
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
            request: PutObjectInput {
                bucket: "bucket".to_string(),
                key: "archive/one".to_string(),
                content_length: Some(3),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                    &b"abc"[..],
                ))),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: snapshot,
        }
    }

    fn snapshot() -> RoutingSnapshot {
        RoutingSnapshot::new(
            Ulid::from_parts(2, 2),
            BackendCatalog::new("default")
                .with_backend("default", None)
                .with_backend("tape", Some("archive".to_string())),
        )
    }

    /// Answers the bucket, purge fence and encryption reads of an ungoverned plain bucket.
    fn begin(operation: &mut PutObjectOperation) -> Turn {
        operation.first().unwrap();
        operation.next(values(vec![None])).unwrap();
        operation.next(values(vec![None])).unwrap();
        operation.next(values(vec![None]))
    }

    fn resolved(turn: &Turn) -> &aruna_core::structs::storage::blob::ResolvedBackend {
        match asked(turn) {
            Effect::Blob(BlobEffect::Write { resolved, .. }) => resolved,
            other => panic!("expected one blob write, got {other:?}"),
        }
    }

    #[test]
    fn stamps_resolved_backend() {
        let snapshot = snapshot().with_bucket_rules(vec![StorageRoutingRule {
            key_prefix: "archive/".to_string(),
            exact: false,
            target: RoutingTarget::Class("archive".to_string()),
        }]);

        let turn = begin(&mut PutObjectOperation::new(config(snapshot)));

        let resolved = resolved(&turn);
        assert_eq!(resolved.backend, BackendRef::Node("tape".to_string()));
        assert_eq!(resolved.storage_class.as_deref(), Some("archive"));
    }

    #[test]
    fn missing_class_stamps() {
        // A class this node does not offer reroutes the write, never fails it.
        let snapshot = snapshot().with_bucket_rules(vec![StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Class("glacier".to_string()),
        }]);

        let turn = begin(&mut PutObjectOperation::new(config(snapshot)));

        let resolved = resolved(&turn);
        assert_eq!(resolved.backend, BackendRef::Node("default".to_string()));
        assert_eq!(resolved.storage_class, None);
    }

    #[test]
    fn unknown_backend_aborts() {
        // A named backend is binding: nothing may be written when it is gone.
        let snapshot = snapshot().with_bucket_rules(vec![StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Backend(BackendRef::Node("ghost".to_string())),
        }]);

        let turn = begin(&mut PutObjectOperation::new(config(snapshot)));

        assert!(matches!(
            turn,
            Err(PutObjectError::RoutingFailed(RoutingError::UnknownBackend(
                _
            )))
        ));
    }

    #[test]
    fn refuses_disabled_backend() {
        // A disabled backend refuses writes: the version does not commit a location on it,
        // and the written blob is rolled back.
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let snapshot = snapshot()
            .with_group_inputs(GroupRoutingInputs {
                default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
                backend_ids: BTreeSet::from([backend_id]),
                ..Default::default()
            })
            .with_bucket_rules(Vec::new());
        let mut operation = PutObjectOperation::new(config(snapshot));
        begin(&mut operation).unwrap();
        let written = BackendLocation {
            backend: BackendRef::Group(backend_id),
            backend_path: "bucket/object".to_string(),
            ulid: Ulid::from_bytes([6u8; 16]),
            blob_size: 3,
            ..test_location(UserId::default())
        };
        let finished = BlobEvent::WriteFinished {
            location: written.clone(),
        };
        let turn = operation.next(event(Event::Blob(finished)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));

        let mut child = operation.publish_child();
        child.first().unwrap();
        child.next(values(vec![None])).unwrap();
        child.next(values(vec![None, None])).unwrap();
        let turn = child.next(values(vec![None]));
        assert_eq!(read_spaces(&turn), [STORAGE_BACKEND_KEYSPACE]);
        let disabled = GroupStorage {
            backend_id,
            group_id: Ulid::from_bytes([7u8; 16]),
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            disabled: true,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        };
        let turn = child.next(values(vec![Some(disabled.to_bytes().unwrap().into())]));
        let error = turn.unwrap_err();
        assert_eq!(error, BackendFenceError::Unavailable.into());

        operation.next(published(Err(error))).unwrap_err();
        assert_rolls_back(&operation.abort(), &written);
    }
}

/// No byte-materialization call and no registration happens before the destination passed
/// the shared placement gate, which runs as a child.
mod decisions {
    use super::*;
    use crate::placement::policy::PolicyCacheStats;
    use aruna_core::structs::placement::policy::{
        PlacementDecision, PlacementPolicy, PlacementSelector, PlacementSubject, VerifiedPolicy,
    };
    use std::collections::BTreeMap;

    const BODY: &[u8] = b"payload";

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    /// A rule that admits exactly one location.
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
            realm_id: realm(),
            subject: PlacementSubject {
                node_id: node(9),
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

    fn bucket(refs: Vec<PlacementPolicyRef>, generation: u64) -> Value {
        let info = BucketInfo {
            group_id: Ulid::from_bytes([2u8; 16]),
            created_at: UNIX_EPOCH,
            created_by: UserId::local(Ulid::from_bytes([3u8; 16]), realm()),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: refs,
            placement_policy_generation: generation,
            compression: Compression::Off,
            distribution: None,
        };
        info.to_bytes().expect("bucket encodes").into()
    }

    fn operation(location: &str) -> PutObjectOperation {
        let group_id = Ulid::from_bytes([2u8; 16]);
        PutObjectOperation::new(PutObjectConfig {
            user_id: UserId::local(Ulid::from_bytes([3u8; 16]), realm()),
            group_id,
            realm_id: realm(),
            node_id: node(9),
            request: PutObjectInput {
                bucket: "bucket".to_string(),
                key: "governed.txt".to_string(),
                content_length: Some(BODY.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(BODY))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        })
        .with_gate(gate(location))
    }

    fn decided(decision: PlacementDecision, foreign_owner: Option<Ulid>) -> IoResult {
        let outcome = PolicyGateOutcome {
            decision,
            foreign_owner,
            stats: PolicyCacheStats::default(),
        };
        IoResult::Child(ChildOutcome::new(Ok::<_, ReadPolicyError>(outcome)))
    }

    /// Answers the bucket read with `refs`; a governed write asks the gate next.
    fn gated(operation: &mut PutObjectOperation, refs: Vec<PlacementPolicyRef>) {
        operation.first().unwrap();
        let turn = operation.next(values(vec![Some(bucket(refs, 1))]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(operation.state, PutObjectState::PolicyGate);
    }

    #[test]
    fn denies_before_write() {
        // The rule admits another location, so nothing may be written at all.
        let rule = policy("us-east");
        let mut operation = operation("eu-west");
        gated(&mut operation, vec![rule.policy_ref()]);
        let policy_ids = vec![rule.policy().policy_id];
        let denied = PlacementDecision::Denied {
            policy_ids: policy_ids.clone(),
        };

        let turn = operation.next(decided(denied, None));

        assert_eq!(
            turn.err(),
            Some(PolicyGateError::Denied { policy_ids }.into())
        );
    }

    #[test]
    fn unresolved_blocks_write() {
        // A rule that cannot be obtained blocks; it is never read as a grant.
        let rule = policy("eu-west");
        let mut operation = operation("eu-west");
        gated(&mut operation, vec![rule.policy_ref()]);
        let unavailable = PlacementDecision::Unavailable {
            policy_ids: vec![rule.policy().policy_id],
        };

        let turn = operation.next(decided(unavailable, None));

        assert!(matches!(
            turn,
            Err(PutObjectError::PolicyGate(
                PolicyGateError::Unavailable { .. }
            ))
        ));
    }

    #[test]
    fn missing_subject_blocks() {
        // A node that advertises no subject may hold nothing governed.
        let mut operation = operation("eu-west");
        operation.gate_context = None;
        operation.first().unwrap();
        let refs = vec![PlacementPolicyRef {
            policy_id: Ulid::from_bytes([1u8; 16]),
            digest: [4u8; 32],
        }];

        let turn = operation.next(values(vec![Some(bucket(refs, 1))]));

        assert_eq!(turn.err(), Some(PolicyGateError::NoSubject.into()));
    }

    #[test]
    fn planned_needs_hint() {
        // A planned id in a distributed bucket must already carry the set hint.
        use aruna_core::structs::storage::distribution::hint::with_hint;
        let set_id = Ulid::from_bytes([5u8; 16]);
        let planned = |id: Ulid| {
            let mut operation = operation("eu-west");
            operation.config.preassigned_version_id = Some(id);
            operation.first().unwrap();
            operation.next(values(vec![None])).unwrap();
            let mut info = BucketInfo::from_bytes(&bucket(Vec::new(), 0)).unwrap();
            info.distribution = Some(DistributionBinding {
                set_id,
                member_id: Ulid::from_bytes([6u8; 16]),
                incarnation: 1,
                revision: Default::default(),
            });
            operation.next(values(vec![Some(info.to_bytes().unwrap().into())]))
        };
        let hint = SetHint::of_set(set_id);
        let plain = Ulid::from_parts(1, 0);
        assert_ne!(SetHint::of_version(plain), hint);
        assert_eq!(
            planned(plain).err(),
            Some(PutObjectError::InvalidPreassignedVersion)
        );
        assert!(planned(with_hint(plain, hint)).is_ok());
    }

    #[test]
    fn ungoverned_skips_gate() {
        // An ungoverned write reaches the blob write with no policy round trip; an allowed
        // governed write reaches it after the gate.
        let mut plain = operation("eu-west");
        plain.first().unwrap();
        plain
            .next(values(vec![Some(bucket(Vec::new(), 0))]))
            .unwrap();
        plain.next(values(vec![None])).unwrap();
        let turn = plain.next(values(vec![None]));
        assert!(matches!(
            asked(&turn),
            Effect::Blob(BlobEffect::Write { .. })
        ));

        let rule = policy("eu-west");
        let mut governed = operation("eu-west");
        gated(&mut governed, vec![rule.policy_ref()]);
        let turn = governed.next(decided(PlacementDecision::Allowed, None));
        assert_eq!(read_spaces(&turn), [PURGE_FENCE_KEYSPACE]);
    }

    #[test]
    fn inherited_ref_gates() {
        // Staging, imports and job outputs carry their source's refs into an otherwise
        // ungoverned destination; a sender may never drop one.
        let rule = policy("us-east");
        let mut operation = operation("eu-west").with_inherited_policies(vec![rule.policy_ref()]);
        operation.first().unwrap();
        let turn = operation.next(values(vec![Some(bucket(Vec::new(), 0))]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    }

    #[test]
    fn refuses_foreign_owner() {
        // A copy inherits a rule another group owns; the subject satisfies it, so only the
        // ownership refuses the write.
        let rule = policy("eu-west");
        let mut operation = operation("eu-west").with_inherited_policies(vec![rule.policy_ref()]);
        gated(&mut operation, Vec::new());
        let owner = Ulid::from_bytes([8u8; 16]);

        let turn = operation.next(decided(PlacementDecision::Allowed, Some(owner)));

        assert!(matches!(
            turn,
            Err(PutObjectError::PolicyGate(
                PolicyGateError::ForeignPolicy { .. }
            ))
        ));
    }

    fn location() -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "aruna".to_string(),
            backend_path: "objects/one".to_string(),
            ulid: Ulid::from_bytes([5u8; 16]),
            format: StoredFormat::default(),
            created_by: UserId::local(Ulid::from_bytes([3u8; 16]), realm()),
            created_at: UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: BODY.len() as u64,
            hashes: HashMap::from([("blake3".to_string(), vec![6u8; 32])]),
        }
    }

    fn subject_row(generation: u64) -> Value {
        let mut record = aruna_core::structs::placement::node_subject::NodeSubjectRecord::seed(
            crate::tests::policy::subject(node(9), "eu-west"),
        )
        .expect("subject is valid");
        record.subject.generation = generation;
        record.to_bytes().expect("record encodes").into()
    }

    /// The version transaction of a write the gate admitted under `refs`, answered up to its
    /// bucket and subject reread.
    fn reread(refs: Vec<PlacementPolicyRef>, bucket_now: Value, subject: Option<Value>) -> Turn {
        let mut operation = operation("eu-west");
        operation.first().unwrap();
        let turn = operation.next(values(vec![Some(bucket(refs.clone(), 1))]));
        if !refs.is_empty() {
            assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
            operation
                .next(decided(PlacementDecision::Allowed, None))
                .unwrap();
        }
        operation.next(values(vec![None])).unwrap();
        operation.next(values(vec![None])).unwrap();
        let finished = BlobEvent::WriteFinished {
            location: location(),
        };
        operation.next(event(Event::Blob(finished))).unwrap();
        let mut child = operation.publish_child();
        child.first().unwrap();
        child.next(values(vec![None])).unwrap();
        child.next(values(vec![Some(bucket_now), subject]))
    }

    #[test]
    fn drift_aborts_commit() {
        // A default changed while the bytes streamed was never evaluated, so the version must
        // not commit the refs it would now inherit.
        let changed = bucket(vec![policy("us-east").policy_ref()], 1);
        let turn = reread(Vec::new(), changed, None);
        assert_eq!(turn.err(), Some(PolicyGateError::Drift.into()));
    }

    #[test]
    fn subject_advance_aborts() {
        // The subject that admitted the write moved on while the bytes streamed.
        let rule = policy("eu-west");
        let same = bucket(vec![rule.policy_ref()], 1);
        let turn = reread(vec![rule.policy_ref()], same, Some(subject_row(2)));
        assert_eq!(turn.err(), Some(PolicyGateError::Drift.into()));
    }
}
