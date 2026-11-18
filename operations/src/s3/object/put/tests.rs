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

