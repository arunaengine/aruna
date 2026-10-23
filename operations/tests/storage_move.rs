//! A storage move copies a bucket's versions to the backend its rules choose now, resumes from
//! stored progress, reclaims the old copies and serves the same bytes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::blob::{BackendRegistry, BlobHandler, NodeBackend};
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::egress::EgressPolicy;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    COMPRESSION_MIGRATION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BackendLocation, BackendRef, BlobVersion, BucketInfo, VersionKey,
};
use aruna_core::structs::storage::cleanup::ReclaimCandidateKey;
use aruna_core::structs::storage::format::{Compression, CompressionMigration};
use aruna_core::structs::storage::routing::{RoutingSnapshot, RoutingTarget, StorageRoutingRule};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::blob::migration::misplaced::count_misplaced;
use aruna_operations::blob::migration::process_migrations;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::bucket::storage_move::StartMoveOperation;
use aruna_operations::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_storage::storage;
use futures_util::TryStreamExt;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use tempfile::TempDir;
use ulid::Ulid;

const BUCKET: &str = "bucket";

struct TestContext {
    _temp_dir: TempDir,
    driver: DriverContext,
    group_id: Ulid,
    user_id: UserId,
}

fn filesystem(root: String, prefix: &str) -> BackendConfig {
    std::fs::create_dir_all(&root).unwrap();
    BackendConfig {
        backend_type: Backend::FileSystem,
        root,
        service_config: HashMap::new(),
        bucket_prefix: Some(prefix.to_string()),
        max_bucket_size: Some(100_000),
        multipart_bucket: Some(format!("{prefix}parts")),
        timeouts: Default::default(),
    }
}

fn bucket_info(group_id: Ulid, user_id: UserId, rules: Vec<StorageRoutingRule>) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: UNIX_EPOCH,
        created_by: user_id,
        cors_configuration: None,
        storage_routing: rules,
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

/// A node with Aruna's default backend and a second one offering the class `cold`.
async fn setup_context() -> TestContext {
    let temp_dir = tempfile::tempdir().unwrap();
    let root = temp_dir.path().to_str().unwrap().to_string();
    let storage_handle = storage::FjallStorage::open(&root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let mut backends = BTreeMap::new();
    let hot = NodeBackend::new(filesystem(format!("{root}/hot"), "hot-"), None);
    let cold = NodeBackend::new(
        filesystem(format!("{root}/cold"), "cold-"),
        Some("cold".into()),
    );
    backends.insert(BackendRef::DEFAULT_NODE_NAME.to_string(), Arc::new(hot));
    backends.insert("cold".to_string(), Arc::new(cold));
    let registry = BackendRegistry::new(backends, BackendRef::DEFAULT_NODE_NAME.into()).unwrap();
    let policy = EgressPolicy::loopback();
    let blob_handle =
        BlobHandler::with_registry(registry, storage_handle.clone(), net_handle.clone(), policy)
            .await
            .unwrap();
    let driver = DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let group_id = Ulid::generate();
    let user_id = UserId::local(Ulid::generate(), RealmId::from_bytes([7u8; 32]));
    let info = bucket_info(group_id, user_id, Vec::new());
    let create = CreateBucketOperation::new(BUCKET.to_string(), info);
    drive(create, &driver).await.unwrap();
    TestContext {
        _temp_dir: temp_dir,
        driver,
        group_id,
        user_id,
    }
}

fn stream(bytes: &[u8]) -> BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>> {
    BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
        bytes.to_vec(),
    )))
}

async fn put(context: &TestContext, key: &str, bytes: &[u8]) -> Ulid {
    let operation = PutObjectOperation::new(PutObjectConfig {
        user_id: context.user_id,
        group_id: context.group_id,
        realm_id: RealmId::from_bytes([7u8; 32]),
        node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
        request: PutObjectInput {
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            content_length: Some(bytes.len() as u64),
            body: Some(stream(bytes)),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling: None,
        routing: RoutingSnapshot::single(context.group_id),
    });
    drive(operation, &context.driver).await.unwrap().version_id
}

async fn read(context: &TestContext, keyspace: &str, key: Vec<u8>) -> Option<Vec<u8>> {
    let event = context
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: keyspace.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("unexpected storage event {event:?}")
    };
    value.map(|value| value.to_vec())
}

async fn write(context: &TestContext, keyspace: &str, key: &str, value: Vec<u8>) {
    let event = context
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: keyspace.to_string(),
            key: key.as_bytes().to_vec().into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn version(context: &TestContext, key: &str, version_id: Ulid) -> BlobVersion {
    let key = VersionKey::new(BUCKET, key, version_id).to_bytes().unwrap();
    BlobVersion::from_bytes(&read(context, BLOB_VERSIONS_KEYSPACE, key).await.unwrap()).unwrap()
}

async fn content(context: &TestContext, version: &BlobVersion) -> Vec<u8> {
    let key = version.location_key().unwrap().to_bytes();
    let location = read(context, BLOB_LOCATIONS_KEYSPACE, key).await.unwrap();
    let location = BackendLocation::from_bytes(&location).unwrap();
    let blob = context.driver.blob_handle.as_ref().unwrap();
    let Event::Blob(BlobEvent::ReadFinished { blob, .. }) =
        blob.send_blob_effect(BlobEffect::Read { location }).await
    else {
        panic!("read failed")
    };
    let chunks: Vec<bytes::Bytes> = blob.try_collect().await.unwrap();
    chunks.concat()
}

/// Sends new writes under `prefix` to the `cold` class.
async fn route_cold(context: &TestContext, prefix: &str) {
    let rule = StorageRoutingRule {
        key_prefix: prefix.to_string(),
        exact: false,
        target: RoutingTarget::Class("cold".to_string()),
    };
    let info = bucket_info(context.group_id, context.user_id, vec![rule]);
    write(
        context,
        S3_BUCKET_KEYSPACE,
        BUCKET,
        info.to_bytes().unwrap(),
    )
    .await;
}

async fn progress(context: &TestContext) -> CompressionMigration {
    let key = BUCKET.as_bytes().to_vec();
    let value = read(context, COMPRESSION_MIGRATION_KEYSPACE, key).await;
    CompressionMigration::from_bytes(&value.unwrap()).unwrap()
}

async fn run(context: &TestContext) -> Option<std::time::Duration> {
    process_migrations(&context.driver, None)
        .await
        .unwrap()
        .next
}

fn cold() -> BackendRef {
    BackendRef::Node("cold".to_string())
}

#[tokio::test]
async fn moves_bucket_copies() {
    let context = setup_context().await;
    let first = put(&context, "data/a.txt", b"moved research data").await;
    let kept = put(&context, "other/b.txt", b"kept research data").await;
    // More versions than one page, so the move has to resume from stored progress.
    let mut small = Vec::new();
    for index in 0..260 {
        let key = format!("data/{index:03}");
        small.push((key.clone(), put(&context, &key, key.as_bytes()).await));
    }
    let old = version(&context, "data/a.txt", first)
        .await
        .location_key()
        .unwrap();
    route_cold(&context, "data/").await;
    let counted = count_misplaced(&context.driver, BUCKET, "", None)
        .await
        .unwrap();
    assert_eq!((counted.count, counted.complete), (261, true));

    let start = StartMoveOperation::new(BUCKET.to_string(), context.group_id, "data/".into(), 1);
    drive(start, &context.driver).await.unwrap();
    // Each run only reads the stored record, so a second run is what a restart sees.
    assert!(run(&context).await.is_some());
    let partial = progress(&context).await;
    assert!(partial.cursor.is_some() && partial.finished_at_ms.is_none());
    assert!(run(&context).await.is_none());
    let done = progress(&context).await;
    assert_eq!((done.migrated, done.failed, done.skipped), (261, 0, 1));

    let moved = version(&context, "data/a.txt", first).await;
    assert_eq!(moved.location_key().unwrap().backend, cold());
    assert_eq!(content(&context, &moved).await, b"moved research data");
    let candidate = ReclaimCandidateKey::new(old.backend, old.encoding, old.blake3_hash);
    let queued = read(&context, BLOB_RECLAIM_KEYSPACE, candidate.to_bytes()).await;
    assert!(queued.is_some());
    let other = version(&context, "other/b.txt", kept).await;
    assert_eq!(
        other.location_key().unwrap().backend,
        BackendRef::node_default()
    );
    for (key, version_id) in &small {
        let small = version(&context, key, *version_id).await;
        assert_eq!(content(&context, &small).await, key.as_bytes());
    }
    let counted = count_misplaced(&context.driver, BUCKET, "", None)
        .await
        .unwrap();
    assert_eq!(counted.count, 0);
}

#[tokio::test]
async fn placed_versions_skip() {
    // Without a rule change nothing moves and nothing is reclaimed.
    let context = setup_context().await;
    let id = put(&context, "a.txt", b"stays").await;
    let before = version(&context, "a.txt", id).await;

    let start = StartMoveOperation::new(BUCKET.to_string(), context.group_id, String::new(), 1);
    drive(start, &context.driver).await.unwrap();
    assert!(run(&context).await.is_none());

    let done = progress(&context).await;
    assert_eq!((done.migrated, done.skipped, done.failed), (0, 1, 0));
    assert_eq!(version(&context, "a.txt", id).await, before);
}

#[tokio::test]
async fn settles_gone_versions() {
    // A move intent of a version that is gone is retired once the storage shows its path
    // holds nothing to release.
    use aruna_core::keyspaces::IMPORT_INTENT_KEYSPACE;
    use aruna_core::structs::storage::backend_import::{WriteIntent, intent_key};
    use aruna_core::structs::storage::blob::VersionKey;
    use aruna_operations::blob::migration::settle_move;
    let context = setup_context().await;
    let id = Ulid::generate();
    let intent = WriteIntent {
        key: "a.csv".to_string(),
        version_id: id,
        backend: BackendRef::node_default(),
        created_by: context.user_id,
        written: Some([1; 32]),
        mover: Some(7),
    };
    let key = intent_key(Ulid::generate(), "raw/a.csv~X", id).to_vec();
    let write = aruna_core::effects::StorageEffect::Write {
        key_space: IMPORT_INTENT_KEYSPACE.to_string(),
        key: key.clone().into(),
        value: intent.to_bytes().unwrap().into(),
        txn_id: None,
    };
    context
        .driver
        .storage_handle
        .send_storage_effect(write)
        .await;
    let version = VersionKey::new(BUCKET, "a.csv", id);
    assert!(
        settle_move(&context.driver, &version, &[(key.clone(), intent)])
            .await
            .unwrap()
    );
    assert!(read(&context, IMPORT_INTENT_KEYSPACE, key).await.is_none());
}
