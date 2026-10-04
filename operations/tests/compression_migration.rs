//! A compression change re-encodes this node's copies in the background, resumes after a restart,
//! keeps shared copies until no version names them, and serves the same bytes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![recursion_limit = "256"]

use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    COMPRESSION_MIGRATION_KEYSPACE, COMPRESSION_QUEUE_KEYSPACE, TASK_TIMER_KEYSPACE,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BackendLocation, BlobVersion, BucketInfo, VersionKey,
};
use aruna_core::structs::storage::cleanup::ReclaimCandidateKey;
use aruna_core::structs::storage::format::{
    Compression, CompressionMigration, EncodingClass, StoredLayout,
};
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::blob::cleanup::process_cleanup_batch;
use aruna_operations::blob::migration::process_migrations;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::s3::bucket::compression::PutCompressionOperation;
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_operations::tasks::incoming::start_task_queues;
use aruna_storage::storage;
use futures_util::TryStreamExt;
use std::collections::HashMap;
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

async fn setup_context() -> TestContext {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_root = temp_dir.path().to_str().unwrap();
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_root,
            service_config: HashMap::new(),
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("uploaded-parts".to_string()),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
    )
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
    let info = BucketInfo {
        group_id,
        created_at: UNIX_EPOCH,
        created_by: user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        encryption: Default::default(),
    };
    drive(
        CreateBucketOperation::new(BUCKET.to_string(), info),
        &driver,
    )
    .await
    .unwrap();
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

/// One task run from the head of the queue; returns when the task must run again.
async fn run(context: &TestContext) -> Option<std::time::Duration> {
    process_migrations(&context.driver, None)
        .await
        .unwrap()
        .next
}

/// Every stored blob file under the test root.
fn blob_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        match path.is_dir() {
            true => files.extend(blob_files(&path)),
            false => files.push(path),
        }
    }
    files
}

async fn progress(context: &TestContext) -> CompressionMigration {
    let value = read(
        context,
        COMPRESSION_MIGRATION_KEYSPACE,
        BUCKET.as_bytes().to_vec(),
    )
    .await;
    CompressionMigration::from_bytes(&value.unwrap()).unwrap()
}

#[tokio::test]
async fn migrates_bucket_copies() {
    let context = setup_context().await;
    let shared: Vec<u8> = b"compressible research data ".repeat(80_000);
    let first = put(&context, "a.txt", &shared).await;
    let second = put(&context, "b.txt", &shared).await;
    // More versions than one page, so the run has to resume from stored progress.
    let mut small = Vec::new();
    for index in 0..70 {
        let key = format!("small/{index:03}");
        small.push((key.clone(), put(&context, &key, key.as_bytes()).await));
    }
    let raw = version(&context, "a.txt", first)
        .await
        .location_key()
        .unwrap();
    let zstd = Compression::Zstd { level: 3 };

    let operation = PutCompressionOperation::new(BUCKET.to_string(), context.group_id, zstd, 1);
    drive(operation, &context.driver).await.unwrap();

    // Each run only reads the stored record, so a second run is what a restart sees.
    assert!(run(&context).await.is_some());
    let partial = progress(&context).await;
    assert!(partial.cursor.is_some() && partial.finished_at_ms.is_none());
    assert!(run(&context).await.is_none());
    let done = progress(&context).await;
    assert_eq!((done.migrated, done.failed), (72, 0));
    assert!(done.finished_at_ms.is_some());

    let class = EncodingClass::Zstd { level: 3 };
    let a = version(&context, "a.txt", first).await;
    let b = version(&context, "b.txt", second).await;
    // Both versions now share one zstd copy; the raw copy waits for reclaim.
    assert_eq!(a.location_key(), b.location_key());
    assert_eq!(a.location_key().unwrap().encoding, class);
    assert_ne!(a.location_key(), Some(raw.clone()));
    let candidate = ReclaimCandidateKey::new(raw.backend.clone(), raw.encoding, raw.blake3_hash);
    assert!(
        read(&context, BLOB_RECLAIM_KEYSPACE, candidate.to_bytes())
            .await
            .is_some()
    );
    let row = read(
        &context,
        BLOB_LOCATIONS_KEYSPACE,
        a.location_key().unwrap().to_bytes(),
    )
    .await;
    let row = BackendLocation::from_bytes(&row.unwrap()).unwrap();
    assert!(matches!(row.format.layout, StoredLayout::Frames(_)));
    assert!(row.stored_size() < row.blob_size);
    assert_eq!(content(&context, &a).await, shared);
    for (key, version_id) in &small {
        let small = version(&context, key, *version_id).await;
        assert_eq!(content(&context, &small).await, key.as_bytes());
    }
    // The second write that lost to the shared copy is removed by reconciliation.
    let outcome = process_cleanup_batch(&context.driver).await.unwrap();
    assert_eq!(outcome.failed, 0);

    // A run after completion has nothing left to do.
    assert!(run(&context).await.is_none());
}

async fn clear_timers(context: &TestContext) {
    let storage = &context.driver.storage_handle;
    let Event::Storage(StorageEvent::IterResult { values, .. }) = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: usize::MAX,
            txn_id: None,
        })
        .await
    else {
        panic!("timer scan failed")
    };
    for (key, _) in values {
        storage
            .send_storage_effect(StorageEffect::Delete {
                key_space: TASK_TIMER_KEYSPACE.to_string(),
                key,
                txn_id: None,
            })
            .await;
    }
}

#[tokio::test]
async fn restart_resumes_migration() {
    // The setting change committed but no timer survived, as after a crash between the
    // two: the restart alone must finish the migration.
    let context = setup_context().await;
    let version_id = put(&context, "a.txt", &b"research data ".repeat(10_000)).await;
    let zstd = Compression::Zstd { level: 3 };
    let operation = PutCompressionOperation::new(BUCKET.to_string(), context.group_id, zstd, 1);
    drive(operation, &context.driver).await.unwrap();
    assert!(progress(&context).await.finished_at_ms.is_none());
    // The run consumed its timer, so no stored timer can resume the migration.
    clear_timers(&context).await;

    let task_handle = aruna_tasks::TaskHandle::new();
    let restarted = Arc::new(DriverContext {
        storage_handle: context.driver.storage_handle.clone(),
        net_handle: context.driver.net_handle.clone(),
        blob_handle: context.driver.blob_handle.clone(),
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    let shutdown = aruna_core::shutdown::Shutdown::new();
    start_task_queues(restarted, task_handle, JobsRuntime::new(), &shutdown).await;

    // A generous cap only detects lost progress; the run itself needs a few seconds.
    let finished = tokio::time::timeout(std::time::Duration::from_secs(300), async {
        loop {
            let record = progress(&context).await;
            if record.finished_at_ms.is_some() {
                return record;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the restart re-arms the migration");
    assert_eq!((finished.migrated, finished.failed), (1, 0));
    let encoding = version(&context, "a.txt", version_id).await.location_key();
    assert_eq!(encoding.unwrap().encoding, EncodingClass::Zstd { level: 3 });
    shutdown.token().cancel();
}

#[tokio::test]
async fn failed_version_retries() {
    // A backend fault fails the only version; once it recovers, the waiting retry pass
    // moves it without anyone sending the setting again.
    let context = setup_context().await;
    let data = b"research data ".repeat(10_000);
    let version_id = put(&context, "a.txt", &data).await;
    let [file] = blob_files(&context._temp_dir.path().join("blobstore"))
        .try_into()
        .unwrap();
    let hidden = file.with_extension("hidden");
    std::fs::rename(&file, &hidden).unwrap();
    let zstd = Compression::Zstd { level: 3 };
    let operation = PutCompressionOperation::new(BUCKET.to_string(), context.group_id, zstd, 1);
    drive(operation, &context.driver).await.unwrap();

    let wait = run(&context).await;

    let waiting = progress(&context).await;
    assert_eq!(wait, Some(std::time::Duration::from_secs(60)));
    assert_eq!((waiting.failed, waiting.retries), (1, 1));
    assert!(waiting.retry_at_ms.is_some() && waiting.finished_at_ms.is_none());
    // Before the retry is due, a run leaves the bucket alone.
    assert!(run(&context).await.is_some());
    assert_eq!(progress(&context).await, waiting);

    std::fs::rename(&hidden, &file).unwrap();
    let due = CompressionMigration {
        retry_at_ms: Some(0),
        ..waiting
    };
    write(
        &context,
        COMPRESSION_MIGRATION_KEYSPACE,
        BUCKET,
        &due.to_bytes().unwrap(),
    )
    .await;
    assert!(run(&context).await.is_none());

    let done = progress(&context).await;
    assert_eq!((done.migrated, done.failed, done.retries), (1, 0, 1));
    assert!(done.finished_at_ms.is_some() && done.retry_at_ms.is_none());
    let encoding = version(&context, "a.txt", version_id).await.location_key();
    assert_eq!(encoding.unwrap().encoding, EncodingClass::Zstd { level: 3 });
    // The finished migration left the queue, so later runs scan nothing.
    let queued = read(
        &context,
        COMPRESSION_QUEUE_KEYSPACE,
        BUCKET.as_bytes().to_vec(),
    )
    .await;
    assert!(queued.is_none());
}

async fn write(context: &TestContext, keyspace: &str, key: &str, value: &[u8]) {
    let event = context
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: keyspace.to_string(),
            key: key.as_bytes().to_vec().into(),
            value: value.to_vec().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}
