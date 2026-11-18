//! Shares the single-node blob fixture of the storage worker tests: one bucket, puts and row reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::UNIX_EPOCH;

use aruna_blob::blob::{BlobHandle, BlobHandler};
use aruna_core::UserId;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use byteview::ByteView;

use crate::convergence::TestResult;
use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, COMPRESSION_MIGRATION_KEYSPACE,
    REALM_CONFIG_KEYSPACE,
};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BackendLocation, BlobVersion, BucketInfo, VersionKey,
};
use aruna_core::structs::storage::format::{Compression, CompressionMigration};
use aruna_core::structs::storage::routing::{RoutingSnapshot, StorageRoutingRule};
use aruna_net::NetHandle;
use aruna_operations::blob::migration::drain::process_migrations;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_storage::{StorageHandle, storage};
use futures_util::TryStreamExt;
use tempfile::TempDir;
use ulid::Ulid;

pub const BUCKET: &str = "bucket";

pub struct TestContext {
    pub temp_dir: TempDir,
    pub driver: DriverContext,
    pub group_id: Ulid,
    pub user_id: UserId,
}

pub fn realm_id() -> RealmId {
    RealmId::from_bytes([7u8; 32])
}

/// A node whose only blob backend is a file system under `blobstore`.
pub async fn setup_context() -> TestContext {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_root = temp_dir.path().to_str().unwrap();
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(
        aruna_operations::testing::local_net(),
        storage_handle.clone(),
    )
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
    setup_on(temp_dir, storage_handle, net_handle, blob_handle).await
}

/// Builds the runner context over the given handles and creates the empty bucket.
pub async fn setup_on(
    temp_dir: TempDir,
    storage_handle: StorageHandle,
    net_handle: NetHandle,
    blob_handle: BlobHandle,
) -> TestContext {
    let driver = DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let group_id = Ulid::generate();
    let user_id = UserId::local(Ulid::generate(), realm_id());
    let info = bucket_info(group_id, user_id, Vec::new());
    drive(
        CreateBucketOperation::new(BUCKET.to_string(), info),
        &driver,
    )
    .await
    .unwrap();
    TestContext {
        temp_dir,
        driver,
        group_id,
        user_id,
    }
}

pub fn bucket_info(group_id: Ulid, user_id: UserId, rules: Vec<StorageRoutingRule>) -> BucketInfo {
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

pub fn stream(bytes: &[u8]) -> BackendStream<Result<bytes::Bytes, StreamError>> {
    BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
        bytes.to_vec(),
    )))
}

pub async fn put(context: &TestContext, key: &str, bytes: &[u8]) -> Ulid {
    let operation = PutObjectOperation::new(PutObjectConfig {
        user_id: context.user_id,
        group_id: context.group_id,
        realm_id: realm_id(),
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

pub async fn read(context: &TestContext, keyspace: &str, key: Vec<u8>) -> Option<Vec<u8>> {
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

pub async fn write(context: &TestContext, keyspace: &str, key: &str, value: &[u8]) {
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

pub async fn version(context: &TestContext, key: &str, version_id: Ulid) -> BlobVersion {
    let key = VersionKey::new(BUCKET, key, version_id).to_bytes().unwrap();
    BlobVersion::from_bytes(&read(context, BLOB_VERSIONS_KEYSPACE, key).await.unwrap()).unwrap()
}

/// The stored bytes of a version, read from the backend that holds its copy.
pub async fn content(context: &TestContext, version: &BlobVersion) -> Vec<u8> {
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
pub async fn run(context: &TestContext) -> Option<std::time::Duration> {
    process_migrations(&context.driver, None)
        .await
        .unwrap()
        .next
}

/// The stored progress of the bucket's background migration.
pub async fn progress(context: &TestContext) -> CompressionMigration {
    let key = BUCKET.as_bytes().to_vec();
    let value = read(context, COMPRESSION_MIGRATION_KEYSPACE, key).await;
    CompressionMigration::from_bytes(&value.unwrap()).unwrap()
}

/// Every stored blob file under `dir`.
pub fn blob_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
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

/// Stores a single-node realm config, with the actor's node as a server, on that node.
pub async fn seed_config(storage: &StorageHandle, actor: &Actor) -> TestResult<()> {
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    config.seed_job_control(actor.node_id, 0);
    write_value(
        storage,
        REALM_CONFIG_KEYSPACE,
        actor.realm_id.as_bytes().to_vec(),
        config.to_bytes(actor)?,
    )
    .await
}

/// Writes one row and fails on any answer other than a write result.
pub async fn write_value(
    storage: &StorageHandle,
    key_space: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> TestResult<()> {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Err(format!("unexpected storage write event: {event:?}").into()),
    }
}
