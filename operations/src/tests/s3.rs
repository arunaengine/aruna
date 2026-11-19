//! Opens temporary storage and full contexts for the S3 tests, with shared seeds, stored versions
//! and a reference server.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_blob::blob::BlobHandler;
use aruna_core::effects::StorageEffect;
use aruna_core::egress::EgressPolicy;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
};
use aruna_net::NetHandle;
use aruna_storage::storage;
use axum::Router;
use axum::routing::get;
use tokio::net::TcpListener;
use ulid::Ulid;

use crate::driver::DriverContext;
use aruna_core::keyspaces::UPLOAD_KEYSPACE;
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_storage::storage::{FjallStorage, StorageHandle};
use tempfile::{TempDir, tempdir};

pub fn test_storage() -> (TempDir, StorageHandle) {
    let directory = tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    (directory, storage)
}

/// Stores a multipart upload record as a test seed.
pub async fn seed_upload(storage: &StorageHandle, record: &MultipartUpload) {
    let write = StorageEffect::Write {
        key_space: UPLOAD_KEYSPACE.to_string(),
        key: record.upload_id.to_bytes().to_vec().into(),
        value: record.to_bytes().unwrap().into(),
        txn_id: None,
    };
    storage.send_storage_effect(write).await;
}

pub async fn full_context() -> (TempDir, DriverContext) {
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
    let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::with_egress(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100000),
            multipart_bucket: Some("multipart".to_string()),
            root: blob_root,
            service_config: HashMap::new(),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::loopback(),
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
    (temp_handle, context)
}

pub async fn spawn_reference_server(body: &'static [u8]) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().route(
        "/folder/file.txt",
        get(move || async move { ([("content-type", "text/plain"), ("etag", "etag-1")], body) }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

pub async fn write_version(context: &DriverContext, bucket: &str, key: &str, version: BlobVersion) {
    let version_id = Ulid::generate();
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        panic!("failed to start transaction");
    };
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
            value: CurrentVersionPointer::new(version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: Some(txn_id),
        })
        .await;
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new(bucket, key, version_id)
                .to_bytes()
                .unwrap()
                .into(),
            value: version.to_bytes().unwrap().into(),
            txn_id: Some(txn_id),
        })
        .await;
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await;
}
