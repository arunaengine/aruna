//! Tests removing drained backends: removed when empty, kept when enabled or still named.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BACKEND_INDEX_KEYSPACE, BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::structs::storage::blob::{BackendLocation, BlobCleanupWork, BlobLocationKey};
use aruna_core::structs::storage::format::{EncodingClass, StoredFormat};
use aruna_core::structs::storage::group_backend::GroupStorageSecret;
use aruna_core::structs::storage::multipart::{MultipartUpload, MultipartUploadStatus};
use aruna_core::types::Key;
use tempfile::tempdir;
use ulid::Ulid;

use super::*;
use crate::groups::backends::remove::tests::record;
use crate::groups::backends::{backend_key, index_key};

fn context(root: &str) -> DriverContext {
    let storage_handle = aruna_storage::FjallStorage::open(root).unwrap();
    storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
    DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        outbound_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

async fn write(context: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn seed(context: &DriverContext, backend_id: Ulid, disabled: bool) {
    let stored = record(backend_id, disabled);
    for (key_space, key, value) in super::super::record_writes(&stored).unwrap() {
        write(context, &key_space, key, value.to_vec()).await;
    }
    let secret = GroupStorageSecret {
        backend_id,
        secret_config: HashMap::new(),
        updated_at: SystemTime::UNIX_EPOCH,
    };
    let entry = GroupStorageSecret::vault_entry(backend_id);
    let effect = crate::node_vault::write_secret(entry, secret.to_secret().unwrap(), None);
    context.storage_handle.send_effect(effect).await;
}

async fn stored_secret(context: &DriverContext, backend_id: Ulid) -> Option<GroupStorageSecret> {
    let entry = GroupStorageSecret::vault_entry(backend_id);
    let event = context
        .storage_handle
        .send_effect(crate::node_vault::read_secret(entry, None))
        .await;
    crate::node_vault::parse_secret(event, entry, GroupStorageSecret::from_bytes).unwrap()
}

async fn read(context: &DriverContext, key_space: &str, key: Key) -> Option<Vec<u8>> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|value| value.to_vec()),
        other => panic!("unexpected read event: {other:?}"),
    }
}

#[tokio::test]
async fn removes_drained_backend() {
    // Record, index and secret go together once nothing names the backend.
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    let backend_id = Ulid::from_bytes([2u8; 16]);
    seed(&ctx, backend_id, true).await;

    assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 1);

    assert!(
        read(&ctx, STORAGE_BACKEND_KEYSPACE, backend_key(backend_id))
            .await
            .is_none()
    );
    assert!(stored_secret(&ctx, backend_id).await.is_none());
    assert!(
        read(
            &ctx,
            BACKEND_INDEX_KEYSPACE,
            index_key(Ulid::from_bytes([1u8; 16]), backend_id)
        )
        .await
        .is_none()
    );
}

#[tokio::test]
async fn enabled_backend_survives() {
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    let backend_id = Ulid::from_bytes([3u8; 16]);
    seed(&ctx, backend_id, false).await;

    assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 0);
    assert!(
        read(&ctx, STORAGE_BACKEND_KEYSPACE, backend_key(backend_id))
            .await
            .is_some()
    );
    assert!(stored_secret(&ctx, backend_id).await.is_some());
}

#[tokio::test]
async fn holding_backend_survives() {
    // A stored copy or a queued physical delete both keep the record alive.
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    let backend_id = Ulid::from_bytes([4u8; 16]);
    seed(&ctx, backend_id, true).await;
    let location = BackendLocation {
        backend: BackendRef::Group(backend_id),
        storage_class: None,
        root: "/data".to_string(),
        storage_bucket: "storage".to_string(),
        backend_path: "bucket/key_01".to_string(),
        ulid: Ulid::from_bytes([5u8; 16]),
        format: StoredFormat::default(),
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 10,
        hashes: HashMap::new(),
    };
    write(
        &ctx,
        BLOB_LOCATIONS_KEYSPACE,
        BlobLocationKey::new([6u8; 32], EncodingClass::Raw, BackendRef::Group(backend_id))
            .to_bytes()
            .into(),
        location.to_bytes().unwrap(),
    )
    .await;

    assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 0);

    // Every queued work that names a location of the storage keeps it, also the newer ones.
    let queued = [
        BlobCleanupWork::DeleteBlob {
            location: location.clone(),
        },
        BlobCleanupWork::ReleasePlanned {
            job_id: Ulid::from_bytes([9u8; 16]),
            location: location.clone(),
            import_id: Ulid::nil(),
            version_id: Ulid::nil(),
            version: "v1".to_string(),
        },
        BlobCleanupWork::SettleUpload {
            attempt: Ulid::from_bytes([9u8; 16]),
            location,
            upload_id: "upload".to_string(),
            since_ms: 0,
        },
    ];
    for queued in queued {
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        seed(&ctx, backend_id, true).await;
        write(
            &ctx,
            BLOB_CLEANUP_KEYSPACE,
            Ulid::from_bytes([7u8; 16]).to_bytes().to_vec().into(),
            queued.to_bytes().unwrap(),
        )
        .await;
        assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 0);
    }

    // An open upload's parts have no location row, yet its abort still needs
    // the credentials to delete them.
    let upload_id = Ulid::from_bytes([8u8; 16]);
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    seed(&ctx, backend_id, true).await;
    write(
        &ctx,
        UPLOAD_KEYSPACE,
        upload_id.to_bytes().to_vec().into(),
        MultipartUpload {
            upload_id,
            backend: BackendRef::Group(backend_id),
            storage_class: None,
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            group_id: Ulid::from_bytes([1u8; 16]),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
            backend_upload: None,
            encryption: None,
        }
        .to_bytes()
        .unwrap(),
    )
    .await;

    assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 0);
}
