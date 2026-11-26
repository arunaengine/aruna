//! Resolves blob hashes to their indexed paths against a real storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::blob_paths::ResolvePathsOperation;
use aruna_core::blob_records::{HeadAliasContext, add_index_effect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::HashIndex;
use aruna_runner::driver::{DriverContext, drive};
use aruna_storage::storage;
use ulid::Ulid;

#[tokio::test]
async fn resolves_empty_hash() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
        vfs_handle: None,
    };

    let result = drive(ResolvePathsOperation::new([1u8; 32]), &context)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn resolves_existing_aliases() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let context = DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
        vfs_handle: None,
    };

    let hash = [8u8; 32];
    let alias = HashIndex::new(
        hash,
        Ulid::from_bytes([4u8; 16]),
        RealmId::from_bytes([1u8; 32]),
        Ulid::from_bytes([2u8; 16]),
        iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
        "bucket",
        "path/file.txt",
    );
    let effect = add_index_effect(
        &HeadAliasContext::new(
            alias.realm_id,
            alias.group_id,
            alias.node_id,
            alias.bucket.clone(),
            alias.key.clone(),
        ),
        hash,
        alias.version_id,
    )
    .unwrap();
    let event = context.storage_handle.send_effect(effect).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let result = drive(ResolvePathsOperation::new(hash), &context)
        .await
        .unwrap();
    assert_eq!(result, vec![alias]);
}
