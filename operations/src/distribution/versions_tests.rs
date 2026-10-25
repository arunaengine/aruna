// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, UNIX_EPOCH};

use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::distribution::OriginalEpoch;
use aruna_core::structs::storage::distribution::inventory::VersionEntry;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_storage::storage::StorageHandle;

use super::*;
use crate::distribution::inventory::{AddedVersion, HeadChange, InventoryChange, InventoryUpdate};
use crate::distribution::test_store::{apply, context, install, open, range};
use crate::driver::drive;

fn set() -> Ulid {
    Ulid::from_bytes([7; 16])
}

fn id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

/// Adds version `seed` of `key` at `generation`, as its head when `head` holds.
async fn version(handle: &StorageHandle, key: &str, seed: u8, generation: u64, head: bool) {
    let entry = VersionEntry {
        marker: false,
        size: 3,
        referenced: false,
        identity: Some([seed; 32]),
        pending: false,
    };
    let mut change = InventoryChange::new(set(), key);
    change.added.push(AddedVersion {
        version_id: id(seed),
        epoch: OriginalEpoch(1),
        generation,
        entry,
    });
    if head {
        change.head = HeadChange::Set(HeadEntry {
            version_id: id(seed),
            epoch: OriginalEpoch(1),
            generation,
            version: entry,
        });
    }
    assert!(apply(handle, InventoryUpdate::new(change)).await.unwrap());
    let stored = BlobVersion::materialized(
        [seed; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        UNIX_EPOCH + Duration::from_secs(u64::from(seed)),
        UserId::local(id(99), RealmId::from_bytes([1; 32])),
        None,
    );
    let write = StorageEffect::Write {
        key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
        key: VersionKey::new("spire", key, id(seed))
            .to_bytes()
            .unwrap()
            .into(),
        value: stored.to_bytes().unwrap().into(),
        txn_id: None,
    };
    handle.send_storage_effect(write).await;
}

async fn seeded() -> (tempfile::TempDir, StorageHandle) {
    let (dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    version(&handle, "a", 1, 1, false).await;
    version(&handle, "a", 2, 2, true).await;
    version(&handle, "b", 3, 1, true).await;
    (dir, handle)
}

fn input(max_keys: usize) -> InventoryVersionsInput {
    InventoryVersionsInput {
        set_id: set(),
        bucket: "spire".to_string(),
        prefix: None,
        delimiter: None,
        key_marker: None,
        version_id_marker: None,
        max_keys,
        lower: String::new(),
        upper: None,
    }
}

fn listed(result: &ListVersionsResult) -> Vec<(String, Ulid, bool)> {
    (result.items.iter())
        .map(|item| match item {
            ListVersionsItem::Version {
                key,
                version_id,
                is_latest,
                ..
            }
            | ListVersionsItem::DeleteMarker {
                key,
                version_id,
                is_latest,
                ..
            } => (key.clone(), *version_id, *is_latest),
        })
        .collect()
}

async fn list(handle: &StorageHandle, input: InventoryVersionsInput) -> ListVersionsResult {
    drive(InventoryVersionsOperation::new(input), &context(handle))
        .await
        .unwrap()
}

#[tokio::test]
async fn newest_first_seeked() {
    let (_dir, handle) = seeded().await;
    let result = list(&handle, input(10)).await;
    assert_eq!(
        listed(&result),
        vec![
            ("a".to_string(), id(2), true),
            ("a".to_string(), id(1), false),
            ("b".to_string(), id(3), true),
        ]
    );
    assert!(!result.is_truncated);
}

#[tokio::test]
async fn markers_resume_inside_key() {
    let (_dir, handle) = seeded().await;
    let first = list(&handle, input(2)).await;
    assert!(first.is_truncated);
    assert_eq!(first.next_key_marker.as_deref(), Some("a"));
    assert_eq!(first.next_version_marker, Some(id(1)));
    let next = InventoryVersionsInput {
        key_marker: first.next_key_marker,
        version_id_marker: first.next_version_marker,
        ..input(2)
    };
    let second = list(&handle, next).await;
    assert_eq!(listed(&second), vec![("b".to_string(), id(3), true)]);
    let past_key = InventoryVersionsInput {
        key_marker: Some("a".to_string()),
        ..input(5)
    };
    assert_eq!(listed(&list(&handle, past_key).await).len(), 1);
}

#[tokio::test]
async fn range_bounds_listing() {
    let (_dir, handle) = seeded().await;
    let upper = InventoryVersionsInput {
        upper: Some("b".to_string()),
        ..input(10)
    };
    assert_eq!(listed(&list(&handle, upper).await).len(), 2);
    let lower = InventoryVersionsInput {
        lower: "b".to_string(),
        ..input(10)
    };
    assert_eq!(
        listed(&list(&handle, lower).await),
        vec![("b".to_string(), id(3), true)]
    );
}

#[tokio::test]
async fn folders_grouped_once() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    version(&handle, "d/x", 4, 1, true).await;
    version(&handle, "d/y", 5, 1, true).await;
    version(&handle, "e", 6, 1, true).await;
    let grouped = InventoryVersionsInput {
        delimiter: Some("/".to_string()),
        ..input(10)
    };
    let result = list(&handle, grouped).await;
    assert_eq!(result.common_prefixes, vec!["d/".to_string()]);
    assert_eq!(listed(&result), vec![("e".to_string(), id(6), true)]);
}

#[tokio::test]
async fn nil_marker_starts_at_key() {
    let (_dir, handle) = seeded().await;
    let start = InventoryVersionsInput {
        key_marker: Some("b".to_string()),
        version_id_marker: Some(Ulid::nil()),
        ..input(10)
    };
    assert_eq!(
        listed(&list(&handle, start).await),
        vec![("b".to_string(), id(3), true)]
    );
}
