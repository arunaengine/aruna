//! Tests listing a range's open uploads: range bounds, delimiters and upload id markers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_storage::storage::StorageHandle;

use super::*;
use crate::distribution::inventory::{InventoryChange, InventoryUpdate};
use crate::distribution::test_store::{apply, context, install, open, range};
use crate::driver::drive;

fn set() -> Ulid {
    Ulid::from_bytes([8; 16])
}

fn id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

/// Opens upload `seed` of `key` in the inventory with its record in `status`.
async fn upload(handle: &StorageHandle, key: &str, seed: u8, status: MultipartUploadStatus) {
    let mut change = InventoryChange::new(set(), key);
    change.upload = Some((id(seed), true));
    assert!(apply(handle, InventoryUpdate::new(change)).await.unwrap());
    let record = MultipartUpload {
        backend: BackendRef::node_default(),
        storage_class: None,
        upload_id: id(seed),
        bucket: "b".to_string(),
        key: key.to_string(),
        group_id: id(90),
        created_by: UserId::local(id(91), RealmId::from_bytes([1; 32])),
        created_at: SystemTime::UNIX_EPOCH,
        status,
        checksum_hint: None,
        metadata: Default::default(),
        placement_policies: Vec::new(),
        subject_generation: 0,
        completing_since_ms: None,
        backend_upload: None,
        encryption: None,
    };
    let write = StorageEffect::Write {
        key_space: UPLOAD_KEYSPACE.to_string(),
        key: id(seed).to_bytes().to_vec().into(),
        value: record.to_bytes().unwrap().into(),
        txn_id: None,
    };
    handle.send_storage_effect(write).await;
}

fn input(lower: &str, upper: Option<&str>, max_uploads: usize) -> InventoryUploadsInput {
    InventoryUploadsInput {
        set_id: set(),
        bucket: "b".to_string(),
        prefix: None,
        delimiter: None,
        key_marker: None,
        upload_id_marker: None,
        max_uploads,
        lower: lower.to_string(),
        upper: upper.map(str::to_string),
    }
}

async fn list(handle: &StorageHandle, input: InventoryUploadsInput) -> ListUploadsResult {
    drive(InventoryUploadsOperation::new(input), &context(handle))
        .await
        .unwrap()
}

fn listed(result: &ListUploadsResult) -> Vec<(String, Ulid)> {
    (result.uploads.iter())
        .map(|upload| (upload.key.clone(), upload.upload_id))
        .collect()
}

async fn fixture() -> (tempfile::TempDir, StorageHandle) {
    let (dir, handle) = open();
    install(
        &handle,
        set(),
        &[range(1, "", Some("m")), range(2, "m", None)],
    )
    .await;
    upload(&handle, "a", 3, MultipartUploadStatus::Open).await;
    upload(&handle, "a", 4, MultipartUploadStatus::Open).await;
    upload(&handle, "d/x", 5, MultipartUploadStatus::Open).await;
    upload(&handle, "d/y", 6, MultipartUploadStatus::Open).await;
    upload(&handle, "k", 7, MultipartUploadStatus::Completing).await;
    upload(&handle, "q", 8, MultipartUploadStatus::Open).await;
    (dir, handle)
}

#[tokio::test]
async fn pages_respect_range() {
    let (_dir, handle) = fixture().await;
    let first = list(&handle, input("", Some("m"), 2)).await;
    assert_eq!(listed(&first), [("a".into(), id(3)), ("a".into(), id(4))]);
    assert!(first.is_truncated);
    assert_eq!(first.next_upload_marker, Some(id(4)));

    let mut next = input("", Some("m"), 10);
    next.key_marker = first.next_key_marker;
    next.upload_id_marker = first.next_upload_marker;
    let rest = list(&handle, next).await;
    // The completing upload of `k` is not listed; `q` lies in the other range.
    assert_eq!(
        listed(&rest),
        [("d/x".into(), id(5)), ("d/y".into(), id(6))]
    );
    assert!(!rest.is_truncated);
}

#[tokio::test]
async fn delimiter_groups_once() {
    let (_dir, handle) = fixture().await;
    let mut grouped = input("", None, 3);
    grouped.delimiter = Some("/".to_string());
    grouped.key_marker = Some("a".to_string());
    let page = list(&handle, grouped).await;
    assert_eq!(page.common_prefixes, ["d/"]);
    assert_eq!(listed(&page), [("q".into(), id(8))]);
    assert!(!page.is_truncated);
}

#[tokio::test]
async fn nil_includes_key() {
    let (_dir, handle) = fixture().await;
    let mut from = input("", None, 1);
    from.key_marker = Some("d/x".to_string());
    from.upload_id_marker = Some(Ulid::nil());
    let page = list(&handle, from).await;
    assert_eq!(listed(&page), [("d/x".into(), id(5))]);
    assert_eq!(page.next_key_marker.as_deref(), Some("d/x"));
}
