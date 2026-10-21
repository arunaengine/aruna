//! Tests the linked-storage files of purged versions: pins, planned releases and the pause.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
use super::*;
use crate::s3::bucket::rename::tests::{BUCKET, group, put, realm, rule, setup};
use crate::tests::records::node;
use aruna_core::keyspaces::PATHS_INDEX_KEYSPACE;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::format::{StoredFormat, StoredLayout};
use std::time::SystemTime;

fn file(path: &str, ulid: Ulid) -> BackendLocation {
    BackendLocation {
        backend: BackendRef::Group(rule().backend_id),
        storage_class: None,
        root: String::new(),
        storage_bucket: String::new(),
        backend_path: path.to_string(),
        ulid,
        format: StoredFormat {
            layout: StoredLayout::Plain(ulid),
            ..StoredFormat::default()
        },
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 10,
        hashes: [(HASH_BLAKE3.to_string(), vec![3; 32])].into(),
    }
}

/// Stores version `id` of `key` reading `location`, and optionally its backend version.
async fn store(
    context: &DriverContext,
    (key, id): (&str, Ulid),
    location: &BackendLocation,
    provider: Option<&str>,
) {
    let version = BlobVersion::materialized(
        [3; 32],
        location.backend.clone(),
        location.format.encoding(),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let version_key = VersionKey::new(BUCKET, key, id).to_bytes().unwrap();
    put(
        context,
        BLOB_VERSIONS_KEYSPACE,
        version_key,
        version.to_bytes().unwrap(),
    )
    .await;
    let row = location.location_key().unwrap().to_bytes();
    put(
        context,
        BLOB_LOCATIONS_KEYSPACE,
        row,
        location.to_bytes().unwrap(),
    )
    .await;
    if let Some(provider) = provider {
        let key = version_key_bytes(location.ulid);
        put(
            context,
            BACKEND_VERSION_KEYSPACE,
            key,
            provider.as_bytes().to_vec(),
        )
        .await;
    }
    let index = HashIndex::new([3; 32], id, realm(), group(), node(4), BUCKET, key);
    put(
        context,
        PATHS_INDEX_KEYSPACE,
        index.to_bytes().unwrap(),
        Vec::new(),
    )
    .await;
}

fn version_key_bytes(ulid: Ulid) -> Vec<u8> {
    version_key(ulid).to_vec()
}

async fn record(context: &DriverContext, import_id: Ulid, path: &str, (key, id): (&str, Ulid)) {
    let row = ImportedObject::written(id, key.to_string(), 10, None);
    let row_key = object_key(import_id, path).to_vec();
    put(
        context,
        IMPORTED_OBJECT_KEYSPACE,
        row_key,
        row.to_bytes().unwrap(),
    )
    .await;
}

fn scope() -> StoragePurgeScope {
    StoragePurgeScope::Prefix {
        bucket: BUCKET.to_string(),
        prefix: "gone/".to_string(),
    }
}

#[tokio::test]
async fn finds_pinned_files() {
    let (_directory, context) = setup().await;
    let (a, b, c) = (
        Ulid::from_parts(10, 1),
        Ulid::from_parts(11, 1),
        Ulid::from_parts(12, 1),
    );
    // A free file, one read by a key outside the scope, one another link records.
    let free = file("raw/gone/a", Ulid::from_parts(20, 1));
    let shared = file("raw/gone/b", Ulid::from_parts(21, 1));
    let claimed = file("raw/gone/c", Ulid::from_parts(22, 1));
    store(&context, ("gone/a", a), &free, Some("p1")).await;
    store(&context, ("gone/b", b), &shared, Some("p2")).await;
    store(&context, ("kept/b", Ulid::from_parts(13, 1)), &shared, None).await;
    store(&context, ("gone/c", c), &claimed, Some("p3")).await;
    record(&context, rule().import_id, "raw/gone/a", ("gone/a", a)).await;
    let mut other = rule();
    other.import_id = Ulid::from_parts(2, 9);
    put(
        &context,
        aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE,
        other.key().to_vec(),
        other.to_bytes().unwrap(),
    )
    .await;
    record(
        &context,
        other.import_id,
        "raw/gone/c",
        ("other/c", Ulid::from_parts(14, 1)),
    )
    .await;
    let versions =
        [("gone/a", a), ("gone/b", b), ("gone/c", c)].map(|(key, id)| (key.to_string(), id));

    let found = storage_files(&context, (BUCKET, group()), &versions, Some(&scope()))
        .await
        .unwrap();
    let kept: Vec<_> = found
        .iter()
        .map(|file| (file.location.backend_path.as_str(), file.kept.is_some()))
        .collect();
    assert_eq!(
        kept,
        [
            ("raw/gone/a", false),
            ("raw/gone/b", true),
            ("raw/gone/c", true)
        ]
    );
    assert_eq!(found[0].owner, Some((rule().import_id, a)));
    assert_eq!(found[0].version.as_deref(), Some("p1"));
}

#[tokio::test]
async fn hands_over_plans() {
    let (_directory, context) = setup().await;
    let job = Ulid::from_parts(50, 1);
    let id = Ulid::from_parts(10, 1);
    let unversioned = file("raw/gone/x", Ulid::from_parts(21, 1));
    let versioned = file("raw/gone/a", Ulid::from_parts(20, 1));
    store(&context, ("gone/a", id), &versioned, Some("p1")).await;
    store(&context, ("gone/x", id), &unversioned, None).await;
    let versions = [("gone/a", id), ("gone/x", id)].map(|(key, id)| (key.to_string(), id));
    let found = storage_files(&context, (BUCKET, group()), &versions, None)
        .await
        .unwrap();
    // Planning twice, as a resumed purge does, plans each file once.
    plan_files(&context, job, &found).await.unwrap();
    plan_files(&context, job, &found).await.unwrap();
    assert_eq!(hand_over(&context, job).await.unwrap(), 1);
    assert_eq!(hand_over(&context, job).await.unwrap(), 0);
    let (works, _) = iter_prefix_page(
        &context.storage_handle,
        BLOB_CLEANUP_KEYSPACE,
        None,
        None,
        10,
        None,
    )
    .await
    .unwrap();
    let [(_, work)] = works.as_slice() else {
        panic!("expected one release, got {works:?}")
    };
    let work = BlobCleanupWork::from_bytes(work).unwrap();
    assert!(matches!(
        work,
        BlobCleanupWork::ReleaseRenamed { location, version, .. }
            if location == versioned && version == "p1"
    ));
}

#[tokio::test]
async fn pauses_purges() {
    let (_directory, context) = setup().await;
    let job = Ulid::from_parts(50, 1);
    assert!(!is_paused(&context, job).await.unwrap());
    set_paused(&context, job, true).await.unwrap();
    assert!(is_paused(&context, job).await.unwrap());
    // A pause marker is not a planned file.
    assert_eq!(hand_over(&context, job).await.unwrap(), 0);
    set_paused(&context, job, false).await.unwrap();
    assert!(!is_paused(&context, job).await.unwrap());
}
