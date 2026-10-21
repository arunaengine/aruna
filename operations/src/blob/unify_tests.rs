//! Tests duplicate file cleanup: paged previews, the kept file, queued releases and pins.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
use super::*;
use crate::blob::release::release_renamed;
use crate::s3::bucket::rename::tests::{BUCKET, access, group, put, realm, rule, setup};
use crate::tests::records::node;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, PATHS_INDEX_KEYSPACE};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer, HashIndex};
use aruna_core::structs::storage::format::{StoredFormat, StoredLayout};

fn file(path: &str, ulid: Ulid, hash: u8) -> BackendLocation {
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
        hashes: [(HASH_BLAKE3.to_string(), vec![hash; 32])].into(),
    }
}

/// Stores version `id` of `key` reading `location`, its location row, backend version and
/// the rule's row of its file.
async fn store(context: &DriverContext, key: &str, id: Ulid, location: &BackendLocation) {
    let hash = location.get_blake3().unwrap().try_into().unwrap();
    let version = BlobVersion::materialized(
        hash,
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
    let provider = version_key_bytes(location.ulid);
    put(context, BACKEND_VERSION_KEYSPACE, provider, b"p1".to_vec()).await;
    let imported = ImportedObject::written(id, key.to_string(), 10, None);
    let path = object_key(rule().import_id, &location.backend_path).to_vec();
    put(
        context,
        IMPORTED_OBJECT_KEYSPACE,
        path,
        imported.to_bytes().unwrap(),
    )
    .await;
    let index = HashIndex::new(hash, id, realm(), group(), node(4), BUCKET, key);
    put(
        context,
        PATHS_INDEX_KEYSPACE,
        index.to_bytes().unwrap(),
        Vec::new(),
    )
    .await;
    let head = CurrentVersionPointer::new(id).to_bytes().unwrap();
    let head_key = BlobHeadKey::new(BUCKET, key).to_bytes().unwrap();
    put(context, BLOB_HEAD_KEYSPACE, head_key, head).await;
}

fn version_key_bytes(ulid: Ulid) -> Vec<u8> {
    version_key(ulid).to_vec()
}

/// Two versions of `key` with the same bytes in an old and a current name.
async fn twins(context: &DriverContext, key: &str, at: u64) -> (BackendLocation, BackendLocation) {
    let (v1, v2) = (Ulid::from_parts(at, 1), Ulid::from_parts(at + 1, 1));
    let old = file(&format!("raw/{v1}_{key}"), Ulid::from_parts(at, 7), 1);
    let new = file(&format!("raw/{key}~{v2}"), Ulid::from_parts(at + 1, 7), 1);
    store(context, key, v1, &old).await;
    store(context, key, v2, &new).await;
    (old, new)
}

async fn load(context: &DriverContext) -> BucketAccess {
    let auth = access(context).await;
    BucketAccess::load(context, &auth, (realm(), group(), node(4)), BUCKET)
        .await
        .unwrap()
}

#[tokio::test]
async fn unifies_twin_files() {
    let (_directory, context) = setup().await;
    let (old, new) = twins(&context, "a.faa.gz", 10).await;
    let single = file("raw/b.faa.gz", Ulid::from_parts(30, 7), 2);
    store(&context, "b.faa.gz", Ulid::from_parts(30, 1), &single).await;
    let access = load(&context).await;

    let page = preview_page(&context, (BUCKET, ""), &access, (None, None, 100))
        .await
        .unwrap();
    assert_eq!((page.keys, page.bytes, page.complete), (1, 10, true));
    let [entry] = page.entries.as_slice() else {
        panic!("expected one key with copies, got {:?}", page.entries)
    };
    assert_eq!(
        (entry.keep.ulid, entry.copies.clone()),
        (new.ulid, vec![old.clone()])
    );

    let step = job_page(&context, (BUCKET, ""), &access, (None, u64::MAX))
        .await
        .unwrap();
    assert_eq!((step.done, step.finished), (1, true));
    let moved = read(
        &context,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new(BUCKET, "a.faa.gz", Ulid::from_parts(10, 1))
            .to_bytes()
            .unwrap()
            .into(),
    );
    let moved = BlobVersion::from_bytes(&moved.await.unwrap().unwrap()).unwrap();
    assert_eq!(moved.location_key(), new.location_key().ok());
    // The old file goes through the release of renamed files at its exact version.
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
    let works: Vec<_> = works
        .iter()
        .map(|(_, value)| BlobCleanupWork::from_bytes(value).unwrap())
        .collect();
    assert_eq!(
        works,
        [BlobCleanupWork::ReleaseRenamed {
            location: old,
            import_id: rule().import_id,
            version_id: Ulid::from_parts(10, 1),
            version: "p1".to_string(),
        }]
    );
    // Nothing is left to unify.
    let page = preview_page(&context, (BUCKET, ""), &access, (None, None, 100))
        .await
        .unwrap();
    assert_eq!(page.keys, 0);
}

#[tokio::test]
async fn pages_all_keys() {
    let (_directory, context) = setup().await;
    for index in 0..150u64 {
        twins(&context, &format!("k{index:03}.faa.gz"), 100 + 2 * index).await;
    }
    let access = load(&context).await;
    let (mut after, mut seen) = (None, Vec::new());
    loop {
        let page = preview_page(&context, (BUCKET, ""), &access, (None, after, 100))
            .await
            .unwrap();
        assert_eq!((page.keys, page.bytes), (150, 1500));
        seen.extend(page.entries.into_iter().map(|entry| entry.key));
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    seen.sort();
    let wanted: Vec<_> = (0..150)
        .map(|index| format!("k{index:03}.faa.gz"))
        .collect();
    assert_eq!(seen, wanted);
}

#[tokio::test]
async fn keeps_shared_copies() {
    let (_directory, context) = setup().await;
    let (old, _) = twins(&context, "a.faa.gz", 10).await;
    // Another key still reads the old file, so its release keeps it.
    let other = VersionKey::new(BUCKET, "c.faa.gz", Ulid::from_parts(40, 1));
    let version = BlobVersion::materialized(
        [1; 32],
        old.backend.clone(),
        old.format.encoding(),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    put(
        &context,
        BLOB_VERSIONS_KEYSPACE,
        other.to_bytes().unwrap(),
        version.to_bytes().unwrap(),
    )
    .await;
    let index = HashIndex::new(
        [1; 32],
        other.version_id,
        realm(),
        group(),
        node(4),
        BUCKET,
        "c.faa.gz",
    );
    put(
        &context,
        PATHS_INDEX_KEYSPACE,
        index.to_bytes().unwrap(),
        Vec::new(),
    )
    .await;
    let access = load(&context).await;
    job_page(&context, (BUCKET, ""), &access, (None, u64::MAX))
        .await
        .unwrap();
    let kept = release_renamed(
        &context,
        old.clone(),
        rule().import_id,
        Ulid::from_parts(10, 1),
        "p1".to_string(),
    )
    .await;
    assert!(kept);
    let row = read(
        &context,
        BLOB_LOCATIONS_KEYSPACE,
        old.location_key().unwrap().to_bytes().into(),
    );
    assert!(row.await.unwrap().is_some());
}
