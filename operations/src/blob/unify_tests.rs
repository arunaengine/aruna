//! Tests duplicate file cleanup: paged previews, the kept file, queued releases and pins.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
use super::*;
use crate::blob::release::release_renamed;
use crate::s3::bucket::rename::tests::{BUCKET, access, group, put, realm, rule, setup, store_job};
use crate::tests::records::node;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, PATHS_INDEX_KEYSPACE};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer, HashIndex};
use aruna_core::structs::storage::format::{StoredFormat, StoredLayout};
use aruna_core::structs::storage::linked_job::LinkedTask;

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

    let step = job_page(&context, (BUCKET, ""), &access, &unifying(&context).await)
        .await
        .unwrap();
    assert!(step.finished);
    assert_eq!(job_row(&context).await.done, 1);
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
    job_page(&context, (BUCKET, ""), &access, &unifying(&context).await)
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

async fn unifying(context: &DriverContext) -> LinkedJob {
    let task = LinkedTask::Unify {
        prefix: String::new(),
    };
    store_job(context, task, u64::MAX).await
}

async fn job_row(context: &DriverContext) -> LinkedJob {
    let key = BUCKET.as_bytes().to_vec().into();
    let value = read(context, LINKED_JOB_KEYSPACE, key)
        .await
        .unwrap()
        .unwrap();
    LinkedJob::from_bytes(&value).unwrap()
}

async fn cleanup_rows(context: &DriverContext) -> usize {
    let page = iter_prefix_page(
        &context.storage_handle,
        BLOB_CLEANUP_KEYSPACE,
        None,
        None,
        10,
        None,
    );
    page.await.unwrap().0.len()
}

fn first_version() -> Vec<u8> {
    VersionKey::new(BUCKET, "a.faa.gz", Ulid::from_parts(10, 1))
        .to_bytes()
        .unwrap()
}

#[tokio::test]
async fn keeps_foreign_rows() {
    let (_directory, context) = setup().await;
    let (old, _) = twins(&context, "a.faa.gz", 10).await;
    // The rule records the old file for another key, so it is that key's file.
    let foreign = ImportedObject::written(Ulid::from_parts(60, 1), "z".to_string(), 10, None);
    let path = object_key(rule().import_id, &old.backend_path).to_vec();
    put(
        &context,
        IMPORTED_OBJECT_KEYSPACE,
        path,
        foreign.to_bytes().unwrap(),
    )
    .await;
    let access = load(&context).await;
    let step = job_page(&context, (BUCKET, ""), &access, &unifying(&context).await)
        .await
        .unwrap();
    assert_eq!(step.skipped, 1);
    assert_eq!(cleanup_rows(&context).await, 0);
    let version = read(&context, BLOB_VERSIONS_KEYSPACE, first_version().into());
    let version = BlobVersion::from_bytes(&version.await.unwrap().unwrap()).unwrap();
    assert_eq!(version.location_key(), old.location_key().ok());
}

#[tokio::test]
async fn moves_copy_records() {
    use aruna_core::structs::storage::blob::ManagedCopyState;
    let (_directory, context) = setup().await;
    let (old, new) = twins(&context, "a.faa.gz", 10).await;
    let version = VersionKey::new(BUCKET, "a.faa.gz", Ulid::from_parts(10, 1));
    let record = ManagedCopyRecord::new(
        version,
        node(4),
        old,
        Vec::new(),
        0,
        ManagedCopyState::Registered,
    )
    .unwrap();
    let key = record.key().to_bytes().unwrap();
    put(
        &context,
        MANAGED_COPY_KEYSPACE,
        key.clone(),
        record.to_bytes().unwrap(),
    )
    .await;
    let access = load(&context).await;
    job_page(&context, (BUCKET, ""), &access, &unifying(&context).await)
        .await
        .unwrap();
    let moved = read(&context, MANAGED_COPY_KEYSPACE, key.into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ManagedCopyRecord::from_bytes(&moved).unwrap().location, new);
}

#[tokio::test]
async fn stops_when_paused() {
    use aruna_core::structs::storage::linked_job::LinkedState;
    let (_directory, context) = setup().await;
    twins(&context, "a.faa.gz", 10).await;
    let access = load(&context).await;
    let running = unifying(&context).await;
    let paused = LinkedJob {
        state: LinkedState::Paused,
        ..running.clone()
    };
    let key = BUCKET.as_bytes().to_vec();
    put(
        &context,
        LINKED_JOB_KEYSPACE,
        key,
        paused.to_bytes().unwrap(),
    )
    .await;
    // A page that read the job before the pause changes nothing.
    let step = job_page(&context, (BUCKET, ""), &access, &running)
        .await
        .unwrap();
    assert_eq!((step.done, step.finished), (0, false));
    assert_eq!(cleanup_rows(&context).await, 0);
    assert_eq!(job_row(&context).await, paused);
}

#[tokio::test]
async fn pages_within_keys() {
    // A key with more duplicate contents than a page holds continues on the next page.
    let (_directory, context) = setup().await;
    for (tag, at) in [(1u8, 10u64), (2, 20)] {
        let (v1, v2) = (Ulid::from_parts(at, 1), Ulid::from_parts(at + 1, 1));
        let old = file(&format!("raw/{v1}_k"), Ulid::from_parts(at, 7), tag);
        let new = file(&format!("raw/k~{v2}"), Ulid::from_parts(at + 1, 7), tag);
        store(&context, "k", v1, &old).await;
        store(&context, "k", v2, &new).await;
    }
    let access = load(&context).await;
    let first = preview_page(&context, (BUCKET, ""), &access, (None, None, 1))
        .await
        .unwrap();
    assert_eq!((first.keys, first.entries.len()), (1, 1));
    assert_eq!(first.next, Some(("k".to_string(), Some(1))));
    let second = preview_page(&context, (BUCKET, ""), &access, (None, first.next, 1))
        .await
        .unwrap();
    assert_eq!(second.entries.len(), 1);
    assert_ne!(second.entries[0].keep.ulid, first.entries[0].keep.ulid);
    assert_eq!(second.next, None);
}
