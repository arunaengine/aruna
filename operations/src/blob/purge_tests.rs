//! Tests the linked-storage files of purged versions: pins, planned releases and the pause.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
use super::*;
use crate::blob::migration::misplaced::AnyKey;
use crate::driver::{DriverContext, drive, node_routing};
use crate::s3::bucket::rename::tests::{BUCKET, group, put, realm, rule, setup};
use crate::tests::records::node;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, PATHS_INDEX_KEYSPACE};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::storage::blob::{BackendRef, BlobHeadKey, CurrentVersionPointer};
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

    let found = drive(
        StorageFilesOperation::new(
            node_routing(&context),
            (BUCKET, group()),
            &versions,
            Some(&scope()),
        ),
        &context,
    )
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
async fn plans_until_ended() {
    let (_directory, context) = setup().await;
    let job = aruna_core::structs::execution::job::JobId::from_bytes([0xE5; 16]).as_ulid();
    let id = Ulid::from_parts(10, 1);
    let unversioned = file("raw/gone/x", Ulid::from_parts(21, 1));
    let versioned = file("raw/gone/a", Ulid::from_parts(20, 1));
    store(&context, ("gone/a", id), &versioned, Some("p1")).await;
    store(&context, ("gone/x", id), &unversioned, None).await;
    let versions = [("gone/a", id), ("gone/x", id)].map(|(key, id)| (key.to_string(), id));
    let found = drive(
        StorageFilesOperation::new(node_routing(&context), (BUCKET, group()), &versions, None),
        &context,
    )
    .await
    .unwrap();
    let storage = aruna_core::structs::storage::group_backend::GroupStorage {
        backend_id: rule().backend_id,
        group_id: group(),
        name: "tenant".to_string(),
        kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
        public_config: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        disabled: false,
        cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
    };
    let backend = crate::groups::backends::backend_key(rule().backend_id).to_vec();
    let space = aruna_core::keyspaces::STORAGE_BACKEND_KEYSPACE;
    put(&context, space, backend, storage.to_bytes().unwrap()).await;
    // Planning twice, as a resumed purge does, plans each file once, in the cleanup queue.
    drive(PlanFilesOperation::new(job, &found), &context)
        .await
        .unwrap();
    drive(PlanFilesOperation::new(job, &found), &context)
        .await
        .unwrap();
    let works = || async {
        let page = aruna_storage::scan::iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            None,
            10,
            None,
        );
        page.await.unwrap().0
    };
    let planned = works().await;
    let [(_, work)] = planned.as_slice() else {
        panic!("expected one release, got {planned:?}")
    };
    let work = BlobCleanupWork::from_bytes(work).unwrap();
    assert!(matches!(
        work,
        BlobCleanupWork::ReleasePlanned { job_id, location, version, .. }
            if job_id == job && location == versioned && version == "p1"
    ));
    // While the purge job runs, its planned files wait; a job that is gone has ended.
    let record = aruna_core::structs::execution::job::JobRecord::new(
        aruna_core::structs::execution::job::JobId::from_bytes(job.to_bytes()),
        aruna_core::structs::execution::job::JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        },
        Default::default(),
        node(4),
        1,
        1,
        None,
    );
    crate::jobs::store::test_calls::insert_job(&context, &record)
        .await
        .unwrap();
    crate::driver::drive(
        crate::blob::cleanup::ProcessCleanupOperation::new(crate::blob::cleanup::CleanupNode {
            routing: crate::driver::node_routing(&context),
            has_blob: (&context).blob_handle.is_some(),
        }),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(works().await.len(), 1);
    let claim = aruna_core::structs::storage::backend_import::claim_key(&versioned, "p1").unwrap();
    let claimed = |context: &DriverContext| {
        let read = vec![(
            aruna_core::keyspaces::DELETE_CLAIM_KEYSPACE.to_string(),
            claim.clone(),
        )];
        let context = context.clone();
        async move {
            let read = aruna_core::effects::StorageEffect::BatchRead {
                reads: read,
                txn_id: None,
            };
            let answer = context.storage_handle.send_storage_effect(read).await;
            let aruna_core::events::Event::Storage(
                aruna_core::events::StorageEvent::BatchReadResult { values },
            ) = answer
            else {
                panic!("the claim read failed");
            };
            values[0].1.is_some()
        }
    };
    assert!(!claimed(&context).await);
    let delete = aruna_core::effects::StorageEffect::Delete {
        key_space: aruna_core::keyspaces::JOB_KEYSPACE.to_string(),
        key: aruna_core::structs::execution::job::job_record_key(record.job_id),
        txn_id: None,
    };
    context.storage_handle.send_storage_effect(delete).await;
    crate::driver::drive(
        crate::blob::cleanup::ProcessCleanupOperation::new(crate::blob::cleanup::CleanupNode {
            routing: crate::driver::node_routing(&context),
            has_blob: (&context).blob_handle.is_some(),
        }),
        &context,
    )
    .await
    .unwrap();
    // The release ran once the job was gone; the version still reads the file, so it stays
    // and its claim went again.
    assert!(works().await.is_empty());
    assert!(!claimed(&context).await);
}

#[tokio::test]
async fn pauses_purges() {
    let (_directory, context) = setup().await;
    let job = Ulid::from_parts(50, 1);
    use crate::blob::pause::{PausePurgeOperation, PausedOperation};
    use crate::driver::drive;
    let paused = || drive(PausedOperation::new(job), &context);
    assert!(!paused().await.unwrap());
    drive(PausePurgeOperation::new(job, true), &context)
        .await
        .unwrap();
    assert!(paused().await.unwrap());
    drive(PausePurgeOperation::new(job, false), &context)
        .await
        .unwrap();
    assert!(!paused().await.unwrap());
}

#[tokio::test]
async fn plans_named_unversioned() {
    // Without a backend version only a file named by its version's id, with a recorded ETag,
    // is planned; a plain name is kept and the preview says why.
    let (_directory, context) = setup().await;
    let (a, b, c) = (
        Ulid::from_parts(10, 1),
        Ulid::from_parts(11, 1),
        Ulid::from_parts(12, 1),
    );
    let named = file(&format!("raw/gone/{a}_a"), Ulid::from_parts(20, 1));
    let plain = file("raw/gone/b", Ulid::from_parts(21, 1));
    let untagged = file(&format!("raw/gone/{c}_c"), Ulid::from_parts(22, 1));
    for ((key, id), location, etag) in [
        (("gone/a", a), &named, Some("\"e1\"")),
        (("gone/b", b), &plain, Some("\"e2\"")),
        (("gone/c", c), &untagged, None),
    ] {
        store(&context, (key, id), location, None).await;
        let row = ImportedObject::written(id, key.to_string(), 10, etag.map(str::to_string));
        let row_key = object_key(rule().import_id, &location.backend_path).to_vec();
        put(
            &context,
            IMPORTED_OBJECT_KEYSPACE,
            row_key,
            row.to_bytes().unwrap(),
        )
        .await;
    }
    let versions =
        [("gone/a", a), ("gone/b", b), ("gone/c", c)].map(|(key, id)| (key.to_string(), id));
    let found = drive(
        StorageFilesOperation::new(
            node_routing(&context),
            (BUCKET, group()),
            &versions,
            Some(&scope()),
        ),
        &context,
    )
    .await
    .unwrap();
    let planned: Vec<_> = (found.iter())
        .map(|file| (file.version.as_deref(), file.kept.is_some()))
        .collect();
    assert_eq!(planned, [(Some(""), false), (None, true), (None, true)]);
}

#[tokio::test]
async fn bounds_scope_totals() {
    let (_directory, context) = setup().await;
    for index in 0..=TOTAL_VERSIONS {
        let (key, id) = (format!("gone/{index:05}"), Ulid::from_parts(index + 1, 1));
        let version = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
        let version_key = VersionKey::new(BUCKET, key.clone(), id).to_bytes().unwrap();
        put(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            version_key,
            version.to_bytes().unwrap(),
        )
        .await;
        let head = CurrentVersionPointer::new(id).to_bytes().unwrap();
        let head_key = BlobHeadKey::new(BUCKET, key).to_bytes().unwrap();
        put(&context, BLOB_HEAD_KEYSPACE, head_key, head).await;
    }
    let routing = node_routing(&context);
    let totals = ScopeTotalsOperation::new(routing, &scope(), AnyKey);
    let totals = drive(totals, &context).await.unwrap();
    assert_eq!((totals.versions, totals.complete), (TOTAL_VERSIONS, false));
}

#[tokio::test]
async fn bounds_alias_checks() {
    let (_directory, context) = setup().await;
    let id = Ulid::from_parts(10, 1);
    let free = file("raw/gone/a", Ulid::from_parts(20, 1));
    store(&context, ("gone/a", id), &free, Some("p1")).await;
    record(&context, rule().import_id, "raw/gone/a", ("gone/a", id)).await;
    // A second file with the same content finds the budget spent and reads no alias.
    let other = Ulid::from_parts(11, 1);
    let second = file("raw/gone/b", Ulid::from_parts(21, 1));
    store(&context, ("gone/b", other), &second, Some("p2")).await;
    record(&context, rule().import_id, "raw/gone/b", ("gone/b", other)).await;
    for index in 0..ALIAS_BUDGET as u64 {
        let key = format!("elsewhere/{index:05}");
        let alias = Ulid::from_parts(100 + index, 1);
        let index = HashIndex::new([3; 32], alias, realm(), group(), node(4), BUCKET, &key);
        put(
            &context,
            PATHS_INDEX_KEYSPACE,
            index.to_bytes().unwrap(),
            Vec::new(),
        )
        .await;
    }
    let versions = [("gone/a".to_string(), id), ("gone/b".to_string(), other)];
    let found = drive(
        StorageFilesOperation::new(
            node_routing(&context),
            (BUCKET, group()),
            &versions,
            Some(&scope()),
        ),
        &context,
    )
    .await
    .unwrap();
    let kept: Vec<_> = found.iter().map(|file| file.kept.as_deref()).collect();
    assert_eq!(kept, [Some(UNCHECKED_READERS); 2]);
}

#[tokio::test]
async fn bounds_long_histories() {
    let (_directory, context) = setup().await;
    let version = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
    let count = TOTAL_VERSIONS + LIST_PAGE as u64 + 1;
    let last = Ulid::from_parts(count, 1);
    for index in 0..count {
        let key = VersionKey::new(BUCKET, "gone/big", Ulid::from_parts(index + 1, 1));
        put(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            key.to_bytes().unwrap(),
            version.to_bytes().unwrap(),
        )
        .await;
    }
    let head = CurrentVersionPointer::new(last).to_bytes().unwrap();
    let head_key = BlobHeadKey::new(BUCKET, "gone/big").to_bytes().unwrap();
    put(&context, BLOB_HEAD_KEYSPACE, head_key, head).await;
    let routing = node_routing(&context);
    let totals = ScopeTotalsOperation::new(routing, &scope(), AnyKey);
    let totals = drive(totals, &context).await.unwrap();
    assert!(!totals.complete);
}

fn linked(path: &str, id: Ulid) -> StorageFile {
    StorageFile {
        key: "gone/a".to_string(),
        version_id: id,
        location: file(path, Ulid::from_parts(30, 1)),
        version: None,
        owner: None,
        kept: None,
        index: None,
    }
}

#[test]
fn unversioned_files_unplanned() {
    // A file without a backend version needs no write at all.
    let files = [linked("raw/gone/a", Ulid::from_parts(10, 1))];
    let mut operation = PlanFilesOperation::new(Ulid::from_parts(1, 1), &files);
    assert_eq!(operation.first(), Ok(Next::Done(())));
}

#[test]
fn versioned_file_planned() {
    let mut planned = linked("raw/gone/a", Ulid::from_parts(10, 1));
    planned.version = Some("p1".to_string());
    let job = Ulid::from_parts(1, 1);
    let mut operation = PlanFilesOperation::new(job, std::slice::from_ref(&planned));
    let Ok(Next::Call(IoCall::Write(writes))) = operation.first() else {
        panic!("expected the plan write");
    };
    assert_eq!(writes[0].0, BLOB_CLEANUP_KEYSPACE);
    assert_eq!(writes[0].1, plan_key(job, planned.location.ulid));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

fn wanted() -> BlobLocationKey {
    file("raw/gone/a", Ulid::from_parts(20, 1))
        .location_key()
        .unwrap()
}

fn alias(key: &str) -> Key {
    let id = Ulid::from_parts(40, 1);
    HashIndex::new([3; 32], id, realm(), group(), node(4), BUCKET, key)
        .to_bytes()
        .unwrap()
        .into()
}

fn alias_page(keys: &[&str], next: Option<Key>) -> IoResult {
    IoResult::Page {
        rows: (keys.iter())
            .map(|key| (alias(key), Value::from(Vec::new())))
            .collect(),
        next,
    }
}

#[test]
fn spent_budget_unknown() {
    let mut operation = ReadElsewhere::new(wanted(), scope(), 0);
    assert_eq!(operation.first(), Ok(Next::Done((None, 0))));
}

#[test]
fn scoped_aliases_pass() {
    // Aliases inside the purge scope go with it and still use up the budget.
    let mut operation = ReadElsewhere::new(wanted(), scope(), 5);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Page { limit: 5, .. }))
    ));
    let turn = operation.next(alias_page(&["gone/a"], None));
    assert_eq!(turn, Ok(Next::Done((Some(false), 4))));
}

#[test]
fn deleted_alias_ignored() {
    // An alias whose version row is gone reads nothing.
    let mut operation = ReadElsewhere::new(wanted(), scope(), 5);
    operation.first().unwrap();
    let turn = operation.next(alias_page(&["kept/a"], None));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let turn = operation.next(IoResult::Values(vec![None]));
    assert_eq!(turn, Ok(Next::Done((Some(false), 4))));
}

#[test]
fn no_versions_routes() {
    let versions = Vec::new();
    let place = (BUCKET, group());
    let mut operation = StorageFilesOperation::new(NodeRouting::default(), place, &versions, None);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
}
