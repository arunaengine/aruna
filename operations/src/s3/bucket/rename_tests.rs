//! Tests folder renames of bucket keys: selection, conflicts, access and guards.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
use super::*;
use crate::driver::{DriverContext, drive};
use crate::tests::records::node;
use crate::tests::s3::{test_context, test_storage};
use aruna_core::structs::storage::backend_import::{DEFAULT_PATTERN, ExternalObject, object_key};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, ManagedCopyKey, ManagedCopyState,
};
use aruna_core::structs::storage::format::Compression;
use std::time::SystemTime;

pub(crate) const BUCKET: &str = "bucket";

pub(crate) fn group() -> GroupId {
    Ulid::from_parts(1, 1)
}

pub(crate) fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

pub(crate) fn rule() -> BackendImport {
    BackendImport {
        import_id: Ulid::from_parts(2, 2),
        group_id: group(),
        backend_id: Ulid::from_parts(3, 3),
        backend_prefix: "raw/".to_string(),
        bucket: BUCKET.to_string(),
        key_prefix: String::new(),
        schedule: None,
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: DEFAULT_PATTERN.to_string(),
    }
}

fn input(from: &str, to: &str, keys: Option<Vec<&str>>, apply: bool) -> RenameInput {
    RenameInput {
        realm_id: realm(),
        node_id: node(4),
        group_id: group(),
        bucket: BUCKET.to_string(),
        from: from.to_string(),
        to: to.to_string(),
        keys: keys.map(|keys| keys.into_iter().map(str::to_string).collect()),
        apply,
        pairs: Vec::new(),
        cutoff_ms: None,
        job: None,
    }
}

pub(crate) fn version(hash: u8) -> BlobVersion {
    let backend = BackendRef::Group(rule().backend_id);
    let at = SystemTime::UNIX_EPOCH;
    BlobVersion::materialized(
        [hash; 32],
        backend,
        EncodingClass::Raw,
        at,
        Default::default(),
        None,
    )
}

pub(crate) async fn put(context: &DriverContext, space: &str, key: Vec<u8>, value: Vec<u8>) {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

pub(crate) async fn get(context: &DriverContext, space: &str, key: Vec<u8>) -> Option<Value> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: space.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("read failed: {event:?}")
    };
    value
}

pub(crate) fn head_key(key: &str) -> Vec<u8> {
    BlobHeadKey::new(BUCKET, key).to_bytes().unwrap()
}

pub(crate) fn version_key(key: &str, id: Ulid) -> Vec<u8> {
    VersionKey::new(BUCKET, key, id).to_bytes().unwrap()
}

fn index_row(key: &str, hash: u8, id: Ulid) -> HashIndex {
    HashIndex::new([hash; 32], id, realm(), group(), node(4), BUCKET, key)
}

/// Stores `key` with the versions `ids`, the last one current; `deleted` makes it a marker.
pub(crate) async fn object(context: &DriverContext, key: &str, ids: &[Ulid], deleted: bool) {
    for (at, id) in ids.iter().enumerate() {
        let last = at + 1 == ids.len();
        let value = match deleted && last {
            true => BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default()),
            false => version(at as u8 + 1),
        };
        put(
            context,
            BLOB_VERSIONS_KEYSPACE,
            version_key(key, *id),
            value.to_bytes().unwrap(),
        )
        .await;
    }
    let head = CurrentVersionPointer::new(*ids.last().unwrap());
    put(
        context,
        BLOB_HEAD_KEYSPACE,
        head_key(key),
        head.to_bytes().unwrap(),
    )
    .await;
}

/// Records that the raw copy with `hash` is the linked file at `path`.
pub(crate) async fn link(context: &DriverContext, hash: u8, path: &str) {
    let location = BackendLocation {
        backend: BackendRef::Group(rule().backend_id),
        storage_class: None,
        root: String::new(),
        storage_bucket: String::new(),
        backend_path: path.to_string(),
        ulid: Ulid::from_parts(30, u128::from(hash)),
        format: Default::default(),
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 4,
        hashes: [(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            vec![hash; 32],
        )]
        .into(),
    };
    let key = location.location_key().unwrap().to_bytes();
    put(
        context,
        BLOB_LOCATIONS_KEYSPACE,
        key,
        location.to_bytes().unwrap(),
    )
    .await;
}

pub(crate) async fn setup() -> (tempfile::TempDir, DriverContext) {
    let (directory, storage) = test_storage();
    let context = test_context(storage);
    let bucket = BucketInfo {
        group_id: group(),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let name = BUCKET.as_bytes().to_vec();
    put(
        &context,
        S3_BUCKET_KEYSPACE,
        name,
        bucket.to_bytes().unwrap(),
    )
    .await;
    let rule = rule();
    put(
        &context,
        BACKEND_IMPORT_KEYSPACE,
        rule.key().to_vec(),
        rule.to_bytes().unwrap(),
    )
    .await;
    (directory, context)
}

async fn row(context: &DriverContext, path: &str) -> ImportedObject {
    let value = get(
        context,
        IMPORTED_OBJECT_KEYSPACE,
        object_key(rule().import_id, path).to_vec(),
    );
    ImportedObject::from_bytes(&value.await.unwrap()).unwrap()
}

#[tokio::test]
async fn renames_into_folder() {
    let (_directory, context) = setup().await;
    let (v1, v2, v3) = (
        Ulid::from_parts(10, 1),
        Ulid::from_parts(11, 1),
        Ulid::from_parts(12, 1),
    );
    object(&context, "x", &[v1, v2], true).await;
    object(&context, "sub/y", &[v3], false).await;
    let other = version(3).to_bytes().unwrap();
    put(
        &context,
        BLOB_VERSIONS_KEYSPACE,
        version_key("sub/y", v3),
        other,
    )
    .await;
    // The rule's rows are found through the files the versions read.
    link(&context, 1, "raw/x").await;
    link(&context, 3, "raw/other").await;
    // The provider file `raw/x` maps by the rule's folders, `raw/other` by its recorded key.
    let plain = ImportedObject::new(
        v1,
        &ExternalObject {
            key: "raw/x".to_string(),
            size: 4,
            etag: Some("e".to_string()),
            modified_ms: None,
        },
    );
    let recorded = ImportedObject::written(v3, "sub/y".to_string(), 4, None);
    for (path, value) in [("raw/x", &plain), ("raw/other", &recorded)] {
        let key = object_key(rule().import_id, path).to_vec();
        put(
            &context,
            IMPORTED_OBJECT_KEYSPACE,
            key,
            value.to_bytes().unwrap(),
        )
        .await;
    }
    let index = index_row("x", 1, v1);
    put(
        &context,
        PATHS_INDEX_KEYSPACE,
        index.to_bytes().unwrap(),
        Vec::new(),
    )
    .await;
    let location = BackendLocation {
        backend: BackendRef::Group(rule().backend_id),
        storage_class: None,
        root: String::new(),
        storage_bucket: String::new(),
        backend_path: "raw/other".to_string(),
        ulid: Ulid::from_parts(20, 1),
        format: Default::default(),
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 4,
        hashes: Default::default(),
    };
    let copy = ManagedCopyRecord::new(
        VersionKey::new(BUCKET, "sub/y", v3),
        node(4),
        location,
        Vec::new(),
        0,
        ManagedCopyState::Registered,
    )
    .unwrap();
    let copy_key = copy.key().to_bytes().unwrap();
    put(
        &context,
        MANAGED_COPY_KEYSPACE,
        copy_key.clone(),
        copy.to_bytes().unwrap(),
    )
    .await;

    let preview = drive(
        RenameKeysOperation::new(input("", "archive/", None, false)),
        &context,
    )
    .await
    .unwrap();
    let pairs: Vec<_> = preview
        .items
        .iter()
        .map(|item| (item.from.as_str(), item.to.as_str(), item.conflict.clone()))
        .collect();
    assert_eq!(
        pairs,
        [("sub/y", "archive/sub/y", None), ("x", "archive/x", None)]
    );
    let x = &preview.items[1];
    assert_eq!(
        (x.versions.clone(), x.current, x.deleted),
        (vec![v1, v2], Some(v2), true)
    );
    assert_eq!(preview.items[0].files, ["raw/other"]);
    assert_eq!(x.files, ["raw/x"]);
    // A preview changes nothing.
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("x"))
            .await
            .is_some()
    );

    let keys = Some(vec!["sub/y", "x"]);
    let plan = drive(
        RenameKeysOperation::new(input("", "archive/", keys.clone(), true)),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(plan.items, preview.items);
    for key in ["x", "sub/y"] {
        assert!(
            get(&context, BLOB_HEAD_KEYSPACE, head_key(key))
                .await
                .is_none()
        );
    }
    let head = get(&context, BLOB_HEAD_KEYSPACE, head_key("archive/x"))
        .await
        .unwrap();
    assert_eq!(
        CurrentVersionPointer::from_bytes(&head).unwrap().version_id,
        v2
    );
    for (key, id) in [("archive/x", v1), ("archive/x", v2), ("archive/sub/y", v3)] {
        assert!(
            get(&context, BLOB_VERSIONS_KEYSPACE, version_key(key, id))
                .await
                .is_some()
        );
    }
    assert!(
        get(&context, BLOB_VERSIONS_KEYSPACE, version_key("x", v1))
            .await
            .is_none()
    );
    // Reverse records keep the files where they are under the new keys.
    assert_eq!(
        row(&context, "raw/x").await.key.as_deref(),
        Some("archive/x")
    );
    assert_eq!(
        row(&context, "raw/other").await.key.as_deref(),
        Some("archive/sub/y")
    );
    assert_eq!(row(&context, "raw/x").await.version_id, v1);
    let moved = index_row("archive/x", 1, v1).to_bytes().unwrap();
    assert!(get(&context, PATHS_INDEX_KEYSPACE, moved).await.is_some());
    assert!(
        get(&context, PATHS_INDEX_KEYSPACE, index.to_bytes().unwrap())
            .await
            .is_none()
    );
    assert!(
        get(&context, MANAGED_COPY_KEYSPACE, copy_key)
            .await
            .is_none()
    );
    let moved = ManagedCopyKey::new(
        VersionKey::new(BUCKET, "archive/sub/y", v3),
        copy.location.backend.clone(),
    );
    let record = get(&context, MANAGED_COPY_KEYSPACE, moved.to_bytes().unwrap())
        .await
        .unwrap();
    assert_eq!(
        ManagedCopyRecord::from_bytes(&record).unwrap().version.key,
        "archive/sub/y"
    );

    // Repeating the same rename changes nothing and never nests the folder twice.
    let again = drive(
        RenameKeysOperation::new(input("", "archive/", keys, true)),
        &context,
    )
    .await;
    assert!(matches!(again, Err(RenameError::Conflict(_))));
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("archive/archive/x"))
            .await
            .is_none()
    );
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("archive/x"))
            .await
            .is_some()
    );
}

#[tokio::test]
async fn refuses_collisions() {
    let (_directory, context) = setup().await;
    let (v1, v2, v3) = (
        Ulid::from_parts(10, 1),
        Ulid::from_parts(11, 1),
        Ulid::from_parts(12, 1),
    );
    object(&context, "a/x", &[v1], false).await;
    object(&context, "b/x", &[v2], false).await;
    object(&context, "a/b/x", &[v3], false).await;

    // An existing key and a key that is itself renamed are conflicts in the preview.
    let preview = drive(
        RenameKeysOperation::new(input("a/", "b/", None, false)),
        &context,
    )
    .await
    .unwrap();
    let conflicts: Vec<_> = preview
        .items
        .iter()
        .map(|item| item.conflict.is_some())
        .collect();
    assert_eq!(conflicts, [false, true]);
    let nested = drive(
        RenameKeysOperation::new(input("a/", "a/b/", None, false)),
        &context,
    )
    .await
    .unwrap();
    let reasons: Vec<_> = nested
        .items
        .iter()
        .map(|item| item.conflict.clone())
        .collect();
    assert_eq!(
        reasons,
        [None, Some("the key `a/b/x` already exists".to_string())]
    );

    // Any conflict leaves every key as it was.
    let keys = Some(vec!["a/b/x", "a/x"]);
    let result = drive(
        RenameKeysOperation::new(input("a/", "b/", keys, true)),
        &context,
    )
    .await;
    assert!(matches!(result, Err(RenameError::Conflict(_))));
    for key in ["a/x", "a/b/x", "b/x"] {
        assert!(
            get(&context, BLOB_HEAD_KEYSPACE, head_key(key))
                .await
                .is_some()
        );
    }
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("b/b/x"))
            .await
            .is_none()
    );
}

#[test]
fn checks_input() {
    let cases = [
        input("a", "b/", Some(vec!["a/x"]), true),
        input("a/", "a/", Some(vec!["a/x"]), true),
        input("a/", "b/", None, true),
        input("a/", "b/", Some(Vec::new()), true),
        input("a/", "b/", Some(vec!["c/x"]), true),
        input("a/", "b/", Some(vec!["a/x", "a/x"]), true),
        input("a/", "../", Some(vec!["a/x"]), true),
        input("a/", "", Some(vec!["a/"]), true),
    ];
    for case in cases {
        let mut operation = RenameKeysOperation::new(case.clone());
        assert!(operation.start().is_empty(), "{case:?}");
        assert!(
            matches!(operation.finalize(), Err(RenameError::Invalid(_))),
            "{case:?}"
        );
    }
    assert_eq!(
        renamed_key("", "archive/", "x/y"),
        Some("archive/x/y".to_string())
    );
    assert_eq!(renamed_key("a/", "", "a/x"), Some("x".to_string()));
    assert_eq!(renamed_key("a/", "b/", "ab/x"), None);
}

/// The owner may write everywhere except below `secret/`.
pub(crate) async fn access(context: &DriverContext) -> AuthContext {
    let secret = aruna_core::structs::storage::blob::object_permission_path(
        realm(),
        group(),
        node(4),
        BUCKET,
        "secret/**",
    );
    crate::tests::access::seed_access(context, (realm(), group(), node(4)), &secret).await
}

#[tokio::test]
async fn refuses_denied_keys() {
    let (_directory, context) = setup().await;
    let auth = access(&context).await;
    let (v1, v2) = (Ulid::from_parts(10, 1), Ulid::from_parts(11, 1));
    object(&context, "a/x", &[v1], false).await;
    object(&context, "secret/y", &[v2], false).await;

    let open = authorized_rename(&context, &auth, input("a/", "b/", None, false)).await;
    assert_eq!(open.unwrap().items.len(), 1);
    // A denied old key or new key refuses the preview and the rename, and nothing moves.
    let cases = [
        input("", "archive/", None, false),
        input("secret/", "open/", Some(vec!["secret/y"]), true),
        input("a/", "secret/", None, false),
        input("a/", "secret/", Some(vec!["a/x"]), true),
    ];
    for case in cases {
        let result = authorized_rename(&context, &auth, case.clone()).await;
        assert_eq!(result, Err(RenameError::Forbidden), "{case:?}");
    }
    for key in ["a/x", "secret/y"] {
        assert!(
            get(&context, BLOB_HEAD_KEYSPACE, head_key(key))
                .await
                .is_some()
        );
    }
}

#[tokio::test]
async fn refuses_nested_parts() {
    let (_directory, context) = setup().await;
    let head = CurrentVersionPointer::new(Ulid::from_parts(10, 1))
        .to_bytes()
        .unwrap();
    let writes = (0..=MAX_RENAME_KEYS)
        .flat_map(|index| [format!("k{index:04}"), format!("a/k{index:04}")])
        .map(|key| {
            let value = head.clone().into();
            (BLOB_HEAD_KEYSPACE.to_string(), head_key(&key).into(), value)
        })
        .collect();
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));

    // A second part of `` to `archive/` would select `archive/*` again.
    let nested = RenameKeysOperation::new(input("", "archive/", None, false));
    let nested = drive(nested, &context).await;
    assert!(matches!(nested, Err(RenameError::Conflict(_))));
    let apart = RenameKeysOperation::new(input("a/", "b/", None, false));
    let apart = drive(apart, &context).await.unwrap();
    assert!(apart.truncated);
    assert_eq!(apart.items.len(), MAX_RENAME_KEYS);
}

async fn preview(context: &DriverContext) -> Result<RenamePlan, RenameError> {
    drive(
        RenameKeysOperation::new(input("a/", "b/", None, false)),
        context,
    )
    .await
}

#[tokio::test]
async fn waits_for_migrations() {
    let (_directory, context) = setup().await;
    object(&context, "a/x", &[Ulid::from_parts(10, 1)], false).await;
    let mut record = CompressionMigration::new(Compression::Zstd { level: 3 }, 1);
    let name = BUCKET.as_bytes().to_vec();
    let space = COMPRESSION_MIGRATION_KEYSPACE;
    put(&context, space, name.clone(), record.to_bytes().unwrap()).await;
    assert!(matches!(
        preview(&context).await,
        Err(RenameError::Conflict(_))
    ));
    record.finished_at_ms = Some(2);
    put(&context, space, name, record.to_bytes().unwrap()).await;
    assert_eq!(preview(&context).await.unwrap().items.len(), 1);
}

#[tokio::test]
async fn waits_for_copies() {
    let (_directory, context) = setup().await;
    object(&context, "a/x", &[Ulid::from_parts(10, 1)], false).await;
    let [_, (space, prefix), _] = queued_prefixes(BUCKET).unwrap();
    let job = [prefix, b"job".to_vec()].concat();
    put(&context, space, job, Vec::new()).await;
    assert!(matches!(
        preview(&context).await,
        Err(RenameError::Conflict(_))
    ));
}

#[tokio::test]
async fn waits_for_uploads() {
    use aruna_core::structs::storage::multipart::{MultipartUpload, MultipartUploadStatus};
    let (_directory, context) = setup().await;
    object(&context, "a/x", &[Ulid::from_parts(10, 1)], false).await;
    object(&context, "a/y", &[Ulid::from_parts(11, 1)], false).await;
    let upload = MultipartUpload {
        upload_id: Ulid::from_parts(30, 1),
        backend: BackendRef::Group(rule().backend_id),
        storage_class: None,
        bucket: BUCKET.to_string(),
        key: "a/x".to_string(),
        group_id: group(),
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        status: MultipartUploadStatus::Open,
        checksum_hint: None,
        metadata: Default::default(),
        placement_policies: Vec::new(),
        subject_generation: 0,
        completing_since_ms: None,
        backend_upload: None,
        encryption: None,
    };
    let key = upload.upload_id.to_bytes().to_vec();
    put(&context, UPLOAD_KEYSPACE, key, upload.to_bytes().unwrap()).await;
    let reasons: Vec<_> = preview(&context)
        .await
        .unwrap()
        .items
        .into_iter()
        .map(|item| item.conflict)
        .collect();
    let open = Some("a multipart upload to the key is open".to_string());
    assert_eq!(reasons, [open, None]);
}

#[tokio::test]
async fn hides_error_names() {
    let (_directory, context) = setup().await;
    object(&context, "secret/x", &[Ulid::from_parts(10, 1)], false).await;
    let long = format!("{}/", "a".repeat(MAX_KEY_LEN));
    let result = drive(
        RenameKeysOperation::new(input("secret/", &long, None, false)),
        &context,
    )
    .await;
    let Err(RenameError::Invalid(message)) = result else {
        panic!("expected an invalid rename, got {result:?}")
    };
    assert_eq!(message, "a key under `from` cannot get a name under `to`");
}

/// Stores a running job of the bucket with `task` that started at `started_ms`, run as the
/// caller of `access`.
pub(crate) async fn store_job(
    context: &DriverContext,
    task: aruna_core::structs::storage::linked_job::LinkedTask,
    started_ms: u64,
) -> aruna_core::structs::storage::linked_job::LinkedJob {
    use aruna_core::structs::storage::backend_import::TokenGrant;
    use aruna_core::structs::storage::linked_job::{LinkedJob, LinkedState};
    let job = LinkedJob {
        id: Ulid::generate(),
        task,
        group_id: group(),
        auth: access(context).await,
        node_id: node(4),
        grant: TokenGrant {
            token_hash: "token".to_string(),
            issuer: realm(),
            issued_at_secs: 1,
            expires_at_secs: u64::MAX,
            via: None,
        },
        state: LinkedState::Running,
        cursor: None,
        total: 0,
        counted: true,
        done: 0,
        skipped: 0,
        failed: 0,
        waiting: None,
        started_at_ms: started_ms,
        updated_at_ms: 0,
        finished_at_ms: None,
    };
    let key = BUCKET.as_bytes().to_vec();
    put(context, LINKED_JOB_KEYSPACE, key, job.to_bytes().unwrap()).await;
    job
}

#[tokio::test]
async fn renames_shared_imports() {
    // Identical imports share the first file's location; the second keeps its own row.
    let (_directory, context) = setup().await;
    let (v1, v2) = (Ulid::from_parts(10, 1), Ulid::from_parts(11, 1));
    object(&context, "x", &[v1], false).await;
    object(&context, "y", &[v2], false).await;
    link(&context, 1, "raw/x").await;
    for (path, id) in [("raw/x", v1), ("raw/y", v2)] {
        let external = ExternalObject {
            key: path.to_string(),
            size: 4,
            etag: Some("e".to_string()),
            modified_ms: None,
        };
        let row = ImportedObject::new(id, &external);
        let key = object_key(rule().import_id, path).to_vec();
        put(
            &context,
            IMPORTED_OBJECT_KEYSPACE,
            key,
            row.to_bytes().unwrap(),
        )
        .await;
    }
    let keys = Some(vec!["y"]);
    let plan = drive(
        RenameKeysOperation::new(input("", "archive/", keys, true)),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(plan.items[0].files, ["raw/y"]);
    assert_eq!(
        row(&context, "raw/y").await.key.as_deref(),
        Some("archive/y")
    );
    assert_eq!(row(&context, "raw/x").await.key, None);
    // A second rename finds the row by its recorded move, not by its path.
    let keys = Some(vec!["archive/y"]);
    let plan = drive(
        RenameKeysOperation::new(input("archive/", "done/", keys, true)),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(plan.items[0].files, ["raw/y"]);
    assert_eq!(row(&context, "raw/y").await.key.as_deref(), Some("done/y"));
}

#[tokio::test]
async fn renames_job_pairs() {
    // A job renames named pairs under its fence and stops at keys written after it started.
    let (_directory, context) = setup().await;
    let (v1, v2) = (Ulid::from_parts(10, 1), Ulid::from_parts(5_000, 1));
    object(&context, "x", &[v1], false).await;
    object(&context, "y", &[v2], false).await;
    let job = store_job(
        &context,
        aruna_core::structs::storage::linked_job::LinkedTask::Unify {
            prefix: String::new(),
        },
        1,
    )
    .await;
    let stored = Value::from(job.to_bytes().unwrap());
    let next = aruna_core::structs::storage::linked_job::LinkedJob { done: 1, ..job };
    let next = Value::from(next.to_bytes().unwrap());
    let paired = |pairs: &[(&str, &str)], job: Option<(Value, Value)>, apply: bool| RenameInput {
        pairs: pairs
            .iter()
            .map(|(from, to)| (from.to_string(), to.to_string()))
            .collect(),
        cutoff_ms: Some(1_000),
        job,
        ..input("", "", None, apply)
    };
    let run = |input| drive(RenameKeysOperation::new(input), &context);

    let mut both = paired(&[("x", "p/x")], None, true);
    both.keys = Some(vec!["x".to_string()]);
    assert!(matches!(run(both).await, Err(RenameError::Invalid(_))));
    let repeated = paired(&[("x", "p/x"), ("y", "p/x")], None, true);
    assert!(matches!(run(repeated).await, Err(RenameError::Invalid(_))));
    // A key written after the job started keeps its name.
    let preview = run(paired(&[("y", "p/y")], None, false)).await.unwrap();
    assert_eq!(
        preview.items[0].conflict.as_deref(),
        Some("the key changed after the job started")
    );
    // A job record that changed since the page was read stops the rename.
    let fence = Some((next.clone(), next.clone()));
    let changed = run(paired(&[("x", "p/x")], fence, true)).await;
    assert!(matches!(changed, Err(RenameError::Conflict(reason)) if reason == JOB_CHANGED));
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("x"))
            .await
            .is_some()
    );

    let fence = Some((stored, next.clone()));
    let plan = run(paired(&[("x", "p/x")], fence, true)).await.unwrap();
    assert_eq!(plan.items[0].to, "p/x");
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("p/x"))
            .await
            .is_some()
    );
    // The job's progress is written with the rename.
    let job_row = get(&context, LINKED_JOB_KEYSPACE, BUCKET.as_bytes().to_vec()).await;
    assert_eq!(job_row, Some(next));
}
