//! Objects Aruna did not write: ordered listing pages and hashing in place.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{read_back, setup_two_backends, test_user_id};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::storage::blob::{BackendRef, ResolvedBackend};

fn place(root: &std::path::Path, path: &str, body: &[u8]) {
    let path = root.join(path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

#[tokio::test]
async fn pages_in_order() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    for path in ["raw/b.txt", "raw/sub/c.txt", "raw/a.txt", "other/x.txt"] {
        place(&root, path, b"data");
    }
    let backend = BackendRef::Node("default".to_string());

    let BlobEvent::ExternalListed { objects, next } =
        handler.list_external(&backend, "raw/", None, 2).await
    else {
        panic!("first page failed")
    };
    let keys: Vec<_> = objects.iter().map(|object| object.key.as_str()).collect();
    assert_eq!(keys, ["raw/a.txt", "raw/b.txt"]);
    assert_eq!(objects[0].size, 4);
    assert_eq!(next.as_deref(), Some("raw/b.txt"));

    let BlobEvent::ExternalListed { objects, next } =
        handler.list_external(&backend, "raw/", next, 2).await
    else {
        panic!("second page failed")
    };
    let keys: Vec<_> = objects.iter().map(|object| object.key.as_str()).collect();
    assert_eq!(keys, ["raw/sub/c.txt"]);
    assert_eq!(next, None);
}

#[tokio::test]
async fn refuses_large_unordered() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    for path in ["raw/a.txt", "raw/b.txt", "raw/c.txt"] {
        place(&root, path, b"data");
    }
    let backend = BackendRef::Node("default".to_string());

    // The file system lists without order, so a folder above the budget is refused.
    let refused = handler.external_page(&backend, "raw/", None, 1, 2).await;
    assert!(
        matches!(refused, Err(BlobError::ListError(_))),
        "{refused:?}"
    );
    let listed = handler.external_page(&backend, "raw/", None, 1, 10).await;
    assert!(matches!(listed, Ok(BlobEvent::ExternalListed { .. })));
}

#[tokio::test]
async fn hashes_in_place() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    place(&root, "raw/a.txt", b"outside bytes");
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);

    let BlobEvent::ExternalHashed { location, .. } = handler
        .hash_external(resolved, "raw/a.txt".to_string(), test_user_id())
        .await
    else {
        panic!("hash failed")
    };

    assert_eq!(location.blob_size, 13);
    assert_eq!(location.backend_path, "raw/a.txt");
    assert!(location.storage_bucket.is_empty());
    assert_eq!(
        location.hashes.get(HASH_BLAKE3).unwrap().as_slice(),
        blake3::hash(b"outside bytes").as_bytes()
    );
    // The bytes stay where they were and read back through the location.
    assert_eq!(read_back(&handler, location).await, b"outside bytes");
    assert!(root.join("raw/a.txt").exists());
}

#[tokio::test]
async fn refuses_inexact_keys() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    place(
        &context._temp_dir.path().join("hot"),
        "raw/a.txt",
        b"other bytes",
    );
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);

    // Each key would read `raw/a.txt`, which is another object.
    for key in ["raw//a.txt", "raw/a.txt ", " raw/a.txt"] {
        let event = handler
            .hash_external(resolved.clone(), key.to_string(), test_user_id())
            .await;
        assert!(
            matches!(event, BlobEvent::Error(BlobError::Unreadable(_))),
            "{key}: {event:?}"
        );
    }
    // A key that is gone fails only this object.
    let event = handler
        .hash_external(resolved, "raw/gone.txt".to_string(), test_user_id())
        .await;
    assert!(
        matches!(event, BlobEvent::Error(BlobError::Unreadable(_))),
        "{event:?}"
    );
}

#[tokio::test]
async fn writes_plain_paths() {
    use super::stream_from_bytes;
    use aruna_core::structs::storage::backend_import::{PlainPath, tagged_path};
    use ulid::Ulid;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    let backend = BackendRef::Node("default".to_string());
    let resolved = ResolvedBackend::new(backend.clone(), None);
    let path = "raw/a.txt".to_string();
    let plain = |path: &str, if_not_exists| PlainPath {
        path: path.to_string(),
        ulid: Ulid::generate(),
        version_id: Ulid::generate(),
        if_not_exists,
        exact: false,
    };
    let write = |plain: PlainPath, body: &'static [u8]| {
        let handler = handler.clone();
        let resolved = resolved.clone();
        async move {
            let blob = stream_from_bytes(body);
            handler
                .write_plain(resolved, plain, test_user_id(), blob)
                .await
        }
    };

    let stat = handler.stat_plain(backend.clone(), path.clone()).await;
    assert_eq!(
        stat,
        BlobEvent::PlainStat {
            exists: false,
            version: None
        }
    );
    let first = plain(&path, true);
    let BlobEvent::WriteFinished { location } = write(first.clone(), b"first").await else {
        panic!("plain write failed")
    };
    assert!(location.storage_bucket.is_empty());
    assert_eq!(location.ulid, first.ulid);
    // A key over the provider limit is refused before the storage is asked.
    let long = format!("raw/{}", "a".repeat(1100));
    let refused = handler.stat_plain(backend.clone(), long).await;
    assert_eq!(refused, BlobEvent::Error(BlobError::KeyTooLong));
    let stat = handler.stat_plain(backend, path.clone()).await;
    assert!(matches!(
        stat,
        BlobEvent::PlainStat {
            exists: true,
            version: None
        }
    ));

    // A conditional write never replaces another writer's file.
    let event = write(plain(&path, true), b"second").await;
    assert_eq!(event, BlobEvent::Error(BlobError::PathTaken));
    assert_eq!(std::fs::read(root.join(&path)).unwrap(), b"first");

    // Without versions the next write takes a tagged path next to the file.
    let tagged = tagged_path(&path, Ulid::generate());
    let BlobEvent::WriteFinished { .. } = write(plain(&tagged, true), b"second").await else {
        panic!("tagged write failed")
    };
    assert_eq!(std::fs::read(root.join(&tagged)).unwrap(), b"second");
    assert_eq!(std::fs::read(root.join(&path)).unwrap(), b"first");

    // A backend that keeps versions is overwritten without the condition.
    let BlobEvent::WriteFinished { .. } = write(plain(&path, false), b"third").await else {
        panic!("overwrite failed")
    };
    assert_eq!(std::fs::read(root.join(&path)).unwrap(), b"third");
}

#[tokio::test]
async fn writes_folder_objects() {
    use super::stream_from_bytes;
    use aruna_core::structs::storage::backend_import::PlainPath;
    use iroh_io::AsyncSliceReader;
    use ulid::Ulid;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let plain = |path: &str| PlainPath {
        path: path.to_string(),
        ulid: Ulid::generate(),
        version_id: Ulid::generate(),
        if_not_exists: false,
        exact: false,
    };
    let event = handler
        .write_plain(
            resolved.clone(),
            plain("raw/reports/"),
            test_user_id(),
            stream_from_bytes(b""),
        )
        .await;
    let BlobEvent::WriteFinished { location } = event else {
        panic!("folder write failed: {event:?}")
    };
    assert!(root.join("raw/reports").is_dir());
    assert_eq!(location.blob_size, 0);
    assert!(location.hashes.contains_key(HASH_BLAKE3));
    // Replication and peer reads open the same empty folder copy.
    let mut reader = handler.slice_reader(&location).await.unwrap();
    assert_eq!(reader.size().await.unwrap(), 0);
    assert!(reader.read_exact_at(0, 0).await.unwrap().is_empty());
    assert!(reader.read_exact_at(0, 1).await.is_err());
    let BlobEvent::ReadFinished { stream_size, .. } = handler.read_blob(location).await else {
        panic!("folder read failed")
    };
    assert_eq!(stream_size, 0);
    // A folder object holds no bytes.
    let event = handler
        .write_plain(
            resolved,
            plain("raw/notes/"),
            test_user_id(),
            stream_from_bytes(b"x"),
        )
        .await;
    assert!(
        matches!(event, BlobEvent::Error(BlobError::WriteError(_))),
        "{event:?}"
    );
}

#[tokio::test]
async fn reads_recorded_version() {
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::handle::Handle as _;
    use aruna_core::keyspaces::BACKEND_VERSION_KEYSPACE;
    use aruna_core::structs::storage::backend_import::version_key;
    use ulid::Ulid;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let mut location = super::make_test_location();
    location.storage_bucket = String::new();
    location.backend = BackendRef::Group(Ulid::generate());
    assert_eq!(handler.read_version(&location).await, Ok(None));

    let write = StorageEffect::Write {
        key_space: BACKEND_VERSION_KEYSPACE.to_string(),
        key: version_key(location.ulid),
        value: b"v1".to_vec().into(),
        txn_id: None,
    };
    context
        .storage_handle
        .send_effect(Effect::Storage(write))
        .await;
    assert_eq!(handler.read_version(&location).await, Ok(Some("v1".into())));
    // An unversioned "null" version reads the newest bytes, like no version.
    let write = StorageEffect::Write {
        key_space: BACKEND_VERSION_KEYSPACE.to_string(),
        key: version_key(location.ulid),
        value: b"null".to_vec().into(),
        txn_id: None,
    };
    context
        .storage_handle
        .send_effect(Effect::Storage(write))
        .await;
    assert_eq!(handler.read_version(&location).await, Ok(None));

    // Aruna's own layout never names a backend version.
    location.storage_bucket = "bucket".to_string();
    assert_eq!(handler.read_version(&location).await, Ok(None));
}

#[tokio::test]
async fn keeps_tenant_paths() {
    use ulid::Ulid;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    place(&root, "raw/a.txt", b"theirs");
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let BlobEvent::ExternalHashed { location, .. } = handler
        .hash_external(resolved, "raw/a.txt".to_string(), test_user_id())
        .await
    else {
        panic!("hash failed")
    };

    let operator = handler.operator_from_location(&location).unwrap();
    let tenant = aruna_core::structs::storage::blob::BackendLocation {
        backend: BackendRef::Group(Ulid::generate()),
        ..location
    };
    assert!(matches!(
        handler.delete_blob(tenant.clone()).await,
        BlobEvent::DeleteFinished
    ));
    // A failed write cleans nothing at a tenant path.
    let path = Some("raw/a.txt");
    let cleanup = handler.clean_partial(None, true, Some(&operator), path, Some(&tenant));
    assert_eq!(cleanup.await, Ok(()));
    assert!(root.join("raw/a.txt").exists());
    // Discarding an upload target at a tenant path keeps the file there.
    assert_eq!(handler.discard_target(&tenant).await, Ok(()));
    assert!(root.join("raw/a.txt").exists());
}

#[test]
fn tags_without_condition() {
    use crate::blob::plain::conditional;
    use aruna_core::structs::storage::backend_import::{PlainPath, tagged_path};
    use ulid::Ulid;

    let config = [
        ("bucket", "b"),
        ("bucket_id", "id"),
        ("application_key_id", "key"),
        ("application_key", "secret"),
    ];
    let config = config.map(|(key, value)| (key.to_string(), value.to_string()));
    let operator = opendal::Operator::from_iter::<opendal::services::B2>(config).unwrap();
    let ulid = Ulid::generate();
    let mut location = super::make_test_location();
    let mut plain = PlainPath {
        path: "raw/a.txt".to_string(),
        ulid,
        version_id: ulid,
        if_not_exists: true,
        exact: false,
    };

    assert_eq!(conditional(&operator, &mut location, &plain), Ok(false));
    assert_eq!(location.backend_path, tagged_path("raw/a.txt", ulid));

    // An already tagged path is not tagged twice.
    plain.path = tagged_path("raw/a.txt", ulid);
    location.backend_path = plain.path.clone();
    assert_eq!(conditional(&operator, &mut location, &plain), Ok(false));
    assert_eq!(location.backend_path, plain.path);
    // A rename must create its exact name, so such a storage refuses it.
    plain.exact = true;
    let refused = conditional(&operator, &mut location, &plain);
    assert_eq!(refused, Err(BlobError::ConditionalUnsupported));
}

#[tokio::test]
async fn deletes_renamed_files() {
    use super::stream_from_bytes;
    use aruna_core::structs::storage::backend_import::PlainPath;
    use ulid::Ulid;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let plain = PlainPath {
        path: "raw/old.txt".to_string(),
        ulid: Ulid::generate(),
        version_id: Ulid::generate(),
        if_not_exists: true,
        exact: false,
    };
    let blob = stream_from_bytes(b"old");
    let written = handler.write_plain(resolved, plain, test_user_id(), blob);
    let BlobEvent::WriteFinished { location } = written.await else {
        panic!("plain write failed")
    };
    // A storage without versions cannot delete only the renamed bytes, so the file stays.
    let deleted = handler
        .delete_plain(location.clone(), "v1".to_string(), None)
        .await;
    assert_eq!(deleted, BlobEvent::Error(BlobError::CleanupUnsupported));
    assert!(root.join("raw/old.txt").exists());
    // A copy in Aruna's own layout is never deleted this way.
    let own = aruna_core::structs::storage::blob::BackendLocation {
        storage_bucket: "data".to_string(),
        ..location
    };
    assert!(matches!(
        handler.delete_plain(own, "v1".to_string(), None).await,
        BlobEvent::Error(BlobError::DeleteError(_))
    ));
}

#[tokio::test]
async fn copies_plain_files() {
    use aruna_core::structs::storage::backend_import::PlainPath;
    use ulid::Ulid;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    place(&root, "raw/7_a.txt", b"copied bytes");
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let BlobEvent::ExternalHashed { location, .. } = handler
        .hash_external(resolved.clone(), "raw/7_a.txt".to_string(), test_user_id())
        .await
    else {
        panic!("hash failed")
    };
    let plain = PlainPath {
        path: "raw/a.txt~7".to_string(),
        ulid: Ulid::generate(),
        version_id: Ulid::generate(),
        if_not_exists: true,
        exact: true,
    };

    // A storage without provider copies streams the bytes and hashes them again.
    let event = handler
        .copy_plain(resolved, plain.clone(), location.clone(), None)
        .await;
    let (BlobEvent::WriteFinished { location: copy }
    | BlobEvent::PlainWritten { location: copy, .. }) = event
    else {
        panic!("copy failed: {event:?}")
    };
    assert_eq!(copy.backend_path, plain.path);
    assert_eq!(copy.get_blake3(), location.get_blake3());
    assert_eq!(
        std::fs::read(root.join("raw/a.txt~7")).unwrap(),
        b"copied bytes"
    );
    assert_eq!(
        std::fs::read(root.join("raw/7_a.txt")).unwrap(),
        b"copied bytes"
    );
}

#[tokio::test]
async fn records_copy_uploads() {
    // A copy's provider upload is recorded by its attempt until it settles, and only that one.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let attempt = ulid::Ulid::generate();
    assert_eq!(handler.copy_upload(attempt).await.unwrap(), None);
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let BlobEvent::WriteFinished { location } = handler
        .write_plain(
            resolved,
            aruna_core::structs::storage::backend_import::PlainPath {
                path: "raw/a.txt~7".to_string(),
                ulid: attempt,
                version_id: attempt,
                if_not_exists: true,
                exact: true,
            },
            test_user_id(),
            super::stream_from_bytes(b"x"),
        )
        .await
    else {
        panic!("plain write failed")
    };
    handler
        .record_upload(attempt, Some((&location, "upload-1")))
        .await
        .unwrap();
    let recorded = handler.copy_upload(attempt).await.unwrap();
    assert_eq!(recorded, Some((location, "upload-1".to_string())));
    assert_eq!(
        handler.copy_upload(ulid::Ulid::generate()).await.unwrap(),
        None
    );
    handler.record_upload(attempt, None).await.unwrap();
    assert_eq!(handler.copy_upload(attempt).await.unwrap(), None);
    // A settlement by attempt alone finds nothing left to abort.
    let settle = aruna_core::effects::BlobEffect::SettleCopy {
        attempt,
        upload: None,
    };
    assert_eq!(
        handler.execute_effect(settle).await,
        BlobEvent::UploadAborted
    );
}

#[tokio::test]
async fn keeps_unsettled_uploads() {
    // A copy upload that cannot be aborted keeps its record for a later try.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let attempt = ulid::Ulid::generate();
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let plain = aruna_core::structs::storage::backend_import::PlainPath {
        path: "raw/a.txt".to_string(),
        ulid: attempt,
        version_id: attempt,
        if_not_exists: true,
        exact: true,
    };
    let blob = super::stream_from_bytes(b"x");
    let written = handler.write_plain(resolved, plain, test_user_id(), blob);
    let BlobEvent::WriteFinished { location } = written.await else {
        panic!("plain write failed")
    };
    let upload = Some((&location, "upload-1"));
    handler.record_upload(attempt, upload).await.unwrap();
    // The filesystem opens no provider uploads, so it has none to abort.
    for given in [None, Some((location.clone(), "upload-1".to_string()))] {
        let event = handler.settle_copy(attempt, given).await;
        assert!(matches!(event, BlobEvent::Error(_)), "{event:?}");
        let kept = handler.copy_upload(attempt).await.unwrap();
        assert_eq!(kept, Some((location.clone(), "upload-1".to_string())));
    }
}

#[tokio::test]
async fn checks_stored_copies() {
    // A copy's stat answers whether its bytes are still there.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    place(&root, "raw/a.txt", b"bytes");
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let BlobEvent::ExternalHashed { mut location, .. } = handler
        .hash_external(resolved, "raw/a.txt".to_string(), test_user_id())
        .await
    else {
        panic!("hash failed")
    };
    assert_eq!(
        handler.stat_copy(location.clone()).await,
        BlobEvent::CopyStat { exists: true }
    );
    location.backend_path = "raw/gone.txt".to_string();
    assert_eq!(
        handler.stat_copy(location).await,
        BlobEvent::CopyStat { exists: false }
    );
}

#[tokio::test]
async fn deletes_exact_versions() {
    // Only a tenant path is deleted, and only where the storage deletes one version.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    place(&root, "raw/a.txt", b"bytes");
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let BlobEvent::ExternalHashed { mut location, .. } = handler
        .hash_external(resolved, "raw/a.txt".to_string(), test_user_id())
        .await
    else {
        panic!("hash failed")
    };
    let unsupported = handler
        .delete_plain(location.clone(), "v1".to_string(), None)
        .await;
    assert_eq!(unsupported, BlobEvent::Error(BlobError::CleanupUnsupported));
    // An unversioned "null" never names the bytes to delete.
    let unversioned = handler
        .delete_plain(location.clone(), "null".to_string(), None)
        .await;
    assert!(matches!(
        unversioned,
        BlobEvent::Error(BlobError::DeleteError(_))
    ));
    assert!(root.join("raw/a.txt").exists());
    location.storage_bucket = "managed".to_string();
    let refused = handler.delete_plain(location, "v1".to_string(), None).await;
    assert!(matches!(
        refused,
        BlobEvent::Error(BlobError::DeleteError(_))
    ));
}

#[tokio::test]
async fn deletes_unversioned_files() {
    // Without a version a file is only deleted while its size and recorded ETag still match;
    // without a recorded ETag, or with another one, it stays.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    place(&root, "raw/a.txt", b"bytes");
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let BlobEvent::ExternalHashed { location, .. } = handler
        .hash_external(resolved, "raw/a.txt".to_string(), test_user_id())
        .await
    else {
        panic!("hash failed")
    };
    for etag in [None, Some("\"other\"".to_string())] {
        let refused = handler
            .delete_plain(location.clone(), String::new(), etag)
            .await;
        assert_eq!(refused, BlobEvent::Error(BlobError::CleanupUnsupported));
        assert!(root.join("raw/a.txt").exists());
    }
}

#[tokio::test]
async fn copies_missing_source() {
    // A copy of a file that is gone fails without writing anything at the new name.
    use aruna_core::structs::storage::backend_import::PlainPath;
    use ulid::Ulid;
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    place(&root, "raw/a.txt", b"bytes");
    let resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let BlobEvent::ExternalHashed { mut location, .. } = handler
        .hash_external(resolved.clone(), "raw/a.txt".to_string(), test_user_id())
        .await
    else {
        panic!("hash failed")
    };
    location.backend_path = "raw/gone.txt".to_string();
    let plain = PlainPath {
        path: "raw/b.txt".to_string(),
        ulid: Ulid::generate(),
        version_id: Ulid::generate(),
        if_not_exists: true,
        exact: true,
    };
    let event = handler.copy_plain(resolved, plain, location, None).await;
    assert!(
        !matches!(
            event,
            BlobEvent::PlainWritten { .. } | BlobEvent::WriteFinished { .. }
        ),
        "{event:?}"
    );
    assert!(!root.join("raw/b.txt").exists());
}
