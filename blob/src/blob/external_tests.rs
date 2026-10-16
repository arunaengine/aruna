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
        if_not_exists,
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
        if_not_exists: false,
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
        if_not_exists: true,
    };

    assert!(!conditional(&operator, &mut location, &plain));
    assert_eq!(location.backend_path, tagged_path("raw/a.txt", ulid));

    // An already tagged path is not tagged twice.
    plain.path = tagged_path("raw/a.txt", ulid);
    location.backend_path = plain.path.clone();
    assert!(!conditional(&operator, &mut location, &plain));
    assert_eq!(location.backend_path, plain.path);
}
