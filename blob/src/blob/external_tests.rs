//! Objects Aruna did not write: ordered listing pages and hashing in place.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{read_back, setup_two_backends, test_user_id};
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

    let BlobEvent::ExternalHashed { location } = handler
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
        assert!(matches!(event, BlobEvent::Error(_)), "{key}: {event:?}");
    }
}

#[tokio::test]
async fn writes_plain_paths() {
    use super::stream_from_bytes;
    use aruna_core::structs::storage::backend_import::PlainPath;
    use ulid::Ulid;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    let backend = BackendRef::Node("default".to_string());
    let resolved = ResolvedBackend::new(backend.clone(), None);
    let plain = PlainPath {
        path: "raw/a.txt".to_string(),
        ulid: Ulid::generate(),
    };

    let BlobEvent::WriteFinished { location: old } = handler
        .write_plain(
            resolved.clone(),
            plain.clone(),
            test_user_id(),
            stream_from_bytes(b"first"),
            Some(5),
        )
        .await
    else {
        panic!("plain write failed")
    };
    assert_eq!(std::fs::read(root.join("raw/a.txt")).unwrap(), b"first");
    assert!(old.storage_bucket.is_empty());
    assert_eq!(old.ulid, plain.ulid);

    let aside = ".aruna-versions/raw/a.txt/01".to_string();
    let BlobEvent::WriteFinished { location: moved } =
        handler.copy_aside(old.clone(), aside.clone()).await
    else {
        panic!("aside copy failed")
    };
    assert_eq!(moved.backend_path, aside);
    assert_eq!(moved.get_blake3(), old.get_blake3());

    // A second same-name write waits until the first releases its claim.
    assert!(handler.claim_plain(backend.clone(), plain.path.clone(), plain.ulid));
    assert!(!handler.claim_plain(backend.clone(), plain.path.clone(), Ulid::generate()));
    handler.release_plain(plain.ulid);
    let next = PlainPath {
        ulid: Ulid::generate(),
        ..plain
    };
    assert!(handler.claim_plain(backend, next.path.clone(), next.ulid));

    let BlobEvent::WriteFinished { .. } = handler
        .write_plain(
            resolved,
            next,
            test_user_id(),
            stream_from_bytes(b"second"),
            None,
        )
        .await
    else {
        panic!("overwrite failed")
    };
    assert_eq!(std::fs::read(root.join("raw/a.txt")).unwrap(), b"second");
    assert_eq!(read_back(&handler, moved).await, b"first");
}

#[tokio::test]
async fn keeps_tenant_paths() {
    use super::stream_from_bytes;
    use aruna_core::structs::storage::backend_import::PlainPath;
    use ulid::Ulid;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let root = context._temp_dir.path().join("hot");
    // Only a tenant backend has tenant paths.
    let mut resolved = ResolvedBackend::new(BackendRef::Node("default".to_string()), None);
    let plain = PlainPath {
        path: "raw/a.txt".to_string(),
        ulid: Ulid::generate(),
    };
    let BlobEvent::WriteFinished { location } = handler
        .write_plain(
            resolved.clone(),
            plain,
            test_user_id(),
            stream_from_bytes(b"mine"),
            None,
        )
        .await
    else {
        panic!("plain write failed")
    };
    let aside = ".aruna-versions/raw/a.txt/01".to_string();
    let BlobEvent::WriteFinished { location: moved } =
        handler.copy_aside(location.clone(), aside).await
    else {
        panic!("aside copy failed")
    };

    resolved.backend = BackendRef::Group(Ulid::generate());
    let tenant = aruna_core::structs::storage::blob::BackendLocation {
        backend: resolved.backend.clone(),
        ..location
    };
    assert!(matches!(
        handler.delete_blob(tenant).await,
        BlobEvent::DeleteFinished
    ));
    assert!(root.join("raw/a.txt").exists());

    assert!(matches!(
        handler.delete_blob(moved).await,
        BlobEvent::DeleteFinished
    ));
    assert!(!root.join(".aruna-versions/raw/a.txt/01").exists());
}
