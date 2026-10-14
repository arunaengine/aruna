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
