//! A storage check finds current versions whose copy cannot be served, resumes from stored
//! progress, and its cleanup deletes only versions unchanged since the check.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob_node::{BUCKET, TestContext, blob_files, put, read, run, setup_context, version};
use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BackendLocation, VersionKey};
use aruna_core::structs::storage::broken::BrokenReason;
use aruna_core::structs::storage::format::Compression;
use aruna_operations::blob::migration::broken::{clean_broken, list_broken, read_check};
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::compression::PutCompressionOperation;
use aruna_operations::s3::bucket::storage_check::StartCheckOperation;
use aruna_operations::s3::bucket::storage_move::StartMoveOperation;
use aruna_operations::s3::object::delete::DeleteObjectInput;
use ulid::Ulid;

/// Deletes the stored file of a version from the backend.
async fn lose_blob(context: &TestContext, key: &str, version_id: Ulid) {
    let version = version(context, key, version_id).await;
    let row = version.location_key().unwrap().to_bytes();
    let row = read(context, BLOB_LOCATIONS_KEYSPACE, row).await.unwrap();
    let location = BackendLocation::from_bytes(&row).unwrap();
    let files = blob_files(&context.temp_dir.path().join("blobstore"));
    let file = (files.into_iter())
        .find(|file| file.to_string_lossy().ends_with(&location.backend_path))
        .expect("the stored file");
    std::fs::remove_file(file).unwrap();
}

async fn start_check(context: &TestContext) {
    let operation = StartCheckOperation::new(BUCKET.to_string(), context.group_id, 1);
    drive(operation, &context.driver).await.unwrap();
}

fn template(context: &TestContext) -> DeleteObjectInput {
    DeleteObjectInput {
        bucket: BUCKET.to_string(),
        key: String::new(),
        version_id: None,
        group_id: context.group_id,
        realm_id: RealmId::from_bytes([7u8; 32]),
        node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
        deleted_by: context.user_id,
    }
}

#[tokio::test]
async fn cleans_broken_files() {
    let context = setup_context().await;
    let healthy = put(&context, "a.txt", b"healthy research data").await;
    let lost = put(&context, "genes/b.faa.gz", b"lost research data").await;
    let changed = put(&context, "genes/c.faa.gz", b"changed research data").await;
    // More versions than one page, so the check has to resume from stored progress.
    for index in 0..70 {
        let key = format!("small/{index:03}");
        put(&context, &key, key.as_bytes()).await;
    }
    lose_blob(&context, "genes/b.faa.gz", lost).await;
    lose_blob(&context, "genes/c.faa.gz", changed).await;

    start_check(&context).await;
    // Each run only reads the stored record, so a second run is what a restart sees.
    assert!(run(&context).await.is_some());
    let partial = read_check(&context.driver, BUCKET).await.unwrap().unwrap();
    assert!(partial.cursor.is_some() && partial.finished_at_ms.is_none());
    assert!(run(&context).await.is_none());
    let done = read_check(&context.driver, BUCKET).await.unwrap().unwrap();
    assert_eq!((done.checked, done.broken, done.failed), (73, 2, 0));

    let (rows, next) = list_broken(&context.driver, BUCKET, None, 10)
        .await
        .unwrap();
    assert!(next.is_none());
    let found: Vec<_> = rows
        .iter()
        .map(|(key, row)| (key.key.as_str(), row.reason))
        .collect();
    let missing = BrokenReason::MissingBlob;
    assert_eq!(
        found,
        [("genes/b.faa.gz", missing), ("genes/c.faa.gz", missing)]
    );

    // A caller without WRITE on the objects deletes nothing.
    let auth = AuthContext {
        user_id: context.user_id,
        realm_id: RealmId::from_bytes([7u8; 32]),
        path_restrictions: None,
        session: None,
    };
    let cleaned = clean_broken(&context.driver, &template(&context), &auth, None, 10, 100)
        .await
        .unwrap();
    assert_eq!(
        (cleaned.deleted, cleaned.denied, cleaned.next),
        (0, 2, None)
    );
    let (rows, _) = list_broken(&context.driver, BUCKET, None, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    // Denied rows never count toward the limit nor name the cursor.
    let paged = clean_broken(&context.driver, &template(&context), &auth, None, 1, 100)
        .await
        .unwrap();
    assert_eq!((paged.denied, paged.next), (2, None));
    for (key, id) in [
        ("genes/b.faa.gz", lost),
        ("genes/c.faa.gz", changed),
        ("a.txt", healthy),
    ] {
        let key = VersionKey::new(BUCKET, key, id).to_bytes().unwrap();
        assert!(read(&context, BLOB_VERSIONS_KEYSPACE, key).await.is_some());
    }
}

#[tokio::test]
async fn move_records_broken() {
    // A move records a copy it cannot read as broken instead of failing every retry pass.
    let context = setup_context().await;
    let lost = put(
        &context,
        "genes/b.faa.gz",
        &b"lost research data ".repeat(1_000),
    )
    .await;
    lose_blob(&context, "genes/b.faa.gz", lost).await;
    let zstd = Compression::Zstd { level: 3 };
    let operation = PutCompressionOperation::new(BUCKET.to_string(), context.group_id, zstd, 1);
    drive(operation, &context.driver).await.unwrap();
    let operation = StartMoveOperation::new(BUCKET.to_string(), context.group_id, String::new(), 2);
    drive(operation, &context.driver).await.unwrap();

    assert!(run(&context).await.is_none());

    let (rows, _) = list_broken(&context.driver, BUCKET, None, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.reason, BrokenReason::MissingBlob);
}
