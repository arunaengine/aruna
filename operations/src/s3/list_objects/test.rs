use super::*;
use crate::driver::{DriverContext, drive};
use crate::tests::fixtures::s3::{test_context, test_storage};
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::{
    BackendRef, BlobVersion, CurrentVersionPointer, PortableSourceDescriptor, RealmId,
    SourceConnectorKind, StagingStrategy, VersionSourceBinding,
};
use aruna_storage::storage;
use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

#[tokio::test]
async fn deleted_versions_skipped() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());

    let group_id = Ulid::generate();
    let realm_id = RealmId([7u8; 32]);
    let created_by = UserId::local(Ulid::generate(), realm_id);
    let live_version_id = Ulid::generate();
    let deleted_version_id = Ulid::generate();
    let live_hash = [3u8; 32];
    let created_at = UNIX_EPOCH + Duration::from_secs(5);

    for (key, version_id, version) in [
        (
            "alpha",
            live_version_id,
            BlobVersion::materialized(
                live_hash,
                BackendRef::node_default(),
                created_at,
                created_by,
                None,
            ),
        ),
        (
            "beta",
            deleted_version_id,
            BlobVersion::deleted(created_at, created_by),
        ),
    ] {
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("bucket", key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("bucket", key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "objects".to_string(),
        backend_path: "path".to_string(),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by,
        created_at,
        staging: false,
        partial: false,
        blob_size: 42,
        hashes: HashMap::new(),
    };
    let event = storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: BlobLocationKey::new(live_hash, location.backend.clone())
                .to_bytes()
                .into(),
            value: location.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let result = drive(
        ListObjectsV2Operation::new(ListObjectsV2Input {
            bucket: "bucket".to_string(),
            group_id,
            continuation_token: None,
            max_keys: Some(10),
            prefix: None,
            delimiter: None,
            start_after: None,
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    assert_eq!(result.objects.len(), 1);
    assert_eq!(result.objects[0].head.key, "alpha");
    assert_eq!(result.objects[0].location, Some(location));
    assert_eq!(result.continuation_token, None);
}

#[tokio::test]
async fn prefix_filtered() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());

    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["common/a", "common/b", "rare/1", "rare/2", "rare/3"],
        created_by,
    )
    .await;

    let mut continuation_token = None;
    let mut all_keys = Vec::new();

    loop {
        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id,
                continuation_token,
                max_keys: Some(2),
                prefix: Some("rare/".to_string()),
                delimiter: None,
                start_after: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        for obj in result.objects {
            all_keys.push(obj.head.key);
        }

        continuation_token = result.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    // Verify prefix filtered correctly: all returned keys start with "rare/"
    assert!(all_keys.iter().all(|k| k.starts_with("rare/")));
    // And we got the right ones
    let mut sorted = all_keys.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["rare/1", "rare/2", "rare/3"]);
}

#[tokio::test]
async fn prefix_miss_continues() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());

    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &[
            "common/01",
            "common/02",
            "common/03",
            "common/04",
            "common/05",
            "rare/01",
        ],
        created_by,
    )
    .await;

    let result = drive(
        ListObjectsV2Operation::new(ListObjectsV2Input {
            bucket: "bucket".to_string(),
            group_id,
            continuation_token: None,
            max_keys: Some(1),
            prefix: Some("rare/".to_string()),
            delimiter: None,
            start_after: None,
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    let keys: Vec<_> = result
        .objects
        .into_iter()
        .map(|object| object.head.key)
        .collect();
    assert_eq!(keys, vec!["rare/01"]);
    assert!(result.continuation_token.is_none());
}

#[tokio::test]
async fn zero_limit_honored() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());

    let group_id = Ulid::generate();
    let realm_id = RealmId([7u8; 32]);
    let created_by = UserId::local(Ulid::generate(), realm_id);
    let created_at = UNIX_EPOCH + Duration::from_secs(5);
    let version_id = Ulid::generate();
    let hash = [3u8; 32];

    let _ = storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new("bucket", "alpha")
                .to_bytes()
                .unwrap()
                .into(),
            value: CurrentVersionPointer::new(version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        })
        .await;
    let _ = storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new("bucket", "alpha", version_id)
                .to_bytes()
                .unwrap()
                .into(),
            value: BlobVersion::materialized(
                hash,
                BackendRef::node_default(),
                created_at,
                created_by,
                None,
            )
            .to_bytes()
            .unwrap()
            .into(),
            txn_id: None,
        })
        .await;

    let result = drive(
        ListObjectsV2Operation::new(ListObjectsV2Input {
            bucket: "bucket".to_string(),
            group_id,
            continuation_token: None,
            max_keys: Some(0),
            prefix: None,
            delimiter: None,
            start_after: None,
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    assert!(result.objects.is_empty());
    assert!(result.continuation_token.is_none());
}

#[tokio::test]
async fn pagination_resumes() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());

    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"],
        created_by,
    )
    .await;

    let mut continuation_token = None;
    let mut all_keys = Vec::new();

    loop {
        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id,
                continuation_token,
                max_keys: Some(3),
                prefix: None,
                delimiter: None,
                start_after: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        for obj in result.objects {
            all_keys.push(obj.head.key);
        }

        continuation_token = result.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    // Verify all 7 keys were returned
    let mut sorted = all_keys.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec!["alpha", "beta", "delta", "epsilon", "eta", "gamma", "zeta"]
    );
}

#[tokio::test]
async fn empty_bucket_lists() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());

    let group_id = Ulid::generate();

    let result = drive(
        ListObjectsV2Operation::new(ListObjectsV2Input {
            bucket: "empty-bucket".to_string(),
            group_id,
            continuation_token: None,
            max_keys: Some(10),
            prefix: None,
            delimiter: None,
            start_after: None,
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    assert!(result.objects.is_empty());
    assert!(result.continuation_token.is_none());
}

#[tokio::test]
async fn reference_object_lists() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());

    let group_id = Ulid::generate();
    let realm_id = RealmId([7u8; 32]);
    let created_by = UserId::local(Ulid::generate(), realm_id);
    let version_id = Ulid::generate();
    let created_at = UNIX_EPOCH + Duration::from_secs(5);
    let last_refresh = UNIX_EPOCH + Duration::from_secs(20);

    let source_metadata = SourceMetadata {
        content_length: 42,
        content_type: Some("text/plain".to_string()),
        etag: Some("ref-etag-1".to_string()),
        last_modified: Some(UNIX_EPOCH + Duration::from_secs(10)),
        source_version: None,
    };

    let version = BlobVersion::reference(
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: "source/path".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: None,
        },
        source_metadata.clone(),
        created_at,
        created_by,
        last_refresh,
    );

    // Write head entry
    let _ = storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new("bucket", "ref-object")
                .to_bytes()
                .unwrap()
                .into(),
            value: CurrentVersionPointer::new(version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        })
        .await;

    // Write version entry (reference, no location)
    let _ = storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new("bucket", "ref-object", version_id)
                .to_bytes()
                .unwrap()
                .into(),
            value: version.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;

    let result = drive(
        ListObjectsV2Operation::new(ListObjectsV2Input {
            bucket: "bucket".to_string(),
            group_id,
            continuation_token: None,
            max_keys: Some(10),
            prefix: None,
            delimiter: None,
            start_after: None,
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    assert_eq!(result.objects.len(), 1);
    assert_eq!(result.objects[0].head.key, "ref-object");
    assert_eq!(result.objects[0].location, None);
    assert_eq!(result.objects[0].source_metadata, Some(source_metadata));
    assert!(result.objects[0].referenced);
    assert_eq!(result.objects[0].kind, Some(SourceConnectorKind::Http));
    assert_eq!(
        result.objects[0].source_path.as_deref(),
        Some("source/path")
    );
    assert_eq!(result.objects[0].connector_id, None);
    assert_eq!(result.objects[0].origin_node_id, None);
    assert_eq!(result.objects[0].last_refresh, Some(last_refresh));
    assert!(result.continuation_token.is_none());
}

async fn seed_materialized_keys(
    storage_handle: &storage::StorageHandle,
    bucket: &str,
    keys: &[&str],
    created_by: UserId,
) {
    let created_at = UNIX_EPOCH + Duration::from_secs(5);
    for (index, key) in keys.iter().enumerate() {
        let version_id = Ulid::generate();
        let hash = [index as u8 + 1; 32];
        let version = BlobVersion::materialized(
            hash,
            BackendRef::node_default(),
            created_at,
            created_by,
            None,
        );
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(bucket, *key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(bucket, *key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: BlobLocationKey::new(hash, BackendRef::node_default())
                    .to_bytes()
                    .into(),
                value: BackendLocation {
                    backend: BackendRef::node_default(),
                    storage_class: None,
                    root: "/tmp".to_string(),
                    storage_bucket: "objects".to_string(),
                    backend_path: format!("path/{key}"),
                    ulid: Ulid::generate(),
                    compressed: false,
                    encrypted: false,
                    created_by,
                    created_at,
                    staging: false,
                    partial: false,
                    blob_size: 42,
                    hashes: HashMap::new(),
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;
    }
}

async fn seed_deleted_keys(
    storage_handle: &storage::StorageHandle,
    bucket: &str,
    keys: &[&str],
    created_by: UserId,
) {
    let created_at = UNIX_EPOCH + Duration::from_secs(5);
    for key in keys.iter() {
        let version_id = Ulid::generate();
        let version = BlobVersion::deleted(created_at, created_by);
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(bucket, *key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(bucket, *key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }
}

async fn list_keys(
    driver_ctx: &DriverContext,
    bucket: &str,
    prefix: Option<&str>,
    start_after: Option<&str>,
) -> Vec<String> {
    let result = drive(
        ListObjectsV2Operation::new(ListObjectsV2Input {
            bucket: bucket.to_string(),
            group_id: Ulid::generate(),
            continuation_token: None,
            max_keys: Some(100),
            prefix: prefix.map(str::to_string),
            delimiter: None,
            start_after: start_after.map(str::to_string),
        }),
        driver_ctx,
    )
    .await
    .unwrap();
    result
        .objects
        .into_iter()
        .map(|object| object.head.key)
        .collect()
}

#[tokio::test]
async fn equal_prefix_included() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["docs/", "docs/readme.md"],
        created_by,
    )
    .await;

    let keys = list_keys(&driver_ctx, "bucket", Some("docs/"), None).await;
    assert_eq!(keys, vec!["docs/", "docs/readme.md"]);

    let keys = list_keys(&driver_ctx, "bucket", Some("docs/readme.md"), None).await;
    assert_eq!(keys, vec!["docs/readme.md"]);
}

#[tokio::test]
async fn short_key_interleaves() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(&storage_handle, "bucket", &["rare0", "rare/1"], created_by).await;

    let keys = list_keys(&driver_ctx, "bucket", Some("rare/"), None).await;
    assert_eq!(keys, vec!["rare/1"]);
}

#[tokio::test]
async fn keys_sorted() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(&storage_handle, "bucket", &["b", "aa", "a/1"], created_by).await;

    let keys = list_keys(&driver_ctx, "bucket", None, None).await;
    assert_eq!(keys, vec!["a/1", "aa", "b"]);
}

#[tokio::test]
async fn start_after_skips() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(&storage_handle, "bucket", &["a", "b", "c"], created_by).await;

    let keys = list_keys(&driver_ctx, "bucket", None, Some("a")).await;
    assert_eq!(keys, vec!["b", "c"]);

    let keys = list_keys(&driver_ctx, "bucket", None, Some("b")).await;
    assert_eq!(keys, vec!["c"]);

    let keys = list_keys(&driver_ctx, "bucket", None, Some("c")).await;
    assert!(keys.is_empty());
}

#[tokio::test]
async fn beyond_prefix_empty() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(&storage_handle, "bucket", &["docs/1", "docs/2"], created_by).await;

    let keys = list_keys(&driver_ctx, "bucket", Some("docs/"), Some("zzz")).await;
    assert!(keys.is_empty());

    let keys = list_keys(&driver_ctx, "bucket", Some("docs/"), Some("a")).await;
    assert_eq!(keys, vec!["docs/1", "docs/2"]);
}

async fn list_page(
    driver_ctx: &DriverContext,
    bucket: &str,
    delimiter: Option<&str>,
    max_keys: usize,
    continuation_token: Option<ListObjectsV2ContinuationToken>,
) -> ListObjectsV2Result {
    drive(
        ListObjectsV2Operation::new(ListObjectsV2Input {
            bucket: bucket.to_string(),
            group_id: Ulid::generate(),
            continuation_token,
            max_keys: Some(max_keys),
            prefix: None,
            delimiter: delimiter.map(str::to_string),
            start_after: None,
        }),
        driver_ctx,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn delimiter_groups() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["a.txt", "dir/1", "dir/2", "z.txt"],
        created_by,
    )
    .await;

    let result = list_page(&driver_ctx, "bucket", Some("/"), 100, None).await;

    let keys: Vec<_> = result
        .objects
        .into_iter()
        .map(|object| object.head.key)
        .collect();
    assert_eq!(keys, vec!["a.txt", "z.txt"]);
    assert_eq!(result.common_prefixes, vec!["dir/"]);
    assert!(result.continuation_token.is_none());
}

#[tokio::test]
async fn delimiter_paginates() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["a", "dir/1", "dir/2", "dir/3", "z"],
        created_by,
    )
    .await;

    let mut continuation_token = None;
    let mut all_keys = Vec::new();
    let mut all_prefixes = Vec::new();
    let mut pages = 0;

    loop {
        let result = list_page(
            &driver_ctx,
            "bucket",
            Some("/"),
            1,
            continuation_token.take(),
        )
        .await;
        all_keys.extend(result.objects.into_iter().map(|object| object.head.key));
        all_prefixes.extend(result.common_prefixes);
        pages += 1;
        assert!(pages < 10);

        continuation_token = result.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    assert_eq!(all_keys, vec!["a", "z"]);
    assert_eq!(all_prefixes, vec!["dir/"]);
}

#[tokio::test]
async fn hydration_preserves_order() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());

    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));
    let created_at = UNIX_EPOCH + Duration::from_secs(5);
    let last_refresh = UNIX_EPOCH + Duration::from_secs(20);

    seed_materialized_keys(&storage_handle, "bucket", &["alpha", "delta"], created_by).await;

    let source_metadata = SourceMetadata {
        content_length: 42,
        content_type: Some("text/plain".to_string()),
        etag: Some("ref-etag-1".to_string()),
        last_modified: Some(UNIX_EPOCH + Duration::from_secs(10)),
        source_version: None,
    };
    let reference = BlobVersion::reference(
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: "source/path".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: None,
        },
        source_metadata.clone(),
        created_at,
        created_by,
        last_refresh,
    );
    let deleted = BlobVersion::deleted(created_at, created_by);

    for (key, version) in [("beta", reference), ("gamma", deleted)] {
        let version_id = Ulid::generate();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("bucket", key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("bucket", key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    let result = drive(
        ListObjectsV2Operation::new(ListObjectsV2Input {
            bucket: "bucket".to_string(),
            group_id: Ulid::generate(),
            continuation_token: None,
            max_keys: Some(10),
            prefix: None,
            delimiter: None,
            start_after: None,
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    let keys: Vec<_> = result
        .objects
        .iter()
        .map(|object| object.head.key.as_str())
        .collect();
    assert_eq!(keys, vec!["alpha", "beta", "delta"]);
    assert!(result.objects[0].location.is_some());
    assert_eq!(result.objects[1].source_metadata, Some(source_metadata));
    assert!(result.objects[2].location.is_some());
}

fn delimiter_input(
    max_keys: usize,
    continuation_token: Option<ListObjectsV2ContinuationToken>,
    delimiter: Option<&str>,
) -> ListObjectsV2Input {
    ListObjectsV2Input {
        bucket: "bucket".to_string(),
        group_id: Ulid::generate(),
        continuation_token,
        max_keys: Some(max_keys),
        prefix: None,
        delimiter: delimiter.map(str::to_string),
        start_after: None,
    }
}

fn step_transaction_started(operation: &mut ListObjectsV2Operation) -> Effects {
    let effects = operation.start();
    assert!(matches!(
        effects[0],
        Effect::Storage(StorageEffect::StartTransaction { .. })
    ));
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::generate(),
    }))
}

#[test]
fn group_round_advances() {
    let mut operation = ListObjectsV2Operation::new(delimiter_input(1, None, Some("/")));

    let effects = step_transaction_started(&mut operation);
    let Effect::Storage(StorageEffect::Iter {
        start: None, limit, ..
    }) = &effects[0]
    else {
        panic!("expected initial scan round: {:?}", effects[0]);
    };

    let pointer: aruna_core::types::Value = CurrentVersionPointer::new(Ulid::generate())
        .to_bytes()
        .unwrap()
        .into();
    let values = (0..*limit)
        .map(|index| {
            (
                BlobHeadKey::new("bucket", format!("dir/{index}"))
                    .to_bytes()
                    .unwrap()
                    .into(),
                pointer.clone(),
            )
        })
        .collect();
    let effects = operation.step(Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after: None,
    }));

    // The heads now need a version read before the group can be emitted.
    let Effect::Storage(StorageEffect::BatchRead { reads, .. }) = &effects[0] else {
        panic!("expected version read for grouped heads: {:?}", effects[0]);
    };
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));
    let created_at = UNIX_EPOCH + Duration::from_secs(5);
    let version: aruna_core::types::Value = BlobVersion::materialized(
        [1u8; 32],
        BackendRef::node_default(),
        created_at,
        created_by,
        None,
    )
    .to_bytes()
    .unwrap()
    .into();
    let version_values = reads
        .iter()
        .map(|(_, key)| (key.clone(), Some(version.clone())))
        .collect();
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: version_values,
    }));

    let Effect::Storage(StorageEffect::Iter { start, .. }) = &effects[0] else {
        panic!("expected follow-up scan round: {:?}", effects[0]);
    };
    assert_eq!(start, &Some(IterStart::At(b"bucket/dir0".to_vec().into())));
}

#[test]
fn group_resume_advances() {
    let token = ListObjectsV2ContinuationToken {
        last_key: BlobHeadKey::new("bucket", "dir/5").to_bytes().unwrap(),
        last_common_prefix: Some("dir/".to_string()),
    };

    let mut operation =
        ListObjectsV2Operation::new(delimiter_input(10, Some(token.clone()), Some("/")));
    let effects = step_transaction_started(&mut operation);
    let Effect::Storage(StorageEffect::Iter { start, .. }) = &effects[0] else {
        panic!("expected resumed scan round: {:?}", effects[0]);
    };
    assert_eq!(start, &Some(IterStart::At(b"bucket/dir0".to_vec().into())));

    // Without the delimiter the group no longer applies: resume behind
    // the exclusive cursor instead of seeking.
    let mut operation = ListObjectsV2Operation::new(delimiter_input(10, Some(token.clone()), None));
    let effects = step_transaction_started(&mut operation);
    let Effect::Storage(StorageEffect::Iter { start, .. }) = &effects[0] else {
        panic!("expected resumed scan round: {:?}", effects[0]);
    };
    assert_eq!(
        start,
        &Some(IterStart::After(token.last_key.clone().into()))
    );
}

#[tokio::test]
async fn large_group_paginates() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    let group_keys: Vec<String> = (0..30).map(|index| format!("dir/{index:02}")).collect();
    let mut keys: Vec<&str> = vec!["a"];
    keys.extend(group_keys.iter().map(String::as_str));
    keys.push("z");
    seed_materialized_keys(&storage_handle, "bucket", &keys, created_by).await;

    let mut continuation_token = None;
    let mut all_keys = Vec::new();
    let mut all_prefixes = Vec::new();
    let mut pages = 0;

    loop {
        let result = list_page(
            &driver_ctx,
            "bucket",
            Some("/"),
            2,
            continuation_token.take(),
        )
        .await;
        all_keys.extend(result.objects.into_iter().map(|object| object.head.key));
        all_prefixes.extend(result.common_prefixes);
        pages += 1;
        assert!(pages <= 3);

        continuation_token = result.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    assert_eq!(all_keys, vec!["a", "z"]);
    assert_eq!(all_prefixes, vec!["dir/"]);
}

// (a) A key whose latest version is a delete marker is absent from Contents,
// including the zero-byte folder marker key equal to the listing prefix.
#[tokio::test]
async fn delete_marker_omitted() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(&storage_handle, "bucket", &["docs/readme.md"], created_by).await;
    seed_deleted_keys(
        &storage_handle,
        "bucket",
        &["docs/", "docs/old.md"],
        created_by,
    )
    .await;

    // Listing inside the folder returns only the live child; neither the
    // delete-markered marker key "docs/" nor the deleted child appear.
    let keys = list_keys(&driver_ctx, "bucket", Some("docs/"), None).await;
    assert_eq!(keys, vec!["docs/readme.md"]);
}

// (b) A prefix whose every key is delete-markered produces NO common prefix
// and lists nothing, even though the head keys still physically exist.
#[tokio::test]
async fn deleted_prefix_hidden() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(&storage_handle, "bucket", &["a.txt"], created_by).await;
    seed_deleted_keys(
        &storage_handle,
        "bucket",
        &["dir/", "dir/1", "dir/2"],
        created_by,
    )
    .await;

    let result = list_page(&driver_ctx, "bucket", Some("/"), 100, None).await;

    let keys: Vec<_> = result
        .objects
        .into_iter()
        .map(|object| object.head.key)
        .collect();
    assert_eq!(keys, vec!["a.txt"]);
    assert!(result.common_prefixes.is_empty());
    assert!(result.continuation_token.is_none());
}

// (c) A prefix mixing live and delete-markered keys still produces the
// common prefix; the scan finds the live sibling behind the marker.
#[tokio::test]
async fn mixed_prefix_kept() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["a.txt", "dir/live"],
        created_by,
    )
    .await;
    seed_deleted_keys(&storage_handle, "bucket", &["dir/dead"], created_by).await;

    let result = list_page(&driver_ctx, "bucket", Some("/"), 100, None).await;

    let keys: Vec<_> = result
        .objects
        .into_iter()
        .map(|object| object.head.key)
        .collect();
    assert_eq!(keys, vec!["a.txt"]);
    assert_eq!(result.common_prefixes, vec!["dir/"]);

    // Listing inside the folder lists only the live key.
    let inside = list_keys(&driver_ctx, "bucket", Some("dir/"), None).await;
    assert_eq!(inside, vec!["dir/live"]);
}

// (d) Pagination across delete-markered keys keeps KeyCount and IsTruncated
// correct: markers never inflate or short-count a page.
#[tokio::test]
async fn markers_skipped() {
    let (_temp_handle, storage_handle) = test_storage();
    let driver_ctx = test_context(storage_handle.clone());
    let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

    seed_materialized_keys(&storage_handle, "bucket", &["a", "c", "e"], created_by).await;
    seed_deleted_keys(&storage_handle, "bucket", &["b", "d"], created_by).await;

    let mut continuation_token = None;
    let mut pages: Vec<Vec<String>> = Vec::new();

    loop {
        let result = list_page(&driver_ctx, "bucket", None, 2, continuation_token.take()).await;
        let truncated = result.continuation_token.is_some();
        let keys: Vec<String> = result
            .objects
            .into_iter()
            .map(|object| object.head.key)
            .collect();
        // A truncated page must be full; only the final page may be short.
        if truncated {
            assert_eq!(keys.len(), 2);
        }
        pages.push(keys);
        continuation_token = result.continuation_token;
        assert!(pages.len() <= 3);
        if continuation_token.is_none() {
            break;
        }
    }

    assert_eq!(pages, vec![vec!["a", "c"], vec!["e"]]);
}
