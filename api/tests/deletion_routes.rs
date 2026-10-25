//! Tests that deletion preflight counts versions and that purge submission is idempotent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::format::StoredFormat;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use aruna_api::error::ServerError;
use aruna_api::routes::storage::deletion::{
    DeleteImpactRequest, DeletionPreflightRequest, PurgeScopeRequest, SubmitPurgeRequest,
    delete_preview, deletion_preflight, submit_purge,
};
use aruna_api::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
    CurrentVersionPointer, VersionKey,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use ed25519_dalek::SigningKey;
use tempfile::TempDir;
use ulid::Ulid;

struct Fixture {
    _directory: TempDir,
    state: Arc<ServerState>,
    auth: AuthContext,
}

async fn write_value(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let event = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
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

async fn setup() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    let realm_id = RealmId::from_bytes(
        SigningKey::from_bytes(&[7u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let owner = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
    let group_id = Ulid::from_bytes([10u8; 16]);
    let state = Arc::new(
        ServerState::new(
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: Some(TaskHandle::new()),
                compute_handle: None,
            }),
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await,
    );
    state
        .register_rest_public("127.0.0.1:0".parse().unwrap(), Some("https://node.test"))
        .await;
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let group = Group {
        display_name: "storage".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    for (key_space, key, value) in [
        (
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(&actor)
                .unwrap(),
        ),
        (
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::default_realm_doc(realm_id)
                .to_bytes(&actor)
                .unwrap(),
        ),
        (
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group_auth.to_bytes(&actor).unwrap(),
        ),
        (
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        ),
        (
            S3_BUCKET_KEYSPACE,
            b"data".to_vec(),
            BucketInfo {
                group_id,
                created_at: UNIX_EPOCH,
                created_by: owner,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            }
            .to_bytes()
            .unwrap(),
        ),
    ] {
        write_value(&state, key_space, key, value).await;
    }

    let current_id = Ulid::from_bytes([11u8; 16]);
    let old_id = Ulid::from_bytes([12u8; 16]);
    let deleted_id = Ulid::from_bytes([13u8; 16]);
    write_value(
        &state,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new("data", "reads/file.fastq")
            .to_bytes()
            .unwrap(),
        CurrentVersionPointer::new(current_id).to_bytes().unwrap(),
    )
    .await;
    for (version_id, version) in [
        (
            current_id,
            BlobVersion::materialized(
                [1u8; 32],
                BackendRef::node_default(),
                EncodingClass::Raw,
                UNIX_EPOCH + Duration::from_secs(3),
                owner,
                None,
            ),
        ),
        (
            old_id,
            BlobVersion::materialized(
                [2u8; 32],
                BackendRef::node_default(),
                EncodingClass::Raw,
                UNIX_EPOCH + Duration::from_secs(2),
                owner,
                None,
            ),
        ),
        (
            deleted_id,
            BlobVersion::deleted(UNIX_EPOCH + Duration::from_secs(1), owner),
        ),
    ] {
        write_value(
            &state,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("data", "reads/file.fastq", version_id)
                .to_bytes()
                .unwrap(),
            version.to_bytes().unwrap(),
        )
        .await;
    }
    for tag in [1u8, 2u8] {
        let hash = [tag; 32];
        write_value(
            &state,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new(hash, EncodingClass::Raw, BackendRef::node_default()).to_bytes(),
            BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/objects".to_string(),
                storage_bucket: "data".to_string(),
                backend_path: format!("{tag}.blob"),
                ulid: Ulid::generate(),
                format: StoredFormat::default(),
                created_by: owner,
                created_at: UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 42,
                hashes: HashMap::new(),
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
    }

    Fixture {
        _directory: directory,
        state,
        auth: AuthContext {
            user_id: owner,
            realm_id,
            path_restrictions: None,
            session: None,
        },
    }
}

#[tokio::test]
async fn preflight_counts_versions() {
    let fixture = setup().await;
    let (status, Json(file)) = deletion_preflight(
        State(fixture.state.clone()),
        Extension(Some(fixture.auth.clone())),
        Extension(None),
        Json(DeletionPreflightRequest {
            scope: PurgeScopeRequest::File {
                bucket: "data".to_string(),
                key: "reads/file.fastq".to_string(),
            },
            limit: Some(10),
            version_key_marker: None,
            version_id_marker: None,
            multipart_key_marker: None,
            multipart_upload_marker: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(file.counts.current_heads, 1);
    assert_eq!(file.counts.noncurrent_versions, 1);
    assert_eq!(file.counts.delete_markers, 1);
    assert!(file.counts.complete);
    assert!(file.permissions.read);
    assert!(file.permissions.purge);
    assert!(!file.apply_relationships_delete);

    let (_, Json(bucket)) = deletion_preflight(
        State(fixture.state.clone()),
        Extension(Some(fixture.auth.clone())),
        Extension(None),
        Json(DeletionPreflightRequest {
            scope: PurgeScopeRequest::Bucket {
                bucket: "data".to_string(),
            },
            limit: Some(10),
            version_key_marker: None,
            version_id_marker: None,
            multipart_key_marker: None,
            multipart_upload_marker: None,
        }),
    )
    .await
    .unwrap();
    assert!(bucket.apply_relationships_delete);
    assert!(bucket.sync_relationships.is_empty());
}

#[tokio::test]
async fn submission_is_idempotent() {
    let fixture = setup().await;
    let request = SubmitPurgeRequest {
        scope: PurgeScopeRequest::Prefix {
            bucket: "data".to_string(),
            prefix: "reads/".to_string(),
        },
        idempotency_key: Some("purge-reads".to_string()),
        delete_storage_files: false,
        confirm: None,
    };
    let (status, Json(first)) = submit_purge(
        State(fixture.state.clone()),
        Extension(Some(fixture.auth.clone())),
        Json(request.clone()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    assert!(first.created);
    assert_eq!(
        first.status_url,
        format!("https://node.test/api/v1/compute/jobs/{}", first.job_id)
    );

    let (status, Json(replayed)) = submit_purge(
        State(fixture.state),
        Extension(Some(fixture.auth)),
        Json(request),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert!(!replayed.created);
    assert_eq!(replayed.job_id, first.job_id);
}

#[tokio::test]
async fn storage_files_confirmed() {
    let fixture = setup().await;
    let request = |confirm: Option<&str>| SubmitPurgeRequest {
        scope: PurgeScopeRequest::Bucket {
            bucket: "data".to_string(),
        },
        idempotency_key: None,
        delete_storage_files: true,
        confirm: confirm.map(str::to_string),
    };
    let state = || State(fixture.state.clone());
    let auth = || Extension(Some(fixture.auth.clone()));
    // The bucket's storage files go only with the bucket name typed back.
    for confirm in [None, Some("other")] {
        let refused = submit_purge(state(), auth(), Json(request(confirm))).await;
        assert!(matches!(refused, Err(ServerError::BadRequestMessage(_))));
    }
    // A token limited to some paths may not delete files other keys could read.
    let mut restricted = fixture.auth.clone();
    restricted.path_restrictions = Some(Vec::new());
    let refused = submit_purge(
        state(),
        Extension(Some(restricted)),
        Json(request(Some("data"))),
    );
    assert!(matches!(refused.await, Err(ServerError::Forbidden)));
    let (status, _) = submit_purge(state(), auth(), Json(request(Some("data"))))
        .await
        .unwrap();
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test]
async fn previews_storage_files() {
    let fixture = setup().await;
    let backend = BackendRef::Group(Ulid::from_bytes([20u8; 16]));
    for (tag, key) in [(30u8, "linked/a.faa.gz"), (31u8, "linked/b.faa.gz")] {
        let ulid = Ulid::from_bytes([tag; 16]);
        let location = BackendLocation {
            backend: backend.clone(),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: format!("raw/{key}"),
            ulid,
            format: StoredFormat {
                layout: aruna_core::structs::storage::format::StoredLayout::Plain(ulid),
                ..StoredFormat::default()
            },
            created_by: fixture.auth.user_id,
            created_at: UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 100,
            hashes: HashMap::from([(
                aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
                vec![tag; 32],
            )]),
        };
        let version = BlobVersion::materialized(
            [tag; 32],
            backend.clone(),
            location.format.encoding(),
            UNIX_EPOCH,
            fixture.auth.user_id,
            None,
        );
        let id = Ulid::from_bytes([tag; 16]);
        let rows = [
            (
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new("data", key, id).to_bytes().unwrap(),
                version.to_bytes().unwrap(),
            ),
            (
                BLOB_HEAD_KEYSPACE,
                BlobHeadKey::new("data", key).to_bytes().unwrap(),
                CurrentVersionPointer::new(id).to_bytes().unwrap(),
            ),
            (
                BLOB_LOCATIONS_KEYSPACE,
                location.location_key().unwrap().to_bytes(),
                location.to_bytes().unwrap(),
            ),
        ];
        for (space, key, value) in rows {
            write_value(&fixture.state, space, key, value).await;
        }
    }
    let request = |after: Option<String>| DeleteImpactRequest {
        kind: "folder".to_string(),
        key: "linked/".to_string(),
        limit: Some(1),
        after,
        filter: None,
    };
    let state = || State(fixture.state.clone());
    let auth = || Extension(Some(fixture.auth.clone()));
    let bucket = || axum::extract::Path("data".to_string());
    let Json(first) = delete_preview(state(), auth(), bucket(), Json(request(None)))
        .await
        .unwrap();
    assert_eq!((first.files, first.versions, first.complete), (2, 2, true));
    assert_eq!((first.storage_files, first.storage_bytes), (2, 200));
    assert_eq!(first.items.len(), 1);
    // Without a backend version the file would stay; the preview says so.
    let file = first.items[0].storage_file.as_ref().unwrap();
    assert_eq!(file.path, "raw/linked/a.faa.gz");
    assert!(file.kept.is_some());
    let Json(second) = delete_preview(state(), auth(), bucket(), Json(request(first.next)))
        .await
        .unwrap();
    assert_eq!(second.items[0].key, "linked/b.faa.gz");
    assert!(second.next.is_none());
    // A filter narrows the page; the totals stay for the folder.
    let filtered = DeleteImpactRequest {
        filter: Some("b.faa".to_string()),
        limit: Some(10),
        ..request(None)
    };
    let Json(only) = delete_preview(state(), auth(), bucket(), Json(filtered))
        .await
        .unwrap();
    let keys: Vec<_> = only.items.iter().map(|item| item.key.as_str()).collect();
    assert_eq!((keys, only.versions), (vec!["linked/b.faa.gz"], 2));
    // A reader limited to one key sees only that key, in the page and in the totals.
    let bucket_path = aruna_core::structs::storage::blob::bucket_permission_path(
        fixture.state.get_realm_id(),
        Ulid::from_bytes([10u8; 16]),
        fixture.state.get_node_id(),
        "data",
    );
    let allow = |pattern: String| aruna_core::structs::identity::auth::PathRestriction {
        pattern,
        permission: aruna_core::structs::identity::auth::Permission::READ,
    };
    let mut limited = fixture.auth.clone();
    limited.path_restrictions = Some(vec![
        allow(bucket_path.clone()),
        allow(format!("{bucket_path}/linked/a.faa.gz")),
    ]);
    let whole = DeleteImpactRequest {
        limit: Some(10),
        ..request(None)
    };
    let Json(seen) = delete_preview(state(), Extension(Some(limited)), bucket(), Json(whole))
        .await
        .unwrap();
    let keys: Vec<_> = seen.items.iter().map(|item| item.key.as_str()).collect();
    assert_eq!(keys, ["linked/a.faa.gz"]);
    assert_eq!((seen.versions, seen.storage_files), (1, 1));
    let bad = DeleteImpactRequest {
        kind: "everything".to_string(),
        ..request(None)
    };
    let refused = delete_preview(state(), auth(), bucket(), Json(bad)).await;
    assert!(matches!(refused, Err(ServerError::BadRequest)));
}

#[tokio::test]
async fn bounds_filtered_preview() {
    let fixture = setup().await;
    let mut writes = Vec::new();
    for index in 0..=10_000u64 {
        let (key, id) = (format!("many/{index:05}"), Ulid::from_parts(index + 1, 1));
        let version = BlobVersion::deleted(UNIX_EPOCH, fixture.auth.user_id);
        let version_key = VersionKey::new("data", key.clone(), id).to_bytes().unwrap();
        let head_key = BlobHeadKey::new("data", key).to_bytes().unwrap();
        let head = CurrentVersionPointer::new(id).to_bytes().unwrap();
        writes.push((
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.into(),
            version.to_bytes().unwrap().into(),
        ));
        writes.push((BLOB_HEAD_KEYSPACE.to_string(), head_key.into(), head.into()));
    }
    let event = fixture
        .state
        .get_ctx()
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
    let request = DeleteImpactRequest {
        kind: "folder".to_string(),
        key: "many/".to_string(),
        limit: Some(1000),
        after: None,
        filter: Some("absent".to_string()),
    };
    let Json(page) = delete_preview(
        State(fixture.state.clone()),
        Extension(Some(fixture.auth.clone())),
        axum::extract::Path("data".to_string()),
        Json(request),
    )
    .await
    .unwrap();
    assert!(page.items.is_empty() && !page.complete);
    assert!(page.next.is_some());
}

#[tokio::test]
async fn shows_purge_pause() {
    use aruna_api::routes::storage::deletion::{control_purge, get_purge};
    let fixture = setup().await;
    let request = SubmitPurgeRequest {
        scope: PurgeScopeRequest::Prefix {
            bucket: "data".to_string(),
            prefix: "reads/".to_string(),
        },
        idempotency_key: None,
        delete_storage_files: true,
        confirm: None,
    };
    let state = || State(fixture.state.clone());
    let auth = || Extension(Some(fixture.auth.clone()));
    let (_, Json(job)) = submit_purge(state(), auth(), Json(request)).await.unwrap();
    let path = |action: &str| axum::extract::Path((job.job_id.clone(), action.to_string()));
    // A restricted token steers no purge of storage files.
    let mut restricted = fixture.auth.clone();
    restricted.path_restrictions = Some(Vec::new());
    let refused = control_purge(state(), Extension(Some(restricted)), path("pause")).await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
    let Json(paused) = control_purge(state(), auth(), path("pause")).await.unwrap();
    assert_eq!((paused.state.as_str(), paused.paused), ("paused", true));
    let id = || axum::extract::Path(job.job_id.clone());
    let Json(shown) = get_purge(state(), auth(), id()).await.unwrap();
    assert!(shown.paused && shown.delete_storage_files);
    let Json(resumed) = control_purge(state(), auth(), path("resume"))
        .await
        .unwrap();
    assert!(!resumed.paused);
    assert_ne!(resumed.state, "paused");
}
