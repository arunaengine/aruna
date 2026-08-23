use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use aruna_api::routes::storage_deletion::{
    DeletionPreflightRequest, PurgeScopeRequest, SubmitPurgeRequest, deletion_preflight,
    submit_purge,
};
use aruna_api::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::{
    Actor, AuthContext, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
    BucketInfo, CurrentVersionPointer, Group, GroupAuthorizationDocument, NodeCapabilities,
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, VersionKey,
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
            NodeCapabilities::local_node(realm_id).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await,
    );
    state
        .register_rest_interface_with_public_url(
            "127.0.0.1:0".parse().unwrap(),
            Some("https://node.test"),
        )
        .await;
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let group_auth = GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
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
            RealmAuthorizationDocument::new_default_realm_doc(realm_id)
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
            BlobLocationKey::new(hash, BackendRef::node_default()).to_bytes(),
            BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/objects".to_string(),
                storage_bucket: "data".to_string(),
                backend_path: format!("{tag}.blob"),
                ulid: Ulid::generate(),
                compressed: false,
                encrypted: false,
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
        },
    }
}

#[tokio::test]
async fn preflight_counts_versions() {
    let fixture = setup().await;
    let (status, Json(file)) = deletion_preflight(
        State(fixture.state.clone()),
        Extension(Some(fixture.auth.clone())),
        Json(DeletionPreflightRequest {
            scope: PurgeScopeRequest::File {
                bucket: "data".to_string(),
                key: "reads/file.fastq".to_string(),
            },
            limit: Some(10),
            version_key_marker: None,
            version_id_marker: None,
            multipart_key_marker: None,
            multipart_upload_id_marker: None,
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
    assert!(!file.sync_relationships_apply_to_bucket_delete);

    let (_, Json(bucket)) = deletion_preflight(
        State(fixture.state.clone()),
        Extension(Some(fixture.auth.clone())),
        Json(DeletionPreflightRequest {
            scope: PurgeScopeRequest::Bucket {
                bucket: "data".to_string(),
            },
            limit: Some(10),
            version_key_marker: None,
            version_id_marker: None,
            multipart_key_marker: None,
            multipart_upload_id_marker: None,
        }),
    )
    .await
    .unwrap();
    assert!(bucket.sync_relationships_apply_to_bucket_delete);
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
        format!("https://node.test/api/v1/jobs/{}", first.job_id)
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
