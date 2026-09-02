use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use aruna_api::error::ServerError;
use aruna_api::routes::bucket_usage::{BucketUsageQuery, get_bucket_usage};
use aruna_api::routes::storage_routing::{
    BucketRoutingRequest, get_bucket_routing, put_bucket_routing,
};
use aruna_api::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE,
};
use aruna_core::structs::{
    Actor, AuthContext, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
    BucketInfo, CurrentVersionPointer, Group, GroupAuthorizationDocument, MultipartUpload,
    MultipartUploadStatus, NodeCapabilities, RealmAuthorizationDocument, RealmConfigDocument,
    RealmId, VersionKey,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use ed25519_dalek::SigningKey;
use tempfile::TempDir;
use ulid::Ulid;

struct Fixture {
    _directory: TempDir,
    state: Arc<ServerState>,
    owner: AuthContext,
    reader: AuthContext,
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

/// One bucket with two stored versions of a key, one delete marker and one open
/// multipart upload, plus a viewer who only holds group data read.
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
    let reader = UserId::local(Ulid::from_bytes([14u8; 16]), realm_id);
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
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let mut group_auth =
        GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
    let viewer_role = group_auth
        .roles
        .values()
        .find(|role| role.name == "viewer")
        .map(|role| role.role_id)
        .expect("default group doc carries a viewer role");
    group_auth
        .roles
        .get_mut(&viewer_role)
        .expect("viewer role")
        .assigned_users = HashSet::from([reader]);
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
        write_value(
            &state,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new([tag; 32], BackendRef::node_default()).to_bytes(),
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
    let upload_id = Ulid::from_bytes([15u8; 16]);
    write_value(
        &state,
        S3_MULTIPART_UPLOAD_KEYSPACE,
        upload_id.to_bytes().to_vec(),
        MultipartUpload {
            upload_id,
            backend: BackendRef::node_default(),
            storage_class: None,
            bucket: "data".to_string(),
            key: "reads/pending.fastq".to_string(),
            group_id,
            created_by: owner,
            created_at: UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
        }
        .to_bytes()
        .unwrap(),
    )
    .await;

    Fixture {
        _directory: directory,
        state,
        owner: AuthContext {
            user_id: owner,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        reader: AuthContext {
            user_id: reader,
            realm_id,
            path_restrictions: None,
            session: None,
        },
    }
}

#[tokio::test]
async fn usage_counts_inventory() {
    let fixture = setup().await;

    let Json(usage) = get_bucket_usage(
        State(fixture.state.clone()),
        Extension(Some(fixture.reader.clone())),
        Path("data".to_string()),
        Query(BucketUsageQuery { limit: None }),
    )
    .await
    .unwrap();

    assert_eq!(usage.bucket, "data");
    assert_eq!(usage.objects, 1);
    assert_eq!(usage.versions, 2);
    assert_eq!(usage.delete_markers, 1);
    assert_eq!(usage.open_multipart_uploads, 1);
    assert_eq!(usage.logical_bytes, 84);
    assert!(usage.complete);
}

#[tokio::test]
async fn usage_reports_truncation() {
    let fixture = setup().await;

    let Json(usage) = get_bucket_usage(
        State(fixture.state.clone()),
        Extension(Some(fixture.owner.clone())),
        Path("data".to_string()),
        Query(BucketUsageQuery { limit: Some(1) }),
    )
    .await
    .unwrap();

    assert!(!usage.complete);
    assert!(usage.versions + usage.delete_markers <= 1);
}

#[tokio::test]
async fn reader_reads_routing() {
    // The bucket overview shows the backend to anyone who may read the bucket.
    let fixture = setup().await;

    let Json(routing) = get_bucket_routing(
        State(fixture.state.clone()),
        Extension(Some(fixture.reader.clone())),
        Path("data".to_string()),
    )
    .await
    .unwrap();

    assert_eq!(routing.bucket, "data");
    assert!(routing.rules.is_empty());
}

#[tokio::test]
async fn reader_denied_write() {
    let fixture = setup().await;

    let denied = put_bucket_routing(
        State(fixture.state.clone()),
        Extension(Some(fixture.reader.clone())),
        Path("data".to_string()),
        Json(BucketRoutingRequest { rules: Vec::new() }),
    )
    .await;

    assert!(matches!(denied, Err(ServerError::Forbidden)));
}
