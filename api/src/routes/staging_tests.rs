use super::*;
use crate::openapi::ApiDoc;
use crate::tests::routes::{
    seed_realm_auth, seed_realm_config, test_context, test_state, test_storage,
};
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::{
    Actor, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
    CurrentVersionPointer, Group, GroupAuthorizationDocument, NodeCapabilities, PathRestriction,
    PortableSourceDescriptor, SourceConnectorKind, SourceMetadata, StagingStrategy, VersionKey,
    VersionSourceBinding,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::replication::queue::{LiveObligationRecord, live_obligation_key};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use tempfile::TempDir;
use ulid::Ulid;

struct TestState {
    _storage_dir: TempDir,
    state: Arc<ServerState>,
    bucket_group_id: Ulid,
    connector_id: Ulid,
    source_path: String,
    bucket: String,
    key: String,
    auth_with_bucket_read: AuthContext,
    auth_with_source_read: AuthContext,
    auth_without_source_read: AuthContext,
}

#[test]
fn job_cursor_roundtrip() {
    let cursor = vec![7u8; 24];
    let encoded = encode_job_cursor(Some(cursor.clone())).unwrap();

    assert_eq!(decode_job_cursor(Some(&encoded)).unwrap(), Some(cursor));
    assert!(decode_job_cursor(Some("invalid")).is_err());
}

#[tokio::test]
async fn snapshot_requires_read() {
    let test = setup_state().await;

    let result = snapshot_blob(
        test.state.clone(),
        test.auth_without_source_read,
        StageTargetRequest {
            group_id: test.bucket_group_id.to_string(),
            connector_id: test.connector_id.to_string(),
            source_path: test.source_path,
            bucket: test.bucket,
            key: test.key,
        },
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn reference_auth_succeeds() {
    let test = setup_state().await;

    let result = reference_blob(
        test.state.clone(),
        test.auth_with_source_read,
        StageTargetRequest {
            group_id: test.bucket_group_id.to_string(),
            connector_id: test.connector_id.to_string(),
            source_path: test.source_path,
            bucket: test.bucket,
            key: test.key,
        },
    )
    .await;

    assert!(matches!(result, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn references_list_bindings() {
    let test = setup_state().await;
    let origin = seed_reference_objects(&test).await;

    let (status, Json(mut first)) = list_references(
        State(test.state.clone()),
        Extension(Some(test.auth_with_bucket_read.clone())),
        Query(ReferenceListQuery {
            bucket: test.bucket.clone(),
            prefix: Some("data/".to_string()),
            limit: Some(2),
            cursor: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first.entries.len(), 2);

    let (_, Json(second)) = list_references(
        State(test.state.clone()),
        Extension(Some(test.auth_with_bucket_read.clone())),
        Query(ReferenceListQuery {
            bucket: test.bucket.clone(),
            prefix: Some("data/".to_string()),
            limit: Some(2),
            cursor: first.next_cursor.take(),
        }),
    )
    .await
    .unwrap();
    assert!(second.next_cursor.is_none());
    first.entries.extend(second.entries);
    assert_eq!(first.entries.len(), 3);
    assert!(
        first
            .entries
            .iter()
            .all(|entry| entry.key.starts_with("data/"))
    );

    let materialized = first
        .entries
        .iter()
        .find(|entry| entry.key == "data/a-materialized")
        .unwrap();
    assert_eq!(materialized.size, 42);
    assert!(!materialized.referenced);
    assert_eq!(materialized.kind, None);
    assert_eq!(materialized.source_path, None);
    assert_eq!(materialized.connector_id, None);
    assert_eq!(materialized.origin_node_id, None);

    let external = first
        .entries
        .iter()
        .find(|entry| entry.key == "data/b-external")
        .unwrap();
    assert_eq!(external.size, 64);
    assert!(external.referenced);
    assert_eq!(external.kind, Some(ApiConnectorKind::Http));
    assert_eq!(external.source_path.as_deref(), Some("remote/file.txt"));
    let connector_id = test.connector_id.to_string();
    assert_eq!(
        external.connector_id.as_deref(),
        Some(connector_id.as_str())
    );
    assert_eq!(external.origin_node_id, None);

    let native = first
        .entries
        .iter()
        .find(|entry| entry.key == "data/c-native")
        .unwrap();
    assert_eq!(native.size, 128);
    assert!(native.referenced);
    assert_eq!(native.kind, Some(ApiConnectorKind::ArunaNative));
    assert_eq!(native.source_path.as_deref(), Some("source-bucket/native"));
    assert_eq!(native.connector_id, None);
    let origin_node_id = origin.to_string();
    assert_eq!(
        native.origin_node_id.as_deref(),
        Some(origin_node_id.as_str())
    );
}

#[tokio::test]
async fn references_deny_read() {
    let test = setup_state().await;

    let result = list_references(
        State(test.state),
        Extension(Some(test.auth_without_source_read)),
        Query(ReferenceListQuery {
            bucket: test.bucket,
            prefix: None,
            limit: None,
            cursor: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn queue_failure_repairable() {
    let test = setup_state().await;
    let version_id = Ulid::generate();
    write_doc(
        &test.state.get_ctx(),
        S3_BUCKET_KEYSPACE,
        test.bucket.as_bytes().to_vec().into(),
        b"not a bucket replication config".to_vec().into(),
    )
    .await;

    let obligation = LiveObligationRecord::new(
        test.state.get_node_id(),
        test.auth_with_source_read.clone(),
        test.bucket.clone(),
        test.key.clone(),
        version_id,
        false,
    );
    let obligation_key = live_obligation_key(&obligation).unwrap();
    write_doc(
        &test.state.get_ctx(),
        BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
        obligation_key.as_ref().to_vec().into(),
        postcard::to_allocvec(&obligation).unwrap().into(),
    )
    .await;

    queue_live_replication(
        &test.state,
        test.auth_with_source_read,
        test.bucket,
        test.key,
        version_id,
        false,
    )
    .await;

    assert!(
        read_doc(
            &test.state.get_ctx(),
            BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
            obligation_key.as_ref().to_vec().into(),
        )
        .await
        .is_some(),
        "durable obligation should remain repairable when staging queue kick fails"
    );
}

#[test]
fn staging_submit_classifies() {
    // Job admission refusals must keep their status instead of collapsing into 500.
    use aruna_operations::jobs::submit::SubmitJobError;

    assert_eq!(
        map_submit_error(SubmitJobError::PlacementUnavailable(
            "no binding".to_string()
        ))
        .status_code(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        map_submit_error(SubmitJobError::InvalidWorkspace("bad".to_string())).status_code(),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn quota_maps_forbidden() {
    let error = map_snapshot_error(MaterializeSnapshotError::Write(
        PutObjectError::QuotaExceeded {
            limit: 100,
            usage: 200,
        },
    ));
    assert!(matches!(error, ServerError::Forbidden));
}

#[test]
fn batch_keeps_failures() {
    let success = stage_result(
        StageBatchItem {
            source_path: "a.txt".to_string(),
            target_key: "a.txt".to_string(),
        },
        Ok(()),
    );
    let failure = stage_result(
        StageBatchItem {
            source_path: "missing.txt".to_string(),
            target_key: "missing.txt".to_string(),
        },
        Err(ServerError::NotFound),
    );

    assert_eq!(success.status, StageBatchStatus::Ok);
    assert_eq!(failure.status, StageBatchStatus::Error);
    assert_eq!(failure.error.as_deref(), Some("Not found"));
}

#[test]
fn prefix_expands_paths() {
    let items = map_prefix_entries(
        vec![SourceEntry {
            name: "file.txt".to_string(),
            path: "folder/nested/file.txt".to_string(),
            kind: SourceEntryKind::File,
            size: Some(4),
            modified: None,
            stat: None,
        }],
        "folder/",
        "imported",
    );

    assert_eq!(
        items,
        vec![StageBatchItem {
            source_path: "folder/nested/file.txt".to_string(),
            target_key: "imported/nested/file.txt".to_string(),
        }]
    );
}

#[test]
fn prefix_normalizes_root() {
    assert_eq!(normalize_prefix(".").unwrap(), "");
    assert_eq!(normalize_prefix("./").unwrap(), "");
    assert_eq!(normalize_prefix("./refseq/").unwrap(), "refseq/");
    assert!(normalize_prefix("refseq/./nested").is_err());
    assert!(normalize_prefix("../refseq").is_err());
}

#[test]
fn batch_enforces_cap() {
    assert!(ensure_batch_capacity(999, 1, 1000).is_ok());
    assert!(matches!(
        ensure_batch_capacity(1000, 1, 1000),
        Err(ServerError::BadRequestReason(message)) if message.contains("1000")
    ));
}

#[tokio::test]
async fn batch_rejects_cap() {
    let test = setup_state().await;
    let items = (0..1001)
        .map(|index| StageBatchItem {
            source_path: format!("source-{index}"),
            target_key: format!("target-{index}"),
        })
        .collect();

    let result = stage_batch(
        State(test.state),
        Extension(Some(test.auth_with_source_read)),
        Json(StageBatchRequest {
            group_id: test.bucket_group_id.to_string(),
            node_id: None,
            connector_id: test.connector_id.to_string(),
            bucket: test.bucket,
            strategy: ApiStagingStrategy::Snapshot,
            items: Some(items),
            prefixes: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn batch_rejects_node() {
    let test = setup_state().await;
    let other_node = iroh::SecretKey::from_bytes(&[17u8; 32]).public();

    let result = stage_batch(
        State(test.state),
        Extension(Some(test.auth_with_source_read)),
        Json(StageBatchRequest {
            group_id: test.bucket_group_id.to_string(),
            node_id: Some(other_node.to_string()),
            connector_id: test.connector_id.to_string(),
            bucket: test.bucket,
            strategy: ApiStagingStrategy::Snapshot,
            items: None,
            prefixes: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn batch_sync_unimplemented() {
    let test = setup_state().await;

    let result = stage_batch(
        State(test.state),
        Extension(Some(test.auth_with_source_read)),
        Json(StageBatchRequest {
            group_id: test.bucket_group_id.to_string(),
            node_id: None,
            connector_id: test.connector_id.to_string(),
            bucket: test.bucket,
            strategy: ApiStagingStrategy::Sync,
            items: None,
            prefixes: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Unimplemented)));
}

#[test]
fn openapi_has_staging() {
    let openapi = ApiDoc::openapi();

    assert!(openapi.paths.paths.contains_key("/data/staging"));
    assert!(openapi.paths.paths.contains_key("/data/staging/batch"));
    assert!(openapi.paths.paths.contains_key("/data/staging/references"));
    assert!(!openapi.paths.paths.contains_key("/blobs/staging"));
}

async fn seed_reference_objects(test: &TestState) -> NodeId {
    let created_by = test.auth_with_bucket_read.user_id;
    let materialized_hash = [21u8; 32];
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "objects".to_string(),
        backend_path: "materialized".to_string(),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by,
        created_at: UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 42,
        hashes: HashMap::new(),
    };
    write_doc(
        &test.state.get_ctx(),
        BLOB_LOCATIONS_KEYSPACE,
        BlobLocationKey::new(materialized_hash, location.backend.clone())
            .to_bytes()
            .into(),
        location.to_bytes().unwrap().into(),
    )
    .await;
    for key in ["data/a-materialized", "other/d-materialized"] {
        write_blob_version(
            &test.state.get_ctx(),
            &test.bucket,
            key,
            BlobVersion::materialized(
                materialized_hash,
                BackendRef::node_default(),
                UNIX_EPOCH,
                created_by,
                None,
            ),
        )
        .await;
    }

    write_blob_version(
        &test.state.get_ctx(),
        &test.bucket,
        "data/b-external",
        BlobVersion::reference(
            VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::Http,
                    public_config: HashMap::new(),
                    source_path: "remote/file.txt".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                },
                connector_id: Some(test.connector_id),
            },
            SourceMetadata {
                content_length: 64,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
            },
            UNIX_EPOCH,
            created_by,
            UNIX_EPOCH,
        ),
    )
    .await;

    let origin = iroh::SecretKey::from_bytes(&[19u8; 32]).public();
    write_blob_version(
        &test.state.get_ctx(),
        &test.bucket,
        "data/c-native",
        BlobVersion::reference(
            VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::ArunaNative,
                    public_config: HashMap::new(),
                    source_path: "source-bucket/native".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: Some(origin),
                },
                connector_id: None,
            },
            SourceMetadata {
                content_length: 128,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
            },
            UNIX_EPOCH,
            created_by,
            UNIX_EPOCH,
        ),
    )
    .await;
    origin
}

async fn write_blob_version(
    driver_ctx: &Arc<DriverContext>,
    bucket: &str,
    key: &str,
    version: BlobVersion,
) {
    let version_id = Ulid::generate();
    write_doc(
        driver_ctx,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
        CurrentVersionPointer::new(version_id)
            .to_bytes()
            .unwrap()
            .into(),
    )
    .await;
    write_doc(
        driver_ctx,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new(bucket, key, version_id)
            .to_bytes()
            .unwrap()
            .into(),
        version.to_bytes().unwrap().into(),
    )
    .await;
}

async fn setup_state() -> TestState {
    let (storage_dir, storage_handle) = test_storage();
    let realm_signing_key = ed25519_dalek::SigningKey::from_bytes(&[5u8; 32]);
    let realm_id =
        aruna_core::structs::RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let node_id = iroh::SecretKey::from_bytes(&[13u8; 32]).public();
    let user_with_source_read = UserId::local(Ulid::generate(), realm_id);
    let user_without_source_read = UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id: user_with_source_read,
        realm_id,
    };
    let driver_ctx = Arc::new(test_context(storage_handle));

    let bucket_group_id = Ulid::generate();
    let source_group_id = Ulid::generate();
    let mut bucket_auth = GroupAuthorizationDocument::default_group_doc(
        user_with_source_read,
        realm_id,
        bucket_group_id,
    );
    for role in bucket_auth.roles.values_mut() {
        role.assigned_users.insert(user_without_source_read);
    }
    let mut source_auth = GroupAuthorizationDocument::default_group_doc(
        user_with_source_read,
        realm_id,
        source_group_id,
    );
    for role in source_auth.roles.values_mut() {
        role.assigned_users.remove(&user_without_source_read);
    }

    let bucket_group = Group {
        display_name: "bucket-group".to_string(),
        group_id: bucket_group_id,
        realm_id,
        owner: user_with_source_read,
        roles: bucket_auth.roles.keys().copied().collect(),
    };
    let source_group = Group {
        display_name: "source-group".to_string(),
        group_id: source_group_id,
        realm_id,
        owner: user_with_source_read,
        roles: source_auth.roles.keys().copied().collect(),
    };
    seed_realm_auth(&driver_ctx, realm_id, &actor).await;
    seed_realm_config(&driver_ctx, realm_id, &actor).await;
    write_doc(
        &driver_ctx,
        AUTH_KEYSPACE,
        bucket_group_id.to_bytes().into(),
        bucket_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
    write_doc(
        &driver_ctx,
        AUTH_KEYSPACE,
        source_group_id.to_bytes().into(),
        source_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
    write_doc(
        &driver_ctx,
        GROUP_KEYSPACE,
        bucket_group_id.to_bytes().into(),
        bucket_group.to_bytes(&actor).unwrap().into(),
    )
    .await;
    write_doc(
        &driver_ctx,
        GROUP_KEYSPACE,
        source_group_id.to_bytes().into(),
        source_group.to_bytes(&actor).unwrap().into(),
    )
    .await;

    let bucket = "stage-bucket".to_string();
    let key = "test.txt".to_string();
    let connector_id = Ulid::generate();
    let source_path = "folder/file.txt".to_string();
    let bucket_info = BucketInfo {
        group_id: bucket_group_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: user_with_source_read,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    write_doc(
        &driver_ctx,
        S3_BUCKET_KEYSPACE,
        bucket.as_bytes().to_vec().into(),
        bucket_info.to_bytes().unwrap().into(),
    )
    .await;

    let state = Arc::new(
        test_state(
            driver_ctx,
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );

    let target_path =
        crate::auth::blob_permission_path(state.as_ref(), bucket_group_id, &bucket, &key);
    let bucket_path = bucket_permission_path(realm_id, bucket_group_id, node_id, &bucket);
    let source_path_restriction =
        connector_permission_path(state.as_ref(), bucket_group_id, connector_id, &source_path);

    TestState {
        _storage_dir: storage_dir,
        state,
        bucket_group_id,
        connector_id,
        source_path,
        bucket,
        key,
        auth_with_bucket_read: AuthContext {
            user_id: user_with_source_read,
            realm_id,
            path_restrictions: Some(vec![PathRestriction {
                pattern: bucket_path,
                permission: Permission::READ,
            }]),
            session: None,
        },
        auth_with_source_read: AuthContext {
            user_id: user_with_source_read,
            realm_id,
            path_restrictions: Some(vec![
                PathRestriction {
                    pattern: target_path.clone(),
                    permission: Permission::WRITE,
                },
                PathRestriction {
                    pattern: source_path_restriction,
                    permission: Permission::READ,
                },
            ]),
            session: None,
        },
        auth_without_source_read: AuthContext {
            user_id: user_without_source_read,
            realm_id,
            path_restrictions: Some(vec![PathRestriction {
                pattern: target_path,
                permission: Permission::WRITE,
            }]),
            session: None,
        },
    }
}

async fn write_doc(
    driver_ctx: &Arc<DriverContext>,
    key_space: &str,
    key: byteview::ByteView,
    value: byteview::ByteView,
) {
    let event = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn read_doc(
    driver_ctx: &Arc<DriverContext>,
    key_space: &str,
    key: byteview::ByteView,
) -> Option<byteview::ByteView> {
    let event = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("unexpected storage event")
    };

    value
}
