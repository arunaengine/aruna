//! Tests RO-Crate upload streaming, media type and size caps, and import job submission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::server::state::ROCRATE_UPLOAD_SLOTS;
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::execution::job::{RoCrateLimits, RoCrateUploadRecord};
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities, PathRestriction};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BackendLocation, BackendRef, BucketInfo,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::import::write_rocrate_upload;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use axum::http::{HeaderValue, Request};
use byteview::ByteView;
use std::collections::HashMap;
use std::time::SystemTime;
use tempfile::TempDir;
use tower::ServiceExt;

#[tokio::test]
async fn invenio_requires_auth() {
    use crate::metadata::InvenioExportRequest;
    use crate::routes::invenio::{
        InvenioImportRequest, SubmitInvenioExport, export_record, import_record,
    };
    let (_root, state, user) = plain_state(test_limits()).await;
    for auth in [None, restricted(user)] {
        let group_id = Ulid::generate().to_string();
        let import = InvenioImportRequest {
            group_id: group_id.clone(),
            connector_id: Ulid::generate().to_string(),
            record_id: Some("42".into()),
            doi: None,
            url: None,
            options: Default::default(),
            keep_updated: false,
            auto_update: None,
            target: ImportTargetRequest {
                bucket: "target".into(),
                prefix: String::new(),
            },
            metadata: ImportMetadataRequest {
                group_id: group_id.clone(),
                path: "datasets/import".into(),
                public: false,
            },
            idempotency_key: None,
        };
        let result =
            import_record(State(state.clone()), Extension(auth.clone()), Json(import)).await;
        assert!(matches!(
            result,
            Err(ServerError::Unauthorized | ServerError::Forbidden)
        ));
        let query = crate::routes::invenio::InvenioSearch {
            group_id: group_id.clone(),
            connector_id: Ulid::generate().to_string(),
            q: "dataset".into(),
            page: 1,
            size: 25,
            all_versions: false,
        };
        let result = crate::routes::invenio::search_records(
            State(state.clone()),
            Extension(auth.clone()),
            axum::extract::Query(query),
        )
        .await;
        assert!(matches!(
            result,
            Err(ServerError::Unauthorized | ServerError::Forbidden)
        ));
        let export = SubmitInvenioExport {
            repository: InvenioExportRequest {
                group_id,
                connector_id: Ulid::generate().to_string(),
                draft_id: None,
                new_version: None,
                metadata: serde_json::json!({}),
                publish: true,
                public_files: false,
                access_token: Some("author-token".into()),
            },
            idempotency_key: None,
        };
        let result = export_record(
            State(state.clone()),
            Extension(auth),
            axum::extract::Path("invalid".into()),
            Json(export),
        )
        .await;
        assert!(matches!(
            result,
            Err(ServerError::Unauthorized | ServerError::Forbidden)
        ));
    }
}

#[tokio::test]
async fn invenio_denies_connector() {
    use crate::routes::invenio::{InvenioImportRequest, import_record};
    let (_root, state, user, group) = submit_state().await;
    let request = InvenioImportRequest {
        group_id: Ulid::generate().to_string(),
        connector_id: Ulid::generate().to_string(),
        record_id: Some("42".into()),
        doi: None,
        url: None,
        options: Default::default(),
        keep_updated: false,
        auto_update: None,
        target: ImportTargetRequest {
            bucket: "target".into(),
            prefix: "import".into(),
        },
        metadata: ImportMetadataRequest {
            group_id: group.to_string(),
            path: "crate".into(),
            public: false,
        },
        idempotency_key: None,
    };
    let result = import_record(State(state), Extension(auth(user)), Json(request)).await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn invenio_names_one_record() {
    use crate::routes::invenio::{InvenioImportRequest, import_record};
    let (_root, state, user, group) = submit_state().await;
    let request = |record_id: Option<&str>, doi: Option<&str>| InvenioImportRequest {
        group_id: group.to_string(),
        connector_id: Ulid::generate().to_string(),
        record_id: record_id.map(str::to_string),
        doi: doi.map(str::to_string),
        url: None,
        options: Default::default(),
        keep_updated: false,
        auto_update: None,
        target: ImportTargetRequest {
            bucket: "target".into(),
            prefix: "import".into(),
        },
        metadata: ImportMetadataRequest {
            group_id: group.to_string(),
            path: "crate".into(),
            public: false,
        },
        idempotency_key: None,
    };
    for (record_id, doi) in [(None, None), (Some("42"), Some("10.5281/zenodo.42"))] {
        let result = import_record(
            State(state.clone()),
            Extension(auth(user)),
            Json(request(record_id, doi)),
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
    }
    let lone = parse_import_source(ImportSourceRequest::Invenio {
        group_id: group.to_string(),
        connector_id: Ulid::generate().to_string(),
        record_id: "42".into(),
        options: Default::default(),
        keep_updated: false,
        auto_update: Some(true),
    });
    assert!(matches!(lone, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn keep_updated_needs_write() {
    use crate::routes::invenio::{InvenioImportRequest, import_record};
    let (_root, state, user, group) = submit_state().await;
    seed_bucket(&state, "target", group, user).await;
    // The connector's group is another owner's; the caller only reads it.
    let shared = Ulid::generate();
    grant_reader(&state, user, shared).await;
    let request = |keep_updated| InvenioImportRequest {
        group_id: shared.to_string(),
        connector_id: Ulid::generate().to_string(),
        record_id: Some("42".into()),
        doi: None,
        url: None,
        options: Default::default(),
        keep_updated,
        auto_update: None,
        target: ImportTargetRequest {
            bucket: "target".into(),
            prefix: "import".into(),
        },
        metadata: ImportMetadataRequest {
            group_id: group.to_string(),
            path: "crate".into(),
            public: false,
        },
        idempotency_key: None,
    };
    let import = |keep_updated| {
        import_record(
            State(state.clone()),
            Extension(auth(user)),
            Json(request(keep_updated)),
        )
    };
    let once = import(false).await;
    assert!(
        once.is_ok(),
        "a one-time import needs only READ on the connector group"
    );
    let kept = import(true).await;
    assert!(matches!(kept, Err(ServerError::Forbidden)));
}

#[test]
fn invenio_openapi_contract() {
    let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();
    assert!(openapi["paths"]["/metadata/invenio/records"]["get"]["responses"]["200"].is_object());
    for path in [
        "/metadata/invenio/imports",
        "/metadata/{document_id}/invenio/exports",
    ] {
        let operation = &openapi["paths"][path]["post"];
        assert!(operation["responses"]["202"].is_object());
        assert_eq!(
            operation["security"][0],
            serde_json::json!({"bearer_auth": []})
        );
    }
    let request =
        serde_json::json!({"group_id": "group", "connector_id": "connector", "metadata": {}});
    let default: crate::metadata::InvenioExportRequest =
        serde_json::from_value(request.clone()).unwrap();
    assert!(!default.publish);
    assert!(!default.public_files);
    let mut configured = request;
    configured["publish"] = serde_json::json!(true);
    let configured: crate::metadata::InvenioExportRequest =
        serde_json::from_value(configured).unwrap();
    assert!(configured.publish);
}

#[test]
fn invenio_mode_contract() {
    for mode in ["copy", "reference", "metadata"] {
        let request: crate::routes::invenio::InvenioImportRequest =
            serde_json::from_value(serde_json::json!({
                "group_id": "group", "connector_id": "connector", "record_id": "42", "mode": mode,
                "all_versions": false, "target": {"bucket": "target", "prefix": "import"},
                "metadata": {"group_id": "group", "path": "crate", "public": false}
            }))
            .unwrap();
        assert!(!request.options.all_versions);
        assert_eq!(serde_json::to_value(&request).unwrap()["mode"], mode);
    }
    let options: crate::routes::invenio::InvenioOptionsRequest =
        serde_json::from_value(serde_json::json!({})).unwrap();
    assert!(options.all_versions);
    assert!(matches!(
        options.mode,
        crate::routes::invenio::InvenioDataMode::Copy
    ));
}

#[test]
fn invenio_login_private() {
    let request: crate::metadata::InvenioExportRequest =
        serde_json::from_value(serde_json::json!({
            "group_id": "group", "connector_id": "connector", "access_token": "author-private-token"
        }))
        .unwrap();
    assert_eq!(
        request.access_token.as_deref(),
        Some("author-private-token")
    );
    assert!(
        serde_json::to_value(&request)
            .unwrap()
            .get("access_token")
            .is_none()
    );
    assert!(!format!("{request:?}").contains("author-private-token"));
    let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();
    assert_eq!(
        openapi["components"]["schemas"]["InvenioExportRequest"]["properties"]["access_token"]["writeOnly"],
        true
    );
}

fn realm() -> RealmId {
    RealmId::from_bytes([1u8; 32])
}

fn auth(user: UserId) -> Option<AuthContext> {
    Some(AuthContext {
        user_id: user,
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    })
}

fn restricted(user: UserId) -> Option<AuthContext> {
    Some(AuthContext {
        user_id: user,
        realm_id: realm(),
        path_restrictions: Some(vec![PathRestriction {
            pattern: "/**".to_string(),
            permission: Permission::READ,
        }]),
        session: None,
    })
}

fn test_limits() -> RoCrateLimits {
    RoCrateLimits {
        direct_upload_bytes: 1024,
        import_source_bytes: 1024,
        ..RoCrateLimits::default()
    }
}

async fn write_doc(state: &ServerState, key_space: &str, key: ByteView, value: ByteView) {
    let event = state
        .get_ctx()
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

async fn grant(state: &ServerState, user: UserId, group: Ulid) {
    let realm = realm();
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: user,
        realm_id: realm,
    };
    let group_auth = GroupAuthorizationDocument::default_group_doc(user, realm, group);
    let group_doc = Group {
        display_name: "import-group".to_string(),
        group_id: group,
        realm_id: realm,
        roles: group_auth.roles.keys().copied().collect(),
        owner: user,
    };
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm);
    write_doc(
        state,
        AUTH_KEYSPACE,
        (*realm.as_bytes()).into(),
        realm_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
    write_doc(
        state,
        AUTH_KEYSPACE,
        group.to_bytes().into(),
        group_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
    write_doc(
        state,
        GROUP_KEYSPACE,
        group.to_bytes().into(),
        group_doc.to_bytes(&actor).unwrap().into(),
    )
    .await;
}

/// Creates `group` owned by another user, with `reader` in its default viewer role.
async fn grant_reader(state: &ServerState, reader: UserId, group: Ulid) {
    let owner = UserId::local(Ulid::generate(), realm());
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: owner,
        realm_id: realm(),
    };
    let mut group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm(), group);
    group_auth
        .roles
        .values_mut()
        .find(|role| role.name == "viewer")
        .unwrap()
        .assigned_users
        .insert(reader);
    let group_doc = Group {
        display_name: "shared-group".to_string(),
        group_id: group,
        realm_id: realm(),
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    let auth_doc = group_auth.to_bytes(&actor).unwrap();
    write_doc(
        state,
        AUTH_KEYSPACE,
        group.to_bytes().into(),
        auth_doc.into(),
    )
    .await;
    let group_bytes = group_doc.to_bytes(&actor).unwrap();
    write_doc(
        state,
        GROUP_KEYSPACE,
        group.to_bytes().into(),
        group_bytes.into(),
    )
    .await;
}

async fn seed_bucket(state: &ServerState, bucket: &str, group: Ulid, user: UserId) {
    let info = BucketInfo {
        group_id: group,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    write_doc(
        state,
        S3_BUCKET_KEYSPACE,
        bucket.as_bytes().into(),
        info.to_bytes().unwrap().into(),
    )
    .await;
}

async fn seed_upload(
    state: &ServerState,
    upload_id: Ulid,
    owner: UserId,
    size: u64,
    expires_at_ms: u64,
) {
    let record = RoCrateUploadRecord {
        upload_id,
        owner,
        location: BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "storage".to_string(),
            backend_path: format!("_jobs/{upload_id}/input"),
            ulid: Ulid::from_bytes([9u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: owner,
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: size,
            hashes: HashMap::new(),
        },
        blake3: [5u8; 32],
        size,
        media_type: RoCrateMediaType::Zip,
        expires_at_ms,
        claimed_by: None,
    };
    write_rocrate_upload(&state.get_ctx().storage_handle, &record)
        .await
        .unwrap();
}

fn submit_request(
    upload_id: Ulid,
    group: Ulid,
    prefix: &str,
    key: Option<&str>,
) -> SubmitImportRequest {
    SubmitImportRequest {
        source: ImportSourceRequest::Upload {
            upload_id: upload_id.to_string(),
        },
        target: ImportTargetRequest {
            bucket: "target".to_string(),
            prefix: prefix.to_string(),
        },
        metadata: ImportMetadataRequest {
            group_id: group.to_string(),
            path: "crate".to_string(),
            public: false,
        },
        idempotency_key: key.map(str::to_string),
    }
}

async fn plain_state(limits: RoCrateLimits) -> (TempDir, Arc<ServerState>, UserId) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let node_id = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
    let user = UserId::local(Ulid::generate(), realm());
    let ctx = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    let state = Arc::new(
        ServerState::new(
            ctx,
            realm(),
            node_id,
            NodeCapabilities::user_node(realm()).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await
        .with_rocrate_limits(limits),
    );
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.seed_default_placement();
    config.ensure_node(node_id, RealmNodeKind::Server);
    config.seed_job_control(node_id, 0);
    let actor = Actor {
        node_id,
        user_id: user,
        realm_id: realm(),
    };
    write_doc(
        &state,
        REALM_CONFIG_KEYSPACE,
        realm().as_bytes().to_vec().into(),
        config.to_bytes(&actor).unwrap().into(),
    )
    .await;
    (dir, state, user)
}

async fn submit_state() -> (TempDir, Arc<ServerState>, UserId, Ulid) {
    let (dir, state, user) = plain_state(test_limits()).await;
    let group = Ulid::generate();
    grant(&state, user, group).await;
    state
        .register_rest_public(
            "127.0.0.1:3000".parse().unwrap(),
            Some("https://owner.example/"),
        )
        .await;
    (dir, state, user, group)
}

async fn blob_state(limits: RoCrateLimits) -> (TempDir, TempDir, Arc<ServerState>, UserId) {
    let storage_dir = tempfile::tempdir().unwrap();
    let blob_dir = tempfile::tempdir().unwrap();
    let blob_root = blob_dir.path().join("blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();
    let storage = FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            realm_id: realm(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .unwrap();
    let blob = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_root.to_str().unwrap().to_string(),
            service_config: HashMap::new(),
            bucket_prefix: Some("aruna-upload-".to_string()),
            max_bucket_size: Some(1_000_000),
            multipart_bucket: Some("multipart".to_string()),
            timeouts: Default::default(),
        },
        storage.clone(),
        net.clone(),
    )
    .await
    .unwrap();
    let node_id = net.node_id();
    let user = UserId::local(Ulid::generate(), realm());
    let ctx = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net),
        blob_handle: Some(blob),
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    let state = Arc::new(
        ServerState::new(
            ctx,
            realm(),
            node_id,
            NodeCapabilities::user_node(realm()).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await
        .with_rocrate_limits(limits),
    );
    state
        .register_rest_public(
            "127.0.0.1:3000".parse().unwrap(),
            Some("https://owner.example/"),
        )
        .await;
    (storage_dir, blob_dir, state, user)
}

#[tokio::test]
async fn upload_streams_hash() {
    let (_storage, _blob, state, user) = blob_state(test_limits()).await;
    let body: &[u8] = b"hello ro-crate payload";
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
    let (status, Json(response)) = upload_rocrate(
        State(state),
        Extension(auth(user)),
        headers,
        Body::from(body),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(response.size, body.len() as u64);
    assert_eq!(response.blake3, hex::encode(blake3::hash(body).as_bytes()));
    assert_eq!(response.owner_node_url, "https://owner.example/api/v1");
    assert!(!response.expires_at.is_empty());
}

#[tokio::test]
async fn upload_accepts_eln() {
    let (_storage, _blob, state, user) = blob_state(test_limits()).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.eln+zip; version=1"),
    );
    let (status, _) = upload_rocrate(
        State(state),
        Extension(auth(user)),
        headers,
        Body::from(&b"eln bytes"[..]),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test]
async fn upload_rejects_media() {
    let (_dir, state, user) = plain_state(test_limits()).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    let result = upload_rocrate(State(state), Extension(auth(user)), headers, Body::empty()).await;
    assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn upload_cap_header() {
    let (_dir, state, user) = plain_state(test_limits()).await;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("2048"));
    let result = upload_rocrate(State(state), Extension(auth(user)), headers, Body::empty()).await;
    assert!(matches!(result, Err(ServerError::PayloadTooLarge(_))));
}

#[tokio::test]
async fn upload_streams_capped() {
    // No Content-Length header: the cap must trip mid-stream, not from the header.
    let (_storage, _blob, state, user) = blob_state(RoCrateLimits {
        direct_upload_bytes: 4,
        ..RoCrateLimits::default()
    })
    .await;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
    let result = upload_rocrate(
        State(state),
        Extension(auth(user)),
        headers,
        Body::from(&b"way past four bytes"[..]),
    )
    .await;
    assert!(matches!(result, Err(ServerError::PayloadTooLarge(_))));
}

#[tokio::test(start_paused = true)]
async fn upload_idle_timeout() {
    let body = Body::from_stream(stream::pending::<Result<Bytes, std::io::Error>>());
    let mut upload = upload_body_stream(body, Instant::now() + UPLOAD_DEADLINE);
    let task = tokio::spawn(async move { upload.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(UPLOAD_IDLE_TIMEOUT).await;
    let item = task.await.unwrap().unwrap().unwrap_err();
    assert!(item.to_string().contains("timed out"));
}

#[tokio::test]
async fn upload_slots_bounded() {
    let (_dir, state, user) = plain_state(test_limits()).await;
    let permits: Vec<_> = (0..ROCRATE_UPLOAD_SLOTS)
        .map(|_| state.try_rocrate_slot().expect("upload slot"))
        .collect();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
    let result = upload_rocrate(
        State(state.clone()),
        Extension(auth(user)),
        headers,
        Body::empty(),
    )
    .await;
    assert!(matches!(
        result,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    drop(permits);
    assert!(state.try_rocrate_slot().is_some());
}

#[tokio::test]
async fn upload_requires_auth() {
    let (_dir, state, user) = plain_state(test_limits()).await;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
    let anonymous = upload_rocrate(
        State(state.clone()),
        Extension(None),
        headers.clone(),
        Body::empty(),
    )
    .await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
    let delegated = upload_rocrate(
        State(state),
        Extension(restricted(user)),
        headers,
        Body::empty(),
    )
    .await;
    assert!(matches!(delegated, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn submit_accepts_upload() {
    let (_dir, state, user, group) = submit_state().await;
    let upload_id = Ulid::generate();
    let future = aruna_core::time::unix_timestamp_millis() + 60_000;
    seed_upload(&state, upload_id, user, 1, future).await;
    seed_bucket(&state, "target", group, user).await;
    let (status, Json(response)) = submit_import(
        State(state),
        Extension(auth(user)),
        Json(submit_request(upload_id, group, "crate", None)),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(response.created);
    assert!(!response.job_id.is_empty());
    assert_eq!(response.owner_node_url, "https://owner.example/api/v1");
    assert!(
        response
            .status_url
            .ends_with(&format!("/compute/jobs/{}", response.job_id))
    );
    assert!(response.report_url.ends_with("/report"));
}

#[tokio::test]
async fn submit_missing_upload() {
    let (_dir, state, user, group) = submit_state().await;
    let result = submit_import(
        State(state),
        Extension(auth(user)),
        Json(submit_request(Ulid::generate(), group, "crate", None)),
    )
    .await;
    assert!(matches!(result, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn submit_foreign_upload() {
    let (_dir, state, user, group) = submit_state().await;
    let upload_id = Ulid::generate();
    let other = UserId::local(Ulid::generate(), realm());
    seed_upload(
        &state,
        upload_id,
        other,
        1,
        aruna_core::time::unix_timestamp_millis() + 60_000,
    )
    .await;
    let result = submit_import(
        State(state),
        Extension(auth(user)),
        Json(submit_request(upload_id, group, "crate", None)),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn submit_expired_upload() {
    // An unclaimed upload past its retention is rejected at submit time.
    let (_dir, state, user, group) = submit_state().await;
    let upload_id = Ulid::generate();
    seed_upload(&state, upload_id, user, 1, 1).await;
    seed_bucket(&state, "target", group, user).await;
    let result = submit_import(
        State(state),
        Extension(auth(user)),
        Json(submit_request(upload_id, group, "crate", None)),
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn submit_oversized_upload() {
    // An upload larger than the import-source cap is rejected at submit time.
    let (_dir, state, user, group) = submit_state().await;
    let upload_id = Ulid::generate();
    let future = aruna_core::time::unix_timestamp_millis() + 60_000;
    seed_upload(&state, upload_id, user, 2048, future).await;
    seed_bucket(&state, "target", group, user).await;
    let result = submit_import(
        State(state),
        Extension(auth(user)),
        Json(submit_request(upload_id, group, "crate", None)),
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn submit_target_forbidden() {
    // Source passes, but the target bucket belongs to a group the caller cannot write.
    let (_dir, state, user, group) = submit_state().await;
    let upload_id = Ulid::generate();
    seed_upload(
        &state,
        upload_id,
        user,
        1,
        aruna_core::time::unix_timestamp_millis() + 60_000,
    )
    .await;
    let foreign = Ulid::generate();
    grant(&state, UserId::local(Ulid::generate(), realm()), foreign).await;
    seed_bucket(&state, "target", foreign, user).await;
    let result = submit_import(
        State(state),
        Extension(auth(user)),
        Json(submit_request(upload_id, group, "crate", None)),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn submit_dedups_key() {
    let (_dir, state, user, group) = submit_state().await;
    let upload_id = Ulid::generate();
    seed_upload(
        &state,
        upload_id,
        user,
        1,
        aruna_core::time::unix_timestamp_millis() + 60_000,
    )
    .await;
    seed_bucket(&state, "target", group, user).await;
    let (_, Json(first)) = submit_import(
        State(state.clone()),
        Extension(auth(user)),
        Json(submit_request(upload_id, group, "crate", Some("key"))),
    )
    .await
    .unwrap();
    assert!(first.created);
    let (_, Json(replay)) = submit_import(
        State(state.clone()),
        Extension(auth(user)),
        Json(submit_request(upload_id, group, "crate", Some("key"))),
    )
    .await
    .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.job_id, first.job_id);
    let conflict = submit_import(
        State(state),
        Extension(auth(user)),
        Json(submit_request(upload_id, group, "other", Some("key"))),
    )
    .await;
    assert!(matches!(conflict, Err(ServerError::JobPlanConflict(_))));
}

#[tokio::test]
async fn submit_requires_auth() {
    let (_dir, state, user, group) = submit_state().await;
    let request = submit_request(Ulid::generate(), group, "crate", None);
    let anonymous =
        submit_import(State(state.clone()), Extension(None), Json(request.clone())).await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
    let delegated = submit_import(State(state), Extension(restricted(user)), Json(request)).await;
    assert!(matches!(delegated, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn submit_object_source() {
    // Object source with a nonexistent bucket fails the fast source check.
    let (_dir, state, user, group) = submit_state().await;
    let request = SubmitImportRequest {
        source: ImportSourceRequest::Object {
            bucket: "missing".to_string(),
            key: "crate.zip".to_string(),
            version: None,
        },
        target: ImportTargetRequest {
            bucket: "target".to_string(),
            prefix: "crate".to_string(),
        },
        metadata: ImportMetadataRequest {
            group_id: group.to_string(),
            path: "crate".to_string(),
            public: false,
        },
        idempotency_key: None,
    };
    let result = submit_import(State(state), Extension(auth(user)), Json(request)).await;
    assert!(matches!(result, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn submit_connector_source() {
    // Connector source requires READ; a non-member group is forbidden before staging.
    let (_dir, state, user, group) = submit_state().await;
    let foreign = Ulid::generate();
    grant(&state, UserId::local(Ulid::generate(), realm()), foreign).await;
    let request = SubmitImportRequest {
        source: ImportSourceRequest::Connector {
            group_id: foreign.to_string(),
            connector_id: Ulid::generate().to_string(),
            path: "folder/crate.zip".to_string(),
        },
        target: ImportTargetRequest {
            bucket: "target".to_string(),
            prefix: "crate".to_string(),
        },
        metadata: ImportMetadataRequest {
            group_id: group.to_string(),
            path: "crate".to_string(),
            public: false,
        },
        idempotency_key: None,
    };
    let result = submit_import(State(state), Extension(auth(user)), Json(request)).await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[test]
fn parses_import_source() {
    let object = parse_import_source(ImportSourceRequest::Object {
        bucket: "b".to_string(),
        key: "k".to_string(),
        version: None,
    })
    .unwrap();
    assert!(matches!(object, ImportRoCrateSource::Object { .. }));
    assert!(
        parse_import_source(ImportSourceRequest::Object {
            bucket: String::new(),
            key: "k".to_string(),
            version: None,
        })
        .is_err()
    );
    let connector = parse_import_source(ImportSourceRequest::Connector {
        group_id: Ulid::generate().to_string(),
        connector_id: Ulid::generate().to_string(),
        path: "a/b".to_string(),
    })
    .unwrap();
    assert!(matches!(connector, ImportRoCrateSource::Connector { .. }));
    assert!(
        parse_import_source(ImportSourceRequest::Connector {
            group_id: "bad".to_string(),
            connector_id: Ulid::generate().to_string(),
            path: "a/b".to_string(),
        })
        .is_err()
    );
    assert!(
        parse_import_source(ImportSourceRequest::Upload {
            upload_id: "bad".to_string(),
        })
        .is_err()
    );
}

#[test]
fn parses_import_target() {
    let target = parse_import_target(
        ImportTargetRequest {
            bucket: "b".to_string(),
            prefix: "/crate/".to_string(),
        },
        1024,
    )
    .unwrap();
    assert_eq!(target.prefix, "crate");
    assert!(
        parse_import_target(
            ImportTargetRequest {
                bucket: String::new(),
                prefix: "crate".to_string(),
            },
            1024,
        )
        .is_err()
    );
    assert!(
        parse_import_target(
            ImportTargetRequest {
                bucket: "b".to_string(),
                prefix: "a/../b".to_string(),
            },
            1024,
        )
        .is_err()
    );
    assert!(
        parse_import_target(
            ImportTargetRequest {
                bucket: "b".to_string(),
                prefix: "x".to_string(),
            },
            0,
        )
        .is_err()
    );
}

#[test]
fn parses_import_metadata() {
    let metadata = parse_import_metadata(
        ImportMetadataRequest {
            group_id: Ulid::generate().to_string(),
            path: "/datasets/crate/".to_string(),
            public: true,
        },
        1024,
    )
    .unwrap();
    assert_eq!(metadata.path, "datasets/crate");
    assert!(metadata.public);
    assert!(
        parse_import_metadata(
            ImportMetadataRequest {
                group_id: Ulid::generate().to_string(),
                path: "/".to_string(),
                public: false,
            },
            1024,
        )
        .is_err()
    );
    assert!(
        parse_import_metadata(
            ImportMetadataRequest {
                group_id: "bad".to_string(),
                path: "crate".to_string(),
                public: false,
            },
            1024,
        )
        .is_err()
    );
}

#[tokio::test]
async fn submit_rejects_body() {
    // Malformed JSON is rejected at the route with 400, never reaching the handler.
    let (_dir, state, user, _group) = submit_state().await;
    let request = Request::builder()
        .method("POST")
        .uri("/metadata/rocrate/imports")
        .header(CONTENT_TYPE, "application/json")
        .extension(auth(user))
        .body(Body::from("{ not json"))
        .unwrap();
    let (app, _) = router().split_for_parts();
    let response = app.with_state(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn upload_accepts_types() {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
    assert!(matches!(
        parse_media_type(&headers),
        Ok(RoCrateMediaType::Zip)
    ));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.eln+zip; version=1"),
    );
    assert!(matches!(
        parse_media_type(&headers),
        Ok(RoCrateMediaType::Eln)
    ));
}

#[test]
fn upload_openapi_types() {
    let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();
    let content = &openapi["paths"]["/metadata/rocrate/uploads"]["post"]["requestBody"]["content"];
    assert!(content.get(ZIP_MEDIA_TYPE).is_some());
    assert!(content.get(ELN_MEDIA_TYPE).is_some());
}

#[test]
fn upload_rejects_type() {
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    assert!(parse_media_type(&headers).is_err());
}

#[test]
fn source_path_checks() {
    assert!(validate_source_path("folder/file.zip").is_ok());
    assert!(validate_source_path("../file.zip").is_err());
    assert!(validate_source_path("/file.zip").is_err());
}
