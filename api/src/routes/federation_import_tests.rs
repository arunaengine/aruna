//! Tests of the destination realm's import intent, pushed upload and import routes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::rocrate_import::{UploadRoCrateResponse, upload_rocrate};
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::federation::{AcceptedRealms, FederationSettings, RegistrationMode};
use aruna_core::handoff::secret_nonce;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BackendLocation, BackendRef, BucketInfo,
};
use aruna_core::structs::storage::format::StoredFormat;
use aruna_core::transfer::intent_digest;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::federation::import::{bound_upload, read_import};
use aruna_operations::jobs::import::{load_rocrate_upload, write_rocrate_upload};
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use axum::body::Body;
use axum::http::HeaderValue;
use axum::http::header::CONTENT_TYPE;
use ed25519_dalek::SigningKey;
use std::collections::HashMap;
use std::time::SystemTime;
use tempfile::TempDir;
use url::Url;

const SECRET: [u8; SECRET_LEN] = [4; SECRET_LEN];
const BODY: &[u8] = b"pushed export artifact";

fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn realm(seed: u8) -> RealmId {
    RealmId::from_bytes(key(seed).verifying_key().to_bytes())
}

struct Fixture {
    _dirs: Vec<TempDir>,
    state: Arc<ServerState>,
    user: UserId,
    group: Ulid,
    descriptor: Signed<RealmDescriptor>,
}

async fn write(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let effect = StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    state
        .get_ctx()
        .storage_handle
        .send_storage_effect(effect)
        .await;
}

/// Destination realm 21 with federation settings, a group the user owns and its bucket `lab`.
async fn fixture(with_blob: bool) -> Fixture {
    let (storage_dir, blob_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let storage = FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let realm_id = realm(21);
    let (mut node_id, mut net_handle, mut blob_handle) =
        (iroh::SecretKey::from_bytes(&[7; 32]).public(), None, None);
    if with_blob {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        };
        let net = NetHandle::new(config, storage.clone()).await.unwrap();
        let backend = BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_dir.path().to_str().unwrap().to_string(),
            service_config: HashMap::new(),
            bucket_prefix: Some("aruna-upload-".to_string()),
            max_bucket_size: Some(1_000_000),
            multipart_bucket: Some("multipart".to_string()),
            timeouts: Default::default(),
        };
        let blob = BlobHandler::new(backend, storage.clone(), net.clone());
        blob_handle = Some(blob.await.unwrap());
        node_id = net.node_id();
        net_handle = Some(net);
    }
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle,
        blob_handle,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    let capabilities = NodeCapabilities::management_node(key(21)).unwrap();
    let limits = RoCrateLimits {
        direct_upload_bytes: 1024,
        import_source_bytes: 1024,
        ..RoCrateLimits::default()
    };
    let state = ServerState::new(
        context,
        realm_id,
        node_id,
        capabilities,
        false,
        None,
        JobsRuntime::new(),
    );
    let state = Arc::new(state.await.with_rocrate_limits(limits));
    let public = Some("https://b.example.org/");
    state
        .register_rest_public("127.0.0.1:3000".parse().unwrap(), public)
        .await;
    let user = UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id: user,
        realm_id,
    };
    let url = |value: &str| Url::parse(value).unwrap();
    let descriptor = RealmDescriptor {
        realm_id,
        name: "B".to_string(),
        description: String::new(),
        api_url: url("https://b.example.org/api/v1"),
        portal_url: url("https://b.example.org"),
        issued_at: 1,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    config.ensure_node(node_id, RealmNodeKind::Server);
    config.seed_job_control(node_id, 0);
    let signed = Signed::sign(descriptor.clone(), state.node_capabilities()).unwrap();
    config.federation = Some(FederationSettings {
        name: "B".to_string(),
        api_url: descriptor.api_url,
        portal_url: descriptor.portal_url,
        registry_url: None,
        registration: RegistrationMode::Enabled,
        accepted_realms: AcceptedRealms::None,
        descriptor: signed.clone(),
    });
    let target = DocumentTarget::RealmConfig { realm_id };
    let config = config.to_bytes(&actor).unwrap();
    write(
        &state,
        target.storage_keyspace(),
        target.storage_key().to_vec(),
        config,
    )
    .await;
    let group = Ulid::generate();
    let group_auth = GroupAuthorizationDocument::default_group_doc(user, realm_id, group);
    let group_doc = Group {
        display_name: "lab".to_string(),
        group_id: group,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner: user,
    };
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let realm_key = realm_id.as_bytes().to_vec();
    write(
        &state,
        AUTH_KEYSPACE,
        realm_key,
        realm_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    let group_key = group.to_bytes().to_vec();
    let auth_doc = group_auth.to_bytes(&actor).unwrap();
    write(&state, AUTH_KEYSPACE, group_key.clone(), auth_doc).await;
    write(
        &state,
        GROUP_KEYSPACE,
        group_key,
        group_doc.to_bytes(&actor).unwrap(),
    )
    .await;
    let info = BucketInfo {
        group_id: group,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
    };
    write(
        &state,
        S3_BUCKET_KEYSPACE,
        b"lab".to_vec(),
        info.to_bytes().unwrap(),
    )
    .await;
    Fixture {
        _dirs: vec![storage_dir, blob_dir],
        state,
        user,
        group,
        descriptor: signed,
    }
}

fn intent(fixture: &Fixture, principal: UserId) -> Signed<ImportIntent> {
    let state = &fixture.state;
    let descriptor = &fixture.descriptor;
    let now = unix_timestamp_secs();
    let intent = ImportIntent {
        realm_id: state.get_realm_id(),
        descriptor_digest: descriptor_digest(descriptor).unwrap(),
        principal,
        destination: ImportDestination {
            group_id: fixture.group,
            bucket: "lab".to_string(),
            prefix: "imports".to_string(),
            metadata_path: "datasets/run".to_string(),
        },
        max_bytes: 1024,
        nonce: secret_nonce(&SECRET),
        issued_at: now,
        expires_at: now + MAX_TRANSFER_SECS,
        intent_id: Ulid::generate(),
    };
    Signed::sign(intent, state.node_capabilities()).unwrap()
}

/// Source realm 22's grant for `intent` over `body`.
fn grant(intent: &Signed<ImportIntent>, body: &[u8]) -> Signed<ExportGrant> {
    let now = unix_timestamp_secs();
    let grant = ExportGrant {
        source: realm(22),
        audience: intent.payload.realm_id,
        intent_digest: intent_digest(intent).unwrap(),
        export_job_id: Ulid::from_bytes([5; 16]),
        document_id: Ulid::from_bytes([6; 16]),
        source_revision: Ulid::from_bytes([7; 16]),
        dataset_digest: "aa".repeat(32),
        selection_digest: "bb".repeat(32),
        artifact_url: Url::parse("https://a.example.org/artifact").unwrap(),
        artifact_blake3: hex::encode(blake3::hash(body).as_bytes()),
        artifact_size: body.len() as u64,
        issued_at: now,
        expires_at: now + MAX_TRANSFER_SECS,
    };
    let source = NodeCapabilities::management_node(key(22)).unwrap();
    Signed::sign(grant, &source).unwrap()
}

async fn push(
    fixture: &Fixture,
    intent: &Signed<ImportIntent>,
    grant: &Signed<ExportGrant>,
) -> ServerResult<UploadRoCrateResponse> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/zip"));
    let value = |text: String| HeaderValue::from_str(&text).unwrap();
    headers.insert(INTENT_HEADER, value(encode_header(intent).unwrap()));
    headers.insert(GRANT_HEADER, value(encode_header(grant).unwrap()));
    let state = State(fixture.state.clone());
    let (_, Json(response)) =
        upload_rocrate(state, Extension(None), headers, Body::from(BODY)).await?;
    Ok::<UploadRoCrateResponse, ServerError>(response)
}

#[tokio::test]
async fn push_binds_principal() {
    // The upload belongs to the intent's principal; a repeated push returns the same upload.
    let fixture = fixture(true).await;
    let intent = intent(&fixture, fixture.user);
    let grant = grant(&intent, BODY);
    let first = push(&fixture, &intent, &grant).await.unwrap();
    let upload_id = Ulid::from_string(&first.upload_id).unwrap();
    let context = fixture.state.get_ctx();
    let record = load_rocrate_upload(&context, upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.owner, fixture.user);
    let key = import_key(&grant.payload, &intent.payload.destination).unwrap();
    let bound = bound_upload(&context, fixture.user, &key).await;
    assert_eq!(bound, Ok(Some(upload_id)));
    let again = push(&fixture, &intent, &grant).await.unwrap();
    assert_eq!(again.upload_id, first.upload_id);
    // A push with fresh consent to the same transfer rebinds the upload, so a paused job resumes.
    let fresh = self::intent(&fixture, fixture.user);
    let renewed = self::grant(&fresh, BODY);
    let resumed = push(&fixture, &fresh, &renewed).await.unwrap();
    assert_eq!(resumed.upload_id, first.upload_id);
    let stored = read_import(&context, upload_id).await.unwrap().unwrap();
    assert_eq!(stored.intent, fresh);
    // Another artifact under the same import key is never handed the bound upload.
    let other = self::grant(&intent, b"another export body");
    let error = push(&fixture, &intent, &other).await.unwrap_err();
    assert!(matches!(
        error,
        ServerError::Refused(_, "import_conflict", _)
    ));
}

#[tokio::test]
async fn push_checks_artifact() {
    // Another artifact than the grant names is neither kept bound nor importable.
    let fixture = fixture(true).await;
    let intent = intent(&fixture, fixture.user);
    let mismatch = grant(&intent, b"other export artifact!");
    let error = push(&fixture, &intent, &mismatch).await.unwrap_err();
    assert!(matches!(
        error,
        ServerError::Refused(_, "artifact_mismatch", _)
    ));
    let key = import_key(&mismatch.payload, &intent.payload.destination).unwrap();
    let bound = bound_upload(&fixture.state.get_ctx(), fixture.user, &key).await;
    assert_eq!(bound, Ok(None));
    // A body above the granted size is cut off as too large.
    let small = grant(&intent, &BODY[..4]);
    let error = push(&fixture, &intent, &small).await.unwrap_err();
    assert!(matches!(error, ServerError::PayloadTooLarge(_)));
}

#[tokio::test]
async fn push_needs_binding() {
    let fixture = fixture(true).await;
    let intent_a = intent(&fixture, fixture.user);
    let intent_b = intent(&fixture, fixture.user);
    // A grant for another intent is refused before any byte is stored.
    let error = push(&fixture, &intent_a, &grant(&intent_b, BODY))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ServerError::Refused(_, "transfer_rejected", _)
    ));
    // A principal without WRITE on the destination is refused.
    let stranger = UserId::local(Ulid::generate(), fixture.state.get_realm_id());
    let intent = intent(&fixture, stranger);
    let error = push(&fixture, &intent, &grant(&intent, BODY))
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::Refused(_, "import_denied", _)));
}

async fn seed_upload(fixture: &Fixture, upload_id: Ulid) {
    let record = RoCrateUploadRecord {
        upload_id,
        owner: fixture.user,
        location: BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "storage".to_string(),
            backend_path: format!("_jobs/{upload_id}/input"),
            ulid: upload_id,
            format: StoredFormat::default(),
            created_by: fixture.user,
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: HashMap::new(),
        },
        blake3: *blake3::hash(BODY).as_bytes(),
        size: BODY.len() as u64,
        media_type: RoCrateMediaType::Zip,
        expires_at_ms: unix_timestamp_millis() + 60_000,
        claimed_by: None,
    };
    let storage = &fixture.state.get_ctx().storage_handle;
    write_rocrate_upload(storage, &record).await.unwrap();
}

async fn import(
    fixture: &Fixture,
    user: UserId,
    intent: &Signed<ImportIntent>,
    grant: &Signed<ExportGrant>,
    secret: [u8; SECRET_LEN],
) -> ServerResult<SubmitImportResponse> {
    let auth = AuthContext {
        user_id: user,
        realm_id: fixture.state.get_realm_id(),
        path_restrictions: None,
        session: None,
    };
    let request = FederatedImportRequest {
        intent: intent.clone(),
        grant: grant.clone(),
        secret: hex::encode(secret),
    };
    let state = State(fixture.state.clone());
    let (_, Json(response)) = create_import(state, Extension(Some(auth)), Json(request)).await?;
    Ok(response)
}

#[tokio::test]
async fn bound_upload_confirmed() {
    // A bound upload is used only after the source confirms its grant; a later upload never
    // replaces the binding.
    let fixture = fixture(false).await;
    let intent = intent(&fixture, fixture.user);
    let grant = grant(&intent, BODY);
    let key = import_key(&grant.payload, &intent.payload.destination).unwrap();
    let binding = ImportRecord {
        intent: intent.clone(),
        grant: grant.clone(),
    };
    let (first, second) = (Ulid::generate(), Ulid::generate());
    seed_upload(&fixture, first).await;
    let context = fixture.state.get_ctx();
    write_import(&context, &key, first, &binding).await.unwrap();
    let user = fixture.user;
    let error = import(&fixture, user, &intent, &grant, SECRET).await;
    assert!(matches!(error, Err(ServerError::ServiceUnavailable)));
    seed_upload(&fixture, second).await;
    let bound = write_import(&context, &key, second, &binding).await;
    assert_eq!(bound, Ok(first));
}

#[tokio::test]
async fn import_needs_principal() {
    let fixture = fixture(false).await;
    let intent = intent(&fixture, fixture.user);
    let grant = grant(&intent, BODY);
    let other = UserId::local(Ulid::generate(), fixture.state.get_realm_id());
    let error = import(&fixture, other, &intent, &grant, SECRET)
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));
    let wrong = import(&fixture, fixture.user, &intent, &grant, [9; SECRET_LEN]).await;
    assert!(matches!(
        wrong,
        Err(ServerError::Refused(_, "transfer_rejected", _))
    ));
}

#[tokio::test]
async fn losing_spool_discarded() {
    // A transfer that loses the binding race gets the winner and its own spool expires.
    let fixture = fixture(false).await;
    let intent = intent(&fixture, fixture.user);
    let grant = grant(&intent, BODY);
    let key = import_key(&grant.payload, &intent.payload.destination).unwrap();
    let (first, second) = (Ulid::generate(), Ulid::generate());
    seed_upload(&fixture, first).await;
    seed_upload(&fixture, second).await;
    let context = fixture.state.get_ctx();
    let binding = ImportRecord {
        intent: intent.clone(),
        grant: grant.clone(),
    };
    write_import(&context, &key, first, &binding).await.unwrap();
    let losing = load_rocrate_upload(&context, second)
        .await
        .unwrap()
        .unwrap();
    let bound = bind_upload(&fixture.state, &intent, &grant, &key, &losing)
        .await
        .unwrap();
    assert_eq!(bound.upload_id, first);
    let discarded = load_rocrate_upload(&context, second)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(discarded.expires_at_ms, 0);
}

#[tokio::test]
async fn retry_keeps_plan() {
    // A federation-bound import retried from another session is the same job, not a conflict.
    let fixture = fixture(false).await;
    let intent = intent(&fixture, fixture.user);
    let grant = grant(&intent, BODY);
    let key = import_key(&grant.payload, &intent.payload.destination).unwrap();
    let binding = ImportRecord {
        intent: intent.clone(),
        grant: grant.clone(),
    };
    let upload_id = Ulid::generate();
    seed_upload(&fixture, upload_id).await;
    let context = fixture.state.get_ctx();
    write_import(&context, &key, upload_id, &binding)
        .await
        .unwrap();
    let submit = |sid: &str| {
        let auth = AuthContext {
            user_id: fixture.user,
            realm_id: fixture.state.get_realm_id(),
            path_restrictions: None,
            session: Some(aruna_core::structs::identity::auth::SessionRef {
                sid: sid.to_string(),
                kind: aruna_core::structs::identity::auth::SessionKind::Portal,
                name: None,
            }),
        };
        let request = SubmitImportRequest {
            source: ImportSourceRequest::Upload {
                upload_id: upload_id.to_string(),
            },
            target: ImportTargetRequest {
                bucket: "lab".to_string(),
                prefix: "imports".to_string(),
            },
            metadata: ImportMetadataRequest {
                group_id: fixture.group.to_string(),
                path: "datasets/run".to_string(),
                public: false,
            },
            idempotency_key: Some(key.clone()),
        };
        submit_import(
            State(fixture.state.clone()),
            Extension(Some(auth)),
            Json(request),
        )
    };
    let (_, Json(first)) = submit("first").await.unwrap();
    let (_, Json(second)) = submit("second").await.unwrap();
    assert!(first.created && !second.created);
    assert_eq!(first.job_id, second.job_id);
}
