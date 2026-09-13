use super::{GetTokenResponse, RegisterUserRequest, RegisterUserResponse, enrollment_status};
use crate::auth::{OidcValidator, handle_token};
use crate::routes::sessions::{CreateSessionRequest, CreateSessionResponse};
use crate::server::Server;
use crate::server::ServerConfig;
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, USER_KEYSPACE};
use aruna_core::onboarding::{
    OnboardingMode, OnboardingPurpose, OnboardingSecret, OnboardingSecretRecord,
};
use aruna_core::structs::{
    Actor, NodeCapabilities, OidcProviderConfig, PathRestriction, Permission, RealmConfigDocument,
    RealmId, SessionKind, TokenClaims, User, oidc_subject_key,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::auth::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::onboarding::create_secret::{
    CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
};
use aruna_operations::realm::announce_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::realm::claim_admin::{
    ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::sync::incoming::initialize_net_incoming;
use aruna_operations::tasks::incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use base64::Engine;
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use ulid::Ulid;

#[derive(Clone)]
struct OidcProviderState {
    issuer: String,
    jwks_uri: String,
    jwks: serde_json::Value,
}

#[derive(Clone, Serialize, Deserialize)]
struct TestOidcClaims {
    sub: String,
    iss: String,
    aud: String,
    exp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

struct TestNode {
    _temp_dir: TempDir,
    context: Arc<DriverContext>,
    state: Arc<ServerState>,
    base_url: String,
    realm_id: RealmId,
    realm_admin_id: UserId,
    net: NetHandle,
    server_task: JoinHandle<()>,
}

async fn oidc_discovery(State(state): State<OidcProviderState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "issuer": state.issuer,
        "jwks_uri": state.jwks_uri,
    }))
}

async fn oidc_jwks(State(state): State<OidcProviderState>) -> Json<serde_json::Value> {
    Json(state.jwks)
}

async fn spawn_oidc_provider(
    issuer: &str,
    kid: &str,
    signing_key: &SigningKey,
) -> (OidcProviderConfig, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let jwks_uri = format!("http://{addr}/jwks.json");
    let discovery_url = format!("http://{addr}/.well-known/openid-configuration");
    let jwks = serde_json::json!({
        "keys": [{
            "kty": "OKP",
            "alg": "EdDSA",
            "use": "sig",
            "kid": kid,
            "crv": "Ed25519",
            "x": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes()),
        }]
    });
    let router = Router::new()
        .route("/.well-known/openid-configuration", get(oidc_discovery))
        .route("/jwks.json", get(oidc_jwks))
        .with_state(OidcProviderState {
            issuer: issuer.to_string(),
            jwks_uri: jwks_uri.clone(),
            jwks,
        });
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    (
        OidcProviderConfig {
            id: "main".to_string(),
            issuer: issuer.to_string(),
            audience: "aruna-api".to_string(),
            discovery_url,
        },
        task,
    )
}

// An unclaimed enrollment past its expiry is dead: reporting it as pending
// would show an enrollment in flight that can never complete.
#[test]
fn reports_expired_enrollment() {
    assert_eq!(enrollment_status(false, 100, 99), "pending");
    assert_eq!(enrollment_status(false, 100, 100), "expired");
    assert_eq!(enrollment_status(false, 100, 101), "expired");
    assert_eq!(enrollment_status(true, 100, 101), "claimed");
}

fn sign_oidc_token(
    issuer: &str,
    kid: &str,
    signing_key: &SigningKey,
    subject: &str,
    name: Option<&str>,
) -> String {
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(kid.to_string());
    let claims = TestOidcClaims {
        sub: subject.to_string(),
        iss: issuer.to_string(),
        aud: "aruna-api".to_string(),
        exp: chrono::Utc::now().timestamp().max(0) as u64 + 600,
        name: name.map(str::to_string),
    };
    let key_pem = signing_key
        .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
        .unwrap();
    encode(
        &header,
        &claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
    )
    .unwrap()
}

fn sign_scoped_token(node: &TestNode, user_id: UserId) -> String {
    sign_aruna_token(
        node,
        user_id,
        Some(vec![PathRestriction {
            pattern: format!("/{}/admin/u/**", node.realm_id),
            permission: Permission::READ,
        }]),
    )
}

fn sign_aruna_token(
    node: &TestNode,
    user_id: UserId,
    restrictions: Option<Vec<PathRestriction>>,
) -> String {
    let now = super::now_timestamp();
    let claims = TokenClaims {
        sub: user_id.to_string(),
        iss: node.realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: Ulid::generate().to_string(),
        sid: None,
        session_kind: None,
        restrictions,
        issuer_pubkey: None,
        delegation_signature: None,
    };
    let NodeCapabilities::Management {
        realm_encoding_key, ..
    } = node.state.node_capabilities()
    else {
        panic!("test node must use management capabilities");
    };
    encode(
        &Header::new(Algorithm::EdDSA),
        &claims,
        &EncodingKey::from_ed_pem(realm_encoding_key).unwrap(),
    )
    .unwrap()
}

async fn read_realm_config(driver_ctx: &DriverContext, realm_id: &RealmId) -> RealmConfigDocument {
    match driver_ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(realm_id.as_bytes().to_vec()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => RealmConfigDocument::from_bytes(&bytes).unwrap(),
        other => panic!("unexpected realm config read result: {other:?}"),
    }
}

async fn spawn_test_node(provider: OidcProviderConfig, claim_initial_admin: bool) -> TestNode {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_handle = FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage_handle.clone(),
    )
    .await
    .unwrap();
    let task_handle = TaskHandle::new();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    initialize_net_incoming(driver_ctx.clone());
    initialize_task_incoming(
        driver_ctx.clone(),
        task_handle,
        aruna_operations::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    let realm_signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let node_id = net_handle.node_id();
    let realm_admin_id = UserId::local(Ulid::generate(), realm_id);

    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id: realm_admin_id,
                realm_id,
            },
            realm_description: "Realm".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
        driver_ctx.as_ref(),
    )
    .await
    .unwrap();

    if claim_initial_admin {
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: Actor {
                    node_id,
                    user_id: realm_admin_id,
                    realm_id,
                },
            }),
            driver_ctx.as_ref(),
        )
        .await
        .unwrap();
    }

    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id,
            node_id,
            schedule_refresh: false,
        }),
        driver_ctx.as_ref(),
    )
    .await
    .unwrap();

    let mut config = read_realm_config(driver_ctx.as_ref(), &realm_id).await;
    config.oidc_providers.push(provider);
    match driver_ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(realm_id.as_bytes().to_vec()),
            value: ByteView::from(
                config
                    .to_bytes(&Actor {
                        node_id,
                        user_id: UserId::nil(realm_id),
                        realm_id,
                    })
                    .unwrap(),
            ),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected realm config write result: {other:?}"),
    }

    let state = Arc::new(
        ServerState::new(
            driver_ctx.clone(),
            realm_id,
            node_id,
            NodeCapabilities::management_node(realm_signing_key).unwrap(),
            false,
            Some(Arc::new(OidcValidator::new().unwrap())),
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Server::new(
        state.clone(),
        ServerConfig {
            http_addr: addr,
            max_http_body_size: crate::server::DEFAULT_MAX_HTTP_BODY_SIZE,
            cors: crate::cors::CorsConfig::default(),
        },
    )
    .build_router();
    let server_task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });

    TestNode {
        _temp_dir: temp_dir,
        context: driver_ctx,
        state,
        base_url: format!("http://{addr}"),
        realm_id,
        realm_admin_id,
        net: net_handle,
        server_task,
    }
}

async fn register_via_oidc(
    node: &TestNode,
    issuer: &str,
    kid: &str,
    signing_key: &SigningKey,
    subject: &str,
    name: &str,
    onboarding_secret: Option<String>,
) -> (RegisterUserResponse, String) {
    let oidc_token = sign_oidc_token(issuer, kid, signing_key, subject, Some(name));
    let register = reqwest::Client::new()
        .post(format!("{}/api/v1/access/users/register", node.base_url))
        .bearer_auth(&oidc_token)
        .json(&RegisterUserRequest { onboarding_secret })
        .send()
        .await
        .unwrap();
    assert_eq!(register.status(), StatusCode::CREATED);
    let registered: RegisterUserResponse = register.json().await.unwrap();

    let token_response = reqwest::Client::new()
        .get(format!("{}/api/v1/access/token", node.base_url))
        .bearer_auth(&oidc_token)
        .send()
        .await
        .unwrap();
    assert_eq!(token_response.status(), StatusCode::OK);
    let token: GetTokenResponse = token_response.json().await.unwrap();

    (registered, token.token)
}

async fn create_local_secret(node: &TestNode) -> String {
    let onboarding_secret = OnboardingSecret {
        seed_url: node.base_url.clone(),
        enrollment_id: Ulid::generate(),
        secret: [7u8; 32],
        mode: OnboardingMode::Server,
        realm_id: node.realm_id,
        purpose: OnboardingPurpose::InitialAdministrator,
    };
    drive(
        CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
            record: OnboardingSecretRecord {
                enrollment_id: onboarding_secret.enrollment_id,
                secret_hash: onboarding_secret.secret_hash(),
                mode: OnboardingMode::Server,
                purpose: OnboardingPurpose::InitialAdministrator,
                expires_at: u64::MAX,
                claimed_node_id: None,
            },
        }),
        node.context.as_ref(),
    )
    .await
    .unwrap();
    onboarding_secret.encode().unwrap()
}

#[tokio::test]
async fn membership_request_flow() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;
    let (_, owner_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "owner",
        "Group Owner",
        None,
    )
    .await;
    let (member, member_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "member",
        "Prospective Member",
        None,
    )
    .await;
    let (_, other_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "other",
        "Other User",
        None,
    )
    .await;
    let client = reqwest::Client::new();
    let groups_url = format!("{}/api/v1/access/groups", node.base_url);
    let group = client
        .post(&groups_url)
        .bearer_auth(&owner_token)
        .json(&serde_json::json!({ "name": "Research group" }))
        .send()
        .await
        .unwrap();
    assert_eq!(group.status(), StatusCode::CREATED);
    let group: crate::routes::groups::CreateGroupResponse = group.json().await.unwrap();
    let groups = client
        .get(&groups_url)
        .bearer_auth(&member_token)
        .send()
        .await
        .unwrap();
    assert_eq!(groups.status(), StatusCode::OK);
    let groups: serde_json::Value = groups.json().await.unwrap();
    assert!(groups.to_string().contains(&group.group_id));
    let requests_url = format!("{groups_url}/{}/join-requests", group.group_id);
    let own_url = format!("{}/api/v1/access/users/join-requests", node.base_url);
    let request = client
        .post(&requests_url)
        .bearer_auth(&member_token)
        .json(&serde_json::json!({"message":"I would like to join"}))
        .send()
        .await
        .unwrap();
    assert_eq!(request.status(), StatusCode::CREATED);
    let request: serde_json::Value = request.json().await.unwrap();
    assert_eq!(request["status"], "pending");
    assert_eq!(request["user_id"], member.id);
    let request_id = request["request_id"].as_str().unwrap();
    let duplicate: serde_json::Value = client
        .post(&requests_url)
        .bearer_auth(&member_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(duplicate["request_id"], request_id);
    for token in [&member_token, &other_token] {
        assert_eq!(
            client
                .get(&requests_url)
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            client
                .post(format!("{requests_url}/{request_id}/decide"))
                .bearer_auth(token)
                .json(&serde_json::json!({"approve":true}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(
        client
            .delete(format!("{requests_url}/{request_id}"))
            .bearer_auth(&other_token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let own: serde_json::Value = client
        .get(&own_url)
        .bearer_auth(&member_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(own["requests"].as_array().unwrap().len(), 1);
    let other: serde_json::Value = client
        .get(&own_url)
        .bearer_auth(&other_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(other["requests"].as_array().unwrap().is_empty());
    let inbox: serde_json::Value = client
        .get(format!("{requests_url}?status=pending"))
        .bearer_auth(&owner_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(inbox["requests"][0]["user_name"], "Prospective Member");
    let denied = client
        .post(format!("{requests_url}/{request_id}/decide"))
        .bearer_auth(&owner_token)
        .json(&serde_json::json!({"approve":false,"reason":"Not yet"}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::OK);
    let denied: serde_json::Value = denied.json().await.unwrap();
    assert_eq!(denied["request"]["status"], "denied");
    assert_eq!(denied["request"]["decision_reason"], "Not yet");
    let members_url = format!("{groups_url}/{}/members", group.group_id);
    let members: serde_json::Value = client
        .get(&members_url)
        .bearer_auth(&owner_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !members["members"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["user_id"] == member.id)
    );
    let withdrawn: serde_json::Value = client
        .post(&requests_url)
        .bearer_auth(&member_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let withdrawn_id = withdrawn["request_id"].as_str().unwrap();
    for _ in 0..2 {
        assert_eq!(
            client
                .delete(format!("{requests_url}/{withdrawn_id}"))
                .bearer_auth(&member_token)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        client
            .post(format!("{requests_url}/{withdrawn_id}/decide"))
            .bearer_auth(&owner_token)
            .json(&serde_json::json!({"approve":true}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let pending: serde_json::Value = client
        .post(&requests_url)
        .bearer_auth(&member_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let pending_id = pending["request_id"].as_str().unwrap();
    let page: serde_json::Value = client
        .get(format!("{own_url}?limit=1"))
        .bearer_auth(&member_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["requests"].as_array().unwrap().len(), 1);
    let first_id = page["requests"][0]["request_id"]
        .as_str()
        .unwrap()
        .to_string();
    let cursor = page["next_start_after"].as_str().unwrap();
    let page: serde_json::Value = client
        .get(format!("{own_url}?limit=1&start_after={cursor}"))
        .bearer_auth(&member_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        std::collections::BTreeSet::from([
            first_id.as_str(),
            page["requests"][0]["request_id"].as_str().unwrap()
        ]),
        std::collections::BTreeSet::from([request_id, pending_id]),
    );
    assert!(page["next_start_after"].is_null());
    assert_eq!(
        client
            .post(format!("{requests_url}/{pending_id}/decide"))
            .bearer_auth(&owner_token)
            .json(&serde_json::json!({"approve":true,"role_ids":[Ulid::generate().to_string()]}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    for _ in 0..2 {
        let approved = client
            .post(format!("{requests_url}/{pending_id}/decide"))
            .bearer_auth(&owner_token)
            .json(&serde_json::json!({"approve":true}))
            .send()
            .await
            .unwrap();
        assert_eq!(approved.status(), StatusCode::OK);
        let approved: serde_json::Value = approved.json().await.unwrap();
        assert_eq!(approved["request"]["status"], "approved");
    }
    let members: serde_json::Value = client
        .get(&members_url)
        .bearer_auth(&owner_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        members["members"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["user_id"] == member.id)
    );
    assert_eq!(
        client
            .post(&requests_url)
            .bearer_auth(&member_token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        client
            .post(format!("{requests_url}/{pending_id}/decide"))
            .bearer_auth(&owner_token)
            .json(&serde_json::json!({"approve":false}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    install_deny_policy(&node, "permission == 'write'").await;
    assert_eq!(
        client
            .post(&requests_url)
            .bearer_auth(&other_token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn public_profile_search() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;
    let (_, reader_token) =
        register_via_oidc(&node, issuer, kid, &signing_key, "reader", "Reader", None).await;
    let (target, target_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "target",
        "Searchable Alice",
        None,
    )
    .await;
    let client = reqwest::Client::new();
    let me_url = format!("{}/api/v1/access/users/me", node.base_url);
    let search_url = format!("{}/api/v1/access/users/search", node.base_url);
    let profile_url = format!("{}/api/v1/access/users/{}", node.base_url, target.id);
    let patch = client
        .patch(&me_url)
        .bearer_auth(&target_token)
        .json(&serde_json::json!({ "set_attributes": { "email": "secret@example.test" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(patch.status(), StatusCode::OK);
    for (query, expected) in [("Searchable", 1), ("secret", 0)] {
        let response = client
            .get(format!("{search_url}?q={query}"))
            .bearer_auth(&reader_token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: super::SearchUsersResponse = response.json().await.unwrap();
        assert_eq!(body.users.len(), expected);
    }
    let body: super::GetUserResponse = client
        .get(&profile_url)
        .bearer_auth(&reader_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body.name, "Searchable Alice");
    assert!(body.attributes.is_empty());
    assert!(body.subject_ids.is_empty());
    let patch = client
        .patch(&me_url)
        .bearer_auth(&target_token)
        .json(&serde_json::json!({ "set_attributes": {
                "profile.visibility.email": "public", "profile.visibility.name": "private",
            } }))
        .send()
        .await
        .unwrap();
    assert_eq!(patch.status(), StatusCode::OK);
    for (query, expected) in [("Searchable", 0), ("secret", 1)] {
        let response = client
            .get(format!("{search_url}?q={query}"))
            .bearer_auth(&reader_token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: super::SearchUsersResponse = response.json().await.unwrap();
        assert_eq!(body.users.len(), expected);
        if let Some(user) = body.users.first() {
            assert_eq!(user.name, target.id);
        }
    }
    let body: super::GetUserResponse = client
        .get(&profile_url)
        .bearer_auth(&reader_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body.name, target.id);
    assert_eq!(
        body.attributes,
        std::collections::HashMap::from([("email".into(), "secret@example.test".into()),])
    );
    assert!(body.subject_ids.is_empty());
    let denied = client
        .get(format!("{search_url}?q=secret"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    install_deny_policy(&node, "permission == 'read'").await;
    let denied = client
        .get(format!("{search_url}?q=secret"))
        .bearer_auth(&reader_token)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn reads_own_profile() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;

    let (registered, aruna_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "subject-123",
        "Alice",
        None,
    )
    .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{}/api/v1/access/users/{}",
            node.base_url, registered.id
        ))
        .bearer_auth(&aruna_token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: super::GetUserResponse = response.json().await.unwrap();
    assert_eq!(body.name, "Alice");
    assert!(!body.subject_ids.is_empty());

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn token_expiry_bounded() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;

    let (_registered, aruna_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "expiry-subject",
        "Expiry Alice",
        None,
    )
    .await;

    let payload = aruna_token.split('.').nth(1).unwrap();
    let claims: TokenClaims = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(claims.exp, claims.iat + super::USER_TOKEN_EXPIRY_SECONDS);

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn registration_consumes_secret() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, false).await;

    let onboarding_secret = create_local_secret(&node).await;
    let (body, _token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "bootstrap-subject",
        "Admin Alice",
        Some(onboarding_secret.clone()),
    )
    .await;
    assert_eq!(body.name, "Admin Alice");

    let second_response = reqwest::Client::new()
        .post(format!("{}/api/v1/access/users/register", node.base_url))
        .bearer_auth(sign_oidc_token(
            issuer,
            kid,
            &signing_key,
            "bootstrap-subject-2",
            Some("Other Admin"),
        ))
        .json(&RegisterUserRequest {
            onboarding_secret: Some(onboarding_secret),
        })
        .send()
        .await
        .unwrap();

    assert_eq!(second_response.status(), StatusCode::UNAUTHORIZED);

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn missing_public_profile() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;

    let (_admin, admin_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "subject-404",
        "Admin Alice",
        None,
    )
    .await;

    let missing_user_id = UserId::local(Ulid::generate(), node.realm_id);
    let response = reqwest::Client::new()
        .get(format!(
            "{}/api/v1/access/users/{}",
            node.base_url, missing_user_id
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn policy_denies_update() {
    // A realm deny-writes policy must block a non-self update at the route,
    // even though the admin holds the RBAC write the operation would accept.
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;

    let (target, _target_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "subject-target",
        "Target Bob",
        None,
    )
    .await;
    let admin = aruna_core::structs::AuthContext {
        user_id: node.realm_admin_id,
        realm_id: node.realm_id,
        path_restrictions: None,
        session: None,
    };

    let rename = |name: &str| super::UpdateUserRequest {
        name: Some(name.to_string()),
        set_attributes: Default::default(),
        remove_attributes: Default::default(),
    };

    let allowed = super::update_user(
        axum::extract::State(node.state.clone()),
        axum::Extension(Some(admin.clone())),
        axum::extract::Path(target.id.clone()),
        axum::Json(rename("FirstRename")),
    )
    .await;
    assert!(matches!(allowed, Ok((StatusCode::OK, _))));

    install_deny_policy(&node, "permission == 'write'").await;

    let denied = super::update_user(
        axum::extract::State(node.state.clone()),
        axum::Extension(Some(admin.clone())),
        axum::extract::Path(target.id.clone()),
        axum::Json(rename("SecondRename")),
    )
    .await;
    assert!(matches!(denied, Err(crate::error::ServerError::Forbidden)));

    let fetched = super::get_user(
        axum::extract::State(node.state.clone()),
        axum::Extension(Some(admin)),
        axum::extract::Path(target.id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(fetched.1.0.name, "FirstRename");

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

async fn install_deny_policy(node: &TestNode, expression: &str) {
    let mut config = read_realm_config(node.context.as_ref(), &node.realm_id).await;
    config
        .request_policies
        .push(aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "deny".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        });
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: node.realm_admin_id,
        realm_id: node.realm_id,
    };
    let bytes = config.to_bytes(&actor).unwrap();
    let _ = node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(node.realm_id.as_bytes().to_vec()),
            value: ByteView::from(bytes),
            txn_id: None,
        }))
        .await;
}

#[tokio::test]
async fn policy_denies_reads() {
    // A realm deny-reads policy must block both admin user reads at the
    // route, though RBAC alone would let the realm admin through.
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;

    let (target, _target_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "subject-read",
        "Target Bob",
        None,
    )
    .await;
    let admin = aruna_core::structs::AuthContext {
        user_id: node.realm_admin_id,
        realm_id: node.realm_id,
        path_restrictions: None,
        session: None,
    };
    let query = || {
        axum::extract::Query(super::ListUsersQuery {
            limit: None,
            start_after: None,
        })
    };

    let fetched = super::get_user(
        axum::extract::State(node.state.clone()),
        axum::Extension(Some(admin.clone())),
        axum::extract::Path(target.id.clone()),
    )
    .await
    .expect("the realm admin may read a user");
    assert_eq!(fetched.1.0.name, "Target Bob");
    let listed = super::list_users(
        axum::extract::State(node.state.clone()),
        axum::Extension(Some(admin.clone())),
        query(),
    )
    .await
    .expect("the realm admin may list users");
    assert!(!listed.1.0.users.is_empty());

    install_deny_policy(&node, "permission == 'read'").await;

    let denied_get = super::get_user(
        axum::extract::State(node.state.clone()),
        axum::Extension(Some(admin.clone())),
        axum::extract::Path(target.id.clone()),
    )
    .await;
    assert!(matches!(
        denied_get,
        Err(crate::error::ServerError::Forbidden)
    ));
    let denied_list = super::list_users(
        axum::extract::State(node.state.clone()),
        axum::Extension(Some(admin)),
        query(),
    )
    .await;
    assert!(matches!(
        denied_list,
        Err(crate::error::ServerError::Forbidden)
    ));

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn user_requires_authentication() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;

    let missing_user_id = UserId::local(Ulid::generate(), node.realm_id);
    let response = reqwest::Client::new()
        .get(format!(
            "{}/api/v1/access/users/{}",
            node.base_url, missing_user_id
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn foreign_realm_unimplemented() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;

    let foreign_realm_signing_key = generate_signing_key();
    let foreign_realm_id =
        RealmId::from_bytes(foreign_realm_signing_key.verifying_key().to_bytes());
    node.state.add_trusted_realm(foreign_realm_id).await;
    let foreign_user_id = UserId::local(Ulid::generate(), foreign_realm_id);
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(foreign_user_id.to_bytes()),
            value: ByteView::from(
                User {
                    user_id: foreign_user_id,
                    name: "Foreign User".to_string(),
                    subject_ids: Vec::new(),
                    alias_user_ids: Default::default(),
                    attributes: Default::default(),
                }
                .to_bytes(&Actor {
                    node_id: node.net.node_id(),
                    user_id: node.realm_admin_id,
                    realm_id: node.realm_id,
                })
                .unwrap(),
            ),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected foreign user write result: {other:?}"),
    }
    // Revocation lookup is fail-closed against the issuing realm, so the
    // foreign config must exist for the request to reach the route.
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(foreign_realm_id.as_bytes().to_vec()),
            value: ByteView::from(
                RealmConfigDocument::default_for_realm(foreign_realm_id, Vec::new())
                    .to_bytes(&Actor {
                        node_id: node.net.node_id(),
                        user_id: foreign_user_id,
                        realm_id: foreign_realm_id,
                    })
                    .unwrap(),
            ),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected foreign realm config write result: {other:?}"),
    }
    let token = drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time: super::now_timestamp(),
            expiry: None,
            user_id: foreign_user_id,
            realm_id: foreign_realm_id,
            node_capabilities: NodeCapabilities::management_node(foreign_realm_signing_key)
                .unwrap(),

            session: None,
        })
        .unwrap(),
        node.context.as_ref(),
    )
    .await
    .unwrap();

    let response = reqwest::Client::new()
        .get(format!(
            "{}/api/v1/access/users/{}",
            node.base_url, node.realm_admin_id
        ))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn registered_user_token() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;

    let oidc_token = sign_oidc_token(issuer, kid, &signing_key, "subject-123", Some("Alice"));
    let register = reqwest::Client::new()
        .post(format!("{}/api/v1/access/users/register", node.base_url))
        .bearer_auth(&oidc_token)
        .json(&RegisterUserRequest {
            onboarding_secret: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(register.status(), StatusCode::CREATED);

    let token_response = reqwest::Client::new()
        .get(format!("{}/api/v1/access/token", node.base_url))
        .bearer_auth(&oidc_token)
        .send()
        .await
        .unwrap();

    assert_eq!(token_response.status(), StatusCode::OK);
    let body: GetTokenResponse = token_response.json().await.unwrap();
    assert!(!body.token.is_empty());

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn refresh_preserves_kind() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;
    let (_registered, portal_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "subject-123",
        "Alice",
        None,
    )
    .await;
    let created = reqwest::Client::new()
        .post(format!("{}/api/v1/access/sessions", node.base_url))
        .bearer_auth(&portal_token)
        .json(&CreateSessionRequest {
            kind: "assistant".to_string(),
            label: None,
            expires_in_seconds: Some(600),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let assistant: CreateSessionResponse = created.json().await.unwrap();

    let refreshed = reqwest::Client::new()
        .get(format!("{}/api/v1/access/token", node.base_url))
        .bearer_auth(&assistant.token)
        .send()
        .await
        .unwrap();
    assert_eq!(refreshed.status(), StatusCode::OK);
    let refreshed: GetTokenResponse = refreshed.json().await.unwrap();
    let claims = handle_token(&node.state, &refreshed.token).await.unwrap();
    assert_eq!(claims.session_kind, Some(SessionKind::Assistant));

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn scoped_token_rejected() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;

    let (registered, _aruna_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "subject-123",
        "Alice",
        None,
    )
    .await;
    let scoped_token = sign_scoped_token(&node, UserId::from_string(&registered.id).unwrap());

    let token_response = reqwest::Client::new()
        .get(format!("{}/api/v1/access/token", node.base_url))
        .bearer_auth(&scoped_token)
        .send()
        .await
        .unwrap();

    assert_eq!(token_response.status(), StatusCode::FORBIDDEN);

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn alias_token_rejected() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider, true).await;

    let (registered, _aruna_token) = register_via_oidc(
        &node,
        issuer,
        kid,
        &signing_key,
        "subject-123",
        "Alice",
        None,
    )
    .await;
    let canonical_user_id = UserId::from_string(&registered.id).unwrap();
    let alias_user_id = UserId::local(Ulid::from_bytes([9u8; 16]), node.realm_id);
    let subject_key = oidc_subject_key(issuer, "subject-123").unwrap();
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(alias_user_id.to_bytes()),
            value: ByteView::from(
                User {
                    user_id: alias_user_id,
                    name: "Alias Alice".to_string(),
                    subject_ids: vec![subject_key],
                    alias_user_ids: Default::default(),
                    attributes: Default::default(),
                }
                .to_bytes(&Actor {
                    node_id: node.net.node_id(),
                    user_id: canonical_user_id,
                    realm_id: node.realm_id,
                })
                .unwrap(),
            ),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected alias user write result: {other:?}"),
    }
    let alias_token = sign_aruna_token(&node, alias_user_id, None);

    let token_response = reqwest::Client::new()
        .get(format!("{}/api/v1/access/token", node.base_url))
        .bearer_auth(&alias_token)
        .send()
        .await
        .unwrap();

    assert_eq!(token_response.status(), StatusCode::FORBIDDEN);

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}
