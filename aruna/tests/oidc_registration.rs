use aruna_api::auth::OidcValidator;
use aruna_api::routes::users::RegisterUserResponse;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE};
use aruna_core::structs::{Actor, NodeCapabilities, OidcProviderConfig, User, oidc_subject_key};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::claim_initial_realm_admin::{
    ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
};
use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use axum::Json;
use axum::extract::State;
use axum::routing::get;
use axum::{Router, http::StatusCode};
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
    base_url: String,
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

async fn read_user(context: &DriverContext, user_id: UserId) -> User {
    match context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(user_id.to_bytes()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => User::from_bytes(&bytes).unwrap(),
        other => panic!("unexpected user read result: {other:?}"),
    }
}

async fn read_subject_index(context: &DriverContext, subject_key: &str) -> UserId {
    match context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: USER_SUBJECT_INDEX_KEYSPACE.to_string(),
            key: ByteView::from(subject_key.as_bytes().to_vec()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => UserId::from_storage_key(&bytes).unwrap(),
        other => panic!("unexpected subject index read result: {other:?}"),
    }
}

async fn spawn_test_node(provider: OidcProviderConfig) -> TestNode {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .unwrap();
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
    });
    initialize_net_incoming(context.clone());
    initialize_task_incoming(context.clone(), task_handle).await;

    let realm_signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
    let realm_id =
        aruna_core::structs::RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let bootstrap_user = UserId::local(Ulid::new(), realm_id);
    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: Actor {
                node_id: net.node_id(),
                user_id: bootstrap_user,
                realm_id,
            },
            realm_description: "Test Realm".to_string(),
            oidc_providers: vec![provider.clone()],
        }),
        context.as_ref(),
    )
    .await
    .unwrap();
    drive(
        ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
            actor: Actor {
                node_id: net.node_id(),
                user_id: bootstrap_user,
                realm_id,
            },
        }),
        context.as_ref(),
    )
    .await
    .unwrap();
    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id,
            node_id: net.node_id(),
            schedule_refresh: false,
        }),
        context.as_ref(),
    )
    .await
    .unwrap();

    let state = Arc::new(
        ServerState::new(
            context.clone(),
            realm_id,
            net.node_id(),
            NodeCapabilities::management_node(realm_signing_key).unwrap(),
            false,
            Some(Arc::new(OidcValidator::new().unwrap())),
        )
        .await,
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Server::new(
        state,
        ServerConfig {
            http_addr: addr,
            max_http_body_size: aruna_api::server::DEFAULT_MAX_HTTP_BODY_SIZE,
            cors: aruna_api::cors::CorsConfig::default(),
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
        context,
        base_url: format!("http://{addr}"),
        net,
        server_task,
    }
}

#[tokio::test]
async fn oidc_registration_route_creates_user_indexes_and_token() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
    let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
    let node = spawn_test_node(provider).await;

    let token = sign_oidc_token(issuer, kid, &signing_key, "subject-123", Some("Alice"));
    let response = reqwest::Client::new()
        .post(format!("{}/api/v1/users/register", node.base_url))
        .bearer_auth(token.clone())
        .json(&serde_json::json!({
            // "name": "Alice",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: RegisterUserResponse = response.json().await.unwrap();
    assert_eq!(body.name, "Alice");

    let stored_user = read_user(
        node.context.as_ref(),
        UserId::from_string(&body.id).unwrap(),
    )
    .await;
    assert_eq!(stored_user.name, "Alice");
    let subject_key = oidc_subject_key(issuer, "subject-123").unwrap();
    assert_eq!(
        read_subject_index(node.context.as_ref(), &subject_key)
            .await
            .to_string(),
        body.id
    );
    let response = reqwest::Client::new()
        .post(format!("{}/api/v1/users/register", node.base_url))
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let repeated: RegisterUserResponse = response.json().await.unwrap();
    assert_eq!(repeated.id, body.id);
    assert_eq!(repeated.name, body.name);

    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}
