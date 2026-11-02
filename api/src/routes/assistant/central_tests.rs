//! Tests central assistant setup rights, key hiding, quota, pseudonyms, sessions and relaying.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::assistant::proxy::proxy_post;
use crate::routes::management_relay::relay_middleware;
use crate::tests::assistant::spawn_mock;
use crate::tests::routes::{test_state, test_storage};
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::{MANAGEMENT_URL_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_operations::driver::DriverContext;
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_tasks::TaskHandle;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::Path;
use axum::middleware::from_fn_with_state;
use axum::response::IntoResponse;
use axum::routing::post;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tower::ServiceExt;
use ulid::Ulid;

const KEY: &str = "sk-central-secret-1234";
const MODEL: &str = "deepseek-v4.1";

struct Fixture {
    _dir: TempDir,
    state: Arc<ServerState>,
    admin: AuthContext,
    member: AuthContext,
}

fn context(dir: &TempDir) -> Arc<DriverContext> {
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    })
}

fn member_of(realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

/// A realm whose only management node is this one, so it holds the central assistant.
/// It runs as a user node, so the mock provider on a loopback address passes the URL check.
async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let context = context(&dir);
    let realm_id = RealmId::from_bytes(generate_signing_key().verifying_key().to_bytes());
    let node_id = iroh::SecretKey::generate().public();
    let admin = member_of(realm_id);
    let actor = || Actor {
        node_id,
        user_id: admin.user_id,
        realm_id,
    };
    let config = CreateRealmConfig {
        actor: actor(),
        realm_description: "Realm".to_string(),
        oidc_providers: vec![],
        node_location: None,
        node_weight: None,
        node_labels: Default::default(),
    };
    drive(CreateRealmOperation::new(config), &context)
        .await
        .unwrap();
    let claim = ClaimInitialInput { actor: actor() };
    drive(ClaimInitialOperation::new(claim), &context)
        .await
        .unwrap();
    let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
    let state = Arc::new(test_state(context, realm_id, node_id, capabilities).await);
    Fixture {
        _dir: dir,
        state,
        admin,
        member: member_of(realm_id),
    }
}

fn json_request(body: serde_json::Value) -> Request {
    Request::builder()
        .method("PUT")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn setup_body(base_url: &str, limit: Option<u32>) -> serde_json::Value {
    serde_json::json!({
        "kind": "openai_compatible",
        "label": "Realm assistant",
        "api_key": KEY,
        "base_url": base_url,
        "models": [{"id": MODEL}],
        "default_model": MODEL,
        "daily_limit": limit,
    })
}

async fn set_up(fixture: &Fixture, base_url: &str, limit: Option<u32>) -> CentralResponse {
    let (_, Json(view)) = put_central(
        State(fixture.state.clone()),
        Extension(Some(fixture.admin.clone())),
        json_request(setup_body(base_url, limit)),
    )
    .await
    .unwrap();
    view
}

struct Seen {
    headers: HeaderMap,
    body: serde_json::Value,
}

/// A provider that records each chat call and answers with a short stream.
async fn mock_provider() -> (String, mpsc::UnboundedReceiver<Seen>, Arc<AtomicUsize>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move |headers: HeaderMap, body: Bytes| {
            let sender = sender.clone();
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                let body = serde_json::from_slice(&body).unwrap();
                sender.send(Seen { headers, body }).unwrap();
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    "data: [DONE]\n\n",
                )
            }
        }),
    );
    let (base_url, _handle) = spawn_mock(router).await;
    (base_url, receiver, calls)
}

fn chat_request(session: Option<&str>, model: &str) -> Request {
    let mut builder = Request::builder()
        .method("POST")
        .header("content-type", "application/json")
        .header("x-forwarded-for", "10.1.2.3")
        .header(USER_HEADER, "spoofed");
    if let Some(session) = session {
        builder = builder.header(SESSION_HEADER, session);
    }
    let body = serde_json::json!({
        "model": model,
        "messages": [],
        "stream": true,
        "user": "x",
        "tools": [{"type": "function", "function": {"name": "submit_job"}}],
        "chat_id": "owner-chat",
        "parent_id": null,
        "features": {"memory": true},
    });
    builder.body(Body::from(body.to_string())).unwrap()
}

async fn call(fixture: &Fixture, auth: &AuthContext, request: Request) -> ServerResult<Response> {
    let path = ("central".to_string(), "v1/chat/completions".to_string());
    proxy_post(
        State(fixture.state.clone()),
        Extension(Some(auth.clone())),
        Path(path),
        request,
    )
    .await
}

#[tokio::test]
async fn visitor_is_refused() {
    let fixture = fixture().await;
    let refused = get_central(State(fixture.state.clone()), Extension(None)).await;
    assert!(matches!(refused, Err(ServerError::Unauthorized)));
    let refused = proxy_post(
        State(fixture.state.clone()),
        Extension(None),
        Path(("central".to_string(), "v1/chat/completions".to_string())),
        chat_request(Some("c1"), MODEL),
    )
    .await;
    assert!(matches!(refused, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn setup_is_admin_only() {
    let fixture = fixture().await;
    let refused = put_central(
        State(fixture.state.clone()),
        Extension(Some(fixture.member.clone())),
        json_request(setup_body("http://127.0.0.1:9", None)),
    )
    .await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
    let refused = delete_central(
        State(fixture.state.clone()),
        Extension(Some(fixture.member.clone())),
    )
    .await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
    let mut chatgpt = setup_body("http://127.0.0.1:9", None);
    chatgpt["kind"] = "chatgpt".into();
    let refused = put_central(
        State(fixture.state.clone()),
        Extension(Some(fixture.admin.clone())),
        json_request(chatgpt),
    )
    .await;
    assert!(matches!(refused, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn key_never_returned() {
    let fixture = fixture().await;
    let view = set_up(&fixture, "http://127.0.0.1:9", Some(50)).await;
    assert!(!serde_json::to_string(&view).unwrap().contains(KEY));
    assert!(view.admin.is_some());
    let (_, Json(member)) = get_central(
        State(fixture.state.clone()),
        Extension(Some(fixture.member.clone())),
    )
    .await
    .unwrap();
    let text = serde_json::to_string(&member).unwrap();
    assert!(!text.contains(KEY));
    assert!(member.admin.is_none());
    assert_eq!((member.daily_limit, member.used_today), (Some(50), 0));
}

#[tokio::test]
async fn member_calls_pseudonymously() {
    let fixture = fixture().await;
    let (base_url, mut seen, _) = mock_provider().await;
    set_up(&fixture, &base_url, None).await;
    let response = call(
        &fixture,
        &fixture.member,
        chat_request(Some("chat_A-1"), MODEL),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let first = seen.recv().await.unwrap();
    let pseudonym = first.body["user"].as_str().unwrap().to_string();
    assert_eq!(first.headers[USER_HEADER], pseudonym.as_str());
    assert_eq!(first.headers[SESSION_HEADER], "chat_A-1");
    assert_eq!(first.body["session_id"], "chat_A-1");
    assert_eq!(first.body["tools"][0]["function"]["name"], "submit_job");
    for field in ["chat_id", "parent_id", "features"] {
        assert!(
            first.body.get(field).is_none(),
            "{field} reached the provider"
        );
    }
    assert_eq!(
        first.headers["authorization"],
        format!("Bearer {KEY}").as_str()
    );
    assert!(!first.headers.contains_key("x-forwarded-for"));
    let raw = fixture.member.user_id.to_string();
    assert!(!first.body.to_string().contains(&raw));
    assert!(
        first
            .headers
            .values()
            .all(|value| !value.to_str().unwrap_or_default().contains(&raw))
    );

    call(
        &fixture,
        &fixture.member,
        chat_request(Some("chat_B"), MODEL),
    )
    .await
    .unwrap();
    assert_eq!(seen.recv().await.unwrap().body["user"], pseudonym.as_str());
    call(
        &fixture,
        &fixture.admin,
        chat_request(Some("chat_A-1"), MODEL),
    )
    .await
    .unwrap();
    assert_ne!(seen.recv().await.unwrap().body["user"], pseudonym.as_str());
}

#[tokio::test]
async fn bad_session_refused() {
    let fixture = fixture().await;
    let (base_url, _seen, calls) = mock_provider().await;
    set_up(&fixture, &base_url, Some(5)).await;
    for session in [None, Some("bad id!"), Some(&"x".repeat(65)[..])] {
        let refused = call(&fixture, &fixture.member, chat_request(session, MODEL)).await;
        assert!(matches!(refused, Err(ServerError::BadRequestReason(_))));
    }
    let refused = call(&fixture, &fixture.member, chat_request(Some("c1"), "gpt-5")).await;
    assert!(matches!(refused, Err(ServerError::BadRequestReason(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (_, Json(view)) = get_central(
        State(fixture.state.clone()),
        Extension(Some(fixture.member.clone())),
    )
    .await
    .unwrap();
    assert_eq!(view.used_today, 0);
}

#[tokio::test]
async fn quota_refuses_at_limit() {
    let fixture = fixture().await;
    let (base_url, _seen, calls) = mock_provider().await;
    set_up(&fixture, &base_url, Some(1)).await;
    call(&fixture, &fixture.member, chat_request(Some("c1"), MODEL))
        .await
        .unwrap();
    let refused = call(&fixture, &fixture.member, chat_request(Some("c1"), MODEL))
        .await
        .unwrap_err()
        .into_response();
    assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry: u64 = refused.headers()[header::RETRY_AFTER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=86_400).contains(&retry));
    let body = to_bytes(refused.into_body(), usize::MAX).await.unwrap();
    let body: ErrorResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.code.as_deref(), Some("assistant_quota_exhausted"));
    let quota = body.quota.unwrap();
    assert_eq!((quota.observed, quota.requested, quota.limit), (1, 1, 1));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Another member still has their own calls.
    call(&fixture, &fixture.admin, chat_request(Some("c1"), MODEL))
        .await
        .unwrap();
}

#[tokio::test]
async fn quota_resets_midnight() {
    let fixture = fixture().await;
    let user_id = fixture.member.user_id;
    let last_second = 86_400 * 20_000 - 1;
    charge_call(&fixture.state, user_id, Some(1), last_second - 10)
        .await
        .unwrap();
    let refused = charge_call(&fixture.state, user_id, Some(1), last_second).await;
    assert!(matches!(
        refused,
        Err(ServerError::AssistantQuota { retry_after: 1, .. })
    ));
    charge_call(&fixture.state, user_id, Some(1), last_second + 1)
        .await
        .unwrap();
}

async fn write_row(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    state
        .get_ctx()
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
}

/// A device that is not the holder and knows the holder's address from its installation.
async fn device_relay(holder_url: String, realm_id: RealmId) -> (TempDir, Router) {
    let (dir, storage) = test_storage();
    let context = Arc::new(crate::tests::routes::test_context(storage));
    let node_id = iroh::SecretKey::generate().public();
    let owner = UserId::local(Ulid::generate(), realm_id);
    let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
    let state = Arc::new(test_state(context, realm_id, node_id, capabilities).await);
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(node_id, RealmNodeKind::User { owner });
    let config = config.to_bytes(&actor).unwrap();
    let realm_key = realm_id.as_bytes().to_vec();
    write_row(&state, REALM_CONFIG_KEYSPACE, realm_key.clone(), config).await;
    let urls = postcard::to_allocvec(&vec![holder_url]).unwrap();
    write_row(&state, MANAGEMENT_URL_KEYSPACE, realm_key, urls).await;
    let local = || async { StatusCode::IM_A_TEAPOT };
    let router = Router::new()
        .route(
            "/system/assistant/providers/{id}/proxy/{*path}",
            post(local),
        )
        .layer(from_fn_with_state(state.clone(), relay_middleware))
        .with_state(state);
    (dir, router)
}

#[tokio::test]
async fn relays_to_holder() {
    let fixture = fixture().await;
    let (base_url, mut seen, _) = mock_provider().await;
    set_up(&fixture, &base_url, None).await;
    let (holder, _) = super::super::router().split_for_parts();
    let holder = holder
        .layer(Extension(Some(fixture.member.clone())))
        .with_state(fixture.state.clone());
    let (holder_url, _handle) = spawn_mock(holder).await;
    let (_dir, device) = device_relay(holder_url, fixture.state.get_realm_id()).await;

    call(&fixture, &fixture.member, chat_request(Some("c1"), MODEL))
        .await
        .unwrap();
    let direct = seen.recv().await.unwrap();
    let mut relayed = chat_request(Some("c1"), MODEL);
    *relayed.uri_mut() = "/system/assistant/providers/central/proxy/v1/chat/completions"
        .parse()
        .unwrap();
    let response = device.oneshot(relayed).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"data: [DONE]\n\n");
    let through = seen.recv().await.unwrap();
    assert_eq!(through.body["user"], direct.body["user"]);
    assert_eq!(through.headers[USER_HEADER], direct.headers[USER_HEADER]);
    assert_eq!(through.headers[SESSION_HEADER], "c1");
}

#[test]
fn session_needs_tools() {
    // Without its own tools a call must not open an Open WebUI session with server tools.
    let mut central = CentralAssistant {
        provider: AssistantProvider {
            provider_id: CENTRAL_PROVIDER_ID.to_string(),
            user_id: member_of(RealmId::from_bytes([3; 32])).user_id,
            kind: AssistantProviderKind::OpenaiCompatible,
            label: "Realm assistant".to_string(),
            base_url: "https://chat.example.org/api".to_string(),
            headers: EncryptedS3Secret::empty(),
            secret: EncryptedS3Secret::empty(),
            models: vec![MODEL.to_string()],
            default_model: None,
            created_at: 0,
            status: AssistantProviderStatus::Ready,
            token_obtained_at: None,
            login_expires_at: None,
            login_interval_seconds: None,
        },
        daily_limit: None,
        user_header: USER_HEADER.to_string(),
        session_header: SESSION_HEADER.to_string(),
        changed_at: 0,
    };
    let plain = serde_json::json!({"model": MODEL, "messages": [], "session_id": "x"});
    let body = central_body(&central, plain.to_string().as_bytes(), "p", "s1").unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(body.get("session_id").is_none());
    assert_eq!(body["user"], "p");
    central.provider.kind = AssistantProviderKind::Anthropic;
    let body = central_body(&central, plain.to_string().as_bytes(), "p", "s1").unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["metadata"]["user_id"], "p");
    assert!(body.get("user").is_none());
}

#[tokio::test]
async fn concurrent_charges_count() {
    // Calls racing on one user's counter all settle: none fails, the limit still holds.
    let fixture = fixture().await;
    let user_id = fixture.member.user_id;
    let now = 86_400 * 20_000;
    let calls = (0..16).map(|_| {
        let state = fixture.state.clone();
        tokio::spawn(async move { charge_call(&state, user_id, Some(10), now).await })
    });
    let results = futures_util::future::join_all(calls).await;
    let allowed = results
        .iter()
        .filter(|result| matches!(result, Ok(Ok(()))))
        .count();
    let refused = results
        .iter()
        .filter(|result| matches!(result, Ok(Err(ServerError::AssistantQuota { .. }))))
        .count();
    assert_eq!((allowed, refused), (10, 6));
}
