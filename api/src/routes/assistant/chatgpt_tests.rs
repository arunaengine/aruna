//! Tests the ChatGPT login start and poll steps, token refresh, and claim parsing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::assistant::{setup_state, spawn_mock};
use axum::Router;
use axum::routing::post;
use serde::Deserialize;

const ID_TOKEN: &str =
    "aaa.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjdF8xIn19.bbb";

#[derive(Deserialize)]
struct IntervalWrapper {
    #[serde(deserialize_with = "super::deserialize_interval")]
    interval: u64,
}

async fn seed_login(
    state: &ServerState,
    auth: &AuthContext,
    base_url: String,
    status: AssistantProviderStatus,
    expires_at: u64,
) -> String {
    let provider = AssistantProvider {
        provider_id: Ulid::generate().to_string(),
        user_id: auth.user_id,
        kind: AssistantProviderKind::Chatgpt,
        label: "ChatGPT".to_string(),
        base_url,
        headers: EncryptedS3Secret::empty(),
        secret: EncryptedS3Secret::empty(),
        models: Vec::new(),
        default_model: None,
        created_at: unix_timestamp_secs(),
        status,
        token_obtained_at: None,
        login_expires_at: Some(expires_at),
        login_interval_seconds: Some(5),
    };
    let secret = AssistantProviderSecret {
        device_auth_id: Some(Secret::new("dev-1")),
        user_code: Some(Secret::new("ABCD-EFGH")),
        ..AssistantProviderSecret::empty()
    };
    drive(
        CreateProviderOperation::new(
            provider.clone(),
            secret,
            AssistantHeaders(BTreeMap::new()),
            state.credential_encryption_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .unwrap();
    provider.provider_id
}

async fn seed_ready(
    state: &ServerState,
    auth: &AuthContext,
    base_url: String,
    obtained: u64,
) -> AssistantProvider {
    let provider = AssistantProvider {
        provider_id: Ulid::generate().to_string(),
        user_id: auth.user_id,
        kind: AssistantProviderKind::Chatgpt,
        label: "ChatGPT".to_string(),
        base_url,
        headers: EncryptedS3Secret::empty(),
        secret: EncryptedS3Secret::empty(),
        models: Vec::new(),
        default_model: None,
        created_at: unix_timestamp_secs(),
        status: AssistantProviderStatus::Ready,
        token_obtained_at: Some(obtained),
        login_expires_at: None,
        login_interval_seconds: None,
    };
    let secret = AssistantProviderSecret {
        access_token: Some(Secret::new("access-1")),
        refresh_token: Some(Secret::new("refresh-1")),
        account_id: Some(Secret::new("acct-0")),
        ..AssistantProviderSecret::empty()
    };
    drive(
        CreateProviderOperation::new(
            provider,
            secret,
            AssistantHeaders(BTreeMap::new()),
            state.credential_encryption_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn start_registers_pending() {
    let router = Router::new().route(
        "/api/accounts/deviceauth/usercode",
        post(|| async {
            axum::Json(serde_json::json!({
                "device_auth_id": "dev-1",
                "user_code": "ABCD-EFGH",
                "interval": 5,
                "expires_in": 900
            }))
        }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state.with_chatgpt_urls(base_url.clone(), base_url.clone()));
    let (status, Json(body)) = start_login(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Some(Json(StartLoginRequest {
            label: Some("  Work  ".to_string()),
        })),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body.user_code, "ABCD-EFGH");
    assert_eq!(body.interval_seconds, 5);
    assert_eq!(body.verification_url, format!("{base_url}/codex/device"));
    let provider = load_provider(&state, auth.user_id, body.provider_id)
        .await
        .unwrap();
    assert_eq!(provider.status, AssistantProviderStatus::PendingLogin);
    assert_eq!(provider.label, "Work");
    let secret = provider
        .open_secret(state.credential_encryption_key())
        .unwrap();
    assert_eq!(secret.device_auth_id.as_ref().unwrap().expose(), "dev-1");
    handle.abort();
}

#[tokio::test]
async fn start_upstream_fails() {
    let router = Router::new().route(
        "/api/accounts/deviceauth/usercode",
        post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state.with_chatgpt_urls(base_url.clone(), base_url));
    let error = start_login(State(state), Extension(Some(auth)), None)
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::BadGatewayReason(_)));
    handle.abort();
}

#[tokio::test]
async fn start_login_guards() {
    let (_dir, state, auth) = setup_state().await;
    let disabled = Arc::new(state.with_assistant_proxy(false));
    let error = start_login(State(disabled), Extension(Some(auth)), None)
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::FeatureDisabled(_)));
    let (_dir, state, _auth) = setup_state().await;
    let error = start_login(State(Arc::new(state)), Extension(None), None)
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::Unauthorized));
}

#[tokio::test]
async fn poll_login_completes() {
    let router = Router::new()
        .route(
            "/api/accounts/deviceauth/token",
            post(|| async {
                axum::Json(serde_json::json!({
                    "authorization_code": "auth-1",
                    "code_verifier": "verify-1"
                }))
            }),
        )
        .route(
            "/oauth/token",
            post(|| async {
                axum::Json(serde_json::json!({
                    "id_token": ID_TOKEN,
                    "access_token": "access-1",
                    "refresh_token": "refresh-1"
                }))
            }),
        );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state.with_chatgpt_urls(base_url.clone(), base_url.clone()));
    let provider_id = seed_login(
        &state,
        &auth,
        base_url,
        AssistantProviderStatus::PendingLogin,
        unix_timestamp_secs() + 900,
    )
    .await;
    let (status, Json(body)) = poll_login(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Path(provider_id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.status, "ready");
    let provider = load_provider(&state, auth.user_id, provider_id)
        .await
        .unwrap();
    assert_eq!(provider.status, AssistantProviderStatus::Ready);
    let secret = provider
        .open_secret(state.credential_encryption_key())
        .unwrap();
    assert_eq!(secret.access_token.as_ref().unwrap().expose(), "access-1");
    assert_eq!(secret.account_id.as_ref().unwrap().expose(), "acct_1");
    assert!(secret.device_auth_id.is_none());
    handle.abort();
}

#[tokio::test]
async fn poll_login_pending() {
    let router = Router::new().route(
        "/api/accounts/deviceauth/token",
        post(|| async { StatusCode::FORBIDDEN }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state.with_chatgpt_urls(base_url.clone(), base_url.clone()));
    let provider_id = seed_login(
        &state,
        &auth,
        base_url,
        AssistantProviderStatus::PendingLogin,
        unix_timestamp_secs() + 900,
    )
    .await;
    let (_status, Json(body)) = poll_login(State(state), Extension(Some(auth)), Path(provider_id))
        .await
        .unwrap();
    assert_eq!(body.status, "pending");
    handle.abort();
}

#[tokio::test]
async fn poll_login_denied() {
    let router = Router::new().route(
        "/api/accounts/deviceauth/token",
        post(|| async {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({"error": "access_denied"})),
            )
        }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state.with_chatgpt_urls(base_url.clone(), base_url.clone()));
    let provider_id = seed_login(
        &state,
        &auth,
        base_url,
        AssistantProviderStatus::PendingLogin,
        unix_timestamp_secs() + 900,
    )
    .await;
    let (_status, Json(body)) = poll_login(State(state), Extension(Some(auth)), Path(provider_id))
        .await
        .unwrap();
    assert_eq!(body.status, "denied");
    handle.abort();
}

#[tokio::test]
async fn poll_login_expired() {
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state);
    let provider_id = seed_login(
        &state,
        &auth,
        "http://127.0.0.1:1".to_string(),
        AssistantProviderStatus::PendingLogin,
        1,
    )
    .await;
    let (_status, Json(body)) = poll_login(State(state), Extension(Some(auth)), Path(provider_id))
        .await
        .unwrap();
    assert_eq!(body.status, "expired");
}

#[tokio::test]
async fn poll_already_ready() {
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state);
    let provider = seed_ready(&state, &auth, "http://127.0.0.1:1".to_string(), 1).await;
    let (_status, Json(body)) = poll_login(
        State(state),
        Extension(Some(auth)),
        Path(provider.provider_id),
    )
    .await
    .unwrap();
    assert_eq!(body.status, "ready");
}

#[tokio::test]
async fn poll_exchange_fails() {
    let router = Router::new()
        .route(
            "/api/accounts/deviceauth/token",
            post(|| async {
                axum::Json(serde_json::json!({
                    "authorization_code": "auth-1",
                    "code_verifier": "verify-1"
                }))
            }),
        )
        .route(
            "/oauth/token",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state.with_chatgpt_urls(base_url.clone(), base_url.clone()));
    let provider_id = seed_login(
        &state,
        &auth,
        base_url,
        AssistantProviderStatus::PendingLogin,
        unix_timestamp_secs() + 900,
    )
    .await;
    let error = poll_login(State(state), Extension(Some(auth)), Path(provider_id))
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::BadGatewayReason(_)));
    handle.abort();
}

#[tokio::test]
async fn refresh_rotates_tokens() {
    let router = Router::new().route(
        "/oauth/token",
        post(|| async {
            axum::Json(serde_json::json!({
                "id_token": ID_TOKEN,
                "access_token": "access-2",
                "refresh_token": "refresh-2"
            }))
        }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state.with_chatgpt_urls(base_url.clone(), base_url.clone()));
    let provider = seed_ready(&state, &auth, base_url, 0).await;
    let refreshed = refresh_provider(&state, provider).await.unwrap();
    let secret = refreshed
        .open_secret(state.credential_encryption_key())
        .unwrap();
    assert_eq!(secret.access_token.as_ref().unwrap().expose(), "access-2");
    assert_eq!(secret.refresh_token.as_ref().unwrap().expose(), "refresh-2");
    assert_eq!(secret.account_id.as_ref().unwrap().expose(), "acct_1");
    assert!(refreshed.token_obtained_at.unwrap() > 0);
    handle.abort();
}

#[tokio::test]
async fn provider_rejects_pending() {
    let (_dir, state, auth) = setup_state().await;
    let provider = AssistantProvider {
        provider_id: Ulid::generate().to_string(),
        user_id: auth.user_id,
        kind: AssistantProviderKind::Chatgpt,
        label: "ChatGPT".to_string(),
        base_url: "http://127.0.0.1:1".to_string(),
        headers: EncryptedS3Secret::empty(),
        secret: EncryptedS3Secret::empty(),
        models: Vec::new(),
        default_model: None,
        created_at: unix_timestamp_secs(),
        status: AssistantProviderStatus::PendingLogin,
        token_obtained_at: None,
        login_expires_at: None,
        login_interval_seconds: None,
    };
    let error = fresh_provider(&state, provider).await.unwrap_err();
    assert!(matches!(error, ServerError::BadRequestReason(_)));
}

#[test]
fn extracts_account_id() {
    let token = "aaa.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjdF8xIn19.bbb";
    assert_eq!(extract_account_id(token).as_deref(), Some("acct_1"));
}

#[test]
fn account_id_missing() {
    assert_eq!(extract_account_id("only-one-segment"), None);
    assert_eq!(extract_account_id("aaa.notbase64!.bbb"), None);
}

#[test]
fn parses_interval_forms() {
    let from_string: IntervalWrapper =
        serde_json::from_value(serde_json::json!({ "interval": "7" })).unwrap();
    assert_eq!(from_string.interval, 7);
    let from_number: IntervalWrapper =
        serde_json::from_value(serde_json::json!({ "interval": 3 })).unwrap();
    assert_eq!(from_number.interval, 3);
    // An unexpected shape falls back to the issuer's minimum poll interval.
    let from_other: IntervalWrapper =
        serde_json::from_value(serde_json::json!({ "interval": true })).unwrap();
    assert_eq!(from_other.interval, 5);
}

#[test]
fn static_model_efforts() {
    for model in static_models() {
        if model.id.starts_with("gpt-5.6") {
            assert_eq!(
                model.reasoning_efforts,
                ["minimal", "low", "medium", "high", "xhigh", "max", "ultra"]
            );
        } else {
            assert_eq!(
                model.reasoning_efforts,
                ["minimal", "low", "medium", "high", "xhigh"]
            );
        }
    }
}
