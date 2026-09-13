use super::*;
use crate::server_state::ServerState;
use crate::tests::fixtures::assistant::{setup_state, spawn_mock};
use aruna_core::compute::Secret;
use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::structs::{AssistantHeaders, AssistantProviderSecret, AssistantProviderStatus};
use aruna_operations::assistant::provider::CreateProviderOperation;
use aruna_operations::driver::drive;
use axum::body::Bytes;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use ulid::Ulid;

struct Observed {
    headers: HeaderMap,
    body: Bytes,
}

fn make_provider(
    state: &ServerState,
    auth: &AuthContext,
    kind: AssistantProviderKind,
    base_url: String,
    api_key: Option<&str>,
) -> AssistantProvider {
    let mut provider = AssistantProvider {
        provider_id: Ulid::generate().to_string(),
        user_id: auth.user_id,
        kind,
        label: "Mock".to_string(),
        base_url,
        headers: EncryptedS3Secret::empty(),
        secret: EncryptedS3Secret::empty(),
        models: Vec::new(),
        default_model: None,
        created_at: aruna_core::time::unix_timestamp_secs(),
        status: AssistantProviderStatus::Ready,
        token_obtained_at: Some(aruna_core::time::unix_timestamp_secs()),
        login_expires_at: None,
        login_interval_seconds: None,
    };
    provider
        .encrypt_secret(
            state.credential_encryption_key(),
            &AssistantProviderSecret {
                api_key: api_key.map(Secret::new),
                ..AssistantProviderSecret::empty()
            },
        )
        .unwrap();
    provider
        .encrypt_headers(
            state.credential_encryption_key(),
            &AssistantHeaders(BTreeMap::from([(
                "x-custom".to_string(),
                Secret::new("custom-value"),
            )])),
        )
        .unwrap();
    provider
}

#[test]
fn rejects_unknown_path() {
    assert!(!allowed_path(
        AssistantProviderKind::Openai,
        &Method::POST,
        "/v1/files"
    ));
}

#[test]
fn forces_chatgpt_store() {
    let body = force_chatgpt(br#"{"store":true,"model":"gpt-5"}"#).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["store"], false);
}

#[test]
fn filters_nontext_models() {
    assert!(!text_model("text-embedding-3-small"));
    assert!(!text_model("gpt-image-1"));
    assert!(text_model("gpt-5.4"));
}

#[test]
fn openrouter_reasoning_param() {
    let listed = ModelItem {
        id: "z-ai/glm".to_string(),
        display_name: None,
        name: None,
        supported_parameters: vec!["reasoning".to_string()],
    };
    let plain = ModelItem {
        id: "z-ai/glm".to_string(),
        display_name: None,
        name: None,
        supported_parameters: Vec::new(),
    };
    assert_eq!(
        model_efforts(AssistantProviderKind::Openrouter, &listed),
        ["off", "low", "medium", "high"]
    );
    assert!(model_efforts(AssistantProviderKind::Openrouter, &plain).is_empty());
}

#[tokio::test]
async fn bounds_model_response() {
    let router = Router::new().route(
        "/v1/models",
        get(|| async {
            let chunks = stream::iter([
                Ok::<_, std::io::Error>(Bytes::from(vec![b' '; PROXY_BODY_LIMIT])),
                Ok(Bytes::from_static(b"x")),
            ]);
            Response::new(Body::from_stream(chunks))
        }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let provider = make_provider(
        &state,
        &auth,
        AssistantProviderKind::OpenaiCompatible,
        base_url,
        None,
    );

    let error = fetch_models(&state, &provider).await.unwrap_err();
    assert!(matches!(
        error,
        ServerError::BadGatewayReason(message) if message.contains("exceeds")
    ));
    handle.abort();
}

#[tokio::test]
async fn chatgpt_models_get() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let router = Router::new().route(
        "/models",
        get(move |method: Method, headers: HeaderMap, body: Bytes| {
            let sender = sender.clone();
            async move {
                sender.send((method, Observed { headers, body })).unwrap();
                "{}"
            }
        }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let mut provider = make_provider(
        &state,
        &auth,
        AssistantProviderKind::Chatgpt,
        base_url,
        None,
    );
    let mut secret = provider
        .open_secret(state.credential_encryption_key())
        .unwrap();
    secret.access_token = Some(Secret::new("access"));
    secret.account_id = Some(Secret::new("account"));
    provider
        .encrypt_secret(state.credential_encryption_key(), &secret)
        .unwrap();

    let models = fetch_models(&state, &provider).await.unwrap();
    assert!(models.is_empty());
    let (method, observed) = receiver.recv().await.unwrap();
    assert_eq!(method, Method::GET);
    assert!(observed.body.is_empty());
    assert_eq!(observed.headers["authorization"], "Bearer access");
    assert_eq!(observed.headers["chatgpt-account-id"], "account");
    handle.abort();
}

#[tokio::test]
async fn strips_inbound_headers() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let router = Router::new().route(
        "/v1/responses",
        post(move |headers: HeaderMap, body: Bytes| {
            let sender = sender.clone();
            async move {
                sender.send(Observed { headers, body }).unwrap();
                "ok"
            }
        }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let provider = make_provider(
        &state,
        &auth,
        AssistantProviderKind::OpenaiCompatible,
        base_url,
        Some("real-key"),
    );
    let inbound = HeaderMap::from_iter([
        (
            "authorization".parse().unwrap(),
            "Bearer attacker".parse().unwrap(),
        ),
        ("x-api-key".parse().unwrap(), "attacker".parse().unwrap()),
        (
            "cookie".parse().unwrap(),
            "session=attacker".parse().unwrap(),
        ),
    ]);
    let response = send_upstream(
        &state,
        &provider,
        Method::POST,
        "/v1/responses",
        &inbound,
        br#"{"model":"mock"}"#.to_vec(),
    )
    .await
    .unwrap();
    assert!(response.status().is_success());
    let observed = receiver.recv().await.unwrap();
    assert_eq!(observed.headers["authorization"], "Bearer real-key");
    assert_eq!(observed.headers["x-custom"], "custom-value");
    assert!(!observed.headers.contains_key("x-api-key"));
    assert!(!observed.headers.contains_key("cookie"));
    assert_eq!(observed.body, br#"{"model":"mock"}"#[..]);
    handle.abort();
}

#[tokio::test]
async fn injects_anthropic_headers() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let router = Router::new().route(
        "/v1/messages",
        post(move |headers: HeaderMap, body: Bytes| {
            let sender = sender.clone();
            async move {
                sender.send(Observed { headers, body }).unwrap();
                "ok"
            }
        }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let provider = make_provider(
        &state,
        &auth,
        AssistantProviderKind::Anthropic,
        base_url,
        Some("anthropic-key"),
    );
    send_upstream(
        &state,
        &provider,
        Method::POST,
        "/v1/messages",
        &HeaderMap::new(),
        br#"{}"#.to_vec(),
    )
    .await
    .unwrap();
    let observed = receiver.recv().await.unwrap();
    assert_eq!(observed.headers["x-api-key"], "anthropic-key");
    assert_eq!(observed.headers["anthropic-version"], "2023-06-01");
    handle.abort();
}

#[tokio::test]
async fn streams_sse_body() {
    let router = Router::new().route(
        "/v1/responses",
        post(|| async {
            let chunks = stream::iter([
                Ok::<_, std::io::Error>(Bytes::from_static(b"data: one\n\n")),
                Ok(Bytes::from_static(b"data: two\n\n")),
            ]);
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(chunks))
                .unwrap()
        }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let provider = make_provider(
        &state,
        &auth,
        AssistantProviderKind::OpenaiCompatible,
        base_url,
        None,
    );
    let upstream = send_upstream(
        &state,
        &provider,
        Method::POST,
        "/v1/responses",
        &HeaderMap::new(),
        br#"{}"#.to_vec(),
    )
    .await
    .unwrap();
    let response = stream_response(upstream).unwrap();
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body, b"data: one\n\ndata: two\n\n"[..]);
    handle.abort();
}

#[tokio::test]
async fn refreshes_after_401() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let response_attempts = attempts.clone();
    let response_sender = sender.clone();
    let router = Router::new()
        .route(
            "/responses",
            post(move |headers: HeaderMap, body: Bytes| {
                let attempt = response_attempts.fetch_add(1, Ordering::SeqCst);
                let sender = response_sender.clone();
                async move {
                    sender.send(Observed { headers, body }).unwrap();
                    if attempt == 0 {
                        StatusCode::UNAUTHORIZED.into_response()
                    } else {
                        "data: ready\n\n".into_response()
                    }
                }
            }),
        )
        .route(
            "/oauth/token",
            post(|| async { Json(serde_json::json!({"access_token":"new-access"})) }),
        );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state.with_chatgpt_urls(base_url.clone(), base_url.clone()));
    let mut provider = make_provider(
        &state,
        &auth,
        AssistantProviderKind::Chatgpt,
        base_url,
        None,
    );
    let mut secret = provider
        .open_secret(state.credential_encryption_key())
        .unwrap();
    secret.access_token = Some(Secret::new("old-access"));
    secret.refresh_token = Some(Secret::new("refresh-token"));
    secret.account_id = Some(Secret::new("account-id"));
    provider
        .encrypt_secret(state.credential_encryption_key(), &secret)
        .unwrap();
    let provider_id = provider.provider_id.clone();
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
    .unwrap();
    let request = Request::builder()
        .method(Method::POST)
        .uri("/")
        .body(Body::from(br#"{"store":true,"model":"gpt-5"}"#.as_slice()))
        .unwrap();
    let response = proxy_request(
        state,
        Some(auth),
        provider_id,
        "responses".to_string(),
        request,
    )
    .await
    .unwrap();
    assert!(response.status().is_success());
    let first = receiver.recv().await.unwrap();
    let second = receiver.recv().await.unwrap();
    assert_eq!(first.headers["authorization"], "Bearer old-access");
    assert_eq!(second.headers["authorization"], "Bearer new-access");
    for observed in [first, second] {
        assert_eq!(observed.headers["chatgpt-account-id"], "account-id");
        assert!(observed.headers.contains_key("session_id"));
        let value: serde_json::Value = serde_json::from_slice(&observed.body).unwrap();
        assert_eq!(value["store"], false);
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    handle.abort();
}

#[tokio::test]
async fn serializes_token_refresh() {
    // The timeout detects a lost single-flight wakeup, not refresh performance.
    tokio::time::timeout(Duration::from_secs(30), refresh_scenario())
        .await
        .expect("refresh single-flight must not deadlock");
}

async fn refresh_scenario() {
    let refreshes = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let response_refreshes = refreshes.clone();
    let response_entered = entered.clone();
    let response_release = release.clone();
    let router = Router::new().route(
        "/oauth/token",
        post(move || {
            let refreshes = response_refreshes.clone();
            let entered = response_entered.clone();
            let release = response_release.clone();
            async move {
                refreshes.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                release.notified().await;
                Json(serde_json::json!({"access_token":"new-access"}))
            }
        }),
    );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    let state = Arc::new(state.with_chatgpt_urls(base_url.clone(), base_url.clone()));
    let mut provider = make_provider(
        &state,
        &auth,
        AssistantProviderKind::Chatgpt,
        base_url,
        None,
    );
    let mut secret = provider
        .open_secret(state.credential_encryption_key())
        .unwrap();
    secret.access_token = Some(Secret::new("old-access"));
    secret.refresh_token = Some(Secret::new("refresh-token"));
    secret.account_id = Some(Secret::new("account-id"));
    provider.token_obtained_at = Some(0);
    let provider = drive(
        CreateProviderOperation::new(
            provider,
            secret,
            AssistantHeaders(BTreeMap::new()),
            state.credential_encryption_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .unwrap();

    let first_state = state.clone();
    let first_provider = provider.clone();
    let first = tokio::spawn(async move {
        super::super::chatgpt::fresh_provider(&first_state, first_provider).await
    });
    entered.notified().await;
    let second_state = state.clone();
    let second = tokio::spawn(async move {
        super::super::chatgpt::fresh_provider(&second_state, provider).await
    });
    tokio::task::yield_now().await;
    release.notify_waiters();
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();

    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(first.secret, second.secret);
    handle.abort();
}

#[test]
fn tracing_hides_secrets() {
    let proxy = include_str!("../proxy.rs")
        .split("#[cfg(test)]")
        .next()
        .unwrap();
    let chatgpt = include_str!("../chatgpt.rs")
        .split("#[cfg(test)]")
        .next()
        .unwrap();
    let sources = format!("{proxy}{chatgpt}");
    assert!(!sources.contains("tracing::"));
}
