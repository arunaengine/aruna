use super::*;
use crate::server_state::ServerState;
use crate::tests::assistant::{setup_state, spawn_mock};
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

struct CaptureWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Captures every event formatted on this test's thread until the guard drops.
fn capture_logs() -> (
    std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    tracing::subscriber::DefaultGuard,
) {
    let sink = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = sink.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || CaptureWriter(writer.clone()))
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (sink, guard)
}

/// Real proxy success and failure paths exercise synthetic secrets; neither the
/// captured output nor any formatted error may disclose them, and the refresh
/// path talks to a controlled local issuer instead of a live one.
#[tokio::test]
async fn proxy_redacts_secrets() {
    const API_KEY: &str = "synthetic-api-key-7f3a";
    const ACCESS: &str = "synthetic-access-7f3a";
    const REFRESH: &str = "synthetic-refresh-7f3a";
    const ROTATED_ACCESS: &str = "rotated-access-7f3a";

    let (logs, _guard) = capture_logs();
    // Prove the capture is live before trusting a negative assertion.
    tracing::info!(probe = "capture-active-7f3a", "capture probe");

    // A closed local port for the transport-failure case; never a fixed one.
    let closed = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe port");
        let address = listener.local_addr().expect("probe address");
        drop(listener);
        format!("http://{address}")
    };
    let refresh_attempts = Arc::new(AtomicUsize::new(0));
    let attempts = refresh_attempts.clone();
    let router = Router::new()
        .route("/v1/responses", post(|| async { "ok" }))
        .route(
            "/oauth/token",
            post(move || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Json(serde_json::json!({
                            "access_token": ROTATED_ACCESS,
                            "refresh_token": "rotated-refresh-7f3a",
                        }))
                        .into_response()
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
                }
            }),
        );
    let (base_url, handle) = spawn_mock(router).await;
    let (_dir, state, auth) = setup_state().await;
    // The refresh issuer must be the controlled local server.
    let state = Arc::new(state.with_chatgpt_urls(base_url.clone(), base_url.clone()));
    let provider = make_provider(
        &state,
        &auth,
        AssistantProviderKind::OpenaiCompatible,
        base_url.clone(),
        Some(API_KEY),
    );

    send_upstream(
        &state,
        &provider,
        Method::POST,
        "/v1/responses",
        &HeaderMap::new(),
        br#"{"model":"mock"}"#.to_vec(),
    )
    .await
    .expect("successful proxy path");

    // A transport failure formats an error chain that must stay redacted too.
    let unreachable = make_provider(
        &state,
        &auth,
        AssistantProviderKind::OpenaiCompatible,
        closed,
        Some(ACCESS),
    );
    let error = send_upstream(
        &state,
        &unreachable,
        Method::POST,
        "/v1/responses",
        &HeaderMap::new(),
        br#"{"model":"mock"}"#.to_vec(),
    )
    .await
    .expect_err("unreachable upstream");
    let formatted = format!("{error} {error:?} {provider:?} {unreachable:?}");
    assert!(!formatted.contains(ACCESS), "{formatted}");
    assert!(!formatted.contains(API_KEY), "{formatted}");

    // The refresh path succeeds against the local issuer, and its tokens and
    // the rotated result must not surface in any output.
    let mut provider = make_provider(
        &state,
        &auth,
        AssistantProviderKind::Chatgpt,
        base_url.clone(),
        None,
    );
    let mut secret = provider
        .open_secret(state.credential_encryption_key())
        .unwrap();
    secret.access_token = Some(Secret::new(ACCESS));
    secret.refresh_token = Some(Secret::new(REFRESH));
    provider.token_obtained_at = Some(0);
    let expected = provider.clone();
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
    .expect("the refresh fixture persists");
    let refreshed = super::super::chatgpt::fresh_provider(&state, expected)
        .await
        .expect("the controlled issuer answers the refresh");
    assert_eq!(
        refresh_attempts.load(Ordering::SeqCst),
        1,
        "the refresh request must reach the local issuer"
    );
    let rotated = refreshed
        .open_secret(state.credential_encryption_key())
        .unwrap();
    assert_eq!(
        rotated.access_token.as_ref().map(Secret::expose),
        Some(ROTATED_ACCESS)
    );

    // A refused refresh reports its category and stays redacted.
    let mut failing = make_provider(
        &state,
        &auth,
        AssistantProviderKind::Chatgpt,
        base_url.clone(),
        None,
    );
    let mut secret = failing
        .open_secret(state.credential_encryption_key())
        .unwrap();
    secret.access_token = Some(Secret::new(ACCESS));
    secret.refresh_token = Some(Secret::new(REFRESH));
    failing.token_obtained_at = Some(0);
    let expected = failing.clone();
    drive(
        CreateProviderOperation::new(
            failing,
            secret,
            AssistantHeaders(BTreeMap::new()),
            state.credential_encryption_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .expect("the failing refresh fixture persists");
    let refresh_error = super::super::chatgpt::fresh_provider(&state, expected)
        .await
        .expect_err("the second refresh is refused");
    assert!(
        matches!(&refresh_error, ServerError::BadGatewayReason(message) if message == "ChatGPT refresh failed"),
        "{refresh_error:?}"
    );
    assert_eq!(refresh_attempts.load(Ordering::SeqCst), 2);
    let refresh_formatted = format!("{refresh_error} {refresh_error:?} {refreshed:?}");
    assert!(!refresh_formatted.contains(ACCESS), "{refresh_formatted}");
    assert!(!refresh_formatted.contains(REFRESH), "{refresh_formatted}");
    assert!(
        !refresh_formatted.contains(ROTATED_ACCESS),
        "{refresh_formatted}"
    );

    let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("capture-active-7f3a"), "{logs}");
    for secret in [API_KEY, ACCESS, REFRESH, ROTATED_ACCESS] {
        assert!(!logs.contains(secret), "log disclosed {secret}: {logs}");
    }
    handle.abort();
}

#[test]
fn debug_redacts_secrets() {
    assert_eq!(
        format!("{:?}", Secret::new("synthetic-secret")),
        "Secret(***)"
    );
    assert_eq!(
        format!("{:?}", EncryptedS3Secret::empty()),
        "EncryptedS3Secret(***)"
    );
}
