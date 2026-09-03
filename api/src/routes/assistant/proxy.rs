use super::{
    PROXY_BODY_LIMIT, ProviderModel, ensure_enabled, forbidden_header, load_provider,
    validate_base_url,
};
use crate::auth::require_unrestricted_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{AssistantProvider, AssistantProviderKind, AuthContext};
use axum::Extension;
use axum::body::{Body, to_bytes};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;
use futures_util::StreamExt;
use serde::Deserialize;
use std::sync::Arc;

fn normalized_path(path: &str) -> String {
    format!("/{}", path.trim_start_matches('/'))
}

fn allowed_path(kind: AssistantProviderKind, method: &Method, path: &str) -> bool {
    let path = normalized_path(path);
    if method == Method::GET {
        return match kind {
            AssistantProviderKind::Chatgpt => path == "/models",
            _ => path == "/v1/models",
        };
    }
    if method != Method::POST {
        return false;
    }
    match kind {
        AssistantProviderKind::Anthropic => path == "/v1/messages",
        AssistantProviderKind::Openai
        | AssistantProviderKind::Openrouter
        | AssistantProviderKind::OpenaiCompatible => {
            matches!(path.as_str(), "/v1/chat/completions" | "/v1/responses")
        }
        AssistantProviderKind::Chatgpt => path == "/responses",
    }
}

fn sanitize_headers(headers: &HeaderMap) -> HeaderMap {
    let nominated = headers
        .get_all("connection")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    headers
        .iter()
        .filter(|(name, _)| {
            !forbidden_header(name) && !nominated.iter().any(|value| value == name.as_str())
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn force_chatgpt(body: &[u8]) -> ServerResult<Vec<u8>> {
    let mut value: serde_json::Value = serde_json::from_slice(body).map_err(|_| {
        ServerError::BadRequestReason("ChatGPT proxy body must be JSON".to_string())
    })?;
    let object = value.as_object_mut().ok_or_else(|| {
        ServerError::BadRequestReason("ChatGPT proxy body must be a JSON object".to_string())
    })?;
    object.insert("store".to_string(), serde_json::Value::Bool(false));
    serde_json::to_vec(&value)
        .map_err(|_| ServerError::InternalError("proxy JSON encoding failed".to_string()))
}

fn target_url(provider: &AssistantProvider, path: &str) -> String {
    format!(
        "{}{}",
        provider.base_url.trim_end_matches('/'),
        normalized_path(path)
    )
}

fn sensitive_header(value: &str) -> ServerResult<HeaderValue> {
    let mut value = HeaderValue::try_from(value).map_err(|_| {
        ServerError::InternalError("stored provider credential is invalid".to_string())
    })?;
    value.set_sensitive(true);
    Ok(value)
}

async fn send_upstream(
    state: &ServerState,
    provider: &AssistantProvider,
    method: Method,
    path: &str,
    inbound_headers: &HeaderMap,
    body: Vec<u8>,
) -> ServerResult<reqwest::Response> {
    validate_base_url(state, &provider.base_url)?;
    if !allowed_path(provider.kind, &method, path) {
        return Err(ServerError::NotFound);
    }
    let secret = provider
        .open_secret(state.credential_encryption_key())
        .map_err(|_| ServerError::InternalError("provider secret unavailable".to_string()))?;
    let custom_headers = provider
        .open_headers(state.credential_encryption_key())
        .map_err(|_| ServerError::InternalError("provider headers unavailable".to_string()))?;
    let body = if provider.kind == AssistantProviderKind::Chatgpt {
        force_chatgpt(&body)?
    } else {
        body
    };
    let mut request = state
        .assistant_client()
        .ok_or_else(|| ServerError::InternalError("assistant client unavailable".to_string()))?
        .request(method, target_url(provider, path))
        .body(body);
    let sanitized = sanitize_headers(inbound_headers);
    for (name, value) in &sanitized {
        request = request.header(name, value);
    }
    for (name, value) in custom_headers.0 {
        let name = HeaderName::try_from(name.as_str()).map_err(|_| {
            ServerError::InternalError("stored provider header is invalid".to_string())
        })?;
        let mut value = HeaderValue::try_from(value.expose()).map_err(|_| {
            ServerError::InternalError("stored provider header is invalid".to_string())
        })?;
        if !forbidden_header(&name) {
            value.set_sensitive(true);
            request = request.header(name, value);
        }
    }
    match provider.kind {
        AssistantProviderKind::Anthropic => {
            let api_key = secret.api_key.as_ref().ok_or_else(|| {
                ServerError::BadRequestReason("provider credentials are incomplete".to_string())
            })?;
            request = request
                .header("x-api-key", sensitive_header(api_key.expose())?)
                .header("anthropic-version", "2023-06-01");
        }
        AssistantProviderKind::Openai
        | AssistantProviderKind::Openrouter
        | AssistantProviderKind::OpenaiCompatible => {
            if let Some(api_key) = secret.api_key.as_ref() {
                request = request.bearer_auth(api_key.expose());
            }
        }
        AssistantProviderKind::Chatgpt => {
            let access_token = secret.access_token.as_ref().ok_or_else(|| {
                ServerError::BadRequestReason("ChatGPT login is not ready".to_string())
            })?;
            let account_id = secret.account_id.as_ref().ok_or_else(|| {
                ServerError::BadRequestReason("ChatGPT login is not ready".to_string())
            })?;
            request = request
                .bearer_auth(access_token.expose())
                .header("chatgpt-account-id", sensitive_header(account_id.expose())?)
                .header("session_id", uuid::Uuid::new_v4().to_string());
        }
    }
    request
        .send()
        .await
        .map_err(|_| ServerError::BadGatewayReason("assistant upstream unavailable".to_string()))
}

fn response_headers(headers: &HeaderMap) -> HeaderMap {
    sanitize_headers(headers)
}

fn stream_response(response: reqwest::Response) -> ServerResult<Response> {
    let status = response.status();
    let headers = response_headers(response.headers());
    let stream = response
        .bytes_stream()
        .map(|chunk| chunk.map_err(std::io::Error::other));
    let mut output = Response::new(Body::from_stream(stream));
    *output.status_mut() = status;
    *output.headers_mut() = headers;
    Ok(output)
}

async fn proxy_request(
    state: Arc<ServerState>,
    auth: Option<AuthContext>,
    provider_id: String,
    path: String,
    request: Request,
) -> ServerResult<Response> {
    ensure_enabled(&state)?;
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let mut provider = load_provider(&state, auth.user_id, provider_id).await?;
    if !allowed_path(provider.kind, request.method(), &path) {
        return Err(ServerError::NotFound);
    }
    if provider.kind == AssistantProviderKind::Chatgpt {
        provider = super::chatgpt::fresh_provider(&state, provider).await?;
    }
    let method = request.method().clone();
    let headers = request.headers().clone();
    let body = to_bytes(request.into_body(), PROXY_BODY_LIMIT)
        .await
        .map_err(|_| ServerError::PayloadTooLarge("proxy body exceeds 4 MiB".to_string()))?
        .to_vec();
    let response = send_upstream(
        &state,
        &provider,
        method.clone(),
        &path,
        &headers,
        body.clone(),
    )
    .await?;
    if provider.kind == AssistantProviderKind::Chatgpt
        && response.status() == StatusCode::UNAUTHORIZED
    {
        let provider = super::chatgpt::refresh_provider(&state, provider).await?;
        let retry = send_upstream(&state, &provider, method, &path, &headers, body).await?;
        return stream_response(retry);
    }
    stream_response(response)
}

#[utoipa::path(
    post,
    path = "/system/assistant/providers/{id}/proxy/{path}",
    tag = "system/assistant",
    summary = "Proxy a provider request",
    description = r#"Forwards one chat request to the provider and streams its answer back unchanged.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Providers are self-scoped, so a caller reaches only their own.

**Behavior**
- Only the kind's chat path is forwarded: `/v1/messages` for `anthropic`, `/v1/chat/completions` or
  `/v1/responses` for the OpenAI-shaped kinds, and `/responses` for `chatgpt`. Any other path is a
  404, so this is not a general proxy.
- The encrypted credentials are attached here; a caller sends none, and authentication and hop-by-hop
  headers are stripped from the request and from the answer.
- A ChatGPT body is rewritten with `store` set to false, so the conversation is not retained
  upstream.
- A ChatGPT request whose access token expired refreshes the login once and retries; the retry's
  answer is the one returned.
- The upstream status, headers and body are streamed through as they arrive and are never buffered,
  so a streaming answer stays a streaming answer.

**Limits**
- The request body is capped at 4 MiB."#,
    params(
        ("id" = String, Path, description = "Provider id, as a 26-character ULID"),
        ("path" = String, Path, description = "Upstream path to forward, one of the chat paths the provider kind allows")
    ),
    responses(
        (status = 200, description = "Upstream response streamed unchanged"),
        (status = 400, description = "The body is not JSON a ChatGPT request can carry, or the provider credentials are incomplete", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No such provider for this user, a path this kind does not allow, or the assistant proxy is disabled", body = ErrorResponse),
        (status = 409, description = "Provider changed concurrently", body = ErrorResponse),
        (status = 502, description = "The provider was unreachable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn proxy_post(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((provider_id, path)): Path<(String, String)>,
    request: Request,
) -> ServerResult<Response> {
    proxy_request(state, auth, provider_id, path, request).await
}

#[utoipa::path(
    get,
    path = "/system/assistant/providers/{id}/proxy/{path}",
    tag = "system/assistant",
    summary = "Proxy a provider models request",
    description = r#"Forwards the provider's own model listing and streams its answer back unchanged.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Providers are self-scoped, so a caller reaches only their own.

**Behavior**
- Only the kind's model path is forwarded: `/models` for `chatgpt` and `/v1/models` for every other
  kind. Any other path is a 404, so this is not a general proxy.
- The encrypted credentials are attached here; a caller sends none, and authentication and hop-by-hop
  headers are stripped from the request and from the answer.
- A ChatGPT request whose access token expired refreshes the login once and retries; the retry's
  answer is the one returned.
- The answer is the provider's own shape, not the filtered list the models route returns."#,
    params(
        ("id" = String, Path, description = "Provider id, as a 26-character ULID"),
        ("path" = String, Path, description = "Upstream path to forward, the model path the provider kind allows")
    ),
    responses(
        (status = 200, description = "Upstream response streamed unchanged"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No such provider for this user, a path this kind does not allow, or the assistant proxy is disabled", body = ErrorResponse),
        (status = 409, description = "Provider changed concurrently", body = ErrorResponse),
        (status = 502, description = "The provider was unreachable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn proxy_get(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((provider_id, path)): Path<(String, String)>,
    request: Request,
) -> ServerResult<Response> {
    proxy_request(state, auth, provider_id, path, request).await
}

/// `data` is the OpenAI shape; `models` with `slug` is what the ChatGPT backend serves.
#[derive(Deserialize)]
struct ModelList {
    #[serde(default)]
    data: Vec<ModelItem>,
    #[serde(default)]
    models: Vec<ModelItem>,
}

#[derive(Deserialize)]
struct ModelItem {
    #[serde(default, alias = "slug")]
    id: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    supported_parameters: Vec<String>,
}

/// OpenRouter advertises reasoning support per model; the Off/Low/Medium/High
/// set maps to a thinking budget. Other kinds keep the family catalog.
fn model_efforts(kind: AssistantProviderKind, model: &ModelItem) -> Vec<String> {
    if kind == AssistantProviderKind::Openrouter
        && model
            .supported_parameters
            .iter()
            .any(|param| param == "reasoning")
    {
        return ["off", "low", "medium", "high"]
            .iter()
            .map(|level| level.to_string())
            .collect();
    }
    super::reasoning_efforts(kind, &model.id)
}

fn models_path(kind: AssistantProviderKind) -> &'static str {
    match kind {
        AssistantProviderKind::Chatgpt => "/models",
        _ => "/v1/models",
    }
}

fn text_model(id: &str) -> bool {
    let id = id.to_ascii_lowercase();
    ![
        "embed",
        "whisper",
        "tts",
        "image",
        "dall-e",
        "audio",
        "moderation",
        "realtime",
    ]
    .iter()
    .any(|term| id.contains(term))
}

pub(super) async fn fetch_models(
    state: &ServerState,
    provider: &AssistantProvider,
) -> ServerResult<Vec<ProviderModel>> {
    let response = send_upstream(
        state,
        provider,
        Method::GET,
        models_path(provider.kind),
        &HeaderMap::new(),
        Vec::new(),
    )
    .await?;
    if !response.status().is_success() {
        return Err(ServerError::BadGatewayReason(
            "provider models request failed".to_string(),
        ));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| {
            ServerError::BadGatewayReason("provider models response failed".to_string())
        })?;
        if chunk.len() > PROXY_BODY_LIMIT.saturating_sub(body.len()) {
            return Err(ServerError::BadGatewayReason(
                "provider models response exceeds 4 MiB".to_string(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    let payload = serde_json::from_slice::<ModelList>(&body).map_err(|_| {
        ServerError::BadGatewayReason("provider models response is invalid".to_string())
    })?;
    Ok(payload
        .data
        .into_iter()
        .chain(payload.models)
        .filter(|model| !model.id.is_empty() && text_model(&model.id))
        .map(|model| ProviderModel {
            reasoning_efforts: model_efforts(provider.kind, &model),
            id: model.id,
            display_name: model.display_name.or(model.name),
            static_model: false,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{setup_state, spawn_mock};
    use super::*;
    use crate::server_state::ServerState;
    use aruna_core::compute::Secret;
    use aruna_core::credential_encryption::EncryptedS3Secret;
    use aruna_core::structs::{AssistantHeaders, AssistantProviderSecret, AssistantProviderStatus};
    use aruna_operations::assistant_provider::CreateProviderOperation;
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
            created_at: aruna_core::util::unix_timestamp_secs(),
            status: AssistantProviderStatus::Ready,
            token_obtained_at: Some(aruna_core::util::unix_timestamp_secs()),
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
        let proxy = include_str!("proxy.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        let chatgpt = include_str!("chatgpt.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        let sources = format!("{proxy}{chatgpt}");
        assert!(!sources.contains("tracing::"));
    }
}
