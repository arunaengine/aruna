mod chatgpt;
mod chats;
mod proxy;
#[cfg(test)]
mod test_support;

use chatgpt::{__path_poll_login, __path_start_login, poll_login, start_login};
use chats::{__path_get_chats, __path_put_chats, get_chats, put_chats};
use proxy::{__path_proxy_get, __path_proxy_post, proxy_get, proxy_post};

use crate::auth::require_unrestricted_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::compute::Secret;
use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::structs::{
    AssistantHeaders, AssistantProvider, AssistantProviderKind, AssistantProviderSecret,
    AssistantProviderStatus, AuthContext,
};
use aruna_core::util::unix_timestamp_secs;
use aruna_operations::assistant_provider::{
    CreateProviderOperation, DeleteProviderOperation, GetProviderOperation, ListProviderOperation,
    ProviderStoreError, UpdateProviderOperation,
};
use aruna_operations::driver::drive;
use axum::body::to_bytes;
use axum::extract::Request;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::sync::Arc;
use ulid::Ulid;
use url::{Host, Url};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

pub(super) const PROXY_BODY_LIMIT: usize = 4 * 1024 * 1024;

#[derive(OpenApi)]
#[openapi(tags((name = "system/assistant", description = "Assistant provider operations")))]
pub struct AssistantApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    let router = OpenApiRouter::with_openapi(AssistantApiDoc::openapi())
        .routes(routes!(list_providers, create_provider))
        .routes(routes!(patch_provider, delete_provider))
        .routes(routes!(get_models))
        .routes(routes!(test_provider))
        .routes(routes!(start_login))
        .routes(routes!(poll_login))
        .routes(routes!(get_chats, put_chats));
    super::routes_at(
        router,
        "/system/assistant/providers/{id}/proxy/{*path}",
        routes!(proxy_post, proxy_get),
    )
    .layer(DefaultBodyLimit::max(PROXY_BODY_LIMIT))
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub struct CreateProviderRequest {
    #[schema(example = "openai")]
    pub kind: String,
    pub label: String,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub headers: Option<BTreeMap<String, String>>,
    pub models: Option<Vec<ModelInput>>,
    pub default_model: Option<String>,
}

/// A model as the portal sends it back after a fetch; only the id is kept.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ModelInput {
    pub id: String,
    pub display_name: Option<String>,
}

fn model_ids(models: Vec<ModelInput>) -> Vec<String> {
    models.into_iter().map(|model| model.id).collect()
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub struct PatchProviderRequest {
    pub label: Option<String>,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub headers: Option<BTreeMap<String, String>>,
    pub models: Option<Vec<ModelInput>>,
    pub default_model: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ProviderSummary {
    pub provider_id: String,
    pub kind: String,
    pub label: String,
    pub base_url: String,
    pub models: Vec<ProviderModel>,
    pub default_model: Option<String>,
    #[schema(example = "2026-04-09T12:00:00Z")]
    pub created_at: String,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ListProvidersResponse {
    pub providers: Vec<ProviderSummary>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ProviderModel {
    pub id: String,
    pub display_name: Option<String>,
    #[serde(rename = "static")]
    pub static_model: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasoning_efforts: Vec<String>,
}

/// Reasoning levels a model family accepts; empty leaves the client its own
/// fallback. Codex gpt-5.6 flagships add max and ultra, older gpt-5.x stop at
/// xhigh, generic OpenAI models take three, Anthropic maps to off/low/medium/high.
pub(super) fn reasoning_efforts(kind: AssistantProviderKind, id: &str) -> Vec<String> {
    let levels: &[&str] = match kind {
        AssistantProviderKind::Chatgpt if id.starts_with("gpt-5.6") => {
            &["minimal", "low", "medium", "high", "xhigh", "max", "ultra"]
        }
        AssistantProviderKind::Chatgpt if id.starts_with("gpt-5") => {
            &["minimal", "low", "medium", "high", "xhigh"]
        }
        AssistantProviderKind::Chatgpt => &[],
        // Haiku models have no extended thinking.
        AssistantProviderKind::Anthropic if id.contains("claude") && !id.contains("haiku") => {
            &["off", "low", "medium", "high"]
        }
        AssistantProviderKind::Anthropic => &[],
        _ if is_openai_reasoning(id) => &["low", "medium", "high"],
        _ => &[],
    };
    levels.iter().map(|level| level.to_string()).collect()
}

fn is_openai_reasoning(id: &str) -> bool {
    id.starts_with("o3") || id.starts_with("o4") || id.starts_with("gpt-5")
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ProviderModelsResponse {
    pub models: Vec<ProviderModel>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ProviderTestResponse {
    pub ok: bool,
    pub message: String,
}

pub(super) fn ensure_enabled(state: &ServerState) -> ServerResult<()> {
    if state.assistant_proxy() {
        Ok(())
    } else {
        Err(ServerError::FeatureDisabled("assistant_proxy_disabled"))
    }
}

fn parse_provider_kind(kind: &str) -> ServerResult<AssistantProviderKind> {
    match kind {
        "anthropic" => Ok(AssistantProviderKind::Anthropic),
        "openai" => Ok(AssistantProviderKind::Openai),
        "openrouter" => Ok(AssistantProviderKind::Openrouter),
        "openai_compatible" => Ok(AssistantProviderKind::OpenaiCompatible),
        "chatgpt" => Ok(AssistantProviderKind::Chatgpt),
        _ => Err(ServerError::BadRequestReason(
            "unknown assistant provider kind".to_string(),
        )),
    }
}

pub(super) fn validate_base_url(state: &ServerState, input: &str) -> ServerResult<String> {
    validate_url_mode(input, state.is_user_node())
}

fn validate_url_mode(input: &str, user_node: bool) -> ServerResult<String> {
    let url = Url::parse(input)
        .map_err(|_| ServerError::BadRequestReason("base_url is invalid".to_string()))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ServerError::BadRequestReason(
            "base_url must be an HTTP origin without credentials, query, or fragment".to_string(),
        ));
    }
    if !user_node {
        if url.scheme() != "https" {
            return Err(ServerError::BadRequestReason(
                "server nodes require an https public base_url".to_string(),
            ));
        }
        match url.host() {
            Some(Host::Domain(host)) => {
                let host = host.to_ascii_lowercase();
                if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") {
                    return Err(ServerError::BadRequestReason(
                        "server nodes require a public base_url host".to_string(),
                    ));
                }
            }
            Some(Host::Ipv4(address)) if crate::server_state::public_address(address.into()) => {}
            Some(Host::Ipv6(address)) if crate::server_state::public_address(address.into()) => {}
            _ => {
                return Err(ServerError::BadRequestReason(
                    "server nodes require a public base_url host".to_string(),
                ));
            }
        }
    }
    Ok(input.trim_end_matches('/').to_string())
}

fn forbidden_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "authorization"
            | "x-api-key"
            | "anthropic-version"
            | "chatgpt-account-id"
            | "session_id"
            | "cookie"
            | "host"
            | "connection"
            | "keep-alive"
            | "proxy-connection"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
    )
}

fn headers_from_input(input: BTreeMap<String, String>) -> ServerResult<AssistantHeaders> {
    if input.len() > 64 {
        return Err(ServerError::BadRequestReason(
            "at most 64 custom headers are allowed".to_string(),
        ));
    }
    let mut headers = BTreeMap::new();
    for (name, value) in input {
        let parsed_name = HeaderName::try_from(name.as_str()).map_err(|_| {
            ServerError::BadRequestReason("custom header name is invalid".to_string())
        })?;
        HeaderValue::try_from(value.as_str()).map_err(|_| {
            ServerError::BadRequestReason("custom header value is invalid".to_string())
        })?;
        if forbidden_header(&parsed_name) {
            return Err(ServerError::BadRequestReason(
                "custom authentication and hop-by-hop headers are not allowed".to_string(),
            ));
        }
        headers.insert(parsed_name.as_str().to_string(), Secret::new(value));
    }
    Ok(AssistantHeaders(headers))
}

async fn parse_json<T: DeserializeOwned>(request: Request) -> ServerResult<T> {
    let body = to_bytes(request.into_body(), PROXY_BODY_LIMIT)
        .await
        .map_err(|_| ServerError::PayloadTooLarge("request body exceeds 4 MiB".to_string()))?;
    serde_json::from_slice(&body)
        .map_err(|_| ServerError::BadRequestReason("request body is invalid".to_string()))
}

pub(super) fn provider_summary(provider: &AssistantProvider) -> ProviderSummary {
    ProviderSummary {
        provider_id: provider.provider_id.clone(),
        kind: provider.kind.to_string(),
        label: provider.label.clone(),
        base_url: provider.base_url.clone(),
        models: provider
            .models
            .iter()
            .map(|id| ProviderModel {
                reasoning_efforts: reasoning_efforts(provider.kind, id),
                id: id.clone(),
                display_name: None,
                static_model: provider.kind == AssistantProviderKind::Chatgpt,
            })
            .collect(),
        default_model: provider.default_model.clone(),
        created_at: crate::routes::sessions::unix_rfc3339(provider.created_at),
        status: provider.status.to_string(),
    }
}

pub(super) async fn load_provider(
    state: &ServerState,
    user_id: aruna_core::UserId,
    provider_id: String,
) -> ServerResult<AssistantProvider> {
    drive(
        GetProviderOperation::new(provider_id, user_id),
        &state.get_ctx(),
    )
    .await
    .map_err(map_store_error)?
    .ok_or(ServerError::NotFound)
}

pub(super) async fn save_provider(
    state: &ServerState,
    user_id: aruna_core::UserId,
    expected: AssistantProvider,
    provider: AssistantProvider,
) -> ServerResult<AssistantProvider> {
    drive(
        UpdateProviderOperation::new(provider, expected, user_id),
        &state.get_ctx(),
    )
    .await
    .map_err(map_store_error)
}

fn map_store_error(error: ProviderStoreError) -> ServerError {
    match error {
        ProviderStoreError::NotFound => ServerError::NotFound,
        ProviderStoreError::IdCollision => {
            ServerError::Conflict("provider id collision".to_string())
        }
        ProviderStoreError::Stale => {
            ServerError::Conflict("provider changed concurrently".to_string())
        }
        error => ServerError::InternalError(error.to_string()),
    }
}

fn provider_base_url(
    state: &ServerState,
    kind: AssistantProviderKind,
    requested: Option<String>,
) -> ServerResult<String> {
    let base_url = match (kind, requested) {
        (AssistantProviderKind::OpenaiCompatible, None) => {
            return Err(ServerError::BadRequestReason(
                "openai_compatible requires base_url".to_string(),
            ));
        }
        (_, Some(base_url)) => base_url,
        (_, None) => kind
            .default_base_url()
            .ok_or_else(|| ServerError::BadRequestReason("base_url is required".to_string()))?
            .to_string(),
    };
    validate_base_url(state, &base_url)
}

#[utoipa::path(
    get,
    path = "/system/assistant/providers",
    tag = "system/assistant",
    summary = "List assistant providers",
    description = r#"Lists the assistant providers the calling user registered on this node.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Providers are self-scoped, so a caller reaches only their own.

**Behavior**
- Providers are stored per user on the node that registered them, so the listing is node-local and
  is not replicated to the realm's other nodes.
- API keys, ChatGPT tokens and custom headers stay encrypted: a summary carries only the label, the
  base URL, the known model ids and the status.
- `status` is `ready` once the provider can serve a request, and `pending` while a ChatGPT device
  login is still open.
- A model marked `static` is one this node knows without asking the provider.
- The listing is not paged; a user holds few providers by construction."#,
    responses(
        (status = 200, description = "The user's providers on this node", body = ListProvidersResponse,
            example = json!({
                "providers": [{
                    "provider_id": "01JCNCTR0123456789ABCDEFGH",
                    "kind": "openai",
                    "label": "OpenAI",
                    "base_url": "https://api.openai.com",
                    "models": [{"id": "gpt-5", "static": false, "reasoning_efforts": ["low", "medium", "high"]}],
                    "default_model": "gpt-5",
                    "created_at": "2026-04-09T12:00:00Z",
                    "status": "ready"
                }]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "Assistant proxy disabled", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_providers(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<ListProvidersResponse>)> {
    ensure_enabled(&state)?;
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let providers = drive(ListProviderOperation::new(auth.user_id), &state.get_ctx())
        .await
        .map_err(map_store_error)?
        .iter()
        .map(provider_summary)
        .collect();
    Ok((StatusCode::OK, Json(ListProvidersResponse { providers })))
}

#[utoipa::path(
    post,
    path = "/system/assistant/providers",
    tag = "system/assistant",
    summary = "Create an assistant provider",
    description = r#"Registers an assistant provider for the calling user and encrypts its credentials.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. The provider is registered for the calling user alone.

**Behavior**
- `kind` picks the wire dialect: `anthropic`, `openai`, `openrouter` or `openai_compatible`.
  `chatgpt` is refused here because that kind is registered by the device login route.
- `base_url` defaults to the kind's official origin and is required for `openai_compatible`; a
  trailing slash is trimmed off.
- `api_key` and `headers` are stored encrypted and are never returned by any route.
- `models` keeps only the ids the portal sends back; display names are dropped.
- The record is written on the node serving the request and is not replicated to the realm.

**Limits**
- The request body is capped at 4 MiB.
- At most 64 custom headers are accepted, and authentication and hop-by-hop header names are
  refused among them.
- `base_url` must be an HTTP origin without credentials, query or fragment. A server node also
  requires https and a public host, while a user node may name a loopback or private address."#,
    request_body(
        content = CreateProviderRequest,
        description = "Provider kind and label, the credentials to encrypt, and the models the portal already fetched",
        example = json!({
            "kind": "openai",
            "label": "OpenAI",
            "api_key": "EXAMPLE-API-KEY-PLACEHOLDER",
            "models": [{"id": "gpt-5"}],
            "default_model": "gpt-5"
        })
    ),
    responses(
        (status = 201, description = "Provider registered; the encrypted credentials are not echoed back", body = ProviderSummary,
            example = json!({
                "provider_id": "01JCNCTR0123456789ABCDEFGH",
                "kind": "openai",
                "label": "OpenAI",
                "base_url": "https://api.openai.com",
                "models": [{"id": "gpt-5", "static": false, "reasoning_efforts": ["low", "medium", "high"]}],
                "default_model": "gpt-5",
                "created_at": "2026-04-09T12:00:00Z",
                "status": "ready"
            })),
        (status = 400, description = "Unknown kind, missing or invalid base URL, or a refused custom header", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "Assistant proxy disabled", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_provider(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    request: Request,
) -> ServerResult<(StatusCode, Json<ProviderSummary>)> {
    ensure_enabled(&state)?;
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let request: CreateProviderRequest = parse_json(request).await?;
    let kind = parse_provider_kind(&request.kind)?;
    if kind == AssistantProviderKind::Chatgpt {
        return Err(ServerError::BadRequestReason(
            "use the ChatGPT login route".to_string(),
        ));
    }
    let base_url = provider_base_url(&state, kind, request.base_url)?;
    let provider = AssistantProvider {
        provider_id: Ulid::generate().to_string(),
        user_id: auth.user_id,
        kind,
        label: request.label,
        base_url,
        headers: EncryptedS3Secret::empty(),
        secret: EncryptedS3Secret::empty(),
        models: request.models.map(model_ids).unwrap_or_default(),
        default_model: request.default_model,
        created_at: unix_timestamp_secs(),
        status: AssistantProviderStatus::Ready,
        token_obtained_at: None,
        login_expires_at: None,
        login_interval_seconds: None,
    };
    let secret = AssistantProviderSecret {
        api_key: request.api_key.map(Secret::new),
        ..AssistantProviderSecret::empty()
    };
    let headers = headers_from_input(request.headers.unwrap_or_default())?;
    let provider = drive(
        CreateProviderOperation::new(
            provider,
            secret,
            headers,
            state.credential_encryption_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_store_error)?;
    Ok((StatusCode::CREATED, Json(provider_summary(&provider))))
}

#[utoipa::path(
    patch,
    path = "/system/assistant/providers/{id}",
    tag = "system/assistant",
    summary = "Update an assistant provider",
    description = r#"Updates one assistant provider, changing only the fields the request carries.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Providers are self-scoped, so a caller reaches only their own.

**Behavior**
- An omitted field keeps its stored value; `api_key` replaces the encrypted key and `headers` replaces
  the whole stored header set rather than merging into it.
- A new `base_url` is validated exactly as it is at registration, so a server node keeps requiring a
  public https origin.
- The write is conditional on the record this request read, so a concurrent change is refused with
  409 instead of overwriting it.

**Limits**
- The request body is capped at 4 MiB.
- At most 64 custom headers are accepted, and authentication and hop-by-hop header names are
  refused among them."#,
    params(("id" = String, Path, description = "Provider id, as a 26-character ULID")),
    request_body(
        content = PatchProviderRequest,
        description = "The fields to change; an omitted field is left as it is",
        example = json!({
            "label": "OpenAI (team)",
            "default_model": "gpt-5"
        })
    ),
    responses(
        (status = 200, description = "The updated provider", body = ProviderSummary,
            example = json!({
                "provider_id": "01JCNCTR0123456789ABCDEFGH",
                "kind": "openai",
                "label": "OpenAI (team)",
                "base_url": "https://api.openai.com",
                "models": [{"id": "gpt-5", "static": false, "reasoning_efforts": ["low", "medium", "high"]}],
                "default_model": "gpt-5",
                "created_at": "2026-04-09T12:00:00Z",
                "status": "ready"
            })),
        (status = 400, description = "Invalid base URL or a refused custom header", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No such provider for this user, or the assistant proxy is disabled", body = ErrorResponse),
        (status = 409, description = "Provider changed concurrently", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn patch_provider(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(provider_id): Path<String>,
    request: Request,
) -> ServerResult<(StatusCode, Json<ProviderSummary>)> {
    ensure_enabled(&state)?;
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let request: PatchProviderRequest = parse_json(request).await?;
    let mut provider = load_provider(&state, auth.user_id, provider_id).await?;
    let expected = provider.clone();
    if let Some(label) = request.label {
        provider.label = label;
    }
    if let Some(models) = request.models {
        provider.models = model_ids(models);
    }
    if let Some(default_model) = request.default_model {
        provider.default_model = Some(default_model);
    }
    if let Some(base_url) = request.base_url {
        provider.base_url = validate_base_url(&state, &base_url)?;
    }
    let mut secret = provider
        .open_secret(state.credential_encryption_key())
        .map_err(|_| ServerError::InternalError("provider secret unavailable".to_string()))?;
    if let Some(api_key) = request.api_key {
        secret.api_key = Some(Secret::new(api_key));
    }
    let headers = match request.headers {
        Some(headers) => headers_from_input(headers)?,
        None => provider
            .open_headers(state.credential_encryption_key())
            .map_err(|_| ServerError::InternalError("provider headers unavailable".to_string()))?,
    };
    provider
        .encrypt_secret(state.credential_encryption_key(), &secret)
        .and_then(|_| provider.encrypt_headers(state.credential_encryption_key(), &headers))
        .map_err(|_| ServerError::InternalError("provider encryption failed".to_string()))?;
    let provider = save_provider(&state, auth.user_id, expected, provider).await?;
    Ok((StatusCode::OK, Json(provider_summary(&provider))))
}

#[utoipa::path(
    delete,
    path = "/system/assistant/providers/{id}",
    tag = "system/assistant",
    summary = "Delete an assistant provider",
    description = r#"Deletes one assistant provider together with the credentials encrypted for it.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Providers are self-scoped, so a caller reaches only their own.

**Behavior**
- The provider record, its encrypted secret and its encrypted headers are removed in one write.
- The deletion is node-local, like the registration it removes; nothing is revoked upstream, so a
  ChatGPT login stays valid at the issuer until it expires there."#,
    params(("id" = String, Path, description = "Provider id, as a 26-character ULID")),
    responses(
        (status = 204, description = "Provider deleted"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No such provider for this user, or the assistant proxy is disabled", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_provider(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(provider_id): Path<String>,
) -> ServerResult<StatusCode> {
    ensure_enabled(&state)?;
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    drive(
        DeleteProviderOperation::new(provider_id, auth.user_id),
        &state.get_ctx(),
    )
    .await
    .map_err(map_store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/system/assistant/providers/{id}/models",
    tag = "system/assistant",
    summary = "List provider models",
    description = r#"Asks the provider itself which models it serves and returns the text ones.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Providers are self-scoped, so a caller reaches only their own.

**Behavior**
- The node reads the provider's own model listing with the encrypted credentials; the result is
  returned as it is read and is not stored on the provider record.
- Embedding, audio, image, moderation and realtime model ids are filtered out, so only models a
  chat request can use remain.
- A ChatGPT provider refreshes its login first and falls back to the fixed set this node knows when
  the backend list is unusable, because that list is not a published contract.
- A model marked `static` came from that fixed set rather than from the provider."#,
    params(("id" = String, Path, description = "Provider id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The models the provider reports", body = ProviderModelsResponse,
            example = json!({
                "models": [
                    {"id": "gpt-5", "display_name": "GPT-5", "static": false,
                        "reasoning_efforts": ["low", "medium", "high"]},
                    {"id": "gpt-5.4", "static": false,
                        "reasoning_efforts": ["low", "medium", "high"]}
                ]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No such provider for this user, or the assistant proxy is disabled", body = ErrorResponse),
        (status = 409, description = "Provider changed concurrently", body = ErrorResponse),
        (status = 502, description = "The provider was unreachable or answered unusably", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_models(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(provider_id): Path<String>,
) -> ServerResult<(StatusCode, Json<ProviderModelsResponse>)> {
    ensure_enabled(&state)?;
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let provider = load_provider(&state, auth.user_id, provider_id).await?;
    let models = if provider.kind == AssistantProviderKind::Chatgpt {
        // The backend list is not a published contract; the static set covers a refusal.
        let provider = chatgpt::fresh_provider(&state, provider).await?;
        match proxy::fetch_models(&state, &provider).await {
            Ok(models) if !models.is_empty() => models,
            _ => chatgpt::static_models(),
        }
    } else {
        proxy::fetch_models(&state, &provider).await?
    };
    Ok((StatusCode::OK, Json(ProviderModelsResponse { models })))
}

#[utoipa::path(
    post,
    path = "/system/assistant/providers/{id}/test",
    tag = "system/assistant",
    summary = "Test an assistant provider",
    description = r#"Checks whether the stored credentials still reach the provider.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Providers are self-scoped, so a caller reaches only their own.

**Behavior**
- The check is the same model listing the models route performs; a ChatGPT provider is checked by
  refreshing its login instead.
- A failed check answers 200 as well, with `ok` false and a fixed message. The upstream reason is
  deliberately not passed on."#,
    params(("id" = String, Path, description = "Provider id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The verdict of the check, whether or not the provider answered", body = ProviderTestResponse,
            example = json!({"ok": true, "message": "Provider is ready"})),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No such provider for this user, or the assistant proxy is disabled", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn test_provider(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(provider_id): Path<String>,
) -> ServerResult<(StatusCode, Json<ProviderTestResponse>)> {
    ensure_enabled(&state)?;
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let provider = load_provider(&state, auth.user_id, provider_id).await?;
    let result = if provider.kind == AssistantProviderKind::Chatgpt {
        chatgpt::fresh_provider(&state, provider).await.map(|_| ())
    } else {
        proxy::fetch_models(&state, &provider).await.map(|_| ())
    };
    let response = match result {
        Ok(()) => ProviderTestResponse {
            ok: true,
            message: "Provider is ready".to_string(),
        },
        Err(_) => ProviderTestResponse {
            ok: false,
            message: "Provider connection failed".to_string(),
        },
    };
    Ok((StatusCode::OK, Json(response)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::credential_encryption::CredentialEncryptionKey;
    use aruna_core::structs::RealmId;
    use aruna_core::types::UserId;

    #[test]
    fn server_rejects_private() {
        assert!(validate_url_mode("http://127.0.0.1:11434", false).is_err());
        assert!(validate_url_mode("https://localhost", false).is_err());
        assert!(validate_url_mode("https://service.local", false).is_err());
    }

    #[test]
    fn device_accepts_private() {
        assert_eq!(
            validate_url_mode("http://127.0.0.1:11434", true).unwrap(),
            "http://127.0.0.1:11434"
        );
    }

    #[test]
    fn response_hides_secrets() {
        let realm_id = RealmId::from_bytes([3; 32]);
        let mut provider = AssistantProvider {
            provider_id: Ulid::from_bytes([4; 16]).to_string(),
            user_id: UserId::local(Ulid::from_bytes([5; 16]), realm_id),
            kind: AssistantProviderKind::Openai,
            label: "OpenAI".to_string(),
            base_url: "https://api.openai.com".to_string(),
            headers: EncryptedS3Secret::empty(),
            secret: EncryptedS3Secret::empty(),
            models: Vec::new(),
            default_model: None,
            created_at: 1,
            status: AssistantProviderStatus::Ready,
            token_obtained_at: None,
            login_expires_at: None,
            login_interval_seconds: None,
        };
        let key = CredentialEncryptionKey::derive(&[7; 32]);
        provider
            .encrypt_secret(
                &key,
                &AssistantProviderSecret {
                    api_key: Some(Secret::new("secret-key")),
                    account_id: Some(Secret::new("secret-account")),
                    ..AssistantProviderSecret::empty()
                },
            )
            .unwrap();
        let body = serde_json::to_string(&provider_summary(&provider)).unwrap();

        assert!(!body.contains("secret-key"));
        assert!(!body.contains("secret-account"));
    }

    #[test]
    fn efforts_by_family() {
        assert_eq!(
            reasoning_efforts(AssistantProviderKind::Chatgpt, "gpt-5.6-sol"),
            ["minimal", "low", "medium", "high", "xhigh", "max", "ultra"]
        );
        assert_eq!(
            reasoning_efforts(AssistantProviderKind::Chatgpt, "gpt-5.5"),
            ["minimal", "low", "medium", "high", "xhigh"]
        );
        assert_eq!(
            reasoning_efforts(AssistantProviderKind::Openai, "gpt-5"),
            ["low", "medium", "high"]
        );
        assert_eq!(
            reasoning_efforts(AssistantProviderKind::Anthropic, "claude-sonnet-4"),
            ["off", "low", "medium", "high"]
        );
        assert!(reasoning_efforts(AssistantProviderKind::Chatgpt, "o3-mini").is_empty());
        assert!(reasoning_efforts(AssistantProviderKind::Anthropic, "claude-3-5-haiku").is_empty());
    }

    #[test]
    fn efforts_round_trip() {
        let response = ProviderModelsResponse {
            models: vec![
                ProviderModel {
                    id: "gpt-5.6-sol".to_string(),
                    display_name: None,
                    static_model: true,
                    reasoning_efforts: vec!["minimal".to_string(), "xhigh".to_string()],
                },
                ProviderModel {
                    id: "text-embedding-3-small".to_string(),
                    display_name: None,
                    static_model: false,
                    reasoning_efforts: Vec::new(),
                },
            ],
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(
            json["models"][0]["reasoning_efforts"],
            serde_json::json!(["minimal", "xhigh"])
        );
        assert!(json["models"][1].get("reasoning_efforts").is_none());
        let parsed: ProviderModelsResponse = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.models[0].reasoning_efforts, ["minimal", "xhigh"]);
        assert!(parsed.models[1].reasoning_efforts.is_empty());
    }

    #[test]
    fn kind_parsing() {
        assert!(matches!(
            parse_provider_kind("anthropic"),
            Ok(AssistantProviderKind::Anthropic)
        ));
        assert!(matches!(
            parse_provider_kind("openai_compatible"),
            Ok(AssistantProviderKind::OpenaiCompatible)
        ));
        assert!(parse_provider_kind("bogus").is_err());
    }

    #[test]
    fn url_rejects_credentials() {
        assert!(validate_url_mode("http://user:pass@host", true).is_err());
        assert!(validate_url_mode("http://host/?q=1", true).is_err());
        assert!(validate_url_mode("http://host/#frag", true).is_err());
        assert!(validate_url_mode("ftp://host", true).is_err());
        // A server node accepts a public https domain and trims the slash.
        assert_eq!(
            validate_url_mode("https://api.example.com/", false).unwrap(),
            "https://api.example.com"
        );
    }

    #[test]
    fn headers_reject_auth() {
        let ok =
            headers_from_input(BTreeMap::from([("x-trace".to_string(), "1".to_string())])).unwrap();
        assert!(ok.0.contains_key("x-trace"));
        assert!(
            headers_from_input(BTreeMap::from([(
                "authorization".to_string(),
                "Bearer x".to_string()
            )]))
            .is_err()
        );
        assert!(
            headers_from_input(BTreeMap::from([(
                "bad header".to_string(),
                "v".to_string()
            )]))
            .is_err()
        );
        let too_many = (0..65)
            .map(|index| (format!("x-h-{index}"), "v".to_string()))
            .collect();
        assert!(headers_from_input(too_many).is_err());
    }

    #[test]
    fn forbidden_header_names() {
        assert!(forbidden_header(&HeaderName::from_static("authorization")));
        assert!(forbidden_header(&HeaderName::from_static("content-length")));
        assert!(!forbidden_header(&HeaderName::from_static("x-custom")));
    }

    #[test]
    fn store_error_mapping() {
        use aruna_operations::assistant_provider::ProviderStoreError;
        assert!(matches!(
            map_store_error(ProviderStoreError::NotFound),
            ServerError::NotFound
        ));
        assert!(matches!(
            map_store_error(ProviderStoreError::IdCollision),
            ServerError::Conflict(_)
        ));
        assert!(matches!(
            map_store_error(ProviderStoreError::Stale),
            ServerError::Conflict(_)
        ));
        assert!(matches!(
            map_store_error(ProviderStoreError::NotFinished),
            ServerError::InternalError(_)
        ));
    }

    #[test]
    fn drops_display_names() {
        let ids = model_ids(vec![
            ModelInput {
                id: "gpt-5".to_string(),
                display_name: Some("GPT 5".to_string()),
            },
            ModelInput {
                id: "o3".to_string(),
                display_name: None,
            },
        ]);
        assert_eq!(ids, ["gpt-5", "o3"]);
    }
}
