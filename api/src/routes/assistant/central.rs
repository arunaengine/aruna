//! Owns the realm's central assistant: admin setup, the member view and quota-charged calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::proxy::{allowed_path, fetch_models, send_upstream, stream_response};
use super::{
    ModelInput, PROXY_BODY_LIMIT, ProviderModel, ensure_enabled, forbidden_header, model_ids,
    parse_json, parse_provider_kind, provider_base_url, provider_summary,
};
use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::sessions::unix_rfc3339;
use crate::routes::info::is_realm_admin;
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::compute::Secret;
use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::{
    AssistantHeaders, AssistantProvider, AssistantProviderKind, AssistantProviderSecret,
    AssistantProviderStatus, AssistantUsage, CENTRAL_PROVIDER_ID, CentralAssistant, SESSION_HEADER,
    USER_HEADER, secs_to_midnight, utc_day,
};
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::assistant::central::{
    CentralRead, CentralStoreError, ChargeUsageOperation, GetCentralOperation, SetCentralOperation,
};
use aruna_operations::driver::drive;
use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;
use utoipa::ToSchema;

#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub struct SetCentralRequest {
    #[schema(example = "openai_compatible")]
    pub kind: String,
    pub label: String,
    /// Omitted on a replace to keep the stored key.
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub models: Vec<ModelInput>,
    pub default_model: Option<String>,
    pub daily_limit: Option<u32>,
    pub user_header: Option<String>,
    pub session_header: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CentralResponse {
    pub kind: String,
    pub label: String,
    pub models: Vec<ProviderModel>,
    pub default_model: Option<String>,
    pub daily_limit: Option<u32>,
    pub used_today: u32,
    #[schema(example = "2026-11-03T00:00:00Z")]
    pub resets_at: String,
    /// Set up details, present for realm admins only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin: Option<CentralAdminView>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CentralAdminView {
    pub base_url: String,
    pub holder_node: String,
    pub changed_by: String,
    pub changed_at: String,
    pub user_header: String,
    pub session_header: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CentralTestResponse {
    pub ok: bool,
    pub message: String,
    pub models: Vec<ProviderModel>,
}

fn map_central_error(error: CentralStoreError) -> ServerError {
    match error {
        CentralStoreError::Stale => {
            ServerError::Conflict("central assistant changed concurrently".to_string())
        }
        error => ServerError::InternalError(error.to_string()),
    }
}

async fn load_central(state: &ServerState, user_id: UserId) -> ServerResult<CentralRead> {
    drive(GetCentralOperation::new(user_id), &state.get_ctx())
        .await
        .map_err(map_central_error)
}

async fn require_admin(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = require_unrestricted_auth(state, auth)?;
    if is_realm_admin(state, &auth).await? {
        Ok(auth)
    } else {
        Err(ServerError::Forbidden)
    }
}

fn central_view(
    state: &ServerState,
    central: &CentralAssistant,
    usage: Option<AssistantUsage>,
    admin: bool,
    now: u64,
) -> CentralResponse {
    let summary = provider_summary(&central.provider);
    let admin = admin.then(|| CentralAdminView {
        base_url: central.provider.base_url.clone(),
        holder_node: state.get_node_id().to_string(),
        changed_by: central.provider.user_id.to_string(),
        changed_at: unix_rfc3339(central.changed_at),
        user_header: central.user_header.clone(),
        session_header: central.session_header.clone(),
    });
    CentralResponse {
        kind: summary.kind,
        label: summary.label,
        models: summary.models,
        default_model: summary.default_model,
        daily_limit: central.daily_limit,
        used_today: AssistantUsage::used_on(usage, utc_day(now)),
        resets_at: unix_rfc3339(now + secs_to_midnight(now)),
        admin,
    }
}

fn header_name(input: Option<String>, default: &str) -> ServerResult<String> {
    let input = input.unwrap_or_else(|| default.to_string());
    let name = HeaderName::try_from(input.as_str()).map_err(|_| {
        ServerError::BadRequestReason("identity header name is invalid".to_string())
    })?;
    if forbidden_header(&name) {
        return Err(ServerError::BadRequestReason(
            "identity header names must not be authentication or hop-by-hop headers".to_string(),
        ));
    }
    Ok(name.as_str().to_string())
}

/// Opens the stored key, or the key of the replaced setup when the request carries none.
fn central_secret(
    state: &ServerState,
    api_key: Option<String>,
    current: Option<&CentralAssistant>,
) -> ServerResult<AssistantProviderSecret> {
    if let Some(api_key) = api_key {
        return Ok(AssistantProviderSecret {
            api_key: Some(Secret::new(api_key)),
            ..AssistantProviderSecret::empty()
        });
    }
    match current {
        Some(current) => current
            .provider
            .open_secret(state.credential_encryption_key())
            .map_err(|_| ServerError::InternalError("provider secret unavailable".to_string())),
        None => Ok(AssistantProviderSecret::empty()),
    }
}

fn build_central(
    state: &ServerState,
    auth: &AuthContext,
    request: SetCentralRequest,
    current: Option<&CentralAssistant>,
) -> ServerResult<CentralAssistant> {
    let kind = parse_provider_kind(&request.kind)?;
    if kind == AssistantProviderKind::Chatgpt {
        return Err(ServerError::BadRequestReason(
            "a ChatGPT login cannot be the central assistant".to_string(),
        ));
    }
    let models = model_ids(request.models);
    if let Some(default) = &request.default_model
        && !models.contains(default)
    {
        return Err(ServerError::BadRequestReason(
            "default_model must be one of the allowed models".to_string(),
        ));
    }
    let now = unix_timestamp_secs();
    let mut provider = AssistantProvider {
        provider_id: CENTRAL_PROVIDER_ID.to_string(),
        user_id: auth.user_id,
        kind,
        label: request.label,
        base_url: provider_base_url(state, kind, request.base_url)?,
        headers: EncryptedS3Secret::empty(),
        secret: EncryptedS3Secret::empty(),
        models,
        default_model: request.default_model,
        created_at: now,
        status: AssistantProviderStatus::Ready,
        token_obtained_at: None,
        login_expires_at: None,
        login_interval_seconds: None,
    };
    let secret = central_secret(state, request.api_key, current)?;
    let key = state.credential_encryption_key();
    provider
        .encrypt_secret(key, &secret)
        .and_then(|_| provider.encrypt_headers(key, &AssistantHeaders(Default::default())))
        .map_err(|_| ServerError::InternalError("provider encryption failed".to_string()))?;
    Ok(CentralAssistant {
        provider,
        daily_limit: request.daily_limit,
        user_header: header_name(request.user_header, USER_HEADER)?,
        session_header: header_name(request.session_header, SESSION_HEADER)?,
        changed_at: now,
    })
}

fn base_host(central: &CentralAssistant) -> String {
    url::Url::parse(&central.provider.base_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_default()
}

#[utoipa::path(
    get,
    path = "/system/assistant/central",
    tag = "system/assistant",
    summary = "Show the central assistant",
    description = r#"Shows the realm's central assistant and how many calls the caller has left today.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused.

**Behavior**
- The central assistant lives on one holder node, the first management node in realm order; every
  other node passes this call on to it.
- `used_today` counts the caller's model calls on the current UTC day; `resets_at` is the next
  UTC midnight. `daily_limit` is empty when the admin set no limit.
- Realm admins also get `admin` with the base URL, the holder node, who changed the setup last,
  and the identity header names. The API key is never returned."#,
    responses(
        (status = 200, description = "The central assistant and the caller's usage today", body = CentralResponse,
            example = json!({
                "kind": "openai_compatible",
                "label": "Realm assistant",
                "models": [{"id": "deepseek-v4.1", "static": false}],
                "default_model": "deepseek-v4.1",
                "daily_limit": 50,
                "used_today": 12,
                "resets_at": "2026-11-03T00:00:00Z"
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No central assistant is set up, or the assistant proxy is disabled", body = ErrorResponse),
        (status = 503, description = "The holder node is not reachable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_central(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<CentralResponse>)> {
    ensure_enabled(&state)?;
    let auth = require_unrestricted_auth(&state, auth)?;
    let (central, usage) = load_central(&state, auth.user_id).await?;
    let central = central.ok_or(ServerError::NotFound)?;
    let admin = is_realm_admin(&state, &auth).await?;
    let view = central_view(&state, &central, usage, admin, unix_timestamp_secs());
    Ok((StatusCode::OK, Json(view)))
}

#[utoipa::path(
    put,
    path = "/system/assistant/central",
    tag = "system/assistant",
    summary = "Set up the central assistant",
    description = r#"Creates or replaces the realm's central assistant that every member may use.

**Authentication**: realm admin (WRITE on the realm admin config) with an unrestricted token.

**Behavior**
- `kind` is `anthropic`, `openai`, `openrouter` or `openai_compatible`; a ChatGPT login cannot
  be the central assistant. `base_url` is required for `openai_compatible`.
- `api_key` is stored encrypted on the holder node and never returned. On a replace an omitted
  `api_key` keeps the stored key.
- Members may only call the models in `models`; `default_model` must be one of them. A setup
  without models is saved so its models can be loaded, and members cannot call it yet.
- `daily_limit` is the number of model calls per user per UTC day; omitted means no limit.
- Every forwarded call names the user by a stable pseudonymous id in the `user_header` header and
  the request's `user` field, and the chat by the `session_header` header. They default to
  `x-user-id` and `x-session-id`.
- Each change is written to the holder node's audit log with the admin and the base URL host."#,
    request_body(
        content = SetCentralRequest,
        description = "Provider kind, endpoint, key, allowed models and the daily limit",
        example = json!({
            "kind": "openai_compatible",
            "label": "Realm assistant",
            "api_key": "EXAMPLE-API-KEY-PLACEHOLDER",
            "base_url": "https://llm.example.org",
            "models": [{"id": "deepseek-v4.1"}],
            "default_model": "deepseek-v4.1",
            "daily_limit": 50
        })
    ),
    responses(
        (status = 200, description = "The stored setup, without the key", body = CentralResponse,
            example = json!({
                "kind": "openai_compatible",
                "label": "Realm assistant",
                "models": [{"id": "deepseek-v4.1", "static": false}],
                "default_model": "deepseek-v4.1",
                "daily_limit": 50,
                "used_today": 0,
                "resets_at": "2026-11-03T00:00:00Z",
                "admin": {
                    "base_url": "https://llm.example.org",
                    "holder_node": "6b7f5c1d3e9a2b4c8d0e1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e",
                    "changed_by": "01JCNCTR0123456789ABCDEFGH",
                    "changed_at": "2026-11-02T12:00:00Z",
                    "user_header": "x-user-id",
                    "session_header": "x-session-id"
                }
            })),
        (status = 400, description = "Unknown or ChatGPT kind, invalid base URL, or an invalid header name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not a realm admin, or the token carries path restrictions", body = ErrorResponse),
        (status = 404, description = "Assistant proxy disabled", body = ErrorResponse),
        (status = 409, description = "The setup changed concurrently", body = ErrorResponse),
        (status = 503, description = "The holder node is not reachable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_central(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    request: Request,
) -> ServerResult<(StatusCode, Json<CentralResponse>)> {
    ensure_enabled(&state)?;
    let auth = require_admin(&state, auth).await?;
    let request: SetCentralRequest = parse_json(request).await?;
    let (current, usage) = load_central(&state, auth.user_id).await?;
    let central = build_central(&state, &auth, request, current.as_ref())?;
    drive(
        SetCentralOperation::new(Some(central.clone()), current),
        &state.get_ctx(),
    )
    .await
    .map_err(map_central_error)?;
    info!(
        user_id = %auth.user_id,
        action = "set",
        kind = %central.provider.kind,
        base_url_host = %base_host(&central),
        "Central assistant changed"
    );
    let view = central_view(&state, &central, usage, true, central.changed_at);
    Ok((StatusCode::OK, Json(view)))
}

#[utoipa::path(
    delete,
    path = "/system/assistant/central",
    tag = "system/assistant",
    summary = "Remove the central assistant",
    description = r#"Removes the realm's central assistant and its stored key.

**Authentication**: realm admin (WRITE on the realm admin config) with an unrestricted token.

**Behavior**
- Members can no longer call it; their usage rows stay and count again if it is set up later on
  the same day.
- The removal is written to the holder node's audit log."#,
    responses(
        (status = 204, description = "Central assistant removed"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not a realm admin, or the token carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No central assistant is set up, or the assistant proxy is disabled", body = ErrorResponse),
        (status = 409, description = "The setup changed concurrently", body = ErrorResponse),
        (status = 503, description = "The holder node is not reachable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_central(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<StatusCode> {
    ensure_enabled(&state)?;
    let auth = require_admin(&state, auth).await?;
    let (current, _) = load_central(&state, auth.user_id).await?;
    let current = current.ok_or(ServerError::NotFound)?;
    drive(
        SetCentralOperation::new(None, Some(current.clone())),
        &state.get_ctx(),
    )
    .await
    .map_err(map_central_error)?;
    info!(
        user_id = %auth.user_id,
        action = "delete",
        kind = %current.provider.kind,
        base_url_host = %base_host(&current),
        "Central assistant changed"
    );
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/system/assistant/central/test",
    tag = "system/assistant",
    summary = "Test the central assistant",
    description = r#"Checks whether the stored key still reaches the provider and lists its models.

**Authentication**: realm admin (WRITE on the realm admin config) with an unrestricted token.

**Behavior**
- The check reads the provider's own model listing; it does not count against any quota.
- A failed check answers 200 as well, with `ok` false, a fixed message and no models."#,
    responses(
        (status = 200, description = "The verdict of the check and the provider's models", body = CentralTestResponse,
            example = json!({
                "ok": true,
                "message": "Provider is ready",
                "models": [{"id": "deepseek-v4.1", "static": false}]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not a realm admin, or the token carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No central assistant is set up, or the assistant proxy is disabled", body = ErrorResponse),
        (status = 503, description = "The holder node is not reachable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn test_central(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<CentralTestResponse>)> {
    ensure_enabled(&state)?;
    let auth = require_admin(&state, auth).await?;
    let (central, _) = load_central(&state, auth.user_id).await?;
    let central = central.ok_or(ServerError::NotFound)?;
    let response = match fetch_models(&state, &central.provider).await {
        Ok(models) => CentralTestResponse {
            ok: true,
            message: "Provider is ready".to_string(),
            models,
        },
        Err(_) => CentralTestResponse {
            ok: false,
            message: "Provider connection failed".to_string(),
            models: Vec::new(),
        },
    };
    Ok((StatusCode::OK, Json(response)))
}

/// The allowed models a member may pick, without asking the provider.
pub(super) async fn central_models(
    state: &ServerState,
    auth: &AuthContext,
) -> ServerResult<Vec<ProviderModel>> {
    let (central, _) = load_central(state, auth.user_id).await?;
    let central = central.ok_or(ServerError::NotFound)?;
    Ok(provider_summary(&central.provider).models)
}

/// Standard chat request fields a central call may carry. Anything else, such as Open WebUI
/// fields that reach chats, folders, files, skills or terminals of the key owner, is dropped.
const CHAT_FIELDS: &[&str] = &[
    "frequency_penalty",
    "input",
    "instructions",
    "logit_bias",
    "logprobs",
    "max_completion_tokens",
    "max_output_tokens",
    "max_tokens",
    "messages",
    "model",
    "n",
    "parallel_tool_calls",
    "presence_penalty",
    "reasoning",
    "reasoning_effort",
    "response_format",
    "seed",
    "stop",
    "stop_sequences",
    "stream",
    "stream_options",
    "system",
    "temperature",
    "text",
    "thinking",
    "tool_choice",
    "tools",
    "top_k",
    "top_logprobs",
    "top_p",
];

/// Checks the model against the allow-list and names the user and the chat session.
fn central_body(
    central: &CentralAssistant,
    body: &[u8],
    pseudonym: &str,
    session: &str,
) -> ServerResult<Vec<u8>> {
    let mut value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| ServerError::BadRequestReason("request body must be JSON".to_string()))?;
    let object = value.as_object_mut().ok_or_else(|| {
        ServerError::BadRequestReason("request body must be a JSON object".to_string())
    })?;
    let model = object.get("model").and_then(|model| model.as_str());
    if !model.is_some_and(|model| {
        central
            .provider
            .models
            .iter()
            .any(|allowed| allowed == model)
    }) {
        return Err(ServerError::BadRequestReason(
            "model is not allowed for the central assistant".to_string(),
        ));
    }
    object.retain(|field, _| CHAT_FIELDS.contains(&field.as_str()));
    // Open WebUI adds its own server tools to a session whose `tools` is missing or null.
    let compatible = central.provider.kind == AssistantProviderKind::OpenaiCompatible;
    let named = object
        .get("tools")
        .and_then(|tools| tools.as_array())
        .is_some_and(|tools| !tools.is_empty());
    if compatible && named {
        object.insert("session_id".to_string(), session.into());
    }
    let pseudonym = serde_json::Value::String(pseudonym.to_string());
    if central.provider.kind == AssistantProviderKind::Anthropic {
        let metadata = serde_json::json!({ "user_id": pseudonym });
        object.insert("metadata".to_string(), metadata);
    } else {
        object.insert("user".to_string(), pseudonym);
    }
    serde_json::to_vec(&value)
        .map_err(|_| ServerError::InternalError("proxy JSON encoding failed".to_string()))
}

fn session_id(headers: &HeaderMap) -> ServerResult<String> {
    let value = headers
        .get(SESSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            ServerError::BadRequestReason(format!("the {SESSION_HEADER} header is required"))
        })?;
    super::chats::check_chat_id(value.to_string()).map_err(|_| {
        ServerError::BadRequestReason(format!(
            "{SESSION_HEADER} must be 1 to 64 characters from A-Z, a-z, 0-9, _ and -"
        ))
    })
}

fn identity_header(name: &str, value: &str) -> ServerResult<(HeaderName, HeaderValue)> {
    let name = HeaderName::try_from(name)
        .map_err(|_| ServerError::InternalError("stored header name is invalid".to_string()))?;
    let value = HeaderValue::try_from(value)
        .map_err(|_| ServerError::InternalError("identity header value is invalid".to_string()))?;
    Ok((name, value))
}

/// Forwards one central call: allow-list, quota charge, then the pseudonymous identity headers.
pub(super) async fn central_proxy(
    state: &ServerState,
    auth: &AuthContext,
    path: &str,
    request: Request,
) -> ServerResult<Response> {
    let (central, _) = load_central(state, auth.user_id).await?;
    let central = central.ok_or(ServerError::NotFound)?;
    let method = request.method().clone();
    if !allowed_path(central.provider.kind, &method, path) {
        return Err(ServerError::NotFound);
    }
    let pseudonym = state
        .credential_encryption_key()
        .user_pseudonym(&auth.user_id);
    let inbound = request.headers().clone();
    let mut headers = HeaderMap::new();
    for name in [header::CONTENT_TYPE, header::ACCEPT] {
        if let Some(value) = inbound.get(&name) {
            headers.insert(name, value.clone());
        }
    }
    let (name, value) = identity_header(&central.user_header, &pseudonym)?;
    headers.insert(name, value);
    let mut body = Vec::new();
    if method == Method::POST {
        let session = session_id(&inbound)?;
        let raw = to_bytes(request.into_body(), PROXY_BODY_LIMIT)
            .await
            .map_err(|_| ServerError::PayloadTooLarge("proxy body exceeds 4 MiB".to_string()))?;
        body = central_body(&central, &raw, &pseudonym, &session)?;
        let (name, value) = identity_header(&central.session_header, &session)?;
        headers.insert(name, value);
        charge_call(
            state,
            auth.user_id,
            central.daily_limit,
            unix_timestamp_secs(),
        )
        .await?;
    }
    let response = send_upstream(state, &central.provider, method, path, &headers, body).await?;
    stream_response(response)
}

async fn charge_call(
    state: &ServerState,
    user_id: UserId,
    limit: Option<u32>,
    now: u64,
) -> ServerResult<()> {
    loop {
        let operation = ChargeUsageOperation::new(user_id, utc_day(now), limit);
        return match drive(operation, &state.get_ctx()).await {
            Ok(_) => Ok(()),
            // Another call of this user committed first; charge again against its count.
            Err(CentralStoreError::Storage(StorageError::TransactionConflict)) => continue,
            Err(CentralStoreError::QuotaReached(reached)) => Err(ServerError::AssistantQuota {
                used: reached.used,
                limit: reached.limit,
                retry_after: secs_to_midnight(now),
            }),
            Err(error) => Err(map_central_error(error)),
        };
    }
}

#[cfg(test)]
#[path = "central_tests.rs"]
mod tests;
