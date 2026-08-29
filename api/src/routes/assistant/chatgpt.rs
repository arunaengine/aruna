use super::{ProviderModel, ensure_enabled, load_provider, save_provider, validate_base_url};
use crate::auth::require_unrestricted_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::compute::Secret;
use aruna_core::credential_seal::SealedS3Secret;
use aruna_core::structs::{
    AssistantHeaders, AssistantProvider, AssistantProviderKind, AssistantProviderSecret,
    AssistantProviderStatus, AuthContext,
};
use aruna_core::util::unix_timestamp_secs;
use aruna_operations::assistant_provider::CreateProviderOperation;
use aruna_operations::driver::drive;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::ToSchema;

const CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const LOGIN_TTL_SECONDS: u64 = 15 * 60;
const REFRESH_AGE_SECONDS: u64 = 8 * 24 * 60 * 60;

#[derive(Debug, Serialize)]
struct DeviceCodeRequest<'a> {
    client_id: &'a str,
}

#[derive(Deserialize)]
struct DeviceCodeResponse {
    device_auth_id: String,
    #[serde(alias = "user_code", alias = "usercode")]
    user_code: String,
    #[serde(default, deserialize_with = "deserialize_interval")]
    interval: u64,
    #[serde(default = "default_login_ttl")]
    expires_in: u64,
}

#[derive(Serialize)]
struct DevicePollRequest<'a> {
    device_auth_id: &'a str,
    user_code: &'a str,
}

#[derive(Deserialize)]
struct DevicePollSuccess {
    authorization_code: String,
    code_verifier: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    id_token: String,
    access_token: String,
    refresh_token: String,
}

#[derive(Serialize)]
struct RefreshRequest<'a> {
    client_id: &'a str,
    grant_type: &'a str,
    refresh_token: &'a str,
}

#[derive(Deserialize)]
struct RefreshResponse {
    id_token: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ChatgptLoginResponse {
    pub provider_id: String,
    pub user_code: String,
    pub verification_url: String,
    pub interval_seconds: u64,
    #[schema(example = "2026-04-09T12:00:00Z")]
    pub expires_at: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct StartLoginRequest {
    pub label: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct LoginPollResponse {
    pub status: String,
}

fn default_login_ttl() -> u64 {
    LOGIN_TTL_SECONDS
}

fn deserialize_interval<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(text) => text.parse().map_err(serde::de::Error::custom),
        serde_json::Value::Number(number) => number
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("invalid interval")),
        _ => Ok(5),
    }
}

fn extract_account_id(id_token: &str) -> Option<String> {
    let payload = id_token.split('.').nth(1)?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let value: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    value
        .get("https://api.openai.com/auth")?
        .get("chatgpt_account_id")?
        .as_str()
        .map(ToOwned::to_owned)
}

pub(super) fn static_models() -> Vec<ProviderModel> {
    [
        "gpt-5.6-sol",
        "gpt-5.6-luna",
        "gpt-5.5",
        "gpt-5.4",
        "gpt-5.3-codex",
        "gpt-5",
    ]
    .into_iter()
    .map(|id| ProviderModel {
        id: id.to_string(),
        display_name: None,
        static_model: true,
    })
    .collect()
}

async fn exchange_tokens(
    state: &ServerState,
    success: DevicePollSuccess,
) -> ServerResult<TokenResponse> {
    let redirect_uri = format!("{}/deviceauth/callback", state.chatgpt_issuer());
    let body = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("grant_type", "authorization_code")
        .append_pair("code", &success.authorization_code)
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("client_id", CODEX_CLIENT_ID)
        .append_pair("code_verifier", &success.code_verifier)
        .finish();
    let response = state
        .assistant_client()
        .ok_or_else(|| ServerError::InternalError("assistant client unavailable".to_string()))?
        .post(format!("{}/oauth/token", state.chatgpt_issuer()))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|_| ServerError::BadGatewayReason("ChatGPT login unavailable".to_string()))?;
    if !response.status().is_success() {
        return Err(ServerError::BadGatewayReason(
            "ChatGPT token exchange failed".to_string(),
        ));
    }
    response
        .json()
        .await
        .map_err(|_| ServerError::BadGatewayReason("ChatGPT token response is invalid".to_string()))
}

#[utoipa::path(
    post,
    path = "/users/assistant/providers/chatgpt/login",
    tag = "assistant",
    summary = "Start ChatGPT device login",
    request_body(content = StartLoginRequest, description = "Optional provider label"),
    responses(
        (status = 201, body = ChatgptLoginResponse),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse),
        (status = 404, description = "Assistant proxy disabled", body = ErrorResponse),
        (status = 502, body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_login(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    body: Option<Json<StartLoginRequest>>,
) -> ServerResult<(StatusCode, Json<ChatgptLoginResponse>)> {
    ensure_enabled(&state)?;
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let label = body
        .and_then(|Json(request)| request.label)
        .map(|label| label.trim().to_string())
        .filter(|label| !label.is_empty())
        .unwrap_or_else(|| "ChatGPT".to_string());
    let response = state
        .assistant_client()
        .ok_or_else(|| ServerError::InternalError("assistant client unavailable".to_string()))?
        .post(format!(
            "{}/api/accounts/deviceauth/usercode",
            state.chatgpt_issuer()
        ))
        .json(&DeviceCodeRequest {
            client_id: CODEX_CLIENT_ID,
        })
        .send()
        .await
        .map_err(|_| ServerError::BadGatewayReason("ChatGPT login unavailable".to_string()))?;
    if !response.status().is_success() {
        return Err(ServerError::BadGatewayReason(
            "ChatGPT device login failed".to_string(),
        ));
    }
    let device: DeviceCodeResponse = response.json().await.map_err(|_| {
        ServerError::BadGatewayReason("ChatGPT device response is invalid".to_string())
    })?;
    let now = unix_timestamp_secs();
    let expires_at = now.saturating_add(device.expires_in.max(1));
    let interval = device.interval.max(1);
    let provider_id = Ulid::generate().to_string();
    let base_url = validate_base_url(&state, state.chatgpt_base_url())?;
    let provider = AssistantProvider {
        provider_id: provider_id.clone(),
        user_id: auth.user_id,
        kind: AssistantProviderKind::Chatgpt,
        label,
        base_url,
        headers: SealedS3Secret::empty(),
        secret: SealedS3Secret::empty(),
        models: static_models().into_iter().map(|model| model.id).collect(),
        default_model: Some("gpt-5.6-sol".to_string()),
        created_at: now,
        status: AssistantProviderStatus::PendingLogin,
        token_obtained_at: None,
        login_expires_at: Some(expires_at),
        login_interval_seconds: Some(interval),
    };
    let secret = AssistantProviderSecret {
        device_auth_id: Some(Secret::new(device.device_auth_id)),
        user_code: Some(Secret::new(device.user_code.clone())),
        ..AssistantProviderSecret::empty()
    };
    drive(
        CreateProviderOperation::new(
            provider,
            secret,
            AssistantHeaders(BTreeMap::new()),
            state.credential_seal_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(ChatgptLoginResponse {
            provider_id,
            user_code: device.user_code,
            verification_url: format!("{}/codex/device", state.chatgpt_issuer()),
            interval_seconds: interval,
            expires_at: crate::routes::sessions::unix_rfc3339(expires_at),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/users/assistant/providers/{id}/login/poll",
    tag = "assistant",
    summary = "Poll ChatGPT device login",
    params(("id" = String, Path)),
    responses(
        (status = 200, body = LoginPollResponse),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse),
        (status = 404, body = ErrorResponse),
        (status = 502, body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn poll_login(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(provider_id): Path<String>,
) -> ServerResult<(StatusCode, Json<LoginPollResponse>)> {
    ensure_enabled(&state)?;
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let mut provider = load_provider(&state, auth.user_id, provider_id).await?;
    if provider.kind != AssistantProviderKind::Chatgpt {
        return Err(ServerError::NotFound);
    }
    if provider.status == AssistantProviderStatus::Ready {
        return Ok((
            StatusCode::OK,
            Json(LoginPollResponse {
                status: "ready".to_string(),
            }),
        ));
    }
    let now = unix_timestamp_secs();
    if provider
        .login_expires_at
        .is_some_and(|expiry| expiry <= now)
    {
        return Ok((
            StatusCode::OK,
            Json(LoginPollResponse {
                status: "expired".to_string(),
            }),
        ));
    }
    let mut secret = provider
        .open_secret(state.credential_seal_key())
        .map_err(|_| ServerError::InternalError("provider secret unavailable".to_string()))?;
    let device_auth_id = secret.device_auth_id.as_ref().ok_or_else(|| {
        ServerError::InternalError("ChatGPT pending login is incomplete".to_string())
    })?;
    let user_code = secret.user_code.as_ref().ok_or_else(|| {
        ServerError::InternalError("ChatGPT pending login is incomplete".to_string())
    })?;
    let response = state
        .assistant_client()
        .ok_or_else(|| ServerError::InternalError("assistant client unavailable".to_string()))?
        .post(format!(
            "{}/api/accounts/deviceauth/token",
            state.chatgpt_issuer()
        ))
        .json(&DevicePollRequest {
            device_auth_id: device_auth_id.expose(),
            user_code: user_code.expose(),
        })
        .send()
        .await
        .map_err(|_| ServerError::BadGatewayReason("ChatGPT login unavailable".to_string()))?;
    if matches!(
        response.status(),
        StatusCode::FORBIDDEN | StatusCode::NOT_FOUND
    ) {
        return Ok((
            StatusCode::OK,
            Json(LoginPollResponse {
                status: "pending".to_string(),
            }),
        ));
    }
    if response.status() == StatusCode::BAD_REQUEST {
        let value = response
            .json::<serde_json::Value>()
            .await
            .unwrap_or_default();
        let error = value.get("error").and_then(|value| value.as_str());
        let status = match error {
            Some("access_denied" | "authorization_declined") => "denied",
            Some("expired_token") => "expired",
            _ => "pending",
        };
        return Ok((
            StatusCode::OK,
            Json(LoginPollResponse {
                status: status.to_string(),
            }),
        ));
    }
    if !response.status().is_success() {
        return Err(ServerError::BadGatewayReason(
            "ChatGPT login poll failed".to_string(),
        ));
    }
    let success = response.json::<DevicePollSuccess>().await.map_err(|_| {
        ServerError::BadGatewayReason("ChatGPT login response is invalid".to_string())
    })?;
    let tokens = exchange_tokens(&state, success).await?;
    let account_id = extract_account_id(&tokens.id_token).ok_or_else(|| {
        ServerError::BadGatewayReason("ChatGPT account claim is missing".to_string())
    })?;
    secret.access_token = Some(Secret::new(tokens.access_token));
    secret.refresh_token = Some(Secret::new(tokens.refresh_token));
    secret.account_id = Some(Secret::new(account_id));
    secret.device_auth_id = None;
    secret.user_code = None;
    provider.status = AssistantProviderStatus::Ready;
    provider.token_obtained_at = Some(now);
    provider.login_expires_at = None;
    provider.login_interval_seconds = None;
    provider
        .seal_secret(state.credential_seal_key(), &secret)
        .map_err(|_| ServerError::InternalError("provider seal failed".to_string()))?;
    save_provider(&state, auth.user_id, provider).await?;
    Ok((
        StatusCode::OK,
        Json(LoginPollResponse {
            status: "ready".to_string(),
        }),
    ))
}

pub(super) async fn refresh_provider(
    state: &ServerState,
    mut provider: AssistantProvider,
) -> ServerResult<AssistantProvider> {
    let mut secret = provider
        .open_secret(state.credential_seal_key())
        .map_err(|_| ServerError::InternalError("provider secret unavailable".to_string()))?;
    let refresh_token = secret.refresh_token.as_ref().ok_or_else(|| {
        ServerError::BadRequestReason("ChatGPT refresh token is unavailable".to_string())
    })?;
    let response = state
        .assistant_client()
        .ok_or_else(|| ServerError::InternalError("assistant client unavailable".to_string()))?
        .post(format!("{}/oauth/token", state.chatgpt_issuer()))
        .json(&RefreshRequest {
            client_id: CODEX_CLIENT_ID,
            grant_type: "refresh_token",
            refresh_token: refresh_token.expose(),
        })
        .send()
        .await
        .map_err(|_| ServerError::BadGatewayReason("ChatGPT refresh unavailable".to_string()))?;
    if !response.status().is_success() {
        return Err(ServerError::BadGatewayReason(
            "ChatGPT refresh failed".to_string(),
        ));
    }
    let refresh: RefreshResponse = response.json().await.map_err(|_| {
        ServerError::BadGatewayReason("ChatGPT refresh response is invalid".to_string())
    })?;
    if let Some(id_token) = refresh.id_token
        && let Some(account_id) = extract_account_id(&id_token)
    {
        secret.account_id = Some(Secret::new(account_id));
    }
    if let Some(access_token) = refresh.access_token {
        secret.access_token = Some(Secret::new(access_token));
    }
    if let Some(refresh_token) = refresh.refresh_token {
        secret.refresh_token = Some(Secret::new(refresh_token));
    }
    provider.token_obtained_at = Some(unix_timestamp_secs());
    provider
        .seal_secret(state.credential_seal_key(), &secret)
        .map_err(|_| ServerError::InternalError("provider seal failed".to_string()))?;
    save_provider(state, provider.user_id, provider).await
}

pub(super) async fn fresh_provider(
    state: &ServerState,
    provider: AssistantProvider,
) -> ServerResult<AssistantProvider> {
    if provider.status != AssistantProviderStatus::Ready {
        return Err(ServerError::BadRequestReason(
            "ChatGPT login is not ready".to_string(),
        ));
    }
    let now = unix_timestamp_secs();
    if provider
        .token_obtained_at
        .is_none_or(|obtained| now.saturating_sub(obtained) >= REFRESH_AGE_SECONDS)
    {
        refresh_provider(state, provider).await
    } else {
        Ok(provider)
    }
}

#[cfg(test)]
mod tests {
    use super::extract_account_id;

    #[test]
    fn extracts_account_id() {
        let token = "aaa.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjdF8xIn19.bbb";
        assert_eq!(extract_account_id(token).as_deref(), Some("acct_1"));
    }
}
