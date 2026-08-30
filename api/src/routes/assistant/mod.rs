mod chatgpt;
mod proxy;

use chatgpt::{__path_poll_login, __path_start_login, poll_login, start_login};
use proxy::{__path_proxy_get, __path_proxy_post, proxy_get, proxy_post};

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
#[openapi(tags((name = "assistant", description = "Assistant provider operations")))]
pub struct AssistantApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    let router = OpenApiRouter::with_openapi(AssistantApiDoc::openapi())
        .routes(routes!(list_providers, create_provider))
        .routes(routes!(patch_provider, delete_provider))
        .routes(routes!(get_models))
        .routes(routes!(test_provider))
        .routes(routes!(start_login))
        .routes(routes!(poll_login));
    super::routes_at(
        router,
        "/users/assistant/providers/{id}/proxy/{*path}",
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
    provider: AssistantProvider,
) -> ServerResult<AssistantProvider> {
    drive(
        UpdateProviderOperation::new(provider, user_id),
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
    path = "/users/assistant/providers",
    tag = "assistant",
    summary = "List assistant providers",
    responses(
        (status = 200, body = ListProvidersResponse),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse),
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
    path = "/users/assistant/providers",
    tag = "assistant",
    summary = "Create an assistant provider",
    request_body = CreateProviderRequest,
    responses(
        (status = 201, body = ProviderSummary),
        (status = 400, body = ErrorResponse),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse),
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
        headers: SealedS3Secret::empty(),
        secret: SealedS3Secret::empty(),
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
            state.credential_seal_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_store_error)?;
    Ok((StatusCode::CREATED, Json(provider_summary(&provider))))
}

#[utoipa::path(
    patch,
    path = "/users/assistant/providers/{id}",
    tag = "assistant",
    summary = "Update an assistant provider",
    params(("id" = String, Path)),
    request_body = PatchProviderRequest,
    responses(
        (status = 200, body = ProviderSummary),
        (status = 400, body = ErrorResponse),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse),
        (status = 404, body = ErrorResponse)
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
        .open_secret(state.credential_seal_key())
        .map_err(|_| ServerError::InternalError("provider secret unavailable".to_string()))?;
    if let Some(api_key) = request.api_key {
        secret.api_key = Some(Secret::new(api_key));
    }
    let headers = match request.headers {
        Some(headers) => headers_from_input(headers)?,
        None => provider
            .open_headers(state.credential_seal_key())
            .map_err(|_| ServerError::InternalError("provider headers unavailable".to_string()))?,
    };
    provider
        .seal_secret(state.credential_seal_key(), &secret)
        .and_then(|_| provider.seal_headers(state.credential_seal_key(), &headers))
        .map_err(|_| ServerError::InternalError("provider seal failed".to_string()))?;
    let provider = save_provider(&state, auth.user_id, provider).await?;
    Ok((StatusCode::OK, Json(provider_summary(&provider))))
}

#[utoipa::path(
    delete,
    path = "/users/assistant/providers/{id}",
    tag = "assistant",
    summary = "Delete an assistant provider",
    params(("id" = String, Path)),
    responses(
        (status = 204),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse),
        (status = 404, body = ErrorResponse)
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
    path = "/users/assistant/providers/{id}/models",
    tag = "assistant",
    summary = "List provider models",
    params(("id" = String, Path)),
    responses(
        (status = 200, body = ProviderModelsResponse),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse),
        (status = 404, body = ErrorResponse),
        (status = 502, body = ErrorResponse)
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
    path = "/users/assistant/providers/{id}/test",
    tag = "assistant",
    summary = "Test an assistant provider",
    params(("id" = String, Path)),
    responses(
        (status = 200, body = ProviderTestResponse),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse),
        (status = 404, body = ErrorResponse)
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
    use aruna_core::credential_seal::CredentialSealKey;
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
            headers: SealedS3Secret::empty(),
            secret: SealedS3Secret::empty(),
            models: Vec::new(),
            default_model: None,
            created_at: 1,
            status: AssistantProviderStatus::Ready,
            token_obtained_at: None,
            login_expires_at: None,
            login_interval_seconds: None,
        };
        let key = CredentialSealKey::derive(&[7; 32]);
        provider
            .seal_secret(
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
}
