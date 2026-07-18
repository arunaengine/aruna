use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::auth::{parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::errors::AuthorizationError;
use aruna_core::errors::SourceConnectorResolutionError;
use aruna_core::structs::{
    AuthContext, Permission, ResolvedSourceAccess, SourceConnector, SourceConnectorKind,
    SourceEntryKind,
};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::connectors::create_source_connector::{
    CreateSourceConnectorError, CreateSourceConnectorInput, CreateSourceConnectorOperation,
};
use aruna_operations::connectors::delete_source_connector::{
    DeleteSourceConnectorError, DeleteSourceConnectorInput, DeleteSourceConnectorOperation,
};
use aruna_operations::connectors::get_source_connector::{
    GetSourceConnectorError, GetSourceConnectorInput, GetSourceConnectorOperation,
};
use aruna_operations::connectors::has_secret_config::{
    ConnectorHasSecretConfigError, ConnectorHasSecretConfigOperation,
};
use aruna_operations::connectors::list_source_connectors::{
    ListSourceConnectorsError, ListSourceConnectorsInput, ListSourceConnectorsOperation,
};
use aruna_operations::connectors::replace_source_connector::{
    ReplaceSourceConnectorError, ReplaceSourceConnectorInput, ReplaceSourceConnectorOperation,
};
use aruna_operations::connectors::resolver::{resolve_inline_access, validate_source_path};
use aruna_operations::connectors::validation::validate_connector_input;
use aruna_operations::connectors::{ResolveSourceConnectorInput, ResolveSourceConnectorOperation};
use aruna_operations::driver::drive;
use aruna_operations::staging::check_source::CheckStagingSourceOperation;
use aruna_operations::staging::list_source::{
    ListStagingSourceError, ListStagingSourceInput, ListStagingSourceOperation,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, UNIX_EPOCH};
use tokio::time::timeout;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

const CONNECTOR_CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_ENTRY_LIMIT: usize = 200;
const MAX_ENTRY_LIMIT: usize = 1000;

#[derive(OpenApi)]
#[openapi(
    tags((name = "connectors", description = "External source connector registration")),
    paths(
        create_source_connector,
        list_source_connectors,
        get_source_connector,
        replace_source_connector,
        delete_source_connector,
        check_source_connector,
        check_stored_connector,
        list_connector_entries
    )
)]
pub struct ConnectorsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route(
            "/groups/{group_id}/connectors/check",
            post(check_source_connector),
        )
        .route(
            "/groups/{group_id}/connectors",
            post(create_source_connector).get(list_source_connectors),
        )
        .route(
            "/groups/{group_id}/connectors/{connector_id}",
            get(get_source_connector)
                .put(replace_source_connector)
                .delete(delete_source_connector),
        )
        .route(
            "/groups/{group_id}/connectors/{connector_id}/check",
            post(check_stored_connector),
        )
        .route(
            "/groups/{group_id}/connectors/{connector_id}/entries",
            get(list_connector_entries),
        )
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiSourceConnectorKind {
    Http,
    S3,
    Webdav,
    Ftp,
    ArunaNative,
}

impl From<ApiSourceConnectorKind> for SourceConnectorKind {
    fn from(value: ApiSourceConnectorKind) -> Self {
        match value {
            ApiSourceConnectorKind::Http => SourceConnectorKind::Http,
            ApiSourceConnectorKind::S3 => SourceConnectorKind::S3,
            ApiSourceConnectorKind::Webdav => SourceConnectorKind::Webdav,
            ApiSourceConnectorKind::Ftp => SourceConnectorKind::Ftp,
            ApiSourceConnectorKind::ArunaNative => SourceConnectorKind::ArunaNative,
        }
    }
}

impl From<SourceConnectorKind> for ApiSourceConnectorKind {
    fn from(value: SourceConnectorKind) -> Self {
        match value {
            SourceConnectorKind::Http => ApiSourceConnectorKind::Http,
            SourceConnectorKind::S3 => ApiSourceConnectorKind::S3,
            SourceConnectorKind::Webdav => ApiSourceConnectorKind::Webdav,
            SourceConnectorKind::Ftp => ApiSourceConnectorKind::Ftp,
            SourceConnectorKind::ArunaNative => ApiSourceConnectorKind::ArunaNative,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSourceConnectorRequest {
    pub name: String,
    pub kind: ApiSourceConnectorKind,
    pub public_config: HashMap<String, String>,
    #[serde(default)]
    pub secret_config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplaceSourceConnectorRequest {
    pub name: String,
    pub kind: ApiSourceConnectorKind,
    pub public_config: HashMap<String, String>,
    #[serde(default)]
    pub secret_config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SourceConnectorRequest {
    pub name: String,
    pub kind: ApiSourceConnectorKind,
    pub public_config: HashMap<String, String>,
    #[serde(default)]
    pub secret_config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ConnectorCheckSuccess {
    pub ok: bool,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ConnectorCheckFailure {
    pub ok: bool,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(untagged)]
pub enum ConnectorCheckResponse {
    Success(ConnectorCheckSuccess),
    Failure(ConnectorCheckFailure),
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorEntriesQuery {
    #[serde(default)]
    pub path: String,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorEntryKind {
    File,
    Dir,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ConnectorEntryResponse {
    pub name: String,
    pub path: String,
    pub kind: ConnectorEntryKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ConnectorEntriesResponse {
    pub entries: Vec<ConnectorEntryResponse>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SourceConnectorResponse {
    pub connector_id: String,
    pub group_id: String,
    pub name: String,
    pub kind: ApiSourceConnectorKind,
    pub public_config: HashMap<String, String>,
    pub created_at: String,
    pub updated_at: String,
    pub created_by: String,
    pub has_secret_config: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ListSourceConnectorsResponse {
    pub connectors: Vec<SourceConnectorResponse>,
}

#[utoipa::path(
    post,
    path = "/groups/{group_id}/connectors",
    tag = "connectors",
    params(("group_id" = String, Path, description = "Group id")),
    request_body = CreateSourceConnectorRequest,
    responses(
        (status = 201, description = "Connector created", body = SourceConnectorResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_source_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<CreateSourceConnectorRequest>,
) -> ServerResult<(StatusCode, Json<SourceConnectorResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::WRITE).await?;

    let result = drive(
        CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
            group_id,
            created_by: auth.user_id,
            name: request.name,
            kind: request.kind.into(),
            public_config: request.public_config,
            secret_config: request.secret_config,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_create_connector_error)?;

    Ok((
        StatusCode::CREATED,
        Json(map_connector_response(
            result.connector,
            result.has_secret_config,
        )),
    ))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/connectors",
    tag = "connectors",
    params(("group_id" = String, Path, description = "Group id")),
    responses(
        (status = 200, description = "List connectors", body = ListSourceConnectorsResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_source_connectors(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<ListSourceConnectorsResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::READ).await?;

    let result = drive(
        ListSourceConnectorsOperation::new(ListSourceConnectorsInput { group_id }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_list_connector_error)?;

    let mut connectors = Vec::with_capacity(result.connectors.len());
    for connector in result.connectors {
        let has_secret_config =
            connector_has_secret_config(state.as_ref(), connector.connector_id).await?;
        connectors.push(map_connector_response(connector, has_secret_config));
    }

    Ok((
        StatusCode::OK,
        Json(ListSourceConnectorsResponse { connectors }),
    ))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/connectors/{connector_id}",
    tag = "connectors",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("connector_id" = String, Path, description = "Connector id")
    ),
    responses(
        (status = 200, description = "Connector details", body = SourceConnectorResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_source_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<SourceConnectorResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector_id(&connector_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::READ).await?;

    let result = drive(
        GetSourceConnectorOperation::new(GetSourceConnectorInput {
            group_id,
            connector_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_get_connector_error)?;

    Ok((
        StatusCode::OK,
        Json(map_connector_response(
            result.connector,
            result.has_secret_config,
        )),
    ))
}

#[utoipa::path(
    put,
    path = "/groups/{group_id}/connectors/{connector_id}",
    tag = "connectors",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("connector_id" = String, Path, description = "Connector id")
    ),
    request_body = ReplaceSourceConnectorRequest,
    responses(
        (status = 200, description = "Connector replaced", body = SourceConnectorResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn replace_source_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
    Json(request): Json<ReplaceSourceConnectorRequest>,
) -> ServerResult<(StatusCode, Json<SourceConnectorResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector_id(&connector_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::WRITE).await?;

    let result = drive(
        ReplaceSourceConnectorOperation::new(ReplaceSourceConnectorInput {
            group_id,
            connector_id,
            name: request.name,
            kind: request.kind.into(),
            public_config: request.public_config,
            secret_config: request.secret_config,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_replace_connector_error)?;

    Ok((
        StatusCode::OK,
        Json(map_connector_response(
            result.connector,
            result.has_secret_config,
        )),
    ))
}

#[utoipa::path(
    delete,
    path = "/groups/{group_id}/connectors/{connector_id}",
    tag = "connectors",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("connector_id" = String, Path, description = "Connector id")
    ),
    responses(
        (status = 204, description = "Connector deleted"),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_source_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector_id(&connector_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::WRITE).await?;

    drive(
        DeleteSourceConnectorOperation::new(DeleteSourceConnectorInput {
            group_id,
            connector_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_delete_connector_error)?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/groups/{group_id}/connectors/check",
    tag = "connectors",
    params(("group_id" = String, Path, description = "Group id")),
    request_body = SourceConnectorRequest,
    responses(
        (status = 200, description = "Connector check result", body = ConnectorCheckResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn check_source_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<SourceConnectorRequest>,
) -> ServerResult<Json<ConnectorCheckResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::WRITE).await?;

    let kind: SourceConnectorKind = request.kind.into();
    validate_connector_input(
        &request.name,
        kind,
        &request.public_config,
        &request.secret_config,
    )
    .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;

    let access = resolve_inline_access(kind, &request.public_config, request.secret_config)
        .map_err(map_resolution_error)?;
    Ok(Json(run_connector_check(&state, access).await))
}

#[utoipa::path(
    post,
    path = "/groups/{group_id}/connectors/{connector_id}/check",
    tag = "connectors",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("connector_id" = String, Path, description = "Connector id")
    ),
    responses(
        (status = 200, description = "Connector check result", body = ConnectorCheckResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn check_stored_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
) -> ServerResult<Json<ConnectorCheckResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector_id(&connector_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::READ).await?;

    let resolved = drive(
        ResolveSourceConnectorOperation::new(ResolveSourceConnectorInput {
            group_id,
            connector_id,
            source_path: String::new(),
            allow_root: true,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_resolution_error)?;

    Ok(Json(run_connector_check(&state, resolved.access).await))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/connectors/{connector_id}/entries",
    tag = "connectors",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("connector_id" = String, Path, description = "Connector id"),
        ("path" = Option<String>, Query, description = "Path relative to connector root"),
        ("limit" = Option<usize>, Query, description = "Maximum entries, capped at 1000")
    ),
    responses(
        (status = 200, description = "Connector entries", body = ConnectorEntriesResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 502, description = "Bad gateway", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_connector_entries(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
    Query(query): Query<ConnectorEntriesQuery>,
) -> ServerResult<Json<ConnectorEntriesResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector_id(&connector_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::READ).await?;
    let source_path = normalize_browse_path(&query.path)?;
    let limit = query.limit.unwrap_or(DEFAULT_ENTRY_LIMIT);
    if limit == 0 {
        return Err(ServerError::BadRequestReason(
            "limit must be greater than zero".to_string(),
        ));
    }

    let result = drive(
        ListStagingSourceOperation::new(ListStagingSourceInput {
            group_id,
            connector_id,
            source_path,
            limit: limit.min(MAX_ENTRY_LIMIT),
            recursive: false,
            files_only: false,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_list_error)?;

    Ok(Json(ConnectorEntriesResponse {
        entries: result
            .entries
            .into_iter()
            .map(|entry| ConnectorEntryResponse {
                name: entry.name,
                path: entry.path,
                kind: match entry.kind {
                    SourceEntryKind::File => ConnectorEntryKind::File,
                    SourceEntryKind::Directory => ConnectorEntryKind::Dir,
                },
                size: entry.size,
                modified_ms: entry.modified.and_then(|modified| {
                    modified
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                }),
            })
            .collect(),
        truncated: result.truncated,
    }))
}

async fn run_connector_check(
    state: &ServerState,
    access: ResolvedSourceAccess,
) -> ConnectorCheckResponse {
    let started = Instant::now();
    match timeout(
        CONNECTOR_CHECK_TIMEOUT,
        drive(CheckStagingSourceOperation::new(access), &state.get_ctx()),
    )
    .await
    {
        Ok(Ok(())) => ConnectorCheckResponse::Success(ConnectorCheckSuccess {
            ok: true,
            latency_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        }),
        Ok(Err(error)) => ConnectorCheckResponse::Failure(ConnectorCheckFailure {
            ok: false,
            error: check_error_message(&error),
        }),
        Err(_) => ConnectorCheckResponse::Failure(ConnectorCheckFailure {
            ok: false,
            error: "connector check timed out".to_string(),
        }),
    }
}

fn check_error_message(
    error: &aruna_operations::staging::check_source::CheckStagingSourceError,
) -> String {
    use aruna_core::errors::StagingSourceError;
    use aruna_operations::staging::check_source::CheckStagingSourceError;

    match error {
        CheckStagingSourceError::Staging(StagingSourceError::OperatorCreationFailed(_)) => {
            "connector configuration is invalid".to_string()
        }
        CheckStagingSourceError::Staging(StagingSourceError::UnsupportedKind(_)) => {
            "connector kind is not supported".to_string()
        }
        CheckStagingSourceError::Staging(StagingSourceError::HandleMissing)
        | CheckStagingSourceError::Staging(StagingSourceError::ChannelClosed) => {
            "connector check is unavailable".to_string()
        }
        _ => "connector is unreachable".to_string(),
    }
}

fn normalize_browse_path(path: &str) -> ServerResult<String> {
    if path.trim().is_empty() {
        return Ok(String::new());
    }
    validate_source_path(path, false).map_err(map_resolution_error)?;
    Ok(format!("{}/", path.trim().trim_end_matches('/')))
}

fn parse_connector_id(connector_id: &str) -> ServerResult<Ulid> {
    Ulid::from_str(connector_id).map_err(|_| ServerError::BadRequest)
}

pub(crate) async fn ensure_group_data_permission(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
    required_permission: Permission,
) -> ServerResult<()> {
    let _ = group_id;
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: format!("/{}/g/{group_id}/data/**", state.get_realm_id()),
            required_permission,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::AuthDocNotFound => ServerError::Forbidden,
        _ => ServerError::InternalError(err.to_string()),
    })?;

    if allowed {
        Ok(())
    } else {
        Err(ServerError::Forbidden)
    }
}

async fn connector_has_secret_config(
    state: &ServerState,
    connector_id: Ulid,
) -> ServerResult<bool> {
    drive(
        ConnectorHasSecretConfigOperation::new(connector_id),
        &state.get_ctx(),
    )
    .await
    .map_err(map_connector_secret_config_error)
}

fn map_connector_secret_config_error(error: ConnectorHasSecretConfigError) -> ServerError {
    ServerError::InternalError(error.to_string())
}

fn map_connector_response(
    connector: SourceConnector,
    has_secret_config: bool,
) -> SourceConnectorResponse {
    SourceConnectorResponse {
        connector_id: connector.connector_id.to_string(),
        group_id: connector.group_id.to_string(),
        name: connector.name,
        kind: connector.kind.into(),
        public_config: connector.public_config,
        created_at: format_system_time(connector.created_at),
        updated_at: format_system_time(connector.updated_at),
        created_by: connector.created_by.to_string(),
        has_secret_config,
    }
}

fn format_system_time(value: std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(value).to_rfc3339()
}

fn map_create_connector_error(error: CreateSourceConnectorError) -> ServerError {
    match error {
        CreateSourceConnectorError::ValidationError(_) => ServerError::BadRequest,
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_list_connector_error(error: ListSourceConnectorsError) -> ServerError {
    ServerError::InternalError(error.to_string())
}

fn map_get_connector_error(
    error: aruna_operations::connectors::get_source_connector::GetSourceConnectorError,
) -> ServerError {
    match error {
        GetSourceConnectorError::NotFound => ServerError::NotFound,
        GetSourceConnectorError::StorageError(_)
        | GetSourceConnectorError::ConversionError(_)
        | GetSourceConnectorError::GetSourceConnectorFailed => {
            ServerError::InternalError(error.to_string())
        }
    }
}

fn map_replace_connector_error(error: ReplaceSourceConnectorError) -> ServerError {
    match error {
        ReplaceSourceConnectorError::ValidationError(_) => ServerError::BadRequest,
        ReplaceSourceConnectorError::NotFound => ServerError::NotFound,
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_delete_connector_error(error: DeleteSourceConnectorError) -> ServerError {
    match error {
        DeleteSourceConnectorError::NotFound => ServerError::NotFound,
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_resolution_error(error: SourceConnectorResolutionError) -> ServerError {
    match error {
        SourceConnectorResolutionError::NotFound => ServerError::NotFound,
        SourceConnectorResolutionError::InvalidSourcePath
        | SourceConnectorResolutionError::UnsupportedConnectorKind(_) => ServerError::BadRequest,
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_list_error(error: ListStagingSourceError) -> ServerError {
    match error {
        ListStagingSourceError::Resolve(error) => map_resolution_error(error),
        ListStagingSourceError::Staging(aruna_core::errors::StagingSourceError::NotFound) => {
            ServerError::NotFound
        }
        ListStagingSourceError::Staging(_) => ServerError::BadGateway,
        _ => ServerError::InternalError(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openapi::ApiDoc;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, NodeCapabilities, RealmAuthorizationDocument,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage;
    use serde_json::json;
    use tempfile::TempDir;

    struct TestState {
        _storage_dir: TempDir,
        auth: AuthContext,
        other_auth: AuthContext,
        group_id: Ulid,
        state: Arc<ServerState>,
    }

    #[tokio::test]
    async fn connector_routes_crud_and_redact_secret_config() {
        let test = setup_state().await;

        let (_, Json(created)) = create_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(CreateSourceConnectorRequest {
                name: "refdata".to_string(),
                kind: ApiSourceConnectorKind::S3,
                public_config: HashMap::from([
                    ("bucket".to_string(), "reads".to_string()),
                    ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ]),
                secret_config: HashMap::from([(
                    "access_key_id".to_string(),
                    "super-secret".to_string(),
                )]),
            }),
        )
        .await
        .unwrap();

        assert_eq!(created.name, "refdata");
        assert!(created.has_secret_config);
        assert!(!created.public_config.contains_key("access_key_id"));

        let (_, Json(listed)) = list_source_connectors(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
        )
        .await
        .unwrap();

        assert_eq!(listed.connectors.len(), 1);
        assert!(listed.connectors[0].has_secret_config);

        let (_, Json(fetched)) = get_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path((test.group_id.to_string(), created.connector_id.clone())),
        )
        .await
        .unwrap();

        assert_eq!(fetched, created);

        let (_, Json(replaced)) = replace_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path((test.group_id.to_string(), created.connector_id.clone())),
            Json(ReplaceSourceConnectorRequest {
                name: "refdata-updated".to_string(),
                kind: ApiSourceConnectorKind::S3,
                public_config: HashMap::from([
                    ("bucket".to_string(), "reads-v2".to_string()),
                    ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ]),
                secret_config: HashMap::new(),
            }),
        )
        .await
        .unwrap();

        assert_eq!(replaced.name, "refdata-updated");
        assert!(!replaced.has_secret_config);

        let delete_status = delete_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path((test.group_id.to_string(), created.connector_id.clone())),
        )
        .await
        .unwrap();

        assert_eq!(delete_status, StatusCode::NO_CONTENT);

        let get_result = get_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth)),
            Path((test.group_id.to_string(), created.connector_id)),
        )
        .await;
        assert!(matches!(get_result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn connector_routes_require_group_data_permission() {
        let test = setup_state().await;

        let result = create_source_connector(
            State(test.state),
            Extension(Some(test.other_auth)),
            Path(test.group_id.to_string()),
            Json(CreateSourceConnectorRequest {
                name: "forbidden".to_string(),
                kind: ApiSourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn check_requires_permission() {
        let test = setup_state().await;

        let result = check_source_connector(
            State(test.state),
            Extension(Some(test.other_auth)),
            Path(test.group_id.to_string()),
            Json(SourceConnectorRequest {
                name: "source".to_string(),
                kind: ApiSourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn check_returns_failure() {
        let test = setup_state().await;

        let Json(result) = check_source_connector(
            State(test.state),
            Extension(Some(test.auth)),
            Path(test.group_id.to_string()),
            Json(SourceConnectorRequest {
                name: "source".to_string(),
                kind: ApiSourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
        )
        .await
        .unwrap();

        assert!(matches!(
            result,
            ConnectorCheckResponse::Failure(ConnectorCheckFailure { ok: false, .. })
        ));
    }

    #[test]
    fn check_maps_unreachable() {
        let error = aruna_operations::staging::check_source::CheckStagingSourceError::Staging(
            aruna_core::errors::StagingSourceError::CheckError("connection refused".to_string()),
        );

        assert_eq!(check_error_message(&error), "connector is unreachable");
    }

    #[tokio::test]
    async fn stored_check_resolves() {
        let test = setup_state().await;
        let (_, Json(connector)) = create_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(CreateSourceConnectorRequest {
                name: "stored-source".to_string(),
                kind: ApiSourceConnectorKind::S3,
                public_config: HashMap::from([
                    ("bucket".to_string(), "reads".to_string()),
                    ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ]),
                secret_config: HashMap::from([(
                    "access_key_id".to_string(),
                    "stored-secret".to_string(),
                )]),
            }),
        )
        .await
        .unwrap();

        let Json(result) = check_stored_connector(
            State(test.state),
            Extension(Some(test.auth)),
            Path((test.group_id.to_string(), connector.connector_id)),
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            ConnectorCheckResponse::Failure(ConnectorCheckFailure {
                ok: false,
                error: "connector check is unavailable".to_string(),
            })
        );
    }

    #[test]
    fn check_serializes_success() {
        let result = ConnectorCheckResponse::Success(ConnectorCheckSuccess {
            ok: true,
            latency_ms: 12,
        });

        assert_eq!(
            serde_json::to_value(result).unwrap(),
            json!({"ok": true, "latency_ms": 12})
        );
    }

    #[tokio::test]
    async fn entries_reject_traversal() {
        let test = setup_state().await;

        let result = list_connector_entries(
            State(test.state),
            Extension(Some(test.auth)),
            Path((test.group_id.to_string(), Ulid::r#gen().to_string())),
            Query(ConnectorEntriesQuery {
                path: "../secret".to_string(),
                limit: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn entries_require_permission() {
        let test = setup_state().await;

        let result = list_connector_entries(
            State(test.state),
            Extension(Some(test.other_auth)),
            Path((test.group_id.to_string(), Ulid::r#gen().to_string())),
            Query(ConnectorEntriesQuery {
                path: String::new(),
                limit: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[test]
    fn entries_normalize_prefix() {
        assert_eq!(normalize_browse_path("prefix").unwrap(), "prefix/");
        assert_eq!(normalize_browse_path("prefix/").unwrap(), "prefix/");
        assert_eq!(normalize_browse_path("").unwrap(), "");
    }

    #[test]
    fn openapi_includes_connector_paths() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

        assert!(
            openapi["paths"]
                .get("/groups/{group_id}/connectors")
                .is_some()
        );
        assert!(
            openapi["paths"]
                .get("/groups/{group_id}/connectors/{connector_id}")
                .is_some()
        );
        assert!(
            openapi["paths"]
                .get("/groups/{group_id}/connectors/check")
                .is_some()
        );
        assert!(
            openapi["paths"]
                .get("/groups/{group_id}/connectors/{connector_id}/check")
                .is_some()
        );
        assert!(
            openapi["paths"]
                .get("/groups/{group_id}/connectors/{connector_id}/entries")
                .is_some()
        );
        assert_eq!(
            openapi["components"]["schemas"]["ApiSourceConnectorKind"]["type"],
            json!("string")
        );
    }

    async fn setup_state() -> TestState {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let realm_id = aruna_core::structs::RealmId([3u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let user_id = UserId::local(Ulid::r#gen(), realm_id);
        let other_user_id = UserId::local(Ulid::r#gen(), realm_id);
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let group_id = Ulid::r#gen();
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, group_id);
        let group = Group {
            display_name: "connector-group".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner: user_id,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);

        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            realm_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            group_id.to_bytes().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            GROUP_KEYSPACE,
            group_id.to_bytes().into(),
            group.to_bytes(&actor).unwrap().into(),
        )
        .await;

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        TestState {
            _storage_dir: storage_dir,
            auth: AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
            },
            other_auth: AuthContext {
                user_id: other_user_id,
                realm_id,
                path_restrictions: None,
            },
            group_id,
            state,
        }
    }

    async fn write_doc(
        driver_ctx: &Arc<DriverContext>,
        key_space: &str,
        key: byteview::ByteView,
        value: byteview::ByteView,
    ) {
        let event = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }
}
