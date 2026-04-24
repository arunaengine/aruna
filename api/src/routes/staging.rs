use crate::auth::{
    bucket_blob_permission_path, ensure_permission, parse_group_id, parse_source_connector_id,
    require_realm_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::s3::replication::spawn_version_replication;
use crate::server_state::ServerState;
use aruna_core::errors::{SourceConnectorResolutionError, StagingSourceError};
use aruna_core::structs::{AuthContext, BucketInfo, Permission};
use aruna_operations::driver::drive;
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::staging::head_source::HeadStagingSourceError;
use aruna_operations::staging::read_source::ReadStagingSourceError;
use aruna_operations::staging::reference::{
    MaterializeReferenceError, MaterializeReferenceInput, materialize_reference,
};
use aruna_operations::staging::snapshot::{
    MaterializeSnapshotError, MaterializeSnapshotInput, materialize_snapshot,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "blobs", description = "Blob staging and replication")),
    paths(stage_blob)
)]
pub struct BlobsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new().route("/blobs/staging", post(stage_blob))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiStagingStrategy {
    Snapshot,
    Reference,
    Sync,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StageBlobTargetRequest {
    pub group_id: String,
    pub connector_id: String,
    pub source_path: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum StageBlobRequest {
    Snapshot(StageBlobTargetRequest),
    Reference(StageBlobTargetRequest),
    Sync(StageBlobTargetRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StageBlobResponse {
    pub strategy: ApiStagingStrategy,
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
}

#[utoipa::path(
    post,
    path = "/blobs/staging",
    tag = "blobs",
    request_body = StageBlobRequest,
    responses(
        (status = 201, description = "Blob staged", body = StageBlobResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 501, description = "Not implemented", body = ErrorResponse),
        (status = 502, description = "Bad gateway", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn stage_blob(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<StageBlobRequest>,
) -> ServerResult<(StatusCode, Json<StageBlobResponse>)> {
    let auth = require_realm_auth(&state, auth)?;

    match request {
        StageBlobRequest::Snapshot(request) => snapshot_blob(state, auth, request).await,
        StageBlobRequest::Reference(request) => reference_blob(state, auth, request).await,
        StageBlobRequest::Sync(_) => Err(ServerError::Unimplemented),
    }
}

async fn snapshot_blob(
    state: Arc<ServerState>,
    auth: AuthContext,
    request: StageBlobTargetRequest,
) -> ServerResult<(StatusCode, Json<StageBlobResponse>)> {
    let group_id = parse_group_id(&request.group_id)?;
    let connector_id = parse_source_connector_id(&request.connector_id)?;
    let bucket_info = load_bucket_info(&state, &request.bucket).await?;
    if bucket_info.group_id != group_id {
        return Err(ServerError::NotFound);
    }

    ensure_permission(
        &state,
        &auth,
        bucket_blob_permission_path(&state, group_id, &request.bucket, &request.key),
        Permission::WRITE,
    )
    .await?;

    let result = materialize_snapshot(
        &state.get_ctx(),
        MaterializeSnapshotInput {
            group_id,
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
            node_id: state.get_node_id(),
            connector_id,
            source_path: request.source_path,
            bucket: request.bucket.clone(),
            key: request.key.clone(),
        },
    )
    .await
    .map_err(map_snapshot_error)?;

    spawn_version_replication(
        state.get_ctx(),
        state.get_realm_id(),
        state.get_node_id(),
        auth,
        request.bucket.clone(),
        request.key.clone(),
        result.version_id,
        false,
    );

    Ok((
        StatusCode::CREATED,
        Json(StageBlobResponse {
            strategy: ApiStagingStrategy::Snapshot,
            bucket: request.bucket,
            key: request.key,
            version_id: result.version_id.to_string(),
            size: result.location.blob_size,
            content_type: result.source_metadata.content_type,
            etag: result.source_metadata.etag,
            last_modified: result.source_metadata.last_modified.map(format_system_time),
        }),
    ))
}

async fn reference_blob(
    state: Arc<ServerState>,
    auth: AuthContext,
    request: StageBlobTargetRequest,
) -> ServerResult<(StatusCode, Json<StageBlobResponse>)> {
    let group_id = parse_group_id(&request.group_id)?;
    let connector_id = parse_source_connector_id(&request.connector_id)?;
    let bucket_info = load_bucket_info(&state, &request.bucket).await?;
    if bucket_info.group_id != group_id {
        return Err(ServerError::NotFound);
    }

    ensure_permission(
        &state,
        &auth,
        bucket_blob_permission_path(&state, group_id, &request.bucket, &request.key),
        Permission::WRITE,
    )
    .await?;

    let result = materialize_reference(
        &state.get_ctx(),
        MaterializeReferenceInput {
            group_id,
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
            node_id: state.get_node_id(),
            connector_id,
            source_path: request.source_path,
            bucket: request.bucket.clone(),
            key: request.key.clone(),
        },
    )
    .await
    .map_err(map_reference_error)?;

    Ok((
        StatusCode::CREATED,
        Json(StageBlobResponse {
            strategy: ApiStagingStrategy::Reference,
            bucket: request.bucket,
            key: request.key,
            version_id: result.version_id.to_string(),
            size: result.source_metadata.content_length,
            content_type: result.source_metadata.content_type,
            etag: result.source_metadata.etag,
            last_modified: result.source_metadata.last_modified.map(format_system_time),
        }),
    ))
}

async fn load_bucket_info(state: &ServerState, bucket: &str) -> ServerResult<BucketInfo> {
    match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(bucket_info)) => Ok(bucket_info),
        Ok(None) | Err(GetBucketInfoError::NotFound) => Err(ServerError::NotFound),
        Err(err) => Err(ServerError::InternalError(err.to_string())),
    }
}

fn map_snapshot_error(error: MaterializeSnapshotError) -> ServerError {
    match error {
        MaterializeSnapshotError::Read(error) => map_read_staging_error(error),
        MaterializeSnapshotError::Write(error) => ServerError::InternalError(error.to_string()),
    }
}

fn map_reference_error(error: MaterializeReferenceError) -> ServerError {
    match error {
        MaterializeReferenceError::Head(error) => map_head_staging_error(error),
        MaterializeReferenceError::Storage(error) => ServerError::InternalError(error.to_string()),
        MaterializeReferenceError::Conversion(error) => {
            ServerError::InternalError(error.to_string())
        }
    }
}

fn map_head_staging_error(error: HeadStagingSourceError) -> ServerError {
    match error {
        HeadStagingSourceError::Resolve(error) => map_connector_resolution_error(error),
        HeadStagingSourceError::Staging(error) => map_staging_source_error(error),
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_read_staging_error(error: ReadStagingSourceError) -> ServerError {
    match error {
        ReadStagingSourceError::Resolve(error) => map_connector_resolution_error(error),
        ReadStagingSourceError::Staging(error) => map_staging_source_error(error),
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_connector_resolution_error(error: SourceConnectorResolutionError) -> ServerError {
    match error {
        SourceConnectorResolutionError::NotFound => ServerError::NotFound,
        SourceConnectorResolutionError::InvalidSourcePath
        | SourceConnectorResolutionError::UnsupportedConnectorKind(_) => ServerError::BadRequest,
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_staging_source_error(error: StagingSourceError) -> ServerError {
    match error {
        StagingSourceError::NotFound => ServerError::NotFound,
        _ => ServerError::BadGateway,
    }
}

fn format_system_time(value: std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(value).to_rfc3339()
}
