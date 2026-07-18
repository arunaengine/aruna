use crate::auth::{
    bucket_blob_permission_path, ensure_permission, parse_group_id, parse_source_connector_id,
    require_realm_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::errors::{SourceConnectorResolutionError, StagingSourceError};
use aruna_core::structs::{AuthContext, BucketInfo, Permission, SourceEntry, SourceEntryKind};
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::replication::queue::{
    QueueLiveVersionReplicationInput, QueueLiveVersionReplicationOperation,
};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::put_object::PutObjectError;
use aruna_operations::staging::head_source::HeadStagingSourceError;
use aruna_operations::staging::list_source::{
    ListStagingSourceError, ListStagingSourceInput, ListStagingSourceOperation,
};
use aruna_operations::staging::read_source::ReadStagingSourceError;
use aruna_operations::staging::reference::{
    MaterializeReferenceError, MaterializeReferenceInput, stage_reference_blob,
};
use aruna_operations::staging::snapshot::{
    MaterializeSnapshotError, MaterializeSnapshotInput, stage_snapshot_blob,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "staging", description = "Blob staging")),
    paths(stage_blob, stage_batch)
)]
pub struct StagingApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/staging/", post(stage_blob))
        .route("/staging/batch", post(stage_batch))
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StageBatchItem {
    pub source_path: String,
    pub target_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StageBatchPrefix {
    pub source_prefix: String,
    pub target_prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StageBatchRequest {
    pub group_id: String,
    #[serde(default)]
    pub node_id: Option<String>,
    pub connector_id: String,
    pub bucket: String,
    pub strategy: ApiStagingStrategy,
    #[serde(default)]
    pub items: Option<Vec<StageBatchItem>>,
    #[serde(default)]
    pub prefixes: Option<Vec<StageBatchPrefix>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StageBatchStatus {
    Ok,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StageBatchResult {
    pub source_path: String,
    pub target_key: String,
    pub status: StageBatchStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StageBatchResponse {
    pub results: Vec<StageBatchResult>,
}

#[utoipa::path(
    post,
    path = "/staging/",
    tag = "staging",
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

#[utoipa::path(
    post,
    path = "/staging/batch",
    tag = "staging",
    request_body = StageBatchRequest,
    responses(
        (status = 200, description = "Batch staging results", body = StageBatchResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 501, description = "Not implemented", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn stage_batch(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<StageBatchRequest>,
) -> ServerResult<(StatusCode, Json<StageBatchResponse>)> {
    const BATCH_LIMIT: usize = 1000;

    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&request.group_id)?;
    let connector_id = parse_source_connector_id(&request.connector_id)?;
    if request.strategy == ApiStagingStrategy::Sync {
        return Err(ServerError::Unimplemented);
    }
    let node_id = request
        .node_id
        .as_deref()
        .map(NodeId::from_str)
        .transpose()
        .map_err(|_| ServerError::BadRequest)?
        .unwrap_or_else(|| state.get_node_id());
    if node_id != state.get_node_id() {
        return Err(ServerError::BadRequestReason(
            "staging node must be the local node".to_string(),
        ));
    }
    let mut items = request.items.unwrap_or_default();
    ensure_batch_capacity(0, items.len(), BATCH_LIMIT)?;
    let prefixes = request.prefixes.unwrap_or_default();
    if !prefixes.is_empty() {
        crate::routes::connectors::ensure_group_data_permission(
            &state,
            &auth,
            group_id,
            Permission::READ,
        )
        .await?;
    }
    let mut expansion_errors = Vec::new();
    for prefix in prefixes {
        let source_prefix = match normalize_prefix(&prefix.source_prefix) {
            Ok(prefix) => prefix,
            Err(error) => {
                expansion_errors.push(StageBatchResult {
                    source_path: prefix.source_prefix,
                    target_key: prefix.target_prefix,
                    status: StageBatchStatus::Error,
                    error: Some(batch_error_message(&error)),
                });
                continue;
            }
        };
        let remaining = BATCH_LIMIT - items.len();
        match drive(
            ListStagingSourceOperation::new(ListStagingSourceInput {
                group_id,
                connector_id,
                source_path: source_prefix.clone(),
                limit: remaining,
                recursive: true,
                files_only: true,
            }),
            &state.get_ctx(),
        )
        .await
        {
            Ok(result) => {
                if result.truncated {
                    return Err(ServerError::BadRequestReason(format!(
                        "batch expands beyond {BATCH_LIMIT} items"
                    )));
                }
                let expanded =
                    map_prefix_entries(result.entries, &source_prefix, &prefix.target_prefix);
                ensure_batch_capacity(items.len(), expanded.len(), BATCH_LIMIT)?;
                items.extend(expanded);
            }
            Err(error) => {
                let error = map_list_error(error);
                expansion_errors.push(StageBatchResult {
                    source_path: prefix.source_prefix,
                    target_key: prefix.target_prefix,
                    status: StageBatchStatus::Error,
                    error: Some(batch_error_message(&error)),
                });
            }
        }
    }

    let mut results = Vec::with_capacity(items.len() + expansion_errors.len());
    for item in items {
        let result = stage_item(
            state.clone(),
            auth.clone(),
            group_id,
            connector_id,
            &request.bucket,
            request.strategy,
            &item,
        )
        .await;
        results.push(stage_result(item, result));
    }
    results.extend(expansion_errors);

    Ok((StatusCode::OK, Json(StageBatchResponse { results })))
}

async fn stage_item(
    state: Arc<ServerState>,
    auth: AuthContext,
    group_id: ulid::Ulid,
    connector_id: ulid::Ulid,
    bucket: &str,
    strategy: ApiStagingStrategy,
    item: &StageBatchItem,
) -> ServerResult<()> {
    let target = StageBlobTargetRequest {
        group_id: group_id.to_string(),
        connector_id: connector_id.to_string(),
        source_path: item.source_path.clone(),
        bucket: bucket.to_string(),
        key: item.target_key.clone(),
    };
    let _ = match strategy {
        ApiStagingStrategy::Snapshot => snapshot_blob(state, auth, target).await?,
        ApiStagingStrategy::Reference => reference_blob(state, auth, target).await?,
        ApiStagingStrategy::Sync => return Err(ServerError::Unimplemented),
    };
    Ok(())
}

fn stage_result(item: StageBatchItem, result: ServerResult<()>) -> StageBatchResult {
    match result {
        Ok(()) => StageBatchResult {
            source_path: item.source_path,
            target_key: item.target_key,
            status: StageBatchStatus::Ok,
            error: None,
        },
        Err(error) => StageBatchResult {
            source_path: item.source_path,
            target_key: item.target_key,
            status: StageBatchStatus::Error,
            error: Some(batch_error_message(&error)),
        },
    }
}

fn batch_error_message(error: &ServerError) -> String {
    match error {
        ServerError::InternalError(_) => "Internal server error".to_string(),
        ServerError::BadGateway => "Bad gateway".to_string(),
        _ => error.to_string(),
    }
}

fn ensure_batch_capacity(current: usize, additional: usize, limit: usize) -> ServerResult<()> {
    if current
        .checked_add(additional)
        .is_none_or(|total| total > limit)
    {
        return Err(ServerError::BadRequestReason(format!(
            "batch expands beyond {limit} items"
        )));
    }
    Ok(())
}

fn normalize_prefix(prefix: &str) -> ServerResult<String> {
    if prefix.trim().is_empty() {
        return Ok(String::new());
    }
    validate_relative_source_path(prefix)?;
    Ok(format!("{}/", prefix.trim().trim_end_matches('/')))
}

fn map_prefix_entries(
    entries: Vec<SourceEntry>,
    source_prefix: &str,
    target_prefix: &str,
) -> Vec<StageBatchItem> {
    entries
        .into_iter()
        .filter(|entry| entry.kind == SourceEntryKind::File)
        .map(|entry| {
            let relative = entry
                .path
                .strip_prefix(source_prefix)
                .unwrap_or(&entry.path)
                .trim_start_matches('/');
            let target_prefix = target_prefix.trim_matches('/');
            let target_key = if target_prefix.is_empty() {
                relative.to_string()
            } else if relative.is_empty() {
                target_prefix.to_string()
            } else {
                format!("{target_prefix}/{relative}")
            };
            StageBatchItem {
                source_path: entry.path,
                target_key,
            }
        })
        .collect()
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
    ensure_source_permission(&state, &auth, group_id, connector_id, &request.source_path).await?;

    let quota_ceiling = resolve_group_quota_ceiling(&state, group_id).await?;

    let result = stage_snapshot_blob(
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
            quota_ceiling,
        },
    )
    .await
    .map_err(map_snapshot_error)?;

    queue_live_version_replication(
        &state,
        auth,
        request.bucket.clone(),
        request.key.clone(),
        result.version_id,
        false,
    )
    .await;

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
    ensure_source_permission(&state, &auth, group_id, connector_id, &request.source_path).await?;

    let quota_ceiling = resolve_group_quota_ceiling(&state, group_id).await?;

    let result = stage_reference_blob(
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
            quota_ceiling,
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

/// Resolves the hard byte ceiling for a group's realm-wide `logical_bytes` from
/// the realm quota config, mirroring the S3 surface's `resolve_quota_ceiling`.
/// `None` means the group is unlimited.
async fn resolve_group_quota_ceiling(
    state: &ServerState,
    group_id: ulid::Ulid,
) -> ServerResult<Option<u64>> {
    let config = drive(
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok(config.quota.effective_group_ceiling(&group_id))
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

async fn ensure_source_permission(
    state: &ServerState,
    auth: &AuthContext,
    group_id: ulid::Ulid,
    connector_id: ulid::Ulid,
    source_path: &str,
) -> ServerResult<()> {
    validate_relative_source_path(source_path)?;

    ensure_permission(
        state,
        auth,
        source_connector_permission_path(state, group_id, connector_id, source_path),
        Permission::READ,
    )
    .await
}

fn source_connector_permission_path(
    state: &ServerState,
    group_id: ulid::Ulid,
    connector_id: ulid::Ulid,
    source_path: &str,
) -> String {
    format!(
        "/{}/g/{group_id}/data/{}/_sources/{connector_id}/{source_path}",
        state.get_realm_id(),
        state.get_node_id(),
    )
}

fn validate_relative_source_path(source_path: &str) -> ServerResult<()> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Err(ServerError::BadRequest);
    }

    let mut has_normal_component = false;
    for component in Path::new(trimmed).components() {
        match component {
            Component::Normal(_) => has_normal_component = true,
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(ServerError::BadRequest);
            }
        }
    }

    has_normal_component
        .then_some(())
        .ok_or(ServerError::BadRequest)
}

fn map_snapshot_error(error: MaterializeSnapshotError) -> ServerError {
    match error {
        MaterializeSnapshotError::Read(error) => map_read_staging_error(error),
        MaterializeSnapshotError::Write(PutObjectError::QuotaExceeded { .. }) => {
            ServerError::Forbidden
        }
        MaterializeSnapshotError::Write(error) => ServerError::InternalError(error.to_string()),
    }
}

fn map_reference_error(error: MaterializeReferenceError) -> ServerError {
    match error {
        MaterializeReferenceError::Head(error) => map_head_staging_error(error),
        MaterializeReferenceError::QuotaExceeded { .. } => ServerError::Forbidden,
        MaterializeReferenceError::Storage(error) => ServerError::InternalError(error.to_string()),
        MaterializeReferenceError::Conversion(error) => {
            ServerError::InternalError(error.to_string())
        }
        MaterializeReferenceError::Usage(error) => ServerError::InternalError(error.to_string()),
        MaterializeReferenceError::QuotaGate(error) => {
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

fn map_list_error(error: ListStagingSourceError) -> ServerError {
    match error {
        ListStagingSourceError::Resolve(error) => map_connector_resolution_error(error),
        ListStagingSourceError::Staging(error) => map_staging_source_error(error),
        _ => ServerError::InternalError(error.to_string()),
    }
}

async fn queue_live_version_replication(
    state: &ServerState,
    auth_context: AuthContext,
    bucket: String,
    key: String,
    version_id: ulid::Ulid,
    delete_marker: bool,
) {
    let result = match drive(
        QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
            local_node_id: state.get_node_id(),
            auth_context,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            delete_marker,
        }),
        &state.get_ctx(),
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            warn!(
                error = %error,
                bucket,
                key,
                version_id = %version_id,
                delete_marker,
                "Failed to queue live replication after committed staging snapshot; durable obligation remains for repair"
            );
            return;
        }
    };

    if result.queued > 0 && !result.scheduled {
        warn!(bucket, key, version_id = %version_id, queued = result.queued, "Live replication jobs persisted but drain scheduling was not acknowledged");
    }
}

fn format_system_time(value: std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(value).to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openapi::ApiDoc;
    use aruna_core::UserId;
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE, GROUP_KEYSPACE,
        S3_BUCKET_KEYSPACE, S3_BUCKET_REPLICATION_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, NodeCapabilities, PathRestriction,
        RealmAuthorizationDocument, RealmConfigDocument,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::replication::queue::{
        LiveReplicationObligationRecord, live_replication_obligation_key,
    };
    use aruna_storage::storage;
    use std::sync::Arc;
    use tempfile::TempDir;
    use ulid::Ulid;

    struct TestState {
        _storage_dir: TempDir,
        state: Arc<ServerState>,
        bucket_group_id: Ulid,
        connector_id: Ulid,
        source_path: String,
        bucket: String,
        key: String,
        auth_with_source_read: AuthContext,
        auth_without_source_read: AuthContext,
    }

    #[tokio::test]
    async fn snapshot_requires_concrete_source_read_permission() {
        let test = setup_state().await;

        let result = snapshot_blob(
            test.state.clone(),
            test.auth_without_source_read,
            StageBlobTargetRequest {
                group_id: test.bucket_group_id.to_string(),
                connector_id: test.connector_id.to_string(),
                source_path: test.source_path,
                bucket: test.bucket,
                key: test.key,
            },
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn reference_allows_request_past_auth_when_source_read_is_granted() {
        let test = setup_state().await;

        let result = reference_blob(
            test.state.clone(),
            test.auth_with_source_read,
            StageBlobTargetRequest {
                group_id: test.bucket_group_id.to_string(),
                connector_id: test.connector_id.to_string(),
                source_path: test.source_path,
                bucket: test.bucket,
                key: test.key,
            },
        )
        .await;

        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn staging_queue_failure_after_snapshot_commit_leaves_obligation_repairable() {
        let test = setup_state().await;
        let version_id = Ulid::r#gen();
        write_doc(
            &test.state.get_ctx(),
            S3_BUCKET_REPLICATION_KEYSPACE,
            test.bucket.as_bytes().to_vec().into(),
            b"not a bucket replication config".to_vec().into(),
        )
        .await;

        let obligation = LiveReplicationObligationRecord::new(
            test.state.get_node_id(),
            test.auth_with_source_read.clone(),
            test.bucket.clone(),
            test.key.clone(),
            version_id,
            false,
        );
        let obligation_key = live_replication_obligation_key(&obligation).unwrap();
        write_doc(
            &test.state.get_ctx(),
            BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
            obligation_key.as_ref().to_vec().into(),
            postcard::to_allocvec(&obligation).unwrap().into(),
        )
        .await;

        queue_live_version_replication(
            &test.state,
            test.auth_with_source_read,
            test.bucket,
            test.key,
            version_id,
            false,
        )
        .await;

        assert!(
            read_doc(
                &test.state.get_ctx(),
                BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
                obligation_key.as_ref().to_vec().into(),
            )
            .await
            .is_some(),
            "durable obligation should remain repairable when staging queue kick fails"
        );
    }

    #[test]
    fn snapshot_quota_exceeded_maps_to_forbidden() {
        let error = map_snapshot_error(MaterializeSnapshotError::Write(
            PutObjectError::QuotaExceeded {
                limit: 100,
                usage: 200,
            },
        ));
        assert!(matches!(error, ServerError::Forbidden));
    }

    #[test]
    fn reference_quota_exceeded_maps_to_forbidden() {
        let error = map_reference_error(MaterializeReferenceError::QuotaExceeded {
            limit: 100,
            usage: 200,
        });

        assert!(matches!(error, ServerError::Forbidden));
    }

    #[test]
    fn batch_keeps_failures() {
        let success = stage_result(
            StageBatchItem {
                source_path: "a.txt".to_string(),
                target_key: "a.txt".to_string(),
            },
            Ok(()),
        );
        let failure = stage_result(
            StageBatchItem {
                source_path: "missing.txt".to_string(),
                target_key: "missing.txt".to_string(),
            },
            Err(ServerError::NotFound),
        );

        assert_eq!(success.status, StageBatchStatus::Ok);
        assert_eq!(failure.status, StageBatchStatus::Error);
        assert_eq!(failure.error.as_deref(), Some("Not found"));
    }

    #[test]
    fn prefix_expands_paths() {
        let items = map_prefix_entries(
            vec![SourceEntry {
                name: "file.txt".to_string(),
                path: "folder/nested/file.txt".to_string(),
                kind: SourceEntryKind::File,
                size: Some(4),
                modified: None,
            }],
            "folder/",
            "imported",
        );

        assert_eq!(
            items,
            vec![StageBatchItem {
                source_path: "folder/nested/file.txt".to_string(),
                target_key: "imported/nested/file.txt".to_string(),
            }]
        );
    }

    #[test]
    fn batch_enforces_cap() {
        assert!(ensure_batch_capacity(999, 1, 1000).is_ok());
        assert!(matches!(
            ensure_batch_capacity(1000, 1, 1000),
            Err(ServerError::BadRequestReason(message)) if message.contains("1000")
        ));
    }

    #[tokio::test]
    async fn batch_rejects_cap() {
        let test = setup_state().await;
        let items = (0..1001)
            .map(|index| StageBatchItem {
                source_path: format!("source-{index}"),
                target_key: format!("target-{index}"),
            })
            .collect();

        let result = stage_batch(
            State(test.state),
            Extension(Some(test.auth_with_source_read)),
            Json(StageBatchRequest {
                group_id: test.bucket_group_id.to_string(),
                node_id: None,
                connector_id: test.connector_id.to_string(),
                bucket: test.bucket,
                strategy: ApiStagingStrategy::Snapshot,
                items: Some(items),
                prefixes: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
    }

    #[tokio::test]
    async fn batch_rejects_node() {
        let test = setup_state().await;
        let other_node = iroh::SecretKey::from_bytes(&[17u8; 32]).public();

        let result = stage_batch(
            State(test.state),
            Extension(Some(test.auth_with_source_read)),
            Json(StageBatchRequest {
                group_id: test.bucket_group_id.to_string(),
                node_id: Some(other_node.to_string()),
                connector_id: test.connector_id.to_string(),
                bucket: test.bucket,
                strategy: ApiStagingStrategy::Snapshot,
                items: None,
                prefixes: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
    }

    #[tokio::test]
    async fn batch_sync_unimplemented() {
        let test = setup_state().await;

        let result = stage_batch(
            State(test.state),
            Extension(Some(test.auth_with_source_read)),
            Json(StageBatchRequest {
                group_id: test.bucket_group_id.to_string(),
                node_id: None,
                connector_id: test.connector_id.to_string(),
                bucket: test.bucket,
                strategy: ApiStagingStrategy::Sync,
                items: None,
                prefixes: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Unimplemented)));
    }

    #[test]
    fn openapi_includes_staging_path() {
        let openapi = ApiDoc::openapi();

        assert!(openapi.paths.paths.contains_key("/staging/"));
        assert!(openapi.paths.paths.contains_key("/staging/batch"));
        assert!(!openapi.paths.paths.contains_key("/blobs/staging"));
    }

    async fn setup_state() -> TestState {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let realm_signing_key = ed25519_dalek::SigningKey::from_bytes(&[5u8; 32]);
        let realm_id =
            aruna_core::structs::RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::from_bytes(&[13u8; 32]).public();
        let user_with_source_read = UserId::local(Ulid::r#gen(), realm_id);
        let user_without_source_read = UserId::local(Ulid::r#gen(), realm_id);
        let actor = Actor {
            node_id,
            user_id: user_with_source_read,
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

        let bucket_group_id = Ulid::r#gen();
        let source_group_id = Ulid::r#gen();
        let mut bucket_auth = GroupAuthorizationDocument::new_default_group_doc(
            user_with_source_read,
            realm_id,
            bucket_group_id,
        );
        for role in bucket_auth.roles.values_mut() {
            role.assigned_users.insert(user_without_source_read);
        }
        let mut source_auth = GroupAuthorizationDocument::new_default_group_doc(
            user_with_source_read,
            realm_id,
            source_group_id,
        );
        for role in source_auth.roles.values_mut() {
            role.assigned_users.remove(&user_without_source_read);
        }

        let bucket_group = Group {
            display_name: "bucket-group".to_string(),
            group_id: bucket_group_id,
            realm_id,
            owner: user_with_source_read,
            roles: bucket_auth.roles.keys().copied().collect(),
        };
        let source_group = Group {
            display_name: "source-group".to_string(),
            group_id: source_group_id,
            realm_id,
            owner: user_with_source_read,
            roles: source_auth.roles.keys().copied().collect(),
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        let realm_config_target = DocumentSyncTarget::RealmConfig { realm_id };

        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            realm_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            realm_config_target.storage_keyspace(),
            realm_config_target.storage_key(),
            realm_config.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            bucket_group_id.to_bytes().into(),
            bucket_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            source_group_id.to_bytes().into(),
            source_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            GROUP_KEYSPACE,
            bucket_group_id.to_bytes().into(),
            bucket_group.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            GROUP_KEYSPACE,
            source_group_id.to_bytes().into(),
            source_group.to_bytes(&actor).unwrap().into(),
        )
        .await;

        let bucket = "stage-bucket".to_string();
        let key = "test.txt".to_string();
        let connector_id = Ulid::r#gen();
        let source_path = "folder/file.txt".to_string();
        let bucket_info = BucketInfo {
            group_id: bucket_group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user_with_source_read,
            cors_configuration: None,
        };
        write_doc(
            &driver_ctx,
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec().into(),
            bucket_info.to_bytes().unwrap().into(),
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

        let target_path = crate::auth::bucket_blob_permission_path(
            state.as_ref(),
            bucket_group_id,
            &bucket,
            &key,
        );
        let source_path_restriction = source_connector_permission_path(
            state.as_ref(),
            bucket_group_id,
            connector_id,
            &source_path,
        );

        TestState {
            _storage_dir: storage_dir,
            state,
            bucket_group_id,
            connector_id,
            source_path,
            bucket,
            key,
            auth_with_source_read: AuthContext {
                user_id: user_with_source_read,
                realm_id,
                path_restrictions: Some(vec![
                    PathRestriction {
                        pattern: target_path.clone(),
                        permission: Permission::WRITE,
                    },
                    PathRestriction {
                        pattern: source_path_restriction,
                        permission: Permission::READ,
                    },
                ]),
            },
            auth_without_source_read: AuthContext {
                user_id: user_without_source_read,
                realm_id,
                path_restrictions: Some(vec![PathRestriction {
                    pattern: target_path,
                    permission: Permission::WRITE,
                }]),
            },
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

    async fn read_doc(
        driver_ctx: &Arc<DriverContext>,
        key_space: &str,
        key: byteview::ByteView,
    ) -> Option<byteview::ByteView> {
        let event = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key,
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            panic!("unexpected storage event")
        };

        value
    }
}
