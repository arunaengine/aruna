use crate::auth::{ValidatedArunaBearerTokenCarrier, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::{
    ArunaArn, AuthContext, BucketInfo, Permission, SyncMode, SyncRelationship, SyncState,
    SyncStatusSnapshot, blob_bucket_permission_path, ensure_confined_relative_path,
};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::driver::drive;
use aruna_operations::metadata::MetadataAuthToken;
use aruna_operations::replication::protocol::ReplicationMode;
use aruna_operations::replication::queue::{QueueBlobReplicationOperation, relationship_job_stats};
use aruna_operations::replication::version_replication::{
    ReplicateScopeInput, ReplicateScopeTarget,
};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::sync_mirror_repair::{
    SyncMirrorRepairIntent, clear_mirror_repair, delete_sync_mirror, kick_mirror_repair,
    request_sync_mirror_create, stage_mirror_delete, stage_mirror_reconcile, store_sync_status,
};
use aruna_operations::sync_relationship::{
    DeleteSyncRelationshipOperation, GetSyncRelationshipOperation, ListSyncRelationshipsOperation,
    StoreSyncRelationshipOperation, SyncRelationshipDirection, SyncRelationshipError,
    create_sync_relationship, remove_outgoing_relationship,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path as StdPath;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::warn;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "sync", description = "S3 bucket synchronization")),
    paths(create_sync, list_sync, get_sync, run_sync, delete_sync)
)]
pub struct SyncApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/data/sync-relationships", post(create_sync).get(list_sync))
        .route(
            "/data/sync-relationships/{id}",
            get(get_sync).delete(delete_sync),
        )
        .route("/data/sync-relationships/{id}/run", post(run_sync))
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
pub struct SyncSourceRequest {
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
pub struct SyncTargetRequest {
    pub node_id: String,
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiSyncMode {
    Once,
    Reference,
    Continuous,
}

impl From<ApiSyncMode> for SyncMode {
    fn from(value: ApiSyncMode) -> Self {
        match value {
            ApiSyncMode::Once => Self::Once,
            ApiSyncMode::Reference => Self::Reference,
            ApiSyncMode::Continuous => Self::Continuous,
        }
    }
}

impl From<SyncMode> for ApiSyncMode {
    fn from(value: SyncMode) -> Self {
        match value {
            SyncMode::Once => Self::Once,
            SyncMode::Reference => Self::Reference,
            SyncMode::Continuous => Self::Continuous,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
pub struct CreateSyncRequest {
    pub source: SyncSourceRequest,
    pub target: SyncTargetRequest,
    pub mode: ApiSyncMode,
    #[serde(default)]
    pub replicate_deletes: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SyncCountersResponse {
    pub versions_synced: u64,
    pub bytes_synced: u64,
    pub failures: u64,
    pub consecutive_failures: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SyncStatusResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_synced_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub counters: SyncCountersResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SyncRelationshipResponse {
    pub id: String,
    pub source: String,
    pub target: String,
    pub mode: ApiSyncMode,
    pub replicate_deletes: bool,
    pub created_by: String,
    pub created_at: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    pub status: SyncStatusResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SyncListResponse {
    pub outgoing: Vec<SyncRelationshipResponse>,
    pub incoming: Vec<SyncRelationshipResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SyncDetailResponse {
    pub relationship: SyncRelationshipResponse,
    pub pending_jobs: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_lag_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_synced_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SyncRunResponse {
    pub relationship_id: String,
    pub queued: usize,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncDirectionParam {
    Out,
    In,
    #[default]
    Both,
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SyncListParams {
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub direction: SyncDirectionParam,
}

#[utoipa::path(
    post,
    path = "/data/sync-relationships",
    tag = "sync",
    request_body = CreateSyncRequest,
    responses(
        (status = 201, description = "Sync relationship created", body = SyncRelationshipResponse),
        (status = 400, description = "Invalid relationship", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Bucket not found", body = ErrorResponse),
        (status = 409, description = "Duplicate relationship", body = ErrorResponse),
        (status = 502, description = "Target unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Json(request): Json<CreateSyncRequest>,
) -> ServerResult<(StatusCode, Json<SyncRelationshipResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    validate_endpoint(&request.source.bucket, request.source.prefix.as_deref())?;
    validate_endpoint(&request.target.bucket, request.target.prefix.as_deref())?;

    let target_node = NodeId::from_str(&request.target.node_id)
        .map_err(|_| ServerError::BadRequestReason("invalid target node id".to_string()))?;
    let source = make_endpoint(
        state.get_realm_id(),
        state.get_node_id(),
        &request.source.bucket,
        request.source.prefix.as_deref(),
    )?;
    let target = make_endpoint(
        state.get_realm_id(),
        target_node,
        &request.target.bucket,
        request.target.prefix.as_deref(),
    )?;
    if source == target {
        return Err(ServerError::BadRequestReason(
            "sync source and target must differ".to_string(),
        ));
    }

    let source_info = load_bucket(&state, &request.source.bucket).await?;
    ensure_permission(
        &state,
        &auth,
        blob_bucket_permission_path(
            state.get_realm_id(),
            source_info.group_id,
            state.get_node_id(),
            &request.source.bucket,
        ),
        Permission::READ,
    )
    .await?;

    let mode = SyncMode::from(request.mode);
    let existing = list_relationships(
        &state,
        SyncRelationshipDirection::Outgoing,
        Some(request.source.bucket.clone()),
    )
    .await?;
    if existing.iter().any(|relationship| {
        relationship.state != SyncState::Detached
            && relationship.source == source
            && relationship.target == target
            && relationship.mode == mode
    }) {
        return Err(ServerError::Conflict(
            "sync relationship already exists".to_string(),
        ));
    }

    let relationship = SyncRelationship {
        id: Ulid::generate(),
        source,
        target,
        mode,
        replicate_deletes: request.replicate_deletes,
        created_by: auth.user_id,
        created_at: SystemTime::now(),
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    };
    let context = state.get_ctx();
    stage_mirror_reconcile(&context, &relationship)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    if let Err(error) = create_mirror(
        &state,
        &auth,
        &bearer,
        source_info.group_id,
        relationship.clone(),
    )
    .await
    {
        kick_mirror_repair(&context).await;
        return Err(error);
    }

    if let Err(error) = create_sync_relationship(&context, relationship.clone())
        .await
        .map_err(map_create_error)
    {
        if let Err(stage_error) = stage_mirror_delete(&context, &relationship).await {
            warn!(%stage_error, relationship_id = %relationship.id, "Failed to stage sync mirror rollback");
        }
        kick_mirror_repair(&context).await;
        if remove_mirror(&state, &relationship).await {
            clear_repair(&state, &relationship, SyncMirrorRepairIntent::Delete).await;
        }
        return Err(error);
    }

    if matches!(relationship.mode, SyncMode::Once | SyncMode::Reference)
        && let Err(error) = queue_relationship(&state, &auth, &relationship).await
    {
        if stage_mirror_delete(&context, &relationship).await.is_ok() {
            let _ = delete_relationship(
                &state,
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            )
            .await;
            kick_mirror_repair(&context).await;
            if remove_mirror(&state, &relationship).await {
                clear_repair(&state, &relationship, SyncMirrorRepairIntent::Delete).await;
            }
        }
        return Err(error);
    }

    clear_repair(&state, &relationship, SyncMirrorRepairIntent::Reconcile).await;

    Ok((StatusCode::CREATED, Json(map_relationship(&relationship))))
}

#[utoipa::path(
    get,
    path = "/data/sync-relationships",
    tag = "sync",
    params(
        ("bucket" = Option<String>, Query, description = "Endpoint bucket filter"),
        ("prefix" = Option<String>, Query, description = "Overlapping endpoint prefix filter"),
        ("direction" = Option<String>, Query, description = "out, in, or both (default)")
    ),
    responses(
        (status = 200, description = "Local sync relationships", body = SyncListResponse),
        (status = 400, description = "Invalid filter", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(params): Query<SyncListParams>,
) -> ServerResult<Json<SyncListResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    if params.bucket.as_deref().is_some_and(str::is_empty)
        || params.prefix.as_deref().is_some_and(str::is_empty)
    {
        return Err(ServerError::BadRequest);
    }

    let outgoing = if matches!(
        params.direction,
        SyncDirectionParam::Out | SyncDirectionParam::Both
    ) {
        list_relationships(
            &state,
            SyncRelationshipDirection::Outgoing,
            params.bucket.clone(),
        )
        .await?
    } else {
        Vec::new()
    };
    let incoming = if matches!(
        params.direction,
        SyncDirectionParam::In | SyncDirectionParam::Both
    ) {
        list_relationships(
            &state,
            SyncRelationshipDirection::Incoming,
            params.bucket.clone(),
        )
        .await?
    } else {
        Vec::new()
    };

    Ok(Json(SyncListResponse {
        outgoing: filter_relationships(
            outgoing,
            auth.user_id,
            SyncRelationshipDirection::Outgoing,
            params.prefix.as_deref(),
        ),
        incoming: filter_relationships(
            incoming,
            auth.user_id,
            SyncRelationshipDirection::Incoming,
            params.prefix.as_deref(),
        ),
    }))
}

#[utoipa::path(
    get,
    path = "/data/sync-relationships/{id}",
    tag = "sync",
    params(("id" = String, Path, description = "Relationship ULID")),
    responses(
        (status = 200, description = "Sync relationship status", body = SyncDetailResponse),
        (status = 400, description = "Invalid relationship id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Relationship not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<Json<SyncDetailResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let id = parse_id(&id)?;
    let (relationship, _) = load_relationship(&state, id).await?;
    ensure_creator(&auth, &relationship)?;
    let (pending_jobs, oldest_lag_ms) = load_job_stats(&state, id).await?;
    let last_synced_at = map_time(relationship.status.last_synced_at);
    let last_error = relationship.status.last_error.clone();

    Ok(Json(SyncDetailResponse {
        relationship: map_relationship(&relationship),
        pending_jobs,
        oldest_lag_ms,
        last_synced_at,
        last_error,
    }))
}

#[utoipa::path(
    post,
    path = "/data/sync-relationships/{id}/run",
    tag = "sync",
    params(("id" = String, Path, description = "Relationship ULID")),
    responses(
        (status = 202, description = "Backfill queued", body = SyncRunResponse),
        (status = 400, description = "Relationship cannot run here", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Relationship not found", body = ErrorResponse),
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<(StatusCode, Json<SyncRunResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let id = parse_id(&id)?;
    let mut relationship =
        get_relationship(&state, id, SyncRelationshipDirection::Outgoing).await?;
    ensure_creator(&auth, &relationship)?;
    ensure_source_read(&state, &auth, &relationship).await?;
    if matches!(relationship.state, SyncState::Failed { .. }) {
        relationship.state = SyncState::Enabled;
        relationship.status.last_error = None;
        relationship.status.counters.consecutive_failures = 0;
        let context = state.get_ctx();
        if !store_sync_status(&context, &relationship)
            .await
            .map_err(|error| ServerError::InternalError(error.to_string()))?
        {
            return Err(ServerError::NotFound);
        }
        kick_mirror_repair(&context).await;
    }
    let queued = queue_relationship(&state, &auth, &relationship).await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(SyncRunResponse {
            relationship_id: id.to_string(),
            queued,
        }),
    ))
}

#[utoipa::path(
    delete,
    path = "/data/sync-relationships/{id}",
    tag = "sync",
    params(("id" = String, Path, description = "Relationship ULID")),
    responses(
        (status = 204, description = "Relationship removed; synchronized data is retained"),
        (status = 400, description = "Invalid relationship id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Relationship not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    let id = parse_id(&id)?;
    let (relationship, direction) = load_relationship(&state, id).await?;
    ensure_creator(&auth, &relationship)?;

    let context = state.get_ctx();
    stage_mirror_delete(&context, &relationship)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    match direction {
        // Reference relationships leave a detached serving stub behind so
        // that data retained by the target stays readable; other modes are
        // removed outright.
        SyncRelationshipDirection::Outgoing => {
            remove_outgoing_relationship(&context, relationship.clone())
                .await
                .map_err(|error| ServerError::InternalError(error.to_string()))?;
        }
        SyncRelationshipDirection::Incoming => {
            delete_relationship(&state, relationship.clone(), direction).await?;
        }
    }
    kick_mirror_repair(&context).await;
    if remove_mirror(&state, &relationship).await {
        clear_repair(&state, &relationship, SyncMirrorRepairIntent::Delete).await;
    }

    Ok(StatusCode::NO_CONTENT)
}

fn validate_endpoint(bucket: &str, prefix: Option<&str>) -> ServerResult<()> {
    if bucket.is_empty() || bucket.contains('/') {
        return Err(ServerError::BadRequestReason(
            "bucket must be non-empty and must not contain '/'".to_string(),
        ));
    }
    if bucket.starts_with("ws-") {
        return Err(ServerError::BadRequestReason(
            "workspace buckets cannot be synchronized".to_string(),
        ));
    }
    if let Some(prefix) = prefix {
        if prefix.is_empty() {
            return Err(ServerError::BadRequestReason(
                "prefix must be non-empty when provided".to_string(),
            ));
        }
        // Replicated keys inherit the prefix via the sync key mapping, so the
        // same confinement rules as object keys must hold here; otherwise
        // replication produces keys that normal S3 operations reject.
        ensure_confined_relative_path(StdPath::new(prefix))
            .map_err(|error| ServerError::BadRequestReason(format!("invalid prefix: {error}")))?;
    }
    Ok(())
}

fn make_endpoint(
    realm_id: aruna_core::structs::RealmId,
    node_id: NodeId,
    bucket: &str,
    prefix: Option<&str>,
) -> ServerResult<ArunaArn> {
    match prefix {
        Some(prefix) => ArunaArn::s3_object_prefix(realm_id, node_id, bucket, prefix),
        None => ArunaArn::s3_bucket(realm_id, node_id, bucket),
    }
    .map_err(|error| ServerError::BadRequestReason(error.to_string()))
}

async fn load_bucket(state: &ServerState, bucket: &str) -> ServerResult<BucketInfo> {
    match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(bucket_info)) => Ok(bucket_info),
        Ok(None) | Err(GetBucketInfoError::NotFound) => Err(ServerError::NotFound),
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

async fn ensure_source_read(
    state: &ServerState,
    auth: &AuthContext,
    relationship: &SyncRelationship,
) -> ServerResult<()> {
    let bucket = relationship
        .source
        .bucket()
        .ok_or_else(|| ServerError::BadRequestReason("invalid source ARN".to_string()))?;
    let bucket_info = load_bucket(state, bucket).await?;
    ensure_permission(
        state,
        auth,
        blob_bucket_permission_path(
            state.get_realm_id(),
            bucket_info.group_id,
            state.get_node_id(),
            bucket,
        ),
        Permission::READ,
    )
    .await
}

async fn create_mirror(
    state: &ServerState,
    auth: &AuthContext,
    bearer: &ValidatedArunaBearerTokenCarrier,
    source_group_id: Ulid,
    relationship: SyncRelationship,
) -> ServerResult<()> {
    if relationship.target.node_id == state.get_node_id() {
        let bucket = relationship
            .target
            .bucket()
            .ok_or(ServerError::BadRequest)?;
        let bucket_info = load_bucket(state, bucket).await?;
        ensure_permission(
            state,
            auth,
            blob_bucket_permission_path(
                state.get_realm_id(),
                bucket_info.group_id,
                state.get_node_id(),
                bucket,
            ),
            Permission::WRITE,
        )
        .await?;
        return store_relationship(state, relationship, SyncRelationshipDirection::Incoming)
            .await
            .map(|_| ());
    }

    let auth_token = MetadataAuthToken::bearer(bearer.as_str().to_string())
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    request_sync_mirror_create(
        &state.get_ctx(),
        relationship.target.node_id,
        auth_token,
        source_group_id,
        relationship,
    )
    .await
    .map_err(map_mirror_error)
}

async fn remove_mirror(state: &ServerState, relationship: &SyncRelationship) -> bool {
    if let Err(error) =
        delete_sync_mirror(&state.get_ctx(), state.get_node_id(), relationship).await
    {
        warn!(%error, relationship_id = %relationship.id, "Failed to remove remote sync mirror");
        return false;
    }
    true
}

async fn clear_repair(
    state: &ServerState,
    relationship: &SyncRelationship,
    expected: SyncMirrorRepairIntent,
) {
    let context = state.get_ctx();
    if let Err(error) = clear_mirror_repair(&context, relationship, expected).await {
        warn!(%error, relationship_id = %relationship.id, "Failed to clear sync mirror repair");
        kick_mirror_repair(&context).await;
    }
}

fn map_mirror_error(error: MetadataError) -> ServerError {
    let message = match &error {
        MetadataError::Backend(message) | MetadataError::InvalidInput(message) => message.as_str(),
        _ => return ServerError::BadGateway,
    };
    match message {
        "access_denied" => ServerError::Forbidden,
        "not_found" => ServerError::NotFound,
        "invalid_relationship" => ServerError::BadRequest,
        _ => ServerError::BadGateway,
    }
}

fn map_create_error(error: SyncRelationshipError) -> ServerError {
    match error {
        SyncRelationshipError::Duplicate => {
            ServerError::Conflict("sync relationship already exists".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

async fn queue_relationship(
    state: &ServerState,
    auth: &AuthContext,
    relationship: &SyncRelationship,
) -> ServerResult<usize> {
    let bucket = relationship
        .source
        .bucket()
        .ok_or_else(|| ServerError::BadRequestReason("invalid source ARN".to_string()))?;
    let target = relationship
        .source
        .key_prefix()
        .map(|prefix| ReplicateScopeTarget::Prefix(prefix.to_string()))
        .unwrap_or(ReplicateScopeTarget::Bucket);
    let result = drive(
        QueueBlobReplicationOperation::new_relationship(
            ReplicateScopeInput {
                bucket: bucket.to_string(),
                target,
                target_node_id: relationship.target.node_id,
                auth_context: auth.clone(),
                replicate_delete_markers: relationship.mode != SyncMode::Once
                    && relationship.replicate_deletes,
                mode: ReplicationMode::OnDemand,
            },
            None,
            relationship.id,
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(result.queued)
}

async fn store_relationship(
    state: &ServerState,
    relationship: SyncRelationship,
    direction: SyncRelationshipDirection,
) -> ServerResult<SyncRelationship> {
    drive(
        StoreSyncRelationshipOperation::new(relationship, direction),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))
}

async fn delete_relationship(
    state: &ServerState,
    relationship: SyncRelationship,
    direction: SyncRelationshipDirection,
) -> ServerResult<()> {
    drive(
        DeleteSyncRelationshipOperation::new(relationship, direction),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))
}

async fn list_relationships(
    state: &ServerState,
    direction: SyncRelationshipDirection,
    bucket: Option<String>,
) -> ServerResult<Vec<SyncRelationship>> {
    drive(
        ListSyncRelationshipsOperation::new(direction, bucket),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))
}

/// Detached stubs only keep retained reference data readable; the management
/// API treats them exactly like removed relationships.
fn visible(relationship: SyncRelationship) -> ServerResult<SyncRelationship> {
    if relationship.state == SyncState::Detached {
        return Err(ServerError::NotFound);
    }
    Ok(relationship)
}

async fn get_relationship(
    state: &ServerState,
    id: Ulid,
    direction: SyncRelationshipDirection,
) -> ServerResult<SyncRelationship> {
    drive(
        GetSyncRelationshipOperation::new(id, direction),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        SyncRelationshipError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })
    .and_then(visible)
}

async fn load_relationship(
    state: &ServerState,
    id: Ulid,
) -> ServerResult<(SyncRelationship, SyncRelationshipDirection)> {
    match drive(
        GetSyncRelationshipOperation::new(id, SyncRelationshipDirection::Outgoing),
        &state.get_ctx(),
    )
    .await
    {
        Ok(relationship) => Ok((visible(relationship)?, SyncRelationshipDirection::Outgoing)),
        Err(SyncRelationshipError::NotFound) => {
            get_relationship(state, id, SyncRelationshipDirection::Incoming)
                .await
                .map(|relationship| (relationship, SyncRelationshipDirection::Incoming))
        }
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

async fn load_job_stats(state: &ServerState, id: Ulid) -> ServerResult<(usize, Option<u64>)> {
    let (pending, oldest) = relationship_job_stats(&state.get_ctx(), id)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok((
        pending,
        oldest.map(|oldest| unix_timestamp_millis().saturating_sub(oldest)),
    ))
}

fn filter_relationships(
    relationships: Vec<SyncRelationship>,
    user_id: aruna_core::UserId,
    direction: SyncRelationshipDirection,
    prefix: Option<&str>,
) -> Vec<SyncRelationshipResponse> {
    relationships
        .into_iter()
        .filter(|relationship| relationship.state != SyncState::Detached)
        .filter(|relationship| relationship.created_by == user_id)
        .filter(|relationship| {
            prefix.is_none_or(|prefix| {
                let endpoint = match direction {
                    SyncRelationshipDirection::Outgoing => &relationship.source,
                    SyncRelationshipDirection::Incoming => &relationship.target,
                };
                endpoint.key_prefix().is_none_or(|relationship_prefix| {
                    relationship_prefix.starts_with(prefix)
                        || prefix.starts_with(relationship_prefix)
                })
            })
        })
        .map(|relationship| map_relationship(&relationship))
        .collect()
}

fn ensure_creator(auth: &AuthContext, relationship: &SyncRelationship) -> ServerResult<()> {
    if relationship.created_by == auth.user_id {
        Ok(())
    } else {
        Err(ServerError::Forbidden)
    }
}

fn parse_id(id: &str) -> ServerResult<Ulid> {
    Ulid::from_string(id)
        .map_err(|_| ServerError::BadRequestReason("invalid relationship id".to_string()))
}

fn map_relationship(relationship: &SyncRelationship) -> SyncRelationshipResponse {
    let (state, failure_reason) = match &relationship.state {
        SyncState::Enabled => ("enabled", None),
        SyncState::Paused => ("paused", None),
        SyncState::Failed { reason } => ("failed", Some(reason.clone())),
        SyncState::Detached => ("detached", None),
    };
    SyncRelationshipResponse {
        id: relationship.id.to_string(),
        source: relationship.source.to_string(),
        target: relationship.target.to_string(),
        mode: relationship.mode.into(),
        replicate_deletes: relationship.replicate_deletes,
        created_by: relationship.created_by.to_string(),
        created_at: map_time(Some(relationship.created_at)).unwrap_or_default(),
        state: state.to_string(),
        failure_reason,
        status: SyncStatusResponse {
            last_synced_at: map_time(relationship.status.last_synced_at),
            last_error: relationship.status.last_error.clone(),
            counters: SyncCountersResponse {
                versions_synced: relationship.status.counters.versions_synced,
                bytes_synced: relationship.status.counters.bytes_synced,
                failures: relationship.status.counters.failures,
                consecutive_failures: relationship.status.counters.consecutive_failures,
            },
        },
    }
}

fn map_time(value: Option<SystemTime>) -> Option<String> {
    value.map(|value| DateTime::<Utc>::from(value).to_rfc3339())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::SYNC_MIRROR_REPAIR_KEYSPACE;
    use aruna_core::structs::{NodeCapabilities, RealmId};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::storage::FjallStorage;
    use tempfile::TempDir;

    fn test_node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn test_realm() -> RealmId {
        RealmId::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[1u8; 32])
                .verifying_key()
                .to_bytes(),
        )
    }

    fn test_relationship() -> SyncRelationship {
        let realm_id = test_realm();
        SyncRelationship {
            id: Ulid::from_bytes([2u8; 16]),
            source: ArunaArn::s3_object_prefix(realm_id, test_node(3), "source", "selected/")
                .unwrap(),
            target: ArunaArn::s3_object_prefix(realm_id, test_node(4), "target", "replica/")
                .unwrap(),
            mode: SyncMode::Continuous,
            replicate_deletes: true,
            created_by: UserId::local(Ulid::from_bytes([5u8; 16]), realm_id),
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        }
    }

    async fn test_state() -> (TempDir, Arc<ServerState>, AuthContext, SyncRelationship) {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let relationship = test_relationship();
        let realm_id = relationship.source.realm_id;
        let node_id = relationship.source.node_id;
        let state = Arc::new(
            ServerState::new(
                Arc::new(DriverContext {
                    storage_handle: storage,
                    net_handle: None,
                    blob_handle: None,
                    metadata_handle: None,
                    task_handle: None,
                    compute_handle: None,
                }),
                realm_id,
                node_id,
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
                JobsRuntime::new(),
            )
            .await,
        );
        let auth = AuthContext {
            user_id: relationship.created_by,
            realm_id,
            path_restrictions: None,
        };
        (storage_dir, state, auth, relationship)
    }

    #[test]
    fn rejects_workspace_endpoints() {
        assert!(validate_endpoint("ws-temporary", None).is_err());
        assert!(validate_endpoint("bucket", Some("")).is_err());
        assert!(validate_endpoint("bucket/name", None).is_err());
        assert!(validate_endpoint("bucket", Some("selected/")).is_ok());
    }

    #[test]
    fn rejects_unsafe_prefixes() {
        assert!(validate_endpoint("bucket", Some("../escape")).is_err());
        assert!(validate_endpoint("bucket", Some("nested/../escape")).is_err());
        assert!(validate_endpoint("bucket", Some("/absolute")).is_err());
        assert!(validate_endpoint("bucket", Some("with\u{7}control")).is_err());
        assert!(validate_endpoint("bucket", Some("nested/prefix/")).is_ok());
    }

    #[test]
    fn serializes_canonical_arns() {
        let relationship = test_relationship();
        let response = map_relationship(&relationship);

        assert_eq!(
            ArunaArn::parse(&response.source).unwrap(),
            relationship.source
        );
        assert_eq!(
            ArunaArn::parse(&response.target).unwrap(),
            relationship.target
        );
    }

    #[test]
    fn filters_prefix_overlap() {
        let relationship = test_relationship();
        let results = filter_relationships(
            vec![relationship.clone()],
            relationship.created_by,
            SyncRelationshipDirection::Outgoing,
            Some("selected/nested"),
        );
        assert_eq!(results.len(), 1);

        let results = filter_relationships(
            vec![relationship.clone()],
            relationship.created_by,
            SyncRelationshipDirection::Outgoing,
            Some("other"),
        );
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn lists_stored_relationship() {
        let (_storage_dir, state, auth, relationship) = test_state().await;
        drive(
            StoreSyncRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            ),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        let Json(response) = list_sync(
            State(state),
            Extension(Some(auth)),
            Query(SyncListParams::default()),
        )
        .await
        .unwrap();

        assert_eq!(response.outgoing.len(), 1);
        assert!(response.incoming.is_empty());
        assert_eq!(
            ArunaArn::parse(&response.outgoing[0].source).unwrap(),
            relationship.source
        );
    }

    #[tokio::test]
    async fn rejects_invalid_id() {
        let (_storage_dir, state, auth, _) = test_state().await;
        let error = get_sync(
            State(state),
            Extension(Some(auth)),
            Path("invalid".to_string()),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, ServerError::BadRequestReason(_)));
    }

    #[tokio::test]
    async fn delete_reference_detaches_serving_stub() {
        let (_storage_dir, state, auth, mut relationship) = test_state().await;
        relationship.mode = SyncMode::Reference;
        drive(
            StoreSyncRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            ),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        assert_eq!(
            delete_sync(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Path(relationship.id.to_string()),
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );

        // The outgoing record survives as a detached serving stub ...
        let stored = drive(
            GetSyncRelationshipOperation::new(relationship.id, SyncRelationshipDirection::Outgoing),
            &state.get_ctx(),
        )
        .await
        .unwrap();
        assert_eq!(stored.state, SyncState::Detached);

        // ... but the management API treats the relationship as removed.
        assert!(matches!(
            get_sync(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Path(relationship.id.to_string()),
            )
            .await,
            Err(ServerError::NotFound)
        ));
        let Json(listed) = list_sync(
            State(state),
            Extension(Some(auth)),
            Query(SyncListParams::default()),
        )
        .await
        .unwrap();
        assert!(listed.outgoing.is_empty());
    }

    #[tokio::test]
    async fn delete_stages_repair() {
        let (_storage_dir, state, auth, relationship) = test_state().await;
        drive(
            StoreSyncRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            ),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        assert_eq!(
            delete_sync(
                State(state.clone()),
                Extension(Some(auth)),
                Path(relationship.id.to_string()),
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
                key: relationship.id.to_bytes().to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing mirror repair read result");
        };
        assert!(value.is_some());
        assert!(matches!(
            get_relationship(&state, relationship.id, SyncRelationshipDirection::Outgoing,).await,
            Err(ServerError::NotFound)
        ));
    }
}
