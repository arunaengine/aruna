use crate::auth::{
    ValidatedArunaBearerTokenCarrier, ensure_permission, ensure_permission_with,
    require_unrestricted_realm_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::{
    ArunaArn, AuthContext, BucketInfo, Permission, ReferenceHandling, SyncMode, SyncRelationship,
    SyncState, SyncStatusSnapshot, blob_bucket_permission_path, ensure_confined_relative_path,
};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::driver::drive;
use aruna_operations::metadata::MetadataAuthToken;
use aruna_operations::replication::protocol::ReplicationMode;
use aruna_operations::replication::queue::{QueueBlobReplicationOperation, relationship_job_stats};
use aruna_operations::replication::version_replication::{
    ReplicateScopeInput, ReplicateScopeTarget,
};
use aruna_operations::request_policy::PolicyRequestExtras;
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
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path as StdPath;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::warn;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags((name = "sync", description = "S3 bucket synchronization"))
)]
pub struct SyncApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(SyncApiDoc::openapi())
        .routes(routes!(create_sync, list_sync))
        .routes(routes!(get_sync, update_sync, delete_sync))
        .routes(routes!(run_sync))
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

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiReferenceHandling {
    #[default]
    Materialize,
    Preserve,
    Skip,
}

impl From<ApiReferenceHandling> for ReferenceHandling {
    fn from(value: ApiReferenceHandling) -> Self {
        match value {
            ApiReferenceHandling::Materialize => Self::Materialize,
            ApiReferenceHandling::Preserve => Self::Preserve,
            ApiReferenceHandling::Skip => Self::Skip,
        }
    }
}

impl From<ReferenceHandling> for ApiReferenceHandling {
    fn from(value: ReferenceHandling) -> Self {
        match value {
            ReferenceHandling::Materialize => Self::Materialize,
            ReferenceHandling::Preserve => Self::Preserve,
            ReferenceHandling::Skip => Self::Skip,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
pub struct CreateSyncRequest {
    pub source: SyncSourceRequest,
    pub target: SyncTargetRequest,
    pub mode: ApiSyncMode,
    #[serde(default)]
    pub reference_handling: ApiReferenceHandling,
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
    pub reference_handling: ApiReferenceHandling,
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

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
pub struct UpdateSyncRequest {
    pub reference_handling: ApiReferenceHandling,
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
    summary = "Create a bucket sync relationship",
    description = r#"Creates a bucket sync relationship from this node to a bucket on a target node.

**Authentication**: realm bearer token; a token confined to a path subset is refused outright,
because this surface cannot honor that confinement. The source always lives on this node and needs
READ on the source bucket; the target node additionally checks WRITE on the target bucket before it
accepts its half of the relationship, so a caller without write rights there is forbidden.

**Behavior**
- A 201 means the relationship record is durable here and its mirror is durable on the target node;
  no data has been copied yet.
- For `once` and `reference` mode an initial backfill run is queued as part of creation, and
  `continuous` instead replicates versions as they are written.
- `reference` mode forces `preserve` reference handling regardless of what the body asks for.
- Completion is observed by polling the relationship, whose `pending_jobs`, `last_synced_at` and
  counters advance as replication drains.

**Limits** (all refused with 400)
- Source and target must differ.
- Bucket names must be non-empty and free of `/`, and workspace buckets are refused.
- A prefix, when given, must be a confined relative path.

**Errors**: an already existing enabled relationship with the same source, target and mode is a
409. A target node that cannot be reached fails the whole creation with 502 and the caller may
retry; a partially created mirror is repaired in the background."#,
    request_body(
        content = CreateSyncRequest,
        description = "Source endpoint on this node, target endpoint on the receiving node, and the sync mode. `reference_handling` defaults to `materialize` and `replicate_deletes` to false.",
        example = json!({
            "source": {
                "bucket": "research-raw",
                "prefix": "2026/"
            },
            "target": {
                "node_id": "2a3b4c5d6e7f80910a1b2c3d4e5f60710a1b2c3d4e5f60710a1b2c3d4e5f6071",
                "bucket": "research-mirror",
                "prefix": "2026/"
            },
            "mode": "once",
            "reference_handling": "materialize",
            "replicate_deletes": false
        })
    ),
    responses(
        (
            status = 201,
            description = "The relationship as stored, with its initial backfill queued for once and reference mode; counters are still zero because nothing has replicated yet",
            body = SyncRelationshipResponse,
            example = json!({
                "id": "01JSYNC0123456789ABCDEFGHJ",
                "source": "arn:aruna:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/research-raw/2026/",
                "target": "arn:aruna:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8:2a3b4c5d6e7f80910a1b2c3d4e5f60710a1b2c3d4e5f60710a1b2c3d4e5f6071:s3/research-mirror/2026/",
                "mode": "once",
                "reference_handling": "materialize",
                "replicate_deletes": false,
                "created_by": "01JUSER01ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                "created_at": "2026-04-09T14:23:11.123+00:00",
                "state": "enabled",
                "status": {
                    "counters": {
                        "versions_synced": 0,
                        "bytes_synced": 0,
                        "failures": 0,
                        "consecutive_failures": 0
                    }
                }
            })
        ),
        (status = 400, description = "The endpoints are identical, a bucket name is empty or contains `/`, the bucket is a workspace bucket, the prefix is not a confined relative path, or the target node id does not parse", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or is path-restricted, the caller lacks READ on the source bucket, or the target node refused the mirror", body = ErrorResponse),
        (status = 404, description = "The source bucket is unknown to this node, or the target node does not know the target bucket", body = ErrorResponse),
        (status = 409, description = "An enabled relationship with the same source, target and mode already exists", body = ErrorResponse),
        (status = 502, description = "The target node could not be reached to store its mirror; nothing was created and the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Json(request): Json<CreateSyncRequest>,
) -> ServerResult<(StatusCode, Json<SyncRelationshipResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
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
    let reference_handling = if mode == SyncMode::Reference {
        ReferenceHandling::Preserve
    } else {
        request.reference_handling.into()
    };
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
        reference_handling,
        reference_serving: reference_handling == ReferenceHandling::Preserve,
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
    summary = "List the sync relationships held on this node",
    description = r#"Lists the caller's own sync relationships held on this node, split by direction.

**Authentication**: realm bearer token; a token confined to a path subset is refused.

**Behavior**
- Only relationships created by the calling user are returned, so this is not a realm-wide view and
  another user's relationships are never listed.
- `outgoing` holds relationships whose source bucket lives on this node, `incoming` holds the mirror
  records this node keeps as a replication target; a relationship therefore appears in `outgoing` on
  one node and in `incoming` on the other.
- The `prefix` filter matches an overlap in either direction, so `2026/` matches both `2026/runs/`
  and `2026`, and a relationship covering a whole bucket always matches.
- Detached serving stubs, left behind when a reference relationship is deleted, are hidden.

**Limits**
- The read is node-local and unpaginated: every matching record is returned in one response, and no
  ordering is guaranteed.
- A `bucket` or `prefix` filter that is present but empty is refused with 400."#,
    params(
        ("bucket" = Option<String>, Query, description = "Keep only relationships whose endpoint for the listed direction uses this bucket: source for outgoing, target for incoming. Omit for all buckets"),
        ("prefix" = Option<String>, Query, description = "Keep only relationships whose endpoint prefix overlaps this one, in either direction. Omit for no prefix filter"),
        ("direction" = Option<String>, Query, description = "Which lists to populate: `out` for source-side relationships, `in` for mirrors held here, `both` for either. Defaults to `both`; the list that is not requested comes back empty")
    ),
    responses(
        (
            status = 200,
            description = "The caller's own relationships held on this node, split by direction, with either list empty when it was filtered out or has no matches",
            body = SyncListResponse,
            example = json!({
                "outgoing": [
                    {
                        "id": "01JSYNC0123456789ABCDEFGHJ",
                        "source": "arn:aruna:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/research-raw/2026/",
                        "target": "arn:aruna:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8:2a3b4c5d6e7f80910a1b2c3d4e5f60710a1b2c3d4e5f60710a1b2c3d4e5f6071:s3/research-mirror/2026/",
                        "mode": "continuous",
                        "reference_handling": "materialize",
                        "replicate_deletes": true,
                        "created_by": "01JUSER01ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "created_at": "2026-04-09T14:23:11.123+00:00",
                        "state": "enabled",
                        "status": {
                            "last_synced_at": "2026-04-09T15:02:44.907+00:00",
                            "counters": {
                                "versions_synced": 128,
                                "bytes_synced": 4294967296_i64,
                                "failures": 0,
                                "consecutive_failures": 0
                            }
                        }
                    }
                ],
                "incoming": []
            })
        ),
        (status = 400, description = "The bucket or prefix filter was present but empty", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or is path-restricted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(params): Query<SyncListParams>,
) -> ServerResult<Json<SyncListResponse>> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
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
    summary = "Read one sync relationship and its progress",
    description = r#"Reads one sync relationship with this node's replication progress for it.

**Authentication**: realm bearer token; a token confined to a path subset is refused, and only the
user who created the relationship may read it, so a relationship created by somebody else is
refused rather than hidden.

**Behavior**
- The lookup checks this node's source-side records first and then its mirror records, so the same
  id can be inspected from either end on the node that holds it; a node holding neither reports it
  as not found, as does a detached serving stub left behind by a deleted reference relationship.
- `pending_jobs` and `oldest_lag_ms` come from this node's own replication queue: `pending_jobs`
  counts the replication jobs still queued for this relationship and `oldest_lag_ms` is the age in
  milliseconds of the oldest of them, omitted once the queue holds none.
- `pending_jobs` reaching zero with no `last_error` is how a caller sees an accepted run finish; the
  counters advance only as replication reports back, so they lag the objects already written."#,
    params(("id" = String, Path, description = "Relationship id as a 26-character ULID, as returned when the relationship was created")),
    responses(
        (
            status = 200,
            description = "The relationship with its queue depth and last outcome; the lag, last sync time and last error are omitted when there is nothing to report",
            body = SyncDetailResponse,
            example = json!({
                "relationship": {
                    "id": "01JSYNC0123456789ABCDEFGHJ",
                    "source": "arn:aruna:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/research-raw/2026/",
                    "target": "arn:aruna:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8:2a3b4c5d6e7f80910a1b2c3d4e5f60710a1b2c3d4e5f60710a1b2c3d4e5f6071:s3/research-mirror/2026/",
                    "mode": "once",
                    "reference_handling": "materialize",
                    "replicate_deletes": false,
                    "created_by": "01JUSER01ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                    "created_at": "2026-04-09T14:23:11.123+00:00",
                    "state": "enabled",
                    "status": {
                        "last_synced_at": "2026-04-09T15:02:44.907+00:00",
                        "counters": {
                            "versions_synced": 128,
                            "bytes_synced": 4294967296_i64,
                            "failures": 0,
                            "consecutive_failures": 0
                        }
                    }
                },
                "pending_jobs": 1,
                "oldest_lag_ms": 8421,
                "last_synced_at": "2026-04-09T15:02:44.907+00:00"
            })
        ),
        (status = 400, description = "The path segment is not a valid relationship ULID", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or is path-restricted, or the relationship was created by another user", body = ErrorResponse),
        (status = 404, description = "This node holds no relationship with that id, or it is a detached serving stub", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<Json<SyncDetailResponse>> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
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
    patch,
    path = "/data/sync-relationships/{id}",
    tag = "sync",
    summary = "Change how a relationship handles referenced objects",
    description = r#"Changes how an existing sync relationship handles referenced objects.

**Authentication**: realm bearer token; a token confined to a path subset is refused. Only the
creator may change a relationship, and READ on the source bucket is checked as well.

**Behavior**
- Only the source side can be changed: a mirror this node merely holds as a target reads as not
  found, so the call must go to the node that owns the source bucket.
- Submitting the value the relationship already has returns it unchanged and touches nothing.
- Otherwise the target node's mirror is updated first and the local record second, so an
  unreachable target fails the whole change with 502, leaves the stored value as it was, and the
  caller may retry once the target is back.
- The new handling applies to versions replicated after the change; objects already copied or
  already left as references are not rewritten, and a relationship that has ever used `preserve`
  keeps serving the references it handed out even after switching to `materialize`.

**Limits**
- Reference handling is the only mutable field; the mode, endpoints and delete behavior are fixed
  at creation.
- A relationship in `reference` mode accepts only `preserve`; anything else is refused with 400."#,
    params(("id" = String, Path, description = "Relationship id as a 26-character ULID; must name a relationship whose source bucket is on this node")),
    request_body(
        content = UpdateSyncRequest,
        description = "The new reference handling: `materialize` copies referenced source bytes, `preserve` replicates the reference itself, `skip` leaves referenced objects out of the sync.",
        example = json!({
            "reference_handling": "preserve"
        })
    ),
    responses(
        (
            status = 200,
            description = "The relationship after the change, or unchanged when the requested handling was already in effect",
            body = SyncRelationshipResponse,
            example = json!({
                "id": "01JSYNC0123456789ABCDEFGHJ",
                "source": "arn:aruna:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/research-raw/2026/",
                "target": "arn:aruna:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8:2a3b4c5d6e7f80910a1b2c3d4e5f60710a1b2c3d4e5f60710a1b2c3d4e5f6071:s3/research-mirror/2026/",
                "mode": "continuous",
                "reference_handling": "preserve",
                "replicate_deletes": false,
                "created_by": "01JUSER01ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                "created_at": "2026-04-09T14:23:11.123+00:00",
                "state": "enabled",
                "status": {
                    "counters": {
                        "versions_synced": 128,
                        "bytes_synced": 4294967296_i64,
                        "failures": 0,
                        "consecutive_failures": 0
                    }
                }
            })
        ),
        (status = 400, description = "The path segment is not a valid relationship ULID, or the relationship is in reference mode and the request asked for anything other than preserve", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or is path-restricted, the relationship was created by another user, or the caller lacks READ on the source bucket", body = ErrorResponse),
        (status = 404, description = "This node holds no source-side relationship with that id, or the source bucket has gone", body = ErrorResponse),
        (status = 502, description = "The target node could not be reached to update its mirror; the stored handling is unchanged and the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn update_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(id): Path<String>,
    Json(request): Json<UpdateSyncRequest>,
) -> ServerResult<Json<SyncRelationshipResponse>> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let id = parse_id(&id)?;
    let mut relationship =
        get_relationship(&state, id, SyncRelationshipDirection::Outgoing).await?;
    ensure_creator(&auth, &relationship)?;
    ensure_source_read(&state, &auth, &relationship).await?;

    let handling = ReferenceHandling::from(request.reference_handling);
    if relationship.mode == SyncMode::Reference && handling != ReferenceHandling::Preserve {
        return Err(ServerError::BadRequestReason(
            "reference sync mode requires preserve reference handling".to_string(),
        ));
    }
    if relationship.reference_handling == handling {
        return Ok(Json(map_relationship(&relationship)));
    }
    relationship.set_reference_handling(handling);
    let source_group_id = load_bucket(
        &state,
        relationship
            .source
            .bucket()
            .ok_or_else(|| ServerError::BadRequestReason("invalid source ARN".to_string()))?,
    )
    .await?
    .group_id;
    let context = state.get_ctx();
    stage_mirror_reconcile(&context, &relationship)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    if let Err(error) = create_mirror(
        &state,
        &auth,
        &bearer,
        source_group_id,
        relationship.clone(),
    )
    .await
    {
        kick_mirror_repair(&context).await;
        return Err(error);
    }
    let updated = match store_relationship(
        &state,
        relationship.clone(),
        SyncRelationshipDirection::Outgoing,
    )
    .await
    {
        Ok(updated) => updated,
        Err(error) => {
            kick_mirror_repair(&context).await;
            return Err(error);
        }
    };
    clear_repair(&state, &updated, SyncMirrorRepairIntent::Reconcile).await;
    Ok(Json(map_relationship(&updated)))
}

#[utoipa::path(
    post,
    path = "/data/sync-relationships/{id}/run",
    tag = "sync",
    summary = "Trigger a backfill run of an existing relationship",
    description = r#"Queues one backfill run over the whole scope of an existing relationship.

**Authentication**: realm bearer token; a token confined to a path subset is refused. Only the
creator may run a relationship, and READ on the source bucket is checked as well.

**Behavior**
- Only the source node can run it: a mirror this node merely holds as a target reads as not found.
- A relationship left in the failed state is returned to enabled first, with its last error and
  consecutive-failure count cleared, so a run is also how a stalled relationship is resumed.
- The 202 means one backfill job covering the relationship's whole scope, the source bucket or its
  prefix, is durably queued on this node; nothing has been copied and no object has been compared
  yet.
- The call is idempotent per relationship: the queued job is keyed by the relationship and its
  scope, so triggering a run while one is already in flight does not start a second pass, it
  re-arms the drain over the same job.
- `queued` reports how many scope jobs were enqueued and is 1 on every success, not a count of
  objects.
- Progress and completion are observed by polling the relationship, whose `pending_jobs` returns to
  zero when the run has drained."#,
    params(("id" = String, Path, description = "Relationship id as a 26-character ULID; must name a relationship whose source bucket is on this node")),
    responses(
        (
            status = 202,
            description = "A backfill job for the relationship is durably queued; the copy itself happens afterwards",
            body = SyncRunResponse,
            example = json!({
                "relationship_id": "01JSYNC0123456789ABCDEFGHJ",
                "queued": 1
            })
        ),
        (status = 400, description = "The path segment is not a valid relationship ULID, or the stored relationship has no usable source bucket", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or is path-restricted, the relationship was created by another user, or the caller lacks READ on the source bucket", body = ErrorResponse),
        (status = 404, description = "This node holds no source-side relationship with that id, or the source bucket has gone", body = ErrorResponse),
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<(StatusCode, Json<SyncRunResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
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
    summary = "Delete a sync relationship",
    description = r#"Deletes one end of a sync relationship from the node that holds it.

**Authentication**: realm bearer token; a token confined to a path subset is refused. Only the
creator may delete a relationship, and WRITE is checked on whichever bucket this node holds for it:
the source bucket for a source-side relationship, the target bucket for a mirror.

**Behavior**
- Either end may be deleted from the node that holds it.
- Objects already synchronized are never removed, on either side; only the relationship goes away,
  and continuous replication simply stops.
- Deleting a source-side `reference` relationship leaves a detached serving stub behind so that data
  the target still holds as references stays readable; the stub is invisible to this API, so reading
  or deleting the same id afterwards is not found. Other modes are removed outright.
- The peer is asked to drop its half as part of the call, and a peer that cannot be reached does not
  fail the delete: the removal is retried in the background until the mirror is gone."#,
    params(("id" = String, Path, description = "Relationship id as a 26-character ULID, from either end of the relationship")),
    responses(
        (status = 204, description = "The relationship is removed on this node and the peer's mirror is being dropped; the response has no body and synchronized objects are retained on both sides"),
        (status = 400, description = "The path segment is not a valid relationship ULID, or the stored relationship has no usable bucket for this node's side", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or is path-restricted, the relationship was created by another user, or the caller lacks WRITE on this node's endpoint bucket", body = ErrorResponse),
        (status = 404, description = "This node holds no relationship with that id, it is already a detached serving stub, or the endpoint bucket has gone", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let id = parse_id(&id)?;
    let (relationship, direction) = load_relationship(&state, id).await?;
    ensure_creator(&auth, &relationship)?;
    ensure_sync_write(&state, &auth, &relationship, direction).await?;

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

async fn ensure_sync_write(
    state: &ServerState,
    auth: &AuthContext,
    relationship: &SyncRelationship,
    direction: SyncRelationshipDirection,
) -> ServerResult<()> {
    let bucket = match direction {
        SyncRelationshipDirection::Outgoing => relationship.source.bucket(),
        SyncRelationshipDirection::Incoming => relationship.target.bucket(),
    }
    .ok_or_else(|| ServerError::BadRequestReason("invalid sync bucket ARN".to_string()))?;
    let bucket_info = load_bucket(state, bucket).await?;
    ensure_permission_with(
        state,
        auth,
        blob_bucket_permission_path(
            state.get_realm_id(),
            bucket_info.group_id,
            state.get_node_id(),
            bucket,
        ),
        Permission::WRITE,
        PolicyRequestExtras::operation("s3.DeleteBucketReplication"),
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
        ensure_permission_with(
            state,
            auth,
            blob_bucket_permission_path(
                state.get_realm_id(),
                bucket_info.group_id,
                state.get_node_id(),
                bucket,
            ),
            Permission::WRITE,
            PolicyRequestExtras::operation("s3.PutBucketReplication"),
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
        PolicyRequestExtras::operation("s3.PutBucketReplication"),
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
        reference_handling: relationship.reference_handling.into(),
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
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
        SYNC_MIRROR_REPAIR_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, NodeCapabilities, PathRestriction,
        RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    };
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

    fn test_group() -> Ulid {
        Ulid::from_bytes([6u8; 16])
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
            reference_handling: Default::default(),
            reference_serving: false,
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
                NodeCapabilities::user_node(realm_id).unwrap(),
                false,
                None,
                JobsRuntime::new(),
            )
            .await,
        );
        let actor = Actor {
            node_id,
            user_id: relationship.created_by,
            realm_id,
        };
        let group_id = test_group();
        let storage = &state.get_ctx().storage_handle;
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let group_auth = GroupAuthorizationDocument::new_default_group_doc(
            relationship.created_by,
            realm_id,
            group_id,
        );
        let group = aruna_core::structs::Group {
            display_name: "sync-test".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner: relationship.created_by,
        };
        // Request-policy loading fails closed without the realm config document.
        for (key_space, key, value) in [
            (
                REALM_CONFIG_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                    .to_bytes(&actor)
                    .unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                realm_auth.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group_auth.to_bytes(&actor).unwrap(),
            ),
            (
                GROUP_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
        ] {
            storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }
        for bucket in ["source", "target"] {
            storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: bucket.as_bytes().to_vec().into(),
                    value: BucketInfo {
                        group_id,
                        created_at: SystemTime::UNIX_EPOCH,
                        created_by: relationship.created_by,
                        cors_configuration: None,
                        storage_routing: Vec::new(),
                        placement_policies: Vec::new(),
                        placement_policy_generation: 0,
                    }
                    .to_bytes()
                    .unwrap()
                    .into(),
                    txn_id: None,
                })
                .await;
        }
        let auth = AuthContext {
            user_id: relationship.created_by,
            realm_id,
            path_restrictions: None,
        };
        (storage_dir, state, auth, relationship)
    }

    fn create_request(target_node: NodeId) -> CreateSyncRequest {
        CreateSyncRequest {
            source: SyncSourceRequest {
                bucket: "source".to_string(),
                prefix: None,
            },
            target: SyncTargetRequest {
                node_id: target_node.to_string(),
                bucket: "target".to_string(),
                prefix: None,
            },
            mode: ApiSyncMode::Once,
            reference_handling: ApiReferenceHandling::default(),
            replicate_deletes: false,
        }
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
    async fn rejects_restricted_run() {
        for restrictions in [
            Some(vec![PathRestriction {
                pattern: "/restricted/**".to_string(),
                permission: Permission::READ,
            }]),
            Some(Vec::new()),
        ] {
            let (_storage_dir, state, mut auth, relationship) = test_state().await;
            auth.path_restrictions = restrictions;
            let error = run_sync(
                State(state),
                Extension(Some(auth)),
                Path(relationship.id.to_string()),
            )
            .await
            .unwrap_err();
            assert!(matches!(error, ServerError::Forbidden));
        }
    }

    #[tokio::test]
    async fn rejects_restricted_control() {
        for restrictions in [
            Some(vec![PathRestriction {
                pattern: "/restricted/**".to_string(),
                permission: Permission::READ,
            }]),
            Some(Vec::new()),
        ] {
            let (_storage_dir, state, mut auth, _) = test_state().await;
            auth.path_restrictions = restrictions;

            assert!(matches!(
                list_sync(
                    State(state.clone()),
                    Extension(Some(auth.clone())),
                    Query(SyncListParams::default()),
                )
                .await,
                Err(ServerError::Forbidden)
            ));
            assert!(matches!(
                get_sync(
                    State(state.clone()),
                    Extension(Some(auth.clone())),
                    Path("invalid".to_string()),
                )
                .await,
                Err(ServerError::Forbidden)
            ));
            assert!(matches!(
                update_sync(
                    State(state.clone()),
                    Extension(Some(auth.clone())),
                    Extension(None),
                    Path("invalid".to_string()),
                    Json(UpdateSyncRequest {
                        reference_handling: ApiReferenceHandling::Materialize,
                    }),
                )
                .await,
                Err(ServerError::Forbidden)
            ));
            assert!(matches!(
                delete_sync(
                    State(state),
                    Extension(Some(auth)),
                    Path("invalid".to_string()),
                )
                .await,
                Err(ServerError::Forbidden)
            ));
        }
    }

    #[tokio::test]
    async fn rejects_restricted_create() {
        for restrictions in [
            Some(vec![PathRestriction {
                pattern: "/restricted/**".to_string(),
                permission: Permission::READ,
            }]),
            Some(Vec::new()),
        ] {
            let (_storage_dir, state, mut auth, _) = test_state().await;
            auth.path_restrictions = restrictions;
            let error = create_sync(
                State(state.clone()),
                Extension(Some(auth)),
                Extension(None),
                Json(create_request(test_node(3))),
            )
            .await
            .unwrap_err();
            assert!(matches!(error, ServerError::Forbidden));
        }
    }

    #[tokio::test]
    async fn accepts_unrestricted_create() {
        let (_storage_dir, state, auth, _) = test_state().await;
        let (status, _) = create_sync(
            State(state),
            Extension(Some(auth)),
            Extension(Some(ValidatedArunaBearerTokenCarrier::new_for_test(
                "sync-test-token",
            ))),
            Json(create_request(test_node(3))),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn mirror_denies_create() {
        let (_storage_dir, state, auth, _) = test_state().await;
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: auth.realm_id,
        };
        let mut group_auth = GroupAuthorizationDocument::new_default_group_doc(
            auth.user_id,
            auth.realm_id,
            test_group(),
        );
        group_auth
            .policies
            .push(aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "deny-sync-create".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: "operation == 's3.PutBucketReplication'".to_string(),
                enabled: true,
            });
        state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: test_group().to_bytes().to_vec().into(),
                value: group_auth.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;

        let error = create_sync(
            State(state),
            Extension(Some(auth)),
            Extension(Some(ValidatedArunaBearerTokenCarrier::new_for_test(
                "sync-test-token",
            ))),
            Json(create_request(test_node(3))),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ServerError::Forbidden));
    }

    #[tokio::test]
    async fn delete_preserve_detaches() {
        let (_storage_dir, state, auth, mut relationship) = test_state().await;
        relationship.set_reference_handling(ReferenceHandling::Preserve);
        relationship.set_reference_handling(ReferenceHandling::Materialize);
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

    #[tokio::test]
    async fn delete_respects_policy() {
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

        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: auth.realm_id,
        };
        let mut group_auth = GroupAuthorizationDocument::new_default_group_doc(
            auth.user_id,
            auth.realm_id,
            test_group(),
        );
        group_auth
            .policies
            .push(aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "deny-sync-delete".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: "operation == 's3.DeleteBucketReplication'".to_string(),
                enabled: true,
            });
        state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: test_group().to_bytes().to_vec().into(),
                value: group_auth.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;

        let error = delete_sync(
            State(state.clone()),
            Extension(Some(auth)),
            Path(relationship.id.to_string()),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ServerError::Forbidden));
        assert!(
            get_relationship(&state, relationship.id, SyncRelationshipDirection::Outgoing,)
                .await
                .is_ok()
        );
    }
}
