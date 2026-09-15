use crate::auth::{
    ValidatedBearer, blob_permission_path, ensure_permission, parse_connector_id, parse_group_id,
    require_realm_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::execution::jobs::{
    decode_cursor as decode_job_cursor, encode_cursor as encode_job_cursor, map_submit_error,
};
use crate::routes::storage::connectors::ApiConnectorKind;
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::errors::{SourceResolutionError, StagingSourceError};
use aruna_core::structs::{
    AuthContext, BucketInfo, JobPayload, JobRecord, JobState, Permission, SourceEntry,
    SourceEntryKind, StagingJobCheckpoint, StagingJobItem, StagingJobPhase, StagingJobPrefix,
    StagingJobSpec, StagingStrategy, bucket_permission_path,
};
use aruna_operations::driver::drive;
use aruna_operations::jobs::service::{list_owned_jobs, read_staging_routed, submit_staging_job};
use aruna_operations::jobs::staging::read_staging_checkpoint;
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::replication::queue::{LiveVersionInput, LiveVersionOperation};
use aruna_operations::s3::get_bucket::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::list_objects::{
    ListBucketInput, ListBucketOperation, ListContinuationToken,
};
use aruna_operations::s3::put_object::PutObjectError;
use aruna_operations::staging::head_source::HeadSourceError;
use aruna_operations::staging::list_source::{
    ListStagingError, ListStagingInput, ListStagingOperation,
};
use aruna_operations::staging::read_source::ReadSourceError;
use aruna_operations::staging::reference::{
    MaterializeReferenceError, MaterializeReferenceInput, stage_reference_blob,
};
use aruna_operations::staging::snapshot::{
    MaterializeSnapshotError, MaterializeSnapshotInput, stage_snapshot_blob,
};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use std::path::{Component, Path as FsPath};
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags((
        name = "data/staging",
        description = "Staging source objects into buckets, inline or as background jobs"
    ))
)]
pub struct StagingApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(StagingApiDoc::openapi())
        .routes(routes!(stage_blob))
        .routes(routes!(stage_batch))
        .routes(routes!(list_staging_jobs, submit_staging))
        .routes(routes!(get_staging_job))
        .routes(routes!(list_references))
}

const DEFAULT_REFERENCE_LIMIT: usize = 500;
const MAX_REFERENCE_LIMIT: usize = 1000;
const DEFAULT_JOB_LIMIT: usize = 50;
const MAX_JOB_LIMIT: usize = 200;

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct StagingJobQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ReferenceListQuery {
    pub bucket: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ReferenceListEntry {
    pub key: String,
    pub size: u64,
    pub referenced: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<ApiConnectorKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_node_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ReferenceListResponse {
    /// Includes materialized and referenced objects so clients can aggregate totals.
    pub entries: Vec<ReferenceListEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiStagingStrategy {
    Snapshot,
    Reference,
    Sync,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[schema(as = StageBlobTargetRequest)]
pub struct StageTargetRequest {
    pub group_id: String,
    pub connector_id: String,
    pub source_path: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "strategy", rename_all = "snake_case")]
#[schema(as = StageBlobRequest)]
pub enum StageRequest {
    Snapshot(StageTargetRequest),
    Reference(StageTargetRequest),
    Sync(StageTargetRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[schema(as = StageBlobResponse)]
pub struct StageResponse {
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[schema(as = SubmitStagingJobResponse)]
pub struct SubmitStagingResponse {
    pub job_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[schema(as = StagingJobProgressResponse)]
pub struct StagingProgressResponse {
    pub items_current: u64,
    pub items_total: Option<u64>,
    pub bytes_current: u64,
    pub bytes_total: Option<u64>,
    pub current_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[schema(as = StagingJobErrorResponse)]
pub struct StagingErrorResponse {
    pub source_path: String,
    pub target_key: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StagingJobResponse {
    pub job_id: String,
    pub strategy: ApiStagingStrategy,
    pub group_id: String,
    pub connector_id: String,
    pub bucket: String,
    pub state: String,
    pub phase: String,
    pub submitted_at: String,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub progress: StagingProgressResponse,
    pub errors: Vec<StagingErrorResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[schema(as = StagingJobListResponse)]
pub struct StagingListResponse {
    pub jobs: Vec<StagingJobResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[utoipa::path(
    post,
    path = "/data/staging",
    tag = "data/staging",
    summary = "Stage a single source object into a bucket",
    description = r#"Stages one source object into a bucket, either by copying its bytes now or by recording a pointer.

**Authentication**: realm bearer token with WRITE on the target bucket key and READ on the
connector's source path, both checked against the concrete path rather than a prefix alone.

**Behavior**
- `snapshot` reads the object from the source connector and commits its bytes to this node's storage
  before answering, so it is as slow as the transfer.
- `reference` records the object as a pointer to its source and copies nothing, so the bytes are
  fetched on demand at read time.
- `sync` is accepted by the schema but not implemented and always fails.
- A 201 means the new version is committed here and immediately readable through S3; propagating it
  to any sync target is queued afterwards and neither awaited nor reported here.

**Limits**
- A snapshot is charged against the realm's quota ceiling for the group."#,
    request_body(
        content = StageRequest,
        description = "The staging strategy and the source and target it applies to",
        example = json!({
            "strategy": "snapshot",
            "group_id": "01JABCDEF0123456789ABCDEFG",
            "connector_id": "01JCONNECTOR0123456789ABCD",
            "source_path": "refseq/2026/genome.fna.gz",
            "bucket": "research-raw",
            "key": "genomes/2026/genome.fna.gz"
        })
    ),
    responses(
        (
            status = 201,
            description = "The object version committed on this node; content type, entity tag and modification time are echoed from the source when it reported them",
            body = StageResponse,
            example = json!({
                "strategy": "snapshot",
                "bucket": "research-raw",
                "key": "genomes/2026/genome.fna.gz",
                "version_id": "01JVERSION0123456789ABCDE",
                "size": 1048576,
                "content_type": "application/gzip",
                "etag": "9b2cf5d8a1e04c7fb3d20e6a5c81f47b",
                "last_modified": "2026-04-09T14:23:11.123+00:00"
            })
        ),
        (status = 400, description = "The group or connector id does not parse, or the source path is empty, absolute or contains a `.` or `..` segment", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, no WRITE on the target key or READ on the source path, or the snapshot would exceed the group's quota ceiling", body = ErrorResponse),
        (status = 404, description = "The bucket is unknown to this node or belongs to another group, the connector does not exist, or the source object is absent", body = ErrorResponse),
        (status = 501, description = "The `sync` strategy is declared but not implemented", body = ErrorResponse),
        (status = 502, description = "The source connector could not be read; nothing was staged and the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn stage_blob(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<StageRequest>,
) -> ServerResult<(StatusCode, Json<StageResponse>)> {
    let auth = require_realm_auth(&state, auth)?;

    match request {
        StageRequest::Snapshot(request) => snapshot_blob(state, auth, request).await,
        StageRequest::Reference(request) => reference_blob(state, auth, request).await,
        StageRequest::Sync(_) => Err(ServerError::Unimplemented),
    }
}

#[utoipa::path(
    post,
    path = "/data/staging/batch",
    tag = "data/staging",
    summary = "Stage many source objects in one blocking request",
    description = r#"Stages many source objects inside one request, reporting the outcome of each separately.

**Authentication**: realm bearer token; each item is authorized exactly as on the single-object
route, and sending `prefixes` additionally requires READ on the group's data path.

**Behavior**
- Every object is staged inside this request, one after another, so the call blocks for the whole
  transfer and a large batch is better submitted as a job.
- The outcome is per item and best effort: a 200 only means the batch ran, and each entry in
  `results` carries its own status, so a caller must inspect them rather than trust the status code.
- `prefixes` are expanded by listing the connector recursively, taking files only; a prefix that
  cannot be listed contributes one error entry naming that prefix instead of its objects.
- Result text is deliberately coarse for server-side and upstream failures, which appear only as
  `Internal server error` and `Bad gateway`.
- Items that succeeded are committed here and their propagation to sync targets is queued
  afterwards.

**Limits**
- Named and expanded objects together may not exceed 1000, and a prefix that would expand past that
  limit fails the entire request rather than truncating.
- `node_id`, when given, must be this node: staging always runs where the request lands."#,
    request_body(
        content = StageBatchRequest,
        description = "One connector and target bucket for the whole batch, plus explicit `items`, `prefixes` to expand, or both; leaving both out runs an empty batch",
        example = json!({
            "group_id": "01JABCDEF0123456789ABCDEFG",
            "connector_id": "01JCONNECTOR0123456789ABCD",
            "bucket": "research-raw",
            "strategy": "reference",
            "items": [
                {
                    "source_path": "refseq/2026/a.fna.gz",
                    "target_key": "genomes/2026/a.fna.gz"
                }
            ],
            "prefixes": [
                {
                    "source_prefix": "refseq/2026/batch-7",
                    "target_prefix": "genomes/2026/batch-7"
                }
            ]
        })
    ),
    responses(
        (
            status = 200,
            description = "One result per staged object plus one per prefix that could not be expanded, in that order",
            body = StageBatchResponse,
            example = json!({
                "results": [
                    {
                        "source_path": "refseq/2026/a.fna.gz",
                        "target_key": "genomes/2026/a.fna.gz",
                        "status": "ok"
                    },
                    {
                        "source_path": "refseq/2026/batch-7/b.fna.gz",
                        "target_key": "genomes/2026/batch-7/b.fna.gz",
                        "status": "error",
                        "error": "Not found"
                    }
                ]
            })
        ),
        (status = 400, description = "The group or connector id does not parse, `node_id` names another node, a prefix is not a confined relative path, or the batch would exceed 1000 objects", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or prefixes were given and the caller has no READ on the group's data path", body = ErrorResponse),
        (status = 501, description = "The `sync` strategy is declared but not implemented", body = ErrorResponse)
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
    let connector_id = parse_connector_id(&request.connector_id)?;
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
        crate::routes::storage::connectors::ensure_data_permission(
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
            ListStagingOperation::new(ListStagingInput {
                group_id,
                connector_id,
                source_path: source_prefix.clone(),
                offset: 0,
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

#[utoipa::path(
    post,
    path = "/data/staging/jobs",
    tag = "data/staging",
    summary = "Submit a background staging job",
    description = r#"Records a staging batch as a durable job on this node and returns before any object is read.

**Authentication**: realm bearer token; every named item and prefix is authorized before the job is
accepted, so acceptance already implies READ on each source path and WRITE on each target key.
Nothing is authorized later while the job runs.

**Behavior**
- Takes the same body as the blocking batch route but returns as soon as the work is recorded: a 202
  means the job is durable and queued on this node, and nothing has been read or copied yet.
- Unlike the blocking route there is no cap on the number of items, and prefixes are stored and
  walked by the job rather than expanded here.
- There is no idempotency key, so resubmitting the same body creates another job and stages the same
  objects a second time.
- Progress and completion are observed by reading the job by its id.

**Limits**
- At least one item or prefix is required.
- `node_id`, when given, must be this node, because the job runs where it was submitted."#,
    request_body(
        content = StageBatchRequest,
        description = "One connector and target bucket for the job, plus the `items` and `prefixes` it should stage; at least one of the two must be non-empty",
        example = json!({
            "group_id": "01JABCDEF0123456789ABCDEFG",
            "connector_id": "01JCONNECTOR0123456789ABCD",
            "bucket": "research-raw",
            "strategy": "snapshot",
            "prefixes": [
                {
                    "source_prefix": "refseq/2026",
                    "target_prefix": "genomes/2026"
                }
            ]
        })
    ),
    responses(
        (
            status = 202,
            description = "The job is durably queued on this node",
            body = SubmitStagingResponse,
            example = json!({
                "job_id": "01JJOB0123456789ABCDEFGHJ"
            })
        ),
        (status = 400, description = "The group or connector id does not parse, `node_id` names another node, a source path or prefix is not confined, a target key is blank, or neither items nor prefixes were given", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on a source path or WRITE on a target key", body = ErrorResponse),
        (status = 404, description = "The bucket is unknown to this node or belongs to another group, or a connector or source path does not exist", body = ErrorResponse),
        (status = 501, description = "The `sync` strategy is declared but not implemented", body = ErrorResponse),
        (status = 503, description = "Job placement or the structured id clock is unavailable; the response carries `Retry-After` and the identical body may be resubmitted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_staging(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<StageBatchRequest>,
) -> ServerResult<(StatusCode, Json<SubmitStagingResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&request.group_id)?;
    let connector_id = parse_connector_id(&request.connector_id)?;
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
    let bucket_info = load_bucket_info(&state, &request.bucket).await?;
    if bucket_info.group_id != group_id {
        return Err(ServerError::NotFound);
    }

    let mut items = Vec::new();
    for item in request.items.unwrap_or_default() {
        validate_source_path(&item.source_path)?;
        if item.target_key.trim().is_empty() {
            return Err(ServerError::BadRequest);
        }
        ensure_source_permission(&state, &auth, group_id, connector_id, &item.source_path).await?;
        ensure_permission(
            &state,
            &auth,
            blob_permission_path(&state, group_id, &request.bucket, &item.target_key),
            Permission::WRITE,
        )
        .await?;
        items.push(StagingJobItem {
            source_path: item.source_path,
            target_key: item.target_key,
        });
    }

    let mut prefixes = Vec::new();
    for prefix in request.prefixes.unwrap_or_default() {
        let source_prefix = normalize_prefix(&prefix.source_prefix)?;
        ensure_prefix_permission(
            &state,
            &auth,
            group_id,
            connector_id,
            source_prefix.trim_end_matches('/'),
        )
        .await?;
        ensure_permission(
            &state,
            &auth,
            blob_permission_path(
                &state,
                group_id,
                &request.bucket,
                prefix.target_prefix.trim_matches('/'),
            ),
            Permission::WRITE,
        )
        .await?;
        prefixes.push(StagingJobPrefix {
            source_prefix: source_prefix.trim_end_matches('/').to_string(),
            target_prefix: prefix.target_prefix.trim_matches('/').to_string(),
        });
    }
    if items.is_empty() && prefixes.is_empty() {
        return Err(ServerError::BadRequestReason(
            "at least one staging item or prefix is required".to_string(),
        ));
    }

    let result = submit_staging_job(
        &state.get_ctx(),
        StagingJobSpec {
            auth_context: auth,
            group_id,
            node_id,
            connector_id,
            bucket: request.bucket,
            strategy: match request.strategy {
                ApiStagingStrategy::Snapshot => StagingStrategy::Snapshot,
                ApiStagingStrategy::Reference => StagingStrategy::Reference,
                ApiStagingStrategy::Sync => unreachable!(),
            },
            items,
            prefixes,
        },
        node_id,
        state.rocrate_limits().artifact_retention_ms,
    )
    .await
    .map_err(map_submit_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(SubmitStagingResponse {
            job_id: result.job_id.to_string(),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/data/staging/jobs",
    tag = "data/staging",
    summary = "List your staging jobs on this node",
    description = r#"Returns one page of the calling user's own staging jobs that this node owns, oldest first.

**Authentication**: realm bearer token; a path-restricted token sees only the jobs submitted under
exactly the same restrictions.

**Behavior**
- A job submitted to another node is not visible here and must be listed there; other users' jobs
  and non-staging jobs never appear.
- Progress and per-object errors come from the runner's last checkpoint, so a running job's counts
  trail the work and a job that has not started reports the queued phase with zero progress."#,
    params(
        ("limit" = Option<usize>, Query, description = "Page size; defaults to 50 when absent or zero and is capped at 200"),
        ("cursor" = Option<String>, Query, description = "Opaque `next_cursor` from the previous page, passed back unchanged; omit for the first page")
    ),
    responses(
        (
            status = 200,
            description = "One page of the caller's staging jobs on this node; `next_cursor` appears only when a further page exists",
            body = StagingListResponse,
            example = json!({
                "jobs": [
                    {
                        "job_id": "01JJOB0123456789ABCDEFGHJ",
                        "strategy": "snapshot",
                        "group_id": "01JABCDEF0123456789ABCDEFG",
                        "connector_id": "01JCONNECTOR0123456789ABCD",
                        "bucket": "research-raw",
                        "state": "running",
                        "phase": "downloading",
                        "submitted_at": "2026-04-09T14:23:11.123+00:00",
                        "finished_at": null,
                        "error": null,
                        "progress": {
                            "items_current": 12,
                            "items_total": 40,
                            "bytes_current": 5242880,
                            "bytes_total": 20971520,
                            "current_path": "refseq/2026/genome.fna.gz"
                        },
                        "errors": []
                    }
                ],
                "next_cursor": "AAABkWPGovsBAgMEBQYHCAkKCwwNDg8Q"
            })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_staging_jobs(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<StagingJobQuery>,
) -> ServerResult<(StatusCode, Json<StagingListResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let cursor = decode_job_cursor(query.cursor.as_deref())?;
    let limit = query
        .limit
        .filter(|limit| *limit > 0)
        .unwrap_or(DEFAULT_JOB_LIMIT)
        .min(MAX_JOB_LIMIT);
    let (records, next_cursor) =
        list_owned_jobs(&state.get_ctx(), auth.user_id, cursor, limit, |record| {
            matches!(record.payload, JobPayload::Staging(_)) && staging_job_visible(record, &auth)
        })
        .await
        .map_err(ServerError::InternalError)?;
    let mut jobs = Vec::with_capacity(records.len());
    for record in records {
        let checkpoint = read_staging_checkpoint(&state.get_ctx(), record.job_id)
            .await
            .map_err(ServerError::InternalError)?;
        jobs.push(staging_job_response(&record, checkpoint.as_ref())?);
    }
    Ok((
        StatusCode::OK,
        Json(StagingListResponse {
            jobs,
            next_cursor: encode_job_cursor(next_cursor),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/data/staging/jobs/{job_id}",
    tag = "data/staging",
    summary = "Read one staging job and its progress",
    description = r#"Returns one staging job as the node that owns it currently records it, with its progress.

**Authentication**: realm bearer token, forwarded unchanged to the owning node, which re-checks it.

**Behavior**
- The job is always answered by the node that owns it: a request landing anywhere else is forwarded
  to that owner, so the same id can be read from any node and the answer is the owner's current
  record rather than a replica.
- `state` collapses the lifecycle into queued, running, done or failed, while `phase` names the
  stage the runner last checkpointed, so a job can read as running long before any bytes move.
- `progress` and `errors` come from that same checkpoint, so they trail the work; per-object
  failures are listed while the job as a whole may still succeed, and `finished_at` is set only once
  the job has stopped."#,
    params(("job_id" = String, Path, description = "Staging job id as a 26-character ULID, as returned when the job was submitted")),
    responses(
        (
            status = 200,
            description = "The job as the owning node currently records it, including per-object errors gathered so far",
            body = StagingJobResponse,
            example = json!({
                "job_id": "01JJOB0123456789ABCDEFGHJ",
                "strategy": "snapshot",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "connector_id": "01JCONNECTOR0123456789ABCD",
                "bucket": "research-raw",
                "state": "done",
                "phase": "completed",
                "submitted_at": "2026-04-09T14:23:11.123+00:00",
                "finished_at": "2026-04-09T14:41:02.884+00:00",
                "error": null,
                "progress": {
                    "items_current": 40,
                    "items_total": 40,
                    "bytes_current": 20971520,
                    "bytes_total": 20971520,
                    "current_path": null
                },
                "errors": [
                    {
                        "source_path": "refseq/2026/broken.fna.gz",
                        "target_key": "genomes/2026/broken.fna.gz",
                        "error": "Not found"
                    }
                ]
            })
        ),
        (status = 401, description = "Missing or invalid bearer token, or the owning node rejected the forwarded one", body = ErrorResponse),
        (status = 404, description = "The owning node, the sole authority on this, holds no such staging job for this caller; another user's job, a non-staging job and a job whose restrictions do not match the token all read as not found rather than forbidden", body = ErrorResponse),
        (status = 503, description = "The node that owns the job could not be reached; the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_staging_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    AxumPath(job_id): AxumPath<String>,
) -> ServerResult<(StatusCode, Json<StagingJobResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let job_id =
        aruna_core::structs::JobId::from_str(&job_id).map_err(|_| ServerError::BadRequest)?;
    // The owner is the sole 404 authority; a non-owner routes or reports 503.
    let (record, checkpoint) = read_staging_routed(
        &state.get_ctx(),
        auth.user_id,
        job_id,
        crate::routes::execution::jobs::forwarded_job_auth(bearer)?,
    )
    .await
    .map_err(crate::routes::execution::jobs::map_job_route)?
    .filter(|(record, _)| staging_job_visible(record, &auth))
    .ok_or(ServerError::NotFound)?;
    Ok((
        StatusCode::OK,
        Json(staging_job_response(&record, checkpoint.as_ref())?),
    ))
}

#[utoipa::path(
    get,
    path = "/data/staging/references",
    tag = "data/staging",
    summary = "List a bucket's objects with their source bindings",
    description = r#"Lists a bucket's live objects in key order, saying for each whether it is stored here or referenced.

**Authentication**: realm bearer token with READ on the bucket, checked against the group that owns
the bucket, which is resolved first.

**Behavior**
- This is a node-local listing of the bucket as this node currently sees it, so objects written
  elsewhere appear once they have replicated here.
- Every live object is returned, materialized and referenced alike, which is what lets a caller
  total a bucket in one pass.
- `referenced` false means the bytes are stored here and `size` is the stored blob size.
- `referenced` true means the object is still only a pointer, `size` is the length its source
  reported, and `kind` with `source_path` describe where it points, `connector_id` for an external
  source connector or `origin_node_id` for another Aruna node."#,
    params(
        ("bucket" = String, Query, description = "Bucket to list, as used by the S3 surface"),
        ("prefix" = Option<String>, Query, description = "Return only objects whose key starts with this prefix; an empty value is treated as no prefix"),
        ("limit" = Option<usize>, Query, description = "Page size; defaults to 500 when absent or zero and is capped at 1000"),
        ("cursor" = Option<String>, Query, description = "Opaque `next_cursor` from the previous page, passed back unchanged; omit for the first page")
    ),
    responses(
        (
            status = 200,
            description = "One page of the bucket's objects with their binding details; `next_cursor` appears only when a further page exists",
            body = ReferenceListResponse,
            example = json!({
                "entries": [
                    {
                        "key": "genomes/2026/a.fna.gz",
                        "size": 1048576,
                        "referenced": false
                    },
                    {
                        "key": "genomes/2026/b.fna.gz",
                        "size": 2097152,
                        "referenced": true,
                        "kind": "http",
                        "source_path": "refseq/2026/b.fna.gz",
                        "connector_id": "01JCONNECTOR0123456789ABCD"
                    }
                ],
                "next_cursor": "AWdlbm9tZXMvMjAyNi9iLmZuYS5neg"
            })
        ),
        (status = 400, description = "The cursor could not be decoded", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the bucket", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_references(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<ReferenceListQuery>,
) -> ServerResult<(StatusCode, Json<ReferenceListResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let bucket_info = load_bucket_info(&state, &query.bucket).await?;
    ensure_permission(
        &state,
        &auth,
        bucket_permission_path(
            state.get_realm_id(),
            bucket_info.group_id,
            state.get_node_id(),
            &query.bucket,
        ),
        Permission::READ,
    )
    .await?;

    let continuation_token = decode_reference_cursor(query.cursor.as_deref())?;
    let limit = query
        .limit
        .filter(|limit| *limit > 0)
        .unwrap_or(DEFAULT_REFERENCE_LIMIT)
        .min(MAX_REFERENCE_LIMIT);
    let result = drive(
        ListBucketOperation::new(ListBucketInput {
            bucket: query.bucket,
            group_id: bucket_info.group_id,
            continuation_token,
            max_keys: Some(limit),
            prefix: query.prefix.filter(|prefix| !prefix.is_empty()),
            delimiter: None,
            start_after: None,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;

    let entries = result
        .objects
        .into_iter()
        .map(|object| ReferenceListEntry {
            size: object
                .location
                .as_ref()
                .map(|location| location.blob_size)
                .or_else(|| {
                    object
                        .source_metadata
                        .as_ref()
                        .map(|metadata| metadata.content_length)
                })
                .unwrap_or_default(),
            key: object.head.key,
            referenced: object.referenced,
            kind: object.kind.map(Into::into),
            source_path: object.source_path,
            connector_id: object.connector_id.map(|id| id.to_string()),
            origin_node_id: object.origin_node_id.map(|id| id.to_string()),
        })
        .collect();
    let next_cursor = result
        .continuation_token
        .map(encode_reference_cursor)
        .transpose()?;

    Ok((
        StatusCode::OK,
        Json(ReferenceListResponse {
            entries,
            next_cursor,
        }),
    ))
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
    let target = StageTargetRequest {
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
        ServerError::BadGateway | ServerError::BadGatewayReason(_) => "Bad gateway".to_string(),
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

fn staging_job_visible(record: &JobRecord, auth: &AuthContext) -> bool {
    let JobPayload::Staging(spec) = &record.payload else {
        return false;
    };
    auth.path_restrictions.is_none() || spec.auth_context == *auth
}

fn staging_job_response(
    record: &JobRecord,
    checkpoint: Option<&StagingJobCheckpoint>,
) -> ServerResult<StagingJobResponse> {
    let JobPayload::Staging(spec) = &record.payload else {
        return Err(ServerError::NotFound);
    };
    let phase = match record.state {
        JobState::Succeeded => StagingJobPhase::Completed,
        JobState::Failed | JobState::Cancelled => StagingJobPhase::Failed,
        _ => checkpoint
            .map(|checkpoint| checkpoint.phase)
            .unwrap_or(StagingJobPhase::Queued),
    };
    let progress = checkpoint
        .map(|checkpoint| StagingProgressResponse {
            items_current: checkpoint.items_current,
            items_total: checkpoint.items_total,
            bytes_current: checkpoint.bytes_current,
            bytes_total: checkpoint.bytes_total,
            current_path: checkpoint.current_path.clone(),
        })
        .unwrap_or_else(|| StagingProgressResponse {
            items_current: record.progress.current,
            items_total: record.progress.total,
            bytes_current: 0,
            bytes_total: None,
            current_path: None,
        });
    Ok(StagingJobResponse {
        job_id: record.job_id.to_string(),
        strategy: match spec.strategy {
            StagingStrategy::Reference => ApiStagingStrategy::Reference,
            StagingStrategy::Snapshot => ApiStagingStrategy::Snapshot,
            StagingStrategy::Sync => ApiStagingStrategy::Sync,
        },
        group_id: spec.group_id.to_string(),
        connector_id: spec.connector_id.to_string(),
        bucket: spec.bucket.clone(),
        state: match record.state {
            JobState::Queued | JobState::Claimed => "queued",
            JobState::Succeeded => "done",
            JobState::Failed | JobState::Cancelled => "failed",
            _ => "running",
        }
        .to_string(),
        phase: staging_phase_name(phase).to_string(),
        submitted_at: format_job_time(record.created_at_ms),
        finished_at: record.finished_at_ms.map(format_job_time),
        error: record
            .last_error
            .as_ref()
            .map(|error| error.message.clone()),
        progress,
        errors: checkpoint
            .map(|checkpoint| {
                checkpoint
                    .errors
                    .iter()
                    .map(|error| StagingErrorResponse {
                        source_path: error.source_path.clone(),
                        target_key: error.target_key.clone(),
                        error: error.error.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
    })
}

fn staging_phase_name(phase: StagingJobPhase) -> &'static str {
    match phase {
        StagingJobPhase::Queued => "queued",
        StagingJobPhase::Discovering => "discovering",
        StagingJobPhase::Inspecting => "inspecting",
        StagingJobPhase::Registering => "registering",
        StagingJobPhase::Downloading => "downloading",
        StagingJobPhase::Writing => "writing",
        StagingJobPhase::Completed => "completed",
        StagingJobPhase::Failed => "failed",
    }
}

fn format_job_time(timestamp_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(timestamp_ms as i64)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_default()
}

fn decode_reference_cursor(cursor: Option<&str>) -> ServerResult<Option<ListContinuationToken>> {
    cursor
        .map(|cursor| {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ServerError::BadRequest)?;
            ListContinuationToken::from_bytes(&bytes).map_err(|_| ServerError::BadRequest)
        })
        .transpose()
}

fn encode_reference_cursor(token: ListContinuationToken) -> ServerResult<String> {
    token
        .to_bytes()
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
        .map_err(|error| ServerError::InternalError(error.to_string()))
}

fn normalize_prefix(prefix: &str) -> ServerResult<String> {
    let mut prefix = prefix.trim();
    while let Some(stripped) = prefix.strip_prefix("./") {
        prefix = stripped;
    }
    if prefix.is_empty() || prefix == "." {
        return Ok(String::new());
    }
    validate_source_path(prefix)?;
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
    request: StageTargetRequest,
) -> ServerResult<(StatusCode, Json<StageResponse>)> {
    let group_id = parse_group_id(&request.group_id)?;
    let connector_id = parse_connector_id(&request.connector_id)?;
    let bucket_info = load_bucket_info(&state, &request.bucket).await?;
    if bucket_info.group_id != group_id {
        return Err(ServerError::NotFound);
    }

    ensure_permission(
        &state,
        &auth,
        blob_permission_path(&state, group_id, &request.bucket, &request.key),
        Permission::WRITE,
    )
    .await?;
    ensure_source_permission(&state, &auth, group_id, connector_id, &request.source_path).await?;

    let quota_ceiling = resolve_quota_ceiling(&state, group_id).await?;

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
            retry_key: None,
            expected_bucket: bucket_info,
            restrictions: auth.path_restrictions.clone(),
        },
    )
    .await
    .map_err(map_snapshot_error)?;

    queue_live_replication(
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
        Json(StageResponse {
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
    request: StageTargetRequest,
) -> ServerResult<(StatusCode, Json<StageResponse>)> {
    let group_id = parse_group_id(&request.group_id)?;
    let connector_id = parse_connector_id(&request.connector_id)?;
    let bucket_info = load_bucket_info(&state, &request.bucket).await?;
    if bucket_info.group_id != group_id {
        return Err(ServerError::NotFound);
    }

    ensure_permission(
        &state,
        &auth,
        blob_permission_path(&state, group_id, &request.bucket, &request.key),
        Permission::WRITE,
    )
    .await?;
    ensure_source_permission(&state, &auth, group_id, connector_id, &request.source_path).await?;

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
            expected_bucket: bucket_info,
            inherited_policies: Vec::new(),
        },
    )
    .await
    .map_err(map_reference_error)?;

    queue_live_replication(
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
        Json(StageResponse {
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
async fn resolve_quota_ceiling(
    state: &ServerState,
    group_id: ulid::Ulid,
) -> ServerResult<Option<u64>> {
    let config = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok(config.quota.effective_group_ceiling(&group_id))
}

async fn load_bucket_info(state: &ServerState, bucket: &str) -> ServerResult<BucketInfo> {
    match drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(bucket_info) => Ok(bucket_info),
        Err(GetBucketError::NotFound) => Err(ServerError::NotFound),
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
    validate_source_path(source_path)?;

    ensure_permission(
        state,
        auth,
        connector_permission_path(state, group_id, connector_id, source_path),
        Permission::READ,
    )
    .await
}

async fn ensure_prefix_permission(
    state: &ServerState,
    auth: &AuthContext,
    group_id: ulid::Ulid,
    connector_id: ulid::Ulid,
    source_prefix: &str,
) -> ServerResult<()> {
    ensure_permission(
        state,
        auth,
        connector_permission_path(state, group_id, connector_id, source_prefix),
        Permission::READ,
    )
    .await
}

fn connector_permission_path(
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

fn validate_source_path(source_path: &str) -> ServerResult<()> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Err(ServerError::BadRequest);
    }

    let mut has_normal_component = false;
    if trimmed
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return Err(ServerError::BadRequest);
    }
    for component in FsPath::new(trimmed).components() {
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
        MaterializeSnapshotError::Read(error) => map_read_error(error),
        MaterializeSnapshotError::Write(PutObjectError::QuotaExceeded { .. }) => {
            ServerError::Forbidden
        }
        MaterializeSnapshotError::Write(error) => ServerError::InternalError(error.to_string()),
        MaterializeSnapshotError::Storage(error) => ServerError::InternalError(error.to_string()),
        MaterializeSnapshotError::Conversion(error) => {
            ServerError::InternalError(error.to_string())
        }
        MaterializeSnapshotError::Routing(error) => ServerError::InternalError(error.to_string()),
        // A node revalidating its inventory admits nothing governed; the client
        // learns only that this node is busy, never which rule applies.
        MaterializeSnapshotError::Gate(_) => ServerError::ServiceUnavailable,
    }
}

fn map_reference_error(error: MaterializeReferenceError) -> ServerError {
    match error {
        MaterializeReferenceError::Head(error) => map_head_error(error),
        MaterializeReferenceError::Storage(error) => ServerError::InternalError(error.to_string()),
        MaterializeReferenceError::Conversion(error) => {
            ServerError::InternalError(error.to_string())
        }
        MaterializeReferenceError::Usage(error) => ServerError::InternalError(error.to_string()),
        // A policy error names the ids it conflicts on, which a client must
        // never learn: the refusal is reported without them.
        MaterializeReferenceError::Policy(_) => ServerError::Forbidden,
        MaterializeReferenceError::Purge(_) => ServerError::ServiceUnavailable,
    }
}

fn map_head_error(error: HeadSourceError) -> ServerError {
    match error {
        HeadSourceError::Resolve(error) => map_resolution_error(error),
        HeadSourceError::Staging(error) => map_source_error(error),
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_read_error(error: ReadSourceError) -> ServerError {
    match error {
        ReadSourceError::Resolve(error) => map_resolution_error(error),
        ReadSourceError::Staging(error) => map_source_error(error),
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_resolution_error(error: SourceResolutionError) -> ServerError {
    match error {
        SourceResolutionError::NotFound => ServerError::NotFound,
        SourceResolutionError::InvalidSourcePath
        | SourceResolutionError::UnsupportedConnectorKind(_) => ServerError::BadRequest,
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_source_error(error: StagingSourceError) -> ServerError {
    match error {
        StagingSourceError::NotFound => ServerError::NotFound,
        _ => ServerError::BadGateway,
    }
}

fn map_list_error(error: ListStagingError) -> ServerError {
    match error {
        ListStagingError::Resolve(error) => map_resolution_error(error),
        ListStagingError::Staging(error) => map_source_error(error),
        _ => ServerError::InternalError(error.to_string()),
    }
}

pub(crate) async fn queue_live_replication(
    state: &ServerState,
    auth_context: AuthContext,
    bucket: String,
    key: String,
    version_id: ulid::Ulid,
    delete_marker: bool,
) {
    let result = match drive(
        LiveVersionOperation::new(LiveVersionInput {
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
#[path = "staging_tests.rs"]
mod tests;
