use crate::auth::{
    ValidatedArunaBearerTokenCarrier, bucket_blob_permission_path, ensure_permission,
    parse_group_id, parse_source_connector_id, require_realm_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::connectors::ApiSourceConnectorKind;
use crate::routes::jobs::map_submit_error;
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::errors::{SourceConnectorResolutionError, StagingSourceError};
use aruna_core::structs::{
    AuthContext, BucketInfo, JobPayload, JobRecord, JobState, Permission, SourceEntry,
    SourceEntryKind, StagingJobCheckpoint, StagingJobItem, StagingJobPhase, StagingJobPrefix,
    StagingJobSpec, StagingStrategy, blob_bucket_permission_path,
};
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::jobs::service::{list_owned_jobs, read_staging_routed, submit_staging_job};
use aruna_operations::jobs::staging::read_staging_checkpoint;
use aruna_operations::replication::queue::{
    QueueLiveVersionReplicationInput, QueueLiveVersionReplicationOperation,
};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::list_objects_v2::{
    ListObjectsV2ContinuationToken, ListObjectsV2Input, ListObjectsV2Operation,
};
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
        name = "staging",
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
pub struct StagingJobListQuery {
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
    pub kind: Option<ApiSourceConnectorKind>,
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SubmitStagingJobResponse {
    pub job_id: String,
    pub created: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StagingJobProgressResponse {
    pub items_current: u64,
    pub items_total: Option<u64>,
    pub bytes_current: u64,
    pub bytes_total: Option<u64>,
    pub current_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StagingJobErrorResponse {
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
    pub progress: StagingJobProgressResponse,
    pub errors: Vec<StagingJobErrorResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StagingJobListResponse {
    pub jobs: Vec<StagingJobResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[utoipa::path(
    post,
    path = "/staging/",
    tag = "staging",
    summary = "Stage a single source object into a bucket",
    description = r#"Stages one source object into a bucket, either by copying its bytes now or by recording a pointer.

**Authentication**: realm bearer token; the caller needs WRITE on the target bucket key and READ on
the connector's source path, and both are checked against the concrete path, not a prefix alone.

**Behavior**
- The body is chosen by its `strategy` field.
- `snapshot` reads the object from the source connector and commits its bytes to this node's storage
  before answering, so it is as slow as the transfer.
- `reference` records the object as a pointer to its source and copies nothing, so the bytes are
  fetched on demand at read time.
- `sync` is accepted by the schema but not implemented and always fails.
- A 201 means the new object version is committed here and immediately readable through S3;
  propagating that version to any sync target is queued afterwards and is neither awaited nor
  reported by this response.

**Limits**
- The bucket must exist on this node and belong to the given group.
- Source paths must be relative and confined, so a leading slash or any `.` or `..` segment is
  rejected with 400.
- A snapshot is charged against the realm's quota ceiling for the group.

**Errors**: a bucket owned by a different group is reported as not found rather than forbidden, and
a snapshot that would exceed the group's quota ceiling is a 403."#,
    request_body(
        content = StageBlobRequest,
        description = "The staging strategy and the source and target it applies to. All five target fields are required, and `strategy` selects between copying the bytes now and recording a reference.",
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
            description = "The object version committed on this node; the content type, entity tag and modification time are echoed from the source and omitted when it reported none",
            body = StageBlobResponse,
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
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, the caller lacks WRITE on the target key or READ on the source path, or the snapshot would exceed the group's quota ceiling", body = ErrorResponse),
        (status = 404, description = "The bucket is unknown to this node or belongs to another group, the connector does not exist, or the source object is absent", body = ErrorResponse),
        (status = 501, description = "The `sync` strategy is declared but not implemented", body = ErrorResponse),
        (status = 502, description = "The source connector could not be read; nothing was staged and the caller may retry", body = ErrorResponse)
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
    summary = "Stage many source objects in one blocking request",
    description = r#"Stages many source objects inside one request, reporting the outcome of each separately.

**Authentication**: realm bearer token; using prefixes additionally requires READ on the group's
data path, while individual items are authorized exactly as the single-object endpoint authorizes
them.

**Behavior**
- Every object is staged inside this request, one after another, so the call blocks for the whole
  transfer and a large batch is better submitted as a job instead.
- The outcome is per item and best effort: a 200 only means the batch ran, and each entry in
  `results` carries its own `ok` or `error` status, so a caller must inspect them rather than trust
  the status code.
- Objects can be named directly in `items` and expanded from `prefixes`, which lists the connector
  recursively and takes files only.
- A prefix that cannot be listed contributes one error entry naming that prefix instead of its
  objects.
- Error text on a result is deliberately coarse for server-side and upstream failures, which appear
  only as `Internal server error` and `Bad gateway`.
- Items that succeeded are committed here and their propagation to sync targets is queued
  afterwards.

**Limits**
- Named and expanded objects together may not exceed 1000, and a prefix that would expand past that
  limit fails the entire request rather than truncating.
- `node_id` is optional and, when given, must be this node: staging always runs where the request
  lands.
- The `sync` strategy is not implemented and fails the whole request."#,
    request_body(
        content = StageBatchRequest,
        description = "One connector and target bucket for the whole batch, plus explicit `items`, `prefixes` to expand, or both; leaving both out runs an empty batch. `node_id` defaults to this node.",
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
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or prefixes were given and the caller has no READ on the group's data path", body = ErrorResponse),
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
    path = "/staging/jobs",
    tag = "staging",
    summary = "Submit a background staging job",
    description = r#"Records a staging batch as a durable job on this node and returns before any object is read.

**Authentication**: realm bearer token; every named item and every prefix is authorized before the
job is accepted, so acceptance already implies READ on each connector source path or prefix and
WRITE on each target key or target prefix. Nothing is authorized later while the job runs.

**Behavior**
- Takes the same body as the blocking batch endpoint but returns as soon as the work is recorded: a
  202 means the job is durable and queued on this node, and no object has been read, copied or
  referenced yet.
- Unlike the blocking endpoint there is no cap on the number of items, and no request-side prefix
  expansion happens: prefixes are stored and walked by the job.
- There is no idempotency key, so every call creates a new job, `created` is always true, and
  resubmitting the same body stages the same objects a second time.
- Progress and completion are observed by reading the job by its id.

**Limits**
- The bucket must exist on this node and belong to the given group, otherwise the request is not
  found.
- At least one item or prefix is required.
- Source paths must be relative and confined, and target keys must not be blank.
- `node_id`, when given, must be this node, because the job runs where it was submitted.
- The `sync` strategy is not implemented.

**Errors**: a refusal from job admission keeps its own status instead of collapsing into a 500. An
unavailable job placement binding or an unhealthy structured id clock is a 503 carrying
`Retry-After`, so the identical body may be resubmitted; a storage failure stays a 500."#,
    request_body(
        content = StageBatchRequest,
        description = "One connector and target bucket for the job, plus the `items` and `prefixes` it should stage; at least one of the two must be non-empty. `node_id` defaults to this node.",
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
            description = "The job is durably queued on this node; `created` is always true because staging jobs are never deduplicated",
            body = SubmitStagingJobResponse,
            example = json!({
                "job_id": "01JJOB0123456789ABCDEFGHJ",
                "created": true
            })
        ),
        (status = 400, description = "The group or connector id does not parse, `node_id` names another node, a source path or prefix is not confined, a target key is blank, or neither items nor prefixes were given", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller lacks READ on a source path or WRITE on a target key", body = ErrorResponse),
        (status = 404, description = "The bucket is unknown to this node or belongs to another group, or a connector or source path does not exist", body = ErrorResponse),
        (status = 501, description = "The `sync` strategy is declared but not implemented", body = ErrorResponse),
        (status = 503, description = "Job placement or the structured id clock is unavailable; retry the same body", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_staging(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<StageBatchRequest>,
) -> ServerResult<(StatusCode, Json<SubmitStagingJobResponse>)> {
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
    let bucket_info = load_bucket_info(&state, &request.bucket).await?;
    if bucket_info.group_id != group_id {
        return Err(ServerError::NotFound);
    }

    let mut items = Vec::new();
    for item in request.items.unwrap_or_default() {
        validate_relative_source_path(&item.source_path)?;
        if item.target_key.trim().is_empty() {
            return Err(ServerError::BadRequest);
        }
        ensure_source_permission(&state, &auth, group_id, connector_id, &item.source_path).await?;
        ensure_permission(
            &state,
            &auth,
            bucket_blob_permission_path(&state, group_id, &request.bucket, &item.target_key),
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
            bucket_blob_permission_path(
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
        Json(SubmitStagingJobResponse {
            job_id: result.job_id.to_string(),
            created: result.created,
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/staging/jobs",
    tag = "staging",
    summary = "List your staging jobs on this node",
    description = r#"Returns one page of the calling user's own staging jobs that this node owns, oldest first.

**Authentication**: realm bearer token; a path-restricted token sees only the jobs submitted under
exactly the same restrictions.

**Behavior**
- A job submitted to another node is not visible here and must be listed there; other users' jobs
  and non-staging jobs never appear.
- Jobs come back oldest first by submission time.
- Each entry carries the job's own progress and per-object errors as last checkpointed by the
  runner, so a running job's counts trail the work slightly and a job that has not started yet
  reports the queued phase with zero progress."#,
    params(
        ("limit" = Option<usize>, Query, description = "Page size; defaults to 50 when absent or zero and is capped at 200"),
        ("cursor" = Option<String>, Query, description = "Opaque cursor returned as `next_cursor` by the previous page, passed back unchanged. Omit for the first page; a cursor that does not decode is rejected")
    ),
    responses(
        (
            status = 200,
            description = "One page of the caller's staging jobs on this node, with `next_cursor` present only when a further page exists",
            body = StagingJobListResponse,
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
        (status = 401, description = "No bearer token was presented", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_staging_jobs(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<StagingJobListQuery>,
) -> ServerResult<(StatusCode, Json<StagingJobListResponse>)> {
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
        Json(StagingJobListResponse {
            jobs,
            next_cursor: encode_job_cursor(next_cursor),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/staging/jobs/{job_id}",
    tag = "staging",
    summary = "Read one staging job and its progress",
    description = r#"Returns one staging job as the node that owns it currently records it, with its progress.

**Authentication**: realm bearer token, which is forwarded unchanged when the job belongs to another
node.

**Behavior**
- The job is always answered by the node that owns it: a request that lands anywhere else is
  forwarded to that owner with the caller's own token, so the same id can be read from any node in
  the realm and the answer is the owner's current record rather than a replica.
- `state` collapses the lifecycle into queued, running, done or failed, while `phase` names the
  stage the runner last checkpointed, so a job can read as running long before any bytes move.
- `progress` and `errors` come from that same checkpoint, so they trail the work slightly;
  per-object failures are listed in `errors` while the job as a whole may still succeed, and
  `finished_at` is set only once the job has stopped.

**Errors**: only the owner decides that a job does not exist, and a job created by another user, a
non-staging job, and a job whose restrictions do not match the presenting token are all reported as
not found rather than forbidden."#,
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
        (status = 401, description = "No bearer token was presented, or the owning node rejected the forwarded token", body = ErrorResponse),
        (status = 404, description = "The owning node holds no staging job with that id for this caller", body = ErrorResponse),
        (status = 503, description = "The node that owns the job could not be reached; the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_staging_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
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
        super::jobs::forwarded_job_auth(bearer)?,
    )
    .await
    .map_err(super::jobs::map_job_route)?
    .filter(|(record, _)| staging_job_visible(record, &auth))
    .ok_or(ServerError::NotFound)?;
    Ok((
        StatusCode::OK,
        Json(staging_job_response(&record, checkpoint.as_ref())?),
    ))
}

#[utoipa::path(
    get,
    path = "/staging/references",
    tag = "staging",
    summary = "List a bucket's objects with their source bindings",
    description = r#"Lists a bucket's live objects in key order, saying for each whether it is stored here or referenced.

**Authentication**: realm bearer token with READ on the bucket; the bucket's owning group is
resolved first, so a bucket unknown to this node is not found and the permission check always runs
against the group that owns it.

**Behavior**
- This is a node-local listing of the bucket as this node currently sees it, so objects written
  elsewhere appear once they have replicated here.
- Every live object is returned, materialized and referenced alike, which is what lets a caller
  total a bucket in one pass.
- `referenced` false means the bytes are stored here and `size` is the stored blob size.
- `referenced` true means the object is still only a pointer, `size` is the length its source
  reported, and `kind` with `source_path` describe where it points, `connector_id` for an external
  source connector or `origin_node_id` for another Aruna node.
- Those four fields are omitted entirely for a materialized object.
- Objects are listed in key order and pagination is by opaque cursor."#,
    params(
        ("bucket" = String, Query, description = "Bucket to list, as used by the S3 surface; required"),
        ("prefix" = Option<String>, Query, description = "Return only objects whose key starts with this prefix; an empty value is treated as no prefix"),
        ("limit" = Option<usize>, Query, description = "Page size; defaults to 500 when absent or zero and is capped at 1000"),
        ("cursor" = Option<String>, Query, description = "Opaque cursor returned as `next_cursor` by the previous page, passed back unchanged. Omit for the first page; a cursor that does not decode is rejected")
    ),
    responses(
        (
            status = 200,
            description = "One page of the bucket's objects with their binding details, with `next_cursor` present only when a further page exists",
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
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller lacks READ on the bucket", body = ErrorResponse),
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
        blob_bucket_permission_path(
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
        ListObjectsV2Operation::new(ListObjectsV2Input {
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
    .and_then(|output| output.transpose())
    .map_err(|error| ServerError::InternalError(error.to_string()))?
    .ok_or_else(|| ServerError::InternalError("object listing produced no result".to_string()))?;

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
        .map(|checkpoint| StagingJobProgressResponse {
            items_current: checkpoint.items_current,
            items_total: checkpoint.items_total,
            bytes_current: checkpoint.bytes_current,
            bytes_total: checkpoint.bytes_total,
            current_path: checkpoint.current_path.clone(),
        })
        .unwrap_or_else(|| StagingJobProgressResponse {
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
                    .map(|error| StagingJobErrorResponse {
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

fn decode_job_cursor(cursor: Option<&str>) -> ServerResult<Option<Vec<u8>>> {
    match cursor {
        Some(cursor) => {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ServerError::BadRequest)?;
            if bytes.len() != 24 {
                return Err(ServerError::BadRequest);
            }
            Ok(Some(bytes))
        }
        None => Ok(None),
    }
}

fn encode_job_cursor(cursor: Option<Vec<u8>>) -> Option<String> {
    cursor.map(|cursor| URL_SAFE_NO_PAD.encode(cursor))
}

fn decode_reference_cursor(
    cursor: Option<&str>,
) -> ServerResult<Option<ListObjectsV2ContinuationToken>> {
    cursor
        .map(|cursor| {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ServerError::BadRequest)?;
            ListObjectsV2ContinuationToken::from_bytes(&bytes).map_err(|_| ServerError::BadRequest)
        })
        .transpose()
}

fn encode_reference_cursor(token: ListObjectsV2ContinuationToken) -> ServerResult<String> {
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
            retry_key: None,
            expected_bucket: bucket_info,
            restrictions: auth.path_restrictions.clone(),
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
        },
    )
    .await
    .map_err(map_reference_error)?;

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
        source_connector_permission_path(state, group_id, connector_id, source_prefix),
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
        MaterializeSnapshotError::Read(error) => map_read_staging_error(error),
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
        MaterializeReferenceError::Head(error) => map_head_staging_error(error),
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
        AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
        BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
        CurrentVersionPointer, Group, GroupAuthorizationDocument, NodeCapabilities,
        PathRestriction, PortableSourceDescriptor, RealmAuthorizationDocument, RealmConfigDocument,
        SourceConnectorKind, SourceMetadata, StagingStrategy, VersionKey, VersionSourceBinding,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::replication::queue::{
        LiveReplicationObligationRecord, live_replication_obligation_key,
    };
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;
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
        auth_with_bucket_read: AuthContext,
        auth_with_source_read: AuthContext,
        auth_without_source_read: AuthContext,
    }

    #[test]
    fn job_cursor_roundtrip() {
        let cursor = vec![7u8; 24];
        let encoded = encode_job_cursor(Some(cursor.clone())).unwrap();

        assert_eq!(decode_job_cursor(Some(&encoded)).unwrap(), Some(cursor));
        assert!(decode_job_cursor(Some("invalid")).is_err());
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
    async fn references_list_bindings() {
        let test = setup_state().await;
        let origin = seed_reference_objects(&test).await;

        let (status, Json(mut first)) = list_references(
            State(test.state.clone()),
            Extension(Some(test.auth_with_bucket_read.clone())),
            Query(ReferenceListQuery {
                bucket: test.bucket.clone(),
                prefix: Some("data/".to_string()),
                limit: Some(2),
                cursor: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first.entries.len(), 2);

        let (_, Json(second)) = list_references(
            State(test.state.clone()),
            Extension(Some(test.auth_with_bucket_read.clone())),
            Query(ReferenceListQuery {
                bucket: test.bucket.clone(),
                prefix: Some("data/".to_string()),
                limit: Some(2),
                cursor: first.next_cursor.take(),
            }),
        )
        .await
        .unwrap();
        assert!(second.next_cursor.is_none());
        first.entries.extend(second.entries);
        assert_eq!(first.entries.len(), 3);
        assert!(
            first
                .entries
                .iter()
                .all(|entry| entry.key.starts_with("data/"))
        );

        let materialized = first
            .entries
            .iter()
            .find(|entry| entry.key == "data/a-materialized")
            .unwrap();
        assert_eq!(materialized.size, 42);
        assert!(!materialized.referenced);
        assert_eq!(materialized.kind, None);
        assert_eq!(materialized.source_path, None);
        assert_eq!(materialized.connector_id, None);
        assert_eq!(materialized.origin_node_id, None);

        let external = first
            .entries
            .iter()
            .find(|entry| entry.key == "data/b-external")
            .unwrap();
        assert_eq!(external.size, 64);
        assert!(external.referenced);
        assert_eq!(external.kind, Some(ApiSourceConnectorKind::Http));
        assert_eq!(external.source_path.as_deref(), Some("remote/file.txt"));
        let connector_id = test.connector_id.to_string();
        assert_eq!(
            external.connector_id.as_deref(),
            Some(connector_id.as_str())
        );
        assert_eq!(external.origin_node_id, None);

        let native = first
            .entries
            .iter()
            .find(|entry| entry.key == "data/c-native")
            .unwrap();
        assert_eq!(native.size, 128);
        assert!(native.referenced);
        assert_eq!(native.kind, Some(ApiSourceConnectorKind::ArunaNative));
        assert_eq!(native.source_path.as_deref(), Some("source-bucket/native"));
        assert_eq!(native.connector_id, None);
        let origin_node_id = origin.to_string();
        assert_eq!(
            native.origin_node_id.as_deref(),
            Some(origin_node_id.as_str())
        );
    }

    #[tokio::test]
    async fn references_deny_read() {
        let test = setup_state().await;

        let result = list_references(
            State(test.state),
            Extension(Some(test.auth_without_source_read)),
            Query(ReferenceListQuery {
                bucket: test.bucket,
                prefix: None,
                limit: None,
                cursor: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn staging_queue_failure_after_snapshot_commit_leaves_obligation_repairable() {
        let test = setup_state().await;
        let version_id = Ulid::generate();
        write_doc(
            &test.state.get_ctx(),
            S3_BUCKET_KEYSPACE,
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
    fn staging_submit_classifies() {
        // Job admission refusals must keep their status instead of collapsing into 500.
        use aruna_operations::jobs::submit::SubmitJobError;

        assert_eq!(
            map_submit_error(SubmitJobError::PlacementUnavailable(
                "no binding".to_string()
            ))
            .status_code(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            map_submit_error(SubmitJobError::InvalidWorkspace("bad".to_string())).status_code(),
            StatusCode::BAD_REQUEST
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
    fn prefix_normalizes_root() {
        assert_eq!(normalize_prefix(".").unwrap(), "");
        assert_eq!(normalize_prefix("./").unwrap(), "");
        assert_eq!(normalize_prefix("./refseq/").unwrap(), "refseq/");
        assert!(normalize_prefix("refseq/./nested").is_err());
        assert!(normalize_prefix("../refseq").is_err());
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
        assert!(openapi.paths.paths.contains_key("/staging/references"));
        assert!(!openapi.paths.paths.contains_key("/blobs/staging"));
    }

    async fn seed_reference_objects(test: &TestState) -> NodeId {
        let created_by = test.auth_with_bucket_read.user_id;
        let materialized_hash = [21u8; 32];
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: "materialized".to_string(),
            ulid: Ulid::generate(),
            compressed: false,
            encrypted: false,
            created_by,
            created_at: UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes: HashMap::new(),
        };
        write_doc(
            &test.state.get_ctx(),
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new(materialized_hash, location.backend.clone())
                .to_bytes()
                .into(),
            location.to_bytes().unwrap().into(),
        )
        .await;
        for key in ["data/a-materialized", "other/d-materialized"] {
            write_blob_version(
                &test.state.get_ctx(),
                &test.bucket,
                key,
                BlobVersion::materialized(
                    materialized_hash,
                    BackendRef::node_default(),
                    UNIX_EPOCH,
                    created_by,
                    None,
                ),
            )
            .await;
        }

        write_blob_version(
            &test.state.get_ctx(),
            &test.bucket,
            "data/b-external",
            BlobVersion::reference(
                VersionSourceBinding {
                    strategy: StagingStrategy::Reference,
                    descriptor: PortableSourceDescriptor {
                        kind: SourceConnectorKind::Http,
                        public_config: HashMap::new(),
                        source_path: "remote/file.txt".to_string(),
                        version_selector: None,
                        capabilities: Vec::new(),
                        origin_node_id: None,
                    },
                    connector_id: Some(test.connector_id),
                },
                SourceMetadata {
                    content_length: 64,
                    content_type: None,
                    etag: None,
                    last_modified: None,
                    source_version: None,
                },
                UNIX_EPOCH,
                created_by,
                UNIX_EPOCH,
            ),
        )
        .await;

        let origin = iroh::SecretKey::from_bytes(&[19u8; 32]).public();
        write_blob_version(
            &test.state.get_ctx(),
            &test.bucket,
            "data/c-native",
            BlobVersion::reference(
                VersionSourceBinding {
                    strategy: StagingStrategy::Reference,
                    descriptor: PortableSourceDescriptor {
                        kind: SourceConnectorKind::ArunaNative,
                        public_config: HashMap::new(),
                        source_path: "source-bucket/native".to_string(),
                        version_selector: None,
                        capabilities: Vec::new(),
                        origin_node_id: Some(origin),
                    },
                    connector_id: None,
                },
                SourceMetadata {
                    content_length: 128,
                    content_type: None,
                    etag: None,
                    last_modified: None,
                    source_version: None,
                },
                UNIX_EPOCH,
                created_by,
                UNIX_EPOCH,
            ),
        )
        .await;
        origin
    }

    async fn write_blob_version(
        driver_ctx: &Arc<DriverContext>,
        bucket: &str,
        key: &str,
        version: BlobVersion,
    ) {
        let version_id = Ulid::generate();
        write_doc(
            driver_ctx,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
            CurrentVersionPointer::new(version_id)
                .to_bytes()
                .unwrap()
                .into(),
        )
        .await;
        write_doc(
            driver_ctx,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(bucket, key, version_id)
                .to_bytes()
                .unwrap()
                .into(),
            version.to_bytes().unwrap().into(),
        )
        .await;
    }

    async fn setup_state() -> TestState {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let realm_signing_key = ed25519_dalek::SigningKey::from_bytes(&[5u8; 32]);
        let realm_id =
            aruna_core::structs::RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::from_bytes(&[13u8; 32]).public();
        let user_with_source_read = UserId::local(Ulid::generate(), realm_id);
        let user_without_source_read = UserId::local(Ulid::generate(), realm_id);
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

        let bucket_group_id = Ulid::generate();
        let source_group_id = Ulid::generate();
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
        let connector_id = Ulid::generate();
        let source_path = "folder/file.txt".to_string();
        let bucket_info = BucketInfo {
            group_id: bucket_group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user_with_source_read,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
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
                NodeCapabilities::user_node(realm_id).unwrap(),
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
        let bucket_path = blob_bucket_permission_path(realm_id, bucket_group_id, node_id, &bucket);
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
            auth_with_bucket_read: AuthContext {
                user_id: user_with_source_read,
                realm_id,
                path_restrictions: Some(vec![PathRestriction {
                    pattern: bucket_path,
                    permission: Permission::READ,
                }]),
            },
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
