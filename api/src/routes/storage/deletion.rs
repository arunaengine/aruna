//! Serves the deletion preflight and purge job routes for files, prefixes and buckets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use aruna_core::structs::execution::job::JobPayload;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::storage_purge::{StoragePurgeScope, StoragePurgeSpec};
use aruna_operations::driver::drive;
use aruna_operations::jobs::JOB_RETENTION_MS;
use aruna_operations::jobs::service::submit_purge_job;
use aruna_operations::metadata::api::set_preview_distributed;
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::multipart::uploads::{ListUploadsInput, ListUploadsOperation};
use aruna_operations::s3::object::versions::{
    ListVersionsError, ListVersionsInput, ListVersionsItem, ListVersionsOperation,
};
use aruna_operations::sync::sync_relationship::{
    ListRelationshipsOperation, SyncRelationshipDirection,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, permission_granted, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::{bearer_token_string, map_api_error};
use crate::routes::metadata::links::DataLinkDocument;
use crate::server::state::ServerState;

const DEFAULT_PREFLIGHT_LIMIT: usize = 1_000;
const MAX_PREFLIGHT_LIMIT: usize = 1_000;
/// Listing pages a delete preview reads past while every version in them is hidden.
const HIDDEN_PAGES: usize = 64;
/// Versions one delete preview page passes over at most, hidden or filtered out ones included.
const PAGE_VERSIONS: usize = 10_000;
/// Datasets a distributed bucket's deletion preflight names at most.
const PREVIEW_DATASETS: usize = 25;

#[derive(OpenApi)]
#[openapi()]
pub struct StorageDeletionDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(StorageDeletionDoc::openapi())
        .routes(routes!(deletion_preflight))
        .routes(routes!(submit_purge))
        .routes(routes!(delete_preview))
        .routes(routes!(control_purge))
        .routes(routes!(get_purge))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PurgeScopeRequest {
    File { bucket: String, key: String },
    Prefix { bucket: String, prefix: String },
    Bucket { bucket: String },
}

impl PurgeScopeRequest {
    fn to_core(&self) -> ServerResult<StoragePurgeScope> {
        let scope = match self {
            Self::File { bucket, key } if !bucket.is_empty() && !key.is_empty() => {
                StoragePurgeScope::File {
                    bucket: bucket.clone(),
                    key: key.clone(),
                }
            }
            Self::Prefix { bucket, prefix } if !bucket.is_empty() && !prefix.is_empty() => {
                StoragePurgeScope::Prefix {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                }
            }
            Self::Bucket { bucket } if !bucket.is_empty() => StoragePurgeScope::Bucket {
                bucket: bucket.clone(),
            },
            _ => return Err(ServerError::BadRequest),
        };
        Ok(scope)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeletionPreflightRequest {
    pub scope: PurgeScopeRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_key_marker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id_marker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_key_marker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "multipart_upload_id_marker")]
    pub multipart_upload_marker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PurgeCountsResponse {
    pub current_heads: u64,
    pub noncurrent_versions: u64,
    pub delete_markers: u64,
    pub open_multipart_uploads: u64,
    /// False means every number is the count in this bounded page, not a total.
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PurgePermissionsResponse {
    pub read: bool,
    pub purge: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PurgeTruncationResponse {
    pub truncated: bool,
    pub versions_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "next_version_key_marker")]
    pub next_version_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "next_version_id_marker")]
    pub next_version_marker: Option<String>,
    pub multipart_uploads_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "next_multipart_key_marker")]
    pub next_multipart_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "next_multipart_upload_id_marker")]
    pub next_multipart_marker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = SyncDeletionSideEffectResponse)]
pub struct SyncSideResponse {
    pub relationship_id: String,
    pub direction: String,
    pub source: String,
    pub target: String,
    pub action: String,
    pub blocker: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReferenceCoverageResponse {
    pub complete: bool,
    pub hidden_references_exist: Option<bool>,
    pub queried_nodes: u32,
    pub failed_nodes: u32,
    pub index_freshness: String,
    pub excluded: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeletionPreflightResponse {
    pub scope: PurgeScopeRequest,
    pub counts: PurgeCountsResponse,
    #[serde(rename = "sync_relationships_apply_to_bucket_delete")]
    pub apply_relationships_delete: bool,
    pub sync_relationships: Vec<SyncSideResponse>,
    pub permissions: PurgePermissionsResponse,
    pub truncation: PurgeTruncationResponse,
    /// The Realm-complete backlink service is a separate dependency. Until it
    /// is available, this field explicitly refuses to imply "unreferenced".
    pub reference_coverage: ReferenceCoverageResponse,
    /// For a whole distributed bucket: readable datasets that link it, its folders or its
    /// files, at most 25.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub linked_datasets: Vec<DataLinkDocument>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitPurgeRequest {
    pub scope: PurgeScopeRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Also deletes the linked-storage files of the purged versions.
    #[serde(default)]
    pub delete_storage_files: bool,
    /// The bucket name typed back; required to delete the storage files of a whole bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitPurgeResponse {
    pub job_id: String,
    pub created: bool,
    pub status_url: String,
}

#[utoipa::path(
    post,
    path = "/data/storage/deletion/preflight",
    tag = "data/storage",
    summary = "Preview what a permanent deletion would destroy",
    description = r#"Reports the bounded inventory and consequences of permanently deleting one storage scope.

**Authentication**: realm bearer token with READ on the selected scope; `permissions.purge` reports
separately whether the same caller may run the purge.

**Behavior**
- Nothing is deleted or reserved here.
- The inventory is one bounded page of this node's own rows: `counts.complete` false means the
  numbers count that page rather than the scope, and `truncation` carries the resume markers.
- Sync relationships a bucket delete would act on are listed, together with the ones that block it.
- `reference_coverage` never implies that the scope is unreferenced: it reports how much of the
  backlink question this node could answer at all.
- For a whole distributed bucket, every metadata node reports the datasets that link the bucket,
  its folders or its verified files: `linked_datasets` names up to 25 readable ones, and
  `reference_coverage.complete` is true only when every node answered all of them with nothing
  unsettled or unverified. References by content hash alone carry no bucket and are excluded."#,
    request_body(
        content = DeletionPreflightRequest,
        description = "The scope to inspect plus an optional page bound and resume markers",
        example = json!({
            "scope": {
                "kind": "prefix",
                "bucket": "datasets",
                "prefix": "raw/"
            },
            "limit": 1000
        })
    ),
    responses(
        (
            status = 200,
            description = "Bounded, scope-aware deletion inventory and consequences",
            body = DeletionPreflightResponse,
            example = json!({
                "scope": {
                    "kind": "prefix",
                    "bucket": "datasets",
                    "prefix": "raw/"
                },
                "counts": {
                    "current_heads": 42,
                    "noncurrent_versions": 7,
                    "delete_markers": 1,
                    "open_multipart_uploads": 0,
                    "complete": true
                },
                "sync_relationships_apply_to_bucket_delete": false,
                "sync_relationships": [],
                "permissions": {
                    "read": true,
                    "purge": false
                },
                "truncation": {
                    "truncated": false,
                    "versions_truncated": false,
                    "multipart_uploads_truncated": false
                },
                "reference_coverage": {
                    "complete": false,
                    "hidden_references_exist": null,
                    "queried_nodes": 1,
                    "failed_nodes": 0,
                    "index_freshness": "local",
                    "excluded": [
                        "remote_nodes"
                    ]
                }
            })
        ),
        (status = 400, description = "Invalid scope, marker, or limit", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller cannot read the selected scope", body = ErrorResponse),
        (status = 404, description = "Bucket not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn deletion_preflight(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<DeletionPreflightRequest>,
) -> ServerResult<(StatusCode, Json<DeletionPreflightResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let scope = request.scope.to_core()?;
    let info = bucket_info(&state, scope.bucket()).await?;
    let path = scope_permission_path(&state, info.group_id, &scope);
    let can_read = permission_granted(&state, &auth, path.clone(), Permission::READ).await?;
    if !can_read {
        return Err(ServerError::Forbidden);
    }
    let can_purge = permission_granted(&state, &auth, path, Permission::WRITE).await?;
    let limit = request.limit.unwrap_or(DEFAULT_PREFLIGHT_LIMIT);
    if !(1..=MAX_PREFLIGHT_LIMIT).contains(&limit)
        || (request.version_id_marker.is_some() && request.version_key_marker.is_none())
        || (request.multipart_upload_marker.is_some() && request.multipart_key_marker.is_none())
    {
        return Err(ServerError::BadRequest);
    }
    let version_id_marker = request
        .version_id_marker
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequest)?;
    let multipart_upload_marker = request
        .multipart_upload_marker
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequest)?;

    let versions = drive(
        ListVersionsOperation::new(ListVersionsInput {
            bucket: scope.bucket().to_string(),
            prefix: scope.list_prefix().map(str::to_string),
            delimiter: None,
            key_marker: request.version_key_marker,
            version_id_marker,
            max_keys: Some(limit),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let uploads = drive(
        ListUploadsOperation::new(ListUploadsInput {
            bucket: scope.bucket().to_string(),
            prefix: scope.list_prefix().map(str::to_string),
            delimiter: None,
            key_marker: request.multipart_key_marker,
            upload_id_marker: multipart_upload_marker,
            max_uploads: limit,
        })
        .complete_scan(),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;

    let (items, versions_truncated, next_version_key, next_version_marker) =
        scoped_version_page(&scope, versions);
    let (uploads, uploads_truncated, next_multipart_key, next_multipart_marker) =
        scoped_multipart_page(&scope, uploads);
    let mut current_heads = 0u64;
    let mut noncurrent_versions = 0u64;
    let mut delete_markers = 0u64;
    for item in items {
        match item {
            ListVersionsItem::Version {
                is_latest: true, ..
            } => current_heads += 1,
            ListVersionsItem::Version { .. } => noncurrent_versions += 1,
            ListVersionsItem::DeleteMarker { .. } => delete_markers += 1,
        }
    }
    let truncated = versions_truncated || uploads_truncated;
    let sync_relationships = if scope.is_bucket() {
        list_sync_effects(&state, scope.bucket()).await?
    } else {
        Vec::new()
    };
    let (reference_coverage, linked_datasets) = match (&info.distribution, scope.is_bucket()) {
        (Some(binding), true) => set_coverage(&state, auth, bearer, binding.set_id).await?,
        _ => (unevaluated_coverage(), Vec::new()),
    };

    Ok((
        StatusCode::OK,
        Json(DeletionPreflightResponse {
            scope: request.scope,
            counts: PurgeCountsResponse {
                current_heads,
                noncurrent_versions,
                delete_markers,
                open_multipart_uploads: uploads.len() as u64,
                complete: !truncated,
            },
            apply_relationships_delete: scope.is_bucket(),
            sync_relationships,
            permissions: PurgePermissionsResponse {
                read: true,
                purge: can_purge,
            },
            truncation: PurgeTruncationResponse {
                truncated,
                versions_truncated,
                next_version_key,
                next_version_marker: next_version_marker.map(|id| id.to_string()),
                multipart_uploads_truncated: uploads_truncated,
                next_multipart_key,
                next_multipart_marker: next_multipart_marker.map(|id| id.to_string()),
            },
            reference_coverage,
            linked_datasets,
        }),
    ))
}

fn unevaluated_coverage() -> ReferenceCoverageResponse {
    ReferenceCoverageResponse {
        complete: false,
        hidden_references_exist: None,
        queried_nodes: 0,
        failed_nodes: 0,
        index_freshness: "not_evaluated".to_string(),
        excluded: vec![
            "realm_backlink_fanout_unavailable".to_string(),
            "imported_relative_or_external_file_ids".to_string(),
        ],
    }
}

/// The datasets that link a distributed bucket, from the set's postings on every metadata
/// node. References that name a file only by its content hash carry no set.
async fn set_coverage(
    state: &Arc<ServerState>,
    auth: AuthContext,
    bearer: Option<ValidatedBearer>,
    set_id: Ulid,
) -> ServerResult<(ReferenceCoverageResponse, Vec<DataLinkDocument>)> {
    let preview = set_preview_distributed(
        state.get_ctx().as_ref(),
        (state.get_realm_id(), state.get_node_id()),
        Some(auth),
        bearer_token_string(bearer),
        (set_id, PREVIEW_DATASETS),
    )
    .await
    .map_err(map_api_error)?;
    let freshness = match (
        preview.pending_materializations,
        preview.failed_materializations,
    ) {
        (0, 0) => "current",
        (_, 0) => "pending",
        (0, _) => "failed",
        _ => "mixed",
    };
    let coverage = ReferenceCoverageResponse {
        complete: preview.complete(),
        hidden_references_exist: Some(preview.hidden),
        queried_nodes: preview.nodes_queried as u32,
        failed_nodes: preview.nodes_failed as u32,
        index_freshness: freshness.to_string(),
        excluded: vec!["hash_only_references".to_string()],
    };
    let datasets = preview.documents.into_iter().map(Into::into).collect();
    Ok((coverage, datasets))
}

#[utoipa::path(
    post,
    path = "/data/storage/purge/jobs",
    tag = "data/storage",
    summary = "Delete one storage scope permanently on this node",
    description = r#"Queues the permanent deletion of one storage scope, which the portal calls Delete permanently, as a node-local purge job.

**Authentication**: realm bearer token with WRITE on the selected scope.

**Behavior**
- The purge runs as a job: a 201 means it was durably queued, never that anything was deleted yet,
  so poll the returned `status_url`.
- Deletion is permanent and node-local: no version survives it here, copies other nodes hold are
  untouched, and the preflight route reports on it beforehand.
- An `idempotency_key` makes a retried submission return the retained job instead of queuing a
  second purge.
- `delete_storage_files` also deletes the files of the purged versions in linked storage. A file
  is deleted only at the exact backend version Aruna recorded, only after the versions are gone
  and on disk, and only while no other key, version, link or copy reads it; such files stay.
  It needs an unrestricted token, and for a bucket `confirm` set to the bucket name.
- `POST /data/storage/purge/jobs/{job_id}/{action}` pauses, resumes or aborts the purge."#,
    request_body(
        content = SubmitPurgeRequest,
        description = "The scope to purge plus an optional idempotency key",
        example = json!({
            "scope": {
                "kind": "prefix",
                "bucket": "datasets",
                "prefix": "raw/"
            },
            "idempotency_key": "purge-raw-2026-04-09-001"
        })
    ),
    responses(
        (
            status = 201,
            description = "Purge job durably queued; nothing is deleted yet",
            body = SubmitPurgeResponse,
            example = json!({
                "job_id": "01JABCDEF0123456789ABCDEFG",
                "created": true,
                "status_url": "/api/v1/compute/jobs/01JABCDEF0123456789ABCDEFG"
            })
        ),
        (
            status = 200,
            description = "The idempotency key matched a retained job, which is returned unchanged",
            body = SubmitPurgeResponse,
            example = json!({
                "job_id": "01JABCDEF0123456789ABCDEFG",
                "created": false,
                "status_url": "/api/v1/compute/jobs/01JABCDEF0123456789ABCDEFG"
            })
        ),
        (status = 400, description = "Invalid scope or idempotency key, or `confirm` does not name the bucket", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller may not delete the selected scope permanently, or a restricted token asks to delete storage files", body = ErrorResponse),
        (status = 404, description = "Bucket not found", body = ErrorResponse),
        (status = 409, description = "The idempotency key is retained for a purge of a different scope", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_purge(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<SubmitPurgeRequest>,
) -> ServerResult<(StatusCode, Json<SubmitPurgeResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    if request
        .idempotency_key
        .as_ref()
        .is_some_and(|key| key.is_empty() || key.len() > 256)
    {
        return Err(ServerError::BadRequest);
    }
    let scope = request.scope.to_core()?;
    let files = request.delete_storage_files;
    if files && scope.is_bucket() && request.confirm.as_deref() != Some(scope.bucket()) {
        return Err(ServerError::BadRequestMessage(
            "set `confirm` to the bucket name to delete its storage files".to_string(),
        ));
    }
    let info = bucket_info(&state, scope.bucket()).await?;
    let path = scope_permission_path(&state, info.group_id, &scope);
    // A token limited to some paths could reach files other keys read through the storage.
    if (files && auth.path_restrictions.is_some())
        || !permission_granted(&state, &auth, path, Permission::WRITE).await?
    {
        return Err(ServerError::Forbidden);
    }
    let spec = StoragePurgeSpec {
        scope,
        group_id: info.group_id,
        auth_context: auth,
        node_id: state.get_node_id(),
    };
    let result = submit_purge_job(
        &state.get_ctx(),
        (spec, files),
        state.get_node_id(),
        request.idempotency_key,
        JOB_RETENTION_MS,
    )
    .await
    .map_err(crate::routes::execution::jobs::map_submit_error)?;
    let urls = crate::routes::execution::jobs::job_urls(&state, result.job_id).await?;
    Ok((
        if result.created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(SubmitPurgeResponse {
            job_id: result.job_id.to_string(),
            created: result.created,
            status_url: urls.status_url,
        }),
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteImpactRequest {
    /// `file`, `folder` or `bucket`.
    pub kind: String,
    /// The file key or the folder prefix ending with `/`; empty for a bucket.
    #[serde(default)]
    pub key: String,
    /// Entries per page: 1 to 1000, default 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// The `next` value of the page before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    /// Only versions of keys that contain this text are listed; the totals stay for the scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StorageFileImpact {
    pub path: String,
    pub bytes: u64,
    /// Why the file stays after the purge; omitted when it is deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteImpactItem {
    pub key: String,
    pub version_id: String,
    /// Bytes of the version in Aruna; 0 for a delete marker.
    pub bytes: u64,
    /// The linked-storage file the version reads, listed with the first version reading it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_file: Option<StorageFileImpact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteImpact {
    /// Keys, versions and their bytes in Aruna.
    pub files: u64,
    pub versions: u64,
    pub bytes: u64,
    /// Files in linked storage the versions read, and their bytes.
    pub storage_files: u64,
    pub storage_bytes: u64,
    /// Files of this page that would stay, because something else reads them.
    pub kept_files: u64,
    /// `false` when the totals stopped at their limit and are lower bounds.
    pub complete: bool,
    pub items: Vec<DeleteImpactItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/delete/preview",
    tag = "data/storage",
    summary = "Preview a permanent delete with its storage files",
    description = r#"Reports what a permanent delete of a file, a folder or the whole bucket removes in Aruna and in linked storage, with one page of the versions and the storage file each reads. Nothing changes.

**Authentication**: realm bearer token with READ on the selected scope.

**Behavior**
- Totals count every version of the scope on this node, its bytes, and each linked-storage file
  once. `kept_files` counts the files of this page that would stay because a key outside the
  scope, another link or a missing exact backend version keeps them.
- Pages follow key and version order; `next` resumes after the last version passed over and
  names only keys the caller may read. `filter` lists only keys that contain its text; the
  totals stay for the whole scope.
- Keys the caller may not read are neither counted nor listed.

**Limits**
- 1 to 1000 entries per page. Totals pass over at most 5,000 versions, hidden ones
  included; `complete` is then false and the totals are lower bounds. A page passes over at most
  10,000 versions, also with a filter, and then names `next`. 64 listing pages of only hidden
  versions answer 409. A page checks at most 10,000 hash aliases for other readers of its files;
  a file left unchecked counts as kept and `complete` is false. A key with more versions than
  these bounds ends the totals and the page with `complete` false."#,
    params(("bucket" = String, Path, description = "Bucket to inspect")),
    request_body(content = DeleteImpactRequest, example = json!({"kind": "folder", "key": "raw/"})),
    responses(
        (status = 200, description = "Totals and one page", body = DeleteImpact, example = json!({
            "files": 2, "versions": 3, "bytes": 3072, "storage_files": 2, "storage_bytes": 2048,
            "kept_files": 0, "complete": true,
            "items": [{
                "key": "raw/a.faa.gz", "version_id": "01JA7Q3K2M4N5P6Q7R8S9T0V1W", "bytes": 1024,
                "storage_file": {"path": "raw/a.faa.gz~01JA7Q3K2M4N5P6Q7R8S9T0V1W", "bytes": 1024}
            }]
        })),
        (status = 400, description = "Invalid kind, key, limit or cursor", body = ErrorResponse),
        (status = 409, description = "Too many versions in a row are hidden from the caller", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller cannot read the scope", body = ErrorResponse),
        (status = 404, description = "Bucket not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_preview(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    axum::extract::Path(bucket): axum::extract::Path<String>,
    Json(request): Json<DeleteImpactRequest>,
) -> ServerResult<Json<DeleteImpact>> {
    let auth = require_realm_auth(&state, auth)?;
    let scope = match request.kind.as_str() {
        "file" => PurgeScopeRequest::File {
            bucket: bucket.clone(),
            key: request.key.clone(),
        },
        "folder" | "prefix" => PurgeScopeRequest::Prefix {
            bucket: bucket.clone(),
            prefix: request.key.clone(),
        },
        "bucket" => PurgeScopeRequest::Bucket {
            bucket: bucket.clone(),
        },
        _ => return Err(ServerError::BadRequest),
    }
    .to_core()?;
    let limit = request.limit.unwrap_or(100);
    if !(1..=MAX_PREFLIGHT_LIMIT).contains(&limit) {
        return Err(ServerError::BadRequest);
    }
    let (key_marker, version_marker) = match request.after.as_deref() {
        Some(after) => decode_marker(after).map(|(key, id)| (Some(key), Some(id)))?,
        None => (None, None),
    };
    let info = bucket_info(&state, &bucket).await?;
    let path = scope_permission_path(&state, info.group_id, &scope);
    if !permission_granted(&state, &auth, path, Permission::READ).await? {
        return Err(ServerError::Forbidden);
    }
    let context = state.get_ctx();
    // Only keys the caller may read are counted, listed or named by a cursor.
    let scope_of = (auth.realm_id, info.group_id, state.get_node_id());
    let access = aruna_operations::auth::request_authorization::BucketAccess::load(
        &context, &auth, scope_of, &bucket,
    )
    .await
    .map_err(|_| ServerError::Forbidden)?;
    let readable = |key: &str| access.allows(Some(key), &Permission::READ);
    let totals = aruna_operations::blob::purge::scope_totals(&context, &scope, &readable)
        .await
        .map_err(ServerError::InternalError)?;
    let (mut items, mut marker) = (Vec::new(), (key_marker, version_marker));
    let (mut next, mut more, mut passed, mut cut) = (None, false, 0, false);
    for _ in 0..HIDDEN_PAGES {
        let listing = ListVersionsOperation::new(ListVersionsInput {
            bucket: bucket.clone(),
            prefix: scope.list_prefix().map(str::to_string),
            delimiter: None,
            key_marker: marker.0.clone(),
            version_id_marker: marker.1,
            max_keys: Some(limit - items.len()),
        });
        let page = match drive(
            listing.with_row_budget(PAGE_VERSIONS - passed + limit),
            &context,
        )
        .await
        {
            Ok(page) => page,
            // A key with more versions than the page may read ends the page incomplete.
            Err(ListVersionsError::TooManyVersions) => {
                (cut, more) = (true, false);
                break;
            }
            Err(error) => return Err(ServerError::InternalError(error.to_string())),
        };
        let (page, truncated, next_key, next_version) = scoped_version_page(&scope, page);
        passed += page.len();
        let mut last_seen = None;
        for item in page {
            let (ListVersionsItem::Version {
                key, version_id, ..
            }
            | ListVersionsItem::DeleteMarker {
                key, version_id, ..
            }) = &item;
            if readable(key) {
                last_seen = Some((key.clone(), *version_id));
                if request
                    .filter
                    .as_deref()
                    .is_none_or(|filter| key.contains(filter))
                {
                    items.push(item);
                }
            }
        }
        more = truncated;
        if !truncated {
            next = None;
            break;
        }
        // A cursor names only a version the caller may read.
        next = last_seen.or(next);
        if items.len() == limit || passed >= PAGE_VERSIONS {
            break;
        }
        marker = (next_key, next_version);
    }
    if more && next.is_none() {
        return Err(ServerError::Conflict(
            "too many versions in a row are hidden from the caller".to_string(),
        ));
    }
    let versions: Vec<_> = items
        .iter()
        .map(|item| match item {
            ListVersionsItem::Version {
                key, version_id, ..
            }
            | ListVersionsItem::DeleteMarker {
                key, version_id, ..
            } => (key.clone(), *version_id),
        })
        .collect();
    let files = aruna_operations::blob::purge::storage_files(
        &context,
        (&bucket, info.group_id),
        &versions,
        Some(&scope),
    )
    .await
    .map_err(ServerError::InternalError)?;
    let kept_files = files.iter().filter(|file| file.kept.is_some()).count() as u64;
    let unchecked = aruna_operations::blob::purge::UNCHECKED_READERS;
    let unchecked = files
        .iter()
        .any(|file| file.kept.as_deref() == Some(unchecked));
    let items = items
        .into_iter()
        .map(|item| {
            let (key, version_id, bytes) = match item {
                ListVersionsItem::Version {
                    key,
                    version_id,
                    location,
                    ..
                } => (
                    key,
                    version_id,
                    location.map_or(0, |location| location.blob_size),
                ),
                ListVersionsItem::DeleteMarker {
                    key, version_id, ..
                } => (key, version_id, 0),
            };
            let file = files
                .iter()
                .find(|file| file.key == key && file.version_id == version_id);
            DeleteImpactItem {
                storage_file: file.map(|file| StorageFileImpact {
                    path: file.location.backend_path.clone(),
                    bytes: file.location.blob_size,
                    kept: file.kept.clone(),
                }),
                key,
                version_id: version_id.to_string(),
                bytes,
            }
        })
        .collect();
    let next = next.map(|(key, id)| encode_marker(&key, id));
    Ok(Json(DeleteImpact {
        files: totals.keys,
        versions: totals.versions,
        bytes: totals.bytes,
        storage_files: totals.storage_files,
        storage_bytes: totals.storage_bytes,
        kept_files,
        complete: totals.complete && !unchecked && !cut,
        items,
        next,
    }))
}

/// A page cursor: the key and version id the next page starts after.
fn encode_marker(key: &str, version_id: Ulid) -> String {
    use base64::Engine;
    let raw = format!("{version_id}{key}");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
}

fn decode_marker(cursor: &str) -> ServerResult<(String, Ulid)> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| ServerError::BadRequest)?;
    let raw = String::from_utf8(bytes).map_err(|_| ServerError::BadRequest)?;
    let (id, key) = raw.split_at_checked(26).ok_or(ServerError::BadRequest)?;
    let id = Ulid::from_string(id).map_err(|_| ServerError::BadRequest)?;
    Ok((key.to_string(), id))
}

#[utoipa::path(
    post,
    path = "/data/storage/purge/jobs/{job_id}/{action}",
    tag = "data/storage",
    summary = "Pause, resume or abort a purge",
    description = r#"Pauses, resumes or aborts a running purge job of this node.

**Authentication**: realm bearer token with WRITE on the purge's scope, as its submission.

**Behavior**
- `pause` stops the purge between batches and keeps its position; `resume` continues. The
  answer, and `GET` on the job, show the pause.
- `abort` ends the purge for good, also a paused one. Versions already deleted stay deleted, and
  the storage files of those versions are still handed to their release, so nothing is left
  half-done. The job then shows `cancelled`, and a new purge may start."#,
    params(
        ("job_id" = String, Path, description = "The purge job"),
        ("action" = String, Path, description = "`pause`, `resume` or `abort`")
    ),
    responses(
        (status = 200, description = "The purge after the change", body = PurgeJobResponse, example = json!({
            "job_id": "01JABCDEF0123456789ABCDEFG", "state": "paused", "paused": true,
            "delete_storage_files": true
        })),
        (status = 400, description = "Invalid job id or unknown action", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller may not delete the purge's scope permanently", body = ErrorResponse),
        (status = 404, description = "No purge job with this id on this node", body = ErrorResponse),
        (status = 409, description = "The purge already ended", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn control_purge(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    axum::extract::Path((job_id, action)): axum::extract::Path<(String, String)>,
) -> ServerResult<Json<PurgeJobResponse>> {
    use aruna_operations::jobs::service::{CancelJobOutcome, cancel_owned_job};
    let auth = require_realm_auth(&state, auth)?;
    let job_id = crate::jobs::parse_job_id(&job_id).map_err(|_| ServerError::BadRequest)?;
    let context = state.get_ctx();
    let record =
        aruna_operations::jobs::store::read_job_record(&context.storage_handle, job_id, None)
            .await
            .map_err(ServerError::InternalError)?
            .ok_or(ServerError::NotFound)?;
    let Some(spec) = record.payload.purge_spec() else {
        return Err(ServerError::NotFound);
    };
    let path = scope_permission_path(&state, spec.group_id, &spec.scope);
    // As at its submission, a purge of storage files is not steered by a restricted token.
    let files = matches!(record.payload, JobPayload::StoragePurgeFiles(_));
    if (files && auth.path_restrictions.is_some())
        || !permission_granted(&state, &auth, path, Permission::WRITE).await?
    {
        return Err(ServerError::Forbidden);
    }
    if record.state.is_terminal() {
        return Err(ServerError::Conflict("the purge already ended".to_string()));
    }
    let failed = |error: String| ServerError::InternalError(error);
    let id = job_id.as_ulid();
    match action.as_str() {
        "pause" | "resume" => {
            let paused = action == "pause";
            aruna_operations::blob::purge::set_paused(&context, id, paused)
                .await
                .map_err(failed)?;
        }
        "abort" => {
            let runtime = state.jobs_runtime();
            let outcome = cancel_owned_job(&context, &runtime, record.created_by, job_id)
                .await
                .map_err(failed)?;
            // Its planned files wait in the cleanup queue, which releases them once it ended.
            if let CancelJobOutcome::AlreadyTerminal(_) | CancelJobOutcome::Requested(_) = outcome
                && purge_ended(&state, job_id).await?
            {
                aruna_operations::blob::purge::set_paused(&context, id, false)
                    .await
                    .map_err(failed)?;
            }
        }
        _ => return Err(ServerError::BadRequest),
    }
    purge_job(&state, job_id).await
}

/// A purge job as its controls see it.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PurgeJobResponse {
    pub job_id: String,
    /// The job state, or `paused` while a running purge waits for its resume.
    pub state: String,
    /// The caller paused the purge; it resumes with `POST .../resume`.
    pub paused: bool,
    /// The purge also deletes the linked-storage files of its versions.
    pub delete_storage_files: bool,
}

async fn purge_job(
    state: &ServerState,
    job_id: aruna_core::structs::execution::job::JobId,
) -> ServerResult<Json<PurgeJobResponse>> {
    let context = state.get_ctx();
    let record =
        aruna_operations::jobs::store::read_job_record(&context.storage_handle, job_id, None)
            .await
            .map_err(ServerError::InternalError)?
            .ok_or(ServerError::NotFound)?;
    let paused = aruna_operations::blob::purge::is_paused(&context, job_id.as_ulid())
        .await
        .map_err(ServerError::InternalError)?;
    let paused = paused && !record.state.is_terminal();
    Ok(Json(PurgeJobResponse {
        job_id: job_id.to_string(),
        state: match paused {
            true => "paused".to_string(),
            false => record.state.name().to_string(),
        },
        paused,
        delete_storage_files: matches!(record.payload, JobPayload::StoragePurgeFiles(_)),
    }))
}

#[utoipa::path(
    get,
    path = "/data/storage/purge/jobs/{job_id}",
    tag = "data/storage",
    summary = "Show whether a purge is paused",
    description = r#"Shows a purge job of this node with its pause: `state` is `paused` while the caller paused a running purge, else the job state. Progress and the result stay on the job status route.

**Authentication**: realm bearer token with READ on the purge's scope."#,
    params(("job_id" = String, Path, description = "The purge job")),
    responses(
        (status = 200, description = "The purge", body = PurgeJobResponse, example = json!({
            "job_id": "01JABCDEF0123456789ABCDEFG", "state": "running", "paused": false,
            "delete_storage_files": true
        })),
        (status = 400, description = "Invalid job id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller cannot read the purge's scope", body = ErrorResponse),
        (status = 404, description = "No purge job with this id on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_purge(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> ServerResult<Json<PurgeJobResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let job_id = crate::jobs::parse_job_id(&job_id).map_err(|_| ServerError::BadRequest)?;
    let storage = &state.get_ctx().storage_handle;
    let record = aruna_operations::jobs::store::read_job_record(storage, job_id, None)
        .await
        .map_err(ServerError::InternalError)?
        .ok_or(ServerError::NotFound)?;
    let Some(spec) = record.payload.purge_spec() else {
        return Err(ServerError::NotFound);
    };
    let path = scope_permission_path(&state, spec.group_id, &spec.scope);
    if !permission_granted(&state, &auth, path, Permission::READ).await? {
        return Err(ServerError::Forbidden);
    }
    purge_job(&state, job_id).await
}

async fn purge_ended(
    state: &ServerState,
    job_id: aruna_core::structs::execution::job::JobId,
) -> ServerResult<bool> {
    let storage = &state.get_ctx().storage_handle;
    let record = aruna_operations::jobs::store::read_job_record(storage, job_id, None)
        .await
        .map_err(ServerError::InternalError)?;
    Ok(record.is_some_and(|record| record.state.is_terminal()))
}

pub(crate) async fn bucket_info(
    state: &ServerState,
    bucket: &str,
) -> ServerResult<aruna_core::structs::storage::blob::BucketInfo> {
    match drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(info) => Ok(info),
        Err(GetBucketError::NotFound) => Err(ServerError::NotFound),
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

fn scope_permission_path(state: &ServerState, group_id: Ulid, scope: &StoragePurgeScope) -> String {
    match scope {
        StoragePurgeScope::Bucket { bucket } | StoragePurgeScope::Prefix { bucket, .. } => {
            bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket)
        }
        StoragePurgeScope::File { bucket, key } => object_permission_path(
            state.get_realm_id(),
            group_id,
            state.get_node_id(),
            bucket,
            key,
        ),
    }
}

fn scoped_version_page(
    scope: &StoragePurgeScope,
    result: aruna_operations::s3::object::versions::ListVersionsResult,
) -> (Vec<ListVersionsItem>, bool, Option<String>, Option<Ulid>) {
    let mut items = result.items;
    let mut truncated = result.is_truncated;
    let mut key_marker = result.next_key_marker;
    let mut version_marker = result.next_version_marker;
    if let StoragePurgeScope::File { key, .. } = scope {
        items.retain(|item| match item {
            ListVersionsItem::Version { key: item, .. }
            | ListVersionsItem::DeleteMarker { key: item, .. } => item == key,
        });
        if key_marker.as_deref() != Some(key.as_str()) {
            truncated = false;
            key_marker = None;
            version_marker = None;
        }
    }
    (items, truncated, key_marker, version_marker)
}

fn scoped_multipart_page(
    scope: &StoragePurgeScope,
    result: aruna_operations::s3::multipart::uploads::ListUploadsResult,
) -> (
    Vec<aruna_core::structs::storage::multipart::MultipartUpload>,
    bool,
    Option<String>,
    Option<Ulid>,
) {
    let mut uploads = result.uploads;
    let mut truncated = result.is_truncated;
    let mut key_marker = result.next_key_marker;
    let mut upload_marker = result.next_upload_marker;
    if let StoragePurgeScope::File { key, .. } = scope {
        uploads.retain(|upload| upload.key == *key);
        if key_marker.as_deref() != Some(key.as_str()) {
            truncated = false;
            key_marker = None;
            upload_marker = None;
        }
    }
    (uploads, truncated, key_marker, upload_marker)
}

async fn list_sync_effects(
    state: &ServerState,
    bucket: &str,
) -> ServerResult<Vec<SyncSideResponse>> {
    let mut response = Vec::new();
    let mut seen = BTreeSet::new();
    for (direction, label) in [
        (SyncRelationshipDirection::Outgoing, "outgoing"),
        (SyncRelationshipDirection::Incoming, "incoming"),
    ] {
        let relationships = drive(
            ListRelationshipsOperation::new(direction, Some(bucket.to_string())),
            &state.get_ctx(),
        )
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
        for relationship in relationships {
            if !seen.insert((relationship.id, label)) {
                continue;
            }
            response.push(SyncSideResponse {
                relationship_id: relationship.id.to_string(),
                direction: label.to_string(),
                source: relationship.source.to_string(),
                target: relationship.target.to_string(),
                action: "remove_local_relationship_and_repair_remote_mirror".to_string(),
                blocker: false,
            });
        }
    }
    Ok(response)
}

#[cfg(test)]
#[path = "deletion_tests.rs"]
mod tests;
