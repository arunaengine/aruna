//! Serves the bucket storage check routes: find broken files and delete them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::routing::ensure_group_admin;
use crate::auth::require_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::broken::{BrokenReason, StorageCheck};
use aruna_operations::blob::migration::check::{clean_broken, list_broken, read_check};
use aruna_operations::driver::{drive, now_ms};
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::bucket::storage_check::{StartCheckError, StartCheckOperation};
use aruna_operations::s3::object::delete::DeleteObjectInput;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Broken versions one listing or cleanup request handles at most.
const PAGE_MAX: usize = 1000;

#[derive(OpenApi)]
#[openapi()]
pub struct StorageCheckDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(StorageCheckDoc::openapi())
        .routes(routes!(get_bucket_check, start_bucket_check))
        .routes(routes!(clean_bucket_check))
}

#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
pub struct BucketCheckQuery {
    /// The `next` value of the previous page; omitted starts at the first broken file.
    #[serde(default)]
    pub after: Option<String>,
    /// Broken files per page, 1 to 1000; omitted is 100.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Progress of this node's last check of the bucket.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CheckProgress {
    /// Current versions checked so far.
    pub checked: u64,
    /// Broken versions the check found.
    pub broken: u64,
    /// Versions whose storage could not answer; a later check tries them again.
    pub failed: u64,
    pub started_at_ms: u64,
    /// Set once every current version was checked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
}

impl From<StorageCheck> for CheckProgress {
    fn from(check: StorageCheck) -> Self {
        Self {
            checked: check.checked,
            broken: check.broken,
            failed: check.failed,
            started_at_ms: check.started_at_ms,
            finished_at_ms: check.finished_at_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BrokenCause {
    /// Aruna has no record of the stored copy.
    MissingRecord,
    /// The group storage holding the copy was removed or disabled.
    StorageRemoved,
    /// The storage has no file at the copy's path.
    MissingBlob,
}

impl From<BrokenReason> for BrokenCause {
    fn from(reason: BrokenReason) -> Self {
        match reason {
            BrokenReason::MissingRecord => Self::MissingRecord,
            BrokenReason::StorageRemoved => Self::StorageRemoved,
            BrokenReason::MissingBlob => Self::MissingBlob,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BrokenFile {
    pub key: String,
    pub version_id: String,
    pub reason: BrokenCause,
    pub found_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketCheckResponse {
    pub bucket: String,
    /// Absent before the first check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<CheckProgress>,
    /// One page of the recorded broken files, in key order.
    pub broken: Vec<BrokenFile>,
    /// Pass as `after` for the next page; absent on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketCleanupResponse {
    pub bucket: String,
    /// Broken versions deleted by this request.
    pub deleted: u64,
    /// Recorded versions that changed since the check or could not be deleted; they stay listed.
    pub kept: u64,
    /// More recorded versions remain; send the request again.
    pub more: bool,
}

async fn bucket_group(state: &ServerState, bucket: &str) -> ServerResult<ulid::Ulid> {
    let info = drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await;
    info.map(|info| info.group_id).map_err(|error| match error {
        GetBucketError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })
}

async fn ensure_bucket(
    state: &ServerState,
    auth: &AuthContext,
    bucket: &str,
    group_id: ulid::Ulid,
    permission: Permission,
) -> ServerResult<()> {
    let path = bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket);
    crate::auth::ensure_permission(state, auth, path, permission).await
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/storage/check",
    tag = "data/storage",
    summary = "List a bucket's broken files",
    description = r#"Returns the progress of this node's last storage check of a bucket and one page of the files it found broken.

**Authentication**: realm bearer token with READ on the bucket.

**Behavior**
- A file is broken when its current version cannot be served: Aruna has no record of the stored
  copy (`missing_record`), its group storage was removed or disabled (`storage_removed`), or the
  storage has no file at its path (`missing_blob`).
- The list is kept until the next check or a cleanup. A storage move also records a version it
  cannot read because its copy is broken.
- `progress.broken` counts what the last check found; the list also shrinks with cleanups.

**Limits**
- `limit` is 1 to 1000; the default is 100."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash"),
        BucketCheckQuery
    ),
    responses(
        (
            status = 200,
            description = "The last check and one page of broken files",
            body = BucketCheckResponse,
            example = json!({
                "bucket": "research-raw",
                "progress": {
                    "checked": 1200,
                    "broken": 2,
                    "failed": 0,
                    "started_at_ms": 1790000000000_u64,
                    "finished_at_ms": 1790000060000_u64
                },
                "broken": [{
                    "key": "genes/a.faa.gz",
                    "version_id": "01JMPRT0123456789ABCDEFGHJ",
                    "reason": "missing_blob",
                    "found_at_ms": 1790000030000_u64
                }]
            })
        ),
        (status = 400, description = "Invalid `after` or `limit`", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_check(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<BucketCheckQuery>,
) -> ServerResult<Json<BucketCheckResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let limit = query.limit.unwrap_or(100);
    if !(1..=PAGE_MAX).contains(&limit) {
        return Err(ServerError::BadRequestReason(
            "`limit` must be 1 to 1000".to_string(),
        ));
    }
    let after = match query.after.as_deref().map(hex::decode).transpose() {
        Ok(after) => after.map(Into::into),
        Err(_) => return Err(ServerError::BadRequestReason("invalid `after`".to_string())),
    };
    let group_id = bucket_group(&state, &bucket).await?;
    ensure_bucket(&state, &auth, &bucket, group_id, Permission::READ).await?;
    let ctx = state.get_ctx();
    let progress = read_check(&ctx, &bucket)
        .await
        .map_err(ServerError::InternalError)?;
    let (rows, next) = list_broken(&ctx, &bucket, after, limit)
        .await
        .map_err(ServerError::InternalError)?;
    let broken = rows
        .into_iter()
        .map(|(key, row)| BrokenFile {
            key: key.key,
            version_id: key.version_id.to_string(),
            reason: row.reason.into(),
            found_at_ms: row.found_at_ms,
        })
        .collect();
    Ok(Json(BucketCheckResponse {
        bucket,
        progress: progress.map(Into::into),
        broken,
        next: next.map(|key| hex::encode(key.as_ref() as &[u8])),
    }))
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/check",
    tag = "data/storage",
    summary = "Check a bucket for broken files",
    description = r#"Starts this node's background check of the current version of every key in a bucket.

**Authentication**: realm bearer token with WRITE on the owning group's admin path.

**Behavior**
- Each current version is checked for its record of the stored copy, for its group storage and
  with one existence request to the storage. Nothing is read or changed.
- Broken versions are recorded; a version found healthy again leaves the list.
- A new request restarts the check from the first key. A node restart resumes it.
- `GET` on this path reports the progress and the broken files."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    responses(
        (
            status = 202,
            description = "The check as started; it runs in the background",
            body = CheckProgress,
            example = json!({
                "checked": 0,
                "broken": 0,
                "failed": 0,
                "started_at_ms": 1790000000000_u64
            })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node, or no longer owned by the authorized group", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_bucket_check(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<(StatusCode, Json<CheckProgress>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = bucket_group(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let operation = StartCheckOperation::new(bucket, group_id, now_ms());
    let record = drive(operation, &state.get_ctx())
        .await
        .map_err(|error| match error {
            StartCheckError::NoSuchBucket => ServerError::NotFound,
            other => ServerError::InternalError(other.to_string()),
        })?;
    Ok((StatusCode::ACCEPTED, Json(record.into())))
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/check/cleanup",
    tag = "data/storage",
    summary = "Delete a bucket's broken files",
    description = r#"Deletes the versions the last storage check recorded as broken, like an S3 `DeleteObject` with their version id.

**Authentication**: realm bearer token with WRITE on the bucket, as for deleting objects, and WRITE
on the owning group's admin path, as for the bucket's storage settings.

**Behavior**
- Only versions whose record did not change since the check are deleted. Others stay listed until
  the next check.
- A deleted current version makes the key show its previous version, if one exists.
- One request handles up to 1000 recorded versions; `more` asks for another request."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    responses(
        (
            status = 200,
            description = "What the request deleted",
            body = BucketCleanupResponse,
            example = json!({ "bucket": "research-raw", "deleted": 2, "kept": 0, "more": false })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no WRITE on the bucket or the group admin path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn clean_bucket_check(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<BucketCleanupResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = bucket_group(&state, &bucket).await?;
    ensure_bucket(&state, &auth, &bucket, group_id, Permission::WRITE).await?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let template = DeleteObjectInput {
        bucket: bucket.clone(),
        key: String::new(),
        version_id: None,
        group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        deleted_by: auth.user_id,
    };
    let restrictions = auth.path_restrictions.clone();
    let cleaned = clean_broken(&state.get_ctx(), &template, restrictions, PAGE_MAX)
        .await
        .map_err(ServerError::InternalError)?;
    Ok(Json(BucketCleanupResponse {
        bucket,
        deleted: cleaned.deleted,
        kept: cleaned.kept,
        more: cleaned.more,
    }))
}
