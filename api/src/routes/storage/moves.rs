//! Serves the bucket storage move routes: count misplaced files and move them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::compression::MigrationProgress;
use super::routing::ensure_group_admin;
use crate::auth::require_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::format::CompressionMigration;
use aruna_operations::blob::migration::misplaced::count_misplaced;
use aruna_operations::driver::{drive, now_ms};
use aruna_operations::s3::bucket::compression::MigrationStatusOperation;
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::bucket::storage_move::{StartMoveError, StartMoveOperation};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi()]
pub struct StorageMoveDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(StorageMoveDoc::openapi())
        .routes(routes!(get_bucket_move, start_bucket_move))
}

#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
pub struct BucketMoveQuery {
    /// Counts only keys under this prefix; omitted counts the whole bucket.
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketMoveRequest {
    /// Moves only keys under this prefix, such as a linked rule's folder; omitted moves all.
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketMoveResponse {
    pub bucket: String,
    /// Stored versions under `prefix` that a move would move now.
    pub misplaced: u64,
    /// `false` when the count stopped at its limit; `misplaced` is then a lower bound.
    pub complete: bool,
    /// The prefix of this node's last move; absent before any move.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub move_prefix: Option<String>,
    /// Progress of this node's last move; absent before any move.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<MigrationProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketMoveStarted {
    pub bucket: String,
    /// The keys the move handles.
    pub prefix: String,
    pub progress: MigrationProgress,
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

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/storage/move",
    tag = "data/storage",
    summary = "Count a bucket's files outside their storage",
    description = r#"Counts the stored versions of a bucket on this node that a storage move would move now, and reports the progress of the last move.

**Authentication**: realm bearer token with READ on the bucket.

**Behavior**
- A version counts when its copy is not on the storage, or not in the encoding, that the bucket,
  group and node rules choose for a new upload of its key.
- Not counted: versions of an encrypted bucket, files in a linked folder, unfinished copies,
  versions with placement policies on another storage, keys the rules cannot place now, and
  broken files a storage check recorded.
- Every version, current or not, counts on its own.
- `prefix` limits the count to keys under it, such as the folder of a linked rule.

**Limits**
- One request reads at most 10000 version rows under `prefix`, passes over at most 200000
  rows of the bucket, and reads one location row for each counted candidate;
  `complete` is `false` when it stopped there."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash"),
        BucketMoveQuery
    ),
    responses(
        (
            status = 200,
            description = "The count and the last move",
            body = BucketMoveResponse,
            example = json!({
                "bucket": "research-raw",
                "misplaced": 12,
                "complete": true,
                "move_prefix": "raw/",
                "progress": {
                    "migrated": 30,
                    "skipped": 2,
                    "failed": 0,
                    "retries": 0,
                    "started_at_ms": 1790000000000_u64,
                    "finished_at_ms": 1790000060000_u64
                }
            })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_move(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<BucketMoveQuery>,
) -> ServerResult<Json<BucketMoveResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = bucket_group(&state, &bucket).await?;
    let path = bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), &bucket);
    crate::auth::ensure_permission(&state, &auth, path, Permission::READ).await?;
    let counted = count_misplaced(&state.get_ctx(), &bucket, &query.prefix, false)
        .await
        .map_err(ServerError::InternalError)?;
    let status = drive(
        MigrationStatusOperation::new(bucket.clone()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let last = status.filter(|record| record.moving);
    Ok(Json(BucketMoveResponse {
        bucket,
        misplaced: counted.count,
        complete: counted.complete,
        move_prefix: last.as_ref().map(|record| record.prefix.clone()),
        progress: last.map(Into::into),
    }))
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/move",
    tag = "data/storage",
    summary = "Move a bucket's files to their storage",
    description = r#"Starts this node's background move of a bucket's stored versions to the storage that the current rules choose for a new upload of each key.

**Authentication**: realm bearer token with WRITE on the owning group's admin path.

**Behavior**
- Each version is copied to the storage and encoding a new upload of its key gets: a group
  storage, Aruna's storage, or the path of a linked rule's write folder. The version then names
  the new copy; the old copy is deleted later unless another version still uses it or its storage
  keeps files (`retain`).
- A version is published only if its rules, bucket settings and the version itself did not change
  during the copy; otherwise it stays and counts as skipped.
- Files in a linked folder stay where they are, and no file there is deleted. Versions already
  in place are skipped.
- A version whose copy cannot be read is recorded as broken, as by a storage check, and skipped.
- `prefix` limits the move to keys under it. Other keys still get the bucket's compression.
- Versions that fail are retried in later passes like a compression change. Sending the same
  request again resumes a running move, or restarts one that finished or waits with failures.
- While a move with another prefix is unfinished, also while it waits for a retry pass, the
  request answers 409 `move_running`.
- `GET` on this path reports the progress and how many versions are still not in place.

**Limits**
- Encrypted buckets and buckets with an unfinished encryption change answer 409
  `bucket_encrypted`.
- Versions with placement policies stay on their storage.
- Buckets on other nodes are not changed."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(
        content = BucketMoveRequest,
        description = "Optional prefix of the keys to move",
        example = json!({ "prefix": "raw/" })
    ),
    responses(
        (
            status = 202,
            description = "The move as stored; it runs in the background",
            body = BucketMoveStarted,
            example = json!({
                "bucket": "research-raw",
                "prefix": "raw/",
                "progress": {
                    "migrated": 0,
                    "skipped": 0,
                    "failed": 0,
                    "retries": 0,
                    "started_at_ms": 1790000000000_u64
                }
            })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node, or no longer owned by the authorized group", body = ErrorResponse),
        (status = 409, description = "`bucket_encrypted`: the bucket is encrypted or changes its encryption; `move_running`: a move with another prefix is unfinished", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_bucket_move(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    request: Option<Json<BucketMoveRequest>>,
) -> ServerResult<(StatusCode, Json<BucketMoveStarted>)> {
    let auth = require_realm_auth(&state, auth)?;
    let prefix = request
        .map(|Json(request)| request.prefix)
        .unwrap_or_default();
    let group_id = bucket_group(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let operation = StartMoveOperation::new(bucket.clone(), group_id, prefix, now_ms());
    let record: CompressionMigration = drive(operation, &state.get_ctx())
        .await
        .map_err(move_refusal)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(BucketMoveStarted {
            bucket,
            prefix: record.prefix.clone(),
            progress: record.into(),
        }),
    ))
}

pub(crate) fn move_refusal(error: StartMoveError) -> ServerError {
    match error {
        StartMoveError::NoSuchBucket => ServerError::NotFound,
        StartMoveError::MoveRunning(prefix) => ServerError::Refused(
            StatusCode::CONFLICT,
            "move_running",
            format!("a move of the keys under `{prefix}` is still running; retry when it finishes"),
        ),
        StartMoveError::Encrypted => ServerError::Refused(
            StatusCode::CONFLICT,
            "bucket_encrypted",
            "an encrypted bucket keeps its stored files where they are".to_string(),
        ),
        other => ServerError::InternalError(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypted_bucket_conflicts() {
        let refusal = move_refusal(StartMoveError::Encrypted);
        assert_eq!(refusal.status_code(), StatusCode::CONFLICT);
        let code = refusal.response_body().code;
        assert_eq!(code.as_deref(), Some("bucket_encrypted"));
    }
}
