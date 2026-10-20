//! Serves the bucket rename routes: preview a folder rename and apply it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::routing::ensure_group_admin;
use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::bucket::rename::{
    RenameError, RenameInput, RenameItem, RenamePlan, authorized_rename,
};
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi()]
pub struct RenameDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(RenameDoc::openapi())
        .routes(routes!(preview_rename))
        .routes(routes!(rename_keys))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RenamePreviewRequest {
    /// Folder whose keys move: empty for the whole bucket, else a relative path ending with `/`.
    #[serde(default)]
    pub from: String,
    /// Folder the keys move to, in the same form.
    #[serde(default)]
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RenameRequest {
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: String,
    /// The keys of the preview to move, each under `from`.
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RenameItemResponse {
    pub from: String,
    pub to: String,
    /// Every stored version of the key, which keeps its id.
    pub versions: Vec<String>,
    /// The current version; omitted without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
    /// The current version is a delete marker.
    pub deleted: bool,
    /// Backend files whose recorded key changes; the files stay where they are.
    pub files: Vec<String>,
    /// Why the key cannot move; omitted when it can.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RenameResponse {
    pub bucket: String,
    pub items: Vec<RenameItemResponse>,
    /// More keys lie under `from` than one rename takes.
    pub truncated: bool,
}

fn item_response(item: RenameItem) -> RenameItemResponse {
    RenameItemResponse {
        from: item.from,
        to: item.to,
        versions: item.versions.iter().map(ToString::to_string).collect(),
        current: item.current.map(|id| id.to_string()),
        deleted: item.deleted,
        files: item.files,
        conflict: item.conflict,
    }
}

fn plan_response(bucket: String, plan: RenamePlan) -> RenameResponse {
    RenameResponse {
        bucket,
        items: plan.items.into_iter().map(item_response).collect(),
        truncated: plan.truncated,
    }
}

fn map_error(error: RenameError) -> ServerError {
    match error {
        RenameError::Invalid(message) => ServerError::BadRequestMessage(message),
        RenameError::Conflict(message) => ServerError::Conflict(message),
        RenameError::NotFound => ServerError::NotFound,
        RenameError::Forbidden => ServerError::Forbidden,
        RenameError::Storage(StorageError::TransactionConflict) => {
            ServerError::Conflict("the bucket changed during the rename; try again".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

/// Renaming rewrites the keys of a group's mappings, so it takes group admin and bucket WRITE.
/// A restricted token could reach keys outside its paths, so it is refused.
async fn authorize(
    state: &ServerState,
    auth: Option<AuthContext>,
    bucket: &str,
) -> ServerResult<(AuthContext, ulid::Ulid)> {
    let auth = require_realm_auth(state, auth)?;
    if auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }
    let group_id = match drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(info) => info.group_id,
        Err(GetBucketError::NotFound) => return Err(ServerError::NotFound),
        Err(other) => return Err(ServerError::InternalError(other.to_string())),
    };
    ensure_group_admin(state, &auth, group_id).await?;
    let path = bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket);
    ensure_permission(state, &auth, path, Permission::WRITE).await?;
    Ok((auth, group_id))
}

fn rename_input(
    state: &ServerState,
    group_id: ulid::Ulid,
    bucket: &str,
    (from, to): (String, String),
    keys: Option<Vec<String>>,
) -> RenameInput {
    RenameInput {
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        group_id,
        bucket: bucket.to_string(),
        from,
        to,
        apply: keys.is_some(),
        keys,
    }
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/rename/preview",
    tag = "data/storage",
    summary = "Preview renaming a bucket folder",
    description = r#"Lists the keys under `from` with the keys they would get under `to`, their versions, the backend files whose recorded key changes, and every conflict. Nothing changes.

**Authentication**: realm bearer token with WRITE on the owning group's admin path, on the bucket, and on every old and new key, including request policies. Tokens with path restrictions are refused.

**Behavior**
- A key `from` + `x` becomes `to` + `x`; folders and characters below `from` stay as they are.
- After the rename, links by path to the old keys stop working; content-hash DRS identifiers
  keep working.
- The selection is the current keys under `from` when the preview runs. Pass its keys to the
  rename, so keys added later or the renamed keys themselves are never selected again.
- A key cannot move when the new key already exists, holds versions, or is itself renamed, when
  either key has an open multipart upload, and when the key holds encrypted, referenced or
  unfinished versions.

**Limits**
- At most 1000 keys; `truncated` is `true` when more lie under `from`. Rename in parts.
- Encrypted buckets and buckets that replicate or have queued copies to other nodes answer 409,
  as do buckets with a running permanent purge, import, compression or storage move."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(
        content = RenamePreviewRequest,
        description = "The folder to rename and its new name",
        example = json!({"from": "", "to": "archive/"})
    ),
    responses(
        (
            status = 200,
            description = "The planned key moves",
            body = RenameResponse,
            example = json!({
                "bucket": "research-raw",
                "items": [{
                    "from": "x.csv",
                    "to": "archive/x.csv",
                    "versions": ["01JMPRT0123456789ABCDEFGHJ"],
                    "current": "01JMPRT0123456789ABCDEFGHJ",
                    "deleted": false,
                    "files": ["instrument/raw/x.csv"]
                }],
                "truncated": false
            })
        ),
        (status = 400, description = "`from` or `to` is not a folder, or both are equal", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path, the bucket or a key, or a restricted token", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "The bucket cannot be renamed now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn preview_rename(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<RenamePreviewRequest>,
) -> ServerResult<Json<RenameResponse>> {
    let (auth, group_id) = authorize(&state, auth, &bucket).await?;
    let folders = (request.from, request.to);
    let input = rename_input(&state, group_id, &bucket, folders, None);
    let plan = authorized_rename(&state.get_ctx(), &auth, input)
        .await
        .map_err(map_error)?;
    Ok(Json(plan_response(bucket, plan)))
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/rename",
    tag = "data/storage",
    summary = "Rename a bucket folder",
    description = r#"Moves the given keys from `from` to `to` in one transaction, keeping their versions and the backend files.

**Authentication**: realm bearer token with WRITE on the owning group's admin path, on the bucket, and on every old and new key, including request policies. Tokens with path restrictions are refused.

**Behavior**
- Every version, delete marker and the current version move to the new key with their ids,
  metadata and policies. No bytes are copied and no version is added, so usage stays the same.
- Recorded backend files of the keys keep their place and now map to the new keys, so a later
  import run, also after a file changed, uses the new keys and never brings back the old ones.
- Links by path stop working, also to an exact version and DRS identifiers that name the key.
  Content-hash DRS identifiers keep working.
- If any key has a conflict, nothing changes (409). A key that no longer exists is a conflict, so
  sending the same rename again changes nothing.
- A concurrent upload, delete or import of a selected key makes the rename fail without changes.
  An open multipart upload to an old or new key is a conflict.
- The rename is node-local; buckets on other nodes are not changed.

**Limits**
- 1 to 1000 keys, each under `from` and listed once.
- The limits of the preview apply."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(
        content = RenameRequest,
        description = "The folders and the keys the preview showed",
        example = json!({"from": "", "to": "archive/", "keys": ["x.csv"]})
    ),
    responses(
        (
            status = 200,
            description = "The key moves as applied",
            body = RenameResponse,
            example = json!({
                "bucket": "research-raw",
                "items": [{
                    "from": "x.csv",
                    "to": "archive/x.csv",
                    "versions": ["01JMPRT0123456789ABCDEFGHJ"],
                    "current": "01JMPRT0123456789ABCDEFGHJ",
                    "deleted": false,
                    "files": ["instrument/raw/x.csv"]
                }],
                "truncated": false
            })
        ),
        (status = 400, description = "Invalid folders, or keys outside `from`, repeated or too many", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path, the bucket or a key, or a restricted token", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "A key has a conflict, or the bucket cannot be renamed now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn rename_keys(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<RenameRequest>,
) -> ServerResult<Json<RenameResponse>> {
    let (auth, group_id) = authorize(&state, auth, &bucket).await?;
    let folders = (request.from, request.to);
    let input = rename_input(&state, group_id, &bucket, folders, Some(request.keys));
    let plan = authorized_rename(&state.get_ctx(), &auth, input)
        .await
        .map_err(map_error)?;
    Ok(Json(plan_response(bucket, plan)))
}
