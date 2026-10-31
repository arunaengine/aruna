//! Serves the routes that add or remove sources of the reference versions under a key prefix.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::errors::StorageError;
use aruna_core::structs::execution::staging::SourceMapping;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_operations::connectors::get_connector::{
    GetSourceError, GetSourceInput, GetSourceOperation,
};
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::object::source_edit::{
    SourceChange, SourceEditError, SourceEditInput, SourceEditOperation, SourceEditResult,
};
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

/// Version rows one call reads; `next` continues after them.
const PAGE: usize = 1000;

#[derive(OpenApi)]
#[openapi()]
pub struct SourceEditDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(SourceEditDoc::openapi())
        .routes(routes!(add_sources))
        .routes(routes!(remove_sources))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct AddSourcesRequest {
    /// Edits only this key; omit it to edit every key under `prefix`.
    #[serde(default)]
    pub key: Option<String>,
    /// Keys under this prefix; empty for the whole bucket.
    #[serde(default)]
    pub prefix: String,
    /// The connector of the bucket's group that reads the new source.
    pub connector_id: String,
    /// Source path prefix of an existing source; empty matches every path.
    #[serde(default)]
    pub from: String,
    /// The new source's path prefix below the connector, in place of `from`.
    #[serde(default)]
    pub to: String,
    /// `next` of the previous call; omit it on the first call.
    #[serde(default)]
    pub after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RemoveSourcesRequest {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub prefix: String,
    /// Removes only sources read through this connector; omit it for any source.
    #[serde(default)]
    pub connector_id: Option<String>,
    /// Removes sources whose path starts with this; empty matches every path.
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SourceEditResponse {
    /// Version rows this call read.
    pub scanned: u64,
    /// Reference versions whose sources changed.
    pub changed: u64,
    /// Reference versions that kept a matching source because it is their only one.
    pub kept_last: u64,
    /// Pass as `after` to continue; absent once every row was read.
    pub next: Option<String>,
}

impl From<SourceEditResult> for SourceEditResponse {
    fn from(result: SourceEditResult) -> Self {
        Self {
            scanned: result.scanned,
            changed: result.changed,
            kept_last: result.kept_last,
            next: result.next.map(hex::encode),
        }
    }
}

/// The bucket's group after bucket WRITE; a distributed bucket refuses, as only its
/// coordinators change its rows.
async fn authorize(
    state: &ServerState,
    auth: Option<AuthContext>,
    bucket: &str,
) -> ServerResult<Ulid> {
    let auth = require_realm_auth(state, auth)?;
    if auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }
    let info = match drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(info) => info,
        Err(GetBucketError::NotFound) => return Err(ServerError::NotFound),
        Err(other) => return Err(ServerError::InternalError(other.to_string())),
    };
    let path = bucket_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        bucket,
    );
    ensure_permission(state, &auth, path, Permission::WRITE).await?;
    if info.distribution.is_some() {
        let reason = "sources of a distributed bucket cannot be edited in place";
        return Err(ServerError::Conflict(reason.to_string()));
    }
    Ok(info.group_id)
}

fn ulid(value: &str) -> ServerResult<Ulid> {
    Ulid::from_string(value)
        .map_err(|_| ServerError::BadRequestMessage(format!("`{value}` is no connector id")))
}

async fn edit(
    state: &ServerState,
    bucket: String,
    (key, prefix, after): (Option<String>, String, Option<String>),
    change: SourceChange,
) -> ServerResult<Json<SourceEditResponse>> {
    let after = (after.as_deref())
        .map(hex::decode)
        .transpose()
        .map_err(|_| ServerError::BadRequestMessage("`after` is no `next` value".to_string()))?;
    let input = SourceEditInput {
        node_id: state.get_node_id(),
        bucket,
        exact: key.is_some(),
        prefix: key.unwrap_or(prefix),
        change,
        after,
        page: PAGE,
    };
    match drive(SourceEditOperation::new(input), &state.get_ctx()).await {
        Ok(result) => Ok(Json(result.into())),
        Err(SourceEditError::Storage(StorageError::TransactionConflict)) => Err(
            ServerError::Conflict("the bucket changed during the edit; try again".to_string()),
        ),
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/sources/add",
    tag = "data/storage",
    summary = "Add a source to referenced files",
    description = r#"Adds another source of the same bytes to the reference versions of one key or of every key under a prefix, such as a second mirror of a public database.

**Authentication**: realm bearer token with WRITE on the bucket. Tokens with path restrictions are refused.

**Behavior**
- For each reference version, the first of its sources whose path starts with `from` gives the new
  source: the same path with `to` in place of `from`, read through `connector_id`. Versions that
  have that source already, or no matching source, stay as they are.
- All sources of a version are equal. A read tries the sources that did not fail lately first and
  moves on when one fails. A source serves only bytes it proves equal to the version: the recorded
  checksums on the whole bytes, or the version's own strong ETag. Without checksums the new
  source serves nothing, so import checksums first.
- A source whose size or checksum differs is a changed source: a current read records a new
  version from it, and a pinned version is no longer available.
- Version ids, observations and current versions stay. Nothing is read from any source.
- The edit is node-local: replicas on other nodes keep their sources.

**Paging**: one call reads up to 1000 version rows of the bucket. Pass `next` as `after` until
it is absent."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(
        content = AddSourcesRequest,
        description = "The keys and how their new source's path follows from an existing one",
        example = json!({
            "prefix": "genomes/",
            "connector_id": "01JMPRT0123456789ABCDEFGHJ",
            "from": "genomes/",
            "to": "genomes/"
        })
    ),
    responses(
        (
            status = 200,
            description = "What one page changed",
            body = SourceEditResponse,
            example = json!({"scanned": 1000, "changed": 998, "kept_last": 0, "next": "0a6e6362692d72656673657131"})
        ),
        (status = 400, description = "Invalid connector id or `after`", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "Bucket or connector not found on this node", body = ErrorResponse),
        (status = 409, description = "A distributed bucket, or the bucket changed during the edit", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn add_sources(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<AddSourcesRequest>,
) -> ServerResult<Json<SourceEditResponse>> {
    let group_id = authorize(&state, auth, &bucket).await?;
    let connector_id = ulid(&request.connector_id)?;
    let input = GetSourceInput {
        group_id,
        connector_id,
    };
    let connector = match drive(GetSourceOperation::new(input), &state.get_ctx()).await {
        Ok(found) => found.connector,
        Err(GetSourceError::NotFound) => return Err(ServerError::NotFound),
        Err(error) => return Err(ServerError::InternalError(error.to_string())),
    };
    let mapping = SourceMapping {
        connector_id,
        from: request.from,
        to: request.to,
    };
    let change = SourceChange::Add { mapping, connector };
    let keys = (request.key, request.prefix, request.after);
    edit(&state, bucket, keys, change).await
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/sources/remove",
    tag = "data/storage",
    summary = "Remove a source of referenced files",
    description = r#"Removes the sources whose path starts with `from`, of `connector_id` when given, from the reference versions of one key or of every key under a prefix.

**Authentication**: realm bearer token with WRITE on the bucket. Tokens with path restrictions are refused.

**Behavior**
- A version keeps at least one source: when every source matches, none is removed and the
  version counts in `kept_last`.
- When the source whose ETag and time the version recorded goes, the next source takes its
  place and the recorded ETag and time are dropped. Reads then rely on the recorded checksums.
- The edit is node-local: replicas on other nodes keep their sources.

**Paging**: as for adding sources."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(
        content = RemoveSourcesRequest,
        description = "The keys and the sources to remove",
        example = json!({"prefix": "genomes/", "connector_id": "01JMPRT0123456789ABCDEFGHJ"})
    ),
    responses(
        (
            status = 200,
            description = "What one page changed",
            body = SourceEditResponse,
            example = json!({"scanned": 2, "changed": 1, "kept_last": 1})
        ),
        (status = 400, description = "Invalid connector id or `after`", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "A distributed bucket, or the bucket changed during the edit", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn remove_sources(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<RemoveSourcesRequest>,
) -> ServerResult<Json<SourceEditResponse>> {
    authorize(&state, auth, &bucket).await?;
    let connector_id = request.connector_id.as_deref().map(ulid).transpose()?;
    let change = SourceChange::Remove {
        connector_id,
        from: request.from,
    };
    let keys = (request.key, request.prefix, request.after);
    edit(&state, bucket, keys, change).await
}
