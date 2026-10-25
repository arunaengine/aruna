//! Serves small text writes over REST, the route a member relays an MCP text write to when the
//! coordinator of the key's range of a distributed bucket is another node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_operations::staging::offered_directory::{OfferedDirectoryError, guard_bucket_write};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::{Extension, Json};
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::mcp::data::{TextObjectRequest, WriteObjectInput, WriteObjectOutput, store_text};
use crate::routes::storage::deletion::bucket_info;
use crate::routes::storage::relay::{relay_json, rest_target};
use crate::server::state::ServerState;

/// The largest text body one write stores, as for the MCP text tool.
const MAX_TEXT_BYTES: usize = 1024 * 1024;

#[derive(OpenApi)]
#[openapi()]
pub struct TextDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(TextDoc::openapi()).routes(routes!(write_text_object))
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/objects/text",
    tag = "data/storage",
    summary = "Write a small text object",
    description = r#"Stores at most 1 MiB of UTF-8 text as a new version of one key.

**Authentication**: realm bearer token with WRITE on the object's path on this node.

**Behavior**
- The whole body is replaced; an existing key gets a new version.
- In a distributed bucket only the coordinator of the key's range writes: another member relays the
  request there with the caller's bearer token, and the coordinator checks the caller again on its
  own path. An unreachable coordinator fails the write at once."#,
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    request_body(content = TextObjectRequest),
    responses(
        (status = 200, description = "The stored version", body = WriteObjectOutput),
        (status = 400, description = "The key is not a valid object key", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the object's path, or the bucket is a read-only offered directory", body = ErrorResponse),
        (status = 404, description = "No such bucket", body = ErrorResponse),
        (status = 413, description = "The text is larger than 1 MiB", body = ErrorResponse),
        (status = 503, description = "The coordinator of the key's range is unreachable or the bucket is not active yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn write_text_object(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    Json(request): Json<TextObjectRequest>,
) -> ServerResult<Json<WriteObjectOutput>> {
    let auth = require_realm_auth(&state, auth)?;
    crate::s3::util::validate_object_key(&request.key)
        .map_err(|_| ServerError::BadRequestReason("the key is not a valid object key".into()))?;
    if request.text.len() > MAX_TEXT_BYTES {
        return Err(ServerError::PayloadTooLarge(format!(
            "text is {} bytes and one write stores at most {MAX_TEXT_BYTES}",
            request.text.len()
        )));
    }
    guard_bucket_write(state.get_ctx().as_ref(), &bucket)
        .await
        .map_err(|error| match error {
            OfferedDirectoryError::ReadOnly(_) => ServerError::Forbidden,
            error => ServerError::InternalError(error.to_string()),
        })?;
    let info = bucket_info(&state, &bucket).await?;
    let path = object_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        &bucket,
        &request.key,
    );
    ensure_permission(&state, &auth, path, Permission::WRITE).await?;
    if let Some(forward) = rest_target(&state, &info, &request.key, &headers).await? {
        let path = format!("/data/buckets/{bucket}/objects/text");
        let (_, answer) = relay_json(&state, &forward, &path, bearer.as_ref(), &request).await?;
        return Ok(Json(answer));
    }
    let input = WriteObjectInput {
        bucket,
        key: request.key,
        text: request.text,
        content_type: request.content_type,
    };
    Ok(Json(store_text(&state, &auth, &info, input).await?))
}
