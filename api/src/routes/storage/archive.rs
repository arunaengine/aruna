//! Downloads an explicit selection of this node's S3 object versions as one streamed ZIP.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::s3::object::archive::{
    SelectionEntry, SelectionError, validate_selection, write_selection,
};
use axum::extract::State;
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_util::task::AbortOnDropHandle;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::require_realm_auth;
use crate::download::{self, AdmissionError};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::rate_limit::LocalKey;
use crate::server::state::ServerState;

/// Bytes buffered between the archive writer and the response body.
const PIPE_BYTES: usize = 128 * 1024;

#[derive(OpenApi)]
#[openapi()]
pub struct ArchiveDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ArchiveDoc::openapi()).routes(routes!(download_selection))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveEntry {
    pub bucket: String,
    pub key: String,
    /// The exact version; omitted means the version current when the download starts.
    #[serde(default)]
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRequest {
    pub entries: Vec<ArchiveEntry>,
}

#[utoipa::path(
    post,
    path = "/data/archives",
    tag = "data/storage",
    summary = "Download selected objects as a ZIP",
    description = r#"Streams the named object versions of this node's buckets as one ZIP archive.

**Authentication**: realm bearer token; each object needs READ on its object path.

**Behavior**
- The exact version of every entry is captured before the first byte is sent: an omitted
  `version_id` takes the version that is current at that moment.
- An entry is stored at `{bucket}/{key}`. A key that is unsafe as a ZIP path, or a path that
  another selected version or a deeper selected key also claims, moves to
  `_renamed/{bucket}/{version_id}`. The same request always yields the same paths.
- The last archive entry, `_report.json`, lists every requested entry in request order with its
  version, archive path, size, BLAKE3 digest and status: `included`, `denied`, `missing`, `locked`
  or `unavailable`. READ is checked again before each entry is read, so a permission that changed
  meanwhile is reported as `denied`.
- Entries are stored uncompressed. Closing the connection cancels the download.

**Limits**
- At most 1000 entries, each entry at most once.

**Errors**
- A failure while an entry streams ends the response with an error instead of a shortened
  archive. A 4xx answer is sent only before the archive starts."#,
    request_body(content = ArchiveRequest, example = json!({
        "entries": [
            {"bucket": "lab-data", "key": "runs/2026-11-03/reads.fastq"},
            {"bucket": "lab-data", "key": "runs/summary.csv", "version_id": "01JVERS0123456789ABCDEFGHJ"}
        ]
    })),
    responses(
        (status = 200, description = "The archive as an `application/zip` stream with an attachment `Content-Disposition`"),
        (status = 400, description = "An empty or oversized selection, a repeated entry, an invalid bucket name or version id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 503, description = "The node has no free download capacity right now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn download_selection(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ArchiveRequest>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let entries = request
        .entries
        .into_iter()
        .map(|entry| {
            let version_id = entry
                .version_id
                .map(|version| ulid::Ulid::from_string(&version))
                .transpose()
                .map_err(|_| ServerError::BadRequestReason("version_id is not a ULID".into()))?;
            Ok(SelectionEntry {
                bucket: entry.bucket,
                key: entry.key,
                version_id,
            })
        })
        .collect::<ServerResult<Vec<_>>>()?;
    validate_selection(&entries)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    let permit = match download::admit(state.as_ref(), LocalKey::User(auth.user_id)) {
        Ok(permit) => permit,
        Err(AdmissionError::Total | AdmissionError::User) => {
            return Err(ServerError::ServiceUnavailableReason(
                "download_capacity".to_string(),
            ));
        }
    };
    let (writer, reader) = tokio::io::duplex(PIPE_BYTES);
    let context = state.get_ctx();
    let scope = (state.get_realm_id(), state.get_node_id());
    // Dropping the response body drops this handle and cancels the archive writer.
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        write_selection(&context, (&auth, scope.0, scope.1), entries, writer).await
    }));
    let tail = futures_util::stream::once(async move {
        match task.await {
            Ok(Ok(_)) | Ok(Err(SelectionError::Closed)) => None,
            Ok(Err(error)) => Some(Err(std::io::Error::other(error.to_string()))),
            Err(error) => Some(Err(std::io::Error::other(error.to_string()))),
        }
    })
    .filter_map(std::future::ready);
    let source = tokio_util::io::ReaderStream::new(reader).chain(tail);
    let mut response = download::body(BackendStream::new(source), permit).into_response();
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/zip"));
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"selection.zip\""),
    );
    Ok(response)
}

#[cfg(test)]
#[path = "archive_tests.rs"]
mod tests;
