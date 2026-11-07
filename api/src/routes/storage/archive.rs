//! Downloads a selection of this node's files and folders as one streamed ZIP.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::s3::object::archive::{SelectionEntry, validate_selection, write_selection};
use aruna_operations::s3::object::pages::PageScope;
use axum::extract::State;
use axum::http::header::{CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_util::task::AbortOnDropHandle;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::download::{self, AdmissionError, DownloadPermit};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::rate_limit::LocalKey;
use crate::routes::storage::objects::BearerRelay;
use crate::server::state::ServerState;

/// Bytes buffered between the archive writer and the response body.
const PIPE_BYTES: usize = 128 * 1024;

#[derive(OpenApi)]
#[openapi()]
pub struct ArchiveDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ArchiveDoc::openapi()).routes(routes!(download_selection))
}

/// One file with `key`, or one folder with `prefix`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveEntry {
    pub bucket: String,
    /// The key of one file.
    #[serde(default)]
    pub key: Option<String>,
    /// Every key that starts with this prefix; empty for the whole bucket.
    #[serde(default)]
    pub prefix: Option<String>,
    /// The exact version of a file; omitted means the version current when the archive
    /// reaches the file.
    #[serde(default)]
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRequest {
    pub entries: Vec<ArchiveEntry>,
}

/// The selection a request names, checked before anything is admitted.
fn selection(request: ArchiveRequest) -> ServerResult<Vec<SelectionEntry>> {
    let invalid = |reason: &str| ServerError::BadRequestReason(reason.to_string());
    let entries = request
        .entries
        .into_iter()
        .map(|entry| match (entry.key, entry.prefix, entry.version_id) {
            (Some(key), None, version_id) => Ok(SelectionEntry::File {
                bucket: entry.bucket,
                key,
                version_id: version_id
                    .map(|version| ulid::Ulid::from_string(&version))
                    .transpose()
                    .map_err(|_| invalid("version_id is not a ULID"))?,
            }),
            (None, Some(prefix), None) => Ok(SelectionEntry::Folder {
                bucket: entry.bucket,
                prefix,
            }),
            (None, Some(_), Some(_)) => Err(invalid("a folder takes no version_id")),
            _ => Err(invalid("an entry names either a key or a prefix")),
        })
        .collect::<ServerResult<Vec<_>>>()?;
    validate_selection(&entries).map_err(|error| invalid(&error.to_string()))?;
    Ok(entries)
}

fn admitted(state: &ServerState, user_id: UserId) -> ServerResult<DownloadPermit> {
    download::admit(state, LocalKey::User(user_id)).map_err(|error| match error {
        AdmissionError::Total | AdmissionError::User => {
            ServerError::ServiceUnavailableReason("download_capacity".to_string())
        }
    })
}

/// The archive response; dropping its body cancels the writer.
fn archive_response(
    state: Arc<ServerState>,
    (auth, bearer): (AuthContext, Option<ValidatedBearer>),
    entries: Vec<SelectionEntry>,
    permit: DownloadPermit,
) -> Response {
    let (writer, reader) = tokio::io::duplex(PIPE_BYTES);
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        let relay = BearerRelay {
            state: &state,
            bearer: bearer.as_ref(),
        };
        let context = state.get_ctx();
        let scope = PageScope {
            context: context.as_ref(),
            realm_id: state.get_realm_id(),
            node_id: state.get_node_id(),
            relay: &relay,
        };
        let expiry = bearer.as_ref().map_or(0, ValidatedBearer::expires_at_secs);
        write_selection(&scope, (&auth, expiry), entries, writer).await
    }));
    // Every failure, also of the writer itself, ends the body with an error, never cleanly.
    let tail = futures_util::stream::once(async move {
        match task.await {
            Ok(Ok(_)) => None,
            Ok(Err(error)) => Some(Err(std::io::Error::other(error.to_string()))),
            Err(error) => Some(Err(std::io::Error::other(error.to_string()))),
        }
    })
    .filter_map(std::future::ready);
    let source = tokio_util::io::ReaderStream::new(reader).chain(tail);
    let mut response = download::archive_body(BackendStream::new(source), permit).into_response();
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/zip"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"selection.zip\""),
    );
    response
}

#[utoipa::path(
    post,
    path = "/data/archives",
    tag = "data/storage",
    summary = "Download selected files and folders as a ZIP",
    description = r#"Streams the selected files and folders of this node's buckets as one ZIP archive.

**Authentication**: realm bearer token; each file needs READ on its object path and each folder
READ on its prefix, as the object listing requires.

**Behavior**
- An entry names a file by `key`, optionally pinned by `version_id`, or a folder by `prefix`
  (empty for the whole bucket). Folders of distributed buckets are listed across their key
  ranges.
- Buckets follow in the order the request first names them, keys in key order within a bucket,
  explicit files merged into the folder listings. Files and folders are resolved while the
  archive streams, never all before the first byte.
- There is no snapshot: a file without `version_id` takes the version that is current when the
  archive reaches it, and that version is then read to its end.
- A version is written once, at `{bucket}/{key}`. A key that is unsafe as a ZIP path, or whose
  path equals a written path, lies below a written file, or equals a written path's folder,
  moves to `_renamed/{bucket}/{version_id}`. The same selection over unchanged data yields the
  same paths.
- The last archive entry, `_report.json`, holds `totals` (entries per status and bytes written)
  and one row in `entries` for each file, folder or key range that is not included at its own
  path: `renamed` with its `path`, `denied`, `missing`, `locked` (bucket key not unlocked) or
  `unavailable`. A folder the caller may not list is one `denied` row and is never enumerated;
  entries the listing hides never appear. A key range no holder answered, or one a copy listed
  because its coordinator could not, is one `unavailable` row with its `range_id`, and the
  archive is incomplete. Keys of ranges this node keeps no records of are `unavailable`.
- Entries are stored uncompressed, with ZIP64 records beyond 65,535 entries or 4 GiB.
- The node keeps per entry its ZIP directory record (the path plus about 100 bytes), the written
  path and its folders, and the report row of an entry that is not included, so memory grows
  with the number of entries and their path lengths.
- Closing the connection cancels the download. A client that stops reading for the download
  stall time ends the body with an error.

**Limits**
- The request body is bounded by the node's request limits; the archive has no entry limit.
- Archives count against the node's and the caller's concurrent downloads.

**Errors**
- A failure while the archive streams ends the response with an error instead of a shortened
  archive. A 4xx answer is sent only before the archive starts."#,
    request_body(content = ArchiveRequest, example = json!({
        "entries": [
            {"bucket": "lab-data", "prefix": "runs/2026-11-03/"},
            {"bucket": "lab-data", "key": "runs/summary.csv", "version_id": "01JVERS0123456789ABCDEFGHJ"}
        ]
    })),
    responses(
        (status = 200, description = "The archive as an `application/zip` stream with an attachment `Content-Disposition` and `Cache-Control: no-store`"),
        (status = 400, description = "An empty selection, an entry with both or neither of key and prefix, an invalid bucket name or version id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 503, description = "The node has no free download capacity right now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn download_selection(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<ArchiveRequest>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let entries = selection(request)?;
    let permit = admitted(&state, auth.user_id)?;
    Ok(archive_response(state, (auth, bearer), entries, permit))
}

#[cfg(test)]
#[path = "archive_tests.rs"]
mod tests;
