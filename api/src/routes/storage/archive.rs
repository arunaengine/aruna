//! Downloads a selection of this node's files and folders as one streamed ZIP, directly or
//! through a single-use link a browser can open.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use aruna_core::UserId;
use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::backend_import::TokenGrant;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_operations::auth::bearer_token::{grant_valid, token_grant};
use aruna_operations::s3::object::archive::{
    EntryStatus, MemberCall, MemberRead, MemberReader, SelectionEntry, validate_selection,
    write_selection,
};
use aruna_operations::s3::object::pages::PageScope;
use axum::extract::{Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_util::task::AbortOnDropHandle;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::download::{self, AdmissionError, DownloadPermit};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::rate_limit::LocalKey;
use crate::routes::federation::import::shared_stream;
use crate::routes::shares::{rfc3339, unix_now};
use crate::routes::storage::objects::BearerRelay;
use crate::s3::service::{ArunaS3Service, ReadAnswer};
use crate::server::state::ServerState;

/// Bytes buffered between the archive writer and the response body.
const PIPE_BYTES: usize = 128 * 1024;
/// Seconds an archive link works at most.
const LINK_SECS: u64 = 60;
const DOWNLOAD_PATH: &str = "/api/v1/data/archives/download";

#[derive(OpenApi)]
#[openapi()]
pub struct ArchiveDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ArchiveDoc::openapi())
        .routes(routes!(download_selection))
        .routes(routes!(create_link))
        .routes(routes!(download_link))
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

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct LinkResponse {
    /// Single-use download URL path on this node.
    pub path: String,
    pub expires_at: String,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct LinkQuery {
    /// Archive ticket; single use, never logged.
    pub ticket: String,
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
- A key of a distributed bucket is read where an S3 GET of the caller would be: a current
  version from its range's coordinator, a pinned one from any member that keeps its records,
  with the caller's credential, restrictions and expiry.
- A version is written once, at `{bucket}/{key}`. A key that is unsafe as a ZIP path, or whose
  path equals a written path, lies below a written file, or equals a written path's folder,
  moves to `_renamed/{bucket}/{version_id}`. The same selection over unchanged data yields the
  same paths.
- The last archive entry, `_report.json`, holds `totals` (entries per status and bytes written)
  and one row in `entries` for each file, folder or key range that is not included at its own
  path: `renamed` with its `path`, `denied`, `missing`, `locked` (bucket key not unlocked) or
  `unavailable`. A folder the caller may not list is one `denied` row and is never
  enumerated; entries the listing hides never appear. A key range no holder answered or served,
  or one a copy listed because its coordinator could not, is one `unavailable` row with its
  `range_id`, the folder's other ranges still follow, and the archive is incomplete.
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

#[utoipa::path(
    post,
    path = "/data/archives/links",
    tag = "data/storage",
    summary = "Get a download link for a ZIP",
    description = r#"Returns a single-use download URL for the ZIP of a selection, so a browser downloads it without the bearer token.

**Authentication**: realm bearer token.

**Behavior**
- The selection is checked as for `POST /data/archives`; the archive itself is built when the
  link is opened, with the same versions, paths and report rules.
- The URL works once, within 60 seconds and never after the token expires. A newer link of the
  same user replaces the older one. Links live in this node's memory and end on a restart.
- Opening the link checks the token again: an expired or revoked token is refused, and the
  token's restrictions and session apply to every entry.
- The ticket travels only in the `ticket` query value of the fixed download path, which logs
  never record. The answer carries `Cache-Control: no-store`."#,
    request_body(content = ArchiveRequest, example = json!({
        "entries": [{"bucket": "lab-data", "prefix": "runs/2026-11-03/"}]
    })),
    responses(
        (status = 200, description = "The download URL path", body = LinkResponse, example = json!({"path": "/api/v1/data/archives/download?ticket=<single-use-ticket>", "expires_at": "2026-11-07T10:01:00Z"})),
        (status = 400, description = "An empty selection, an entry with both or neither of key and prefix, an invalid bucket name or version id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<ArchiveRequest>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let grant = token_grant(bearer.as_str()).map_err(|_| ServerError::Unauthorized)?;
    let entries = selection(request)?;
    let now = unix_now();
    let link = PendingLink {
        auth,
        grant,
        bearer,
        entries,
        expires_at: 0,
    };
    let (ticket, expires_at) = state.archive_links().create(link, now);
    let body = LinkResponse {
        path: format!("{DOWNLOAD_PATH}?ticket={ticket}"),
        expires_at: rfc3339(expires_at),
    };
    let no_store = [(CACHE_CONTROL, HeaderValue::from_static("no-store"))];
    Ok((no_store, Json(body)).into_response())
}

#[utoipa::path(
    get,
    path = "/data/archives/download",
    tag = "data/storage",
    summary = "Download a ZIP with a link",
    description = r#"Streams the ZIP of the selection an archive link names, as an attachment.

**Authentication**: the `ticket` query value only; it works once, within 60 seconds.

**Behavior**
- The download is admitted against the link creator's downloads before the ticket is used, so
  a refusal for capacity leaves the link usable until it expires.
- The creator's token is checked again; READ is checked per file and per folder page with the
  token's restrictions, and the archive follows the rules of `POST /data/archives`.
- The answer carries `Cache-Control: no-store`."#,
    params(LinkQuery),
    responses(
        (status = 200, description = "The archive as an `application/zip` stream with an attachment `Content-Disposition`"),
        (status = 401, description = "The creator's token expired or was revoked", body = ErrorResponse),
        (status = 404, description = "The ticket was used, expired, replaced by a newer link or never issued", body = ErrorResponse),
        (status = 503, description = "No free download capacity, or the token could not be checked", body = ErrorResponse)
    ),
    security(())
)]
pub async fn download_link(
    State(state): State<Arc<ServerState>>,
    Query(query): Query<LinkQuery>,
) -> ServerResult<Response> {
    let now = unix_now();
    let links = state.archive_links();
    let creator = links
        .creator(&query.ticket, now)
        .ok_or(ServerError::NotFound)?;
    let permit = admitted(&state, creator)?;
    let link = links
        .redeem(&query.ticket, now)
        .ok_or(ServerError::NotFound)?;
    let storage = &state.get_ctx().storage_handle;
    match grant_valid(storage, &link.grant, &link.auth.user_id).await {
        Ok(true) => {}
        Ok(false) => return Err(ServerError::Unauthorized),
        Err(_) => {
            return Err(ServerError::ServiceUnavailableReason(
                "the token behind the link could not be checked".to_string(),
            ));
        }
    }
    let caller = (link.auth, Some(link.bearer));
    Ok(archive_response(state, caller, link.entries, permit))
}

// Distributed keys are read through the S3 forwarding, with the delegation the S3 GET route sends.
impl MemberReader for BearerRelay<'_> {
    async fn member_read(&self, call: MemberCall<'_>) -> Result<MemberRead, EntryStatus> {
        let state = self.state;
        let (realm_id, node_id) = (state.get_realm_id(), state.get_node_id());
        let service = ArunaS3Service::new(state.get_ctx(), realm_id, node_id).await;
        let expiry = self.bearer.map_or(0, ValidatedBearer::expires_at_secs);
        // An expiry past what `SystemTime` holds becomes the latest one it always holds.
        let expiry = std::time::UNIX_EPOCH
            .checked_add(std::time::Duration::from_secs(expiry))
            .unwrap_or(std::time::UNIX_EPOCH + std::time::Duration::from_secs(u32::MAX.into()));
        // The bearer was verified here; the delegation carries its expiry and restrictions.
        let user = UserAccess {
            access_key: String::new(),
            user_identity: call.auth.user_id,
            group_id: call.info.group_id,
            secret: EncryptedS3Secret::empty(),
            expiry,
            path_restrictions: call.auth.path_restrictions.clone(),
            issued_by: *node_id.as_bytes(),
            revoked_at: None,
        };
        let read = service.member_get(&user, (call.bucket, call.info), call.key, call.version);
        match read.await {
            Ok(ReadAnswer::Local(_)) => Ok(MemberRead::Local),
            Ok(ReadAnswer::Forwarded(response, _)) => {
                let version = (response.headers().get("x-amz-version-id"))
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| ulid::Ulid::from_string(value).ok())
                    .ok_or(EntryStatus::Unavailable)?;
                let blob = shared_stream(response.bytes_stream());
                Ok(MemberRead::Remote { version, blob })
            }
            Err(error) => Err(match error.code() {
                s3s::S3ErrorCode::NoSuchKey
                | s3s::S3ErrorCode::NoSuchVersion
                | s3s::S3ErrorCode::MethodNotAllowed => EntryStatus::Missing,
                s3s::S3ErrorCode::AccessDenied => EntryStatus::Denied,
                _ => EntryStatus::Unavailable,
            }),
        }
    }
}

/// What an unredeemed link runs as. The bearer stays in memory for relayed listings and is
/// never stored or logged, so this has no `Debug`.
pub(crate) struct PendingLink {
    pub auth: AuthContext,
    pub grant: TokenGrant,
    pub bearer: ValidatedBearer,
    pub entries: Vec<SelectionEntry>,
    /// Unix seconds; set when the link is created.
    pub expires_at: u64,
}

/// The unredeemed archive links of this node, at most one per user.
#[derive(Default)]
pub(crate) struct ArchiveLinks {
    table: Mutex<LinkTable>,
}

// Shows only the number of links, never a ticket or token.
impl std::fmt::Debug for ArchiveLinks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let links = self.table().tickets.len();
        formatter
            .debug_struct("ArchiveLinks")
            .field("links", &links)
            .finish()
    }
}

#[derive(Default)]
struct LinkTable {
    tickets: HashMap<[u8; 32], PendingLink>,
    users: HashMap<UserId, [u8; 32]>,
}

impl LinkTable {
    fn prune(&mut self, now: u64) {
        self.tickets.retain(|_, link| link.expires_at > now);
        let tickets = &self.tickets;
        self.users.retain(|_, ticket| tickets.contains_key(ticket));
    }
}

/// Tickets are kept by digest, so the table never holds one that could be replayed.
fn ticket_digest(ticket: &str) -> [u8; 32] {
    blake3::hash(ticket.as_bytes()).into()
}

impl ArchiveLinks {
    fn table(&self) -> std::sync::MutexGuard<'_, LinkTable> {
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Keeps `link` as its user's only link and returns its ticket and expiry; `now` is Unix
    /// seconds.
    pub(crate) fn create(&self, mut link: PendingLink, now: u64) -> (String, u64) {
        let ticket = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>());
        link.expires_at = (now + LINK_SECS).min(link.grant.expires_at_secs);
        let expires_at = link.expires_at;
        let user_id = link.auth.user_id;
        let digest = ticket_digest(&ticket);
        let mut table = self.table();
        table.prune(now);
        if let Some(older) = table.users.insert(user_id, digest) {
            table.tickets.remove(&older);
        }
        table.tickets.insert(digest, link);
        (ticket, expires_at)
    }

    /// The user an unexpired ticket belongs to, without using it.
    pub(crate) fn creator(&self, ticket: &str, now: u64) -> Option<UserId> {
        let mut table = self.table();
        table.prune(now);
        let link = table.tickets.get(&ticket_digest(ticket))?;
        Some(link.auth.user_id)
    }

    /// Uses an unexpired ticket once.
    pub(crate) fn redeem(&self, ticket: &str, now: u64) -> Option<PendingLink> {
        let mut table = self.table();
        table.prune(now);
        let link = table.tickets.remove(&ticket_digest(ticket))?;
        table.users.remove(&link.auth.user_id);
        Some(link)
    }
}

#[cfg(test)]
#[path = "archive_tests.rs"]
mod tests;
