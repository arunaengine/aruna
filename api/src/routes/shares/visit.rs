//! Visitor routes of share links: open a link once, then list, download and upload inside its
//! scope as the link's creator. No account; the token travels in `Authorization: Share`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{
    BucketInfo, CONTENT_TYPE_KEY, UserAccess, object_permission_path,
};
use aruna_core::structs::storage::share::{
    SHARE_SCHEME, ShareAction, ShareEvent, ShareLink, ShareTarget, ShareToken, TICKET_SECS,
    TicketBody, client_network,
};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::driver::{drive, now_ms};
use aruna_operations::notifications::emit::{EmitNotificationsInput, EmitNotificationsOperation};
use aruna_operations::s3::object::get::{GetObjectInput, get_object_routed};
use aruna_operations::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use aruna_operations::shares::lifecycle::{
    OpenShareConfig, OpenShareOperation, OpenedShare, ShareError,
};
use aruna_operations::shares::names::{ClaimNameOperation, ReleaseNameOperation};
use aruna_operations::shares::rules::{
    NodeScope, numbered_key, relative_path, scope_pattern, upload_key, visit_context,
};
use aruna_operations::shares::store::GetShareOperation;
use aruna_operations::staging::offered_directory::guard_bucket_write;
use axum::Json;
use axum::body::Body;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::relay::{
    ShareRelay, UploadStream, counted_blob, member_error, share_access, upload_error,
};
use super::{ShareModeBody, log_event, rfc3339, unix_now};
use crate::auth::ensure_permission_with;
use crate::download::{self, AdmissionError};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::forwarded::client_ip;
use crate::mcp::data::store_stream;
use crate::rate_limit::LocalKey;
use crate::routes::federation::import::shared_stream;
use crate::routes::storage::deletion::bucket_info;
use crate::routes::storage::objects::page_error;
use crate::s3::service::{ArunaS3Service, ReadAnswer};
use crate::server::state::ServerState;
use aruna_operations::s3::object::pages::{self, ListRequest, PageError, PageScope};
use std::sync::atomic::{AtomicU64, Ordering};

/// Entries per listing page when the visitor names no limit.
const PAGE: usize = 100;
/// Bytes of a visitor JSON body; none of them needs more.
const MAX_JSON_BYTES: usize = 64 * 1024;
/// The longest one upload may take; a stalled body also ends after the import idle timeout.
const UPLOAD_SECS: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

#[derive(OpenApi)]
#[openapi()]
pub struct VisitDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    // Uploads stream their body unbuffered; every other visitor body is small JSON.
    let small = OpenApiRouter::with_openapi(VisitDoc::openapi())
        .routes(routes!(open_share))
        .routes(routes!(list_files))
        .routes(routes!(create_ticket))
        .routes(routes!(download_file))
        .merge(super::dataset::router())
        .layer(DefaultBodyLimit::max(MAX_JSON_BYTES));
    small.routes(routes!(upload_file))
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct OpenShareRequest {
    /// Required when the link has a password.
    pub password: Option<String>,
}

/// What a visitor learns about the link; never the owner's label or the bucket layout above it.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct VisitInfo {
    /// `bucket`, `folder`, `object` or `dataset`.
    pub kind: String,
    /// The shared folder's or file's name.
    pub name: Option<String>,
    pub mode: ShareModeBody,
    pub expires_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct OpenShareResponse {
    /// Send as `Authorization: Share <visit_token>` on every later visitor request.
    pub visit_token: String,
    pub expires_at: String,
    pub share: VisitInfo,
}

#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct VisitListQuery {
    /// A folder below the shared root, ending with `/`; empty for the root.
    pub prefix: Option<String>,
    /// The `next_cursor` of the previous page.
    pub cursor: Option<String>,
    /// Entries per page, 1 to 1000; defaults to 100.
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct VisitFile {
    /// Path below the shared root.
    pub name: String,
    pub size: Option<u64>,
    pub last_modified: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct VisitListing {
    pub files: Vec<VisitFile>,
    /// Subfolders below the listed folder, each ending with `/`.
    pub folders: Vec<String>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TicketRequest {
    /// Path below the shared root, as listed.
    pub name: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct TicketResponse {
    /// Single-use download URL path on this node, valid for one minute.
    pub path: String,
    pub expires_at: String,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct DownloadQuery {
    /// Download ticket; single use, never logged.
    pub ticket: String,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UploadQuery {
    /// File name or relative path below the shared folder.
    pub name: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct UploadResponse {
    /// Where the file landed below the shared root; a taken name gets a number.
    pub name: String,
    pub size: u64,
}

/// The answer a visitor gets for an operation error.
pub(crate) fn visit_error(error: ShareError) -> ServerError {
    match error {
        ShareError::Unavailable => unavailable(),
        ShareError::PasswordRequired => ServerError::Refused(
            StatusCode::UNAUTHORIZED,
            "password_required",
            "this link needs a password".to_string(),
        ),
        ShareError::WrongPassword => ServerError::Refused(
            StatusCode::UNAUTHORIZED,
            "wrong_password",
            "the password is not correct".to_string(),
        ),
        ShareError::Denied => ServerError::Forbidden,
        ShareError::Rule(error) => ServerError::BadRequestReason(error.to_string()),
        error => ServerError::InternalError(error.to_string()),
    }
}

/// The answer while the member holding part of a distributed bucket cannot be reached.
pub(crate) fn range_unavailable(action: &str) -> ServerError {
    ServerError::Refused(
        StatusCode::SERVICE_UNAVAILABLE,
        "range_unavailable",
        format!("part of this folder {action} right now; try again later"),
    )
}

/// The one answer for unknown, expired, revoked and mistyped links.
pub(crate) fn unavailable() -> ServerError {
    ServerError::Refused(
        StatusCode::NOT_FOUND,
        "share_unavailable",
        "this link does not work anymore".to_string(),
    )
}

fn share_token(headers: &HeaderMap) -> Option<ShareToken> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    (scheme == SHARE_SCHEME)
        .then(|| ShareToken::parse(token.trim()))
        .flatten()
}

/// The visitor's address behind trusted proxies.
pub(crate) fn visitor_ip(state: &ServerState, peer: SocketAddr, headers: &HeaderMap) -> IpAddr {
    client_ip(state.trusted_proxies(), peer.ip(), headers)
}

pub(crate) fn event(action: ShareAction, key: Option<String>, ip: IpAddr) -> ShareEvent {
    ShareEvent {
        at: now_ms(),
        action,
        key,
        bytes: None,
        client: Some(client_network(ip)),
    }
}

async fn active_link(state: &ServerState, id: Ulid, now: u64) -> ServerResult<ShareLink> {
    let link = drive(GetShareOperation::new(id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .filter(|link| link.active(now))
        .ok_or_else(unavailable)?;
    // A deactivated creator ends every link at once.
    crate::routes::access::users::ensure_active(state, link.created_by)
        .await
        .map_err(|_| unavailable())?;
    Ok(link)
}

/// The link a visit token in `headers` opens and when the visit ends (Unix seconds); link
/// tokens are only accepted by `open`.
pub(crate) async fn visited_link(
    state: &ServerState,
    headers: &HeaderMap,
) -> ServerResult<(ShareLink, u64)> {
    let token = share_token(headers).ok_or_else(unavailable)?;
    let ShareToken::Visit { expires_at, .. } = token else {
        return Err(unavailable());
    };
    let now = unix_now();
    let link = active_link(state, token.share_id(), now).await?;
    match link.admits(&token, now) {
        true => Ok((link, expires_at)),
        false => Err(unavailable()),
    }
}

/// The key prefix names below the link are relative to.
fn root(target: &ShareTarget) -> &str {
    match target {
        ShareTarget::Object { key, .. } => key
            .rsplit_once('/')
            .map_or("", |(folder, _)| &key[..folder.len() + 1]),
        target => target.prefix().unwrap_or(""),
    }
}

/// The bucket of a bucket, folder or object link, and the narrowed creator acting on it.
pub(crate) async fn visit_scope(
    state: &ServerState,
    link: &ShareLink,
) -> ServerResult<(String, BucketInfo, AuthContext)> {
    let bucket = link.target.bucket().ok_or(ServerError::Refused(
        StatusCode::BAD_REQUEST,
        "dataset_link",
        "this link shares a dataset; use the dataset route".to_string(),
    ))?;
    let info = bucket_info(state, bucket)
        .await
        .map_err(|_| unavailable())?;
    let nodes = NodeScope::of(&info, state.get_node_id());
    let pattern = scope_pattern(link, state.get_realm_id(), nodes).ok_or_else(unavailable)?;
    if info.group_id != link.group_id {
        return Err(unavailable());
    }
    let auth = visit_context(link, state.get_realm_id(), pattern);
    Ok((bucket.to_string(), info, auth))
}

/// Checks the creator's current permission on one object for this visit.
pub(crate) async fn authorize_object(
    state: &ServerState,
    auth: &AuthContext,
    (bucket, info): (&str, &BucketInfo),
    key: &str,
    (permission, operation): (Permission, &str),
) -> ServerResult<()> {
    let path = object_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        bucket,
        key,
    );
    ensure_permission_with(
        state,
        auth,
        path,
        permission,
        PolicyRequestExtras::operation(operation),
    )
    .await
    .map_err(|error| match error {
        ServerError::Forbidden => unavailable(),
        error => error,
    })
}

fn relative_folder(prefix: Option<&str>) -> ServerResult<String> {
    let prefix = prefix.unwrap_or("");
    let valid = prefix.is_empty()
        || (prefix.ends_with('/')
            && prefix
                .trim_end_matches('/')
                .split('/')
                .all(|segment| !segment.is_empty() && segment != "." && segment != ".."));
    match valid {
        true => Ok(prefix.to_string()),
        false => Err(ServerError::BadRequestReason(
            "prefix must be a relative folder ending with `/`".into(),
        )),
    }
}

#[utoipa::path(
    post,
    path = "/shares/open",
    tag = "shares",
    summary = "Open a share link",
    description = r#"Exchanges the link token, and the password if the link has one, for a visit token.

**Authentication**: `Authorization: Share <link token>`; no account.

**Behavior**
- Unknown, expired and revoked links and wrong link tokens all answer the same 404.
- Attempts are throttled per client address and per link; wrong passwords are logged for the
  owner with the client's network only.
- The visit token lasts at most one hour and never past the link's expiry.
- The password is sent here once; later requests carry only the visit token."#,
    request_body(content = OpenShareRequest, example = json!({"password": "correct horse"})),
    responses(
        (status = 200, description = "The visit token and what the link shares", body = OpenShareResponse, example = json!({"visit_token": "v.01JSHARE0123456789ABCDEFGH.1793440800.<signature>", "expires_at": "2026-10-31T11:00:00Z", "share": {"kind": "folder", "name": "results", "mode": "read", "expires_at": "2026-11-30T00:00:00Z"}})),
        (status = 401, description = "`password_required` or `wrong_password`", body = ErrorResponse),
        (status = 404, description = "`share_unavailable`: the link does not work (any reason)", body = ErrorResponse)
    ),
    security(("share_token" = []))
)]
pub async fn open_share(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    request: Option<Json<OpenShareRequest>>,
) -> ServerResult<Json<OpenShareResponse>> {
    let ip = visitor_ip(&state, peer, &headers);
    let token = share_token(&headers).ok_or_else(unavailable)?;
    if !matches!(token, ShareToken::Link { .. }) {
        return Err(unavailable());
    }
    if state
        .rate_limits()
        .check_share_open(ip, token.share_id())
        .is_err()
    {
        return Err(ServerError::Refused(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "too many attempts; wait a minute".to_string(),
        ));
    }
    let config = OpenShareConfig {
        token,
        password: request.and_then(|Json(request)| request.password),
        now: unix_now(),
        entry_id: Ulid::generate(),
        client: client_network(ip),
    };
    // The password check is CPU work; it runs off the request threads.
    let context = state.get_ctx();
    let runtime = tokio::runtime::Handle::current();
    let opened = tokio::task::spawn_blocking(move || {
        runtime.block_on(drive(OpenShareOperation::new(config), &context))
    })
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?
    .map_err(visit_error)?;
    let OpenedShare {
        link,
        visit_token,
        expires_at,
    } = opened;
    let (kind, name) = match &link.target {
        ShareTarget::Bucket { bucket } => ("bucket", Some(bucket.clone())),
        ShareTarget::Folder { prefix, .. } => (
            "folder",
            prefix
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .map(str::to_string),
        ),
        ShareTarget::Object { key, .. } => ("object", key.rsplit('/').next().map(str::to_string)),
        ShareTarget::Dataset { .. } => ("dataset", None),
    };
    Ok(Json(OpenShareResponse {
        visit_token,
        expires_at: rfc3339(expires_at),
        share: VisitInfo {
            kind: kind.to_string(),
            name,
            mode: link.mode.into(),
            expires_at: link.expires_at.map(rfc3339),
        },
    }))
}

#[utoipa::path(
    get,
    path = "/shares/visit/files",
    tag = "shares",
    summary = "List the files of an open share link",
    description = r#"Lists one folder level below the shared root, with names relative to that root.

**Authentication**: `Authorization: Share <visit token>`.

**Behavior**
- Only what the link's creator may still read shows; in a distributed bucket each member
  decides for its own keys.
- Upload-only links list nothing and answer 403 `upload_only`."#,
    params(VisitListQuery),
    responses(
        (status = 200, description = "One page of the folder", body = VisitListing, example = json!({"files": [{"name": "table.csv", "size": 2048, "last_modified": "2026-10-30T08:00:00Z"}], "folders": ["plots/"], "next_cursor": null})),
        (status = 400, description = "Malformed prefix, cursor or limit, or `dataset_link`", body = ErrorResponse),
        (status = 403, description = "`upload_only`", body = ErrorResponse),
        (status = 404, description = "`share_unavailable`", body = ErrorResponse),
        (status = 503, description = "`range_unavailable`: the member holding part of a distributed bucket cannot be reached", body = ErrorResponse)
    ),
    security(("share_token" = []))
)]
pub async fn list_files(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(query): Query<VisitListQuery>,
) -> ServerResult<Json<VisitListing>> {
    let (link, visit_end) = visited_link(&state, &headers).await?;
    require_reads(&link)?;
    let (bucket, info, auth) = visit_scope(&state, &link).await?;
    let root = root(&link.target).to_string();
    let covers = |key: &str| link.target.covers(&bucket, key);
    let scope = (bucket.as_str(), &info, &auth);
    let visit = (&link, visit_end);
    let listing = list_below(&state, scope, &root, query, visit, covers).await?;
    Ok(Json(listing))
}

/// One folder level below `root` of a bucket as the narrowed creator sees it, names relative
/// to `root`; only keys `covers` admits show. Other members list their ranges of a
/// distributed bucket for the visit, which ends at `visit_end`.
pub(crate) async fn list_below(
    state: &ServerState,
    (bucket, info, auth): (&str, &BucketInfo, &AuthContext),
    root: &str,
    query: VisitListQuery,
    (link, visit_end): (&ShareLink, u64),
    covers: impl Fn(&str) -> bool,
) -> ServerResult<VisitListing> {
    let limit = query.limit.unwrap_or(PAGE);
    if !(1..=1000).contains(&limit) {
        return Err(ServerError::BadRequestReason(
            "limit must be 1 to 1000".into(),
        ));
    }
    let folder = relative_folder(query.prefix.as_deref())?;
    let request = ListRequest {
        bucket: bucket.to_string(),
        prefix: Some(format!("{root}{folder}")),
        delimiter: Some("/".to_string()),
        cursor: query.cursor,
        limit,
        partial: false,
    };
    let relay = ShareRelay {
        state,
        user: share_access(state, link, (auth, info), visit_end),
    };
    let context = state.get_ctx();
    let scope = PageScope {
        context: context.as_ref(),
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        relay: &relay,
    };
    let page = pages::object_page(&scope, info, Some((auth, visit_end)), request).await;
    let page = page.map_err(|error| match error {
        PageError::Unanswered { .. } => range_unavailable("cannot be listed"),
        error => page_error(error),
    })?;
    let relative = |key: &str| key.strip_prefix(root).map(str::to_string);
    Ok(VisitListing {
        files: page
            .objects
            .iter()
            .filter(|object| covers(&object.key))
            .filter_map(|object| {
                Some(VisitFile {
                    name: relative(&object.key)?,
                    size: object.size,
                    last_modified: object.last_modified.clone(),
                })
            })
            .collect(),
        folders: page
            .prefixes
            .iter()
            .filter_map(|prefix| relative(prefix))
            .collect(),
        next_cursor: page.next_cursor,
    })
}

fn require_reads(link: &ShareLink) -> ServerResult<()> {
    match link.mode.reads() {
        true => Ok(()),
        false => Err(ServerError::Refused(
            StatusCode::FORBIDDEN,
            "upload_only",
            "this link only accepts uploads".to_string(),
        )),
    }
}

/// Signs a one-minute single-use ticket for `bucket`/`key` of `link`.
/// A ticket never outlives the visit that asked for it.
pub(crate) fn sign_ticket(
    (link, visit_end): (&ShareLink, u64),
    (bucket, key): (&str, &str),
    version_id: Option<Ulid>,
) -> ServerResult<TicketResponse> {
    let expires_at = (unix_now() + TICKET_SECS).min(visit_end);
    let body = TicketBody {
        share_id: link.id,
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id,
        expires_at,
        nonce: rand::random(),
    };
    let ticket = link
        .ticket(&body)
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(TicketResponse {
        path: format!("/api/v1/shares/download?ticket={ticket}"),
        expires_at: rfc3339(expires_at),
    })
}

/// The current version of `key`, or 404 when there is none.
pub(crate) async fn current_version(
    state: &ServerState,
    bucket: &str,
    key: &str,
) -> ServerResult<Option<Ulid>> {
    let head = HeadObjectOperation::new(HeadObjectInput {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: None,
    });
    match drive(head, &state.get_ctx()).await {
        Ok(head) => Ok(head.resolved_version_id.or(head.version_id)),
        Err(
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker,
        ) => Err(ServerError::NotFound),
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

/// The current version of `key` as the range's coordinator of a distributed bucket names it,
/// asked as `user`; an ordinary bucket answers here. 404 when there is none.
pub(crate) async fn head_version(
    state: &ServerState,
    (bucket, info): (&str, &BucketInfo),
    key: &str,
    user: &UserAccess,
) -> ServerResult<Option<Ulid>> {
    if info.distribution.is_none() {
        return current_version(state, bucket, key).await;
    }
    let service = member_service(state).await;
    match service.member_head(user, (bucket, info), key).await {
        Ok(ReadAnswer::Local(_)) => current_version(state, bucket, key).await,
        Ok(ReadAnswer::Forwarded(response, _)) => Ok((response.headers().get("x-amz-version-id"))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| Ulid::from_string(value).ok())),
        Err(error) => Err(member_error(&error)),
    }
}

async fn member_service(state: &ServerState) -> ArunaS3Service {
    ArunaS3Service::new(state.get_ctx(), state.get_realm_id(), state.get_node_id()).await
}

#[utoipa::path(
    post,
    path = "/shares/visit/tickets",
    tag = "shares",
    summary = "Get a download URL for one shared file",
    description = r#"Returns a single-use download URL for one file below the shared root.

**Authentication**: `Authorization: Share <visit token>`.

**Behavior**: the URL works once within one minute, so a browser downloads without the visit token."#,
    request_body(content = TicketRequest, example = json!({"name": "results/table.csv"})),
    responses(
        (status = 200, description = "The download URL path", body = TicketResponse, example = json!({"path": "/api/v1/shares/download?ticket=<single-use-ticket>", "expires_at": "2026-10-31T10:01:00Z"})),
        (status = 400, description = "Malformed name, or `dataset_link`", body = ErrorResponse),
        (status = 403, description = "`upload_only`", body = ErrorResponse),
        (status = 404, description = "`share_unavailable`, or no such file", body = ErrorResponse),
        (status = 503, description = "`range_unavailable`: the member holding the file cannot be reached", body = ErrorResponse)
    ),
    security(("share_token" = []))
)]
pub async fn create_ticket(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<TicketRequest>,
) -> ServerResult<Json<TicketResponse>> {
    let (link, visit_end) = visited_link(&state, &headers).await?;
    require_reads(&link)?;
    relative_path(&request.name)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    let (bucket, info, auth) = visit_scope(&state, &link).await?;
    let key = format!("{}{}", root(&link.target), request.name);
    if !link.target.covers(&bucket, &key) {
        return Err(ServerError::NotFound);
    }
    authorize_object(
        &state,
        &auth,
        (&bucket, &info),
        &key,
        (Permission::READ, "share.read"),
    )
    .await?;
    let user = share_access(&state, &link, (&auth, &info), visit_end);
    let version = head_version(&state, (&bucket, &info), &key, &user).await?;
    Ok(Json(sign_ticket(
        (&link, visit_end),
        (&bucket, &key),
        version,
    )?))
}

/// Reads the share id out of a ticket before its signature can be checked.
fn ticket_share(ticket: &str) -> Option<Ulid> {
    use base64::Engine;
    let (body, _) = ticket.split_once('.')?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(body)
        .ok()?;
    postcard::from_bytes::<TicketBody>(&bytes)
        .ok()
        .map(|body| body.share_id)
}

#[utoipa::path(
    get,
    path = "/shares/download",
    tag = "shares",
    summary = "Download one shared file with a ticket",
    description = r#"Streams the file a download ticket names, as an attachment.

**Authentication**: the `ticket` query value only; it works once and for one minute.

**Behavior**
- The link and the creator's permission are checked again; a revoked link stops at once.
- The answer is `application/octet-stream` with `Content-Disposition: attachment`, so the
  browser never renders shared content."#,
    params(DownloadQuery),
    responses(
        (status = 200, description = "The file bytes"),
        (status = 404, description = "`share_unavailable`: ticket used, expired, forged, or the link ended", body = ErrorResponse),
        (status = 503, description = "The node's download capacity is exhausted, the bucket is locked, or `range_unavailable` when the member holding the file cannot be reached", body = ErrorResponse)
    ),
    security(())
)]
pub async fn download_file(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<DownloadQuery>,
) -> ServerResult<Response> {
    let ip = visitor_ip(&state, peer, &headers);
    let now = unix_now();
    let share_id = ticket_share(&query.ticket).ok_or_else(unavailable)?;
    let link = active_link(&state, share_id, now).await?;
    let ticket = link
        .open_ticket(&query.ticket, now)
        .ok_or_else(unavailable)?;
    require_reads(&link)?;
    let (bucket, info, auth) = super::dataset::ticket_scope(&state, &link, &ticket).await?;
    authorize_object(
        &state,
        &auth,
        (&bucket, &info),
        &ticket.key,
        (Permission::READ, "share.read"),
    )
    .await?;
    let ctx = state.get_ctx();
    aruna_operations::distribution::delegate::spend_nonce(&ctx, ticket.nonce, ticket.expires_at)
        .await
        .map_err(|_| unavailable())?;
    aruna_operations::distribution::delegate::prune_nonces(&ctx, now).await;
    let permit = download::admit(&state, LocalKey::Ip(ip)).map_err(|error| match error {
        AdmissionError::Total => {
            ServerError::ServiceUnavailableReason("download capacity exhausted".into())
        }
        AdmissionError::User => ServerError::Refused(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "too many downloads at once".to_string(),
        ),
    })?;
    // A key another member serves streams from there; the holder checks the creator again.
    let remote = match info.distribution.is_some() {
        true => {
            let user = share_access(&state, &link, (&auth, &info), ticket.expires_at);
            let service = member_service(&state).await;
            let read = service.member_get(&user, (&bucket, &info), &ticket.key, ticket.version_id);
            match read.await.map_err(|error| member_error(&error))? {
                ReadAnswer::Forwarded(response, _) => {
                    let size = (response.headers().get(header::CONTENT_LENGTH))
                        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok());
                    Some((shared_stream(response.bytes_stream()), size))
                }
                ReadAnswer::Local(_) => None,
            }
        }
        false => None,
    };
    let (blob, size) = match remote {
        Some(remote) => remote,
        None => {
            let result = get_object_routed(
                &state.get_ctx(),
                GetObjectInput {
                    bucket: bucket.clone(),
                    key: ticket.key.clone(),
                    version_id: ticket.version_id,
                    range: None,
                    group_id: info.group_id,
                    user_identity: auth.user_id,
                    realm_id: state.get_realm_id(),
                    node_id: state.get_node_id(),
                },
                auth.path_restrictions.clone(),
            )
            .await
            .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?;
            (result.blob, Some(result.info.size))
        }
    };
    let mut logged = event(ShareAction::Download, Some(ticket.key.clone()), ip);
    logged.bytes = size;
    log_event(&state, link.id, logged).await;
    let name = ticket.key.rsplit('/').next().unwrap_or("download");
    let disposition = format!(
        "attachment; filename*=UTF-8''{}",
        utf8_percent_encode(name, NON_ALPHANUMERIC)
    );
    let mut response = Response::new(download::body(blob, permit));
    let headers = response.headers_mut();
    if let Some(size) = size {
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from(size));
    }
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Ok(value) = HeaderValue::from_str(&disposition) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }
    Ok(response.into_response())
}

/// Claims the first free key of `wanted`, `wanted (1)` and so on that the visit may write.
/// The claim keeps concurrent share uploads apart; the create-only write keeps every file.
async fn claim_key(
    state: &ServerState,
    auth: &AuthContext,
    (bucket, info): (&str, &BucketInfo),
    wanted: &str,
    user: &UserAccess,
) -> ServerResult<String> {
    let now = unix_now();
    let until = now + UPLOAD_SECS.as_secs();
    let mut number = 0;
    loop {
        let candidate = match number {
            0 => wanted.to_string(),
            number => numbered_key(wanted, number),
        };
        number += 1;
        match head_version(state, (bucket, info), &candidate, user).await {
            Err(ServerError::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => continue,
        }
        let permission = (Permission::WRITE, "share.upload");
        authorize_object(state, auth, (bucket, info), &candidate, permission).await?;
        let claim = ClaimNameOperation::new(bucket, &candidate, now, until);
        if drive(claim, &state.get_ctx()).await.map_err(visit_error)? {
            return Ok(candidate);
        }
    }
}

#[utoipa::path(
    put,
    path = "/shares/visit/uploads",
    tag = "shares",
    summary = "Upload a file through a share link",
    description = r#"Stores the request body as a new file below the shared bucket or folder.

**Authentication**: `Authorization: Share <visit token>` of a `read_upload` or `upload` link.

**Behavior**
- The file lands like any upload of the link's creator: everyone with access to the bucket sees
  it, and it counts against the bucket's group quota. In a distributed bucket it is written,
  and its quota charged, at the member that coordinates its key.
- An upload never replaces a file: a taken name gets a number, such as `report (1).pdf`.
  When another writer creates the same name during the upload, it fails with 409
  `name_taken` and the client uploads again.
- The owner gets a notification and an access log entry for each upload."#,
    params(UploadQuery),
    request_body(content = Vec<u8>, content_type = "application/octet-stream"),
    responses(
        (status = 201, description = "Where the file landed", body = UploadResponse, example = json!({"name": "report (1).pdf", "size": 52344})),
        (status = 400, description = "The name is not a valid relative path", body = ErrorResponse),
        (status = 403, description = "The link does not accept uploads, or the folder is read-only", body = ErrorResponse),
        (status = 404, description = "`share_unavailable`", body = ErrorResponse),
        (status = 409, description = "The group storage quota is exhausted, or `name_taken`", body = ErrorResponse),
        (status = 503, description = "`range_unavailable`: the member that coordinates the name cannot be reached", body = ErrorResponse)
    ),
    security(("share_token" = []))
)]
pub async fn upload_file(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<UploadQuery>,
    body: Body,
) -> ServerResult<(StatusCode, Json<UploadResponse>)> {
    let ip = visitor_ip(&state, peer, &headers);
    let (link, visit_end) = visited_link(&state, &headers).await?;
    if !link.mode.uploads() {
        return Err(ServerError::Refused(
            StatusCode::FORBIDDEN,
            "read_only",
            "this link does not accept uploads".to_string(),
        ));
    }
    let wanted = upload_key(&link.target, &query.name)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    crate::s3::util::validate_object_key(&wanted)
        .map_err(|_| ServerError::BadRequestReason("the name is not a valid object key".into()))?;
    let (bucket, info, auth) = visit_scope(&state, &link).await?;
    guard_bucket_write(state.get_ctx().as_ref(), &bucket)
        .await
        .map_err(|_| ServerError::Forbidden)?;
    let user = share_access(&state, &link, (&auth, &info), visit_end);
    let key = claim_key(&state, &auth, (&bucket, &info), &wanted, &user).await?;
    let content_length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() <= 255 && !value.chars().any(char::is_control))
        .unwrap_or("application/octet-stream")
        .to_string();
    let deadline = tokio::time::Instant::now() + UPLOAD_SECS;
    let stream = crate::routes::rocrate_import::upload_body_stream(body, deadline);
    let upload = (stream, content_length, content_type);
    let stored = store_upload(&state, (&auth, &user), (&bucket, &info), &key, upload).await;
    let release = ReleaseNameOperation::new(&bucket, &key);
    if let Err(error) = drive(release, &state.get_ctx()).await {
        tracing::warn!(share_id = %link.id, error = %error, "Share upload name claim not released");
    }
    let size = stored?;
    let mut logged = event(ShareAction::Upload, Some(key.clone()), ip);
    logged.bytes = Some(size);
    log_event(&state, link.id, logged).await;
    notify_owner(&state, &link, (&bucket, &info), &key, size).await;
    let name = key
        .strip_prefix(root(&link.target))
        .unwrap_or(&key)
        .to_string();
    Ok((StatusCode::CREATED, Json(UploadResponse { name, size })))
}

/// Stores an upload create-only at the coordinator of its key and returns its size. Another
/// member's range takes it as `user`, and its quota is charged there.
async fn store_upload(
    state: &Arc<ServerState>,
    (auth, user): (&AuthContext, &UserAccess),
    (bucket, info): (&str, &BucketInfo),
    key: &str,
    (stream, content_length, content_type): (UploadStream, Option<u64>, String),
) -> ServerResult<u64> {
    let service = member_service(state).await;
    let extensions = axum::http::Extensions::new();
    let target = service.forward_target(&extensions, Some(info), key).await;
    let Some(forward) = target.map_err(|error| upload_error(&error))? else {
        let metadata = [(CONTENT_TYPE_KEY.to_string(), content_type)].into();
        let upload = (stream, content_length);
        let stored = store_stream(state, auth, info, (bucket, key), upload, (metadata, true));
        return Ok(stored.await?.location.blob_size);
    };
    let counted = Arc::new(AtomicU64::new(0));
    let body = counted_blob(stream, counted.clone());
    let upload = (body, content_length, content_type.as_str());
    let sent = service
        .member_put(user, &forward, (bucket, key), upload)
        .await;
    sent.map_err(|error| upload_error(&error))?;
    Ok(counted.load(Ordering::Relaxed))
}

/// Tells the link's creator about an upload; a failed notice never fails the upload.
async fn notify_owner(
    state: &ServerState,
    link: &ShareLink,
    (bucket, info): (&str, &BucketInfo),
    key: &str,
    size: u64,
) {
    let kind = NotificationKind::ShareUpload {
        share_id: link.id,
        node_id: state.get_node_id(),
        group_id: info.group_id,
        bucket: bucket.to_string(),
        key: key.to_string(),
        size_bytes: size,
    };
    let record =
        NotificationRecord::new(link.created_by, NotificationClass::Direct, kind, now_ms());
    let operation = EmitNotificationsOperation::new(EmitNotificationsInput {
        records: vec![record],
    });
    if let Err(error) = drive(operation, &state.get_ctx()).await {
        tracing::warn!(share_id = %link.id, error = %error, "Share upload notice not sent");
    }
}
