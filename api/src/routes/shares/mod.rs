//! Share links for people without an account: owners create, list, copy and revoke links on
//! this node; visitors open them (see `visit`).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::password::{Kdf, PasswordHash};
use aruna_core::structs::identity::auth::{AuthContext, SessionKind};
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::share::{
    ShareAction, ShareEvent, ShareLink, ShareMode, ShareTarget, ShareUses,
};
use aruna_operations::auth::request_authorization::AuthorizeError;
use aruna_operations::auth::request_policy::{
    PolicyRequestExtras, enforce_policies, policy_request_with,
};
use aruna_operations::driver::drive;
use aruna_operations::shares::lifecycle::{
    CreateShareConfig, CreateShareOperation, RevokeShareOperation, ShareError,
};
use aruna_operations::shares::rules::required_permission;
use aruna_operations::shares::store::{
    GetShareOperation, ListSharesOperation, ReadUsesOperation, ShareEventsOperation,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{map_authorize_error, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::deletion::bucket_info;
use crate::server::state::ServerState;

mod dataset;
mod relay;
mod visit;

/// Bytes of a link password; argon2id hashes any length, this only bounds the request.
const MAX_PASSWORD: usize = 1024;
/// Log entries per page when the caller names no limit.
const LOG_PAGE: usize = 100;

#[derive(OpenApi)]
#[openapi(tags((name = "shares", description = "Share links for people without an account")))]
pub struct SharesDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(SharesDoc::openapi())
        .routes(routes!(create_share, list_shares))
        .routes(routes!(get_share, revoke_share))
        .routes(routes!(share_log))
        .merge(visit::router())
}

/// What a link opens.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ShareTargetBody {
    Bucket {
        bucket: String,
    },
    /// `prefix` is a folder path ending with `/`.
    Folder {
        bucket: String,
        prefix: String,
    },
    Object {
        bucket: String,
        key: String,
    },
    /// A dataset this node holds, with the files it names on this node.
    Dataset {
        dataset_id: String,
    },
}

impl From<&ShareTarget> for ShareTargetBody {
    fn from(target: &ShareTarget) -> Self {
        match target {
            ShareTarget::Bucket { bucket } => Self::Bucket {
                bucket: bucket.clone(),
            },
            ShareTarget::Folder { bucket, prefix } => Self::Folder {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
            },
            ShareTarget::Object { bucket, key } => Self::Object {
                bucket: bucket.clone(),
                key: key.clone(),
            },
            ShareTarget::Dataset { document_id } => Self::Dataset {
                dataset_id: document_id.to_string(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShareModeBody {
    Read,
    ReadUpload,
    Upload,
}

impl From<ShareModeBody> for ShareMode {
    fn from(mode: ShareModeBody) -> Self {
        match mode {
            ShareModeBody::Read => Self::Read,
            ShareModeBody::ReadUpload => Self::ReadUpload,
            ShareModeBody::Upload => Self::Upload,
        }
    }
}

impl From<ShareMode> for ShareModeBody {
    fn from(mode: ShareMode) -> Self {
        match mode {
            ShareMode::Read => Self::Read,
            ShareMode::ReadUpload => Self::ReadUpload,
            ShareMode::Upload => Self::Upload,
        }
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateShareRequest {
    pub target: ShareTargetBody,
    pub mode: ShareModeBody,
    /// Shown in the owner's list only.
    #[serde(default)]
    pub label: String,
    /// RFC 3339 time after which the link no longer opens; omitted for no expiry.
    pub expires_at: Option<DateTime<Utc>>,
    /// Visitors enter it once per visit.
    pub password: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShareStatus {
    Active,
    Expired,
    Revoked,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ShareInfo {
    pub id: String,
    pub target: ShareTargetBody,
    pub mode: ShareModeBody,
    pub label: String,
    pub created_at: String,
    pub expires_at: Option<String>,
    /// Whether visitors must enter a password.
    pub password: bool,
    pub revoked_at: Option<String>,
    pub status: ShareStatus,
    /// Downloads and uploads through the link, counted as they are logged.
    pub downloads: u64,
    pub uploads: u64,
    /// The last open, download or upload; absent while the link was never used.
    pub last_access_at: Option<String>,
}

/// A link and the token its URL carries in the fragment: `/s/<node>#<token>`.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ShareDetails {
    pub share: ShareInfo,
    pub token: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ShareList {
    pub shares: Vec<ShareInfo>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ShareLogEntry {
    pub id: String,
    pub at: String,
    /// `open`, `wrong_password`, `download` or `upload`.
    pub action: String,
    pub key: Option<String>,
    pub bytes: Option<u64>,
    /// The visitor's network, an IPv4 /24 or IPv6 /48.
    pub client: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ShareLog {
    pub entries: Vec<ShareLogEntry>,
    /// Pass as `after` for the next page; absent on the last page.
    pub next: Option<String>,
}

#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ShareLogQuery {
    /// The `next` of the previous page.
    pub after: Option<String>,
    /// Entries per page, 1 to 1000; defaults to 100.
    pub limit: Option<usize>,
}

pub(crate) fn unix_now() -> u64 {
    Utc::now().timestamp().max(0) as u64
}

pub(crate) fn rfc3339(secs: u64) -> String {
    DateTime::<Utc>::from_timestamp(secs as i64, 0)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub(crate) fn share_info(link: &ShareLink, uses: ShareUses, now: u64) -> ShareInfo {
    let status = match (link.revoked_at, link.active(now)) {
        (Some(_), _) => ShareStatus::Revoked,
        (None, true) => ShareStatus::Active,
        (None, false) => ShareStatus::Expired,
    };
    ShareInfo {
        id: link.id.to_string(),
        target: ShareTargetBody::from(&link.target),
        mode: link.mode.into(),
        label: link.label.clone(),
        created_at: rfc3339(link.created_at),
        expires_at: link.expires_at.map(rfc3339),
        password: link.password.is_some(),
        revoked_at: link.revoked_at.map(rfc3339),
        status,
        downloads: uses.downloads,
        uploads: uses.uploads,
        last_access_at: uses.last_access_at.map(|at| {
            DateTime::<Utc>::from_timestamp_millis(at as i64)
                .unwrap_or_default()
                .to_rfc3339_opts(SecondsFormat::Millis, true)
        }),
    }
}

/// The use counters of `links`, in order.
async fn link_uses(state: &ServerState, links: &[ShareLink]) -> ServerResult<Vec<ShareUses>> {
    let ids = links.iter().map(|link| link.id).collect();
    drive(ReadUsesOperation::new(ids), &state.get_ctx())
        .await
        .map_err(store_error)
}

fn store_error(error: impl std::fmt::Display) -> ServerError {
    ServerError::InternalError(error.to_string())
}

/// Only a full portal or API session may act for its user beyond its own lifetime.
fn require_creator(state: &ServerState, auth: Option<AuthContext>) -> ServerResult<AuthContext> {
    let auth = require_unrestricted_auth(state, auth)?;
    let kind = auth.session.as_ref().map(|session| session.kind);
    match kind {
        None | Some(SessionKind::Portal | SessionKind::Api) => Ok(auth),
        Some(SessionKind::Assistant | SessionKind::Federated | SessionKind::Share) => {
            Err(ServerError::Forbidden)
        }
    }
}

/// Loads one of the caller's links; another user's link answers 404 like an unknown one.
async fn own_share(state: &ServerState, auth: &AuthContext, id: &str) -> ServerResult<ShareLink> {
    let id = Ulid::from_string(id).map_err(|_| ServerError::NotFound)?;
    let read = GetShareOperation::for_owner(id, auth.user_id);
    match drive(read, &state.get_ctx()).await {
        Ok(Some(link)) => Ok(link),
        Ok(None) => Err(ServerError::NotFound),
        Err(error) => Err(store_error(error)),
    }
}

/// The group, target and permission path of a requested link on this node.
async fn resolve_target(
    state: &ServerState,
    auth: &AuthContext,
    target: &ShareTargetBody,
) -> ServerResult<(Ulid, ShareTarget, String)> {
    let (bucket, target) = match target {
        ShareTargetBody::Dataset { dataset_id } => {
            return dataset::resolve_dataset(state, auth, dataset_id).await;
        }
        ShareTargetBody::Bucket { bucket } => (
            bucket,
            ShareTarget::Bucket {
                bucket: bucket.clone(),
            },
        ),
        ShareTargetBody::Folder { bucket, prefix } => (
            bucket,
            ShareTarget::Folder {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
            },
        ),
        ShareTargetBody::Object { bucket, key } => (
            bucket,
            ShareTarget::Object {
                bucket: bucket.clone(),
                key: key.clone(),
            },
        ),
    };
    let info = bucket_info(state, bucket).await?;
    let (realm, node) = (state.get_realm_id(), state.get_node_id());
    let path = match &target {
        ShareTarget::Folder { prefix, .. } | ShareTarget::Object { key: prefix, .. } => {
            object_permission_path(realm, info.group_id, node, bucket, prefix)
        }
        _ => bucket_permission_path(realm, info.group_id, node, bucket),
    };
    Ok((info.group_id, target, path))
}

/// Realm and group policies see the same `share.create` request for every kind of target.
async fn share_policies(
    state: &ServerState,
    auth: &AuthContext,
    path: &str,
    request: &CreateShareRequest,
) -> ServerResult<()> {
    let mut extras = PolicyRequestExtras::operation("share.create");
    extras.body = Some(serde_json::json!({
        "target": request.target,
        "mode": request.mode,
        "expires": request.expires_at.is_some(),
        "password": request.password.is_some(),
    }));
    let permission = required_permission(request.mode.into());
    let policy = policy_request_with(path, &permission, Some(auth), extras);
    enforce_policies(&state.get_ctx(), state.get_realm_id(), &policy)
        .await
        .map_err(|error| map_authorize_error(AuthorizeError::Policy(error)))
}

fn share_error(error: ShareError) -> ServerError {
    match error {
        ShareError::Rule(error) => ServerError::BadRequestReason(error.to_string()),
        ShareError::Denied => ServerError::Forbidden,
        ShareError::Unavailable => ServerError::NotFound,
        error => ServerError::InternalError(error.to_string()),
    }
}

#[utoipa::path(
    post,
    path = "/shares",
    tag = "shares",
    summary = "Create a share link",
    description = r#"Creates a link on this node that opens a bucket, folder, file or dataset as the caller.

**Authentication**: realm bearer token of a portal or API session without path restrictions.
The caller needs READ on the target, or WRITE for `read_upload` and `upload`.

**Behavior**
- Every visit acts as the creator and is checked again on each request, so the link stops
  working as soon as the creator loses access. Uploads count against the bucket's group quota.
- `upload` links let visitors add files without seeing anything stored.
- Upload modes need a bucket or folder target.
- A link to a distributed bucket is served by this node, which reads and writes the keys of
  other members' ranges there; each member checks the creator on its own path.
- The returned `token` goes into the URL fragment (`/s/<node>#<token>`), never into a path.
  The owner can read it again with `GET /shares/{id}`.
- Realm and group policies see the operation `share.create` with the target and mode."#,
    request_body(
        content = CreateShareRequest,
        example = json!({
            "target": {"kind": "folder", "bucket": "project-data", "prefix": "results/"},
            "mode": "read",
            "label": "Results for reviewers",
            "expires_at": "2026-11-30T00:00:00Z",
            "password": "correct horse"
        })
    ),
    responses(
        (status = 201, description = "The link and its token", body = ShareDetails, example = json!({"share": {"id": "01JSHARE0123456789ABCDEFGH", "target": {"kind": "folder", "bucket": "project-data", "prefix": "results/"}, "mode": "read", "label": "Results for reviewers", "created_at": "2026-10-31T09:00:00Z", "expires_at": "2026-11-30T00:00:00Z", "password": true, "revoked_at": null, "status": "active", "downloads": 12, "uploads": 0, "last_access_at": "2026-10-31T10:00:00.000Z"}, "token": "01JSHARE0123456789ABCDEFGH.<link-secret-shown-to-the-owner-only>"})),
        (status = 400, description = "Malformed target, upload mode on an object or dataset, an expiry in the past, or a label or password that is too long", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller lacks access to the target, holds a restricted, assistant or federated session, or a policy denies the link", body = ErrorResponse),
        (status = 404, description = "No such bucket or dataset on this node", body = ErrorResponse),
        (status = 409, description = "`dataset_not_here` when this node holds no copy of the dataset", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_share(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateShareRequest>,
) -> ServerResult<(StatusCode, Json<ShareDetails>)> {
    let auth = require_creator(&state, auth)?;
    let now = unix_now();
    let password = match request.password.as_deref() {
        Some(password) if password.is_empty() || password.len() > MAX_PASSWORD => {
            return Err(ServerError::BadRequestReason(format!(
                "a password has 1 to {MAX_PASSWORD} bytes"
            )));
        }
        Some(password) => {
            // Hashing is CPU work; it runs off the request threads.
            let (password, salt) = (password.to_string(), rand::random());
            let hash =
                tokio::task::spawn_blocking(move || PasswordHash::new(&password, Kdf::SHARE, salt))
                    .await
                    .map_err(|error| ServerError::InternalError(error.to_string()))?
                    .map_err(|error| ServerError::InternalError(error.to_string()))?;
            Some(hash)
        }
        None => None,
    };
    let (group_id, target, path) = resolve_target(&state, &auth, &request.target).await?;
    share_policies(&state, &auth, &path, &request).await?;
    let link = ShareLink {
        id: Ulid::generate(),
        group_id,
        target,
        mode: request.mode.into(),
        created_by: auth.user_id,
        created_at: now,
        label: request.label,
        expires_at: request
            .expires_at
            .map(|expiry| expiry.timestamp().max(0) as u64),
        password,
        secret: rand::random(),
        signing_key: rand::random(),
        revoked_at: None,
    };
    let config = CreateShareConfig {
        auth,
        link,
        path,
        now,
    };
    let link = drive(CreateShareOperation::new(config), &state.get_ctx())
        .await
        .map_err(share_error)?;
    tracing::info!(share_id = %link.id, "Created share link");
    let details = ShareDetails {
        share: share_info(&link, ShareUses::default(), now),
        token: link.link_token(),
    };
    Ok((StatusCode::CREATED, Json(details)))
}

#[utoipa::path(
    get,
    path = "/shares",
    tag = "shares",
    summary = "List my share links on this node",
    description = r#"Lists every link the caller created on this node, oldest first, with ended ones.

**Authentication**: realm bearer token without path restrictions.

**Behavior**
- Tokens are not listed; read one link to copy its token again.
- `downloads`, `uploads` and `last_access_at` are counted with each access log entry, so the
  list needs no log reads."#,
    responses(
        (status = 200, description = "The caller's links", body = ShareList, example = json!({"shares": [{"id": "01JSHARE0123456789ABCDEFGH", "target": {"kind": "folder", "bucket": "project-data", "prefix": "results/"}, "mode": "read", "label": "Results for reviewers", "created_at": "2026-10-31T09:00:00Z", "expires_at": "2026-11-30T00:00:00Z", "password": true, "revoked_at": null, "status": "active", "downloads": 12, "uploads": 0, "last_access_at": "2026-10-31T10:00:00.000Z"}]})),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is restricted or belongs to another realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_shares(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<ShareList>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let links = drive(ListSharesOperation::new(auth.user_id), &state.get_ctx())
        .await
        .map_err(store_error)?;
    let uses = link_uses(&state, &links).await?;
    let now = unix_now();
    let shares = links
        .iter()
        .zip(uses)
        .map(|(link, uses)| share_info(link, uses, now))
        .collect();
    Ok(Json(ShareList { shares }))
}

#[utoipa::path(
    get,
    path = "/shares/{id}",
    tag = "shares",
    summary = "Read a share link with its token",
    description = r#"Returns the link and its token, so the owner can copy the link again.

**Authentication**: realm bearer token without path restrictions; only the creator reads a
link, every other caller gets 404."#,
    params(("id" = String, Path, description = "Share link id")),
    responses(
        (status = 200, description = "The link and its token", body = ShareDetails, example = json!({"share": {"id": "01JSHARE0123456789ABCDEFGH", "target": {"kind": "folder", "bucket": "project-data", "prefix": "results/"}, "mode": "read", "label": "Results for reviewers", "created_at": "2026-10-31T09:00:00Z", "expires_at": "2026-11-30T00:00:00Z", "password": true, "revoked_at": null, "status": "active", "downloads": 12, "uploads": 0, "last_access_at": "2026-10-31T10:00:00.000Z"}, "token": "01JSHARE0123456789ABCDEFGH.<link-secret-shown-to-the-owner-only>"})),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No link of the caller has this id", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_share(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<Json<ShareDetails>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let link = own_share(&state, &auth, &id).await?;
    let uses = link_uses(&state, std::slice::from_ref(&link)).await?;
    let uses = uses.first().copied().unwrap_or_default();
    Ok(Json(ShareDetails {
        share: share_info(&link, uses, unix_now()),
        token: link.link_token(),
    }))
}

#[utoipa::path(
    delete,
    path = "/shares/{id}",
    tag = "shares",
    summary = "Revoke one of my share links",
    description = r#"Ends the link at once; open visits fail on their next request.

**Authentication**: realm bearer token without path restrictions; only the creator revokes.

**Behavior**
- The link stays in the owner's list as revoked, with its access log.
- Open visits fail on their next request, also for keys other members of a distributed bucket
  serve; a download that already started runs to its end."#,
    params(("id" = String, Path, description = "Share link id")),
    responses(
        (status = 204, description = "Revoked, or already revoked"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No link of the caller has this id", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_share(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let id = Ulid::from_string(&id).map_err(|_| ServerError::NotFound)?;
    let revoke = RevokeShareOperation::new(auth.user_id, id, unix_now());
    drive(revoke, &state.get_ctx()).await.map_err(share_error)?;
    tracing::info!(share_id = %id, "Revoked share link");
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/shares/{id}/log",
    tag = "shares",
    summary = "Read the access log of a share link",
    description = r#"Lists opens, wrong passwords, downloads and uploads of the link, oldest first.

**Authentication**: realm bearer token without path restrictions; only the creator reads it.

**Behavior**: client addresses are shortened to their network (IPv4 /24, IPv6 /48)."#,
    params(("id" = String, Path, description = "Share link id"), ShareLogQuery),
    responses(
        (status = 200, description = "One page of the log", body = ShareLog, example = json!({"entries": [{"id": "01JLOG00123456789ABCDEFGHJ", "at": "2026-10-31T10:00:00.000Z", "action": "download", "key": "results/table.csv", "bytes": 2048, "client": "192.0.2.0/24"}], "next": null})),
        (status = 400, description = "Malformed `after` or a limit outside 1 to 1000", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No link of the caller has this id", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn share_log(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
    Query(query): Query<ShareLogQuery>,
) -> ServerResult<Json<ShareLog>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let link = own_share(&state, &auth, &id).await?;
    let limit = query.limit.unwrap_or(LOG_PAGE);
    if !(1..=1000).contains(&limit) {
        return Err(ServerError::BadRequestReason(
            "limit must be 1 to 1000".into(),
        ));
    }
    let after = query
        .after
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequestReason("after is not a log entry id".into()))?;
    let (events, next) = drive(
        ShareEventsOperation::new(link.id, after, limit),
        &state.get_ctx(),
    )
    .await
    .map_err(store_error)?;
    let entries = events
        .into_iter()
        .map(|(id, event)| ShareLogEntry {
            id: id.to_string(),
            at: DateTime::<Utc>::from_timestamp_millis(event.at as i64)
                .unwrap_or_default()
                .to_rfc3339_opts(SecondsFormat::Millis, true),
            action: action_name(event.action).to_string(),
            key: event.key,
            bytes: event.bytes,
            client: event.client,
        })
        .collect();
    Ok(Json(ShareLog {
        entries,
        next: next.map(|id| id.to_string()),
    }))
}

fn action_name(action: ShareAction) -> &'static str {
    match action {
        ShareAction::Open => "open",
        ShareAction::WrongPassword => "wrong_password",
        ShareAction::Download => "download",
        ShareAction::Upload => "upload",
    }
}

/// Appends a log entry; a failed write is traced and never fails the visit.
pub(crate) async fn log_event(state: &ServerState, share_id: Ulid, event: ShareEvent) {
    let entry = Ulid::generate();
    let operation = aruna_operations::shares::store::LogShareOperation::new(share_id, entry, event);
    if let Err(error) = drive(operation, &state.get_ctx()).await {
        tracing::warn!(share_id = %share_id, error = %error, "Share link log entry not written");
    }
}

#[cfg(test)]
mod members_tests;
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
