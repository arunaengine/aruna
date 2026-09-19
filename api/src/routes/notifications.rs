//! Serves the notification inbox, its live stream and the caller's notification watches.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ValidatedBearer, ensure_permission_with, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::execution::jobs::{decode_cursor, encode_cursor};
use crate::server::state::ServerState;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::metrics::WatchMetricReason;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::execution::notification_watch::{
    MAX_PREFIX_LEN, WatchAuthorizationBinding, WatchEventKind, WatchEventMask, WatchSubscription,
    parse_watch_path, watch_resource_path,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::node::dashboard::subscribe_dashboard_changes;
use aruna_operations::notifications::dispatch;
use aruna_operations::notifications::dispatch::{
    InboxWakeReceiver, NotificationDispatchError, WatchDispatchError, create_for_user,
    delete_for_user, list_for_user, mark_for_user, resolve_user_holder, subscribe_inbox_wakes,
    unread_for_user,
};
use aruna_operations::notifications::list::LIST_MAX_LIMIT;
use aruna_operations::notifications::mark_read::MARK_MAX_IDS;
use aruna_operations::notifications::watch::authorization::{
    WatchAuthorization, evaluate_watch_creation, watch_permission_path,
};
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::{Extension, Json};
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

const DEFAULT_LIST_LIMIT: usize = 50;
/// Burst-coalescing window on the local wake arm: after a wake, wait briefly and
/// drain further wakes so a delivery storm collapses into one unread refetch.
const NOTIFICATION_STREAM_COALESCE: Duration = Duration::from_millis(200);
/// Remote-holder poll interval; emits only when the unread count changed.
const STREAM_REMOTE_POLL: Duration = Duration::from_secs(5);
/// State snapshot and keep-alive cadence so proxies do not cut an idle stream.
const STREAM_KEEP_ALIVE: Duration = Duration::from_secs(20);
/// Re-resolve cadence for detecting an inbox holder that moved without a local wake.
/// The local arm switches to remote polling after a move.
const NOTIFICATION_STREAM_RECHECK: Duration = Duration::from_secs(60);

#[derive(OpenApi)]
#[openapi(
    tags((name = "system/notifications", description = "User notification inbox")),
    components(schemas(
        NotificationStreamResponse,
        UnreadCountResponse
    ))
)]
pub struct NotificationsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(NotificationsApiDoc::openapi())
        .routes(routes!(list_notifications))
        .routes(routes!(unread_count))
        .routes(routes!(stream_notifications))
        .routes(routes!(mark_read))
        .routes(routes!(list_watches, create_watch))
        .routes(routes!(delete_watch))
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub struct ListNotificationsQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct NotificationResponse {
    pub id: String,
    pub category: String,
    pub kind: String,
    pub class: String,
    pub created_at_ms: u64,
    pub read: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub versions_synced: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct NotificationListResponse {
    pub notifications: Vec<NotificationResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = UnreadCountApiResponse)]
pub struct UnreadCountResponse {
    pub count: u32,
    pub capped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = NotificationStreamStateResponse)]
pub struct NotificationStreamResponse {
    pub epoch: String,
    pub revision: u64,
    pub unread: UnreadCountResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = MarkReadApiRequest)]
pub struct MarkReadRequest {
    #[serde(default)]
    pub ids: Vec<String>,
    #[serde(default)]
    pub up_to_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = MarkReadApiResponse)]
pub struct MarkReadResponse {
    pub marked: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WatchResponse {
    pub id: String,
    pub path_prefix: String,
    pub events: Vec<String>,
    pub created_at_ms: u64,
    /// False when the owner may no longer read the watched prefix, so the watch
    /// no longer delivers. Its details are withheld while it is unauthorized.
    pub authorized: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WatchListResponse {
    pub watches: Vec<WatchResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateWatchRequest {
    /// Canonical data (`s3/{group}/{node}/{bucket}/{key}`) or metadata prefix.
    /// A local data bucket's owning group replaces the supplied group.
    /// Prefixes require the bucket/group slash and one event namespace.
    pub path_prefix: String,
    pub events: Vec<String>,
}

fn map_dispatch_error(error: NotificationDispatchError, operation: &str) -> ServerError {
    match error {
        NotificationDispatchError::Unavailable => ServerError::ServiceUnavailable,
        NotificationDispatchError::Internal(reason) => ServerError::InternalError(reason),
        NotificationDispatchError::Remote(reason) => {
            warn!(operation, reason = %reason, "notification holder proxy failed");
            ServerError::BadGateway
        }
    }
}

fn map_watch_error(error: WatchDispatchError, operation: &str) -> ServerError {
    match error {
        WatchDispatchError::Unavailable => ServerError::ServiceUnavailable,
        WatchDispatchError::CapExceeded => {
            ServerError::Conflict("notification watch subscription cap reached".to_string())
        }
        // A holder-side denial answers exactly as the create-time check does, so
        // an unreadable path never separates into a distinct existence signal.
        WatchDispatchError::Unauthorized(_) => ServerError::Forbidden,
        WatchDispatchError::Internal(reason) => ServerError::InternalError(reason),
        WatchDispatchError::Remote(reason) => {
            warn!(operation, reason = %reason, "notification holder proxy failed");
            ServerError::BadGateway
        }
    }
}

fn watch_response(subscription: &WatchSubscription) -> WatchResponse {
    WatchResponse {
        id: subscription.watch_id.to_string(),
        path_prefix: subscription.path_prefix.clone(),
        events: subscription
            .event_mask
            .kinds()
            .iter()
            .map(|kind| kind.name().to_string())
            .collect(),
        created_at_ms: subscription.created_at_ms,
        authorized: watch_authorized(subscription),
    }
}

/// The holder withholds the details of a watch its owner may no longer read but
/// keeps the row, so an emptied prefix is the "no longer delivering" marker.
fn watch_authorized(subscription: &WatchSubscription) -> bool {
    !subscription.path_prefix.is_empty()
}

fn record_watch_denial(state: &ServerState, reason: WatchMetricReason) {
    dispatch::record_watch_denial(state.get_ctx().as_ref(), reason);
    warn!(
        parent: None,
        reason = reason.as_str(),
        "Notification watch creation denied"
    );
}

fn notification_response(record: &NotificationRecord) -> NotificationResponse {
    let mut response = NotificationResponse {
        id: record.notification_id.to_string(),
        category: record.kind.category().to_string(),
        kind: record.kind.name().to_string(),
        class: match record.class {
            NotificationClass::Direct => "direct",
            NotificationClass::Transient => "transient",
        }
        .to_string(),
        created_at_ms: record.created_at_ms,
        read: record.read_at_ms.is_some(),
        group_id: None,
        request_id: None,
        member_user_id: None,
        actor_user_id: None,
        node_id: None,
        realm_id: None,
        path: None,
        document_id: None,
        bucket: None,
        key: None,
        size_bytes: None,
        relationship_id: None,
        versions_synced: None,
        error: None,
    };
    match &record.kind {
        NotificationKind::GroupJoinRequested {
            group_id,
            request_id,
            actor_user_id,
        } => {
            response.group_id = Some(group_id.to_string());
            response.request_id = Some(request_id.to_string());
            response.actor_user_id = Some(actor_user_id.to_string());
        }
        NotificationKind::AddedToGroup {
            group_id,
            actor_user_id,
        }
        | NotificationKind::RemovedFromGroup {
            group_id,
            actor_user_id,
        } => {
            response.group_id = Some(group_id.to_string());
            response.actor_user_id = Some(actor_user_id.to_string());
        }
        NotificationKind::GroupMemberAdded {
            group_id,
            member_user_id,
            actor_user_id,
        } => {
            response.group_id = Some(group_id.to_string());
            response.member_user_id = Some(member_user_id.to_string());
            response.actor_user_id = Some(actor_user_id.to_string());
        }
        NotificationKind::NodeOnboarded { realm_id, node_id } => {
            response.realm_id = Some(realm_id.to_string());
            response.node_id = Some(node_id.to_string());
        }
        NotificationKind::MetadataCreated {
            path,
            group_id,
            document_id,
            actor_user_id,
        } => {
            response.path = Some(path.clone());
            response.group_id = Some(group_id.to_string());
            response.document_id = Some(document_id.to_string());
            response.actor_user_id = Some(actor_user_id.to_string());
        }
        NotificationKind::DataUploaded {
            path,
            group_id,
            node_id,
            bucket,
            key,
            size_bytes,
            actor_user_id,
        } => {
            response.path = Some(path.clone());
            response.group_id = Some(group_id.to_string());
            response.node_id = Some(node_id.to_string());
            response.bucket = Some(bucket.clone());
            response.key = Some(key.clone());
            response.size_bytes = Some(*size_bytes);
            response.actor_user_id = Some(actor_user_id.to_string());
        }
        NotificationKind::SyncCompleted {
            path,
            group_id,
            node_id,
            bucket,
            relationship_id,
            versions_synced,
            actor_user_id,
        } => {
            response.path = Some(path.clone());
            response.group_id = Some(group_id.to_string());
            response.node_id = Some(node_id.to_string());
            response.bucket = Some(bucket.clone());
            response.relationship_id = Some(relationship_id.to_string());
            response.versions_synced = Some(*versions_synced);
            response.actor_user_id = Some(actor_user_id.to_string());
        }
        NotificationKind::SyncFailed {
            path,
            group_id,
            node_id,
            bucket,
            relationship_id,
            error,
            actor_user_id,
        } => {
            response.path = Some(path.clone());
            response.group_id = Some(group_id.to_string());
            response.node_id = Some(node_id.to_string());
            response.bucket = Some(bucket.clone());
            response.relationship_id = Some(relationship_id.to_string());
            response.error = Some(error.clone());
            response.actor_user_id = Some(actor_user_id.to_string());
        }
    }
    response
}

/// Creation checks the request's current restrictions on the canonical path.
/// Invalid identities are malformed; unreadable resources answer Forbidden.
async fn authorize_watch(
    state: &ServerState,
    auth: &AuthContext,
    path_prefix: &str,
    event_mask: WatchEventMask,
) -> ServerResult<()> {
    let Some(permission_path) =
        watch_permission_path(state.get_realm_id(), path_prefix, event_mask)
    else {
        record_watch_denial(state, WatchMetricReason::InvalidResource);
        return Err(ServerError::BadRequest);
    };
    if let Err(error) = ensure_permission_with(
        state,
        auth,
        permission_path,
        Permission::READ,
        aruna_operations::auth::request_policy::PolicyRequestExtras::operation(
            "notifications.create_watch",
        ),
    )
    .await
    {
        record_watch_denial(
            state,
            match error {
                ServerError::Forbidden => WatchMetricReason::PermissionDenied,
                _ => WatchMetricReason::AuthorizationUnavailable,
            },
        );
        return Err(error);
    }
    match evaluate_watch_creation(
        &state.get_ctx(),
        state.get_realm_id(),
        auth,
        path_prefix,
        event_mask,
    )
    .await
    {
        Ok(WatchAuthorization::Authorized) => Ok(()),
        Ok(WatchAuthorization::Denied(reason)) => {
            record_watch_denial(state, reason.metric_reason());
            Err(ServerError::Forbidden)
        }
        Ok(WatchAuthorization::Unavailable(_)) => {
            record_watch_denial(state, WatchMetricReason::AuthorizationUnavailable);
            Err(ServerError::Forbidden)
        }
        Err(error) => {
            record_watch_denial(state, WatchMetricReason::AuthorizationUnavailable);
            Err(ServerError::InternalError(error))
        }
    }
}

async fn canonicalize_watch_path(
    state: &ServerState,
    path_prefix: String,
    event_mask: WatchEventMask,
) -> ServerResult<String> {
    if event_mask.bits() != WatchEventMask::DATA_UPLOADED {
        return Ok(path_prefix);
    }
    let Some((node_id, bucket, key_prefix)) = parse_watch_path(&path_prefix).map(|resource| {
        (
            resource.node_id,
            resource.bucket.to_string(),
            resource.key_prefix.to_string(),
        )
    }) else {
        return Ok(path_prefix);
    };
    if node_id != state.get_node_id() {
        return Ok(path_prefix);
    }
    match drive(GetBucketOperation::new(bucket.clone()), &state.get_ctx()).await {
        Ok(info) => Ok(watch_resource_path(
            info.group_id,
            node_id,
            &bucket,
            &key_prefix,
        )),
        Err(GetBucketError::NotFound) => Ok(path_prefix),
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

#[utoipa::path(
    get,
    path = "/system/notifications",
    tag = "system/notifications",
    summary = "List the caller's notification inbox",
    description = r#"Lists the calling user's notification inbox, newest first.

**Authentication**: realm bearer token; the inbox is self-scoped to the calling user, so no further
permission is checked, and a path-restricted token is refused because a user-scoped surface cannot
honor a delegated token's confinement.

**Behavior**
- The request is served by the node that holds the caller's inbox, either locally or proxied to
  that holder over the realm network, so results are as consistent as that single holder.
- Resource-watch notifications are re-authorized while the page is assembled and dropped when the
  caller may no longer read the resource; suppressed rows do not consume the page.
- A response without `next_cursor` is the last page.

**Limits**
- `limit` defaults to 50 and is capped at 200; a limit of 0 is raised to 1.
- `cursor` must be an unpadded base64url encoding of exactly 24 bytes."#,
    params(
        ("limit" = Option<usize>, Query, description = "Max notifications to return, default 50, capped at 200; a limit of 0 is raised to 1"),
        ("cursor" = Option<String>, Query, description = "Opaque pagination cursor: pass the `next_cursor` of the previous page, an unpadded base64url encoding of 24 bytes; omit it to start at the newest notification")
    ),
    responses(
        (
            status = 200,
            description = "Page of the caller's notifications, newest first",
            body = NotificationListResponse,
            example = json!({
                "notifications": [
                    {
                        "id": "01JABCDEF0123456789ABCDEFG",
                        "category": "group.membership",
                        "kind": "added_to_group",
                        "class": "direct",
                        "created_at_ms": 1775744591123_i64,
                        "read": false,
                        "group_id": "01JGRP000123456789ABCDEFGH",
                        "actor_user_id": "01JACTR00123456789ABCDEFGH"
                    },
                    {
                        "id": "01JMETADATA0123456789ABCDE",
                        "category": "resource.watch",
                        "kind": "data_uploaded",
                        "class": "transient",
                        "created_at_ms": 1775744501001_i64,
                        "read": true,
                        "group_id": "01JGRP000123456789ABCDEFGH",
                        "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                        "bucket": "reads",
                        "key": "run-42/sample.fastq.gz",
                        "size_bytes": 10485760,
                        "path": "s3/01JGRP000123456789ABCDEFGH/1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978/reads/run-42/sample.fastq.gz",
                        "actor_user_id": "01JACTR00123456789ABCDEFGH"
                    }
                ],
                "next_cursor": "___-Yo1f2uwBAgMEBQYHCAkKCwwNDg8Q"
            })
        ),
        (status = 400, description = "Cursor is not unpadded base64url of exactly 24 bytes", body = ErrorResponse),
        (status = 401, description = "Missing, malformed or expired bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 502, description = "The inbox holder was reached but did not answer; the caller may retry", body = ErrorResponse),
        (status = 503, description = "No inbox holder is currently available, or this node has no realm network handle to reach it; the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_notifications(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<ListNotificationsQuery>,
) -> ServerResult<(StatusCode, Json<NotificationListResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .min(LIST_MAX_LIMIT);

    let (records, next_cursor) = list_for_user(
        &state.get_ctx(),
        state.get_node_id(),
        auth.user_id,
        cursor,
        limit,
    )
    .await
    .map_err(|error| map_dispatch_error(error, "list"))?;

    let notifications = records.iter().map(notification_response).collect();
    Ok((
        StatusCode::OK,
        Json(NotificationListResponse {
            notifications,
            next_cursor: encode_cursor(next_cursor),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/system/notifications/unread",
    tag = "system/notifications",
    summary = "Count the caller's unread notifications",
    description = r#"Returns the unread badge value for the calling user's inbox.

**Authentication**: realm bearer token; the count is self-scoped to the calling user and a
path-restricted token is refused.

**Behavior**
- The count is produced by the node holding the caller's inbox, locally or proxied to that holder.
- Resource-watch notifications the caller may no longer read are excluded.

**Limits**
- It is a badge value, not an exact total: counting stops at 100 unread or after 2000 scanned inbox
  rows, and `capped` is true in either case, meaning at least `count` unread."#,
    responses(
        (
            status = 200,
            description = "Unread badge value for the caller",
            body = UnreadCountResponse,
            example = json!({
                "count": 7,
                "capped": false
            })
        ),
        (status = 401, description = "Missing, malformed or expired bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 502, description = "The inbox holder was reached but did not answer; the caller may retry", body = ErrorResponse),
        (status = 503, description = "No inbox holder is currently available, or this node has no realm network handle to reach it; the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn unread_count(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<UnreadCountResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;

    let (count, capped) = unread_for_user(&state.get_ctx(), state.get_node_id(), auth.user_id)
        .await
        .map_err(|error| map_dispatch_error(error, "unread"))?;

    Ok((StatusCode::OK, Json(UnreadCountResponse { count, capped })))
}

/// Transport for the live unread-count stream. The local arm reacts to the
/// per-node wake bus; the remote arm degrades to polling the holder, keeping the
/// notification RPC surface unchanged.
enum UnreadStreamMode {
    Local(InboxWakeReceiver),
    Remote,
}

struct UnreadStreamState {
    context: Arc<DriverContext>,
    local_node_id: NodeId,
    recipient: UserId,
    mode: UnreadStreamMode,
    shutdown: CancellationToken,
    initial_done: bool,
    last_emitted: Option<u64>,
    remote_poll: Duration,
    local_recheck: Duration,
    next_holder_recheck: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamStep {
    /// Unconditional emit: a wake fired (or the wake bus lagged).
    Emit,
    /// Emit only if the count changed: remote poll tick or local recheck backstop.
    EmitOnChange,
    /// Local recheck tick: re-resolve the holder, maybe degrade, then emit on change.
    Recheck,
    Wait,
    End,
}

fn drain_pending_wakes(rx: &mut InboxWakeReceiver) {
    // Collapse any wakes buffered during the coalesce window into the single
    // refetch that follows; a closed/lagged channel simply stops the drain.
    while rx.try_recv().is_ok() {}
}

async fn next_local_step(
    rx: &mut InboxWakeReceiver,
    recipient: UserId,
    recheck_deadline: Instant,
) -> StreamStep {
    use tokio::sync::broadcast::error::RecvError;

    // Check elapsed deadlines before select so a ready wake bus cannot starve rechecks.
    if Instant::now() >= recheck_deadline {
        return StreamStep::Recheck;
    }

    tokio::select! {
        biased;
        // Once overdue, holder re-resolution must win over a continuously ready
        // node-wide wake bus carrying traffic for other recipients.
        _ = tokio::time::sleep_until(recheck_deadline) => StreamStep::Recheck,
        recv = rx.recv() => match recv {
            Ok(woken) if woken == recipient => {
                tokio::time::sleep(NOTIFICATION_STREAM_COALESCE).await;
                drain_pending_wakes(rx);
                StreamStep::Emit
            }
            Ok(_) => StreamStep::Wait,
            // Never tear the stream down for lag; refetch instead.
            Err(RecvError::Lagged(_)) => StreamStep::Emit,
            Err(RecvError::Closed) => StreamStep::End,
        },
    }
}

async fn fetch_unread_count(state: &UnreadStreamState) -> Option<(u64, bool)> {
    unread_for_user(state.context.as_ref(), state.local_node_id, state.recipient)
        .await
        .ok()
        .map(|(count, capped)| (count as u64, capped))
}

/// Yields the recipient's unread count initially and whenever it may have changed.
/// Ends on shutdown, wake-channel closure, or client disconnect. Transient fetch
/// failures are skipped so a blip never tears the stream down.
fn unread_count_stream(
    context: Arc<DriverContext>,
    local_node_id: NodeId,
    recipient: UserId,
    mode: UnreadStreamMode,
    shutdown: CancellationToken,
    remote_poll: Duration,
    local_recheck: Duration,
) -> impl Stream<Item = (u64, bool)> + Send {
    let state = UnreadStreamState {
        context,
        local_node_id,
        recipient,
        mode,
        shutdown,
        initial_done: false,
        last_emitted: None,
        remote_poll,
        local_recheck,
        next_holder_recheck: Instant::now() + local_recheck,
    };
    stream::unfold(state, |mut state| async move {
        loop {
            if state.shutdown.is_cancelled() {
                return None;
            }
            if !state.initial_done {
                state.initial_done = true;
                let fetched = state
                    .shutdown
                    .run_until_cancelled(fetch_unread_count(&state))
                    .await?;
                if let Some((count, capped)) = fetched {
                    state.last_emitted = Some(count);
                    return Some(((count, capped), state));
                }
            }

            let recipient = state.recipient;
            let remote_poll = state.remote_poll;
            let recheck_deadline = state.next_holder_recheck;
            let shutdown = state.shutdown.clone();
            // Ingress drain waits for this response, so shutdown must end each wait.
            let step = tokio::select! {
                biased;
                _ = shutdown.cancelled() => StreamStep::End,
                step = async {
                    match &mut state.mode {
                        UnreadStreamMode::Local(rx) => {
                            next_local_step(rx, recipient, recheck_deadline).await
                        }
                        UnreadStreamMode::Remote => {
                            tokio::time::sleep(remote_poll).await;
                            StreamStep::EmitOnChange
                        }
                    }
                } => step,
            };

            match step {
                StreamStep::End => return None,
                StreamStep::Wait => continue,
                StreamStep::Emit | StreamStep::EmitOnChange | StreamStep::Recheck => {
                    if matches!(step, StreamStep::Recheck) {
                        // Re-resolve after wake silence, switching to remote polling if needed.
                        // Resolution failure skips this cycle; refetch covers a missed wake.
                        state.next_holder_recheck = Instant::now() + state.local_recheck;
                        let resolved = state
                            .shutdown
                            .run_until_cancelled(resolve_user_holder(
                                state.context.as_ref(),
                                state.recipient,
                            ))
                            .await?;
                        match resolved {
                            Ok(holder) if holder != state.local_node_id => {
                                state.mode = UnreadStreamMode::Remote;
                            }
                            Ok(_) => {}
                            Err(_) => continue,
                        }
                    }
                    let fetched = state
                        .shutdown
                        .run_until_cancelled(fetch_unread_count(&state))
                        .await?;
                    let Some((count, capped)) = fetched else {
                        continue;
                    };
                    // Wakes emit unconditionally (the bell refetches the list); the
                    // poll and recheck backstop emit only when the count changed.
                    if !matches!(step, StreamStep::Emit) && state.last_emitted == Some(count) {
                        continue;
                    }
                    state.last_emitted = Some(count);
                    return Some(((count, capped), state));
                }
            }
        }
    })
}

struct NotificationStateStream<S> {
    unread: Pin<Box<S>>,
    epoch: String,
    revisions: watch::Receiver<u64>,
    cadence: tokio::time::Interval,
    current_unread: Option<UnreadCountResponse>,
    last_revision: Option<u64>,
    unread_open: bool,
    revisions_open: bool,
}

impl<S> NotificationStateStream<S> {
    fn state(&mut self) -> Option<NotificationStreamResponse> {
        let unread = self.current_unread.clone()?;
        let revision = *self.revisions.borrow_and_update();
        self.last_revision = Some(revision);
        Some(NotificationStreamResponse {
            epoch: self.epoch.clone(),
            revision,
            unread,
        })
    }
}

enum NotificationStateStep {
    Unread(Option<(u64, bool)>),
    Revision(Result<(), watch::error::RecvError>),
    Periodic,
}

fn notification_state_stream<S>(
    unread: S,
    epoch: String,
    revisions: watch::Receiver<u64>,
    cadence: Duration,
) -> impl Stream<Item = NotificationStreamResponse> + Send
where
    S: Stream<Item = (u64, bool)> + Send,
{
    let mut interval = tokio::time::interval_at(Instant::now() + cadence, cadence);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let state = NotificationStateStream {
        unread: Box::pin(unread),
        epoch,
        revisions,
        cadence: interval,
        current_unread: None,
        last_revision: None,
        unread_open: true,
        revisions_open: true,
    };
    stream::unfold(state, |mut state| async move {
        loop {
            if state.current_unread.is_none() {
                let (count, capped) = state.unread.next().await?;
                state.current_unread = Some(UnreadCountResponse {
                    count: count as u32,
                    capped,
                });
                let response = state.state()?;
                return Some((response, state));
            }

            let unread_open = state.unread_open;
            let revisions_open = state.revisions_open;
            let step = tokio::select! {
                unread = state.unread.next(), if unread_open => {
                    NotificationStateStep::Unread(unread)
                }
                revision = state.revisions.changed(), if revisions_open => {
                    NotificationStateStep::Revision(revision)
                }
                _ = state.cadence.tick() => NotificationStateStep::Periodic,
            };

            match step {
                NotificationStateStep::Unread(Some((count, capped))) => {
                    // Sources emit only wakes or changes, so repeated totals still trigger refetch.
                    state.current_unread = Some(UnreadCountResponse {
                        count: count as u32,
                        capped,
                    });
                }
                NotificationStateStep::Unread(None) => {
                    return None;
                }
                NotificationStateStep::Revision(Ok(())) => {
                    let revision = *state.revisions.borrow_and_update();
                    if state.last_revision == Some(revision) {
                        continue;
                    }
                }
                NotificationStateStep::Revision(Err(_)) => {
                    state.revisions_open = false;
                    continue;
                }
                NotificationStateStep::Periodic => {}
            }

            let response = state.state()?;
            return Some((response, state));
        }
    })
}

fn state_event(state: NotificationStreamResponse) -> Event {
    let data = serde_json::to_string(&state).unwrap_or_else(|_| {
        format!(
            "{{\"epoch\":\"{}\",\"revision\":{},\"unread\":{{\"count\":{},\"capped\":{}}}}}",
            state.epoch, state.revision, state.unread.count, state.unread.capped
        )
    });
    Event::default().event("state").data(data)
}

#[utoipa::path(
    get,
    path = "/system/notifications/stream",
    tag = "system/notifications",
    summary = "Stream the caller's notification state",
    description = r#"Streams a small state frame the client uses as a trigger to refetch the inbox.

**Authentication**: realm bearer token; the stream is self-scoped to the calling user and a
path-restricted token is refused.

**Behavior**
- The response is a `text/event-stream` that stays open until the client disconnects or the node
  shuts down; it carries no notification payloads.
- Every application frame is `event: state` followed by one `data:` line holding JSON
  `{"epoch": <string>, "revision": <u64>, "unread": {"count": <u32>, "capped": <bool>}}` and a
  blank line; frames carry no `id:` field, so `Last-Event-ID` is not honored.
- The current state is sent on connect, after a dashboard revision or unread-state change, and
  about every 20s without advancing the revision; that periodic frame doubles as the keep-alive,
  and an SSE comment line is sent instead whenever 20s pass with no frame, so proxies do not cut an
  idle stream.
- The epoch changes when the retained counter is reset, which tells a client its cached revision is
  meaningless.
- When this node holds the caller's inbox the stream reacts to local delivery wakes within about
  200ms of coalescing; otherwise it polls the holder every 5s and emits only on change. A local
  stream re-resolves the holder after 60s of silence and degrades to polling if the inbox moved, so
  a client never has to reconnect for that.
- Transient fetch failures are skipped rather than ending the stream.
- A client resumes by simply reconnecting: the first frame of the new stream is the full current
  state, and any state missed while disconnected is recovered by refetching the inbox, never
  replayed on the stream."#,
    responses(
        (status = 200, description = "Server-sent state stream, media type `text/event-stream`; it ends on client disconnect or node shutdown", body = NotificationStreamResponse, content_type = "text/event-stream"),
        (status = 401, description = "Missing, malformed or expired bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 503, description = "No inbox holder is available, this node has no realm network handle, or the dashboard change feed is not running; the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn stream_notifications(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Sse<impl Stream<Item = Result<Event, Infallible>> + Send>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let context = state.get_ctx();
    let local_node_id = state.get_node_id();
    let recipient = auth.user_id;
    let (dashboard_epoch, dashboard_revisions) =
        subscribe_dashboard_changes(context.as_ref()).ok_or(ServerError::ServiceUnavailable)?;

    let holder = resolve_user_holder(context.as_ref(), recipient)
        .await
        .map_err(|error| map_dispatch_error(error, "stream"))?;
    // Subscribe on both arms: the remote arm needs the net handle to poll the
    // holder, so a node without one answers 503 up front either way.
    let wake_rx = subscribe_inbox_wakes(context.as_ref())
        .map_err(|error| map_dispatch_error(error, "stream"))?;
    let mode = if holder == local_node_id {
        UnreadStreamMode::Local(wake_rx)
    } else {
        UnreadStreamMode::Remote
    };

    let unread = unread_count_stream(
        context.clone(),
        local_node_id,
        recipient,
        mode,
        state.shutdown_token(),
        STREAM_REMOTE_POLL,
        NOTIFICATION_STREAM_RECHECK,
    );
    let events = notification_state_stream(
        unread,
        dashboard_epoch,
        dashboard_revisions,
        STREAM_KEEP_ALIVE,
    )
    .map(state_event)
    .map(Ok::<_, Infallible>);
    Ok(Sse::new(events).keep_alive(KeepAlive::new().interval(STREAM_KEEP_ALIVE)))
}

#[utoipa::path(
    post,
    path = "/system/notifications/read",
    tag = "system/notifications",
    summary = "Mark the caller's notifications as read",
    description = r#"Marks the calling user's notifications as read, by id, by age, or both.

**Authentication**: realm bearer token; only the calling user's own inbox is affected and a
path-restricted token is refused.

**Behavior**
- The update is applied on the node holding the caller's inbox, locally or proxied to that holder.
- Both selectors may be combined: `ids` marks those notifications, `up_to_ms` marks every
  notification created at or before that epoch-millisecond timestamp.
- Unknown ids are ignored and already-read notifications are left untouched, so the returned count
  is the number of notifications this call actually flipped to read and repeating a call is safe.
- A body with no ids and no `up_to_ms` marks nothing and returns 0.

**Limits**
- At most 512 ids, and duplicates are collapsed."#,
    request_body(
        content = MarkReadRequest,
        description = "Notifications to mark read, by id, by age, or both; at most 512 ids, duplicates are collapsed",
        example = json!({
            "ids": ["01JABCDEF0123456789ABCDEFG"],
            "up_to_ms": 1775744591123_i64
        })
    ),
    responses(
        (
            status = 200,
            description = "Number of notifications flipped from unread to read by this call",
            body = MarkReadResponse,
            example = json!({
                "marked": 3
            })
        ),
        (status = 400, description = "An id is not a ULID, or more than 512 ids were supplied", body = ErrorResponse),
        (status = 401, description = "Missing, malformed or expired bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 502, description = "The inbox holder was reached but did not answer; the caller may retry", body = ErrorResponse),
        (status = 503, description = "No inbox holder is currently available, or this node has no realm network handle to reach it; the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn mark_read(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<MarkReadRequest>,
) -> ServerResult<(StatusCode, Json<MarkReadResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    if request.ids.len() > MARK_MAX_IDS {
        return Err(ServerError::BadRequest);
    }
    let ids = request
        .ids
        .iter()
        .map(|id| Ulid::from_str(id).map_err(|_| ServerError::BadRequest))
        .collect::<ServerResult<Vec<Ulid>>>()?;

    let marked = mark_for_user(
        &state.get_ctx(),
        state.get_node_id(),
        auth.user_id,
        ids,
        request.up_to_ms,
    )
    .await
    .map_err(|error| map_dispatch_error(error, "mark_read"))?;

    Ok((StatusCode::OK, Json(MarkReadResponse { marked })))
}

#[utoipa::path(
    get,
    path = "/system/notifications/watches",
    tag = "system/notifications",
    summary = "List the caller's notification watches",
    description = r#"Lists every watch subscription owned by the calling user.

**Authentication**: realm bearer token; watches are per-user, so this returns only the calling
user's own subscriptions and a path-restricted token is refused.

**Behavior**
- The list is read from the node holding the caller's inbox, locally or proxied to that holder.
- Entries are returned as stored, in watch-id order, with the canonical path prefix that was
  recorded at creation.
- Authorization is re-evaluated when an event is delivered, so a listed watch may stop producing
  notifications after a permission change without disappearing here.
- Such a watch is reported with `authorized` false and without its prefix, events or creation time,
  so its owner can still see it and delete it to release quota.

**Limits**
- The list is complete and unpaginated, because a user may hold at most 50 watches."#,
    responses(
        (
            status = 200,
            description = "Every watch subscription owned by the caller",
            body = WatchListResponse,
            example = json!({
                "watches": [
                    {
                        "id": "01JWATCH0123456789ABCDEFGH",
                        "path_prefix": "s3/01JGRP000123456789ABCDEFGH/1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978/reads/run-42/",
                        "events": ["data_uploaded"],
                        "created_at_ms": 1775744591123_i64,
                        "authorized": true
                    },
                    {
                        "id": "01JWATCH0123456789ABCDEFGJ",
                        "path_prefix": "",
                        "events": [],
                        "created_at_ms": 0,
                        "authorized": false
                    }
                ]
            })
        ),
        (status = 401, description = "Missing, malformed or expired bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 502, description = "The inbox holder was reached but did not answer; the caller may retry", body = ErrorResponse),
        (status = 503, description = "No inbox holder is currently available, or this node has no realm network handle to reach it; the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_watches(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<WatchListResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;

    let subscriptions = dispatch::list_watches(&state.get_ctx(), state.get_node_id(), auth.user_id)
        .await
        .map_err(|error| map_watch_error(error, "list_watches"))?;

    let watches = subscriptions.iter().map(watch_response).collect();
    Ok((StatusCode::OK, Json(WatchListResponse { watches })))
}

#[utoipa::path(
    post,
    path = "/system/notifications/watches",
    tag = "system/notifications",
    summary = "Create a notification watch for the caller",
    description = r#"Creates an authorized watch subscription over a canonical resource prefix.

**Authentication**: realm bearer token; a path-restricted token is refused. Data events require READ
on that exact node's blob bucket permission path and metadata events READ on the group's metadata
permission path, evaluated against the caller's current grants at creation time.

**Behavior**
- Data events use `s3/{group_id}/{node_id}/{bucket}/{key-prefix}`; for a bucket on this node the
  supplied group is canonicalized to the bucket-owning group and the response exposes that
  canonical prefix. Metadata events use `meta/{group_id}/{normalized_document_path-prefix}`.
- The subscription is stored on the node holding the caller's inbox, locally or proxied to that
  holder; 201 means it is durably recorded there, while replication to the other holders and
  publication of the watch interest are scheduled afterwards, so the first matching events may
  occur before delivery starts.
- Every delivery is re-authorized, so revoking the caller's READ silently stops notifications
  without deleting the watch.

**Limits**
- The prefix must be non-empty and at most 1024 bytes, must carry no leading slash, must keep the
  slash after the bucket or metadata group, and at least one event name must be given.
- A metadata prefix that ends right after the group, `meta/{group_id}/`, watches every dataset of
  that group.
- Metadata and data event kinds cannot be combined, because their canonical namespaces differ.
- A user may hold at most 50 watches."#,
    request_body(
        content = CreateWatchRequest,
        description = "Canonical resource prefix plus the event names to subscribe to; valid names are `metadata_created`, `data_uploaded`, `sync_completed` and `sync_failed`",
        example = json!({
            "path_prefix": "s3/01JGRP000123456789ABCDEFGH/1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978/reads/run-42/",
            "events": ["data_uploaded"]
        })
    ),
    responses(
        (
            status = 201,
            description = "Watch subscription created, echoing the canonical prefix that was stored",
            body = WatchResponse,
            example = json!({
                "id": "01JWATCH0123456789ABCDEFGH",
                "path_prefix": "s3/01JGRP000123456789ABCDEFGH/1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978/reads/run-42/",
                "events": ["data_uploaded"],
                "created_at_ms": 1775744591123_i64,
                "authorized": true
            })
        ),
        (status = 400, description = "Invalid or non-canonical path prefix, prefix longer than 1024 bytes, empty event list, or invalid event name", body = ErrorResponse),
        (status = 401, description = "Missing, malformed or expired bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, carries path restrictions, or the caller may not read the watched prefix; an unreadable and a missing resource are deliberately indistinguishable", body = ErrorResponse),
        (status = 409, description = "The caller already holds the maximum of 50 watches", body = ErrorResponse),
        (status = 502, description = "The inbox holder was reached but did not answer; the caller may retry", body = ErrorResponse),
        (status = 503, description = "No inbox holder is currently available, or this node has no realm network handle to reach it; the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_watch(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(_bearer_token): Extension<Option<ValidatedBearer>>,
    Json(request): Json<CreateWatchRequest>,
) -> ServerResult<(StatusCode, Json<WatchResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    if request.path_prefix.is_empty()
        || request.path_prefix.starts_with('/')
        || request.path_prefix.len() > MAX_PREFIX_LEN
        || request.events.is_empty()
    {
        record_watch_denial(&state, WatchMetricReason::InvalidResource);
        return Err(ServerError::BadRequest);
    }
    let mut event_mask = WatchEventMask::empty();
    for name in &request.events {
        let Some(kind) = WatchEventKind::from_name(name) else {
            record_watch_denial(&state, WatchMetricReason::InvalidResource);
            return Err(ServerError::BadRequest);
        };
        event_mask.insert(kind);
    }
    let path_prefix = canonicalize_watch_path(&state, request.path_prefix, event_mask).await?;
    authorize_watch(&state, &auth, &path_prefix, event_mask).await?;
    let authorization = WatchAuthorizationBinding {
        watch_path_prefix: path_prefix.clone(),
        ..Default::default()
    };

    let subscription = create_for_user(
        &state.get_ctx(),
        state.get_node_id(),
        auth.user_id,
        path_prefix,
        event_mask,
        authorization,
    )
    .await
    .map_err(|error| {
        if let WatchDispatchError::Unauthorized(reason) = &error {
            record_watch_denial(&state, *reason);
        }
        map_watch_error(error, "create_watch")
    })?;

    Ok((StatusCode::CREATED, Json(watch_response(&subscription))))
}

#[utoipa::path(
    delete,
    path = "/system/notifications/watches/{id}",
    tag = "system/notifications",
    summary = "Delete one of the caller's notification watches",
    description = r#"Deletes one watch subscription owned by the calling user.

**Authentication**: realm bearer token; the id is resolved inside the caller's own set, so another
user's watch can never be deleted and no further permission is checked. A path-restricted token is
refused.

**Behavior**
- The delete is applied on the node holding the caller's inbox, locally or proxied to that holder.
- It is idempotent: an id the caller does not own, including one already deleted, also answers 204.
- The 204 means the subscription is durably removed on that holder; removing the watch interest
  from the other holders is replicated afterwards, so a small number of already-matched events may
  still arrive."#,
    params(("id" = String, Path, description = "ULID of a watch subscription owned by the caller, as returned when the watch was created or listed")),
    responses(
        (status = 204, description = "Watch subscription deleted, or it did not exist; no response body"),
        (status = 400, description = "The id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing, malformed or expired bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 502, description = "The inbox holder was reached but did not answer; the caller may retry", body = ErrorResponse),
        (status = 503, description = "No inbox holder is currently available, or this node has no realm network handle to reach it; the caller may retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_watch(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let watch_id = Ulid::from_str(&id).map_err(|_| ServerError::BadRequest)?;

    delete_for_user(
        &state.get_ctx(),
        state.get_node_id(),
        auth.user_id,
        watch_id,
    )
    .await
    .map_err(|error| map_watch_error(error, "delete_watch"))?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
#[path = "notifications_tests.rs"]
mod tests;
