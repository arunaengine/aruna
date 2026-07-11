use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::{
    AuthContext, NOTIFICATION_WATCH_MAX_PREFIX_LEN, NotificationClass, NotificationKind,
    NotificationRecord, Permission, WatchEventKind, WatchEventMask, WatchSubscription,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::notifications::dispatch::{
    InboxWakeReceiver, NotificationDispatchError, WatchDispatchError, create_watch_for_user,
    delete_watch_for_user, list_notifications_for_user, list_watches_for_user, mark_read_for_user,
    resolve_inbox_holder_for_user, subscribe_inbox_wakes, unread_count_for_user,
};
use aruna_operations::notifications::list::LIST_NOTIFICATIONS_MAX_LIMIT;
use aruna_operations::notifications::mark_read::MARK_READ_MAX_IDS;
use aruna_operations::notifications::watch::authorization::watch_permission_path;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::warn;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

const DEFAULT_LIST_LIMIT: usize = 50;
/// Burst-coalescing window on the local wake arm: after a wake, wait briefly and
/// drain further wakes so a delivery storm collapses into one unread refetch.
const NOTIFICATION_STREAM_COALESCE: Duration = Duration::from_millis(200);
/// Remote-holder poll interval; emits only when the unread count changed.
const NOTIFICATION_STREAM_REMOTE_POLL: Duration = Duration::from_secs(5);
/// Keep-alive comment cadence so proxies do not cut an idle stream.
const NOTIFICATION_STREAM_KEEP_ALIVE: Duration = Duration::from_secs(20);
/// Coarse holder re-resolve cadence on the local wake arm. If the inbox holder
/// re-ranks to another node mid-stream, deliveries land there with no local wake,
/// so after this much wake silence the arm re-resolves the holder and degrades to
/// the remote poll when it has moved off this node.
const NOTIFICATION_STREAM_LOCAL_RECHECK: Duration = Duration::from_secs(60);

#[derive(OpenApi)]
#[openapi(
    tags((name = "notifications", description = "User notification inbox")),
    paths(
        list_notifications,
        unread_count,
        stream_notifications,
        mark_read,
        list_watches,
        create_watch,
        delete_watch
    )
)]
pub struct NotificationsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/notifications", get(list_notifications))
        .route("/notifications/unread", get(unread_count))
        .route("/notifications/stream", get(stream_notifications))
        .route("/notifications/read", post(mark_read))
        .route(
            "/notifications/watches",
            get(list_watches).post(create_watch),
        )
        .route("/notifications/watches/{id}", delete(delete_watch))
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct NotificationListResponse {
    pub notifications: Vec<NotificationResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UnreadCountApiResponse {
    pub count: u32,
    pub capped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MarkReadApiRequest {
    #[serde(default)]
    pub ids: Vec<String>,
    #[serde(default)]
    pub up_to_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MarkReadApiResponse {
    pub marked: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WatchResponse {
    pub id: String,
    pub path_prefix: String,
    pub events: Vec<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WatchListResponse {
    pub watches: Vec<WatchResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateWatchRequest {
    /// Canonical watched resource prefix. Data events use
    /// `s3/{group_id}/{node_id}/{bucket}/{key-prefix}` and metadata events use
    /// `meta/{group_id}/{normalized_document_path-prefix}`. The slash after the
    /// bucket or metadata group is required; no leading slash is allowed. Event
    /// kinds cannot be combined because they use distinct namespaces.
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

fn map_watch_dispatch_error(error: WatchDispatchError, operation: &str) -> ServerError {
    match error {
        WatchDispatchError::Unavailable => ServerError::ServiceUnavailable,
        WatchDispatchError::CapExceeded => {
            ServerError::Conflict("notification watch subscription cap reached".to_string())
        }
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
    }
}

/// Watch subscriptions cannot retain token-only path restrictions on their
/// holder, so path-restricted (delegated) tokens must not reach notification
/// endpoints even though create performs a resource permission check.
fn require_unrestricted_realm_auth(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = require_realm_auth(state, auth)?;
    if auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }
    Ok(auth)
}

fn decode_cursor(cursor: Option<&str>) -> ServerResult<Option<Vec<u8>>> {
    match cursor {
        Some(cursor) => {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ServerError::BadRequest)?;
            if bytes.len() != 24 {
                return Err(ServerError::BadRequest);
            }
            Ok(Some(bytes))
        }
        None => Ok(None),
    }
}

fn encode_cursor(cursor: Option<Vec<u8>>) -> Option<String> {
    cursor.map(|cursor| URL_SAFE_NO_PAD.encode(cursor))
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
        member_user_id: None,
        actor_user_id: None,
        node_id: None,
        realm_id: None,
        path: None,
        document_id: None,
        bucket: None,
        key: None,
        size_bytes: None,
    };
    match &record.kind {
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
    }
    response
}

async fn authorize_watch(
    state: &ServerState,
    auth: &AuthContext,
    path_prefix: &str,
    event_mask: WatchEventMask,
) -> ServerResult<()> {
    let permission_path = watch_permission_path(state.get_realm_id(), path_prefix, event_mask)
        .ok_or(ServerError::BadRequest)?;
    ensure_permission(state, auth, permission_path, Permission::READ).await
}

#[utoipa::path(
    get,
    path = "/notifications",
    tag = "notifications",
    params(
        ("limit" = Option<usize>, Query, description = "Max notifications to return (default 50, max 200)"),
        ("cursor" = Option<String>, Query, description = "Opaque pagination cursor from a previous page")
    ),
    responses(
        (status = 200, description = "Notifications page", body = NotificationListResponse),
        (status = 400, description = "Invalid cursor", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 502, description = "Holder proxy failed", body = ErrorResponse),
        (status = 503, description = "No inbox holder available", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_notifications(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<ListNotificationsQuery>,
) -> ServerResult<(StatusCode, Json<NotificationListResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .min(LIST_NOTIFICATIONS_MAX_LIMIT);

    let (records, next_cursor) = list_notifications_for_user(
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
    path = "/notifications/unread",
    tag = "notifications",
    responses(
        (status = 200, description = "Unread notification count", body = UnreadCountApiResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 502, description = "Holder proxy failed", body = ErrorResponse),
        (status = 503, description = "No inbox holder available", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn unread_count(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<UnreadCountApiResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;

    let (count, capped) =
        unread_count_for_user(&state.get_ctx(), state.get_node_id(), auth.user_id)
            .await
            .map_err(|error| map_dispatch_error(error, "unread"))?;

    Ok((
        StatusCode::OK,
        Json(UnreadCountApiResponse { count, capped }),
    ))
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

async fn next_local_stream_step(
    rx: &mut InboxWakeReceiver,
    recipient: UserId,
    recheck_deadline: Instant,
) -> StreamStep {
    use tokio::sync::broadcast::error::RecvError;

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
    unread_count_for_user(state.context.as_ref(), state.local_node_id, state.recipient)
        .await
        .ok()
        .map(|(count, capped)| (count as u64, capped))
}

/// Yields the recipient's unread count: once initially, then whenever it may have
/// changed. Ends only when the wake channel closes or the consumer drops the
/// stream (client disconnect); transient count-fetch failures are skipped so a
/// blip never tears the stream down.
fn unread_count_stream(
    context: Arc<DriverContext>,
    local_node_id: NodeId,
    recipient: UserId,
    mode: UnreadStreamMode,
    remote_poll: Duration,
    local_recheck: Duration,
) -> impl Stream<Item = (u64, bool)> + Send {
    let state = UnreadStreamState {
        context,
        local_node_id,
        recipient,
        mode,
        initial_done: false,
        last_emitted: None,
        remote_poll,
        local_recheck,
        next_holder_recheck: Instant::now() + local_recheck,
    };
    stream::unfold(state, |mut state| async move {
        loop {
            if !state.initial_done {
                state.initial_done = true;
                if let Some((count, capped)) = fetch_unread_count(&state).await {
                    state.last_emitted = Some(count);
                    return Some(((count, capped), state));
                }
            }

            let recipient = state.recipient;
            let remote_poll = state.remote_poll;
            let recheck_deadline = state.next_holder_recheck;
            let step = match &mut state.mode {
                UnreadStreamMode::Local(rx) => {
                    next_local_stream_step(rx, recipient, recheck_deadline).await
                }
                UnreadStreamMode::Remote => {
                    tokio::time::sleep(remote_poll).await;
                    StreamStep::EmitOnChange
                }
            };

            match step {
                StreamStep::End => return None,
                StreamStep::Wait => continue,
                StreamStep::Emit | StreamStep::EmitOnChange | StreamStep::Recheck => {
                    if matches!(step, StreamStep::Recheck) {
                        // Wake silence for a full recheck period: the inbox holder
                        // may have re-ranked to another node where deliveries land
                        // with no local wake. Re-resolve and degrade to the remote
                        // poll if it moved off this node; a resolution failure is a
                        // transient skip. Then refetch as a missed-wake backstop.
                        state.next_holder_recheck = Instant::now() + state.local_recheck;
                        match resolve_inbox_holder_for_user(state.context.as_ref(), state.recipient)
                            .await
                        {
                            Ok(holder) if holder != state.local_node_id => {
                                state.mode = UnreadStreamMode::Remote;
                            }
                            Ok(_) => {}
                            Err(_) => continue,
                        }
                    }
                    let Some((count, capped)) = fetch_unread_count(&state).await else {
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

fn unread_stream_event(count: u64, capped: bool) -> Event {
    let data = serde_json::to_string(&UnreadCountApiResponse {
        count: count as u32,
        capped,
    })
    .unwrap_or_else(|_| format!("{{\"count\":{count}}}"));
    Event::default().event("unread").data(data)
}

#[utoipa::path(
    get,
    path = "/notifications/stream",
    tag = "notifications",
    responses(
        (status = 200, description = "Server-sent event stream of the recipient's unread count. Emits `event: unread` frames whose JSON data is `{\"count\": <u64>}` on connect and again whenever the count may have changed; the bell refetches the list on each wake and the stream itself never carries notification bodies. Keep-alive comments are sent about every 20s and the stream ends on client disconnect.", content_type = "text/event-stream"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 503, description = "No inbox holder available", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn stream_notifications(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Sse<impl Stream<Item = Result<Event, Infallible>> + Send>> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let context = state.get_ctx();
    let local_node_id = state.get_node_id();
    let recipient = auth.user_id;

    let holder = resolve_inbox_holder_for_user(context.as_ref(), recipient)
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

    let events = unread_count_stream(
        context.clone(),
        local_node_id,
        recipient,
        mode,
        NOTIFICATION_STREAM_REMOTE_POLL,
        NOTIFICATION_STREAM_LOCAL_RECHECK,
    )
    .map(|(count, capped)| Ok::<_, Infallible>(unread_stream_event(count, capped)));
    Ok(Sse::new(events).keep_alive(KeepAlive::new().interval(NOTIFICATION_STREAM_KEEP_ALIVE)))
}

#[utoipa::path(
    post,
    path = "/notifications/read",
    tag = "notifications",
    request_body = MarkReadApiRequest,
    responses(
        (status = 200, description = "Notifications marked read", body = MarkReadApiResponse),
        (status = 400, description = "Invalid notification id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 502, description = "Holder proxy failed", body = ErrorResponse),
        (status = 503, description = "No inbox holder available", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn mark_read(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<MarkReadApiRequest>,
) -> ServerResult<(StatusCode, Json<MarkReadApiResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    if request.ids.len() > MARK_READ_MAX_IDS {
        return Err(ServerError::BadRequest);
    }
    let ids = request
        .ids
        .iter()
        .map(|id| Ulid::from_str(id).map_err(|_| ServerError::BadRequest))
        .collect::<ServerResult<Vec<Ulid>>>()?;

    let marked = mark_read_for_user(
        &state.get_ctx(),
        state.get_node_id(),
        auth.user_id,
        ids,
        request.up_to_ms,
    )
    .await
    .map_err(|error| map_dispatch_error(error, "mark_read"))?;

    Ok((StatusCode::OK, Json(MarkReadApiResponse { marked })))
}

#[utoipa::path(
    get,
    path = "/notifications/watches",
    tag = "notifications",
    responses(
        (status = 200, description = "Watch subscriptions", body = WatchListResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 502, description = "Holder proxy failed", body = ErrorResponse),
        (status = 503, description = "No inbox holder available", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_watches(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<WatchListResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;

    let subscriptions = list_watches_for_user(&state.get_ctx(), state.get_node_id(), auth.user_id)
        .await
        .map_err(|error| map_watch_dispatch_error(error, "list_watches"))?;

    let watches = subscriptions.iter().map(watch_response).collect();
    Ok((StatusCode::OK, Json(WatchListResponse { watches })))
}

#[utoipa::path(
    post,
    path = "/notifications/watches",
    tag = "notifications",
    description = "Create an authorized watch subscription using a canonical resource prefix. Data events use `s3/{group_id}/{node_id}/{bucket}/{key-prefix}` and require READ on that exact node's blob bucket permission path. Metadata events use `meta/{group_id}/{normalized_document_path-prefix}` and require READ on the group's metadata permission path. The slash after the bucket or metadata group is required, no leading slash is allowed, and metadata/data event kinds cannot be combined because their canonical namespaces differ.",
    request_body = CreateWatchRequest,
    responses(
        (status = 201, description = "Watch subscription created", body = WatchResponse),
        (status = 400, description = "Invalid or non-canonical path prefix, or invalid event name", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 409, description = "Per-user watch cap reached", body = ErrorResponse),
        (status = 502, description = "Holder proxy failed", body = ErrorResponse),
        (status = 503, description = "No inbox holder available", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_watch(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateWatchRequest>,
) -> ServerResult<(StatusCode, Json<WatchResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    if request.path_prefix.is_empty()
        || request.path_prefix.starts_with('/')
        || request.path_prefix.len() > NOTIFICATION_WATCH_MAX_PREFIX_LEN
        || request.events.is_empty()
    {
        return Err(ServerError::BadRequest);
    }
    let mut event_mask = WatchEventMask::empty();
    for name in &request.events {
        let kind = WatchEventKind::from_name(name).ok_or(ServerError::BadRequest)?;
        event_mask.insert(kind);
    }
    // A remote holder receives only the durable subscription, not this request's
    // AuthContext. Authorize the immutable canonical identity before dispatch.
    authorize_watch(&state, &auth, &request.path_prefix, event_mask).await?;

    let subscription = create_watch_for_user(
        &state.get_ctx(),
        state.get_node_id(),
        auth.user_id,
        request.path_prefix,
        event_mask,
    )
    .await
    .map_err(|error| map_watch_dispatch_error(error, "create_watch"))?;

    Ok((StatusCode::CREATED, Json(watch_response(&subscription))))
}

#[utoipa::path(
    delete,
    path = "/notifications/watches/{id}",
    tag = "notifications",
    params(("id" = String, Path, description = "Watch subscription id")),
    responses(
        (status = 204, description = "Watch subscription deleted"),
        (status = 400, description = "Invalid watch id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 502, description = "Holder proxy failed", body = ErrorResponse),
        (status = 503, description = "No inbox holder available", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_watch(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let watch_id = Ulid::from_str(&id).map_err(|_| ServerError::BadRequest)?;

    delete_watch_for_user(
        &state.get_ctx(),
        state.get_node_id(),
        auth.user_id,
        watch_id,
    )
    .await
    .map_err(|error| map_watch_dispatch_error(error, "delete_watch"))?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, NodeCapabilities, PathRestriction, Permission,
        RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
        data_watch_resource_path,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::notifications::inbox::upsert_inbox_records;
    use aruna_storage::storage::FjallStorage;
    use byteview::ByteView;
    use tempfile::TempDir;
    use tokio::time::timeout;

    fn node(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn realm_id(seed: u8) -> RealmId {
        RealmId::from_bytes(
            *ed25519_dalek::SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .as_bytes(),
        )
    }

    async fn build_state(realm_id: RealmId, node_id: NodeId) -> (TempDir, Arc<ServerState>) {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let state = ServerState::new(
            ctx,
            realm_id,
            node_id,
            NodeCapabilities::local_node(realm_id).expect("capabilities"),
            false,
            None,
        )
        .await;
        (dir, Arc::new(state))
    }

    async fn build_state_with_net(
        realm_id: RealmId,
        secret: [u8; 32],
    ) -> (TempDir, Arc<ServerState>, NetHandle) {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                secret_key: Some(iroh::SecretKey::from_bytes(&secret)),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle");
        let node_id = net.node_id();
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let state = ServerState::new(
            ctx,
            realm_id,
            node_id,
            NodeCapabilities::local_node(realm_id).expect("capabilities"),
            false,
            None,
        )
        .await;
        (dir, Arc::new(state), net)
    }

    async fn install_local_holder_config(state: &ServerState, realm_id: RealmId, holder: NodeId) {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        config.ensure_node(holder, RealmNodeKind::Server);
        let actor = Actor {
            node_id: holder,
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let bytes = config.to_bytes(&actor).expect("config serializes");
        match state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                value: ByteView::from(bytes),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected realm config write event: {other:?}"),
        }
    }

    async fn write_fixture(state: &ServerState, key_space: &str, key: ByteView, value: ByteView) {
        match state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected fixture write event: {other:?}"),
        }
    }

    async fn install_group_authorization(
        state: &ServerState,
        realm_id: RealmId,
        group_id: Ulid,
        owner: UserId,
        readers: &[UserId],
    ) {
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: owner,
            realm_id,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let mut group_auth =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
        let viewer = group_auth
            .roles
            .values_mut()
            .find(|role| role.name == "viewer")
            .expect("default viewer role");
        viewer.assigned_users.extend(readers.iter().copied());

        write_fixture(
            state,
            AUTH_KEYSPACE,
            ByteView::from(realm_id.as_bytes().to_vec()),
            ByteView::from(realm_auth.to_bytes(&actor).expect("realm auth serializes")),
        )
        .await;
        write_fixture(
            state,
            AUTH_KEYSPACE,
            ByteView::from(group_id.to_bytes().to_vec()),
            ByteView::from(group_auth.to_bytes(&actor).expect("group auth serializes")),
        )
        .await;
    }

    fn direct_record(recipient: UserId, seed: u8) -> NotificationRecord {
        NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::AddedToGroup {
                group_id: Ulid::from_bytes([seed; 16]),
                actor_user_id: UserId::new(Ulid::from_bytes([200u8; 16]), recipient.realm_id),
            },
            1_700_000_000_000 + seed as u64,
        )
    }

    fn auth_for(user_id: UserId, realm_id: RealmId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        }
    }

    #[tokio::test]
    async fn list_requires_auth() {
        let realm_id = realm_id(1);
        let (_dir, state) = build_state(realm_id, node(1)).await;
        let error = list_notifications(
            State(state),
            Extension(None),
            Query(ListNotificationsQuery::default()),
        )
        .await
        .expect_err("missing auth must be rejected");
        assert!(matches!(error, ServerError::Unauthorized));
    }

    #[tokio::test]
    async fn path_restricted_token_rejected() {
        let realm_id = realm_id(5);
        let (_dir, state) = build_state(realm_id, node(5)).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);
        let mut auth = auth_for(user_id, realm_id);
        auth.path_restrictions = Some(vec![PathRestriction {
            pattern: "/bucket/**".to_string(),
            permission: Permission::READ,
        }]);

        let list_err = list_notifications(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Query(ListNotificationsQuery::default()),
        )
        .await
        .expect_err("delegated token must be rejected");
        assert!(matches!(list_err, ServerError::Forbidden));

        let unread_err = unread_count(State(state.clone()), Extension(Some(auth.clone())))
            .await
            .expect_err("delegated token must be rejected");
        assert!(matches!(unread_err, ServerError::Forbidden));

        let mark_err = mark_read(
            State(state),
            Extension(Some(auth)),
            Json(MarkReadApiRequest {
                ids: Vec::new(),
                up_to_ms: None,
            }),
        )
        .await
        .expect_err("delegated token must be rejected");
        assert!(matches!(mark_err, ServerError::Forbidden));
    }

    #[test]
    fn cursor_decoding_rejects_garbage() {
        assert!(matches!(
            decode_cursor(Some("not base64 !!")),
            Err(ServerError::BadRequest)
        ));
        let short = URL_SAFE_NO_PAD.encode([0u8; 23]);
        assert!(matches!(
            decode_cursor(Some(&short)),
            Err(ServerError::BadRequest)
        ));
        assert_eq!(decode_cursor(None).unwrap(), None);

        let raw = vec![7u8; 24];
        let encoded = encode_cursor(Some(raw.clone()));
        assert_eq!(decode_cursor(encoded.as_deref()).unwrap(), Some(raw));
    }

    #[test]
    fn mark_read_request_defaults_missing_ids() {
        let request: MarkReadApiRequest =
            serde_json::from_str(r#"{"up_to_ms":123}"#).expect("request deserializes");

        assert!(request.ids.is_empty());
        assert_eq!(request.up_to_ms, Some(123));
    }

    #[tokio::test]
    async fn mark_read_rejects_bad_ids() {
        let realm_id = realm_id(2);
        let (_dir, state) = build_state(realm_id, node(2)).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);
        let error = mark_read(
            State(state),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(MarkReadApiRequest {
                ids: vec!["not-a-ulid".to_string()],
                up_to_ms: None,
            }),
        )
        .await
        .expect_err("bad id must be rejected");
        assert!(matches!(error, ServerError::BadRequest));
    }

    #[tokio::test]
    async fn mark_read_rejects_too_many_ids() {
        let realm_id = realm_id(6);
        let (_dir, state) = build_state(realm_id, node(6)).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);
        let error = mark_read(
            State(state),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(MarkReadApiRequest {
                ids: (0..=MARK_READ_MAX_IDS)
                    .map(|_| Ulid::r#gen().to_string())
                    .collect(),
                up_to_ms: None,
            }),
        )
        .await
        .expect_err("too many ids must be rejected");
        assert!(matches!(error, ServerError::BadRequest));
    }

    #[tokio::test]
    async fn local_path_serves_without_net() {
        let realm_id = realm_id(3);
        let holder = node(3);
        let (_dir, state) = build_state(realm_id, holder).await;
        install_local_holder_config(&state, realm_id, holder).await;

        let user_id = UserId::new(Ulid::r#gen(), realm_id);
        let records = vec![
            direct_record(user_id, 1),
            direct_record(user_id, 2),
            direct_record(user_id, 3),
        ];
        upsert_inbox_records(&state.get_ctx().storage_handle, &records)
            .await
            .expect("seed inbox");

        let (_, listed) = list_notifications(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
            Query(ListNotificationsQuery::default()),
        )
        .await
        .expect("list succeeds");
        assert_eq!(listed.notifications.len(), 3);
        assert_eq!(listed.notifications[0].created_at_ms, 1_700_000_000_000 + 3);
        assert!(listed.notifications.iter().all(|n| !n.read));

        let (_, unread) = unread_count(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
        )
        .await
        .expect("unread succeeds");
        assert_eq!(unread.count, 3);
        assert!(!unread.capped);

        let ids = listed
            .notifications
            .iter()
            .map(|n| n.id.clone())
            .collect::<Vec<_>>();
        let (_, marked) = mark_read(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(MarkReadApiRequest {
                ids,
                up_to_ms: None,
            }),
        )
        .await
        .expect("mark read succeeds");
        assert_eq!(marked.marked, 3);

        let (_, unread_after) =
            unread_count(State(state), Extension(Some(auth_for(user_id, realm_id))))
                .await
                .expect("unread after succeeds");
        assert_eq!(unread_after.count, 0);
    }

    #[tokio::test]
    async fn stream_local_arm_emits_initial_and_on_wake() {
        let realm_id = realm_id(11);
        let (_dir, state, net) = build_state_with_net(realm_id, [11u8; 32]).await;
        install_local_holder_config(&state, realm_id, state.get_node_id()).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);

        let mut events = Box::pin(unread_count_stream(
            state.get_ctx(),
            state.get_node_id(),
            user_id,
            UnreadStreamMode::Local(net.subscribe_notification_wakes()),
            Duration::from_secs(5),
            // Long recheck so this test exercises only the wake path.
            Duration::from_secs(60),
        ));

        let (initial, initial_capped) = timeout(Duration::from_secs(2), events.next())
            .await
            .expect("initial event arrives")
            .expect("stream open");
        assert_eq!(initial, 0);
        assert!(!initial_capped);

        upsert_inbox_records(
            &state.get_ctx().storage_handle,
            &[direct_record(user_id, 1)],
        )
        .await
        .expect("seed inbox");
        net.notify_inbox_activity(user_id);

        let (after_wake, after_wake_capped) = timeout(Duration::from_secs(2), events.next())
            .await
            .expect("wake event arrives")
            .expect("stream open");
        assert_eq!(after_wake, 1);
        assert!(!after_wake_capped);
    }

    // The recheck tick is the missed-wake backstop: with no wake fired, the local
    // arm still periodically re-resolves the holder and refetches, emitting when the
    // count changed. Driving a live holder re-rank (degrade to the remote arm) in a
    // single in-process node is impractical — that path needs a real second node
    // holding the inbox — so it is covered by the multi-node and remote-arm tests;
    // here we lock in the same-node backstop refetch.
    #[tokio::test]
    async fn stream_local_arm_recheck_refetches_on_change_without_wake() {
        let realm_id = realm_id(12);
        let (_dir, state, net) = build_state_with_net(realm_id, [12u8; 32]).await;
        install_local_holder_config(&state, realm_id, state.get_node_id()).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);

        let mut events = Box::pin(unread_count_stream(
            state.get_ctx(),
            state.get_node_id(),
            user_id,
            UnreadStreamMode::Local(net.subscribe_notification_wakes()),
            Duration::from_secs(60),
            // Short recheck so the backstop refetch fires promptly.
            Duration::from_millis(150),
        ));

        let (initial, _) = timeout(Duration::from_secs(2), events.next())
            .await
            .expect("initial event arrives")
            .expect("stream open");
        assert_eq!(initial, 0);

        // Seed a record but deliberately fire no wake: only the recheck backstop
        // can surface it.
        upsert_inbox_records(
            &state.get_ctx().storage_handle,
            &[direct_record(user_id, 1)],
        )
        .await
        .expect("seed inbox");

        let (after_recheck, _) = timeout(Duration::from_secs(2), events.next())
            .await
            .expect("recheck backstop emits without a wake")
            .expect("stream open");
        assert_eq!(after_recheck, 1);
    }

    #[tokio::test]
    async fn expired_local_recheck_wins_over_unrelated_wake() {
        let realm_id = realm_id(15);
        let recipient = UserId::new(Ulid::r#gen(), realm_id);
        let unrelated = UserId::new(Ulid::r#gen(), realm_id);
        let (tx, mut rx) = tokio::sync::broadcast::channel(4);
        tx.send(unrelated).expect("receiver is open");

        let step = next_local_stream_step(
            &mut rx,
            recipient,
            Instant::now() - Duration::from_millis(1),
        )
        .await;

        assert_eq!(step, StreamStep::Recheck);
    }

    // The remote arm polls the holder and emits only on change: the initial poll
    // reports the current count, a later poll reports it again once it moved.
    #[tokio::test]
    async fn stream_remote_arm_emits_initial_and_on_change() {
        let realm_id = realm_id(13);
        let (_dir, state, _net) = build_state_with_net(realm_id, [13u8; 32]).await;
        install_local_holder_config(&state, realm_id, state.get_node_id()).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);

        let mut events = Box::pin(unread_count_stream(
            state.get_ctx(),
            state.get_node_id(),
            user_id,
            UnreadStreamMode::Remote,
            Duration::from_millis(100),
            Duration::from_secs(60),
        ));

        let (initial, _) = timeout(Duration::from_secs(2), events.next())
            .await
            .expect("initial event arrives")
            .expect("stream open");
        assert_eq!(initial, 0);

        upsert_inbox_records(
            &state.get_ctx().storage_handle,
            &[direct_record(user_id, 1)],
        )
        .await
        .expect("seed inbox");

        let (changed, _) = timeout(Duration::from_secs(2), events.next())
            .await
            .expect("poll emits once the count changed")
            .expect("stream open");
        assert_eq!(changed, 1);
    }

    // A poll that cannot reach the holder is skipped silently: the stream neither
    // emits nor ends, it just keeps polling.
    #[tokio::test]
    async fn stream_remote_arm_skips_silently_on_poll_failure() {
        let realm_id = realm_id(14);
        let (_dir, state, _net) = build_state_with_net(realm_id, [14u8; 32]).await;
        // The holder is a node that is in no mesh, so every remote poll fails.
        install_local_holder_config(&state, realm_id, node(200)).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);

        let mut events = Box::pin(unread_count_stream(
            state.get_ctx(),
            state.get_node_id(),
            user_id,
            UnreadStreamMode::Remote,
            Duration::from_millis(100),
            Duration::from_secs(60),
        ));

        // Nothing is ever emitted and the stream stays open (times out) rather than
        // ending on the unreachable-holder errors.
        let outcome = timeout(Duration::from_millis(800), events.next()).await;
        assert!(
            outcome.is_err(),
            "a failing poll must be skipped silently, not emitted or ended: {outcome:?}"
        );
    }

    #[test]
    fn notification_response_maps_deep_link_ids() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let recipient = UserId::new(Ulid::r#gen(), realm_id);
        let group_id = Ulid::r#gen();
        let actor = UserId::new(Ulid::r#gen(), realm_id);
        let member = UserId::new(Ulid::r#gen(), realm_id);
        let onboarded_node = node(9);

        let added = notification_response(&NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::AddedToGroup {
                group_id,
                actor_user_id: actor,
            },
            1,
        ));
        assert_eq!(added.group_id, Some(group_id.to_string()));
        assert_eq!(added.actor_user_id, Some(actor.to_string()));
        assert_eq!(added.member_user_id, None);

        let removed = notification_response(&NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::RemovedFromGroup {
                group_id,
                actor_user_id: actor,
            },
            1,
        ));
        assert_eq!(removed.group_id, Some(group_id.to_string()));
        assert_eq!(removed.actor_user_id, Some(actor.to_string()));

        let member_added = notification_response(&NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::GroupMemberAdded {
                group_id,
                member_user_id: member,
                actor_user_id: actor,
            },
            1,
        ));
        assert_eq!(member_added.group_id, Some(group_id.to_string()));
        assert_eq!(member_added.member_user_id, Some(member.to_string()));
        assert_eq!(member_added.actor_user_id, Some(actor.to_string()));

        let onboarded = notification_response(&NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::NodeOnboarded {
                realm_id,
                node_id: onboarded_node,
            },
            1,
        ));
        assert_eq!(onboarded.realm_id, Some(realm_id.to_string()));
        assert_eq!(onboarded.node_id, Some(onboarded_node.to_string()));
        assert_eq!(onboarded.group_id, None);

        let document_id = Ulid::r#gen();
        let metadata_created = notification_response(&NotificationRecord::new(
            recipient,
            NotificationClass::Transient,
            NotificationKind::MetadataCreated {
                path: format!("meta/{group_id}/datasets/project/run-42"),
                group_id,
                document_id,
                actor_user_id: actor,
            },
            1,
        ));
        assert_eq!(metadata_created.kind, "metadata_created");
        assert_eq!(metadata_created.category, "resource.watch");
        assert_eq!(
            metadata_created.path,
            Some(format!("meta/{group_id}/datasets/project/run-42"))
        );
        assert_eq!(metadata_created.group_id, Some(group_id.to_string()));
        assert_eq!(metadata_created.document_id, Some(document_id.to_string()));
        assert_eq!(metadata_created.actor_user_id, Some(actor.to_string()));

        let data_uploaded = notification_response(&NotificationRecord::new(
            recipient,
            NotificationClass::Transient,
            NotificationKind::DataUploaded {
                path: data_watch_resource_path(group_id, onboarded_node, "bucket", "object"),
                group_id,
                node_id: onboarded_node,
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                size_bytes: 4096,
                actor_user_id: actor,
            },
            1,
        ));
        assert_eq!(data_uploaded.kind, "data_uploaded");
        assert_eq!(data_uploaded.category, "resource.watch");
        assert_eq!(
            data_uploaded.path,
            Some(data_watch_resource_path(
                group_id,
                onboarded_node,
                "bucket",
                "object"
            ))
        );
        assert_eq!(data_uploaded.group_id, Some(group_id.to_string()));
        assert_eq!(data_uploaded.node_id, Some(onboarded_node.to_string()));
        assert_eq!(data_uploaded.bucket, Some("bucket".to_string()));
        assert_eq!(data_uploaded.key, Some("object".to_string()));
        assert_eq!(data_uploaded.size_bytes, Some(4096));
        assert_eq!(data_uploaded.actor_user_id, Some(actor.to_string()));
    }

    #[tokio::test]
    async fn watch_local_path_crud() {
        let realm_id = realm_id(6);
        let holder = node(6);
        let (_dir, state) = build_state(realm_id, holder).await;
        install_local_holder_config(&state, realm_id, holder).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);
        let group_id = Ulid::r#gen();
        install_group_authorization(&state, realm_id, group_id, user_id, &[]).await;
        let path_prefix = data_watch_resource_path(group_id, node(60), "bucket", "prefix");

        let (status, created) = create_watch(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: path_prefix.clone(),
                events: vec!["data_uploaded".to_string()],
            }),
        )
        .await
        .expect("create succeeds");
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created.path_prefix, path_prefix);
        assert_eq!(created.events, vec!["data_uploaded"]);

        let (_, listed) = list_watches(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
        )
        .await
        .expect("list succeeds");
        assert_eq!(listed.watches.len(), 1);
        assert_eq!(listed.watches[0].id, created.id);

        let status = delete_watch(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
            Path(created.id.clone()),
        )
        .await
        .expect("delete succeeds");
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (_, empty) = list_watches(State(state), Extension(Some(auth_for(user_id, realm_id))))
            .await
            .expect("list after delete succeeds");
        assert!(empty.watches.is_empty());
    }

    #[tokio::test]
    async fn create_watch_requires_metadata_group_read_permission() {
        let realm_id = realm_id(15);
        let holder = node(15);
        let (_dir, state) = build_state(realm_id, holder).await;
        install_local_holder_config(&state, realm_id, holder).await;
        let authorized = UserId::new(Ulid::r#gen(), realm_id);
        let unauthorized = UserId::new(Ulid::r#gen(), realm_id);
        let group_id = Ulid::r#gen();
        install_group_authorization(&state, realm_id, group_id, authorized, &[]).await;
        let path_prefix = format!("meta/{group_id}/datasets/proteomics");

        let error = create_watch(
            State(state.clone()),
            Extension(Some(auth_for(unauthorized, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: path_prefix.clone(),
                events: vec!["metadata_created".to_string()],
            }),
        )
        .await
        .expect_err("user without metadata READ must be rejected");
        assert!(matches!(error, ServerError::Forbidden));

        let (status, created) = create_watch(
            State(state),
            Extension(Some(auth_for(authorized, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: path_prefix.clone(),
                events: vec!["metadata_created".to_string()],
            }),
        )
        .await
        .expect("metadata reader may create a watch");
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created.path_prefix, path_prefix);
    }

    #[tokio::test]
    async fn create_watch_uses_canonical_remote_node_bucket_permission() {
        let realm_id = realm_id(16);
        let holder = node(16);
        let (_dir, state) = build_state(realm_id, holder).await;
        install_local_holder_config(&state, realm_id, holder).await;
        let authorized = UserId::new(Ulid::r#gen(), realm_id);
        let unauthorized = UserId::new(Ulid::r#gen(), realm_id);
        let bucket_group_id = Ulid::r#gen();
        install_group_authorization(&state, realm_id, bucket_group_id, authorized, &[]).await;
        let remote_bucket_node = node(99);
        let path_prefix =
            data_watch_resource_path(bucket_group_id, remote_bucket_node, "reports", "quarterly");

        let error = create_watch(
            State(state.clone()),
            Extension(Some(auth_for(unauthorized, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: path_prefix.clone(),
                events: vec!["data_uploaded".to_string()],
            }),
        )
        .await
        .expect_err("user without bucket READ must be rejected");
        assert!(matches!(error, ServerError::Forbidden));

        let (status, created) = create_watch(
            State(state),
            Extension(Some(auth_for(authorized, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: path_prefix.clone(),
                events: vec!["data_uploaded".to_string()],
            }),
        )
        .await
        .expect("bucket reader may create a watch");
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created.path_prefix, path_prefix);
    }

    #[tokio::test]
    async fn create_watch_rejects_mixed_event_namespaces() {
        let realm_id = realm_id(17);
        let holder = node(17);
        let (_dir, state) = build_state(realm_id, holder).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);
        let metadata_group_id = Ulid::r#gen();
        let path_prefix = format!("meta/{metadata_group_id}/datasets/shared");
        let events = vec!["metadata_created".to_string(), "data_uploaded".to_string()];

        let error = create_watch(
            State(state),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(CreateWatchRequest {
                path_prefix,
                events,
            }),
        )
        .await
        .expect_err("one prefix cannot represent both canonical namespaces");
        assert!(matches!(error, ServerError::BadRequest));
    }

    #[tokio::test]
    async fn create_watch_validates_input() {
        let realm_id = realm_id(7);
        let (_dir, state) = build_state(realm_id, node(7)).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);

        let empty_prefix = create_watch(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: String::new(),
                events: vec!["metadata_created".to_string()],
            }),
        )
        .await
        .expect_err("empty prefix must be rejected");
        assert!(matches!(empty_prefix, ServerError::BadRequest));

        let leading_slash = create_watch(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: "/bucket".to_string(),
                events: vec!["metadata_created".to_string()],
            }),
        )
        .await
        .expect_err("leading-slash prefix must be rejected");
        assert!(matches!(leading_slash, ServerError::BadRequest));

        let empty_events = create_watch(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: "bucket".to_string(),
                events: Vec::new(),
            }),
        )
        .await
        .expect_err("empty events must be rejected");
        assert!(matches!(empty_events, ServerError::BadRequest));

        let unknown_event = create_watch(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: "bucket".to_string(),
                events: vec!["not_an_event".to_string()],
            }),
        )
        .await
        .expect_err("unknown event must be rejected");
        assert!(matches!(unknown_event, ServerError::BadRequest));

        let unscoped_data = create_watch(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: "reports".to_string(),
                events: vec!["data_uploaded".to_string()],
            }),
        )
        .await
        .expect_err("a prefix without a bucket boundary is ambiguous");
        assert!(matches!(unscoped_data, ServerError::BadRequest));

        let group_id = Ulid::r#gen();
        let unscoped_metadata = create_watch(
            State(state.clone()),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: format!("meta/{group_id}"),
                events: vec!["metadata_created".to_string()],
            }),
        )
        .await
        .expect_err("a metadata prefix without the group boundary is ambiguous");
        assert!(matches!(unscoped_metadata, ServerError::BadRequest));

        let noncanonical_metadata = create_watch(
            State(state),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(CreateWatchRequest {
                path_prefix: format!("meta/{group_id}/datasets/proteomics/"),
                events: vec!["metadata_created".to_string()],
            }),
        )
        .await
        .expect_err("metadata prefixes must use normalized document paths");
        assert!(matches!(noncanonical_metadata, ServerError::BadRequest));
    }

    #[tokio::test]
    async fn delete_watch_rejects_bad_id() {
        let realm_id = realm_id(8);
        let (_dir, state) = build_state(realm_id, node(8)).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);
        let error = delete_watch(
            State(state),
            Extension(Some(auth_for(user_id, realm_id))),
            Path("not-a-ulid".to_string()),
        )
        .await
        .expect_err("bad id must be rejected");
        assert!(matches!(error, ServerError::BadRequest));
    }

    #[tokio::test]
    async fn watch_endpoints_reject_path_restricted_token() {
        let realm_id = realm_id(9);
        let (_dir, state) = build_state(realm_id, node(9)).await;
        let user_id = UserId::new(Ulid::r#gen(), realm_id);
        let mut auth = auth_for(user_id, realm_id);
        auth.path_restrictions = Some(vec![PathRestriction {
            pattern: "/bucket/**".to_string(),
            permission: Permission::READ,
        }]);

        let list_err = list_watches(State(state.clone()), Extension(Some(auth.clone())))
            .await
            .expect_err("delegated token must be rejected");
        assert!(matches!(list_err, ServerError::Forbidden));

        let create_err = create_watch(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(CreateWatchRequest {
                path_prefix: "bucket".to_string(),
                events: vec!["metadata_created".to_string()],
            }),
        )
        .await
        .expect_err("delegated token must be rejected");
        assert!(matches!(create_err, ServerError::Forbidden));

        let delete_err = delete_watch(
            State(state),
            Extension(Some(auth)),
            Path(Ulid::r#gen().to_string()),
        )
        .await
        .expect_err("delegated token must be rejected");
        assert!(matches!(delete_err, ServerError::Forbidden));
    }
}
