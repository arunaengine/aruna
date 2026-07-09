use crate::auth::require_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{AuthContext, NotificationClass, NotificationKind, NotificationRecord};
use aruna_operations::notifications::dispatch::{
    NotificationDispatchError, list_notifications_for_user, mark_read_for_user,
    unread_count_for_user,
};
use aruna_operations::notifications::list::LIST_NOTIFICATIONS_MAX_LIMIT;
use aruna_operations::notifications::mark_read::MARK_READ_MAX_IDS;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

const DEFAULT_LIST_LIMIT: usize = 50;

#[derive(OpenApi)]
#[openapi(
    tags((name = "notifications", description = "User notification inbox")),
    paths(list_notifications, unread_count, mark_read)
)]
pub struct NotificationsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/notifications", get(list_notifications))
        .route("/notifications/unread", get(unread_count))
        .route("/notifications/read", post(mark_read))
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
    pub ids: Vec<String>,
    #[serde(default)]
    pub up_to_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MarkReadApiResponse {
    pub marked: u32,
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

/// Notification endpoints are identity-scoped with no path-based permission
/// check, so path-restricted (delegated) tokens must not reach them.
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
    }
    response
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::structs::{
        Actor, NodeCapabilities, PathRestriction, Permission, RealmConfigDocument, RealmId,
        RealmNodeKind,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::notifications::inbox::upsert_inbox_records;
    use aruna_storage::storage::FjallStorage;
    use byteview::ByteView;
    use tempfile::TempDir;

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

    async fn install_local_holder_config(state: &ServerState, realm_id: RealmId, holder: NodeId) {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
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
        let user_id = UserId::new(Ulid::new(), realm_id);
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

    #[tokio::test]
    async fn mark_read_rejects_bad_ids() {
        let realm_id = realm_id(2);
        let (_dir, state) = build_state(realm_id, node(2)).await;
        let user_id = UserId::new(Ulid::new(), realm_id);
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
        let user_id = UserId::new(Ulid::new(), realm_id);
        let error = mark_read(
            State(state),
            Extension(Some(auth_for(user_id, realm_id))),
            Json(MarkReadApiRequest {
                ids: (0..=MARK_READ_MAX_IDS)
                    .map(|_| Ulid::new().to_string())
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

        let user_id = UserId::new(Ulid::new(), realm_id);
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

    #[test]
    fn notification_response_maps_deep_link_ids() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let recipient = UserId::new(Ulid::new(), realm_id);
        let group_id = Ulid::new();
        let actor = UserId::new(Ulid::new(), realm_id);
        let member = UserId::new(Ulid::new(), realm_id);
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
    }
}
