use crate::auth::require_unrestricted_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::sessions::unix_rfc3339;
use crate::server_state::ServerState;
use aruna_core::structs::{AuthContext, MAX_ASSISTANT_CHAT_BYTES};
use aruna_core::util::unix_timestamp_secs;
use aruna_operations::assistant_chat::{ChatStoreError, ReadChatsOperation, WriteChatsOperation};
use aruna_operations::driver::drive;
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;

/// The caller's stored conversations. `payload` is absent until one is saved.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct AssistantChatsResponse {
    pub payload: Option<String>,
    /// 0 before the first save; pass it back with the next save.
    pub revision: u64,
    pub updated_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct SaveAssistantChatsRequest {
    pub payload: String,
    /// The revision the caller last read. Absent overwrites whatever is stored.
    pub revision: Option<u64>,
}

fn map_chat_error(error: ChatStoreError) -> ServerError {
    match error {
        ChatStoreError::Stale => {
            ServerError::Conflict("assistant chats changed in another browser".to_string())
        }
        ChatStoreError::TooLarge => ServerError::PayloadTooLarge(format!(
            "assistant chats exceed {MAX_ASSISTANT_CHAT_BYTES} bytes"
        )),
        error => ServerError::InternalError(error.to_string()),
    }
}

#[utoipa::path(
    get,
    path = "/access/users/me/assistant/chats",
    tag = "access/users",
    summary = "Read the caller's assistant chats",
    description = r#"Returns the assistant conversations this node holds for the calling user.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Chats are self-scoped, so a caller reaches only their own.

**Behavior**
- The payload is the portal's own chat state, stored as opaque text and returned unchanged.
- Chats live on the node that received them and are not replicated to the realm's other nodes.
- A user who never saved gets an absent payload and revision 0.
- `revision` counts accepted saves; pass it back to `PUT` so a save from a second browser cannot
  silently drop what this one holds."#,
    responses(
        (status = 200, description = "The caller's stored chats", body = AssistantChatsResponse,
            example = json!({
                "payload": "{\"chats\":[]}",
                "revision": 3,
                "updated_at": "2026-04-09T12:00:00Z"
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_chats(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<AssistantChatsResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let stored = drive(ReadChatsOperation::new(auth.user_id), &state.get_ctx())
        .await
        .map_err(map_chat_error)?;
    let response = stored.map_or(
        AssistantChatsResponse {
            payload: None,
            revision: 0,
            updated_at: None,
        },
        |chats| AssistantChatsResponse {
            payload: Some(chats.payload),
            revision: chats.revision,
            updated_at: Some(unix_rfc3339(chats.updated_at)),
        },
    );
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    put,
    path = "/access/users/me/assistant/chats",
    tag = "access/users",
    summary = "Save the caller's assistant chats",
    description = r#"Replaces the assistant conversations this node holds for the calling user.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Chats are self-scoped, so a caller writes only their own.

**Behavior**
- The payload is stored as opaque text; the node never reads inside it.
- Pass the `revision` last read. A save whose revision is not the stored one is refused with 409,
  so the caller can read, merge and save again.
- Leaving `revision` out overwrites whatever is stored.
- A payload above the node's limit is refused with 413."#,
    request_body(
        content = SaveAssistantChatsRequest,
        description = "The portal's chat state and the revision it was read at",
        example = json!({
            "payload": "{\"chats\":[]}",
            "revision": 3
        })
    ),
    responses(
        (status = 200, description = "The stored chats after the save", body = AssistantChatsResponse,
            example = json!({
                "payload": "{\"chats\":[]}",
                "revision": 4,
                "updated_at": "2026-04-09T12:00:00Z"
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 409, description = "The chats changed in another browser", body = ErrorResponse),
        (status = 413, description = "The payload is larger than the node accepts", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_chats(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<SaveAssistantChatsRequest>,
) -> ServerResult<(StatusCode, Json<AssistantChatsResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let stored = drive(
        WriteChatsOperation::new(
            auth.user_id,
            request.payload,
            request.revision,
            unix_timestamp_secs(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_chat_error)?;
    Ok((
        StatusCode::OK,
        Json(AssistantChatsResponse {
            payload: Some(stored.payload),
            revision: stored.revision,
            updated_at: Some(unix_rfc3339(stored.updated_at)),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::{Actor, NodeCapabilities, RealmId};
    use aruna_core::types::UserId;
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::storage::FjallStorage;
    use axum::response::IntoResponse;
    use tempfile::TempDir;
    use ulid::Ulid;

    async fn setup_state() -> (TempDir, Arc<ServerState>, AuthContext) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let node_id = iroh::SecretKey::generate().public();
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id,
                    user_id: UserId::nil(realm_id),
                    realm_id,
                },
                realm_description: "Realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &context,
        )
        .await
        .unwrap();
        let state = Arc::new(
            ServerState::new(
                context,
                realm_id,
                node_id,
                NodeCapabilities::management_node(signing_key).unwrap(),
                false,
                None,
                JobsRuntime::new(),
            )
            .await,
        );
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        (dir, state, auth)
    }

    fn save(payload: &str, revision: Option<u64>) -> Json<SaveAssistantChatsRequest> {
        Json(SaveAssistantChatsRequest {
            payload: payload.to_string(),
            revision,
        })
    }

    #[tokio::test]
    async fn reads_empty_first() {
        let (_dir, state, auth) = setup_state().await;
        let (status, Json(body)) = get_chats(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.payload, None);
        assert_eq!(body.revision, 0);
        assert_eq!(body.updated_at, None);
    }

    #[tokio::test]
    async fn saves_then_reads() {
        // Every accepted save bumps the revision the next read reports.
        let (_dir, state, auth) = setup_state().await;
        let (status, Json(saved)) = put_chats(
            State(state.clone()),
            Extension(Some(auth.clone())),
            save("{\"chats\":[]}", None),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(saved.revision, 1);
        assert!(saved.updated_at.is_some());

        let (_, Json(read)) = get_chats(State(state.clone()), Extension(Some(auth.clone())))
            .await
            .unwrap();
        assert_eq!(read.payload.as_deref(), Some("{\"chats\":[]}"));
        assert_eq!(read.revision, 1);

        let (_, Json(again)) = put_chats(State(state), Extension(Some(auth)), save("{}", Some(1)))
            .await
            .unwrap();
        assert_eq!(again.revision, 2);
        assert_eq!(again.payload.as_deref(), Some("{}"));
    }

    #[tokio::test]
    async fn rejects_stale_save() {
        let (_dir, state, auth) = setup_state().await;
        let (_, Json(first)) = put_chats(
            State(state.clone()),
            Extension(Some(auth.clone())),
            save("{}", None),
        )
        .await
        .unwrap();
        assert_eq!(first.revision, 1);
        let error = put_chats(State(state), Extension(Some(auth)), save("{}", Some(5)))
            .await
            .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn rejects_oversized_payload() {
        let (_dir, state, auth) = setup_state().await;
        let payload = "x".repeat(MAX_ASSISTANT_CHAT_BYTES + 1);
        let error = put_chats(State(state), Extension(Some(auth)), save(&payload, None))
            .await
            .unwrap_err();
        assert_eq!(
            error.into_response().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[tokio::test]
    async fn requires_unrestricted_token() {
        let (_dir, state, mut auth) = setup_state().await;
        let error = get_chats(State(state.clone()), Extension(None))
            .await
            .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::UNAUTHORIZED);

        auth.path_restrictions = Some(Vec::new());
        let error = put_chats(State(state), Extension(Some(auth)), save("{}", None))
            .await
            .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn maps_internal_faults() {
        let error = map_chat_error(ChatStoreError::NotFinished);
        assert_eq!(
            error.into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
