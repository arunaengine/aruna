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
        (status = 200, description = "The caller's stored chats", body = AssistantChatsResponse),
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
    request_body = SaveAssistantChatsRequest,
    responses(
        (status = 200, description = "The stored chats after the save", body = AssistantChatsResponse),
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
