use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::sessions::unix_rfc3339;
use crate::server_state::ServerState;
use aruna_core::errors::StorageError;
use aruna_core::structs::{AssistantChatHead, AssistantChatTurn};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::assistant::{
    ChatStoreError, DeleteChatOperation, ListChatOperation, ReadChatOperation, WriteChatOperation,
    WriteTurnOperation,
};
use aruna_operations::driver::drive;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};

const MAX_ID_CHARS: usize = 64;
const MAX_TITLE_CHARS: usize = 80;
const MAX_SUBJECT_CHARS: usize = 200;

/// One chat of the caller without its turns.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ChatHeadResponse {
    pub id: String,
    pub title: String,
    pub subject: Option<String>,
    #[schema(example = "2026-04-09T12:00:00Z")]
    pub created_at: String,
    #[schema(example = "2026-04-09T12:00:00Z")]
    pub updated_at: String,
    /// Live turns have a seq from `first_seq` up to, not including, `next_seq`.
    pub first_seq: u32,
    /// The seq the next appended turn must use.
    pub next_seq: u32,
    /// Sum of the live turn payload lengths.
    pub bytes: u64,
    /// Bumped by every accepted head or turn write; pass it back with the next head or turn save.
    pub revision: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = ChatHeadListResponse)]
pub struct ChatListResponse {
    pub chats: Vec<ChatHeadResponse>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = SaveChatHeadRequest)]
pub struct SaveChatRequest {
    /// 1 to 80 characters after trimming.
    pub title: String,
    /// At most 200 characters.
    pub subject: Option<String>,
    /// The revision the caller last read. Absent overwrites whatever is stored.
    pub revision: Option<u64>,
}

/// One turn of a chat; the payload is the portal's own text.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ChatTurnResponse {
    pub seq: u32,
    pub payload: String,
    #[schema(example = "2026-04-09T12:00:00Z")]
    pub updated_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = ChatTurnListResponse)]
pub struct TurnListResponse {
    pub turns: Vec<ChatTurnResponse>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = SaveChatTurnRequest)]
pub struct SaveTurnRequest {
    pub payload: String,
    /// The head revision the caller last read. Absent skips the check.
    pub revision: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct ChatTurnsQuery {
    /// Return only the turns with a seq above this one.
    #[serde(default)]
    pub after: Option<u32>,
}

fn map_chat_error(error: ChatStoreError) -> ServerError {
    match error {
        ChatStoreError::NotFound => ServerError::NotFound,
        ChatStoreError::Deleted => ServerError::Gone("the chat was deleted".to_string()),
        ChatStoreError::Stale => {
            ServerError::Conflict("the chat changed in another browser".to_string())
        }
        ChatStoreError::StaleTurn { .. } => ServerError::Conflict(error.to_string()),
        ChatStoreError::Storage(StorageError::TransactionConflict) => {
            ServerError::Conflict("the chat is being written concurrently; retry".to_string())
        }
        ChatStoreError::TooLarge(cap) => ServerError::PayloadTooLarge(cap.to_string()),
        error => ServerError::InternalError(error.to_string()),
    }
}

fn check_chat_id(id: String) -> ServerResult<String> {
    let valid = !id.is_empty()
        && id.len() <= MAX_ID_CHARS
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    if valid {
        Ok(id)
    } else {
        Err(ServerError::BadRequestReason(
            "chat id must be 1 to 64 characters from A-Z, a-z, 0-9, _ and -".to_string(),
        ))
    }
}

fn check_title(title: &str) -> ServerResult<String> {
    let title = title.trim();
    let length = title.chars().count();
    if length == 0 || length > MAX_TITLE_CHARS {
        return Err(ServerError::BadRequestReason(
            "title must be 1 to 80 characters".to_string(),
        ));
    }
    Ok(title.to_string())
}

fn check_subject(subject: Option<String>) -> ServerResult<Option<String>> {
    if subject
        .as_ref()
        .is_some_and(|subject| subject.chars().count() > MAX_SUBJECT_CHARS)
    {
        return Err(ServerError::BadRequestReason(
            "subject must be at most 200 characters".to_string(),
        ));
    }
    Ok(subject)
}

fn head_response(head: AssistantChatHead) -> ChatHeadResponse {
    ChatHeadResponse {
        id: head.chat_id,
        title: head.title,
        subject: head.subject,
        created_at: unix_rfc3339(head.created_at),
        updated_at: unix_rfc3339(head.updated_at),
        first_seq: head.first_seq,
        next_seq: head.next_seq,
        bytes: head.bytes,
        revision: head.revision,
    }
}

fn turn_response(turn: AssistantChatTurn) -> ChatTurnResponse {
    ChatTurnResponse {
        seq: turn.seq,
        payload: turn.payload,
        updated_at: unix_rfc3339(turn.updated_at),
    }
}

#[utoipa::path(
    get,
    path = "/access/users/me/assistant/chats",
    tag = "access/users",
    summary = "List the caller's assistant chats",
    description = r#"Lists the assistant chats this node holds for the calling user, newest change first.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Chats are self-scoped, so a caller reaches only their own.

**Behavior**
- Only the chat heads are listed; the turns of a chat are read by their own route.
- A deleted chat is left out.
- Chats live on the node that received them and are not replicated to the realm's other nodes."#,
    responses(
        (status = 200, description = "The caller's live chats", body = ChatListResponse,
            example = json!({
                "chats": [{
                    "id": "c-01JCNCTR0123456789ABCDEF",
                    "title": "Sequencing run QC",
                    "subject": "Quality checks on the March run",
                    "created_at": "2026-04-09T12:00:00Z",
                    "updated_at": "2026-04-09T12:30:00Z",
                    "first_seq": 0,
                    "next_seq": 3,
                    "bytes": 12345,
                    "revision": 7
                }]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_chats(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<ChatListResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let chats = drive(ListChatOperation::new(auth.user_id), &state.get_ctx())
        .await
        .map_err(map_chat_error)?
        .into_iter()
        .map(head_response)
        .collect();
    Ok((StatusCode::OK, Json(ChatListResponse { chats })))
}

#[utoipa::path(
    put,
    path = "/access/users/me/assistant/chats/{id}",
    tag = "access/users",
    summary = "Save an assistant chat head",
    description = r#"Creates or renames one assistant chat of the calling user.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Chats are self-scoped, so a caller writes only their own.

**Behavior**
- An unknown `id` creates the chat with `first_seq` and `next_seq` at 0 and revision 1.
- A known chat takes the new `title` and `subject`. Pass the `revision` last read, so a save from
  a second browser cannot silently drop what this one holds; leaving it out overwrites.
- The returned head carries `next_seq`, the seq the next appended turn must use.

**Limits**
- `id` is 1 to 64 characters from `A-Z`, `a-z`, `0-9`, `_` and `-`.
- `title` is 1 to 80 characters after trimming; `subject` is at most 200 characters.
- A user keeps at most 20 live chats."#,
    params(("id" = String, Path, description = "Chat id chosen by the portal, 1 to 64 characters from A-Z, a-z, 0-9, _ and -")),
    request_body(
        content = SaveChatRequest,
        description = "The title and subject to store, and the revision they were read at",
        example = json!({
            "title": "Sequencing run QC",
            "subject": "Quality checks on the March run",
            "revision": 6
        })
    ),
    responses(
        (status = 200, description = "The chat head after the save", body = ChatHeadResponse,
            example = json!({
                "id": "c-01JCNCTR0123456789ABCDEF",
                "title": "Sequencing run QC",
                "subject": "Quality checks on the March run",
                "created_at": "2026-04-09T12:00:00Z",
                "updated_at": "2026-04-09T12:30:00Z",
                "first_seq": 0,
                "next_seq": 3,
                "bytes": 12345,
                "revision": 7
            })),
        (status = 400, description = "Invalid chat id, title or subject", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 409, description = "The chat changed in another browser", body = ErrorResponse),
        (status = 410, description = "The chat was deleted", body = ErrorResponse),
        (status = 413, description = "The user already keeps the most chats the node allows", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_chat(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
    Json(request): Json<SaveChatRequest>,
) -> ServerResult<(StatusCode, Json<ChatHeadResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let id = check_chat_id(id)?;
    let title = check_title(&request.title)?;
    let subject = check_subject(request.subject)?;
    let head = drive(
        WriteChatOperation::new(
            auth.user_id,
            id,
            title,
            subject,
            request.revision,
            unix_timestamp_secs(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_chat_error)?;
    Ok((StatusCode::OK, Json(head_response(head))))
}

#[utoipa::path(
    get,
    path = "/access/users/me/assistant/chats/{id}/turns",
    tag = "access/users",
    summary = "Read the turns of an assistant chat",
    description = r#"Returns the live turns of one assistant chat in seq order.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Chats are self-scoped, so a caller reaches only their own.

**Behavior**
- `after` skips every turn up to and including that seq, so a browser fetches only what it misses.
- A turn payload is the portal's own text, stored opaque and returned unchanged.
- Turns older than the chat keeps are gone; `first_seq` on the head names the oldest live one."#,
    params(
        ("id" = String, Path, description = "Chat id chosen by the portal, 1 to 64 characters from A-Z, a-z, 0-9, _ and -"),
        ChatTurnsQuery
    ),
    responses(
        (status = 200, description = "The live turns of the chat", body = TurnListResponse,
            example = json!({
                "turns": [{
                    "seq": 2,
                    "payload": "{\"messages\":[],\"history\":[]}",
                    "updated_at": "2026-04-09T12:30:00Z"
                }]
            })),
        (status = 400, description = "Invalid chat id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No such chat for this user", body = ErrorResponse),
        (status = 410, description = "The chat was deleted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_turns(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
    Query(query): Query<ChatTurnsQuery>,
) -> ServerResult<(StatusCode, Json<TurnListResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let id = check_chat_id(id)?;
    let turns = drive(
        ReadChatOperation::new(auth.user_id, id, query.after),
        &state.get_ctx(),
    )
    .await
    .map_err(map_chat_error)?
    .into_iter()
    .map(turn_response)
    .collect();
    Ok((StatusCode::OK, Json(TurnListResponse { turns })))
}

#[utoipa::path(
    put,
    path = "/access/users/me/assistant/chats/{id}/turns/{seq}",
    tag = "access/users",
    summary = "Save an assistant chat turn",
    description = r#"Appends one turn to an assistant chat or rewrites its last turn.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Chats are self-scoped, so a caller writes only their own.

**Behavior**
- A `seq` equal to the head's `next_seq` appends; a `seq` one below it rewrites the tail turn
  while it still streams. Any other `seq` is refused with 409, and the message names the
  current `next_seq`.
- A `seq` that is not a number is rejected by the path parser with a plain text 400 before the
  route runs, so that answer carries no JSON error body.
- Pass the head `revision` last read, so a write from an older read cannot replace a turn another
  browser appended in between; a differing revision is refused with 409 the same way.
- The payload is the portal's own text and is stored opaque.
- An append past the turns a chat keeps drops the oldest ones and advances `first_seq`.
- The head after the write is returned, so the caller learns `next_seq` and `revision`.

**Limits**
- A turn payload holds at most 256 KiB.
- A chat keeps its newest 120 turns.
- All chats of a user hold at most 8 MiB together; a write past that is refused with 413."#,
    params(
        ("id" = String, Path, description = "Chat id chosen by the portal, 1 to 64 characters from A-Z, a-z, 0-9, _ and -"),
        ("seq" = u32, Path, description = "The head's next_seq to append, or one below it to rewrite the tail turn")
    ),
    request_body(
        content = SaveTurnRequest,
        description = "The turn payload as the portal encodes it, and the head revision it was read at",
        example = json!({
            "payload": "{\"messages\":[],\"history\":[]}",
            "revision": 7
        })
    ),
    responses(
        (status = 200, description = "The chat head after the write", body = ChatHeadResponse,
            example = json!({
                "id": "c-01JCNCTR0123456789ABCDEF",
                "title": "Sequencing run QC",
                "subject": "Quality checks on the March run",
                "created_at": "2026-04-09T12:00:00Z",
                "updated_at": "2026-04-09T12:30:00Z",
                "first_seq": 0,
                "next_seq": 4,
                "bytes": 13456,
                "revision": 8
            })),
        (status = 400, description = "Invalid chat id or seq", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "No such chat for this user", body = ErrorResponse),
        (status = 409, description = "The seq is neither the next one nor the tail, or the revision is not the head's; the message names the current next_seq", body = ErrorResponse),
        (status = 410, description = "The chat was deleted", body = ErrorResponse),
        (status = 413, description = "The turn payload or the user's chats exceed what the node keeps", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_turn(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, seq)): Path<(String, u32)>,
    Json(request): Json<SaveTurnRequest>,
) -> ServerResult<(StatusCode, Json<ChatHeadResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let id = check_chat_id(id)?;
    let head = drive(
        WriteTurnOperation::new(
            auth.user_id,
            id,
            seq,
            request.payload,
            request.revision,
            unix_timestamp_secs(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_chat_error)?;
    Ok((StatusCode::OK, Json(head_response(head))))
}

#[utoipa::path(
    delete,
    path = "/access/users/me/assistant/chats/{id}",
    tag = "access/users",
    summary = "Delete an assistant chat",
    description = r#"Deletes one assistant chat of the calling user together with its turns.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. Chats are self-scoped, so a caller deletes only their own.

**Behavior**
- The turns are removed and the head stays as a tombstone, so the id is not reused and a later
  read or write of it answers 410.
- Deleting an unknown or already deleted chat answers 204 as well."#,
    params(("id" = String, Path, description = "Chat id chosen by the portal, 1 to 64 characters from A-Z, a-z, 0-9, _ and -")),
    responses(
        (status = 204, description = "The chat is deleted or was never there"),
        (status = 400, description = "Invalid chat id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_chat(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let id = check_chat_id(id)?;
    drive(
        DeleteChatOperation::new(auth.user_id, id, unix_timestamp_secs()),
        &state.get_ctx(),
    )
    .await
    .map_err(map_chat_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
#[path = "chats_tests.rs"]
mod tests;
