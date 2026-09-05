use crate::auth::require_unrestricted_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::sessions::unix_rfc3339;
use crate::server_state::ServerState;
use aruna_core::errors::StorageError;
use aruna_core::structs::{AssistantChatHead, AssistantChatTurn, AuthContext};
use aruna_core::util::unix_timestamp_secs;
use aruna_operations::assistant_chat::{
    ChatStoreError, DeleteChatOperation, ListChatHeadsOperation, ReadChatTurnsOperation,
    WriteChatHeadOperation, WriteChatTurnOperation,
};
use aruna_operations::driver::drive;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};

const MAX_CHAT_ID_CHARS: usize = 64;
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
    /// Bumped by every accepted head or turn write; pass it back with a head save.
    pub revision: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ChatHeadListResponse {
    pub chats: Vec<ChatHeadResponse>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct SaveChatHeadRequest {
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
pub struct ChatTurnListResponse {
    pub turns: Vec<ChatTurnResponse>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct SaveChatTurnRequest {
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
        && id.len() <= MAX_CHAT_ID_CHARS
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
        (status = 200, description = "The caller's live chats", body = ChatHeadListResponse,
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
) -> ServerResult<(StatusCode, Json<ChatHeadListResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let chats = drive(ListChatHeadsOperation::new(auth.user_id), &state.get_ctx())
        .await
        .map_err(map_chat_error)?
        .into_iter()
        .map(head_response)
        .collect();
    Ok((StatusCode::OK, Json(ChatHeadListResponse { chats })))
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
        content = SaveChatHeadRequest,
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
    Json(request): Json<SaveChatHeadRequest>,
) -> ServerResult<(StatusCode, Json<ChatHeadResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let id = check_chat_id(id)?;
    let title = check_title(&request.title)?;
    let subject = check_subject(request.subject)?;
    let head = drive(
        WriteChatHeadOperation::new(
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
        (status = 200, description = "The live turns of the chat", body = ChatTurnListResponse,
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
) -> ServerResult<(StatusCode, Json<ChatTurnListResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let id = check_chat_id(id)?;
    let turns = drive(
        ReadChatTurnsOperation::new(auth.user_id, id, query.after),
        &state.get_ctx(),
    )
    .await
    .map_err(map_chat_error)?
    .into_iter()
    .map(turn_response)
    .collect();
    Ok((StatusCode::OK, Json(ChatTurnListResponse { turns })))
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
- Pass the head `revision` last read, so a write from an older read cannot replace a turn another
  browser appended in between; a differing revision is refused with 409 the same way.
- The payload is the portal's own text and is stored opaque.
- An append past the turns a chat keeps drops the oldest ones and advances `first_seq`.
- The head after the write is returned, so the caller learns `next_seq` and `revision`.

**Limits**
- A turn payload holds at most 64 KiB.
- A chat keeps its newest 120 turns.
- All chats of a user hold at most 8 MiB together; a write past that is refused with 413."#,
    params(
        ("id" = String, Path, description = "Chat id chosen by the portal, 1 to 64 characters from A-Z, a-z, 0-9, _ and -"),
        ("seq" = u32, Path, description = "The head's next_seq to append, or one below it to rewrite the tail turn")
    ),
    request_body(
        content = SaveChatTurnRequest,
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
    Json(request): Json<SaveChatTurnRequest>,
) -> ServerResult<(StatusCode, Json<ChatHeadResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let id = check_chat_id(id)?;
    let head = drive(
        WriteChatTurnOperation::new(
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
    let auth = require_unrestricted_realm_auth(&state, auth)?;
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
mod tests {
    use super::*;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::{
        Actor, MAX_ASSISTANT_CHAT_BYTES, MAX_ASSISTANT_CHAT_TURNS, MAX_ASSISTANT_CHATS,
        MAX_ASSISTANT_TURN_BYTES, NodeCapabilities, RealmId,
    };
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

    fn head_request(title: &str, revision: Option<u64>) -> Json<SaveChatHeadRequest> {
        Json(SaveChatHeadRequest {
            title: title.to_string(),
            subject: None,
            revision,
        })
    }

    fn turn_request(payload: &str, revision: Option<u64>) -> Json<SaveChatTurnRequest> {
        Json(SaveChatTurnRequest {
            payload: payload.to_string(),
            revision,
        })
    }

    async fn save_head(
        state: &Arc<ServerState>,
        auth: &AuthContext,
        id: &str,
        title: &str,
        revision: Option<u64>,
    ) -> Result<ChatHeadResponse, StatusCode> {
        put_chat(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Path(id.to_string()),
            head_request(title, revision),
        )
        .await
        .map(|(_, Json(head))| head)
        .map_err(|error| error.into_response().status())
    }

    async fn save_turn(
        state: &Arc<ServerState>,
        auth: &AuthContext,
        id: &str,
        seq: u32,
        payload: &str,
    ) -> Result<ChatHeadResponse, ServerError> {
        put_turn(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Path((id.to_string(), seq)),
            turn_request(payload, None),
        )
        .await
        .map(|(_, Json(head))| head)
    }

    async fn read_turns(
        state: &Arc<ServerState>,
        auth: &AuthContext,
        id: &str,
        after: Option<u32>,
    ) -> Result<Vec<ChatTurnResponse>, StatusCode> {
        get_turns(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Path(id.to_string()),
            Query(ChatTurnsQuery { after }),
        )
        .await
        .map(|(_, Json(body))| body.turns)
        .map_err(|error| error.into_response().status())
    }

    async fn list(state: &Arc<ServerState>, auth: &AuthContext) -> Vec<ChatHeadResponse> {
        let (status, Json(body)) = list_chats(State(state.clone()), Extension(Some(auth.clone())))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        body.chats
    }

    async fn delete(state: &Arc<ServerState>, auth: &AuthContext, id: &str) -> StatusCode {
        delete_chat(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Path(id.to_string()),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn creates_and_renames() {
        // A rename must carry the revision it read; the head keeps its creation time.
        let (_dir, state, auth) = setup_state().await;
        assert!(list(&state, &auth).await.is_empty());

        let created = save_head(&state, &auth, "c-1", "  First  ", None)
            .await
            .unwrap();
        assert_eq!(created.id, "c-1");
        assert_eq!(created.title, "First");
        assert_eq!(
            (
                created.revision,
                created.first_seq,
                created.next_seq,
                created.bytes
            ),
            (1, 0, 0, 0)
        );

        let renamed = save_head(&state, &auth, "c-1", "Second", Some(1))
            .await
            .unwrap();
        assert_eq!(renamed.title, "Second");
        assert_eq!(renamed.revision, 2);
        assert_eq!(renamed.created_at, created.created_at);

        assert_eq!(
            save_head(&state, &auth, "c-1", "Third", Some(1))
                .await
                .unwrap_err(),
            StatusCode::CONFLICT
        );
        let listed = list(&state, &auth).await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].title, "Second");
    }

    #[tokio::test]
    async fn rejects_bad_input() {
        let (_dir, state, auth) = setup_state().await;
        for id in ["", "has space", "a/b", &"x".repeat(65)] {
            assert_eq!(
                save_head(&state, &auth, id, "Title", None)
                    .await
                    .unwrap_err(),
                StatusCode::BAD_REQUEST
            );
        }
        for title in ["", "   ", &"t".repeat(81)] {
            assert_eq!(
                save_head(&state, &auth, "c-1", title, None)
                    .await
                    .unwrap_err(),
                StatusCode::BAD_REQUEST
            );
        }
        let error = put_chat(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Path("c-1".to_string()),
            Json(SaveChatHeadRequest {
                title: "Title".to_string(),
                subject: Some("s".repeat(201)),
                revision: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            read_turns(&state, &auth, "bad id", None).await.unwrap_err(),
            StatusCode::BAD_REQUEST
        );
        let error = delete_chat(State(state), Extension(Some(auth)), Path(String::new()))
            .await
            .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn appends_and_reads() {
        // Appends advance next_seq; `after` returns only the newer turns; the tail can be rewritten.
        let (_dir, state, auth) = setup_state().await;
        save_head(&state, &auth, "c-1", "Chat", None).await.unwrap();

        let head = save_turn(&state, &auth, "c-1", 0, "turn zero")
            .await
            .unwrap();
        assert_eq!((head.next_seq, head.bytes, head.revision), (1, 9, 2));
        let head = save_turn(&state, &auth, "c-1", 1, "turn one")
            .await
            .unwrap();
        assert_eq!((head.next_seq, head.bytes, head.revision), (2, 17, 3));

        let turns = read_turns(&state, &auth, "c-1", None).await.unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!((turns[0].seq, turns[0].payload.as_str()), (0, "turn zero"));
        assert_eq!((turns[1].seq, turns[1].payload.as_str()), (1, "turn one"));

        let turns = read_turns(&state, &auth, "c-1", Some(0)).await.unwrap();
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].seq, 1);
        assert!(
            read_turns(&state, &auth, "c-1", Some(1))
                .await
                .unwrap()
                .is_empty()
        );

        let head = save_turn(&state, &auth, "c-1", 1, "turn one, longer")
            .await
            .unwrap();
        assert_eq!((head.next_seq, head.bytes, head.revision), (2, 25, 4));
        let turns = read_turns(&state, &auth, "c-1", Some(0)).await.unwrap();
        assert_eq!(turns[0].payload, "turn one, longer");

        let listed = list(&state, &auth).await;
        assert_eq!((listed[0].next_seq, listed[0].bytes), (2, 25));
    }

    #[tokio::test]
    async fn refuses_wrong_seq() {
        // The 409 message tells the caller which seq the node expects next.
        let (_dir, state, auth) = setup_state().await;
        save_head(&state, &auth, "c-1", "Chat", None).await.unwrap();
        save_turn(&state, &auth, "c-1", 0, "zero").await.unwrap();

        let error = save_turn(&state, &auth, "c-1", 3, "three")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("1"), "{error}");
        assert_eq!(error.into_response().status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn refuses_stale_revision() {
        // A turn write from an older head read is refused, whether it appends or rewrites the tail.
        let (_dir, state, auth) = setup_state().await;
        save_head(&state, &auth, "c-1", "Chat", None).await.unwrap();
        let write = |seq: u32, revision: Option<u64>| {
            let state = state.clone();
            let auth = auth.clone();
            async move {
                put_turn(
                    State(state),
                    Extension(Some(auth)),
                    Path(("c-1".to_string(), seq)),
                    turn_request("turn", revision),
                )
                .await
                .map(|(_, Json(head))| head)
                .map_err(|error| error.into_response().status())
            }
        };
        let head = write(0, Some(1)).await.unwrap();
        assert_eq!(head.revision, 2);
        assert_eq!(write(1, Some(1)).await.unwrap_err(), StatusCode::CONFLICT);
        assert_eq!(write(0, Some(1)).await.unwrap_err(), StatusCode::CONFLICT);
        let head = write(0, Some(2)).await.unwrap();
        assert_eq!((head.revision, head.next_seq), (3, 1));
        assert_eq!(write(1, None).await.unwrap().next_seq, 2);
    }

    #[tokio::test]
    async fn handles_missing_chats() {
        // Unknown is 404; after a delete every route answers 410 and the listing drops the chat.
        let (_dir, state, auth) = setup_state().await;
        assert_eq!(
            read_turns(&state, &auth, "c-1", None).await.unwrap_err(),
            StatusCode::NOT_FOUND
        );
        let error = save_turn(&state, &auth, "c-1", 0, "zero")
            .await
            .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::NOT_FOUND);

        save_head(&state, &auth, "c-1", "Chat", None).await.unwrap();
        save_turn(&state, &auth, "c-1", 0, "zero").await.unwrap();
        assert_eq!(delete(&state, &auth, "c-1").await, StatusCode::NO_CONTENT);
        assert_eq!(delete(&state, &auth, "c-1").await, StatusCode::NO_CONTENT);
        assert_eq!(delete(&state, &auth, "never").await, StatusCode::NO_CONTENT);

        assert!(list(&state, &auth).await.is_empty());
        assert_eq!(
            read_turns(&state, &auth, "c-1", None).await.unwrap_err(),
            StatusCode::GONE
        );
        assert_eq!(
            save_head(&state, &auth, "c-1", "Again", None)
                .await
                .unwrap_err(),
            StatusCode::GONE
        );
        let error = save_turn(&state, &auth, "c-1", 1, "one").await.unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::GONE);
    }

    #[tokio::test]
    async fn refuses_large_turn() {
        let (_dir, state, auth) = setup_state().await;
        save_head(&state, &auth, "c-1", "Chat", None).await.unwrap();
        let payload = "x".repeat(MAX_ASSISTANT_TURN_BYTES + 1);
        let error = save_turn(&state, &auth, "c-1", 0, &payload)
            .await
            .unwrap_err();
        assert_eq!(
            error.into_response().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[tokio::test]
    async fn refuses_chat_cap() {
        // A deleted chat frees its slot.
        let (_dir, state, auth) = setup_state().await;
        for index in 0..MAX_ASSISTANT_CHATS {
            save_head(&state, &auth, &format!("c-{index}"), "Chat", None)
                .await
                .unwrap();
        }
        assert_eq!(
            save_head(&state, &auth, "c-more", "Chat", None)
                .await
                .unwrap_err(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(delete(&state, &auth, "c-0").await, StatusCode::NO_CONTENT);
        assert!(
            save_head(&state, &auth, "c-more", "Chat", None)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn refuses_over_budget() {
        // 64 KiB turns across two chats fill the 8 MiB budget exactly; one more is refused.
        let (_dir, state, auth) = setup_state().await;
        let payload = "x".repeat(MAX_ASSISTANT_TURN_BYTES);
        let fitting =
            u32::try_from(MAX_ASSISTANT_CHAT_BYTES / MAX_ASSISTANT_TURN_BYTES as u64).unwrap();
        assert!(fitting > MAX_ASSISTANT_CHAT_TURNS);
        save_head(&state, &auth, "c-1", "Chat", None).await.unwrap();
        save_head(&state, &auth, "c-2", "Chat", None).await.unwrap();
        for seq in 0..MAX_ASSISTANT_CHAT_TURNS {
            save_turn(&state, &auth, "c-1", seq, &payload)
                .await
                .unwrap();
        }
        for seq in 0..fitting - MAX_ASSISTANT_CHAT_TURNS {
            save_turn(&state, &auth, "c-2", seq, &payload)
                .await
                .unwrap();
        }
        let error = save_turn(
            &state,
            &auth,
            "c-2",
            fitting - MAX_ASSISTANT_CHAT_TURNS,
            "y",
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.into_response().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );

        // A full chat trims its oldest turn on the next append and stays within budget.
        let head = save_turn(&state, &auth, "c-1", MAX_ASSISTANT_CHAT_TURNS, "y")
            .await
            .unwrap();
        assert_eq!(
            (head.first_seq, head.next_seq),
            (1, MAX_ASSISTANT_CHAT_TURNS + 1)
        );
        assert_eq!(
            head.bytes,
            (MAX_ASSISTANT_CHAT_TURNS as u64 - 1) * MAX_ASSISTANT_TURN_BYTES as u64 + 1
        );
        let turns = read_turns(&state, &auth, "c-1", None).await.unwrap();
        assert_eq!(turns.len() as u32, MAX_ASSISTANT_CHAT_TURNS);
        assert_eq!(turns[0].seq, 1);
    }

    #[tokio::test]
    async fn requires_unrestricted_token() {
        let (_dir, state, mut auth) = setup_state().await;
        let error = list_chats(State(state.clone()), Extension(None))
            .await
            .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::UNAUTHORIZED);

        auth.path_restrictions = Some(Vec::new());
        assert_eq!(
            save_head(&state, &auth, "c-1", "Chat", None)
                .await
                .unwrap_err(),
            StatusCode::FORBIDDEN
        );
        let error = save_turn(&state, &auth, "c-1", 0, "zero")
            .await
            .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
        assert_eq!(
            read_turns(&state, &auth, "c-1", None).await.unwrap_err(),
            StatusCode::FORBIDDEN
        );
        let error = delete_chat(State(state), Extension(Some(auth)), Path("c-1".to_string()))
            .await
            .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn maps_store_errors() {
        assert_eq!(
            map_chat_error(ChatStoreError::NotFinished)
                .into_response()
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            map_chat_error(ChatStoreError::Storage(StorageError::TransactionConflict))
                .into_response()
                .status(),
            StatusCode::CONFLICT
        );
        let stale = map_chat_error(ChatStoreError::StaleTurn { next_seq: 7 });
        assert!(stale.to_string().contains('7'));
        assert_eq!(stale.into_response().status(), StatusCode::CONFLICT);
        assert_eq!(
            map_chat_error(ChatStoreError::Deleted)
                .into_response()
                .status(),
            StatusCode::GONE
        );
    }
}
