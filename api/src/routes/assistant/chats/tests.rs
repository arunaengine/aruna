use super::*;
use crate::tests::fixtures::routes::{test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::keys::generate_signing_key;
use aruna_core::structs::{
    Actor, MAX_ASSISTANT_CHAT_BYTES, MAX_ASSISTANT_CHAT_TURNS, MAX_ASSISTANT_CHATS,
    MAX_ASSISTANT_TURN_BYTES, NodeCapabilities, RealmId,
};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use axum::response::IntoResponse;
use tempfile::TempDir;
use ulid::Ulid;

async fn setup_state() -> (TempDir, Arc<ServerState>, AuthContext) {
    let (dir, storage) = test_storage();
    let context = Arc::new(test_context(storage));
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
        test_state(
            context,
            realm_id,
            node_id,
            NodeCapabilities::management_node(signing_key).unwrap(),
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
    let fill = 64 * 1024;
    assert!(fill <= MAX_ASSISTANT_TURN_BYTES);
    let payload = "x".repeat(fill);
    let fitting = u32::try_from(MAX_ASSISTANT_CHAT_BYTES / fill as u64).unwrap();
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
        (MAX_ASSISTANT_CHAT_TURNS as u64 - 1) * fill as u64 + 1
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
