use super::{ResolveUsersRequest, resolve_users};
use crate::error::ServerError;
use crate::tests::users::{realm_auth, setup_state};
use aruna_core::UserId;
use aruna_core::structs::RealmId;
use axum::extract::State;
use axum::{Extension, Json};
use ulid::Ulid;

#[tokio::test]
async fn requires_auth() {
    let (state, _tempdir) = setup_state().await;
    let result = resolve_users(
        State(state),
        Extension(None),
        Json(ResolveUsersRequest { user_ids: vec![] }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn rejects_restricted_directory() {
    let (state, _tempdir) = setup_state().await;
    let realm_id = state.get_realm_id();
    let mut auth = realm_auth(realm_id);
    auth.path_restrictions = Some(Vec::new());
    let result = resolve_users(
        State(state),
        Extension(Some(auth)),
        Json(ResolveUsersRequest { user_ids: vec![] }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn rejects_foreign_realm() {
    let (state, _tempdir) = setup_state().await;
    let foreign = realm_auth(RealmId::from_bytes([7u8; 32]));
    let result = resolve_users(
        State(state),
        Extension(Some(foreign)),
        Json(ResolveUsersRequest { user_ids: vec![] }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn caps_batch_size() {
    let (state, _tempdir) = setup_state().await;
    let realm_id = state.get_realm_id();
    let user_ids = (0..=super::MAX_RESOLVE_USER_IDS)
        .map(|_| UserId::local(Ulid::generate(), realm_id).to_string())
        .collect();
    let result = resolve_users(
        State(state),
        Extension(Some(realm_auth(realm_id))),
        Json(ResolveUsersRequest { user_ids }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequest)));
}

#[tokio::test]
async fn rejects_bad_id() {
    let (state, _tempdir) = setup_state().await;
    let realm_id = state.get_realm_id();
    let result = resolve_users(
        State(state),
        Extension(Some(realm_auth(realm_id))),
        Json(ResolveUsersRequest {
            user_ids: vec!["not-a-user-id".to_string()],
        }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequest)));
}
