//! Tests that the user resolve route refuses anonymous, restricted, and foreign callers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{ResolveUsersRequest, resolve_users};
use crate::auth::RealmAuth;
use crate::auth::extract;
use crate::error::ServerError;
use crate::tests::users::{realm_auth, setup_state};
use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use ulid::Ulid;

#[tokio::test]
async fn requires_auth() {
    let (state, _tempdir) = setup_state().await;
    // `resolve_users` takes `RealmAuth`, so the refusal is the extractor's.
    let result = extract::<RealmAuth>(&state, None).await;
    assert_eq!(result.err(), Some(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn rejects_restricted_directory() {
    let (state, _tempdir) = setup_state().await;
    let realm_id = state.get_realm_id();
    let mut auth = realm_auth(realm_id);
    auth.path_restrictions = Some(Vec::new());
    let result = resolve_users(
        State(state),
        RealmAuth(auth),
        Json(ResolveUsersRequest { user_ids: vec![] }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn rejects_foreign_realm() {
    let (state, _tempdir) = setup_state().await;
    let foreign = realm_auth(RealmId::from_bytes([7u8; 32]));
    // `resolve_users` takes `RealmAuth`, so the refusal is its check.
    let result = crate::auth::require_realm_auth(&state, Some(foreign));
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn caps_batch_size() {
    let (state, _tempdir) = setup_state().await;
    let realm_id = state.get_realm_id();
    let user_ids = (0..=super::MAX_USER_IDS)
        .map(|_| UserId::local(Ulid::generate(), realm_id).to_string())
        .collect();
    let result = resolve_users(
        State(state),
        RealmAuth(realm_auth(realm_id)),
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
        RealmAuth(realm_auth(realm_id)),
        Json(ResolveUsersRequest {
            user_ids: vec!["not-a-user-id".to_string()],
        }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequest)));
}
