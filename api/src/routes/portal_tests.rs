//! Tests who may read and replace the realm portal feature defaults.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::auth::RealmAuth;
use crate::auth::extract;
use crate::tests::realms::{admin_state, state_auth};
use aruna_core::UserId;
use axum::http::StatusCode;
use ulid::Ulid;

fn body() -> Json<PortalFeatures> {
    Json(PortalFeatures {
        features: BTreeMap::from([("compute".to_string(), false)]),
    })
}

#[tokio::test]
async fn member_reads_defaults() {
    // Every portal needs the defaults, so a member without admin rights reads them.
    let (_dir, state, admin) = admin_state().await;
    let member = UserId::local(Ulid::generate(), state.get_realm_id());
    let Json(stored) = set_portal_features(
        State(state.clone()),
        RealmAuth(state_auth(&state, admin).expect("a signed-in caller")),
        body(),
    )
    .await
    .unwrap();
    assert_eq!(stored, body().0);

    let Json(read) = get_portal_features(
        State(state.clone()),
        RealmAuth(state_auth(&state, member).expect("a signed-in caller")),
    )
    .await
    .unwrap();
    assert_eq!(read, body().0);

    // `get_portal_features` takes `RealmAuth`, so the refusal is the extractor's.
    let error = extract::<RealmAuth>(&state, None).await;
    assert_eq!(error.err(), Some(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn member_cannot_replace() {
    let (_dir, state, _admin) = admin_state().await;
    let member = UserId::local(Ulid::generate(), state.get_realm_id());
    let error = set_portal_features(
        State(state.clone()),
        RealmAuth(state_auth(&state, member).expect("a signed-in caller")),
        body(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));

    let Json(read) = get_portal_features(
        State(state.clone()),
        RealmAuth(state_auth(&state, member).expect("a signed-in caller")),
    )
    .await
    .unwrap();
    assert!(read.features.is_empty());
}

#[tokio::test]
async fn rejects_invalid_name() {
    let (_dir, state, admin) = admin_state().await;
    let invalid = Json(PortalFeatures {
        features: BTreeMap::from([("Compute".to_string(), false)]),
    });
    let error = set_portal_features(
        State(state.clone()),
        RealmAuth(state_auth(&state, admin).expect("a signed-in caller")),
        invalid,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::BadRequestReason(_)));
}
