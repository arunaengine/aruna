//! Tests who may read and change the realm email profile and that no password is ever shown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::auth::UnrestrictedAuth;
use crate::tests::realms::{admin_state, state_auth};
use aruna_core::UserId;

const PASSWORD: &str = "canary-route-77";

fn body(password: Option<&str>) -> Json<EmailProfileRequest> {
    Json(EmailProfileRequest {
        host: "smtp.example.org".to_string(),
        port: Some(587),
        tls: TlsMode::Starttls,
        username: Some("aruna".to_string()),
        password: password.map(str::to_string),
        from: "Aruna <aruna@example.org>".to_string(),
    })
}

#[tokio::test]
async fn admin_sets_profile() {
    let (_dir, state, admin) = admin_state().await;
    let member = UserId::local(Ulid::generate(), state.get_realm_id());
    let get = |user_id| {
        get_email_profile(
            State(state.clone()),
            UnrestrictedAuth(state_auth(&state, user_id).expect("a signed-in caller")),
        )
    };
    let Json(empty) = get(member).await.unwrap();
    assert_eq!(
        empty,
        EmailProfileResponse {
            configured: false,
            profile: None
        }
    );

    let refused = set_email_profile(
        State(state.clone()),
        UnrestrictedAuth(state_auth(&state, member).expect("a signed-in caller")),
        body(Some(PASSWORD)),
    )
    .await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
    let Json(stored) = set_email_profile(
        State(state.clone()),
        UnrestrictedAuth(state_auth(&state, admin).expect("a signed-in caller")),
        body(Some(PASSWORD)),
    )
    .await
    .unwrap();
    assert!(stored.has_password);

    let Json(seen) = get(member).await.unwrap();
    assert_eq!(
        seen,
        EmailProfileResponse {
            configured: true,
            profile: None
        }
    );
    let Json(full) = get(admin).await.unwrap();
    assert_eq!(full.profile, Some(stored));
    assert!(!serde_json::to_string(&full).unwrap().contains(PASSWORD));

    let renamed = Json(EmailProfileRequest {
        username: Some("other".to_string()),
        ..body(None).0
    });
    let refused = set_email_profile(
        State(state.clone()),
        UnrestrictedAuth(state_auth(&state, admin).expect("a signed-in caller")),
        renamed,
    );
    assert!(matches!(
        refused.await,
        Err(ServerError::BadRequestReason(_))
    ));
    let bad_sender = Json(EmailProfileRequest {
        from: "not an address".to_string(),
        ..body(None).0
    });
    let refused = set_email_profile(
        State(state.clone()),
        UnrestrictedAuth(state_auth(&state, admin).expect("a signed-in caller")),
        bad_sender,
    );
    assert!(matches!(
        refused.await,
        Err(ServerError::BadRequestReason(_))
    ));

    let removed = delete_email_profile(
        State(state.clone()),
        UnrestrictedAuth(state_auth(&state, admin).expect("a signed-in caller")),
    )
    .await;
    assert_eq!(removed.unwrap(), StatusCode::NO_CONTENT);
    let Json(gone) = get(admin).await.unwrap();
    assert!(!gone.configured);
}
