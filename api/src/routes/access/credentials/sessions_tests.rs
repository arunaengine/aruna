//! Tests S3 session minting limits, listing, secret hiding, and revocation behavior.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::FjallStorage;
use axum::response::IntoResponse;
use tempfile::TempDir;

async fn setup_node() -> (TempDir, Arc<ServerState>, AuthContext) {
    let directory = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let state = Arc::new(
        ServerState::new(
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
            realm_id,
            iroh::SecretKey::generate().public(),
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await,
    );
    let auth = AuthContext {
        user_id: UserId::new(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    (directory, state, auth)
}

async fn issue_session(
    state: &ServerState,
    user_identity: UserId,
    group_id: Ulid,
    issued_by: [u8; 32],
) -> S3SessionCredentials {
    let now = SystemTime::now();
    drive(
        CreateS3Operation::new(
            CreateS3Config {
                user_identity,
                group_id,
                now,
                expiry: now + Duration::from_secs(600),
                path_restrictions: None,
                issued_by,
            },
            state.credential_encryption_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .unwrap()
}

async fn stored_session(state: &ServerState, access_key: &str) -> Option<S3Session> {
    drive(
        GetS3Operation::new(access_key.to_string()),
        &state.get_ctx(),
    )
    .await
    .unwrap()
}

#[test]
fn mint_needs_group() {
    assert!(serde_json::from_value::<S3SessionRequest>(serde_json::json!({})).is_err());
}

#[test]
fn expiry_caps_hour() {
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    assert_eq!(
        session_expiry(now, 10_000).unwrap(),
        UNIX_EPOCH + Duration::from_secs(4_600)
    );
}

#[test]
fn expiry_caps_bearer() {
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    assert_eq!(
        session_expiry(now, 2_500).unwrap(),
        UNIX_EPOCH + Duration::from_secs(2_500)
    );
}

#[tokio::test]
async fn mint_checks_member() {
    let directory = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
    let state = ServerState::new(
        Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }),
        realm_id,
        node_id,
        NodeCapabilities::user_node(realm_id).unwrap(),
        false,
        None,
        JobsRuntime::new(),
    )
    .await;
    let group_id = Ulid::from_bytes([3u8; 16]);
    let member = UserId::new(Ulid::from_bytes([4u8; 16]), realm_id);
    let stranger = UserId::new(Ulid::from_bytes([5u8; 16]), realm_id);
    let actor = Actor {
        node_id,
        user_id: member,
        realm_id,
    };
    let authorization = GroupAuthorizationDocument::default_group_doc(member, realm_id, group_id);
    let group = Group {
        display_name: "session-group".to_string(),
        group_id,
        realm_id,
        roles: authorization.roles.keys().copied().collect(),
        owner: member,
    };
    for (key_space, value) in [
        (AUTH_KEYSPACE, authorization.to_bytes(&actor).unwrap()),
        (GROUP_KEYSPACE, group.to_bytes(&actor).unwrap()),
    ] {
        state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: group_id.to_bytes().to_vec().into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
    }
    let member_auth = AuthContext {
        user_id: member,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let stranger_auth = AuthContext {
        user_id: stranger,
        ..member_auth.clone()
    };

    assert!(
        ensure_membership(&state, &member_auth, group_id)
            .await
            .is_ok()
    );
    assert!(matches!(
        ensure_membership(&state, &stranger_auth, group_id).await,
        Err(ServerError::Forbidden)
    ));
}

#[tokio::test]
async fn lists_own_sessions() {
    // Sessions of every group the caller holds one in, and of no other user.
    let (_directory, state, auth) = setup_node().await;
    let issuer = *state.get_node_id().as_bytes();
    let first_group = Ulid::generate();
    let second_group = Ulid::generate();
    let first = issue_session(&state, auth.user_id, first_group, issuer).await;
    let second = issue_session(&state, auth.user_id, second_group, issuer).await;
    let stranger = AuthContext {
        user_id: UserId::new(Ulid::generate(), auth.realm_id),
        ..auth.clone()
    };
    let theirs = issue_session(&state, stranger.user_id, first_group, issuer).await;

    let (_, Json(listed)) = list_s3_sessions(State(state.clone()), Extension(Some(auth)))
        .await
        .unwrap();
    let keys: Vec<&str> = listed
        .sessions
        .iter()
        .map(|session| session.access_key_id.as_str())
        .collect();
    assert_eq!(listed.sessions.len(), 2);
    assert!(keys.contains(&first.access_key_id.as_str()));
    assert!(keys.contains(&second.access_key_id.as_str()));
    assert!(
        listed
            .sessions
            .iter()
            .all(|session| session.created_at.is_some())
    );
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.group.id == second_group.to_string())
    );

    let (_, Json(listed)) = list_s3_sessions(State(state), Extension(Some(stranger)))
        .await
        .unwrap();
    assert_eq!(listed.sessions.len(), 1);
    assert_eq!(listed.sessions[0].access_key_id, theirs.access_key_id);
}

#[tokio::test]
async fn revoke_prunes_index() {
    // A stale owner index would break the next exchange for that group.
    let (_directory, state, auth) = setup_node().await;
    let issuer = *state.get_node_id().as_bytes();
    let group_id = Ulid::generate();
    let dropped = issue_session(&state, auth.user_id, group_id, issuer).await;
    let kept = issue_session(&state, auth.user_id, group_id, issuer).await;

    revoke_s3_session(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Path(dropped.access_key_id),
    )
    .await
    .unwrap();
    let issued = issue_session(&state, auth.user_id, group_id, issuer).await;

    let (_, Json(listed)) = list_s3_sessions(State(state), Extension(Some(auth)))
        .await
        .unwrap();
    let keys: Vec<&str> = listed
        .sessions
        .iter()
        .map(|session| session.access_key_id.as_str())
        .collect();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&kept.access_key_id.as_str()));
    assert!(keys.contains(&issued.access_key_id.as_str()));
}

#[tokio::test]
async fn hides_session_secrets() {
    let (_directory, state, auth) = setup_node().await;
    let issuer = *state.get_node_id().as_bytes();
    let issued = issue_session(&state, auth.user_id, Ulid::generate(), issuer).await;

    let (_, Json(listed)) = list_s3_sessions(State(state), Extension(Some(auth)))
        .await
        .unwrap();
    let body = serde_json::to_string(&listed).unwrap();
    assert!(!body.contains(issued.secret_access_key.expose()));
    assert!(!body.contains(issued.session_token.expose()));
    assert!(!body.contains("secret"));
    assert!(!body.contains("token"));
}

#[tokio::test]
async fn revoke_drops_session() {
    let (_directory, state, auth) = setup_node().await;
    let issuer = *state.get_node_id().as_bytes();
    let issued = issue_session(&state, auth.user_id, Ulid::generate(), issuer).await;

    let status = revoke_s3_session(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Path(issued.access_key_id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
        stored_session(&state, &issued.access_key_id)
            .await
            .is_none()
    );

    let (_, Json(listed)) = list_s3_sessions(State(state.clone()), Extension(Some(auth.clone())))
        .await
        .unwrap();
    assert!(listed.sessions.is_empty());
    let refreshed = refresh_s3_session(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("bearer"))),
        Path(issued.access_key_id.clone()),
    )
    .await;
    assert!(matches!(refreshed, Err(ServerError::NotFound)));
    let repeated = revoke_s3_session(
        State(state),
        Extension(Some(auth)),
        Path(issued.access_key_id),
    )
    .await
    .unwrap_err();
    assert_eq!(repeated.into_response().status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn revoke_hides_foreign() {
    // Another user's key must answer as unknown rather than denied.
    let (_directory, state, auth) = setup_node().await;
    let issuer = *state.get_node_id().as_bytes();
    let issued = issue_session(&state, auth.user_id, Ulid::generate(), issuer).await;
    let stranger = AuthContext {
        user_id: UserId::new(Ulid::generate(), auth.realm_id),
        ..auth
    };

    let error = revoke_s3_session(
        State(state.clone()),
        Extension(Some(stranger)),
        Path(issued.access_key_id.clone()),
    )
    .await
    .unwrap_err();
    assert_eq!(error.into_response().status(), StatusCode::NOT_FOUND);
    assert!(
        stored_session(&state, &issued.access_key_id)
            .await
            .is_some()
    );
}

#[tokio::test]
async fn revoke_scoped_node() {
    // A session another node issued is neither listed nor revocable here.
    let (_directory, state, auth) = setup_node().await;
    let issued = issue_session(&state, auth.user_id, Ulid::generate(), [9u8; 32]).await;

    let error = revoke_s3_session(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Path(issued.access_key_id.clone()),
    )
    .await
    .unwrap_err();
    assert_eq!(error.into_response().status(), StatusCode::NOT_FOUND);
    assert!(
        stored_session(&state, &issued.access_key_id)
            .await
            .is_some()
    );

    let (_, Json(listed)) = list_s3_sessions(State(state), Extension(Some(auth)))
        .await
        .unwrap();
    assert!(listed.sessions.is_empty());
}
