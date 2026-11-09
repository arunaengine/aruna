//! Tests who may manage node and realm secrets, the change flow, and that no value is shown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::keys::generate_signing_key;
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::driver::drive;
use aruna_operations::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::{Extension, Json};
use tempfile::TempDir;
use ulid::Ulid;

use super::{group, node, realm, user, *};
use crate::server::state::ServerState;
use crate::tests::routes::{test_context, test_state, test_storage};

const CANARY: &str = "canary-route-4e1";

/// A management node state with one realm admin; returns the admin's id.
async fn setup_state() -> (TempDir, Arc<ServerState>, UserId) {
    let (dir, storage) = test_storage();
    let context = Arc::new(test_context(storage));
    let signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let actor = Actor {
        node_id: iroh::SecretKey::generate().public(),
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
    };
    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: actor.clone(),
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
    let claim = ClaimInitialInput {
        actor: actor.clone(),
    };
    drive(ClaimInitialOperation::new(claim), &context)
        .await
        .unwrap();
    let capabilities = NodeCapabilities::management_node(signing_key).unwrap();
    let state = Arc::new(test_state(context, realm_id, actor.node_id, capabilities).await);
    (dir, state, actor.user_id)
}

fn auth(state: &ServerState, user_id: UserId) -> Extension<Option<AuthContext>> {
    Extension(Some(AuthContext {
        user_id,
        realm_id: state.get_realm_id(),
        path_restrictions: None,
        session: None,
    }))
}

fn create(name: &str) -> Json<CreateSecretRequest> {
    Json(CreateSecretRequest {
        name: name.to_string(),
        kind: "bearer".to_string(),
        description: "Catalog API".to_string(),
        settings: BTreeMap::new(),
        fields: [("token".to_string(), CANARY.to_string())].into(),
        uses: Vec::new(),
        readable: false,
    })
}

fn rotate() -> Json<RotateSecretRequest> {
    Json(RotateSecretRequest {
        fields: [("token".to_string(), "canary-route-4e2".to_string())].into(),
    })
}

fn matching(revision: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let value = HeaderValue::from_str(&format!("\"{revision}\"")).unwrap();
    headers.insert(axum::http::header::IF_MATCH, value);
    headers
}

fn path(name: &str) -> Path<String> {
    Path(name.to_string())
}

#[tokio::test]
async fn node_secret_flow() {
    let (_dir, state, admin) = setup_state().await;
    let member = UserId::local(Ulid::generate(), state.get_realm_id());
    let refused =
        node::create_node_secret(State(state.clone()), auth(&state, member), create("a")).await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));

    let created = node::create_node_secret(State(state.clone()), auth(&state, admin), create("a"));
    let (status, Json(created)) = created.await.unwrap();
    assert_eq!(
        (status, created.fields.as_slice()),
        (StatusCode::CREATED, ["token".to_string()].as_slice())
    );
    let again =
        node::create_node_secret(State(state.clone()), auth(&state, admin), create("a")).await;
    assert!(matches!(again, Err(ServerError::Conflict(_))));
    let second = node::create_node_secret(State(state.clone()), auth(&state, admin), create("b"));
    assert_eq!(second.await.unwrap().0, StatusCode::CREATED);

    let query = Query(SecretListQuery {
        cursor: None,
        limit: Some(1),
    });
    let Json(first) = node::list_node_secrets(State(state.clone()), auth(&state, admin), query)
        .await
        .unwrap();
    assert_eq!(first.secrets.len(), 1);
    assert_eq!(first.next_cursor.as_deref(), Some("a"));
    let shown = serde_json::to_string(&first).unwrap();
    assert!(!shown.contains(CANARY));

    let stale = node::rotate_node_secret(
        State(state.clone()),
        auth(&state, admin),
        path("a"),
        matching("01JREVIS00123456789ABCDEFG.x"),
        rotate(),
    );
    assert!(matches!(
        stale.await,
        Err(ServerError::PreconditionFailed(_))
    ));
    let rotated = node::rotate_node_secret(
        State(state.clone()),
        auth(&state, admin),
        path("a"),
        matching(&created.revision),
        rotate(),
    );
    let Json(rotated) = rotated.await.unwrap();
    assert_ne!(rotated.revision, created.revision);
    let Json(read) = node::get_node_secret(State(state.clone()), auth(&state, admin), path("a"))
        .await
        .unwrap();
    assert_eq!(read, rotated);

    let deleted = node::delete_node_secret(
        State(state.clone()),
        auth(&state, admin),
        path("a"),
        HeaderMap::new(),
    );
    assert_eq!(deleted.await.unwrap(), StatusCode::NO_CONTENT);
    let gone = node::get_node_secret(State(state.clone()), auth(&state, admin), path("a")).await;
    assert!(matches!(gone, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn realm_secret_flow() {
    let (_dir, state, admin) = setup_state().await;
    let mut readable = create("a");
    readable.0.readable = true;
    let refused =
        realm::create_realm_secret(State(state.clone()), auth(&state, admin), readable).await;
    assert!(matches!(refused, Err(ServerError::BadRequestReason(_))));
    let mut bad = create("a");
    bad.0.kind = "plugin.x.y".to_string();
    let refused = realm::create_realm_secret(State(state.clone()), auth(&state, admin), bad).await;
    assert!(matches!(refused, Err(ServerError::BadRequestReason(_))));

    let created =
        realm::create_realm_secret(State(state.clone()), auth(&state, admin), create("a"));
    let (_, Json(created)) = created.await.unwrap();
    let replace = Json(ReplaceSecretRequest {
        description: "Renamed".to_string(),
        settings: BTreeMap::new(),
        uses: Vec::new(),
    });
    let replaced = realm::replace_realm_secret(
        State(state.clone()),
        auth(&state, admin),
        path("a"),
        matching(&created.revision),
        replace,
    );
    let Json(replaced) = replaced.await.unwrap();
    assert_eq!(replaced.description, "Renamed");
    let query = Query(SecretListQuery::default());
    let Json(listed) = realm::list_realm_secrets(State(state.clone()), auth(&state, admin), query)
        .await
        .unwrap();
    assert_eq!(listed.secrets, std::slice::from_ref(&replaced));
    assert!(!serde_json::to_string(&listed).unwrap().contains(CANARY));

    let deleted = realm::delete_realm_secret(
        State(state.clone()),
        auth(&state, admin),
        path("a"),
        HeaderMap::new(),
    );
    assert_eq!(deleted.await.unwrap(), StatusCode::NO_CONTENT);
    let gone = realm::get_realm_secret(State(state.clone()), auth(&state, admin), path("a")).await;
    assert!(matches!(gone, Err(ServerError::NotFound)));
}

#[test]
fn requests_hide_values() {
    let request = create("a").0;
    let shown = format!("{request:?}{:?}", rotate().0);
    assert!(shown.contains("token") && !shown.contains("canary"));
    let error = parse_name("Bad Name").unwrap_err();
    assert!(matches!(error, ServerError::BadRequestReason(_)));
}

#[tokio::test]
async fn user_secret_readback() {
    let (_dir, state, admin) = setup_state().await;
    let mut readable = create("orcid");
    readable.0.readable = true;
    let created = user::create_user_secret(State(state.clone()), auth(&state, admin), readable);
    let (_, Json(created)) = created.await.unwrap();
    assert!(created.readable && !created.incomplete);
    let read = user::read_user_secret(State(state.clone()), auth(&state, admin), path("orcid"));
    let Json(value) = read.await.unwrap();
    assert_eq!(value.fields["token"], CANARY);
    assert!(!format!("{value:?}").contains(CANARY));

    // An unreadable secret never reads back; another user never sees the caller's secrets.
    user::create_user_secret(State(state.clone()), auth(&state, admin), create("hidden"))
        .await
        .map(|(status, _)| assert_eq!(status, StatusCode::CREATED))
        .unwrap();
    let refused = user::read_user_secret(State(state.clone()), auth(&state, admin), path("hidden"));
    assert!(matches!(refused.await, Err(ServerError::Forbidden)));
    let other = UserId::local(Ulid::generate(), state.get_realm_id());
    let query = Query(SecretListQuery::default());
    let Json(listed) = user::list_user_secrets(State(state.clone()), auth(&state, other), query)
        .await
        .unwrap();
    assert!(listed.secrets.is_empty());
    let query = Query(SecretListQuery::default());
    let Json(own) = user::list_user_secrets(State(state.clone()), auth(&state, admin), query)
        .await
        .unwrap();
    assert_eq!(own.secrets.len(), 2);
    assert!(!serde_json::to_string(&own).unwrap().contains(CANARY));
}

#[tokio::test]
async fn group_secret_managers() {
    let (_dir, state, admin) = setup_state().await;
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: admin,
        realm_id: state.get_realm_id(),
    };
    let config = CreateGroupConfig {
        actor,
        display_name: "secret group".to_string(),
        owner_cap: None,
    };
    let (created, _) = drive(CreateGroupOperation::new(config), &state.get_ctx())
        .await
        .unwrap();
    let group_id = created.group_id.to_string();
    let member = UserId::local(Ulid::generate(), state.get_realm_id());
    let refused = group::create_group_secret(
        State(state.clone()),
        auth(&state, member),
        Path(group_id.clone()),
        create("a"),
    );
    assert!(matches!(refused.await, Err(ServerError::Forbidden)));
    let mut readable = create("a");
    readable.0.readable = true;
    let refused = group::create_group_secret(
        State(state.clone()),
        auth(&state, admin),
        Path(group_id.clone()),
        readable,
    );
    assert!(matches!(
        refused.await,
        Err(ServerError::BadRequestReason(_))
    ));
    let created = group::create_group_secret(
        State(state.clone()),
        auth(&state, admin),
        Path(group_id.clone()),
        create("a"),
    );
    let (_, Json(created)) = created.await.unwrap();
    let rotated = group::rotate_group_secret(
        State(state.clone()),
        auth(&state, admin),
        Path((group_id.clone(), "a".to_string())),
        matching(&created.revision),
        rotate(),
    );
    let Json(rotated) = rotated.await.unwrap();
    assert_ne!(rotated.revision, created.revision);
    let stale = group::delete_group_secret(
        State(state.clone()),
        auth(&state, admin),
        Path((group_id.clone(), "a".to_string())),
        matching(&created.revision),
    );
    assert!(matches!(
        stale.await,
        Err(ServerError::PreconditionFailed(_))
    ));
}
