//! Tests repository connector routes for authorization, group scoping and secret redaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::routes::{
    seed_group_docs, seed_realm_auth, seed_realm_config, test_context, test_state, test_storage,
};
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use tempfile::TempDir;

struct Setup {
    _dir: TempDir,
    state: Arc<ServerState>,
    owner: AuthContext,
    stranger: AuthContext,
    group_id: Ulid,
    other_group: Ulid,
}

async fn setup() -> Setup {
    let (dir, storage_handle) = test_storage();
    let realm_id = aruna_core::structs::identity::realm::RealmId([3u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
    let owner = UserId::local(Ulid::generate(), realm_id);
    let stranger = UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let context = Arc::new(test_context(storage_handle));
    let group_id = Ulid::generate();
    let other_group = Ulid::generate();
    seed_realm_config(&context, realm_id, &actor).await;
    seed_realm_auth(&context, realm_id, &actor).await;
    seed_group_docs(&context, realm_id, &actor, group_id, "repos", owner).await;
    seed_group_docs(&context, realm_id, &actor, other_group, "other", stranger).await;
    let state = Arc::new(
        test_state(
            context,
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );
    let auth = |user_id| AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    Setup {
        _dir: dir,
        state,
        owner: auth(owner),
        stranger: auth(stranger),
        group_id,
        other_group,
    }
}

fn request(secret: Option<HashMap<String, String>>) -> RepositoryRequest {
    RepositoryRequest {
        name: "zenodo".into(),
        kind: ApiRepositoryKind::Invenio,
        endpoint: "https://zenodo.org/api/".into(),
        community: Some("aruna".into()),
        secret_config: secret,
    }
}

async fn create(setup: &Setup) -> RepositoryResponse {
    let token = HashMap::from([("token".to_string(), "secret-token-value".to_string())]);
    create_repository(
        State(setup.state.clone()),
        Extension(Some(setup.owner.clone())),
        Path(setup.group_id.to_string()),
        Json(request(Some(token))),
    )
    .await
    .unwrap()
    .1
    .0
}

#[tokio::test]
async fn crud_hides_secret() {
    let setup = setup().await;
    let created = create(&setup).await;
    assert!(created.has_secret_config);
    assert_eq!(created.community.as_deref(), Some("aruna"));
    let body = serde_json::to_string(&created).unwrap();
    assert!(!body.contains("secret-token-value") && !body.contains("\"secret_config\""));

    let Json(listed) = list_repositories(
        State(setup.state.clone()),
        Extension(Some(setup.owner.clone())),
        Path(setup.group_id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(listed.connectors, vec![created.clone()]);
    assert!(
        !serde_json::to_string(&listed)
            .unwrap()
            .contains("secret-token-value")
    );

    let path = (setup.group_id.to_string(), created.connector_id.clone());
    let Json(kept) = replace_repository(
        State(setup.state.clone()),
        Extension(Some(setup.owner.clone())),
        Path(path.clone()),
        Json(RepositoryRequest {
            community: None,
            ..request(None)
        }),
    )
    .await
    .unwrap();
    assert!(kept.has_secret_config);
    assert_eq!(kept.community, None);

    let Json(cleared) = replace_repository(
        State(setup.state.clone()),
        Extension(Some(setup.owner.clone())),
        Path(path.clone()),
        Json(request(Some(HashMap::new()))),
    )
    .await
    .unwrap();
    assert!(!cleared.has_secret_config);

    let status = delete_repository(
        State(setup.state.clone()),
        Extension(Some(setup.owner.clone())),
        Path(path.clone()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT);
    let missing = get_repository(
        State(setup.state.clone()),
        Extension(Some(setup.owner.clone())),
        Path(path),
    )
    .await;
    assert!(matches!(missing, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn routes_require_permission() {
    let setup = setup().await;
    let created = create(&setup).await;
    let anonymous = list_repositories(
        State(setup.state.clone()),
        Extension(None),
        Path(setup.group_id.to_string()),
    )
    .await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));

    let denied = get_repository(
        State(setup.state.clone()),
        Extension(Some(setup.stranger.clone())),
        Path((setup.group_id.to_string(), created.connector_id.clone())),
    )
    .await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));

    let wrong_group = get_repository(
        State(setup.state.clone()),
        Extension(Some(setup.stranger.clone())),
        Path((setup.other_group.to_string(), created.connector_id)),
    )
    .await;
    assert!(matches!(wrong_group, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn rejects_invalid_config() {
    let setup = setup().await;
    for body in [
        RepositoryRequest {
            endpoint: "http://zenodo.org/api/".into(),
            ..request(None)
        },
        request(Some(HashMap::from([("password".into(), "p".into())]))),
    ] {
        let result = create_repository(
            State(setup.state.clone()),
            Extension(Some(setup.owner.clone())),
            Path(setup.group_id.to_string()),
            Json(body),
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
    }
}

#[test]
fn request_hides_secrets() {
    let request = RepositoryRequest {
        name: "n".into(),
        kind: ApiRepositoryKind::Invenio,
        endpoint: "https://example.org/api/".into(),
        community: None,
        secret_config: Some(HashMap::from([(
            "token".to_string(),
            "canary-2c9d".to_string(),
        )])),
    };
    let text = format!("{request:?}");
    assert!(!text.contains("canary-2c9d"), "{text}");
    assert!(text.contains("token"), "{text}");
}
