//! Tests harvest submission for authorization, source reuse, idempotency and conflicts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::metadata::repositories::{
    ApiRepositoryKind, RepositoryRequest, create_repository,
};
use crate::tests::routes::{
    seed_group_docs, seed_realm_auth, seed_realm_config, test_context, test_state, test_storage,
};
use aruna_core::UserId;
use aruna_core::structs::execution::job::JobState;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities, PathRestriction};
use aruna_operations::harvest::create_source::source_id;
use aruna_operations::jobs::store::{JobMutation, mutate_job};
use tempfile::TempDir;

struct Setup {
    _dir: TempDir,
    state: Arc<ServerState>,
    owner: AuthContext,
    stranger: AuthContext,
    group_id: Ulid,
}

async fn setup() -> Setup {
    let (dir, storage_handle) = test_storage();
    let realm_id = aruna_core::structs::identity::realm::RealmId([3u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[13u8; 32]).public();
    let owner = UserId::local(Ulid::generate(), realm_id);
    let stranger = UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let context = Arc::new(test_context(storage_handle));
    let group_id = Ulid::generate();
    seed_realm_config(&context, realm_id, &actor).await;
    seed_realm_auth(&context, realm_id, &actor).await;
    seed_group_docs(&context, realm_id, &actor, group_id, "harvest", owner).await;
    let state = Arc::new(
        test_state(
            context,
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );
    state
        .register_rest_public(
            "127.0.0.1:3000".parse().unwrap(),
            Some("https://owner.example/"),
        )
        .await;
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
    }
}

async fn connector(setup: &Setup, kind: ApiRepositoryKind) -> String {
    let endpoint = match kind {
        ApiRepositoryKind::OaiPmh => "https://zenodo.org/oai2d",
        ApiRepositoryKind::Invenio => "https://zenodo.org/api/",
    };
    let (_, Json(created)) = create_repository(
        State(setup.state.clone()),
        Extension(Some(setup.owner.clone())),
        Path(setup.group_id.to_string()),
        Json(RepositoryRequest {
            name: "zenodo".into(),
            kind,
            endpoint: endpoint.into(),
            community: None,
            secret_config: None,
        }),
    )
    .await
    .unwrap();
    created.connector_id
}

fn request(target: &str, key: Option<&str>) -> HarvestRequest {
    HarvestRequest {
        namespace: "zenodo".into(),
        target_prefix: target.into(),
        set: None,
        metadata_prefix: None,
        idempotency_key: key.map(str::to_string),
    }
}

async fn submit(
    setup: &Setup,
    auth: AuthContext,
    connector_id: &str,
    body: HarvestRequest,
) -> ServerResult<HarvestResponse> {
    submit_harvest(
        State(setup.state.clone()),
        Extension(Some(auth)),
        Path((setup.group_id.to_string(), connector_id.to_string())),
        Json(body),
    )
    .await
    .map(|(status, Json(response))| {
        assert_eq!(status, StatusCode::ACCEPTED);
        response
    })
}

#[tokio::test]
async fn submit_reuses_source() {
    let setup = setup().await;
    let connector_id = connector(&setup, ApiRepositoryKind::OaiPmh).await;
    let first = submit(
        &setup,
        setup.owner.clone(),
        &connector_id,
        request("imported/zenodo", Some("run-1")),
    )
    .await
    .unwrap();
    assert!(first.created);
    let expected = source_id(Ulid::from_string(&connector_id).unwrap(), "zenodo");
    assert_eq!(first.source_id, expected.to_string());
    assert!(
        first
            .status_url
            .ends_with(&format!("/compute/jobs/{}", first.job_id))
    );

    let replay = submit(
        &setup,
        setup.owner.clone(),
        &connector_id,
        request("imported/zenodo", Some("run-1")),
    )
    .await
    .unwrap();
    assert_eq!(replay.job_id, first.job_id);
    assert!(!replay.created);

    // One run per source: a later run waits until the first one ended.
    let running = submit(
        &setup,
        setup.owner.clone(),
        &connector_id,
        request("imported/zenodo", None),
    )
    .await;
    assert!(matches!(running, Err(ServerError::Conflict(_))));
    let job_id = first.job_id.parse().unwrap();
    let context = setup.state.get_ctx();
    mutate_job(&context.storage_handle, job_id, |record| {
        record.state = JobState::Cancelled;
        Ok(JobMutation::Persist)
    })
    .await
    .unwrap();

    // A later run keeps the source, so it continues from the stored cursor.
    let next = submit(
        &setup,
        setup.owner.clone(),
        &connector_id,
        request("imported/zenodo", None),
    )
    .await
    .unwrap();
    assert!(next.created);
    assert_ne!(next.job_id, first.job_id);
    assert_eq!(next.source_id, first.source_id);
}

#[tokio::test]
async fn submit_reports_conflicts() {
    let setup = setup().await;
    let connector_id = connector(&setup, ApiRepositoryKind::OaiPmh).await;
    submit(
        &setup,
        setup.owner.clone(),
        &connector_id,
        request("imported/zenodo", Some("run-1")),
    )
    .await
    .unwrap();
    let moved = submit(
        &setup,
        setup.owner.clone(),
        &connector_id,
        request("imported/other", None),
    )
    .await;
    assert!(matches!(moved, Err(ServerError::Conflict(_))));

    let other_namespace = HarvestRequest {
        namespace: "dryad".into(),
        ..request("imported/dryad", Some("run-1"))
    };
    let rebound = submit(&setup, setup.owner.clone(), &connector_id, other_namespace).await;
    assert!(matches!(rebound, Err(ServerError::JobPlanConflict(_))));
}

#[tokio::test]
async fn submit_checks_access() {
    let setup = setup().await;
    let connector_id = connector(&setup, ApiRepositoryKind::OaiPmh).await;
    let stranger = submit(
        &setup,
        setup.stranger.clone(),
        &connector_id,
        request("imported/zenodo", None),
    )
    .await;
    assert!(matches!(stranger, Err(ServerError::Forbidden)));

    let restricted = AuthContext {
        path_restrictions: Some(vec![PathRestriction {
            pattern: "/**".to_string(),
            permission: Permission::WRITE,
        }]),
        ..setup.owner.clone()
    };
    let delegated = submit(
        &setup,
        restricted,
        &connector_id,
        request("imported/zenodo", None),
    )
    .await;
    assert!(matches!(delegated, Err(ServerError::Forbidden)));

    let anonymous = submit_harvest(
        State(setup.state.clone()),
        Extension(None),
        Path((setup.group_id.to_string(), connector_id)),
        Json(request("imported/zenodo", None)),
    )
    .await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn submit_needs_oai() {
    let setup = setup().await;
    let invenio = connector(&setup, ApiRepositoryKind::Invenio).await;
    let refused = submit(
        &setup,
        setup.owner.clone(),
        &invenio,
        request("imported/zenodo", None),
    )
    .await;
    assert!(matches!(refused, Err(ServerError::BadRequestReason(_))));

    let missing = submit(
        &setup,
        setup.owner.clone(),
        &Ulid::generate().to_string(),
        request("imported/zenodo", None),
    )
    .await;
    assert!(matches!(missing, Err(ServerError::NotFound)));
}
