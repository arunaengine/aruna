//! Tests the database routes: error mapping, listing, marking, removal and the owner consent
//! flow over a single node whose datasets link the bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::metadata::tests::{TestState, setup_state};
use crate::tests::datasets::{self, store_bucket as store};
use crate::tests::distributed::write;
use aruna_core::UserId;
use aruna_core::keyspaces::DATABASE_REQUEST_KEYSPACE;
use aruna_core::structs::identity::database::{DatabaseEntry, DatabaseRequest, DatabaseTarget};
use aruna_operations::realm::database::list::DatabaseServiceError;

const BUCKET: &str = "spire";

fn stranger(test: &TestState) -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::from_bytes([90; 16]), test.state.get_realm_id()),
        realm_id: test.state.get_realm_id(),
        path_restrictions: None,
        session: None,
    }
}

fn target(test: &TestState) -> DatabaseTarget {
    DatabaseTarget::Ordinary {
        node_id: test.state.get_node_id(),
        bucket: BUCKET.to_string(),
    }
}

async fn describe(test: &TestState) -> Ulid {
    let link = target(test).root_link(test.state.get_realm_id()).to_id();
    datasets::describe(test, "databases/spire", true, &link).await
}

fn mark_body(test: &TestState, dataset_id: &str) -> Json<MarkDatabaseRequest> {
    Json(MarkDatabaseRequest {
        entry_id: None,
        dataset_id: dataset_id.to_string(),
        target: DatabaseTargetBody::from(&target(test)),
    })
}

async fn marked(
    test: &TestState,
    auth: &AuthContext,
    dataset: &str,
) -> ServerResult<MarkDatabaseResponse> {
    mark(
        State(test.state.clone()),
        Extension(Some(auth.clone())),
        Extension(None),
        mark_body(test, dataset),
    )
    .await
    .map(|Json(response)| response)
}

async fn listed(
    test: &TestState,
    auth: &AuthContext,
    curation: bool,
) -> ServerResult<DatabaseListResponse> {
    get_databases(
        State(test.state.clone()),
        Extension(Some(auth.clone())),
        Extension(None),
        Query(DatabaseListParams {
            after: None,
            limit: None,
            curation,
        }),
    )
    .await
    .map(|Json(response)| response)
}

async fn seed_request(test: &TestState, dataset: Ulid, curator: UserId) -> DatabaseRequest {
    let request = DatabaseRequest {
        request_id: Ulid::from_bytes([91; 16]),
        entry: DatabaseEntry {
            entry_id: Ulid::from_bytes([92; 16]),
            dataset_id: dataset,
            target: target(test),
            owner_group: test.group_id,
        },
        predecessor: None,
        curator,
        created_at_ms: 1,
    };
    let key = request.request_id.to_bytes().to_vec();
    write(
        &test.state,
        DATABASE_REQUEST_KEYSPACE,
        key,
        request.to_bytes().unwrap(),
    )
    .await;
    request
}

#[test]
fn errors_map_statuses() {
    assert!(matches!(
        map_error(DatabaseServiceError::Forbidden),
        ServerError::Forbidden
    ));
    assert!(matches!(
        map_error(DatabaseServiceError::NotFound("x".to_string())),
        ServerError::NotFound
    ));
    assert!(matches!(
        map_error(DatabaseServiceError::NotLinked(Ulid::nil())),
        ServerError::BadRequestReason(_)
    ));
    assert!(matches!(
        map_error(DatabaseServiceError::BadRequest("x".to_string())),
        ServerError::BadRequestReason(_)
    ));
    assert!(matches!(
        map_error(DatabaseServiceError::Unavailable("x".to_string())),
        ServerError::ServiceUnavailableReason(_)
    ));
    assert!(matches!(
        map_error(DatabaseServiceError::Internal("x".to_string())),
        ServerError::InternalError(_)
    ));
}

#[test]
fn publish_errors_map() {
    let publish = |error| map_error(DatabaseServiceError::Publish(error));
    assert!(matches!(
        publish(PublishDatabaseError::Unauthorized),
        ServerError::Forbidden
    ));
    assert!(matches!(
        publish(PublishDatabaseError::NotManagementNode),
        ServerError::NoManagementNode
    ));
    assert!(matches!(
        publish(PublishDatabaseError::NotFound(Ulid::nil())),
        ServerError::NotFound
    ));
    assert!(matches!(
        publish(PublishDatabaseError::PredecessorChanged(Ulid::nil())),
        ServerError::Conflict(_)
    ));
    assert!(matches!(
        publish(PublishDatabaseError::Database(
            aruna_core::structs::identity::database::DatabaseError::TooMany
        )),
        ServerError::BadRequestReason(_)
    ));
    assert!(matches!(
        publish(PublishDatabaseError::Storage(
            StorageError::TransactionConflict
        )),
        ServerError::Conflict(_)
    ));
    assert!(matches!(
        publish(PublishDatabaseError::ConfigMissing),
        ServerError::InternalError(_)
    ));
}

#[tokio::test]
async fn marks_lists_removes() {
    let test = setup_state().await;
    datasets::grant_curator(&test).await;
    store(&test, BUCKET).await;
    let dataset = describe(&test).await;
    assert!(
        listed(&test, &test.auth, false)
            .await
            .unwrap()
            .databases
            .is_empty()
    );

    let response = marked(&test, &test.auth, &dataset.to_string())
        .await
        .unwrap();
    assert_eq!(response.status, "published");
    assert!(response.request_id.is_none());
    let entry_id = response.entry.entry_id.clone();

    let page = listed(&test, &test.auth, false).await.unwrap();
    assert_eq!(page.databases.len(), 1);
    assert_eq!(page.databases[0].state, "active");
    assert_eq!(page.databases[0].bucket, BUCKET);
    assert_eq!(page.databases[0].title.as_deref(), Some("Described data"));
    assert!(page.next_after.is_none());
    // A user without access to the bucket sees nothing; the curator view needs WRITE.
    let other = stranger(&test);
    assert!(
        listed(&test, &other, false)
            .await
            .unwrap()
            .databases
            .is_empty()
    );
    assert!(matches!(
        listed(&test, &other, true).await,
        Err(ServerError::Forbidden)
    ));

    let removed = remove(
        State(test.state.clone()),
        Extension(Some(other.clone())),
        Extension(None),
        Path(entry_id.clone()),
    )
    .await;
    assert!(matches!(removed, Err(ServerError::Forbidden)));
    let removed = remove(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(entry_id.clone()),
    )
    .await;
    assert_eq!(removed.unwrap(), StatusCode::NO_CONTENT);
    assert!(
        listed(&test, &test.auth, true)
            .await
            .unwrap()
            .databases
            .is_empty()
    );
    let again = remove(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(entry_id),
    )
    .await;
    assert!(matches!(again, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn marks_need_rights() {
    let test = setup_state().await;
    datasets::grant_curator(&test).await;
    store(&test, BUCKET).await;
    let dataset = Ulid::from_bytes([93; 16]).to_string();
    let denied = marked(&test, &stranger(&test), &dataset).await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));
    // The dataset does not exist, so the link cannot be confirmed.
    let unlinked = marked(&test, &test.auth, &dataset).await;
    assert!(matches!(
        unlinked,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    let malformed = marked(&test, &test.auth, "not-a-ulid").await;
    assert!(matches!(malformed, Err(ServerError::BadRequestReason(_))));
    let mut body = mark_body(&test, &dataset);
    body.0.entry_id = Some("not-a-ulid".to_string());
    let entry = mark(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        body,
    )
    .await;
    assert!(matches!(entry, Err(ServerError::BadRequestReason(_))));
    let mut body = mark_body(&test, &dataset);
    body.0.target = DatabaseTargetBody::Distributed {
        set_id: Ulid::from_bytes([94; 16]).to_string(),
    };
    let unknown = mark(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        body,
    )
    .await;
    assert!(matches!(unknown, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn request_visibility() {
    let test = setup_state().await;
    store(&test, BUCKET).await;
    let other = stranger(&test);
    let request = seed_request(&test, Ulid::from_bytes([95; 16]), other.user_id).await;
    let id = request.request_id.to_string();

    let requests = |auth: AuthContext| {
        let state = test.state.clone();
        async move {
            get_requests(State(state), Extension(Some(auth)), Extension(None))
                .await
                .map(|Json(list)| list.requests)
        }
    };
    // The curator and an admin of the owning group see it, nobody else.
    assert_eq!(requests(other.clone()).await.unwrap().len(), 1);
    assert_eq!(requests(test.auth.clone()).await.unwrap().len(), 1);
    let third = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([96; 16]), test.state.get_realm_id()),
        ..other.clone()
    };
    assert!(requests(third.clone()).await.unwrap().is_empty());

    let reject = |auth: AuthContext, id: String| {
        let state = test.state.clone();
        async move {
            reject(
                State(state),
                Extension(Some(auth)),
                Extension(None),
                Path(id),
            )
            .await
        }
    };
    assert!(matches!(
        reject(third, id.clone()).await,
        Err(ServerError::Forbidden)
    ));
    assert!(matches!(
        reject(other.clone(), "bad".to_string()).await,
        Err(ServerError::BadRequestReason(_))
    ));
    assert_eq!(
        reject(other.clone(), id.clone()).await.unwrap(),
        StatusCode::NO_CONTENT
    );
    assert!(requests(other.clone()).await.unwrap().is_empty());
    assert!(matches!(
        reject(other, id).await,
        Err(ServerError::NotFound)
    ));
}

#[tokio::test]
async fn approval_needs_owner() {
    let test = setup_state().await;
    store(&test, BUCKET).await;
    let other = stranger(&test);
    let dataset = describe(&test).await;
    let request = seed_request(&test, dataset, other.user_id).await;
    let id = request.request_id.to_string();
    let approve = |auth: AuthContext, id: String| {
        let state = test.state.clone();
        async move {
            approve(
                State(state),
                Extension(Some(auth)),
                Extension(None),
                Path(id),
            )
            .await
        }
    };
    assert!(matches!(
        approve(other, id.clone()).await,
        Err(ServerError::Forbidden)
    ));
    assert!(matches!(
        approve(test.auth.clone(), Ulid::from_bytes([97; 16]).to_string()).await,
        Err(ServerError::NotFound)
    ));
    assert!(matches!(
        approve(test.auth.clone(), "bad".to_string()).await,
        Err(ServerError::BadRequestReason(_))
    ));
    // The owner passes the link check; this node has no key to sign the consent with.
    assert!(matches!(
        approve(test.auth.clone(), id).await,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
}
