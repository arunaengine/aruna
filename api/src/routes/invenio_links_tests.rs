//! Tests Invenio link routes for authorization, token secrecy and connector protection.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::metadata::{CreateMetadataRequest, CreateRoCrateRequest};
use crate::routes::link_routes::*;
use crate::routes::metadata::documents::create_metadata_document;
use crate::routes::metadata::repositories::delete_repository;
use crate::routes::metadata::tests::{TestState, drain_metadata_background, setup_network_state};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{INVENIO_LINK_KEYSPACE, LINK_SECRET_KEYSPACE};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_operations::harvest::create_connector::{CreateConnectorInput, CreateConnectorOperation};
use std::collections::HashMap;

const TOKEN: &str = "personal-link-token";

struct Linked {
    test: TestState,
    document_id: String,
    connector_id: Ulid,
}

async fn setup() -> Linked {
    let test = setup_network_state().await;
    test.state
        .register_rest_interface("127.0.0.1:3000".parse().unwrap())
        .await;
    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::RoCrate(CreateRoCrateRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/linked".to_string(),
            public: false,
            rocrate: serde_json::json!({
                "@context": "https://w3id.org/ro/crate/1.2/context",
                "@graph": [
                    {"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
                        "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                        "about": {"@id": "./"}},
                    {"@id": "./", "@type": "Dataset", "name": "Linked",
                        "description": "Pushed to a repository", "datePublished": "2026-01-01",
                        "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}}
                ]
            }),
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;
    let connector_id = drive(
        CreateConnectorOperation::new(CreateConnectorInput {
            group_id: test.group_id,
            created_by: test.auth.user_id,
            name: "zenodo".into(),
            kind: RepositoryConnectorKind::Invenio,
            endpoint: "https://zenodo.example/api/".into(),
            public_config: HashMap::new(),
            secret_config: HashMap::new(),
        }),
        test.state.get_ctx().as_ref(),
    )
    .await
    .unwrap()
    .connector
    .connector_id;
    Linked {
        document_id: created.summary.document_id,
        test,
        connector_id,
    }
}

fn request(linked: &Linked) -> CreateLinkRequest {
    CreateLinkRequest {
        group_id: linked.test.group_id.to_string(),
        connector_id: linked.connector_id.to_string(),
        access_token: TOKEN.into(),
        parent_id: Some("abcde-12345".into()),
        auto_publish: false,
        public_files: false,
        metadata: None,
    }
}

async fn create(linked: &Linked, auth: Option<AuthContext>) -> ServerResult<InvenioLinkResponse> {
    create_link(
        State(linked.test.state.clone()),
        Extension(auth),
        Path(linked.document_id.clone()),
        Json(request(linked)),
    )
    .await
    .map(|(status, Json(link))| {
        assert_eq!(status, StatusCode::CREATED);
        link
    })
}

fn stranger(linked: &Linked) -> AuthContext {
    AuthContext {
        user_id: aruna_core::UserId::local(Ulid::generate(), linked.test.auth.realm_id),
        ..linked.test.auth.clone()
    }
}

fn paths(linked: &Linked, link: &InvenioLinkResponse) -> Path<(String, String)> {
    Path((linked.document_id.clone(), link.link_id.clone()))
}

#[test]
fn requests_hide_token() {
    let linked_request = CreateLinkRequest {
        group_id: "g".into(),
        connector_id: "c".into(),
        access_token: TOKEN.into(),
        parent_id: None,
        auto_publish: false,
        public_files: false,
        metadata: None,
    };
    let rotate = RotateTokenRequest {
        access_token: TOKEN.into(),
    };
    for text in [
        serde_json::to_string(&linked_request).unwrap(),
        format!("{linked_request:?}"),
        serde_json::to_string(&rotate).unwrap(),
        format!("{rotate:?}"),
    ] {
        assert!(!text.contains(TOKEN), "{text}");
    }
}

#[tokio::test]
async fn link_routes_authorize() {
    let linked = setup().await;
    let state = || State(linked.test.state.clone());
    let owner = || Extension(Some(linked.test.auth.clone()));
    assert!(matches!(
        create(&linked, None).await,
        Err(ServerError::Unauthorized)
    ));
    assert!(create(&linked, Some(stranger(&linked))).await.is_err());
    let link = create(&linked, Some(linked.test.auth.clone()))
        .await
        .unwrap();
    assert_eq!((link.status.as_str(), link.pending), ("enabled", true));
    assert_eq!(link.remote.parent_id.as_deref(), Some("abcde-12345"));
    assert_eq!(link.owner_node_url, "http://127.0.0.1:3000/api/v1");
    let body = serde_json::to_string(&link).unwrap();
    assert!(!body.contains(TOKEN));

    let context = linked.test.state.get_ctx();
    let link_id = Ulid::from_string(&link.link_id).unwrap();
    let document_id = parse_document_id(&linked.document_id).unwrap();
    for (key_space, key) in [
        (
            INVENIO_LINK_KEYSPACE,
            aruna_core::invenio::link_key(document_id, link_id),
        ),
        (LINK_SECRET_KEYSPACE, link_id.to_bytes().to_vec()),
    ] {
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.into(),
                key: key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("stored {key_space} row missing");
        };
        assert!(!String::from_utf8_lossy(&value).contains(TOKEN));
    }

    let listed =
        list_repository_links(state(), Extension(None), Path(linked.document_id.clone())).await;
    assert!(matches!(listed, Err(ServerError::Unauthorized)));
    let Json(listed) = list_repository_links(state(), owner(), Path(linked.document_id.clone()))
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);

    let pause = || {
        Json(PatchLinkRequest {
            paused: Some(true),
            ..PatchLinkRequest::default()
        })
    };
    let denied = patch_link(
        state(),
        Extension(Some(stranger(&linked))),
        paths(&linked, &link),
        pause(),
    )
    .await;
    assert!(denied.is_err());
    let Json(paused) = patch_link(state(), owner(), paths(&linked, &link), pause())
        .await
        .unwrap();
    assert_eq!(paused.status, "paused");
    let pushed = push_link(state(), owner(), paths(&linked, &link)).await;
    assert!(matches!(pushed, Err(ServerError::Conflict(_))));
    let published = publish_link(state(), owner(), paths(&linked, &link)).await;
    assert!(matches!(published, Err(ServerError::Conflict(_))));
    let rotate = || {
        Json(RotateTokenRequest {
            access_token: "second-link-token".into(),
        })
    };
    let refused = rotate_token(
        state(),
        Extension(Some(stranger(&linked))),
        paths(&linked, &link),
        rotate(),
    )
    .await;
    assert!(refused.is_err());
    assert_eq!(
        rotate_token(state(), owner(), paths(&linked, &link), rotate())
            .await
            .unwrap(),
        StatusCode::NO_CONTENT
    );
    let resume = Json(PatchLinkRequest {
        paused: Some(false),
        ..PatchLinkRequest::default()
    });
    let _ = patch_link(state(), owner(), paths(&linked, &link), resume)
        .await
        .unwrap();
    let (status, Json(first)) = push_link(state(), owner(), paths(&linked, &link))
        .await
        .unwrap();
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(first.status_url.ends_with(&first.job_id));
    let (_, Json(again)) = push_link(state(), owner(), paths(&linked, &link))
        .await
        .unwrap();
    assert_eq!(again.job_id, first.job_id);
    let Json(running) = get_link(state(), owner(), paths(&linked, &link))
        .await
        .unwrap();
    assert!(running.pending);

    assert_eq!(
        delete_link(state(), owner(), paths(&linked, &link))
            .await
            .unwrap(),
        StatusCode::NO_CONTENT
    );
    let gone = get_link(state(), owner(), paths(&linked, &link)).await;
    assert!(matches!(gone, Err(ServerError::NotFound)));
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: LINK_SECRET_KEYSPACE.into(),
            key: link_id.to_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    else {
        panic!("secret read failed");
    };
    assert!(value.is_none());
}

#[tokio::test]
async fn connector_delete_conflicts() {
    let linked = setup().await;
    let link = create(&linked, Some(linked.test.auth.clone()))
        .await
        .unwrap();
    let remove_connector = || {
        delete_repository(
            State(linked.test.state.clone()),
            Extension(Some(linked.test.auth.clone())),
            Path((
                linked.test.group_id.to_string(),
                linked.connector_id.to_string(),
            )),
        )
    };
    assert!(matches!(
        remove_connector().await,
        Err(ServerError::Conflict(_))
    ));
    delete_link(
        State(linked.test.state.clone()),
        Extension(Some(linked.test.auth.clone())),
        paths(&linked, &link),
    )
    .await
    .unwrap();
    assert_eq!(remove_connector().await.unwrap(), StatusCode::NO_CONTENT);
}
