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
use aruna_core::keyspaces::{LINK_SECRET_KEYSPACE, REPOSITORY_LINK_KEYSPACE};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_operations::driver::drive;
use aruna_operations::harvest::create_connector::{CreateConnectorInput, CreateConnectorOperation};
use std::collections::HashMap;

const TOKEN: &str = "personal-link-token";

struct Linked {
    test: TestState,
    document_id: String,
    connector_id: Ulid,
}

async fn setup() -> Linked {
    let root = serde_json::json!({"@id": "./", "@type": "Dataset", "name": "Linked",
        "description": "Pushed to a repository", "datePublished": "2026-01-01",
        "publisher": "Aruna test", "creator": {"@type": "Person", "familyName": "Doe"},
        "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}});
    setup_crate(root, "https://zenodo.example/api/").await
}

/// A dataset with this crate root and an Invenio connector at `endpoint`.
async fn setup_crate(root: serde_json::Value, endpoint: &str) -> Linked {
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
                    root
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
            endpoint: endpoint.into(),
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

async fn create(
    linked: &Linked,
    auth: Option<AuthContext>,
) -> ServerResult<RepositoryLinkResponse> {
    Box::pin(create_link(
        State(linked.test.state.clone()),
        Extension(auth),
        Path(linked.document_id.clone()),
        Json(request(linked)),
    ))
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

fn paths(linked: &Linked, link: &RepositoryLinkResponse) -> Path<(String, String)> {
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
            REPOSITORY_LINK_KEYSPACE,
            aruna_core::repository::link_key(document_id, link_id),
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

#[tokio::test]
async fn holder_copy_refuses() {
    let linked = setup().await;
    let state = || State(linked.test.state.clone());
    let owner = || Extension(Some(linked.test.auth.clone()));
    let document_id = parse_document_id(&linked.document_id).unwrap();
    // A replicated copy of a link another node owns; that node is not a holder of the dataset.
    let now = std::time::SystemTime::now();
    let copy = RepositoryLink {
        link_id: Ulid::generate(),
        document_id,
        group_id: linked.test.group_id,
        connector_id: linked.connector_id,
        endpoint: "https://zenodo.example/api/".into(),
        owner_node: iroh::SecretKey::from_bytes(&[42; 32]).public(),
        owner_node_url: "https://owner.example/api/v1".into(),
        created_by: linked.test.auth.user_id,
        status: LinkStatus::Enabled,
        auto_publish: false,
        public_files: false,
        metadata_json: "{}".into(),
        remote: LinkRemote::default(),
        last_push: None,
        active_job: None,
        sequence: 0,
        limits: aruna_core::structs::execution::job::RoCrateLimits::default(),
        created_at: now,
        updated_at: now,
        generation: 1,
        warning: None,
        direction: aruna_core::repository::LinkDirection::Push,
        kind: aruna_core::structs::execution::harvest::RepositoryConnectorKind::Invenio,
    };
    let written = linked
        .test
        .state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: REPOSITORY_LINK_KEYSPACE.into(),
            key: copy.target().storage_key(),
            value: copy.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        written,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let Json(listed) = list_repository_links(state(), owner(), Path(linked.document_id.clone()))
        .await
        .unwrap();
    let [shown] = &listed[..] else {
        panic!("the holder lists the copy");
    };
    assert_eq!(shown.owner_node_url, "https://owner.example/api/v1");
    assert_eq!(shown.status, "failed");
    assert_eq!(shown.reason.as_deref(), Some("owner_not_holder"));
    let pause = Json(PatchLinkRequest {
        paused: Some(true),
        ..PatchLinkRequest::default()
    });
    match patch_link(state(), owner(), paths(&linked, shown), pause).await {
        Err(ServerError::Conflict(message)) => {
            assert!(
                message.contains("https://owner.example/api/v1"),
                "{message}"
            );
        }
        other => panic!("a copy must be managed on its owner node: {other:?}"),
    }
    let pushed = push_link(state(), owner(), paths(&linked, shown)).await;
    assert!(matches!(pushed, Err(ServerError::Conflict(_))));
}

#[tokio::test]
async fn requirements_refuse_links() {
    // Only the crate counts: overrides that name creators do not satisfy the requirement.
    let root = serde_json::json!({"@id": "./", "@type": "Dataset", "name": "Bare",
        "description": "No creators", "datePublished": "2026-01-01", "publisher": "Aruna test"});
    let linked = setup_crate(root, "https://rdm.example.org/api/").await;
    let mut request = request(&linked);
    request.metadata = Some(serde_json::json!({"creators": [
        {"person_or_org": {"type": "personal", "family_name": "Doe"}}]}));
    let refused = create_link(
        State(linked.test.state.clone()),
        Extension(Some(linked.test.auth.clone())),
        Path(linked.document_id.clone()),
        Json(request),
    )
    .await;
    let Err(error) = refused else {
        panic!("a link without creators was created");
    };
    assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
    let body = serde_json::to_value(error.response_body()).unwrap();
    assert_eq!(body["code"], "requirements_unmet");
    let findings = body["findings"].as_array().unwrap();
    assert!(findings.iter().any(|finding| {
        finding["path"] == "(<http://schema.org/author> | <http://schema.org/creator>)"
            && finding["focus_node"] == "./"
            && finding["severity"] == "violation"
    }));
    assert!(
        findings
            .iter()
            .any(|finding| finding["severity"] == "warning")
    );
}

#[tokio::test]
async fn publisher_needed_off_zenodo() {
    // Zenodo sets the publisher itself; another InvenioRDM needs it in the crate.
    let root = serde_json::json!({"@id": "./", "@type": "Dataset", "name": "No publisher",
        "description": "Published without a publisher", "datePublished": "2026-01-01",
        "author": {"@type": "Person", "name": "Ada Lovelace"},
        "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}});
    let zenodo = setup_crate(root.clone(), "https://sandbox.zenodo.org/api/").await;
    create(&zenodo, Some(zenodo.test.auth.clone()))
        .await
        .expect("Zenodo needs no publisher");
    let rdm = setup_crate(root, "https://rdm.example.org/api/").await;
    let error = create(&rdm, Some(rdm.test.auth.clone()))
        .await
        .expect_err("InvenioRDM needs a publisher");
    let body = serde_json::to_value(error.response_body()).unwrap();
    assert_eq!(body["code"], "requirements_unmet");
    let paths = body["findings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|finding| finding["path"].as_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>();
    assert_eq!(paths, ["http://schema.org/publisher"]);
}

#[tokio::test]
async fn unmet_link_findings() {
    let linked = setup().await;
    let link = create(&linked, Some(linked.test.auth.clone()))
        .await
        .unwrap();
    let mut stored = aruna_operations::jobs::repository::links::read_link(
        &linked.test.state.get_ctx().storage_handle,
        parse_document_id(&linked.document_id).unwrap(),
        Ulid::from_string(&link.link_id).unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    let finding = aruna_core::repository::rules::finding(
        "content_violation",
        Some("./".into()),
        None,
        "file/max_files".into(),
        "The export has 101 files, more than 100.".into(),
    );
    stored.status = LinkStatus::Failed {
        reason: LinkFailure::RequirementsUnmet(vec![finding]),
    };
    let shown = serde_json::to_value(response(stored, false, true)).unwrap();
    assert_eq!(shown["reason"], "requirements_unmet");
    assert_eq!(shown["kind"], "invenio");
    assert_eq!(shown["findings"][0]["rule"], "file/max_files");
    assert_eq!(shown["findings"][0]["severity"], "violation");
    assert!(
        serde_json::to_value(&link)
            .unwrap()
            .get("findings")
            .is_none()
    );
}

#[tokio::test]
async fn admin_rights_limited() {
    let linked = setup().await;
    let state = || State(linked.test.state.clone());
    let admin = || Extension(Some(linked.test.auth.clone()));
    let link = create(&linked, Some(linked.test.auth.clone()))
        .await
        .unwrap();
    // Another member created the link; the caller only administers the group.
    let document_id = parse_document_id(&linked.document_id).unwrap();
    let link_id = Ulid::from_string(&link.link_id).unwrap();
    let context = linked.test.state.get_ctx();
    let mut stored = aruna_operations::jobs::repository::links::read_link(
        &context.storage_handle,
        document_id,
        link_id,
    )
    .await
    .unwrap()
    .unwrap();
    stored.created_by = stranger(&linked).user_id;
    context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: REPOSITORY_LINK_KEYSPACE.into(),
            key: aruna_core::repository::link_key(document_id, link_id).into(),
            value: stored.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    let settings = Json(PatchLinkRequest {
        auto_publish: Some(true),
        ..PatchLinkRequest::default()
    });
    let refused = patch_link(state(), admin(), paths(&linked, &link), settings).await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
    let published = publish_link(state(), admin(), paths(&linked, &link)).await;
    assert!(matches!(published, Err(ServerError::Forbidden)));
    let pause = Json(PatchLinkRequest {
        paused: Some(true),
        ..PatchLinkRequest::default()
    });
    let Json(paused) = patch_link(state(), admin(), paths(&linked, &link), pause)
        .await
        .unwrap();
    assert_eq!(paused.status, "paused");
    assert_eq!(
        delete_link(state(), admin(), paths(&linked, &link))
            .await
            .unwrap(),
        StatusCode::NO_CONTENT
    );
}

/// Stores a pull link on the dataset the way a keep_updated import creates it.
async fn pull_link_for(linked: &Linked) -> RepositoryLink {
    use aruna_core::repository::{LinkDirection, LinkPull, RepositoryRecord};
    let now = std::time::SystemTime::now();
    let mut link = RepositoryLink {
        link_id: Ulid::generate(),
        document_id: parse_document_id(&linked.document_id).unwrap(),
        group_id: linked.test.group_id,
        connector_id: linked.connector_id,
        endpoint: "https://zenodo.example/api/".into(),
        owner_node: linked.test.state.get_node_id(),
        owner_node_url: "http://127.0.0.1:3000/api/v1".into(),
        created_by: linked.test.auth.user_id,
        status: LinkStatus::Enabled,
        auto_publish: false,
        public_files: false,
        metadata_json: "{}".into(),
        remote: LinkRemote::default(),
        last_push: None,
        active_job: None,
        sequence: 0,
        limits: aruna_core::structs::execution::job::RoCrateLimits::default(),
        created_at: now,
        updated_at: now,
        generation: 0,
        warning: None,
        kind: aruna_core::structs::execution::harvest::RepositoryConnectorKind::Invenio,
        direction: LinkDirection::Pull(Box::new(LinkPull {
            auto_update: false,
            options: Default::default(),
            target: aruna_core::structs::execution::job::ImportRoCrateTarget {
                bucket: "research".into(),
                prefix: "zenodo".into(),
            },
            latest_remote_id: None,
            latest_revision: None,
            last_checked_at: None,
            next_check_ms: u64::MAX,
            failures: 0,
            revision: None,
            local_changed: false,
        })),
    };
    let record = RepositoryRecord {
        id: "v1".into(),
        url: "https://zenodo.example/api/records/v1".into(),
        published: true,
        parent_id: "abcde-12345".into(),
        revision_id: 2,
        doi: Some("10.1234/v1".into()),
        html_url: None,
        concept_doi: None,
        in_review: false,
        identifiers: Vec::new(),
        warning: None,
    };
    link.hold(&record, Ulid::generate(), now);
    let change = LinkChange::Create {
        link: Box::new(link.clone()),
        secret: None,
    };
    change_link(linked.test.state.get_ctx().as_ref(), &link, change)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn pull_link_routes() {
    let linked = setup().await;
    let state = || State(linked.test.state.clone());
    let owner = || Extension(Some(linked.test.auth.clone()));
    let link = pull_link_for(&linked).await;
    let path = || Path((linked.document_id.clone(), link.link_id.to_string()));
    let Json(view) = get_link(state(), owner(), path()).await.unwrap();
    assert_eq!(view.direction, "pull");
    assert_eq!(view.auto_update, Some(false));
    assert_eq!(view.remote.record_id.as_deref(), Some("v1"));
    assert_eq!(view.remote.latest_remote_id.as_deref(), Some("v1"));
    assert_eq!((view.reason, view.pending), (None, false));
    // Push actions and push settings do not apply to a pull link.
    assert!(matches!(
        push_link(state(), owner(), path()).await,
        Err(ServerError::Conflict(_))
    ));
    assert!(matches!(
        accept_remote(state(), owner(), path()).await,
        Err(ServerError::Conflict(_))
    ));
    let settings = |request: PatchLinkRequest| patch_link(state(), owner(), path(), Json(request));
    let push_settings = PatchLinkRequest {
        auto_publish: Some(true),
        ..PatchLinkRequest::default()
    };
    assert!(matches!(
        settings(push_settings).await,
        Err(ServerError::BadRequestReason(_))
    ));
    let auto = PatchLinkRequest {
        auto_update: Some(true),
        ..PatchLinkRequest::default()
    };
    let Json(view) = settings(auto).await.unwrap();
    assert_eq!(view.auto_update, Some(true));
    // One lineage cannot be pushed and pulled at once on one dataset.
    assert!(matches!(
        create(&linked, Some(linked.test.auth.clone())).await,
        Err(ServerError::Conflict(_))
    ));
    let pause = PatchLinkRequest {
        paused: Some(true),
        ..PatchLinkRequest::default()
    };
    let Json(paused) = settings(pause).await.unwrap();
    assert_eq!(paused.status, "paused");
    assert!(matches!(
        pull_link(state(), owner(), path()).await,
        Err(ServerError::Conflict(_))
    ));
    let push = create(&linked, Some(linked.test.auth.clone()))
        .await
        .unwrap();
    assert_eq!(push.direction, "push");
    let resume = PatchLinkRequest {
        paused: Some(false),
        ..PatchLinkRequest::default()
    };
    assert!(matches!(
        settings(resume).await,
        Err(ServerError::Conflict(_))
    ));
}

#[tokio::test]
async fn link_requests_checked() {
    let linked = setup().await;
    let owner = || Extension(Some(linked.test.auth.clone()));
    let mut unknown = request(&linked);
    unknown.connector_id = Ulid::generate().to_string();
    let missing = Box::pin(create_link(
        State(linked.test.state.clone()),
        owner(),
        Path(linked.document_id.clone()),
        Json(unknown),
    ))
    .await;
    assert!(matches!(missing, Err(ServerError::NotFound)));

    // auto_update belongs to pull links, even next to valid push settings.
    let link = create(&linked, Some(linked.test.auth.clone()))
        .await
        .unwrap();
    let mixed = Json(PatchLinkRequest {
        auto_publish: Some(true),
        auto_update: Some(true),
        ..PatchLinkRequest::default()
    });
    let refused = patch_link(
        State(linked.test.state.clone()),
        owner(),
        paths(&linked, &link),
        mixed,
    )
    .await;
    assert!(matches!(refused, Err(ServerError::BadRequestReason(_))));
}

/// A new group member with the default viewer role: READ on the group's data and metadata.
async fn viewer(linked: &Linked) -> AuthContext {
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    let context = linked.test.state.get_ctx();
    let key = linked.test.group_id.to_bytes().to_vec();
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(value), ..
    }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.into(),
            key: key.clone().into(),
            txn_id: None,
        })
        .await
    else {
        panic!("group authorization missing");
    };
    let mut group = GroupAuthorizationDocument::from_bytes(&value).unwrap();
    let reader = stranger(linked);
    group
        .roles
        .values_mut()
        .find(|role| role.name == "viewer")
        .unwrap()
        .assigned_users
        .insert(reader.user_id);
    let actor = aruna_core::structs::identity::auth::Actor {
        node_id: linked.test.state.get_node_id(),
        user_id: linked.test.auth.user_id,
        realm_id: linked.test.auth.realm_id,
    };
    context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: AUTH_KEYSPACE.into(),
            key: key.into(),
            value: group.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        })
        .await;
    reader
}

#[tokio::test]
async fn readers_cannot_push() {
    use crate::metadata::RepositoryExportRequest;
    use crate::routes::repository::{SubmitRepositoryExport, export_record};
    let linked = setup().await;
    let reader = viewer(&linked).await;
    // The reader sees the dataset's links, so only the missing WRITE refuses the pushes below.
    let listed = list_repository_links(
        State(linked.test.state.clone()),
        Extension(Some(reader.clone())),
        Path(linked.document_id.clone()),
    )
    .await;
    assert!(listed.is_ok());
    let created = create(&linked, Some(reader.clone())).await;
    assert!(matches!(created, Err(ServerError::Forbidden)));
    let export = SubmitRepositoryExport {
        repository: RepositoryExportRequest {
            group_id: linked.test.group_id.to_string(),
            connector_id: linked.connector_id.to_string(),
            draft_id: None,
            new_version: None,
            metadata: serde_json::json!({}),
            publish: false,
            public_files: false,
            access_token: Some(TOKEN.into()),
        },
        idempotency_key: None,
    };
    let exported = export_record(
        State(linked.test.state.clone()),
        Extension(Some(reader)),
        Path(linked.document_id.clone()),
        Json(export),
    )
    .await;
    assert!(matches!(exported, Err(ServerError::Forbidden)));
}
