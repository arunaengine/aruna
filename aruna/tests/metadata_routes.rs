//! Tests metadata routes that forward to or fan out over other nodes through the peer handler.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_api::auth::RealmAuth;
use aruna_api::auth::ValidatedBearer;
use aruna_api::error::ServerError;
use aruna_api::metadata::*;
use aruna_api::routes::metadata::documents::*;
use aruna_api::routes::metadata::query::*;
use aruna_api::routes::metadata::references::*;
use aruna_api::routes::metadata::rocrate::*;
use aruna_api::routes::metadata::validation::*;
use aruna_api::server::state::ServerState;
use aruna_api::tests::metadata_fixtures::{
    create_test_document, drain_metadata_background, state_realm_nodes, test_realm_id, write_doc,
};
use aruna_core::MetaResourceId;
use aruna_core::keys::generate_signing_key;
use aruna_core::metadata::MetadataQueryResults;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_operations::driver::drive;
use aruna_operations::metadata::api::MetadataApiError;
use aruna_operations::metadata::api::read::query_metadata as run_query_metadata;
use aruna_operations::metadata::api::read_types::{ApiQueryMode, MetadataQueryRequest};
use aruna_operations::metadata::api::realm_search::search_metadata as run_search_metadata;
use aruna_operations::metadata::api::search_types::MetadataSearchRequest;
use aruna_operations::testing::installed_graph_handle;
use aruna_operations::testing::sign_token;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use std::sync::Arc;
use ulid::Ulid;

use aruna::inbound::initialize_incoming_fixture;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::identity::auth::{NodeCapabilities, TokenClaims};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::structs::placement::record::METADATA_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_graph::GraphHandle;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::realm::announce_presence::{
    AnnouncePresenceConfig, AnnouncePresenceOperation,
};
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use serde_json::json;
use std::collections::HashSet;
use tempfile::TempDir;

#[tokio::test]
async fn presence_tracks_config() {
    let realm_id = test_realm_id(31);
    let coordinator = spawn_metadata_node(realm_id).await;
    let remote = spawn_metadata_node(realm_id).await;

    coordinator
        .net
        .add_peer_addr(remote.net.endpoint_addr())
        .await;
    remote
        .net
        .add_peer_addr(coordinator.net.endpoint_addr())
        .await;
    install_realm_config(&[&coordinator, &remote], realm_id, None).await;

    let initial = state_realm_nodes(coordinator.state.as_ref()).await.unwrap();
    assert_eq!(initial, vec![coordinator.net.node_id()]);

    let remote_ctx = remote.state.get_ctx();
    drive(
        AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id,
            node_id: remote.net.node_id(),
            schedule_refresh: false,
        }),
        remote_ctx.as_ref(),
    )
    .await
    .unwrap();

    let discovered = state_realm_nodes(coordinator.state.as_ref()).await.unwrap();

    assert!(discovered.contains(&coordinator.net.node_id()));
    assert!(discovered.contains(&remote.net.node_id()));

    coordinator.net.shutdown().await;
    remote.net.shutdown().await;
}

#[tokio::test]
async fn query_forwards_token() {
    let test = setup_access_state().await;
    let token_auth: AuthContext =
        aruna_api::auth::handle_token(test.coordinator.state.as_ref(), &test.valid_bearer_token)
            .await
            .unwrap()
            .try_into()
            .unwrap();
    assert_eq!(token_auth, test.auth);

    let authorized = query_remote_names(
        &test,
        Some(token_auth.clone()),
        Some(ValidatedBearer::new_for_test(
            test.valid_bearer_token.clone(),
        )),
    )
    .await;
    assert_eq!(authorized.nodes_queried, 1);
    assert_eq!(authorized.nodes_failed, 0);
    assert_contains_name(&authorized.names, "Remote Public Dataset");
    assert_contains_name(&authorized.names, "Remote Private Dataset");

    let anonymous = query_remote_names(&test, None, None).await;
    assert_eq!(anonymous.nodes_failed, 0);
    assert_contains_name(&anonymous.names, "Remote Public Dataset");
    assert_excludes_name(&anonymous.names, "Remote Private Dataset");

    let no_forwardable_token = query_remote_names(&test, Some(token_auth.clone()), None).await;
    assert_eq!(no_forwardable_token.nodes_failed, 0);
    assert_contains_name(&no_forwardable_token.names, "Remote Public Dataset");
    assert_excludes_name(&no_forwardable_token.names, "Remote Private Dataset");

    let oversized_token = query_remote_names(
        &test,
        Some(token_auth.clone()),
        Some(ValidatedBearer::new_for_test("x".repeat(4097))),
    )
    .await;
    assert_eq!(oversized_token.nodes_failed, 0);
    assert_contains_name(&oversized_token.names, "Remote Public Dataset");
    assert_excludes_name(&oversized_token.names, "Remote Private Dataset");

    assert!(
        aruna_api::auth::handle_token(test.coordinator.state.as_ref(), "not-a-jwt")
            .await
            .is_err()
    );
    let invalid_forwarded_token = query_remote_names(
        &test,
        Some(token_auth),
        Some(ValidatedBearer::new_for_test("not-a-jwt")),
    )
    .await;
    assert_eq!(invalid_forwarded_token.nodes_queried, 1);
    assert_eq!(invalid_forwarded_token.nodes_failed, 1);
    assert_eq!(
        invalid_forwarded_token.failed_partitions,
        vec![test.remote.net.node_id()]
    );
    assert_excludes_name(&invalid_forwarded_token.names, "Remote Public Dataset");
    assert_excludes_name(&invalid_forwarded_token.names, "Remote Private Dataset");

    let ctx = test.coordinator.state.get_ctx();
    let strict = drive(
        run_query_metadata(
            test.coordinator.state.get_realm_id(),
            test.coordinator.state.get_node_id(),
            MetadataQueryRequest {
                auth: None,
                bearer_token: Some("not-a-jwt".to_string()),
                graph_iris: None,
                query: "SELECT DISTINCT ?name WHERE { ?s <http://schema.org/name> ?name }"
                    .to_string(),
                mode: Some(ApiQueryMode::Distributed),
                target_nodes: Some(vec![test.remote.net.node_id()]),
                allow_partial: false,
            },
        ),
        ctx.as_ref(),
    )
    .await;
    assert!(matches!(strict, Err(MetadataApiError::ServiceUnavailable)));

    test.shutdown().await;
}

#[tokio::test]
async fn private_get_forwards() {
    let test = setup_access_state().await;
    let (_, Json(listed)) = list_metadata_documents(
        State(test.remote.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Query(ListMetadataQuery::default()),
    )
    .await
    .unwrap();
    let private = listed
        .documents
        .into_iter()
        .find(|document| document.document_path == "datasets/remote-private")
        .expect("private document exists on its holder");
    let (_, Json(path)) = get_metadata_path(
        State(test.coordinator.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test(
            test.valid_bearer_token.clone(),
        ))),
        Path(test.group_id.to_string()),
        Query(MetadataPathQuery {
            path: "datasets/remote-private".to_string(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(path.winner.document_id, private.document_id);
    assert!(path.conflicts.is_empty());

    let (_, Json(fetched)) = get_metadata_document(
        State(test.coordinator.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test(
            test.valid_bearer_token.clone(),
        ))),
        Path(private.document_id),
    )
    .await
    .unwrap();

    assert_eq!(fetched.document_path, "datasets/remote-private");
    test.shutdown().await;
}

#[tokio::test]
async fn user_writes_forward() {
    let test = setup_access_state().await;
    let denied = create_metadata_document(
        State(test.coordinator.state.clone()),
        RealmAuth(test.denied_auth.clone()),
        Extension(Some(ValidatedBearer::new_for_test(
            test.denied_bearer_token.clone(),
        ))),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/user-denied".to_string(),
            name: "User Denied".to_string(),
            description: "Forwarded write without group permission".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public: false,
            message: None,
            storage_location: None,
        })),
    )
    .await;
    // A device forwards only for the owner its realm config names, so
    // another user's token is refused before any group check.
    assert!(matches!(denied, Err(ServerError::Forbidden)));

    let bearer = || {
        Extension(Some(ValidatedBearer::new_for_test(
            test.valid_bearer_token.clone(),
        )))
    };

    // The owner binding does not replace the group check: a group the owner
    // holds no role in is still refused at the ingress.
    let foreign_group = Ulid::generate();
    install_group_documents(
        &test.remote,
        test.auth.realm_id,
        test.denied_auth.user_id,
        foreign_group,
    )
    .await;
    let unpermitted = create_metadata_document(
        State(test.coordinator.state.clone()),
        RealmAuth(test.auth.clone()),
        bearer(),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: foreign_group.to_string(),
            path: "datasets/user-unpermitted".to_string(),
            name: "User Unpermitted".to_string(),
            description: "Forwarded write into a foreign group".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public: false,
            message: None,
            storage_location: None,
        })),
    )
    .await;
    assert!(matches!(unpermitted, Err(ServerError::Forbidden)));
    let missing = MetaResourceId::from_parts(
        1,
        PlacementHandle::new(METADATA_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
        1,
    )
    .unwrap();
    let missing_result = delete_metadata_document(
        State(test.coordinator.state.clone()),
        RealmAuth(test.auth.clone()),
        bearer(),
        Path(missing.to_string()),
    )
    .await;
    assert!(matches!(missing_result, Err(ServerError::NotFound)));

    let (_, Json(created)) = create_metadata_document(
        State(test.coordinator.state.clone()),
        RealmAuth(test.auth.clone()),
        bearer(),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/user-forward".to_string(),
            name: "User Forward".to_string(),
            description: "Forwarded from a User node".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public: false,
            message: None,
            storage_location: None,
        })),
    )
    .await
    .unwrap();
    let document_id = created.summary.document_id;
    drain_metadata_background(test.remote.state.as_ref()).await;

    let _ = add_data_entity(
        State(test.coordinator.state.clone()),
        RealmAuth(test.auth.clone()),
        bearer(),
        Path(document_id.clone()),
        Json(json!({
            "@id": "./forwarded.txt",
            "@type": "File",
            "name": "forwarded.txt"
        })),
    )
    .await
    .unwrap();
    assert_eq!(
        delete_metadata_document(
            State(test.coordinator.state.clone()),
            RealmAuth(test.auth.clone()),
            bearer(),
            Path(document_id),
        )
        .await
        .unwrap(),
        StatusCode::NO_CONTENT
    );

    test.shutdown().await;
}

#[tokio::test]
async fn missing_config_fails() {
    let test = setup_access_state().await;
    let context = test.coordinator.state.get_ctx();
    let deleted = context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: (*test.coordinator.state.get_realm_id().as_bytes()).into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        deleted,
        Event::Storage(StorageEvent::DeleteResult { .. })
    ));

    let result = create_metadata_document(
        State(test.coordinator.state.clone()),
        RealmAuth(test.auth.clone()),
        Extension(Some(ValidatedBearer::new_for_test(
            test.valid_bearer_token.clone(),
        ))),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/missing-config".to_string(),
            name: "Missing Config".to_string(),
            description: "Origin must fail closed".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public: false,
            message: None,
            storage_location: None,
        })),
    )
    .await;
    assert!(matches!(result, Err(ServerError::ServiceUnavailable)));

    test.shutdown().await;
}

#[tokio::test]
async fn search_without_token() {
    let test = setup_access_state().await;
    let ctx = test.remote.state.get_ctx();
    installed_graph_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();

    let anonymous = search_remote_paths(&test, None, None).await;
    assert_eq!(anonymous.nodes_queried, 1);
    assert_eq!(anonymous.nodes_failed, 0);
    assert_contains_path(&anonymous.paths, "datasets/remote-public");
    assert_excludes_path(&anonymous.paths, "datasets/remote-private");

    test.shutdown().await;
}

async fn flush_node_search(node: &DistributedMetadataNode) {
    let ctx = node.state.get_ctx();
    installed_graph_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();
}

struct SearchPaginationCluster {
    auth: AuthContext,
    group_id: Ulid,
    nodes: Vec<DistributedMetadataNode>,
}

impl SearchPaginationCluster {
    fn coordinator(&self) -> &DistributedMetadataNode {
        &self.nodes[0]
    }

    async fn shutdown(self) {
        for node in self.nodes {
            node.net.shutdown().await;
        }
    }
}

async fn setup_search_cluster(node_count: usize) -> SearchPaginationCluster {
    let realm_signing_key = test_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();

    let mut nodes = Vec::new();
    for _ in 0..node_count {
        nodes.push(spawn_metadata_node(realm_id).await);
    }
    for i in 0..nodes.len() {
        for j in 0..nodes.len() {
            if i != j {
                let addr = nodes[j].net.endpoint_addr();
                nodes[i].net.add_peer_addr(addr).await;
            }
        }
    }
    let node_refs: Vec<&DistributedMetadataNode> = nodes.iter().collect();
    install_realm_config(&node_refs, realm_id, None).await;
    for node in &nodes {
        install_auth_documents(node, realm_id, user_id, group_id).await;
    }

    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    SearchPaginationCluster {
        auth,
        group_id,
        nodes,
    }
}

async fn seed_public_document(
    node: &DistributedMetadataNode,
    auth: &AuthContext,
    group_id: Ulid,
    path: &str,
    name: &str,
) {
    create_test_document(node.state.clone(), auth.clone(), group_id, path, name, true).await;
    drain_metadata_background(node.state.as_ref()).await;
    flush_node_search(node).await;
}

// Drives the coordinator against an explicit node set, matching how the other
// distributed metadata tests fan out (the REST route never sets target_nodes).
async fn search_cluster_page(
    cluster: &SearchPaginationCluster,
    query: &str,
    limit: usize,
    cursor: Option<String>,
) -> aruna_operations::metadata::api::search_types::MetadataSearchExecution {
    let coordinator = cluster.coordinator();
    let ctx = coordinator.state.get_ctx();
    let target_nodes = cluster
        .nodes
        .iter()
        .map(|node| node.net.node_id())
        .collect();
    drive(
        run_search_metadata(
            coordinator.state.get_realm_id(),
            coordinator.state.get_node_id(),
            MetadataSearchRequest {
                auth: Some(cluster.auth.clone()),
                bearer_token: None,
                graph_iris: None,
                query: query.to_string(),
                conforms_to: None,
                group_id: None,
                limit: Some(limit),
                cursor,
                mode: Some(ApiQueryMode::Distributed),
                target_nodes: Some(target_nodes),
            },
        ),
        ctx.as_ref(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn search_paginates_uniquely() {
    let cluster = setup_search_cluster(3).await;
    for index in 0..9 {
        let node = &cluster.nodes[index % cluster.nodes.len()];
        seed_public_document(
            node,
            &cluster.auth,
            cluster.group_id,
            &format!("datasets/corpus-{index}"),
            &format!("Corpus Document {index}"),
        )
        .await;
    }

    let mut seen_keys: Vec<(String, String)> = Vec::new();
    let mut seen_paths: HashSet<String> = HashSet::new();
    let mut cursor = None;
    let mut last_score = f32::INFINITY;
    let mut pages = 0;
    loop {
        let response = search_cluster_page(&cluster, "Corpus", 3, cursor.clone()).await;
        assert_eq!(response.fanout_stats.nodes_failed, 0);
        assert!(response.hits.len() <= 3);
        for hit in &response.hits {
            assert!(
                hit.score <= last_score,
                "scores must be non-increasing across pages"
            );
            last_score = hit.score;
            seen_keys.push((hit.graph_iri.clone(), hit.subject_iri.clone()));
            seen_paths.insert(hit.document_path.clone());
        }
        pages += 1;
        assert!(pages <= 20, "pagination failed to terminate");
        match response.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    let unique: HashSet<_> = seen_keys.iter().cloned().collect();
    assert_eq!(
        unique.len(),
        seen_keys.len(),
        "pages must not repeat a (graph_iri, subject_iri)"
    );
    for index in 0..9 {
        assert!(
            seen_paths.contains(&format!("datasets/corpus-{index}")),
            "every seeded document must appear across the pages"
        );
    }

    cluster.shutdown().await;
}

#[tokio::test]
async fn pagination_survives_failure() {
    let cluster = setup_search_cluster(3).await;
    for index in 0..9 {
        let node = &cluster.nodes[index % cluster.nodes.len()];
        seed_public_document(
            node,
            &cluster.auth,
            cluster.group_id,
            &format!("datasets/corpus-{index}"),
            &format!("Corpus Document {index}"),
        )
        .await;
    }

    let page1 = search_cluster_page(&cluster, "Corpus", 3, None).await;
    assert_eq!(page1.fanout_stats.nodes_failed, 0);
    let cursor = page1.next_cursor.clone().expect("more pages remain");

    // Drop a non-coordinator holder mid-session; paging must continue.
    cluster.nodes[2].net.shutdown().await;

    let page2 = search_cluster_page(&cluster, "Corpus", 3, Some(cursor)).await;
    assert!(
        page2.fanout_stats.nodes_failed >= 1,
        "the downed node must surface as a failed partition"
    );

    cluster.shutdown().await;
}

async fn search_cluster_group(
    cluster: &SearchPaginationCluster,
    query: &str,
    group_id: Option<Ulid>,
) -> aruna_operations::metadata::api::search_types::MetadataSearchExecution {
    let coordinator = cluster.coordinator();
    let ctx = coordinator.state.get_ctx();
    let target_nodes = cluster
        .nodes
        .iter()
        .map(|node| node.net.node_id())
        .collect();
    drive(
        run_search_metadata(
            coordinator.state.get_realm_id(),
            coordinator.state.get_node_id(),
            MetadataSearchRequest {
                auth: Some(cluster.auth.clone()),
                bearer_token: None,
                graph_iris: None,
                query: query.to_string(),
                conforms_to: None,
                group_id,
                limit: Some(50),
                cursor: None,
                mode: Some(ApiQueryMode::Distributed),
                target_nodes: Some(target_nodes),
            },
        ),
        ctx.as_ref(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn group_filter_fanout() {
    // Group filter must hold across every fanned-out node.
    let cluster = setup_search_cluster(3).await;
    let other_group = Ulid::generate();
    for node in &cluster.nodes {
        install_auth_documents(
            node,
            node.state.get_realm_id(),
            cluster.auth.user_id,
            other_group,
        )
        .await;
    }
    for index in 0..6 {
        let node = &cluster.nodes[index % cluster.nodes.len()];
        let (group_id, label) = if index % 2 == 0 {
            (cluster.group_id, "primary")
        } else {
            (other_group, "other")
        };
        seed_public_document(
            node,
            &cluster.auth,
            group_id,
            &format!("datasets/fanout-{label}-{index}"),
            &format!("Fanout Document {index}"),
        )
        .await;
    }

    let filtered = search_cluster_group(&cluster, "Fanout", Some(cluster.group_id)).await;
    assert!(!filtered.hits.is_empty());
    assert!(
        filtered
            .hits
            .iter()
            .all(|hit| hit.group_id == cluster.group_id.to_string()),
        "the group filter must hold across every fanned-out node"
    );

    let unfiltered = search_cluster_group(&cluster, "Fanout", None).await;
    let groups: HashSet<String> = unfiltered
        .hits
        .iter()
        .map(|hit| hit.group_id.clone())
        .collect();
    assert!(groups.contains(&cluster.group_id.to_string()));
    assert!(groups.contains(&other_group.to_string()));

    cluster.shutdown().await;
}

struct DistributedAccessState {
    auth: AuthContext,
    denied_auth: AuthContext,
    group_id: Ulid,
    valid_bearer_token: String,
    denied_bearer_token: String,
    coordinator: DistributedMetadataNode,
    remote: DistributedMetadataNode,
}

impl DistributedAccessState {
    async fn shutdown(self) {
        self.coordinator.net.shutdown().await;
        self.remote.net.shutdown().await;
    }
}

struct DistributedMetadataNode {
    _node_dir: TempDir,
    net: NetHandle,
    state: Arc<ServerState>,
}

struct QueryNamesResult {
    names: Vec<String>,
    nodes_queried: usize,
    nodes_failed: usize,
    failed_partitions: Vec<aruna_core::NodeId>,
}

struct SearchPathsResult {
    paths: Vec<String>,
    nodes_queried: usize,
    nodes_failed: usize,
}

async fn setup_access_state() -> DistributedAccessState {
    let realm_signing_key = test_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let denied_user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    let coordinator = spawn_metadata_node(realm_id).await;
    let remote = spawn_metadata_node(realm_id).await;
    let nodes = [&coordinator, &remote];

    coordinator
        .net
        .add_peer_addr(remote.net.endpoint_addr())
        .await;
    remote
        .net
        .add_peer_addr(coordinator.net.endpoint_addr())
        .await;
    // The device forwards for the owner its realm config names, so the
    // fixture binds it to the user whose token the writes carry.
    install_realm_config(&nodes, realm_id, Some((coordinator.net.node_id(), user_id))).await;
    install_auth_documents(&remote, realm_id, user_id, group_id).await;

    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let denied_auth = AuthContext {
        user_id: denied_user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    create_test_document(
        remote.state.clone(),
        auth.clone(),
        group_id,
        "datasets/remote-public",
        "Remote Public Dataset",
        true,
    )
    .await;
    create_test_document(
        remote.state.clone(),
        auth.clone(),
        group_id,
        "datasets/remote-private",
        "Remote Private Dataset",
        false,
    )
    .await;
    drain_metadata_background(remote.state.as_ref()).await;

    let valid_bearer_token = sign_token(&realm_signing_key, &test_token_claims(realm_id, user_id));
    let denied_bearer_token = sign_token(
        &realm_signing_key,
        &test_token_claims(realm_id, denied_user_id),
    );

    DistributedAccessState {
        auth,
        denied_auth,
        group_id,
        valid_bearer_token,
        denied_bearer_token,
        coordinator,
        remote,
    }
}

async fn spawn_metadata_node(realm_id: RealmId) -> DistributedMetadataNode {
    let node_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(node_dir.path().to_str().unwrap()).unwrap();
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage_handle.clone(),
    )
    .await
    .unwrap();
    let graph_handle = aruna_graph::GraphHandle::open(
        node_dir.path().join("metadata"),
        net.node_id(),
        Some(net.document_sync_node()),
        Some(net.document_sync_database()),
        aruna_graph::GraphOptions::default(),
    )
    .unwrap();
    let metadata_handle = MetadataHandle::new(
        node_dir.path().join("metadata"),
        storage_handle.clone(),
        Some(net.clone()),
        Default::default(),
    );
    let context = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        graph_handle: Some(graph_handle),
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
        outbound_handle: None,
    });
    initialize_incoming_fixture(context.clone());
    let state = Arc::new(
        ServerState::new(
            context,
            realm_id,
            net.node_id(),
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );

    DistributedMetadataNode {
        _node_dir: node_dir,
        net,
        state,
    }
}

async fn install_realm_config(
    nodes: &[&DistributedMetadataNode],
    realm_id: RealmId,
    user_node: Option<(aruna_core::NodeId, aruna_core::UserId)>,
) {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for (band, node) in nodes.iter().enumerate() {
        let kind = match user_node {
            Some((node_id, owner)) if node_id == node.net.node_id() => {
                RealmNodeKind::User { owner }
            }
            _ => RealmNodeKind::Server,
        };
        config.ensure_node(node.net.node_id(), kind);
        config.seed_job_control(node.net.node_id(), band as u32);
    }

    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: aruna_core::UserId::nil(realm_id),
            realm_id,
        };
        write_doc(
            &node.state.get_ctx(),
            REALM_CONFIG_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            config.to_bytes(&actor).unwrap().into(),
        )
        .await;
        node.net.refresh_document_peers(&config).await.unwrap();
    }
}

async fn install_auth_documents(
    node: &DistributedMetadataNode,
    realm_id: RealmId,
    user_id: aruna_core::UserId,
    group_id: Ulid,
) {
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id,
        realm_id,
    };
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    write_doc(
        &node.state.get_ctx(),
        AUTH_KEYSPACE,
        (*realm_id.as_bytes()).into(),
        realm_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
    install_group_documents(node, realm_id, user_id, group_id).await;
}

/// A group owned by `owner`, with its authorization document, so a caller
/// outside it is refused by the group check rather than by its absence.
async fn install_group_documents(
    node: &DistributedMetadataNode,
    realm_id: RealmId,
    owner: aruna_core::UserId,
    group_id: Ulid,
) {
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: owner,
        realm_id,
    };
    let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let group = Group {
        display_name: "distributed-metadata-group".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    let context = node.state.get_ctx();

    write_doc(
        &context,
        AUTH_KEYSPACE,
        group_id.to_bytes().into(),
        group_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
    write_doc(
        &context,
        GROUP_KEYSPACE,
        group_id.to_bytes().into(),
        group.to_bytes(&actor).unwrap().into(),
    )
    .await;
}

async fn query_remote_names(
    test: &DistributedAccessState,
    auth: Option<AuthContext>,
    bearer_token: Option<ValidatedBearer>,
) -> QueryNamesResult {
    let ctx = test.coordinator.state.get_ctx();
    let result = drive(
        run_query_metadata(
            test.coordinator.state.get_realm_id(),
            test.coordinator.state.get_node_id(),
            MetadataQueryRequest {
                auth,
                bearer_token: bearer_token_string(bearer_token),
                graph_iris: None,
                query: "SELECT DISTINCT ?name WHERE { ?s <http://schema.org/name> ?name }"
                    .to_string(),
                mode: Some(ApiQueryMode::Distributed),
                target_nodes: Some(vec![test.remote.net.node_id()]),
                allow_partial: true,
            },
        ),
        ctx.as_ref(),
    )
    .await
    .unwrap();
    let MetadataQueryResults::Solutions(rows) = result.results else {
        panic!("expected SELECT solutions");
    };
    QueryNamesResult {
        names: rows.into_iter().flat_map(|row| row.into_values()).collect(),
        nodes_queried: result.fanout_stats.nodes_queried,
        nodes_failed: result.fanout_stats.nodes_failed,
        failed_partitions: result.fanout_stats.failed_partitions,
    }
}

async fn search_remote_paths(
    test: &DistributedAccessState,
    auth: Option<AuthContext>,
    bearer_token: Option<ValidatedBearer>,
) -> SearchPathsResult {
    let ctx = test.coordinator.state.get_ctx();
    let result = drive(
        run_search_metadata(
            test.coordinator.state.get_realm_id(),
            test.coordinator.state.get_node_id(),
            MetadataSearchRequest {
                auth,
                bearer_token: bearer_token_string(bearer_token),
                graph_iris: None,
                query: "Remote".to_string(),
                conforms_to: None,
                group_id: None,
                limit: Some(10),
                cursor: None,
                mode: Some(ApiQueryMode::Distributed),
                target_nodes: Some(vec![test.remote.net.node_id()]),
            },
        ),
        ctx.as_ref(),
    )
    .await
    .unwrap();
    SearchPathsResult {
        paths: result
            .hits
            .into_iter()
            .map(|hit| hit.document_path)
            .collect(),
        nodes_queried: result.fanout_stats.nodes_queried,
        nodes_failed: result.fanout_stats.nodes_failed,
    }
}

fn assert_contains_name(names: &[String], expected: &str) {
    assert!(
        names.iter().any(|name| name.contains(expected)),
        "expected {names:?} to contain {expected:?}"
    );
}

fn assert_excludes_name(names: &[String], unexpected: &str) {
    assert!(
        !names.iter().any(|name| name.contains(unexpected)),
        "expected {names:?} not to contain {unexpected:?}"
    );
}

fn assert_contains_path(paths: &[String], expected: &str) {
    assert!(
        paths.iter().any(|path| path == expected),
        "expected {paths:?} to contain {expected:?}"
    );
}

fn assert_excludes_path(paths: &[String], unexpected: &str) {
    assert!(
        !paths.iter().any(|path| path == unexpected),
        "expected {paths:?} not to contain {unexpected:?}"
    );
}

fn test_signing_key() -> SigningKey {
    generate_signing_key()
}

fn test_token_claims(realm_id: RealmId, user_id: aruna_core::UserId) -> TokenClaims {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    TokenClaims {
        sub: user_id.to_string(),
        iss: realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: Ulid::generate().to_string(),
        sid: None,
        session_kind: None,
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
        name: None,
        via: None,
        auth_time: None,
    }
}
