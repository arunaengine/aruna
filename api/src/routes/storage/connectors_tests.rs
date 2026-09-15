use super::*;
use crate::openapi::ApiDoc;
use crate::tests::routes::{
    seed_group_docs, seed_realm_auth, seed_realm_config, test_context, test_state, test_storage,
};
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use serde_json::json;
use tempfile::TempDir;

struct TestState {
    _storage_dir: TempDir,
    auth: AuthContext,
    other_auth: AuthContext,
    group_id: Ulid,
    state: Arc<ServerState>,
}

#[tokio::test]
async fn connector_crud_redacts() {
    let test = setup_state().await;

    let (_, Json(created)) = create_source_connector(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(CreateConnectorRequest {
            name: "refdata".to_string(),
            kind: ApiConnectorKind::S3,
            public_config: HashMap::from([
                ("bucket".to_string(), "reads".to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
            ]),
            secret_config: HashMap::from([
                ("access_key_id".to_string(), "AKIA".to_string()),
                ("secret_access_key".to_string(), "super-secret".to_string()),
            ]),
        }),
    )
    .await
    .unwrap();

    assert_eq!(created.name, "refdata");
    assert!(created.has_secret_config);
    assert!(!created.public_config.contains_key("access_key_id"));

    let (_, Json(listed)) = list_source_connectors(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
    )
    .await
    .unwrap();

    assert_eq!(listed.connectors.len(), 1);
    assert!(listed.connectors[0].has_secret_config);

    let (_, Json(fetched)) = get_source_connector(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path((test.group_id.to_string(), created.connector_id.clone())),
    )
    .await
    .unwrap();

    assert_eq!(fetched, created);

    let (_, Json(replaced)) = replace_source_connector(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path((test.group_id.to_string(), created.connector_id.clone())),
        Json(ReplaceConnectorRequest {
            name: "refdata-updated".to_string(),
            kind: ApiConnectorKind::S3,
            public_config: HashMap::from([
                ("bucket".to_string(), "reads-v2".to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ("skip_signature".to_string(), "true".to_string()),
            ]),
            secret_config: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(replaced.name, "refdata-updated");
    assert!(!replaced.has_secret_config);

    let delete_status = delete_source_connector(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path((test.group_id.to_string(), created.connector_id.clone())),
    )
    .await
    .unwrap();

    assert_eq!(delete_status, StatusCode::NO_CONTENT);

    let get_result = get_source_connector(
        State(test.state.clone()),
        Extension(Some(test.auth)),
        Path((test.group_id.to_string(), created.connector_id)),
    )
    .await;
    assert!(matches!(get_result, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn connectors_require_permission() {
    let test = setup_state().await;

    let result = create_source_connector(
        State(test.state),
        Extension(Some(test.other_auth)),
        Path(test.group_id.to_string()),
        Json(CreateConnectorRequest {
            name: "forbidden".to_string(),
            kind: ApiConnectorKind::Http,
            public_config: HashMap::from([(
                "endpoint".to_string(),
                "https://example.org".to_string(),
            )]),
            secret_config: HashMap::new(),
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn check_requires_permission() {
    let test = setup_state().await;

    let result = check_source_connector(
        State(test.state),
        Extension(Some(test.other_auth)),
        Path(test.group_id.to_string()),
        Json(SourceConnectorRequest {
            name: "source".to_string(),
            kind: ApiConnectorKind::Http,
            public_config: HashMap::from([(
                "endpoint".to_string(),
                "https://example.org".to_string(),
            )]),
            secret_config: HashMap::new(),
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn check_returns_failure() {
    let test = setup_state().await;

    let Json(result) = check_source_connector(
        State(test.state),
        Extension(Some(test.auth)),
        Path(test.group_id.to_string()),
        Json(SourceConnectorRequest {
            name: "source".to_string(),
            kind: ApiConnectorKind::Http,
            public_config: HashMap::from([(
                "endpoint".to_string(),
                "https://example.org".to_string(),
            )]),
            secret_config: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    assert!(matches!(
        result,
        ConnectorCheckResponse::Failure(ConnectorCheckFailure { ok: false, .. })
    ));
}

#[test]
fn check_maps_unreachable() {
    let error = aruna_operations::staging::check_source::CheckSourceError::Staging(
        aruna_core::errors::StagingSourceError::CheckError("connection refused".to_string()),
    );

    assert_eq!(check_error_message(&error), "connector is unreachable");
}

#[tokio::test]
async fn stored_check_resolves() {
    let test = setup_state().await;
    let (_, Json(connector)) = create_source_connector(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(CreateConnectorRequest {
            name: "stored-source".to_string(),
            kind: ApiConnectorKind::S3,
            public_config: HashMap::from([
                ("bucket".to_string(), "reads".to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
            ]),
            secret_config: HashMap::from([
                ("access_key_id".to_string(), "AKIA".to_string()),
                ("secret_access_key".to_string(), "stored-secret".to_string()),
            ]),
        }),
    )
    .await
    .unwrap();

    let Json(result) = check_stored_connector(
        State(test.state),
        Extension(Some(test.auth)),
        Path((test.group_id.to_string(), connector.connector_id)),
    )
    .await
    .unwrap();

    assert_eq!(
        result,
        ConnectorCheckResponse::Failure(ConnectorCheckFailure {
            ok: false,
            error: "connector check is unavailable".to_string(),
        })
    );
}

#[test]
fn check_serializes_success() {
    let result = ConnectorCheckResponse::Success(ConnectorCheckSuccess {
        ok: true,
        latency_ms: 12,
    });

    assert_eq!(
        serde_json::to_value(result).unwrap(),
        json!({"ok": true, "latency_ms": 12})
    );
}

#[tokio::test]
async fn entries_reject_traversal() {
    let test = setup_state().await;

    let result = list_connector_entries(
        State(test.state),
        Extension(Some(test.auth)),
        Path((test.group_id.to_string(), Ulid::generate().to_string())),
        Query(ConnectorEntriesQuery {
            path: "../secret".to_string(),
            limit: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::BadRequest)));
}

#[tokio::test]
async fn entries_require_permission() {
    let test = setup_state().await;

    let result = list_connector_entries(
        State(test.state),
        Extension(Some(test.other_auth)),
        Path((test.group_id.to_string(), Ulid::generate().to_string())),
        Query(ConnectorEntriesQuery {
            path: String::new(),
            limit: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[test]
fn entries_normalize_prefix() {
    assert_eq!(normalize_browse_path("prefix").unwrap(), "prefix/");
    assert_eq!(normalize_browse_path("prefix/").unwrap(), "prefix/");
    assert_eq!(normalize_browse_path("").unwrap(), "");
}

#[test]
fn list_preserves_reason() {
    let error = map_list_error(ListStagingError::Staging(
        aruna_core::errors::StagingSourceError::ListError("not an index".to_string()),
    ));

    assert!(matches!(
        error,
        ServerError::BadGatewayReason(message) if message == "List error: not an index"
    ));
}

#[test]
fn referenced_connector_conflicts() {
    // A still-referenced credential is a policy refusal, not an internal error.
    assert!(matches!(
        map_replace_error(ReplaceSourceError::ReferencedByObjectVersion),
        ServerError::Conflict(_)
    ));
    assert!(matches!(
        map_delete_error(DeleteSourceError::ReferencedByObjectVersion),
        ServerError::Conflict(_)
    ));
}

#[test]
fn openapi_has_connectors() {
    let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

    assert!(
        openapi["paths"]
            .get("/data/groups/{group_id}/connectors")
            .is_some()
    );
    assert!(
        openapi["paths"]
            .get("/data/groups/{group_id}/connectors/{connector_id}")
            .is_some()
    );
    assert!(
        openapi["paths"]
            .get("/data/groups/{group_id}/connectors/check")
            .is_some()
    );
    assert!(
        openapi["paths"]
            .get("/data/groups/{group_id}/connectors/{connector_id}/check")
            .is_some()
    );
    assert!(
        openapi["paths"]
            .get("/data/groups/{group_id}/connectors/{connector_id}/entries")
            .is_some()
    );
    assert_eq!(
        openapi["components"]["schemas"]["ApiSourceConnectorKind"]["type"],
        json!("string")
    );
}

async fn setup_state() -> TestState {
    let (storage_dir, storage_handle) = test_storage();
    let realm_id = aruna_core::structs::identity::realm::RealmId([3u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let other_user_id = UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    let driver_ctx = Arc::new(test_context(storage_handle));
    let group_id = Ulid::generate();

    // Request-policy loading fails closed without the realm config document.
    seed_realm_config(&driver_ctx, realm_id, &actor).await;
    seed_realm_auth(&driver_ctx, realm_id, &actor).await;
    seed_group_docs(
        &driver_ctx,
        realm_id,
        &actor,
        group_id,
        "connector-group",
        user_id,
    )
    .await;

    let state = Arc::new(
        test_state(
            driver_ctx,
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );

    TestState {
        _storage_dir: storage_dir,
        auth: AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        other_auth: AuthContext {
            user_id: other_user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        group_id,
        state,
    }
}
