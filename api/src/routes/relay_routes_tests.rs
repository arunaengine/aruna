use std::sync::Arc;

use super::{RELAY_HOP_HEADER, relay_middleware};
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{MANAGEMENT_URL_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use axum::middleware::from_fn_with_state;
use axum::routing::get;
use ed25519_dalek::SigningKey;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tower::ServiceExt;
use ulid::Ulid;

struct Fixture {
    _dir: TempDir,
    state: Arc<ServerState>,
}

async fn write_row(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let event = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn setup(installed: Vec<String>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let realm_id = RealmId::from_bytes(
        SigningKey::from_bytes(&[7u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let owner = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
    let state = Arc::new(
        ServerState::new(
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: Some(TaskHandle::new()),
                compute_handle: None,
            }),
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await,
    );
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(node_id, RealmNodeKind::User { owner });
    write_row(
        &state,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        config.to_bytes(&actor).unwrap(),
    )
    .await;
    if !installed.is_empty() {
        write_row(
            &state,
            MANAGEMENT_URL_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            postcard::to_allocvec(&installed).unwrap(),
        )
        .await;
    }
    Fixture { _dir: dir, state }
}

fn relay_app(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/access/token", get(|| async { StatusCode::IM_A_TEAPOT }))
        .layer(from_fn_with_state(state.clone(), relay_middleware))
        .with_state(state)
}

async fn fake_upstream() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().fallback(|| async {
        (
            [(header::CONTENT_TYPE, "application/json")],
            "{\"relayed\":true}",
        )
    });
    let handle = tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });
    (format!("http://{address}"), handle)
}

fn token_request() -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri("/access/token")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn relays_to_management() {
    let (upstream, handle) = fake_upstream().await;
    let fixture = setup(vec![upstream]).await;
    let response = relay_app(fixture.state.clone())
        .oneshot(token_request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"{\"relayed\":true}");
    handle.abort();
}

#[tokio::test]
async fn no_management_target() {
    // A resolved realm config with no reachable management node answers 503.
    let fixture = setup(Vec::new()).await;
    let response = relay_app(fixture.state.clone())
        .oneshot(token_request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn hop_stays_local() {
    // A request already carrying a hop is answered locally, never relayed on.
    let fixture = setup(vec!["http://127.0.0.1:1".to_string()]).await;
    let request = Request::builder()
        .method("GET")
        .uri("/access/token")
        .header(RELAY_HOP_HEADER, "peer")
        .body(Body::empty())
        .unwrap();
    let response = relay_app(fixture.state.clone())
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
}
