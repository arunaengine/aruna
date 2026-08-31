use crate::server_state::ServerState;
use aruna_core::structs::{AuthContext, NodeCapabilities, RealmId};
use aruna_core::types::UserId;
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::storage::FjallStorage;
use axum::Router;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use ulid::Ulid;

pub(super) async fn setup_state() -> (TempDir, ServerState, AuthContext) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId::from_bytes([3; 32]);
    let user_id = UserId::local(Ulid::from_bytes([4; 16]), realm_id);
    let state = ServerState::new(
        context,
        realm_id,
        iroh::SecretKey::generate().public(),
        NodeCapabilities::user_node(realm_id).unwrap(),
        false,
        None,
        JobsRuntime::new(),
    )
    .await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    (dir, state, auth)
}

pub(super) async fn spawn_mock(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{address}"), handle)
}
