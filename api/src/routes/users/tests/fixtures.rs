use crate::routes::tests::fixtures::{test_context, test_state, test_storage};
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::keys::generate_signing_key;
use aruna_core::structs::{AuthContext, NodeCapabilities, RealmId};
use std::sync::Arc;
use tempfile::TempDir;
use ulid::Ulid;

pub(crate) async fn setup_state() -> (Arc<ServerState>, TempDir) {
    let (tempdir, storage_handle) = test_storage();
    let driver_ctx = Arc::new(test_context(storage_handle));
    let realm_signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let state = Arc::new(
        test_state(
            driver_ctx,
            realm_id,
            iroh::SecretKey::generate().public(),
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );
    (state, tempdir)
}

pub(crate) fn realm_auth(realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    }
}
