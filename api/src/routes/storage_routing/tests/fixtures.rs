use crate::routes::tests::fixtures::{
    seed_group_docs, seed_realm_auth, seed_realm_config, test_context, test_state, test_storage,
};
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::structs::{Actor, AuthContext, BucketInfo, NodeCapabilities, RealmId};
use aruna_operations::driver::DriverContext;
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::TempDir;
use ulid::Ulid;

pub(crate) struct TestState {
    _storage_dir: TempDir,
    pub(crate) auth: AuthContext,
    pub(crate) other_auth: AuthContext,
    pub(crate) group_id: Ulid,
    pub(crate) bucket: String,
    pub(crate) state: Arc<ServerState>,
}

pub(crate) async fn setup_state() -> TestState {
    let (storage_dir, storage_handle) = test_storage();
    let realm_id = RealmId([3u8; 32]);
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
    let bucket = "routed".to_string();
    let bucket_info = BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };

    // Request-policy loading fails closed without the realm config document.
    seed_realm_config(&driver_ctx, realm_id, &actor).await;
    seed_realm_auth(&driver_ctx, realm_id, &actor).await;
    seed_group_docs(
        &driver_ctx,
        realm_id,
        &actor,
        group_id,
        "routing-group",
        user_id,
    )
    .await;
    write_doc(
        &driver_ctx,
        S3_BUCKET_KEYSPACE,
        bucket.as_bytes().to_vec().into(),
        bucket_info.to_bytes().unwrap().into(),
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
        bucket,
        state,
    }
}

async fn write_doc(
    driver_ctx: &Arc<DriverContext>,
    key_space: &str,
    key: byteview::ByteView,
    value: byteview::ByteView,
) {
    let event = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}
