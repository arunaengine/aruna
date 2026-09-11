use crate::server_state::ServerState;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::{
    Actor, Group, GroupAuthorizationDocument, NodeCapabilities, RealmAuthorizationDocument,
    RealmConfigDocument, RealmId,
};
use aruna_core::{NodeId, UserId};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::storage::{FjallStorage, StorageHandle};
use byteview::ByteView;
use std::sync::Arc;
use tempfile::TempDir;
use ulid::Ulid;

pub(crate) fn test_storage() -> (TempDir, StorageHandle) {
    let directory = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    (directory, storage)
}

pub(crate) fn test_context(storage: StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

pub(crate) async fn test_state(
    context: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: NodeId,
    capabilities: NodeCapabilities,
) -> ServerState {
    ServerState::new(
        context,
        realm_id,
        node_id,
        capabilities,
        false,
        None,
        JobsRuntime::new(),
    )
    .await
}

pub(crate) async fn write_doc(
    context: &Arc<DriverContext>,
    key_space: &str,
    key: ByteView,
    value: ByteView,
) {
    let event = context
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

pub(crate) async fn seed_realm_auth(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    actor: &Actor,
) {
    write_doc(
        context,
        AUTH_KEYSPACE,
        (*realm_id.as_bytes()).into(),
        RealmAuthorizationDocument::new_default_realm_doc(realm_id)
            .to_bytes(actor)
            .unwrap()
            .into(),
    )
    .await;
}

pub(crate) async fn seed_realm_config(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    actor: &Actor,
) {
    write_doc(
        context,
        REALM_CONFIG_KEYSPACE,
        (*realm_id.as_bytes()).into(),
        RealmConfigDocument::default_for_realm(realm_id, Vec::new())
            .to_bytes(actor)
            .unwrap()
            .into(),
    )
    .await;
}

pub(crate) async fn seed_group_docs(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    actor: &Actor,
    group_id: Ulid,
    display_name: &str,
    owner: UserId,
) {
    let group_auth = GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
    let group = Group {
        display_name: display_name.to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    write_doc(
        context,
        AUTH_KEYSPACE,
        group_id.to_bytes().into(),
        group_auth.to_bytes(actor).unwrap().into(),
    )
    .await;
    write_doc(
        context,
        GROUP_KEYSPACE,
        group_id.to_bytes().into(),
        group.to_bytes(actor).unwrap().into(),
    )
    .await;
}
