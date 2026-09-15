use super::*;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::RELATIONSHIP_OUT_KEYSPACE;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::{
    SyncMode, SyncRelationship, SyncState, SyncStatusSnapshot, sync_relationship_key,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::storage::FjallStorage;
use std::time::SystemTime;

#[tokio::test]
async fn preflight_discloses_cleanup() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let realm_id = RealmId::from_bytes(
        ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let local_node = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let remote_node = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let state = ServerState::new(
        Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }),
        realm_id,
        local_node,
        NodeCapabilities::user_node(realm_id).unwrap(),
        false,
        None,
        JobsRuntime::new(),
    )
    .await;
    let relationship = SyncRelationship {
        id: Ulid::from_bytes([3u8; 16]),
        source: ArunaArn::s3_bucket(realm_id, local_node, "bucket").unwrap(),
        target: ArunaArn::s3_bucket(realm_id, remote_node, "replica").unwrap(),
        mode: SyncMode::Continuous,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: true,
        created_by: aruna_core::UserId::nil(realm_id),
        created_at: SystemTime::UNIX_EPOCH,
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    };
    assert!(matches!(
        storage
            .send_storage_effect(StorageEffect::Write {
                key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
                key: sync_relationship_key("bucket", relationship.id).into(),
                value: relationship.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let disclosed = list_sync_effects(&state, "bucket").await.unwrap();

    assert_eq!(disclosed.len(), 1);
    assert_eq!(disclosed[0].relationship_id, relationship.id.to_string());
    assert_eq!(disclosed[0].direction, "outgoing");
    assert_eq!(
        disclosed[0].action,
        "remove_local_relationship_and_repair_remote_mirror"
    );
    assert!(!disclosed[0].blocker);
}
