//! Route tests of the bucket key holders: list them, then remove an explicit grant.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::routes::{seed_group_docs, seed_realm_auth, seed_realm_config, test_state};
use aruna_blob::blob::BlobHandler;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE, KEY_COPY_KEYSPACE,
    S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, BucketKeyRecord, EncryptionMode, GrantState, SealedCopy,
};
use aruna_core::structs::storage::format::Compression;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_storage::FjallStorage;
use std::collections::HashMap;
use std::time::SystemTime;
use tempfile::TempDir;

const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);
const BUCKET: &str = "keys";

struct Fixture {
    _dir: TempDir,
    state: Arc<ServerState>,
    owner: AuthContext,
    grantee: UserId,
}

/// A node with a blob plane, a group owned by `owner` and one bucket of that group.
async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let storage = FjallStorage::open(root).unwrap();
    storage.open_vault(aruna_core::node_vault::NodeVaultKey::random());
    let realm_id = RealmId([3; 32]);
    let config = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        realm_id,
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    };
    let net = NetHandle::new(config, storage.clone()).await.unwrap();
    let backend = BackendConfig {
        backend_type: Backend::FileSystem,
        root: format!("{root}/blobs"),
        service_config: HashMap::new(),
        bucket_prefix: Some("aruna-".to_string()),
        max_bucket_size: Some(1_000_000),
        multipart_bucket: Some("multipart".to_string()),
        timeouts: Default::default(),
    };
    let blob = BlobHandler::new(backend, storage.clone(), net.clone())
        .await
        .unwrap();
    let node_id = net.node_id();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net),
        blob_handle: Some(blob),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let owner = UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let group_id = Ulid::generate();
    seed_realm_config(&context, realm_id, &actor).await;
    seed_realm_auth(&context, realm_id, &actor).await;
    seed_group_docs(&context, realm_id, &actor, group_id, "keys", owner).await;
    let info = BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: owner,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
    };
    let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
    let state = Arc::new(test_state(context, realm_id, node_id, capabilities).await);
    write_row(
        &state,
        S3_BUCKET_KEYSPACE,
        BUCKET.into(),
        info.to_bytes().unwrap(),
    )
    .await;
    Fixture {
        _dir: dir,
        state,
        owner: AuthContext {
            user_id: owner,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        grantee: UserId::local(Ulid::generate(), realm_id),
    }
}

async fn write_row(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let write = StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    state
        .get_ctx()
        .storage_handle
        .send_storage_effect(write)
        .await;
}

fn copy(user_id: UserId, seed: u8) -> SealedCopy {
    SealedCopy {
        key: BucketKeyRef::new(BUCKET_ID, 1),
        user_id,
        key_record: Ulid::from_bytes([seed; 16]),
        key_id: "slot".to_string(),
        enc: [seed; 32],
        ciphertext: vec![seed; 48],
        created_at_ms: 1,
    }
}

#[tokio::test]
async fn lists_then_removes() {
    let fixture = fixture().await;
    let (owner, grantee) = (fixture.owner.user_id, fixture.grantee);
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(BUCKET_ID),
        key_generation: 1,
        storage_generation: 1,
        ..Default::default()
    };
    let key = BucketKeyRef::new(BUCKET_ID, 1);
    let record = BucketKeyRecord::new(key, Ulid::from_bytes([8; 16]), [7; 32], 1);
    let grant = BucketHolder {
        bucket_id: BUCKET_ID,
        user_id: grantee,
        origin: HolderOrigin::Explicit,
        state: GrantState::Ready,
        granted_by: owner,
        granted_at_ms: 1,
    };
    let (owner_copy, grantee_copy) = (copy(owner, 1), copy(grantee, 2));
    let rows = [
        (
            BUCKET_ENCRYPTION_KEYSPACE,
            BUCKET.as_bytes().to_vec(),
            settings.to_bytes().unwrap(),
        ),
        (BUCKET_KEY_KEYSPACE, key.key(), record.to_bytes().unwrap()),
        (
            BUCKET_HOLDER_KEYSPACE,
            grant.key(),
            grant.to_bytes().unwrap(),
        ),
        (
            KEY_COPY_KEYSPACE,
            owner_copy.key(),
            owner_copy.to_bytes().unwrap(),
        ),
        (
            KEY_COPY_KEYSPACE,
            grantee_copy.key(),
            grantee_copy.to_bytes().unwrap(),
        ),
    ];
    for (key_space, key, value) in rows {
        write_row(&fixture.state, key_space, key, value).await;
    }
    let list = || {
        list_holders(
            State(fixture.state.clone()),
            Extension(Some(fixture.owner.clone())),
            Path(BUCKET.to_string()),
        )
    };

    let Json(before) = list().await.unwrap();
    let users: Vec<_> = before
        .holders
        .iter()
        .map(|holder| holder.user_id.clone())
        .collect();
    assert!(users.contains(&grantee.to_string()));
    let removed = remove_holder(
        State(fixture.state.clone()),
        Extension(Some(fixture.owner.clone())),
        Path((BUCKET.to_string(), grantee.to_string())),
        Query(RemovalQuery {
            revision: before.revision.clone(),
            confirm_recovery: true,
        }),
    )
    .await
    .unwrap();
    assert_eq!(removed, StatusCode::NO_CONTENT);

    let Json(after) = list().await.unwrap();
    assert!(
        after
            .holders
            .iter()
            .all(|holder| holder.user_id != grantee.to_string())
    );
    assert_ne!(after.revision, before.revision);
    // The old revision no longer names the current holders.
    let stale = remove_holder(
        State(fixture.state.clone()),
        Extension(Some(fixture.owner.clone())),
        Path((BUCKET.to_string(), grantee.to_string())),
        Query(RemovalQuery {
            revision: before.revision,
            confirm_recovery: true,
        }),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        stale.status_code(),
        StatusCode::CONFLICT | StatusCode::NOT_FOUND
    ));
}
