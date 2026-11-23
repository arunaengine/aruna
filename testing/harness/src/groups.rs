//! Builds a stored group of a fresh realm for the group deletion tests and seeds its buckets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::group_delete::GroupDeletePlan;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::types::Value;
use aruna_ops_data::s3::bucket::create::CreateBucketOperation;
pub use aruna_ops_identity::testing::node;
use aruna_ops_identity::testing::{GroupSeed, group_bucket};
use aruna_runner::driver::{DriverContext, drive};
use tempfile::{TempDir, tempdir};

/// One group of a fresh realm stored on a temporary node, with the plan to delete it.
pub struct Fixture {
    pub dir: TempDir,
    pub context: DriverContext,
    pub actor: Actor,
    pub auth: AuthContext,
    pub plan: GroupDeletePlan,
    pub config: RealmConfigDocument,
}

/// A storage-only context on a store opened at `path`, with an open vault.
pub fn context(path: &std::path::Path) -> DriverContext {
    let storage_handle =
        aruna_storage::storage::worker::FjallStorage::open(path.to_str().unwrap()).unwrap();
    storage_handle.open_vault(aruna_core::node::vault::NodeVaultKey::random());
    DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    }
}

/// Writes one row outside a transaction.
pub async fn write(context: &DriverContext, space: &str, key: Vec<u8>, value: Vec<u8>) {
    aruna_runner::testing::write_value(&context.storage_handle, space, key, value).await;
}

/// The stored row of `key` in `space`.
pub async fn read(context: &DriverContext, space: &str, key: Vec<u8>) -> Option<Value> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: space.into(),
            key: key.into(),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("read failed: {event:?}");
    };
    value
}

impl Fixture {
    /// Stores the realm and the group on a node whose plan names the nodes of `members`.
    pub async fn new(members: &[u8]) -> Self {
        let dir = tempdir().unwrap();
        let context = context(dir.path());
        let seed = GroupSeed::new(members);
        let writes = StorageEffect::BatchWrite {
            writes: seed.rows,
            txn_id: None,
        };
        assert!(matches!(
            context.storage_handle.send_storage_effect(writes).await,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
        Self {
            dir,
            context,
            actor: seed.actor,
            auth: seed.auth,
            plan: seed.plan,
            config: seed.config,
        }
    }

    /// A plain bucket of the group, made by the owner.
    pub fn bucket(&self) -> BucketInfo {
        group_bucket(self.plan.group_id, self.actor.user_id)
    }
}

/// Creates the buckets and denies the caller READ on `hidden` at `seed`.
pub async fn buckets(fixture: &Fixture, seed: u8, names: &[&str]) {
    for name in names {
        drive(
            CreateBucketOperation::new(name.to_string(), fixture.bucket()),
            &fixture.context,
        )
        .await
        .unwrap();
    }
    let group_id = fixture.plan.group_id;
    let key = group_id.to_bytes().to_vec();
    let bytes = read(&fixture.context, AUTH_KEYSPACE, key.clone())
        .await
        .unwrap();
    let mut document = GroupAuthorizationDocument::from_bytes(&bytes).unwrap();
    let path = bucket_permission_path(fixture.auth.realm_id, group_id, node(seed), "hidden");
    for role in document.roles.values_mut() {
        role.permissions.insert(path.clone(), Permission::DENY);
    }
    let bytes = document.to_bytes(&fixture.actor).unwrap();
    write(&fixture.context, AUTH_KEYSPACE, key, bytes).await;
}
