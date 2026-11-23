//! Stores a group of a fresh realm on a temporary node for the group deletion tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::{AUTH_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::group_delete::GroupDeletePlan;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::types::Value;
use aruna_runner::driver::DriverContext;
use tempfile::{TempDir, tempdir};

pub(crate) use crate::testing::node;
use crate::testing::{GroupSeed, group_bucket};

/// One group of a fresh realm stored on a temporary node, with the plan to delete it.
pub(crate) struct Fixture {
    pub dir: TempDir,
    pub context: DriverContext,
    pub actor: Actor,
    pub auth: AuthContext,
    pub plan: GroupDeletePlan,
    pub config: RealmConfigDocument,
}

/// A storage-only context on a store opened at `path`, with an open vault.
pub(crate) fn context(path: &std::path::Path) -> DriverContext {
    let storage_handle =
        aruna_storage::storage::worker::FjallStorage::open(path.to_str().unwrap()).unwrap();
    storage_handle.open_vault(aruna_core::node::vault::NodeVaultKey::random());
    aruna_runner::testing::test_context(storage_handle)
}

/// Writes one row outside a transaction.
pub(crate) async fn write(context: &DriverContext, space: &str, key: Vec<u8>, value: Vec<u8>) {
    aruna_runner::testing::write_value(&context.storage_handle, space, key, value).await;
}

/// The stored row of `key` in `space`.
pub(crate) async fn read(context: &DriverContext, space: &str, key: Vec<u8>) -> Option<Value> {
    aruna_runner::testing::read_value(&context.storage_handle, space, key).await
}

impl Fixture {
    /// Stores the realm and the group on a node whose plan names the nodes of `members`.
    pub(crate) async fn new(members: &[u8]) -> Self {
        let dir = tempdir().unwrap();
        let context = context(dir.path());
        let seed = GroupSeed::new(members);
        aruna_runner::testing::write_entries(&context.storage_handle, seed.rows).await;
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
    pub(crate) fn bucket(&self) -> BucketInfo {
        group_bucket(self.plan.group_id, self.actor.user_id)
    }
}

/// Stores bucket rows of the group and denies the caller READ on `hidden` at `seed`.
pub(crate) async fn buckets(fixture: &Fixture, seed: u8, names: &[&str]) {
    let info = fixture.bucket().to_bytes().unwrap();
    for name in names {
        let key = name.as_bytes().to_vec();
        write(&fixture.context, S3_BUCKET_KEYSPACE, key, info.clone()).await;
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
