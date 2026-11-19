//! Builds a stored group of a fresh realm for the group deletion tests and seeds its buckets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, OWNER_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::reducer::AdminDocumentState;
use aruna_core::storage_entries::reducer_state_entry;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument, owner_group_key};
use aruna_core::structs::identity::group_delete::GroupDeletePlan;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNode, RealmNodeKind,
};
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::structs::storage::format::Compression;
use aruna_core::types::Value;
use aruna_drivers::driver::{DriverContext, drive};
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

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
    let storage_handle = aruna_storage::storage::worker::FjallStorage::open(path.to_str().unwrap()).unwrap();
    storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
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

/// The node id of seed `seed`.
pub fn node(seed: u8) -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

/// Writes one row outside a transaction.
pub async fn write(context: &DriverContext, space: &str, key: Vec<u8>, value: Vec<u8>) {
    aruna_drivers::testing::write_value(&context.storage_handle, space, key, value).await;
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
        let realm_id = RealmId::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[9; 32])
                .verifying_key()
                .to_bytes(),
        );
        let owner = UserId::local(Ulid::from_bytes([3; 16]), realm_id);
        let group_id = Ulid::from_bytes([4; 16]);
        let nodes = members
            .iter()
            .copied()
            .map(node)
            .collect::<std::collections::BTreeSet<_>>();
        let actor = Actor {
            node_id: *nodes.first().unwrap(),
            user_id: owner,
            realm_id,
        };
        let auth = AuthContext {
            user_id: owner,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let plan = GroupDeletePlan {
            request_id: Ulid::from_bytes([5; 16]),
            group_id,
            realm_id,
            owner,
            requested_by: owner,
            coordinator: actor.node_id,
            nodes,
        };
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 1);
        config.nodes = plan
            .nodes
            .iter()
            .map(|node| RealmNode {
                node_id: node.to_string(),
                kind: RealmNodeKind::Server,
            })
            .collect();
        config.seed_default_placement();
        let authorization =
            GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
        let group = Group {
            group_id,
            realm_id,
            owner,
            display_name: "Empty".into(),
            roles: authorization.roles.keys().copied().collect(),
        };
        let mut reducer = AdminDocumentState::new(AdminDocumentTarget::Group { group_id });
        reducer
            .apply_operation(
                &actor,
                AdminDocumentOperation::GroupCreated {
                    realm_id,
                    display_name: group.display_name.clone(),
                    owner,
                },
                ulid::Ulid::generate(),
            )
            .unwrap();
        let writes = vec![
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                realm_id.as_bytes().to_vec().into(),
                config.to_bytes(&actor).unwrap().into(),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                realm_id.as_bytes().to_vec().into(),
                RealmAuthorizationDocument::default_realm_doc(realm_id)
                    .to_bytes(&actor)
                    .unwrap()
                    .into(),
            ),
            (
                GROUP_KEYSPACE.to_string(),
                group_id.to_bytes().into(),
                group.to_bytes(&actor).unwrap().into(),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                group_id.to_bytes().into(),
                authorization.to_bytes(&actor).unwrap().into(),
            ),
            (
                OWNER_INDEX_KEYSPACE.to_string(),
                owner_group_key(owner, group_id).into(),
                Vec::new().into(),
            ),
            reducer_state_entry(&reducer).unwrap(),
        ];
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes,
                    txn_id: None
                })
                .await,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
        Self {
            dir,
            context,
            actor,
            auth,
            plan,
            config,
        }
    }

    /// A plain bucket of the group, made by the owner.
    pub fn bucket(&self) -> BucketInfo {
        BucketInfo {
            group_id: self.plan.group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: self.actor.user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
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
