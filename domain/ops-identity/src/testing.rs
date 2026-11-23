//! Builds pure fixtures for tests: fresh job ids, group backends and groups to delete.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::id_mint::IdMint;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structured_id::clock_health::ClockHealthError;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::time::unix_timestamp_millis;

/// A fresh job id for `(handle, bucket)` at the current time, as a submit would mint it.
pub fn fresh_job_id(handle: PlacementHandle, bucket: BucketId) -> Result<JobId, ClockHealthError> {
    let mut ids = IdMint::new(ulid::Ulid::generate().random());
    aruna_core::job_request::mint_job_id(handle, bucket, &mut ids, unix_timestamp_millis())
}

/// An enabled or `disabled` S3 backend of group 1 that keeps its data on removal.
pub fn group_storage(
    backend_id: ulid::Ulid,
    disabled: bool,
) -> aruna_core::structs::storage::group_backend::GroupStorage {
    aruna_core::structs::storage::group_backend::GroupStorage {
        backend_id,
        group_id: ulid::Ulid::from_bytes([1u8; 16]),
        name: "tenant".to_string(),
        kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
        public_config: std::collections::HashMap::new(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        updated_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        disabled,
        cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
    }
}

/// The node id of seed `seed`.
pub fn node(seed: u8) -> aruna_core::id::NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

/// A plain bucket of `group_id`, made by `creator`.
pub fn group_bucket(
    group_id: ulid::Ulid,
    creator: aruna_core::user::id::UserId,
) -> aruna_core::structs::storage::blob::BucketInfo {
    aruna_core::structs::storage::blob::BucketInfo {
        group_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: creator,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: aruna_core::structs::storage::format::Compression::Off,
        distribution: None,
    }
}

/// One group of a fresh realm, the plan to delete it and the rows that store both.
pub struct GroupSeed {
    pub actor: aruna_core::structs::identity::auth::Actor,
    pub auth: aruna_core::structs::identity::auth::AuthContext,
    pub plan: aruna_core::structs::identity::group_delete::GroupDeletePlan,
    pub config: aruna_core::structs::identity::realm::RealmConfigDocument,
    pub rows: Vec<(String, aruna_core::types::Key, aruna_core::types::Value)>,
}

impl GroupSeed {
    /// The realm and the group of a plan that names the nodes of `members`.
    pub fn new(members: &[u8]) -> Self {
        use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
        use aruna_core::keyspaces::{
            AUTH_KEYSPACE, GROUP_KEYSPACE, OWNER_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE,
        };
        use aruna_core::reducer::AdminDocumentState;
        use aruna_core::storage_entries::reducer_state_entry;
        use aruna_core::structs::identity::auth::{Actor, AuthContext};
        use aruna_core::structs::identity::group::{
            Group, GroupAuthorizationDocument, owner_group_key,
        };
        use aruna_core::structs::identity::group_delete::GroupDeletePlan;
        use aruna_core::structs::identity::realm::{
            RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNode, RealmNodeKind,
        };
        use aruna_core::user::id::UserId;
        use ulid::Ulid;

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
                Ulid::generate(),
            )
            .unwrap();
        let rows = vec![
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
        Self {
            actor,
            auth,
            plan,
            config,
            rows,
        }
    }
}
