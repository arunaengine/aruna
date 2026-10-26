//! Exercises deletion decisions against independent local stores and concurrent writers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::format::Compression;
use std::collections::VecDeque;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::*;
use aruna_core::operation::Operation;
use aruna_core::reducer::AdminDocumentState;
use aruna_core::storage_entries::reducer_state_entry;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument, owner_group_key};
use aruna_core::structs::identity::group_delete::*;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNode, RealmNodeKind,
};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::types::Value;
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

use super::commit::CommitOperation;
use super::prepare::PrepareOperation;
use crate::driver::{DriverContext, drive};
use crate::groups::create_group::{CreateGroupConfig, CreateGroupError, CreateGroupOperation};
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};

pub(super) struct Fixture {
    pub dir: TempDir,
    pub context: DriverContext,
    pub actor: Actor,
    pub auth: AuthContext,
    pub plan: GroupDeletePlan,
    pub config: RealmConfigDocument,
}

pub(super) fn context(path: &std::path::Path) -> DriverContext {
    let storage_handle = aruna_storage::FjallStorage::open(path.to_str().unwrap()).unwrap();
    storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
    DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

pub(super) fn empty(kinds: &str) -> GroupDeletionError {
    GroupDeletionError::NotEmpty {
        kinds: vec![kinds.into()],
        buckets: Vec::new(),
        more: false,
        hidden: 0,
    }
}

pub(super) fn node(seed: u8) -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(super) async fn write(context: &DriverContext, space: &str, key: Vec<u8>, value: Vec<u8>) {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: space.into(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

pub(super) async fn read(context: &DriverContext, space: &str, key: Vec<u8>) -> Option<Value> {
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

    pub async fn prepare(&self, seed: u8) -> Result<GroupDeleteProof, GroupDeletionError> {
        drive(
            PrepareOperation::new(
                self.plan.clone(),
                GroupDeletePhase::Preparing,
                node(seed),
                SystemTime::UNIX_EPOCH,
            ),
            &self.context,
        )
        .await?;
        Ok(GroupDeleteProof {
            node_id: node(seed),
            signature: iroh::SecretKey::from_bytes(&[seed; 32])
                .sign(&self.plan.signing_bytes().unwrap()),
        })
    }

    pub async fn commit(
        &self,
        proofs: Vec<GroupDeleteProof>,
    ) -> Result<AdminDocumentEvent, GroupDeletionError> {
        drive(
            CommitOperation::new(
                GroupDeleteCertificate {
                    plan: self.plan.clone(),
                    proofs,
                },
                self.actor.clone(),
                Some(self.auth.clone()),
                None,
            ),
            &self.context,
        )
        .await
    }

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

#[tokio::test]
async fn commit_releases_owner() {
    let fixture = Fixture::new(&[1]).await;
    let create = CreateGroupConfig {
        actor: fixture.actor.clone(),
        display_name: "Another".into(),
        owner_cap: Some(1),
    };
    assert!(matches!(
        drive(CreateGroupOperation::new(create.clone()), &fixture.context).await,
        Err(CreateGroupError::GroupLimitReached { limit: 1 })
    ));
    let proof = fixture.prepare(1).await.unwrap();
    let deleted = fixture.commit(vec![proof.clone()]).await.unwrap();
    for space in [GROUP_KEYSPACE, AUTH_KEYSPACE] {
        assert!(
            read(
                &fixture.context,
                space,
                fixture.plan.group_id.to_bytes().to_vec()
            )
            .await
            .is_none()
        );
    }
    assert!(
        read(
            &fixture.context,
            OWNER_INDEX_KEYSPACE,
            owner_group_key(fixture.plan.owner, fixture.plan.group_id)
        )
        .await
        .is_none()
    );
    let record = read(
        &fixture.context,
        GROUP_DELETE_KEYSPACE,
        fixture.plan.group_id.to_bytes().to_vec(),
    )
    .await
    .unwrap();
    let record = GroupDeleteRecord::from_bytes(&record).unwrap();
    assert_eq!(record.phase, GroupDeletePhase::Deleted);
    assert_eq!(record.event.as_deref(), Some(&deleted));
    assert_eq!(fixture.commit(vec![proof]).await.unwrap(), deleted);
    let (outbox, _) = crate::jobs::store::iter_prefix_page(
        &fixture.context.storage_handle,
        SYNC_OUTBOX_KEYSPACE,
        None,
        None,
        10,
        None,
    )
    .await
    .unwrap();
    assert_eq!(outbox.len(), 1);
    drive(CreateGroupOperation::new(create), &fixture.context)
        .await
        .unwrap();
}

#[tokio::test]
async fn occupied_peer_refuses() {
    let first = Fixture::new(&[1, 2]).await;
    let second = Fixture::new(&[1, 2]).await;
    drive(
        CreateBucketOperation::new("owned".into(), second.bucket()),
        &second.context,
    )
    .await
    .unwrap();
    first.prepare(1).await.unwrap();
    assert_eq!(
        second.prepare(2).await,
        Err(GroupDeletionError::NotEmpty {
            kinds: vec!["S3 buckets".into()],
            buckets: vec!["owned".into()],
            more: false,
            hidden: 0,
        })
    );
    for fixture in [&first, &second] {
        assert!(
            read(
                &fixture.context,
                GROUP_KEYSPACE,
                fixture.plan.group_id.to_bytes().to_vec()
            )
            .await
            .is_some()
        );
        assert!(
            read(
                &fixture.context,
                AUTH_KEYSPACE,
                fixture.plan.group_id.to_bytes().to_vec()
            )
            .await
            .is_some()
        );
        drive(
            PrepareOperation::new(
                fixture.plan.clone(),
                GroupDeletePhase::Cancelled,
                fixture.actor.node_id,
                SystemTime::UNIX_EPOCH,
            ),
            &fixture.context,
        )
        .await
        .unwrap();
    }
    assert!(
        read(&second.context, S3_BUCKET_KEYSPACE, b"owned".to_vec())
            .await
            .is_some()
    );
    assert!(matches!(
        first.prepare(1).await,
        Err(GroupDeletionError::Conflict(_))
    ));
    drive(
        CreateBucketOperation::new("after-cancel".into(), first.bucket()),
        &first.context,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn writer_conflicts_freeze() {
    let fixture = Fixture::new(&[1]).await;
    let mut operation = CreateBucketOperation::new("racing".into(), fixture.bucket());
    let mut effects = VecDeque::from_iter(operation.start());
    let mut prepared = false;
    while !operation.is_complete() {
        let effect = effects
            .pop_front()
            .expect("bucket creation must make progress");
        let Effect::Storage(effect) = effect else {
            panic!("unexpected bucket effect");
        };
        if matches!(effect, StorageEffect::CommitTransaction { .. }) {
            assert!(!prepared);
            fixture.prepare(1).await.unwrap();
            prepared = true;
        }
        let event = fixture
            .context
            .storage_handle
            .send_storage_effect(effect)
            .await;
        effects.extend(operation.step(event));
    }
    assert!(prepared);
    assert!(matches!(
        operation.finalize(),
        Err(CreateBucketError::StorageError(
            StorageError::TransactionConflict
        ))
    ));
    assert!(
        read(&fixture.context, S3_BUCKET_KEYSPACE, b"racing".to_vec())
            .await
            .is_none()
    );
    assert!(matches!(
        drive(
            CreateBucketOperation::new("blocked".into(), fixture.bucket()),
            &fixture.context
        )
        .await,
        Err(CreateBucketError::GroupWrite(GroupWriteError::Frozen))
    ));
}

#[tokio::test]
async fn membership_change_refuses() {
    let fixture = Fixture::new(&[1]).await;
    let proof = fixture.prepare(1).await.unwrap();
    let mut config = fixture.config.clone();
    config.nodes.push(RealmNode {
        node_id: node(2).to_string(),
        kind: RealmNodeKind::Server,
    });
    write(
        &fixture.context,
        REALM_CONFIG_KEYSPACE,
        fixture.plan.realm_id.as_bytes().to_vec(),
        config.to_bytes(&fixture.actor).unwrap(),
    )
    .await;
    assert!(matches!(
        fixture.commit(vec![proof]).await,
        Err(GroupDeletionError::Conflict(_))
    ));
    assert!(
        read(
            &fixture.context,
            GROUP_KEYSPACE,
            fixture.plan.group_id.to_bytes().to_vec()
        )
        .await
        .is_some()
    );
}

#[tokio::test]
async fn unauthorized_commit_refuses() {
    let mut fixture = Fixture::new(&[1]).await;
    let proof = fixture.prepare(1).await.unwrap();
    let stranger = UserId::local(Ulid::from_bytes([8; 16]), fixture.plan.realm_id);
    fixture.actor.user_id = stranger;
    fixture.auth.user_id = stranger;
    assert_eq!(
        fixture.commit(vec![proof]).await,
        Err(GroupDeletionError::Unauthorized)
    );
    assert!(
        read(
            &fixture.context,
            GROUP_KEYSPACE,
            fixture.plan.group_id.to_bytes().to_vec()
        )
        .await
        .is_some()
    );
}

#[tokio::test]
async fn retained_backend_removed() {
    use crate::groups::backends::{backend_key, index_key, record_writes};
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::group_backend::{
        GroupBackendKind, GroupStorage, GroupStorageSecret,
    };

    let fixture = Fixture::new(&[1]).await;
    let storage = &fixture.context.storage_handle;
    let mut record = GroupStorage {
        backend_id: Ulid::from_bytes([6; 16]),
        group_id: fixture.plan.group_id,
        name: "tenant".into(),
        kind: GroupBackendKind::S3,
        public_config: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        created_by: fixture.actor.user_id,
        disabled: true,
        cleanup: CleanupStrategy::node_default(),
    };
    let entry = GroupStorageSecret::vault_entry(record.backend_id);
    let secret = SharedSecret::new(SecretBytes::new(vec![1]));
    let writes = record_writes(&record).unwrap();
    for effect in [
        StorageEffect::VaultWrite {
            entry,
            secret,
            txn_id: None,
        },
        StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        },
    ] {
        let event = storage.send_storage_effect(effect).await;
        assert!(!matches!(event, Event::Storage(StorageEvent::Error { .. })));
    }
    // A reclaiming backend still has bytes to drain, so it blocks the deletion.
    assert_eq!(fixture.prepare(1).await, Err(empty("storage backends")));

    record.cleanup = CleanupStrategy::Retain;
    let writes = record_writes(&record).unwrap();
    let effect = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    storage.send_storage_effect(effect).await;
    let proof = fixture.prepare(1).await.unwrap();
    fixture.commit(vec![proof]).await.unwrap();

    let group_id = fixture.plan.group_id;
    for (space, key) in [
        (STORAGE_BACKEND_KEYSPACE, backend_key(record.backend_id)),
        (
            BACKEND_INDEX_KEYSPACE,
            index_key(group_id, record.backend_id),
        ),
        (NODE_VAULT_KEYSPACE, entry.key().into()),
    ] {
        assert!(read(&fixture.context, space, key.to_vec()).await.is_none());
    }
}

async fn seen(fixture: &Fixture, auth: &AuthContext, group_id: Ulid) -> Vec<String> {
    let names = ["a", "b)x"].map(String::from).to_vec();
    super::coordinator::visible(
        &fixture.context,
        auth,
        group_id,
        fixture.actor.node_id,
        names,
    )
    .await
}

#[tokio::test]
async fn filters_names() {
    let fixture = Fixture::new(&[1]).await;
    let group_id = fixture.plan.group_id;
    let both = ["a", "b)x"].map(String::from).to_vec();
    assert_eq!(seen(&fixture, &fixture.auth, group_id).await, both);

    // A caller without any role in the group sees no names.
    let stranger = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([7; 16]), fixture.auth.realm_id),
        ..fixture.auth.clone()
    };
    assert!(seen(&fixture, &stranger, group_id).await.is_empty());

    // A group that no longer has an authorization document drops every name.
    assert!(
        seen(&fixture, &fixture.auth, Ulid::from_bytes([8; 16]))
            .await
            .is_empty()
    );

    // Group-wide READ does not reveal a bucket with its own DENY.
    let key = group_id.to_bytes().to_vec();
    let bytes = read(&fixture.context, AUTH_KEYSPACE, key.clone())
        .await
        .unwrap();
    let mut document = GroupAuthorizationDocument::from_bytes(&bytes).unwrap();
    let path = aruna_core::structs::storage::blob::bucket_permission_path(
        fixture.auth.realm_id,
        group_id,
        fixture.actor.node_id,
        "b)x",
    );
    for role in document.roles.values_mut() {
        if role.assigned_users.contains(&fixture.auth.user_id) {
            role.permissions.insert(
                path.clone(),
                aruna_core::structs::identity::auth::Permission::DENY,
            );
        }
    }
    let bytes = document.to_bytes(&fixture.actor).unwrap();
    write(&fixture.context, AUTH_KEYSPACE, key, bytes).await;
    assert_eq!(
        seen(&fixture, &fixture.auth, group_id).await,
        vec!["a".to_string()]
    );
}

#[tokio::test]
async fn renders_refusals() {
    let fixture = Fixture::new(&[1, 2]).await;
    let group_id = fixture.plan.group_id;
    let key = group_id.to_bytes().to_vec();
    let bytes = read(&fixture.context, AUTH_KEYSPACE, key.clone())
        .await
        .unwrap();
    let mut document = GroupAuthorizationDocument::from_bytes(&bytes).unwrap();
    for node in [node(1), node(2)] {
        let path = aruna_core::structs::storage::blob::bucket_permission_path(
            fixture.auth.realm_id,
            group_id,
            node,
            "hidden",
        );
        for role in document.roles.values_mut() {
            role.permissions.insert(
                path.clone(),
                aruna_core::structs::identity::auth::Permission::DENY,
            );
        }
    }
    let bytes = document.to_bytes(&fixture.actor).unwrap();
    write(&fixture.context, AUTH_KEYSPACE, key, bytes).await;
    let reply = GroupDeletionError::NotEmpty {
        kinds: vec!["S3 buckets".into()],
        buckets: vec!["shown".into(), "hidden".into()],
        more: false,
        hidden: 0,
    };
    // The local refusal and a peer refusal both pass through the coordinator filter.
    for node in [node(1), node(2)] {
        let error = super::coordinator::blocked(
            &fixture.context,
            &fixture.auth,
            group_id,
            node,
            reply.clone(),
        )
        .await;
        let GroupDeletionError::NotEmpty { kinds, .. } = error else {
            panic!("expected a refusal, got {error:?}");
        };
        assert_eq!(
            kinds,
            [format!(
                "node {node}: S3 buckets (shown, 1 you cannot read)"
            )]
        );
    }
    let readable = GroupDeletionError::NotEmpty {
        kinds: vec!["S3 buckets".into()],
        buckets: vec!["shown".into()],
        more: false,
        hidden: 0,
    };
    let error =
        super::coordinator::blocked(&fixture.context, &fixture.auth, group_id, node(1), readable)
            .await;
    assert_eq!(
        error.to_string(),
        format!("group is not empty: node {}: S3 buckets (shown)", node(1))
    );
}
