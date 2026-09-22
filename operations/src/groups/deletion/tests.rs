//! Exercises deletion decisions against independent local stores and concurrent writers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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
    DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(path.to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
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
        Err(GroupDeletionError::NotEmpty("S3 buckets".into()))
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
