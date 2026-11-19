//! Exercises deletion decisions against independent local stores and concurrent writers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::SystemTime;
use aruna_core::UserId;
use aruna_core::calls::CallRun;
use aruna_core::admin_documents::AdminDocumentEvent;
use aruna_core::effects::Effect;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::*;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::group::{GroupAuthorizationDocument, owner_group_key};
use aruna_core::structs::identity::group_delete::*;
use aruna_core::structs::identity::realm::{RealmNode, RealmNodeKind};
use ulid::Ulid;
use super::commit::CommitOperation;
use super::prepare::PrepareOperation;
use crate::groups::create_group::{CreateGroupConfig, CreateGroupError, CreateGroupOperation};
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use aruna_drivers::driver::drive;
use aruna_test_support::groups::{Fixture, context, node, read, write};

pub fn empty(kinds: &str) -> GroupDeletionError {
    GroupDeletionError::NotEmpty {
        kinds: vec![kinds.into()],
        buckets: Vec::new(),
        more: false,
        hidden: 0,
    }
}

pub async fn prepare(fixture: &Fixture, seed: u8) -> Result<GroupDeleteProof, GroupDeletionError> {
    drive(
        PrepareOperation::new(
            fixture.plan.clone(),
            GroupDeletePhase::Preparing,
            node(seed),
            SystemTime::UNIX_EPOCH,
        ),
        &fixture.context,
    )
    .await?;
    Ok(GroupDeleteProof {
        node_id: node(seed),
        signature: iroh::SecretKey::from_bytes(&[seed; 32])
            .sign(&fixture.plan.signing_bytes().unwrap()),
    })
}

pub async fn commit(
    fixture: &Fixture,
    proofs: Vec<GroupDeleteProof>,
) -> Result<AdminDocumentEvent, GroupDeletionError> {
    drive(
        CommitOperation::new(
            GroupDeleteCertificate {
                plan: fixture.plan.clone(),
                proofs,
            },
            fixture.actor.clone(),
            Some(fixture.auth.clone()),
            None,
        ),
        &fixture.context,
    )
    .await
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
    let proof = prepare(&fixture, 1).await.unwrap();
    let deleted = commit(&fixture, vec![proof.clone()]).await.unwrap();
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
    assert_eq!(commit(&fixture, vec![proof]).await.unwrap(), deleted);
    let (outbox, _) = aruna_storage::scan::iter_prefix_page(
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
    prepare(&first, 1).await.unwrap();
    assert_eq!(
        prepare(&second, 2).await,
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
        prepare(&first, 1).await,
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
    let mut operation =
        CallRun::new(CreateBucketOperation::new("racing".into(), fixture.bucket()));
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
            prepare(&fixture, 1).await.unwrap();
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
    let proof = prepare(&fixture, 1).await.unwrap();
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
        commit(&fixture, vec![proof]).await,
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
    let proof = prepare(&fixture, 1).await.unwrap();
    let stranger = UserId::local(Ulid::from_bytes([8; 16]), fixture.plan.realm_id);
    fixture.actor.user_id = stranger;
    fixture.auth.user_id = stranger;
    assert_eq!(
        commit(&fixture, vec![proof]).await,
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
    assert_eq!(prepare(&fixture, 1).await, Err(empty("storage backends")));

    record.cleanup = CleanupStrategy::Retain;
    let writes = record_writes(&record).unwrap();
    let effect = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    storage.send_storage_effect(effect).await;
    let proof = prepare(&fixture, 1).await.unwrap();
    commit(&fixture, vec![proof]).await.unwrap();

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
    drive(
        super::access::VisibleOperation::new(auth, group_id, fixture.actor.node_id, names),
        &fixture.context,
    )
    .await
    .unwrap()
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
        let error = drive(
            super::access::BlockedOperation::new(&fixture.auth, group_id, node, reply.clone()),
            &fixture.context,
        )
        .await
        .unwrap();
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
    let error = drive(
        super::access::BlockedOperation::new(&fixture.auth, group_id, node(1), readable),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(
        error.to_string(),
        format!("group is not empty: node {}: S3 buckets (shown)", node(1))
    );
}
