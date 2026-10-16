//! Checks deletion blockers without network peers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::group_delete::{GroupDeletionError, MembershipFence};
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::types::TxnId;
use ulid::Ulid;

use super::check::{CheckOperation, check_node, scan};
use super::tests::{Fixture, node, read, write};
use crate::driver::drive;
use crate::s3::bucket::create::CreateBucketOperation;

/// Creates the buckets and denies the caller READ on `hidden` at `seed`.
pub(super) async fn buckets(fixture: &Fixture, seed: u8, names: &[&str]) {
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

fn keyspaces(fixture: &Fixture) -> usize {
    std::fs::read_dir(fixture.dir.path().join("keyspaces"))
        .unwrap()
        .count()
}

fn stranger(fixture: &Fixture) -> AuthContext {
    let mut auth = fixture.auth.clone();
    auth.user_id = UserId::local(Ulid::from_bytes([8; 16]), auth.realm_id);
    auth
}

fn blocked(names: &[&str], more: bool, hidden: u64) -> Result<(), GroupDeletionError> {
    Err(GroupDeletionError::NotEmpty {
        kinds: vec!["S3 buckets".into()],
        buckets: names.iter().map(|name| name.to_string()).collect(),
        more,
        hidden,
    })
}

#[tokio::test]
async fn check_changes_nothing() {
    let fixture = Fixture::new(&[1]).await;
    let group_id = fixture.plan.group_id;
    let before = keyspaces(&fixture);
    assert_eq!(
        scan(&fixture.context, &fixture.auth, group_id, node(1)).await,
        Ok(())
    );
    assert_eq!(keyspaces(&fixture), before);
    buckets(&fixture, 1, &["shown", "hidden"]).await;
    let before = keyspaces(&fixture);
    // The denied name is dropped and counted as hidden.
    assert_eq!(
        scan(&fixture.context, &fixture.auth, group_id, node(1)).await,
        blocked(&["shown"], false, 1)
    );
    let fence = MembershipFence::key(fixture.plan.realm_id).to_vec();
    for key in [group_id.to_bytes().to_vec(), fence] {
        assert!(
            read(&fixture.context, GROUP_DELETE_KEYSPACE, key)
                .await
                .is_none()
        );
    }
    assert_eq!(keyspaces(&fixture), before);
}

#[tokio::test]
async fn check_names_buckets() {
    let fixture = Fixture::new(&[1]).await;
    let group_id = fixture.plan.group_id;
    buckets(&fixture, 1, &["b-1", "b-2"]).await;
    assert_eq!(
        scan(&fixture.context, &fixture.auth, group_id, node(1)).await,
        blocked(&["b-1", "b-2"], false, 0)
    );
    buckets(&fixture, 1, &["b-3", "b-4", "b-5", "b-6"]).await;
    assert_eq!(
        scan(&fixture.context, &fixture.auth, group_id, node(1)).await,
        blocked(&["b-1", "b-2", "b-3", "b-4", "b-5"], true, 0)
    );
}

#[tokio::test]
async fn check_refuses_strangers() {
    let fixture = Fixture::new(&[1]).await;
    let group_id = fixture.plan.group_id;
    let config = fixture.config.clone();
    let auth = fixture.auth.clone();
    let outsider = stranger(&fixture);
    let context = Arc::new(fixture.context);
    assert_eq!(
        check_node(&context, &config, &outsider, group_id, node(1)).await,
        Err(GroupDeletionError::Unauthorized)
    );
    assert_eq!(
        check_node(&context, &config, &auth, group_id, node(1)).await,
        Ok(())
    );
}

#[tokio::test]
async fn check_scans_devices() {
    let fixture = Fixture::new(&[1]).await;
    let group_id = fixture.plan.group_id;
    buckets(&fixture, 1, &["owned"]).await;
    let event = fixture
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: GROUP_KEYSPACE.into(),
            key: group_id.to_bytes().to_vec().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::DeleteResult { .. })
    ));
    let auth = stranger(&fixture);
    let mut config = fixture.config.clone();
    let context = Arc::new(fixture.context);
    // A server without the group row refuses, as Prepare does.
    assert!(matches!(
        check_node(&context, &config, &auth, group_id, node(1)).await,
        Err(GroupDeletionError::Unavailable(_))
    ));
    config.nodes[0].kind = RealmNodeKind::User {
        owner: auth.user_id,
    };
    // A device without the row still scans; the stranger may not read the name.
    assert_eq!(
        check_node(&context, &config, &auth, group_id, node(1)).await,
        blocked(&[], false, 1)
    );
}

#[test]
fn check_finishes_first() {
    let mut check = CheckOperation::new(Ulid::from_bytes([4; 16]), SystemTime::UNIX_EPOCH);
    let txn_id = TxnId::default();
    check.start();
    let mut effects = check.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    while !matches!(
        effects.first(),
        Some(Effect::Storage(StorageEffect::CommitTransaction { .. }))
    ) {
        effects = check.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
    }
    assert!(!check.is_complete());
    assert!(matches!(
        check.finalize(),
        Err(GroupDeletionError::Unavailable(_))
    ));
}
