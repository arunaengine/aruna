// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::realm::get_config::GetConfigOperation;
use aruna_core::UserId;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::database::{ConsentClaim, DatabaseTarget, database_admin_path};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId, RealmNodeKind};
use tempfile::tempdir;

fn context(root: &str) -> DriverContext {
    DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

fn actor() -> Actor {
    let realm_id = RealmId::from_bytes([1; 32]);
    Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        user_id: UserId::local(Ulid::from_bytes([1; 16]), realm_id),
        realm_id,
    }
}

fn auth(actor: &Actor) -> AuthContext {
    AuthContext {
        user_id: actor.user_id,
        realm_id: actor.realm_id,
        path_restrictions: None,
        session: None,
    }
}

fn entry() -> DatabaseEntry {
    DatabaseEntry {
        entry_id: Ulid::from_bytes([3; 16]),
        dataset_id: Ulid::from_bytes([4; 16]),
        target: DatabaseTarget::Distributed {
            set_id: Ulid::from_bytes([5; 16]),
        },
        owner_group: Ulid::from_bytes([6; 16]),
    }
}

async fn write(ctx: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value: value.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected event: {other:?}"),
    }
}

/// A management node config and a realm admin actor, whose role covers the databases path.
async fn seed(ctx: &DriverContext, actor: &Actor) {
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, RealmNodeKind::Management);
    let target = DocumentTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let value = config.to_bytes(actor).unwrap();
    write(ctx, target.storage_keyspace(), target.storage_key(), value).await;
    let mut document = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
    for role in document.roles.values_mut() {
        role.assigned_users.insert(actor.user_id);
    }
    let key = (*actor.realm_id.as_bytes()).into();
    write(ctx, AUTH_KEYSPACE, key, document.to_bytes(actor).unwrap()).await;
}

fn publish(change: DatabaseChange) -> PublishDatabaseOperation {
    let actor = actor();
    PublishDatabaseOperation::new(PublishDatabaseConfig {
        checks: vec![(auth(&actor), database_admin_path(actor.realm_id))],
        actor,
        change,
    })
}

#[tokio::test]
async fn mark_then_remove() {
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    seed(&ctx, &actor()).await;

    let stored = drive(
        publish(DatabaseChange::Set {
            entry: entry(),
            consent: None,
        }),
        &ctx,
    )
    .await
    .expect("mark publishes");
    assert_eq!(stored.databases, vec![entry()]);
    let reread = drive(GetConfigOperation::new(actor().realm_id), &ctx)
        .await
        .unwrap();
    assert_eq!(reread.databases, vec![entry()]);

    let removed = drive(
        publish(DatabaseChange::Remove {
            entry_id: entry().entry_id,
        }),
        &ctx,
    )
    .await
    .expect("removal publishes");
    assert!(removed.databases.is_empty());
}

#[tokio::test]
async fn removal_needs_entry() {
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    seed(&ctx, &actor()).await;
    let error = drive(
        publish(DatabaseChange::Remove {
            entry_id: entry().entry_id,
        }),
        &ctx,
    )
    .await
    .expect_err("nothing to remove");
    assert_eq!(error, PublishDatabaseError::NotFound(entry().entry_id));
}

#[tokio::test]
async fn stale_consent_refused() {
    // A consent given before a removal names the old register version and is refused.
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    seed(&ctx, &actor()).await;
    let key = iroh::SecretKey::from_bytes(&[1; 32]);
    let claim = ConsentClaim {
        realm_id: actor().realm_id,
        entry: entry(),
        predecessor: None,
        approver: actor().user_id,
    };
    let consent = DatabaseConsent {
        signature: key.sign(&claim.signing_bytes().unwrap()),
        signer: key.public(),
        claim,
    };
    let change = |consent: &DatabaseConsent| DatabaseChange::Set {
        entry: entry(),
        consent: Some(Box::new(consent.clone())),
    };
    drive(publish(change(&consent)), &ctx)
        .await
        .expect("first use publishes");
    let error = drive(publish(change(&consent)), &ctx)
        .await
        .expect_err("a consent is valid for one publication");
    assert_eq!(
        error,
        PublishDatabaseError::PredecessorChanged(entry().entry_id)
    );
}

#[test]
fn denied_check_terminal() {
    let mut operation = publish(DatabaseChange::Set {
        entry: entry(),
        consent: None,
    });
    let effects = operation.start();
    assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
    let effects = operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
    ));
    assert!(effects.is_empty());
    assert_eq!(
        operation.finalize(),
        Err(PublishDatabaseError::Unauthorized)
    );
}
