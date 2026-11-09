//! Tests user and group secret writes, listing, writer eligibility and owner read-back.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use crate::secrets::tests::{create, name, token};
use aruna_core::keyspaces::ADMIN_AUDIT_KEYSPACE;
use aruna_core::realm_vault::RealmSealKey;
use aruna_core::secrets::SecretFields;
use aruna_core::structs::identity::admin_audit::{AuditIndex, audit_prefix};
use aruna_core::structs::identity::auth::Actor;
use ed25519_dalek::SigningKey;
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

fn realm_key() -> SigningKey {
    SigningKey::from_bytes(&[5u8; 32])
}

fn actor() -> Actor {
    let realm_id = RealmId::from_bytes(realm_key().verifying_key().to_bytes());
    Actor {
        node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
        user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
        realm_id,
    }
}

/// A realm whose config made the actor's node a management node.
async fn seeded(root: &str) -> DriverContext {
    let ctx = context(root);
    let config = CreateRealmConfig {
        actor: actor(),
        realm_description: "Realm".to_string(),
        oidc_providers: Vec::new(),
        node_location: None,
        node_weight: None,
        node_labels: Default::default(),
    };
    drive(CreateRealmOperation::new(config), &ctx)
        .await
        .unwrap();
    ctx
}

fn group() -> SecretOwner {
    SecretOwner::Group(Ulid::from_bytes([7; 16]))
}

async fn run(
    ctx: &DriverContext,
    owner: SecretOwner,
    change: SecretChange,
    readable: bool,
    seed: u8,
) -> Result<Option<SecretEntry>, OwnerSecretError> {
    let actor = actor();
    let input = OwnerChange {
        realm_id: actor.realm_id,
        node_id: actor.node_id,
        owner,
        actor: actor.user_id,
        change,
        readable,
        now_ms: u64::from(seed),
        write_ids: [1, 2, 3]
            .map(|part| Ulid::from_bytes([seed, part, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])),
        txn_id: None,
    };
    drive(OwnerSecretOperation::new(input), ctx).await
}

async fn listed(ctx: &DriverContext, owner: SecretOwner) -> Vec<SecretEntry> {
    let operation = OwnerSecretsOperation::new(actor().realm_id, owner, None, 50);
    drive(operation, ctx).await.unwrap().0
}

#[tokio::test]
async fn registers_follow_changes() {
    let dir = tempdir().unwrap();
    let ctx = seeded(dir.path().to_str().unwrap()).await;
    let created = run(&ctx, group(), create(), false, 10)
        .await
        .unwrap()
        .unwrap();
    assert!(created.complete());
    assert_eq!(listed(&ctx, group()).await, std::slice::from_ref(&created));
    let again = run(&ctx, group(), create(), false, 11).await;
    assert_eq!(again, Err(ChangeError::Exists.into()));

    let rotate = SecretChange::Rotate {
        name: name(),
        fields: token("canary-5b74"),
        expected: Some(created.revision()),
    };
    let rotated = run(&ctx, group(), rotate, false, 12)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rotated.info, created.info,
        "a rotation keeps the info register"
    );
    let stale = SecretChange::Replace {
        name: name(),
        description: "New".to_string(),
        settings: Default::default(),
        uses: Vec::new(),
        expected: Some(created.revision()),
    };
    assert_eq!(
        run(&ctx, group(), stale, false, 13).await,
        Err(ChangeError::Stale.into())
    );

    let delete = SecretChange::Delete {
        name: name(),
        expected: None,
    };
    assert_eq!(run(&ctx, group(), delete, false, 14).await, Ok(None));
    assert!(listed(&ctx, group()).await.is_empty());
    // A new create starts a fresh incarnation; the old value never comes back.
    let recreated = run(&ctx, group(), create(), false, 15)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        recreated.lifecycle.incarnation,
        created.lifecycle.incarnation
    );
    let shown = format!("{:?}", listed(&ctx, group()).await);
    assert!(!shown.contains("canary"));
}

#[tokio::test]
async fn foreign_writer_ignored() {
    let dir = tempdir().unwrap();
    let ctx = seeded(dir.path().to_str().unwrap()).await;
    let created = run(&ctx, group(), create(), false, 10)
        .await
        .unwrap()
        .unwrap();
    // A document stamped by a node the realm never added never counts.
    let mut forged = created.info.clone().unwrap();
    forged.stamp.counter = 99;
    forged.stamp.node_id = iroh::SecretKey::from_bytes(&[8; 32]).public();
    forged.stamp.write_id = Ulid::from_bytes([42; 16]);
    let write = StorageEffect::Write {
        key_space: OWNER_SECRETS_KEYSPACE.to_string(),
        key: forged.key().into(),
        value: forged.to_bytes().unwrap().into(),
        txn_id: None,
    };
    ctx.storage_handle.send_storage_effect(write).await;
    assert_eq!(listed(&ctx, group()).await, [created]);
}

#[tokio::test]
async fn readback_audits_first() {
    let dir = tempdir().unwrap();
    let ctx = seeded(dir.path().to_str().unwrap()).await;
    let owner = SecretOwner::User(actor().user_id);
    let refused = run(&ctx, group(), create(), true, 9).await;
    assert_eq!(refused, Err(aruna_core::secrets::SecretError::Rule.into()));
    run(&ctx, owner, create(), true, 10).await.unwrap();

    let read = ReadbackOperation::new(actor().realm_id, owner, name(), actor().user_id);
    let entry = drive(read, &ctx).await.unwrap();
    let key = RealmSealKey::derive(&realm_key());
    let opened = open_value(&key, &actor().realm_id, &entry).unwrap();
    assert_eq!(SecretFields::decode(&opened).unwrap(), token("canary-5b71"));
    let audit = StorageEffect::Iter {
        key_space: ADMIN_AUDIT_KEYSPACE.to_string(),
        prefix: Some(audit_prefix(AuditIndex::User(actor().user_id)).into()),
        start: None,
        limit: 10,
        txn_id: None,
    };
    let rows = match ctx.storage_handle.send_storage_effect(audit).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(rows, 2, "the create and the read-back");

    // Group secrets and secrets created unreadable never read back.
    run(&ctx, group(), create(), false, 11).await.unwrap();
    let read = ReadbackOperation::new(actor().realm_id, group(), name(), actor().user_id);
    assert_eq!(drive(read, &ctx).await, Err(OwnerSecretError::NotReadable));
}

#[test]
fn parent_transaction_kept() {
    // Inside a parent's transaction the change writes there and leaves commit and abort to it.
    let actor = actor();
    let parent = Ulid::from_bytes([77; 16]);
    let input = OwnerChange {
        realm_id: actor.realm_id,
        node_id: actor.node_id,
        owner: group(),
        actor: actor.user_id,
        change: create(),
        readable: false,
        now_ms: 1,
        write_ids: [1, 2, 3].map(|part| Ulid::from_bytes([part; 16])),
        txn_id: Some(parent),
    };
    let mut operation = OwnerSecretOperation::new(input);
    let effects = operation.start();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { txn_id: Some(id), .. })] if *id == parent
    ));
    let key = aruna_core::types::Key::from(Vec::new());
    let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
        key,
        value: None,
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { .. })]
    ));
    let page = StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    };
    let effects = operation.step(Event::Storage(page));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { txn_id: Some(id), .. })] if *id == parent
    ));
    let key = aruna_core::types::Key::from(Vec::new());
    let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
        key,
        value: None,
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchWrite { txn_id: Some(id), .. })] if *id == parent
    ));
    assert!(
        operation.abort().is_empty(),
        "the parent aborts its own transaction"
    );
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: vec![],
    }));
    assert!(effects.is_empty() && operation.is_complete());
}

fn named(name: &str) -> SecretChange {
    let SecretChange::Create {
        kind,
        description,
        settings,
        uses,
        fields,
        ..
    } = create()
    else {
        unreachable!("create() builds a create change");
    };
    SecretChange::Create {
        name: SecretName::try_from(name.to_string()).unwrap(),
        kind,
        description,
        settings,
        uses,
        fields,
    }
}

#[tokio::test]
async fn pages_skip_deleted() {
    let dir = tempdir().unwrap();
    let ctx = seeded(dir.path().to_str().unwrap()).await;
    for (seed, name) in [(10, "a"), (11, "b"), (12, "c")] {
        run(&ctx, group(), named(name), false, seed).await.unwrap();
    }
    for (seed, name) in [(13, "a"), (14, "c")] {
        let delete = SecretChange::Delete {
            name: SecretName::try_from(name.to_string()).unwrap(),
            expected: None,
        };
        run(&ctx, group(), delete, false, seed).await.unwrap();
    }
    let page = |after: Option<&str>| {
        let after = after.map(|name| SecretName::try_from(name.to_string()).unwrap());
        let operation = OwnerSecretsOperation::new(actor().realm_id, group(), after.as_ref(), 1);
        drive(operation, &ctx)
    };
    // Deleted `a` comes first by key; the page still holds the live `b` and ends the listing.
    let (entries, more) = page(None).await.unwrap();
    let names: Vec<_> = entries
        .iter()
        .map(|entry| entry.lifecycle.name.to_string())
        .collect();
    assert_eq!((names, more), (vec!["b".to_string()], false));
    run(&ctx, group(), named("d"), false, 15).await.unwrap();
    let (entries, more) = page(None).await.unwrap();
    assert_eq!((entries.len(), more), (1, true));
    let (entries, more) = page(Some("b")).await.unwrap();
    assert_eq!((entries[0].lifecycle.name.as_str(), more), ("d", false));
}

#[tokio::test]
async fn long_description_refused() {
    let dir = tempdir().unwrap();
    let ctx = seeded(dir.path().to_str().unwrap()).await;
    let long = "x".repeat(aruna_core::secrets::MAX_DESCRIPTION + 1);
    let mut change = create();
    if let SecretChange::Create { description, .. } = &mut change {
        *description = long.clone();
    }
    let refused = run(&ctx, group(), change, false, 10).await;
    assert_eq!(refused, Err(SecretError::Description.into()));
    run(&ctx, group(), create(), false, 11).await.unwrap();
    let replace = SecretChange::Replace {
        name: name(),
        description: long,
        settings: Default::default(),
        uses: Vec::new(),
        expected: None,
    };
    let refused = run(&ctx, group(), replace, false, 12).await;
    assert_eq!(refused, Err(SecretError::Description.into()));
    let listed = listed(&ctx, group()).await;
    assert_eq!(listed.len(), 1);
    assert!(
        matches!(&listed[0].info.as_ref().unwrap().body, SecretBody::Info(info) if info.description == "Catalog API")
    );
}

#[tokio::test]
async fn readback_needs_audit() {
    // When the audit row cannot be written, the read-back ends without an entry to open.
    let dir = tempdir().unwrap();
    let ctx = seeded(dir.path().to_str().unwrap()).await;
    let owner = SecretOwner::User(actor().user_id);
    run(&ctx, owner, create(), true, 10).await.unwrap();
    let mut read = ReadbackOperation::new(actor().realm_id, owner, name(), actor().user_id);
    let mut effects = read.start();
    while let Some(effect) = effects.pop() {
        let Effect::Storage(storage) = effect else {
            panic!("only storage effects, got {effect:?}");
        };
        let event = match storage {
            StorageEffect::BatchWrite { .. } => Event::Storage(StorageEvent::Error {
                error: aruna_core::errors::StorageError::WriteError("disk full".to_string()),
            }),
            other => ctx.storage_handle.send_storage_effect(other).await,
        };
        effects = read.step(event);
    }
    assert!(read.is_complete());
    assert!(matches!(read.finalize(), Err(OwnerSecretError::Storage(_))));
}
