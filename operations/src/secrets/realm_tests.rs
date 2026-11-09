//! Tests realm secret writes: record and value together, rotation, deletion and refusals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::realm::get_config::GetConfigOperation;
use crate::secrets::tests::{create, name, token};
use aruna_core::UserId;
use aruna_core::secrets::SecretFields;
use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
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

fn actor() -> Actor {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    Actor {
        node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
        user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
        realm_id,
    }
}

fn key() -> RealmVaultKey {
    RealmVaultKey::derive(&SigningKey::from_bytes(&[5u8; 32]))
}

async fn seed(ctx: &DriverContext, kind: RealmNodeKind) {
    let actor = actor();
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, kind);
    let target = DocumentTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let write = StorageEffect::Write {
        key_space: target.storage_keyspace().to_string(),
        key: target.storage_key(),
        value: config.to_bytes(&actor).unwrap().into(),
        txn_id: None,
    };
    let event = ctx.storage_handle.send_storage_effect(write).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn run(
    ctx: &DriverContext,
    change: SecretChange,
    write: u8,
) -> Result<Option<SecretRecord>, RealmSecretError> {
    let input = RealmChange {
        actor: actor(),
        vault_key: key(),
        change,
        now_ms: u64::from(write),
        write_id: Ulid::from_bytes([write; 16]),
    };
    drive(RealmSecretOperation::new(input), ctx).await
}

async fn config(ctx: &DriverContext) -> RealmConfigDocument {
    drive(GetConfigOperation::new(actor().realm_id), ctx)
        .await
        .unwrap()
}

fn opened(config: &RealmConfigDocument) -> Vec<SecretFields> {
    (config.vault.iter())
        .map(|record| SecretFields::decode(&key().open(&config.realm_id, record).unwrap()).unwrap())
        .collect()
}

#[tokio::test]
async fn value_follows_record() {
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    seed(&ctx, RealmNodeKind::Management).await;

    let created = run(&ctx, create(), 10).await.unwrap().unwrap();
    let stored = config(&ctx).await;
    assert_eq!(stored.secrets, std::slice::from_ref(&created));
    assert_eq!(opened(&stored), [token("canary-5b71")]);
    assert!(!format!("{stored:?}").contains("canary"));

    let rotate = SecretChange::Rotate {
        name: name(),
        fields: token("canary-5b73"),
        expected: Some(created.revision_token()),
    };
    let rotated = run(&ctx, rotate, 11).await.unwrap().unwrap();
    let stored = config(&ctx).await;
    assert_eq!(stored.secrets[0].value_id, rotated.value_id);
    // The retired value leaves the vault with the rotation.
    assert_eq!(opened(&stored), [token("canary-5b73")]);

    let delete = SecretChange::Delete {
        name: name(),
        expected: None,
    };
    assert_eq!(run(&ctx, delete, 12).await, Ok(None));
    let stored = config(&ctx).await;
    assert!(stored.secrets.is_empty() && stored.vault.is_empty());
}

#[tokio::test]
async fn refusals_write_nothing() {
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    seed(&ctx, RealmNodeKind::Server).await;
    let refused = run(&ctx, create(), 10).await;
    assert_eq!(refused, Err(RealmSecretError::NotManagementNode));

    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    seed(&ctx, RealmNodeKind::Management).await;
    let created = run(&ctx, create(), 10).await.unwrap().unwrap();
    let stale = SecretChange::Replace {
        name: name(),
        description: String::new(),
        settings: Default::default(),
        uses: Vec::new(),
        expected: Some(format!("{}.x", created.revision)),
    };
    let refused = run(&ctx, stale, 11).await;
    assert_eq!(refused, Err(RealmSecretError::Change(ChangeError::Stale)));
    assert_eq!(
        run(&ctx, create(), 12).await,
        Err(ChangeError::Exists.into())
    );
    assert_eq!(config(&ctx).await.secrets, [created]);
}

#[tokio::test]
async fn early_value_kept() {
    // B holds A's value before A's record; an unrelated edit on B must not retire that value.
    use aruna_core::admin_documents::AdminDocumentOperation as Op;
    use aruna_core::reducer::AdminDocumentState;
    use aruna_core::storage_entries::{reducer_state_entry, reducer_state_key};
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    seed(&ctx, RealmNodeKind::Management).await;
    let other = Actor {
        node_id: iroh::SecretKey::from_bytes(&[8; 32]).public(),
        ..actor()
    };
    let target = AdminDocumentTarget::RealmConfig {
        realm_id: actor().realm_id,
    };
    let early = {
        let mut on_a = AdminDocumentState::new(target.clone());
        let reference = RealmSecretRef::named(Ulid::from_bytes([40; 16]));
        let record = RealmVaultRecord {
            entry: reference.entry,
            revision: reference.revision,
            sealed: vec![1; 48],
            changed_by: other.user_id,
            changed_at_ms: 1,
        };
        let vault = Op::ConfigVaultSet {
            entry: reference.entry,
            record: Some(record),
        };
        let value = on_a.apply_operation(&other, vault).unwrap();
        let SecretChange::Create { name, kind, .. } = crate::secrets::tests::create() else {
            unreachable!("create() builds a create change");
        };
        let info = SecretRecord {
            name: name.clone(),
            kind,
            description: String::new(),
            settings: Default::default(),
            uses: Vec::new(),
            fields: vec!["token".to_string()],
            revision: reference.revision,
            value_id: reference.revision,
            created_by: other.user_id,
            created_at_ms: 1,
            updated_by: other.user_id,
            updated_at_ms: 1,
        };
        let named = on_a.apply_operation(
            &other,
            Op::ConfigSecretSet {
                name,
                info: Some(info),
            },
        );
        (value, named.unwrap())
    };
    let mut on_b = AdminDocumentState::new(target.clone());
    on_b.apply(&early.0).unwrap();
    let (space, key, value) = reducer_state_entry(&on_b).unwrap();
    let write = StorageEffect::Write {
        key_space: space,
        key,
        value,
        txn_id: None,
    };
    ctx.storage_handle.send_storage_effect(write).await;

    let mut unrelated = crate::secrets::tests::create();
    if let SecretChange::Create { name, .. } = &mut unrelated {
        *name = aruna_core::secrets::SecretName::try_from("other".to_string()).unwrap();
    }
    run(&ctx, unrelated, 10).await.unwrap();
    let read = StorageEffect::Read {
        key_space: DOCUMENT_STATE_KEYSPACE.to_string(),
        key: reducer_state_key(&target),
        txn_id: None,
    };
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(bytes), ..
    }) = ctx.storage_handle.send_storage_effect(read).await
    else {
        panic!("the reducer state is stored");
    };
    let mut state = aruna_core::reducer::decode_reducer_state(&bytes).unwrap();
    state.apply(&early.1).unwrap();
    let values: Vec<_> = (state.materialized_realm_vault().iter())
        .map(|record| record.entry.id)
        .collect();
    assert!(
        values.contains(&Ulid::from_bytes([40; 16])),
        "the late record finds its value"
    );
    assert_eq!(state.materialized_realm_secrets().len(), 2);
}

#[tokio::test]
async fn widening_notifies_admins() {
    use aruna_core::keyspaces::{AUTH_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE};
    use aruna_core::secrets::{Delivery, ResponseExposure, SecretConsumer, UseRule};
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    let dir = tempdir().unwrap();
    let ctx = context(dir.path().to_str().unwrap());
    seed(&ctx, RealmNodeKind::Management).await;
    let mut auth = RealmAuthorizationDocument::default_realm_doc(actor().realm_id);
    for role in auth
        .roles
        .values_mut()
        .filter(|role| role.name == "realm_admin")
    {
        role.assigned_users.insert(actor().user_id);
    }
    let write = StorageEffect::Write {
        key_space: AUTH_KEYSPACE.to_string(),
        key: actor().realm_id.as_bytes().to_vec().into(),
        value: postcard::to_allocvec(&auth).unwrap().into(),
        txn_id: None,
    };
    ctx.storage_handle.send_storage_effect(write).await;
    let notices = || async {
        let iter = StorageEffect::Iter {
            key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 10,
            txn_id: None,
        };
        match ctx.storage_handle.send_storage_effect(iter).await {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
            other => panic!("unexpected {other:?}"),
        }
    };
    run(&ctx, create(), 10).await.unwrap();
    assert_eq!(notices().await, 0, "a secret without rules widens nothing");
    let rule = UseRule {
        consumer: SecretConsumer::Plugin {
            plugin_id: "announcer".to_string(),
            install_id: Ulid::from_bytes([3; 16]),
        },
        delivery: Delivery::Inject,
        origins: vec!["https://api.example.org".to_string()],
        paths: Vec::new(),
        methods: Vec::new(),
        response: ResponseExposure::Status,
        groups: vec![Ulid::from_bytes([7; 16])],
    };
    let widen = SecretChange::Replace {
        name: name(),
        description: String::new(),
        settings: Default::default(),
        uses: vec![rule],
        expected: None,
    };
    run(&ctx, widen, 11).await.unwrap();
    assert_eq!(notices().await, 1, "the one realm admin is told");
}
