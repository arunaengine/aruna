//! Tests the node secret operation's transitions and effects without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::secrets::tests::{actor, create, name, token};
use aruna_core::keyspaces::ADMIN_AUDIT_KEYSPACE;
use byteview::ByteView;

const TXN: Ulid = Ulid::from_bytes([9; 16]);

fn change(change: SecretChange, write: u8) -> NodeSecretOperation {
    NodeSecretOperation::new(NodeChange {
        realm_id: actor().realm_id,
        node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
        actor: actor(),
        change,
        now_ms: u64::from(write),
        write_id: Ulid::from_bytes([write; 16]),
    })
}

fn storage(event: StorageEvent) -> Event {
    Event::Storage(event)
}

/// Starts the operation and answers the record read with `stored` and no realm roles.
fn read(operation: &mut NodeSecretOperation, stored: Option<&SecretRecord>) -> Effects {
    read_with(operation, stored, None)
}

fn read_with(
    operation: &mut NodeSecretOperation,
    stored: Option<&SecretRecord>,
    auth: Option<Vec<u8>>,
) -> Effects {
    let effects = operation.start();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    ));
    let effects = operation.step(storage(StorageEvent::TransactionStarted { txn_id: TXN }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads, txn_id: Some(TXN) })]
            if reads[0].0 == NODE_SECRETS_KEYSPACE && reads[1].0 == AUTH_KEYSPACE
    ));
    let value = stored.map(|info| ByteView::from(postcard::to_allocvec(info).unwrap()));
    let values = vec![
        (ByteView::from(record_key(&name())), value),
        (ByteView::from(Vec::new()), auth.map(ByteView::from)),
    ];
    operation.step(storage(StorageEvent::BatchReadResult { values }))
}

fn commit(operation: &mut NodeSecretOperation, effects: &Effects) {
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction {
            txn_id: TXN
        })]
    ));
    let effects = operation.step(storage(StorageEvent::TransactionCommitted { txn_id: TXN }));
    assert!(effects.is_empty() && operation.is_complete());
}

fn stored_info() -> SecretRecord {
    let mut operation = change(create(), 10);
    let effects = read(&mut operation, None);
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the record and audit write, got {effects:?}");
    };
    let row = writes
        .iter()
        .find(|(space, ..)| space == NODE_SECRETS_KEYSPACE)
        .unwrap();
    postcard::from_bytes(&row.2).unwrap()
}

#[test]
fn create_writes_record() {
    let mut operation = change(create(), 10);
    let effects = read(&mut operation, None);
    let [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })] = effects.as_slice() else {
        panic!("expected the record and audit write, got {effects:?}");
    };
    assert_eq!(*txn_id, Some(TXN));
    let spaces: Vec<_> = writes.iter().map(|(space, ..)| space.as_str()).collect();
    assert_eq!(spaces, [ADMIN_AUDIT_KEYSPACE, NODE_SECRETS_KEYSPACE]);
    let effects = operation.step(storage(StorageEvent::BatchWriteResult { entries: vec![] }));
    let [Effect::Storage(StorageEffect::VaultWrite { entry, txn_id, .. })] = effects.as_slice()
    else {
        panic!("expected the vault write, got {effects:?}");
    };
    assert_eq!(
        (*entry, *txn_id),
        (value_entry(Ulid::from_bytes([10; 16])), Some(TXN))
    );
    assert!(!format!("{effects:?}").contains("canary"));
    let key = ByteView::from(entry.key());
    let effects = operation.step(storage(StorageEvent::WriteResult { key }));
    commit(&mut operation, &effects);
    let info = operation.finalize().unwrap().unwrap();
    assert_eq!(info.value_id, Ulid::from_bytes([10; 16]));
}

#[test]
fn rotate_replaces_value() {
    let current = stored_info();
    let rotate = SecretChange::Rotate {
        name: name(),
        fields: token("canary-77aa"),
        expected: Some(current.revision_token()),
    };
    let mut operation = change(rotate, 11);
    let effects = read(&mut operation, Some(&current));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchWrite { .. })]
    ));
    let effects = operation.step(storage(StorageEvent::BatchWriteResult { entries: vec![] }));
    let [Effect::Storage(StorageEffect::VaultWrite { entry, .. })] = effects.as_slice() else {
        panic!("expected the new value, got {effects:?}");
    };
    assert_eq!(*entry, value_entry(Ulid::from_bytes([11; 16])));
    let key = ByteView::from(entry.key());
    let effects = operation.step(storage(StorageEvent::WriteResult { key }));
    let [Effect::Storage(StorageEffect::VaultDelete { entry, .. })] = effects.as_slice() else {
        panic!("expected the old value removal, got {effects:?}");
    };
    assert_eq!(*entry, value_entry(current.value_id));
    let key = ByteView::from(entry.key());
    let effects = operation.step(storage(StorageEvent::DeleteResult { key }));
    commit(&mut operation, &effects);
    assert!(operation.finalize().unwrap().is_some());
}

#[test]
fn delete_removes_both() {
    let current = stored_info();
    let delete = SecretChange::Delete {
        name: name(),
        expected: None,
    };
    let mut operation = change(delete, 12);
    let effects = read(&mut operation, Some(&current));
    let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = effects.as_slice() else {
        panic!("expected the record removal, got {effects:?}");
    };
    assert_eq!(deletes[0].0, NODE_SECRETS_KEYSPACE);
    let effects = operation.step(storage(StorageEvent::BatchDeleteResult { entries: vec![] }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchWrite { .. })]
    ));
    let effects = operation.step(storage(StorageEvent::BatchWriteResult { entries: vec![] }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::VaultDelete { .. })]
    ));
    let key = ByteView::from(value_entry(current.value_id).key());
    let effects = operation.step(storage(StorageEvent::DeleteResult { key }));
    commit(&mut operation, &effects);
    assert_eq!(operation.finalize(), Ok(None));
}

#[test]
fn refusal_aborts() {
    let current = stored_info();
    let mut operation = change(create(), 13);
    let effects = read(&mut operation, Some(&current));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction {
            txn_id: TXN
        })]
    ));
    assert!(operation.is_complete());
    let error = operation.finalize().unwrap_err();
    assert_eq!(error, NodeSecretError::Change(ChangeError::Exists));
}

#[test]
fn wrong_event_fails() {
    let mut operation = change(create(), 14);
    let _ = read(&mut operation, None);
    let effects = operation.step(storage(StorageEvent::SyncAllFinished));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction {
            txn_id: TXN
        })]
    ));
    let error = operation.finalize().unwrap_err();
    assert!(matches!(error, NodeSecretError::UnexpectedEvent { .. }));
}

#[test]
fn widening_notifies_admins() {
    use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
    use aruna_core::secrets::{Delivery, ResponseExposure, SecretConsumer, UseRule};
    use aruna_core::structs::execution::notification::NotificationKind;
    use aruna_core::structs::execution::notification::NotificationOutboxRecord;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use aruna_core::task::TaskEvent;
    let current = stored_info();
    let mut auth = RealmAuthorizationDocument::default_realm_doc(actor().realm_id);
    let other = UserId::local(Ulid::from_bytes([6; 16]), actor().realm_id);
    for role in auth
        .roles
        .values_mut()
        .filter(|role| role.name == "realm_admin")
    {
        role.assigned_users.extend([actor(), other]);
    }
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
    let mut operation = change(widen.clone(), 11);
    let bytes = postcard::to_allocvec(&auth).unwrap();
    let effects = read_with(&mut operation, Some(&current), Some(bytes.clone()));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the record, audit and notice writes, got {effects:?}");
    };
    let notices: Vec<NotificationOutboxRecord> = (writes.iter())
        .filter(|(space, ..)| space == NOTIFICATION_OUTBOX_KEYSPACE)
        .map(|(_, _, value)| postcard::from_bytes(value).unwrap())
        .collect();
    let mut recipients: Vec<_> = notices
        .iter()
        .map(|notice| notice.record.recipient)
        .collect();
    recipients.sort();
    assert_eq!(recipients, [actor(), other], "the acting admin is told too");
    assert!(matches!(
        &notices[0].record.kind,
        NotificationKind::SecretUseWidened { name: notified, group_id: None, node_id: Some(_), .. }
            if notified == "catalog-key"
    ));
    let effects = operation.step(storage(StorageEvent::BatchWriteResult { entries: vec![] }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { .. })]
    ));
    let effects = operation.step(storage(StorageEvent::TransactionCommitted { txn_id: TXN }));
    assert!(
        matches!(effects.as_slice(), [Effect::Task(_)]),
        "the notice drain is scheduled"
    );
    let key = aruna_core::task::TaskKey::DrainNotificationOutbox;
    let after = std::time::Duration::ZERO;
    operation.step(Event::Task(TaskEvent::TimerScheduled { key, after }));
    assert!(operation.finalize().unwrap().is_some());

    // The same rules again widen nothing and send no notice.
    let mut stored = current.clone();
    stored.uses = match &widen {
        SecretChange::Replace { uses, .. } => uses.clone(),
        _ => Vec::new(),
    };
    let mut again = change(widen, 12);
    let effects = read_with(&mut again, Some(&stored), Some(bytes));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the record and audit write, got {effects:?}");
    };
    assert!(
        !writes
            .iter()
            .any(|(space, ..)| space == NOTIFICATION_OUTBOX_KEYSPACE)
    );
}
