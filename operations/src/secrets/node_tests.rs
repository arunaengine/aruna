//! Tests the node secret operations as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::secrets::tests::{actor, create, name, token};
use aruna_core::keyspaces::ADMIN_AUDIT_KEYSPACE;
use byteview::ByteView;

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

fn storage(event: StorageEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Storage(event)))
}

/// The storage effect a step passes through.
fn passed(next: &Next<Option<SecretRecord>>) -> &StorageEffect {
    match next {
        Next::Call(IoCall::Effect(effect)) => match effect.as_ref() {
            Effect::Storage(storage) => storage,
            other => panic!("expected a storage effect, got {other:?}"),
        },
        other => panic!("expected a passed-through effect, got {other:?}"),
    }
}

/// Starts the operation and answers the record read with `stored` and no realm roles.
fn read(
    operation: &mut NodeSecretOperation,
    stored: Option<&SecretRecord>,
) -> Result<Next<Option<SecretRecord>>, NodeSecretError> {
    read_with(operation, stored, None)
}

fn read_with(
    operation: &mut NodeSecretOperation,
    stored: Option<&SecretRecord>,
    auth: Option<Vec<u8>>,
) -> Result<Next<Option<SecretRecord>>, NodeSecretError> {
    assert_eq!(
        operation.transaction(),
        aruna_core::operation::TransactionMode::Write
    );
    let first = operation.first().unwrap();
    assert!(matches!(
        &first,
        Next::Call(IoCall::Read(reads))
            if reads[0].0 == NODE_SECRETS_KEYSPACE && reads[1].0 == AUTH_KEYSPACE
    ));
    let value = stored.map(|info| ByteView::from(postcard::to_allocvec(info).unwrap()));
    operation.next(IoResult::Values(vec![value, auth.map(ByteView::from)]))
}

fn stored_info() -> SecretRecord {
    let mut operation = change(create(), 10);
    let next = read(&mut operation, None).unwrap();
    let Next::Call(IoCall::Write(writes)) = next else {
        panic!("expected the record and audit write, got {next:?}");
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
    let next = read(&mut operation, None).unwrap();
    let Next::Call(IoCall::Write(writes)) = next else {
        panic!("expected the record and audit write, got {next:?}");
    };
    let spaces: Vec<_> = writes.iter().map(|(space, ..)| space.as_str()).collect();
    assert_eq!(spaces, [ADMIN_AUDIT_KEYSPACE, NODE_SECRETS_KEYSPACE]);
    let next = operation.next(IoResult::Done).unwrap();
    let StorageEffect::VaultWrite { entry, txn_id, .. } = passed(&next) else {
        panic!("expected the vault write, got {next:?}");
    };
    assert_eq!(
        (*entry, *txn_id),
        (value_entry(Ulid::from_bytes([10; 16])), None)
    );
    assert!(!format!("{next:?}").contains("canary"));
    let key = ByteView::from(entry.key());
    let done = operation.next(storage(StorageEvent::WriteResult { key }));
    let Ok(Next::Then {
        output: Some(info),
        calls,
    }) = done
    else {
        panic!("expected the stored record, got {done:?}");
    };
    assert_eq!(info.value_id, Ulid::from_bytes([10; 16]));
    assert!(calls.is_empty(), "no notice, no drain");
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
    let next = read(&mut operation, Some(&current)).unwrap();
    assert!(matches!(next, Next::Call(IoCall::Write(_))));
    let next = operation.next(IoResult::Done).unwrap();
    let StorageEffect::VaultWrite { entry, .. } = passed(&next) else {
        panic!("expected the new value, got {next:?}");
    };
    assert_eq!(*entry, value_entry(Ulid::from_bytes([11; 16])));
    let key = ByteView::from(entry.key());
    let next = operation
        .next(storage(StorageEvent::WriteResult { key }))
        .unwrap();
    let StorageEffect::VaultDelete { entry, .. } = passed(&next) else {
        panic!("expected the old value removal, got {next:?}");
    };
    assert_eq!(*entry, value_entry(current.value_id));
    let key = ByteView::from(entry.key());
    let done = operation.next(storage(StorageEvent::DeleteResult { key }));
    assert!(matches!(
        done,
        Ok(Next::Then {
            output: Some(_),
            ..
        })
    ));
}

#[test]
fn delete_removes_both() {
    let current = stored_info();
    let delete = SecretChange::Delete {
        name: name(),
        expected: None,
    };
    let mut operation = change(delete, 12);
    let next = read(&mut operation, Some(&current)).unwrap();
    let Next::Call(IoCall::Delete(deletes)) = next else {
        panic!("expected the record removal, got {next:?}");
    };
    assert_eq!(deletes[0].0, NODE_SECRETS_KEYSPACE);
    let next = operation.next(IoResult::Done).unwrap();
    assert!(matches!(next, Next::Call(IoCall::Write(_))));
    let next = operation.next(IoResult::Done).unwrap();
    assert!(matches!(passed(&next), StorageEffect::VaultDelete { .. }));
    let key = ByteView::from(value_entry(current.value_id).key());
    let done = operation.next(storage(StorageEvent::DeleteResult { key }));
    assert!(matches!(done, Ok(Next::Then { output: None, .. })));
}

#[test]
fn refusal_fails() {
    let current = stored_info();
    let mut operation = change(create(), 13);
    let error = read(&mut operation, Some(&current)).unwrap_err();
    assert_eq!(error, NodeSecretError::Change(ChangeError::Exists));
}

#[test]
fn wrong_event_fails() {
    let mut operation = change(create(), 14);
    let _ = read(&mut operation, None).unwrap();
    let error = operation.next(storage(StorageEvent::SyncAllFinished));
    assert!(matches!(
        error,
        Err(NodeSecretError::Storage(StorageError::ReadError(_)))
    ));
}

#[test]
fn widening_notifies_admins() {
    use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
    use aruna_core::secrets::{Delivery, ResponseExposure, SecretConsumer, UseRule};
    use aruna_core::structs::execution::notification::NotificationKind;
    use aruna_core::structs::execution::notification::NotificationOutboxRecord;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
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
    let next = read_with(&mut operation, Some(&current), Some(bytes.clone())).unwrap();
    let Next::Call(IoCall::Write(writes)) = next else {
        panic!("expected the record, audit and notice writes, got {next:?}");
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
    let done = operation.next(IoResult::Done);
    let Ok(Next::Then {
        output: Some(_),
        calls,
    }) = done
    else {
        panic!("expected the stored record, got {done:?}");
    };
    assert!(
        matches!(calls.as_slice(), [IoCall::Effect(effect)] if matches!(**effect, Effect::Task(_))),
        "the notice drain is scheduled"
    );

    // The same rules again widen nothing and send no notice.
    let mut stored = current.clone();
    stored.uses = match &widen {
        SecretChange::Replace { uses, .. } => uses.clone(),
        _ => Vec::new(),
    };
    let mut again = change(widen, 12);
    let next = read_with(&mut again, Some(&stored), Some(bytes)).unwrap();
    let Next::Call(IoCall::Write(writes)) = next else {
        panic!("expected the record and audit write, got {next:?}");
    };
    assert!(
        !writes
            .iter()
            .any(|(space, ..)| space == NOTIFICATION_OUTBOX_KEYSPACE)
    );
}
