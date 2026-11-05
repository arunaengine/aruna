//! Tests the realm admin grant and removal transitions without storage or network.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::document::DocumentOutboxRecord;
use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
use ulid::Ulid;

fn realm_id() -> RealmId {
    RealmId::from_bytes([3u8; 32])
}

fn user(seed: u8) -> UserId {
    UserId::local(Ulid::from_bytes([seed; 16]), realm_id())
}

fn config(target: UserId, admin: bool) -> SetAdminConfig {
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
        user_id: user(1),
        realm_id: realm_id(),
    };
    SetAdminConfig {
        auth_context: AuthContext {
            user_id: actor.user_id,
            realm_id: actor.realm_id,
            path_restrictions: None,
            session: None,
        },
        actor,
        user_id: target,
        admin,
    }
}

/// Steps an allowed caller to the read result of a realm with these admins.
fn step_to_write(operation: &mut SetAdminOperation, admins: &[UserId]) -> Effects {
    let actor = operation.config.actor.clone();
    let mut realm_config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    realm_config.ensure_node(actor.node_id, RealmNodeKind::Management);
    let mut auth_doc = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
    for role in auth_doc.roles.values_mut() {
        role.assigned_users.extend(admins.iter().copied());
    }
    assert!(matches!(
        operation.start().as_slice(),
        [Effect::SubOperation(_)]
    ));
    operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
    ));
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::from_bytes([7u8; 16]),
    }));
    let value = |bytes: Vec<u8>| Some(Value::from(bytes));
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                Key::from(b"config".as_slice()),
                value(realm_config.to_bytes(&actor).unwrap()),
            ),
            (
                Key::from(b"auth".as_slice()),
                value(auth_doc.to_bytes(&actor).unwrap()),
            ),
            (Key::from(b"state".as_slice()), None),
        ],
    }))
}

/// The stored admins and the replicated admin operations of one batch write.
fn written(effects: &Effects) -> (HashSet<UserId>, Vec<AdminDocumentOperation>) {
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected one batch write, got {effects:?}");
    };
    let auth_doc = RealmAuthorizationDocument::from_bytes(&writes[0].2).unwrap();
    let admins = auth_doc.roles.into_values().next().unwrap().assigned_users;
    let ops = writes
        .iter()
        .filter(|(key_space, _, _)| key_space == SYNC_OUTBOX_KEYSPACE)
        .map(|(_, _, value)| {
            let record: DocumentOutboxRecord = postcard::from_bytes(value).unwrap();
            match record.event {
                DocumentOutboxEvent::AdminOperation { event, .. } => event.op,
                other => panic!("expected an admin operation, got {other:?}"),
            }
        })
        .collect();
    (admins, ops)
}

#[test]
fn grants_admin_role() {
    let mut operation = SetAdminOperation::new(config(user(2), true));
    let effects = step_to_write(&mut operation, &[user(1)]);
    let (admins, ops) = written(&effects);
    assert_eq!(admins, HashSet::from([user(1), user(2)]));
    assert!(matches!(
        ops.as_slice(),
        [
            AdminDocumentOperation::RealmRoleAdded { .. },
            AdminDocumentOperation::RealmAssignmentAdded { user_id, .. },
        ] if *user_id == user(2)
    ));

    operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    let txn_id = Ulid::from_bytes([7u8; 16]);
    let commit = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id,
    }));
    assert!(matches!(commit.as_slice(), [Effect::Task(_)]));
    operation.step(Event::Task(TaskEvent::TimerScheduled {
        key: aruna_core::task::TaskKey::DrainSyncOutbox,
        after: std::time::Duration::ZERO,
    }));
    assert!(operation.is_complete());
    assert_eq!(operation.finalize(), Ok(HashSet::from([user(1), user(2)])));
}

#[test]
fn removes_with_epochs() {
    // A removal narrows access, so key epochs are marked due before the commit.
    let mut operation = SetAdminOperation::new(config(user(2), false));
    let effects = step_to_write(&mut operation, &[user(1), user(2)]);
    let (admins, ops) = written(&effects);
    assert_eq!(admins, HashSet::from([user(1)]));
    assert!(matches!(
        ops.last(),
        Some(AdminDocumentOperation::RealmAssignmentRemoved { user_id, .. }) if *user_id == user(2)
    ));

    let marking = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(marking.as_slice(), [Effect::SubOperation(_)]));
    let commit = operation.step(Event::SubOperation(SubOperationEvent::EpochsMarked {
        result: Ok(()),
    }));
    assert!(matches!(
        commit.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { .. })]
    ));
}

#[test]
fn keeps_last_admin() {
    let mut operation = SetAdminOperation::new(config(user(1), false));
    let effects = step_to_write(&mut operation, &[user(1)]);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(operation.finalize(), Err(SetAdminError::LastAdmin));
}

#[test]
fn refuses_non_admin() {
    let mut operation = SetAdminOperation::new(config(user(2), true));
    operation.start();
    let effects = operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
    ));
    assert!(effects.is_empty());
    assert_eq!(operation.finalize(), Err(SetAdminError::Unauthorized));

    let mut operation = SetAdminOperation::new(config(UserId::nil(realm_id()), true));
    assert!(operation.start().is_empty());
    assert_eq!(operation.finalize(), Err(SetAdminError::InvalidUser));
}
