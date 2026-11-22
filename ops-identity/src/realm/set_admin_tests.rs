//! Tests the realm admin grant and removal transitions without storage or network.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::ChildOutcome;
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

#[test]
fn conflict_reruns_change() {
    // A commit lost to a background config write reruns the whole change, reading again.
    let conflict = SetAdminError::from(IoError::Conflict);
    assert!(SetAdminWrite::is_conflict(&conflict));
    assert_eq!(
        SetAdminWrite::conflict_retries(),
        COMMIT_ATTEMPTS as u32 - 1
    );
    assert_eq!(
        SetAdminWrite::new(config(user(5), true)).transaction(),
        TransactionMode::Write
    );
}

/// Feeds the read of a realm with these admins to a fresh change transaction.
fn change(
    target: UserId,
    admin: bool,
    admins: &[UserId],
) -> (SetAdminWrite, Result<Next<HashSet<UserId>>, SetAdminError>) {
    let mut operation = SetAdminWrite::new(config(target, admin));
    let actor = operation.config.actor.clone();
    let mut realm_config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    realm_config.ensure_node(actor.node_id, RealmNodeKind::Management);
    let mut auth_doc = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
    for role in auth_doc.roles.values_mut() {
        role.assigned_users.extend(admins.iter().copied());
    }
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 3));
    let value = |bytes: Vec<u8>| Some(Value::from(bytes));
    let rows = vec![
        value(realm_config.to_bytes(&actor).unwrap()),
        value(auth_doc.to_bytes(&actor).unwrap()),
        None,
    ];
    let turn = operation.next(IoResult::Values(rows));
    (operation, turn)
}

/// The stored admins and the replicated admin operations of one write call.
fn written(
    turn: Result<Next<HashSet<UserId>>, SetAdminError>,
) -> (HashSet<UserId>, Vec<AdminDocumentOperation>) {
    let Ok(Next::Call(IoCall::Write(writes))) = turn else {
        panic!("expected one write, got {turn:?}");
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
    let (mut operation, turn) = change(user(2), true, &[user(1)]);
    let (admins, ops) = written(turn);
    assert_eq!(admins, HashSet::from([user(1), user(2)]));
    assert!(matches!(
        ops.as_slice(),
        [
            AdminDocumentOperation::RealmRoleAdded { .. },
            AdminDocumentOperation::RealmAssignmentAdded { user_id, .. },
        ] if *user_id == user(2)
    ));
    // The drain timer runs after the commit; the admins are the outcome.
    let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
        panic!("expected the outcome with the drain timer");
    };
    assert_eq!(output, HashSet::from([user(1), user(2)]));
    assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
}

#[test]
fn removes_with_epochs() {
    // A removal narrows access, so key epochs are marked due before the commit.
    let (mut operation, turn) = change(user(2), false, &[user(1), user(2)]);
    let (admins, ops) = written(turn);
    assert_eq!(admins, HashSet::from([user(1)]));
    assert!(matches!(
        ops.last(),
        Some(AdminDocumentOperation::RealmAssignmentRemoved { user_id, .. }) if *user_id == user(2)
    ));
    let marking = operation.next(IoResult::Done);
    assert!(matches!(marking, Ok(Next::Call(IoCall::Page { .. }))));
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert!(matches!(operation.next(empty), Ok(Next::Then { .. })));
}

#[test]
fn keeps_last_admin() {
    let (_, turn) = change(user(1), false, &[user(1)]);
    assert_eq!(turn, Err(SetAdminError::LastAdmin));
}

#[test]
fn refuses_non_admin() {
    let mut operation = SetAdminOperation::new(config(user(2), true));
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let denied = ChildOutcome::new(Ok::<bool, AuthorizationError>(false));
    assert_eq!(
        operation.next(IoResult::Child(denied)),
        Err(SetAdminError::Unauthorized)
    );

    let mut operation = SetAdminOperation::new(config(UserId::nil(realm_id()), true));
    assert_eq!(operation.first(), Err(SetAdminError::InvalidUser));
}
