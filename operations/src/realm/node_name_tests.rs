//! Tests node display name transitions: set, clear, refusals and the read of stored names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::UserId;
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::task::TaskKey;
use ulid::Ulid;

use super::*;

const REALM: RealmId = RealmId([5; 32]);

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn user(seed: u8) -> UserId {
    UserId::local(Ulid::from_bytes([seed; 16]), REALM)
}

fn actor() -> Actor {
    Actor {
        node_id: node(1),
        user_id: user(1),
        realm_id: REALM,
    }
}

fn auth(user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: REALM,
        path_restrictions: None,
        session: None,
    }
}

/// Management node 1 serves; node 2 is a server, node 3 a device of user 3.
fn document_bytes(serving: RealmNodeKind) -> Value {
    let mut document = RealmConfigDocument::new(REALM, Vec::new(), 3);
    document.ensure_node(node(1), serving);
    document.ensure_node(node(2), RealmNodeKind::Server);
    document.ensure_node(node(3), RealmNodeKind::User { owner: user(3) });
    document.to_bytes(&actor()).unwrap().into()
}

fn config(
    node_id: NodeId,
    name: Option<&str>,
    scope: NameScope,
    caller: UserId,
) -> SetNodeNameConfig {
    SetNodeNameConfig {
        actor: Actor {
            user_id: caller,
            ..actor()
        },
        auth_context: auth(caller),
        node_id,
        display_name: name.map(str::to_string),
        scope,
    }
}

/// Drives the operation to its stored reducer state, given the state it reads.
fn drive_write(
    operation: &mut SetNodeNameOperation,
    stored: Option<&AdminDocumentState>,
    serving: RealmNodeKind,
) -> Effects {
    let mut effects = operation.start();
    if matches!(effects.first(), Some(Effect::SubOperation(_))) {
        effects = operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
    }
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction { .. })]
    ));
    let txn_id = Ulid::from_bytes([9; 16]);
    let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { .. })]
    ));
    let state = stored.map(|state| postcard::to_allocvec(state).unwrap().into());
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (Key::from(&b"config"[..]), Some(document_bytes(serving))),
            (Key::from(&b"state"[..]), state),
        ],
    }))
}

/// The reducer state a batch write stores.
fn written_state(effects: &Effects) -> AdminDocumentState {
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected one batch write, got {effects:?}");
    };
    writes
        .iter()
        .find(|(key_space, _, _)| key_space == DOCUMENT_STATE_KEYSPACE)
        .map(|(_, _, value)| decode_reducer_state(value).unwrap())
        .expect("reducer state is written")
}

fn finish(mut operation: SetNodeNameOperation) -> Result<Option<String>, SetNodeNameError> {
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { .. })]
    ));
    let txn_id = Ulid::from_bytes([9; 16]);
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id,
    }));
    operation.step(Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::DrainSyncOutbox,
        after: Duration::ZERO,
    }));
    assert!(operation.is_complete());
    operation.finalize()
}

fn refusal(operation: SetNodeNameOperation, effects: Effects) -> SetNodeNameError {
    assert!(effects.iter().all(|effect| matches!(
        effect,
        Effect::Storage(StorageEffect::AbortTransaction { .. })
    )));
    assert!(operation.is_complete());
    operation.finalize().unwrap_err()
}

#[test]
fn admin_sets_name() {
    // The trimmed name is what every node stores and shows.
    let mut operation = SetNodeNameOperation::new(config(
        node(2),
        Some("  Giessen storage 1 "),
        NameScope::RealmAdmin,
        user(1),
    ));
    let effects = drive_write(&mut operation, None, RealmNodeKind::Management);
    let state = written_state(&effects);
    assert_eq!(
        state.materialized_node_names(),
        BTreeMap::from([(node(2), "Giessen storage 1".to_string())])
    );
    assert_eq!(finish(operation), Ok(Some("Giessen storage 1".to_string())));
}

#[test]
fn admin_clears_name() {
    // A clear must replace the stored name, not leave it behind.
    let mut named =
        SetNodeNameOperation::new(config(node(2), Some("old"), NameScope::RealmAdmin, user(1)));
    let stored = written_state(&drive_write(&mut named, None, RealmNodeKind::Management));
    let mut operation =
        SetNodeNameOperation::new(config(node(2), None, NameScope::RealmAdmin, user(1)));
    let effects = drive_write(&mut operation, Some(&stored), RealmNodeKind::Management);
    assert!(written_state(&effects).materialized_node_names().is_empty());
    assert_eq!(finish(operation), Ok(None));
}

#[test]
fn owner_names_device() {
    // The owner needs no admin grant for its own device, so no permission check runs.
    let mut operation =
        SetNodeNameOperation::new(config(node(3), Some("Laptop"), NameScope::Owner, user(3)));
    let effects = drive_write(&mut operation, None, RealmNodeKind::Management);
    assert_eq!(
        written_state(&effects).materialized_node_names(),
        BTreeMap::from([(node(3), "Laptop".to_string())])
    );
    assert_eq!(finish(operation), Ok(Some("Laptop".to_string())));
}

#[test]
fn refuses_invalid_name() {
    // An invalid name fails before any transaction starts.
    let long = "a".repeat(MAX_NODE_NAME + 1);
    for name in ["   ", "bell\u{7}", long.as_str()] {
        let mut operation =
            SetNodeNameOperation::new(config(node(2), Some(name), NameScope::RealmAdmin, user(1)));
        let effects = operation.start();
        assert!(effects.is_empty());
        assert_eq!(refusal(operation, effects), SetNodeNameError::InvalidName);
    }
}

#[test]
fn refuses_unknown_node() {
    let mut operation = SetNodeNameOperation::new(config(
        node(7),
        Some("ghost"),
        NameScope::RealmAdmin,
        user(1),
    ));
    let effects = drive_write(&mut operation, None, RealmNodeKind::Management);
    assert_eq!(
        refusal(operation, effects),
        SetNodeNameError::UnknownNode { node_id: node(7) }
    );
}

#[test]
fn refuses_foreign_nodes() {
    // The owner scope reaches only the caller's own device, never a server or another device.
    for (node_id, caller) in [(node(3), user(4)), (node(2), user(3))] {
        let mut operation =
            SetNodeNameOperation::new(config(node_id, Some("mine"), NameScope::Owner, caller));
        let effects = drive_write(&mut operation, None, RealmNodeKind::Management);
        assert_eq!(
            refusal(operation, effects),
            SetNodeNameError::UnknownNode { node_id }
        );
    }
}

#[test]
fn refuses_unauthorized_callers() {
    let mut operation =
        SetNodeNameOperation::new(config(node(2), Some("x"), NameScope::RealmAdmin, user(1)));
    assert!(matches!(
        operation.start().as_slice(),
        [Effect::SubOperation(_)]
    ));
    let effects = operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
    ));
    assert_eq!(refusal(operation, effects), SetNodeNameError::Unauthorized);

    // A delegated token cannot speak for the owner.
    let mut delegated = config(node(3), Some("x"), NameScope::Owner, user(3));
    delegated.auth_context.path_restrictions = Some(vec![PathRestriction {
        pattern: "/**".to_string(),
        permission: Permission::WRITE,
    }]);
    let mut operation = SetNodeNameOperation::new(delegated);
    let effects = operation.start();
    assert_eq!(refusal(operation, effects), SetNodeNameError::Unauthorized);
}

#[test]
fn refuses_server_origin() {
    // Peers drop realm config events from other origins, so only a management node writes.
    let mut operation =
        SetNodeNameOperation::new(config(node(2), Some("x"), NameScope::RealmAdmin, user(1)));
    let effects = drive_write(&mut operation, None, RealmNodeKind::Server);
    assert_eq!(
        refusal(operation, effects),
        SetNodeNameError::NotManagementNode
    );
}

#[test]
fn reads_stored_names() {
    let mut empty = ReadNamesOperation::new(REALM);
    empty.start();
    empty.step(Event::Storage(StorageEvent::ReadResult {
        key: Key::from(&b"state"[..]),
        value: None,
    }));
    assert_eq!(empty.finalize(), Ok(BTreeMap::new()));

    let mut named = SetNodeNameOperation::new(config(
        node(2),
        Some("Giessen"),
        NameScope::RealmAdmin,
        user(1),
    ));
    let stored = written_state(&drive_write(&mut named, None, RealmNodeKind::Management));
    let mut operation = ReadNamesOperation::new(REALM);
    operation.start();
    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: Key::from(&b"state"[..]),
        value: Some(postcard::to_allocvec(&stored).unwrap().into()),
    }));
    assert_eq!(
        operation.finalize(),
        Ok(BTreeMap::from([(node(2), "Giessen".to_string())]))
    );
}
