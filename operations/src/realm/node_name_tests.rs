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
use aruna_core::structs::identity::realm::{NodeCoordinates, NodePlace};

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

fn config(node_id: NodeId, name: Option<&str>, scope: NameScope, caller: UserId) -> NodeNameConfig {
    NodeNameConfig {
        actor: Actor {
            user_id: caller,
            ..actor()
        },
        auth_context: auth(caller),
        node_id,
        change: NodeChange {
            display_name: Some(name.map(str::to_string)),
            ..NodeChange::default()
        },
        scope,
    }
}

fn named(name: Option<&str>) -> Result<NodeDisplay, NodeNameError> {
    Ok(NodeDisplay {
        name: name.map(str::to_string),
        place: None,
    })
}

/// Drives the operation to its stored reducer state, given the state it reads.
fn drive_write(
    operation: &mut NodeNameOperation,
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

fn finish(mut operation: NodeNameOperation) -> Result<NodeDisplay, NodeNameError> {
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

fn refusal(operation: NodeNameOperation, effects: Effects) -> NodeNameError {
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
    let mut operation = NodeNameOperation::new(config(
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
    assert_eq!(finish(operation), named(Some("Giessen storage 1")));
}

#[test]
fn retry_rereads_config() {
    // A commit lost to a background config write reads again and keeps a name set elsewhere.
    let document = document_bytes(RealmNodeKind::Management).to_vec();
    let target = aruna_core::admin_documents::AdminDocumentTarget::RealmConfig { realm_id: REALM };
    let mut elsewhere = aruna_core::reducer::AdminDocumentState::new(target);
    let laptop = AdminDocumentOperation::NodeNameSet {
        node_id: node(3),
        display_name: Some("laptop".to_string()),
    };
    elsewhere.apply_operation(&actor(), laptop).unwrap();
    let (_, _, state) = reducer_state_entry(&elsewhere).unwrap();
    let mut operation = NodeNameOperation::new(config(
        node(2),
        Some("store"),
        NameScope::RealmAdmin,
        user(1),
    ));
    let writes = crate::realm::commit_retry::conflict_then_write(
        &mut operation,
        vec![Some(document.clone()), None],
        vec![Some(document), Some(state.to_vec())],
    );
    let (_, _, written) = writes
        .iter()
        .find(|(space, _, _)| space == aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE)
        .expect("the retry rewrites the reducer state");
    let names = decode_reducer_state(written)
        .unwrap()
        .materialized_node_names();
    assert_eq!(names.get(&node(3)).map(String::as_str), Some("laptop"));
    assert_eq!(names.get(&node(2)).map(String::as_str), Some("store"));
}

#[test]
fn admin_clears_name() {
    // A clear must replace the stored name, not leave it behind.
    let mut first =
        NodeNameOperation::new(config(node(2), Some("old"), NameScope::RealmAdmin, user(1)));
    let stored = written_state(&drive_write(&mut first, None, RealmNodeKind::Management));
    let mut operation =
        NodeNameOperation::new(config(node(2), None, NameScope::RealmAdmin, user(1)));
    let effects = drive_write(&mut operation, Some(&stored), RealmNodeKind::Management);
    assert!(written_state(&effects).materialized_node_names().is_empty());
    assert_eq!(finish(operation), named(None));
}

#[test]
fn owner_names_device() {
    // The owner needs no admin grant for its own device, so no permission check runs.
    let mut operation =
        NodeNameOperation::new(config(node(3), Some("Laptop"), NameScope::Owner, user(3)));
    let effects = drive_write(&mut operation, None, RealmNodeKind::Management);
    assert_eq!(
        written_state(&effects).materialized_node_names(),
        BTreeMap::from([(node(3), "Laptop".to_string())])
    );
    assert_eq!(finish(operation), named(Some("Laptop")));
}

#[test]
fn refuses_invalid_name() {
    // An invalid name fails before any transaction starts.
    let long = "a".repeat(MAX_NODE_NAME + 1);
    for name in ["   ", "bell\u{7}", long.as_str()] {
        let mut operation =
            NodeNameOperation::new(config(node(2), Some(name), NameScope::RealmAdmin, user(1)));
        let effects = operation.start();
        assert!(effects.is_empty());
        assert_eq!(refusal(operation, effects), NodeNameError::InvalidName);
    }
}

#[test]
fn refuses_unknown_node() {
    let mut operation = NodeNameOperation::new(config(
        node(7),
        Some("ghost"),
        NameScope::RealmAdmin,
        user(1),
    ));
    let effects = drive_write(&mut operation, None, RealmNodeKind::Management);
    assert_eq!(
        refusal(operation, effects),
        NodeNameError::UnknownNode { node_id: node(7) }
    );
}

#[test]
fn refuses_foreign_nodes() {
    // The owner scope reaches only the caller's own device, never a server or another device.
    for (node_id, caller) in [(node(3), user(4)), (node(2), user(3))] {
        let mut operation =
            NodeNameOperation::new(config(node_id, Some("mine"), NameScope::Owner, caller));
        let effects = drive_write(&mut operation, None, RealmNodeKind::Management);
        assert_eq!(
            refusal(operation, effects),
            NodeNameError::UnknownNode { node_id }
        );
    }
}

#[test]
fn refuses_unauthorized_callers() {
    let mut operation =
        NodeNameOperation::new(config(node(2), Some("x"), NameScope::RealmAdmin, user(1)));
    assert!(matches!(
        operation.start().as_slice(),
        [Effect::SubOperation(_)]
    ));
    let effects = operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
    ));
    assert_eq!(refusal(operation, effects), NodeNameError::Unauthorized);

    // A delegated token cannot speak for the owner.
    let mut delegated = config(node(3), Some("x"), NameScope::Owner, user(3));
    delegated.auth_context.path_restrictions = Some(vec![PathRestriction {
        pattern: "/**".to_string(),
        permission: Permission::WRITE,
    }]);
    let mut operation = NodeNameOperation::new(delegated);
    let effects = operation.start();
    assert_eq!(refusal(operation, effects), NodeNameError::Unauthorized);
}

#[test]
fn refuses_server_origin() {
    // Peers drop realm config events from other origins, so only a management node writes.
    let mut operation =
        NodeNameOperation::new(config(node(2), Some("x"), NameScope::RealmAdmin, user(1)));
    let effects = drive_write(&mut operation, None, RealmNodeKind::Server);
    assert_eq!(
        refusal(operation, effects),
        NodeNameError::NotManagementNode
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
    assert_eq!(empty.finalize(), Ok(NodeDisplays::default()));

    let mut named = NodeNameOperation::new(config(
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
    let displays = operation.finalize().unwrap();
    assert_eq!(
        displays.names,
        BTreeMap::from([(node(2), "Giessen".to_string())])
    );
}

fn placed(change: NodeChange) -> NodeNameOperation {
    NodeNameOperation::new(NodeNameConfig {
        change,
        ..config(node(2), None, NameScope::RealmAdmin, user(1))
    })
}

// A place change keeps the name and the other place field, and a cleared place goes whole.
#[test]
fn places_merge_clear() {
    let giessen = NodeCoordinates::from_degrees(50.58, 8.68);
    let mut first = placed(NodeChange {
        display_name: Some(Some("Storage 1".to_string())),
        place: Some(Some(" Giessen ".to_string())),
        coordinates: None,
    });
    let stored = written_state(&drive_write(&mut first, None, RealmNodeKind::Management));
    let place = |name: &str, coordinates| NodePlace {
        name: Some(name.to_string()),
        coordinates,
    };
    assert_eq!(
        stored.materialized_node_places(),
        BTreeMap::from([(node(2), place("Giessen", None))])
    );

    let mut moved = placed(NodeChange {
        coordinates: Some(giessen),
        ..NodeChange::default()
    });
    let effects = drive_write(&mut moved, Some(&stored), RealmNodeKind::Management);
    let stored = written_state(&effects);
    assert_eq!(stored.materialized_node_names()[&node(2)], "Storage 1");
    assert_eq!(
        stored.materialized_node_places()[&node(2)],
        place("Giessen", giessen)
    );
    assert_eq!(
        finish(moved),
        Ok(NodeDisplay {
            name: Some("Storage 1".to_string()),
            place: Some(place("Giessen", giessen)),
        })
    );

    let mut cleared = placed(NodeChange {
        place: Some(None),
        coordinates: Some(None),
        ..NodeChange::default()
    });
    let stored = written_state(&drive_write(
        &mut cleared,
        Some(&stored),
        RealmNodeKind::Management,
    ));
    assert!(stored.materialized_node_places().is_empty());
    assert_eq!(stored.materialized_node_names()[&node(2)], "Storage 1");
}

#[test]
fn refuses_invalid_places() {
    let out = NodeCoordinates {
        lat_e7: 900_000_001,
        lon_e7: 0,
    };
    let changes = [
        (NodeChange::default(), NodeNameError::EmptyChange),
        (
            NodeChange {
                place: Some(Some("\u{7}".to_string())),
                ..NodeChange::default()
            },
            NodeNameError::InvalidName,
        ),
        (
            NodeChange {
                coordinates: Some(Some(out)),
                ..NodeChange::default()
            },
            NodeNameError::InvalidCoordinates,
        ),
    ];
    for (change, error) in changes {
        let mut operation = placed(change);
        let effects = operation.start();
        assert_eq!(refusal(operation, effects), error);
    }
}
