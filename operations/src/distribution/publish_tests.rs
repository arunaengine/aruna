// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::storage::distribution::presets::replica_layout;
use aruna_core::structs::storage::distribution::snapshot::install;
use aruna_core::structs::storage::distribution::{
    DistributionConfig, DistributionSettings, SetRoot, Statement, StatementKind,
};
use aruna_core::types::Key;

const PUBLISHER: u8 = 1;

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn root() -> SetRoot {
    SetRoot {
        publisher: secret(PUBLISHER).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    }
}

fn create() -> DistributionRequest {
    let nodes = [
        (Ulid::from_bytes([1; 16]), secret(1).public()),
        (Ulid::from_bytes([2; 16]), secret(2).public()),
    ];
    let layout = replica_layout(root().set_id(), &nodes).unwrap();
    DistributionRequest::Create {
        root: root(),
        config: DistributionConfig {
            bucket: "spire".to_string(),
            owner_group: root().owner_group,
            node_set: None,
            members: layout.members,
            ranges: layout.ranges,
            settings: DistributionSettings::default(),
        },
    }
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::from_bytes([3; 16]), realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

fn operation(node: u8, request: DistributionRequest) -> PublishOperation {
    PublishOperation::new(PublishConfig {
        realm_id: realm(),
        node_id: secret(node).public(),
        set_id: root().set_id(),
        request_id: Ulid::from_bytes([4; 16]),
        request,
        auth_context: Some(auth()),
    })
}

fn config_value() -> Value {
    let actor = Actor {
        node_id: secret(PUBLISHER).public(),
        user_id: auth().user_id,
        realm_id: realm(),
    };
    let mut document = RealmConfigDocument::new(realm(), Vec::new(), 3);
    document.ensure_node(secret(PUBLISHER).public(), RealmNodeKind::Server);
    document.to_bytes(&actor).unwrap().into()
}

#[test]
fn removed_publisher_refuses() {
    let actor = Actor {
        node_id: secret(2).public(),
        user_id: auth().user_id,
        realm_id: realm(),
    };
    let mut document = RealmConfigDocument::new(realm(), Vec::new(), 3);
    document.ensure_node(secret(2).public(), RealmNodeKind::Server);
    let config: Value = document.to_bytes(&actor).unwrap().into();
    let mut operation = operation(PUBLISHER, create());
    begin(&mut operation, [None, None, Some(config), None]);
    assert_eq!(operation.finalize(), Err(PublishError::NotPublisher));
}

fn view_value(snapshot: DistributionSnapshot) -> Value {
    let signed = SignedSnapshot::sign(snapshot, realm(), &secret(PUBLISHER)).unwrap();
    let installed = install(None, signed).unwrap();
    let aruna_core::structs::storage::distribution::snapshot::Installed::Changed(view) = installed
    else {
        panic!("first snapshot installs");
    };
    view.to_bytes().unwrap().into()
}

fn read(values: [Option<Value>; 4]) -> Event {
    Event::Storage(StorageEvent::BatchReadResult {
        values: values
            .into_iter()
            .map(|value| (Key::from(Vec::new()), value))
            .collect(),
    })
}

/// Drives an operation through its transaction start and state read.
fn begin(operation: &mut PublishOperation, values: [Option<Value>; 4]) -> Effects {
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::from_bytes([5; 16]),
    }));
    operation.step(read(values))
}

fn signed_reply(effects: &Effects) -> Event {
    let [Effect::Net(NetEffect::SnapshotSign(claim))] = effects.as_slice() else {
        panic!("expected a signing request, got {effects:?}");
    };
    let signed =
        SignedSnapshot::sign(claim.snapshot.clone(), claim.realm_id, &secret(PUBLISHER)).unwrap();
    Event::Net(NetEvent::SnapshotSign(SnapshotSignEvent::Signed(Box::new(
        signed,
    ))))
}

fn authorized(allowed: bool) -> Event {
    Event::SubOperation(SubOperationEvent::AuthorizationResult {
        allowed: Ok(allowed),
    })
}

#[test]
fn head_and_outbox_atomic() {
    let mut operation = operation(PUBLISHER, create());
    let effects = begin(&mut operation, [None, None, Some(config_value()), None]);
    assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
    let effects = operation.step(authorized(true));
    let effects = operation.step(signed_reply(&effects));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })] = effects.as_slice() else {
        panic!("expected one transactional batch, got {effects:?}");
    };
    assert_eq!(*txn_id, Some(Ulid::from_bytes([5; 16])));
    let spaces: Vec<&str> = writes.iter().map(|(space, ..)| space.as_str()).collect();
    assert_eq!(
        spaces,
        vec![
            DISTRIBUTION_KEYSPACE,
            DOCUMENT_STATE_KEYSPACE,
            DISTRIBUTION_ACCEPT_KEYSPACE,
            SYNC_OUTBOX_KEYSPACE,
            DISTRIBUTION_PUBLISHER_KEYSPACE
        ]
    );
    // Nothing is published before the commit; the drain follows it.
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { .. })]
    ));
    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_bytes([5; 16]),
    }));
    assert!(matches!(effects.as_slice(), [Effect::Task(_)]));
    operation.step(Event::Task(TaskEvent::TimerScheduled {
        key: aruna_core::TaskKey::DrainSyncOutbox,
        after: std::time::Duration::ZERO,
    }));
    assert_eq!(operation.finalize().unwrap().number, 1);
}

#[test]
fn admin_needs_write() {
    let mut operation = operation(PUBLISHER, create());
    begin(&mut operation, [None, None, Some(config_value()), None]);
    let effects = operation.step(authorized(false));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(operation.finalize(), Err(PublishError::Unauthorized));
}

#[test]
fn accepted_request_replays() {
    let revision = DistributionRevision {
        number: 3,
        digest: [8; 32],
    };
    let accepted = |request: &DistributionRequest, requester: Option<UserId>| -> Value {
        let row = AcceptedRequest {
            revision,
            fingerprint: *blake3::hash(&postcard::to_allocvec(request).unwrap()).as_bytes(),
            requester,
        };
        postcard::to_allocvec(&row).unwrap().into()
    };
    let mut replay = operation(PUBLISHER, create());
    let row = accepted(&create(), Some(auth().user_id));
    let effects = begin(&mut replay, [None, Some(row), None, None]);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    replay.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: Ulid::from_bytes([5; 16]),
    }));
    assert_eq!(replay.finalize(), Ok(revision));

    // The same id for another request or another requester is refused.
    let other = DistributionRequest::Activate;
    for row in [
        accepted(&other, Some(auth().user_id)),
        accepted(&create(), None),
    ] {
        let mut reused = operation(PUBLISHER, create());
        begin(&mut reused, [None, Some(row), None, None]);
        assert_eq!(reused.finalize(), Err(PublishError::RequestReused));
    }
}

#[test]
fn only_publisher_publishes() {
    let mut operation = operation(2, create());
    begin(&mut operation, [None, None, Some(config_value()), None]);
    assert_eq!(operation.finalize(), Err(PublishError::NotPublisher));
}

#[test]
fn stale_automatic_dropped() {
    let created = next_snapshot(realm(), None, &create()).unwrap();
    let settings = DistributionRequest::ChangeSettings {
        settings: DistributionSettings {
            public: true,
            ..DistributionSettings::default()
        },
    };
    let head = next_snapshot(realm(), Some(&created), &settings).unwrap();
    let split = Statement {
        realm_id: realm(),
        set_id: root().set_id(),
        statement_id: Ulid::from_bytes([6; 16]),
        member_id: Ulid::from_bytes([1; 16]),
        incarnation: 1,
        intent: [1; 32],
        kind: StatementKind::Split {
            basis: 1,
            range_id: head.config.ranges[0].range_id,
            epoch: 1,
            split_key: "m".to_string(),
            lower_id: Ulid::from_bytes([7; 16]),
            upper_id: Ulid::from_bytes([8; 16]),
        },
    }
    .sign(&secret(1));
    let request = DistributionRequest::Statement(Box::new(split));
    let mut operation = operation(PUBLISHER, request);
    begin(
        &mut operation,
        [Some(view_value(head)), None, Some(config_value()), None],
    );
    assert_eq!(operation.finalize(), Err(PublishError::Dropped));
}

#[test]
fn conflicted_set_stops() {
    let created = next_snapshot(realm(), None, &create()).unwrap();
    let mut fork = created.clone();
    fork.config.settings.public = true;
    let sign = |snapshot| SignedSnapshot::sign(snapshot, realm(), &secret(PUBLISHER)).unwrap();
    let view = DistributionView {
        head: sign(created),
        conflict: Some(sign(fork)),
    };
    let mut operation = operation(PUBLISHER, DistributionRequest::Activate);
    let value: Value = view.to_bytes().unwrap().into();
    begin(
        &mut operation,
        [Some(value), None, Some(config_value()), None],
    );
    assert_eq!(operation.finalize(), Err(PublishError::Conflicted));
}
