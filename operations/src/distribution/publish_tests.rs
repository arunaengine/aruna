//! Tests the publisher's request acceptance: atomic commit, admin checks, replays and conflicts.
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
use aruna_core::types::Value;

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
    let refused = begin(&mut operation, [None, None, Some(config), None]);
    assert_eq!(refused, Err(PublishError::NotPublisher));
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

/// Answers the first read of the operation with `values`.
fn begin(operation: &mut PublishOperation, values: [Option<Value>; 4]) -> PublishNext {
    assert_eq!(operation.transaction(), TransactionMode::Write);
    operation.first().unwrap();
    operation.next(IoResult::Values(values.into()))
}

fn signed_reply(next: PublishNext) -> IoResult {
    let Ok(Next::Call(IoCall::Effect(effect))) = next else {
        panic!("expected a signing request, got {next:?}");
    };
    let Effect::Net(NetEffect::SnapshotSign(claim)) = *effect else {
        panic!("expected a signing request, got {effect:?}");
    };
    let signed =
        SignedSnapshot::sign(claim.snapshot.clone(), claim.realm_id, &secret(PUBLISHER)).unwrap();
    let signed = SnapshotSignEvent::Signed(Box::new(signed));
    IoResult::Event(Box::new(Event::Net(NetEvent::SnapshotSign(signed))))
}

/// The rows of the admin check: the realm and the owner group's authorization documents. The
/// caller administers the group only when `allowed`.
fn authorized(allowed: bool) -> IoResult {
    use aruna_core::structs::identity::auth::Role;
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use std::collections::{HashMap, HashSet};
    let actor = Actor {
        node_id: secret(PUBLISHER).public(),
        user_id: auth().user_id,
        realm_id: realm(),
    };
    let group_id = root().owner_group;
    let admin = Role {
        role_id: Ulid::from_bytes([91; 16]),
        name: "admin".to_string(),
        permissions: HashMap::from([(format!("/{}/g/{group_id}/**", realm()), Permission::WRITE)]),
        assigned_users: allowed
            .then_some(auth().user_id)
            .into_iter()
            .collect::<HashSet<_>>(),
    };
    let group = GroupAuthorizationDocument {
        group_id,
        roles: HashMap::from([(admin.role_id, admin)]),
        policies: Vec::new(),
    };
    let realm_doc = RealmAuthorizationDocument::default_realm_doc(realm());
    let row = |bytes: Vec<u8>| Some(Value::from(bytes));
    IoResult::Values(vec![
        row(realm_doc.to_bytes(&actor).unwrap()),
        row(group.to_bytes(&actor).unwrap()),
    ])
}

#[test]
fn head_outbox_atomic() {
    let mut operation = operation(PUBLISHER, create());
    let check = begin(&mut operation, [None, None, Some(config_value()), None]);
    assert!(matches!(check, Ok(Next::Call(IoCall::Read(_)))));
    let sign = operation.next(authorized(true));
    let Ok(Next::Call(IoCall::Write(writes))) = operation.next(signed_reply(sign)) else {
        panic!("expected one transactional batch");
    };
    let spaces: Vec<&str> = writes.iter().map(|(space, ..)| space.as_str()).collect();
    assert_eq!(
        spaces,
        vec![
            DISTRIBUTION_KEYSPACE,
            DOCUMENT_STATE_KEYSPACE,
            DISTRIBUTION_ACCEPT_KEYSPACE,
            SYNC_OUTBOX_KEYSPACE,
            DISTRIBUTION_PUBLISHER_KEYSPACE,
            aruna_core::keyspaces::NODE_STATS_KEYSPACE
        ]
    );
    // The record and the publisher's replica share marker commit together.
    let share = aruna_core::structs::storage::usage::dirty_share_key(root().owner_group);
    assert_eq!(writes.last().unwrap().1.as_ref(), share);
    // Nothing is published before the commit; the drain follows it.
    let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
        panic!("expected the revision and the drain wake");
    };
    assert_eq!(output.number, 1);
    let [IoCall::Effect(wake)] = calls.as_slice() else {
        panic!("expected one wake, got {calls:?}");
    };
    assert!(matches!(**wake, Effect::Task(_)));
}

#[test]
fn admin_needs_write() {
    let mut operation = operation(PUBLISHER, create());
    begin(&mut operation, [None, None, Some(config_value()), None]).unwrap();
    assert_eq!(
        operation.next(authorized(false)),
        Err(PublishError::Unauthorized)
    );
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
    let replayed = begin(&mut replay, [None, Some(row), None, None]);
    assert_eq!(replayed, Ok(Next::Done(revision)));

    // The same id for another request or another requester is refused.
    let other = DistributionRequest::Activate;
    for row in [
        accepted(&other, Some(auth().user_id)),
        accepted(&create(), None),
    ] {
        let mut reused = operation(PUBLISHER, create());
        let refused = begin(&mut reused, [None, Some(row), None, None]);
        assert_eq!(refused, Err(PublishError::RequestReused));
    }
}

// An unavailable signer stops the publication before anything is written.
#[test]
fn unavailable_signer_stops() {
    let mut operation = operation(PUBLISHER, create());
    begin(&mut operation, [None, None, Some(config_value()), None]).unwrap();
    operation.next(authorized(true)).unwrap();
    let reason = "offline".to_string();
    let answer = SnapshotSignEvent::Unavailable(reason.clone());
    let answer = IoResult::Event(Box::new(Event::Net(NetEvent::SnapshotSign(answer))));
    assert_eq!(
        operation.next(answer),
        Err(PublishError::SignUnavailable(reason))
    );
}

#[test]
fn only_publisher_publishes() {
    let mut operation = operation(2, create());
    let refused = begin(&mut operation, [None, None, Some(config_value()), None]);
    assert_eq!(refused, Err(PublishError::NotPublisher));
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
    let dropped = begin(
        &mut operation,
        [Some(view_value(head)), None, Some(config_value()), None],
    );
    assert_eq!(dropped, Err(PublishError::Dropped));
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
    let stopped = begin(
        &mut operation,
        [Some(value), None, Some(config_value()), None],
    );
    assert_eq!(stopped, Err(PublishError::Conflicted));
}

/// The create revision with both members admitted and the set activated.
fn active() -> DistributionSnapshot {
    let mut head = next_snapshot(realm(), None, &create()).unwrap();
    for seed in [1, 2] {
        let member_id = Ulid::from_bytes([seed; 16]);
        let intent = head.member(member_id).unwrap().intent;
        let statement = Statement::admit(realm(), head.set_id(), member_id, 5, intent);
        let request = DistributionRequest::Statement(Box::new(statement.sign(&secret(seed))));
        head = next_snapshot(realm(), Some(&head), &request).unwrap();
    }
    next_snapshot(realm(), Some(&head), &DistributionRequest::Activate).unwrap()
}

#[test]
fn repeated_activation_answers() {
    let head = active();
    let mut operation = operation(PUBLISHER, DistributionRequest::Activate);
    let values = [
        Some(view_value(head.clone())),
        None,
        Some(config_value()),
        None,
    ];
    let check = begin(&mut operation, values);
    assert!(matches!(check, Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(
        operation.next(authorized(true)),
        Ok(Next::Done(head.revision()))
    );
    // Without group admin WRITE the caller learns nothing.
    let mut denied = self::operation(PUBLISHER, DistributionRequest::Activate);
    let values = [Some(view_value(head)), None, Some(config_value()), None];
    begin(&mut denied, values).unwrap();
    assert_eq!(
        denied.next(authorized(false)),
        Err(PublishError::Unauthorized)
    );
}
