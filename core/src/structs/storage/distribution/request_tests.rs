// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::storage::distribution::Statement;
use crate::structs::storage::distribution::presets::replica_layout;

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn root() -> SetRoot {
    SetRoot {
        publisher: secret(1).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    }
}

fn member_id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

fn config() -> DistributionConfig {
    let nodes = [
        (member_id(1), secret(1).public()),
        (member_id(2), secret(2).public()),
        (member_id(3), secret(3).public()),
    ];
    let layout = replica_layout(root().set_id(), &nodes).unwrap();
    DistributionConfig {
        bucket: "spire".to_string(),
        owner_group: root().owner_group,
        node_set: None,
        members: layout.members,
        ranges: layout.ranges,
        settings: DistributionSettings::default(),
    }
}

fn created() -> DistributionSnapshot {
    let create = DistributionRequest::Create {
        root: root(),
        config: config(),
    };
    next_snapshot(realm(), None, &create).unwrap()
}

fn statement(member: u8, statement: u8, intent: [u8; 32], kind: StatementKind) -> Statement {
    Statement {
        realm_id: realm(),
        set_id: root().set_id(),
        statement_id: Ulid::from_bytes([statement; 16]),
        member_id: member_id(member),
        incarnation: u128::from(member),
        intent,
        kind,
    }
}

fn admit(member: u8, signer: u8) -> DistributionRequest {
    let intent = created().config.members[0].intent;
    let signed = statement(member, member + 40, intent, StatementKind::Admit).sign(&secret(signer));
    DistributionRequest::Statement(Box::new(signed))
}

fn step(head: &DistributionSnapshot, request: &DistributionRequest) -> DistributionSnapshot {
    next_snapshot(realm(), Some(head), request).unwrap()
}

#[test]
fn create_binds_root() {
    let snapshot = created();
    assert_eq!((snapshot.number, snapshot.previous), (1, None));
    assert_eq!(snapshot.set_id(), root().set_id());
    let mut foreign = config();
    foreign.owner_group = Ulid::from_bytes([91; 16]);
    let create = DistributionRequest::Create {
        root: root(),
        config: foreign,
    };
    assert!(matches!(
        next_snapshot(realm(), None, &create),
        Err(DistributionError::ForeignSet(_))
    ));
    let again = DistributionRequest::Create {
        root: root(),
        config: config(),
    };
    assert!(next_snapshot(realm(), Some(&snapshot), &again).is_err());
    assert_eq!(
        next_snapshot(realm(), None, &DistributionRequest::Activate),
        Err(DistributionError::UnknownSet)
    );
}

#[test]
fn concurrent_admissions_apply() {
    // Admissions signed against the same create revision do not invalidate each other.
    let head = step(&created(), &admit(1, 1));
    let (first, second) = (admit(2, 2), admit(3, 3));
    let ordered = step(&step(&head, &first), &second);
    let reversed = step(&step(&head, &second), &first);
    for admitted in [&ordered, &reversed] {
        assert_eq!((admitted.number, admitted.admin_number), (4, 1));
        assert!(
            admitted
                .config
                .members
                .iter()
                .all(|m| m.incarnation.is_some())
        );
    }
    let active = step(&ordered, &DistributionRequest::Activate);
    assert!(active.serving());
    assert_eq!(active.admin_number, active.number);
    assert_eq!(active.previous, Some(ordered.digest()));
}

#[test]
fn statements_are_checked() {
    let head = created();
    assert_eq!(
        next_snapshot(realm(), Some(&head), &admit(2, 3)),
        Err(DistributionError::AdmissionOrigin(member_id(2)))
    );
    let admitted = step(&head, &admit(2, 2));
    assert_eq!(
        next_snapshot(realm(), Some(&admitted), &admit(2, 2)),
        Err(DistributionError::Replayed(member_id(42)))
    );
    let wrong_intent = statement(3, 43, [2; 32], StatementKind::Admit).sign(&secret(3));
    let request = DistributionRequest::Statement(Box::new(wrong_intent));
    assert!(next_snapshot(realm(), Some(&admitted), &request).is_err());
}

#[test]
fn activation_needs_admissions() {
    let head = step(&created(), &admit(1, 1));
    assert_eq!(
        next_snapshot(realm(), Some(&head), &DistributionRequest::Activate),
        Err(DistributionError::MissingAdmission(member_id(2)))
    );
}

#[test]
fn stale_automatic_dropped() {
    let settings = DistributionRequest::ChangeSettings {
        settings: DistributionSettings {
            public: true,
            ..DistributionSettings::default()
        },
    };
    let head = step(&created(), &settings);
    assert_eq!(head.admin_number, 2);
    let split = |intent| {
        let kind = StatementKind::Split {
            basis: intent,
            range_id: head.config.ranges[0].range_id,
            epoch: 1,
            split_key: "m".to_string(),
            lower_id: Ulid::from_bytes([60; 16]),
            upper_id: Ulid::from_bytes([61; 16]),
        };
        let signed = statement(1, 50, [3; 32], kind).sign(&secret(1));
        DistributionRequest::Statement(Box::new(signed))
    };
    assert_eq!(
        next_snapshot(realm(), Some(&head), &split(1)),
        Err(DistributionError::StaleRevision)
    );
    assert_eq!(
        next_snapshot(realm(), Some(&head), &split(2)),
        Err(DistributionError::Unsupported("split"))
    );
    assert_eq!(
        next_snapshot(realm(), Some(&head), &DistributionRequest::DeleteCommit),
        Err(DistributionError::Unsupported("delete commit"))
    );
}

#[test]
fn admission_binds_create() {
    // Two creates of one set id are a fork; an admission for one never fits the other.
    let mut other = config();
    other.settings.public = true;
    let fork = next_snapshot(
        realm(),
        None,
        &DistributionRequest::Create {
            root: root(),
            config: other,
        },
    )
    .unwrap();
    assert_eq!(fork.number, created().number);
    assert_ne!(
        fork.config.members[0].intent,
        created().config.members[0].intent
    );
    assert!(next_snapshot(realm(), Some(&fork), &admit(2, 2)).is_err());
    assert!(next_snapshot(realm(), Some(&created()), &admit(2, 2)).is_ok());
}

/// Member 1 coordinates the only range; stream edge member 2 is admitted after activation.
fn late_edge() -> DistributionSnapshot {
    use crate::structs::storage::distribution::DataStrategy;
    use crate::structs::storage::distribution::presets::add_edges;
    let nodes = [(member_id(1), secret(1).public())];
    let mut layout = replica_layout(root().set_id(), &nodes).unwrap();
    let edge = [(member_id(2), secret(2).public())];
    add_edges(&mut layout, &edge, DataStrategy::Stream).unwrap();
    let mut config = config();
    config.members = layout.members;
    config.ranges = layout.ranges;
    let create = DistributionRequest::Create {
        root: root(),
        config,
    };
    let head = next_snapshot(realm(), None, &create).unwrap();
    let intent = head.config.members[0].intent;
    let admit = |member: u8| {
        let signed = statement(member, member + 40, intent, StatementKind::Admit);
        DistributionRequest::Statement(Box::new(signed.sign(&secret(member))))
    };
    let head = step(&head, &admit(1));
    let head = step(&head, &DistributionRequest::Activate);
    step(&head, &admit(2))
}

#[test]
fn late_edge_admission() {
    use crate::structs::storage::distribution::MemberState;
    let head = late_edge();
    assert_eq!(head.member(member_id(1)).unwrap().state, MemberState::Ready);
    assert_eq!(
        head.member(member_id(2)).unwrap().state,
        MemberState::CatchingUp
    );
    assert!(head.serving());
}

#[test]
fn caught_up_readies() {
    use crate::structs::storage::distribution::MemberState;
    let head = late_edge();
    let (coordinator, edge) = (
        head.member(member_id(1)).unwrap(),
        head.member(member_id(2)).unwrap(),
    );
    let statement = Statement::caught_up(realm(), head.set_id(), coordinator, edge, 7);
    let request = DistributionRequest::Statement(Box::new(statement.clone().sign(&secret(1))));
    let forged = Statement::caught_up(realm(), head.set_id(), edge, edge, 7);
    let forged = DistributionRequest::Statement(Box::new(forged.sign(&secret(2))));
    assert!(
        next_snapshot(realm(), Some(&head), &forged).is_err(),
        "only a coordinator of a held range states it"
    );
    let ready = step(&head, &request);
    assert_eq!(
        ready.member(member_id(2)).unwrap().state,
        MemberState::Ready
    );
    assert!(
        ready
            .statements
            .iter()
            .all(|signed| signed.statement.statement_id != statement.statement_id),
        "the statement leaves the record with the catch-up"
    );
    assert!(next_snapshot(realm(), Some(&ready), &request).is_err());
}
