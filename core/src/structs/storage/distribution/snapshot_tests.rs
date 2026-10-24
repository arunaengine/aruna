// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::storage::distribution::presets::{replica_layout, shard_layout};
use crate::structs::storage::distribution::{DistributionSettings, Statement};

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

fn created() -> DistributionSnapshot {
    let nodes = [
        (member_id(1), secret(1).public()),
        (member_id(2), secret(2).public()),
    ];
    let layout = replica_layout(root().set_id(), &nodes).unwrap();
    DistributionSnapshot {
        root: root(),
        number: 1,
        previous: None,
        phase: SetPhase::Pending,
        config: DistributionConfig {
            bucket: "spire".to_string(),
            owner_group: root().owner_group,
            node_set: None,
            members: layout.members,
            ranges: layout.ranges,
            settings: DistributionSettings::default(),
        },
        lifecycle: Lifecycle::default(),
        statements: Vec::new(),
        tombstones: Vec::new(),
        admin_number: 1,
    }
}

fn admission(member: u8, incarnation: u128, signer: u8) -> SignedStatement {
    Statement {
        realm_id: realm(),
        set_id: root().set_id(),
        statement_id: Ulid::from_bytes([member + 40; 16]),
        member_id: member_id(member),
        incarnation,
        intent: 1,
        kind: StatementKind::Admit,
    }
    .sign(&secret(signer))
}

/// The successor of `snapshot` with `change` applied, as the publisher would number it.
fn successor(
    snapshot: &DistributionSnapshot,
    change: impl FnOnce(&mut DistributionSnapshot),
) -> DistributionSnapshot {
    let mut next = snapshot.clone();
    next.number += 1;
    next.previous = Some(snapshot.digest());
    change(&mut next);
    next
}

fn signed(snapshot: DistributionSnapshot) -> SignedSnapshot {
    SignedSnapshot::sign(snapshot, realm(), &secret(1)).unwrap()
}

#[test]
fn publisher_signature_binds() {
    let snapshot = signed(created());
    assert_eq!(snapshot.verify(realm()), Ok(()));
    assert!(snapshot.verify(RealmId::from_bytes([8; 32])).is_err());
    let foreign = SignedSnapshot::sign(created(), realm(), &secret(3)).unwrap();
    assert!(foreign.verify(realm()).is_err());
    let mut tampered = snapshot.clone();
    tampered.snapshot.config.settings.public = true;
    assert!(tampered.verify(realm()).is_err());
    let back = SignedSnapshot::from_bytes(&snapshot.to_bytes().unwrap()).unwrap();
    assert_eq!(back, snapshot);
}

#[test]
fn statements_checked_inside() {
    let admitted = |statement: SignedStatement, incarnation: Option<u128>| {
        signed(successor(&created(), |next| {
            next.config.members[1].incarnation = incarnation;
            next.statements.push(statement);
        }))
        .verify(realm())
    };
    assert_eq!(admitted(admission(2, 5, 2), Some(5)), Ok(()));
    // Another node cannot admit a member, even when the publisher embeds it.
    assert_eq!(
        admitted(admission(2, 5, 3), Some(5)),
        Err(DistributionError::AdmissionOrigin(member_id(2)))
    );
    assert!(admitted(admission(2, 6, 2), Some(5)).is_err());
    let mut forged = admission(2, 5, 2);
    forged.statement.incarnation = 7;
    assert!(admitted(forged, Some(7)).is_err());
    let mut later = admission(2, 5, 2);
    later.statement.intent = 2;
    let resigned = later.statement.clone().sign(&secret(2));
    assert!(admitted(resigned, Some(5)).is_err());
    let twice = signed(successor(&created(), |next| {
        next.config.members[1].incarnation = Some(5);
        next.statements = vec![admission(2, 5, 2), admission(2, 5, 2)];
    }));
    assert!(matches!(
        twice.verify(realm()),
        Err(DistributionError::Replayed(_))
    ));
}

#[test]
fn snapshot_size_is_bounded() {
    let boundaries: Vec<String> = (1..crate::structs::storage::distribution::MAX_RANGES)
        .map(|index| format!("{index:05}{}", "k".repeat(500)))
        .collect();
    let nodes = [
        (member_id(1), secret(1).public()),
        (member_id(2), secret(2).public()),
    ];
    let layout = shard_layout(root().set_id(), &nodes, &boundaries, 0).unwrap();
    let mut snapshot = created();
    snapshot.config.members = layout.members;
    snapshot.config.ranges = layout.ranges;
    assert_eq!(
        signed(snapshot).verify(realm()),
        Err(DistributionError::TooLarge)
    );
}

#[test]
fn install_keeps_highest() {
    let first = signed(created());
    let second = signed(successor(&created(), |next| {
        next.config.settings.public = true;
    }));
    let Installed::Changed(view) = install(None, first.clone()).unwrap() else {
        panic!("first snapshot installs");
    };
    let Installed::Changed(view) = install(Some(&view), second.clone()).unwrap() else {
        panic!("a higher snapshot replaces the head");
    };
    assert_eq!(view.head, second);
    assert_eq!(install(Some(&view), first), Ok(Installed::Unchanged));
    assert_eq!(install(Some(&view), second), Ok(Installed::Unchanged));
    // Skipped history is not replayed: a far newer snapshot installs directly.
    let far = signed(successor(&successor(&view.head.snapshot, |_| {}), |_| {}));
    assert!(matches!(
        install(Some(&view), far),
        Ok(Installed::Changed(_))
    ));
}

#[test]
fn equivocation_marks_conflict() {
    let first = signed(created());
    let Installed::Changed(view) = install(None, first.clone()).unwrap() else {
        panic!("first snapshot installs");
    };
    let public = signed(successor(&created(), |next| {
        next.config.settings.public = true
    }));
    let private = signed(successor(&created(), |next| {
        next.config.settings.search_index = true
    }));
    let Installed::Changed(view) = install(Some(&view), public.clone()).unwrap() else {
        panic!("successor installs");
    };
    let Installed::Changed(conflicted) = install(Some(&view), private.clone()).unwrap() else {
        panic!("equivocation is recorded");
    };
    assert!(conflicted.conflicted());
    assert_eq!(conflicted.head, public);
    let next = signed(successor(&public.snapshot, |_| {}));
    assert_eq!(install(Some(&conflicted), next), Ok(Installed::Unchanged));
    // A successor naming another predecessor is a fork as well.
    let Installed::Changed(view) = install(None, first).unwrap() else {
        panic!("first snapshot installs");
    };
    let mut fork = successor(&created(), |_| {});
    fork.previous = Some([1; 32]);
    let Installed::Changed(forked) = install(Some(&view), signed(fork)).unwrap() else {
        panic!("fork is recorded");
    };
    assert!(forked.conflicted());
    let mut other = created();
    other.root.nonce = [8; 16];
    assert!(install(Some(&view), signed(other)).is_err());
}
