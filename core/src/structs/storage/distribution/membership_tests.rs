// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::*;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::distribution::presets::replica_layout;
use crate::structs::storage::distribution::snapshot::Deletion;
use crate::structs::storage::distribution::{
    DataStrategy, DistributionConfig, DistributionRequest, DistributionSettings, MetadataCoverage,
    SetRoot, Statement, next_snapshot,
};

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn member_id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

fn step(head: &DistributionSnapshot, request: &DistributionRequest) -> DistributionSnapshot {
    next_snapshot(realm(), Some(head), request).unwrap()
}

fn admit(head: &DistributionSnapshot, seed: u8) -> DistributionSnapshot {
    let intent = head.member(member_id(seed)).unwrap().intent;
    let statement = Statement::admit(realm(), head.set_id(), member_id(seed), 7, intent);
    let request = DistributionRequest::Statement(Box::new(statement.sign(&secret(seed))));
    step(head, &request)
}

/// Member 1 coordinates the one range, member 2 replicates it; both admitted and active.
fn active() -> DistributionSnapshot {
    let root = SetRoot {
        publisher: secret(1).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    };
    let nodes = [
        (member_id(1), secret(1).public()),
        (member_id(2), secret(2).public()),
    ];
    let layout = replica_layout(root.set_id(), &nodes).unwrap();
    let create = DistributionRequest::Create {
        root: root.clone(),
        config: DistributionConfig {
            bucket: "spire".to_string(),
            owner_group: root.owner_group,
            node_set: None,
            members: layout.members,
            ranges: layout.ranges,
            settings: DistributionSettings::default(),
        },
    };
    let head = next_snapshot(realm(), None, &create).unwrap();
    let head = admit(&admit(&head, 1), 2);
    step(&head, &DistributionRequest::Activate)
}

fn newcomer(seed: u8, coverage: MetadataCoverage) -> DistributionMember {
    DistributionMember {
        member_id: member_id(seed),
        node_id: secret(seed).public(),
        incarnation: None,
        state: MemberState::Created,
        coverage,
        strategy: DataStrategy::Copy,
        range_behaviour: None,
        intent: [0; 32],
    }
}

fn join_request(seed: u8, ranges: Vec<Ulid>) -> DistributionRequest {
    DistributionRequest::Join {
        member: newcomer(seed, MetadataCoverage::Assigned),
        ranges,
    }
}

#[test]
fn joiner_catches_up() {
    // A joined replica starts created, admits itself late and then catches up.
    let head = active();
    let range_id = head.config.ranges[0].range_id;
    let joined = step(&head, &join_request(3, vec![range_id]));
    let member = joined.member(member_id(3)).unwrap();
    assert_eq!(member.state, MemberState::Created);
    assert_eq!(
        member.intent,
        join_intent(
            head.set_id(),
            head.digest(),
            &newcomer(3, MetadataCoverage::Assigned),
            &[range_id]
        )
    );
    let other = DistributionRequest::Join {
        member: newcomer(3, MetadataCoverage::All),
        ranges: vec![range_id],
    };
    let other = step(&head, &other);
    assert_ne!(
        other.member(member_id(3)).unwrap().intent,
        member.intent,
        "the intent binds the complete join request"
    );
    assert!(joined.config.ranges[0].replicas.contains(&member_id(3)));
    assert!(joined.serving(), "a created replica does not stop service");
    let admitted = admit(&joined, 3);
    assert_eq!(
        admitted.member(member_id(3)).unwrap().state,
        MemberState::CatchingUp
    );
    assert_eq!(admitted.catching_up().len(), 1);
}

#[test]
fn join_needs_newcomer() {
    let head = active();
    let mut member = newcomer(3, MetadataCoverage::All);
    member.incarnation = Some(1);
    let request = DistributionRequest::Join {
        member,
        ranges: Vec::new(),
    };
    assert_eq!(
        next_snapshot(realm(), Some(&head), &request),
        Err(DistributionError::InvalidCreate)
    );
    // An assigned member without ranges would hold nothing.
    assert_eq!(
        next_snapshot(realm(), Some(&head), &join_request(3, Vec::new())),
        Err(DistributionError::IdleMember(member_id(3)))
    );
}

#[test]
fn coordinator_cannot_leave() {
    let head = active();
    let leave = DistributionRequest::Leave {
        member_id: member_id(1),
    };
    assert_eq!(
        next_snapshot(realm(), Some(&head), &leave),
        Err(DistributionError::CoordinatorMember(member_id(1)))
    );
}

#[test]
fn leaving_member_rejoins() {
    // A leaving replica keeps no range; the same node joins again under a new member.
    let head = active();
    let leave = DistributionRequest::Leave {
        member_id: member_id(2),
    };
    let left = step(&head, &leave);
    assert_eq!(
        left.member(member_id(2)).unwrap().state,
        MemberState::Leaving
    );
    assert!(left.config.ranges[0].replicas.is_empty());
    assert!(left.lifecycle_open());
    assert!(
        left.range_deliveries(member_id(1), &left.config.ranges[0])
            .is_empty()
    );
    let mut again = newcomer(2, MetadataCoverage::All);
    again.member_id = member_id(4);
    let rejoin = DistributionRequest::Join {
        member: again,
        ranges: Vec::new(),
    };
    let rejoined = step(&left, &rejoin);
    assert!(rejoined.member(member_id(2)).is_none());
    assert!(rejoined.tombstoned(member_id(2), 7));
    assert_eq!(
        rejoined.member(member_id(4)).unwrap().state,
        MemberState::Created
    );
    assert!(
        rejoined.lifecycle_open(),
        "the node still releases the replaced member's bucket"
    );
    assert!(!active().lifecycle_open());
}

#[test]
fn deletion_stops_changes() {
    let mut head = active();
    head.lifecycle.deletion = Some(Deletion {
        members: vec![(member_id(1), 7), (member_id(2), 7)],
        fenced: Vec::new(),
        committed: false,
    });
    assert_eq!(
        next_snapshot(realm(), Some(&head), &join_request(3, Vec::new())),
        Err(DistributionError::Deleting)
    );
    let leave = DistributionRequest::Leave {
        member_id: member_id(2),
    };
    assert_eq!(
        next_snapshot(realm(), Some(&head), &leave),
        Err(DistributionError::Deleting)
    );
}

#[test]
fn epoch_covers_ranges() {
    let mut head = active();
    assert_eq!(head.max_epoch(), 1);
    head.config.ranges[0].epoch = 4;
    assert_eq!(head.max_epoch(), 4);
}
