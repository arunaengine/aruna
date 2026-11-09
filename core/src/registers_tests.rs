//! Tests owner secret selection, tombstones, incarnations and writer eligibility.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::NodeId;
use crate::admin_documents::AdminDocumentDot;
use crate::structs::identity::realm::RealmId;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn owner() -> SecretOwner {
    SecretOwner::Group(Ulid::from_bytes([7; 16]))
}

fn name() -> SecretName {
    SecretName::try_from("catalog-key".to_string()).unwrap()
}

fn write(counter: u64, seed: u8, id: u8, incarnation: u8, body: SecretBody) -> SecretDocument {
    let realm = RealmId::from_bytes([1; 32]);
    SecretDocument {
        owner: owner(),
        name: name(),
        stamp: RegisterStamp {
            counter,
            node_id: node(seed),
            write_id: Ulid::from_bytes([id; 16]),
        },
        incarnation: Ulid::from_bytes([incarnation; 16]),
        config_clock: AdminDocumentClock::default(),
        author: UserId::local(Ulid::from_bytes([2; 16]), realm),
        written_at_ms: counter,
        body,
    }
}

fn create(counter: u64, seed: u8, id: u8) -> SecretDocument {
    let kind = SecretKind::parse("bearer").unwrap();
    let created = SecretLifecycle::Created {
        kind,
        readable: false,
    };
    write(counter, seed, id, id, SecretBody::Lifecycle(created))
}

fn delete(counter: u64, seed: u8, id: u8, incarnation: u8) -> SecretDocument {
    let body = SecretBody::Lifecycle(SecretLifecycle::Deleted);
    write(counter, seed, id, incarnation, body)
}

fn info(counter: u64, seed: u8, id: u8, incarnation: u8, text: &str) -> SecretDocument {
    let body = SecretBody::Info(SecretInfo {
        description: text.to_string(),
        settings: SecretSettings::new(),
        uses: Vec::new(),
    });
    write(counter, seed, id, incarnation, body)
}

fn value(counter: u64, seed: u8, id: u8, incarnation: u8) -> SecretDocument {
    let body = SecretBody::Value(SecretValue {
        fields: vec!["token".to_string()],
        sealed: vec![id; 48],
    });
    write(counter, seed, id, incarnation, body)
}

fn select(documents: &[SecretDocument]) -> Option<SecretEntry> {
    current_entry(documents, |_| true)
}

fn ids(entry: &SecretEntry) -> (u8, Option<u8>, Option<u8>) {
    let first = |document: &SecretDocument| document.stamp.write_id.to_bytes()[0];
    let part = |document: &Option<SecretDocument>| document.as_ref().map(first);
    (
        first(&entry.lifecycle),
        part(&entry.info),
        part(&entry.value),
    )
}

#[test]
fn converges_any_order() {
    let mut documents = vec![
        create(1, 1, 10),
        info(1, 1, 11, 10, "a"),
        value(1, 1, 12, 10),
        info(2, 2, 13, 10, "b"),
        value(2, 1, 14, 10),
    ];
    let expected = select(&documents).unwrap();
    assert_eq!(ids(&expected), (10, Some(13), Some(14)));
    for _ in 0..documents.len() {
        documents.rotate_left(1);
        assert_eq!(select(&documents), Some(expected.clone()));
        documents.reverse();
        assert_eq!(select(&documents), Some(expected.clone()));
    }
}

#[test]
fn ties_by_node() {
    let (low, high) = match node(1) < node(2) {
        true => (1, 2),
        false => (2, 1),
    };
    let documents = [
        create(1, 1, 10),
        info(2, low, 20, 10, "low"),
        info(2, high, 21, 10, "high"),
        value(1, 1, 12, 10),
    ];
    let entry = select(&documents).unwrap();
    assert_eq!(ids(&entry).1, Some(21));
}

#[test]
fn tombstone_by_stamp() {
    let created = [
        create(1, 1, 10),
        info(1, 1, 11, 10, "a"),
        value(1, 1, 12, 10),
    ];
    let mut documents = created.to_vec();
    documents.push(delete(2, 1, 13, 10));
    assert_eq!(select(&documents), None);
    // An older create loses to the delete; a newer one starts a fresh incarnation.
    documents.push(create(3, 2, 20));
    documents.push(value(2, 2, 21, 10));
    let entry = select(&documents).unwrap();
    assert_eq!(ids(&entry), (20, None, None));
    assert!(!entry.complete());
}

#[test]
fn stale_incarnation_ignored() {
    // A deletes X at 2 and creates Y at 3; partitioned B edits X up to counter 4.
    let documents = [
        create(1, 1, 10),
        info(1, 1, 11, 10, "x"),
        value(1, 1, 12, 10),
        delete(2, 1, 13, 10),
        create(3, 1, 20),
        info(1, 1, 21, 20, "y"),
        value(1, 1, 22, 20),
        info(4, 2, 30, 10, "stale"),
        value(4, 2, 31, 10),
    ];
    let entry = select(&documents).unwrap();
    assert_eq!(ids(&entry), (20, Some(21), Some(22)));
}

#[test]
fn incomplete_until_value() {
    let partial = [create(1, 1, 10), info(1, 1, 11, 10, "a")];
    let entry = select(&partial).unwrap();
    assert!(!entry.complete());
    assert!(entry.revision().ends_with(".-"));
    let full = [
        create(1, 1, 10),
        info(1, 1, 11, 10, "a"),
        value(1, 1, 12, 10),
    ];
    let entry = select(&full).unwrap();
    assert!(entry.complete());
    assert_ne!(entry.revision(), select(&partial).unwrap().revision());
}

#[test]
fn structure_checked() {
    let mut forged = info(1, 1, 11, 11, "a");
    assert!(forged.check().is_err(), "only a create names itself");
    forged.incarnation = Ulid::from_bytes([10; 16]);
    assert!(forged.check().is_ok());
    let mut zero = create(1, 1, 10);
    zero.stamp.counter = 0;
    assert!(zero.check().is_err());
    let mut readable = create(1, 1, 10);
    readable.body = SecretBody::Lifecycle(SecretLifecycle::Created {
        kind: SecretKind::parse("bearer").unwrap(),
        readable: true,
    });
    assert!(readable.check().is_err(), "group secrets never read back");
    let user = SecretOwner::User(readable.author);
    assert_ne!(
        user.to_bytes(),
        SecretOwner::Group(readable.author.user_ulid).to_bytes()
    );
    let bytes = readable.to_bytes().unwrap();
    assert_eq!(SecretDocument::from_bytes(&bytes).unwrap(), readable);
}

fn dot(origin: u8, seq: u64) -> AdminDocumentDot {
    AdminDocumentDot {
        event_id: Ulid::from_bytes([origin; 16]),
        origin_node_id: node(origin),
        origin_seq: seq,
    }
}

fn clock(seq: u64) -> AdminDocumentClock {
    AdminDocumentClock::default().with_observed(node(9), seq)
}

#[test]
fn writers_by_history() {
    // Node 2 was added at seq 2, removed at seq 5 and added again at seq 8.
    let intervals = [
        NodeInterval {
            node_id: node(2),
            added: dot(9, 2),
            removed: vec![dot(9, 5)],
        },
        NodeInterval {
            node_id: node(2),
            added: dot(9, 8),
            removed: Vec::new(),
        },
    ];
    let cited = |seq| {
        let mut document = value(1, 2, 12, 10);
        document.config_clock = clock(seq);
        document
    };
    let local = clock(9);
    let decide = |seq| writer_eligibility(&cited(seq), &intervals, &local);
    assert_eq!(
        decide(1),
        Eligibility::Ignored,
        "a clock before the addition"
    );
    assert_eq!(decide(3), Eligibility::Counts);
    assert_eq!(decide(6), Eligibility::Ignored, "a clock in the gap");
    assert_eq!(decide(8), Eligibility::Counts);
    assert_eq!(
        decide(10),
        Eligibility::Deferred,
        "ahead of the local config"
    );
    // Concurrent clocks stay deferred until the local config covers the cited one.
    let mut concurrent = cited(3);
    concurrent.config_clock = concurrent.config_clock.with_observed(node(4), 1);
    assert_eq!(
        writer_eligibility(&concurrent, &intervals, &local),
        Eligibility::Deferred
    );
    let caught_up = local.clone().with_observed(node(4), 1);
    assert_eq!(
        writer_eligibility(&concurrent, &intervals, &caught_up),
        Eligibility::Counts
    );
}

#[test]
fn removal_order_converges() {
    // A rotation by node 2 cites a clock where it is a member; whether its removal arrives
    // before or after, every node ends with the same selection.
    let member = NodeInterval {
        node_id: node(2),
        added: dot(9, 2),
        removed: Vec::new(),
    };
    let removed = NodeInterval {
        removed: vec![dot(9, 5)],
        ..member.clone()
    };
    let mut rotation = value(5, 2, 30, 10);
    rotation.config_clock = clock(3);
    let documents = [
        create(1, 1, 10),
        info(1, 1, 11, 10, "a"),
        value(1, 1, 12, 10),
        rotation,
    ];
    let pick = |intervals: &[NodeInterval], local: &AdminDocumentClock| {
        let counts = |document: &SecretDocument| {
            document.stamp.node_id == node(1)
                || writer_eligibility(document, intervals, local) == Eligibility::Counts
        };
        current_entry(&documents, counts).map(|entry| ids(&entry))
    };
    let early = pick(std::slice::from_ref(&member), &clock(3));
    let late = pick(std::slice::from_ref(&removed), &clock(6));
    assert_eq!(early, late);
    assert_eq!(early, Some((10, Some(11), Some(30))));
    // A rotation by another member after the original writer's removal counts.
    let mut later = value(6, 1, 40, 10);
    later.config_clock = clock(6);
    let mut with_later = documents.to_vec();
    with_later.push(later);
    let counts = |document: &SecretDocument| {
        document.stamp.node_id == node(1)
            || writer_eligibility(document, std::slice::from_ref(&removed), &clock(6))
                == Eligibility::Counts
    };
    let entry = current_entry(&with_later, counts).unwrap();
    assert_eq!(ids(&entry).2, Some(40));
}
