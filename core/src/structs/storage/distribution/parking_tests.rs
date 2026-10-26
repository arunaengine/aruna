//! Tests parked snapshots of unknown publishers: the highest and forked ones and retry authority.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::admin_documents::{AdminDocumentClock, AdminDocumentTarget};
use crate::structs::identity::auth::Actor;
use crate::structs::identity::realm::RealmNodeKind;
use crate::structs::storage::distribution::presets::replica_layout;
use crate::structs::storage::distribution::{
    DistributionConfig, DistributionRequest, DistributionRevision, DistributionSettings,
    DistributionSnapshot, SetRoot, next_snapshot,
};
use crate::user::id::UserId;
use ulid::Ulid;

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

fn created() -> DistributionSnapshot {
    let nodes = [
        (Ulid::from_bytes([1; 16]), secret(1).public()),
        (Ulid::from_bytes([2; 16]), secret(2).public()),
    ];
    let layout = replica_layout(root().set_id(), &nodes).unwrap();
    let create = DistributionRequest::Create {
        root: root(),
        config: DistributionConfig {
            bucket: "spire".to_string(),
            owner_group: root().owner_group,
            node_set: None,
            members: layout.members,
            ranges: layout.ranges,
            settings: DistributionSettings::default(),
        },
    };
    next_snapshot(realm(), None, &create).unwrap()
}

fn settings(head: &DistributionSnapshot, public: bool, search: bool) -> DistributionSnapshot {
    let request = DistributionRequest::ChangeSettings {
        settings: DistributionSettings {
            public,
            search_index: search,
            ..DistributionSettings::default()
        },
    };
    next_snapshot(realm(), Some(head), &request).unwrap()
}

fn event(snapshot: DistributionSnapshot) -> AdminDocumentEvent {
    let signed = SignedSnapshot::sign(snapshot, realm(), &secret(1)).unwrap();
    AdminDocumentEvent {
        event_id: Ulid::from_bytes([3; 16]),
        target: AdminDocumentTarget::Distribution {
            realm_id: realm(),
            set_id: root().set_id(),
        },
        origin_node_id: secret(1).public(),
        origin_seq: 1,
        observed: AdminDocumentClock::default(),
        actor: Actor {
            node_id: secret(1).public(),
            user_id: UserId::nil(realm()),
            realm_id: realm(),
        },
        op: AdminDocumentOperation::DistributionPublished {
            snapshot: Box::new(signed),
        },
    }
}

fn stored(decision: ParkDecision) -> ParkedEntry {
    match decision {
        ParkDecision::Store(entry) => *entry,
        other => panic!("expected a stored entry, got {other:?}"),
    }
}

#[test]
fn keeps_highest_forks() {
    let first = created();
    let public = settings(&first, true, false);
    let search = settings(&first, false, true);
    let entry = stored(park(None, 0, event(first.clone())).unwrap());
    let entry = stored(park(Some(&entry), 1, event(public.clone())).unwrap());
    assert_eq!(entry.number(), 2);
    assert_eq!(
        park(Some(&entry), 1, event(first)),
        Ok(ParkDecision::Unchanged)
    );
    // A second snapshot of one number is kept as evidence, not dropped by arrival order.
    let forked = stored(park(Some(&entry), 1, event(search.clone())).unwrap());
    assert_eq!(forked.head, event(public.clone()));
    assert_eq!(forked.conflict, Some(event(search)));
    let later = settings(&public, false, false);
    assert_eq!(
        park(Some(&forked), 1, event(later)),
        Ok(ParkDecision::Unchanged)
    );
    assert_eq!(
        park(None, MAX_PARKED, event(created())),
        Ok(ParkDecision::Full)
    );
}

#[test]
fn retry_follows_authority() {
    let first = created();
    let fork = settings(&first, true, false);
    let other = settings(&first, false, true);
    let entry = ParkedEntry {
        head: event(fork.clone()),
        conflict: Some(event(other.clone())),
    };
    assert_eq!(retry(&entry, |_| Authority::Unknown), RetryDecision::Wait);
    assert_eq!(
        retry(&entry, |_| Authority::Current),
        RetryDecision::Install(vec![event(fork.clone()), event(other)])
    );
    assert!(matches!(
        retry(&entry, |_| Authority::Ended("gone")),
        RetryDecision::Drop("gone")
    ));
}

#[test]
fn authority_from_config() {
    let signed = SignedSnapshot::sign(created(), realm(), &secret(1)).unwrap();
    assert_eq!(
        publisher_authority(realm(), None, None, &signed),
        Authority::Unknown
    );
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    assert_eq!(
        publisher_authority(realm(), Some(&config), None, &signed),
        Authority::Unknown
    );
    let cutoff = PublisherCutoff {
        set_id: root().set_id(),
        revision: signed.snapshot.revision(),
    };
    assert_eq!(
        publisher_authority(realm(), Some(&config), Some(&[cutoff]), &signed),
        Authority::Current
    );
    let later = PublisherCutoff {
        revision: DistributionRevision {
            number: 2,
            digest: [1; 32],
        },
        ..cutoff
    };
    assert!(matches!(
        publisher_authority(realm(), Some(&config), Some(&[later]), &signed),
        Authority::Ended(_)
    ));
    config.ensure_node(secret(1).public(), RealmNodeKind::Server);
    assert_eq!(
        publisher_authority(realm(), Some(&config), None, &signed),
        Authority::Current
    );
}
