//! Tests that a distribution target keeps the highest valid snapshot of its bound publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::admin_documents::AdminDocumentTarget;
use crate::structs::storage::distribution::presets::replica_layout;
use crate::structs::storage::distribution::{
    DistributionConfig, DistributionError, DistributionRequest, DistributionSettings,
    DistributionSnapshot, SetRoot, SignedSnapshot, next_snapshot,
};

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn root() -> SetRoot {
    SetRoot {
        publisher: secret(1).public(),
        owner_group: group_id(),
        nonce: [4; 16],
    }
}

fn target() -> AdminDocumentTarget {
    AdminDocumentTarget::Distribution {
        realm_id: realm_id(),
        set_id: root().set_id(),
    }
}

fn created() -> DistributionSnapshot {
    let nodes = [
        (Ulid::from_bytes([1; 16]), node(1)),
        (Ulid::from_bytes([2; 16]), node(2)),
    ];
    let layout = replica_layout(root().set_id(), &nodes).unwrap();
    let config = DistributionConfig {
        bucket: "spire".to_string(),
        owner_group: group_id(),
        node_set: None,
        members: layout.members,
        ranges: layout.ranges,
        settings: DistributionSettings::default(),
    };
    let create = DistributionRequest::Create {
        root: root(),
        config,
    };
    next_snapshot(realm_id(), None, &create).unwrap()
}

fn settings(head: &DistributionSnapshot, public: bool) -> DistributionSnapshot {
    let request = DistributionRequest::ChangeSettings {
        settings: DistributionSettings {
            public,
            ..DistributionSettings::default()
        },
    };
    next_snapshot(realm_id(), Some(head), &request).unwrap()
}

fn published(
    event_seed: u8,
    origin_seed: u8,
    snapshot: DistributionSnapshot,
    signer: u8,
) -> AdminDocumentEvent {
    let signed = SignedSnapshot::sign(snapshot, realm_id(), &secret(signer)).unwrap();
    AdminDocumentEvent {
        event_id: Ulid::from_bytes([event_seed; 16]),
        target: target(),
        origin_node_id: node(origin_seed),
        origin_seq: 1,
        observed: AdminDocumentClock::default(),
        actor: actor(node(origin_seed)),
        op: AdminDocumentOperation::DistributionPublished {
            snapshot: Box::new(signed),
        },
    }
}

fn head_number(state: &AdminDocumentState) -> Option<u64> {
    state.distribution_view().map(|view| view.snapshot().number)
}

#[test]
fn keeps_highest_snapshot() {
    let mut state = AdminDocumentState::new(target());
    let first = created();
    let second = settings(&first, true);
    let third = settings(&second, false);
    assert_eq!(
        state.apply(&published(1, 1, third.clone(), 1)),
        Ok(AdminApplyStatus::Applied)
    );
    // Skipped history is not replayed and older revisions are ignored.
    assert_eq!(
        state.apply(&published(2, 1, second, 1)),
        Ok(AdminApplyStatus::Duplicate)
    );
    assert_eq!(
        state.apply(&published(3, 1, third, 1)),
        Ok(AdminApplyStatus::Duplicate)
    );
    assert_eq!(head_number(&state), Some(3));
    assert!(state.applied_event_ids.is_empty());
    assert_eq!(state.clock, AdminDocumentClock::default());
}

#[test]
fn reused_event_ids_apply() {
    // Event ids carry no authority: a foreign event cannot shadow the publisher's next one.
    let mut state = AdminDocumentState::new(target());
    let first = created();
    assert!(state.apply(&published(7, 3, first.clone(), 3)).is_err());
    state.apply(&published(7, 1, first.clone(), 1)).unwrap();
    state
        .apply(&published(7, 1, settings(&first, true), 1))
        .unwrap();
    assert_eq!(head_number(&state), Some(2));
}

#[test]
fn foreign_origins_refused() {
    let mut state = AdminDocumentState::new(target());
    assert_eq!(
        state.apply(&published(1, 2, created(), 1)),
        Err(DistributionError::NotPublisher.into())
    );
    assert!(state.apply(&published(1, 1, created(), 2)).is_err());
    let mut other = created();
    other.root.nonce = [5; 16];
    assert!(state.apply(&published(1, 1, other, 1)).is_err());
    assert_eq!(state.distribution_view(), None);
}

#[test]
fn equivocation_stops_changes() {
    let mut state = AdminDocumentState::new(target());
    let first = created();
    state.apply(&published(1, 1, first.clone(), 1)).unwrap();
    state
        .apply(&published(2, 1, settings(&first, true), 1))
        .unwrap();
    let mut fork = settings(&first, true);
    fork.config.settings.search_index = true;
    state.apply(&published(3, 1, fork, 1)).unwrap();
    let view = state.distribution_view().unwrap();
    assert!(view.conflicted());
    let later = settings(&view.head.snapshot, false);
    assert_eq!(
        state.apply(&published(4, 1, later, 1)),
        Ok(AdminApplyStatus::Duplicate)
    );
    assert_eq!(head_number(&state), Some(2));
}

#[test]
fn fences_survive_installs() {
    // Installing a snapshot never clears a fence a newer installed snapshot holds.
    use crate::structs::storage::distribution::snapshot::Deletion;
    let mut state = AdminDocumentState::new(target());
    let first = created();
    let mut fenced = settings(&first, true);
    fenced.lifecycle.deletion = Some(Deletion {
        members: vec![(Ulid::from_bytes([1; 16]), 1)],
        fenced: vec![Ulid::from_bytes([1; 16])],
        committed: false,
    });
    state.apply(&published(1, 1, fenced.clone(), 1)).unwrap();
    assert_eq!(
        state.apply(&published(2, 1, first, 1)),
        Ok(AdminApplyStatus::Duplicate)
    );
    let view = state.distribution_view().unwrap();
    assert_eq!(view.snapshot().lifecycle, fenced.lifecycle);
}
