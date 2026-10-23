//! Tests that distribution changes reduce to one record in any order and refuse stale input.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::admin_documents::AdminDocumentTarget;
use crate::reducer::{FINALITY_WINDOW, MAX_PENDING};
use crate::structs::storage::distribution::presets::replica_layout;
use crate::structs::storage::distribution::{
    DistributionChange, DistributionConfig, DistributionError, DistributionOp,
    DistributionRevision, DistributionSettings, SetPhase, owned_set_id,
};

fn set_id() -> Ulid {
    owned_set_id(group_id(), 1, 40)
}

fn target() -> AdminDocumentTarget {
    AdminDocumentTarget::Distribution {
        realm_id: realm_id(),
        set_id: set_id(),
    }
}

fn state() -> AdminDocumentState {
    AdminDocumentState::new(target())
}

fn config() -> DistributionConfig {
    let nodes = [
        (Ulid::from_bytes([1; 16]), node(1)),
        (Ulid::from_bytes([2; 16]), node(2)),
    ];
    let layout = replica_layout(set_id(), &nodes).unwrap();
    DistributionConfig {
        bucket: "spire".to_string(),
        owner_group: group_id(),
        node_set: None,
        members: layout.members,
        ranges: layout.ranges,
        settings: DistributionSettings::default(),
    }
}

fn change_event(
    event_seed: u8,
    origin_seed: u8,
    origin_seq: u64,
    predecessor: Option<DistributionRevision>,
    op: DistributionOp,
) -> AdminDocumentEvent {
    AdminDocumentEvent {
        event_id: Ulid::from_bytes([event_seed; 16]),
        target: target(),
        origin_node_id: node(origin_seed),
        origin_seq,
        observed: AdminDocumentClock::default(),
        actor: actor(node(origin_seed)),
        op: AdminDocumentOperation::DistributionChanged {
            change: Box::new(DistributionChange {
                set_id: set_id(),
                predecessor,
                op,
            }),
        },
    }
}

fn revision(state: &AdminDocumentState) -> Option<DistributionRevision> {
    state
        .materialized_distribution()
        .map(|record| record.revision)
}

fn admit(
    event_seed: u8,
    member: u8,
    predecessor: Option<DistributionRevision>,
) -> AdminDocumentEvent {
    let op = DistributionOp::Admit {
        member_id: Ulid::from_bytes([member; 16]),
        incarnation: u128::from(member),
    };
    change_event(event_seed, member, u64::from(event_seed), predecessor, op)
}

fn public(public: bool) -> DistributionOp {
    DistributionOp::ChangeSettings {
        settings: DistributionSettings {
            public,
            ..DistributionSettings::default()
        },
    }
}

#[test]
fn creation_activates_set() {
    let mut state = state();
    let create = change_event(10, 1, 1, None, DistributionOp::Create { config: config() });
    assert_eq!(state.apply(&create), Ok(AdminApplyStatus::Applied));
    let created = revision(&state);

    let foreign = change_event(
        11,
        3,
        1,
        created,
        DistributionOp::Admit {
            member_id: Ulid::from_bytes([1; 16]),
            incarnation: 1,
        },
    );
    assert_eq!(
        state.apply(&foreign),
        Err(DistributionError::AdmissionOrigin(Ulid::from_bytes([1; 16])).into())
    );
    state.apply(&admit(12, 1, created)).unwrap();
    let early = change_event(13, 1, 2, revision(&state), DistributionOp::Activate);
    assert_eq!(
        state.apply(&early),
        Err(DistributionError::MissingAdmission(Ulid::from_bytes([2; 16])).into())
    );
    state.apply(&admit(14, 2, revision(&state))).unwrap();
    state
        .apply(&change_event(
            15,
            1,
            3,
            revision(&state),
            DistributionOp::Activate,
        ))
        .unwrap();
    let record = state.materialized_distribution().unwrap();
    assert_eq!(
        (record.phase, record.revision.number),
        (SetPhase::Active, 4)
    );
    assert!(record.serving());
    assert_eq!(group_state().materialized_distribution(), None);
}

#[test]
fn concurrent_successors_converge() {
    let create = change_event(10, 1, 1, None, DistributionOp::Create { config: config() });
    let mut base = state();
    base.apply(&create).unwrap();
    let created = revision(&base);
    // Two admins change the settings of one revision; the smaller event id wins everywhere.
    let first = change_event(20, 1, 2, created, public(true));
    let second = change_event(30, 2, 1, created, public(false));

    let mut ordered = base.clone();
    ordered.apply(&first).unwrap();
    assert_eq!(
        ordered.apply(&second),
        Err(DistributionError::StaleRevision.into())
    );

    let mut reversed = base.clone();
    reversed.apply(&second).unwrap();
    assert!(
        !reversed
            .materialized_distribution()
            .unwrap()
            .config
            .settings
            .public
    );
    reversed.apply(&first).unwrap();

    let winner = ordered.materialized_distribution().unwrap();
    assert!(winner.config.settings.public);
    assert_eq!(reversed.materialized_distribution(), Some(winner.clone()));

    // A change built on the losing branch never takes effect.
    let mut loser = base.clone();
    loser.apply(&second).unwrap();
    let on_loser = change_event(31, 2, 2, revision(&loser), public(true));
    reversed.apply(&on_loser).unwrap();
    assert_eq!(reversed.materialized_distribution(), Some(winner));
}

#[test]
fn late_changes_apply() {
    let create = change_event(10, 1, 1, None, DistributionOp::Create { config: config() });
    let mut source = state();
    source.apply(&create).unwrap();
    let admitted = admit(12, 1, revision(&source));
    source.apply(&admitted).unwrap();

    let mut late = state();
    late.apply(&admitted).unwrap();
    assert_eq!(late.materialized_distribution(), None);
    late.apply(&create).unwrap();
    assert_eq!(
        late.materialized_distribution(),
        source.materialized_distribution()
    );
}

#[test]
fn foreign_changes_refused() {
    let mut state = state();
    state
        .apply(&change_event(
            10,
            1,
            1,
            None,
            DistributionOp::Create { config: config() },
        ))
        .unwrap();
    let leave = change_event(
        11,
        1,
        2,
        revision(&state),
        DistributionOp::Leave {
            member_id: Ulid::from_bytes([2; 16]),
        },
    );
    assert_eq!(
        state.apply(&leave),
        Err(DistributionError::Unsupported("leave").into())
    );
    let mut other = change_event(12, 1, 2, revision(&state), public(true));
    if let AdminDocumentOperation::DistributionChanged { change } = &mut other.op {
        change.set_id = Ulid::from_bytes([41; 16]);
    }
    assert_eq!(
        state.apply(&other),
        Err(DistributionError::SetMismatch(Ulid::from_bytes([41; 16])).into())
    );
    let on_config = AdminDocumentEvent {
        target: AdminDocumentTarget::RealmConfig {
            realm_id: realm_id(),
        },
        ..change_event(13, 1, 2, revision(&state), public(true))
    };
    assert_eq!(
        realm_config_state().apply(&on_config),
        Err(AdminDocumentError::UnsupportedTarget)
    );
}

#[test]
fn foreign_create_refused() {
    // Another group's admin cannot replace a set, however early its event sorts.
    let mut state = state();
    state
        .apply(&change_event(
            10,
            1,
            1,
            None,
            DistributionOp::Create { config: config() },
        ))
        .unwrap();
    let mut foreign = config();
    foreign.owner_group = Ulid::from_bytes([77; 16]);
    let early = change_event(1, 3, 1, None, DistributionOp::Create { config: foreign });
    assert_eq!(
        state.apply(&early),
        Err(DistributionError::ForeignSet(set_id()).into())
    );
    assert_eq!(
        state
            .materialized_distribution()
            .unwrap()
            .config
            .owner_group,
        group_id()
    );
}

fn stored(state: &AdminDocumentState) -> usize {
    state
        .user_subject_ids
        .keys()
        .filter(|path| path.starts_with("distribution.changes."))
        .count()
}

#[test]
fn foreign_admit_cannot_shadow() {
    // A foreign admission seen before the create must not hide the member's own one.
    let create = change_event(10, 1, 1, None, DistributionOp::Create { config: config() });
    let mut scratch = state();
    scratch.apply(&create).unwrap();
    let created = revision(&scratch);
    let op = DistributionOp::Admit {
        member_id: Ulid::from_bytes([2; 16]),
        incarnation: 2,
    };
    let foreign = change_event(11, 3, 1, created, op.clone());
    let own = change_event(12, 2, 1, created, op);

    let mut early = state();
    early.apply(&foreign).unwrap();
    early.apply(&create).unwrap();
    early.apply(&own).unwrap();

    let mut ordered = state();
    ordered.apply(&create).unwrap();
    assert!(ordered.apply(&foreign).is_err());
    ordered.apply(&own).unwrap();

    let record = ordered.materialized_distribution().unwrap();
    let member = record.member(Ulid::from_bytes([2; 16])).unwrap();
    assert_eq!(member.incarnation, Some(2));
    assert_eq!(early.materialized_distribution(), Some(record));
    assert_eq!(stored(&early), stored(&ordered));
}

#[test]
fn pending_is_bounded() {
    let mut state = state();
    let unknown = |seed: u8| DistributionRevision {
        number: 5,
        digest: [seed; 32],
    };
    for seed in 0..MAX_PENDING as u8 {
        let event = change_event(
            20 + seed,
            5,
            u64::from(seed) + 1,
            Some(unknown(seed)),
            public(true),
        );
        state.apply(&event).unwrap();
    }
    let over = change_event(60, 5, 60, Some(unknown(60)), public(true));
    assert_eq!(
        state.apply(&over),
        Err(DistributionError::TooManyPending.into())
    );
    let other = change_event(61, 6, 1, Some(unknown(61)), public(true));
    assert_eq!(state.apply(&other), Ok(AdminApplyStatus::Applied));
}

#[test]
fn history_is_bounded() {
    let mut state = state();
    state
        .apply(&change_event(
            10,
            1,
            1,
            None,
            DistributionOp::Create { config: config() },
        ))
        .unwrap();
    let early = revision(&state);
    for step in 0..40u8 {
        let event = change_event(
            100 + step,
            1,
            u64::from(step) + 2,
            revision(&state),
            public(step % 2 == 0),
        );
        state.apply(&event).unwrap();
    }
    let record = state.materialized_distribution().unwrap();
    assert_eq!(record.revision.number, 41);
    assert!(!record.config.settings.public);
    assert!(stored(&state) <= FINALITY_WINDOW);
    // A change on a revision folded into the checkpoint can no longer win.
    let late = change_event(1, 2, 1, early, public(true));
    assert_eq!(
        state.apply(&late),
        Err(DistributionError::StaleRevision.into())
    );
}
