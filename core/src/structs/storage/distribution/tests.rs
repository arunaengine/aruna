// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::change::{MAX_DESCRIPTOR_BYTES, apply_change};
use super::presets::{add_edges, replica_layout, shard_layout};
use super::validate::{validate_config, validate_record};
use super::*;
use crate::NodeId;
use ulid::Ulid;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

fn nodes(count: u8) -> Vec<(Ulid, NodeId)> {
    (1..=count).map(|seed| (id(seed), node(seed))).collect()
}

fn config(members: Vec<DistributionMember>, ranges: Vec<KeyRange>) -> DistributionConfig {
    DistributionConfig {
        bucket: "spire".to_string(),
        owner_group: id(90),
        node_set: None,
        members,
        ranges,
        settings: DistributionSettings::default(),
    }
}

fn sharded(count: u8, boundaries: &[&str], replicas: usize) -> DistributionConfig {
    let boundaries: Vec<String> = boundaries.iter().map(|key| key.to_string()).collect();
    let layout = shard_layout(id(99), &nodes(count), &boundaries, replicas).unwrap();
    config(layout.members, layout.ranges)
}

fn change(predecessor: Option<DistributionRevision>, op: DistributionOp) -> DistributionChange {
    DistributionChange {
        set_id: owned_set_id(id(90), 1, 99),
        predecessor,
        op,
    }
}

#[test]
fn replica_preset_validates() {
    let layout = replica_layout(id(99), &nodes(3)).unwrap();
    let range = &layout.ranges[0];
    assert_eq!(
        (range.coordinator, range.replicas.clone()),
        (id(1), vec![id(2), id(3)])
    );
    assert_eq!(range.epoch, FIRST_EPOCH);
    assert!(
        layout
            .members
            .iter()
            .all(|member| member.coverage == MetadataCoverage::All)
    );
    assert_eq!(
        validate_config(&config(layout.members, layout.ranges)),
        Ok(())
    );
    assert_eq!(
        replica_layout(id(99), &[]),
        Err(DistributionError::MemberCount)
    );
}

#[test]
fn shard_preset_spreads() {
    let config = sharded(3, &["g", "p"], 1);
    assert_eq!(validate_config(&config), Ok(()));
    let roles: Vec<_> = config
        .ranges
        .iter()
        .map(|range| (range.coordinator, range.replicas.clone()))
        .collect();
    assert_eq!(
        roles,
        vec![
            (id(1), vec![id(2)]),
            (id(2), vec![id(3)]),
            (id(3), vec![id(1)])
        ]
    );
    assert_ne!(config.ranges[0].range_id, config.ranges[1].range_id);
    assert!(shard_layout(id(99), &nodes(2), &[], 2).is_err());
    let unsorted = vec!["p".to_string(), "g".to_string()];
    assert_eq!(
        shard_layout(id(99), &nodes(2), &unsorted, 0),
        Err(DistributionError::IncompleteRanges)
    );
}

#[test]
fn edges_hold_records() {
    let mut layout = shard_layout(id(99), &nodes(2), &["m".to_string()], 0).unwrap();
    add_edges(&mut layout, &[(id(7), node(7))], DataStrategy::CacheOnUse).unwrap();
    assert!(add_edges(&mut layout, &[(id(8), node(8))], DataStrategy::Copy).is_err());
    let config = config(layout.members, layout.ranges);
    assert_eq!(validate_config(&config), Ok(()));
    let edge = config.members.last().unwrap();
    assert_eq!(
        (edge.coverage, edge.strategy),
        (MetadataCoverage::All, DataStrategy::CacheOnUse)
    );
}

type Mutation = fn(&mut Vec<KeyRange>);

#[test]
fn ranges_stay_complete() {
    let base = sharded(2, &["m"], 0);
    let mutations: Vec<(Mutation, DistributionError)> = vec![
        (
            |ranges| ranges[0].lower = "a".into(),
            DistributionError::IncompleteRanges,
        ),
        (
            |ranges| ranges[0].upper = Some("k".into()),
            DistributionError::IncompleteRanges,
        ),
        (
            |ranges| ranges[0].upper = Some("n".into()),
            DistributionError::IncompleteRanges,
        ),
        (
            |ranges| ranges[1].upper = Some("z".into()),
            DistributionError::IncompleteRanges,
        ),
        (
            |ranges| ranges.swap(0, 1),
            DistributionError::IncompleteRanges,
        ),
        (|ranges| ranges.clear(), DistributionError::RangeCount),
        (
            |ranges| {
                ranges[0].upper = Some(String::new());
                ranges[1].lower = String::new();
            },
            DistributionError::IncompleteRanges,
        ),
        (
            |ranges| {
                ranges[0].upper = Some("m".repeat(MAX_BOUND_BYTES + 1));
                ranges[1].lower = "m".repeat(MAX_BOUND_BYTES + 1);
            },
            DistributionError::BoundTooLong,
        ),
    ];
    for (mutate, expected) in mutations {
        let mut config = base.clone();
        mutate(&mut config.ranges);
        assert_eq!(validate_config(&config), Err(expected));
    }
    let mut duplicate = base.clone();
    duplicate.ranges[1].range_id = base.ranges[0].range_id;
    assert_eq!(
        validate_config(&duplicate),
        Err(DistributionError::DuplicateRange(base.ranges[0].range_id))
    );
}

#[test]
fn range_count_bounded() {
    let boundaries: Vec<String> = (1..MAX_RANGES).map(|index| format!("{index:05}")).collect();
    let layout = shard_layout(id(99), &nodes(4), &boundaries, 0).unwrap();
    assert_eq!(
        validate_config(&config(layout.members, layout.ranges)),
        Ok(())
    );
    let mut boundaries = boundaries;
    boundaries.push("99999".to_string());
    assert_eq!(
        shard_layout(id(99), &nodes(4), &boundaries, 0),
        Err(DistributionError::RangeCount)
    );
}

#[test]
fn record_size_bounded() {
    let boundaries: Vec<String> = (1..MAX_RANGES)
        .map(|index| format!("{index:05}{}", "k".repeat(1000)))
        .collect();
    let layout = shard_layout(id(99), &nodes(4), &boundaries, 0).unwrap();
    let record = DistributionRecord {
        set_id: id(99),
        revision: DistributionRevision::default(),
        phase: SetPhase::Pending,
        config: config(layout.members, layout.ranges),
    };
    assert_eq!(validate_config(&record.config), Ok(()));
    assert_eq!(validate_record(&record), Err(DistributionError::TooLarge));
}

#[test]
fn combinations_are_checked() {
    let mut config = sharded(3, &["m"], 1);
    config.members[0].strategy = DataStrategy::Stream;
    let first = config.ranges[0].range_id;
    assert_eq!(
        validate_config(&config),
        Err(DistributionError::CoordinatorNotCopy(first))
    );

    let mut config = sharded(3, &["m"], 1);
    config.ranges[0].replicas.push(id(1));
    assert_eq!(
        validate_config(&config),
        Err(DistributionError::DuplicateReplica(first))
    );

    let mut config = sharded(3, &["m"], 1);
    config.ranges[0].replicas = vec![id(50)];
    assert_eq!(
        validate_config(&config),
        Err(DistributionError::UnknownMember(first))
    );

    let mut config = sharded(3, &["m"], 0);
    assert_eq!(
        validate_config(&config),
        Err(DistributionError::IdleMember(id(3)))
    );
    config.members[2].coverage = MetadataCoverage::All;
    assert_eq!(validate_config(&config), Ok(()));

    let mut config = sharded(2, &["m"], 0);
    config.ranges[1].epoch = 0;
    let second = config.ranges[1].range_id;
    assert_eq!(
        validate_config(&config),
        Err(DistributionError::ZeroEpoch(second))
    );

    let mut config = sharded(2, &["m"], 0);
    config.members[1].node_id = node(1);
    assert_eq!(
        validate_config(&config),
        Err(DistributionError::DuplicateNode)
    );

    let mut config = sharded(2, &["m"], 0);
    config.settings.refresh_secs = Some(0);
    assert_eq!(
        validate_config(&config),
        Err(DistributionError::InvalidRefresh)
    );
    config.settings.refresh_secs = Some(60);
    config.bucket = "a/b".to_string();
    assert_eq!(
        validate_config(&config),
        Err(DistributionError::InvalidBucket)
    );
}

#[test]
fn record_finds_ranges() {
    let config = sharded(3, &["g", "p"], 0);
    let record = DistributionRecord {
        set_id: id(99),
        revision: DistributionRevision::default(),
        phase: SetPhase::Active,
        config,
    };
    let coordinator = |key: &str| record.range_for(key).map(|range| range.coordinator);
    assert_eq!(coordinator(""), Some(id(1)));
    assert_eq!(coordinator("f/zz"), Some(id(1)));
    assert_eq!(coordinator("g"), Some(id(2)));
    assert_eq!(coordinator("zzz"), Some(id(3)));
    assert!(!record.serving());
    let member = &record.config.members[0];
    assert_eq!(record.range_behaviour(member), RangeBehaviour::ServeAndFill);
    let mut stream = member.clone();
    stream.strategy = DataStrategy::Stream;
    assert_eq!(record.range_behaviour(&stream), RangeBehaviour::Passthrough);
    stream.range_behaviour = Some(RangeBehaviour::FillFirst);
    assert_eq!(record.range_behaviour(&stream), RangeBehaviour::FillFirst);
}

#[test]
fn creation_reaches_active() {
    let created = apply_change(
        None,
        &change(
            None,
            DistributionOp::Create {
                config: sharded(2, &["m"], 1),
            },
        ),
        node(1),
        b"create",
    )
    .unwrap();
    assert_eq!(
        (created.revision.number, created.phase),
        (1, SetPhase::Pending)
    );

    let admit = |record: &DistributionRecord, member: u8, origin: u8| {
        apply_change(
            Some(record),
            &change(
                Some(record.revision),
                DistributionOp::Admit {
                    member_id: id(member),
                    incarnation: u128::from(member),
                },
            ),
            node(origin),
            &[member],
        )
    };
    assert_eq!(
        admit(&created, 1, 2),
        Err(DistributionError::AdmissionOrigin(id(1)))
    );
    let first = admit(&created, 1, 1).unwrap();
    assert_eq!(first.member(id(1)).unwrap().state, MemberState::Ready);
    let activate = change(Some(first.revision), DistributionOp::Activate);
    assert_eq!(
        apply_change(Some(&first), &activate, node(1), b"a"),
        Err(DistributionError::MissingAdmission(id(2)))
    );
    let second = admit(&first, 2, 2).unwrap();
    let active = apply_change(
        Some(&second),
        &change(Some(second.revision), DistributionOp::Activate),
        node(1),
        b"a",
    )
    .unwrap();
    assert_eq!(
        (active.revision.number, active.phase),
        (4, SetPhase::Active)
    );
    assert!(active.serving());
    assert_eq!(admit(&active, 1, 1), Err(DistributionError::NotPending));
}

#[test]
fn stale_changes_refused() {
    let created = apply_change(
        None,
        &change(
            None,
            DistributionOp::Create {
                config: sharded(2, &["m"], 0),
            },
        ),
        node(1),
        b"create",
    )
    .unwrap();
    let settings = DistributionSettings {
        public: true,
        ..DistributionSettings::default()
    };
    let update = change(
        Some(created.revision),
        DistributionOp::ChangeSettings { settings },
    );
    let updated = apply_change(Some(&created), &update, node(1), b"one").unwrap();
    assert!(updated.config.settings.public);
    assert_eq!(
        apply_change(Some(&updated), &update, node(1), b"two"),
        Err(DistributionError::StaleRevision)
    );
    let other = apply_change(Some(&created), &update, node(1), b"two").unwrap();
    assert_ne!(other.revision, updated.revision);
    assert_eq!(other.revision.number, updated.revision.number);
}

#[test]
fn shapes_and_unsupported() {
    let create = DistributionOp::Create {
        config: sharded(2, &["m"], 0),
    };
    let revision = Some(DistributionRevision::default());
    assert!(matches!(
        change(revision, create).validate_shape(),
        Err(DistributionError::Malformed(_))
    ));
    let mut admitted = sharded(2, &["m"], 0);
    admitted.members[0].incarnation = Some(3);
    assert_eq!(
        change(None, DistributionOp::Create { config: admitted }).validate_shape(),
        Err(DistributionError::InvalidCreate)
    );
    let fenced = DistributionOp::TransferFenced {
        transfer_id: id(5),
        range_id: id(6),
        watermark: vec![0; 8],
        import_cursor: None,
        refresh_cursor: Some(vec![0; MAX_DESCRIPTOR_BYTES + 1]),
        open_uploads: 0,
    };
    assert!(change(revision, fenced).validate_shape().is_err());

    let created = apply_change(
        None,
        &change(
            None,
            DistributionOp::Create {
                config: sharded(2, &["m"], 0),
            },
        ),
        node(1),
        b"create",
    )
    .unwrap();
    let later = [
        DistributionOp::Leave { member_id: id(1) },
        DistributionOp::DeleteCommit,
        DistributionOp::TransferComplete {
            transfer_id: id(5),
            range_id: id(6),
        },
    ];
    for op in later {
        let name = op.name();
        assert!(!op.supported());
        assert_eq!(
            apply_change(
                Some(&created),
                &change(Some(created.revision), op),
                node(1),
                b"x"
            ),
            Err(DistributionError::Unsupported(name))
        );
    }
}

#[test]
fn sets_bind_owners() {
    let set_id = owned_set_id(id(90), 7, 3);
    assert!(owned_by(set_id, id(90)));
    assert!(!owned_by(set_id, id(91)));
    assert_eq!(set_id.timestamp_ms(), 7);
    assert!(owned_by(mint_set_id(id(91)), id(91)));
    // Another group cannot create a set under an id it does not own.
    let mut foreign = sharded(2, &["m"], 0);
    foreign.owner_group = id(91);
    let create = change(None, DistributionOp::Create { config: foreign });
    assert_eq!(
        create.validate_shape(),
        Err(DistributionError::ForeignSet(create.set_id))
    );
}
