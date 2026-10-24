// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::presets::{add_edges, replica_layout, shard_layout};
use super::validate::validate_config;
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
fn range_behaviour_falls_back() {
    let config = sharded(2, &["m"], 0);
    let member = &config.members[0];
    assert_eq!(config.range_behaviour(member), RangeBehaviour::ServeAndFill);
    let mut stream = member.clone();
    stream.strategy = DataStrategy::Stream;
    assert_eq!(config.range_behaviour(&stream), RangeBehaviour::Passthrough);
    stream.range_behaviour = Some(RangeBehaviour::FillFirst);
    assert_eq!(config.range_behaviour(&stream), RangeBehaviour::FillFirst);
}
