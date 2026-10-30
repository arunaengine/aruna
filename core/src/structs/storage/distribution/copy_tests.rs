//! Tests new copies of ranges in a set's record: who may take one, how reads and transfers wait
//! for the fill, the coordinator's statement that ends it, and splits by a group admin.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::*;
use crate::structs::storage::distribution::presets::shard_layout;
use crate::structs::storage::distribution::split::split_ids;
use crate::structs::storage::distribution::{
    DistributionConfig, DistributionRequest, DistributionSettings, KeyRange, ReadRoute, SetRoot,
    TransferRole, next_snapshot,
};

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn member(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

/// Two shards split at "m" without copies: member 1 coordinates the lower one, member 2 the
/// upper one, both admitted and active.
fn active() -> DistributionSnapshot {
    let root = SetRoot {
        publisher: secret(1).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    };
    let nodes = [
        (member(1), secret(1).public()),
        (member(2), secret(2).public()),
    ];
    let layout = shard_layout(root.set_id(), &nodes, &["m".to_string()], 0).unwrap();
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
    let mut head = next_snapshot(realm(), None, &create).unwrap();
    for seed in [1, 2] {
        let intent = head.member(member(seed)).unwrap().intent;
        let statement = Statement::admit(realm(), head.set_id(), member(seed), 5, intent);
        let request = DistributionRequest::Statement(Box::new(statement.sign(&secret(seed))));
        head = next_snapshot(realm(), Some(&head), &request).unwrap();
    }
    next_snapshot(realm(), Some(&head), &DistributionRequest::Activate).unwrap()
}

fn fill(range: &KeyRange, seed: u8) -> RangeFill {
    RangeFill {
        fill_id: Ulid::from_bytes([40 + seed; 16]),
        range_id: range.range_id,
        member_id: member(seed),
    }
}

fn add(
    head: &DistributionSnapshot,
    fill: &RangeFill,
) -> Result<DistributionSnapshot, DistributionError> {
    let request = DistributionRequest::AddCopy { fill: fill.clone() };
    next_snapshot(realm(), Some(head), &request)
}

fn filled(
    head: &DistributionSnapshot,
    signer: u8,
    fill: &RangeFill,
    epoch: u64,
) -> DistributionRequest {
    let member = head.member(member(signer)).unwrap();
    let statement = Statement::copy_filled(realm(), head.set_id(), member, fill, (epoch, 9));
    DistributionRequest::Statement(Box::new(statement.sign(&secret(signer))))
}

fn transfer(range: &KeyRange, to: u8) -> DistributionRequest {
    DistributionRequest::TransferPrepare {
        transfer_id: Ulid::from_bytes([60; 16]),
        range_id: range.range_id,
        to: member(to),
        epoch: 0,
        role: TransferRole::Replica,
    }
}

#[test]
fn copy_fills_range() {
    // Member 2 gains a copy of member 1's range; reads and transfers wait until it is filled.
    let head = active();
    let lower = head.config.ranges[0].clone();
    let copy = fill(&lower, 2);
    let next = add(&head, &copy).unwrap();
    let range = &next.config.ranges[0];
    assert_eq!(range.replicas, vec![member(2)]);
    assert_eq!(next.lifecycle.fills, vec![copy.clone()]);
    assert!(next.bootstrap_open() && next.filling(range.range_id, member(2)));
    assert!(next.copy_holders(range).is_empty());
    let coordinator = next.member(member(1)).unwrap().node_id;
    let holder = ReadRoute::Holder {
        range_id: range.range_id,
        node_id: coordinator,
    };
    assert_eq!(next.read_route(member(2), "a", false), Ok(holder.clone()));
    assert_eq!(next.read_route(member(2), "", true), Ok(holder));
    assert!(next.transfer_targets(range).is_empty());
    assert!(next_snapshot(realm(), Some(&next), &transfer(range, 2)).is_err());
    // Only the coordinator states the copy filled, and only at the range's epoch.
    let epoch = range.epoch;
    assert!(next_snapshot(realm(), Some(&next), &filled(&next, 2, &copy, epoch)).is_err());
    assert!(next_snapshot(realm(), Some(&next), &filled(&next, 1, &copy, epoch + 1)).is_err());
    let done = next_snapshot(realm(), Some(&next), &filled(&next, 1, &copy, epoch)).unwrap();
    let range = &done.config.ranges[0];
    assert!(done.lifecycle.fills.is_empty() && !done.bootstrap_open());
    assert_eq!(done.copy_holders(range).len(), 1);
    let local = ReadRoute::Local { catching_up: false };
    assert_eq!(done.read_route(member(2), "a", false), Ok(local));
    assert_eq!(done.transfer_targets(range).len(), 1);
    assert!(next_snapshot(realm(), Some(&done), &transfer(range, 2)).is_ok());
    // A replayed statement finds no filling copy.
    assert!(next_snapshot(realm(), Some(&done), &filled(&next, 1, &copy, epoch)).is_err());
}

#[test]
fn copy_refusals() {
    let head = active();
    let lower = head.config.ranges[0].clone();
    // The coordinator keeps the range already, and an unknown member takes nothing.
    assert!(add(&head, &fill(&lower, 1)).is_err());
    assert!(add(&head, &fill(&lower, 3)).is_err());
    let unknown = RangeFill {
        range_id: Ulid::from_bytes([77; 16]),
        ..fill(&lower, 2)
    };
    assert!(add(&head, &unknown).is_err());
    // A second copy on the same member, or one under the same id, is refused.
    let next = add(&head, &fill(&lower, 2)).unwrap();
    assert!(add(&next, &fill(&lower, 2)).is_err());
    let upper = next.config.ranges[1].clone();
    let reused = RangeFill {
        member_id: member(1),
        ..fill(&upper, 2)
    };
    assert!(add(&next, &reused).is_err());
    // A member that is not ready, or keeps no bytes, takes no copy.
    let changes: [fn(&mut DistributionMember); 2] = [
        |member| member.state = MemberState::CatchingUp,
        |member| member.strategy = DataStrategy::Stream,
    ];
    for change in changes {
        let mut changed = head.clone();
        let index = (changed.config.members.iter())
            .position(|known| known.member_id == member(2))
            .unwrap();
        change(&mut changed.config.members[index]);
        assert!(add_copy(&mut changed, &fill(&lower, 2)).is_err());
    }
    // A range that moves takes no copy until the transfer ends.
    let mut moving = head.clone();
    moving
        .lifecycle
        .transfers
        .push(super::super::snapshot::OpenTransfer {
            transfer_id: Ulid::from_bytes([61; 16]),
            range_id: lower.range_id,
            from: member(1),
            to: member(3),
            epoch: 4,
            role: TransferRole::Replica,
            fenced: None,
            completed: None,
        });
    assert!(add_copy(&mut moving, &fill(&lower, 2)).is_err());
}

#[test]
fn leave_drops_fill() {
    let head = active();
    let lower = head.config.ranges[0].clone();
    let upper = head.config.ranges[1].clone();
    let mut next = add(&head, &fill(&lower, 2)).unwrap();
    // Member 2 still coordinates the upper range; hand it over by hand to let it leave.
    let index = (next.config.ranges.iter())
        .position(|range| range.range_id == upper.range_id)
        .unwrap();
    next.config.ranges[index].coordinator = member(1);
    leave(&mut next, member(2)).unwrap();
    assert!(next.lifecycle.fills.is_empty());
    assert!(next.config.ranges[0].replicas.is_empty());
}

#[test]
fn admin_splits_range() {
    // A group admin's split makes the children an automatic split at that key would make.
    let head = active();
    let lower = head.config.ranges[0].clone();
    let request = |epoch, key: &str| DistributionRequest::Split {
        range_id: lower.range_id,
        epoch,
        split_key: key.to_string(),
    };
    let next = next_snapshot(realm(), Some(&head), &request(lower.epoch, "f/")).unwrap();
    let [_, lower_id, upper_id] = split_ids(head.set_id(), lower.range_id, lower.epoch, "f/");
    let ids: Vec<Ulid> = next.config.ranges.iter().map(|r| r.range_id).collect();
    assert_eq!(ids[..2], [lower_id, upper_id]);
    assert_eq!(next.config.ranges[1].lower, "f/");
    assert_eq!(next.admin_number, next.number);
    // A stale epoch, a key outside the range and the range's own bound are refused.
    for (epoch, key) in [
        (lower.epoch + 1, "f/"),
        (lower.epoch, "x"),
        (lower.epoch, ""),
    ] {
        assert!(next_snapshot(realm(), Some(&head), &request(epoch, key)).is_err());
    }
    // The retired range resolves to its children and no longer splits.
    assert!(next_snapshot(realm(), Some(&next), &request(lower.epoch, "c")).is_err());
    // A range whose new copy is still filling does not split.
    let copying = add(&head, &fill(&lower, 2)).unwrap();
    assert!(next_snapshot(realm(), Some(&copying), &request(lower.epoch, "f/")).is_err());
}
