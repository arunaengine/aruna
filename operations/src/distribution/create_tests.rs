// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::distribution::{MemberState, MetadataCoverage};

use super::*;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn input(layout: LayoutKind, edges: Vec<(NodeId, DataStrategy)>) -> CreateInput {
    CreateInput {
        bucket: "spire".to_string(),
        owner_group: Ulid::from_bytes([90; 16]),
        nodes: vec![node(1), node(2)],
        layout,
        edges,
        settings: DistributionSettings::default(),
    }
}

#[test]
fn replica_plan_with_edge() {
    let set_id = Ulid::from_bytes([3; 16]);
    let edges = vec![(node(3), DataStrategy::Stream)];
    let layout = plan_layout(set_id, &input(LayoutKind::Replica, edges)).unwrap();
    assert_eq!(layout.members.len(), 3);
    assert!(
        layout
            .members
            .iter()
            .all(|member| member.state == MemberState::Created)
    );
    let [range] = layout.ranges.as_slice() else {
        panic!("a replica layout has one range");
    };
    let coordinator = layout
        .members
        .iter()
        .find(|m| m.node_id == node(1))
        .unwrap();
    let replica = layout
        .members
        .iter()
        .find(|m| m.node_id == node(2))
        .unwrap();
    let edge = layout
        .members
        .iter()
        .find(|m| m.node_id == node(3))
        .unwrap();
    assert_eq!(range.coordinator, coordinator.member_id);
    assert_eq!(range.replicas, vec![replica.member_id]);
    assert_eq!(edge.strategy, DataStrategy::Stream);
    assert_eq!(edge.coverage, MetadataCoverage::All);
}

#[test]
fn shard_plan_alternates() {
    let layout = LayoutKind::Sharding {
        boundaries: vec!["m".to_string()],
        replicas: 1,
    };
    let planned = plan_layout(Ulid::from_bytes([3; 16]), &input(layout, Vec::new())).unwrap();
    let [lower, upper] = planned.ranges.as_slice() else {
        panic!("one boundary gives two ranges");
    };
    assert_eq!(lower.coordinator, upper.replicas[0]);
    assert_eq!(upper.coordinator, lower.replicas[0]);
    assert_eq!(upper.lower, "m");
}

#[test]
fn copy_edges_refused() {
    let edges = vec![(node(3), DataStrategy::Copy)];
    let set_id = Ulid::from_bytes([3; 16]);
    assert!(plan_layout(set_id, &input(LayoutKind::Replica, edges)).is_err());
}

#[test]
fn forbidden_member_hints_globs() {
    assert!(refusal_text(&SyncRefusal::Forbidden).contains("node glob"));
}
