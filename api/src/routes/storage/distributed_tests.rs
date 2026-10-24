// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::structs::storage::distribution::DistributionRevision;
use aruna_operations::distribution::status::{MemberStatus, RangeStatus};

use super::*;

fn node_text(seed: u8) -> String {
    iroh::SecretKey::from_bytes(&[seed; 32])
        .public()
        .to_string()
}

fn request(layout: LayoutName) -> CreateDistributedRequest {
    CreateDistributedRequest {
        bucket: "spire".to_string(),
        layout,
        nodes: vec![node_text(1), node_text(2)],
        boundaries: Vec::new(),
        replicas: 0,
        edges: vec![EdgeRequest {
            node_id: node_text(3),
            strategy: StrategyName::Stream,
        }],
        settings: SettingsRequest::default(),
    }
}

#[test]
fn create_request_converts() {
    let group_id = Ulid::from_bytes([9; 16]);
    let input = create_input(group_id, request(LayoutName::Replica)).unwrap();
    assert_eq!(input.layout, LayoutKind::Replica);
    assert_eq!(input.owner_group, group_id);
    assert_eq!(input.nodes.len(), 2);
    assert_eq!(input.edges[0].1, DataStrategy::Stream);
    let mut bounded = request(LayoutName::Replica);
    bounded.boundaries = vec!["m".to_string()];
    assert!(matches!(
        create_input(group_id, bounded),
        Err(ServerError::BadRequestMessage(_))
    ));
    let mut unknown = request(LayoutName::Sharding);
    unknown.nodes.push("not-a-node".to_string());
    assert!(matches!(
        create_input(group_id, unknown),
        Err(ServerError::BadRequestMessage(_))
    ));
}

#[test]
fn publisher_outage_unavailable() {
    let unavailable = PublishRefusal::Unavailable("publisher node x is unreachable".to_string());
    let ServerError::ServiceUnavailableReason(message) =
        map_create_error(CreateError::Publish(unavailable))
    else {
        panic!("an unreachable publisher answers 503");
    };
    assert!(message.starts_with("configuration changes unavailable"));
    assert!(matches!(
        map_create_error(CreateError::Publish(PublishRefusal::Forbidden)),
        ServerError::Forbidden
    ));
    assert!(matches!(
        map_create_error(CreateError::UnknownSet(Ulid::nil())),
        ServerError::NotFound
    ));
}

#[test]
fn degraded_names_coordinator() {
    let node: NodeId = node_text(2).parse().unwrap();
    let status = DistributionStatus {
        set_id: Ulid::from_bytes([1; 16]),
        bucket: "spire".to_string(),
        owner_group: Ulid::from_bytes([9; 16]),
        publisher: node,
        revision: DistributionRevision {
            number: 4,
            digest: [0; 32],
        },
        phase: SetPhase::Active,
        serving: true,
        conflicted: false,
        configuration_unavailable: true,
        degraded: true,
        members: vec![MemberStatus {
            member_id: Ulid::from_bytes([2; 16]),
            node_id: node,
            state: MemberState::CatchingUp,
            coverage: MetadataCoverage::All,
            strategy: DataStrategy::CacheOnUse,
            admitted: true,
        }],
        ranges: vec![RangeStatus {
            range_id: Ulid::from_bytes([3; 16]),
            lower: String::new(),
            upper: None,
            coordinator: Ulid::from_bytes([2; 16]),
            coordinator_node: Some(node),
            epoch: 1,
            replicas: Vec::new(),
            degraded: Some(Reach::Unreachable { since_ms: Some(77) }),
        }],
        recommended_grant: "/r/g/9/data/*/spire/**".to_string(),
    };
    let response = status_response(status);
    assert!(response.configuration_changes_unavailable);
    assert_eq!(response.members[0].state, "catching_up");
    assert_eq!(response.members[0].strategy, StrategyName::CacheOnUse);
    assert!(response.ranges[0].degraded);
    assert_eq!(response.ranges[0].unreachable_since_ms, Some(77));
    assert_eq!(response.ranges[0].coordinator_node, Some(node_text(2)));
}

#[test]
fn guard_refusals_map() {
    let set_id = Ulid::from_bytes([1; 16]);
    let wrong = GuardError::NotCoordinator {
        set_id,
        range_id: Ulid::from_bytes([2; 16]),
        node_id: node_text(2).parse().unwrap(),
    };
    assert!(matches!(
        map_guard_error(wrong),
        ServerError::ServiceUnavailableReason(_)
    ));
    assert!(matches!(
        map_guard_error(GuardError::Unbound(set_id)),
        ServerError::Conflict(_)
    ));
}
