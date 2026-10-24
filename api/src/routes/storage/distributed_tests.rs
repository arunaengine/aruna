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

#[test]
fn degraded_ranges_name_coordinator() {
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
