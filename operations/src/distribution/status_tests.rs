// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::distribution::presets::shard_layout;
use aruna_core::structs::storage::distribution::{
    DistributionConfig, DistributionRequest, DistributionSettings, SetRoot, SignedSnapshot,
    next_snapshot,
};

use super::*;

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

/// Two shards: node 1 publishes and coordinates the lower range, node 2 the upper one.
fn view() -> DistributionView {
    let root = SetRoot {
        publisher: secret(1).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    };
    let nodes = [
        (Ulid::from_bytes([1; 16]), secret(1).public()),
        (Ulid::from_bytes([2; 16]), secret(2).public()),
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
    let snapshot = next_snapshot(realm(), None, &create).unwrap();
    DistributionView {
        head: SignedSnapshot::sign(snapshot, realm(), &secret(1)).unwrap(),
        conflict: None,
    }
}

fn only(reachable: u8) -> impl Fn(NodeId) -> Reach {
    move |node_id| match node_id == secret(reachable).public() {
        true => Reach::Reachable,
        false => Reach::Unreachable { since_ms: Some(5) },
    }
}

#[test]
fn coordinator_degrades_range() {
    let status = summarize(realm(), &view(), only(1));
    assert!(status.degraded);
    assert!(!status.configuration_unavailable);
    assert_eq!(status.ranges[0].degraded, None);
    assert_eq!(
        status.ranges[1].degraded,
        Some(Reach::Unreachable { since_ms: Some(5) })
    );
    assert!(status.members.iter().all(|member| !member.admitted));
    assert!(!status.serving);
    assert_eq!(
        status.recommended_grant,
        format!(
            "/{}/g/{}/data/*/spire/**",
            realm(),
            Ulid::from_bytes([90; 16])
        )
    );
}

#[test]
fn publisher_stops_configuration() {
    let status = summarize(realm(), &view(), only(2));
    assert!(status.configuration_unavailable);
    assert_eq!(status.ranges[1].degraded, None);
    assert!(status.ranges[0].degraded.is_some());
}

#[test]
fn heartbeats_decide_reach() {
    let heard = BTreeMap::from([
        (secret(2).public(), (0, 77)),
        (secret(3).public(), (u64::MAX, 77)),
    ]);
    let reach = heartbeat_reach(Some(secret(1).public()), heard);
    assert_eq!(reach(secret(1).public()), Reach::Reachable);
    assert_eq!(reach(secret(2).public()), Reach::Reachable);
    assert_eq!(
        reach(secret(3).public()),
        Reach::Unreachable { since_ms: Some(77) }
    );
    assert_eq!(
        reach(secret(4).public()),
        Reach::Unreachable { since_ms: None }
    );
}
