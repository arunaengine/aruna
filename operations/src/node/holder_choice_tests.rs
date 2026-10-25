// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{HEARTBEAT_STALE_MS, HolderCandidate, candidate, link_bandwidth, pick};
use aruna_core::download_slots::DownloadSlots;
use aruna_core::heartbeat::{HeldHeartbeat, NodeHeartbeat};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::compute_config::{LocationLink, RealmComputeConfig};
use aruna_core::structs::storage::node_info::{NodeInfoDocument, NodeUrls, NodeUtilization};

fn node(seed: u8) -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn holder(seed: u8, free_slots: u32, bandwidth: u64) -> HolderCandidate {
    HolderCandidate {
        node_id: node(seed),
        urls: NodeUrls {
            api: Some(format!("https://node-{seed}.example.org/api/v1")),
            s3: None,
        },
        free_slots,
        bandwidth,
    }
}

fn document(seed: u8) -> NodeInfoDocument {
    NodeInfoDocument {
        node_id: node(seed),
        executors: Vec::new(),
        labels: Default::default(),
        urls: NodeUrls {
            api: Some("https://node.example.org/api/v1".to_string()),
            s3: None,
        },
        utilization: utilization(None),
        updated_at_ms: 0,
        epoch: Default::default(),
        compute_draining: false,
        leaving: false,
        demand: Default::default(),
        reservation: Default::default(),
    }
}

fn utilization(slots: Option<DownloadSlots>) -> NodeUtilization {
    NodeUtilization {
        storage_bytes_used: 0,
        documents_held: None,
        load_permille: None,
        heartbeat_at_ms: 0,
        download_slots: slots,
        storage_capacity_bytes: None,
    }
}

fn held(seed: u8, age_ms: u64, slots: Option<DownloadSlots>) -> HeldHeartbeat {
    HeldHeartbeat {
        node_id: node(seed),
        heartbeat: NodeHeartbeat {
            realm_id: RealmId::from_bytes([1; 32]),
            epoch: Default::default(),
            sequence: 1,
            utilization: utilization(slots),
            availability: Vec::new(),
            reservation: Default::default(),
            demand: Default::default(),
        },
        age_ms,
        received_at_ms: 0,
    }
}

fn slots(active: u32, refused: u32) -> Option<DownloadSlots> {
    Some(DownloadSlots {
        active,
        max: 256,
        refused,
    })
}

#[test]
fn free_slots_win() {
    // With exactly two candidates both are compared, whatever the shuffle order.
    for _ in 0..16 {
        assert_eq!(
            pick(vec![holder(1, 10, 0), holder(2, 200, 0)]),
            Some(holder(2, 200, 0))
        );
        assert_eq!(
            pick(vec![holder(1, 10, 5), holder(2, 10, 9)]),
            Some(holder(2, 10, 9))
        );
    }
    assert_eq!(pick(Vec::new()), None);
}

#[test]
fn skips_unusable_holders() {
    let document = document(1);
    let fresh = held(1, 1_000, slots(6, 0));
    let chosen = candidate(&document, Some(&fresh), false, 0).unwrap();
    assert_eq!(chosen.free_slots, 250);
    assert!(candidate(&document, None, false, 0).is_none());
    let stale = held(1, HEARTBEAT_STALE_MS, slots(6, 0));
    assert!(candidate(&document, Some(&stale), false, 0).is_none());
    let refusing = held(1, 1_000, slots(256, 3));
    assert!(candidate(&document, Some(&refusing), false, 0).is_none());
    assert!(candidate(&document, Some(&fresh), true, 0).is_none());
    // Without slot telemetry a holder stays a candidate but ranks last.
    let unknown = held(1, 1_000, None);
    assert_eq!(
        candidate(&document, Some(&unknown), false, 0)
            .unwrap()
            .free_slots,
        0
    );
}

#[test]
fn links_rank_bandwidth() {
    let mut config = RealmComputeConfig::default();
    config.links.push(LocationLink {
        from: "eu-west".to_string(),
        to: "eu-central".to_string(),
        bandwidth_per_sec: 77,
    });
    let config = Some(&config);
    assert_eq!(link_bandwidth(config, Some("a"), Some("a")), u64::MAX);
    assert_eq!(
        link_bandwidth(config, Some("eu-west"), Some("eu-central")),
        77
    );
    let unknown = link_bandwidth(config, Some("eu-central"), Some("eu-west"));
    assert_eq!(unknown, RealmComputeConfig::default().pessimistic_per_sec);
    assert_eq!(link_bandwidth(config, None, Some("a")), 0);
}
