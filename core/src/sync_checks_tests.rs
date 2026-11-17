//! Tests the replicated document checks: owners, bounds, advertisements and watch fences.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::NodeId;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

#[test]
fn usage_owner_validation() {
    use crate::structs::storage::usage::UsageCounters;

    let node_id = node(7);
    let realm_id = RealmId::from_bytes([2u8; 32]);
    let group_id = Ulid::from_bytes([4u8; 16]);
    let global = DocumentTarget::NodeUsage {
        realm_id,
        node_id,
        group_id: None,
    };
    let group = DocumentTarget::NodeUsage {
        realm_id,
        node_id,
        group_id: Some(group_id),
    };

    // The owning node's own snapshot validates as global and per-group.
    let owned = NodeUsageSnapshot {
        node_id,
        counters: UsageCounters {
            buckets: 3,
            ..Default::default()
        },
        replicas: Default::default(),
        revisions: Default::default(),
    };
    let owned_bytes = owned.to_bytes().unwrap();
    assert!(validate_usage_upsert(&global, &owned_bytes).is_ok());
    assert!(validate_usage_upsert(&group, &owned_bytes).is_ok());

    // Zero-counter snapshots (stale-group cleanup) are legitimate upserts.
    let zero = NodeUsageSnapshot {
        node_id,
        counters: UsageCounters::default(),
        replicas: Default::default(),
        revisions: Default::default(),
    };
    assert!(validate_usage_upsert(&global, &zero.to_bytes().unwrap()).is_ok());

    // A snapshot whose embedded node id is a different node is rejected.
    let misattributed = NodeUsageSnapshot {
        node_id: node(9),
        counters: UsageCounters {
            buckets: 99,
            ..Default::default()
        },
        replicas: Default::default(),
        revisions: Default::default(),
    };
    assert!(validate_usage_upsert(&global, &misattributed.to_bytes().unwrap()).is_err());

    // Undecodable payloads and non node-usage targets are rejected.
    assert!(validate_usage_upsert(&global, b"not-a-snapshot").is_err());
    assert!(
        validate_usage_upsert(&DocumentTarget::RealmConfig { realm_id }, &owned_bytes).is_err()
    );
}

#[test]
fn watch_interest_validation() {
    use crate::structs::execution::notification_watch::{WatchEventKind, WatchEventMask};

    let node_id = node(7);
    let realm_id = RealmId::from_bytes([12u8; 32]);
    let target = DocumentTarget::WatchInterest { realm_id, node_id };

    // The owning node's own digest validates.
    let owned = WatchInterestDigest::from_subscriptions(
        node_id,
        [(
            "/owned/**".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        )],
    );
    let owned_bytes = owned.to_bytes().unwrap();
    assert!(validate_watch_interest(&target, &owned_bytes).is_ok());

    // Empty digests are legitimate upserts that clear a node's interest.
    let empty = WatchInterestDigest {
        node_id,
        entries: Vec::new(),
    };
    assert!(validate_watch_interest(&target, &empty.to_bytes().unwrap()).is_ok());

    let too_many = WatchInterestDigest::from_subscriptions(
        node_id,
        (0..=INTEREST_ENTRY_CAP).map(|index| {
            (
                format!("/entry/{index}"),
                WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            )
        }),
    );
    assert!(validate_watch_interest(&target, &too_many.to_bytes().unwrap()).is_err());
    assert!(validate_watch_interest(&target, &vec![0; INTEREST_BYTES_CAP + 1],).is_err());

    // A digest whose embedded node id is a different node is rejected.
    let misattributed = WatchInterestDigest::from_subscriptions(
        node(9),
        [(
            "/forged/**".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        )],
    );
    assert!(validate_watch_interest(&target, &misattributed.to_bytes().unwrap()).is_err());

    // Undecodable payloads and non watch-interest targets are rejected.
    assert!(validate_watch_interest(&target, b"not-a-digest").is_err());
    assert!(
        validate_watch_interest(&DocumentTarget::RealmConfig { realm_id }, &owned_bytes,).is_err()
    );
}

#[test]
fn stale_advertisement_skipped() {
    // A delayed advertisement from an older rejoin epoch must not replace
    // the newer one this node already stored.
    use crate::structs::storage::node_info::{
        AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization,
    };

    let document = |membership: u64, publisher: u64| NodeInfoDocument {
        node_id: node(7),
        executors: Vec::new(),
        labels: std::collections::BTreeMap::new(),
        urls: NodeUrls {
            api: None,
            s3: None,
        },
        utilization: NodeUtilization {
            storage_bytes_used: 1,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 5,
            download_slots: None,
            storage_capacity_bytes: None,
        },
        updated_at_ms: 5,
        epoch: AdvertisementEpoch {
            membership_generation: membership,
            publisher_generation: publisher,
            observed_at_ms: 5,
        },
        compute_draining: false,
        leaving: false,
        demand: Default::default(),
        reservation: Default::default(),
    };
    let current = document(7, 1).to_bytes().expect("document serializes");

    assert!(node_info_supersedes(&document(7, 2), Some(&current)));
    assert!(!node_info_supersedes(&document(6, 900), Some(&current)));
    assert!(!node_info_supersedes(&document(7, 1), Some(&current)));
    assert!(node_info_supersedes(&document(1, 1), None));
    assert!(node_info_supersedes(&document(1, 1), Some(b"corrupt")));

    // An unbounded or self-contradicting snapshot never reaches storage.
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let target = DocumentTarget::NodeInfo {
        realm_id,
        node_id: node(7),
    };
    let mut ahead = document(7, 1);
    ahead.demand.epoch.membership_generation = 8;
    assert!(validate_node_upsert(&target, &postcard::to_allocvec(&ahead).unwrap()).is_err());
}

#[test]
fn node_owner_validation() {
    use crate::structs::storage::node_info::{
        AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization,
    };

    let node_id = node(7);
    let realm_id = RealmId::from_bytes([2u8; 32]);
    let target = DocumentTarget::NodeInfo { realm_id, node_id };

    let owned = NodeInfoDocument {
        node_id,
        executors: Vec::new(),
        labels: std::collections::BTreeMap::new(),
        urls: NodeUrls {
            api: None,
            s3: None,
        },
        utilization: NodeUtilization {
            storage_bytes_used: 1,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 5,
            download_slots: None,
            storage_capacity_bytes: None,
        },
        updated_at_ms: 5,
        epoch: AdvertisementEpoch {
            membership_generation: 1,
            publisher_generation: 1,
            observed_at_ms: 5,
        },
        compute_draining: false,
        leaving: false,
        demand: Default::default(),
        reservation: Default::default(),
    };
    assert!(validate_node_upsert(&target, &owned.to_bytes().unwrap()).is_ok());

    // A document whose embedded node id is a different node is rejected.
    let misattributed = NodeInfoDocument {
        node_id: node(9),
        ..owned.clone()
    };
    assert!(validate_node_upsert(&target, &misattributed.to_bytes().unwrap()).is_err());

    // Undecodable payloads and non node-info targets are rejected.
    assert!(validate_node_upsert(&target, b"not-a-document").is_err());
    assert!(
        validate_node_upsert(
            &DocumentTarget::RealmConfig { realm_id },
            &owned.to_bytes().unwrap()
        )
        .is_err()
    );
}

#[test]
fn watch_changes_fence() {
    use DocumentChangeKind::{Delete, Upsert};
    assert!(watch_change_applies(None, Upsert));
    assert!(watch_change_applies(None, Delete));
    assert!(watch_change_applies(Some(Upsert), Delete));
    assert!(!watch_change_applies(Some(Upsert), Upsert));
    assert!(!watch_change_applies(Some(Delete), Upsert));
    assert!(!watch_change_applies(Some(Delete), Delete));
}
