//! Tests holder choice: free download slots, skipped holders and location link ranking.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    HEARTBEAT_STALE_MS, HolderCandidate, HolderChoiceError, HolderChoiceOperation, candidate,
    link_bandwidth, pick,
};
use crate::document::DocumentTarget;
use crate::download_slots::DownloadSlots;
use crate::effects::{Effect, NetEffect, StorageEffect};
use crate::events::{Event, NetError, NetEvent, StorageEvent};
use crate::heartbeat::{HeartbeatEffect, HeartbeatEvent, HolderFacts};
use crate::heartbeat::{HeldHeartbeat, NodeHeartbeat};
use crate::keyspaces::NODE_INFO_KEYSPACE;
use crate::operation::EffectOperation;
use crate::operation_check::{
    check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
};
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::compute_config::{LocationLink, RealmComputeConfig};
use crate::structs::storage::node_info::node_info_key;
use crate::structs::storage::node_info::{NodeInfoDocument, NodeUrls, NodeUtilization};
use crate::types::{Key, Value};

fn node(seed: u8) -> crate::NodeId {
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
            jobs: None,
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
    for seed in 0..16 {
        assert_eq!(
            pick(vec![holder(1, 10, 0), holder(2, 200, 0)], seed),
            Some(holder(2, 200, 0))
        );
        assert_eq!(
            pick(vec![holder(1, 10, 5), holder(2, 10, 9)], seed),
            Some(holder(2, 10, 9))
        );
    }
    assert_eq!(pick(Vec::new(), 3), None);
    // The seed alone decides which two of more candidates are compared.
    let three = || vec![holder(1, 10, 0), holder(2, 20, 0), holder(3, 30, 0)];
    assert_eq!(pick(three(), 0), Some(holder(3, 30, 0)));
    assert_eq!(pick(three(), 1), Some(holder(2, 20, 0)));
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

const REALM: RealmId = RealmId([1; 32]);

fn facts(held: Vec<HeldHeartbeat>, avoided: Vec<crate::NodeId>) -> Event {
    Event::Net(NetEvent::Heartbeat(HeartbeatEvent::Holders(Box::new(
        HolderFacts {
            local: node(9),
            realm_id: REALM,
            held,
            avoided,
        },
    ))))
}

fn reads(holders: &[u8]) -> Effect {
    let config = DocumentTarget::RealmConfig { realm_id: REALM };
    let mut reads: Vec<_> = (holders.iter().chain([9u8].iter()))
        .map(|seed| {
            let key = Key::from(node_info_key(node(*seed)));
            (NODE_INFO_KEYSPACE.to_string(), key)
        })
        .collect();
    reads.push((config.storage_keyspace().to_string(), config.storage_key()));
    Effect::Storage(StorageEffect::BatchRead {
        reads,
        txn_id: None,
    })
}

fn rows(documents: Vec<Option<NodeInfoDocument>>) -> Event {
    let mut values: Vec<_> = (documents.into_iter())
        .map(|document| {
            let value = document.map(|document| Value::from(document.to_bytes().unwrap()));
            (Key::from(Vec::new()), value)
        })
        .collect();
    values.push((Key::from(Vec::new()), None));
    Event::Storage(StorageEvent::BatchReadResult { values })
}

fn facts_effect(holders: &[u8]) -> Effect {
    Effect::Net(NetEffect::Heartbeat(Box::new(HeartbeatEffect::Holders {
        holders: holders.iter().map(|seed| node(*seed)).collect(),
        now_ms: 5,
    })))
}

fn chooser(holders: &[u8]) -> HolderChoiceOperation {
    let holders: Vec<_> = holders.iter().map(|seed| node(*seed)).collect();
    HolderChoiceOperation::new(&holders, 5)
}

#[test]
fn fresh_holders_qualify() {
    // Holder 1 is fresh, 2 is avoided, 3 has no heartbeat and 4 no document.
    let mut operation = chooser(&[1, 2, 3, 4]);
    let held = vec![held(1, 1_000, slots(6, 0)), held(2, 1_000, slots(6, 0))];
    let documents = vec![
        Some(document(1)),
        Some(document(2)),
        Some(document(3)),
        None,
        None,
    ];
    check_trace(
        &mut operation,
        &[facts_effect(&[1, 2, 3, 4])],
        vec![
            (facts(held, vec![node(2)]), vec![reads(&[1, 2, 3, 4])]),
            (rows(documents), vec![]),
        ],
    );
    let chosen = operation.finalize().unwrap();
    assert_eq!(
        chosen
            .iter()
            .map(|chosen| chosen.node_id)
            .collect::<Vec<_>>(),
        vec![node(1)]
    );
}

#[test]
fn failures_leave_none() {
    // No network, an unreadable advertisement or a failed read leave no candidate.
    let mut offline = chooser(&[1]);
    offline.start();
    assert!(
        offline
            .step(Event::Net(NetEvent::Error(NetError::ChannelClosed)))
            .is_empty()
    );
    assert_eq!(offline.finalize(), Ok(Vec::new()));
    let mut garbled = chooser(&[1]);
    garbled.start();
    garbled.step(facts(vec![held(1, 1_000, slots(6, 0))], Vec::new()));
    let values = vec![
        (Key::from(Vec::new()), Some(Value::from(vec![0xff; 3]))),
        (Key::from(Vec::new()), None),
        (Key::from(Vec::new()), None),
    ];
    garbled.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    assert_eq!(garbled.finalize(), Ok(Vec::new()));
    let mut failed = chooser(&[1]);
    failed.start();
    failed.step(facts(Vec::new(), Vec::new()));
    failed.step(Event::Storage(StorageEvent::Error {
        error: crate::errors::StorageError::Timeout,
    }));
    assert_eq!(failed.finalize(), Ok(Vec::new()));
}

#[test]
fn rejects_stray_events() {
    let started = || {
        let mut operation = chooser(&[1]);
        operation.start();
        operation
    };
    let expected = |event: &Event| matches!(event, Event::Net(_));
    for (label, effects, error) in rejects_events(started, sample_events, expected) {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert!(
            matches!(error, HolderChoiceError::UnexpectedEvent(_)),
            "{label}"
        );
    }
    assert_eq!(
        rejects_events(|| chooser(&[1]), sample_events, |_| false).len(),
        sample_events().len()
    );
}

#[test]
fn abort_and_finalize() {
    let mut operation = chooser(&[1]);
    check_abort(&mut operation, &[]);
    operation.start();
    check_abort(&mut operation, &[]);
    rejects_finalize(operation);
}
