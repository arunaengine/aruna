//! Tests choosing the scan subject and mode: placed, draining, removed and unknown nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::errors::StorageError;
use aruna_core::operation_check::{check_trace, rejects_events, rejects_finalize, sample_events};
use aruna_core::structs::placement::record::NodePlacementEntry;

use super::*;
use crate::tests::policy::{admin_user, realm_view};

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[5u8; 32]).public()
}

fn realm_id() -> RealmId {
    RealmId::from_bytes([3u8; 32])
}

fn entry(draining: bool) -> NodePlacementEntry {
    NodePlacementEntry {
        node_id: node(),
        location: "eu-west".to_string(),
        weight: 1,
        full: false,
        draining,
        labels: BTreeMap::new(),
    }
}

fn read(value: Option<Value>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: Key::from(Vec::new()),
        value,
    })
}

/// The stored realm config, with this node placed as `entry` or not at all.
fn config(entry: Option<NodePlacementEntry>) -> Event {
    let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 2);
    config.placement_map.extend(entry);
    let (config, _) = realm_view(&config, admin_user(realm_id()));
    read(Some(config))
}

fn config_read() -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: REALM_CONFIG_KEYSPACE.to_string(),
        key: realm_id().as_bytes().to_vec().into(),
        txn_id: None,
    })
}

fn sync(trigger: SubjectTrigger) -> SubjectSyncOperation {
    SubjectSyncOperation::new(realm_id(), node(), trigger, 1_000)
}

/// The scan the operation starts as its child, as its first effects.
fn scan_start(observed: PlacementSubject, mode: SubjectScanMode) -> Vec<Effect> {
    let mut scan = SubjectScanOperation::new(SubjectScanConfig {
        realm_id: realm_id(),
        observed,
        mode,
        now_ms: 1_000,
    });
    scan.start().into_vec()
}

#[test]
fn placed_node_revalidates() {
    // An observed change of a placed node rescans under its new subject.
    let mode = SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition);
    let mut operation = sync(SubjectTrigger::Observe);
    check_trace(
        &mut operation,
        &[config_read()],
        vec![(
            config(Some(entry(false))),
            scan_start(storage_subject(&entry(false), 1), mode),
        )],
    );
    assert!(!operation.is_complete());
}

#[test]
fn draining_node_departs() {
    // A draining node departs, and the scan's own answers finish the operation.
    let mut operation = sync(SubjectTrigger::Observe);
    operation.start();
    let effects = operation.step(config(Some(entry(true))));
    let subject = storage_subject(&entry(true), 1);
    assert_eq!(
        effects.into_vec(),
        scan_start(subject.clone(), SubjectScanMode::Depart)
    );
    // Same subject as stored and nothing blocked: the scan ends without reading a copy.
    let stored = NodeSubjectRecord::seed(subject).expect("subject is valid");
    let effects = operation.step(read(Some(stored.to_bytes().unwrap().into())));
    assert!(effects.is_empty());
    let (mode, result) = operation.finalize().unwrap().expect("a scan ran");
    assert_eq!(mode, SubjectScanMode::Depart);
    assert_eq!(result.generation, 1);
}

#[test]
fn removed_node_departs() {
    // A node the realm no longer places departs under the subject it advertised.
    let advertised = NodeSubjectRecord::seed(storage_subject(&entry(false), 4)).unwrap();
    let mut operation = sync(SubjectTrigger::Observe);
    operation.start();
    let effects = operation.step(config(None));
    assert_eq!(effects.into_vec(), vec![local_read()]);
    let effects = operation.step(read(Some(advertised.to_bytes().unwrap().into())));
    assert_eq!(
        effects.into_vec(),
        scan_start(advertised.subject, SubjectScanMode::Depart)
    );
}

#[test]
fn nothing_to_scan() {
    // No config, no entry on reconcile, or no advertised subject: nothing is scanned.
    let mode = SubjectScanMode::Revalidate(ManagedCopyQuarantine::Rejoin);
    let mut operation = sync(SubjectTrigger::Reconcile(mode));
    operation.start();
    assert!(operation.step(read(None)).is_empty());
    assert_eq!(operation.finalize(), Ok(None));

    let mut operation = sync(SubjectTrigger::Reconcile(mode));
    operation.start();
    assert!(operation.step(config(None)).is_empty());
    assert_eq!(operation.finalize(), Ok(None));

    let mut operation = sync(SubjectTrigger::Observe);
    operation.start();
    operation.step(config(None));
    assert!(operation.step(read(None)).is_empty());
    assert_eq!(operation.finalize(), Ok(None));
}

#[test]
fn reconcile_keeps_mode() {
    // A reconcile scans a placed node in the mode it was asked for.
    let mode = SubjectScanMode::Revalidate(ManagedCopyQuarantine::Rejoin);
    let mut operation = sync(SubjectTrigger::Reconcile(mode));
    operation.start();
    let effects = operation.step(config(Some(entry(true))));
    assert_eq!(
        effects.into_vec(),
        scan_start(storage_subject(&entry(true), 1), mode)
    );
}

#[test]
fn read_failures_fail() {
    // A storage error or an undecodable row fails the operation without effects.
    let error = Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    });
    let mut operation = sync(SubjectTrigger::Observe);
    operation.start();
    assert!(operation.step(error).is_empty());
    assert_eq!(
        operation.finalize(),
        Err(SubjectScanError::InvalidEvent("ReadRealmConfig"))
    );
    let mut operation = sync(SubjectTrigger::Observe);
    operation.start();
    operation.step(config(None));
    assert!(operation.step(read(Some(vec![0xff; 3].into()))).is_empty());
    assert!(matches!(
        operation.finalize(),
        Err(SubjectScanError::Conversion(_))
    ));
}

#[test]
fn rejects_unexpected_events() {
    // Before start every event fails both operations, without effects.
    let make = || sync(SubjectTrigger::Observe);
    for (label, effects, error) in rejects_events(make, sample_events, |_| false) {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert_eq!(
            error,
            SubjectScanError::InvalidEvent("SubjectSync"),
            "{label}"
        );
    }
    for (label, effects, error) in
        rejects_events(LocalSubjectOperation::new, sample_events, |_| false)
    {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert_eq!(
            error,
            SubjectScanError::InvalidEvent("ReadLocalSubject"),
            "{label}"
        );
    }
}

#[test]
fn reads_local_subject() {
    // The local read answers the advertised record, or none.
    let advertised = NodeSubjectRecord::seed(storage_subject(&entry(false), 2)).unwrap();
    let mut operation = LocalSubjectOperation::new();
    check_trace(
        &mut operation,
        &[local_read()],
        vec![(read(Some(advertised.to_bytes().unwrap().into())), vec![])],
    );
    assert_eq!(operation.finalize(), Ok(Some(advertised)));
    let mut operation = LocalSubjectOperation::new();
    operation.start();
    operation.step(read(None));
    assert_eq!(operation.finalize(), Ok(None));
}

#[test]
fn abort_and_finalize() {
    // Choosing the subject only reads: abort has nothing to clean up, and an early finalize fails.
    let mut operation = sync(SubjectTrigger::Observe);
    operation.start();
    assert!(operation.abort().is_empty());
    rejects_finalize(operation);
    let mut operation = LocalSubjectOperation::new();
    operation.start();
    assert!(operation.abort().is_empty());
    rejects_finalize(operation);
}
