//! Tests choosing the scan subject and mode: placed, draining, removed and unknown nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::ChildOutcome;
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

fn read(value: Option<Value>) -> IoResult {
    IoResult::Values(vec![value])
}

/// The stored realm config, with this node placed as `entry` or not at all.
fn config(entry: Option<NodePlacementEntry>) -> IoResult {
    let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 2);
    config.placement_map.extend(entry);
    let (config, _) = realm_view(&config, admin_user(realm_id()));
    read(Some(config))
}

fn config_read() -> SyncTurn {
    let row = (
        REALM_CONFIG_KEYSPACE.to_string(),
        Key::from(realm_id().as_bytes().to_vec()),
    );
    Ok(Next::Call(IoCall::Read(vec![row])))
}

fn sync(trigger: SubjectTrigger) -> SubjectSyncOperation {
    SubjectSyncOperation::new(realm_id(), node(), trigger, 1_000)
}

/// Checks that the operation runs the scan child under `observed` in `mode`.
fn scans(
    operation: &SubjectSyncOperation,
    turn: SyncTurn,
    observed: PlacementSubject,
    mode: SubjectScanMode,
) {
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let config = SubjectScanConfig {
        realm_id: realm_id(),
        observed,
        mode,
        now_ms: 1_000,
    };
    assert_eq!(operation.state, SyncState::Scan(config));
}

#[test]
fn placed_node_revalidates() {
    // An observed change of a placed node rescans under its new subject.
    let mode = SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition);
    let mut operation = sync(SubjectTrigger::Observe);
    assert_eq!(operation.first(), config_read());
    let turn = operation.next(config(Some(entry(false))));
    scans(&operation, turn, storage_subject(&entry(false), 1), mode);
}

#[test]
fn draining_node_departs() {
    // A draining node departs, and the scan's answer finishes the operation.
    let mut operation = sync(SubjectTrigger::Observe);
    operation.first().unwrap();
    let turn = operation.next(config(Some(entry(true))));
    let subject = storage_subject(&entry(true), 1);
    scans(&operation, turn, subject, SubjectScanMode::Depart);
    let result = SubjectScanResult {
        generation: 1,
        ..SubjectScanResult::default()
    };
    let scanned = IoResult::Child(ChildOutcome::new(Ok::<_, SubjectScanError>(result.clone())));
    assert_eq!(
        operation.next(scanned),
        Ok(Next::Done(Some((SubjectScanMode::Depart, result))))
    );
}

#[test]
fn removed_node_departs() {
    // A node the realm no longer places departs under the subject it advertised.
    let advertised = NodeSubjectRecord::seed(storage_subject(&entry(false), 4)).unwrap();
    let mut operation = sync(SubjectTrigger::Observe);
    operation.first().unwrap();
    assert_eq!(
        operation.next(config(None)),
        Ok(Next::Call(IoCall::Read(vec![local_row()])))
    );
    let turn = operation.next(read(Some(advertised.to_bytes().unwrap().into())));
    scans(
        &operation,
        turn,
        advertised.subject,
        SubjectScanMode::Depart,
    );
}

#[test]
fn nothing_to_scan() {
    // No config, no entry on reconcile, or no advertised subject: nothing is scanned.
    let mode = SubjectScanMode::Revalidate(ManagedCopyQuarantine::Rejoin);
    let mut operation = sync(SubjectTrigger::Reconcile(mode));
    operation.first().unwrap();
    assert_eq!(operation.next(read(None)), Ok(Next::Done(None)));

    let mut operation = sync(SubjectTrigger::Reconcile(mode));
    operation.first().unwrap();
    assert_eq!(operation.next(config(None)), Ok(Next::Done(None)));

    let mut operation = sync(SubjectTrigger::Observe);
    operation.first().unwrap();
    operation.next(config(None)).unwrap();
    assert_eq!(operation.next(read(None)), Ok(Next::Done(None)));
}

#[test]
fn reconcile_keeps_mode() {
    // A reconcile scans a placed node in the mode it was asked for.
    let mode = SubjectScanMode::Revalidate(ManagedCopyQuarantine::Rejoin);
    let mut operation = sync(SubjectTrigger::Reconcile(mode));
    operation.first().unwrap();
    let turn = operation.next(config(Some(entry(true))));
    scans(&operation, turn, storage_subject(&entry(true), 1), mode);
}

#[test]
fn read_failures_fail() {
    // An answer that is not a read, an undecodable row or a failed scan fails the operation.
    let mut operation = sync(SubjectTrigger::Observe);
    operation.first().unwrap();
    assert!(matches!(
        operation.next(IoResult::Done),
        Err(SubjectScanError::Io(_))
    ));
    let mut operation = sync(SubjectTrigger::Observe);
    operation.first().unwrap();
    operation.next(config(None)).unwrap();
    assert!(matches!(
        operation.next(read(Some(vec![0xff; 3].into()))),
        Err(SubjectScanError::Conversion(_))
    ));
    let mut operation = sync(SubjectTrigger::Observe);
    operation.first().unwrap();
    operation.next(config(Some(entry(false)))).unwrap();
    let failed = SubjectScanError::InvalidEvent("Clear");
    let scanned = IoResult::Child(ChildOutcome::new(Err::<SubjectScanResult, _>(failed)));
    assert_eq!(
        operation.next(scanned),
        Err(SubjectScanError::InvalidEvent("Clear"))
    );
}

#[test]
fn reads_local_subject() {
    // One read of the advertised record; the answer is that record, or none.
    let advertised = NodeSubjectRecord::seed(storage_subject(&entry(false), 2)).unwrap();
    let mut operation = LocalSubjectOperation::new();
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::Read(vec![local_row()])))
    );
    let stored = IoResult::Values(vec![Some(advertised.to_bytes().unwrap().into())]);
    assert_eq!(operation.next(stored), Ok(Next::Done(Some(advertised))));
    let mut operation = LocalSubjectOperation::new();
    operation.first().unwrap();
    assert_eq!(
        operation.next(IoResult::Values(vec![None])),
        Ok(Next::Done(None))
    );
}

#[test]
fn local_read_failures() {
    // An undecodable row or an answer that is not a read fails the read.
    let mut operation = LocalSubjectOperation::new();
    operation.first().unwrap();
    let broken = IoResult::Values(vec![Some(Value::from(vec![0xff; 3]))]);
    assert!(matches!(
        operation.next(broken),
        Err(SubjectScanError::Conversion(_))
    ));
    let mut operation = LocalSubjectOperation::new();
    operation.first().unwrap();
    assert_eq!(
        operation.next(IoResult::Done),
        Err(SubjectScanError::InvalidEvent("ReadLocalSubject"))
    );
}
