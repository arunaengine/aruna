//! Tests serving a policy to a peer: peer check, hash and realm check, and refused events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::StorageError;
use aruna_core::operation_check::{check_trace, rejects_events, rejects_finalize, sample_events};
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::placement::policy::{PlacementPolicy, PlacementSelector};
use aruna_core::types::Key;
use ulid::Ulid;

use super::*;
use crate::tests::policy::{admin_user, realm_view, signed_document};

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm_id() -> RealmId {
    RealmId::from_bytes([3u8; 32])
}

/// Node 1 is a server, node 2 a user device.
fn config() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 2);
    config.ensure_node(node(1), RealmNodeKind::Server);
    let owner = admin_user(realm_id());
    config.ensure_node(node(2), RealmNodeKind::User { owner });
    config
}

fn policy(seed: u8) -> VerifiedPolicy {
    let selector = PlacementSelector {
        node_id: None,
        location: Some("eu-west".to_string()),
        labels: Vec::new(),
        executor_kind: None,
    };
    let policy = PlacementPolicy::new(
        Ulid::from_bytes([seed; 16]),
        "eu".to_string(),
        vec![selector],
    )
    .expect("policy is valid");
    VerifiedPolicy::verify(policy).expect("policy verifies")
}

fn read(value: Option<Value>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: Key::from(Vec::new()),
        value,
    })
}

fn config_read() -> Effect {
    read_effect(
        &DocumentTarget::RealmConfig {
            realm_id: realm_id(),
        },
        None,
    )
}

fn policy_read(policy: &VerifiedPolicy) -> Effect {
    let target = placement_policy_target(policy.policy_ref().policy_id);
    Effect::Storage(StorageEffect::Read {
        key_space: target.storage_keyspace().to_string(),
        key: target.storage_key(),
        txn_id: None,
    })
}

fn stored_config() -> Event {
    let (config, _) = realm_view(&config(), admin_user(realm_id()));
    read(Some(config))
}

#[test]
fn serves_stored_policy() {
    // A server peer gets the stored, authentic document for the exact ref.
    let policy = policy(8);
    let document = signed_document(realm_id(), &policy, 1);
    let row = Value::from(document.to_bytes().unwrap());
    let mut operation = ServePolicyOperation::new(realm_id(), node(1), policy.policy_ref());
    check_trace(
        &mut operation,
        &[config_read()],
        vec![
            (stored_config(), vec![policy_read(&policy)]),
            (read(Some(row)), vec![]),
        ],
    );
    assert_eq!(operation.finalize(), Ok(Ok(Some(Box::new(document)))));
}

#[test]
fn refuses_device_peer() {
    // A user device never gets a policy, and no policy row is read for it.
    let policy = policy(8);
    let mut operation = ServePolicyOperation::new(realm_id(), node(2), policy.policy_ref());
    check_trace(
        &mut operation,
        &[config_read()],
        vec![(stored_config(), vec![])],
    );
    assert_eq!(operation.finalize(), Ok(Err(MetadataReadError::Forbidden)));
}

#[test]
fn other_rows_none() {
    // A missing row, a row of another policy and a row of another realm answer none.
    let asked = policy(8);
    let other = signed_document(realm_id(), &policy(9), 1);
    let foreign = signed_document(RealmId::from_bytes([4u8; 32]), &asked, 1);
    for row in [None, Some(other), Some(foreign)] {
        let row = row.map(|document| Value::from(document.to_bytes().unwrap()));
        let mut operation = ServePolicyOperation::new(realm_id(), node(1), asked.policy_ref());
        operation.start();
        operation.step(stored_config());
        operation.step(read(row));
        assert_eq!(operation.finalize(), Ok(Ok(None)));
    }
}

#[test]
fn read_failures_unavailable() {
    // A missing config, a storage error and an undecodable row answer unavailable.
    let policy = policy(8);
    let error = || {
        Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })
    };
    let mut operation = ServePolicyOperation::new(realm_id(), node(1), policy.policy_ref());
    operation.start();
    operation.step(read(None));
    assert_eq!(
        operation.finalize(),
        Ok(Err(MetadataReadError::Unavailable))
    );
    let mut operation = ServePolicyOperation::new(realm_id(), node(1), policy.policy_ref());
    operation.start();
    operation.step(error());
    assert_eq!(
        operation.finalize(),
        Ok(Err(MetadataReadError::Unavailable))
    );
    let mut operation = ServePolicyOperation::new(realm_id(), node(1), policy.policy_ref());
    operation.start();
    operation.step(stored_config());
    operation.step(read(Some(Value::from(vec![0xff; 4]))));
    assert_eq!(
        operation.finalize(),
        Ok(Err(MetadataReadError::Unavailable))
    );
}

#[test]
fn rejects_unexpected_events() {
    // Before start every event fails the operation, without effects.
    let make = || ServePolicyOperation::new(realm_id(), node(1), policy(8).policy_ref());
    for (label, effects, error) in rejects_events(make, sample_events, |_| false) {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert_eq!(error, MetadataReadError::Unavailable, "{label}");
    }
}

#[test]
fn abort_and_finalize() {
    // Serving only reads: abort has nothing to clean up, and an early finalize fails.
    let mut operation = ServePolicyOperation::new(realm_id(), node(1), policy(8).policy_ref());
    operation.start();
    assert!(operation.abort().is_empty());
    rejects_finalize(operation);
}
