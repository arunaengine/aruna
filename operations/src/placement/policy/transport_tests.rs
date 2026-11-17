//! Tests serving a policy to a peer: peer check, hash and realm check, and refused events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::IoError;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::placement::policy::{PlacementPolicy, PlacementSelector};
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

fn values(value: Option<Value>) -> IoResult {
    IoResult::Values(vec![value])
}

fn config_read() -> Next<PolicyAnswer> {
    let config = DocumentTarget::RealmConfig {
        realm_id: realm_id(),
    };
    Next::Call(IoCall::Read(vec![read_row(&config)]))
}

fn policy_read(policy: &VerifiedPolicy) -> Next<PolicyAnswer> {
    let target = placement_policy_target(policy.policy_ref().policy_id);
    let row = (target.storage_keyspace().to_string(), target.storage_key());
    Next::Call(IoCall::Read(vec![row]))
}

fn stored_config() -> IoResult {
    let (config, _) = realm_view(&config(), admin_user(realm_id()));
    values(Some(config))
}

#[test]
fn serves_stored_policy() {
    // A server peer gets the stored, authentic document for the exact ref.
    let policy = policy(8);
    let document = signed_document(realm_id(), &policy, 1);
    let row = Value::from(document.to_bytes().unwrap());
    let mut operation = ServePolicyOperation::new(realm_id(), node(1), policy.policy_ref());
    assert_eq!(operation.first(), Ok(config_read()));
    assert_eq!(operation.next(stored_config()), Ok(policy_read(&policy)));
    let answer = operation.next(values(Some(row)));
    assert_eq!(answer, Ok(Next::Done(Ok(Some(Box::new(document))))));
}

#[test]
fn refuses_device_peer() {
    // A user device never gets a policy, and no policy row is read for it.
    let policy = policy(8);
    let mut operation = ServePolicyOperation::new(realm_id(), node(2), policy.policy_ref());
    operation.first().unwrap();
    let answer = operation.next(stored_config());
    assert_eq!(answer, Ok(Next::Done(Err(MetadataReadError::Forbidden))));
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
        operation.first().unwrap();
        operation.next(stored_config()).unwrap();
        assert_eq!(operation.next(values(row)), Ok(Next::Done(Ok(None))));
    }
}

#[test]
fn read_failures_unavailable() {
    // A missing config, an undecodable row, a wrong answer or a runner failure is unavailable.
    let policy = policy(8);
    let unavailable = Ok(Next::Done(Err(MetadataReadError::Unavailable)));
    let mut operation = ServePolicyOperation::new(realm_id(), node(1), policy.policy_ref());
    operation.first().unwrap();
    assert_eq!(operation.next(values(None)), unavailable);
    let mut operation = ServePolicyOperation::new(realm_id(), node(1), policy.policy_ref());
    operation.first().unwrap();
    operation.next(stored_config()).unwrap();
    assert_eq!(
        operation.next(values(Some(Value::from(vec![0xff; 4])))),
        unavailable
    );
    let mut operation = ServePolicyOperation::new(realm_id(), node(1), policy.policy_ref());
    operation.first().unwrap();
    assert_eq!(
        operation.next(IoResult::Done),
        Err(MetadataReadError::Unavailable)
    );
    let error = MetadataReadError::from(IoError::Storage(StorageError::Timeout));
    assert_eq!(error, MetadataReadError::Unavailable);
    assert!(!ServePolicyOperation::is_conflict(&error));
}
