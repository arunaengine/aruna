use super::pure_tests::{make_manifest, make_reference_manifest};
use super::*;
use crate::placement::policy::PolicyCacheEntry;
use aruna_core::keyspaces::MANAGED_COPY_KEYSPACE;
use aruna_core::structs::placement::placement_policy::{
    PlacementPolicy, PlacementSelector, PlacementSubject, VerifiedPolicy,
};
use aruna_core::structs::storage::replication::ReplicationItemKind;
use std::collections::BTreeMap;

fn realm() -> RealmId {
    RealmId::from_bytes([3u8; 32])
}

fn policy(location: &str) -> VerifiedPolicy {
    let policy = PlacementPolicy::new(
        Ulid::from_bytes([1u8; 16]),
        "residency".to_string(),
        vec![PlacementSelector {
            node_id: None,
            location: Some(location.to_string()),
            labels: Vec::new(),
            executor_kind: None,
        }],
    )
    .expect("policy is valid");
    VerifiedPolicy::verify(policy).expect("policy verifies")
}

fn gate(location: &str) -> GateContext {
    GateContext {
        realm_id: realm(),
        subject: PlacementSubject {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            generation: 1,
            location: location.to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        },
        now_ms: 1_000,
        admitting: true,
    }
}

fn governed(rule: &VerifiedPolicy) -> IncomingVersionOperation {
    let mut manifest = make_manifest(ReplicationItemKind::Materialized);
    manifest.placement_policies = vec![rule.policy_ref()];
    IncomingVersionOperation::new(
        Ulid::from_parts(1, 1),
        iroh::SecretKey::from_bytes(&[65; 32]).public(),
        realm(),
        manifest,
    )
}

fn rejected(operation: &IncomingVersionOperation) -> bool {
    matches!(
        operation.negotiation_result,
        Some(ReplicationNegotiationResult::Rejected(_))
    )
}

#[test]
fn denies_incoming_replica() {
    // The manifest's rule admits another location, so the negotiation that
    // would invite bytes is refused instead.
    let rule = policy("us-east");
    let mut operation = governed(&rule).with_gate(gate("eu-west"));
    operation.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion);

    let document = crate::tests::policy::signed_document(realm(), &rule, 9);
    let cached = PolicyCacheEntry::verified(&document, 10)
        .to_bytes()
        .expect("entry encodes");
    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: Some(cached.into()),
    }));
    operation.step(crate::tests::policy::authority(realm()));

    assert!(rejected(&operation));
}

#[test]
fn missing_subject_refuses() {
    // A node that advertises no subject may hold nothing governed, so it
    // never invites the bytes.
    let rule = policy("eu-west");
    let mut operation = governed(&rule);
    operation.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion);
    assert!(rejected(&operation));
}

#[test]
fn reference_registers_nothing() {
    // A reference materializes no bytes here, so no managed copy may claim
    // this node holds one.
    let manifest = make_reference_manifest();
    let mut operation = IncomingVersionOperation::new(
        Ulid::from_parts(2, 2),
        iroh::SecretKey::from_bytes(&[66; 32]).public(),
        realm(),
        manifest,
    );
    operation.txn_id = Some(Ulid::from_parts(3, 3));
    operation.destination_group_id = Some(Ulid::from_parts(4, 4));

    operation.write_version();
    assert!(
        operation
            .pending_version_effects
            .iter()
            .all(|effect| !matches!(
                effect,
                Effect::Storage(StorageEffect::Write { key_space, .. })
                    if key_space == MANAGED_COPY_KEYSPACE
            ))
    );
}
