//! Tests the checks in front of a served shard manifest as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmNodeKind;
use ulid::Ulid;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn placement() -> PlacementRef {
    PlacementRef {
        strategy_id: Ulid::from_bytes([9; 16]),
        shard: 0,
    }
}

fn request(realm_id: RealmId) -> ShardTransportMessage {
    ShardTransportMessage::ManifestRequest {
        realm_id,
        placement: placement(),
    }
}

fn config_answer(config: RealmConfigDocument) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(config)))
}

fn rejected(turn: Result<Next<ManifestAnswer>, IoError>) -> String {
    match turn {
        Ok(Next::Done(ManifestAnswer::Reject(reason))) => reason,
        other => panic!("expected a rejection, got {other:?}"),
    }
}

#[test]
fn foreign_realm_rejected() {
    let foreign = RealmId::from_bytes([2; 32]);
    let mut operation = ServeManifestOperation::new((node(1), realm()), node(2), request(foreign));
    assert!(rejected(operation.first()).contains("foreign realm"));
}

fn asked(peer: NodeId) -> ServeManifestOperation {
    let mut operation = ServeManifestOperation::new((node(1), realm()), peer, request(realm()));
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    operation
}

#[test]
fn missing_config_rejected() {
    let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    let turn = asked(node(2)).next(IoResult::Child(ChildOutcome::new(missing)));
    assert!(rejected(turn).contains("config unavailable"));
}

#[test]
fn unknown_peer_rejected() {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.ensure_node(node(1), RealmNodeKind::Server);
    let turn = asked(node(2)).next(config_answer(config));
    assert!(rejected(turn).contains("not a sync-eligible"));
}

#[test]
fn assembly_failure_rejected() {
    let mut operation = asked(node(2));
    operation.assembling = true;
    let failed = Err::<ShardManifest, _>(ManifestError("scan failed".to_string()));
    let turn = operation.next(IoResult::Child(ChildOutcome::new(failed)));
    assert_eq!(rejected(turn), "scan failed");
}

fn holder_config() -> RealmConfigDocument {
    use aruna_core::structs::placement::record::PlacementStrategy;
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.strategies = vec![PlacementStrategy {
        strategy_id: placement().strategy_id,
        name: "default".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 1,
    }];
    config.default_strategy_id = Some(placement().strategy_id);
    config.ensure_node(node(1), RealmNodeKind::Server);
    config.ensure_node(node(2), RealmNodeKind::Server);
    config.snapshot_candidate_map();
    config
}

#[test]
fn outside_holder_rejected() {
    let mut operation = ServeManifestOperation::new((node(9), realm()), node(2), request(realm()));
    operation.first().unwrap();
    let turn = operation.next(config_answer(holder_config()));
    assert!(rejected(turn).contains("does not hold"));
}

#[test]
fn co_holder_served() {
    let mut operation = asked(node(2));
    let turn = operation.next(config_answer(holder_config()));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let manifest = ShardManifest {
        placement: placement(),
        holder: node(1),
        entries: Vec::new(),
        cursor: Vec::new(),
        digest: [1; 32],
        updated_at_ms: 0,
    };
    let answer = IoResult::Child(ChildOutcome::new(Ok::<_, ManifestError>(manifest.clone())));
    assert_eq!(
        operation.next(answer),
        Ok(Next::Done(ManifestAnswer::Manifest(Box::new(manifest))))
    );
}
