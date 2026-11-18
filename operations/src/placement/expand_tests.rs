//! Tests activation and expansion decisions as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementStrategy;

fn realm() -> RealmId {
    RealmId::from_bytes([8; 32])
}

fn actor() -> Actor {
    Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        user_id: UserId::nil(realm()),
        realm_id: realm(),
    }
}

fn config() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.strategies = vec![PlacementStrategy {
        strategy_id: Ulid::from_bytes([9; 16]),
        name: "default".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 4,
    }];
    config.ensure_node(actor().node_id, RealmNodeKind::Server);
    config
}

fn read_answer(config: RealmConfigDocument) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(config)))
}

fn mutation_answer(config: RealmConfigDocument) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, MutatePlacementError>(config)))
}

#[test]
fn missing_config_reported() {
    let mut operation = EnsureActivatedOperation::new(&actor());
    operation.first().unwrap();
    let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    assert_eq!(
        operation.next(IoResult::Child(ChildOutcome::new(missing))),
        Err(MutatePlacementError::ConfigMissing)
    );
}

#[test]
fn first_map_published() {
    let mut operation = EnsureActivatedOperation::new(&actor());
    operation.first().unwrap();
    assert!(matches!(
        operation.next(read_answer(config())),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.stage, ActivateStage::Publish);
    assert_eq!(operation.epoch, 1);
}

#[test]
fn published_map_initializes() {
    let mut operation = EnsureActivatedOperation::new(&actor());
    operation.first().unwrap();
    operation.next(read_answer(config())).unwrap();
    assert!(matches!(
        operation.next(mutation_answer(config())),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.stage, ActivateStage::Initialize);
    let done = operation.next(mutation_answer(config()));
    assert_eq!(done, Ok(Next::Done(config())));
}

#[test]
fn lost_start_race_continues() {
    let mut operation = ExpandPlacementOperation::new(&actor());
    operation.config = Some(config());
    let lost = Ulid::from_bytes([3; 16]);
    operation.stage = ExpandStage::Start(lost);
    let raced = Err::<RealmConfigDocument, _>(MutatePlacementError::TransitionInFlight {
        transition_id: lost,
    });
    assert_eq!(
        operation.next(IoResult::Child(ChildOutcome::new(raced))),
        Ok(Next::Done(Vec::new()))
    );
}
