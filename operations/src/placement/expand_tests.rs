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
fn lost_race_continues() {
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

fn other_node() -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

fn activated() -> RealmConfigDocument {
    let mut config = config();
    config.snapshot_candidate_map();
    config
}

/// A realm whose newest map does not list the node that joined after it.
fn joined() -> RealmConfigDocument {
    let mut config = activated();
    config.ensure_node(other_node(), RealmNodeKind::Server);
    config
}

/// `joined` after its view was frozen into the next map.
fn published() -> RealmConfigDocument {
    let mut config = joined();
    config.candidate_maps.push(config.freeze_map(2));
    config
}

fn expanding() -> ExpandPlacementOperation {
    let mut operation = ExpandPlacementOperation::new(&actor());
    operation.stamp(9);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    operation
}

fn publishing() -> ExpandPlacementOperation {
    let mut operation = expanding();
    assert!(matches!(
        operation.next(mutation_answer(joined())),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.stage, ExpandStage::Publish);
    operation
}

#[test]
fn current_view_reused() {
    assert_eq!(
        expanding().next(mutation_answer(activated())),
        Ok(Next::Done(Vec::new()))
    );
}

#[test]
fn growth_starts_transition() {
    let mut operation = publishing();
    assert!(matches!(
        operation.next(mutation_answer(published())),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let ExpandStage::Start(started) = operation.stage else {
        panic!("expected a started transition");
    };
    assert_eq!(
        operation.next(mutation_answer(published())),
        Ok(Next::Done(vec![started]))
    );
}

#[test]
fn running_transition_defers() {
    use aruna_core::structs::placement::transition::{
        BucketPlan, PlacementTransition, TransitionLimits, TransitionPlan,
    };
    let mut config = published();
    let strategy_id = config.strategies[0].strategy_id;
    config
        .placement_transitions
        .push(PlacementTransition::new(TransitionPlan {
            transition_id: Ulid::from_bytes([4; 16]),
            strategy_id,
            buckets: vec![BucketPlan {
                bucket: 0,
                old_holders: vec![actor().node_id],
                target_holders: vec![actor().node_id, other_node()],
                predecessor_epoch: 1,
            }],
            target_map_epoch: 2,
            limits: TransitionLimits::default(),
            created_by: actor().node_id,
            created_at_ms: 1,
        }));
    assert_eq!(
        publishing().next(mutation_answer(config)),
        Ok(Next::Done(Vec::new()))
    );
}

#[test]
fn failed_start_reported() {
    let mut operation = publishing();
    operation.next(mutation_answer(published())).unwrap();
    let failed = Err::<RealmConfigDocument, _>(MutatePlacementError::ConfigMissing);
    assert_eq!(
        operation.next(IoResult::Child(ChildOutcome::new(failed))),
        Err(MutatePlacementError::ConfigMissing)
    );
}
