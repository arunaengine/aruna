//! Tests shard topic reconciliation as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{called, child, node, runs_child, shard_effect, shard_event};
use aruna_core::effects::Effect;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::placement::record::PlacementStrategy;
use aruna_core::structs::placement::transition::{BucketPlan, PlacementTransition, TransitionPlan};
use aruna_core::task::{TaskEffect, TaskKey};
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([8; 32])
}

fn topic(seed: u8) -> TopicId {
    TopicId::from_bytes([seed; 32])
}

fn strategy_id() -> Ulid {
    Ulid::from_bytes([9; 16])
}

fn placement() -> PlacementRef {
    PlacementRef {
        strategy_id: strategy_id(),
        shard: 0,
    }
}

/// A realm whose one single-bucket strategy is activated over `nodes`.
fn config(nodes: &[NodeId]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    config.strategies = vec![PlacementStrategy {
        strategy_id: strategy_id(),
        name: "default".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 1,
    }];
    config.default_strategy_id = Some(strategy_id());
    for node_id in nodes {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config.snapshot_candidate_map();
    config
}

fn existing(topics: &[TopicId]) -> IoResult {
    shard_event(ShardNetEvent::Existing(topics.iter().copied().collect()))
}

fn applied() -> IoResult {
    shard_event(ShardNetEvent::Outcome(Ok(())))
}

fn refused() -> IoResult {
    shard_event(ShardNetEvent::Outcome(Err("refused".to_string())))
}

fn pulled() -> IoResult {
    shard_event(ShardNetEvent::Pulled { applied: true })
}

fn probed(probe: GenesisProbe) -> IoResult {
    shard_event(ShardNetEvent::Probed(probe))
}

fn resolving(co_members: Vec<NodeId>, may_mint: bool) -> ResolveTopicsOperation {
    let topics = vec![topic(1)];
    let mut operation = ResolveTopicsOperation::new(co_members, topics, may_mint, BTreeSet::new());
    assert_eq!(
        shard_effect(operation.first()),
        ShardNetEffect::Existing(vec![topic(1)])
    );
    operation
}

fn resolved(to_ensure: Vec<TopicId>, withheld: bool) -> ResolvedTopics {
    ResolvedTopics {
        to_ensure,
        withheld,
        unreachable: BTreeSet::new(),
    }
}

/// Runs up to the answer of the probe of `node(2)`.
fn probing() -> ResolveTopicsOperation {
    let mut operation = resolving(vec![node(2)], true);
    assert_eq!(
        shard_effect(operation.next(existing(&[]))),
        ShardNetEffect::Probe {
            topics: vec![topic(1)],
            peers: vec![node(2)],
        }
    );
    operation
}

#[test]
fn known_topics_kept() {
    let mut operation = resolving(vec![node(2)], true);
    assert_eq!(
        operation.next(existing(&[topic(1)])),
        Ok(Next::Done(resolved(vec![topic(1)], false)))
    );
}

#[test]
fn sole_holder_mints() {
    let mut operation = resolving(Vec::new(), true);
    assert_eq!(
        operation.next(existing(&[])),
        Ok(Next::Done(resolved(vec![topic(1)], false)))
    );
}

#[test]
fn sole_joiner_withholds() {
    let mut operation = resolving(Vec::new(), false);
    assert_eq!(
        operation.next(existing(&[])),
        Ok(Next::Done(resolved(Vec::new(), true)))
    );
}

#[test]
fn missing_network_withholds() {
    let mut operation = resolving(vec![node(2)], true);
    let turn = operation.next(shard_event(ShardNetEvent::NoNetwork));
    assert_eq!(turn, Ok(Next::Done(resolved(Vec::new(), true))));
}

#[test]
fn absent_topic_minted() {
    let mut operation = probing();
    assert_eq!(
        operation.next(probed(GenesisProbe::default())),
        Ok(Next::Done(resolved(vec![topic(1)], false)))
    );
}

#[test]
fn unconfirmed_topic_withheld() {
    let mut operation = probing();
    let probe = GenesisProbe {
        unconfirmed: BTreeSet::from([topic(1)]),
        ..GenesisProbe::default()
    };
    assert_eq!(
        operation.next(probed(probe)),
        Ok(Next::Done(resolved(Vec::new(), true)))
    );
}

#[test]
fn unreachable_peer_withholds() {
    let mut operation = probing();
    let probe = GenesisProbe {
        unreachable: vec![node(2)],
        ..GenesisProbe::default()
    };
    let Ok(Next::Done(answer)) = operation.next(probed(probe)) else {
        panic!("expected the resolved topics");
    };
    assert!(answer.withheld);
    assert!(answer.to_ensure.is_empty());
    assert_eq!(answer.unreachable, BTreeSet::from([node(2)]));
}

#[test]
fn dead_peers_unprobed() {
    // A peer already found unreachable in this pass is not asked again and still withholds.
    let dead = BTreeSet::from([node(2)]);
    let mut operation = ResolveTopicsOperation::new(vec![node(2)], vec![topic(1)], true, dead);
    operation.first().unwrap();
    let Ok(Next::Done(answer)) = operation.next(existing(&[])) else {
        panic!("expected no probe");
    };
    assert!(answer.withheld);
    assert!(answer.to_ensure.is_empty());
}

fn adopting() -> ResolveTopicsOperation {
    let mut operation = probing();
    let probe = GenesisProbe {
        known_co_holder: BTreeSet::from([topic(1)]),
        ..GenesisProbe::default()
    };
    assert_eq!(
        shard_effect(operation.next(probed(probe))),
        ShardNetEffect::Pull {
            topics: vec![topic(1)],
            peers: vec![node(2)],
        }
    );
    assert_eq!(
        shard_effect(operation.next(pulled())),
        ShardNetEffect::Existing(vec![topic(1)])
    );
    operation
}

#[test]
fn probe_adopts_known() {
    assert_eq!(
        adopting().next(existing(&[topic(1)])),
        Ok(Next::Done(resolved(vec![topic(1)], false)))
    );
}

#[test]
fn failed_adopt_withheld() {
    // A topic a peer holds is never minted here, even when the pull brought nothing.
    assert_eq!(
        adopting().next(existing(&[])),
        Ok(Next::Done(resolved(Vec::new(), true)))
    );
}

fn genesis(co_members: Vec<NodeId>) -> EnsureGenesisOperation {
    let mut operation = EnsureGenesisOperation::new(
        node(1),
        co_members,
        vec![node(1)],
        vec![topic(1)],
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
    );
    assert!(matches!(
        shard_effect(operation.first()),
        ShardNetEffect::Membership(_)
    ));
    assert!(runs_child(operation.next(applied())));
    operation
}

fn resolved_answer(to_ensure: Vec<TopicId>) -> IoResult {
    child(Ok::<_, IoError>(resolved(to_ensure, false)))
}

#[test]
fn withheld_genesis_reported() {
    let mut operation = genesis(vec![node(2)]);
    let answer = ResolvedTopics {
        to_ensure: Vec::new(),
        withheld: true,
        unreachable: BTreeSet::from([node(3)]),
    };
    assert_eq!(
        operation.next(child(Ok::<_, IoError>(answer))),
        Ok(Next::Done((true, BTreeSet::from([node(3)]))))
    );
}

#[test]
fn lone_genesis_local() {
    let mut operation = genesis(Vec::new());
    assert_eq!(
        shard_effect(operation.next(resolved_answer(vec![topic(1)]))),
        ShardNetEffect::EnsureLocal(vec![topic(1)])
    );
    assert!(matches!(
        shard_effect(operation.next(applied())),
        ShardNetEffect::Membership(_)
    ));
    assert_eq!(
        operation.next(applied()),
        Ok(Next::Done((false, BTreeSet::new())))
    );
}

#[test]
fn shared_genesis_peers() {
    let mut operation = genesis(vec![node(2)]);
    assert_eq!(
        shard_effect(operation.next(resolved_answer(vec![topic(1)]))),
        ShardNetEffect::EnsureShared {
            topics: vec![topic(1)],
            peers: vec![node(2)],
        }
    );
}

#[test]
fn failed_ensure_withheld() {
    let mut operation = genesis(vec![node(2)]);
    operation.next(resolved_answer(vec![topic(1)])).unwrap();
    assert_eq!(
        operation.next(refused()),
        Ok(Next::Done((true, BTreeSet::new())))
    );
}

#[test]
fn failed_members_withheld() {
    let mut operation = genesis(vec![node(2)]);
    operation.next(resolved_answer(vec![topic(1)])).unwrap();
    operation.next(applied()).unwrap();
    assert_eq!(
        operation.next(refused()),
        Ok(Next::Done((true, BTreeSet::new())))
    );
}

fn joining() -> MemberGroupOperation {
    let mut operation = MemberGroupOperation {
        co_members: vec![node(2)],
        members: vec![node(1), node(2)],
        publishers: vec![node(2)],
        topics: vec![topic(1)],
        retained: BTreeSet::new(),
        verified: BTreeSet::new(),
        stage: MemberStage::Policy,
        outcome: HeldTopicOutcome::default(),
    };
    operation.first().unwrap();
    assert_eq!(
        shard_effect(operation.next(applied())),
        ShardNetEffect::Existing(vec![topic(1)])
    );
    operation
}

#[test]
fn known_topic_joined() {
    let mut operation = joining();
    assert!(matches!(
        shard_effect(operation.next(existing(&[topic(1)]))),
        ShardNetEffect::Membership(_)
    ));
    assert_eq!(
        operation.next(applied()),
        Ok(Next::Done(HeldTopicOutcome::default()))
    );
}

#[test]
fn incomplete_members_withheld() {
    let mut operation = joining();
    operation.next(existing(&[topic(1)])).unwrap();
    let outcome = HeldTopicOutcome {
        withheld: true,
        pull_pending: false,
    };
    assert_eq!(operation.next(refused()), Ok(Next::Done(outcome)));
}

#[test]
fn unserved_pull_pending() {
    let mut operation = joining();
    assert!(matches!(
        shard_effect(operation.next(existing(&[]))),
        ShardNetEffect::Pull { .. }
    ));
    operation.next(pulled()).unwrap();
    let outcome = HeldTopicOutcome {
        withheld: false,
        pull_pending: true,
    };
    assert_eq!(operation.next(existing(&[])), Ok(Next::Done(outcome)));
}

#[test]
fn pulled_topic_joined() {
    let mut operation = joining();
    operation.next(existing(&[])).unwrap();
    operation.next(pulled()).unwrap();
    assert!(matches!(
        shard_effect(operation.next(existing(&[topic(1)]))),
        ShardNetEffect::Membership(_)
    ));
}

#[test]
fn sole_holder_leads() {
    let held = held_topics(&config(&[node(1)]), realm(), node(1), 0);
    assert_eq!(held.reopen, vec![shard_topic_id(realm(), &placement())]);
    assert_eq!(held.leading.len(), 1);
    assert!(held.joining.is_empty());
}

#[test]
fn stranger_holds_nothing() {
    assert_eq!(
        held_topics(&config(&[node(1)]), realm(), node(9), 0),
        HeldTopics::default()
    );
}

fn holding() -> HeldTopicsOperation {
    let config = Arc::new(config(&[node(1)]));
    let mut operation = HeldTopicsOperation::new(realm(), node(1), config, BTreeSet::new());
    assert_eq!(
        shard_effect(operation.first()),
        ShardNetEffect::Reopen(vec![shard_topic_id(realm(), &placement())])
    );
    operation
}

#[test]
fn held_group_finishes() {
    let mut operation = holding();
    let reopened = shard_event(ShardNetEvent::Reopened { failed: 0 });
    assert!(runs_child(operation.next(reopened)));
    let answer = (true, BTreeSet::<NodeId>::new());
    let outcome = HeldTopicOutcome {
        withheld: true,
        pull_pending: false,
    };
    assert_eq!(
        operation.next(child(Ok::<_, IoError>(answer))),
        Ok(Next::Done(outcome))
    );
}

#[test]
fn failed_reopen_pending() {
    let mut operation = holding();
    operation.leading.clear();
    let outcome = HeldTopicOutcome {
        withheld: false,
        pull_pending: true,
    };
    assert_eq!(
        operation.next(shard_event(ShardNetEvent::Reopened { failed: 1 })),
        Ok(Next::Done(outcome))
    );
}

fn reconciling(run_transitions: bool) -> ReconcilePlacementsOperation {
    let mut operation = ReconcilePlacementsOperation::new(realm(), node(1), run_transitions);
    operation.stamp(1_000);
    assert!(runs_child(operation.first()));
    operation
}

fn config_answer() -> IoResult {
    child(Ok::<_, GetConfigError>(config(&[node(1)])))
}

/// Runs up to the end of the held topic work, which answers `held`.
fn after_held(run_transitions: bool, held: HeldTopicOutcome) -> ReconcilePlacementsOperation {
    let mut operation = reconciling(run_transitions);
    assert!(matches!(
        shard_effect(operation.next(config_answer())),
        ShardNetEffect::Existing(_)
    ));
    assert!(runs_child(operation.next(existing(&[]))));
    let verified = BTreeSet::<TopicId>::new();
    assert!(runs_child(
        operation.next(child(Ok::<_, IoError>(verified)))
    ));
    assert!(runs_child(operation.next(child(Ok::<_, IoError>(held)))));
    operation
}

fn empty_page() -> IoResult {
    let page: (Vec<(Key, Value)>, Option<Key>) = (Vec::new(), None);
    child(Ok::<_, IoError>(page))
}

fn withheld() -> HeldTopicOutcome {
    HeldTopicOutcome {
        withheld: true,
        pull_pending: false,
    }
}

fn timer_set() -> IoResult {
    let event = Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::DrainSyncOutbox,
        after: Duration::ZERO,
    });
    IoResult::Event(Box::new(event))
}

#[test]
fn missing_config_clean() {
    let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    assert_eq!(
        reconciling(true).next(child(missing)),
        Ok(Next::Done(PlacementReconcileOutcome::clean()))
    );
}

#[test]
fn unreadable_config_fails() {
    let failed = Err::<RealmConfigDocument, _>(GetConfigError::NotFinished);
    assert_eq!(
        reconciling(true).next(child(failed)),
        Ok(Next::Done(PlacementReconcileOutcome::storage_failure()))
    );
}

#[test]
fn no_network_clean() {
    let mut operation = reconciling(true);
    operation.next(config_answer()).unwrap();
    assert_eq!(
        operation.next(shard_event(ShardNetEvent::NoNetwork)),
        Ok(Next::Done(PlacementReconcileOutcome::clean()))
    );
}

#[test]
fn quiet_pass_clean() {
    let mut operation = after_held(false, HeldTopicOutcome::default());
    assert_eq!(operation.stage, ReconcileStage::Page);
    assert_eq!(
        operation.next(empty_page()),
        Ok(Next::Done(PlacementReconcileOutcome::clean()))
    );
}

#[test]
fn request_skips_transitions() {
    assert_eq!(
        after_held(false, HeldTopicOutcome::default()).stage,
        ReconcileStage::Page
    );
    assert_eq!(
        after_held(true, HeldTopicOutcome::default()).stage,
        ReconcileStage::Transitions
    );
}

#[test]
fn failed_page_reported() {
    let failed = Err::<(Vec<(Key, Value)>, Option<Key>), _>(IoError::Conflict);
    assert_eq!(
        after_held(false, HeldTopicOutcome::default()).next(child(failed)),
        Ok(Next::Done(PlacementReconcileOutcome::storage_failure()))
    );
}

#[test]
fn withheld_pass_retries() {
    let mut operation = after_held(false, withheld());
    let IoCall::Effect(effect) = called(operation.next(empty_page())) else {
        panic!("expected the retry timer");
    };
    let Effect::Task(TaskEffect::ResetTimer { after, .. }) = *effect else {
        panic!("expected the retry timer");
    };
    assert_eq!(after, PLACEMENT_RETRY_AFTER);
    assert_eq!(
        operation.next(timer_set()),
        Ok(Next::Done(PlacementReconcileOutcome::retry_scheduled(
            false
        )))
    );
}

#[test]
fn pull_retries_soon() {
    let held = HeldTopicOutcome {
        withheld: false,
        pull_pending: true,
    };
    let mut operation = after_held(false, held);
    let IoCall::Effect(effect) = called(operation.next(empty_page())) else {
        panic!("expected the retry timer");
    };
    let Effect::Task(TaskEffect::ResetTimer { after, .. }) = *effect else {
        panic!("expected the retry timer");
    };
    assert_eq!(after, PULL_RETRY_AFTER);
}

#[test]
fn missing_tasks_clean() {
    let mut operation = after_held(false, withheld());
    operation.next(empty_page()).unwrap();
    let event = Event::Task(TaskEvent::Error {
        key: None,
        message: "task handle unavailable".to_string(),
    });
    assert_eq!(
        operation.next(IoResult::Event(Box::new(event))),
        Ok(Next::Done(PlacementReconcileOutcome::clean()))
    );
}

fn page_with_row() -> IoResult {
    let rows = vec![(Key::from(vec![1]), Value::from(vec![2]))];
    child(Ok::<_, IoError>((rows, None::<Key>)))
}

#[test]
fn record_retry_counts() {
    let mut operation = after_held(false, HeldTopicOutcome::default());
    assert!(runs_child(operation.next(page_with_row())));
    assert!(matches!(
        called(operation.next(child(Ok::<_, IoError>(true)))),
        IoCall::Effect(_)
    ));
    assert_eq!(operation.stage, ReconcileStage::Retry);
}

#[test]
fn failed_record_retries() {
    let mut operation = after_held(false, HeldTopicOutcome::default());
    operation.next(page_with_row()).unwrap();
    let failed = Err::<bool, _>(IoError::Conflict);
    operation.next(child(failed)).unwrap();
    assert_eq!(operation.stage, ReconcileStage::Retry);
}

#[test]
fn next_page_follows() {
    let mut operation = after_held(false, HeldTopicOutcome::default());
    let rows: Vec<(Key, Value)> = Vec::new();
    let next = Some(Key::from(vec![7]));
    assert!(runs_child(
        operation.next(child(Ok::<_, IoError>((rows, next))))
    ));
    assert_eq!(operation.stage, ReconcileStage::Page);
}

fn record_step(value: Value, nodes: &[NodeId], local: NodeId) -> PlacementRecordOperation {
    PlacementRecordOperation {
        realm_id: realm(),
        local,
        config: Arc::new(config(nodes)),
        verified: BTreeSet::new(),
        key: Key::from(vec![1]),
        value,
        now_ms: 0,
        stage: RecordStage::Decide,
        record: None,
    }
}

fn stored_record(realm_id: RealmId, placement: PlacementRef) -> Value {
    let record = new_placement(realm_id, placement, node(1), Vec::new());
    postcard::to_allocvec(&record).unwrap().into()
}

fn current_record() -> Value {
    stored_record(realm(), placement())
}

#[test]
fn malformed_record_deleted() {
    let mut operation = record_step(Value::from(vec![0xff]), &[node(1)], node(1));
    assert!(matches!(called(operation.first()), IoCall::Delete(_)));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(false)));
}

#[test]
fn foreign_record_ignored() {
    let value = stored_record(RealmId::from_bytes([7; 32]), placement());
    let mut operation = record_step(value, &[node(1)], node(1));
    assert_eq!(operation.first(), Ok(Next::Done(false)));
}

#[test]
fn unresolved_record_kept() {
    let unknown = PlacementRef {
        strategy_id: Ulid::from_bytes([1; 16]),
        shard: 0,
    };
    let mut operation = record_step(stored_record(realm(), unknown), &[node(1)], node(1));
    assert!(matches!(called(operation.first()), IoCall::Effect(_)));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
}

#[test]
fn former_holder_forgets() {
    let mut operation = record_step(current_record(), &[node(1)], node(9));
    assert!(matches!(called(operation.first()), IoCall::Delete(_)));
    let IoCall::Delete(rows) = called(operation.next(IoResult::Done)) else {
        panic!("expected the verification marker to go");
    };
    assert_eq!(
        rows[0].0,
        aruna_core::keyspaces::SHARD_VERIFICATION_KEYSPACE
    );
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(false)));
}

#[test]
fn sole_holder_settled() {
    let mut operation = record_step(current_record(), &[node(1)], node(1));
    assert!(matches!(called(operation.first()), IoCall::Delete(_)));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(false)));
}

fn shared_record() -> PlacementRecordOperation {
    let mut operation = record_step(current_record(), &[node(1), node(2)], node(1));
    assert!(matches!(
        shard_effect(operation.first()),
        ShardNetEffect::Existing(_)
    ));
    operation
}

fn own_topic() -> TopicId {
    shard_topic_id(realm(), &placement())
}

#[test]
fn missing_genesis_kept() {
    let mut operation = shared_record();
    assert!(matches!(
        called(operation.next(existing(&[]))),
        IoCall::Effect(_)
    ));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
}

#[test]
fn joined_record_deleted() {
    let mut operation = shared_record();
    assert!(matches!(
        shard_effect(operation.next(existing(&[own_topic()]))),
        ShardNetEffect::Membership(_)
    ));
    assert!(matches!(
        called(operation.next(applied())),
        IoCall::Delete(_)
    ));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(false)));
}

#[test]
fn incomplete_record_kept() {
    let mut operation = shared_record();
    operation.next(existing(&[own_topic()])).unwrap();
    assert!(matches!(
        called(operation.next(refused())),
        IoCall::Effect(_)
    ));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
}

fn pruning() -> PruneReleasedOperation {
    let mut operation = PruneReleasedOperation {
        realm_id: realm(),
        local: node(1),
        now_ms: 0,
        written: false,
    };
    assert!(matches!(called(operation.first()), IoCall::Read(_)));
    operation
}

#[test]
fn prune_needs_rows() {
    let answer = IoResult::Values(vec![None, None]);
    assert_eq!(pruning().next(answer), Ok(Next::Done(())));
}

#[test]
fn prune_skips_garbage() {
    let garbage = Value::from(vec![0xff]);
    let answer = IoResult::Values(vec![Some(garbage.clone()), Some(garbage)]);
    assert_eq!(pruning().next(answer), Ok(Next::Done(())));
}

#[test]
fn settled_realm_idle() {
    assert!(!has_transition_work(&config(&[node(1)]), 0));
}

#[test]
fn open_transition_counts() {
    let mut config = config(&[node(1)]);
    config
        .placement_transitions
        .push(PlacementTransition::new(TransitionPlan {
            transition_id: Ulid::from_bytes([3; 16]),
            strategy_id: strategy_id(),
            buckets: vec![BucketPlan {
                bucket: 0,
                old_holders: vec![node(1)],
                target_holders: vec![node(1), node(2)],
                predecessor_epoch: 1,
            }],
            target_map_epoch: 2,
            limits: Default::default(),
            created_by: node(1),
            created_at_ms: 1,
        }));
    assert!(has_transition_work(&config, 0));
}
