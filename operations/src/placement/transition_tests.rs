//! Tests the transition steps as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{called, child, node, runs_child, shard_effect, shard_event};
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision, DocumentTarget,
};
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::placement::record::PlacementStrategy;
use aruna_core::structs::placement::transition::{
    BucketBarrier, BucketCompletion, BucketPlan, CompletionProof, StallReport, TransitionPlan,
};
use aruna_core::types::Value;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([8; 32])
}

fn strategy_id() -> Ulid {
    Ulid::from_bytes([9; 16])
}

fn transition_id() -> Ulid {
    Ulid::from_bytes([3; 16])
}

fn placement() -> PlacementRef {
    PlacementRef {
        strategy_id: strategy_id(),
        shard: 0,
    }
}

fn transition(old_holders: Vec<NodeId>, target_holders: Vec<NodeId>) -> PlacementTransition {
    PlacementTransition::new(TransitionPlan {
        transition_id: transition_id(),
        strategy_id: strategy_id(),
        buckets: vec![BucketPlan {
            bucket: 0,
            old_holders,
            target_holders,
            predecessor_epoch: 1,
        }],
        target_map_epoch: 2,
        limits: Default::default(),
        created_by: node(1),
        created_at_ms: 1,
    })
}

/// A realm with one activated single-bucket strategy, its nodes of `kind`.
fn config(nodes: &[NodeId], kind: RealmNodeKind) -> RealmConfigDocument {
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
        config.ensure_node(*node_id, kind.clone());
    }
    config.snapshot_candidate_map();
    config
}

fn config_answer(config: RealmConfigDocument) -> IoResult {
    child(Ok::<_, GetConfigError>(config))
}

fn barrier(reported_by: NodeId) -> BucketBarrier {
    BucketBarrier {
        bucket: 0,
        reported_by,
        frontier: postcard::to_allocvec(&irokle::ActorClock::default()).unwrap(),
    }
}

fn step_answer(plan: StepPlan) -> IoResult {
    child(Ok::<_, IoError>(plan))
}

fn mutation_failed(error: MutatePlacementError) -> IoResult {
    child(Err::<RealmConfigDocument, _>(error))
}

fn conflict() -> MutatePlacementError {
    MutatePlacementError::StorageError(StorageError::TransactionConflict)
}

fn stored() -> IoResult {
    child(Ok::<_, MutatePlacementError>(RealmConfigDocument::new(
        realm(),
        Vec::new(),
        3,
    )))
}

fn drained() -> StepPlan {
    StepPlan::Ready(RealmPlacementMutation::ReportDrained {
        transition_id: transition_id(),
        bucket: 0,
        reported_by: node(1),
    })
}

fn draining() -> DrainStepOperation {
    let transition = transition(vec![node(1)], vec![node(2)]);
    let mut operation = DrainStepOperation::new(realm(), &transition, &placement(), node(1));
    assert!(runs_child(operation.first()));
    operation
}

fn fence_closed() -> IoResult {
    child(Ok::<(), IoError>(()))
}

/// Runs up to the check for what still blocks the drain.
fn drain_checked() -> DrainStepOperation {
    let mut operation = draining();
    let topic = shard_topic_id(realm(), &placement());
    assert_eq!(
        shard_effect(operation.next(fence_closed())),
        ShardNetEffect::Close(topic)
    );
    let closed = shard_event(ShardNetEvent::Closed(Ok(false)));
    assert!(runs_child(operation.next(closed)));
    operation
}

#[test]
fn unplanned_bucket_skipped() {
    let transition = transition(vec![node(1)], vec![node(2)]);
    let other = PlacementRef {
        strategy_id: strategy_id(),
        shard: 9,
    };
    let mut operation = DrainStepOperation::new(realm(), &transition, &other, node(1));
    assert_eq!(operation.first(), Ok(Next::Done(StepPlan::Done)));
}

#[test]
fn open_fence_pending() {
    let failed = Err::<(), _>(IoError::Conflict);
    assert_eq!(
        draining().next(child(failed)),
        Ok(Next::Done(StepPlan::Pending))
    );
}

#[test]
fn syncing_topic_pending() {
    let mut operation = draining();
    operation.next(fence_closed()).unwrap();
    let closing = shard_event(ShardNetEvent::Closed(Ok(true)));
    assert_eq!(operation.next(closing), Ok(Next::Done(StepPlan::Pending)));
}

#[test]
fn topic_error_pending() {
    let mut operation = draining();
    operation.next(fence_closed()).unwrap();
    let failed = shard_event(ShardNetEvent::Closed(Err("closed".to_string())));
    assert_eq!(operation.next(failed), Ok(Next::Done(StepPlan::Pending)));
}

#[test]
fn no_network_continues() {
    let mut operation = draining();
    operation.next(fence_closed()).unwrap();
    let absent = shard_event(ShardNetEvent::NoNetwork);
    assert!(runs_child(operation.next(absent)));
}

#[test]
fn clear_bucket_reported() {
    let clear = child(Ok::<_, IoError>(None::<DrainBlocker>));
    assert_eq!(drain_checked().next(clear), Ok(Next::Done(drained())));
}

#[test]
fn blocked_bucket_pending() {
    let blocked = child(Ok::<_, IoError>(Some(DrainBlocker::OutboxRow)));
    assert_eq!(
        drain_checked().next(blocked),
        Ok(Next::Done(StepPlan::Pending))
    );
}

#[test]
fn failed_scan_pending() {
    let failed = child(Err::<Option<DrainBlocker>, _>(IoError::Conflict));
    assert_eq!(
        drain_checked().next(failed),
        Ok(Next::Done(StepPlan::Pending))
    );
}

fn outbox_row(placement: PlacementRef, generation: u64) -> Value {
    let change = DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::from_bytes([4; 16]),
            actor: node(1),
            updated_at_ms: 1,
        },
        kind: DocumentChangeKind::Delete,
        placement,
    };
    let record = DocumentOutboxRecord {
        outbox_id: Ulid::from_bytes([5; 16]),
        node_id: node(1),
        target: DocumentTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([6; 16]),
        },
        peers: Vec::new(),
        event: DocumentOutboxEvent::Delete { change },
        placement,
        generation,
        updated_at: 1,
        allow_genesis: false,
    };
    postcard::to_allocvec(&record).unwrap().into()
}

fn outbox_page(rows: Vec<Value>) -> IoResult {
    IoResult::Page {
        rows: (rows.into_iter())
            .map(|value| (Key::from(vec![1]), value))
            .collect(),
        next: None,
    }
}

fn scanning() -> DrainBlockerOperation {
    let mut operation = DrainBlockerOperation::new(&placement(), 3);
    assert_eq!(
        shard_effect(operation.first()),
        ShardNetEffect::EvictionPending(placement())
    );
    assert!(matches!(
        called(operation.next(shard_event(ShardNetEvent::Flag(false)))),
        IoCall::Page { .. }
    ));
    operation
}

#[test]
fn eviction_blocks_first() {
    let mut operation = DrainBlockerOperation::new(&placement(), 3);
    operation.first().unwrap();
    assert_eq!(
        operation.next(shard_event(ShardNetEvent::Flag(true))),
        Ok(Next::Done(Some(DrainBlocker::Eviction)))
    );
}

#[test]
fn closed_row_blocks() {
    let row = outbox_row(placement(), 3);
    assert_eq!(
        scanning().next(outbox_page(vec![row])),
        Ok(Next::Done(Some(DrainBlocker::OutboxRow)))
    );
}

#[test]
fn unfenced_row_blocks() {
    let row = outbox_row(placement(), 0);
    assert_eq!(
        scanning().next(outbox_page(vec![row])),
        Ok(Next::Done(Some(DrainBlocker::OutboxRow)))
    );
}

#[test]
fn later_rows_pass() {
    // A newer generation and another bucket say nothing about this closed fence.
    let other = PlacementRef {
        strategy_id: strategy_id(),
        shard: 1,
    };
    let rows = vec![outbox_row(placement(), 4), outbox_row(other, 1)];
    let mut operation = scanning();
    assert!(matches!(
        called(operation.next(outbox_page(rows))),
        IoCall::Page { .. }
    ));
}

#[test]
fn empty_streams_clear() {
    let mut operation = scanning();
    assert!(matches!(
        called(operation.next(outbox_page(Vec::new()))),
        IoCall::Page { .. }
    ));
    assert!(matches!(
        called(operation.next(outbox_page(Vec::new()))),
        IoCall::Page { .. }
    ));
    assert_eq!(
        operation.next(outbox_page(Vec::new())),
        Ok(Next::Done(None))
    );
}

fn manifest() -> ShardManifest {
    ShardManifest {
        placement: placement(),
        holder: node(1),
        entries: Vec::new(),
        cursor: vec![7],
        digest: [0; 32],
        updated_at_ms: 0,
    }
}

fn barrier_step(transition: &PlacementTransition) -> BarrierStepOperation {
    BarrierStepOperation::new(realm(), node(1), transition, &placement())
}

#[test]
fn reported_barrier_done() {
    let mut transition = transition(vec![node(1)], vec![node(2)]);
    transition.barriers.push(barrier(node(1)));
    assert_eq!(
        barrier_step(&transition).first(),
        Ok(Next::Done(StepPlan::Done))
    );
}

#[test]
fn barrier_reports_cursor() {
    let mut operation = barrier_step(&transition(vec![node(1)], vec![node(2)]));
    assert!(runs_child(operation.first()));
    assert_eq!(
        operation.next(child(Ok::<_, ManifestError>(manifest()))),
        Ok(Next::Done(StepPlan::Ready(
            RealmPlacementMutation::ReportBarrier {
                transition_id: transition_id(),
                bucket: 0,
                reported_by: node(1),
                frontier: vec![7],
            }
        )))
    );
}

#[test]
fn barrier_waits_manifest() {
    let mut operation = barrier_step(&transition(vec![node(1)], vec![node(2)]));
    operation.first().unwrap();
    let failed = Err::<ShardManifest, _>(ManifestError("no net".to_string()));
    assert_eq!(
        operation.next(child(failed)),
        Ok(Next::Done(StepPlan::Pending))
    );
}

fn completion(transition: &PlacementTransition) -> CompletionStepOperation {
    CompletionStepOperation::new(realm(), node(1), transition, &placement())
}

fn signature() -> iroh::Signature {
    iroh::SecretKey::from_bytes(&[1; 32]).sign(b"claim")
}

fn proof() -> CompletionProof {
    CompletionProof {
        bucket: 0,
        holder: node(1),
        old_activation_epoch: 1,
        target_map_epoch: 2,
        barrier_digest: [0; 32],
        checkpoint_root: [7; 32],
        signature: signature(),
    }
}

/// A target that is its bucket's only old holder, so no source has to serve it.
fn sourceless(predecessor_epoch: u64) -> CompletionStepOperation {
    let mut transition = transition(vec![node(1)], vec![node(1)]);
    transition.plan.buckets[0].predecessor_epoch = predecessor_epoch;
    transition.barriers.push(barrier(node(1)));
    let mut operation = completion(&transition);
    assert!(matches!(
        shard_effect(operation.first()),
        ShardNetEffect::FrontierRoot { .. }
    ));
    let root = shard_event(ShardNetEvent::Root(Ok([7; 32])));
    assert!(runs_child(operation.next(root)));
    operation
}

/// A target with one other old holder, past its barrier check.
fn with_source() -> CompletionStepOperation {
    let mut transition = transition(vec![node(2)], vec![node(1)]);
    transition.barriers.push(barrier(node(2)));
    let mut operation = completion(&transition);
    assert!(matches!(
        shard_effect(operation.first()),
        ShardNetEffect::Existing(_)
    ));
    operation
}

fn topic_known() -> IoResult {
    let topic = shard_topic_id(realm(), &placement());
    shard_event(ShardNetEvent::Existing([topic].into_iter().collect()))
}

fn stall() -> StepPlan {
    StepPlan::Stalled(RealmPlacementMutation::ReportStall {
        transition_id: transition_id(),
        bucket: 0,
        reported_by: node(1),
        reason: "no old holder served a verifiable copy".to_string(),
    })
}

fn unconverged() -> IoResult {
    child(Ok::<_, IoError>(None::<(NodeId, [u8; 32])>))
}

#[test]
fn missing_barrier_pending() {
    let transition = transition(vec![node(2)], vec![node(1)]);
    assert_eq!(
        completion(&transition).first(),
        Ok(Next::Done(StepPlan::Pending))
    );
}

#[test]
fn unplanned_completion_done() {
    let transition = transition(vec![node(2)], vec![node(1)]);
    let other = PlacementRef {
        strategy_id: strategy_id(),
        shard: 9,
    };
    let mut operation = CompletionStepOperation::new(realm(), node(1), &transition, &other);
    assert_eq!(operation.first(), Ok(Next::Done(StepPlan::Done)));
}

#[test]
fn matching_activation_signs() {
    let mut operation = sourceless(1);
    let config = config(&[node(1)], RealmNodeKind::Server);
    let ShardNetEffect::SignProof(claim) = shard_effect(operation.next(config_answer(config)))
    else {
        panic!("expected the proof to be signed");
    };
    assert_eq!(claim.checkpoint_root, [7; 32]);
    assert_eq!(claim.old_activation_epoch, 1);
    assert_eq!(claim.holder, node(1));
    assert_eq!(
        operation.next(shard_event(ShardNetEvent::Proof(proof()))),
        Ok(Next::Done(StepPlan::Ready(
            RealmPlacementMutation::SubmitCompletion {
                transition_id: transition_id(),
                strategy_id: strategy_id(),
                proof: proof(),
            }
        )))
    );
}

#[test]
fn moved_activation_unsigned() {
    // The reducer would ignore a proof from another epoch, so none is signed.
    let config = config(&[node(1)], RealmNodeKind::Server);
    assert_eq!(
        sourceless(5).next(config_answer(config)),
        Ok(Next::Done(StepPlan::Pending))
    );
}

#[test]
fn unreadable_config_pending() {
    let failed = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    assert_eq!(
        sourceless(1).next(child(failed)),
        Ok(Next::Done(StepPlan::Pending))
    );
}

#[test]
fn failed_root_pending() {
    let mut transition = transition(vec![node(1)], vec![node(1)]);
    transition.barriers.push(barrier(node(1)));
    let mut operation = completion(&transition);
    operation.first().unwrap();
    let failed = shard_event(ShardNetEvent::Root(Err("no genesis".to_string())));
    assert_eq!(operation.next(failed), Ok(Next::Done(StepPlan::Pending)));
}

#[test]
fn garbled_barrier_pending() {
    let mut transition = transition(vec![node(1)], vec![node(1)]);
    let mut garbled = barrier(node(1));
    garbled.frontier = vec![0xff; 40];
    transition.barriers.push(garbled);
    assert_eq!(
        completion(&transition).first(),
        Ok(Next::Done(StepPlan::Pending))
    );
}

#[test]
fn missing_topic_pulled() {
    let mut operation = with_source();
    let empty = shard_event(ShardNetEvent::Existing(Default::default()));
    assert_eq!(
        operation.next(empty),
        Ok(Next::Call(IoCall::child(
            crate::shard::pull::PullTopicsOperation::new(
                vec![shard_topic_id(realm(), &placement())],
                vec![node(2)],
            )
        )))
    );
    let pulled = IoResult::Child(aruna_core::calls::ChildOutcome::new(Ok::<
        bool,
        aruna_core::calls::IoError,
    >(true)));
    assert!(runs_child(operation.next(pulled)));
}

#[test]
fn no_network_pending() {
    assert_eq!(
        with_source().next(shard_event(ShardNetEvent::NoNetwork)),
        Ok(Next::Done(StepPlan::Pending))
    );
}

#[test]
fn unserved_source_stalls() {
    let mut operation = with_source();
    assert!(runs_child(operation.next(topic_known())));
    assert_eq!(operation.next(unconverged()), Ok(Next::Done(stall())));
}

#[test]
fn reported_stall_pending() {
    let mut transition = transition(vec![node(2)], vec![node(1)]);
    transition.barriers.push(barrier(node(2)));
    transition.stalls.push(StallReport {
        bucket: 0,
        reported_by: node(1),
        reason: "earlier".to_string(),
    });
    let mut operation = completion(&transition);
    operation.first().unwrap();
    operation.next(topic_known()).unwrap();
    assert_eq!(
        operation.next(unconverged()),
        Ok(Next::Done(StepPlan::Pending))
    );
}

#[test]
fn converged_source_hashed() {
    let mut operation = with_source();
    operation.next(topic_known()).unwrap();
    let converged = Some((node(2), [1u8; 32]));
    assert!(matches!(
        shard_effect(operation.next(child(Ok::<_, IoError>(converged)))),
        ShardNetEffect::FrontierRoot { .. }
    ));
}

fn submitting() -> SubmitStepsOperation {
    let actor = realm_actor(realm(), node(1));
    let mut operation = SubmitStepsOperation::new(actor, Vec::new());
    assert!(runs_child(operation.first()));
    operation
}

#[test]
fn applied_steps_settle() {
    assert_eq!(submitting().next(stored()), Ok(Next::Done(false)));
}

#[test]
fn refused_steps_retry() {
    let refused = mutation_failed(MutatePlacementError::ConfigMissing);
    assert_eq!(submitting().next(refused), Ok(Next::Done(true)));
}

#[test]
fn conflicted_steps_wait() {
    let mut operation = submitting();
    for _ in 0..CONFLICT_ATTEMPTS {
        assert!(matches!(
            called(operation.next(mutation_failed(conflict()))),
            IoCall::Delay(_)
        ));
        assert!(runs_child(operation.next(IoResult::Woke)));
    }
    assert_eq!(
        operation.next(mutation_failed(conflict())),
        Ok(Next::Done(true))
    );
}

#[test]
fn server_issues_nothing() {
    let config = config(&[node(1)], RealmNodeKind::Server);
    let mut expansions = ExpansionsOperation::new(realm(), node(1), Arc::new(config.clone()));
    assert_eq!(expansions.first(), Ok(Next::Done(false)));
    let mut activations = ActivationsOperation::new(realm(), node(1), Arc::new(config));
    assert_eq!(activations.first(), Ok(Next::Done(false)));
}

/// Terminal transition history plus a node the frozen map does not know yet.
fn expandable() -> RealmConfigDocument {
    let mut config = config(&[node(1)], RealmNodeKind::Management);
    let mut finished = transition(Vec::new(), Vec::new());
    finished.plan.buckets.clear();
    config.placement_transitions.push(finished);
    config.ensure_node(node(2), RealmNodeKind::Management);
    let epoch = config.newest_map_epoch().unwrap_or_default() + 1;
    config.candidate_maps.push(config.freeze_map(epoch));
    config
}

#[test]
fn trailing_map_expands() {
    let mut operation = ExpansionsOperation::new(realm(), node(1), Arc::new(expandable()));
    operation.stamp(9);
    assert!(runs_child(operation.first()));
    assert_eq!(operation.next(stored()), Ok(Next::Done(true)));
}

#[test]
fn failed_start_pending() {
    let mut operation = ExpansionsOperation::new(realm(), node(1), Arc::new(expandable()));
    operation.first().unwrap();
    operation.pending = false;
    let failed = mutation_failed(MutatePlacementError::ConfigMissing);
    assert_eq!(operation.next(failed), Ok(Next::Done(true)));
}

/// A management realm with a strategy created after its only map was frozen.
fn late_strategy() -> RealmConfigDocument {
    let mut config = config(&[node(1), node(2)], RealmNodeKind::Management);
    config.strategies.push(PlacementStrategy {
        strategy_id: Ulid::from_bytes([10; 16]),
        name: "late".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 1,
    });
    config
}

fn publisher(config: &RealmConfigDocument) -> (NodeId, NodeId) {
    let issuer = map_publisher(config).map(str::to_string);
    match issuer == Some(node(1).to_string()) {
        true => (node(1), node(2)),
        false => (node(2), node(1)),
    }
}

#[test]
fn late_strategy_unfrozen() {
    let (actions, pending, unfrozen) = plan_activations(&late_strategy(), node(1));
    assert!(actions.is_empty());
    assert!(pending);
    assert!(unfrozen);
}

#[test]
fn other_node_waits() {
    let config = late_strategy();
    let (_, other) = publisher(&config);
    let mut operation = ActivationsOperation::new(realm(), other, Arc::new(config));
    assert_eq!(operation.first(), Ok(Next::Done(true)));
}

#[test]
fn publisher_rereads_config() {
    let config = late_strategy();
    let (issuer, _) = publisher(&config);
    let mut operation = ActivationsOperation::new(realm(), issuer, Arc::new(config.clone()));
    assert!(runs_child(operation.first()));
    assert_eq!(operation.stage, ActivationStage::Publish);
    assert!(runs_child(operation.next(stored())));
    assert_eq!(operation.stage, ActivationStage::Reread);
    assert_eq!(
        operation.next(config_answer(config)),
        Ok(Next::Done(true)),
        "the map is still missing the strategy, so nothing can activate yet"
    );
}

#[test]
fn failed_publish_stops() {
    let config = late_strategy();
    let (issuer, _) = publisher(&config);
    let mut operation = ActivationsOperation::new(realm(), issuer, Arc::new(config));
    operation.first().unwrap();
    let failed = mutation_failed(MutatePlacementError::ConfigMissing);
    assert_eq!(operation.next(failed), Ok(Next::Done(true)));
}

fn fence_attempt(generation: u64) -> FenceAttemptOperation {
    let (key_space, key) = fence_read(&realm(), &placement());
    let mut operation = FenceAttemptOperation {
        key_space,
        key,
        placement: placement(),
        generation,
        writing: false,
    };
    assert!(matches!(called(operation.first()), IoCall::Read(_)));
    operation
}

fn fence_value(generation: u64) -> IoResult {
    let value = Value::from(generation.to_be_bytes().to_vec());
    IoResult::Values(vec![Some(value)])
}

#[test]
fn closed_fence_untouched() {
    assert_eq!(fence_attempt(3).next(fence_value(5)), Ok(Next::Done(())));
    assert_eq!(fence_attempt(5).next(fence_value(5)), Ok(Next::Done(())));
}

#[test]
fn lower_fence_raised() {
    let mut operation = fence_attempt(5);
    let IoCall::Write(rows) = called(operation.next(fence_value(3))) else {
        panic!("expected the fence write");
    };
    assert_eq!(&rows[0].2[..], 5u64.to_be_bytes().as_slice());
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn absent_fence_written() {
    let turn = fence_attempt(5).next(IoResult::Values(vec![None]));
    assert!(matches!(called(turn), IoCall::Write(_)));
}

#[test]
fn garbled_fence_rewritten() {
    let garbled = IoResult::Values(vec![Some(Value::from(vec![1]))]);
    assert!(matches!(
        called(fence_attempt(5).next(garbled)),
        IoCall::Write(_)
    ));
}

#[test]
fn close_retries_conflicts() {
    let mut operation = CloseFenceOperation::new(realm(), placement(), 5);
    assert!(runs_child(operation.first()));
    for _ in 1..CLOSE_ATTEMPTS {
        let lost = child(Err::<(), _>(IoError::Conflict));
        assert!(runs_child(operation.next(lost)));
    }
    let lost = child(Err::<(), _>(IoError::Conflict));
    assert_eq!(operation.next(lost), Err(IoError::Conflict));
}

#[test]
fn close_finishes() {
    let mut operation = CloseFenceOperation::new(realm(), placement(), 5);
    operation.first().unwrap();
    assert_eq!(operation.next(fence_closed()), Ok(Next::Done(())));
}

fn pass(transition: Option<PlacementTransition>) -> ProcessTransitionsOperation {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    config.placement_transitions.extend(transition);
    let mut operation = ProcessTransitionsOperation::new(realm(), node(1), Arc::new(config));
    operation.stamp(1_000);
    assert!(runs_child(operation.first()));
    assert!(runs_child(operation.next(child(Ok::<_, IoError>(false)))));
    operation
}

fn no_expansion() -> IoResult {
    child(Ok::<_, IoError>(false))
}

#[test]
fn idle_pass_finishes() {
    assert_eq!(pass(None).next(no_expansion()), Ok(Next::Done(false)));
}

#[test]
fn open_expansion_pending() {
    let answer = child(Ok::<_, IoError>(true));
    assert_eq!(pass(None).next(answer), Ok(Next::Done(true)));
}

#[test]
fn holder_steps_run() {
    // An old holder that is also a target owes its barrier and then its proof.
    let mut operation = pass(Some(transition(vec![node(1)], vec![node(1)])));
    assert!(runs_child(operation.next(no_expansion())));
    assert!(runs_child(operation.next(step_answer(StepPlan::Done))));
    assert_eq!(
        operation.next(step_answer(StepPlan::Pending)),
        Ok(Next::Done(true))
    );
}

#[test]
fn stranger_owes_nothing() {
    let transition = transition(vec![node(2)], vec![node(3)]);
    assert_eq!(
        pass(Some(transition)).next(no_expansion()),
        Ok(Next::Done(false))
    );
}

#[test]
fn queued_bucket_pending() {
    let mut transition = transition(vec![node(1)], vec![node(2)]);
    transition.plan.limits.max_incomplete_buckets = 0;
    assert_eq!(
        pass(Some(transition)).next(no_expansion()),
        Ok(Next::Done(true))
    );
}

#[test]
fn ready_steps_submitted() {
    let mut operation = pass(Some(transition(vec![node(1)], vec![node(2)])));
    operation.next(no_expansion()).unwrap();
    assert!(runs_child(operation.next(step_answer(drained()))));
    assert_eq!(operation.stage, PassStage::Submit);
    let failed = child(Ok::<_, IoError>(true));
    assert_eq!(operation.next(failed), Ok(Next::Done(true)));
}

fn departed() -> PlacementTransition {
    let mut transition = transition(vec![node(1)], vec![node(2)]);
    transition.completed.push(BucketCompletion {
        bucket: 0,
        completed_at_ms: 900,
    });
    transition
}

#[test]
fn departure_kicks_drain() {
    // Within the grace period no drain report is owed, but the outbox still flushes.
    let mut operation = pass(Some(departed()));
    assert!(runs_child(operation.next(no_expansion())));
    assert_eq!(operation.stage, PassStage::Kick);
    let failed = child(Err::<(), _>(IoError::Conflict));
    assert_eq!(operation.next(failed), Ok(Next::Done(false)));
}

#[test]
fn past_grace_drains() {
    let mut transition = departed();
    transition.plan.limits.grace_ms = 10;
    let mut operation = pass(Some(transition));
    assert!(runs_child(operation.next(no_expansion())));
    assert_eq!(operation.stage, PassStage::Work);
}

#[test]
fn stale_plan_ignored() {
    let mut transition = transition(vec![node(1)], vec![node(2)]);
    transition.plan.buckets[0].predecessor_epoch = 7;
    let mut config = config(&[node(1)], RealmNodeKind::Server);
    config.placement_transitions.push(transition.clone());
    assert!(transition_stale(&config, &transition));
    let mut operation = ProcessTransitionsOperation::new(realm(), node(1), Arc::new(config));
    operation.first().unwrap();
    operation.next(child(Ok::<_, IoError>(false))).unwrap();
    assert_eq!(operation.next(no_expansion()), Ok(Next::Done(false)));
}

#[test]
fn stale_plan_aborted() {
    let mut transition = transition(vec![node(1)], vec![node(2)]);
    transition.plan.buckets[0].predecessor_epoch = 7;
    let mut config = config(&[node(1)], RealmNodeKind::Management);
    config.placement_transitions.push(transition);
    let mut operation = ProcessTransitionsOperation::new(realm(), node(1), Arc::new(config));
    operation.first().unwrap();
    operation.next(child(Ok::<_, IoError>(false))).unwrap();
    assert!(runs_child(operation.next(no_expansion())));
    assert_eq!(operation.stage, PassStage::Submit);
}
