//! Tests the calls and results of one reconcile pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::fixtures::{actor, child, config_target, config_topic, realm};
use super::*;
use crate::document::DocumentEvent;
use crate::structs::SyncQuarantineIdentity;

type Answer = Result<Next<DocumentReconcileResult>, ApplyError>;

fn other_topic() -> irokle::TopicId {
    irokle::TopicId::from_bytes([8; 32])
}

fn dependency() -> DocumentSyncDependency {
    DocumentSyncDependency::RealmConfig(realm())
}

fn value_of(byte: u8) -> Value {
    Value::from(vec![byte])
}

fn cursor_key() -> Key {
    Key::from(vec![7])
}

fn cursor_row() -> Row {
    (APPLIED_OPS_KEYSPACE.to_string(), cursor_key(), value_of(2))
}

fn batch() -> ReconcileBatch {
    ReconcileBatch {
        start_value: value_of(1),
        cursor_key: cursor_key(),
        cursor_value: value_of(2),
        rejections: Vec::new(),
        events: Vec::new(),
    }
}

fn net(event: DocumentNetEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::DocumentSync(event))))
}

fn batch_answer(batch: Option<ReconcileBatch>) -> IoResult {
    net(DocumentNetEvent::ReconcileBatch(batch.map(Box::new)))
}

fn effect_call(effect: DocumentEffect) -> Answer {
    let effect = Effect::Net(NetEffect::DocumentSync(effect));
    Ok(Next::Call(IoCall::Effect(Box::new(effect))))
}

fn work(topics: &[irokle::TopicId]) -> ReconcileWork {
    let mut work = ReconcileWork::default();
    for topic in topics {
        work.push(*topic);
    }
    work
}

fn cursor_read(topic: irokle::TopicId) -> Answer {
    Ok(Next::Call(read_row(
        APPLIED_OPS_KEYSPACE,
        topic_cursor_key(topic),
    )))
}

fn deferred_write(deferred: &DeferredTopics) -> Answer {
    let row = (
        APPLIED_OPS_KEYSPACE.to_string(),
        deferred_topics_key(),
        Value::from(postcard::to_allocvec(deferred).unwrap()),
    );
    Ok(Next::Call(IoCall::Write(vec![row])))
}

fn creates_plan(cursors: Vec<(irokle::TopicId, Row)>, rejections: Vec<SyncRejection>) -> Answer {
    let plan = CreateBatchPlan::new(Vec::new(), cursors, rejections).unwrap();
    Ok(Next::Call(plan_call(plan)))
}

/// A pass over `topics` that waits for the batch of its first topic.
fn loading(topics: &[irokle::TopicId]) -> ReconcileTopicsOperation {
    let mut pass = ReconcileTopicsOperation::new(realm(), topics.to_vec());
    pass.first().unwrap();
    assert_eq!(pass.next(child(work(topics))), cursor_read(topics[0]));
    let load = DocumentEffect::ReconcileBatch {
        topic: topics[0],
        stored_cursor: None,
    };
    assert_eq!(pass.next(IoResult::Values(vec![None])), effect_call(load));
    pass
}

/// A pass whose only batch is applying.
fn applying() -> ReconcileTopicsOperation {
    let mut pass = loading(&[config_topic()]);
    let apply = ApplyBatchOperation::new(config_topic(), realm(), Vec::new(), Vec::new());
    assert_eq!(
        pass.next(batch_answer(Some(batch()))),
        Ok(Next::Call(IoCall::child(apply)))
    );
    pass
}

fn cursor_write() -> Answer {
    Ok(Next::Call(IoCall::Write(vec![cursor_row()])))
}

/// Runs the end of a pass that saved `deferred` and has no create work.
fn finish_pass(pass: &mut ReconcileTopicsOperation, deferred: &DeferredTopics) -> Answer {
    assert_eq!(pass.next(IoResult::Done), deferred_write(deferred));
    assert_eq!(
        pass.next(IoResult::Done),
        creates_plan(Vec::new(), Vec::new())
    );
    let parked = RetryParkedPlan { realm_id: realm() };
    assert_eq!(
        pass.next(child(CreateBatchOutcome::default())),
        Ok(Next::Call(plan_call(parked)))
    );
    let replay = ReplayOverflowOperation::new(realm());
    assert_eq!(pass.next(child(())), Ok(Next::Call(IoCall::child(replay))));
    pass.next(child(()))
}

fn rejection() -> SyncRejection {
    let identity = SyncQuarantineIdentity {
        topic: config_topic(),
        actor: actor(),
        actor_seq: 1,
    };
    let event = DocumentEvent::Delete {
        event_id: ulid::Ulid::from_parts(1, 1),
        target: config_target(),
        change: crate::document::DocumentChange {
            base: None,
            current: crate::document::DocumentSyncRevision {
                generation: 1,
                event_id: ulid::Ulid::from_parts(1, 1),
                actor: super::super::fixtures::node(),
                updated_at_ms: 1,
            },
            kind: crate::document::DocumentChangeKind::Delete,
            placement: crate::structs::placement::record::PlacementRef::NIL,
        },
    };
    SyncRejection::new(identity, event, "refused")
}

#[test]
fn holds_reconcile_scope() {
    let pass = ReconcileTopicsOperation::new(realm(), Vec::new());
    assert_eq!(pass.hold_scope(), Some(HoldScope::DocumentReconcile));
}

#[test]
fn plans_requested_topics() {
    let topics = vec![config_topic(), other_topic(), config_topic()];
    let plan = PendingWorkPlan {
        topics: topics.clone(),
    };
    let mut pass = ReconcileTopicsOperation::new(realm(), topics);
    assert_eq!(pass.first(), Ok(Next::Call(plan_call(plan.clone()))));

    let known = [(
        (APPLIED_OPS_KEYSPACE.to_string(), deferred_topics_key()),
        None,
    )];
    let planned = plan.plan(&mut PlanRows::with_rows(known, 1)).unwrap();
    assert_eq!(planned.output, work(&[config_topic(), other_topic()]));
    assert!(planned.writes.is_empty() && planned.deletes.is_empty());
}

#[test]
fn skips_empty_topic() {
    let mut pass = loading(&[config_topic()]);
    let deferred = DeferredTopics::new();
    assert_eq!(pass.next(batch_answer(None)), deferred_write(&deferred));
    let mut replayed = pass;
    replayed.stage = Stage::SavingDeferred;
    assert_eq!(
        replayed.next(IoResult::Done),
        creates_plan(Vec::new(), Vec::new())
    );
}

#[test]
fn writes_batch_cursor() {
    let mut pass = applying();
    let outcome = BatchOutcome {
        applied_targets: vec![config_target()],
        ..BatchOutcome::default()
    };
    assert_eq!(pass.next(child(outcome)), cursor_write());
    let result = finish_pass(&mut pass, &DeferredTopics::new());
    let Ok(Next::Done(result)) = result else {
        panic!("the pass should have ended: {result:?}");
    };
    assert_eq!(result.targets, vec![config_target()]);
}

#[test]
fn reloads_changed_peers() {
    let mut pass = ReconcileTopicsOperation::new(realm(), vec![config_topic()]).reloading_peers();
    pass.first().unwrap();
    pass.next(child(work(&[config_topic()]))).unwrap();
    pass.next(IoResult::Values(vec![None])).unwrap();
    pass.next(batch_answer(Some(batch()))).unwrap();
    let outcome = BatchOutcome {
        applied_targets: vec![config_target()],
        ..BatchOutcome::default()
    };
    pass.next(child(outcome)).unwrap();
    assert_eq!(
        finish_pass(&mut pass, &DeferredTopics::new()),
        effect_call(DocumentEffect::ReloadRealmPeers)
    );
    let Ok(Next::Done(result)) = pass.next(net(DocumentNetEvent::RealmPeersReloaded)) else {
        panic!("the pass should have ended");
    };
    assert_eq!(result.targets, vec![config_target()]);
}

#[test]
fn stores_rejection_evidence() {
    let mut pass = applying();
    let outcome = BatchOutcome {
        rejections: vec![rejection()],
        ..BatchOutcome::default()
    };
    let plan = CursorEvidencePlan {
        rejections: vec![rejection()],
        cursor: cursor_row(),
    };
    assert_eq!(pass.next(child(outcome)), Ok(Next::Call(plan_call(plan))));
    assert_eq!(
        pass.next(child(true)),
        deferred_write(&DeferredTopics::new())
    );
}

#[test]
fn full_keeps_cursor() {
    let mut pass = applying();
    let outcome = BatchOutcome {
        rejections: vec![rejection()],
        satisfied: BTreeSet::from([dependency()]),
        ..BatchOutcome::default()
    };
    pass.work
        .deferred
        .insert(dependency(), BTreeSet::from([other_topic()]));
    pass.next(child(outcome)).unwrap();
    // The refused batch redelivers, so what it satisfied releases nothing yet.
    let deferred = pass.work.deferred.clone();
    assert_eq!(pass.next(child(false)), deferred_write(&deferred));
}

#[test]
fn releases_satisfied_topics() {
    let mut pass = applying();
    pass.work
        .deferred
        .insert(dependency(), BTreeSet::from([other_topic()]));
    let outcome = BatchOutcome {
        satisfied: BTreeSet::from([dependency()]),
        ..BatchOutcome::default()
    };
    assert_eq!(pass.next(child(outcome)), cursor_write());
    assert_eq!(pass.next(IoResult::Done), cursor_read(other_topic()));
    assert!(pass.work.deferred.is_empty());
}

#[test]
fn defers_crossing_topic() {
    let mut pass = applying();
    let outcome = BatchOutcome {
        cross_topic: BTreeSet::from([dependency()]),
        ..BatchOutcome::default()
    };
    let deferred = DeferredTopics::from([(dependency(), BTreeSet::from([config_topic()]))]);
    assert_eq!(pass.next(child(outcome)), deferred_write(&deferred));
}

#[test]
fn holds_create_cursor() {
    let mut pass = applying();
    let outcome = BatchOutcome {
        deferred_creates: true,
        rejections: vec![rejection()],
        ..BatchOutcome::default()
    };
    assert_eq!(
        pass.next(child(outcome)),
        deferred_write(&DeferredTopics::new())
    );
    assert_eq!(
        pass.next(IoResult::Done),
        creates_plan(vec![(config_topic(), cursor_row())], vec![rejection()])
    );
}

#[test]
fn records_new_overflow() {
    let mut pass = applying();
    let outcome = BatchOutcome {
        overflowed: true,
        ..BatchOutcome::default()
    };
    let key = overflow_key(config_topic());
    assert_eq!(
        pass.next(child(outcome)),
        Ok(Next::Call(read_row(APPLIED_OPS_KEYSPACE, key.clone())))
    );
    let row = (APPLIED_OPS_KEYSPACE.to_string(), key, value_of(1));
    assert_eq!(
        pass.next(IoResult::Values(vec![None])),
        Ok(Next::Call(IoCall::Write(vec![row])))
    );
    assert_eq!(pass.next(IoResult::Done), cursor_write());
}

#[test]
fn keeps_earlier_overflow() {
    let mut pass = applying();
    let outcome = BatchOutcome {
        overflowed: true,
        ..BatchOutcome::default()
    };
    pass.next(child(outcome)).unwrap();
    let stored = IoResult::Values(vec![Some(value_of(5))]);
    assert_eq!(pass.next(stored), cursor_write());
}

#[test]
fn saves_create_deferrals() {
    let mut pass = loading(&[config_topic()]);
    pass.next(batch_answer(None)).unwrap();
    pass.next(IoResult::Done).unwrap();
    let outcome = CreateBatchOutcome {
        deferred: vec![(dependency(), other_topic())],
        ..CreateBatchOutcome::default()
    };
    let deferred = DeferredTopics::from([(dependency(), BTreeSet::from([other_topic()]))]);
    assert_eq!(pass.next(child(outcome)), deferred_write(&deferred));
    let parked = RetryParkedPlan { realm_id: realm() };
    assert_eq!(pass.next(IoResult::Done), Ok(Next::Call(plan_call(parked))));
}

#[test]
fn reports_net_failure() {
    let mut pass = loading(&[config_topic()]);
    let failed = net(DocumentNetEvent::Error {
        target: None,
        error: "offline".to_string(),
    });
    assert_eq!(
        pass.next(failed),
        Err(ApplyError::Failed("offline".to_string()))
    );
    let mut pass = loading(&[config_topic()]);
    assert!(matches!(
        pass.next(net(DocumentNetEvent::RealmPeersReloaded)),
        Err(ApplyError::Failed(_))
    ));
}
