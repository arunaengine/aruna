//! Tests the calls and results of applying one batch.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::super::fixtures::{
    actor, batch_event, check_plan, checked, child, config_target, config_topic, failed, node,
    realm, run_event, signature, single_event, snapshot_event, wrapped,
};
use super::*;
use crate::calls::IoError;
use crate::document::{DocumentChange, DocumentChangeKind, DocumentSyncRevision};
use crate::events::{Event, NetEvent};

type Answer = Result<Next<BatchOutcome>, ApplyError>;

fn operation(events: Vec<DocumentEvent>) -> ApplyBatchOperation {
    let events = events.into_iter().map(batch_event).collect();
    ApplyBatchOperation::new(config_topic(), realm(), events, Vec::new())
}

fn check(event: AdminDocumentEvent) -> Answer {
    Ok(Next::Call(plan_call(check_plan(event))))
}

fn admin_plan(event: AdminDocumentEvent) -> Answer {
    Ok(Next::Call(plan_call(AdminOperationPlan {
        target: config_target(),
        event,
    })))
}

fn run_plan(events: Vec<AdminDocumentEvent>) -> Answer {
    Ok(Next::Call(plan_call(ConfigRunPlan {
        target: config_target(),
        events,
    })))
}

fn done(result: Answer) -> BatchOutcome {
    match result {
        Ok(Next::Done(outcome)) => outcome,
        other => panic!("the batch should have ended: {other:?}"),
    }
}

fn accepted() -> IoResult {
    checked(AdminEventValidation::Accepted)
}

fn deferred(dependency: Option<DocumentSyncDependency>) -> IoResult {
    checked(AdminEventValidation::Deferred {
        dependency,
        reason: "waiting".to_string(),
    })
}

fn upsert(target: DocumentTarget) -> DocumentEvent {
    let event_id = Ulid::from_parts(9, 9);
    DocumentEvent::Upsert {
        event_id,
        target,
        bytes: vec![1],
        change: DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id,
                actor: node(),
                updated_at_ms: 1,
            },
            kind: DocumentChangeKind::Upsert,
            placement: PlacementRef::NIL,
        },
    }
}

fn identity() -> SyncQuarantineIdentity {
    SyncQuarantineIdentity {
        topic: config_topic(),
        actor: actor(),
        actor_seq: 1,
    }
}

#[test]
fn applies_document_event() {
    let event = upsert(config_target());
    let mut batch = operation(vec![event.clone()]);
    let plan = ApplyEventPlan {
        topic_id: config_topic(),
        actor_id: actor(),
        realm_id: realm(),
        identity: identity(),
        event,
    };
    assert_eq!(batch.first(), Ok(Next::Call(plan_call(plan))));
    let applied = ApplyOutcome::Applied {
        target: config_target(),
        tombstone: None,
        git_document: None,
    };
    let outcome = done(batch.next(child(applied)));
    assert_eq!(outcome.applied_targets, vec![config_target()]);
}

fn git_applied(document_id: Ulid) -> IoResult {
    child(ApplyOutcome::Applied {
        target: config_target(),
        tombstone: None,
        git_document: Some(document_id),
    })
}

fn net_answer_of(event: DocumentNetEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::DocumentSync(event))))
}

#[test]
fn notes_git_records() {
    let document_id = Ulid::from_parts(5, 5);
    let mut batch = operation(vec![upsert(config_target())]);
    batch.first().unwrap();
    let note = DocumentEffect::NoteGitRecord { document_id };
    let note = Effect::Net(NetEffect::DocumentSync(note));
    assert_eq!(
        batch.next(git_applied(document_id)),
        Ok(Next::Call(IoCall::Effect(Box::new(note))))
    );
    let noted = net_answer_of(DocumentNetEvent::GitRecordNoted);
    let outcome = done(batch.next(noted));
    assert_eq!(outcome.applied_targets, vec![config_target()]);
}

#[test]
fn reports_note_failure() {
    let document_id = Ulid::from_parts(5, 5);
    let mut batch = operation(vec![upsert(config_target())]);
    batch.first().unwrap();
    batch.next(git_applied(document_id)).unwrap();
    let failed = net_answer_of(DocumentNetEvent::Error {
        target: None,
        error: "offline".to_string(),
    });
    assert_eq!(
        batch.next(failed),
        Err(ApplyError::Failed("offline".to_string()))
    );

    let mut batch = operation(vec![upsert(config_target())]);
    batch.first().unwrap();
    batch.next(git_applied(document_id)).unwrap();
    let other = net_answer_of(DocumentNetEvent::RealmPeersReloaded);
    assert!(matches!(batch.next(other), Err(ApplyError::Failed(_))));
}

#[test]
fn collects_event_outcomes() {
    let dependency = DocumentSyncDependency::RealmConfig(realm());
    let events = vec![upsert(config_target()), upsert(config_target())];
    let mut batch = operation(events);
    batch.first().unwrap();
    let waits = batch.next(child(ApplyOutcome::Deferred(dependency)));
    assert!(matches!(waits, Ok(Next::Call(IoCall::Child(_)))));
    let outcome = done(batch.next(child(ApplyOutcome::Skipped)));
    assert_eq!(outcome.cross_topic, BTreeSet::from([dependency]));
    assert!(outcome.applied_targets.is_empty());
}

#[test]
fn refuses_barred_publisher() {
    let mut event = batch_event(upsert(config_target()));
    event.publisher_allowed = false;
    let mut batch = ApplyBatchOperation::new(config_topic(), realm(), vec![event], Vec::new());
    let outcome = done(batch.first());
    assert_eq!(outcome.rejections.len(), 1);
    assert_eq!(outcome.rejections[0].identity, identity());
}

#[test]
fn screens_foreign_targets() {
    let other_topic = irokle::TopicId::from_bytes([9; 32]);
    let node_info = batch_event(upsert(DocumentTarget::NodeInfo {
        realm_id: realm(),
        node_id: node(),
    }));
    let mut moved = ApplyBatchOperation::new(other_topic, realm(), vec![node_info], Vec::new());
    assert_eq!(done(moved.first()), BatchOutcome::default());

    let config = batch_event(upsert(config_target()));
    let mut foreign = ApplyBatchOperation::new(other_topic, realm(), vec![config], Vec::new());
    assert_eq!(done(foreign.first()).rejections.len(), 1);
}

#[test]
fn keeps_given_rejections() {
    let rejection = SyncRejection::new(identity(), upsert(config_target()), "undecodable");
    let mut batch =
        ApplyBatchOperation::new(config_topic(), realm(), Vec::new(), vec![rejection.clone()]);
    assert_eq!(done(batch.first()).rejections, vec![rejection]);
}

#[test]
fn routes_family_admin() {
    let target = DocumentTarget::NodeInfo {
        realm_id: realm(),
        node_id: node(),
    };
    let event = DocumentEvent::AdminOperation {
        target: target.clone(),
        event: Box::new(single_event(1)),
        placement: PlacementRef::NIL,
        origin_signature: signature(),
    };
    let topic = target.sync_topic_id(realm(), &PlacementRef::NIL);
    let events = vec![batch_event(event.clone())];
    let mut batch = ApplyBatchOperation::new(topic, realm(), events, Vec::new());
    let plan = ApplyEventPlan {
        topic_id: topic,
        actor_id: actor(),
        realm_id: realm(),
        identity: SyncQuarantineIdentity {
            topic,
            actor: actor(),
            actor_seq: 1,
        },
        event,
    };
    assert_eq!(batch.first(), Ok(Next::Call(plan_call(plan))));
}

#[test]
fn coalesces_config_runs() {
    let (one, two) = (run_event(1), run_event(2));
    let mut batch = operation(vec![wrapped(one.clone()), wrapped(two.clone())]);
    assert_eq!(batch.first(), check(one.clone()));
    assert_eq!(batch.next(accepted()), check(two.clone()));
    assert_eq!(batch.next(accepted()), run_plan(vec![one, two]));
    let outcome = done(batch.next(child(())));
    assert_eq!(
        outcome.applied_targets,
        vec![config_target(), config_target()]
    );
    assert_eq!(
        outcome.satisfied,
        BTreeSet::from([DocumentSyncDependency::RealmConfig(realm())])
    );
}

#[test]
fn flushes_before_others() {
    let (run, single) = (run_event(1), single_event(2));
    let mut batch = operation(vec![wrapped(run.clone()), wrapped(single.clone())]);
    assert_eq!(batch.first(), check(run.clone()));
    assert_eq!(batch.next(accepted()), run_plan(vec![run]));
    assert_eq!(batch.next(child(())), check(single.clone()));
    assert_eq!(batch.next(accepted()), admin_plan(single));
    let follow = AdminFollowUp {
        sync: true,
        retry_parked: Some(realm()),
    };
    assert_eq!(batch.next(child(follow)), Ok(Next::Call(IoCall::Sync)));
    let parked = RetryParkedPlan { realm_id: realm() };
    assert_eq!(
        batch.next(IoResult::Done),
        Ok(Next::Call(plan_call(parked)))
    );
    assert_eq!(done(batch.next(child(()))).applied_targets.len(), 2);
}

#[test]
fn flags_full_parking() {
    let snapshot = snapshot_event(1, 1);
    let mut batch = operation(vec![wrapped(snapshot.clone())]);
    batch.first().unwrap();
    let plan = DistributionPlan {
        target: config_target(),
        event: snapshot,
    };
    assert_eq!(batch.next(accepted()), Ok(Next::Call(plan_call(plan))));
    let outcome = done(batch.next(child(Handled::Full)));
    assert!(outcome.overflowed);
    assert_eq!(outcome.applied_targets, vec![config_target()]);
}

#[test]
fn records_rejected_admin() {
    let mut batch = operation(vec![wrapped(single_event(1))]);
    batch.first().unwrap();
    let refused = checked(AdminEventValidation::Rejected("no".to_string()));
    let outcome = done(batch.next(refused));
    assert_eq!(outcome.rejections[0].reason, "no");
    assert!(outcome.applied_targets.is_empty());
}

#[test]
fn retries_deferred_admin() {
    let (waits, helps) = (single_event(1), single_event(2));
    let mut batch = operation(vec![wrapped(waits.clone()), wrapped(helps.clone())]);
    batch.first().unwrap();
    assert_eq!(batch.next(deferred(None)), check(helps.clone()));
    assert_eq!(batch.next(accepted()), admin_plan(helps));
    assert_eq!(
        batch.next(child(AdminFollowUp::default())),
        check(waits.clone())
    );
    assert_eq!(batch.next(accepted()), admin_plan(waits));
    let outcome = done(batch.next(child(AdminFollowUp::default())));
    assert_eq!(outcome.applied_targets.len(), 2);
    assert!(outcome.rejections.is_empty());
}

#[test]
fn settles_unresolved_admin() {
    let dependency = DocumentSyncDependency::RealmAuthorization(realm());
    let (crossing, local) = (single_event(1), single_event(2));
    let mut batch = operation(vec![wrapped(crossing.clone()), wrapped(local.clone())]);
    batch.first().unwrap();
    assert_eq!(batch.next(deferred(Some(dependency))), check(local.clone()));
    assert_eq!(batch.next(deferred(None)), check(crossing));
    assert_eq!(batch.next(deferred(Some(dependency))), check(local));
    let outcome = done(batch.next(deferred(None)));
    assert_eq!(outcome.cross_topic, BTreeSet::from([dependency]));
    assert_eq!(outcome.rejections.len(), 1);
    assert_eq!(outcome.rejections[0].reason, "waiting");
}

#[test]
fn stops_on_failure() {
    let mut batch = operation(vec![wrapped(single_event(1))]);
    batch.first().unwrap();
    let error = ApplyError::Failed("broken".to_string());
    let answer = failed::<(AdminEventValidation, Option<Arc<ConfigSnapshot>>)>(error.clone());
    assert_eq!(batch.next(answer), Err(error));
}

#[test]
fn refuses_early_results() {
    let mut batch = operation(Vec::new());
    assert_eq!(
        batch.next(IoResult::Done),
        Err(ApplyError::from(IoError::Unfinished))
    );
}
