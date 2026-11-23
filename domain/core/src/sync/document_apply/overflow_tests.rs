//! Tests the calls and results of replaying overflowed distribution snapshots.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::fixtures::{
    actor, check_plan, checked, child, config_target, config_topic, realm, single_event,
    snapshot_event, wrapped,
};
use super::*;

type Answer = Result<Next<()>, ApplyError>;

fn value(byte: u8) -> Value {
    Value::from(vec![byte])
}

fn scan_key() -> Key {
    topic_key(SCAN_PREFIX, config_topic())
}

fn full_key() -> Key {
    topic_key(FULL_PREFIX, config_topic())
}

fn checkpoint() -> Key {
    overflow_key(config_topic())
}

fn page_call_for(start: Value) -> Answer {
    let effect = DocumentEffect::OverflowPage {
        topic: config_topic(),
        start,
        applied: Some(value(9)),
    };
    let effect = Effect::Net(NetEffect::DocumentSync(effect));
    Ok(Next::Call(IoCall::Effect(Box::new(effect))))
}

fn answer(page: OverflowPage) -> IoResult {
    let event = DocumentNetEvent::OverflowPage(Box::new(page));
    IoResult::Event(Box::new(Event::Net(NetEvent::DocumentSync(event))))
}

fn page_of(at: u8, next: u8, events: Vec<AdminDocumentEvent>) -> IoResult {
    answer(OverflowPage::Page {
        at: value(at),
        next: value(next),
        events: events
            .into_iter()
            .map(|event| (wrapped(event), actor()))
            .collect(),
    })
}

fn check(event: AdminDocumentEvent) -> Answer {
    Ok(Next::Call(plan_call(check_plan(event))))
}

fn install(event: AdminDocumentEvent) -> Answer {
    Ok(Next::Call(plan_call(DistributionPlan {
        target: config_target(),
        event,
    })))
}

fn accepted() -> IoResult {
    checked(AdminEventValidation::Accepted)
}

fn call(call: IoCall) -> Answer {
    Ok(Next::Call(call))
}

/// An operation that has read the sweep rows and asked for its first page.
fn sweeping(full: Option<Value>) -> ReplayOverflowOperation {
    let mut operation = ReplayOverflowOperation::new(realm());
    operation.first().unwrap();
    let listed = IoResult::Page {
        rows: vec![(checkpoint(), value(1))],
        next: None,
    };
    let reads = [scan_key(), full_key(), topic_cursor_key(config_topic())]
        .into_iter()
        .map(|key| (APPLIED_OPS_KEYSPACE.to_string(), key))
        .collect();
    assert_eq!(operation.next(listed), call(IoCall::Read(reads)));
    let stored = IoResult::Values(vec![None, full, Some(value(9))]);
    assert_eq!(operation.next(stored), page_call_for(value(1)));
    operation
}

#[test]
fn lists_checkpoints() {
    let mut operation = ReplayOverflowOperation::new(realm());
    let list = IoCall::Page {
        space: APPLIED_OPS_KEYSPACE.to_string(),
        prefix: ByteView::from(OVERFLOW_PREFIX.to_vec()),
        after: None,
        limit: MAX_OVERFLOW_TOPICS,
    };
    assert_eq!(operation.first(), call(list));
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(operation.next(empty), Ok(Next::Done(())));
}

#[test]
fn removes_malformed_checkpoint() {
    let mut operation = ReplayOverflowOperation::new(realm());
    operation.first().unwrap();
    let key = Key::from(OVERFLOW_PREFIX.to_vec());
    let listed = IoResult::Page {
        rows: vec![(key.clone(), value(1))],
        next: None,
    };
    assert_eq!(operation.next(listed), call(delete_rows(vec![key])));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn resumes_saved_scan() {
    let mut operation = ReplayOverflowOperation::new(realm());
    operation.first().unwrap();
    let listed = IoResult::Page {
        rows: vec![(checkpoint(), value(1))],
        next: None,
    };
    operation.next(listed).unwrap();
    let stored = IoResult::Values(vec![Some(value(4)), None, Some(value(9))]);
    assert_eq!(operation.next(stored), page_call_for(value(4)));
}

#[test]
fn forgets_gone_topic() {
    let mut operation = sweeping(None);
    let keys = vec![scan_key(), full_key(), checkpoint()];
    assert_eq!(
        operation.next(answer(OverflowPage::TopicGone)),
        call(delete_rows(keys))
    );
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn clears_finished_sweep() {
    let mut operation = sweeping(None);
    assert_eq!(
        operation.next(answer(OverflowPage::Finished)),
        call(delete_rows(vec![scan_key()]))
    );
    assert_eq!(
        operation.next(IoResult::Done),
        call(delete_rows(vec![checkpoint()]))
    );
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn moves_checkpoint_back() {
    let mut operation = sweeping(Some(value(5)));
    operation.next(answer(OverflowPage::Finished)).unwrap();
    assert_eq!(
        operation.next(IoResult::Done),
        call(write_row(checkpoint(), value(5)))
    );
    assert_eq!(
        operation.next(IoResult::Done),
        call(delete_rows(vec![full_key()]))
    );
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn marks_first_full() {
    let (one, two) = (snapshot_event(1, 1), snapshot_event(2, 2));
    let mut operation = sweeping(None);
    let page = page_of(3, 4, vec![one.clone(), two.clone()]);
    assert_eq!(operation.next(page), check(one.clone()));
    assert_eq!(operation.next(accepted()), install(one));
    assert_eq!(
        operation.next(child(Handled::Full)),
        call(write_row(full_key(), value(3)))
    );
    assert_eq!(operation.next(IoResult::Done), check(two.clone()));
    assert_eq!(operation.next(accepted()), install(two));
    assert_eq!(
        operation.next(child(Handled::Full)),
        page_call_for(value(4))
    );
}

#[test]
fn skips_other_events() {
    let mut operation = sweeping(None);
    let page = page_of(3, 4, vec![single_event(1), snapshot_event(2, 1)]);
    assert_eq!(operation.next(page), check(snapshot_event(2, 1)));
    let refused = checked(AdminEventValidation::Rejected("no".to_string()));
    assert_eq!(operation.next(refused), page_call_for(value(4)));
}

#[test]
fn reports_replay_forks() {
    let (first, fork, later) = (
        snapshot_event(1, 1),
        snapshot_event(2, 1),
        snapshot_event(3, 1),
    );
    let mut operation = sweeping(None);
    let events = vec![first.clone(), fork.clone(), later.clone()];
    operation.next(page_of(3, 4, events)).unwrap();
    operation.next(accepted()).unwrap();
    assert_eq!(operation.next(child(Handled::Done)), check(fork.clone()));
    let forked = ReplayForkPlan {
        earlier: first.clone(),
        event: fork,
    };
    assert_eq!(operation.next(accepted()), call(plan_call(forked)));
    assert_eq!(operation.next(child(true)), check(later.clone()));
    let clean = ReplayForkPlan {
        earlier: first,
        event: later.clone(),
    };
    assert_eq!(operation.next(accepted()), call(plan_call(clean)));
    assert_eq!(operation.next(child(false)), install(later));
}

#[test]
fn saves_scan_position() {
    let mut operation = sweeping(None);
    for page in 1..REPLAY_PAGES as u8 {
        let result = operation.next(page_of(page, page + 1, Vec::new()));
        assert_eq!(result, page_call_for(value(page + 1)));
    }
    let last = REPLAY_PAGES as u8;
    assert_eq!(
        operation.next(page_of(last, last + 1, Vec::new())),
        call(write_row(scan_key(), value(last + 1)))
    );
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn reports_page_failure() {
    let mut operation = sweeping(None);
    let event = DocumentNetEvent::Error {
        target: None,
        error: "offline".to_string(),
    };
    let failed = IoResult::Event(Box::new(Event::Net(NetEvent::DocumentSync(event))));
    assert_eq!(
        operation.next(failed),
        Err(ApplyError::Failed("offline".to_string()))
    );
    let mut operation = sweeping(None);
    assert!(matches!(
        operation.next(IoResult::Done),
        Err(ApplyError::Failed(_))
    ));
}
