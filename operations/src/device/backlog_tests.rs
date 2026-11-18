//! Tests the device queue drain: paging, due entries and what asks for another pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::types::Key;

use super::*;

#[derive(Clone, Debug, PartialEq)]
struct Entry {
    due_at_ms: u64,
}

impl RetryView for Entry {
    fn due_at_ms(&self) -> Option<u64> {
        Some(self.due_at_ms)
    }

    fn attempts(&self) -> u32 {
        0
    }
}

#[derive(Debug, PartialEq)]
struct Queue;

impl DrainQueue for Queue {
    type Entry = Entry;

    fn page(&self) -> (&'static str, usize) {
        ("queue", 2)
    }

    fn entry(&self, value: &Value) -> Option<Entry> {
        let due_at_ms = u64::from(*value.first()?);
        (due_at_ms != 255).then_some(Entry { due_at_ms })
    }

    fn forward(&self, _entry: Entry) -> Effect {
        Effect::Delay(Duration::ZERO)
    }

    fn rechecks(&self, outcome: ChildOutcome) -> bool {
        outcome_rechecks::<IoError>(outcome)
    }
}

fn drain() -> DrainOperation<Queue> {
    DrainOperation::new(Queue, 10, 3)
}

fn page(rows: &[u8], next: Option<u8>) -> IoResult {
    let rows = (rows.iter())
        .map(|due| (Key::from(vec![*due]), Value::from(vec![*due])))
        .collect();
    let next = next.map(|key| Key::from(vec![key]));
    let answer = IoResult::Page { rows, next };
    IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(answer)))
}

fn member(outcome: Result<ForwardOutcome, IoError>) -> IoResult {
    let event = Event::Child(ChildOutcome::new(outcome));
    IoResult::Group(GroupEvent::Member {
        index: 0,
        result: GroupResult::Done(Box::new(event)),
    })
}

fn closed() -> IoResult {
    IoResult::Group(GroupEvent::Closed)
}

fn window_size(next: Result<Next<DrainOutcome>, IoError>) -> (usize, usize) {
    let Ok(Next::Call(IoCall::Group(group))) = next else {
        panic!("expected the forward window");
    };
    assert_eq!(group.form, GroupForm::Window);
    (group.members.len(), group.limit)
}

// Only due entries are forwarded, at most the limit at once; undecodable rows are skipped.
#[test]
fn forwards_due_entries() {
    let mut drain = drain();
    drain.first().unwrap();
    let window = drain.next(page(&[1, 10, 11, 255], None));
    assert_eq!(window_size(window), (2, 3));
}

// A settled forward leaves the pass idle; one that asks again or fails makes it recheck.
#[test]
fn rechecks_on_request() {
    for (outcome, expected) in [
        (Ok(ForwardOutcome::Settled), DrainOutcome::Idle),
        (Ok(ForwardOutcome::Recheck), DrainOutcome::Recheck),
        (Err(IoError::Unfinished), DrainOutcome::Recheck),
    ] {
        let mut drain = drain();
        drain.first().unwrap();
        drain.next(page(&[1], None)).unwrap();
        assert_eq!(drain.next(member(outcome)), Ok(Next::Wait));
        assert_eq!(drain.next(closed()), Ok(Next::Done(expected)));
    }
}

// Entries that are not due do not end the walk: the next page is still read.
#[test]
fn pages_past_future() {
    let mut drain = drain();
    drain.first().unwrap();
    let next = drain.next(page(&[99], Some(99))).unwrap();
    assert!(matches!(next, Next::Call(IoCall::Child(_))));
    assert_eq!(drain.cursor, Some(Key::from(vec![99])));
    assert_eq!(drain.next(page(&[], None)), Ok(Next::Done(DrainOutcome::Idle)));
}

// A page that cannot be read defers the pass, whatever earlier pages forwarded.
#[test]
fn defers_failed_page() {
    let mut drain = drain();
    drain.first().unwrap();
    let failed = IoResult::Child(ChildOutcome::new(Err::<IoResult, _>(IoError::Unfinished)));
    assert_eq!(drain.next(failed), Ok(Next::Done(DrainOutcome::Deferred)));
}
