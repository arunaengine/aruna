//! Tests the watch expansion as calls: the guard reads in its transaction and the retry row of
//! the next page within the queue's caps.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::notifications::user;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::notification_watch::{WatchEventKind, WatchEventMask};
use ulid::Ulid;

const REALM: RealmId = RealmId([4; 32]);

fn node() -> NodeId {
    crate::tests::locations::node_id(1)
}

fn event(seed: u8) -> WatchEvent {
    WatchEvent {
        event_id: Ulid::from_bytes([seed; 16]),
        realm_id: REALM,
        kind: WatchEventKind::DataUploaded,
        path: "data/bucket/a.csv".to_string(),
        actor: user(4, 2),
        occurred_at_ms: 1,
        detail: WatchEventDetail::DataUploaded {
            group_id: Ulid::from_bytes([5; 16]),
            node_id: node(),
            bucket: "bucket".to_string(),
            key: "a.csv".to_string(),
            size_bytes: 9,
        },
    }
}

fn expanding(retry: Option<(Vec<u8>, Option<Vec<u8>>)>) -> WatchExpandOperation {
    let config = RealmConfigDocument::new(REALM, Vec::new(), 3);
    WatchExpandOperation::new(REALM, config, node(), vec![event(1)], retry)
}

fn retry_row(seed: u8, cursor: &[u8]) -> (Key, Value) {
    let retry = WatchEventRetry {
        events: vec![event(seed)],
        cursor: Some(cursor.to_vec()),
    };
    let key = watch_retry_key(REALM, event(seed).event_id);
    (key.into(), postcard::to_allocvec(&retry).unwrap().into())
}

/// An expansion with the next page waiting, ready to write its retry row.
fn retrying(cursor: &[u8]) -> WatchExpandOperation {
    let mut operation = expanding(None);
    operation.retry_key = Some(watch_retry_key(REALM, event(1).event_id));
    let retry = WatchEventRetry {
        events: vec![event(1)],
        cursor: Some(cursor.to_vec()),
    };
    operation.retry_value = Some(postcard::to_allocvec(&retry).unwrap());
    operation.next = Some(cursor.to_vec());
    operation
}

#[test]
fn edge_batches() {
    let config = RealmConfigDocument::new(REALM, Vec::new(), 3);
    let mut empty = WatchExpandOperation::new(REALM, config.clone(), node(), Vec::new(), None);
    assert_eq!(empty.first(), Ok(Next::Done(ExpandOutcome::default())));
    let events = vec![event(1); EVENT_BATCH_SIZE + 1];
    let mut over = WatchExpandOperation::new(REALM, config, node(), events, None);
    assert!(
        matches!(over.first(), Err(ExpandError::Fatal(error)) if error.contains("exceeds cap"))
    );
    let mut list = expanding(None);
    assert!(matches!(list.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert!(list.transaction() == TransactionMode::Write);
}

/// The realm's and the event group's authorization rows are read in the transaction, so a
/// change to them conflicts at commit; a changed watch row conflicts at once.
#[test]
fn guard_reads_authorization() {
    let mut operation = expanding(None);
    let mut watch = WatchSubscription::new(
        user(4, 2),
        "data/bucket/".to_string(),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        1,
    );
    watch.watch_id = Ulid::from_bytes([6; 16]);
    let candidates: VecDeque<_> = [Candidate {
        subscription: watch.clone(),
        event: event(1),
        routed: Vec::new(),
    }]
    .into();
    let reads = operation.guard_reads(&candidates);
    let realm_row = (
        AUTH_KEYSPACE.to_string(),
        Key::from(REALM.as_bytes().to_vec()),
    );
    let group_row = (AUTH_KEYSPACE.to_string(), Key::from(vec![5; 16]));
    assert_eq!(reads.len(), 3);
    assert!(reads.contains(&realm_row) && reads.contains(&group_row));
    let changed = vec![Some(Value::from(vec![1])), None, None];
    assert_eq!(operation.guard_checked(changed), Err(ExpandError::Conflict));
}

/// A drained retry row that is gone meanwhile is a conflict; a present one is deleted.
#[test]
fn drained_row_deleted() {
    let key = watch_retry_key(REALM, event(1).event_id);
    let mut operation = expanding(Some((key.clone(), None)));
    assert!(matches!(
        operation.retry_change(),
        Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 1
    ));
    assert!(matches!(
        operation.next(IoResult::Values(vec![Some(Value::from(vec![1]))])),
        Ok(Next::Call(IoCall::Delete(rows))) if rows.len() == 1
    ));
    let mut gone = expanding(Some((key, None)));
    gone.retry_change().unwrap();
    assert_eq!(
        gone.next(IoResult::Values(vec![None])),
        Err(ExpandError::Conflict)
    );
}

/// A stored retry row further along stays; a new row past the row cap is refused, while an
/// existing row still advances.
#[test]
fn retry_row_bounded() {
    let behind = watch_subscription_key(user(4, 2), Ulid::nil()).to_vec();
    let ahead = watch_subscription_key(user(4, 9), Ulid::nil()).to_vec();
    let own = retry_row(1, &ahead);
    let mut kept = retrying(&behind);
    let Ok(Next::Call(IoCall::Write(rows))) = kept.write_retry(vec![own.clone()]) else {
        panic!("the retry row is written");
    };
    assert_eq!(rows[0].2, own.1, "the row further along stays");

    let full: Vec<_> = (20..20 + RETRY_BATCH_CAP as u8)
        .map(|seed| retry_row(seed, &behind))
        .collect();
    let mut refused = retrying(&behind);
    assert!(matches!(
        refused.write_retry(full.clone()),
        Err(ExpandError::Fatal(error)) if error.contains("row cap")
    ));
    let mut advancing = retrying(&ahead);
    let mut queued = full;
    queued[0] = retry_row(1, &behind);
    assert!(matches!(
        advancing.write_retry(queued),
        Ok(Next::Call(IoCall::Write(_)))
    ));
}

/// A queued next page asks for an interest publish after the commit.
#[test]
fn queued_page_publishes() {
    let mut operation = retrying(b"cursor");
    operation.queue_changed = true;
    operation.state = ExpandState::Marker;
    assert!(matches!(
        operation.next(IoResult::Done),
        Ok(Next::Then { calls, .. }) if calls.len() == 1
    ));
    let listed = IoResult::Child(ChildOutcome::new(Ok::<_, WatchSubscriptionError>(
        WatchPage {
            subscriptions: Vec::new(),
            next: None,
        },
    )));
    let mut quiet = expanding(None);
    quiet.first().unwrap();
    assert_eq!(quiet.next(listed), Ok(Next::Done(ExpandOutcome::default())));
}

/// Valid retry rows expand one at a time; a malformed row stays queued and keeps the marker.
#[test]
fn drain_defers_failures() {
    let config = RealmConfigDocument::new(REALM, Vec::new(), 3);
    let mut drain = RetryDrainOperation::new(REALM, config, node());
    assert!(matches!(drain.first(), Ok(Next::Call(IoCall::Effect(_)))));
    let cursor = watch_subscription_key(user(4, 2), Ulid::nil()).to_vec();
    let (key, value) = retry_row(1, &cursor);
    let malformed = (
        Key::from(watch_retry_key(REALM, event(2).event_id)),
        Value::from(vec![1]),
    );
    let scanned = IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
        values: vec![(key, value), malformed],
        next_start_after: None,
    })));
    assert!(matches!(
        drain.next(scanned),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let expanded = IoResult::Child(ChildOutcome::new(Ok::<_, ExpandError>(
        ExpandOutcome::default(),
    )));
    assert!(
        matches!(drain.next(expanded), Ok(Next::Call(IoCall::Child(_)))),
        "the malformed row keeps the realm's marker"
    );
    let marked = IoResult::Child(ChildOutcome::new(Ok::<_, InterestError>(true)));
    assert_eq!(
        drain.next(marked),
        Err(RetryDrainError(
            "watch retry drain deferred work".to_string()
        ))
    );

    let config = RealmConfigDocument::new(REALM, Vec::new(), 3);
    let mut empty = RetryDrainOperation::new(REALM, config, node());
    empty.first().unwrap();
    let none = IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    })));
    assert_eq!(empty.next(none), Ok(Next::Done(())));
}
