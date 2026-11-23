//! Tests the interest publish, marker cleanup and cache refresh as calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::locations::node_id;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::notification_watch::interest_dirty_key;

const REALM: RealmId = RealmId([7; 32]);

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn marker(realm_id: RealmId, generation: u8) -> (Key, Value) {
    (
        Key::from(interest_dirty_key(realm_id)),
        Value::from(vec![generation]),
    )
}

/// Only markers whose generation did not move on are deleted, in one write transaction.
#[test]
fn markers_by_generation() {
    let (kept, moved) = (marker(REALM, 1), marker(RealmId([8; 32]), 1));
    let mut clear = ClearMarkersOperation::new(vec![kept.clone(), moved]);
    assert_eq!(clear.transaction(), TransactionMode::Write);
    assert!(matches!(clear.first(), Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 2));
    let current = IoResult::Values(vec![Some(kept.1.clone()), Some(Value::from(vec![2]))]);
    let delete = vec![(WATCH_INTEREST_KEYSPACE.to_string(), kept.0)];
    assert_eq!(clear.next(current), Ok(Next::Call(IoCall::Delete(delete))));
    assert_eq!(clear.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn publish_needs_markers() {
    let mut offline = PublishInterestOperation::new(None);
    assert_eq!(offline.first(), Ok(Next::Done(false)));
    let mut quiet = PublishInterestOperation::new(Some(node_id(1)));
    assert!(matches!(quiet.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(quiet.next(empty), Ok(Next::Done(false)));
}

/// One dirty realm: its config, retry drain, watch list and filter build the digest; a changed
/// digest is written with its pending marker, replicated, and the markers are cleared before
/// the interest cache is refreshed.
#[test]
fn publish_changed_digest() {
    let mut publish = PublishInterestOperation::new(Some(node_id(1)));
    publish.first().unwrap();
    let markers = IoResult::Page {
        rows: vec![marker(REALM, 1)],
        next: None,
    };
    assert!(matches!(
        publish.next(markers),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let config = RealmConfigDocument::new(REALM, Vec::new(), 3);
    let drain = publish.next(child::<_, GetConfigError>(Ok(config)));
    assert!(matches!(drain, Ok(Next::Call(IoCall::Child(_)))));
    let failed = RetryDrainError("deferred".to_string());
    let listed = publish.next(child::<(), _>(Err(failed)));
    assert!(
        matches!(listed, Ok(Next::Call(IoCall::Child(_)))),
        "a failed drain only warns"
    );
    let page = WatchPage {
        subscriptions: Vec::new(),
        next: None,
    };
    let filter = publish.next(child::<_, WatchSubscriptionError>(Ok(page)));
    assert!(matches!(filter, Ok(Next::Call(IoCall::Child(_)))));
    let filtered = AuthorizedWatchSubscriptions::default();
    let read = publish.next(child::<_, WatchCheckError>(Ok(filtered)));
    assert!(matches!(read, Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 2));
    let written = publish.next(IoResult::Values(vec![None, None]));
    assert!(matches!(written, Ok(Next::Call(IoCall::Write(rows))) if rows.len() == 2));
    let replicate = publish.next(IoResult::Done);
    assert!(matches!(replicate, Ok(Next::Call(IoCall::Child(_)))));
    let pending = publish.next(child::<_, ReplicateDocumentsError>(Ok(())));
    assert!(matches!(pending, Ok(Next::Call(IoCall::Delete(rows))) if rows.len() == 1));
    let clear = publish.next(IoResult::Done);
    assert!(matches!(clear, Ok(Next::Call(IoCall::Child(_)))));
    let conflicted = publish.next(child::<(), _>(Err(IoError::Conflict)));
    assert!(
        matches!(conflicted, Ok(Next::Call(IoCall::Child(_)))),
        "a re-dirtied marker stays and the cache is still refreshed"
    );
    let Ok(Next::Then { output, calls }) =
        publish.next(child::<_, InterestError>(Ok(WatchInterestTable::default())))
    else {
        panic!("the refreshed cache is handed to the net state");
    };
    assert!(output);
    assert_eq!(calls.len(), 1);
}

#[test]
fn publish_failures_stop() {
    let mut publish = PublishInterestOperation::new(Some(node_id(1)));
    publish.first().unwrap();
    let markers = IoResult::Page {
        rows: vec![marker(REALM, 1)],
        next: None,
    };
    publish.next(markers).unwrap();
    let missing = child::<RealmConfigDocument, _>(Err(GetConfigError::DocumentNotFound));
    assert!(
        matches!(publish.next(missing), Err(PublishError(error)) if error.contains("realm config"))
    );
}

/// Changed watches mark their realm; changed digests rebuild that realm's cache, which a failed
/// rebuild clears.
#[test]
fn refresh_rebuilds_realms() {
    let owner = crate::tests::notifications::user(7, 2);
    let targets = [
        DocumentTarget::WatchSubscription {
            owner,
            watch_id: ulid::Ulid::nil(),
        },
        DocumentTarget::WatchInterest {
            realm_id: REALM,
            node_id: node_id(2),
        },
    ];
    let mut refresh = RefreshInterestOperation::new(&targets, REALM);
    assert!(matches!(refresh.first(), Ok(Next::Call(IoCall::Child(_)))));
    let rebuild = refresh.next(child::<_, InterestError>(Ok(true)));
    assert!(matches!(rebuild, Ok(Next::Call(IoCall::Child(_)))));
    let failed = child::<WatchInterestTable, _>(Err(InterestError::Io(IoError::Unfinished)));
    let update = NetEffect::UpdateRealmInterest {
        realm_id: REALM,
        nodes: Default::default(),
    };
    assert_eq!(
        refresh.next(failed),
        Ok(Next::Then {
            output: (),
            calls: vec![IoCall::from(Effect::Net(update))],
        })
    );
    let mut idle = RefreshInterestOperation::new(&[], REALM);
    assert_eq!(idle.first(), Ok(Next::Done(())));
}
