//! Tests refreshing device replicas from a holder's graph state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::structs::identity::realm::RealmNodeKind;

use super::*;
use crate::device::replica::ReplicaOrigin;
use crate::metadata::protocol::encode_message;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn owner() -> UserId {
    UserId::nil(realm())
}

fn replica() -> ReplicaRecord {
    let (document_id, group_id) = (Ulid::from_bytes([2; 16]), Ulid::from_bytes([1; 16]));
    ReplicaRecord::new(document_id, group_id, "notes".to_string(), ReplicaOrigin::Realm)
}

fn config() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 1);
    let kind = RealmNodeKind::User { owner: owner() };
    config.ensure_node(node(1), kind);
    config
}

fn plan() -> RefreshPlan {
    RefreshPlan {
        realm_id: realm(),
        owner: owner(),
        config: config(),
    }
}

fn refresh(holders: &[u8]) -> PlannedRefreshOperation {
    let mut refresh = PlannedRefreshOperation::new(plan(), replica());
    refresh.holders = holders.iter().map(|seed| node(*seed)).collect();
    refresh
}

fn answer(result: Result<Vec<u8>, PeerRequestError>, elapsed: Duration) -> IoResult {
    let answer = MetadataAnswer {
        peer: node(2),
        elapsed,
        result,
    };
    let event = Event::Net(NetEvent::MetadataAnswer(Box::new(answer)));
    IoResult::Event(Box::new(event))
}

fn refused(refusal: SyncRefusal) -> IoResult {
    let message = MetadataTransportMessage::FetchedGraphState {
        result: Err(refusal),
    };
    answer(Ok(encode_message(&message).unwrap()), Duration::ZERO)
}

fn asks_peer(next: &Refreshing) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Effect(_))))
}

// An owner comes from the device's own node entry; another node kind has none.
#[test]
fn plan_needs_owner() {
    let ids = Some((node(1), realm()));
    assert_eq!(RefreshPlan::of(ids, Some(config())), Some(plan()));
    assert_eq!(RefreshPlan::of(Some((node(3), realm())), Some(config())), None);
    assert_eq!(RefreshPlan::of(ids, None), None);
    assert_eq!(RefreshPlan::of(None, Some(config())), None);
}

// A holder that cannot serve leaves the next one to try; none left is silence.
#[test]
fn tries_next_holder() {
    let mut refresh = refresh(&[2, 3]);
    assert!(asks_peer(&refresh.first()));
    let failed = answer(Err(PeerRequestError::TimedOut), Duration::from_secs(1));
    assert!(asks_peer(&refresh.next(failed)));
    assert_eq!(refresh.budget, REFRESH_BUDGET - Duration::from_secs(1));
    let next = refresh.next(refused(SyncRefusal::NotFound));
    assert_eq!(next, Ok(Next::Done(RefreshOutcome::Unreachable)));
}

// An exhausted budget ends the exchange before another holder is asked.
#[test]
fn budget_ends_exchange() {
    let mut refresh = refresh(&[2, 3]);
    refresh.first().unwrap();
    let slow = answer(Err(PeerRequestError::TimedOut), REFRESH_BUDGET);
    let next = refresh.next(slow);
    assert_eq!(next, Ok(Next::Done(RefreshOutcome::Unreachable)));
}

// An authorization verdict holds at every holder and is noted on the replica.
#[test]
fn verdict_marks_replica() {
    let mut refresh = refresh(&[2, 3]);
    refresh.first().unwrap();
    let next = refresh.next(refused(SyncRefusal::Forbidden)).unwrap();
    assert!(matches!(next, Next::Call(IoCall::Child(_))));
    assert_eq!(refresh.replica.state, ReplicaState::Failed);
    let reason = refusal_reason(&SyncRefusal::Forbidden);
    assert_eq!(refresh.replica.last_error, Some(reason));
    let stored = IoResult::Child(ChildOutcome::new(Ok::<(), IoError>(())));
    let outcome = RefreshOutcome::Refused(SyncRefusal::Forbidden);
    assert_eq!(refresh.next(stored), Ok(Next::Done(outcome)));
}

// A pass needs this node's identity before it can ask anybody.
#[test]
fn needs_transport() {
    let mut one = RefreshReplicaOperation::new(None, replica().document_id);
    assert_eq!(one.first(), Ok(Next::Done(RefreshOutcome::Unreachable)));
}
