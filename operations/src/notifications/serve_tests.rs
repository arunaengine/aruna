//! Tests the inbox and watch requests of a peer as calls: the realm and holder checks, the
//! answers of each request kind, the wake after a mark and the publish after a new watch.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::notifications::watch::paths::WatchAuthorizationDenial;
use crate::tests::notifications::{peer_input, user};
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::notification_watch::{WatchEventKind, WatchEventMask};
use ulid::Ulid;

fn serving(request: InboxRequest) -> ServeInboxOperation {
    ServeInboxOperation::new(peer_input(), user(1, 2), request)
}

fn rejected(turn: AnswerTurn) -> String {
    match turn {
        Ok(Next::Done(NotificationTransportMessage::Reject(reason))) => reason,
        other => panic!("expected a rejection, got {other:?}"),
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn create() -> InboxRequest {
    InboxRequest::CreateWatch {
        path_prefix: format!("meta/{}/", Ulid::from_parts(5, 5)),
        event_mask: WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        authorization: WatchAuthorizationBinding::default(),
    }
}

#[test]
fn refused_before_reads() {
    let mut foreign = ServeInboxOperation::new(peer_input(), user(9, 2), InboxRequest::ListWatches);
    assert!(rejected(foreign.first()).contains("foreign realm"));
    let many = HolderRead::Mark {
        ids: vec![Ulid::nil(); MARK_MAX_IDS + 1],
        up_to_ms: None,
        now_ms: 1,
    };
    assert!(rejected(serving(InboxRequest::Read(many)).first()).contains("exceeds cap"));
    let mut missing = serving(InboxRequest::ListWatches);
    assert!(matches!(missing.first(), Ok(Next::Call(IoCall::Read(_)))));
    let turn = missing.next(IoResult::Values(vec![None]));
    assert!(rejected(turn).contains("config unavailable"));
}

/// Each read answers with its own frame; a mark that changed rows wakes the user first.
#[test]
fn reads_answered() {
    let mut list = serving(InboxRequest::Read(HolderRead::Unread));
    list.state = ServeState::Read;
    let counted = HolderAnswer::Unread {
        count: 3,
        capped: false,
    };
    assert_eq!(
        list.next(child::<_, HolderReadError>(Ok(counted))),
        Ok(Next::Done(
            NotificationTransportMessage::UnreadCountResult {
                count: 3,
                capped: false
            }
        ))
    );

    let mut mark = serving(InboxRequest::Read(HolderRead::Unread));
    mark.state = ServeState::Read;
    let wake = mark.next(child::<_, HolderReadError>(Ok(HolderAnswer::Marked(2))));
    assert_eq!(
        wake,
        Ok(Next::Call(IoCall::from(Effect::Net(NetEffect::WakeInbox(
            vec![user(1, 2)]
        )))))
    );
    let woken = IoResult::Event(Box::new(Event::Net(NetEvent::InboxWoken)));
    assert_eq!(
        mark.next(woken),
        Ok(Next::Done(NotificationTransportMessage::MarkReadResult {
            marked: 2
        }))
    );

    let mut failed = serving(InboxRequest::Read(HolderRead::Unread));
    failed.state = ServeState::Read;
    let down = HolderReadError::Unavailable("down".to_string());
    assert_eq!(
        rejected(failed.next(child::<HolderAnswer, _>(Err(down)))),
        "down"
    );
}

/// A denied forwarded watch is refused with its reason; a created one asks for a publish.
#[test]
fn watch_created() {
    let mut denied = serving(create());
    denied.state = ServeState::Forwarded;
    let reason = WatchAuthorizationDenial::PermissionDenied;
    let refused = denied.next(child::<_, WatchCheckError>(Ok(WatchAuthorization::Denied(
        reason,
    ))));
    assert!(rejected(refused).starts_with(WATCH_SUBSCRIPTION_UNAUTHORIZED));

    let mut created = serving(create());
    created.state = ServeState::Forwarded;
    let allowed = created.next(child::<_, WatchCheckError>(Ok(
        WatchAuthorization::Authorized,
    )));
    assert!(matches!(allowed, Ok(Next::Call(IoCall::Child(_)))));
    let subscription = WatchSubscription::new(
        user(1, 2),
        format!("meta/{}/", Ulid::from_parts(5, 5)),
        WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        1,
    );
    let stored = created.next(child::<_, WatchSubscriptionError>(Ok(subscription.clone())));
    assert!(matches!(stored, Ok(Next::Call(IoCall::Child(_)))));
    let published = created.next(child::<_, InterestError>(Ok(true)));
    assert_eq!(
        published,
        Ok(Next::Done(NotificationTransportMessage::WatchCreated {
            subscription
        }))
    );
}

#[test]
fn unexpected_results_fail() {
    let mut list = serving(InboxRequest::ListWatches);
    list.first().unwrap();
    assert!(list.next(IoResult::Done).is_err());
}

fn watch_event(realm_id: aruna_core::structs::identity::realm::RealmId) -> WatchEvent {
    use aruna_core::structs::execution::notification_watch::WatchEventDetail;
    WatchEvent {
        event_id: Ulid::from_parts(3, 3),
        realm_id,
        kind: WatchEventKind::DataUploaded,
        path: "data/bucket/a.csv".to_string(),
        actor: user(1, 2),
        occurred_at_ms: 1,
        detail: WatchEventDetail::DataUploaded {
            group_id: Ulid::from_parts(5, 5),
            node_id: crate::tests::locations::node_id(1),
            bucket: "bucket".to_string(),
            key: "a.csv".to_string(),
            size_bytes: 9,
        },
    }
}

#[test]
fn events_refused_early() {
    let mut empty = WatchEventsOperation::new(peer_input(), Vec::new());
    assert_eq!(rejected(empty.first()), "empty batch");
    let foreign = vec![watch_event(user(9, 2).realm_id)];
    let mut foreign = WatchEventsOperation::new(peer_input(), foreign);
    assert!(rejected(foreign.first()).contains("foreign realm"));
}

/// Dropped watches ask for an interest publish, then the new records' recipients are woken.
#[test]
fn events_wake_recipients() {
    let events = vec![watch_event(user(1, 2).realm_id)];
    let mut operation = WatchEventsOperation::new(peer_input(), events);
    operation.state = EventsState::Expand;
    let outcome = ExpandOutcome {
        inbox: aruna_core::structs::execution::notification::InboxWriteOutcome {
            written: 1,
            recipients: vec![user(1, 2)],
            ..Default::default()
        },
        dropped: true,
    };
    let expanded = operation.next(child::<_, ExpandError>(Ok(outcome)));
    assert!(matches!(expanded, Ok(Next::Call(IoCall::Child(_)))));
    let marked = child::<bool, _>(Err(InterestError::Io(
        aruna_core::calls::IoError::Unfinished,
    )));
    assert_eq!(
        operation.next(marked),
        Ok(Next::Call(IoCall::from(Effect::Net(NetEffect::WakeInbox(
            vec![user(1, 2)]
        )))))
    );
    let woken = IoResult::Event(Box::new(Event::Net(NetEvent::InboxWoken)));
    assert_eq!(
        operation.next(woken),
        Ok(Next::Done(NotificationTransportMessage::WatchEventsAck {
            written: 1
        }))
    );

    let mut failed =
        WatchEventsOperation::new(peer_input(), vec![watch_event(user(1, 2).realm_id)]);
    failed.state = EventsState::Expand;
    let conflict = child::<ExpandOutcome, _>(Err(ExpandError::Conflict));
    assert_eq!(
        rejected(failed.next(conflict)),
        ExpandError::Conflict.to_string()
    );
}
