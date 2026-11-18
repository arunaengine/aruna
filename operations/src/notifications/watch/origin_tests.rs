//! Tests the watch emission as calls: the nil actor, the local holder from this node's own
//! watches, the expansion and its follow-ups, and forwarding to remote holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::locations::node_id;
use crate::tests::notifications::user;
use aruna_core::structs::execution::notification::InboxWriteOutcome;
use aruna_core::structs::execution::notification_watch::{
    WatchEventDetail, WatchEventKind, WatchEventMask, WatchSubscription, watch_resource_path,
};
use aruna_core::structs::identity::realm::RealmNodeKind;
use ulid::Ulid;

fn event(actor: UserId) -> WatchEvent {
    WatchEvent {
        event_id: Ulid::from_parts(3, 3),
        realm_id: actor.realm_id,
        kind: WatchEventKind::DataUploaded,
        path: watch_resource_path(Ulid::from_parts(5, 5), node_id(1), "bucket", "a.csv"),
        actor,
        occurred_at_ms: 10,
        detail: WatchEventDetail::DataUploaded {
            group_id: Ulid::from_parts(5, 5),
            node_id: node_id(1),
            bucket: "bucket".to_string(),
            key: "a.csv".to_string(),
            size_bytes: 9,
        },
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

/// A config where the one server `node_id(1)` holds every inbox.
fn single_server() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(user(1, 2).realm_id, Vec::new(), 3);
    config.ensure_node(node_id(1), RealmNodeKind::Server);
    config
}

#[test]
fn nil_actor_skipped() {
    let realm_id = user(1, 2).realm_id;
    let mut emit = EmitWatchOperation::new(event(UserId::nil(realm_id)), node_id(1), Vec::new());
    assert_eq!(emit.first(), Ok(Next::Done(())));
}

/// Without the realm config only remote holders get the event; a failed forward only warns.
#[test]
fn remote_forward_only() {
    let holders = vec![node_id(1), node_id(2)];
    let mut emit = EmitWatchOperation::new(event(user(1, 2)), node_id(1), holders);
    emit.first().unwrap();
    let unread = child::<RealmConfigDocument, _>(Err(GetConfigError::DocumentNotFound));
    assert!(matches!(
        emit.next(unread),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(emit.state, EmitState::Forward(node_id(2)));
    let failed = child::<NotificationTransportMessage, _>(Err(RemoteError("down".into())));
    assert_eq!(emit.next(failed), Ok(Next::Done(())));
}

/// A matching watch held here makes this node a holder; the expansion's new records wake their
/// recipients and dropped watches ask for a publish.
#[test]
fn local_watch_expands() {
    let mut emit = EmitWatchOperation::new(event(user(1, 2)), node_id(1), Vec::new());
    emit.first().unwrap();
    let config = child::<_, GetConfigError>(Ok(single_server()));
    assert!(matches!(
        emit.next(config),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
    let prefix = watch_resource_path(Ulid::from_parts(5, 5), node_id(1), "bucket", "");
    let watch = WatchSubscription::new(user(1, 2), prefix, mask, 1);
    let page = WatchPage {
        subscriptions: vec![watch],
        next: None,
    };
    let listed = emit.next(child::<_, WatchSubscriptionError>(Ok(page)));
    assert!(matches!(listed, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(emit.state, EmitState::Expand);
    let outcome = ExpandOutcome {
        inbox: InboxWriteOutcome {
            written: 1,
            recipients: vec![user(1, 2)],
            ..Default::default()
        },
        dropped: true,
    };
    let wake = IoCall::from(Effect::Net(NetEffect::WakeInbox(vec![user(1, 2)])));
    assert_eq!(
        emit.next(child::<_, ExpandError>(Ok(outcome))),
        Ok(Next::Call(wake))
    );
    let woken = IoResult::Event(Box::new(Event::Net(NetEvent::InboxWoken)));
    assert!(matches!(emit.next(woken), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(
        emit.next(child::<_, InterestError>(Ok(true))),
        Ok(Next::Done(()))
    );
}

#[test]
fn unexpected_results_fail() {
    let mut emit = EmitWatchOperation::new(event(user(1, 2)), node_id(1), Vec::new());
    emit.first().unwrap();
    assert!(emit.next(IoResult::Done).is_err());
}

fn holders_answer(holders: Option<(NodeId, Vec<NodeId>)>) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::WatchHolders(holders))))
}

/// The event asks for its holders, then emits to them; a failed emission reaches the caller.
#[test]
fn event_reads_holders() {
    let event = event(user(1, 2));
    let mut emit = EmitEventOperation::new(event.clone());
    let holders = NetEffect::WatchHolders {
        realm_id: event.realm_id,
        path: event.path.clone(),
        kind: event.kind,
    };
    assert_eq!(
        emit.first(),
        Ok(Next::Call(IoCall::from(Effect::Net(holders))))
    );
    let found = holders_answer(Some((node_id(1), vec![node_id(2)])));
    assert!(matches!(emit.next(found), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(emit.next(child::<(), IoError>(Ok(()))), Ok(Next::Done(())));

    let mut failed = EmitEventOperation::new(event);
    failed.first().unwrap();
    failed
        .next(holders_answer(Some((node_id(1), Vec::new()))))
        .unwrap();
    let error = IoError::Unexpected {
        expected: "an answer",
        got: "another",
    };
    assert_eq!(
        failed.next(child::<(), IoError>(Err(error.clone()))),
        Err(error)
    );
}

/// Without a net handle nothing is emitted, and any other answer is refused.
#[test]
fn event_without_net() {
    let mut emit = EmitEventOperation::new(event(user(1, 2)));
    emit.first().unwrap();
    assert_eq!(emit.next(holders_answer(None)), Ok(Next::Done(())));

    let mut wrong = EmitEventOperation::new(event(user(1, 2)));
    wrong.first().unwrap();
    let other = IoResult::Event(Box::new(Event::Net(NetEvent::Recorded)));
    assert!(matches!(wrong.next(other), Err(IoError::Unexpected { .. })));
}

#[test]
fn created_event_dated() {
    let (group_id, document_id) = (Ulid::from_parts(5, 5), Ulid::from_parts(6, 6));
    let event_id = Ulid::from_parts(77, 7);
    let actor = user(1, 2);
    let emit = EmitEventOperation::metadata_created(
        actor.realm_id,
        actor,
        group_id,
        document_id,
        "datasets/a",
        event_id,
    );
    let expected = WatchEvent {
        event_id,
        realm_id: actor.realm_id,
        kind: WatchEventKind::MetadataCreated,
        path: format!("meta/{group_id}/datasets/a"),
        actor,
        occurred_at_ms: 77,
        detail: WatchEventDetail::MetadataCreated {
            group_id,
            document_id,
        },
    };
    assert_eq!(emit, EmitEventOperation::new(expected));
}
