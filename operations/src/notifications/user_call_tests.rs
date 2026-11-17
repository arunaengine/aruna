//! Tests a user's call as calls: holder resolution, the local and remote paths, and the wake,
//! publish and interest changes that follow.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::locations::node_id;
use crate::tests::notifications::user;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::notification_watch::WatchEventKind;
use aruna_core::structs::identity::realm::RealmNodeKind;

fn owner() -> UserId {
    user(1, 2)
}

/// Two servers; the first value is the node that holds `owner()`'s inbox.
fn placed() -> (NodeId, NodeId, RealmConfigDocument) {
    let nodes = [1, 2].map(node_id);
    let mut config = RealmConfigDocument::new(owner().realm_id, Vec::new(), 3);
    for node in nodes {
        config.ensure_node(node, RealmNodeKind::Server);
    }
    let holder = resolve_inbox_holder(&owner(), &config).unwrap().unwrap();
    let other = nodes.into_iter().find(|node| *node != holder).unwrap();
    (holder, other, config)
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

/// A call on `local` that has read the realm config.
fn resolved(local: NodeId, transport: bool, call: UserCall) -> (UserCallOperation, Turn) {
    let mut operation = UserCallOperation::new(owner(), local, transport, call);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let config = child::<_, GetConfigError>(Ok(placed().2));
    let turn = operation.next(config);
    (operation, turn)
}

fn create() -> UserCall {
    UserCall::CreateWatch {
        path_prefix: format!("meta/{}/", Ulid::from_parts(5, 5)),
        event_mask: WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        authorization: WatchAuthorizationBinding::default(),
        now_ms: 1_000,
    }
}

fn subscription() -> WatchSubscription {
    let mask = WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]);
    WatchSubscription::new(
        owner(),
        format!("meta/{}/", Ulid::from_parts(5, 5)),
        mask,
        1,
    )
}

#[test]
fn unplaced_unavailable() {
    let mut operation = UserCallOperation::new(owner(), node_id(1), true, UserCall::ListWatches);
    operation.first().unwrap();
    let missing = child::<RealmConfigDocument, _>(Err(GetConfigError::DocumentNotFound));
    assert_eq!(
        operation.next(missing),
        Err(WatchDispatchError::Unavailable)
    );
    let (_, other, _) = placed();
    let (_, offline) = resolved(other, false, UserCall::ListWatches);
    assert_eq!(offline, Err(WatchDispatchError::Unavailable));
}

/// A local mark that changed rows wakes the user's live streams after it ends.
#[test]
fn local_mark_wakes() {
    let (holder, _, _) = placed();
    let mark = HolderRead::Mark {
        ids: vec![Ulid::nil()],
        up_to_ms: None,
        now_ms: 1,
    };
    let (mut operation, turn) = resolved(holder, true, UserCall::Read(mark));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let marked = child::<_, HolderReadError>(Ok(HolderAnswer::Marked(2)));
    let wake = IoCall::from(Effect::Net(NetEffect::WakeInbox(vec![owner()])));
    assert_eq!(
        operation.next(marked),
        Ok(Next::Then {
            output: UserAnswer::Read(HolderAnswer::Marked(2)),
            calls: vec![wake],
        })
    );
}

/// A local delete publishes the interest and retracts the watch from this node's interest.
#[test]
fn local_delete_retracts() {
    let (holder, _, _) = placed();
    let watch_id = Ulid::from_parts(9, 9);
    let delete = UserCall::DeleteWatch {
        watch_id,
        now_ms: 1,
    };
    let (mut operation, _) = resolved(holder, true, delete);
    let Ok(Next::Then { output, calls }) =
        operation.next(child::<_, WatchSubscriptionError>(Ok(true)))
    else {
        panic!("the delete ends with calls after it");
    };
    assert_eq!(output, UserAnswer::Deleted);
    assert_eq!(calls.len(), 2);
    let retract = IoCall::from(Effect::Net(NetEffect::RetractInterest(watch_id)));
    assert_eq!(calls[1], retract);
}

/// A watch created on another holder is noted in this node's interest; the holder's refusals
/// keep their kind.
#[test]
fn remote_create_registers() {
    let (holder, other, _) = placed();
    let (mut operation, turn) = resolved(other, true, create());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let created = NotificationTransportMessage::WatchCreated {
        subscription: subscription(),
    };
    let Ok(Next::Then { output, calls }) = operation.next(child::<_, RemoteError>(Ok(created)))
    else {
        panic!("the create ends with calls after it");
    };
    assert_eq!(output, UserAnswer::Created(subscription()));
    let register = NetEffect::RegisterInterest {
        watch_id: subscription().watch_id,
        realm_id: owner().realm_id,
        holder,
        entry: WatchInterestEntry {
            path_prefix: subscription().path_prefix,
            event_mask: subscription().event_mask,
        },
    };
    assert_eq!(calls, vec![IoCall::from(Effect::Net(register))]);

    for (reason, expected) in [
        (
            WATCH_CAP_REACHED.to_string(),
            WatchDispatchError::CapExceeded,
        ),
        (
            format!("{WATCH_SUBSCRIPTION_UNAUTHORIZED}: permission_denied"),
            WatchDispatchError::Unauthorized(WatchMetricReason::PermissionDenied),
        ),
        (
            WATCH_SUBSCRIPTION_UNAVAILABLE.to_string(),
            WatchDispatchError::Unavailable,
        ),
    ] {
        let (mut refused, _) = resolved(other, true, create());
        let reject = NotificationTransportMessage::Reject(reason);
        assert_eq!(
            refused.next(child::<_, RemoteError>(Ok(reject))),
            Err(expected)
        );
    }
}

#[test]
fn unexpected_results_fail() {
    let mut operation = UserCallOperation::new(owner(), node_id(1), true, UserCall::ListWatches);
    operation.first().unwrap();
    assert!(matches!(
        operation.next(IoResult::Done),
        Err(WatchDispatchError::Internal(_))
    ));
}
