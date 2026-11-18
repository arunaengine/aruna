//! Tests the typed results of user calls and the inbox holder lookup as calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::locations::node_id;
use crate::tests::notifications::user;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::notification_watch::WatchEventMask;
use aruna_core::structs::identity::realm::RealmNodeKind;

fn config(servers: &[NodeId]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(user(1, 2).realm_id, Vec::new(), 3);
    for server in servers {
        config.ensure_node(*server, RealmNodeKind::Server);
    }
    config
}

fn looked_up(config: Result<RealmConfigDocument, GetConfigError>) -> Result<NodeId, String> {
    let mut holder = InboxHolderOperation::new(user(1, 2));
    assert!(matches!(holder.first(), Ok(Next::Call(IoCall::Child(_)))));
    match holder.next(IoResult::Child(ChildOutcome::new(config))) {
        Ok(Next::Done(node)) => Ok(node),
        Err(error) => Err(error.to_string()),
        Ok(other) => panic!("the lookup ends after the config: {other:?}"),
    }
}

#[test]
fn holder_from_config() {
    assert_eq!(looked_up(Ok(config(&[node_id(1)]))), Ok(node_id(1)));
    let unavailable = NotificationDispatchError::Unavailable.to_string();
    assert_eq!(looked_up(Ok(config(&[]))), Err(unavailable.clone()));
    let missing = looked_up(Err(GetConfigError::DocumentNotFound));
    assert_eq!(missing, Err(unavailable));

    let failed = GetConfigError::from(IoError::Unexpected {
        expected: "an answer",
        got: "another",
    });
    let reason = failed.to_string();
    assert_eq!(looked_up(Err(failed)), Err(reason));
}

#[test]
fn answers_unpack() {
    let watch = WatchSubscription::new(
        user(1, 2),
        "meta/".to_string(),
        WatchEventMask::default(),
        1,
    );
    let read = |answer| UserAnswer::Read(answer);
    let listed = read(HolderAnswer::Listed {
        records: Vec::new(),
        next_cursor: Some(vec![1]),
    });
    assert_eq!(listed.listed(), Ok((Vec::new(), Some(vec![1]))));
    let unread = read(HolderAnswer::Unread {
        count: 3,
        capped: true,
    });
    assert_eq!(unread.unread(), Ok((3, true)));
    assert_eq!(read(HolderAnswer::Marked(2)).marked(), Ok(2));
    let created = UserAnswer::Created(watch.clone());
    assert_eq!(created.created(), Ok(watch.clone()));
    assert_eq!(UserAnswer::Deleted.deleted(), Ok(()));
    let watches = UserAnswer::Watches(vec![watch]);
    assert_eq!(watches.watches().map(|list| list.len()), Ok(1));
}

#[test]
fn other_answers_refused() {
    assert!(matches!(
        UserAnswer::Deleted.unread(),
        Err(NotificationDispatchError::Internal(_))
    ));
    assert!(matches!(
        UserAnswer::Read(HolderAnswer::Marked(1)).listed(),
        Err(NotificationDispatchError::Internal(_))
    ));
    assert!(matches!(
        UserAnswer::Deleted.created(),
        Err(WatchDispatchError::Internal(_))
    ));
    assert!(matches!(
        UserAnswer::Watches(Vec::new()).deleted(),
        Err(WatchDispatchError::Internal(_))
    ));
}
