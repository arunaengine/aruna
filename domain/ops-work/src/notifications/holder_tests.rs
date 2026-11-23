//! Tests the holder read as calls: pages that hidden watch notices do not use up, the unread
//! cap, and marking in chunks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::notifications::watch::paths::WatchAuthorizationDenial;
use crate::tests::notifications::{record, user};
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::notification_watch::WatchAuthorizationBinding;

fn recipient() -> UserId {
    user(1, 2)
}

fn notice(at: u64) -> NotificationRecord {
    record(recipient(), NotificationClass::Direct, at)
}

/// A transient watch notice, which shows only while its watch is authorized.
fn watched(at: u64) -> NotificationRecord {
    let mut notice = record(recipient(), NotificationClass::Transient, at);
    notice.kind = NotificationKind::MetadataCreated {
        path: "meta/group/dataset".to_string(),
        group_id: Ulid::from_parts(5, 5),
        document_id: Ulid::from_parts(6, 6),
        actor_user_id: recipient(),
    };
    notice.watch_authorization = Some(WatchAuthorizationBinding::default());
    notice
}

fn page(records: Vec<NotificationRecord>, next: Option<&[u8]>) -> IoResult {
    let output = ListNotificationsOutput {
        records,
        next_cursor: next.map(<[u8]>::to_vec),
    };
    IoResult::Child(ChildOutcome::new(Ok::<_, ListNotificationsError>(output)))
}

fn verdict(authorization: WatchAuthorization) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, WatchCheckError>(authorization)))
}

fn denied() -> WatchAuthorization {
    WatchAuthorization::Denied(WatchAuthorizationDenial::PermissionDenied)
}

/// A hidden watch notice does not use up the page: the next page fills it.
#[test]
fn hidden_rows_refill() {
    let read = HolderRead::List {
        cursor: None,
        limit: 2,
    };
    let mut list = HolderReadOperation::new(recipient(), read);
    assert!(matches!(list.first(), Ok(Next::Call(IoCall::Child(_)))));
    let (newest, oldest) = (notice(30), notice(10));
    let first = page(vec![newest.clone(), watched(20)], Some(b"c1"));
    assert!(matches!(list.next(first), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(list.state, HolderState::Check);
    assert!(matches!(
        list.next(verdict(denied())),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(list.state, HolderState::Page);
    let Ok(Next::Done(HolderAnswer::Listed {
        records,
        next_cursor,
    })) = list.next(page(vec![oldest.clone()], None))
    else {
        panic!("the second page fills the list");
    };
    assert_eq!(records, vec![newest, oldest]);
    assert_eq!(next_cursor, None);
}

/// Read and hidden notices are not counted; an unavailable check fails the read.
#[test]
fn unread_counts_visible() {
    let mut unread = HolderReadOperation::new(recipient(), HolderRead::Unread);
    unread.first().unwrap();
    let mut read = notice(40);
    read.read_at_ms = Some(41);
    unread
        .next(page(vec![read, notice(30), watched(20)], None))
        .unwrap();
    assert_eq!(
        unread.next(verdict(WatchAuthorization::Authorized)),
        Ok(Next::Done(HolderAnswer::Unread {
            count: 2,
            capped: false
        }))
    );

    let mut failing = HolderReadOperation::new(recipient(), HolderRead::Unread);
    failing.first().unwrap();
    failing.next(page(vec![watched(20)], None)).unwrap();
    let down = WatchAuthorization::Unavailable("down".to_string());
    assert_eq!(
        failing.next(verdict(down)),
        Err(HolderReadError::Unavailable("down".to_string()))
    );
}

/// Marking reads full pages, marks the named visible records, and sums the marks.
#[test]
fn marks_named_records() {
    let named = notice(30);
    let read = HolderRead::Mark {
        ids: vec![named.notification_id],
        up_to_ms: None,
        now_ms: 99,
    };
    let mut mark = HolderReadOperation::new(recipient(), read);
    mark.first().unwrap();
    let marking = mark.next(page(vec![named, notice(20)], None));
    assert!(matches!(marking, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(mark.state, HolderState::Mark);
    let done = MarkReadOutput { marked: 1 };
    let done = IoResult::Child(ChildOutcome::new(Ok::<_, MarkReadError>(done)));
    assert_eq!(mark.next(done), Ok(Next::Done(HolderAnswer::Marked(1))));

    let too_many = HolderRead::Mark {
        ids: vec![Ulid::nil(); MARK_MAX_IDS + 1],
        up_to_ms: None,
        now_ms: 99,
    };
    let mut refused = HolderReadOperation::new(recipient(), too_many);
    assert_eq!(refused.first(), Err(HolderReadError::TooManyIds));
    let nothing = HolderRead::Mark {
        ids: Vec::new(),
        up_to_ms: None,
        now_ms: 99,
    };
    let mut idle = HolderReadOperation::new(recipient(), nothing);
    assert_eq!(idle.first(), Ok(Next::Done(HolderAnswer::Marked(0))));
}

#[test]
fn unexpected_results_fail() {
    let mut list = HolderReadOperation::new(recipient(), HolderRead::Unread);
    list.first().unwrap();
    assert!(matches!(
        list.next(IoResult::Done),
        Err(HolderReadError::Io(IoError::Unexpected { .. }))
    ));
}
