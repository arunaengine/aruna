//! Tests the chat head and turn operations as calls and results against caps and stale revisions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::{
    AssistantChatTurn, MAX_ASSISTANT_BYTES, MAX_ASSISTANT_CHATS, MAX_TURN_BYTES,
};
use ulid::Ulid;

const CHAT: &str = "c-1";

fn user() -> UserId {
    UserId::local(Ulid::from_bytes([7; 16]), RealmId::from_bytes([3; 32]))
}

fn head(chat_id: &str, next_seq: u32, bytes: u64) -> AssistantChatHead {
    AssistantChatHead {
        user_id: user(),
        chat_id: chat_id.to_string(),
        title: "Run".to_string(),
        subject: None,
        created_at: 10,
        updated_at: 10,
        first_seq: 0,
        next_seq,
        bytes,
        revision: 3,
        deleted_at: None,
    }
}

fn turn(seq: u32, payload: &str) -> AssistantChatTurn {
    AssistantChatTurn {
        seq,
        payload: payload.to_string(),
        updated_at: 10,
    }
}

fn head_value(head: Option<&AssistantChatHead>) -> IoResult {
    IoResult::Values(vec![head.map(|head| head.to_bytes().unwrap().into())])
}

fn heads_rows(heads: &[AssistantChatHead]) -> IoResult {
    let rows = (heads.iter())
        .map(|head| {
            let key = head_key(user(), &head.chat_id);
            (key, head.to_bytes().unwrap().into())
        })
        .collect();
    IoResult::Page { rows, next: None }
}

fn turns_rows(turns: &[AssistantChatTurn]) -> IoResult {
    let rows = (turns.iter())
        .map(|turn| {
            let key = turn_key(user(), CHAT, turn.seq);
            (key, turn.to_bytes().unwrap().into())
        })
        .collect();
    IoResult::Page { rows, next: None }
}

fn read_op(after: Option<u32>) -> ReadChatOperation {
    ReadChatOperation::new(user(), CHAT.to_string(), after)
}

fn head_op(expected: Option<u64>) -> WriteChatOperation {
    WriteChatOperation::new(
        user(),
        CHAT.to_string(),
        "Run QC".to_string(),
        Some("subject".to_string()),
        expected,
        50,
    )
}

fn turn_op(seq: u32, payload: &str) -> WriteTurnOperation {
    WriteTurnOperation::new(user(), CHAT.to_string(), seq, payload.to_string(), None, 50)
}

fn turn_op_at(seq: u32, revision: u64) -> WriteTurnOperation {
    WriteTurnOperation::new(
        user(),
        CHAT.to_string(),
        seq,
        "x".to_string(),
        Some(revision),
        50,
    )
}

fn delete_op() -> DeleteChatOperation {
    DeleteChatOperation::new(user(), CHAT.to_string(), 70)
}

/// The call of a turn, failing the test on an outcome or an error.
fn call<T: std::fmt::Debug>(turn: Result<Next<T>, ChatStoreError>) -> IoCall {
    match turn {
        Ok(Next::Call(call)) => call,
        other => panic!("expected a call, got {other:?}"),
    }
}

fn done<T: std::fmt::Debug>(turn: Result<Next<T>, ChatStoreError>) -> T {
    match turn {
        Ok(Next::Done(output)) => output,
        other => panic!("expected an outcome, got {other:?}"),
    }
}

fn is_heads_page(call: &IoCall) -> bool {
    matches!(call, IoCall::Page { space, .. } if space == CHAT_HEAD_KEYSPACE)
}

/// The first seq and the limit of a turn page.
fn turns_from(call: &IoCall) -> (u32, usize) {
    let IoCall::Page {
        space,
        prefix,
        after,
        limit,
    } = call
    else {
        panic!("expected a turn page, got {call:?}");
    };
    assert_eq!(space, CHAT_TURN_KEYSPACE);
    assert_eq!(*prefix, ByteView::from(turn_prefix(user(), CHAT)));
    let from = after.as_ref().map_or(0, |after| {
        let bytes = after.to_vec();
        let seq: [u8; 4] = bytes[bytes.len() - 4..].try_into().unwrap();
        u32::from_be_bytes(seq) + 1
    });
    (from, *limit)
}

fn written_head(call: &IoCall) -> AssistantChatHead {
    let IoCall::Write(rows) = call else {
        panic!("expected a write, got {call:?}");
    };
    let (_, key, value) = rows.last().unwrap();
    assert_eq!(*key, head_key(user(), CHAT));
    AssistantChatHead::from_bytes(value.as_ref()).unwrap()
}

#[test]
fn lists_live_heads() {
    // Newest change first; a tombstone is left out.
    let mut old = head("a", 1, 5);
    old.updated_at = 5;
    let mut new = head("b", 1, 5);
    new.updated_at = 9;
    let mut gone = head("c", 1, 5);
    gone.deleted_at = Some(11);
    let mut operation = ListChatOperation::new(user());
    assert!(is_heads_page(&call(operation.first())));
    let listed = done(operation.next(heads_rows(&[old.clone(), gone, new.clone()])));
    assert_eq!(listed, vec![new, old]);
}

#[test]
fn reads_after_seq() {
    // `after` below first_seq starts at first_seq; above it starts one past `after`.
    let mut stored = head(CHAT, 130, 9);
    stored.first_seq = 10;
    for (after, from) in [(Some(3), 10), (Some(20), 21), (None, 10)] {
        let mut operation = read_op(after);
        operation.first().unwrap();
        let page = call(operation.next(head_value(Some(&stored))));
        assert_eq!(turns_from(&page), (from, usize::MAX));
    }
    let mut operation = read_op(Some(3));
    operation.first().unwrap();
    operation.next(head_value(Some(&stored))).unwrap();
    let turns = done(operation.next(turns_rows(&[turn(10, "x"), turn(11, "y")])));
    assert_eq!(turns, vec![turn(10, "x"), turn(11, "y")]);
}

#[test]
fn refuses_missing_chat() {
    // Unknown is not found; a tombstone is deleted.
    let mut gone = head(CHAT, 2, 4);
    gone.deleted_at = Some(12);
    let error = |stored: Option<&AssistantChatHead>| match stored {
        None => ChatStoreError::NotFound,
        Some(_) => ChatStoreError::Deleted,
    };
    for stored in [None, Some(&gone)] {
        let mut read = read_op(None);
        read.first().unwrap();
        assert_eq!(read.next(head_value(stored)), Err(error(stored)));
        let mut write = turn_op(2, "x");
        write.first().unwrap();
        assert_eq!(write.next(head_value(stored)), Err(error(stored)));
    }
    let mut rename = head_op(None);
    rename.first().unwrap();
    assert_eq!(
        rename.next(head_value(Some(&gone))),
        Err(ChatStoreError::Deleted)
    );
}

#[test]
fn creates_a_head() {
    let mut operation = head_op(None);
    assert!(operation.transaction());
    assert_eq!(call(operation.first()), read_head(user(), CHAT));
    assert!(is_heads_page(&call(operation.next(head_value(None)))));
    let write = call(operation.next(heads_rows(&[head("other", 1, 1)])));
    assert_eq!(written_head(&write).title, "Run QC");
    let saved = done(operation.next(IoResult::Done));
    assert_eq!(saved.chat_id, CHAT);
    assert_eq!(saved.subject.as_deref(), Some("subject"));
    assert_eq!(
        (
            saved.revision,
            saved.first_seq,
            saved.next_seq,
            saved.bytes,
            saved.created_at
        ),
        (1, 0, 0, 0, 50)
    );
}

#[test]
fn refuses_chat_cap() {
    // Tombstones do not count against the cap.
    let mut heads: Vec<_> = (0..MAX_ASSISTANT_CHATS)
        .map(|index| head(&format!("c{index}"), 1, 1))
        .collect();
    let mut operation = head_op(None);
    operation.first().unwrap();
    operation.next(head_value(None)).unwrap();
    assert_eq!(
        operation.next(heads_rows(&heads)),
        Err(ChatStoreError::TooLarge(CHAT_CAP))
    );

    heads[0].deleted_at = Some(1);
    let mut operation = head_op(None);
    operation.first().unwrap();
    operation.next(head_value(None)).unwrap();
    let write = call(operation.next(heads_rows(&heads)));
    assert!(matches!(write, IoCall::Write(_)));
}

#[test]
fn renames_with_revision() {
    let mut operation = head_op(Some(3));
    operation.first().unwrap();
    let write = call(operation.next(head_value(Some(&head(CHAT, 4, 9)))));
    assert_eq!(written_head(&write).title, "Run QC");
    let saved = done(operation.next(IoResult::Done));
    assert_eq!(
        (
            saved.revision,
            saved.updated_at,
            saved.created_at,
            saved.next_seq,
            saved.bytes
        ),
        (4, 50, 10, 4, 9)
    );
}

#[test]
fn refuses_stale_head() {
    let mut operation = head_op(Some(2));
    operation.first().unwrap();
    assert_eq!(
        operation.next(head_value(Some(&head(CHAT, 4, 9)))),
        Err(ChatStoreError::Stale)
    );

    // No expectation overwrites.
    let mut operation = head_op(None);
    operation.first().unwrap();
    let write = call(operation.next(head_value(Some(&head(CHAT, 4, 9)))));
    assert!(matches!(write, IoCall::Write(_)));
}

#[test]
fn appends_a_turn() {
    let mut operation = turn_op(4, "abcd");
    assert!(operation.transaction());
    operation.first().unwrap();
    let sum = call(operation.next(head_value(Some(&head(CHAT, 4, 9)))));
    assert!(is_heads_page(&sum));
    let write = call(operation.next(heads_rows(&[head(CHAT, 4, 9), head("other", 1, 100)])));
    let IoCall::Write(rows) = &write else {
        panic!("expected a write, got {write:?}");
    };
    assert_eq!(rows[0].1, turn_key(user(), CHAT, 4));
    assert_eq!(
        AssistantChatTurn::from_bytes(rows[0].2.as_ref()).unwrap(),
        AssistantChatTurn {
            seq: 4,
            payload: "abcd".to_string(),
            updated_at: 50
        }
    );
    let saved = done(operation.next(IoResult::Done));
    assert_eq!(saved, written_head(&write));
    assert_eq!(
        (
            saved.first_seq,
            saved.next_seq,
            saved.bytes,
            saved.revision,
            saved.updated_at
        ),
        (0, 5, 13, 4, 50)
    );
}

#[test]
fn rewrites_the_tail() {
    // The old tail's bytes are released; next_seq stays.
    let mut operation = turn_op(3, "abcdef");
    operation.first().unwrap();
    let page = call(operation.next(head_value(Some(&head(CHAT, 4, 9)))));
    assert_eq!(turns_from(&page), (3, 1));
    assert!(is_heads_page(&call(operation.next(turns_rows(&[turn(3, "abcd")])))));
    let write = call(operation.next(heads_rows(&[head(CHAT, 4, 9)])));
    let saved = written_head(&write);
    assert_eq!(
        (saved.first_seq, saved.next_seq, saved.bytes, saved.revision),
        (0, 4, 11, 4)
    );
}

#[test]
fn trims_old_turns() {
    // An append at the turn cap drops the oldest turn and advances first_seq.
    let mut full = head(CHAT, 130, 500);
    full.first_seq = 10;
    let mut operation = turn_op(130, "new");
    operation.first().unwrap();
    let page = call(operation.next(head_value(Some(&full))));
    assert_eq!(turns_from(&page), (10, 1));
    operation.next(turns_rows(&[turn(10, "old!!")])).unwrap();
    let trim = call(operation.next(heads_rows(&[full.clone()])));
    assert_eq!(
        trim,
        IoCall::Delete(vec![(
            CHAT_TURN_KEYSPACE.to_string(),
            turn_key(user(), CHAT, 10)
        )])
    );
    let write = call(operation.next(IoResult::Done));
    let saved = written_head(&write);
    assert_eq!(
        (saved.first_seq, saved.next_seq, saved.bytes),
        (11, 131, 498)
    );
}

#[test]
fn refuses_wrong_seq() {
    // Anything but next_seq or the tail is stale, and the error names next_seq.
    for seq in [2, 5] {
        let mut operation = turn_op(seq, "x");
        operation.first().unwrap();
        assert_eq!(
            operation.next(head_value(Some(&head(CHAT, 4, 9)))),
            Err(ChatStoreError::StaleTurn { next_seq: 4 })
        );
    }
}

#[test]
fn refuses_stale_revision() {
    // An append and a tail rewrite from an older read both stop; a matching one goes on.
    for seq in [3, 4] {
        let mut operation = turn_op_at(seq, 2);
        operation.first().unwrap();
        assert_eq!(
            operation.next(head_value(Some(&head(CHAT, 4, 9)))),
            Err(ChatStoreError::StaleTurn { next_seq: 4 })
        );
    }
    let mut operation = turn_op_at(4, 3);
    operation.first().unwrap();
    let sum = call(operation.next(head_value(Some(&head(CHAT, 4, 9)))));
    assert!(is_heads_page(&sum));
}

#[test]
fn refuses_large_turn() {
    let payload = "x".repeat(MAX_TURN_BYTES + 1);
    let mut operation = WriteTurnOperation::new(user(), CHAT.to_string(), 0, payload, None, 50);
    assert_eq!(operation.first(), Err(ChatStoreError::TooLarge(TURN_CAP)));
}

#[test]
fn refuses_over_budget() {
    // The bytes a write releases count in the user's favour.
    let other = head("other", 1, MAX_ASSISTANT_BYTES - 12);
    let mut operation = turn_op(4, "abcd");
    operation.first().unwrap();
    operation.next(head_value(Some(&head(CHAT, 4, 9)))).unwrap();
    assert_eq!(
        operation.next(heads_rows(&[head(CHAT, 4, 9), other.clone()])),
        Err(ChatStoreError::TooLarge(BUDGET_CAP))
    );

    let mut operation = turn_op(3, "abcd");
    operation.first().unwrap();
    operation.next(head_value(Some(&head(CHAT, 4, 9)))).unwrap();
    operation.next(turns_rows(&[turn(3, "abcd")])).unwrap();
    let write = call(operation.next(heads_rows(&[head(CHAT, 4, 9), other])));
    assert!(matches!(write, IoCall::Write(_)));
}

#[test]
fn deletes_once() {
    let mut operation = delete_op();
    assert!(operation.transaction());
    operation.first().unwrap();
    let page = call(operation.next(head_value(Some(&head(CHAT, 2, 8)))));
    assert_eq!(turns_from(&page), (0, usize::MAX));
    let delete = call(operation.next(turns_rows(&[turn(0, "a"), turn(1, "b")])));
    assert!(matches!(&delete, IoCall::Delete(keys) if keys.len() == 2));
    let tombstone = written_head(&call(operation.next(IoResult::Done)));
    assert_eq!(tombstone.deleted_at, Some(70));
    assert_eq!(
        (
            tombstone.bytes,
            tombstone.first_seq,
            tombstone.next_seq,
            tombstone.revision
        ),
        (0, 2, 2, 4)
    );
    done(operation.next(IoResult::Done));
}

#[test]
fn skips_missing_chat() {
    // Unknown or already deleted: the delete succeeds without a write.
    let mut gone = head(CHAT, 2, 8);
    gone.deleted_at = Some(1);
    for stored in [None, Some(&gone)] {
        let mut operation = delete_op();
        operation.first().unwrap();
        done(operation.next(head_value(stored)));
    }

    // No live turns: the tombstone is written without a delete.
    let mut operation = delete_op();
    operation.first().unwrap();
    operation.next(head_value(Some(&head(CHAT, 0, 0)))).unwrap();
    let write = call(operation.next(turns_rows(&[])));
    assert!(matches!(write, IoCall::Write(_)));
}

#[test]
fn rejects_wrong_results() {
    // A result of another kind than the call fails with the runner's unexpected error.
    let unexpected = |turn: Result<Next<AssistantChatHead>, ChatStoreError>| {
        assert!(matches!(
            turn,
            Err(ChatStoreError::Storage(StorageError::ReadError(_)))
        ));
    };
    let mut operation = head_op(None);
    operation.first().unwrap();
    unexpected(operation.next(IoResult::Done));
    let mut operation = turn_op(4, "x");
    operation.first().unwrap();
    operation.next(head_value(Some(&head(CHAT, 4, 9)))).unwrap();
    unexpected(operation.next(head_value(None)));
}

#[test]
fn conflict_is_storage() {
    // A lost conflict keeps the storage error the api maps to 409, and counts as a conflict.
    let error = ChatStoreError::from(IoError::Conflict);
    assert_eq!(
        error,
        ChatStoreError::Storage(StorageError::TransactionConflict)
    );
    assert!(WriteTurnOperation::is_conflict(&error));
}

#[test]
fn rejects_corrupt_records() {
    let corrupt = || IoResult::Values(vec![Some(vec![0xff; 3].into())]);
    let mut read = read_op(None);
    read.first().unwrap();
    assert!(matches!(
        read.next(corrupt()),
        Err(ChatStoreError::Conversion(_))
    ));

    let mut write = turn_op(0, "x");
    write.first().unwrap();
    assert!(matches!(
        write.next(corrupt()),
        Err(ChatStoreError::Conversion(_))
    ));

    let mut list = ListChatOperation::new(user());
    list.first().unwrap();
    let rows = vec![(head_key(user(), CHAT), vec![0xff; 3].into())];
    assert!(matches!(
        list.next(IoResult::Page { rows, next: None }),
        Err(ChatStoreError::Conversion(_))
    ));
}
