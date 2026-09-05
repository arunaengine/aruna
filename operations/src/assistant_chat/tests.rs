use super::*;
use aruna_core::events::StorageEvent;
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AssistantChatTurn, MAX_ASSISTANT_CHAT_BYTES, MAX_ASSISTANT_CHATS, MAX_ASSISTANT_TURN_BYTES,
    RealmId,
};
use ulid::Ulid;

const CHAT: &str = "c-1";

fn user() -> UserId {
    UserId::local(Ulid::from_bytes([7; 16]), RealmId::from_bytes([3; 32]))
}

fn txn() -> Ulid {
    Ulid::from_bytes([6; 16])
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

fn started() -> Event {
    Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
}

fn committed() -> Event {
    Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() })
}

fn head_read(head: Option<&AssistantChatHead>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: head_key(user(), CHAT),
        value: head.map(|head| head.to_bytes().unwrap().into()),
    })
}

fn heads_iter(heads: &[AssistantChatHead]) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values: heads
            .iter()
            .map(|head| {
                (
                    head_key(user(), &head.chat_id),
                    head.to_bytes().unwrap().into(),
                )
            })
            .collect(),
        next_start_after: None,
    })
}

fn turns_iter(turns: &[AssistantChatTurn]) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values: turns
            .iter()
            .map(|turn| {
                (
                    turn_key(user(), CHAT, turn.seq),
                    turn.to_bytes().unwrap().into(),
                )
            })
            .collect(),
        next_start_after: None,
    })
}

fn written() -> Event {
    Event::Storage(StorageEvent::WriteResult {
        key: head_key(user(), CHAT),
    })
}

fn batch_written() -> Event {
    Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    })
}

fn batch_deleted() -> Event {
    Event::Storage(StorageEvent::BatchDeleteResult {
        entries: Vec::new(),
    })
}

fn read_op(after: Option<u32>) -> ReadChatTurnsOperation {
    ReadChatTurnsOperation::new(user(), CHAT.to_string(), after)
}

fn head_op(expected: Option<u64>) -> WriteChatHeadOperation {
    WriteChatHeadOperation::new(
        user(),
        CHAT.to_string(),
        "Run QC".to_string(),
        Some("subject".to_string()),
        expected,
        50,
    )
}

fn turn_op(seq: u32, payload: &str) -> WriteChatTurnOperation {
    WriteChatTurnOperation::new(user(), CHAT.to_string(), seq, payload.to_string(), None, 50)
}

fn turn_op_at(seq: u32, revision: u64) -> WriteChatTurnOperation {
    WriteChatTurnOperation::new(
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

fn is_iter(effects: &Effects, key_space: &str) -> bool {
    matches!(
        effects.first(),
        Some(Effect::Storage(StorageEffect::Iter { key_space: space, .. })) if space == key_space
    )
}

fn is_write(effects: &Effects) -> bool {
    matches!(
        effects.first(),
        Some(Effect::Storage(StorageEffect::Write { .. }))
    )
}

fn is_batch_write(effects: &Effects) -> bool {
    matches!(
        effects.first(),
        Some(Effect::Storage(StorageEffect::BatchWrite { .. }))
    )
}

/// The start key and limit of a turn iteration.
fn iter_start(effects: &Effects) -> Option<(Key, usize)> {
    match effects.first() {
        Some(Effect::Storage(StorageEffect::Iter {
            start: Some(IterStart::At(key)),
            limit,
            ..
        })) => Some((key.clone(), *limit)),
        _ => None,
    }
}

fn deleted_keys(effects: &Effects) -> Vec<(String, Key)> {
    match effects.first() {
        Some(Effect::Storage(StorageEffect::BatchDelete { deletes, .. })) => deletes.clone(),
        _ => panic!("expected a batch delete, got {effects:?}"),
    }
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
    let mut operation = ListChatHeadsOperation::new(user());
    assert!(is_iter(&operation.start(), ASSISTANT_CHAT_HEAD_KEYSPACE));
    assert!(!operation.is_complete());
    operation.step(heads_iter(&[old.clone(), gone, new.clone()]));

    assert!(operation.is_complete());
    assert_eq!(operation.finalize().unwrap(), vec![new, old]);
}

#[test]
fn reads_after_seq() {
    // `after` below first_seq starts at first_seq; above it starts one past `after`.
    let mut stored = head(CHAT, 130, 9);
    stored.first_seq = 10;
    let mut operation = read_op(Some(3));
    operation.start();
    let effects = operation.step(head_read(Some(&stored)));
    assert_eq!(
        iter_start(&effects),
        Some((turn_key(user(), CHAT, 10), usize::MAX))
    );
    operation.step(turns_iter(&[turn(10, "x"), turn(11, "y")]));
    assert_eq!(
        operation.finalize().unwrap(),
        vec![turn(10, "x"), turn(11, "y")]
    );

    let mut operation = read_op(Some(20));
    operation.start();
    let effects = operation.step(head_read(Some(&stored)));
    assert_eq!(
        iter_start(&effects),
        Some((turn_key(user(), CHAT, 21), usize::MAX))
    );

    let mut operation = read_op(None);
    operation.start();
    let effects = operation.step(head_read(Some(&stored)));
    assert_eq!(
        iter_start(&effects),
        Some((turn_key(user(), CHAT, 10), usize::MAX))
    );
}

#[test]
fn refuses_missing_chat() {
    // Unknown is not found; a tombstone is deleted. A write also releases its transaction.
    let mut gone = head(CHAT, 2, 4);
    gone.deleted_at = Some(12);

    let mut read = read_op(None);
    read.start();
    assert!(read.step(head_read(None)).is_empty());
    assert_eq!(read.finalize().unwrap_err(), ChatStoreError::NotFound);

    let mut read = read_op(None);
    read.start();
    read.step(head_read(Some(&gone)));
    assert_eq!(read.finalize().unwrap_err(), ChatStoreError::Deleted);

    let mut write = turn_op(2, "x");
    write.start();
    write.step(started());
    assert_eq!(write.step(head_read(None)).len(), 1);
    assert_eq!(write.finalize().unwrap_err(), ChatStoreError::NotFound);

    let mut write = turn_op(2, "x");
    write.start();
    write.step(started());
    assert_eq!(write.step(head_read(Some(&gone))).len(), 1);
    assert_eq!(write.finalize().unwrap_err(), ChatStoreError::Deleted);

    let mut rename = head_op(None);
    rename.start();
    rename.step(started());
    assert_eq!(rename.step(head_read(Some(&gone))).len(), 1);
    assert_eq!(rename.finalize().unwrap_err(), ChatStoreError::Deleted);
}

#[test]
fn creates_a_head() {
    let mut operation = head_op(None);
    assert_eq!(operation.start().len(), 1);
    operation.step(started());
    let effects = operation.step(head_read(None));
    assert!(is_iter(&effects, ASSISTANT_CHAT_HEAD_KEYSPACE));
    let effects = operation.step(heads_iter(&[head("other", 1, 1)]));
    assert!(is_write(&effects));
    operation.step(written());
    operation.step(committed());

    assert!(operation.is_complete());
    let saved = operation.finalize().unwrap();
    assert_eq!(saved.chat_id, CHAT);
    assert_eq!(saved.title, "Run QC");
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
    operation.start();
    operation.step(started());
    operation.step(head_read(None));
    let cleanup = operation.step(heads_iter(&heads));
    assert_eq!(cleanup.len(), 1);
    assert_eq!(
        operation.finalize().unwrap_err(),
        ChatStoreError::TooLarge(CHAT_CAP)
    );

    heads[0].deleted_at = Some(1);
    let mut operation = head_op(None);
    operation.start();
    operation.step(started());
    operation.step(head_read(None));
    assert!(is_write(&operation.step(heads_iter(&heads))));
}

#[test]
fn renames_with_revision() {
    let mut operation = head_op(Some(3));
    operation.start();
    operation.step(started());
    assert!(is_write(
        &operation.step(head_read(Some(&head(CHAT, 4, 9))))
    ));
    operation.step(written());
    operation.step(committed());

    let saved = operation.finalize().unwrap();
    assert_eq!(saved.title, "Run QC");
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
    operation.start();
    operation.step(started());
    assert_eq!(operation.step(head_read(Some(&head(CHAT, 4, 9)))).len(), 1);
    assert_eq!(operation.finalize().unwrap_err(), ChatStoreError::Stale);

    // No expectation overwrites.
    let mut operation = head_op(None);
    operation.start();
    operation.step(started());
    assert!(is_write(
        &operation.step(head_read(Some(&head(CHAT, 4, 9))))
    ));
}

#[test]
fn appends_a_turn() {
    let mut operation = turn_op(4, "abcd");
    assert_eq!(operation.start().len(), 1);
    operation.step(started());
    let effects = operation.step(head_read(Some(&head(CHAT, 4, 9))));
    assert!(is_iter(&effects, ASSISTANT_CHAT_HEAD_KEYSPACE));
    let effects = operation.step(heads_iter(&[head(CHAT, 4, 9), head("other", 1, 100)]));
    let Some(Effect::Storage(StorageEffect::BatchWrite { writes, .. })) = effects.first() else {
        panic!("expected a batch write, got {effects:?}");
    };
    assert_eq!(writes[0].1, turn_key(user(), CHAT, 4));
    assert_eq!(writes[1].1, head_key(user(), CHAT));
    assert_eq!(
        AssistantChatTurn::from_bytes(writes[0].2.as_ref()).unwrap(),
        AssistantChatTurn {
            seq: 4,
            payload: "abcd".to_string(),
            updated_at: 50
        }
    );
    operation.step(batch_written());
    operation.step(committed());

    let saved = operation.finalize().unwrap();
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
    operation.start();
    operation.step(started());
    let effects = operation.step(head_read(Some(&head(CHAT, 4, 9))));
    assert_eq!(iter_start(&effects), Some((turn_key(user(), CHAT, 3), 1)));
    let effects = operation.step(turns_iter(&[turn(3, "abcd")]));
    assert!(is_iter(&effects, ASSISTANT_CHAT_HEAD_KEYSPACE));
    assert!(is_batch_write(
        &operation.step(heads_iter(&[head(CHAT, 4, 9)]))
    ));
    operation.step(batch_written());
    operation.step(committed());

    let saved = operation.finalize().unwrap();
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
    operation.start();
    operation.step(started());
    let effects = operation.step(head_read(Some(&full)));
    assert_eq!(iter_start(&effects), Some((turn_key(user(), CHAT, 10), 1)));
    let effects = operation.step(turns_iter(&[turn(10, "old!!")]));
    assert!(is_iter(&effects, ASSISTANT_CHAT_HEAD_KEYSPACE));
    let effects = operation.step(heads_iter(&[full.clone()]));
    assert_eq!(
        deleted_keys(&effects),
        vec![(
            ASSISTANT_CHAT_TURN_KEYSPACE.to_string(),
            turn_key(user(), CHAT, 10)
        )]
    );
    assert!(is_batch_write(&operation.step(batch_deleted())));
    operation.step(batch_written());
    operation.step(committed());

    let saved = operation.finalize().unwrap();
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
        operation.start();
        operation.step(started());
        assert_eq!(operation.step(head_read(Some(&head(CHAT, 4, 9)))).len(), 1);
        assert_eq!(
            operation.finalize().unwrap_err(),
            ChatStoreError::StaleTurn { next_seq: 4 }
        );
    }
}

#[test]
fn refuses_stale_revision() {
    // An append and a tail rewrite from an older read both stop; a matching one goes on.
    for seq in [3, 4] {
        let mut operation = turn_op_at(seq, 2);
        operation.start();
        operation.step(started());
        assert_eq!(operation.step(head_read(Some(&head(CHAT, 4, 9)))).len(), 1);
        assert_eq!(
            operation.finalize().unwrap_err(),
            ChatStoreError::StaleTurn { next_seq: 4 }
        );
    }
    let mut operation = turn_op_at(4, 3);
    operation.start();
    operation.step(started());
    let effects = operation.step(head_read(Some(&head(CHAT, 4, 9))));
    assert!(is_iter(&effects, ASSISTANT_CHAT_HEAD_KEYSPACE));
}

#[test]
fn refuses_large_turn() {
    let payload = "x".repeat(MAX_ASSISTANT_TURN_BYTES + 1);
    let mut operation = WriteChatTurnOperation::new(user(), CHAT.to_string(), 0, payload, None, 50);

    assert!(operation.start().is_empty());
    assert_eq!(
        operation.finalize().unwrap_err(),
        ChatStoreError::TooLarge(TURN_CAP)
    );
}

#[test]
fn refuses_over_budget() {
    // The bytes a write releases count in the user's favour.
    let other = head("other", 1, MAX_ASSISTANT_CHAT_BYTES - 12);
    let mut operation = turn_op(4, "abcd");
    operation.start();
    operation.step(started());
    operation.step(head_read(Some(&head(CHAT, 4, 9))));
    let cleanup = operation.step(heads_iter(&[head(CHAT, 4, 9), other.clone()]));
    assert_eq!(cleanup.len(), 1);
    assert_eq!(
        operation.finalize().unwrap_err(),
        ChatStoreError::TooLarge(BUDGET_CAP)
    );

    let mut operation = turn_op(3, "abcd");
    operation.start();
    operation.step(started());
    operation.step(head_read(Some(&head(CHAT, 4, 9))));
    operation.step(turns_iter(&[turn(3, "abcd")]));
    assert!(is_batch_write(
        &operation.step(heads_iter(&[head(CHAT, 4, 9), other]))
    ));
}

#[test]
fn deletes_once() {
    let mut operation = delete_op();
    assert_eq!(operation.start().len(), 1);
    operation.step(started());
    let effects = operation.step(head_read(Some(&head(CHAT, 2, 8))));
    assert_eq!(
        iter_start(&effects),
        Some((turn_key(user(), CHAT, 0), usize::MAX))
    );
    let effects = operation.step(turns_iter(&[turn(0, "a"), turn(1, "b")]));
    assert_eq!(deleted_keys(&effects).len(), 2);
    let effects = operation.step(batch_deleted());
    let Some(Effect::Storage(StorageEffect::Write { value, .. })) = effects.first() else {
        panic!("expected the tombstone write, got {effects:?}");
    };
    let tombstone = AssistantChatHead::from_bytes(value.as_ref()).unwrap();
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
    operation.step(written());
    operation.step(committed());

    assert!(operation.is_complete());
    assert_eq!(operation.finalize().unwrap(), ());
}

#[test]
fn skips_missing_chat() {
    // Unknown or already deleted: the transaction is released and the delete succeeds.
    let mut operation = delete_op();
    operation.start();
    operation.step(started());
    let cleanup = operation.step(head_read(None));
    assert!(matches!(
        cleanup.first(),
        Some(Effect::Storage(StorageEffect::AbortTransaction { .. }))
    ));
    assert!(operation.is_complete());
    assert_eq!(operation.finalize().unwrap(), ());

    let mut gone = head(CHAT, 2, 8);
    gone.deleted_at = Some(1);
    let mut operation = delete_op();
    operation.start();
    operation.step(started());
    assert_eq!(operation.step(head_read(Some(&gone))).len(), 1);
    assert!(operation.finalize().is_ok());

    // No live turns: the tombstone is written without a batch delete.
    let mut operation = delete_op();
    operation.start();
    operation.step(started());
    operation.step(head_read(Some(&head(CHAT, 0, 0))));
    assert!(is_write(&operation.step(turns_iter(&[]))));
}

fn expect_unexpected<O: Operation<Error = ChatStoreError>>(
    mut operation: O,
    events: Vec<Event>,
    wrong: Event,
    cleanup: usize,
) {
    operation.start();
    for event in events {
        operation.step(event);
    }
    assert_eq!(operation.step(wrong).len(), cleanup);
    assert!(operation.is_complete());
    assert!(matches!(
        operation.finalize().unwrap_err(),
        ChatStoreError::UnexpectedEvent { .. }
    ));
}

#[test]
fn rejects_unexpected_events() {
    // Each state accepts one event kind; anything else fails and aborts the transaction.
    let stored = head(CHAT, 4, 9);
    let mut full = head(CHAT, 130, 500);
    full.first_seq = 10;
    let live = || head_read(Some(&stored));

    expect_unexpected(ListChatHeadsOperation::new(user()), vec![], started(), 0);
    expect_unexpected(read_op(None), vec![], started(), 0);
    expect_unexpected(read_op(None), vec![live()], head_read(None), 0);

    expect_unexpected(head_op(None), vec![], head_read(None), 0);
    expect_unexpected(head_op(None), vec![started()], started(), 1);
    expect_unexpected(
        head_op(None),
        vec![started(), head_read(None)],
        written(),
        1,
    );
    expect_unexpected(head_op(None), vec![started(), live()], head_read(None), 1);
    expect_unexpected(
        head_op(None),
        vec![started(), live(), written()],
        written(),
        1,
    );

    expect_unexpected(turn_op(4, "x"), vec![], head_read(None), 0);
    expect_unexpected(turn_op(4, "x"), vec![started()], started(), 1);
    expect_unexpected(turn_op(3, "x"), vec![started(), live()], written(), 1);
    expect_unexpected(turn_op(4, "x"), vec![started(), live()], written(), 1);
    expect_unexpected(
        turn_op(130, "x"),
        vec![
            started(),
            head_read(Some(&full)),
            turns_iter(&[turn(10, "old")]),
            heads_iter(&[full.clone()]),
        ],
        written(),
        1,
    );
    expect_unexpected(
        turn_op(4, "x"),
        vec![started(), live(), heads_iter(&[])],
        written(),
        1,
    );
    expect_unexpected(
        turn_op(4, "x"),
        vec![started(), live(), heads_iter(&[]), batch_written()],
        written(),
        1,
    );

    expect_unexpected(delete_op(), vec![], head_read(None), 0);
    expect_unexpected(delete_op(), vec![started()], started(), 1);
    expect_unexpected(delete_op(), vec![started(), live()], written(), 1);
    expect_unexpected(
        delete_op(),
        vec![started(), live(), turns_iter(&[turn(0, "a")])],
        written(),
        1,
    );
    expect_unexpected(
        delete_op(),
        vec![started(), live(), turns_iter(&[])],
        batch_deleted(),
        1,
    );
    expect_unexpected(
        delete_op(),
        vec![started(), live(), turns_iter(&[]), written()],
        written(),
        1,
    );
}

#[test]
fn aborts_on_error() {
    let error = || {
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        })
    };
    let mut write = turn_op(4, "x");
    write.start();
    write.step(started());
    assert_eq!(write.step(error()).len(), 1);
    assert!(write.is_complete());
    assert_eq!(
        write.finalize().unwrap_err(),
        ChatStoreError::Storage(StorageError::TransactionConflict)
    );

    let mut rename = head_op(None);
    rename.start();
    rename.step(started());
    assert_eq!(rename.step(error()).len(), 1);
    assert!(rename.finalize().is_err());

    let mut delete = delete_op();
    delete.start();
    delete.step(started());
    assert_eq!(delete.step(error()).len(), 1);
    assert!(delete.finalize().is_err());

    let mut list = ListChatHeadsOperation::new(user());
    list.start();
    assert!(list.step(error()).is_empty());
    assert!(list.finalize().is_err());

    let mut read = read_op(None);
    read.start();
    assert!(read.step(error()).is_empty());
    assert_eq!(
        read.finalize().unwrap_err(),
        ChatStoreError::Storage(StorageError::TransactionConflict)
    );
}

#[test]
fn rejects_corrupt_records() {
    let corrupt = || {
        Event::Storage(StorageEvent::ReadResult {
            key: head_key(user(), CHAT),
            value: Some(vec![0xff; 3].into()),
        })
    };
    let mut read = read_op(None);
    read.start();
    read.step(corrupt());
    assert!(matches!(
        read.finalize().unwrap_err(),
        ChatStoreError::Conversion(_)
    ));

    let mut write = turn_op(0, "x");
    write.start();
    write.step(started());
    assert_eq!(write.step(corrupt()).len(), 1);
    assert!(matches!(
        write.finalize().unwrap_err(),
        ChatStoreError::Conversion(_)
    ));

    let mut list = ListChatHeadsOperation::new(user());
    list.start();
    list.step(Event::Storage(StorageEvent::IterResult {
        values: vec![(head_key(user(), CHAT), vec![0xff; 3].into())],
        next_start_after: None,
    }));
    assert!(matches!(
        list.finalize().unwrap_err(),
        ChatStoreError::Conversion(_)
    ));
}

#[test]
fn finalize_needs_completion() {
    let mut operation = turn_op(0, "x");
    assert!(!operation.is_complete());
    // A step before start behaves like start.
    assert_eq!(operation.step(head_read(None)).len(), 1);
    assert!(operation.abort().is_empty());
    assert_eq!(
        operation.finalize().unwrap_err(),
        ChatStoreError::NotFinished
    );
    assert_eq!(
        ListChatHeadsOperation::new(user()).finalize().unwrap_err(),
        ChatStoreError::NotFinished
    );
    assert_eq!(
        read_op(None).finalize().unwrap_err(),
        ChatStoreError::NotFinished
    );
    assert_eq!(
        head_op(None).finalize().unwrap_err(),
        ChatStoreError::NotFinished
    );
    assert_eq!(
        delete_op().finalize().unwrap_err(),
        ChatStoreError::NotFinished
    );
}
