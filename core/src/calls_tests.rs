//! Tests the call runner: every call kind, refused and late events, commits and conflicts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use super::*;
use crate::effects::BlobEffect;
use crate::events::{BlobEvent, GroupResult, SubOperationEvent};
use crate::operation::boxed_suboperation;
use crate::task::{TaskEffect, TaskEvent, TaskKey};

/// Makes its calls in order, keeps every answer and ends with them, then the `then` calls.
/// After a window member it adds `on_member` once, otherwise waits. Aborts with `cleanup`.
#[derive(Debug, PartialEq)]
struct Script {
    calls: VecDeque<IoCall>,
    seen: Vec<IoResult>,
    on_member: Option<IoCall>,
    scope: Option<HoldScope>,
    transaction: bool,
    then: Vec<IoCall>,
    cleanup: Vec<IoCall>,
    after: Vec<Effect>,
}

impl Script {
    fn turn(&mut self) -> Next<Vec<IoResult>> {
        let output = match self.calls.pop_front() {
            Some(call) => return Next::Call(call),
            None => std::mem::take(&mut self.seen),
        };
        match std::mem::take(&mut self.then) {
            calls if calls.is_empty() => Next::Done(output),
            calls => Next::Then { output, calls },
        }
    }
}

impl Operation for Script {
    type Output = Vec<IoResult>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        Ok(self.turn())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        let member = matches!(result, IoResult::Group(GroupEvent::Member { .. }));
        self.seen.push(result);
        if member {
            return Ok(self.on_member.take().map_or(Next::Wait, Next::Call));
        }
        Ok(self.turn())
    }

    fn hold_scope(&self) -> Option<HoldScope> {
        self.scope
    }

    fn transaction(&self) -> bool {
        self.transaction
    }

    fn abort(&mut self) -> Vec<IoCall> {
        std::mem::take(&mut self.cleanup)
    }

    fn after_commit(&mut self) -> Option<Effect> {
        (!self.after.is_empty()).then(|| self.after.remove(0))
    }
}

fn script(calls: Vec<IoCall>) -> Script {
    Script {
        calls: calls.into(),
        seen: Vec::new(),
        on_member: None,
        scope: None,
        transaction: false,
        then: Vec::new(),
        cleanup: Vec::new(),
        after: Vec::new(),
    }
}

/// The runner's machine for `script`, in one transaction or not.
fn run(mut script: Script, transaction: bool) -> CallRun<Script> {
    script.transaction = transaction;
    CallRun::new(script)
}

fn key(text: &str) -> Key {
    Key::from(text.as_bytes())
}

fn txn() -> TxnId {
    TxnId::from(7u128)
}

fn started() -> Event {
    Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
}

fn storage(effect: StorageEffect) -> Effect {
    Effect::Storage(effect)
}

fn read_a() -> IoCall {
    IoCall::Read(vec![("rows".to_string(), key("a"))])
}

#[test]
fn effects_join_transaction() {
    // A passed-through storage effect without a transaction id runs in the operation's one.
    let entry = crate::node_vault::VaultEntry::new(
        crate::node_vault::VaultPurpose::NamedSecret,
        ulid::Ulid::from_parts(1, 1),
    );
    let delete = StorageEffect::VaultDelete {
        entry,
        txn_id: None,
    };
    let mut operation = run(
        script(vec![IoCall::Effect(Box::new(storage(delete)))]),
        true,
    );
    let start = storage(StorageEffect::StartTransaction { read: false });
    assert_eq!(operation.start().as_slice(), &[start]);
    let joined = StorageEffect::VaultDelete {
        entry,
        txn_id: Some(txn()),
    };
    assert_eq!(operation.step(started()).as_slice(), &[storage(joined)]);
}

#[test]
fn sends_after_commit() {
    // The effects for a committed outcome follow the commit; even a refusal keeps the outcome.
    let write = vec![("rows".to_string(), key("a"), key("1"))];
    let mut script = script(vec![IoCall::Write(write)]);
    let wake = || {
        Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::DrainNotificationOutbox,
            after: Duration::ZERO,
        })
    };
    let sync = || Effect::Storage(StorageEffect::SyncAll);
    script.after = vec![wake(), sync()];
    let mut operation = run(script, true);
    operation.start();
    operation.step(started());
    let written = StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    };
    operation.step(Event::Storage(written));
    let committed = StorageEvent::TransactionCommitted { txn_id: txn() };
    assert_eq!(
        operation.step(Event::Storage(committed)).as_slice(),
        &[wake()]
    );
    assert!(!operation.is_complete());
    let refused = TaskEvent::Error {
        key: None,
        message: "timer refused".to_string(),
    };
    assert_eq!(operation.step(Event::Task(refused)).as_slice(), &[sync()]);
    let synced = Event::Storage(StorageEvent::SyncAllFinished);
    assert!(operation.step(synced).is_empty());
    assert!(operation.is_complete());
    assert_eq!(operation.finalize(), Ok(vec![IoResult::Done]));
}

#[test]
fn storage_refusal_fails() {
    // A storage effect answered with a storage error fails the operation like a storage call.
    let read = StorageEffect::Read {
        key_space: "rows".to_string(),
        key: key("a"),
        txn_id: None,
    };
    let mut operation = run(script(vec![IoCall::Effect(Box::new(storage(read)))]), false);
    operation.start();
    let error = StorageError::ReadError("disk".to_string());
    let refused = Event::Storage(StorageEvent::Error {
        error: error.clone(),
    });
    assert!(operation.step(refused).is_empty());
    assert_eq!(operation.finalize(), Err(IoError::Storage(error)));
}

#[test]
fn transaction_commits_outcome() {
    let write = vec![("rows".to_string(), key("a"), key("1"))];
    let mut operation = run(script(vec![read_a(), IoCall::Write(write.clone())]), true);
    let start = storage(StorageEffect::StartTransaction { read: false });
    assert_eq!(operation.start().as_slice(), &[start]);
    let read = StorageEffect::BatchRead {
        reads: vec![("rows".to_string(), key("a"))],
        txn_id: Some(txn()),
    };
    assert_eq!(operation.step(started()).as_slice(), &[storage(read)]);
    let values = vec![(key("a"), None)];
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    let write = StorageEffect::BatchWrite {
        writes: write,
        txn_id: Some(txn()),
    };
    assert_eq!(effects.as_slice(), &[storage(write)]);
    let written = StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    };
    let commit = StorageEffect::CommitTransaction { txn_id: txn() };
    assert_eq!(
        operation.step(Event::Storage(written)).as_slice(),
        &[storage(commit)]
    );
    assert!(!operation.is_complete());
    let committed = StorageEvent::TransactionCommitted { txn_id: txn() };
    assert!(operation.step(Event::Storage(committed)).is_empty());
    assert_eq!(
        operation.finalize(),
        Ok(vec![IoResult::Values(vec![None]), IoResult::Done])
    );
}

#[test]
fn direct_pages_syncs() {
    let page = IoCall::Page {
        space: "rows".to_string(),
        prefix: key("p/"),
        after: Some(key("p/1")),
        limit: 10,
    };
    let calls = vec![IoCall::Read(Vec::new()), page, IoCall::Sync];
    let mut operation = run(script(calls), false);
    // The empty read is answered without storage.
    let iter = StorageEffect::Iter {
        key_space: "rows".to_string(),
        prefix: Some(key("p/")),
        start: Some(IterStart::After(key("p/1"))),
        limit: 10,
        txn_id: None,
    };
    assert_eq!(operation.start().as_slice(), &[storage(iter)]);
    let rows = vec![(key("p/2"), key("2"))];
    let page = StorageEvent::IterResult {
        values: rows.clone(),
        next_start_after: None,
    };
    let effects = operation.step(Event::Storage(page));
    assert_eq!(effects.as_slice(), &[storage(StorageEffect::SyncAll)]);
    assert!(
        operation
            .step(Event::Storage(StorageEvent::SyncAllFinished))
            .is_empty()
    );
    let expected = vec![
        IoResult::Values(Vec::new()),
        IoResult::Page { rows, next: None },
        IoResult::Done,
    ];
    assert_eq!(operation.finalize(), Ok(expected));
}

#[test]
fn passes_other_calls() {
    let id = ulid::Ulid::from(3u128);
    let child = boxed_suboperation(script(Vec::new()), |_| Event::Delayed);
    let calls = vec![
        IoCall::Effect(Box::new(Effect::Task(TaskEffect::CancelTimer {
            key: TaskKey::DrainSyncOutbox,
        }))),
        IoCall::Effect(Box::new(Effect::Blob(BlobEffect::ReleaseReservation {
            id,
        }))),
        IoCall::Child(child),
        IoCall::Group(EffectGroup {
            members: vec![Effect::Delay(Duration::from_secs(1))],
            limit: 1,
            form: GroupForm::All { deadline: None },
        }),
        IoCall::Delay(Duration::from_secs(5)),
    ];
    let mut operation = run(script(calls), false);
    assert!(matches!(
        operation.start().as_slice(),
        [Effect::Task(TaskEffect::CancelTimer { .. })]
    ));
    let blob = || Event::Blob(BlobEvent::ReservationReleased { id });
    let child = || Event::SubOperation(SubOperationEvent::NotificationsEmitted);
    // Each answer leads to the next call's one effect.
    assert_eq!(operation.step(cancelled()).len(), 1);
    assert_eq!(operation.step(blob()).len(), 1);
    assert_eq!(operation.step(child()).len(), 1);
    assert_eq!(operation.step(Event::Group(finished())).len(), 1);
    assert!(operation.step(Event::Delayed).is_empty());
    let expected = vec![
        IoResult::Event(Box::new(cancelled())),
        IoResult::Event(Box::new(blob())),
        IoResult::Event(Box::new(child())),
        IoResult::Group(finished()),
        IoResult::Woke,
    ];
    assert_eq!(operation.finalize(), Ok(expected));
}

fn cancelled() -> Event {
    Event::Task(TaskEvent::TimerCancelled {
        key: TaskKey::DrainSyncOutbox,
    })
}

fn finished() -> GroupEvent {
    GroupEvent::Finished {
        results: vec![GroupResult::Done(Box::new(Event::Delayed))],
    }
}

#[test]
fn window_adds_members() {
    let window = IoCall::Group(EffectGroup {
        members: vec![Effect::Delay(Duration::from_secs(1))],
        limit: 2,
        form: GroupForm::Window,
    });
    let mut procedure = script(vec![window]);
    procedure.on_member = Some(IoCall::Delay(Duration::from_secs(2)));
    let mut operation = run(procedure, false);
    assert!(matches!(operation.start().as_slice(), [Effect::Group(_)]));
    let first = GroupEvent::Member {
        index: 0,
        result: GroupResult::Done(Box::new(Event::Delayed)),
    };
    let effects = operation.step(Event::Group(first));
    assert_eq!(
        effects.as_slice(),
        &[Effect::Delay(Duration::from_secs(2))],
        "the call joins the open window"
    );
    let second = GroupEvent::Member {
        index: 1,
        result: GroupResult::Done(Box::new(Event::Delayed)),
    };
    assert!(operation.step(Event::Group(second)).is_empty());
    assert!(!operation.is_complete());
    assert!(operation.step(Event::Group(GroupEvent::Closed)).is_empty());
    let Ok(seen) = operation.finalize() else {
        panic!("the window run finishes");
    };
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[2], IoResult::Group(GroupEvent::Closed));
}

#[test]
fn refuses_wrong_event() {
    let mut operation = run(script(vec![read_a()]), true);
    operation.start();
    operation.step(started());
    let effects = operation.step(Event::Delayed);
    let abort = StorageEffect::AbortTransaction { txn_id: txn() };
    assert_eq!(effects.as_slice(), &[storage(abort)]);
    assert!(operation.is_complete());
    let refused = IoError::Unexpected {
        expected: "StorageEvent::BatchReadResult",
        got: "Event::Delayed",
    };
    assert_eq!(operation.finalize(), Err(refused));

    let task = TaskEffect::CancelTimer {
        key: TaskKey::DrainSyncOutbox,
    };
    let mut operation = run(
        script(vec![IoCall::Effect(Box::new(Effect::Task(task)))]),
        false,
    );
    operation.start();
    let id = ulid::Ulid::from(4u128);
    operation.step(Event::Blob(BlobEvent::ReservationReleased { id }));
    let refused = IoError::Unexpected {
        expected: "a task event",
        got: "BlobEvent::ReservationReleased",
    };
    assert_eq!(operation.finalize(), Err(refused));
}

#[test]
fn refuses_late_event() {
    let mut operation = run(script(Vec::new()), false);
    assert!(operation.start().is_empty());
    assert!(operation.is_complete());
    assert!(operation.step(Event::Delayed).is_empty());
    let refused = IoError::Unexpected {
        expected: "no event",
        got: "Event::Delayed",
    };
    assert_eq!(operation.finalize(), Err(refused));
}

#[test]
fn refuses_control_effect() {
    let start = storage(StorageEffect::StartTransaction { read: false });
    let mut operation = run(script(vec![IoCall::Effect(Box::new(start))]), false);
    assert!(operation.start().is_empty());
    let refused = IoError::Unexpected {
        expected: "an effect without its own call",
        got: "a transaction control effect",
    };
    assert_eq!(operation.finalize(), Err(refused));
}

#[test]
fn abort_ends_transaction() {
    let mut operation = run(script(vec![read_a()]), true);
    operation.start();
    operation.step(started());
    let abort = StorageEffect::AbortTransaction { txn_id: txn() };
    assert_eq!(operation.abort().as_slice(), &[storage(abort)]);
    assert!(operation.is_complete());
    assert_eq!(operation.finalize(), Err(IoError::Aborted));

    // A sent commit owns the transaction: abort adds nothing.
    let write = vec![("rows".to_string(), key("a"), key("1"))];
    let mut operation = run(script(vec![IoCall::Write(write)]), true);
    operation.start();
    operation.step(started());
    let written = StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    };
    operation.step(Event::Storage(written));
    assert!(operation.abort().is_empty());
    assert!(!operation.is_complete());
}

#[test]
fn conflict_marks_retry() {
    let write = vec![("rows".to_string(), key("a"), key("1"))];
    let mut operation = run(script(vec![IoCall::Write(write)]), true);
    operation.start();
    operation.step(started());
    let written = StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    };
    operation.step(Event::Storage(written));
    let conflict = StorageEvent::Error {
        error: StorageError::TransactionConflict,
    };
    assert!(operation.step(Event::Storage(conflict)).is_empty());
    let Err(error) = operation.finalize() else {
        panic!("a lost commit fails");
    };
    assert_eq!(error, IoError::Conflict);
    assert!(CallRun::<Script>::is_conflict(&error));
    let full = IoError::Storage(StorageError::QueueFull);
    assert!(!CallRun::<Script>::is_conflict(&full));
    assert_eq!(CallRun::<Script>::conflict_retries(), 3);
}

#[test]
fn passes_hold_scope() {
    let mut procedure = script(Vec::new());
    procedure.scope = Some(HoldScope::DocumentReconcile);
    let operation = run(procedure, false);
    assert_eq!(operation.hold_scope(), Some(HoldScope::DocumentReconcile));
}

#[test]
fn hung_adapter_fails() {
    let mut operation = run(script(vec![read_a()]), true);
    operation.start();
    operation.step(started());
    let effects = operation.step(Event::Hung { effect: "storage" });
    let abort = StorageEffect::AbortTransaction { txn_id: txn() };
    assert_eq!(effects.as_slice(), &[storage(abort)]);
    let hung = IoError::Hung { effect: "storage" };
    assert_eq!(operation.finalize(), Err(hung));
}

#[test]
fn child_returns_output() {
    let mut operation = run(script(vec![IoCall::child(script(Vec::new()))]), true);
    let effects = operation.start();
    assert_eq!(
        effects.len(),
        1,
        "a child call never opens the parent's transaction"
    );
    let Some(Effect::SubOperation(mut child)) = effects.into_iter().next() else {
        panic!("a child call is one sub operation");
    };
    assert!(child.start().is_empty(), "the child needs no I/O");
    assert!(child.is_complete());
    let event = child.finalize();
    let outcome = ChildOutcome::new::<Vec<IoResult>, IoError>(Ok(Vec::new()));
    assert_eq!(event, Event::Child(outcome));
    assert!(operation.step(event).is_empty());
    let Ok(mut seen) = operation.finalize() else {
        panic!("the parent finishes");
    };
    let result = seen.pop().map(IoResult::child::<Vec<IoResult>, IoError>);
    assert_eq!(result, Some(Ok(Ok(Vec::new()))));
}

#[test]
fn child_refuses_type() {
    let outcome = IoResult::Child(ChildOutcome::new::<u8, IoError>(Err(IoError::Aborted)));
    let refused = IoError::Unexpected {
        expected: type_name::<Result<u16, IoError>>(),
        got: "a child of another type",
    };
    assert_eq!(outcome.child::<u16, IoError>(), Err(refused));
    let refused = IoError::Unexpected {
        expected: type_name::<Result<u8, IoError>>(),
        got: "IoResult::Done",
    };
    assert_eq!(IoResult::Done.child::<u8, IoError>(), Err(refused));
    let failed = IoResult::Child(ChildOutcome::new::<u8, IoError>(Err(IoError::Aborted)));
    assert_eq!(failed.child::<u8, IoError>(), Ok(Err(IoError::Aborted)));
}

#[test]
fn adds_counters_inside() {
    let usage = IoCall::AddUsage {
        space: "usage".to_string(),
        deltas: Vec::new(),
        buckets: Vec::new(),
    };
    let counters = IoCall::AddCounters {
        space: "counters".to_string(),
        deltas: Vec::new(),
    };
    let mut operation = run(script(vec![usage, counters]), true);
    let start = storage(StorageEffect::StartTransaction { read: false });
    assert_eq!(operation.start().as_slice(), &[start]);
    let usage = StorageEffect::AddUsage {
        key_space: "usage".to_string(),
        deltas: Vec::new(),
        buckets: Vec::new(),
        txn_id: txn(),
    };
    assert_eq!(operation.step(started()).as_slice(), &[storage(usage)]);
    let written = || {
        Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        })
    };
    let counters = StorageEffect::AddCounters {
        key_space: "counters".to_string(),
        deltas: Vec::new(),
        txn_id: txn(),
    };
    assert_eq!(operation.step(written()).as_slice(), &[storage(counters)]);
    let commit = StorageEffect::CommitTransaction { txn_id: txn() };
    assert_eq!(operation.step(written()).as_slice(), &[storage(commit)]);

    // Without a transaction the call has nothing to add to.
    let counters = IoCall::AddCounters {
        space: "counters".to_string(),
        deltas: Vec::new(),
    };
    let mut operation = run(script(vec![counters]), false);
    assert!(operation.start().is_empty());
    let refused = IoError::Unexpected {
        expected: "a transaction",
        got: "IoCall::AddCounters",
    };
    assert_eq!(operation.finalize(), Err(refused));
}

#[test]
fn abort_sends_cleanup() {
    let id = ulid::Ulid::from(5u128);
    let release = || Effect::Blob(BlobEffect::ReleaseReservation { id });
    let mut procedure = script(vec![read_a()]);
    procedure.cleanup = vec![IoCall::Effect(Box::new(release()))];
    let mut operation = run(procedure, true);
    operation.start();
    operation.step(started());
    let abort = storage(StorageEffect::AbortTransaction { txn_id: txn() });
    assert_eq!(operation.abort().as_slice(), &[abort, release()]);
    assert!(operation.abort().is_empty(), "cleanup is sent once");
    assert_eq!(operation.finalize(), Err(IoError::Aborted));

    // A hung adapter ends the run the same way.
    let mut procedure = script(vec![read_a()]);
    procedure.cleanup = vec![IoCall::Effect(Box::new(release()))];
    let mut operation = run(procedure, true);
    operation.start();
    operation.step(started());
    let effects = operation.step(Event::Hung { effect: "storage" });
    let abort = storage(StorageEffect::AbortTransaction { txn_id: txn() });
    assert_eq!(effects.as_slice(), &[abort, release()]);
}

#[test]
fn calls_after_commit() {
    let write = vec![("rows".to_string(), key("a"), key("1"))];
    let mut procedure = script(vec![IoCall::Write(write)]);
    procedure.then = vec![
        IoCall::Delete(vec![("rows".to_string(), key("old"))]),
        IoCall::Delay(Duration::from_secs(1)),
    ];
    let mut operation = run(procedure, true);
    operation.start();
    operation.step(started());
    let written = StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    };
    let commit = StorageEffect::CommitTransaction { txn_id: txn() };
    assert_eq!(
        operation.step(Event::Storage(written)).as_slice(),
        &[storage(commit)]
    );
    let committed = StorageEvent::TransactionCommitted { txn_id: txn() };
    let delete = StorageEffect::BatchDelete {
        deletes: vec![("rows".to_string(), key("old"))],
        txn_id: None,
    };
    let effects = operation.step(Event::Storage(committed));
    assert_eq!(
        effects.as_slice(),
        &[storage(delete)],
        "outside the transaction"
    );
    // A failed call after the commit is logged and the next one runs.
    let failed = StorageEvent::Error {
        error: StorageError::QueueFull,
    };
    let effects = operation.step(Event::Storage(failed));
    assert_eq!(effects.as_slice(), &[Effect::Delay(Duration::from_secs(1))]);
    assert!(!operation.is_complete());
    // An abort now keeps the committed outcome.
    assert!(operation.abort().is_empty());
    assert_eq!(operation.finalize(), Ok(vec![IoResult::Done]));
}

#[test]
fn reads_expected_answers() {
    let value: crate::types::Value = b"v".to_vec().into();
    let one = IoResult::Values(vec![Some(value.clone())]);
    assert_eq!(one.value(), Ok(Some(value.clone())));
    assert_eq!(IoResult::Values(vec![None]).value(), Ok(None));
    assert_eq!(IoResult::Done.done(), Ok(()));
    let page = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(page.page(), Ok((Vec::new(), None)));
    assert_eq!(
        IoResult::Event(Box::new(crate::events::Event::Delayed)).event(),
        Ok(crate::events::Event::Delayed)
    );
}

#[test]
fn names_other_answers() {
    let two = IoResult::Values(vec![None, None]);
    assert_eq!(
        two.value(),
        Err(IoError::Unexpected {
            expected: "IoResult::Values with one value",
            got: "IoResult::Values",
        })
    );
    assert_eq!(
        IoResult::Woke.done(),
        Err(IoError::Unexpected {
            expected: "IoResult::Done",
            got: "IoResult::Woke",
        })
    );
    let event = IoResult::Event(Box::new(crate::events::Event::Delayed));
    assert!(matches!(
        event.values(),
        Err(IoError::Unexpected {
            expected: "IoResult::Values",
            ..
        })
    ));
}
