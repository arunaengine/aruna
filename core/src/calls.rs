//! Declares the I/O calls of an operation and runs an operation as calls, events and checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::any::{Any, type_name};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use smallvec::smallvec;
use tracing::warn;

use crate::effects::{Effect, EffectGroup, GroupForm, IterStart, StorageEffect, abort_effects};
use crate::errors::StorageError;
use crate::events::{Event, GroupEvent, StorageEvent};
use crate::operation::{
    AfterConflict, EffectOperation, HoldScope, Operation, Runnable, SubOperation, TransactionMode,
    boxed_suboperation,
};
use crate::structs::storage::distribution::accounting::RangeCounters;
use crate::structs::storage::usage::{BucketDelta, UsageDelta};
use crate::types::{Effects, Key, KeySpace, TxnId, Value};

/// One I/O request of an operation. Storage reads, pages, writes and deletes run in the
/// operation's transaction when it has one; every other call runs on its own.
#[derive(Debug, PartialEq)]
pub enum IoCall {
    /// The values of these rows, in this order; an empty list is answered without storage.
    Read(Vec<(KeySpace, Key)>),
    /// At most `limit` rows below `prefix` after the key `after`.
    Page {
        space: KeySpace,
        prefix: Key,
        after: Option<Key>,
        limit: usize,
    },
    /// Writes these rows; an empty list is answered without storage.
    Write(Vec<(KeySpace, Key, Value)>),
    /// Deletes these rows; an empty list is answered without storage.
    Delete(Vec<(KeySpace, Key)>),
    /// Persists every pending write with `SyncAll` durability.
    Sync,
    /// A net, blob, metadata, task, staging source, local file or other storage effect, answered
    /// by the event of the same family. Transaction control effects are refused.
    Effect(Box<Effect>),
    /// A child operation, answered by the event its result maps to; see [`IoCall::child`].
    Child(Box<dyn SubOperation>),
    /// Independent effects run at the same time. An all-at-once group is answered once; a
    /// window answers each member, then `GroupEvent::Closed`.
    Group(EffectGroup),
    /// A wake after this long on the runner's clock.
    Delay(Duration),
    /// Adds usage deltas to the latest stored counters when the operation's transaction
    /// commits; needs `TransactionMode::Write`. Answered by `Done`.
    AddUsage {
        space: KeySpace,
        deltas: Vec<(Key, UsageDelta)>,
        buckets: Vec<(Key, BucketDelta)>,
    },
    /// Adds range counter deltas when the operation's transaction commits, like `AddUsage`.
    AddCounters {
        space: KeySpace,
        deltas: Vec<(Key, RangeCounters)>,
    },
}

/// A sub-operation, group or delay effect becomes its own call; any other effect passes through.
impl From<Effect> for IoCall {
    fn from(effect: Effect) -> Self {
        match effect {
            Effect::SubOperation(child) => Self::Child(child),
            Effect::Group(group) => Self::Group(group),
            Effect::Delay(after) => Self::Delay(after),
            other => Self::Effect(Box::new(other)),
        }
    }
}

/// The answer to an [`IoCall`].
#[derive(Debug, PartialEq)]
pub enum IoResult {
    Values(Vec<Option<Value>>),
    /// The rows of a page, and the key to continue after when more may follow.
    Page {
        rows: Vec<(Key, Value)>,
        next: Option<Key>,
    },
    /// A write, delete or sync finished.
    Done,
    /// The event that answered an `IoCall::Effect`, or an `IoCall::Child` that maps to an event.
    Event(Box<Event>),
    Group(GroupEvent),
    /// The delay passed.
    Woke,
    /// The outcome of a child made with [`IoCall::child`]; read it with [`IoResult::child`].
    Child(ChildOutcome),
}

impl IoCall {
    /// Runs `operation` of either kind as a child; its result comes back as [`IoResult::Child`].
    pub fn child<O, K>(operation: O) -> Self
    where
        O: Runnable<K> + 'static,
        K: 'static,
        O::Output: PartialEq + 'static,
        O::Error: PartialEq + 'static,
    {
        Self::Child(boxed_suboperation(operation, |result| {
            Event::Child(ChildOutcome::new(result))
        }))
    }
}

/// A child's `Result<Output, Error>`, boxed so events stay free of operation types. The value
/// sits behind a mutex, so an output that is only `Send`, such as a stream, can be carried.
pub struct ChildOutcome(Box<dyn ChildValue>);

trait ChildValue: Any + Send + Sync + Debug {
    fn eq_dyn(&self, other: &dyn ChildValue) -> bool;
}

/// One child result, locked only to compare or print it and taken out whole.
struct Held<T>(Mutex<T>);

impl<T: Debug> Debug for Held<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).fmt(f)
    }
}

impl<T: Any + Send + Debug + PartialEq> ChildValue for Held<T> {
    fn eq_dyn(&self, other: &dyn ChildValue) -> bool {
        let Some(other) = (other as &dyn Any).downcast_ref::<Held<T>>() else {
            return false;
        };
        if std::ptr::eq(self, other) {
            return true;
        }
        let mine = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let theirs = other.0.lock().unwrap_or_else(PoisonError::into_inner);
        *mine == *theirs
    }
}

impl ChildOutcome {
    /// Wraps a child's result; a group member child maps its outcome with this.
    pub fn new<T, E>(result: Result<T, E>) -> Self
    where
        T: Send + Debug + PartialEq + 'static,
        E: Send + Debug + PartialEq + 'static,
    {
        Self(Box::new(Held(Mutex::new(result))))
    }

    /// The child's result, or `None` when the child's output or error has another type.
    pub fn take<T: 'static, E: 'static>(self) -> Option<Result<T, E>> {
        let value: Box<dyn Any> = self.0;
        let held = value.downcast::<Held<Result<T, E>>>().ok()?;
        Some(held.0.into_inner().unwrap_or_else(PoisonError::into_inner))
    }
}

impl Debug for ChildOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialEq for ChildOutcome {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_dyn(&*other.0)
    }
}

impl IoResult {
    /// Names the result kind without its values, for unexpected-result errors.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Values(_) => "IoResult::Values",
            Self::Page { .. } => "IoResult::Page",
            Self::Done => "IoResult::Done",
            Self::Event(event) => event.label(),
            Self::Group(_) => "IoResult::Group",
            Self::Woke => "IoResult::Woke",
            Self::Child(_) => "IoResult::Child",
        }
    }

    /// The result of a child made with [`IoCall::child`] whose output is `T` and error `E`.
    pub fn child<T: 'static, E: 'static>(self) -> Result<Result<T, E>, IoError> {
        let expected = type_name::<Result<T, E>>();
        match self {
            Self::Child(outcome) => outcome
                .take()
                .ok_or_else(|| unexpected(expected, "a child of another type")),
            other => Err(unexpected(expected, other.label())),
        }
    }

    fn other(&self, expected: &'static str) -> IoError {
        IoError::Unexpected {
            expected,
            got: self.label(),
        }
    }

    /// The values of a read, or the error naming the result that came instead.
    pub fn values(self) -> Result<Vec<Option<Value>>, IoError> {
        match self {
            Self::Values(values) => Ok(values),
            other => Err(other.other("IoResult::Values")),
        }
    }

    /// The value of a read of exactly one row.
    pub fn value(self) -> Result<Option<Value>, IoError> {
        match self {
            Self::Values(values) if values.len() == 1 => Ok(values.into_iter().next().flatten()),
            other => Err(other.other("IoResult::Values with one value")),
        }
    }

    /// The end of a write, a delete or a sync.
    pub fn done(self) -> Result<(), IoError> {
        match self {
            Self::Done => Ok(()),
            other => Err(other.other("IoResult::Done")),
        }
    }

    /// The rows of a page and the key to continue after.
    pub fn page(self) -> Result<(Vec<(Key, Value)>, Option<Key>), IoError> {
        match self {
            Self::Page { rows, next } => Ok((rows, next)),
            other => Err(other.other("IoResult::Page")),
        }
    }

    /// The event that answered an effect or a child.
    pub fn event(self) -> Result<Event, IoError> {
        match self {
            Self::Event(event) => Ok(*event),
            other => Err(other.other("IoResult::Event")),
        }
    }
}

/// What an operation does next.
#[derive(Debug, PartialEq)]
pub enum Next<T> {
    Call(IoCall),
    Done(T),
    /// Only after a window member: add no member and wait for the next result.
    Wait,
    /// The outcome, then `calls` made in order after the commit, outside any transaction.
    /// Their answers and failures never change the outcome; failures are logged.
    Then {
        output: T,
        calls: Vec<IoCall>,
    },
}

/// An I/O failure the runner reports into the operation's error type.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IoError {
    #[error("a storage transaction conflicted")]
    Conflict,
    #[error(transparent)]
    Storage(StorageError),
    #[error("expected {expected}, got {got}")]
    Unexpected {
        expected: &'static str,
        got: &'static str,
    },
    #[error("the runner aborted the operation")]
    Aborted,
    #[error("the {effect} adapter did not answer in time")]
    Hung { effect: &'static str },
    #[error("the operation did not finish")]
    Unfinished,
    #[error("a row did not encode or decode: {0}")]
    Codec(String),
}

/// A runner failure says nothing about the request, so the node counts as unavailable.
impl From<IoError> for crate::structs::SyncRefusal {
    fn from(_: IoError) -> Self {
        Self::Unavailable
    }
}

impl From<IoError> for StorageError {
    /// A storage failure as itself, a conflict as the transaction conflict, any other as a read
    /// error naming it.
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => error,
            IoError::Conflict => StorageError::TransactionConflict,
            other => StorageError::ReadError(other.to_string()),
        }
    }
}

/// A runner failure as an authorization error: storage faults stay storage errors, a conflict
/// stays the storage conflict, so the conflict check finds it.
impl From<IoError> for crate::metadata::MetadataError {
    /// A storage failure stays typed; any other runner failure is a backend error naming it.
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::Backend(other.to_string()),
        }
    }
}

impl From<IoError> for crate::errors::AuthorizationError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "call".to_string(),
                expected,
                got: got.to_string(),
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => Self::NotFinished,
            IoError::Codec(reason) => Self::StorageError(StorageError::ReadError(reason)),
        }
    }
}

/// The runner's machine for an [`Operation`]: it makes each call, checks every event, runs the
/// storage calls in the operation's transaction mode, commits its outcome
/// and aborts on an error.
#[derive(Debug)]
pub(crate) struct CallRun<P: Operation> {
    operation: P,
    mode: TransactionMode,
    txn_id: Option<TxnId>,
    state: State<P::Output, P::Error>,
}

#[derive(Debug)]
enum State<O, E> {
    Created,
    /// Waits for the transaction that this storage call needs.
    Starting(IoCall),
    Waiting(Wait),
    /// Waits for the commit; the calls run after it.
    Committing(O, Vec<IoCall>),
    /// The committed outcome while the calls after the commit run.
    After {
        output: O,
        calls: VecDeque<IoCall>,
        wait: Wait,
    },
    Finished(Result<O, E>),
}

/// The event a call waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wait {
    Values(usize),
    Page,
    Written,
    Deleted,
    Synced,
    Family(Family),
    Child,
    Group,
    Window,
    Delay,
}

/// The event family that answers a passed-through effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Family {
    Blob,
    StagingSource,
    LocalFile,
    Storage,
    Net,
    Metadata,
    Task,
    Index,
    Session,
    Outbound,
    Compute,
    Job,
}

impl Wait {
    fn expected(self) -> &'static str {
        match self {
            Self::Values(_) => "StorageEvent::BatchReadResult",
            Self::Page => "StorageEvent::IterResult",
            Self::Written => "StorageEvent::BatchWriteResult",
            Self::Deleted => "StorageEvent::BatchDeleteResult",
            Self::Synced => "StorageEvent::SyncAllFinished",
            Self::Family(Family::Blob) => "a blob event",
            Self::Family(Family::StagingSource) => "a staging source event",
            Self::Family(Family::LocalFile) => "a local file event",
            Self::Family(Family::Storage) => "a storage event",
            Self::Family(Family::Net) => "a net event",
            Self::Family(Family::Metadata) => "a metadata event",
            Self::Family(Family::Task) => "a task event",
            Self::Family(Family::Index) => "an index event",
            Self::Family(Family::Session) => "a session event",
            Self::Family(Family::Outbound) => "an outbound event",
            Self::Family(Family::Compute) => "a compute event",
            Self::Family(Family::Job) => "a job host event",
            Self::Child => "the child's event",
            Self::Group => "GroupEvent::Finished",
            Self::Window => "GroupEvent::Member or GroupEvent::Closed",
            Self::Delay => "Event::Delayed",
        }
    }
}

impl<P: Operation> PartialEq for CallRun<P> {
    fn eq(&self, other: &Self) -> bool {
        self.operation == other.operation
            && self.mode == other.mode
            && self.txn_id == other.txn_id
            && std::mem::discriminant(&self.state) == std::mem::discriminant(&other.state)
    }
}

impl<P: Operation> CallRun<P> {
    pub(crate) fn new(operation: P) -> Self {
        Self {
            mode: operation.transaction(),
            operation,
            txn_id: None,
            state: State::Created,
        }
    }

    fn advance(&mut self, mut turn: Result<Next<P::Output>, P::Error>) -> Effects {
        loop {
            let mut call = match turn {
                Err(error) => return self.fail(error),
                Ok(Next::Done(output)) => return self.finish(output, Vec::new()),
                Ok(Next::Then { output, calls }) => return self.finish(output, calls),
                Ok(Next::Wait) => {
                    let got = "Next::Wait outside a window";
                    return self.fail(unexpected("a call or an outcome", got));
                }
                Ok(Next::Call(call)) => call,
            };
            if let Some(result) = empty_result(&call) {
                turn = self.operation.next(result);
                continue;
            }
            if self.txn_id.is_none() && in_mode(&mut call, self.mode) {
                self.state = State::Starting(call);
                let read = self.mode == TransactionMode::ReadSnapshot;
                let start = StorageEffect::StartTransaction { read };
                return smallvec![Effect::Storage(start)];
            }
            return self.request(call);
        }
    }

    /// The transaction a call runs in: the open one when the call joins the mode, else none.
    fn txn_for(&self, call: &mut IoCall) -> Option<TxnId> {
        self.txn_id.filter(|_| in_mode(call, self.mode))
    }

    fn request(&mut self, mut call: IoCall) -> Effects {
        let txn_id = self.txn_for(&mut call);
        match call_effect(call, txn_id) {
            Ok((wait, effect)) => {
                self.state = State::Waiting(wait);
                smallvec![effect]
            }
            Err(error) => self.fail(error),
        }
    }

    /// Hands a window member to the operation; a call it returns joins the window.
    fn member(&mut self, event: GroupEvent) -> Effects {
        self.state = State::Waiting(Wait::Window);
        match self.operation.next(IoResult::Group(event)) {
            Ok(Next::Wait) => smallvec![],
            // A window group returned from a member adds all its members to the open window.
            Ok(Next::Call(IoCall::Group(EffectGroup {
                members,
                form: GroupForm::Window,
                ..
            }))) => members.into_iter().collect(),
            Ok(Next::Call(mut call)) => {
                let txn_id = self.txn_for(&mut call);
                match call_effect(call, txn_id) {
                    Ok((_, effect)) => smallvec![effect],
                    Err(error) => self.fail(error),
                }
            }
            turn => self.advance(turn),
        }
    }

    fn finish(&mut self, output: P::Output, calls: Vec<IoCall>) -> Effects {
        match self.txn_id {
            Some(txn_id) => {
                self.state = State::Committing(output, calls);
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            None => self.after(output, calls.into()),
        }
    }

    /// Makes the next call after the commit; a refused call is logged and skipped.
    fn after(&mut self, output: P::Output, mut calls: VecDeque<IoCall>) -> Effects {
        while let Some(call) = calls.pop_front() {
            if empty_result(&call).is_some() {
                continue;
            }
            match call_effect(call, None) {
                Ok((wait, effect)) => {
                    self.state = State::After {
                        output,
                        calls,
                        wait,
                    };
                    return smallvec![effect];
                }
                Err(error) => warn!(%error, "A call after the commit was refused"),
            }
        }
        self.state = State::Finished(Ok(output));
        smallvec![]
    }

    /// Ends with `error`: aborts an open transaction unless its commit was sent, then sends the
    /// operation's cleanup calls outside any transaction.
    fn fail(&mut self, error: impl Into<P::Error>) -> Effects {
        let committing = matches!(self.state, State::Committing(..));
        self.state = State::Finished(Err(error.into()));
        let mut effects = match committing {
            true => {
                self.txn_id = None;
                smallvec![]
            }
            false => abort_effects(&mut self.txn_id),
        };
        let cleanup = self.operation.abort().into_iter();
        effects.extend(
            cleanup.filter_map(|call| call_effect(call, None).ok().map(|(_, effect)| effect)),
        );
        effects
    }
}

impl<P: Operation> EffectOperation for CallRun<P> {
    type Output = P::Output;
    type Error = P::Error;

    fn start(&mut self) -> Effects {
        let turn = self.operation.first();
        self.advance(turn)
    }

    fn step(&mut self, event: Event) -> Effects {
        match std::mem::replace(&mut self.state, State::Created) {
            State::Starting(call) => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    self.request(call)
                }
                other => self.fail(failure("StorageEvent::TransactionStarted", other)),
            },
            State::Waiting(Wait::Window) => match event {
                Event::Group(member @ GroupEvent::Member { .. }) => self.member(member),
                other => self.deliver(Wait::Window, other),
            },
            State::Waiting(wait) => self.deliver(wait, event),
            State::Committing(output, calls) => match event {
                Event::Storage(StorageEvent::TransactionCommitted { txn_id })
                    if Some(txn_id) == self.txn_id =>
                {
                    self.txn_id = None;
                    self.after(output, calls.into())
                }
                other => {
                    // The commit was sent: the runner owns its outcome, so no abort follows.
                    self.state = State::Committing(output, calls);
                    self.fail(failure("StorageEvent::TransactionCommitted", other))
                }
            },
            State::After {
                output,
                calls,
                wait: Wait::Window,
            } if matches!(event, Event::Group(GroupEvent::Member { .. })) => {
                let wait = Wait::Window;
                self.state = State::After {
                    output,
                    calls,
                    wait,
                };
                smallvec![]
            }
            State::After {
                output,
                calls,
                wait,
            } => {
                if let Err(error) = answer(wait, event) {
                    warn!(%error, "A call after the commit failed");
                }
                self.after(output, calls)
            }
            State::Created | State::Finished(_) => {
                let late = unexpected("no event", event.label());
                self.state = State::Finished(Err(late.into()));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Finished(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            State::Finished(result) => result,
            State::After { output, .. } => Ok(output),
            _ => Err(IoError::Unfinished.into()),
        }
    }

    fn abort(&mut self) -> Effects {
        match std::mem::replace(&mut self.state, State::Created) {
            State::After { output, .. } => {
                self.state = State::Finished(Ok(output));
                smallvec![]
            }
            state @ (State::Committing(..) | State::Finished(_)) => {
                self.state = state;
                smallvec![]
            }
            _ => self.fail(IoError::Aborted),
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.operation.stamp(now_ms);
    }

    fn hold_scope(&self) -> Option<HoldScope> {
        self.operation.hold_scope()
    }

    fn expected_error(error: &Self::Error) -> bool {
        P::expected_error(error)
    }

    fn is_conflict(error: &Self::Error) -> bool {
        P::is_conflict(error)
    }

    fn conflict_retries() -> u32 {
        P::conflict_retries()
    }

    fn on_conflict(&mut self, attempt: u32) -> AfterConflict<P::Output> {
        self.operation.on_conflict(attempt)
    }
}

impl<P: Operation> CallRun<P> {
    /// Hands the answer to the operation, or fails when the event does not answer the call.
    fn deliver(&mut self, wait: Wait, event: Event) -> Effects {
        match answer(wait, event) {
            Ok(result) => {
                let turn = self.operation.next(result);
                self.advance(turn)
            }
            Err(error) => self.fail(error),
        }
    }
}

fn unexpected(expected: &'static str, got: &'static str) -> IoError {
    IoError::Unexpected { expected, got }
}

/// The error for an event that does not answer the call: a storage failure, or a refusal.
fn failure(expected: &'static str, event: Event) -> IoError {
    match event {
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => IoError::Conflict,
        Event::Storage(StorageEvent::Error { error }) => IoError::Storage(error),
        Event::Hung { effect } => IoError::Hung { effect },
        other => unexpected(expected, other.label()),
    }
}

fn answer(wait: Wait, event: Event) -> Result<IoResult, IoError> {
    match (wait, event) {
        (Wait::Values(count), Event::Storage(StorageEvent::BatchReadResult { values }))
            if values.len() == count =>
        {
            Ok(IoResult::Values(
                values.into_iter().map(|(_, value)| value).collect(),
            ))
        }
        (
            Wait::Page,
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }),
        ) => Ok(IoResult::Page {
            rows: values,
            next: next_start_after,
        }),
        (Wait::Written, Event::Storage(StorageEvent::BatchWriteResult { .. }))
        | (Wait::Deleted, Event::Storage(StorageEvent::BatchDeleteResult { .. }))
        | (Wait::Synced, Event::Storage(StorageEvent::SyncAllFinished)) => Ok(IoResult::Done),
        // A refused storage effect fails like a storage call, a lost conflict as a conflict.
        (Wait::Family(Family::Storage), event @ Event::Storage(StorageEvent::Error { .. })) => {
            Err(failure("a storage event", event))
        }
        (Wait::Family(family), event) if event_family(&event) == Some(family) => {
            Ok(IoResult::Event(Box::new(event)))
        }
        (Wait::Child, Event::Child(outcome)) => Ok(IoResult::Child(outcome)),
        (Wait::Child, event) => Ok(IoResult::Event(Box::new(event))),
        (Wait::Group, Event::Group(finished @ GroupEvent::Finished { .. }))
        | (Wait::Window, Event::Group(finished @ GroupEvent::Closed)) => {
            Ok(IoResult::Group(finished))
        }
        (Wait::Delay, Event::Delayed) => Ok(IoResult::Woke),
        (wait, event) => Err(failure(wait.expected(), event)),
    }
}

/// The answer to an empty read, write or delete, which needs no storage.
fn empty_result(call: &IoCall) -> Option<IoResult> {
    match call {
        IoCall::Read(reads) if reads.is_empty() => Some(IoResult::Values(Vec::new())),
        IoCall::Write(rows) if rows.is_empty() => Some(IoResult::Done),
        IoCall::Delete(keys) if keys.is_empty() => Some(IoResult::Done),
        _ => None,
    }
}

/// Whether the call runs in the operation's transaction of `mode`: a storage call, or a storage
/// effect that takes a transaction id and names none; a snapshot takes only reads.
fn in_mode(call: &mut IoCall, mode: TransactionMode) -> bool {
    let snapshot = match mode {
        TransactionMode::None => return false,
        TransactionMode::Write => false,
        TransactionMode::ReadSnapshot => true,
    };
    match call {
        IoCall::Read(_) | IoCall::Page { .. } => true,
        IoCall::Write(_)
        | IoCall::Delete(_)
        | IoCall::AddUsage { .. }
        | IoCall::AddCounters { .. } => !snapshot,
        IoCall::Effect(effect) => match effect.as_mut() {
            Effect::Storage(storage) => {
                let reads = matches!(
                    storage,
                    StorageEffect::Read { .. }
                        | StorageEffect::BatchRead { .. }
                        | StorageEffect::Iter { .. }
                        | StorageEffect::IterRange { .. }
                        | StorageEffect::Last { .. }
                        | StorageEffect::VaultRead { .. }
                );
                (reads || !snapshot) && storage.txn_slot().is_some_and(|slot| slot.is_none())
            }
            _ => false,
        },
        _ => false,
    }
}

fn call_effect(call: IoCall, txn_id: Option<TxnId>) -> Result<(Wait, Effect), IoError> {
    let (wait, storage) = match call {
        IoCall::Read(reads) => (
            Wait::Values(reads.len()),
            StorageEffect::BatchRead { reads, txn_id },
        ),
        IoCall::Page {
            space,
            prefix,
            after,
            limit,
        } => (
            Wait::Page,
            StorageEffect::Iter {
                key_space: space,
                prefix: Some(prefix),
                start: after.map(IterStart::After),
                limit,
                txn_id,
            },
        ),
        IoCall::Write(writes) => (Wait::Written, StorageEffect::BatchWrite { writes, txn_id }),
        IoCall::Delete(deletes) => (
            Wait::Deleted,
            StorageEffect::BatchDelete { deletes, txn_id },
        ),
        IoCall::Sync => (Wait::Synced, StorageEffect::SyncAll),
        IoCall::Effect(mut effect) => {
            if let Effect::Storage(storage) = effect.as_mut()
                && let Some(slot) = storage.txn_slot()
                && slot.is_none()
            {
                *slot = txn_id;
            }
            return Ok((Wait::Family(effect_family(&effect)?), *effect));
        }
        IoCall::Child(child) => return Ok((Wait::Child, Effect::SubOperation(child))),
        IoCall::Group(group) => {
            let wait = match group.form {
                GroupForm::All { .. } => Wait::Group,
                GroupForm::Window => Wait::Window,
            };
            return Ok((wait, Effect::Group(group)));
        }
        IoCall::Delay(after) => return Ok((Wait::Delay, Effect::Delay(after))),
        IoCall::AddUsage {
            space,
            deltas,
            buckets,
        } => (
            Wait::Written,
            StorageEffect::AddUsage {
                key_space: space,
                deltas,
                buckets,
                txn_id: txn_id.ok_or(unexpected("a transaction", "IoCall::AddUsage"))?,
            },
        ),
        IoCall::AddCounters { space, deltas } => (
            Wait::Written,
            StorageEffect::AddCounters {
                key_space: space,
                deltas,
                txn_id: txn_id.ok_or(unexpected("a transaction", "IoCall::AddCounters"))?,
            },
        ),
    };
    Ok((wait, Effect::Storage(storage)))
}

/// The family whose event answers a passed-through effect; effects with their own call and
/// transaction control effects are refused.
fn effect_family(effect: &Effect) -> Result<Family, IoError> {
    let expected = "an effect without its own call";
    match effect {
        Effect::Blob(_) => Ok(Family::Blob),
        Effect::StagingSource(_) => Ok(Family::StagingSource),
        Effect::LocalFile(_) => Ok(Family::LocalFile),
        Effect::Net(_) => Ok(Family::Net),
        Effect::Metadata(_) => Ok(Family::Metadata),
        Effect::Task(_) => Ok(Family::Task),
        Effect::Index(_) => Ok(Family::Index),
        Effect::Session(_) => Ok(Family::Session),
        Effect::Outbound(_) => Ok(Family::Outbound),
        Effect::Compute(_) => Ok(Family::Compute),
        Effect::Job(_) => Ok(Family::Job),
        Effect::Storage(
            StorageEffect::StartTransaction { .. }
            | StorageEffect::CommitTransaction { .. }
            | StorageEffect::AbortTransaction { .. }
            | StorageEffect::BeginUsageRebuild { .. }
            | StorageEffect::AddUsage { .. }
            | StorageEffect::AddCounters { .. },
        ) => Err(unexpected(expected, "a transaction control effect")),
        Effect::Storage(_) => Ok(Family::Storage),
        Effect::SubOperation(_) => Err(unexpected(expected, "Effect::SubOperation")),
        Effect::Group(_) => Err(unexpected(expected, "Effect::Group")),
        Effect::Delay(_) => Err(unexpected(expected, "Effect::Delay")),
        Effect::Search() => Err(unexpected(expected, "Effect::Search")),
        Effect::Stream() => Err(unexpected(expected, "Effect::Stream")),
    }
}

fn event_family(event: &Event) -> Option<Family> {
    match event {
        Event::Blob(_) => Some(Family::Blob),
        Event::StagingSource(_) => Some(Family::StagingSource),
        Event::LocalFile(_) => Some(Family::LocalFile),
        Event::Storage(_) => Some(Family::Storage),
        Event::Net(_) => Some(Family::Net),
        Event::Metadata(_) => Some(Family::Metadata),
        Event::Task(_) => Some(Family::Task),
        Event::Index(_) => Some(Family::Index),
        Event::Session(_) => Some(Family::Session),
        Event::Outbound(_) => Some(Family::Outbound),
        Event::Compute(_) => Some(Family::Compute),
        Event::Job(_) => Some(Family::Job),
        Event::SubOperation(_)
        | Event::Search()
        | Event::Stream()
        | Event::Group(_)
        | Event::Delayed
        | Event::Hung { .. }
        | Event::Child(_) => None,
    }
}

#[cfg(test)]
#[path = "calls_tests.rs"]
mod tests;
