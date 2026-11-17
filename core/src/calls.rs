//! Declares the I/O calls of an operation and runs an operation as calls, events and checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use smallvec::smallvec;

use crate::effects::{Effect, EffectGroup, GroupForm, IterStart, StorageEffect, abort_effects};
use crate::errors::StorageError;
use crate::events::{Event, GroupEvent, StorageEvent};
use crate::operation::{EffectOperation, HoldScope, Operation, SubOperation};
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
    Effect(Effect),
    /// A child operation, answered by the event its result maps to.
    Child(Box<dyn SubOperation>),
    /// Independent effects run at the same time. An all-at-once group is answered once; a
    /// window answers each member, then `GroupEvent::Closed`.
    Group(EffectGroup),
    /// A wake after this long on the runner's clock.
    Delay(Duration),
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
    /// The event that answered an `IoCall::Effect` or an `IoCall::Child`.
    Event(Event),
    Group(GroupEvent),
    /// The delay passed.
    Woke,
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

impl IoError {
    /// The storage failure this error is, a lost conflict as `TransactionConflict`; any other
    /// error as it is.
    pub fn into_storage(self) -> Result<StorageError, IoError> {
        match self {
            Self::Conflict => Ok(StorageError::TransactionConflict),
            Self::Storage(error) => Ok(error),
            other => Err(other),
        }
    }
}

/// The runner's machine for an [`Operation`]: it makes each call, checks every event, runs the
/// storage calls in one write transaction when the operation asks for one, commits its outcome
/// and aborts on an error.
#[derive(Debug)]
pub(crate) struct CallRun<P: Operation> {
    operation: P,
    transaction: bool,
    txn_id: Option<TxnId>,
    state: State<P::Output, P::Error>,
}

#[derive(Debug)]
enum State<O, E> {
    Created,
    /// Waits for the transaction that this storage call needs.
    Starting(IoCall),
    Waiting(Wait),
    Committing(O),
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
            && self.transaction == other.transaction
            && self.txn_id == other.txn_id
            && std::mem::discriminant(&self.state) == std::mem::discriminant(&other.state)
    }
}

impl<P: Operation> CallRun<P> {
    pub(crate) fn new(operation: P) -> Self {
        Self {
            transaction: operation.transaction(),
            operation,
            txn_id: None,
            state: State::Created,
        }
    }

    fn advance(&mut self, mut turn: Result<Next<P::Output>, P::Error>) -> Effects {
        loop {
            let mut call = match turn {
                Err(error) => return self.fail(error),
                Ok(Next::Done(output)) => return self.finish(output),
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
            if self.transaction && self.txn_id.is_none() && in_storage(&mut call) {
                self.state = State::Starting(call);
                let start = StorageEffect::StartTransaction { read: false };
                return smallvec![Effect::Storage(start)];
            }
            return self.request(call);
        }
    }

    fn request(&mut self, call: IoCall) -> Effects {
        match call_effect(call, self.txn_id) {
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
            Ok(Next::Call(call)) => match call_effect(call, self.txn_id) {
                Ok((_, effect)) => smallvec![effect],
                Err(error) => self.fail(error),
            },
            turn => self.advance(turn),
        }
    }

    fn finish(&mut self, output: P::Output) -> Effects {
        match self.txn_id {
            Some(txn_id) => {
                self.state = State::Committing(output);
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            None => {
                self.state = State::Finished(Ok(output));
                smallvec![]
            }
        }
    }

    /// Ends with `error`; an open transaction is aborted unless its commit was sent.
    fn fail(&mut self, error: impl Into<P::Error>) -> Effects {
        let committing = matches!(self.state, State::Committing(_));
        self.state = State::Finished(Err(error.into()));
        match committing {
            true => {
                self.txn_id = None;
                smallvec![]
            }
            false => abort_effects(&mut self.txn_id),
        }
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
            State::Committing(output) => match event {
                Event::Storage(StorageEvent::TransactionCommitted { txn_id })
                    if Some(txn_id) == self.txn_id =>
                {
                    self.txn_id = None;
                    self.state = State::Finished(Ok(output));
                    smallvec![]
                }
                other => {
                    // The commit was sent: the runner owns its outcome, so no abort follows.
                    self.state = State::Committing(output);
                    self.fail(failure("StorageEvent::TransactionCommitted", other))
                }
            },
            State::Created | State::Finished(_) => self.fail(unexpected("no event", event.label())),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Finished(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            State::Finished(result) => result,
            _ => Err(IoError::Unfinished.into()),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            State::Committing(_) | State::Finished(_) => smallvec![],
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
        (Wait::Family(family), event) if event_family(&event) == Some(family) => {
            Ok(IoResult::Event(event))
        }
        (Wait::Child, event) => Ok(IoResult::Event(event)),
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

/// Whether the call runs in the operation's transaction: a storage call, or a storage effect
/// that takes a transaction id and names none.
fn in_storage(call: &mut IoCall) -> bool {
    match call {
        IoCall::Read(_) | IoCall::Page { .. } | IoCall::Write(_) | IoCall::Delete(_) => true,
        IoCall::Effect(Effect::Storage(effect)) => effect.txn_slot().is_some_and(|slot| slot.is_none()),
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
            if let Effect::Storage(storage) = &mut effect
                && let Some(slot) = storage.txn_slot()
                && slot.is_none()
            {
                *slot = txn_id;
            }
            return Ok((Wait::Family(effect_family(&effect)?), effect));
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
        Effect::Storage(
            StorageEffect::StartTransaction { .. }
            | StorageEffect::CommitTransaction { .. }
            | StorageEffect::AbortTransaction { .. }
            | StorageEffect::BeginUsageRebuild { .. },
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
        Event::SubOperation(_)
        | Event::Search()
        | Event::Stream()
        | Event::Group(_)
        | Event::Delayed
        | Event::Hung { .. } => None,
    }
}

#[cfg(test)]
#[path = "calls_tests.rs"]
mod tests;
