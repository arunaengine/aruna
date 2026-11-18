//! Declares the operation traits the runner drives: calls and results, or effects and events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::any::Any;

use smallvec::smallvec;

use crate::backoff::conflict_backoff;
use crate::calls::{CallRun, IoCall, IoError, IoResult, Next};
use crate::{effects::Effect, events::Event, types::Effects};

/// A backend scope at most one operation holds at a time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HoldScope {
    /// The document reconcile of `net`, which also excludes its shard membership changes and
    /// topic evictions.
    DocumentReconcile,
    /// The plugin run admissions of this node, so no install passes its queue bound.
    PluginAdmissions,
    /// The plugin artifact copies of this node, so progress rows never lose a chunk.
    PluginCopies,
    /// The changes of one plugin install on this node, so they never overlap here.
    PluginInstall(ulid::Ulid),
    /// This node's writes to one metadata document, so its actor's dots stay in order.
    DocumentWrite(ulid::Ulid),
}

/// One use case as decisions without I/O: `first` and `next` return the next call or the
/// outcome. The runner makes each call, checks that the event answers it, runs the storage calls
/// in one write transaction when `transaction` holds, commits the outcome and aborts on an error.
pub trait Operation: Send + std::fmt::Debug + PartialEq {
    type Output: Send + std::fmt::Debug;
    /// `From<IoError::Conflict>` must give a value equal only to itself (see `is_conflict`).
    type Error: Send + std::fmt::Debug + PartialEq + From<IoError>;

    /// The first call, or the outcome when the operation needs no I/O.
    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error>;
    /// The next call after `result`, or the outcome.
    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error>;

    /// How the runner groups the storage calls; by default each runs on its own.
    fn transaction(&self) -> TransactionMode {
        TransactionMode::None
    }

    /// Cleanup calls the runner sends once when the run ends in an error before its commit is
    /// acknowledged (deadline, hang limit, refused event, own error); unanswered, best effort.
    fn abort(&mut self) -> Vec<IoCall> {
        Vec::new()
    }

    /// See [`EffectOperation::stamp`].
    fn stamp(&mut self, _now_ms: u64) {}

    /// See [`EffectOperation::hold_scope`].
    fn hold_scope(&self) -> Option<HoldScope> {
        None
    }

    /// See [`EffectOperation::expected_error`].
    fn expected_error(_error: &Self::Error) -> bool
    where
        Self: Sized,
    {
        false
    }

    /// Whether `error` is a lost transaction conflict; by default the runner's own conflict.
    fn is_conflict(error: &Self::Error) -> bool
    where
        Self: Sized,
    {
        *error == Self::Error::from(IoError::Conflict)
    }

    /// See [`EffectOperation::conflict_retries`].
    fn conflict_retries() -> u32
    where
        Self: Sized,
    {
        3
    }

    /// Decides on the freshly built next attempt after attempt `attempt` (from 1) of
    /// `drive_retry` lost a conflict; it may set its own state first, such as a recheck flag.
    /// By default it reruns `conflict_retries()` times, then fails.
    fn on_conflict(&mut self, attempt: u32) -> AfterConflict<Self::Output>
    where
        Self: Sized,
    {
        AfterConflict::rerun_within(attempt, Self::conflict_retries())
    }
}

/// How the runner groups an operation's storage calls.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TransactionMode {
    /// Each storage call runs on its own.
    #[default]
    None,
    /// Every storage call runs in one write transaction, committed with the outcome and aborted
    /// on an error.
    Write,
    /// Reads and pages run in one read-only snapshot; writes, deletes and syncs run on their own.
    ReadSnapshot,
}

/// What the conflict retry does after a run lost a transaction conflict.
#[derive(Debug, PartialEq)]
pub enum AfterConflict<T> {
    /// Runs the fresh attempt after the backoff.
    Rerun,
    /// Ends with this output, without a rerun.
    Finish(T),
    /// Ends with the conflict error.
    Fail,
}

impl<T> AfterConflict<T> {
    /// The default policy: rerun while `attempt` is at most `retries`, then fail.
    pub fn rerun_within(attempt: u32, retries: u32) -> Self {
        match attempt <= retries {
            true => Self::Rerun,
            false => Self::Fail,
        }
    }
}

/// An operation written against raw effects and events: `start` returns the first effects,
/// `step` consumes each dispatched event, `is_complete` ends the stream, `finalize` reads the
/// outcome, `abort` requests cleanup. It goes away once every one is an [`Operation`].
pub trait EffectOperation: Send + std::fmt::Debug + PartialEq {
    type Output: Send + std::fmt::Debug;
    type Error: Send + std::fmt::Debug;

    fn start(&mut self) -> Effects;
    fn step(&mut self, events: Event) -> Effects;
    fn is_complete(&self) -> bool;
    /// Consumes the operation once the runner stops and reads its outcome. The
    /// loop calls it even after an early stop, so a nonterminal state must return
    /// an error rather than a successful default.
    fn finalize(self) -> Result<Self::Output, Self::Error>;
    /// Requests cleanup effects for a run that stops before completion. They are
    /// requests the runner may drop, time out, or hand to storage, so returning
    /// them is not proof that cleanup finished.
    fn abort(&mut self) -> Effects;

    /// Whether an error is an ordinary client-visible outcome (not found, denied)
    /// rather than an operational failure. Expected errors are logged at debug so
    /// polling clients cannot flood the error stream and its span exporter.
    fn expected_error(_error: &Self::Error) -> bool
    where
        Self: Sized,
    {
        false
    }

    /// The runner's clock in Unix milliseconds, given right before `start` and each `step`.
    /// An operation that needs the time keeps it here instead of reading a clock; tests forge it.
    fn stamp(&mut self, _now_ms: u64) {}

    /// The backend scope this operation holds from before its first effect until it ends, so
    /// no two operations with the same scope overlap. Only a top-level drive takes it.
    fn hold_scope(&self) -> Option<HoldScope> {
        None
    }

    /// Whether an error is a lost transaction conflict that a fresh run of the operation may
    /// avoid. The runner's conflict retry reruns the operation only after such an error.
    fn is_conflict(_error: &Self::Error) -> bool
    where
        Self: Sized,
    {
        false
    }

    /// How often the runner's conflict retry reruns the operation after a conflict before it
    /// returns the error: reruns after the first run, so three attempts are two retries.
    fn conflict_retries() -> u32
    where
        Self: Sized,
    {
        3
    }

    /// See [`Operation::on_conflict`].
    fn on_conflict(&mut self, attempt: u32) -> AfterConflict<Self::Output>
    where
        Self: Sized,
    {
        AfterConflict::rerun_within(attempt, Self::conflict_retries())
    }

    /// Whether a deadline must still run `abort` after one of the operation's
    /// transactions committed. Only an operation whose `abort` protects already
    /// committed work may opt in.
    fn abort_after_commit(&self) -> bool {
        false
    }
}

pub trait SubOperation: Send + std::fmt::Debug {
    fn start(&mut self) -> Effects;
    fn step(&mut self, event: Event) -> Effects;
    fn is_complete(&self) -> bool;
    fn finalize(self: Box<Self>) -> Event;
    fn abort(&mut self) -> Effects;
    /// See [`EffectOperation::stamp`].
    fn stamp(&mut self, _now_ms: u64) {}
    fn as_any(&self) -> &dyn Any;
    fn eq_dyn(&self, other: &dyn SubOperation) -> bool;
}

impl PartialEq for dyn SubOperation {
    fn eq(&self, other: &Self) -> bool {
        self.eq_dyn(other)
    }
}

struct MappedSubOperation<O, F>
where
    O: EffectOperation,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send,
{
    operation: Option<O>,
    map_result: Option<F>,
}

impl<O, F> PartialEq for MappedSubOperation<O, F>
where
    O: EffectOperation,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send,
{
    fn eq(&self, other: &Self) -> bool {
        self.operation == other.operation
    }
}

impl<O, F> std::fmt::Debug for MappedSubOperation<O, F>
where
    O: EffectOperation,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedSubOperation")
            .field("operation", &self.operation)
            .finish()
    }
}

impl<O, F> MappedSubOperation<O, F>
where
    O: EffectOperation,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send,
{
    fn new(operation: O, map_result: F) -> Self {
        Self {
            operation: Some(operation),
            map_result: Some(map_result),
        }
    }
}

impl<O, F> SubOperation for MappedSubOperation<O, F>
where
    O: EffectOperation + 'static,
    O::Output: Send + 'static,
    O::Error: Send + 'static,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send + 'static,
{
    fn start(&mut self) -> Effects {
        self.operation
            .as_mut()
            .expect("suboperation must be present in start")
            .start()
    }

    fn step(&mut self, event: Event) -> Effects {
        self.operation
            .as_mut()
            .expect("suboperation must be present in step")
            .step(event)
    }

    fn is_complete(&self) -> bool {
        self.operation
            .as_ref()
            .expect("suboperation must be present in is_complete")
            .is_complete()
    }

    fn finalize(self: Box<Self>) -> Event {
        let mut this = *self;
        let operation = this
            .operation
            .take()
            .expect("suboperation must be present in finalize");
        let map_result = this
            .map_result
            .take()
            .expect("result mapper must be present in finalize");
        map_result(operation.finalize())
    }

    fn abort(&mut self) -> Effects {
        self.operation
            .as_mut()
            .expect("suboperation must be present in abort")
            .abort()
    }

    fn stamp(&mut self, now_ms: u64) {
        if let Some(operation) = self.operation.as_mut() {
            operation.stamp(now_ms);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn eq_dyn(&self, other: &dyn SubOperation) -> bool {
        other
            .as_any()
            .downcast_ref::<MappedSubOperation<O, F>>()
            .is_some_and(|o| self == o)
    }
}

/// Boxes an operation of either kind as a child whose outcome `map_result` turns into an event.
pub fn boxed_suboperation<O, K, F>(operation: O, map_result: F) -> Box<dyn SubOperation>
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: Send + 'static,
    O::Error: Send + 'static,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send + 'static,
{
    Box::new(MappedSubOperation::new(
        operation.into_machine(),
        map_result,
    ))
}

/// Boxes a child built by `build` that reruns after a lost conflict as `drive_retry` does: each
/// rerun is a fresh build whose `on_conflict` decides, after a backoff seeded by `seed`.
pub fn boxed_retry<O, K, B, F>(mut build: B, seed: &[u8], map_result: F) -> Box<dyn SubOperation>
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: Send + 'static,
    O::Error: Send + 'static,
    B: FnMut() -> O + Send + 'static,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send + 'static,
{
    let mut build = move || build().into_machine();
    Box::new(RetrySubOperation {
        attempt: Some(Attempt::Running(build())),
        build,
        lost: 0,
        seed: seed.to_vec(),
        now_ms: None,
        map_result,
    })
}

/// A child that reruns per `on_conflict`. `attempt` is `None` only inside a method.
struct RetrySubOperation<M: EffectOperation, B, F> {
    build: B,
    attempt: Option<Attempt<M>>,
    lost: u32,
    seed: Vec<u8>,
    now_ms: Option<u64>,
    map_result: F,
}

#[derive(Debug)]
enum Attempt<M: EffectOperation> {
    Running(M),
    /// A fresh build waits for its backoff.
    Waiting(M),
    Ended(Result<M::Output, M::Error>),
}

impl<M: EffectOperation, B, F> RetrySubOperation<M, B, F>
where
    B: FnMut() -> M,
{
    /// Starts a fresh run, then reads its outcome if it needed no I/O.
    fn run(&mut self, mut run: M) -> Effects {
        if let Some(now_ms) = self.now_ms {
            run.stamp(now_ms);
        }
        let effects = run.start();
        self.settle(run, effects)
    }

    /// Keeps a running run; reads a finished one, where a lost conflict asks a fresh build.
    fn settle(&mut self, run: M, mut effects: Effects) -> Effects {
        if !run.is_complete() {
            self.attempt = Some(Attempt::Running(run));
            return effects;
        }
        let error = match run.finalize() {
            Err(error) if M::is_conflict(&error) => error,
            result => {
                self.attempt = Some(Attempt::Ended(result));
                return effects;
            }
        };
        self.lost = self.lost.saturating_add(1);
        let mut next = (self.build)();
        self.attempt = Some(match next.on_conflict(self.lost) {
            AfterConflict::Rerun => {
                let index = usize::try_from(self.lost - 1).unwrap_or(usize::MAX);
                effects.push(Effect::Delay(conflict_backoff(index, &self.seed)));
                Attempt::Waiting(next)
            }
            AfterConflict::Finish(output) => Attempt::Ended(Ok(output)),
            AfterConflict::Fail => Attempt::Ended(Err(error)),
        });
        effects
    }
}

/// Equal runs in the same phase; ended attempts compare by their count of lost runs only.
impl<M: EffectOperation, B, F> PartialEq for RetrySubOperation<M, B, F> {
    fn eq(&self, other: &Self) -> bool {
        let same = match (&self.attempt, &other.attempt) {
            (Some(Attempt::Running(one)), Some(Attempt::Running(two)))
            | (Some(Attempt::Waiting(one)), Some(Attempt::Waiting(two))) => one == two,
            (Some(Attempt::Ended(_)), Some(Attempt::Ended(_))) | (None, None) => true,
            _ => false,
        };
        same && self.lost == other.lost
    }
}

impl<M: EffectOperation, B, F> std::fmt::Debug for RetrySubOperation<M, B, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetrySubOperation")
            .field("attempt", &self.attempt)
            .field("lost", &self.lost)
            .finish()
    }
}

impl<M, B, F> SubOperation for RetrySubOperation<M, B, F>
where
    M: EffectOperation + 'static,
    M::Output: Send + 'static,
    M::Error: Send + 'static,
    B: FnMut() -> M + Send + 'static,
    F: FnOnce(Result<M::Output, M::Error>) -> Event + Send + 'static,
{
    fn start(&mut self) -> Effects {
        match self.attempt.take() {
            Some(Attempt::Running(run)) => self.run(run),
            other => {
                self.attempt = other;
                smallvec![]
            }
        }
    }

    /// While a rerun waits for its backoff, only `Event::Delayed` matters: other events answer
    /// the cleanup of the lost run.
    fn step(&mut self, event: Event) -> Effects {
        match self.attempt.take() {
            Some(Attempt::Running(mut run)) => {
                let effects = run.step(event);
                self.settle(run, effects)
            }
            Some(Attempt::Waiting(run)) if matches!(event, Event::Delayed) => self.run(run),
            other => {
                self.attempt = other;
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.attempt, Some(Attempt::Ended(_)))
    }

    fn finalize(self: Box<Self>) -> Event {
        let this = *self;
        let result = match this
            .attempt
            .expect("a retry child holds an attempt between calls")
        {
            Attempt::Running(run) | Attempt::Waiting(run) => run.finalize(),
            Attempt::Ended(result) => result,
        };
        (this.map_result)(result)
    }

    fn abort(&mut self) -> Effects {
        match self.attempt.take() {
            Some(Attempt::Running(mut run)) => {
                let effects = run.abort();
                self.attempt = Some(Attempt::Running(run));
                effects
            }
            Some(Attempt::Waiting(run)) => {
                self.attempt = Some(Attempt::Ended(run.finalize()));
                smallvec![]
            }
            other => {
                self.attempt = other;
                smallvec![]
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = Some(now_ms);
        if let Some(Attempt::Running(run)) = self.attempt.as_mut() {
            run.stamp(now_ms);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn eq_dyn(&self, other: &dyn SubOperation) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| self == other)
    }
}

/// Marks an [`EffectOperation`] for [`Runnable`].
pub enum ByEffects {}

/// Marks an [`Operation`] for [`Runnable`].
pub enum ByCalls {}

/// An operation of either kind, as the effect machine the runner drives. `K` only tells the
/// two blanket implementations apart, so callers never name it.
pub trait Runnable<K> {
    type Output: Send + std::fmt::Debug;
    type Error: Send + std::fmt::Debug;

    fn into_machine(self) -> impl EffectOperation<Output = Self::Output, Error = Self::Error>;
}

impl<T: EffectOperation> Runnable<ByEffects> for T {
    type Output = T::Output;
    type Error = T::Error;

    fn into_machine(self) -> impl EffectOperation<Output = Self::Output, Error = Self::Error> {
        self
    }
}

impl<T: Operation> Runnable<ByCalls> for T {
    type Output = T::Output;
    type Error = T::Error;

    fn into_machine(self) -> impl EffectOperation<Output = Self::Output, Error = Self::Error> {
        CallRun::new(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::calls::ChildOutcome;

    /// Loses a conflict until its hook sets `recheck`; finishes on the lost run `finish_at`.
    #[derive(Debug, PartialEq)]
    struct Rechecking {
        recheck: bool,
        finish_at: Option<u32>,
    }

    impl Operation for Rechecking {
        type Output = &'static str;
        type Error = IoError;

        fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
            match self.recheck {
                true => Ok(Next::Done("rechecked")),
                false => Err(IoError::Conflict),
            }
        }

        fn next(&mut self, _: IoResult) -> Result<Next<Self::Output>, IoError> {
            Err(IoError::Conflict)
        }

        fn on_conflict(&mut self, lost: u32) -> AfterConflict<Self::Output> {
            if self.finish_at == Some(lost) {
                return AfterConflict::Finish("finished");
            }
            self.recheck = lost >= 2;
            AfterConflict::Rerun
        }
    }

    const SEED: [u8; 16] = [7; 16];

    /// A retry child of `Rechecking` and the count of runs it built.
    fn child(finish_at: Option<u32>) -> (Box<dyn SubOperation>, Arc<AtomicU32>) {
        let built = Arc::new(AtomicU32::new(0));
        let count = built.clone();
        let build = move || {
            count.fetch_add(1, Ordering::SeqCst);
            Rechecking {
                recheck: false,
                finish_at,
            }
        };
        let child = boxed_retry(build, &SEED, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        (child, built)
    }

    fn outcome(result: Result<&'static str, IoError>) -> Event {
        Event::Child(ChildOutcome::new(result))
    }

    #[test]
    fn child_reruns_conflict() {
        let (mut child, built) = child(None);
        let first = Effect::Delay(conflict_backoff(0, &SEED));
        assert_eq!(child.start().as_slice(), &[first]);
        // While a rerun waits, other events answer the cleanup of the lost run.
        assert!(child.step(Event::Hung { effect: "storage" }).is_empty());
        let second = Effect::Delay(conflict_backoff(1, &SEED));
        assert_eq!(child.step(Event::Delayed).as_slice(), &[second]);
        assert!(!child.is_complete());
        assert!(child.step(Event::Delayed).is_empty());
        assert!(child.is_complete());
        assert_eq!(built.load(Ordering::SeqCst), 3);
        assert_eq!(child.finalize(), outcome(Ok("rechecked")));
    }

    #[test]
    fn child_finishes_conflict() {
        let (mut child, built) = child(Some(1));
        assert!(child.start().is_empty());
        assert!(child.is_complete());
        assert_eq!(built.load(Ordering::SeqCst), 2);
        assert_eq!(child.finalize(), outcome(Ok("finished")));
    }

    #[test]
    fn abort_ends_backoff() {
        let (mut child, _) = child(None);
        assert_eq!(child.start().len(), 1);
        assert!(child.abort().is_empty());
        assert!(child.is_complete());
        assert_eq!(child.finalize(), outcome(Err(IoError::Unfinished)));
    }
}
