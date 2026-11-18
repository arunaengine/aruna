//! Run records on an install's receiver: admission within the install's bounds, reads, pages,
//! and saving a run, which drops it from the queue once it ended.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{PLUGIN_QUEUE_KEYSPACE, PLUGIN_RUNS_KEYSPACE};
use aruna_core::operation::{HoldScope, Operation, TransactionMode};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{
    ATTENTION_ROW, MAX_ATTEMPTS, PluginRun, RUN_RETENTION_MS, RUN_ROW, RUNS_QUEUED, RunState,
    RunTrigger, SourceEvent, WAITING_ROW, parse_queue_key, queue_key, run_key, run_prefix,
    timer_key,
};
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugins::trigger::{Trigger, TriggerKind};
use std::time::Duration;

use aruna_core::backoff::capped_backoff;
use aruna_plugins::{FailureKind, InvokeError};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::install::{pick_version, read_manifest};
use super::intents::{first_open, intent_keys, intent_scan, resolved_rows};
use super::matcher::event_trigger;
use super::rows::{
    Row, Rows, Scan, StoreError, Writes, decode, delete_call, done, encode, page_call, page_of,
    read_call, row, unexpected, values, writes,
};
use super::work::{WorkRow, work_scan};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RunError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the install has {0} unfinished or unresolved runs, its limit")]
    Full(usize),
    #[error("no run with this id")]
    NotFound,
    #[error("the run is {0}, not waiting for a resolution")]
    NotWaiting(&'static str),
    #[error("the run is {0}; only a failed or cancelled run is retried")]
    NotRetried(&'static str),
    #[error("no job end of the install waits to be delivered")]
    NotHeld,
}

impl From<IoError> for RunError {
    fn from(error: IoError) -> Self {
        Self::Store(error.into())
    }
}

/// A sub-plan that admits a run, a new one or one queued again, within its install's bound.
#[derive(Debug, PartialEq)]
pub struct Admission {
    run: Box<PluginRun>,
    queued: Scan,
}

impl Admission {
    pub fn new(run: PluginRun) -> Self {
        let prefix = run.install_id.to_bytes().to_vec();
        Self {
            run: Box::new(run),
            queued: Scan::new(PLUGIN_QUEUE_KEYSPACE, prefix),
        }
    }

    pub fn ask(&self) -> IoCall {
        self.queued.ask()
    }

    /// Takes a page of the install's queue; returns the run's rows once the queue is read.
    pub fn take(&mut self, answer: IoResult) -> Result<Option<Vec<Row>>, RunError> {
        let Some(queued) = self.queued.take(answer)? else {
            return Ok(None);
        };
        if queued.len() >= RUNS_QUEUED {
            return Err(RunError::Full(queued.len()));
        }
        let (install_id, run_id) = (self.run.install_id, self.run.run_id);
        Ok(Some(vec![
            row(
                PLUGIN_QUEUE_KEYSPACE,
                queue_key(install_id, run_id),
                Vec::new(),
            ),
            row(
                PLUGIN_RUNS_KEYSPACE,
                run_key(install_id, run_id),
                encode(&*self.run)?,
            ),
        ]))
    }
}

/// Stores a new queued run, unless its install already has `RUNS_QUEUED` unfinished ones.
pub fn admit(run: PluginRun) -> Admit {
    Admit {
        admission: Admission::new(run),
        written: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct Admit {
    admission: Admission,
    written: bool,
}

impl Operation for Admit {
    type Output = ();
    type Error = RunError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginAdmissions)
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, RunError> {
        Ok(Next::Call(self.admission.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<()>, RunError> {
        if self.written {
            return Ok(done(answer).map(|()| Next::Done(()))?);
        }
        Ok(match self.admission.take(answer)? {
            None => Next::Call(self.admission.ask()),
            Some(rows) => {
                self.written = true;
                Next::Call(IoCall::Write(rows))
            }
        })
    }
}

pub fn read_run(install_id: Ulid, run_id: Ulid) -> ReadRun {
    ReadRun(run_key(install_id, run_id))
}

#[derive(Debug, PartialEq)]
pub struct ReadRun(Vec<u8>);

impl Operation for ReadRun {
    type Output = PluginRun;
    type Error = RunError;

    fn first(&mut self) -> Result<Next<PluginRun>, RunError> {
        let read = vec![(PLUGIN_RUNS_KEYSPACE, self.0.clone())];
        Ok(Next::Call(read_call(read)))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<PluginRun>, RunError> {
        let [found] = values::<1>(answer)?;
        Ok(Next::Done(decode(&found.ok_or(RunError::NotFound)?)?))
    }
}

/// At most `limit` runs of an install after the run `after`, oldest first, and the id of the
/// last one when more may follow.
pub fn list_runs(install_id: Ulid, after: Option<Ulid>, limit: usize) -> ListRuns {
    ListRuns(Some(page_call(
        PLUGIN_RUNS_KEYSPACE,
        run_prefix(install_id),
        after.map(|run_id| run_key(install_id, run_id)),
        limit,
    )))
}

#[derive(Debug, PartialEq)]
pub struct ListRuns(Option<IoCall>);

impl Operation for ListRuns {
    type Output = (Vec<PluginRun>, Option<Ulid>);
    type Error = RunError;

    fn first(&mut self) -> Result<Next<Self::Output>, RunError> {
        let page = self.0.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(page))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, RunError> {
        let (rows, next) = page_of(answer)?;
        let runs = (rows.iter())
            .map(|(_, value)| decode::<PluginRun>(value))
            .collect::<Result<Vec<_>, _>>()?;
        let next = next.and(runs.last().map(|run| run.run_id));
        Ok(Next::Done((runs, next)))
    }
}

/// The install and run ids of every unfinished run on this node; unresolved runs wait for a
/// person and are left out.
pub fn unfinished() -> Unfinished {
    Unfinished(Scan::new(PLUGIN_QUEUE_KEYSPACE, Vec::new()))
}

#[derive(Debug, PartialEq)]
pub struct Unfinished(Scan);

impl Operation for Unfinished {
    type Output = Vec<(Ulid, Ulid)>;
    type Error = RunError;

    fn first(&mut self) -> Result<Next<Self::Output>, RunError> {
        Ok(Next::Call(self.0.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, RunError> {
        let Some(rows) = self.0.take(answer)? else {
            return Ok(Next::Call(self.0.ask()));
        };
        Ok(Next::Done(
            (rows.iter())
                .filter(|(_, value)| ![ATTENTION_ROW, WAITING_ROW].contains(&value.as_slice()))
                .filter_map(|(key, _)| parse_queue_key(key))
                .collect(),
        ))
    }
}

/// The rows that save a run: its record, and its queue row marked for attention or, once it
/// ended, removed.
pub(crate) fn saved_rows(
    run: &PluginRun,
) -> Result<(Vec<Row>, Vec<(&'static str, Vec<u8>)>), RunError> {
    let mut rows = vec![row(
        PLUGIN_RUNS_KEYSPACE,
        run_key(run.install_id, run.run_id),
        encode(run)?,
    )];
    let queued = queue_key(run.install_id, run.run_id);
    let mut deletes = Vec::new();
    match run.state {
        RunState::NeedsAttention => {
            rows.push(row(PLUGIN_QUEUE_KEYSPACE, queued, ATTENTION_ROW.to_vec()));
        }
        state if state.finished() => deletes.push((PLUGIN_QUEUE_KEYSPACE, queued)),
        _ => {}
    }
    Ok((rows, deletes))
}

/// Writes the run; a run that ended leaves the queue in the same transaction.
pub fn save_run(run: &PluginRun) -> Result<Writes, RunError> {
    let (rows, deletes) = saved_rows(run)?;
    Ok(writes(rows, deletes))
}

/// Ends a run that needs attention as a person decided: `done` when its effects happened.
/// Its open intents record the decision and the audit row is written in the same transaction.
pub fn resolve_run(
    (install_id, run_id): (Ulid, Ulid),
    done: bool,
    audit: &AdminAuditRecord,
    now_ms: u64,
) -> Result<ResolveRun, RunError> {
    let outcome = match done {
        true => "done",
        false => "not done",
    };
    let entries = audit_rows(audit)?;
    Ok(ResolveRun {
        key: run_key(install_id, run_id),
        done,
        message: format!("{} resolved it as {outcome}", audit.actor),
        now_ms,
        audit: entries,
        phase: ResolvePhase::Reading,
    })
}

#[derive(Debug, PartialEq)]
pub struct ResolveRun {
    key: Vec<u8>,
    done: bool,
    message: String,
    now_ms: u64,
    audit: Vec<Row>,
    phase: ResolvePhase,
}

#[derive(Debug, PartialEq)]
enum ResolvePhase {
    Reading,
    Intents(Box<PluginRun>, Scan),
    Writing(Box<PluginRun>, Vec<(&'static str, Vec<u8>)>),
    Deleting(Box<PluginRun>),
}

impl Operation for ResolveRun {
    type Output = PluginRun;
    type Error = RunError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<PluginRun>, RunError> {
        let read = vec![(PLUGIN_RUNS_KEYSPACE, self.key.clone())];
        Ok(Next::Call(read_call(read)))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<PluginRun>, RunError> {
        match std::mem::replace(&mut self.phase, ResolvePhase::Reading) {
            ResolvePhase::Reading => {
                let [found] = values::<1>(answer)?;
                let mut run = decode::<PluginRun>(&found.ok_or(RunError::NotFound)?)?;
                if run.state != RunState::NeedsAttention {
                    return Err(RunError::NotWaiting(run.state.as_str()));
                }
                let state = match self.done {
                    true => RunState::Succeeded,
                    false => RunState::Failed,
                };
                run.finish(state, Some(std::mem::take(&mut self.message)), self.now_ms);
                let scan = intent_scan(run.run_id);
                let ask = scan.ask();
                self.phase = ResolvePhase::Intents(Box::new(run), scan);
                Ok(Next::Call(ask))
            }
            ResolvePhase::Intents(run, mut scan) => {
                let Some(intents) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = ResolvePhase::Intents(run, scan);
                    return Ok(Next::Call(ask));
                };
                let (mut rows, deletes) = saved_rows(&run)?;
                rows.extend(resolved_rows(intents, self.done)?);
                rows.append(&mut self.audit);
                self.phase = ResolvePhase::Writing(run, deletes);
                Ok(Next::Call(IoCall::Write(rows)))
            }
            ResolvePhase::Writing(run, deletes) => {
                done(answer)?;
                self.phase = ResolvePhase::Deleting(run);
                Ok(Next::Call(delete_call(deletes)))
            }
            ResolvePhase::Deleting(run) => {
                done(answer)?;
                Ok(Next::Done(*run))
            }
        }
    }
}

/// The runs that ended longer than `RUN_RETENTION_MS` ago and may go, as install and run ids.
/// A run waiting for a person, for its job's end, or whose calls may have had effects or cannot
/// be read stays.
pub(crate) fn expired_runs(now_ms: u64) -> ExpiredRuns {
    ExpiredRuns {
        now_ms,
        phase: ExpiredPhase::Runs(Scan::new(PLUGIN_RUNS_KEYSPACE, vec![RUN_ROW])),
        prunable: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct ExpiredRuns {
    now_ms: u64,
    phase: ExpiredPhase,
    prunable: Vec<(Ulid, Ulid)>,
}

#[derive(Debug, PartialEq)]
enum ExpiredPhase {
    Runs(Scan),
    Held(Vec<PluginRun>, Scan),
    /// Reads the intents of the last expired run; the others follow.
    Intents(Vec<PluginRun>, Scan),
    Done,
}

impl ExpiredRuns {
    /// The intents of the next expired run, or the outcome when none is left.
    fn next_run(&mut self, runs: Vec<PluginRun>) -> Next<Vec<(Ulid, Ulid)>> {
        let Some(run_id) = runs.last().map(|run| run.run_id) else {
            return Next::Done(std::mem::take(&mut self.prunable));
        };
        let scan = intent_scan(run_id);
        let ask = scan.ask();
        self.phase = ExpiredPhase::Intents(runs, scan);
        Next::Call(ask)
    }
}

impl Operation for ExpiredRuns {
    type Output = Vec<(Ulid, Ulid)>;
    type Error = RunError;

    fn first(&mut self) -> Result<Next<Self::Output>, RunError> {
        match &self.phase {
            ExpiredPhase::Runs(scan) => Ok(Next::Call(scan.ask())),
            _ => Err(StoreError("prune started twice".into()).into()),
        }
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, RunError> {
        match std::mem::replace(&mut self.phase, ExpiredPhase::Done) {
            ExpiredPhase::Runs(mut scan) => {
                let Some(rows) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = ExpiredPhase::Runs(scan);
                    return Ok(Next::Call(ask));
                };
                let mut expired = Vec::new();
                for (_, value) in rows {
                    let run = decode::<PluginRun>(&value)?;
                    let ended = (run.finished_ms).is_some_and(|finished| {
                        finished.saturating_add(RUN_RETENTION_MS) <= self.now_ms
                    });
                    if ended && run.state != RunState::NeedsAttention {
                        expired.push(run);
                    }
                }
                let scan = work_scan();
                let ask = scan.ask();
                self.phase = ExpiredPhase::Held(expired, scan);
                Ok(Next::Call(ask))
            }
            ExpiredPhase::Held(mut expired, mut scan) => {
                let Some(rows) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = ExpiredPhase::Held(expired, scan);
                    return Ok(Next::Call(ask));
                };
                let held = held_runs(rows)?;
                expired.retain(|run| !held.contains(&run.run_id));
                expired.reverse();
                Ok(self.next_run(expired))
            }
            ExpiredPhase::Intents(mut runs, mut scan) => {
                let intents = match scan.take(answer) {
                    Ok(None) => {
                        let ask = scan.ask();
                        self.phase = ExpiredPhase::Intents(runs, scan);
                        return Ok(Next::Call(ask));
                    }
                    Ok(Some(intents)) => Some(intents),
                    Err(_) => None,
                };
                let run = runs.pop().ok_or(RunError::NotFound)?;
                if intents.is_some_and(|intents| matches!(first_open(&intents), Ok(None))) {
                    self.prunable.push((run.install_id, run.run_id));
                }
                Ok(self.next_run(runs))
            }
            ExpiredPhase::Done => Err(unexpected(answer).into()),
        }
    }
}

/// Deletes one run record and every intent and stored reply of the run in one transaction.
pub(crate) fn drop_run((install_id, run_id): (Ulid, Ulid)) -> DropRun {
    DropRun {
        key: run_key(install_id, run_id),
        intents: intent_scan(run_id),
        phase: DropPhase::Run,
    }
}

#[derive(Debug, PartialEq)]
pub struct DropRun {
    key: Vec<u8>,
    intents: Scan,
    phase: DropPhase,
}

#[derive(Debug, PartialEq)]
enum DropPhase {
    Run,
    Intents,
    Deleting,
}

impl Operation for DropRun {
    type Output = ();
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, StoreError> {
        Ok(Next::Call(delete_call(vec![(
            PLUGIN_RUNS_KEYSPACE,
            self.key.clone(),
        )])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<()>, StoreError> {
        match self.phase {
            DropPhase::Run => {
                done(answer)?;
                self.phase = DropPhase::Intents;
                Ok(Next::Call(self.intents.ask()))
            }
            DropPhase::Intents => match self.intents.take(answer)? {
                None => Ok(Next::Call(self.intents.ask())),
                Some(rows) => {
                    self.phase = DropPhase::Deleting;
                    Ok(Next::Call(delete_call(intent_keys(rows))))
                }
            },
            DropPhase::Deleting => {
                done(answer)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// The runs a completion still waits on among the work `rows`: those of works not notified yet,
/// which keep their record until the completion is delivered or dropped.
pub(crate) fn held_runs(rows: Rows) -> Result<Vec<Ulid>, StoreError> {
    let mut runs = Vec::new();
    for (_, value) in rows {
        let row: WorkRow = decode(&value)?;
        if !row.notified {
            runs.push(row.run_id);
        }
    }
    Ok(runs)
}

/// The rows of an admin audit record.
pub fn audit_rows(audit: &AdminAuditRecord) -> Result<Vec<Row>, StoreError> {
    audit
        .entries()
        .map_err(|error| StoreError(error.to_string()))
}

/// How often the worker looks for due runs.
pub const TICK: Duration = Duration::from_secs(1);
/// The longest wait before a failed attempt runs again.
const MAX_BACKOFF: Duration = Duration::from_secs(10 * 60);

/// How one attempt of a run ended, before its intents are inspected.
#[derive(Debug)]
pub(crate) enum Ended {
    Output(Option<String>),
    Failed(String),
    /// A host or infrastructure error: the run starts again later.
    Retry(String),
    Cancelled(String),
    /// The runtime had no free slot: the run waits without using an attempt.
    Busy(String),
    /// The node stops; the run starts again from the top at the next start.
    Stopped,
}

/// How an invocation ended. A runtime without a free slot never entered the guest, so the run
/// waits without using an attempt.
pub(crate) fn ended(invoked: Result<Option<String>, InvokeError>, stopping: bool) -> Ended {
    match invoked {
        Ok(output) => Ended::Output(output),
        Err(InvokeError::Guest(failure)) => match failure.kind {
            FailureKind::Permanent | FailureKind::InvalidInput => Ended::Failed(failure.message),
            FailureKind::Retryable => Ended::Retry(failure.message),
        },
        Err(InvokeError::Cancelled) if stopping => Ended::Stopped,
        Err(InvokeError::Cancelled) => Ended::Cancelled("the run was cancelled".into()),
        Err(error @ (InvokeError::Overloaded | InvokeError::Closed)) => {
            Ended::Busy(error.to_string())
        }
        Err(error) => Ended::Failed(error.to_string()),
    }
}

/// Ends or queues a run after an attempt. Its intents decide first: one pending or unknown
/// makes it need attention, and unreadable intents keep it queued without using an attempt.
pub(crate) fn conclude(
    run: &mut PluginRun,
    ended: Ended,
    changed: bool,
    open: Result<Option<String>, StoreError>,
    now_ms: u64,
) {
    match open {
        Ok(Some(reason)) => return run.finish(RunState::NeedsAttention, Some(reason), now_ms),
        Err(error) => return hold(run, format!("its calls are not readable: {error}"), now_ms),
        Ok(None) => {}
    }
    if changed {
        let message = "the run made a call that differs from its earlier attempt";
        return run.finish(RunState::Failed, Some(message.into()), now_ms);
    }
    match ended {
        Ended::Output(output) => {
            run.output = output;
            run.finish(RunState::Succeeded, None, now_ms);
        }
        Ended::Failed(message) => run.finish(RunState::Failed, Some(message), now_ms),
        Ended::Cancelled(message) => run.finish(RunState::Cancelled, Some(message), now_ms),
        Ended::Retry(message) => retry(run, message, now_ms),
        Ended::Busy(message) => hold(run, message, now_ms),
        Ended::Stopped => {}
    }
}

/// Queues the run again soon without counting the attempt that just ended.
fn hold(run: &mut PluginRun, message: String, now_ms: u64) {
    run.attempts = run.attempts.saturating_sub(1);
    run.state = RunState::Queued;
    run.message = Some(message);
    run.due_ms = now_ms.saturating_add(u64::try_from(TICK.as_millis()).unwrap_or(u64::MAX));
}

/// Queues the run again after a backoff, or fails it after its last attempt.
fn retry(run: &mut PluginRun, message: String, now_ms: u64) {
    if run.attempts >= MAX_ATTEMPTS {
        run.finish(RunState::Failed, Some(message), now_ms);
        return;
    }
    let backoff = capped_backoff(Duration::from_secs(1), MAX_BACKOFF, run.attempts, 0);
    run.state = RunState::Queued;
    run.message = Some(message);
    run.due_ms = now_ms.saturating_add(u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX));
}

/// The last due occurrence of an install's timer, and occurrences skipped at the queue bound.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerRow {
    pub last_due_ms: u64,
    pub skipped: u64,
}

/// A queued run of the install's current revision, due at `due_ms`.
pub(crate) fn new_run(
    install: &PluginInstall,
    handler: String,
    trigger: RunTrigger,
    due_ms: u64,
) -> PluginRun {
    PluginRun {
        run_id: Ulid::generate(),
        install_id: install.install_id,
        group_id: install.group_id,
        plugin: install.plugin.clone(),
        version: install.version.clone(),
        revision: install.revision,
        digest: install.digest.clone(),
        handler,
        settings: install.settings.clone(),
        trigger,
        state: RunState::Queued,
        attempts: 0,
        due_ms,
        created_ms: due_ms,
        finished_ms: None,
        message: None,
        output: None,
        log: Vec::new(),
    }
}

/// Queues the due occurrence of an install's timer, or counts it as skipped over the queue bound.
pub(crate) fn enqueue_timer(
    install: &PluginInstall,
    handler: String,
    interval_ms: u64,
    now_ms: u64,
) -> EnqueueTimer {
    EnqueueTimer {
        install: Box::new(install.clone()),
        handler,
        interval_ms,
        now_ms,
        key: timer_key(install.install_id),
        phase: TimerPhase::Reading,
    }
}

#[derive(Debug, PartialEq)]
pub struct EnqueueTimer {
    install: Box<PluginInstall>,
    handler: String,
    interval_ms: u64,
    now_ms: u64,
    key: Vec<u8>,
    phase: TimerPhase,
}

#[derive(Debug, PartialEq)]
enum TimerPhase {
    Reading,
    Admitting(TimerRow, Admission),
    Writing(bool),
}

impl Operation for EnqueueTimer {
    type Output = bool;
    type Error = StoreError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginAdmissions)
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<bool>, StoreError> {
        Ok(Next::Call(read_call(vec![(
            PLUGIN_RUNS_KEYSPACE,
            self.key.clone(),
        )])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<bool>, StoreError> {
        match std::mem::replace(&mut self.phase, TimerPhase::Writing(false)) {
            TimerPhase::Reading => {
                let [found] = values::<1>(answer)?;
                let mut row = match found {
                    Some(bytes) => decode::<TimerRow>(&bytes)?,
                    None => TimerRow::default(),
                };
                // Occurrences count from the last activation, so a pause is never replayed.
                let base =
                    (row.last_due_ms).max(self.install.active_since_ms.unwrap_or(self.now_ms));
                if self.now_ms < base.saturating_add(self.interval_ms) {
                    return Ok(Next::Done(false));
                }
                let occurrence = base + (self.now_ms - base) / self.interval_ms * self.interval_ms;
                let trigger = RunTrigger::Timer {
                    occurrence_ms: occurrence,
                };
                let handler = std::mem::take(&mut self.handler);
                let run = new_run(&self.install, handler, trigger, self.now_ms);
                row.last_due_ms = occurrence;
                let admission = Admission::new(run);
                let ask = admission.ask();
                self.phase = TimerPhase::Admitting(row, admission);
                Ok(Next::Call(ask))
            }
            TimerPhase::Admitting(row, mut admission) => {
                let (admitted, rows) = match admission.take(answer) {
                    Ok(None) => {
                        let ask = admission.ask();
                        self.phase = TimerPhase::Admitting(row, admission);
                        return Ok(Next::Call(ask));
                    }
                    Ok(Some(rows)) => (true, rows),
                    Err(RunError::Store(error)) => return Err(error),
                    Err(_) => (false, Vec::new()),
                };
                let mut row = row;
                row.skipped += u64::from(!admitted);
                let mut rows = rows;
                let value = encode(&row)?;
                rows.push(super::rows::row(
                    PLUGIN_RUNS_KEYSPACE,
                    self.key.clone(),
                    value,
                ));
                self.phase = TimerPhase::Writing(admitted);
                Ok(Next::Call(IoCall::Write(rows)))
            }
            TimerPhase::Writing(admitted) => {
                done(answer)?;
                Ok(Next::Done(admitted))
            }
        }
    }
}

/// Removes the runs that ended longer than the retention ago, each with its intents in one
/// transaction; answers how many went.
#[derive(Debug, PartialEq)]
pub(crate) struct PruneRuns {
    now_ms: u64,
    expired: Option<VecDeque<(Ulid, Ulid)>>,
    count: usize,
}

impl PruneRuns {
    pub(crate) fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            expired: None,
            count: 0,
        }
    }

    fn drop_next(&mut self) -> Next<usize> {
        match self.expired.as_mut().and_then(VecDeque::pop_front) {
            Some(ids) => Next::Call(IoCall::child(drop_run(ids))),
            None => Next::Done(self.count),
        }
    }
}

impl Operation for PruneRuns {
    type Output = usize;
    type Error = RunError;

    fn first(&mut self) -> Result<Next<usize>, RunError> {
        Ok(Next::Call(IoCall::child(expired_runs(self.now_ms))))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, RunError> {
        match self.expired {
            None => {
                let expired = answer.child::<Vec<(Ulid, Ulid)>, RunError>()??;
                self.count = expired.len();
                self.expired = Some(expired.into());
            }
            Some(_) => answer.child::<(), StoreError>()??,
        }
        Ok(self.drop_next())
    }
}

/// The trigger of `kind` the install's pinned version declares.
pub(crate) fn declared_trigger(
    config: &RealmConfigDocument,
    install: &PluginInstall,
    kind: TriggerKind,
) -> Option<Trigger> {
    let plugin = pick_version(config, &install.plugin, Some(&install.digest)).ok()?;
    read_manifest(&plugin).ok()?.triggers.remove(&kind)
}

/// A run of the install's handler for the event's kind, due at `now_ms`, or `None` when the
/// manifest has no trigger of that kind.
pub(crate) fn event_run(
    config: &RealmConfigDocument,
    install: &PluginInstall,
    event: &SourceEvent,
    now_ms: u64,
) -> Option<PluginRun> {
    let kind = event_trigger(event.kind);
    declared_trigger(config, install, kind)?;
    let trigger = RunTrigger::Event(event.clone());
    Some(new_run(install, kind.as_str().to_string(), trigger, now_ms))
}

#[cfg(test)]
#[path = "runs_tests.rs"]
pub(crate) mod tests;
