//! Native work of plugin installs on their receiver: one row per install and identity keeps the
//! work's generated ids, binding and state, so a repeated call never starts the work twice.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod calls;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{PLUGIN_RUNS_KEYSPACE, PLUGIN_WORK_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{RUN_RETENTION_MS, run_key};
use aruna_core::structs::execution::job::identity::{JobId, JobState};
use aruna_core::structs::execution::job::input::{JobPayload, PluginWorkSpec};
use aruna_core::structs::execution::job::local::{JobRecord, JobResultPayload};
use aruna_core::types::Key;
use aruna_core::user::id::UserId;
use std::collections::BTreeSet;
use ulid::Ulid;

use super::marks::{Mark, mark_row};
use super::rows::{
    Row, Rows, Scan, StoreError, all_values, decode, delete_call, done, encode, page_call, page_of,
    read_call, row, unexpected, values,
};
use crate::jobs::prune::{
    DueJobs, JobPruneOutcome, PruneDueOperation, PruneError, PruneJobsOperation,
};
use aruna_core::install_work::{
    FinalState, Found, Identity, SendBoundary, Summary, WORK_INDEX, WORK_ROW, WorkKind, WorkLookup,
    WorkRow, WorkState, index_key, row_key,
};

/// The row of a work under its key.
pub(crate) fn row_of(key: Vec<u8>, work: &WorkRow) -> Result<Row, StoreError> {
    Ok(row(PLUGIN_WORK_KEYSPACE, key, encode(work)?))
}

/// What an attempt that sends may do, from the boundary earlier attempts left.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Send {
    /// The attempt wrote `Sending` and sends now.
    Now,
    /// An earlier attempt may have sent; nothing is sent again and the outcome stays unknown.
    Undecided,
    /// An earlier attempt has the answer; it decides without a second request.
    Answered {
        status: u16,
        reply: Option<String>,
        bytes: u64,
        version: Ulid,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WorkError {
    #[error("the call started other work with other arguments")]
    Conflict,
    #[error("the install has {0} unfinished tasks already")]
    Limit(usize),
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl From<IoError> for WorkError {
    fn from(error: IoError) -> Self {
        Self::Store(error.into())
    }
}

/// How the row of a call was found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reserve {
    /// This call wrote the row.
    Fresh,
    /// An earlier call with the same arguments wrote it.
    Earlier,
}

/// Writes `proposed` for `identity` in one transaction, or adopts the row an earlier call with the
/// same digest wrote; another digest conflicts, a former install's row is replaced. `marked` marks
/// the version the work writes there with the row, before any byte of it exists.
pub fn reserve(
    (install, identity): (&PluginInstall, &Identity),
    proposed: WorkRow,
    marked: Option<(&str, &str)>,
) -> Result<ReserveWork, WorkError> {
    let mark = match (marked, proposed.version_id) {
        (Some((bucket, object)), Some(version_id)) => {
            let mark = Mark {
                install_id: install.install_id,
                work_id: proposed.work_id,
                job: None,
                kind: proposed.kind,
                at_ms: proposed.created_ms,
            };
            Some(mark_row((bucket, object, version_id), &mark)?)
        }
        _ => None,
    };
    Ok(ReserveWork {
        key: row_key(install.install_id, identity)?,
        install_id: install.install_id,
        account: install.account,
        proposed: Some(Box::new(proposed)),
        mark,
        limit: None,
        phase: ReservePhase::Reading,
    })
}

impl ReserveWork {
    /// A fresh row is refused while the install has `limit` unsettled tasks, counted in the same
    /// transaction, so the row itself is the reservation.
    pub fn limited(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Writes the new rows, deleting a former install's index first.
    fn write(
        &mut self,
        rows: Vec<Row>,
        former: Option<Ulid>,
    ) -> Result<Next<(WorkRow, Reserve)>, WorkError> {
        // A former install's index goes too, so no index row leads to the replacement.
        let Some(former) = former else {
            return Ok(Next::Call(IoCall::Write(rows)));
        };
        self.phase = ReservePhase::Deleting(rows);
        let index = index_key(self.install_id, former);
        Ok(Next::Call(delete_call(vec![(PLUGIN_WORK_KEYSPACE, index)])))
    }
}

#[derive(Debug, PartialEq)]
pub struct ReserveWork {
    key: Vec<u8>,
    install_id: Ulid,
    account: UserId,
    proposed: Option<Box<WorkRow>>,
    mark: Option<Row>,
    limit: Option<usize>,
    phase: ReservePhase,
}

#[derive(Debug, PartialEq)]
enum ReservePhase {
    Reading,
    /// The install's open tasks are counted before a fresh task row is written.
    Counting(Scan, Vec<Row>, Option<Ulid>),
    /// A former install's index row is being deleted; the new rows follow.
    Deleting(Vec<Row>),
    Writing,
}

impl Operation for ReserveWork {
    type Output = (WorkRow, Reserve);
    type Error = WorkError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Self::Output>, WorkError> {
        let read = vec![(PLUGIN_WORK_KEYSPACE, self.key.clone())];
        Ok(Next::Call(read_call(read)))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, WorkError> {
        match std::mem::replace(&mut self.phase, ReservePhase::Writing) {
            ReservePhase::Reading => {
                let [stored] = values::<1>(answer)?;
                let stored = stored.map(|bytes| decode::<WorkRow>(&bytes)).transpose()?;
                let proposed = self.proposed.as_deref().ok_or(WorkError::Conflict)?;
                if let Some(stored) = stored.as_ref().filter(|row| row.account == self.account) {
                    return match stored.digest == proposed.digest {
                        true => Ok(Next::Done((stored.clone(), Reserve::Earlier))),
                        false => Err(WorkError::Conflict),
                    };
                }
                let index = index_key(self.install_id, proposed.work_id);
                let mut rows = vec![
                    row(PLUGIN_WORK_KEYSPACE, index, self.key.clone()),
                    row(PLUGIN_WORK_KEYSPACE, self.key.clone(), encode(proposed)?),
                ];
                rows.extend(self.mark.take());
                let former = stored.map(|former| former.work_id);
                if self.limit.is_none() {
                    return self.write(rows, former);
                }
                let prefix = [&[WORK_ROW][..], &self.install_id.to_bytes()].concat();
                let scan = Scan::new(PLUGIN_WORK_KEYSPACE, prefix);
                let ask = scan.ask();
                self.phase = ReservePhase::Counting(scan, rows, former);
                Ok(Next::Call(ask))
            }
            ReservePhase::Counting(mut scan, rows, former) => {
                let Some(found) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = ReservePhase::Counting(scan, rows, former);
                    return Ok(Next::Call(ask));
                };
                let mut open = 0;
                for (_, value) in found {
                    let row = decode::<WorkRow>(&value)?;
                    let task = row.kind == WorkKind::Recipe && row.account == self.account;
                    open += usize::from(task && row.state != WorkState::Settled);
                }
                match self.limit {
                    Some(limit) if open >= limit => Err(WorkError::Limit(limit)),
                    _ => self.write(rows, former),
                }
            }
            ReservePhase::Deleting(rows) => {
                done(answer)?;
                Ok(Next::Call(IoCall::Write(rows)))
            }
            ReservePhase::Writing => {
                done(answer)?;
                let proposed = self.proposed.take().ok_or(WorkError::Conflict)?;
                Ok(Next::Done((*proposed, Reserve::Fresh)))
            }
        }
    }
}

pub fn read_work(install_id: Ulid, work_id: Ulid) -> ReadWork {
    ReadWork(WorkLookup::new(install_id, work_id))
}

#[derive(Debug, PartialEq)]
pub struct ReadWork(WorkLookup);

impl Operation for ReadWork {
    type Output = Option<WorkRow>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(self.0.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        Ok(match self.0.take(answer)? {
            Found::Ask(ask) => Next::Call(ask),
            Found::Row(found) => Next::Done(found.map(|(_, row)| row)),
        })
    }
}

/// Whether a plugin work job must keep its record: its work row exists and is not settled yet.
pub fn holds_job(spec: &PluginWorkSpec) -> HoldsJob {
    HoldsJob(ReadWork(WorkLookup::new(spec.install_id, spec.work_id)))
}

#[derive(Debug, PartialEq)]
pub struct HoldsJob(ReadWork);

impl Operation for HoldsJob {
    type Output = bool;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<bool>, StoreError> {
        Ok(Next::Call(self.0.0.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<bool>, StoreError> {
        let holds = |row: Option<WorkRow>| row.is_some_and(|row| row.state != WorkState::Settled);
        Ok(match self.0.next(answer)? {
            Next::Call(ask) => Next::Call(ask),
            Next::Done(row) => Next::Done(holds(row)),
            Next::Wait => Next::Wait,
            Next::Then { output, calls } => Next::Then {
                output: holds(output),
                calls,
            },
        })
    }
}

/// One job prune pass that keeps a due plugin work job while [`holds_job`] says so, at the run's
/// start time. It deletes only checked jobs; held jobs do not fill a window, and a round that
/// deleted nothing while more jobs were due goes on after its checked window.
#[derive(Clone, Debug, PartialEq)]
pub struct PruneKeepingWork {
    page_size: usize,
    now_ms: Option<u64>,
    checks: Vec<(JobId, PluginWorkSpec)>,
    checked: BTreeSet<JobId>,
    kept: BTreeSet<JobId>,
    after: Option<Key>,
    start: Option<Key>,
    phase: KeepPhase,
}

#[derive(Clone, Debug, PartialEq)]
enum KeepPhase {
    Due,
    Holds(JobId),
    Prune,
}

type PruneNext = Result<Next<JobPruneOutcome>, PruneError>;

impl PruneKeepingWork {
    pub fn new(page_size: usize) -> Self {
        Self {
            page_size,
            now_ms: None,
            checks: Vec::new(),
            checked: BTreeSet::new(),
            kept: BTreeSet::new(),
            after: None,
            start: None,
            phase: KeepPhase::Due,
        }
    }

    fn due(&mut self) -> PruneNext {
        self.phase = KeepPhase::Due;
        let due = PruneDueOperation::new(self.now(), self.page_size, self.after.take());
        Ok(Next::Call(IoCall::child(due)))
    }

    fn now(&self) -> u64 {
        self.now_ms.unwrap_or_default()
    }

    fn check_next(&mut self) -> PruneNext {
        if let Some((job_id, spec)) = self.checks.pop() {
            self.phase = KeepPhase::Holds(job_id);
            return Ok(Next::Call(IoCall::child(holds_job(&spec))));
        }
        let waiting = self.checked.len().saturating_sub(self.kept.len());
        if self.after.is_some() && waiting < self.page_size.saturating_mul(4) {
            return self.due();
        }
        self.phase = KeepPhase::Prune;
        let (kept, checked) = (
            std::mem::take(&mut self.kept),
            std::mem::take(&mut self.checked),
        );
        let prune = PruneJobsOperation::new(self.now(), self.page_size, kept);
        let prune = prune.within(checked, self.start.clone());
        Ok(Next::Call(IoCall::child(prune)))
    }
}

impl Operation for PruneKeepingWork {
    type Output = JobPruneOutcome;
    type Error = PruneError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms.get_or_insert(now_ms);
    }

    fn first(&mut self) -> PruneNext {
        self.due()
    }

    fn next(&mut self, result: IoResult) -> PruneNext {
        match std::mem::replace(&mut self.phase, KeepPhase::Prune) {
            KeepPhase::Due => {
                let due = result.child::<DueJobs, PruneError>()??;
                self.after = due.after;
                for (job_id, payload) in due.jobs.into_iter().rev() {
                    self.checked.insert(job_id);
                    if let JobPayload::PluginWork(spec) = payload {
                        self.checks.push((job_id, spec));
                    }
                }
                self.check_next()
            }
            KeepPhase::Holds(job_id) => {
                let holds = result.child::<bool, StoreError>()?;
                if holds.map_err(|error| PruneError(error.0))? {
                    self.kept.insert(job_id);
                }
                self.check_next()
            }
            KeepPhase::Prune => {
                let outcome = result.child::<JobPruneOutcome, PruneError>()??;
                // Cleanups held every checked job and nothing was deleted; only deleted rows count.
                let empty = outcome.pruned == 0 && outcome.deleted_rows == 0;
                if empty && outcome.has_more && self.after.is_some() {
                    self.start = self.after.clone();
                    return self.due();
                }
                Ok(Next::Done(outcome))
            }
        }
    }
}

/// A change of one work row.
#[derive(Clone, Debug, PartialEq)]
pub enum WorkChange {
    /// Records the job a reserved work was submitted as; a later state keeps its job.
    Dispatched(JobId),
    /// Keeps the answer of a sent request: its status and its reply as exposed now, with the
    /// bytes and version it sent.
    Sent {
        status: u16,
        reply: Option<String>,
        bytes: u64,
        version: Ulid,
    },
    /// Forgets the send boundary, so the next attempt sends again.
    Reopened,
    /// Keeps the summary of a dispatched work whose job finished, at `now_ms`.
    Settled(Box<Summary>, u64),
    /// A removed install's job accepted its cancel.
    Cancelling(JobId),
}

impl WorkChange {
    fn apply(self, row: &mut WorkRow) {
        match self {
            WorkChange::Dispatched(job) => {
                if row.state == WorkState::Reserved {
                    row.state = WorkState::Dispatched;
                    row.job = Some(job);
                }
            }
            WorkChange::Sent {
                status,
                reply,
                bytes,
                version,
            } => {
                row.boundary = Some(SendBoundary::Sent {
                    status,
                    reply,
                    bytes,
                    version,
                });
            }
            WorkChange::Reopened => row.boundary = None,
            WorkChange::Settled(summary, now_ms) => {
                if row.state == WorkState::Dispatched {
                    // The paths and an upload's answer stay answerable after the job record is
                    // pruned.
                    let (source, target) = (row.source.clone(), row.target.clone());
                    let (status, reply) = match &row.boundary {
                        Some(SendBoundary::Sent { status, reply, .. }) => {
                            (Some(*status), reply.clone())
                        }
                        _ => (None, None),
                    };
                    row.state = WorkState::Settled;
                    row.summary = Some(Summary {
                        source,
                        target,
                        status,
                        reply,
                        ..*summary
                    });
                    row.settled_ms = Some(now_ms);
                }
            }
            WorkChange::Cancelling(job) => {
                row.job = Some(job);
                row.state = WorkState::Dispatched;
                row.cancel = true;
            }
        }
    }
}

/// Changes the row of work `work_id` in one transaction; `None` when it is gone.
pub fn update_work(ids: (Ulid, Ulid), change: WorkChange) -> UpdateWork {
    UpdateWork {
        lookup: WorkLookup::new(ids.0, ids.1),
        change: Some(change),
        written: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct UpdateWork {
    lookup: WorkLookup,
    change: Option<WorkChange>,
    /// The changed row once it is sent.
    written: Option<Box<WorkRow>>,
}

impl Operation for UpdateWork {
    type Output = Option<WorkRow>;
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(self.lookup.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        if let Some(row) = self.written.take() {
            done(answer)?;
            return Ok(Next::Done(Some(*row)));
        }
        let (key, mut row) = match self.lookup.take(answer)? {
            Found::Ask(ask) => return Ok(Next::Call(ask)),
            Found::Row(None) => return Ok(Next::Done(None)),
            Found::Row(Some(found)) => found,
        };
        let change = self
            .change
            .take()
            .ok_or_else(|| StoreError("changed twice".into()))?;
        change.apply(&mut row);
        let write = vec![row_of(key, &row)?];
        self.written = Some(Box::new(row));
        Ok(Next::Call(IoCall::Write(write)))
    }
}

/// Moves the send boundary of work `work_id` before an attempt sends, in one transaction, unless
/// the boundary earlier attempts left decides already.
pub fn begin_send(ids: (Ulid, Ulid), again: bool) -> BeginSend {
    BeginSend {
        lookup: WorkLookup::new(ids.0, ids.1),
        again,
        written: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct BeginSend {
    lookup: WorkLookup,
    again: bool,
    written: bool,
}

impl Operation for BeginSend {
    type Output = Option<Send>;
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(self.lookup.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        if self.written {
            done(answer)?;
            return Ok(Next::Done(Some(Send::Now)));
        }
        let (key, mut row) = match self.lookup.take(answer)? {
            Found::Ask(ask) => return Ok(Next::Call(ask)),
            Found::Row(None) => return Ok(Next::Done(None)),
            Found::Row(Some(found)) => found,
        };
        if let Some(decided) = decided(row.boundary.as_ref(), self.again) {
            return Ok(Next::Done(Some(decided)));
        }
        let attempt = match &row.boundary {
            Some(SendBoundary::Sending { attempt }) => attempt + 1,
            _ => 1,
        };
        row.boundary = Some(SendBoundary::Sending { attempt });
        self.written = true;
        Ok(Next::Call(IoCall::Write(vec![row_of(key, &row)?])))
    }
}

/// What a boundary decides without a new request: a kept answer, unless it asks a request that may
/// go twice (`again`) to try later, and a begun request that may not go twice. `None` sends.
pub fn decided(boundary: Option<&SendBoundary>, again: bool) -> Option<Send> {
    match boundary {
        Some(SendBoundary::Sent { status, .. }) if again && retried(*status) => None,
        Some(SendBoundary::Sent {
            status,
            reply,
            bytes,
            version,
        }) => Some(Send::Answered {
            status: *status,
            reply: reply.clone(),
            bytes: *bytes,
            version: *version,
        }),
        Some(SendBoundary::Sending { .. }) if !again => Some(Send::Undecided),
        _ => None,
    }
}

/// Whether an answer asks to try again later: a timeout, a throttle or a server error.
pub fn retried(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..=599)
}

/// The summary of a finished job, or `None` while it may still run. A job record that is gone
/// leaves the outcome unknown.
pub(crate) fn summary_of(record: Option<&JobRecord>) -> Option<Summary> {
    let Some(record) = record else {
        let mut gone = Summary::of(FinalState::Unknown);
        gone.message = Some("the job record is gone".into());
        return Some(gone);
    };
    let state = match record.state {
        JobState::Succeeded => FinalState::Succeeded,
        JobState::Failed => FinalState::Failed,
        JobState::Cancelled => FinalState::Cancelled,
        JobState::Indeterminate if record.locally_exhausted => FinalState::Unknown,
        _ => return None,
    };
    let mut summary = Summary::of(state);
    summary.message = (record.last_error.as_ref()).map(|error| error.message.clone());
    if let Some(JobResultPayload::PluginWork { bytes, version_id }) = &record.result {
        summary.bytes = Some(*bytes);
        summary.version_id = *version_id;
    }
    Some(summary)
}

/// Reads every work row.
pub(crate) fn work_scan() -> Scan {
    Scan::new(PLUGIN_WORK_KEYSPACE, vec![WORK_ROW])
}

/// Deletes the rows nobody can ask for again: settled and delivered, no cancel open, kept past
/// the run retention, and their run record pruned.
pub fn prune_work(now_ms: u64) -> PruneWork {
    PruneWork {
        now_ms,
        phase: PrunePhase::Scanning(work_scan()),
    }
}

#[derive(Debug, PartialEq)]
pub struct PruneWork {
    now_ms: u64,
    phase: PrunePhase,
}

#[derive(Debug, PartialEq)]
enum PrunePhase {
    Scanning(Scan),
    /// Reads the run of each aged work; holds the index and row keys of each, in that order.
    Runs(Vec<[Vec<u8>; 2]>),
    Deleting(usize),
}

impl Operation for PruneWork {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        match &self.phase {
            PrunePhase::Scanning(scan) => Ok(Next::Call(scan.ask())),
            _ => Err(StoreError("prune started twice".into())),
        }
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, StoreError> {
        match std::mem::replace(&mut self.phase, PrunePhase::Deleting(0)) {
            PrunePhase::Scanning(mut scan) => {
                let Some(rows) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = PrunePhase::Scanning(scan);
                    return Ok(Next::Call(ask));
                };
                let (aged, runs) = aged_work(rows, self.now_ms)?;
                self.phase = PrunePhase::Runs(aged);
                Ok(Next::Call(read_call(runs)))
            }
            PrunePhase::Runs(aged) => {
                let runs = all_values(answer)?;
                let dropped: Vec<_> = (aged.into_iter().zip(runs))
                    .filter(|(_, run)| run.is_none())
                    .flat_map(|(keys, _)| keys.map(|key| (PLUGIN_WORK_KEYSPACE, key)))
                    .collect();
                self.phase = PrunePhase::Deleting(dropped.len() / 2);
                Ok(Next::Call(delete_call(dropped)))
            }
            PrunePhase::Deleting(count) => {
                done(answer)?;
                Ok(Next::Done(count))
            }
        }
    }
}

/// The index and row keys of the works among `rows` nobody can ask for again once their run is
/// gone, and the run key of each, in the same order.
fn aged_work(rows: Rows, now_ms: u64) -> Result<WorkRuns, StoreError> {
    let (mut aged, mut runs) = (Vec::new(), Vec::new());
    for (key, value) in rows {
        let row: WorkRow = decode(&value)?;
        let (Some(install_id), Some(Identity::Call { run_id, .. })) =
            (install_at(&key), identity_at(&key))
        else {
            continue;
        };
        let kept = (row.settled_ms)
            .is_some_and(|settled| settled.saturating_add(RUN_RETENTION_MS) <= now_ms);
        if row.state != WorkState::Settled || !row.notified || row.cancel || !kept {
            continue;
        }
        runs.push((PLUGIN_RUNS_KEYSPACE, run_key(install_id, run_id)));
        aged.push([index_key(install_id, row.work_id), key]);
    }
    Ok((aged, runs))
}

type WorkRuns = (Vec<[Vec<u8>; 2]>, Vec<(&'static str, Vec<u8>)>);

/// At most `limit` works of an install after the work `after`, oldest first, and the id of the
/// last one when more may follow.
pub fn list_work(install_id: Ulid, after: Option<Ulid>, limit: usize) -> ListWork {
    ListWork {
        page: Some(page_call(
            PLUGIN_WORK_KEYSPACE,
            [&[WORK_INDEX][..], &install_id.to_bytes()].concat(),
            after.map(|work_id| index_key(install_id, work_id)),
            limit,
        )),
        indexes: Vec::new(),
        next: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct ListWork {
    page: Option<IoCall>,
    /// The index rows read, whose rows are read next.
    indexes: Vec<Vec<u8>>,
    next: Option<Ulid>,
}

impl Operation for ListWork {
    type Output = (Vec<WorkRow>, Option<Ulid>);
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        let page = self
            .page
            .take()
            .ok_or_else(|| StoreError("listed twice".into()))?;
        Ok(Next::Call(page))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        match answer {
            IoResult::Page { .. } => {
                let (rows, next) = page_of(answer)?;
                // The cursor is the last index row read, whatever row it leads to.
                let last = (rows.last()).and_then(|(index, _)| work_at(index));
                self.next = next.and(last);
                let reads = (rows.iter())
                    .map(|(_, key)| (PLUGIN_WORK_KEYSPACE, key.clone()))
                    .collect();
                self.indexes = rows.into_iter().map(|(index, _)| index).collect();
                Ok(Next::Call(read_call(reads)))
            }
            IoResult::Values(_) => {
                let values = all_values(answer)?;
                let mut works = Vec::with_capacity(values.len());
                for (index, value) in self.indexes.iter().zip(values) {
                    let Some(value) = value else {
                        continue;
                    };
                    let row = decode::<WorkRow>(&value)?;
                    if index.ends_with(&row.work_id.to_bytes()) {
                        works.push(row);
                    }
                }
                Ok(Next::Done((works, self.next)))
            }
            other => Err(unexpected(other)),
        }
    }
}

/// The state a work shows: from its summary once settled, else from its job record.
pub fn work_state(row: &WorkRow, record: Option<&JobRecord>) -> &'static str {
    if let Some(summary) = &row.summary {
        return match summary.state {
            FinalState::Succeeded => "succeeded",
            FinalState::Failed => "failed",
            FinalState::Cancelled => "cancelled",
            FinalState::Unknown => "unknown",
        };
    }
    let Some(record) = record else {
        return "queued";
    };
    match record.state {
        JobState::Queued | JobState::AwaitingKey => match record.last_error {
            Some(_) => "error",
            None => "queued",
        },
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
        JobState::NotRun => "unknown",
        JobState::Indeterminate if record.locally_exhausted => "unknown",
        JobState::Indeterminate => "error",
        JobState::Claimed
        | JobState::Preparing
        | JobState::Ready
        | JobState::Running
        | JobState::Cancelling => "running",
    }
}

/// The job dedup key of a work: its id, so a repeated submission finds the same job.
pub fn dedup_key(work_id: Ulid) -> Vec<u8> {
    [&b"plugin-work/"[..], &work_id.to_bytes()].concat()
}

pub(crate) fn install_at(key: &[u8]) -> Option<Ulid> {
    Some(Ulid::from_bytes(key.get(1..17)?.try_into().ok()?))
}

/// The work id of an index row.
fn work_at(index: &[u8]) -> Option<Ulid> {
    Some(Ulid::from_bytes(index.get(17..33)?.try_into().ok()?))
}

fn identity_at(key: &[u8]) -> Option<Identity> {
    decode(key.get(17..)?).ok()
}

#[cfg(test)]
#[path = "tests.rs"]
pub(crate) mod tests;
