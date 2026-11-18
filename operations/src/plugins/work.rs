//! Native work of plugin installs on their receiver: one row per install and identity keeps the
//! work's generated ids, binding and state, so a repeated call never starts the work twice.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{PLUGIN_RUNS_KEYSPACE, PLUGIN_WORK_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{RUN_RETENTION_MS, run_key};
use aruna_core::structs::execution::job::{
    JobId, JobRecord, JobResultPayload, JobState, OutputObject, PluginWorkSpec,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::marks::{Mark, mark_row};
use super::rows::{
    Row, Rows, Scan, StoreError, all_values, decode, delete_call, done, encode, page_call, page_of,
    read_call, row, unexpected, values,
};

/// The row of a work under its key.
pub(crate) fn row_of(key: Vec<u8>, work: &WorkRow) -> Result<Row, StoreError> {
    Ok(row(PLUGIN_WORK_KEYSPACE, key, encode(work)?))
}

/// Row kinds of the work keyspace: the work by install and identity, and its identity by work id.
pub const WORK_ROW: u8 = b'w';
pub const WORK_INDEX: u8 = b'i';

/// What names one work across calls: the run and the ordinal of the call that started it, so a
/// rerun of that run gets the same work back.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Identity {
    Call { run_id: Ulid, ordinal: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkKind {
    Copy,
    Download,
    Upload,
    Recipe,
}

/// What the work was approved under; a later manifest digest stops it before it runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub digest: String,
    pub revision: u64,
    pub recipe: Option<String>,
}

/// A transfer's request as resolved when the work started, without any secret, so the work
/// still runs after its run was pruned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContext {
    pub connection: String,
    /// The connection's base URL resolved from the run's saved settings.
    pub base: String,
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
}

/// `Reserved` until its job is known, then `Dispatched`, and `Settled` once the summary is kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkState {
    Reserved,
    Dispatched,
    Settled,
}

/// How far a sending attempt came; an attempt that finds `Sending` never sends again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SendBoundary {
    Sending {
        attempt: u32,
    },
    /// The answer: its status and its reply as exposed when stored, absent when too large, and
    /// the bytes and version sent.
    Sent {
        status: u16,
        reply: Option<String>,
        bytes: u64,
        version: Ulid,
    },
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinalState {
    Succeeded,
    Failed,
    Cancelled,
    /// The outcome cannot be known; nothing is tried again.
    Unknown,
}

/// What a settled work answers after its job record is gone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub state: FinalState,
    /// The object path the work reads: a copy's source or an upload's object.
    pub source: Option<String>,
    /// The object path or prefix the work writes: a copy or download target, a recipe's outputs.
    pub target: Option<String>,
    pub message: Option<String>,
    pub bytes: Option<u64>,
    pub version_id: Option<Ulid>,
    /// The answer status of an upload, and its reply as exposed when it was stored.
    pub status: Option<u16>,
    pub reply: Option<String>,
    pub exit_code: Option<i32>,
    pub outputs: Vec<OutputObject>,
    pub report: Option<String>,
}

/// One work of an install on its receiver.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkRow {
    /// The account of the install that wrote it; a reinstall under the same id has another one.
    pub account: UserId,
    pub work_id: Ulid,
    pub kind: WorkKind,
    /// The object path the work reads: a copy's source or an upload's object.
    pub source: Option<String>,
    /// The object path or prefix the work writes: a copy or download target, a recipe's outputs.
    pub target: Option<String>,
    /// BLAKE3 of the stable arguments, never of generated ids.
    pub digest: [u8; 32],
    pub binding: Binding,
    pub context: Option<RequestContext>,
    /// The run that started it; an audit reference only, the run may be pruned first.
    pub run_id: Ulid,
    /// The version a copy or download writes, minted once.
    pub version_id: Option<Ulid>,
    pub job: Option<JobId>,
    pub state: WorkState,
    pub boundary: Option<SendBoundary>,
    pub summary: Option<Summary>,
    pub notified: bool,
    pub cancel: bool,
    pub created_ms: u64,
    pub settled_ms: Option<u64>,
}

impl WorkRow {
    /// A new row of `install` in state `Reserved`.
    pub fn reserved(install: &PluginInstall, kind: WorkKind, run_id: Ulid, now_ms: u64) -> Self {
        Self {
            account: install.account,
            work_id: Ulid::generate(),
            kind,
            source: None,
            target: None,
            digest: [0; 32],
            binding: Binding {
                digest: install.digest.clone(),
                revision: install.revision,
                recipe: None,
            },
            context: None,
            run_id,
            version_id: None,
            job: None,
            state: WorkState::Reserved,
            boundary: None,
            summary: None,
            notified: false,
            cancel: false,
            created_ms: now_ms,
            settled_ms: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WorkError {
    #[error("the call started other work with other arguments")]
    Conflict,
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

pub fn row_key(install_id: Ulid, identity: &Identity) -> Result<Vec<u8>, StoreError> {
    Ok([&[WORK_ROW][..], &install_id.to_bytes(), &encode(identity)?].concat())
}

pub fn index_key(install_id: Ulid, work_id: Ulid) -> Vec<u8> {
    [
        &[WORK_INDEX][..],
        &install_id.to_bytes(),
        &work_id.to_bytes(),
    ]
    .concat()
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
        phase: ReservePhase::Reading,
    })
}

#[derive(Debug, PartialEq)]
pub struct ReserveWork {
    key: Vec<u8>,
    install_id: Ulid,
    account: UserId,
    proposed: Option<Box<WorkRow>>,
    mark: Option<Row>,
    phase: ReservePhase,
}

#[derive(Debug, PartialEq)]
enum ReservePhase {
    Reading,
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
                // A former install's index goes too, so no index row leads to the replacement.
                let Some(former) = stored else {
                    return Ok(Next::Call(IoCall::Write(rows)));
                };
                self.phase = ReservePhase::Deleting(rows);
                let index = index_key(self.install_id, former.work_id);
                Ok(Next::Call(delete_call(vec![(PLUGIN_WORK_KEYSPACE, index)])))
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

/// A sub-plan that reads the row of work `work_id` of an install and its key, through the index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkLookup {
    install_id: Ulid,
    work_id: Ulid,
    /// The row key the index named, once read.
    key: Option<Vec<u8>>,
}

/// What a lookup needs next, or the row it found.
#[derive(Debug, PartialEq)]
pub enum Found {
    Ask(IoCall),
    Row(Option<(Vec<u8>, WorkRow)>),
}

impl WorkLookup {
    pub fn new(install_id: Ulid, work_id: Ulid) -> Self {
        Self {
            install_id,
            work_id,
            key: None,
        }
    }

    pub fn ask(&self) -> IoCall {
        let index = index_key(self.install_id, self.work_id);
        read_call(vec![(PLUGIN_WORK_KEYSPACE, index)])
    }

    pub fn take(&mut self, answer: IoResult) -> Result<Found, StoreError> {
        let [value] = values::<1>(answer)?;
        let Some(value) = value else {
            return Ok(Found::Row(None));
        };
        let Some(key) = self.key.take() else {
            self.key = Some(value.clone());
            return Ok(Found::Ask(read_call(vec![(PLUGIN_WORK_KEYSPACE, value)])));
        };
        let row: WorkRow = decode(&value)?;
        Ok(Found::Row(
            (row.work_id == self.work_id).then_some((key, row)),
        ))
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
pub fn summary_of(record: Option<&JobRecord>) -> Option<Summary> {
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

impl Summary {
    fn of(state: FinalState) -> Self {
        Self {
            state,
            source: None,
            target: None,
            message: None,
            bytes: None,
            version_id: None,
            status: None,
            reply: None,
            exit_code: None,
            outputs: Vec::new(),
            report: None,
        }
    }
}

/// Reads every work row.
pub fn work_scan() -> Scan {
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

pub fn install_at(key: &[u8]) -> Option<Ulid> {
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
#[path = "work_tests.rs"]
pub(crate) mod tests;
