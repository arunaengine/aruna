//! Delivers the end of each plugin job as a `job_finished` run, only after the run that started
//! the job succeeded, so the mapping that run wrote exists; the others are held.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{PLUGIN_RUNS_KEYSPACE, PLUGIN_WORK_KEYSPACE};
use aruna_core::operation::{HoldScope, Operation, TransactionMode};
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::{PluginRun, RunState, SourceEvent, SourceKind, run_key};
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use tracing::warn;
use ulid::Ulid;

use super::intents::{intent_scan, undone_keys};
use super::rows::{
    Row, Scan, StoreError, decode, delete_call, done, encode, mapped_child, read_call, row, values,
};
use super::runs::{Admission, RunError, audit_rows, event_run, read_run};
use super::work::{Found, WorkKind, WorkLookup, WorkRow, WorkState, work_scan, work_state};

/// Marks a settled work notified, with the `job_finished` run it starts admitted in the same
/// transaction; `false` when the work is gone or notified already.
#[derive(Debug, PartialEq)]
pub struct NotifyWork {
    work_id: Ulid,
    lookup: WorkLookup,
    admission: Option<Admission>,
    phase: NotifyPhase,
}

#[derive(Debug, PartialEq)]
enum NotifyPhase {
    Looking,
    Admitting(Row),
    Writing,
}

impl NotifyWork {
    pub fn new(install_id: Ulid, work_id: Ulid, run: Option<PluginRun>) -> Self {
        Self {
            work_id,
            lookup: WorkLookup::new(install_id, work_id),
            admission: run.map(Admission::new),
            phase: NotifyPhase::Looking,
        }
    }
}

impl NotifyWork {
    /// The work this delivery settles.
    pub fn work_id(&self) -> Ulid {
        self.work_id
    }
}

impl Operation for NotifyWork {
    type Output = bool;
    type Error = RunError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginAdmissions)
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<bool>, RunError> {
        Ok(Next::Call(self.lookup.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<bool>, RunError> {
        match std::mem::replace(&mut self.phase, NotifyPhase::Writing) {
            NotifyPhase::Looking => {
                let (key, mut stored) = match self.lookup.take(answer)? {
                    Found::Ask(ask) => {
                        self.phase = NotifyPhase::Looking;
                        return Ok(Next::Call(ask));
                    }
                    Found::Row(None) => return Ok(Next::Done(false)),
                    Found::Row(Some(found)) => found,
                };
                if stored.notified {
                    return Ok(Next::Done(false));
                }
                stored.notified = true;
                let work = row(PLUGIN_WORK_KEYSPACE, key, encode(&stored)?);
                match &self.admission {
                    Some(admission) => {
                        let ask = admission.ask();
                        self.phase = NotifyPhase::Admitting(work);
                        Ok(Next::Call(ask))
                    }
                    None => Ok(Next::Call(IoCall::Write(vec![work]))),
                }
            }
            NotifyPhase::Admitting(work) => {
                let admission = self.admission.as_mut().ok_or(RunError::NotHeld)?;
                let Some(mut rows) = admission.take(answer)? else {
                    let ask = admission.ask();
                    self.phase = NotifyPhase::Admitting(work);
                    return Ok(Next::Call(ask));
                };
                rows.push(work);
                Ok(Next::Call(IoCall::Write(rows)))
            }
            NotifyPhase::Writing => {
                done(answer)?;
                Ok(Next::Done(true))
            }
        }
    }
}

/// The run a settled, undelivered job waits for: one that ended without success, so an admin
/// retries it or drops the completion. `None` once delivered, or while the run may still succeed.
pub fn waits_for(row: &WorkRow, run: Option<RunState>) -> Option<Ulid> {
    let held = matches!(
        run,
        Some(RunState::Failed | RunState::Cancelled | RunState::NeedsAttention)
    );
    (row.state == WorkState::Settled && !row.notified && held).then_some(row.run_id)
}

/// Queues a failed or cancelled run again with its intents kept, so its calls reach the same
/// ordinals and its jobs the same ids; a call resolved as not done is made again. The audit row
/// commits with it.
pub fn retry_run(
    (install_id, run_id): (Ulid, Ulid),
    audit: &AdminAuditRecord,
    now_ms: u64,
) -> Result<RetryRun, RunError> {
    Ok(RetryRun {
        key: run_key(install_id, run_id),
        now_ms,
        audit: audit_rows(audit)?,
        phase: RetryPhase::Reading,
    })
}

#[derive(Debug, PartialEq)]
pub struct RetryRun {
    key: Vec<u8>,
    now_ms: u64,
    audit: Vec<Row>,
    phase: RetryPhase,
}

#[derive(Debug, PartialEq)]
enum RetryPhase {
    Reading,
    Admitting(Box<PluginRun>, Admission),
    Intents(Box<PluginRun>, Scan),
    Deleting(Box<PluginRun>),
    Writing(Box<PluginRun>),
}

impl Operation for RetryRun {
    type Output = PluginRun;
    type Error = RunError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginAdmissions)
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<PluginRun>, RunError> {
        Ok(Next::Call(read_call(vec![(
            PLUGIN_RUNS_KEYSPACE,
            self.key.clone(),
        )])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<PluginRun>, RunError> {
        match std::mem::replace(&mut self.phase, RetryPhase::Reading) {
            RetryPhase::Reading => {
                let [found] = values::<1>(answer)?;
                let mut run = decode::<PluginRun>(&found.ok_or(RunError::NotFound)?)?;
                if !matches!(run.state, RunState::Failed | RunState::Cancelled) {
                    return Err(RunError::NotRetried(run.state.as_str()));
                }
                (run.state, run.attempts, run.due_ms) = (RunState::Queued, 0, self.now_ms);
                (run.finished_ms, run.message, run.output) = (None, None, None);
                run.log.clear();
                let admission = Admission::new(run.clone());
                let ask = admission.ask();
                self.phase = RetryPhase::Admitting(Box::new(run), admission);
                Ok(Next::Call(ask))
            }
            RetryPhase::Admitting(run, mut admission) => {
                let Some(rows) = admission.take(answer)? else {
                    let ask = admission.ask();
                    self.phase = RetryPhase::Admitting(run, admission);
                    return Ok(Next::Call(ask));
                };
                self.audit.splice(0..0, rows);
                let scan = intent_scan(run.run_id);
                let ask = scan.ask();
                self.phase = RetryPhase::Intents(run, scan);
                Ok(Next::Call(ask))
            }
            RetryPhase::Intents(run, mut scan) => {
                let Some(intents) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = RetryPhase::Intents(run, scan);
                    return Ok(Next::Call(ask));
                };
                self.phase = RetryPhase::Deleting(run);
                Ok(Next::Call(delete_call(undone_keys(intents)?)))
            }
            RetryPhase::Deleting(run) => {
                done(answer)?;
                self.phase = RetryPhase::Writing(run);
                Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.audit))))
            }
            RetryPhase::Writing(run) => {
                done(answer)?;
                Ok(Next::Done(*run))
            }
        }
    }
}

/// Marks the held end of job `work_id` delivered without a run; the audit row commits with it.
pub fn drop_completion(
    (install_id, work_id): (Ulid, Ulid),
    audit: &AdminAuditRecord,
) -> Result<DropCompletion, RunError> {
    Ok(DropCompletion {
        lookup: WorkLookup::new(install_id, work_id),
        audit: audit_rows(audit)?,
        dropped: None,
    })
}

#[derive(Debug, PartialEq)]
pub struct DropCompletion {
    lookup: WorkLookup,
    audit: Vec<Row>,
    dropped: Option<Box<WorkRow>>,
}

impl Operation for DropCompletion {
    type Output = WorkRow;
    type Error = RunError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<WorkRow>, RunError> {
        Ok(Next::Call(self.lookup.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<WorkRow>, RunError> {
        if let Some(row) = self.dropped.take() {
            done(answer)?;
            return Ok(Next::Done(*row));
        }
        let (key, mut stored) = match self.lookup.take(answer)? {
            Found::Ask(ask) => return Ok(Next::Call(ask)),
            Found::Row(None) => return Err(RunError::NotFound),
            Found::Row(Some(found)) => found,
        };
        if stored.state != WorkState::Settled || stored.notified {
            return Err(RunError::NotHeld);
        }
        stored.notified = true;
        let mut rows = vec![row(PLUGIN_WORK_KEYSPACE, key, encode(&stored)?)];
        rows.append(&mut self.audit);
        self.dropped = Some(Box::new(stored));
        Ok(Next::Call(IoCall::Write(rows)))
    }
}

/// The event a `job_finished` run gets: the job's id, the object path it wrote or read, and
/// how it ended.
pub(crate) fn finished_event(install: &PluginInstall, row: &WorkRow, now_ms: u64) -> SourceEvent {
    let path = match row.kind {
        WorkKind::Copy | WorkKind::Download => row.target.clone(),
        WorkKind::Upload => row.source.clone(),
        WorkKind::Recipe => None,
    };
    SourceEvent {
        kind: SourceKind::JobFinished,
        group_id: install.group_id,
        path: path.unwrap_or_default(),
        resource: row.work_id.to_string(),
        revision: work_state(row, None).to_string(),
        source: format!("job_finished/{}", row.work_id),
        anchor_ms: now_ms,
        journaled_ms: now_ms,
        actor: None,
    }
}

/// The completion waiting for one work's run to be read.
#[derive(Debug, PartialEq)]
struct Candidate {
    install: PluginInstall,
    row: WorkRow,
}

/// The `job_finished` deliveries due now: one per settled work whose starting run succeeded or is
/// gone, for the installs this node receives. A manifest without the trigger delivers no run.
#[derive(Debug, PartialEq)]
pub struct DueCompletions {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    now_ms: u64,
    scan: Scan,
    rows: VecDeque<(Vec<u8>, Vec<u8>)>,
    current: Option<Box<Candidate>>,
    due: Vec<NotifyWork>,
}

impl DueCompletions {
    pub fn new(config: Arc<RealmConfigDocument>, local: NodeId, now_ms: u64) -> Self {
        Self {
            config,
            local,
            now_ms,
            scan: work_scan(),
            rows: VecDeque::new(),
            current: None,
            due: Vec::new(),
        }
    }

    fn candidate(&self, key: &[u8], row: WorkRow) -> Option<Candidate> {
        if row.state != WorkState::Settled || row.notified {
            return None;
        }
        let install = (self.config.installs.iter()).find(|install| {
            key.get(1..17) == Some(&install.install_id.to_bytes()[..])
                && install.account == row.account
                && install.receiver == self.local
                && install.state != InstallState::Removing
        })?;
        Some(Candidate {
            install: install.clone(),
            row,
        })
    }

    fn advance(&mut self) -> Result<Next<Vec<NotifyWork>>, StoreError> {
        while let Some((key, value)) = self.rows.pop_front() {
            let row: WorkRow = decode(&value)?;
            let Some(candidate) = self.candidate(&key, row) else {
                continue;
            };
            let read = read_run(candidate.install.install_id, candidate.row.run_id);
            self.current = Some(Box::new(candidate));
            // Runs stay while a completion waits; one that is gone anyway cannot be retried.
            return Ok(Next::Call(mapped_child(read, |run| match run {
                Ok(run) => Ok(Some(run.state)),
                Err(RunError::NotFound) => Ok(None),
                Err(error) => Err(error.to_string()),
            })));
        }
        Ok(Next::Done(std::mem::take(&mut self.due)))
    }
}

impl Operation for DueCompletions {
    type Output = Vec<NotifyWork>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(self.scan.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        let Some(candidate) = self.current.take() else {
            return match self.scan.take(answer)? {
                Some(rows) => {
                    self.rows = rows.into();
                    self.advance()
                }
                None => Ok(Next::Call(self.scan.ask())),
            };
        };
        let state = answer
            .child::<Option<RunState>, String>()?
            .map_err(StoreError)?;
        if state.is_none_or(|state| state == RunState::Succeeded) {
            let Candidate { install, row } = *candidate;
            let event = finished_event(&install, &row, self.now_ms);
            let run = event_run(&self.config, &install, &event, self.now_ms);
            self.due
                .push(NotifyWork::new(install.install_id, row.work_id, run));
        }
        self.advance()
    }
}

/// Admits a `job_finished` run for every settled work whose starting run succeeded and marks the
/// work notified in the same transaction; a manifest without the trigger is notified without a
/// run. Answers how many works were notified.
#[derive(Debug, PartialEq)]
pub struct NotifyDue {
    due: Option<DueCompletions>,
    pending: VecDeque<NotifyWork>,
    current: Option<Ulid>,
    notified: usize,
}

impl NotifyDue {
    pub fn new(config: Arc<RealmConfigDocument>, local: NodeId, now_ms: u64) -> Self {
        Self {
            due: Some(DueCompletions::new(config, local, now_ms)),
            pending: VecDeque::new(),
            current: None,
            notified: 0,
        }
    }

    fn advance(&mut self) -> Next<usize> {
        let Some(notify) = self.pending.pop_front() else {
            return Next::Done(self.notified);
        };
        self.current = Some(notify.work_id());
        Next::Call(IoCall::child(notify))
    }
}

impl Operation for NotifyDue {
    type Output = usize;
    type Error = RunError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginAdmissions)
    }

    fn first(&mut self) -> Result<Next<usize>, RunError> {
        let due = self.due.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(due)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, RunError> {
        let Some(work) = self.current.take() else {
            self.pending = result.child::<Vec<NotifyWork>, StoreError>()??.into();
            return Ok(self.advance());
        };
        match result.child::<bool, RunError>()? {
            Ok(true) => self.notified += 1,
            Ok(false) => {}
            // A full queue takes the completion at a later pass.
            Err(RunError::Full(_)) => {}
            Err(error) => warn!(%error, %work, "Could not deliver a finished plugin job"),
        }
        Ok(self.advance())
    }
}

#[cfg(test)]
#[path = "finished_tests.rs"]
mod tests;
