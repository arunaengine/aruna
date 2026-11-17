//! Delivers the end of each plugin job as a `job_finished` run, only after the run that started
//! the job succeeded, so the mapping that run wrote exists; the others are held.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::{PLUGIN_RUNS_KEYSPACE, PLUGIN_WORK_KEYSPACE};
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::{PluginRun, RunState, SourceEvent, SourceKind, run_key};
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use tracing::warn;
use ulid::Ulid;

use super::dispatch::event_run;
use super::intents::{intent_scan, undone_keys};
use super::rows::{
    Answer, Ask, Plan, Row, RowsOperation, Scan, Turn, decode, done, encode, row, scan_rows, values,
};
use super::runs::{ADMISSIONS, Admission, RunError, audit_rows, read_run};
use super::work::{Found, WorkKind, WorkLookup, WorkRow, WorkState, work_scan, work_state};
use crate::driver::{DriverContext, drive};

/// Admits a `job_finished` run for every settled work whose starting run succeeded and marks the
/// work notified in the same transaction; a manifest without the trigger is notified without a
/// run. Answers how many works were notified.
pub(crate) async fn notify_work(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
) -> Result<usize, RunError> {
    let rows = drive(scan_rows(work_scan()), ctx).await?;
    let mut notified = 0;
    for (key, value) in rows {
        let row: WorkRow = decode(&value)?;
        if row.state != WorkState::Settled || row.notified {
            continue;
        }
        let Some(install) = (config.installs.iter()).find(|install| {
            key.get(1..17) == Some(&install.install_id.to_bytes()[..])
                && install.account == row.account
                && install.receiver == local
                && install.state != InstallState::Removing
        }) else {
            continue;
        };
        let run = match drive(read_run(install.install_id, row.run_id), ctx).await {
            Ok(run) => Some(run),
            // Runs stay while a completion waits; one that is gone anyway cannot be retried.
            Err(RunError::NotFound) => None,
            Err(error) => return Err(error),
        };
        if run.is_some_and(|run| run.state != RunState::Succeeded) {
            continue;
        }
        let event = finished_event(install, &row, now_ms);
        let finished = event_run(config, install, &event, now_ms);
        let _admitting = ADMISSIONS.lock().await;
        let notify = NotifyWork::new(install.install_id, row.work_id, finished);
        match drive(RowsOperation::transaction(notify), ctx).await {
            Ok(true) => notified += 1,
            Ok(false) => {}
            // A full queue takes the completion at a later pass.
            Err(RunError::Full(_)) => {}
            Err(error) => {
                warn!(%error, work = %row.work_id, "Could not deliver a finished plugin job")
            }
        }
    }
    Ok(notified)
}

/// Marks a settled work notified, with the `job_finished` run it starts admitted in the same
/// transaction; `false` when the work is gone or notified already. The caller holds `ADMISSIONS`.
#[derive(Debug, PartialEq)]
pub struct NotifyWork {
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
            lookup: WorkLookup::new(install_id, work_id),
            admission: run.map(Admission::new),
            phase: NotifyPhase::Looking,
        }
    }
}

impl Plan for NotifyWork {
    type Output = bool;
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<bool>, RunError> {
        Ok(Turn::Ask(self.lookup.ask()))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<bool>, RunError> {
        match std::mem::replace(&mut self.phase, NotifyPhase::Writing) {
            NotifyPhase::Looking => {
                let (key, mut stored) = match self.lookup.take(answer)? {
                    Found::Ask(ask) => {
                        self.phase = NotifyPhase::Looking;
                        return Ok(Turn::Ask(ask));
                    }
                    Found::Row(None) => return Ok(Turn::Finish(false)),
                    Found::Row(Some(found)) => found,
                };
                if stored.notified {
                    return Ok(Turn::Finish(false));
                }
                stored.notified = true;
                let work = row(PLUGIN_WORK_KEYSPACE, key, encode(&stored)?);
                match &self.admission {
                    Some(admission) => {
                        let ask = admission.ask();
                        self.phase = NotifyPhase::Admitting(work);
                        Ok(Turn::Ask(ask))
                    }
                    None => Ok(Turn::Ask(Ask::Write(vec![work]))),
                }
            }
            NotifyPhase::Admitting(work) => {
                let admission = self.admission.as_mut().ok_or(RunError::NotHeld)?;
                let Some(mut rows) = admission.take(answer)? else {
                    let ask = admission.ask();
                    self.phase = NotifyPhase::Admitting(work);
                    return Ok(Turn::Ask(ask));
                };
                rows.push(work);
                Ok(Turn::Ask(Ask::Write(rows)))
            }
            NotifyPhase::Writing => {
                done(answer)?;
                Ok(Turn::Finish(true))
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
/// commits with it. The caller holds `ADMISSIONS`.
pub fn retry_run(
    (install_id, run_id): (Ulid, Ulid),
    audit: &AdminAuditRecord,
    now_ms: u64,
) -> Result<RowsOperation<RetryRun>, RunError> {
    Ok(RowsOperation::transaction(RetryRun {
        key: run_key(install_id, run_id),
        now_ms,
        audit: audit_rows(audit)?,
        phase: RetryPhase::Reading,
    }))
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

impl Plan for RetryRun {
    type Output = PluginRun;
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<PluginRun>, RunError> {
        Ok(Turn::Ask(Ask::Read(vec![(
            PLUGIN_RUNS_KEYSPACE,
            self.key.clone(),
        )])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<PluginRun>, RunError> {
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
                Ok(Turn::Ask(ask))
            }
            RetryPhase::Admitting(run, mut admission) => {
                let Some(rows) = admission.take(answer)? else {
                    let ask = admission.ask();
                    self.phase = RetryPhase::Admitting(run, admission);
                    return Ok(Turn::Ask(ask));
                };
                self.audit.splice(0..0, rows);
                let scan = intent_scan(run.run_id);
                let ask = scan.ask();
                self.phase = RetryPhase::Intents(run, scan);
                Ok(Turn::Ask(ask))
            }
            RetryPhase::Intents(run, mut scan) => {
                let Some(intents) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = RetryPhase::Intents(run, scan);
                    return Ok(Turn::Ask(ask));
                };
                self.phase = RetryPhase::Deleting(run);
                Ok(Turn::Ask(Ask::Delete(undone_keys(intents)?)))
            }
            RetryPhase::Deleting(run) => {
                done(answer)?;
                self.phase = RetryPhase::Writing(run);
                Ok(Turn::Ask(Ask::Write(std::mem::take(&mut self.audit))))
            }
            RetryPhase::Writing(run) => {
                done(answer)?;
                Ok(Turn::Finish(*run))
            }
        }
    }
}

/// Marks the held end of job `work_id` delivered without a run; the audit row commits with it.
pub fn drop_completion(
    (install_id, work_id): (Ulid, Ulid),
    audit: &AdminAuditRecord,
) -> Result<RowsOperation<DropCompletion>, RunError> {
    Ok(RowsOperation::transaction(DropCompletion {
        lookup: WorkLookup::new(install_id, work_id),
        audit: audit_rows(audit)?,
        dropped: None,
    }))
}

#[derive(Debug, PartialEq)]
pub struct DropCompletion {
    lookup: WorkLookup,
    audit: Vec<Row>,
    dropped: Option<Box<WorkRow>>,
}

impl Plan for DropCompletion {
    type Output = WorkRow;
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<WorkRow>, RunError> {
        Ok(Turn::Ask(self.lookup.ask()))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<WorkRow>, RunError> {
        if let Some(row) = self.dropped.take() {
            done(answer)?;
            return Ok(Turn::Finish(*row));
        }
        let (key, mut stored) = match self.lookup.take(answer)? {
            Found::Ask(ask) => return Ok(Turn::Ask(ask)),
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
        Ok(Turn::Ask(Ask::Write(rows)))
    }
}

/// The event a `job_finished` run gets: the job's id, the object path it wrote or read, and
/// how it ended.
fn finished_event(install: &PluginInstall, row: &WorkRow, now_ms: u64) -> SourceEvent {
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

#[cfg(test)]
#[path = "finished_tests.rs"]
mod tests;
