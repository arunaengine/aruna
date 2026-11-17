//! Run records on an install's receiver: admission within the install's bounds, reads, pages,
//! and saving a run, which drops it from the queue once it ended.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::{PLUGIN_QUEUE_KEYSPACE, PLUGIN_RUNS_KEYSPACE};
use aruna_core::plugin_runs::{
    ATTENTION_ROW, PluginRun, RUN_RETENTION_MS, RUN_ROW, RUNS_QUEUED, RunState, WAITING_ROW,
    parse_queue_key, queue_key, run_key, run_prefix,
};
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use tokio::sync::Mutex;
use ulid::Ulid;

use super::intents::{first_open, intent_keys, intent_scan, resolved_rows};
use super::rows::{
    Answer, Ask, Plan, Row, Rows, RowsOperation, Scan, StoreError, Turn, Writes, decode, done,
    encode, row, unexpected, values, writes,
};
use super::work::{WorkRow, work_scan};

/// Serializes admissions on this node, so no install passes its queue bound.
pub static ADMISSIONS: Mutex<()> = Mutex::const_new(());

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

/// A sub-plan that admits a run, a new one or one queued again, within its install's bound.
/// The caller holds `ADMISSIONS` while the transaction runs.
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

    pub fn ask(&self) -> Ask {
        self.queued.ask()
    }

    /// Takes a page of the install's queue; returns the run's rows once the queue is read.
    pub fn take(&mut self, answer: Answer) -> Result<Option<Vec<Row>>, RunError> {
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

/// Stores a new queued run, unless its install already has `RUNS_QUEUED` unfinished ones. The
/// caller holds `ADMISSIONS`.
pub fn admit(run: PluginRun) -> RowsOperation<Admit> {
    RowsOperation::transaction(Admit {
        admission: Admission::new(run),
        written: false,
    })
}

#[derive(Debug, PartialEq)]
pub struct Admit {
    admission: Admission,
    written: bool,
}

impl Plan for Admit {
    type Output = ();
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<()>, RunError> {
        Ok(Turn::Ask(self.admission.ask()))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<()>, RunError> {
        if self.written {
            return Ok(done(answer).map(|()| Turn::Finish(()))?);
        }
        Ok(match self.admission.take(answer)? {
            None => Turn::Ask(self.admission.ask()),
            Some(rows) => {
                self.written = true;
                Turn::Ask(Ask::Write(rows))
            }
        })
    }
}

pub fn read_run(install_id: Ulid, run_id: Ulid) -> RowsOperation<ReadRun> {
    RowsOperation::direct(ReadRun(run_key(install_id, run_id)))
}

#[derive(Debug, PartialEq)]
pub struct ReadRun(Vec<u8>);

impl Plan for ReadRun {
    type Output = PluginRun;
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<PluginRun>, RunError> {
        let read = vec![(PLUGIN_RUNS_KEYSPACE, self.0.clone())];
        Ok(Turn::Ask(Ask::Read(read)))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<PluginRun>, RunError> {
        let [found] = values::<1>(answer)?;
        Ok(Turn::Finish(decode(&found.ok_or(RunError::NotFound)?)?))
    }
}

/// At most `limit` runs of an install after the run `after`, oldest first, and the id of the
/// last one when more may follow.
pub fn list_runs(install_id: Ulid, after: Option<Ulid>, limit: usize) -> RowsOperation<ListRuns> {
    RowsOperation::direct(ListRuns(Ask::Page {
        space: PLUGIN_RUNS_KEYSPACE,
        prefix: run_prefix(install_id),
        after: after.map(|run_id| run_key(install_id, run_id)),
        limit,
    }))
}

#[derive(Debug, PartialEq)]
pub struct ListRuns(Ask);

impl Plan for ListRuns {
    type Output = (Vec<PluginRun>, Option<Ulid>);
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<Self::Output>, RunError> {
        Ok(Turn::Ask(self.0.clone()))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, RunError> {
        let Answer::Page(rows, next) = answer else {
            return Err(unexpected(answer).into());
        };
        let runs = (rows.iter())
            .map(|(_, value)| decode::<PluginRun>(value))
            .collect::<Result<Vec<_>, _>>()?;
        let next = next.and(runs.last().map(|run| run.run_id));
        Ok(Turn::Finish((runs, next)))
    }
}

/// The install and run ids of every unfinished run on this node; unresolved runs wait for a
/// person and are left out.
pub fn unfinished() -> RowsOperation<Unfinished> {
    RowsOperation::direct(Unfinished(Scan::new(PLUGIN_QUEUE_KEYSPACE, Vec::new())))
}

#[derive(Debug, PartialEq)]
pub struct Unfinished(Scan);

impl Plan for Unfinished {
    type Output = Vec<(Ulid, Ulid)>;
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<Self::Output>, RunError> {
        Ok(Turn::Ask(self.0.ask()))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, RunError> {
        let Some(rows) = self.0.take(answer)? else {
            return Ok(Turn::Ask(self.0.ask()));
        };
        Ok(Turn::Finish(
            (rows.iter())
                .filter(|(_, value)| ![ATTENTION_ROW, WAITING_ROW].contains(&value.as_slice()))
                .filter_map(|(key, _)| parse_queue_key(key))
                .collect(),
        ))
    }
}

/// The rows that save a run: its record, and its queue row marked for attention or, once it
/// ended, removed.
pub fn saved_rows(run: &PluginRun) -> Result<(Vec<Row>, Vec<(&'static str, Vec<u8>)>), RunError> {
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
pub fn save_run(run: &PluginRun) -> Result<RowsOperation<Writes>, RunError> {
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
) -> Result<RowsOperation<ResolveRun>, RunError> {
    let outcome = match done {
        true => "done",
        false => "not done",
    };
    let entries = audit_rows(audit)?;
    Ok(RowsOperation::transaction(ResolveRun {
        key: run_key(install_id, run_id),
        done,
        message: format!("{} resolved it as {outcome}", audit.actor),
        now_ms,
        audit: entries,
        phase: ResolvePhase::Reading,
    }))
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

impl Plan for ResolveRun {
    type Output = PluginRun;
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<PluginRun>, RunError> {
        let read = vec![(PLUGIN_RUNS_KEYSPACE, self.key.clone())];
        Ok(Turn::Ask(Ask::Read(read)))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<PluginRun>, RunError> {
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
                Ok(Turn::Ask(ask))
            }
            ResolvePhase::Intents(run, mut scan) => {
                let Some(intents) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = ResolvePhase::Intents(run, scan);
                    return Ok(Turn::Ask(ask));
                };
                let (mut rows, deletes) = saved_rows(&run)?;
                rows.extend(resolved_rows(intents, self.done)?);
                rows.append(&mut self.audit);
                self.phase = ResolvePhase::Writing(run, deletes);
                Ok(Turn::Ask(Ask::Write(rows)))
            }
            ResolvePhase::Writing(run, deletes) => {
                done(answer)?;
                self.phase = ResolvePhase::Deleting(run);
                Ok(Turn::Ask(Ask::Delete(deletes)))
            }
            ResolvePhase::Deleting(run) => {
                done(answer)?;
                Ok(Turn::Finish(*run))
            }
        }
    }
}

/// Removes runs that ended longer than `RUN_RETENTION_MS` ago, each with its intents in one batch.
/// A run waiting for a person, for its job's end, or whose calls may have had effects or cannot
/// be read stays.
pub fn prune_runs(now_ms: u64) -> RowsOperation<PruneRuns> {
    RowsOperation::direct(PruneRuns {
        now_ms,
        phase: PrunePhase::Runs(Scan::new(PLUGIN_RUNS_KEYSPACE, vec![RUN_ROW])),
        pruned: 0,
    })
}

#[derive(Debug, PartialEq)]
pub struct PruneRuns {
    now_ms: u64,
    phase: PrunePhase,
    pruned: usize,
}

#[derive(Debug, PartialEq)]
enum PrunePhase {
    Runs(Scan),
    Held(Vec<PluginRun>, Scan),
    /// Reads the intents of the last expired run; the others follow.
    Intents(Vec<PluginRun>, Scan),
    /// Deletes the last run and its intents.
    Deleting(Vec<PluginRun>),
}

impl PruneRuns {
    /// The intents of the next expired run, or the outcome when none is left.
    fn next_run(&mut self, runs: Vec<PluginRun>) -> Turn<usize> {
        match runs.last() {
            None => Turn::Finish(self.pruned),
            Some(run) => {
                let scan = intent_scan(run.run_id);
                let ask = scan.ask();
                self.phase = PrunePhase::Intents(runs, scan);
                Turn::Ask(ask)
            }
        }
    }
}

impl Plan for PruneRuns {
    type Output = usize;
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<usize>, RunError> {
        match &self.phase {
            PrunePhase::Runs(scan) => Ok(Turn::Ask(scan.ask())),
            _ => Err(StoreError("prune started twice".into()).into()),
        }
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<usize>, RunError> {
        let phase = std::mem::replace(&mut self.phase, PrunePhase::Deleting(Vec::new()));
        match phase {
            PrunePhase::Runs(mut scan) => {
                let Some(rows) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = PrunePhase::Runs(scan);
                    return Ok(Turn::Ask(ask));
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
                self.phase = PrunePhase::Held(expired, scan);
                Ok(Turn::Ask(ask))
            }
            PrunePhase::Held(mut expired, mut scan) => {
                let Some(rows) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = PrunePhase::Held(expired, scan);
                    return Ok(Turn::Ask(ask));
                };
                let held = held_runs(rows)?;
                expired.retain(|run| !held.contains(&run.run_id));
                expired.reverse();
                Ok(self.next_run(expired))
            }
            PrunePhase::Intents(mut runs, mut scan) => {
                let intents = match scan.take(answer) {
                    Ok(None) => {
                        let ask = scan.ask();
                        self.phase = PrunePhase::Intents(runs, scan);
                        return Ok(Turn::Ask(ask));
                    }
                    Ok(Some(intents)) => intents,
                    Err(_) => {
                        runs.pop();
                        return Ok(self.next_run(runs));
                    }
                };
                let run = runs.last().ok_or(RunError::NotFound)?;
                if !matches!(first_open(&intents), Ok(None)) {
                    runs.pop();
                    return Ok(self.next_run(runs));
                }
                let mut keys = vec![(PLUGIN_RUNS_KEYSPACE, run_key(run.install_id, run.run_id))];
                keys.extend(intent_keys(intents));
                self.phase = PrunePhase::Deleting(runs);
                Ok(Turn::Ask(Ask::Delete(keys)))
            }
            PrunePhase::Deleting(mut runs) => {
                done(answer)?;
                runs.pop();
                self.pruned += 1;
                Ok(self.next_run(runs))
            }
        }
    }
}

/// The runs a completion still waits on among the work `rows`: those of works not notified yet,
/// which keep their record until the completion is delivered or dropped.
pub fn held_runs(rows: Rows) -> Result<Vec<Ulid>, StoreError> {
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

#[cfg(test)]
#[path = "runs_tests.rs"]
pub(crate) mod tests;
