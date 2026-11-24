//! Workflow runs on one node: admission, resume and repeat, the coordinator session, child tasks
//! and their cache, data calls, the run log, events, cancellation, reports and the run crate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod admit;
pub mod cache;
pub mod cancel;
pub mod control;
pub mod data;
pub mod events;
pub mod log;
pub mod provenance;
pub mod reads;
pub mod recover;
pub mod reports;
pub mod run;
pub mod tasks;

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "cache_tests.rs"]
mod cache_tests;

#[cfg(test)]
#[path = "report_tests.rs"]
mod report_tests;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{WORKFLOW_RUN_KEYSPACE, WORKFLOW_TASK_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::workflow_run::{
    RunFile, RunOrigin, RunState, StepRecord, WorkflowRunRecord, WorkflowTaskRecord, run_key,
    task_key,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use ulid::Ulid;

use super::{Access, CatalogError, GroupAccessOperation, Need, access_answer};

/// Rows of one storage page of a run scan.
const SCAN_ROWS: usize = 256;

type Writes = Vec<(KeySpace, Key, Value)>;

/// Reads a run and decides the caller's access through its group: a run the caller cannot read is
/// not found, one it reads but may not change is forbidden for a write.
#[derive(Debug, PartialEq)]
pub struct RunAccessOperation {
    auth: AuthContext,
    run_id: Ulid,
    need: Need,
    record: Option<WorkflowRunRecord>,
}

impl RunAccessOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid, need: Need) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            need,
            record: None,
        }
    }
}

impl Operation for RunAccessOperation {
    type Output = (WorkflowRunRecord, Access);
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Self::Output>, CatalogError> {
        Ok(Next::Call(read_run(self.run_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CatalogError> {
        let Some(record) = self.record.take() else {
            let record = run_answer(result)?.ok_or(CatalogError::NotFound)?;
            let check = GroupAccessOperation::new(&self.auth, [record.group_id]);
            self.record = Some(record);
            return Ok(Next::Call(IoCall::child(check)));
        };
        let access = (access_answer(result)?.get(&record.group_id).copied()).unwrap_or_default();
        if !access.read {
            return Err(CatalogError::NotFound);
        }
        if self.need == Need::Write && !access.write {
            return Err(CatalogError::Forbidden);
        }
        Ok(Next::Done((record, access)))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound | CatalogError::Forbidden)
    }
}

/// The answer of a [`RunAccessOperation`] child.
pub(crate) fn access_of(result: IoResult) -> Result<(WorkflowRunRecord, Access), CatalogError> {
    result.child::<(WorkflowRunRecord, Access), CatalogError>()?
}

pub(crate) fn read_run(run_id: Ulid) -> IoCall {
    IoCall::Read(vec![(WORKFLOW_RUN_KEYSPACE.to_string(), run_key(run_id))])
}

pub(crate) fn run_answer(result: IoResult) -> Result<Option<WorkflowRunRecord>, CatalogError> {
    let value = result.value()?;
    Ok(value
        .map(|value| WorkflowRunRecord::from_bytes(&value))
        .transpose()?)
}

pub(crate) fn run_row(record: &WorkflowRunRecord) -> Result<(KeySpace, Key, Value), CatalogError> {
    Ok((
        WORKFLOW_RUN_KEYSPACE.to_string(),
        run_key(record.run_id),
        ByteView::from(record.to_bytes()?),
    ))
}

pub(crate) fn task_row(task: &WorkflowTaskRecord) -> Result<(KeySpace, Key, Value), CatalogError> {
    Ok((
        WORKFLOW_TASK_KEYSPACE.to_string(),
        task_key(task.run_id, task.task_id),
        ByteView::from(task.to_bytes()?),
    ))
}

/// One change of a run record and its tasks; each emits the event rows the portal follows.
#[derive(Clone, Debug, PartialEq)]
pub enum RunChange {
    /// The run's state; a terminal state stamps its end and is never left again.
    State {
        state: RunState,
        error: Option<String>,
    },
    /// A coordinator attempt started on `node_id` for the claim with this digest.
    Attempt { node_id: NodeId, claim: String },
    /// The newest attempt of one task.
    Task(Box<WorkflowTaskRecord>),
    /// The job that drives the run on this node.
    Job(JobId),
    /// A file the run delivered to its output, and the origin it names when it has none yet.
    Delivered {
        file: RunFile,
        origin: Option<RunOrigin>,
    },
    /// How the publication of the run's crate ended, and its document once published.
    Provenance {
        status: String,
        document_id: Option<Ulid>,
    },
}

/// Applies changes to a run, its tasks and its event rows in one write transaction, so the
/// stream never shows a state the records do not hold.
#[derive(Clone, Debug, PartialEq)]
pub struct ChangeRunOperation {
    run_id: Ulid,
    changes: Vec<RunChange>,
    now_ms: u64,
    reading: bool,
    record: Option<WorkflowRunRecord>,
}

impl ChangeRunOperation {
    pub fn new(run_id: Ulid, changes: Vec<RunChange>) -> Self {
        Self {
            run_id,
            changes,
            now_ms: 0,
            reading: true,
            record: None,
        }
    }

    /// The tasks the changes name, read beside the run.
    fn task_ids(&self) -> Vec<u64> {
        (self.changes.iter())
            .filter_map(|change| match change {
                RunChange::Task(task) => Some(task.task_id),
                _ => None,
            })
            .collect()
    }

    fn apply(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        let mut values = values.into_iter();
        let run = values.next().flatten().ok_or(CatalogError::NotFound)?;
        let mut record = WorkflowRunRecord::from_bytes(&run)?;
        let mut stored = Vec::new();
        for value in values {
            stored.push(
                value
                    .map(|value| WorkflowTaskRecord::from_bytes(&value))
                    .transpose()?,
            );
        }
        let mut writes = Vec::new();
        let mut stored = stored.into_iter();
        for change in std::mem::take(&mut self.changes) {
            match change {
                RunChange::Task(task) => {
                    let old = stored.next().flatten();
                    change_task(&mut record, old, *task, &mut writes)?;
                }
                other => change_run(&mut record, other, self.now_ms, &mut writes)?,
            }
        }
        writes.push(run_row(&record)?);
        self.record = Some(record);
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for ChangeRunOperation {
    type Output = WorkflowRunRecord;
    type Error = CatalogError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        let mut reads = vec![(WORKFLOW_RUN_KEYSPACE.to_string(), run_key(self.run_id))];
        for task_id in self.task_ids() {
            reads.push((
                WORKFLOW_TASK_KEYSPACE.to_string(),
                task_key(self.run_id, task_id),
            ));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        if std::mem::replace(&mut self.reading, false) {
            return self.apply(result.values()?);
        }
        result.done()?;
        let record = self.record.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Done(record))
    }
}

/// The answer of a [`ChangeRunOperation`] child.
pub(crate) fn changed(result: IoResult) -> Result<WorkflowRunRecord, CatalogError> {
    result.child::<WorkflowRunRecord, CatalogError>()?
}

/// A change run as a child that reruns on a lost conflict, since tasks change runs in parallel.
pub(crate) fn change_call(run_id: Ulid, changes: Vec<RunChange>) -> IoCall {
    IoCall::retry(ChangeRunOperation::new(run_id, changes), &run_id.to_bytes())
}

fn change_run(
    record: &mut WorkflowRunRecord,
    change: RunChange,
    now_ms: u64,
    writes: &mut Writes,
) -> Result<(), CatalogError> {
    match change {
        RunChange::State { state, error } => {
            let settled = record.state.is_terminal()
                || (record.state == RunState::Cancelling && !state.is_terminal());
            if settled || record.state == state {
                return Ok(());
            }
            record.state = state;
            if state.is_terminal() {
                record.finished_at_ms = Some(now_ms);
                record.error = error;
            }
            let payload = events::state_json(record);
            events::push(record, "state", payload, writes)
        }
        RunChange::Attempt { node_id, claim } => {
            record.node_id = node_id;
            record.attempts += 1;
            record.claim_token_digest = Some(claim);
            let payload = events::run_json(record);
            events::push(record, "run", payload, writes)
        }
        RunChange::Delivered { file, origin } => {
            record.outputs.retain(|known| known.name != file.name);
            record.outputs.push(file);
            if record.origin.is_none() {
                record.origin = origin;
            }
            let payload = events::outputs_json(record);
            events::push(record, "state", payload, writes)
        }
        RunChange::Job(job_id) => {
            record.parent_job_id = Some(job_id);
            Ok(())
        }
        RunChange::Provenance {
            status,
            document_id,
        } => {
            record.provenance_status = Some(status);
            record.provenance_document_id = document_id.or(record.provenance_document_id);
            Ok(())
        }
        RunChange::Task(_) => Ok(()),
    }
}

/// Stores the newest attempt of a task: an older attempt, or a running answer after the attempt
/// ended, never replaces what is stored.
fn change_task(
    record: &mut WorkflowRunRecord,
    old: Option<WorkflowTaskRecord>,
    mut task: WorkflowTaskRecord,
    writes: &mut Writes,
) -> Result<(), CatalogError> {
    if let Some(old) = &old {
        let stale = task.attempt < old.attempt
            || (task.attempt == old.attempt
                && old.state.is_terminal()
                && !task.state.is_terminal());
        if stale {
            return Ok(());
        }
        keep_known(&mut task, old);
    }
    let position = match record.steps.iter().position(|step| step.step == task.step) {
        Some(position) => position,
        None => {
            record.steps.push(StepRecord {
                step: task.step.clone(),
                ..StepRecord::default()
            });
            record.steps.len() - 1
        }
    };
    let step = &mut record.steps[position];
    let counted = old.as_ref().filter(|old| old.step == task.step);
    step.count(counted.map(|old| old.state), Some(task.state));
    if task.cached && !counted.is_some_and(|old| old.cached) {
        step.cached += 1;
    }
    step.node_id = task.node_id.or(step.node_id);
    step.mode = task.mode.clone().or(step.mode.take());
    let step = step.clone();
    writes.push(task_row(&task)?);
    let task_payload = events::task_json(&task);
    events::push(record, "task", task_payload, writes)?;
    let step_payload = events::step_json(record.run_id, &step);
    events::push(record, "step", step_payload, writes)
}

/// Fills what a newer report of a task leaves out from what is stored: the coordinator and the
/// child job each know only part of an attempt.
fn keep_known(task: &mut WorkflowTaskRecord, old: &WorkflowTaskRecord) {
    let take = |new: &mut String, old: &String| {
        if new.is_empty() {
            new.clone_from(old);
        }
    };
    take(&mut task.task_key, &old.task_key);
    take(&mut task.name, &old.name);
    task.child_job_id = task.child_job_id.or(old.child_job_id);
    task.started_at_ms = task.started_at_ms.or(old.started_at_ms);
    task.finished_at_ms = task.finished_at_ms.or(old.finished_at_ms);
    task.exit_code = task.exit_code.or(old.exit_code);
    task.node_id = task.node_id.or(old.node_id);
    task.index = task.index.or(old.index);
    task.mode = task.mode.take().or_else(|| old.mode.clone());
    task.cache_key = task.cache_key.take().or_else(|| old.cache_key.clone());
    task.deterministic = task.deterministic.or(old.deterministic);
    task.error = task.error.take().or_else(|| old.error.clone());
    let lists = [
        (&mut task.inputs, &old.inputs),
        (&mut task.outputs, &old.outputs),
    ];
    for (new, old) in lists {
        if new.is_empty() {
            new.clone_from(old);
        }
    }
    for (new, old) in [
        (&mut task.stdout_tail, &old.stdout_tail),
        (&mut task.stderr_tail, &old.stderr_tail),
    ] {
        if new.is_empty() {
            new.clone_from(old);
        }
    }
}

/// Every task of a run, page by page, in task number order.
#[derive(Debug, PartialEq)]
pub struct RunTasksOperation {
    run_id: Ulid,
    tasks: Vec<WorkflowTaskRecord>,
}

impl RunTasksOperation {
    pub fn new(run_id: Ulid) -> Self {
        Self {
            run_id,
            tasks: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> Next<Vec<WorkflowTaskRecord>> {
        Next::Call(IoCall::Page {
            space: WORKFLOW_TASK_KEYSPACE.to_string(),
            prefix: run_key(self.run_id),
            after,
            limit: SCAN_ROWS,
        })
    }
}

impl Operation for RunTasksOperation {
    type Output = Vec<WorkflowTaskRecord>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Self::Output>, CatalogError> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CatalogError> {
        let (rows, next) = result.page()?;
        for (_, value) in rows {
            self.tasks.push(WorkflowTaskRecord::from_bytes(&value)?);
        }
        match next {
            Some(after) => Ok(self.page(Some(after))),
            None => Ok(Next::Done(std::mem::take(&mut self.tasks))),
        }
    }
}

/// The answer of a [`RunTasksOperation`] child.
pub(crate) fn tasks_answer(result: IoResult) -> Result<Vec<WorkflowTaskRecord>, CatalogError> {
    result.child::<Vec<WorkflowTaskRecord>, CatalogError>()?
}

/// The caller the node acts as for a run: its creator, never the node itself.
pub fn creator_auth(record: &WorkflowRunRecord) -> AuthContext {
    AuthContext::new(record.created_by, record.realm_id)
}
