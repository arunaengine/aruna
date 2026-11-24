//! The reads of runs the portal makes: lists, the snapshot with step counts, the graph, tasks,
//! the output tails of a task and the origin of a run. Each is checked through the run's group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{WORKFLOW_RUN_KEYSPACE, WORKFLOW_TASK_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{
    RunOrigin, RunState, WorkflowRunRecord, WorkflowTaskRecord, task_key,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::{GroupId, Key};
use aruna_workflow_contract::message::LogStream;
use ulid::Ulid;

use super::events::LISTED_TASKS;
use super::{RunAccessOperation, RunTasksOperation, access_of, creator_auth, tasks_answer};
use crate::workflows::inspect::{Facet, FacetValue, InspectOperation};
use crate::workflows::{
    Access, CatalogError, GraphView, GroupAccessOperation, Need, access_answer, page_of,
};

/// Rows one storage page of a run list reads.
const SCAN_ROWS: usize = 256;
/// Bytes one log read answers at most.
pub const LOG_CHUNK: usize = 64 * 1024;

/// Which runs a list keeps.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunFilter {
    pub group_id: Option<GroupId>,
    pub state: Option<RunState>,
    pub workflow_id: Option<Ulid>,
}

impl RunFilter {
    fn keeps(&self, record: &WorkflowRunRecord) -> bool {
        self.group_id.is_none_or(|group| group == record.group_id)
            && self.state.is_none_or(|state| state == record.state)
            && self
                .workflow_id
                .is_none_or(|workflow| workflow == record.workflow_id)
    }
}

/// One page of runs, newest first, and the run to continue after.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunPage {
    pub items: Vec<WorkflowRunRecord>,
    pub next: Option<Ulid>,
    pub total: usize,
    /// The groups of the listed runs whose runs the caller may change.
    pub writable: BTreeSet<GroupId>,
}

/// Lists the runs of this node the caller reads, newest first, paged by run id.
#[derive(Debug, PartialEq)]
pub struct ListRunsOperation {
    auth: AuthContext,
    filter: RunFilter,
    after: Option<Ulid>,
    limit: usize,
    found: Vec<WorkflowRunRecord>,
    scanning: bool,
}

impl ListRunsOperation {
    pub fn new(auth: &AuthContext, filter: RunFilter, after: Option<Ulid>, limit: usize) -> Self {
        Self {
            auth: auth.clone(),
            filter,
            after,
            limit,
            found: Vec::new(),
            scanning: true,
        }
    }

    fn page(after: Option<Key>) -> Next<RunPage> {
        Next::Call(IoCall::Page {
            space: WORKFLOW_RUN_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: SCAN_ROWS,
        })
    }
}

impl Operation for ListRunsOperation {
    type Output = RunPage;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<RunPage>, CatalogError> {
        Ok(Self::page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RunPage>, CatalogError> {
        if self.scanning {
            let (rows, next) = result.page()?;
            for (_, value) in rows {
                let record = WorkflowRunRecord::from_bytes(&value)?;
                if self.filter.keeps(&record) {
                    self.found.push(record);
                }
            }
            if let Some(after) = next {
                return Ok(Self::page(Some(after)));
            }
            self.scanning = false;
            let groups: BTreeSet<GroupId> = self.found.iter().map(|run| run.group_id).collect();
            if groups.is_empty() {
                return Ok(Next::Done(RunPage {
                    items: Vec::new(),
                    next: None,
                    total: 0,
                    writable: BTreeSet::new(),
                }));
            }
            return Ok(Next::Call(IoCall::child(GroupAccessOperation::new(
                &self.auth, groups,
            ))));
        }
        let access: BTreeMap<GroupId, Access> = access_answer(result)?;
        let readable = (std::mem::take(&mut self.found).into_iter())
            .filter(|run| access.get(&run.group_id).is_some_and(|access| access.read))
            .map(|run| (Reverse(run.run_id), run));
        let mut runs: Vec<_> = readable.collect();
        runs.sort_by_key(|(order, _)| *order);
        let after = self.after.map(Reverse);
        let (items, next, total) = page_of(runs, after.as_ref(), self.limit);
        let writable = (access.iter())
            .filter(|(_, access)| access.write)
            .map(|(group, _)| *group)
            .collect();
        Ok(Next::Done(RunPage {
            items,
            next: next.map(|Reverse(run_id)| run_id),
            total,
            writable,
        }))
    }
}

/// A run with the tasks of each step small enough to list them, by step.
#[derive(Clone, Debug, PartialEq)]
pub struct RunSnapshot {
    pub record: WorkflowRunRecord,
    pub listed: BTreeMap<String, Vec<WorkflowTaskRecord>>,
    /// The caller may change the run: cancel, delete, resume or repeat it.
    pub writable: bool,
}

/// Reads a run for its page: the record with its step counts, and the tasks of small steps.
#[derive(Debug, PartialEq)]
pub struct RunSnapshotOperation {
    auth: AuthContext,
    run_id: Ulid,
    record: Option<WorkflowRunRecord>,
    writable: bool,
}

impl RunSnapshotOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            record: None,
            writable: false,
        }
    }
}

impl Operation for RunSnapshotOperation {
    type Output = RunSnapshot;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<RunSnapshot>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RunSnapshot>, CatalogError> {
        let Some(record) = self.record.take() else {
            let (record, access) = access_of(result)?;
            self.writable = access.write;
            let small = record.steps.iter().any(|step| step.total() <= LISTED_TASKS);
            if !small {
                return Ok(Next::Done(RunSnapshot {
                    record,
                    listed: BTreeMap::new(),
                    writable: self.writable,
                }));
            }
            self.record = Some(record);
            return Ok(Next::Call(IoCall::child(RunTasksOperation::new(
                self.run_id,
            ))));
        };
        let small: BTreeSet<&str> = (record.steps.iter())
            .filter(|step| step.total() <= LISTED_TASKS)
            .map(|step| step.step.as_str())
            .collect();
        let mut listed: BTreeMap<String, Vec<WorkflowTaskRecord>> = BTreeMap::new();
        for task in tasks_answer(result)? {
            if small.contains(task.step.as_str()) {
                listed.entry(task.step.clone()).or_default().push(task);
            }
        }
        let writable = self.writable;
        Ok(Next::Done(RunSnapshot {
            record,
            listed,
            writable,
        }))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

/// The step graph of a run's workflow version, read as the run's creator once the caller may
/// read the run.
#[derive(Debug, PartialEq)]
pub struct RunGraphOperation {
    auth: AuthContext,
    run_id: Ulid,
    accessed: bool,
}

impl RunGraphOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            accessed: false,
        }
    }
}

impl Operation for RunGraphOperation {
    type Output = GraphView;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<GraphView>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<GraphView>, CatalogError> {
        if !std::mem::replace(&mut self.accessed, true) {
            let (record, _) = access_of(result)?;
            let inspect = InspectOperation::new(
                &creator_auth(&record),
                record.workflow_id,
                record.version,
                Facet::Graph,
            );
            return Ok(Next::Call(IoCall::child(inspect)));
        }
        match result.child::<FacetValue, CatalogError>()?? {
            FacetValue::Graph(graph) => Ok(Next::Done(graph)),
            _ => Err(crate::workflows::unexpected("FacetValue::Graph")),
        }
    }
}

/// Which tasks a task list keeps.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskFilter {
    pub step: Option<String>,
    pub state: Option<RunState>,
}

/// One page of tasks in task number order, and the task to continue after.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskPage {
    pub items: Vec<WorkflowTaskRecord>,
    pub next: Option<u64>,
    pub total: usize,
}

/// Lists the tasks of a run, filtered by step and state, paged by task number.
#[derive(Debug, PartialEq)]
pub struct ListTasksOperation {
    auth: AuthContext,
    run_id: Ulid,
    filter: TaskFilter,
    after: Option<u64>,
    limit: usize,
    accessed: bool,
}

impl ListTasksOperation {
    pub fn new(
        auth: &AuthContext,
        run_id: Ulid,
        filter: TaskFilter,
        (after, limit): (Option<u64>, usize),
    ) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            filter,
            after,
            limit,
            accessed: false,
        }
    }
}

impl Operation for ListTasksOperation {
    type Output = TaskPage;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<TaskPage>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TaskPage>, CatalogError> {
        if !std::mem::replace(&mut self.accessed, true) {
            access_of(result)?;
            return Ok(Next::Call(IoCall::child(RunTasksOperation::new(
                self.run_id,
            ))));
        }
        let filter = &self.filter;
        let tasks = (tasks_answer(result)?.into_iter())
            .filter(|task| filter.step.as_ref().is_none_or(|step| *step == task.step))
            .filter(|task| filter.state.is_none_or(|state| state == task.state))
            .map(|task| (task.task_id, task))
            .collect();
        let (items, next, total) = page_of(tasks, self.after.as_ref(), self.limit);
        Ok(Next::Done(TaskPage { items, next, total }))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

/// Reads one task of a run.
#[derive(Debug, PartialEq)]
pub struct GetTaskOperation {
    auth: AuthContext,
    run_id: Ulid,
    task_id: u64,
    accessed: bool,
}

impl GetTaskOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid, task_id: u64) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            task_id,
            accessed: false,
        }
    }
}

impl Operation for GetTaskOperation {
    type Output = WorkflowTaskRecord;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<WorkflowTaskRecord>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkflowTaskRecord>, CatalogError> {
        if !std::mem::replace(&mut self.accessed, true) {
            access_of(result)?;
            return Ok(Next::Call(IoCall::Read(vec![(
                WORKFLOW_TASK_KEYSPACE.to_string(),
                task_key(self.run_id, self.task_id),
            )])));
        }
        let value = result.value()?.ok_or(CatalogError::NotFound)?;
        Ok(Next::Done(WorkflowTaskRecord::from_bytes(&value)?))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

/// One piece of a task's output stream from an offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogChunk {
    pub stream: LogStream,
    pub offset: u64,
    pub text: String,
    pub next_offset: u64,
    /// The task ended, so no more output follows.
    pub complete: bool,
}

/// The stored output tail of a task from `offset`, at most [`LOG_CHUNK`] bytes.
pub fn log_chunk(task: &WorkflowTaskRecord, stream: LogStream, offset: u64) -> LogChunk {
    let bytes = match stream {
        LogStream::Stdout => &task.stdout_tail,
        LogStream::Stderr => &task.stderr_tail,
    };
    let start = usize::try_from(offset)
        .unwrap_or(usize::MAX)
        .min(bytes.len());
    let end = start.saturating_add(LOG_CHUNK).min(bytes.len());
    LogChunk {
        stream,
        offset: start as u64,
        text: String::from_utf8_lossy(&bytes[start..end]).into_owned(),
        next_offset: end as u64,
        complete: task.state.is_terminal() && end == bytes.len(),
    }
}

/// The origin of a run: the first output version it delivered.
#[derive(Debug, PartialEq)]
pub struct RunOriginOperation {
    access: Option<RunAccessOperation>,
}

impl RunOriginOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid) -> Self {
        Self {
            access: Some(RunAccessOperation::new(auth, run_id, Need::Read)),
        }
    }
}

impl Operation for RunOriginOperation {
    type Output = RunOrigin;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<RunOrigin>, CatalogError> {
        let access = self.access.take().ok_or(CatalogError::NotFound)?;
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RunOrigin>, CatalogError> {
        let (record, _) = access_of(result)?;
        record.origin.map(Next::Done).ok_or(CatalogError::NotFound)
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}
