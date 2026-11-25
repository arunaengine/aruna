//! Cancels a run with its job and every child job still running, and deletes an ended run with
//! its tasks, events and run log.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    RUN_EVENT_KEYSPACE, RUN_LOG_KEYSPACE, WORKFLOW_CRATE_KEYSPACE, WORKFLOW_RUN_KEYSPACE,
    WORKFLOW_TASK_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::workflow_run::{
    RunState, WorkflowRunRecord, WorkflowTaskRecord, run_key, run_prefix,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::KeySpace;
use tracing::warn;
use ulid::Ulid;

use super::{
    RunAccessOperation, RunChange, RunTasksOperation, access_of, change_call, changed,
    creator_auth, tasks_answer,
};
use crate::jobs::job::cancel::{CancelJobOutcome, CancelOwnedOperation, CancelReport};
use crate::jobs::job::reads::JobReadError;
use crate::jobs::lifecycle::cancel::CancelFamilyOperation;
use crate::jobs::route::JobRouteError;
use crate::workflows::{CatalogError, Need};

/// Rows one delete page removes.
const DELETE_ROWS: usize = 256;

/// A cancelled run and the in-process attempts of this node the caller must signal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelledRun {
    pub record: WorkflowRunRecord,
    pub running: Vec<JobId>,
}

#[derive(Debug, PartialEq)]
enum CancelStep {
    Access,
    Cancelling,
    Parent(Box<WorkflowRunRecord>),
    Stopped(Box<WorkflowRunRecord>),
    Tasks(Box<WorkflowRunRecord>),
    Child(Box<WorkflowRunRecord>, Vec<JobId>),
    LocalChild(Box<WorkflowRunRecord>, Vec<JobId>),
}

/// Cancels an active run: the run turns `cancelling`, its job is cancelled, and every child job
/// that has not ended is cancelled through its family as the run's creator.
#[derive(Debug, PartialEq)]
pub struct CancelRunOperation {
    auth: AuthContext,
    run_id: Ulid,
    serving: (NodeId, RealmId),
    now_ms: u64,
    running: Vec<JobId>,
    step: CancelStep,
}

type CancelNext = Result<Next<CancelledRun>, CatalogError>;

impl CancelRunOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid, serving: (NodeId, RealmId)) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            serving,
            now_ms: 0,
            running: Vec::new(),
            step: CancelStep::Access,
        }
    }

    fn children(&mut self, record: Box<WorkflowRunRecord>) -> CancelNext {
        self.step = CancelStep::Tasks(record);
        Ok(Next::Call(IoCall::child(RunTasksOperation::new(
            self.run_id,
        ))))
    }

    fn next_child(&mut self, record: Box<WorkflowRunRecord>, mut jobs: Vec<JobId>) -> CancelNext {
        let Some(job_id) = jobs.pop() else {
            return Ok(Next::Done(CancelledRun {
                record: *record,
                running: std::mem::take(&mut self.running),
            }));
        };
        if record.local_only {
            let cancel = CancelOwnedOperation::new(record.created_by, job_id, self.now_ms);
            self.step = CancelStep::LocalChild(record, jobs);
            return Ok(Next::Call(IoCall::child(cancel)));
        }
        let cancel = CancelFamilyOperation::new(
            &creator_auth(&record),
            job_id,
            None,
            Some(self.serving),
            self.now_ms,
        );
        self.step = CancelStep::Child(record, jobs);
        Ok(Next::Call(IoCall::child(cancel)))
    }
}

impl Operation for CancelRunOperation {
    type Output = CancelledRun;
    type Error = CatalogError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> CancelNext {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> CancelNext {
        match std::mem::replace(&mut self.step, CancelStep::Cancelling) {
            CancelStep::Access => {
                let (record, _) = access_of(result)?;
                if record.state.is_terminal() {
                    return Err(CatalogError::Conflict(format!(
                        "the run already ended as {}",
                        record.state.as_str()
                    )));
                }
                let change = RunChange::State {
                    state: RunState::Cancelling,
                    error: None,
                };
                Ok(Next::Call(change_call(self.run_id, vec![change])))
            }
            CancelStep::Cancelling => {
                let record = Box::new(changed(result)?);
                let Some(job_id) = record.parent_job_id else {
                    return self.children(record);
                };
                let cancel = CancelOwnedOperation::new(record.created_by, job_id, self.now_ms);
                self.step = CancelStep::Parent(record);
                Ok(Next::Call(IoCall::child(cancel)))
            }
            CancelStep::Parent(record) => {
                let report = result.child::<CancelReport<CancelJobOutcome>, JobReadError>()?;
                let report = report.map_err(|error| CatalogError::Storage(error.0))?;
                self.running = report.running;
                // A job that never started ends here; a running one ends the run itself.
                let ended = match &report.outcome {
                    CancelJobOutcome::Requested(job) | CancelJobOutcome::AlreadyTerminal(job) => {
                        job.state.is_terminal()
                    }
                    CancelJobOutcome::NotFound => true,
                };
                if !ended || !self.running.is_empty() {
                    return self.children(record);
                }
                let change = RunChange::State {
                    state: RunState::Cancelled,
                    error: Some("cancelled before it started".into()),
                };
                self.step = CancelStep::Stopped(record);
                Ok(Next::Call(change_call(self.run_id, vec![change])))
            }
            CancelStep::Stopped(_) => {
                let record = Box::new(changed(result)?);
                self.children(record)
            }
            CancelStep::Tasks(record) => {
                let jobs = (tasks_answer(result)?.into_iter())
                    .filter(|task| !task.state.is_terminal())
                    .filter_map(|task: WorkflowTaskRecord| task.child_job_id)
                    .collect();
                self.next_child(record, jobs)
            }
            CancelStep::Child(record, jobs) => {
                match result.child::<Option<()>, JobRouteError>()? {
                    Ok(_) => {}
                    Err(error) => {
                        warn!(%error, run = %self.run_id, "a child job was not cancelled")
                    }
                }
                self.next_child(record, jobs)
            }
            CancelStep::LocalChild(record, jobs) => {
                match result.child::<CancelReport<CancelJobOutcome>, JobReadError>()? {
                    Ok(report) => self.running.extend(report.running),
                    Err(error) => {
                        warn!(%error, run = %self.run_id, "a local child job was not cancelled")
                    }
                }
                self.next_child(record, jobs)
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(
            error,
            CatalogError::NotFound | CatalogError::Forbidden | CatalogError::Conflict(_)
        )
    }
}

/// Deletes an ended run: its row first, so it is gone at once, then its tasks, events, log and
/// stored crate.
#[derive(Debug, PartialEq)]
pub struct DeleteRunOperation {
    auth: AuthContext,
    run_id: Ulid,
    spaces: Vec<KeySpace>,
    accessed: bool,
}

impl DeleteRunOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            spaces: vec![
                WORKFLOW_CRATE_KEYSPACE.to_string(),
                RUN_LOG_KEYSPACE.to_string(),
                RUN_EVENT_KEYSPACE.to_string(),
                WORKFLOW_TASK_KEYSPACE.to_string(),
            ],
            accessed: false,
        }
    }

    fn page(&self) -> Next<()> {
        match self.spaces.last() {
            Some(space) => Next::Call(IoCall::Page {
                space: space.clone(),
                prefix: run_prefix(self.run_id),
                after: None,
                limit: DELETE_ROWS,
            }),
            None => Next::Done(()),
        }
    }
}

impl Operation for DeleteRunOperation {
    type Output = ();
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<()>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CatalogError> {
        if !self.accessed {
            self.accessed = true;
            let (record, _) = access_of(result)?;
            if record.state.is_active() {
                return Err(CatalogError::Conflict(format!(
                    "the run is still {}",
                    record.state.as_str()
                )));
            }
            return Ok(Next::Call(IoCall::Delete(vec![(
                WORKFLOW_RUN_KEYSPACE.to_string(),
                run_key(self.run_id),
            )])));
        }
        match result {
            IoResult::Done => Ok(self.page()),
            page => {
                let (rows, _) = page.page()?;
                if rows.is_empty() {
                    self.spaces.pop();
                    return Ok(self.page());
                }
                let space = self.spaces.last().cloned().unwrap_or_default();
                let deletes = rows.into_iter().map(|(key, _)| (space.clone(), key));
                Ok(Next::Call(IoCall::Delete(deletes.collect())))
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(
            error,
            CatalogError::NotFound | CatalogError::Forbidden | CatalogError::Conflict(_)
        )
    }
}
