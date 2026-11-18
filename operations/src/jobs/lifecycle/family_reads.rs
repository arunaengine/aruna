//! Reads one request family through its alias: the projection and stored spec, and the status,
//! session, responder, report and audit answers built from them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::node_state::{NodeInfoError, ReadInfoOperation};
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::structs::execution::job::{
    ExecutionRole, JobFamilyId, JobFamilyRecord, JobId, JobPayload, JobProjection, JobRecord,
    JobRecordEnvelope, LogicalJobSpec, PhysicalExecutionState, ProjectedExecution,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::node_info::NodeInfoDocument;

use super::ids::session_of;
use super::report::{
    AuditPaging, AuditRange, FamilyReport, ReportDetails, ReportDetailsOperation, started_at,
};
use super::routing::{decidable, status_view};
use crate::jobs::records::RecordStoreError;
use crate::jobs::records::audit::{AuditPage, AuditScope, FamilyAuditConfig, FamilyAuditOperation};
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::records::pages::{PageReadError, ServingNode, SpecInputsOperation};
use crate::jobs::records::project::{
    FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation, ProjectedFamily,
};
use crate::jobs::route::{JobRouteError, RoutedJobStatus};
use crate::jobs::store::job_rows::ReadJobOperation;

/// A projection that lost its transaction every time is busy, not failed: the
/// caller reads again rather than seeing a storage error.
const BUSY: &str = "job status is busy, try again";

fn unavailable(error: impl std::fmt::Display) -> JobRouteError {
    JobRouteError::Unavailable(error.to_string())
}

fn no_projection() -> JobRouteError {
    unavailable("job family has no projection")
}

/// A projected family with the stored spec of its canonical claim.
pub type FamilyView = (ProjectedFamily, LogicalJobSpec);

/// The reduced family of one alias plus its stored spec; `None` names no family here. A lost
/// optimistic transaction is retried with the runner's conflict rule.
#[derive(Debug, PartialEq)]
pub struct FamilyProjectionOperation {
    job_id: JobId,
    now_ms: u64,
    projected: Option<ProjectedFamily>,
}

type ProjectionNext = Result<Next<Option<FamilyView>>, JobRouteError>;

impl FamilyProjectionOperation {
    pub fn new(job_id: JobId, now_ms: u64) -> Self {
        Self {
            job_id,
            now_ms,
            projected: None,
        }
    }

    fn projected(&mut self, read: Result<ProjectedFamily, RecordStoreError>) -> ProjectionNext {
        let projected = match read {
            Ok(projected) => projected,
            Err(RecordStoreError::UnknownAlias) => return Ok(Next::Done(None)),
            Err(error) if error.is_conflict() => return Err(unavailable(BUSY)),
            Err(error) => return Err(unavailable(error)),
        };
        if projected.projection.is_none() {
            return Err(no_projection());
        }
        let read = LoadFamilyOperation::family(projected.family);
        self.projected = Some(projected);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn with_spec(
        &self,
        projected: ProjectedFamily,
        records: &[JobRecordEnvelope],
    ) -> ProjectionNext {
        let canonical = (projected.projection.as_ref())
            .ok_or_else(no_projection)?
            .canonical_job_id;
        let spec = records.iter().find_map(|envelope| match &envelope.record {
            JobFamilyRecord::Spec(spec) if spec.job_id == canonical => Some(spec.as_ref().clone()),
            _ => None,
        });
        let spec = spec.ok_or_else(|| unavailable("job family spec unavailable"))?;
        Ok(Next::Done(Some((projected, spec))))
    }
}

impl Operation for FamilyProjectionOperation {
    type Output = Option<FamilyView>;
    type Error = JobRouteError;

    fn first(&mut self) -> ProjectionNext {
        let (job_id, now_ms) = (self.job_id, self.now_ms);
        let build = move || {
            ProjectFamilyOperation::new(ProjectFamilyConfig {
                family: FamilyRef::Alias(job_id),
                now_ms,
                rebuild: false,
            })
        };
        let child = boxed_retry(build, &job_id.to_bytes(), |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn next(&mut self, result: IoResult) -> ProjectionNext {
        match self.projected.take() {
            None => self.projected(result.child::<ProjectedFamily, RecordStoreError>()?),
            Some(projected) => {
                let records = result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()?;
                self.with_spec(projected, &records.map_err(unavailable)?)
            }
        }
    }
}

fn projection_read(result: IoResult) -> Result<Option<FamilyView>, JobRouteError> {
    result.child::<Option<FamilyView>, JobRouteError>()?
}

/// Answers one status read from the family; `None` means the alias names no family here, so the
/// caller keeps its ordinary routing. Another submitter's job is absent, never refused.
#[derive(Debug, PartialEq)]
pub struct FamilyStatusOperation {
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
}

impl FamilyStatusOperation {
    pub fn new(auth: &AuthContext, job_id: JobId, now_ms: u64) -> Self {
        Self {
            user_id: auth.user_id,
            job_id,
            now_ms,
        }
    }
}

impl Operation for FamilyStatusOperation {
    type Output = Option<RoutedJobStatus>;
    type Error = JobRouteError;

    fn first(&mut self) -> Result<Next<Self::Output>, JobRouteError> {
        let read = FamilyProjectionOperation::new(self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, JobRouteError> {
        let Some((projected, spec)) = projection_read(result)? else {
            return Ok(Next::Done(None));
        };
        if spec.created_by != self.user_id {
            return Err(JobRouteError::NotFound);
        }
        decidable(&projected)?;
        let status = projected.projection.map(|projection| RoutedJobStatus {
            job: status_view(self.job_id, &projection, &spec),
            run_crate: None,
        });
        Ok(Next::Done(status))
    }
}

/// The execution a session belongs to: the canonical one, otherwise the newest active one.
fn session_execution(projection: &JobProjection) -> Option<&ProjectedExecution> {
    let executions = &projection.executions;
    executions
        .iter()
        .find(|execution| Some(execution.execution_id) == projection.canonical_execution_id)
        .or_else(|| {
            executions.iter().max_by_key(|execution| {
                (
                    !execution.state.is_terminal(),
                    execution.started_at_ms,
                    execution.execution_id,
                )
            })
        })
}

/// The record a session route reads for a family job.
fn session_record(
    job_id: JobId,
    user_id: UserId,
    projection: &JobProjection,
    spec: LogicalJobSpec,
) -> JobRecord {
    let status = status_view(job_id, projection, &spec);
    let execution = session_execution(projection);
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(spec.payload),
        user_id,
        execution.map_or(spec.origin_node_id, |execution| execution.executor_node_id),
        spec.created_at_ms,
        spec.created_at_ms,
        None,
    );
    record.state = status.state;
    record.updated_at_ms = status.updated_at_ms;
    record.attempts = status.attempts;
    record.cancel_requested = status.cancel_requested;
    record.last_error = status.last_error;
    record.progress = status.progress;
    record.started_at_ms = execution.and_then(|execution| execution.started_at_ms);
    record.workspace_bucket = status.workspace_bucket;
    record.workspace_mode = status.workspace_mode;
    record.finished_at_ms = status.finished_at_ms;
    record
}

#[derive(Debug, PartialEq)]
enum SessionStep {
    Projection,
    LocalJob,
    Receipt(Box<JobRecord>, ulid::Ulid),
}

/// Resolves an owner's public session alias and its durable physical execution.
#[derive(Debug, PartialEq)]
pub struct SessionJobOperation {
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
    step: SessionStep,
}

type SessionNext = Result<Next<(JobRecord, Option<JobId>)>, JobRouteError>;

impl SessionJobOperation {
    pub fn new(user_id: UserId, job_id: JobId, now_ms: u64) -> Self {
        Self {
            user_id,
            job_id,
            now_ms,
            step: SessionStep::Projection,
        }
    }

    fn projected(&mut self, read: Option<FamilyView>) -> SessionNext {
        let Some((projected, spec)) = read else {
            self.step = SessionStep::LocalJob;
            let read = ReadJobOperation::new(self.job_id);
            return Ok(Next::Call(IoCall::child(read)));
        };
        if spec.created_by != self.user_id || session_of(&spec.payload).is_none() {
            return Err(JobRouteError::NotFound);
        }
        decidable(&projected)?;
        let projection = projected.projection.ok_or(JobRouteError::NotFound)?;
        let execution_id = session_execution(&projection).map(|run| run.execution_id);
        let record = session_record(self.job_id, self.user_id, &projection, spec);
        let Some(execution_id) = execution_id else {
            return Ok(Next::Done((record, None)));
        };
        self.step = SessionStep::Receipt(Box::new(record), execution_id);
        let read = LoadFamilyOperation::family(projected.family);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn local(&self, record: Option<JobRecord>) -> SessionNext {
        let owned = record
            .filter(|record| record.created_by == self.user_id && !record.payload.is_internal());
        let record = owned.ok_or(JobRouteError::NotFound)?;
        match &record.payload {
            JobPayload::Execution(spec) if session_of(spec).is_some() => {
                Ok(Next::Done((record, Some(self.job_id))))
            }
            _ => Err(JobRouteError::NotFound),
        }
    }
}

impl Operation for SessionJobOperation {
    type Output = (JobRecord, Option<JobId>);
    type Error = JobRouteError;

    fn first(&mut self) -> SessionNext {
        let read = FamilyProjectionOperation::new(self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> SessionNext {
        match std::mem::replace(&mut self.step, SessionStep::Projection) {
            SessionStep::Projection => self.projected(projection_read(result)?),
            SessionStep::LocalJob => {
                let read = result.child::<Option<JobRecord>, aruna_core::calls::IoError>()?;
                let record = read.map_err(|error| JobRouteError::Internal(error.to_string()))?;
                self.local(record)
            }
            SessionStep::Receipt(record, execution_id) => {
                let records = result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()?;
                let physical = (records.map_err(unavailable)?)
                    .iter()
                    .find_map(|envelope| match &envelope.record {
                        JobFamilyRecord::Receipt(receipt)
                            if receipt.execution_id == execution_id =>
                        {
                            Some(receipt.physical_job_id)
                        }
                        _ => None,
                    })
                    .ok_or_else(|| unavailable("execution receipt unavailable"))?;
                Ok(Next::Done((*record, Some(physical))))
            }
        }
    }
}
