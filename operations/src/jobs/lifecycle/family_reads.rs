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
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::row_calls::read_job_record;

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
            let read = read_job_record(self.job_id);
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
                let read = result.child::<Option<JobRecord>, JobMutationError>()?;
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

/// The canonical execution's node, otherwise a successful execution's node, otherwise any
/// execution's node. `None` names no family here, or a family without executions.
#[derive(Debug, PartialEq)]
pub struct FamilyResponderOperation {
    job_id: JobId,
    now_ms: u64,
}

impl FamilyResponderOperation {
    pub fn new(job_id: JobId, now_ms: u64) -> Self {
        Self { job_id, now_ms }
    }
}

impl Operation for FamilyResponderOperation {
    type Output = Option<NodeId>;
    type Error = JobRouteError;

    fn first(&mut self) -> Result<Next<Option<NodeId>>, JobRouteError> {
        let read = FamilyProjectionOperation::new(self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<NodeId>>, JobRouteError> {
        let Some((projected, _)) = projection_read(result)? else {
            return Ok(Next::Done(None));
        };
        decidable(&projected)?;
        let projection = projected.projection.ok_or_else(no_projection)?;
        let executions = &projection.executions;
        let responder = executions
            .iter()
            .find(|execution| execution.role == ExecutionRole::Canonical)
            .or_else(|| {
                executions
                    .iter()
                    .find(|execution| execution.state == PhysicalExecutionState::Succeeded)
            })
            .or_else(|| executions.first())
            .map(|execution| execution.executor_node_id);
        Ok(Next::Done(responder))
    }
}

/// The report of a projected family with everything read for it.
fn family_report(
    job_id: JobId,
    projected: &ProjectedFamily,
    projection: &JobProjection,
    spec: LogicalJobSpec,
    details: ReportDetails,
    responder: Option<NodeId>,
) -> Result<FamilyReport, JobRouteError> {
    let digest = projection
        .digest()
        .map_err(|error| JobRouteError::Internal(error.to_string()))?;
    let executions = &projection.executions;
    let canonical = projection.canonical_execution_id;
    Ok(FamilyReport {
        job: status_view(job_id, projection, &spec),
        submission_id: projection.submission_id,
        request_digest: projection.request_digest,
        canonical_job_id: projection.canonical_job_id,
        aliases: projection.aliases.clone(),
        conflicts: details.conflicts,
        state: projection.state,
        canonical_execution_id: canonical,
        canonical_result: canonical.and_then(|execution_id| {
            executions
                .iter()
                .find(|execution| execution.execution_id == execution_id)
                .and_then(|execution| execution.result.clone())
        }),
        executions: executions.len() as u32,
        started_at_ms: started_at(projection),
        execution_list: executions.clone(),
        duplicate_successes: executions
            .iter()
            .filter(|execution| execution.role == ExecutionRole::DuplicateSuccess)
            .count() as u32,
        outputs: projection.outputs.as_slice().to_vec(),
        output_endpoints: details.output_endpoints,
        revision: projected.revision,
        digest,
        cancel_requested: projection.cancel_requested,
        responder,
        partial: projected.truncated,
        locally_exhausted: details.locally_exhausted,
        plan: details.plan,
        spec,
    })
}

#[derive(Debug, PartialEq)]
enum ReportStep {
    Projection,
    Inputs(Box<ProjectedFamily>),
    Details(Box<ProjectedFamily>, Box<LogicalJobSpec>),
}

/// Reduces one alias into the full report; `None` names no family here. Reads stay self-scoped,
/// and a run its submitter deleted answers as absent on every holder the deletion reached.
#[derive(Debug, PartialEq)]
pub struct FamilyReportOperation {
    user_id: UserId,
    job_id: JobId,
    node: ServingNode,
    now_ms: u64,
    step: ReportStep,
}

type ReportNext = Result<Next<Option<FamilyReport>>, JobRouteError>;

impl FamilyReportOperation {
    pub fn new(auth: &AuthContext, job_id: JobId, node: ServingNode, now_ms: u64) -> Self {
        Self {
            user_id: auth.user_id,
            job_id,
            node,
            now_ms,
            step: ReportStep::Projection,
        }
    }

    fn responder(&self) -> Option<NodeId> {
        self.node.map(|(local, _)| local)
    }

    fn details(&mut self, projected: ProjectedFamily, spec: LogicalJobSpec) -> ReportNext {
        let Some(projection) = projected.projection.as_ref() else {
            return Ok(Next::Done(None));
        };
        let family = JobFamilyId {
            submission_id: projection.submission_id,
            request_digest: projection.request_digest,
        };
        let details = ReportDetailsOperation::new(family, projection, self.responder());
        self.step = ReportStep::Details(Box::new(projected), Box::new(spec));
        Ok(Next::Call(IoCall::child(details)))
    }
}

impl Operation for FamilyReportOperation {
    type Output = Option<FamilyReport>;
    type Error = JobRouteError;

    fn first(&mut self) -> ReportNext {
        let read = FamilyProjectionOperation::new(self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> ReportNext {
        match std::mem::replace(&mut self.step, ReportStep::Projection) {
            ReportStep::Projection => {
                let Some((projected, spec)) = projection_read(result)? else {
                    return Ok(Next::Done(None));
                };
                if spec.created_by != self.user_id {
                    return Err(JobRouteError::NotFound);
                }
                // A paged spec reports its inputs as if they were inline.
                let fill = SpecInputsOperation::new(&spec, self.node);
                self.step = ReportStep::Inputs(Box::new(projected));
                Ok(Next::Call(IoCall::child(fill)))
            }
            ReportStep::Inputs(projected) => {
                let spec = result.child::<LogicalJobSpec, PageReadError>()?;
                self.details(*projected, spec.map_err(unavailable)?)
            }
            ReportStep::Details(projected, spec) => {
                let details = result.child::<ReportDetails, JobRouteError>()??;
                if details.deleted {
                    return Err(JobRouteError::NotFound);
                }
                let projection = projected.projection.as_ref().ok_or_else(no_projection)?;
                let report = family_report(
                    self.job_id,
                    &projected,
                    projection,
                    *spec,
                    details,
                    self.responder(),
                )?;
                Ok(Next::Done(Some(report)))
            }
        }
    }
}

/// Pages the immutable log of one alias's family; `None` names no family here. A caller that
/// did not submit the job gets `NotFound`, so the surface never confirms another user's id.
#[derive(Debug, PartialEq)]
pub struct FamilyLogOperation {
    user_id: UserId,
    job_id: JobId,
    range: AuditRange,
    paging: Option<AuditPaging>,
    now_ms: u64,
}

impl FamilyLogOperation {
    pub fn new(
        auth: &AuthContext,
        job_id: JobId,
        (range, paging): (AuditRange, AuditPaging),
        now_ms: u64,
    ) -> Self {
        Self {
            user_id: auth.user_id,
            job_id,
            range,
            paging: Some(paging),
            now_ms,
        }
    }
}

impl Operation for FamilyLogOperation {
    type Output = Option<AuditPage>;
    type Error = JobRouteError;

    fn first(&mut self) -> Result<Next<Option<AuditPage>>, JobRouteError> {
        let read = FamilyProjectionOperation::new(self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<AuditPage>>, JobRouteError> {
        let Some(paging) = self.paging.take() else {
            let page = result.child::<AuditPage, RecordStoreError>()?;
            let page = page.map_err(|error| JobRouteError::Internal(error.to_string()))?;
            return Ok(Next::Done(Some(page)));
        };
        let Some((projected, spec)) = projection_read(result)? else {
            return Ok(Next::Done(None));
        };
        if spec.created_by != self.user_id {
            return Err(JobRouteError::NotFound);
        }
        let scope = match self.range {
            AuditRange::Family => AuditScope::Family(projected.family),
            AuditRange::Submission => AuditScope::Submission(projected.family.submission_id),
        };
        let page = FamilyAuditOperation::new(FamilyAuditConfig {
            scope,
            cursor: paging.cursor,
            limit: paging.limit,
        });
        Ok(Next::Call(IoCall::child(page)))
    }
}

/// Adds the S3 endpoint of every node that holds an output named by `records`. A node whose
/// advertisement cannot be read simply stays without an endpoint.
#[derive(Debug, PartialEq)]
pub struct AuditEndpointsOperation {
    /// Nodes still to read, the next one last.
    nodes: Vec<NodeId>,
    asked: Option<NodeId>,
    endpoints: BTreeMap<NodeId, String>,
}

type EndpointsNext = Result<Next<BTreeMap<NodeId, String>>, aruna_core::calls::IoError>;

impl AuditEndpointsOperation {
    pub fn new(records: &[JobRecordEnvelope], endpoints: BTreeMap<NodeId, String>) -> Self {
        let mut nodes = Vec::new();
        for envelope in records {
            let JobFamilyRecord::Output(output) = &envelope.record else {
                continue;
            };
            for object in output.outputs.as_slice() {
                let known = endpoints.contains_key(&object.node_id);
                if !known && !nodes.contains(&object.node_id) {
                    nodes.push(object.node_id);
                }
            }
        }
        nodes.reverse();
        Self {
            nodes,
            asked: None,
            endpoints,
        }
    }

    fn read_next(&mut self) -> EndpointsNext {
        let Some(node) = self.nodes.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.endpoints)));
        };
        self.asked = Some(node);
        let read = ReadInfoOperation::new(vec![node]);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for AuditEndpointsOperation {
    type Output = BTreeMap<NodeId, String>;
    type Error = aruna_core::calls::IoError;

    fn first(&mut self) -> EndpointsNext {
        self.read_next()
    }

    fn next(&mut self, result: IoResult) -> EndpointsNext {
        let read = result.child::<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError>()?;
        if let (Some(node), Ok(mut documents)) = (self.asked.take(), read)
            && let Some(document) = documents.remove(&node)
            && let Some(endpoint) = document.urls.s3
        {
            self.endpoints.insert(node, endpoint);
        }
        self.read_next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{Family, REALM, node, user};

    fn job() -> JobId {
        JobId::from_bytes([3; 16])
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn projected(family: &Family, truncated: bool) -> ProjectedFamily {
        ProjectedFamily {
            family: family.family(),
            revision: 0,
            projection: None,
            truncated,
            cached: false,
        }
    }

    fn view(family: &Family, truncated: bool) -> IoResult {
        let read = Some((projected(family, truncated), family.spec()));
        child(Ok::<_, JobRouteError>(read))
    }

    fn absent() -> IoResult {
        child(Ok::<Option<FamilyView>, JobRouteError>(None))
    }

    fn auth(user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id: REALM,
            path_restrictions: None,
            session: None,
        }
    }

    fn stranger() -> UserId {
        UserId::local(ulid::Ulid::from_parts(9, 9), REALM)
    }

    // An unknown alias names no family; a lost transaction reads as busy.
    #[test]
    fn projection_outcomes() {
        let answer = |error| {
            let mut read = FamilyProjectionOperation::new(job(), 1);
            assert!(matches!(read.first(), Ok(Next::Call(IoCall::Child(_)))));
            read.next(child(Err::<ProjectedFamily, _>(error)))
        };
        assert_eq!(answer(RecordStoreError::UnknownAlias), Ok(Next::Done(None)));
        let conflict =
            RecordStoreError::Storage(aruna_core::errors::StorageError::TransactionConflict);
        assert_eq!(answer(conflict), Err(unavailable(BUSY)));
    }

    // A family reduced to nothing has no spec to look up.
    #[test]
    fn projection_needs_content() {
        let family = Family::new([1; 32]);
        let mut read = FamilyProjectionOperation::new(job(), 1);
        read.first().expect("the projection starts");
        let done = read.next(child(Ok::<_, RecordStoreError>(projected(&family, false))));
        assert_eq!(done, Err(no_projection()));
    }

    // Status stays self-scoped, and a truncated projection answers nothing.
    #[test]
    fn status_rules() {
        let family = Family::new([1; 32]);
        let answer = |user_id, truncated| {
            let mut read = FamilyStatusOperation::new(&auth(user_id), job(), 1);
            read.first().expect("the projection starts");
            read.next(view(&family, truncated))
        };
        let owner = family.spec().created_by;
        assert_eq!(answer(stranger(), false), Err(JobRouteError::NotFound));
        assert!(matches!(
            answer(owner, true),
            Err(JobRouteError::Unavailable(_))
        ));
        assert_eq!(answer(owner, false), Ok(Next::Done(None)));
    }

    // Without a family the session is looked up as this owner's local job.
    #[test]
    fn session_falls_back() {
        let mut read = SessionJobOperation::new(user(), job(), 1);
        read.first().expect("the projection starts");
        assert!(matches!(
            read.next(absent()),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let missing = read.next(child(Ok::<Option<JobRecord>, JobMutationError>(None)));
        assert_eq!(missing, Err(JobRouteError::NotFound));
    }

    // A session of another submitter is absent.
    #[test]
    fn session_stays_private() {
        let family = Family::new([1; 32]);
        let mut read = SessionJobOperation::new(stranger(), job(), 1);
        read.first().expect("the projection starts");
        assert_eq!(
            read.next(view(&family, false)),
            Err(JobRouteError::NotFound)
        );
    }

    // No family means no responder, and a truncated one cannot choose.
    #[test]
    fn responder_rules() {
        let family = Family::new([1; 32]);
        let mut none = FamilyResponderOperation::new(job(), 1);
        none.first().expect("the projection starts");
        assert_eq!(none.next(absent()), Ok(Next::Done(None)));
        let mut partial = FamilyResponderOperation::new(job(), 1);
        partial.first().expect("the projection starts");
        assert!(matches!(
            partial.next(view(&family, true)),
            Err(JobRouteError::Unavailable(_))
        ));
    }

    // A report is read only by the submitter, whose paged inputs are filled next.
    #[test]
    fn report_flow() {
        let family = Family::new([1; 32]);
        let local = Some((node(1), REALM));
        let mut foreign = FamilyReportOperation::new(&auth(stranger()), job(), local, 1);
        foreign.first().expect("the projection starts");
        assert_eq!(
            foreign.next(view(&family, false)),
            Err(JobRouteError::NotFound)
        );
        let owner = auth(family.spec().created_by);
        let mut report = FamilyReportOperation::new(&owner, job(), local, 1);
        report.first().expect("the projection starts");
        assert!(matches!(
            report.next(view(&family, false)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let spec = child(Ok::<_, PageReadError>(family.spec()));
        assert_eq!(report.next(spec), Ok(Next::Done(None)));
    }

    // A deleted run answers as absent once its details are read.
    #[test]
    fn deleted_run_absent() {
        let family = Family::new([1; 32]);
        let owner = auth(family.spec().created_by);
        let mut report = FamilyReportOperation::new(&owner, job(), None, 1);
        report.step =
            ReportStep::Details(Box::new(projected(&family, false)), Box::new(family.spec()));
        let details = ReportDetails {
            deleted: true,
            ..ReportDetails::default()
        };
        let answer = report.next(child(Ok::<_, JobRouteError>(details)));
        assert_eq!(answer, Err(JobRouteError::NotFound));
    }

    fn paging() -> AuditPaging {
        AuditPaging {
            cursor: None,
            limit: Default::default(),
        }
    }

    // The log is paged for the submitter and a failed page is an internal error.
    #[test]
    fn log_flow() {
        let family = Family::new([1; 32]);
        let owner = auth(family.spec().created_by);
        let range = (AuditRange::Family, paging());
        let mut log = FamilyLogOperation::new(&owner, job(), range, 1);
        log.first().expect("the projection starts");
        assert!(matches!(
            log.next(view(&family, false)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let failed = Err::<AuditPage, _>(RecordStoreError::UnknownAlias);
        assert!(matches!(
            log.next(child(failed)),
            Err(JobRouteError::Internal(_))
        ));
    }

    // Known endpoints are kept, and records without outputs read nothing.
    #[test]
    fn endpoints_keep_known() {
        let known = BTreeMap::from([(node(1), "https://s3".to_string())]);
        let mut read = AuditEndpointsOperation::new(&[], known.clone());
        assert_eq!(read.first(), Ok(Next::Done(known)));
    }

    // An advertisement that cannot be read leaves that node without an endpoint.
    #[test]
    fn unread_endpoint_skipped() {
        let mut read = AuditEndpointsOperation {
            nodes: Vec::new(),
            asked: Some(node(2)),
            endpoints: BTreeMap::new(),
        };
        let failed = NodeInfoError::Decode("unreadable".to_string());
        let answer = child(Err::<BTreeMap<NodeId, NodeInfoDocument>, _>(failed));
        assert_eq!(read.next(answer), Ok(Next::Done(BTreeMap::new())));
    }
}
