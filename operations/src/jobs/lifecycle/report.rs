//! Reports one request family to external surfaces: its records, status and audit pages.
//! The witness plan and retry state stay outside the projection digest as local diagnostics.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::ExecutionTargetId;
use aruna_core::effects::{FetchCursor, PageLimit};
use aruna_core::id::NodeId;
use aruna_core::jobs::JobStatusView;
use aruna_core::keyspaces::{FAMILY_RECORD_KEYSPACE, PLAN_EXPLAIN_KEYSPACE};
use aruna_core::scheduling::{PlanCandidate, PlannedInput};
use aruna_core::structs::execution::job::{
    CancelAuthority, ExecutionRole, JobFamilyId, JobFamilyRecord, JobId, JobProjection,
    JobRecordEnvelope, JobRecordKey, JobRecordKind, LogicalJobSpec, LogicalJobState, OutputObject,
    PhysicalExecutionResult, PhysicalExecutionState, ProjectedExecution, SubmissionId,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::Key;
use std::collections::{BTreeMap, BTreeSet};
use tracing::debug;

use super::routing::{family_projection, status_view};
use super::witness_arm::{WitnessDeadline, WitnessExplain, deadline_index_key};
use crate::driver::{DriverContext, drive};
use crate::jobs::records::audit::{AuditPage, AuditScope, FamilyAuditConfig, FamilyAuditOperation};
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::route::JobRouteError;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::DEADLINE_INDEX_KEYSPACE;
use aruna_core::node_state::{NodeInfoError, ReadInfoOperation};
use aruna_core::operation::Operation;
use aruna_core::rows::{page, values};
use aruna_core::structs::execution::job::record_keys::submission_prefix;
use aruna_core::structs::execution::job::record_rows::from_bytes;
use aruna_core::structs::storage::node_info::NodeInfoDocument;

/// Sibling families of one submission a conflict count may scan.
const MAX_SIBLING_SCAN: usize = 256;
/// Explain rows one report reads; only this node ever writes them.
const MAX_EXPLAIN_ROWS: usize = 4;

/// The placement behind one family: the plan this responder's witness round
/// stored, or the newest launch record any witness published when it never
/// planned. Counted alternatives and rejections exist only in the first case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanEstimate {
    pub target: Option<ExecutionTargetId>,
    /// The node that planned the launch.
    pub scheduler_node_id: Option<NodeId>,
    pub estimated_transfer_bytes: u64,
    pub estimated_transfer_ms: u64,
    pub alternatives: u32,
    pub rejected: u32,
    /// Rejections the bound dropped, so truncation is never read as agreement.
    pub omitted: u32,
    pub stored_at_ms: u64,
    /// Inputs the plan moves to the target, so a caller can show where the data
    /// comes from. Empty when nothing has to move.
    pub inputs: Vec<PlannedInput>,
    /// Every target the planning round saw, with its verdict. Only a local
    /// round keeps them, so a launch read leaves this empty.
    pub candidates: Vec<PlanCandidate>,
}

/// One family as this responder currently reduces it.
#[derive(Clone, Debug, PartialEq)]
pub struct FamilyReport {
    /// The legacy per-job view the existing status shape is built from.
    pub job: JobStatusView,
    pub spec: LogicalJobSpec,
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub canonical_job_id: JobId,
    pub aliases: Vec<JobId>,
    /// Families of the same submission bound to another request digest.
    pub conflicts: u32,
    pub state: LogicalJobState,
    pub canonical_execution_id: Option<ulid::Ulid>,
    pub canonical_result: Option<PhysicalExecutionResult>,
    pub executions: u32,
    /// Every known execution of the family, canonical or not.
    pub execution_list: Vec<ProjectedExecution>,
    /// When the work started, taken from the canonical execution and otherwise
    /// from the newest one this responder knows.
    pub started_at_ms: Option<u64>,
    pub duplicate_successes: u32,
    pub outputs: Vec<OutputObject>,
    /// Public storage endpoint for each output-owning node.
    pub output_endpoints: BTreeMap<NodeId, String>,
    pub revision: u64,
    pub digest: [u8; 32],
    pub cancel_requested: bool,
    /// The node that answered. Reads of an eventually consistent view name it.
    pub responder: Option<NodeId>,
    /// The family holds more records than one projection may reduce.
    pub partial: bool,
    /// Responder-local only: every known execution is terminal without success
    /// and no retry is armed here. Deliberately outside the projection digest,
    /// and not evidence of a permanent failure.
    pub locally_exhausted: bool,
    pub plan: Option<PlanEstimate>,
}

/// Reduces one alias into the full report. `None` means the alias names no
/// family here, so the caller keeps its ordinary routing.
pub async fn family_report(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
) -> Option<Result<FamilyReport, JobRouteError>> {
    let (projected, spec) = match family_projection(context, job_id).await {
        Ok(Some(projected)) => projected,
        Ok(None) => return None,
        Err(error) => return Some(Err(error)),
    };
    // Reads stay self-scoped: another submitter's job is absent, never refused.
    if spec.created_by != auth.user_id {
        return Some(Err(JobRouteError::NotFound));
    }
    // A paged spec reports its inputs as if they were inline.
    let spec = match crate::jobs::records::pages::with_inputs(context, &spec).await {
        Ok(spec) => spec,
        Err(error) => return Some(Err(JobRouteError::Unavailable(error.to_string()))),
    };
    let projection = projected.projection.clone()?;
    let family = JobFamilyId {
        submission_id: projection.submission_id,
        request_digest: projection.request_digest,
    };
    let responder = context.net_handle.as_ref().map(|net| net.node_id());
    let details = ReportDetailsOperation::new(family, &projection, responder);
    let details = match drive(details, context).await {
        Ok(details) => details,
        Err(error) => return Some(Err(error)),
    };
    // A run its submitter deleted answers as absent on every holder its deletion reached.
    if details.deleted {
        return Some(Err(JobRouteError::NotFound));
    }
    let digest = match projection.digest() {
        Ok(digest) => digest,
        Err(error) => return Some(Err(JobRouteError::Internal(error.to_string()))),
    };
    Some(Ok(FamilyReport {
        job: status_view(job_id, &projection, &spec),
        submission_id: projection.submission_id,
        request_digest: projection.request_digest,
        canonical_job_id: projection.canonical_job_id,
        aliases: projection.aliases.clone(),
        conflicts: details.conflicts,
        state: projection.state,
        canonical_execution_id: projection.canonical_execution_id,
        canonical_result: projection.canonical_execution_id.and_then(|execution_id| {
            projection
                .executions
                .iter()
                .find(|execution| execution.execution_id == execution_id)
                .and_then(|execution| execution.result.clone())
        }),
        executions: projection.executions.len() as u32,
        started_at_ms: started_at(&projection),
        execution_list: projection.executions.clone(),
        duplicate_successes: projection
            .executions
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
    }))
}

fn terminal(state: PhysicalExecutionState) -> bool {
    state.is_terminal()
}

/// When the family's work started: the canonical execution's start, else the
/// newest start any execution reports.
fn started_at(projection: &JobProjection) -> Option<u64> {
    projection
        .canonical_execution_id
        .and_then(|execution_id| {
            projection
                .executions
                .iter()
                .find(|execution| execution.execution_id == execution_id)
        })
        .and_then(|execution| execution.started_at_ms)
        .or_else(|| {
            projection
                .executions
                .iter()
                .filter_map(|execution| execution.started_at_ms)
                .max()
        })
}

/// What a report reads beside the projection: the deletion, the conflicts, the exhaustion, the
/// placement and the output endpoints.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ReportDetails {
    /// An authentic cancel record of the family records its deletion.
    pub deleted: bool,
    pub conflicts: u32,
    pub locally_exhausted: bool,
    pub plan: Option<PlanEstimate>,
    pub output_endpoints: BTreeMap<NodeId, String>,
}

#[derive(Debug, PartialEq)]
enum DetailStep {
    Cancels,
    Siblings,
    Deadline,
    Explain,
    Launches,
    Endpoint(NodeId),
}

/// Reads the [`ReportDetails`] of one family. The conflicts are the distinct other request
/// families of the submission, which a partition may have accepted elsewhere. The plan is this
/// responder's own stored plan, else the newest launch any witness published.
#[derive(Debug, PartialEq)]
pub(crate) struct ReportDetailsOperation {
    family: JobFamilyId,
    /// Every known execution is terminal without success; exhausted unless a deadline is armed.
    exhaustible: bool,
    responder: Option<NodeId>,
    /// Output nodes whose endpoint is still to read, the next one last.
    nodes: Vec<NodeId>,
    details: ReportDetails,
    step: DetailStep,
}

type DetailNext = Result<Next<ReportDetails>, JobRouteError>;

impl ReportDetailsOperation {
    pub(crate) fn new(
        family: JobFamilyId,
        projection: &JobProjection,
        responder: Option<NodeId>,
    ) -> Self {
        let executions = &projection.executions;
        let exhaustible = projection.state == LogicalJobState::Indeterminate
            && !executions.is_empty()
            && executions.iter().all(|execution| terminal(execution.state));
        let mut nodes: Vec<NodeId> = projection
            .outputs
            .as_slice()
            .iter()
            .map(|o| o.node_id)
            .collect();
        nodes.sort_unstable();
        nodes.dedup();
        nodes.reverse();
        Self {
            family,
            exhaustible,
            responder,
            nodes,
            details: ReportDetails::default(),
            step: DetailStep::Cancels,
        }
    }

    fn siblings(&mut self, result: IoResult) -> DetailNext {
        let (rows, _) = page(result)?;
        let digests: BTreeSet<[u8; 32]> = rows
            .iter()
            .filter_map(|(key, _)| key.get(32..64)?.try_into().ok())
            .filter(|digest| *digest != self.family.request_digest)
            .collect();
        self.details.conflicts = digests.len() as u32;
        if !self.exhaustible {
            return self.explain();
        }
        self.step = DetailStep::Deadline;
        let row = (
            DEADLINE_INDEX_KEYSPACE.to_string(),
            deadline_index_key(&self.family),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn explain(&mut self) -> DetailNext {
        self.step = DetailStep::Explain;
        Ok(Next::Call(IoCall::Page {
            space: PLAN_EXPLAIN_KEYSPACE.to_string(),
            prefix: Key::from(self.family.to_bytes().as_slice()),
            after: None,
            limit: MAX_EXPLAIN_ROWS,
        }))
    }

    fn stored_plan(&mut self, result: IoResult) -> DetailNext {
        let (rows, _) = page(result)?;
        let explain = rows
            .iter()
            .find_map(|(_, value)| from_bytes::<WitnessExplain>(value).ok());
        let Some(explain) = explain else {
            self.step = DetailStep::Launches;
            let launches = LoadFamilyOperation::kind(self.family, JobRecordKind::Launch);
            return Ok(Next::Call(IoCall::child(launches)));
        };
        debug!(
            alternatives = explain.plan.alternatives.len(),
            rejected = explain.plan.rejected.len(),
            "Reporting the responder's own stored plan"
        );
        self.details.plan = Some(stored_estimate(&explain, self.responder));
        self.endpoint()
    }

    fn endpoint(&mut self) -> DetailNext {
        let Some(node) = self.nodes.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.details)));
        };
        self.step = DetailStep::Endpoint(node);
        Ok(Next::Call(IoCall::child(ReadInfoOperation::new(vec![
            node,
        ]))))
    }
}

impl Operation for ReportDetailsOperation {
    type Output = ReportDetails;
    type Error = JobRouteError;

    fn first(&mut self) -> DetailNext {
        let cancels = LoadFamilyOperation::kind(self.family, JobRecordKind::Cancel);
        Ok(Next::Call(IoCall::child(cancels)))
    }

    fn next(&mut self, result: IoResult) -> DetailNext {
        match self.step {
            DetailStep::Cancels => {
                let cancels = result
                    .child::<Vec<JobRecordEnvelope>, FamilyReadError>()?
                    .map_err(|error| JobRouteError::Unavailable(error.to_string()))?;
                let deleted = cancels.iter().any(|envelope| {
                    matches!(&envelope.record, JobFamilyRecord::Cancel(cancel)
                        if cancel.authority == CancelAuthority::Deletion)
                });
                if deleted {
                    self.details.deleted = true;
                    return Ok(Next::Done(std::mem::take(&mut self.details)));
                }
                self.step = DetailStep::Siblings;
                Ok(Next::Call(IoCall::Page {
                    space: FAMILY_RECORD_KEYSPACE.to_string(),
                    prefix: submission_prefix(self.family.submission_id),
                    after: None,
                    limit: MAX_SIBLING_SCAN,
                }))
            }
            DetailStep::Siblings => self.siblings(result),
            DetailStep::Deadline => {
                let [value] = values::<1>(result)?;
                let armed = value.and_then(|bytes| from_bytes::<WitnessDeadline>(&bytes).ok());
                self.details.locally_exhausted = armed.is_none();
                self.explain()
            }
            DetailStep::Explain => self.stored_plan(result),
            DetailStep::Launches => {
                let launches = result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()?;
                self.details.plan = launches
                    .ok()
                    .and_then(|records| launched_estimate(&records));
                self.endpoint()
            }
            DetailStep::Endpoint(node) => {
                let read = result.child::<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError>()?;
                if let Ok(mut documents) = read
                    && let Some(document) = documents.remove(&node)
                    && let Some(endpoint) = document.urls.s3
                {
                    self.details.output_endpoints.insert(node, endpoint);
                }
                self.endpoint()
            }
        }
    }
}

/// The plan this responder stored for the family.
fn stored_estimate(explain: &WitnessExplain, responder: Option<NodeId>) -> PlanEstimate {
    let selected = explain.plan.selected.as_ref();
    PlanEstimate {
        target: selected.map(|selection| selection.target.clone()),
        scheduler_node_id: responder,
        estimated_transfer_bytes: selected.map_or(0, |selection| selection.score.transfer_bytes),
        estimated_transfer_ms: selected
            .map_or(0, |selection| selection.score.estimated_transfer_ms),
        alternatives: explain.plan.alternatives.len() as u32,
        rejected: explain.plan.rejected.len() as u32,
        omitted: explain.plan.omitted,
        stored_at_ms: explain.stored_at_ms,
        inputs: selected
            .map(|selection| selection.inputs.clone())
            .unwrap_or_default(),
        candidates: explain.plan.candidates(),
    }
}

/// The newest launch of the family, read as its placement. It is replicated evidence, so any
/// responder can answer it even when another node planned.
fn launched_estimate(records: &[JobRecordEnvelope]) -> Option<PlanEstimate> {
    let launch = records
        .iter()
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Launch(launch) => Some(launch.as_ref()),
            _ => None,
        })
        .max_by_key(|launch| (launch.created_at_ms, launch.scheduler_seq))?;
    // Only inputs that have to move count toward the transfer estimate.
    let moved = launch
        .inputs
        .iter()
        .filter(|input| input.source_node_id.is_some());
    Some(PlanEstimate {
        target: Some(launch.target.clone()),
        scheduler_node_id: Some(launch.scheduler_node_id),
        estimated_transfer_bytes: moved.clone().map(|input| input.bytes).sum(),
        estimated_transfer_ms: moved.map(|input| input.transfer_ms).sum(),
        alternatives: 0,
        rejected: 0,
        omitted: 0,
        stored_at_ms: launch.created_at_ms,
        inputs: launch.inputs.clone(),
        candidates: Vec::new(),
    })
}

/// Records one audit page may return. The transport clamps to this before it
/// reaches the record store, so a caller cannot ask for an unbounded page.
pub const MAX_AUDIT_PAGE: usize = aruna_core::effects::MAX_RECORD_PAGE;

/// Why one audit request could not be paged. Both are caller mistakes, so they
/// map to a bad request rather than to an availability answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PagingError {
    #[error("cursor is not a record key of this log")]
    Cursor,
    #[error("page limit must be 1..={MAX_AUDIT_PAGE}")]
    Limit,
}

/// Validated paging of one audit request, built before any effect exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditPaging {
    cursor: Option<FetchCursor>,
    limit: PageLimit,
}

impl AuditPaging {
    /// `cursor` is the opaque marker of a previous page and `limit` defaults to
    /// the maximum page. Anything out of bounds is refused, never clamped.
    pub fn new(cursor: Option<Vec<u8>>, limit: Option<usize>) -> Result<Self, PagingError> {
        let cursor = cursor
            .map(|cursor| {
                JobRecordKey::from_bytes(&cursor).map_err(|_| PagingError::Cursor)?;
                FetchCursor::new(cursor).map_err(|_| PagingError::Cursor)
            })
            .transpose()?;
        let limit = match limit {
            None => PageLimit::default(),
            Some(limit) => PageLimit::try_from(limit).map_err(|_| PagingError::Limit)?,
        };
        Ok(Self { cursor, limit })
    }

    /// Reject a cursor minted for a different family or submission scope.
    pub fn validate_scope(
        &self,
        range: AuditRange,
        family: JobFamilyId,
    ) -> Result<(), PagingError> {
        let Some(cursor) = self.cursor.as_ref() else {
            return Ok(());
        };
        let key = JobRecordKey::from_bytes(cursor.as_slice()).map_err(|_| PagingError::Cursor)?;
        let valid = match range {
            AuditRange::Family => key.family == family,
            AuditRange::Submission => key.family.submission_id == family.submission_id,
        };
        valid.then_some(()).ok_or(PagingError::Cursor)
    }
}

/// Which records of one family an audit page covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditRange {
    /// The request family the alias resolves to.
    Family,
    /// Every request family of the submission, including idempotency conflicts.
    Submission,
}

/// Pages the immutable log of one alias's family. `None` means the alias names
/// no family here; a caller that did not submit the job gets `NotFound`, so the
/// surface never confirms that another user's id exists.
pub async fn family_audit(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    range: AuditRange,
    paging: AuditPaging,
) -> Option<Result<AuditPage, JobRouteError>> {
    let (projected, spec) = match family_projection(context, job_id).await {
        Ok(Some(projected)) => projected,
        Ok(None) => return None,
        Err(error) => return Some(Err(error)),
    };
    if spec.created_by != auth.user_id {
        return Some(Err(JobRouteError::NotFound));
    }
    let scope = match range {
        AuditRange::Family => AuditScope::Family(projected.family),
        AuditRange::Submission => AuditScope::Submission(projected.family.submission_id),
    };
    Some(
        drive(
            FamilyAuditOperation::new(FamilyAuditConfig {
                scope,
                cursor: paging.cursor,
                limit: paging.limit,
            }),
            context,
        )
        .await
        .map_err(|error| JobRouteError::Internal(error.to_string())),
    )
}

pub async fn audit_endpoints(
    context: &DriverContext,
    records: &[JobRecordEnvelope],
    mut endpoints: BTreeMap<NodeId, String>,
) -> BTreeMap<NodeId, String> {
    for envelope in records {
        let JobFamilyRecord::Output(output) = &envelope.record else {
            continue;
        };
        for object in output.outputs.as_slice() {
            if endpoints.contains_key(&object.node_id) {
                continue;
            }
            let read = drive(ReadInfoOperation::new(vec![object.node_id]), context).await;
            if let Ok(mut documents) = read
                && let Some(document) = documents.remove(&object.node_id)
                && let Some(endpoint) = document.urls.s3
            {
                endpoints.insert(object.node_id, endpoint);
            }
        }
    }
    endpoints
}
