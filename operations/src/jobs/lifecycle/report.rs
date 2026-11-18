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
    CancelAuthority, JobFamilyId, JobFamilyRecord, JobId, JobProjection, JobRecordEnvelope,
    JobRecordKey, JobRecordKind, LogicalJobSpec, LogicalJobState, OutputObject,
    PhysicalExecutionResult, PhysicalExecutionState, ProjectedExecution, SubmissionId,
};
use aruna_core::types::Key;
use std::collections::{BTreeMap, BTreeSet};
use tracing::debug;

use super::witness_arm::{WitnessDeadline, WitnessExplain, deadline_index_key};
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

fn terminal(state: PhysicalExecutionState) -> bool {
    state.is_terminal()
}

/// When the family's work started: the canonical execution's start, else the
/// newest start any execution reports.
pub(super) fn started_at(projection: &JobProjection) -> Option<u64> {
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
    pub(super) cursor: Option<FetchCursor>,
    pub(super) limit: PageLimit,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{Family, node};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::{CancelAuthority, OutputSet};

    fn projection(fixture: &Family) -> JobProjection {
        let family = fixture.family();
        JobProjection {
            submission_id: family.submission_id,
            request_digest: family.request_digest,
            canonical_job_id: fixture.spec().job_id,
            aliases: Vec::new(),
            state: LogicalJobState::Indeterminate,
            canonical_execution_id: None,
            executions: Vec::new(),
            outputs: OutputSet::try_from(Vec::new()).expect("no outputs"),
            cancel_requested: false,
        }
    }

    fn records(result: Vec<JobRecordEnvelope>) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, FamilyReadError>(result)))
    }

    // A deletion cancel ends the read at once; nothing else is read.
    #[test]
    fn deleted_family_stops() {
        let fixture = Family::new([1u8; 32]);
        let mut cancel = fixture.cancel(&fixture.spec());
        cancel.authority = CancelAuthority::Deletion;
        let deleted = fixture.sign(&fixture.holder, JobFamilyRecord::Cancel(cancel));
        let mut details =
            ReportDetailsOperation::new(fixture.family(), &projection(&fixture), None);
        assert!(matches!(details.first(), Ok(Next::Call(IoCall::Child(_)))));
        let Ok(Next::Done(found)) = details.next(records(vec![deleted])) else {
            panic!("a deleted family answers at once");
        };
        assert!(found.deleted);
    }

    // Other request digests count as conflicts; without a stored plan the newest launch places.
    #[test]
    fn reads_conflicts_and_launch() {
        let fixture = Family::new([2u8; 32]);
        let family = fixture.family();
        let mut details = ReportDetailsOperation::new(family, &projection(&fixture), None);
        details.first().expect("cancels read");
        let Ok(Next::Call(IoCall::Page { .. })) = details.next(records(Vec::new())) else {
            panic!("the siblings are paged");
        };
        let sibling = |digest: [u8; 32]| {
            let key = [family.submission_id.0.as_slice(), &digest, &[0u8; 8]].concat();
            (Key::from(key), aruna_core::types::Value::from(vec![1]))
        };
        let siblings = vec![sibling(family.request_digest), sibling([9u8; 32])];
        let Ok(Next::Call(IoCall::Page { .. })) = details.next(IoResult::Page {
            rows: siblings,
            next: None,
        }) else {
            panic!("the stored plan is paged");
        };
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        let Ok(Next::Call(IoCall::Child(_))) = details.next(empty) else {
            panic!("the launches are read");
        };
        let launch = fixture.launch(&fixture.spec(), node(4), 0);
        let launched = fixture.sign(&fixture.holder, JobFamilyRecord::Launch(Box::new(launch)));
        let Ok(Next::Done(found)) = details.next(records(vec![launched])) else {
            panic!("no output endpoint is read");
        };
        assert_eq!(found.conflicts, 1);
        assert_eq!(
            found.plan.and_then(|plan| plan.scheduler_node_id),
            Some(node(4))
        );
    }
}
