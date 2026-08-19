//! What the external surfaces report about one request family.
//!
//! Everything here is derived from the immutable records this responder holds,
//! plus two explicitly responder-local diagnostics: the plan this node sealed
//! when it was a witness, and whether it still has a retry armed. Both are kept
//! outside the replicated projection digest, so a client can tell a local view
//! apart from realm-wide truth.

use aruna_core::compute::ExecutionTargetId;
use aruna_core::effects::{FetchCursor, PageLimit};
use aruna_core::jobs::JobStatusView;
use aruna_core::keyspaces::{
    JOB_FAMILY_RECORD_KEYSPACE, JOB_PLAN_EXPLAIN_KEYSPACE, JOB_WITNESS_DEADLINE_KEYSPACE,
};
use aruna_core::structs::{
    AuthContext, ExecutionRole, JobFamilyId, JobId, JobProjection, LogicalJobSpec, LogicalJobState,
    OutputObject, PhysicalExecutionResult, PhysicalExecutionState, SubmissionId,
};
use aruna_core::types::{Key, NodeId};
use tracing::debug;

use super::routing::{family_projection, status_view};
use super::witness::WitnessExplain;
use crate::driver::{DriverContext, drive};
use crate::jobs::JobRouteError;
use crate::jobs::records::keys::submission_prefix;
use crate::jobs::records::rows::from_bytes;
use crate::jobs::records::{AuditPage, AuditScope, FamilyAuditConfig, FamilyAuditOperation};
use crate::jobs::store::iter_prefix_page;

/// Sibling families of one submission a conflict count may scan.
const MAX_SIBLING_SCAN: usize = 256;
/// Explain rows one report reads; only this node ever writes them.
const MAX_EXPLAIN_ROWS: usize = 4;

/// The placement the responder's own witness round sealed. It explains one
/// launch this node published and is never another node's decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanEstimate {
    pub target: Option<ExecutionTargetId>,
    pub estimated_transfer_bytes: u64,
    pub estimated_transfer_ms: u64,
    pub alternatives: u32,
    pub rejected: u32,
    /// Rejections the bound dropped, so truncation is never read as agreement.
    pub omitted: u32,
    pub sealed_at_ms: u64,
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
    pub duplicate_successes: u32,
    pub outputs: Vec<OutputObject>,
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
    let projection = projected.projection.clone()?;
    let family = JobFamilyId {
        submission_id: projection.submission_id,
        request_digest: projection.request_digest,
    };
    let digest = match projection.digest() {
        Ok(digest) => digest,
        Err(error) => return Some(Err(JobRouteError::Internal(error.to_string()))),
    };
    let exhausted = locally_exhausted(context, &projection, family).await;
    Some(Ok(FamilyReport {
        job: status_view(job_id, &projection, &spec),
        submission_id: projection.submission_id,
        request_digest: projection.request_digest,
        canonical_job_id: projection.canonical_job_id,
        aliases: projection.aliases.clone(),
        conflicts: sibling_families(context, family).await,
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
        duplicate_successes: projection
            .executions
            .iter()
            .filter(|execution| execution.role == ExecutionRole::DuplicateSuccess)
            .count() as u32,
        outputs: projection.outputs.as_slice().to_vec(),
        revision: projected.revision,
        digest,
        cancel_requested: projection.cancel_requested,
        responder: context.net_handle.as_ref().map(|net| net.node_id()),
        partial: projected.truncated,
        locally_exhausted: exhausted,
        plan: plan_estimate(context, family).await,
        spec,
    }))
}

/// Distinct other request families under the same submission: the idempotency
/// conflicts a partition may have accepted elsewhere.
async fn sibling_families(context: &DriverContext, family: JobFamilyId) -> u32 {
    let Ok((rows, _)) = iter_prefix_page(
        &context.storage_handle,
        JOB_FAMILY_RECORD_KEYSPACE,
        Some(submission_prefix(family.submission_id)),
        None,
        MAX_SIBLING_SCAN,
        None,
    )
    .await
    else {
        return 0;
    };
    let mut digests: std::collections::BTreeSet<[u8; 32]> = std::collections::BTreeSet::new();
    for (key, _) in &rows {
        if let Some(digest) = key
            .get(32..64)
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            && digest != family.request_digest
        {
            digests.insert(digest);
        }
    }
    digests.len() as u32
}

/// True when every known execution is terminal without success and this node
/// has no witness deadline armed for the family.
async fn locally_exhausted(
    context: &DriverContext,
    projection: &JobProjection,
    family: JobFamilyId,
) -> bool {
    if projection.state != LogicalJobState::Indeterminate || projection.executions.is_empty() {
        return false;
    }
    if !projection
        .executions
        .iter()
        .all(|execution| terminal(execution.state))
    {
        return false;
    }
    let Ok((rows, _)) = iter_prefix_page(
        &context.storage_handle,
        JOB_WITNESS_DEADLINE_KEYSPACE,
        Some(Key::from(family.to_bytes().as_slice())),
        None,
        1,
        None,
    )
    .await
    else {
        return false;
    };
    rows.is_empty()
}

fn terminal(state: PhysicalExecutionState) -> bool {
    state.is_terminal()
}

/// The plan this responder sealed for the family, when it planned one at all.
async fn plan_estimate(context: &DriverContext, family: JobFamilyId) -> Option<PlanEstimate> {
    let (rows, _) = iter_prefix_page(
        &context.storage_handle,
        JOB_PLAN_EXPLAIN_KEYSPACE,
        Some(Key::from(family.to_bytes().as_slice())),
        None,
        MAX_EXPLAIN_ROWS,
        None,
    )
    .await
    .ok()?;
    let explain: WitnessExplain = rows
        .iter()
        .find_map(|(_, value)| from_bytes::<WitnessExplain>(value).ok())?;
    debug!(
        alternatives = explain.plan.alternatives.len(),
        rejected = explain.plan.rejected.len(),
        "Reporting the responder's own sealed plan"
    );
    let selected = explain.plan.selected.as_ref();
    Some(PlanEstimate {
        target: selected.map(|selection| selection.target.clone()),
        estimated_transfer_bytes: selected.map_or(0, |selection| selection.score.transfer_bytes),
        estimated_transfer_ms: selected
            .map_or(0, |selection| selection.score.estimated_transfer_ms),
        alternatives: explain.plan.alternatives.len() as u32,
        rejected: explain.plan.rejected.len() as u32,
        omitted: explain.plan.omitted,
        sealed_at_ms: explain.sealed_at_ms,
    })
}

/// Records one audit page may return. The transport clamps to this before it
/// reaches the record store, so a caller cannot ask for an unbounded page.
pub const MAX_AUDIT_PAGE: usize = aruna_core::effects::MAX_JOB_RECORD_PAGE;

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
            .map(FetchCursor::new)
            .transpose()
            .map_err(|_| PagingError::Cursor)?;
        let limit = match limit {
            None => PageLimit::default(),
            Some(limit) => PageLimit::try_from(limit).map_err(|_| PagingError::Limit)?,
        };
        Ok(Self { cursor, limit })
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
