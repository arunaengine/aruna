//! External reads answered from the family projection.
//!
//! An external job has no single owner: any node that reduced the family can
//! answer for it, and the user-facing alias stays the stable handle. Reads of
//! bytes one node produced still route to that node, but the responder is
//! chosen from the projection rather than from the alias's origin.

use aruna_core::jobs::{JobKind, JobStatusView};
use aruna_core::keyspaces::JOB_FAMILY_ALIAS_KEYSPACE;
use aruna_core::structs::{
    AuthContext, ExecutionRole, JobFamilyId, JobFamilyRecord, JobId, JobProgress, JobProjection,
    JobResultPayload, JobState, LogicalJobSpec, LogicalJobState, PhysicalExecutionState,
    WorkspaceMode,
};
use aruna_core::types::NodeId;
use aruna_core::util::unix_timestamp_millis;

use super::ids::workspace_of;
use super::witness::load_family;
use crate::driver::{DriverContext, drive};
use crate::jobs::JobRouteError;
use crate::jobs::records::keys::{alias_family, alias_prefix};
use crate::jobs::records::{FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation};
use crate::jobs::service::RoutedJobStatus;
use crate::jobs::store::iter_prefix_page;

/// Families one alias may resolve to. Two families claiming one id is an
/// anomaly that stays visible instead of rebinding the first one.
const MAX_ALIAS_FAMILIES: usize = 8;

/// The request family one accepted alias belongs to. Ordered by key, so two
/// families claiming one alias resolve identically on every replica.
pub async fn family_of_alias(context: &DriverContext, job_id: JobId) -> Option<JobFamilyId> {
    let (rows, _) = iter_prefix_page(
        &context.storage_handle,
        JOB_FAMILY_ALIAS_KEYSPACE,
        Some(alias_prefix(job_id)),
        None,
        MAX_ALIAS_FAMILIES,
        None,
    )
    .await
    .ok()?;
    rows.iter().filter_map(|(key, _)| alias_family(key)).min()
}

/// The reduced family of one alias plus the sealed spec of its canonical claim.
pub async fn family_projection(
    context: &DriverContext,
    job_id: JobId,
) -> Option<(JobProjection, LogicalJobSpec)> {
    let projected = drive(
        ProjectFamilyOperation::new(ProjectFamilyConfig {
            family: FamilyRef::Alias(job_id),
            now_ms: unix_timestamp_millis(),
            rebuild: false,
        }),
        context,
    )
    .await
    .ok()?;
    let projection = projected.projection?;
    let records = load_family(context, projected.family).await;
    let spec = records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Spec(spec) if spec.job_id == projection.canonical_job_id => {
            Some(spec.as_ref().clone())
        }
        _ => None,
    })?;
    Some((projection, spec))
}

/// Answers one status read from the family. `None` means the alias names no
/// family here, so the caller keeps its ordinary routing.
pub async fn family_status(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
) -> Option<Result<RoutedJobStatus, JobRouteError>> {
    let (projection, spec) = family_projection(context, job_id).await?;
    // Reads stay self-scoped: another submitter's job is absent, never refused.
    if spec.created_by != auth.user_id {
        return Some(Err(JobRouteError::NotFound));
    }
    Some(Ok(RoutedJobStatus {
        job: status_view(job_id, &projection, &spec),
        run_crate: None,
    }))
}

/// The node that can serve bytes for this family: the canonical successful
/// execution's executor, otherwise any execution's, otherwise none.
pub async fn family_responder(context: &DriverContext, job_id: JobId) -> Option<NodeId> {
    let (projection, _) = family_projection(context, job_id).await?;
    projection
        .executions
        .iter()
        .find(|execution| execution.role == ExecutionRole::Canonical)
        .or_else(|| {
            projection
                .executions
                .iter()
                .find(|execution| execution.state == PhysicalExecutionState::Succeeded)
        })
        .or_else(|| projection.executions.first())
        .map(|execution| execution.executor_node_id)
}

/// The current response shape, rebuilt from immutable records. Extra fields of
/// the family model belong to the surface round, not to this routing swap.
fn status_view(job_id: JobId, projection: &JobProjection, spec: &LogicalJobSpec) -> JobStatusView {
    let (mode, bucket) = workspace_of(&spec.payload);
    let workspace_bucket = match mode {
        WorkspaceMode::Existing => bucket,
        WorkspaceMode::Temporary | WorkspaceMode::Kept => Some(
            aruna_core::structs::JobRecord::workspace_bucket_name(projection.canonical_job_id),
        ),
        WorkspaceMode::None => None,
    };
    JobStatusView {
        job_id,
        created_by: spec.created_by,
        kind: JobKind::Execution,
        state: local_state(projection.state),
        attempts: projection.executions.len() as u32,
        cancel_requested: projection.cancel_requested,
        created_at_ms: spec.created_at_ms,
        updated_at_ms: spec.created_at_ms,
        finished_at_ms: None,
        progress: JobProgress::new("phases"),
        last_error: None,
        result: result_view(projection, workspace_bucket.clone()),
        workspace_bucket,
        workspace_mode: mode,
    }
}

/// Outputs of the canonical success only. A family without one has no result,
/// because a duplicate execution's outputs are not this job's answer.
fn result_view(projection: &JobProjection, bucket: Option<String>) -> Option<serde_json::Value> {
    if projection.state != LogicalJobState::Succeeded {
        return None;
    }
    Some(
        JobResultPayload::Execution {
            exit_code: None,
            workspace_bucket: bucket,
            outputs: projection.outputs.as_slice().to_vec(),
            stdout: String::new(),
            stderr: String::new(),
            output_digest: None,
        }
        .to_public_json(),
    )
}

/// There is deliberately no convergent failure: an unsuccessful family stays
/// indeterminate until a success, a cancellation, or a new execution appears.
fn local_state(state: LogicalJobState) -> JobState {
    match state {
        LogicalJobState::Queued => JobState::Queued,
        LogicalJobState::Running => JobState::Running,
        LogicalJobState::Indeterminate => JobState::Indeterminate,
        LogicalJobState::Succeeded => JobState::Succeeded,
        LogicalJobState::Cancelled => JobState::Cancelled,
    }
}
