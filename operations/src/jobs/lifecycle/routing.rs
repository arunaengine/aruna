//! External reads answered from the family projection.
//!
//! An external job has no single owner: any node that reduced the family can
//! answer for it, and the user-facing alias stays the stable handle. Reads of
//! bytes one node produced still route to that node, but the responder is
//! chosen from the projection rather than from the alias's origin.

use aruna_core::jobs::{JobKind, JobStatusView};
use aruna_core::keyspaces::JOB_FAMILY_ALIAS_KEYSPACE;
use aruna_core::structs::{
    AuthContext, ExecutionRole, JobError, JobFamilyId, JobFamilyRecord, JobId, JobProgress,
    JobProjection, JobResultPayload, JobState, LogicalJobSpec, LogicalJobState,
    PhysicalExecutionState, WorkspaceMode,
};
use aruna_core::types::NodeId;
use aruna_core::util::unix_timestamp_millis;

use super::ids::workspace_of;
use super::witness::load_family;
use crate::driver::{DriverContext, drive};
use crate::jobs::JobRouteError;
use crate::jobs::records::keys::{alias_family, alias_prefix};
use crate::jobs::records::{
    FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation, ProjectedFamily, RecordStoreError,
};
use crate::jobs::service::RoutedJobStatus;
use crate::jobs::store::iter_prefix_page;

/// Families one alias may resolve to. Two families claiming one id is an
/// anomaly that stays visible instead of rebinding the first one.
const MAX_ALIAS_FAMILIES: usize = 8;

/// The request family one accepted alias belongs to. Ordered by key, so two
/// families claiming one alias resolve identically on every replica.
pub async fn family_of_alias(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<JobFamilyId>, JobRouteError> {
    let (rows, _) = iter_prefix_page(
        &context.storage_handle,
        JOB_FAMILY_ALIAS_KEYSPACE,
        Some(alias_prefix(job_id)),
        None,
        MAX_ALIAS_FAMILIES,
        None,
    )
    .await
    .map_err(JobRouteError::Unavailable)?;
    Ok(rows.iter().filter_map(|(key, _)| alias_family(key)).min())
}

/// The reduced family of one alias plus the sealed spec of its canonical claim.
pub async fn family_projection(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<(ProjectedFamily, LogicalJobSpec)>, JobRouteError> {
    let projected = match drive(
        ProjectFamilyOperation::new(ProjectFamilyConfig {
            family: FamilyRef::Alias(job_id),
            now_ms: unix_timestamp_millis(),
            rebuild: false,
        }),
        context,
    )
    .await
    {
        Ok(projected) => projected,
        Err(RecordStoreError::UnknownAlias) => return Ok(None),
        Err(error) => return Err(JobRouteError::Unavailable(error.to_string())),
    };
    let canonical = projected
        .projection
        .as_ref()
        .ok_or_else(|| JobRouteError::Unavailable("job family has no projection".to_string()))?
        .canonical_job_id;
    let records = load_family(context, projected.family).await;
    let spec = records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Spec(spec) if spec.job_id == canonical => Some(spec.as_ref().clone()),
        _ => None,
    });
    Ok(Some((
        projected,
        spec.ok_or_else(|| JobRouteError::Unavailable("job family spec unavailable".to_string()))?,
    )))
}

/// Answers one status read from the family. `None` means the alias names no
/// family here, so the caller keeps its ordinary routing.
pub async fn family_status(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
) -> Option<Result<RoutedJobStatus, JobRouteError>> {
    let (projected, spec) = match family_projection(context, job_id).await {
        Ok(Some(projected)) => projected,
        Ok(None) => return None,
        Err(error) => return Some(Err(error)),
    };
    // Reads stay self-scoped: another submitter's job is absent, never refused.
    if spec.created_by != auth.user_id {
        return Some(Err(JobRouteError::NotFound));
    }
    let projection = projected.projection?;
    Some(Ok(RoutedJobStatus {
        job: status_view(job_id, &projection, &spec),
        run_crate: None,
    }))
}

/// The canonical execution's node, otherwise a successful execution's node,
/// otherwise any execution's node.
pub async fn family_responder(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<NodeId>, JobRouteError> {
    let Some((projected, _)) = family_projection(context, job_id).await? else {
        return Ok(None);
    };
    let projection = projected
        .projection
        .ok_or_else(|| JobRouteError::Unavailable("job family has no projection".to_string()))?;
    Ok(projection
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
        .map(|execution| execution.executor_node_id))
}

/// The current response shape, rebuilt from immutable records. Extra fields of
/// the family model belong to the surface round, not to this routing swap.
pub(crate) fn status_view(
    job_id: JobId,
    projection: &JobProjection,
    spec: &LogicalJobSpec,
) -> JobStatusView {
    let (mode, bucket) = workspace_of(&spec.payload);
    let workspace_bucket = match mode {
        WorkspaceMode::Existing => bucket,
        WorkspaceMode::Temporary | WorkspaceMode::Kept => Some(
            aruna_core::structs::JobRecord::workspace_bucket_name(projection.canonical_job_id),
        ),
        WorkspaceMode::None => None,
    };
    let terminal = projection
        .canonical_execution_id
        .and_then(|execution_id| {
            projection
                .executions
                .iter()
                .find(|execution| execution.execution_id == execution_id)
        })
        .or_else(|| {
            projection
                .executions
                .iter()
                .filter(|execution| execution.state.is_terminal())
                .max_by_key(|execution| execution.observed_at_ms)
        });
    let updated_at_ms = projection
        .executions
        .iter()
        .filter_map(|execution| execution.observed_at_ms)
        .max()
        .unwrap_or(spec.created_at_ms);
    JobStatusView {
        job_id,
        created_by: spec.created_by,
        kind: JobKind::Execution,
        state: local_state(projection.state),
        attempts: projection.executions.len() as u32,
        cancel_requested: projection.cancel_requested,
        created_at_ms: spec.created_at_ms,
        updated_at_ms,
        finished_at_ms: local_state(projection.state)
            .is_terminal()
            .then(|| terminal.and_then(|execution| execution.observed_at_ms))
            .flatten(),
        progress: JobProgress::new("phases"),
        last_error: terminal
            .filter(|execution| execution.state == PhysicalExecutionState::Failed)
            .and_then(|execution| execution.result.as_ref())
            .and_then(|result| result.message.as_ref())
            .map(|message| JobError::permanent(message.as_str())),
        result: result_view(projection, workspace_bucket.clone()),
        workspace_bucket,
        workspace_mode: mode,
    }
}

/// Result of the canonical success or proven permanent failure. Only a success
/// supplies outputs.
fn result_view(projection: &JobProjection, bucket: Option<String>) -> Option<serde_json::Value> {
    if !matches!(
        projection.state,
        LogicalJobState::Succeeded | LogicalJobState::Failed
    ) {
        return None;
    }
    let result = projection.canonical_execution_id.and_then(|execution_id| {
        projection
            .executions
            .iter()
            .find(|execution| execution.execution_id == execution_id)
            .and_then(|execution| execution.result.as_ref())
    });
    Some(
        JobResultPayload::Execution {
            exit_code: result.and_then(|result| result.exit_code),
            workspace_bucket: bucket,
            outputs: if projection.state == LogicalJobState::Succeeded {
                projection.outputs.as_slice().to_vec()
            } else {
                Vec::new()
            },
            stdout: String::new(),
            stderr: String::new(),
            output_digest: result.and_then(|result| result.output_digest),
        }
        .to_public_json(),
    )
}

/// Only a signed permanent execution failure becomes a logical failure.
fn local_state(state: LogicalJobState) -> JobState {
    match state {
        LogicalJobState::Queued => JobState::Queued,
        LogicalJobState::Running => JobState::Running,
        LogicalJobState::Indeterminate => JobState::Indeterminate,
        LogicalJobState::Succeeded => JobState::Succeeded,
        LogicalJobState::Cancelled => JobState::Cancelled,
        LogicalJobState::Failed => JobState::Failed,
    }
}
