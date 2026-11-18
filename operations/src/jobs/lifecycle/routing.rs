//! Answers external job reads from the family projection, with the alias as the stable handle.
//! Any node that reduced the family can answer; byte reads still route to the producing node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::jobs::{JobKind, JobStatusView};
use aruna_core::structs::execution::job::{
    JobError, JobId, JobProgress, JobProjection, JobResultPayload, JobState, LogicalJobSpec,
    LogicalJobState, PhysicalExecutionState, ResultMessage, WorkspaceMode,
};

use super::ids::workspace_of;
use crate::jobs::records::project::ProjectedFamily;
use crate::jobs::route::JobRouteError;

/// Why a partial projection answers nothing: it reduced only part of its
/// family, so its state, executions, and outputs are unknown, never absent.
const TRUNCATED: &str = "job family projection is truncated";

/// Whether a projection may be decided on. An audit read may inspect a
/// truncated one; a status answer or a routing choice may not.
pub(crate) fn decidable(projected: &ProjectedFamily) -> Result<(), JobRouteError> {
    match projected.truncated {
        true => Err(JobRouteError::Unavailable(TRUNCATED.to_string())),
        false => Ok(()),
    }
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
        locally_exhausted: false,
        session_runtime: spec.payload.session_runtime(),
        awaiting_keys: Vec::new(),
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
            stdout: tail_text(result.and_then(|result| result.stdout.as_ref())),
            stderr: tail_text(result.and_then(|result| result.stderr.as_ref())),
            output_digest: result.and_then(|result| result.output_digest),
        }
        .to_public_json(),
    )
}

/// A missing tail reads as an empty stream, which is what a run with no output
/// produced anyway.
fn tail_text(tail: Option<&ResultMessage>) -> String {
    tail.map(|tail| tail.as_str().to_string())
        .unwrap_or_default()
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

#[cfg(test)]
mod pure_tests {
    use aruna_core::structs::execution::job::{JobFamilyId, SubmissionId};

    use super::*;

    fn projected(truncated: bool) -> ProjectedFamily {
        ProjectedFamily {
            family: JobFamilyId {
                submission_id: SubmissionId([1u8; 32]),
                request_digest: [2u8; 32],
            },
            revision: 0,
            projection: None,
            truncated,
            cached: false,
        }
    }

    #[test]
    fn partial_is_unavailable() {
        // A truncated projection reduced only part of its family, so a status
        // answer or a routing choice must not be decided from it.
        assert!(decidable(&projected(false)).is_ok());
        assert_eq!(
            decidable(&projected(true)),
            Err(JobRouteError::Unavailable(TRUNCATED.to_string()))
        );
    }
}
