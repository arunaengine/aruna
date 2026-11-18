//! Runs the job operations the integration tests read and cancel jobs through.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::ops::Range;

use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::execution::job::{
    ArtifactRef, ExecutionSpec, ExportRoCrateSpec, ImportRoCrateSpec, JobId, JobRecord,
    WorkspaceMode,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::JOB_PRUNE_PAGE;
pub use aruna_operations::jobs::drain::JobClassBudget;
use aruna_operations::jobs::drain::{DrainError, DrainJobsOperation, JobDrainResult};
use aruna_operations::jobs::job_artifacts::{ArtifactRangeOperation, ArtifactRead};
pub use aruna_operations::jobs::job_cancel::RoutedCancelOutcome;
use aruna_operations::jobs::job_cancel::{CancelOwnedOperation, RoutedCancelOperation};
use aruna_operations::jobs::job_reads::OwnedJobOperation;
pub use aruna_operations::jobs::job_reports::JobReportLookup;
use aruna_operations::jobs::job_reports::{ReportQuery, ReportRouteOperation};
use aruna_operations::jobs::job_routes::{RecordRouteOperation, StatusRouteOperation};
use aruna_operations::jobs::prune::{JobPruneOutcome, PruneJobsOperation};
use aruna_operations::jobs::route::{JobRouteError, RoutedJobStatus};
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::jobs::submit::{SubmitJobError, SubmitJobResult};
use aruna_operations::jobs::submit_execution::{ExecutionSubmission, SubmitExecutionOperation};
use aruna_operations::jobs::submit_local::SubmitLocalOperation;

pub async fn read_owned_job(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Result<Option<JobRecord>, String> {
    drive(OwnedJobOperation::new(user_id, job_id), context)
        .await
        .map_err(|error| error.0)
}

pub async fn read_record_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<AuthToken>,
) -> Result<Option<JobRecord>, JobRouteError> {
    let serving = context.node_and_realm();
    let now_ms = unix_timestamp_millis();
    let route = RecordRouteOperation::new(user_id, job_id, auth_token, serving, now_ms);
    Ok(drive(route, context).await?.map(|(record, _)| record))
}

pub async fn read_job_routed(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    auth_token: Option<AuthToken>,
) -> Result<RoutedJobStatus, JobRouteError> {
    let serving = context.node_and_realm();
    let now_ms = unix_timestamp_millis();
    let route = StatusRouteOperation::new(auth, job_id, auth_token, serving, now_ms);
    drive(route, context).await
}

pub async fn read_report_routed(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    expected_digest: Option<[u8; 32]>,
    last_key: Option<Vec<u8>>,
    limit: usize,
    auth_token: Option<AuthToken>,
) -> Result<JobReportLookup, JobRouteError> {
    let query = ReportQuery {
        expected_digest,
        last_key,
        limit,
    };
    let serving = context.node_and_realm();
    let now_ms = unix_timestamp_millis();
    let route = ReportRouteOperation::new(auth, job_id, query, auth_token, serving, now_ms);
    drive(route, context).await
}

pub async fn cancel_owned_job(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
) -> Result<(), String> {
    let cancel = CancelOwnedOperation::new(user_id, job_id, unix_timestamp_millis());
    let report = drive(cancel, context).await.map_err(|error| error.0)?;
    for running in report.running {
        runtime.request_cancel(running);
    }
    Ok(())
}

pub async fn cancel_job_routed(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<AuthToken>,
) -> Result<RoutedCancelOutcome, JobRouteError> {
    let serving = context.node_and_realm();
    let now_ms = unix_timestamp_millis();
    let cancel = RoutedCancelOperation::new(user_id, job_id, auth_token, serving, now_ms);
    let report = drive(cancel, context).await?;
    for running in report.running {
        runtime.request_cancel(running);
    }
    Ok(report.outcome)
}

pub async fn drain_job_batch(
    context: &DriverContext,
    owner: NodeId,
    budget: JobClassBudget,
    _reconciler: Option<()>,
) -> Result<JobDrainResult, DrainError> {
    let drain = DrainJobsOperation::new(owner, budget, unix_timestamp_millis());
    drive(drain, context).await
}

/// One prune pass with nothing kept back by a payload handler.
pub async fn prune_job_batch(context: &DriverContext) -> Result<JobPruneOutcome, String> {
    let prune = PruneJobsOperation::new(unix_timestamp_millis(), JOB_PRUNE_PAGE, BTreeSet::new());
    drive(prune, context).await.map_err(|error| error.0)
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_execution_job(
    context: &DriverContext,
    spec: ExecutionSpec,
    created_by: UserId,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
    workspace_mode: WorkspaceMode,
    workspace_bucket: Option<String>,
    retention_ms: u64,
    active_cap: Option<u32>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submission = ExecutionSubmission {
        spec,
        created_by,
        owner_node_id,
        idempotency_key,
        workspace_mode,
        workspace_bucket,
        retention_ms,
        active_cap,
        serving: context.node_and_realm(),
        now_ms: unix_timestamp_millis(),
    };
    drive(SubmitExecutionOperation::new(submission), context).await
}

pub async fn submit_rocrate_import(
    context: &DriverContext,
    spec: ImportRoCrateSpec,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::rocrate_import(
        spec,
        owner_node_id,
        idempotency_key,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

pub async fn submit_export_job(
    context: &DriverContext,
    spec: ExportRoCrateSpec,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::rocrate_export(
        spec,
        owner_node_id,
        idempotency_key,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

pub async fn read_artifact_range(
    context: &DriverContext,
    artifact: &ArtifactRef,
    range: Range<u64>,
) -> Result<ArtifactRead, String> {
    drive(ArtifactRangeOperation::new(artifact, range), context)
        .await
        .map_err(|error| error.0)
}
