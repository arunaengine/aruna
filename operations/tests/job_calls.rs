//! Runs the job operations the integration tests read and cancel jobs through.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::AttemptControl;
use aruna_core::structs::execution::job::AttemptIntent;
use aruna_core::structs::execution::job::JobError;
use aruna_core::structs::execution::job::JobProgress;
use aruna_core::structs::execution::job::JobResultPayload;
use aruna_core::structs::execution::job::LaunchRecord;
use aruna_core::structs::storage::distribution::hint::SetHint;
use aruna_core::structs::storage::storage_purge::StoragePurgeCheckpoint;
use aruna_core::types::TxnId;
use aruna_core::types::Value;
use aruna_operations::jobs::store::JobMutationError;
use aruna_operations::jobs::store::attempt_intent::AttemptCommit;
use aruna_operations::jobs::store::job_calls;
use aruna_operations::jobs::store::job_calls::CancelRequestOutcome;
use aruna_operations::jobs::store::job_calls::ClaimOutcome;
use aruna_operations::jobs::store::job_calls::ReleaseOutcome;
use aruna_operations::jobs::store::row_calls;
use std::collections::BTreeMap;
use ulid::Ulid;
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

pub async fn claim_job(
    context: &DriverContext,
    job_id: JobId,
    holder_node_id: NodeId,
    now_ms: u64,
) -> Result<ClaimOutcome, JobMutationError> {
    drive(
        job_calls::claim_job(job_id, holder_node_id, now_ms),
        context,
    )
    .await
}

pub async fn complete_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::complete_job(job_id, token, result, final_progress, now_ms),
        context,
    )
    .await
}

pub async fn fail_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(job_calls::fail_job(job_id, token, error, now_ms), context).await
}

pub async fn find_dedup_job(
    context: &DriverContext,
    created_by: UserId,
    dedup_key: &[u8],
    _txn_id: Option<TxnId>,
) -> Result<Option<JobId>, String> {
    drive(row_calls::find_dedup_job(created_by, dedup_key), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn find_dedup_plan(
    context: &DriverContext,
    created_by: UserId,
    dedup_key: &[u8],
    _txn_id: Option<TxnId>,
) -> Result<Option<(JobId, [u8; 32])>, String> {
    drive(row_calls::find_dedup_plan(created_by, dedup_key), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn insert_job(context: &DriverContext, record: &JobRecord) -> Result<(), String> {
    drive(row_calls::insert_job(record), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn list_job_entries(
    context: &DriverContext,
    job_id: JobId,
    last_key: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<(Vec<u8>, Value)>, Option<Vec<u8>>), String> {
    drive(
        row_calls::list_job_entries(job_id, last_key, limit),
        context,
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn put_crate_status(
    context: &DriverContext,
    job_id: JobId,
    status: &aruna_core::structs::execution::job::RunCrateStatus,
) -> Result<(), String> {
    drive(row_calls::put_crate_status(job_id, status), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn put_purge_checkpoint(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    checkpoint: &StoragePurgeCheckpoint,
) -> Result<(), JobMutationError> {
    drive(
        job_calls::put_purge_checkpoint(job_id, token, checkpoint),
        context,
    )
    .await
}

pub async fn read_crate_status(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<aruna_core::structs::execution::job::RunCrateStatus>, String> {
    drive(row_calls::read_crate_status(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn read_job_record(
    context: &DriverContext,
    job_id: JobId,
    _txn_id: Option<TxnId>,
) -> Result<Option<JobRecord>, String> {
    drive(row_calls::read_job_record(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn read_run_link(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<aruna_core::structs::execution::job::RunLink>, String> {
    drive(row_calls::read_run_link(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn record_attempt_intent(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    draft: AttemptIntent,
    execution_id: Option<Ulid>,
    launch: Option<&LaunchRecord>,
    now_ms: u64,
) -> Result<AttemptCommit, JobMutationError> {
    drive(
        job_calls::record_attempt_intent(
            job_id,
            token,
            draft,
            execution_id,
            launch.cloned(),
            now_ms,
        ),
        context,
    )
    .await
}

pub async fn release_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<ReleaseOutcome, JobMutationError> {
    drive(job_calls::release_job(job_id, token, now_ms), context).await
}

pub async fn reserve_output_commits(
    context: &DriverContext,
    job_id: JobId,
    destinations: &[(NodeId, String, String)],
    hints: &BTreeMap<(NodeId, String), SetHint>,
) -> Result<AttemptControl, JobMutationError> {
    drive(
        job_calls::reserve_output_commits(job_id, destinations.to_vec(), hints.clone()),
        context,
    )
    .await
}

pub async fn set_cancel_requested(
    context: &DriverContext,
    job_id: JobId,
    now_ms: u64,
) -> Result<CancelRequestOutcome, JobMutationError> {
    drive(job_calls::set_cancel_requested(job_id, now_ms), context).await
}

pub async fn transition_to_running(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::transition_to_running(job_id, token, now_ms),
        context,
    )
    .await
}
