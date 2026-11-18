//! Runs the job operations for the REST and MCP routes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::ops::Range;

use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::execution::harvest::HarvestJobSpec;
use aruna_core::structs::execution::job::{
    CopyJobSpec, ExportRoCrateSpec, ImportRoCrateSpec, JobId, JobRecord, StagingJobCheckpoint,
    StagingJobSpec,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::storage_purge::StoragePurgeSpec;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::{DriverContext, drive};
pub use aruna_operations::jobs::job_artifacts::{ArtifactLookup, OwnedArtifact};
use aruna_operations::jobs::job_artifacts::{ArtifactRouteOperation, RoutedArtifact};
pub use aruna_operations::jobs::job_cancel::{CancelJobOutcome, RoutedCancelOutcome};
use aruna_operations::jobs::job_cancel::{CancelOwnedOperation, RoutedCancelOperation};
use aruna_operations::jobs::job_delete::DeleteOwnedOperation;
use aruna_operations::jobs::job_reads::{JobOwnerOperation, OwnedJobOperation};
pub use aruna_operations::jobs::job_reports::{JobReportLookup, ReportQuery};
use aruna_operations::jobs::job_reports::{ReportRouteOperation, SessionReasonOperation};
use aruna_operations::jobs::job_routes::{RecordRouteOperation, StatusRouteOperation};
use aruna_operations::jobs::route::{JobRouteError, RoutedJobStatus};
use aruna_operations::jobs::runs::{ReadRunOperation, RunReadError, RunView};
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::jobs::store::list_jobs::ListJobsOperation;
use aruna_operations::jobs::store::{JobMutationError, RunDelete};
use aruna_operations::jobs::submit::{SubmitJobError, SubmitJobResult};
use aruna_operations::jobs::submit_local::{DedupLookupOperation, SubmitLocalOperation};

/// Jobs the serving node owns for this user. Listings are per origin by contract.
pub async fn list_owned_jobs(
    context: &DriverContext,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    filter: impl Fn(&JobRecord) -> bool + Send,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    drive(
        ListJobsOperation::new(user_id, cursor, limit, filter),
        context,
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn delete_owned_run(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
) -> Result<RunDelete, JobMutationError> {
    let serving = context.node_and_realm();
    let delete = DeleteOwnedOperation::new(auth, job_id, serving, unix_timestamp_millis());
    drive(delete, context).await
}

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
    Ok(read_staging_routed(context, user_id, job_id, auth_token)
        .await?
        .map(|(record, _)| record))
}

pub async fn read_staging_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<AuthToken>,
) -> Result<Option<(JobRecord, Option<StagingJobCheckpoint>)>, JobRouteError> {
    let route = RecordRouteOperation::new(
        user_id,
        job_id,
        auth_token,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(route, context).await
}

pub async fn resolve_job_owner(
    context: &DriverContext,
    job_id: JobId,
) -> Result<NodeId, JobRouteError> {
    let owner = JobOwnerOperation::new(job_id, context.node_and_realm());
    drive(owner, context).await
}

pub async fn read_job_routed(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    auth_token: Option<AuthToken>,
) -> Result<RoutedJobStatus, JobRouteError> {
    let route = StatusRouteOperation::new(
        auth,
        job_id,
        auth_token,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(route, context).await
}

/// Why a finished session stopped; a failed read reads as no reason.
pub async fn read_session_reason(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Option<String> {
    drive(SessionReasonOperation::new(user_id, job_id), context)
        .await
        .ok()
        .flatten()
}

pub async fn read_report_routed(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    query: ReportQuery,
    auth_token: Option<AuthToken>,
) -> Result<JobReportLookup, JobRouteError> {
    let route = ReportRouteOperation::new(
        auth,
        job_id,
        query,
        auth_token,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(route, context).await
}

pub async fn read_artifact_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
    range: Option<Range<u64>>,
    auth_token: Option<AuthToken>,
) -> Result<RoutedArtifact, JobRouteError> {
    let route = ArtifactRouteOperation::new(
        user_id,
        job_id,
        now_ms,
        range,
        auth_token,
        context.node_and_realm(),
    );
    drive(route, context).await
}

pub async fn cancel_owned_job(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
) -> Result<CancelJobOutcome, String> {
    let cancel = CancelOwnedOperation::new(user_id, job_id, unix_timestamp_millis());
    let report = drive(cancel, context).await.map_err(|error| error.0)?;
    for running in report.running {
        runtime.request_cancel(running);
    }
    Ok(report.outcome)
}

/// Cancels a job where it is answered for and signals the attempts running here.
pub async fn cancel_job_routed(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<AuthToken>,
) -> Result<RoutedCancelOutcome, JobRouteError> {
    let cancel = RoutedCancelOperation::new(
        user_id,
        job_id,
        auth_token,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    let report = drive(cancel, context).await?;
    for running in report.running {
        runtime.request_cancel(running);
    }
    Ok(report.outcome)
}

pub async fn lookup_job_dedup(
    context: &DriverContext,
    created_by: UserId,
    idempotency_key: &str,
    plan_digest: [u8; 32],
) -> Result<Option<SubmitJobResult>, SubmitJobError> {
    let lookup = DedupLookupOperation::new(created_by, idempotency_key, plan_digest);
    drive(lookup, context).await
}

pub async fn submit_staging_job(
    context: &DriverContext,
    spec: StagingJobSpec,
    owner_node_id: NodeId,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::staging(
        spec,
        owner_node_id,
        retention_ms,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

pub async fn submit_copy_job(
    context: &DriverContext,
    spec: CopyJobSpec,
    owner_node_id: NodeId,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::copy(
        spec,
        owner_node_id,
        retention_ms,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

pub async fn submit_harvest_job(
    context: &DriverContext,
    spec: HarvestJobSpec,
    created_by: UserId,
    realm_id: RealmId,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::harvest(
        spec,
        (created_by, realm_id),
        owner_node_id,
        idempotency_key,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

/// Queues a purge; with `files` it also deletes the linked-storage files of the versions.
pub async fn submit_purge_job(
    context: &DriverContext,
    (spec, files): (StoragePurgeSpec, bool),
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::purge(
        (spec, files),
        owner_node_id,
        (idempotency_key, retention_ms),
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
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

pub async fn read_run(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    run_id: JobId,
) -> Result<RunView, RunReadError> {
    drive(ReadRunOperation::new(realm_id, auth, run_id), context).await
}
