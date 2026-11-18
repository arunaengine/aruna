//! Collects the lifecycle test modules so they compile as one test tree.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod admission;
mod admission_race;
mod cancel;
mod capacity;
mod family_reads;
mod launch_admit;
mod refusals;
mod report;
mod run_link;
mod scheduling;
mod terminal;
mod uncertain_commit;

use aruna_core::UserId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::execution::job::{ExecutionSpec, JobId, JobRecord, WorkspaceMode};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::admit_here::{AcceptedSubmission, placement_failure};
use crate::jobs::lifecycle::family_reads::{
    FamilyLogOperation, FamilyReportOperation, FamilyStatusOperation, SessionJobOperation,
};
use crate::jobs::lifecycle::ids::SubmissionScope;
use crate::jobs::lifecycle::ingress::ingress_node;
use crate::jobs::lifecycle::report::{AuditPaging, AuditRange, FamilyReport};
use crate::jobs::lifecycle::submit_external::{ExternalSubmission, SubmitExternalOperation};
use crate::jobs::records::audit::AuditPage;
use crate::jobs::route::{JobRouteError, RoutedJobStatus};
use crate::jobs::submit::SubmitJobError;

pub(super) async fn family_report(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
) -> Option<Result<FamilyReport, JobRouteError>> {
    let node = context.node_and_realm();
    let read = FamilyReportOperation::new(auth, job_id, node, unix_timestamp_millis());
    drive(read, context).await.transpose()
}

pub(super) async fn family_status(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
) -> Option<Result<RoutedJobStatus, JobRouteError>> {
    let read = FamilyStatusOperation::new(auth, job_id, unix_timestamp_millis());
    drive(read, context).await.transpose()
}

pub(super) async fn session_job(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Result<(JobRecord, Option<JobId>), JobRouteError> {
    drive(
        SessionJobOperation::new(user_id, job_id, unix_timestamp_millis()),
        context,
    )
    .await
}

pub(super) async fn family_audit(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    range: AuditRange,
    paging: AuditPaging,
) -> Option<Result<AuditPage, JobRouteError>> {
    let read = FamilyLogOperation::new(auth, job_id, (range, paging), unix_timestamp_millis());
    drive(read, context).await.transpose()
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn submit_external_job(
    context: &DriverContext,
    spec: ExecutionSpec,
    created_by: UserId,
    idempotency_key: Option<String>,
    workspace_mode: WorkspaceMode,
    workspace_bucket: Option<String>,
    retention_ms: u64,
    auth_token: Option<AuthToken>,
) -> Result<AcceptedSubmission, SubmitJobError> {
    let scope = match idempotency_key {
        Some(key) => SubmissionScope::Keyed(key),
        None => SubmissionScope::Unkeyed(Ulid::generate()),
    };
    let submission = ExternalSubmission {
        spec,
        created_by,
        scope,
        workspace_mode,
        workspace_bucket,
        retention_ms,
        auth_token,
    };
    let submit = SubmitExternalOperation::new(submission, ingress_node(context));
    drive(submit, context).await.map_err(placement_failure)
}
