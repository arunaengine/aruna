//! Collects the lifecycle test modules so they compile as one test tree.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod admission;
mod admission_race;
mod cancel;
mod capacity;
mod family_reads;
mod launch_admit;
mod prune_index;
mod pruning;
mod refusals;
mod report;
mod run_link;
mod scheduling;
mod terminal;
mod uncertain_commit;
mod window;

use aruna_core::effects::{LaunchFrame, ReceiptFrame};
use aruna_core::events::LaunchDecline;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::execution::job::family::EffectiveResources;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::input::ExecutionSpec;
use aruna_core::structs::execution::job::local::{JobRecord, WorkspaceMode};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::user::id::UserId;
use ulid::Ulid;

use crate::jobs::lifecycle::admit::here::{AcceptedSubmission, placement_failure};
use crate::jobs::lifecycle::cancel::{CancelFamilyOperation, PublishDeletionOperation};
use crate::jobs::lifecycle::family_reads::{
    FamilyLogOperation, FamilyReportOperation, FamilyStatusOperation, SessionJobOperation,
};
use crate::jobs::lifecycle::launch_admit::{
    AcceptLaunchOperation, AdmitLaunchOperation, LaunchAnswer,
};
use crate::jobs::lifecycle::report::{AuditPaging, AuditRange, FamilyReport};
use crate::jobs::lifecycle::submit_external::{ExternalSubmission, SubmitExternalOperation};
use crate::jobs::node_inputs::{ingress_node, launch_node};
use crate::jobs::records::audit::AuditPage;
use crate::jobs::route::{JobRouteError, RoutedJobStatus};
use aruna_core::job_request::{SubmissionScope, SubmitJobError};
use aruna_runner::driver::{DriverContext, drive};

pub(super) fn resources() -> EffectiveResources {
    EffectiveResources {
        cpu_cores: 1,
        ram_bytes: 1024,
        disk_bytes: 2048,
        max_walltime_ms: 60_000,
        preemptible: false,
    }
}

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

/// Submits `spec` for the test user under the idempotency key `key`, without a workspace.
pub(super) async fn submit_external_job(
    context: &DriverContext,
    spec: ExecutionSpec,
    key: &str,
    auth_token: Option<AuthToken>,
) -> Result<AcceptedSubmission, SubmitJobError> {
    let submission = ExternalSubmission {
        spec,
        created_by: crate::tests::records::user(),
        scope: SubmissionScope::Keyed(key.to_string()),
        workspace_mode: WorkspaceMode::None,
        workspace_bucket: None,
        retention_ms: 60_000,
        auth_token,
    };
    let submit = SubmitExternalOperation::new(
        submission,
        ingress_node(
            context.node_and_realm(),
            context.fresh_heartbeats(),
            unix_timestamp_millis(),
        ),
    );
    drive(submit, context).await.map_err(placement_failure)
}

pub(super) async fn cancel_family(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    token: Option<AuthToken>,
) -> Option<Result<(), JobRouteError>> {
    let serving = context.node_and_realm();
    let cancel = CancelFamilyOperation::new(auth, job_id, token, serving, unix_timestamp_millis());
    drive(cancel, context).await.transpose()
}

pub(super) async fn publish_deletion(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
) -> Result<(), JobRouteError> {
    let serving = context.node_and_realm();
    let deletion = PublishDeletionOperation::new(auth, job_id, serving, unix_timestamp_millis());
    drive(deletion, context).await
}

pub(super) async fn admit_launch(
    context: &DriverContext,
    launch: LaunchFrame,
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    let (now_ms, execution_id) = (unix_timestamp_millis(), Ulid::generate());
    let limits = context.compute_limits();
    let node = launch_node(
        context.node_and_realm(),
        &limits,
        &launch,
        now_ms,
        execution_id,
    )?;
    drive(AdmitLaunchOperation::new(launch, node), context)
        .await
        .ok()
}

/// Performs what an answered offer owes, as the launch admission does.
pub(super) async fn settle(
    context: &DriverContext,
    answer: LaunchAnswer,
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    let serving = context.node_and_realm();
    let accept = AcceptLaunchOperation::new(answer, serving, unix_timestamp_millis());
    drive(accept, context).await.ok()
}
