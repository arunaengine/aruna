//! Runs the job operations the integration tests read and cancel jobs through.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::execution::job::{JobId, JobRecord};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::{DriverContext, drive};
pub use aruna_operations::jobs::job_cancel::RoutedCancelOutcome;
use aruna_operations::jobs::job_cancel::{CancelOwnedOperation, RoutedCancelOperation};
use aruna_operations::jobs::job_reads::OwnedJobOperation;
pub use aruna_operations::jobs::job_reports::JobReportLookup;
use aruna_operations::jobs::job_reports::{ReportQuery, ReportRouteOperation};
use aruna_operations::jobs::job_routes::{RecordRouteOperation, StatusRouteOperation};
use aruna_operations::jobs::route::{JobRouteError, RoutedJobStatus};
use aruna_operations::jobs::runtime::JobsRuntime;

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
