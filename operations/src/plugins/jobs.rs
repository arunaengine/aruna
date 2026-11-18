//! Answers a run's calls about its install's jobs from their work rows: status, outputs and
//! cancel.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::structs::execution::job::{JobId, JobPayload, RETENTION_MS, WorkspaceMode};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::{ErrorKind, Field, HostError, JobStatus, ObjectInfo, ObjectPage};
use base64::Engine;
use serde_json::Value;
use tracing::warn;
use ulid::Ulid;

use super::check::RunScope;
use super::grants::short_path;
use super::marks::is_marked;
use super::rows::{StoreError, decode, delete_rows, scan_rows};
use super::submit::{PrepareWork, PreparedWork, WORK_ACTIVE, WorkRequest};
use super::unavailable;
use super::work::{
    SendBoundary, Summary, WorkChange, WorkKind, WorkRow, WorkState, dedup_key, index_key,
    install_at, read_work, summary_of, update_work, work_scan, work_state,
};
use crate::driver::{DriverContext, drive};
use crate::jobs::service::{find_dedup_job, mint_local_job, submit_local_job};
use crate::jobs::store::JobMutationError;
use crate::jobs::store::attempt::set_cancel_requested;
use crate::jobs::store::state::read_job_record;
use crate::jobs::submit::{SubmitJobError, SubmitJobSpec};

/// Outputs of one page.
const PAGE: usize = 100;

/// The work row of job `id` of `install`; a row of an earlier install under the same id is not
/// this install's job.
pub(crate) async fn job_row(
    ctx: &DriverContext,
    install: &PluginInstall,
    id: &str,
) -> Result<WorkRow, HostError> {
    let missing = || HostError::new(ErrorKind::NotFound, format!("the install has no job {id}"));
    let work_id = Ulid::from_string(id).map_err(|_| missing())?;
    let row = drive(read_work(install.install_id, work_id), ctx).await;
    let row = row.map_err(|error| unavailable(error.0))?;
    row.filter(|row| row.account == install.account)
        .ok_or_else(missing)
}

/// What job `id` did: from its kept summary once settled, else from its job record.
pub(crate) async fn job_status(
    ctx: &DriverContext,
    (install, local): (&PluginInstall, NodeId),
    id: &str,
) -> Result<JobStatus, HostError> {
    let row = job_row(ctx, install, id).await?;
    let record = match (&row.summary, row.job) {
        (None, Some(job)) => read_job_record(ctx, job, None).await.map_err(unavailable)?,
        _ => None,
    };
    let state = work_state(&row, record.as_ref());
    let finished = record.as_ref().and_then(|record| summary_of(Some(record)));
    let summary = row.summary.clone().or(finished);
    let path = match row.kind {
        WorkKind::Copy | WorkKind::Download => row.target.as_deref(),
        WorkKind::Upload => row.source.as_deref(),
        WorkKind::Recipe => None,
    };
    let mut status = JobStatus {
        id: row.work_id.to_string(),
        kind: kind_name(row.kind).to_string(),
        path: path.map_or_else(String::new, |path| short_path(path, (install, local))),
        state: state.to_string(),
        message: None,
        bytes: None,
        version: None,
        reply_status: None,
        reply_headers: Vec::new(),
        reply_body: None,
        exit_code: None,
        report: None,
    };
    if let Some(summary) = summary {
        fill(&mut status, summary);
    } else if let Some(record) = &record {
        status.message = (record.last_error.as_ref()).map(|error| error.message.clone());
    }
    // An upload's answer is kept as soon as it came, before the work settles.
    if let (
        None,
        Some(SendBoundary::Sent {
            status: code,
            reply,
            ..
        }),
    ) = (status.reply_status, &row.boundary)
    {
        answer(&mut status, *code, reply.as_deref());
    }
    Ok(status)
}

/// One page of the objects job `id` wrote, after the output at `cursor`.
pub(crate) async fn job_outputs(
    ctx: &DriverContext,
    (install, local): (&PluginInstall, NodeId),
    id: &str,
    cursor: Option<String>,
) -> Result<ObjectPage, HostError> {
    let row = job_row(ctx, install, id).await?;
    let start = match cursor {
        Some(text) => text.parse::<usize>().map_err(|_| {
            HostError::new(
                ErrorKind::Invalid,
                format!("`{text}` is not an outputs cursor"),
            )
        })?,
        None => 0,
    };
    let outputs = (row.summary.map(|summary| summary.outputs)).unwrap_or_default();
    let mut objects = Vec::new();
    for output in (outputs.iter().skip(start).take(PAGE)).filter(|output| output.node_id == local) {
        let version = (
            output.bucket.as_str(),
            output.key.as_str(),
            output.version_id,
        );
        let marked = match is_marked(version) {
            Ok(marked) => drive(marked, ctx).await,
            Err(error) => Err(error),
        };
        objects.push(ObjectInfo {
            path: format!("{}/{}", output.bucket, output.key),
            size: output.size,
            version: output.version_id.to_string(),
            marked: marked.map_err(|error| unavailable(error.0))?,
        });
    }
    let next = start + PAGE;
    let cursor = (next < outputs.len()).then(|| next.to_string());
    Ok(ObjectPage { objects, cursor })
}

/// Asks the job of `id` to stop. A job that ended, or whose record is gone, has nothing left to
/// stop; a work whose job is not known yet cannot be stopped yet.
pub async fn cancel_job(
    ctx: &DriverContext,
    install: &PluginInstall,
    id: &str,
    now_ms: u64,
) -> Result<(), HostError> {
    let row = job_row(ctx, install, id).await?;
    let job = match (row.state, row.job) {
        (WorkState::Settled, _) => return Ok(()),
        (_, Some(job)) => job,
        (_, None) => match reconcile(ctx, install.install_id, &row).await {
            Ok(Some(job)) => job,
            Ok(None) => return Err(unavailable("the job is not submitted yet")),
            Err(error) => return Err(unavailable(error.0)),
        },
    };
    // A running job sees the request at its next lease renewal and stops.
    match set_cancel_requested(ctx, job, now_ms).await {
        Ok(_) | Err(JobMutationError::NotFound) => Ok(()),
        Err(error) => Err(unavailable(error.to_string())),
    }
}

/// Keeps the summary of every work whose job finished, so its status outlives the job record;
/// a reserved work first takes the job its dedup key names. Answers how many settled.
pub(crate) async fn settle_work(ctx: &DriverContext, now_ms: u64) -> Result<usize, StoreError> {
    let mut settled = 0;
    for (key, value) in drive(scan_rows(work_scan()), ctx).await? {
        let row: WorkRow = decode(&value)?;
        let Some(install_id) = install_at(&key) else {
            continue;
        };
        let job = match (row.state, row.job) {
            (WorkState::Dispatched, Some(job)) => job,
            (WorkState::Reserved, _) => match reconcile(ctx, install_id, &row).await? {
                Some(job) => job,
                None => continue,
            },
            _ => continue,
        };
        let record = read_job_record(ctx, job, None).await;
        let record = record.map_err(StoreError)?;
        let Some(summary) = summary_of(record.as_ref()) else {
            continue;
        };
        let change = WorkChange::Settled(Box::new(summary), now_ms);
        let changed = drive(update_work((install_id, row.work_id), change), ctx);
        if changed.await?.is_some() {
            settled += 1;
        }
    }
    Ok(settled)
}

/// Records the job of a reserved work whose submission committed but whose answer was lost; the
/// job is found by the work's dedup key. `None` while no job was submitted.
pub(crate) async fn reconcile(
    ctx: &DriverContext,
    install_id: Ulid,
    row: &WorkRow,
) -> Result<Option<JobId>, StoreError> {
    let found = find_dedup_job(ctx, row.account, &dedup_key(row.work_id)).await;
    let Some((job, _)) = found.map_err(|error| StoreError(error.to_string()))? else {
        return Ok(None);
    };
    let change = WorkChange::Dispatched(job);
    drive(update_work((install_id, row.work_id), change), ctx).await?;
    Ok(Some(job))
}

/// Asks the jobs of removed installs to cancel and deletes their rows once each job is settled
/// and kept; a row stays as long as its cancel was not accepted. Answers how many rows went.
pub(crate) async fn retire_work(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> Result<usize, StoreError> {
    let mut dropped = Vec::new();
    for (key, value) in drive(scan_rows(work_scan()), ctx).await? {
        let Some(install_id) = install_at(&key) else {
            continue;
        };
        let kept = (config.installs.iter()).any(|install| {
            install.install_id == install_id && install.state != InstallState::Removing
        });
        if kept {
            continue;
        }
        let mut row: WorkRow = decode(&value)?;
        if row.state == WorkState::Reserved {
            match reconcile(ctx, install_id, &row).await? {
                Some(job) => (row.job, row.state) = (Some(job), WorkState::Dispatched),
                None => {
                    dropped.extend([index_key(install_id, row.work_id), key]);
                    continue;
                }
            }
        }
        let Some(job) = row.job.filter(|_| row.state != WorkState::Settled) else {
            dropped.extend([index_key(install_id, row.work_id), key]);
            continue;
        };
        let record = read_job_record(ctx, job, None).await.map_err(StoreError)?;
        let Some(record) = record else {
            dropped.extend([index_key(install_id, row.work_id), key]);
            continue;
        };
        // A settled job keeps its outcome; the settle step stores it before the row goes.
        if row.cancel || record.is_settled() {
            continue;
        }
        match set_cancel_requested(ctx, job, now_ms).await {
            Ok(_) => {
                let change = WorkChange::Cancelling(job);
                drive(update_work((install_id, row.work_id), change), ctx).await?;
            }
            Err(JobMutationError::NotFound) => {
                dropped.extend([index_key(install_id, row.work_id), key]);
            }
            Err(error) => warn!(%error, "A removed install's plugin work did not accept a cancel"),
        }
    }
    let count = dropped.len() / 2;
    let dropped = (dropped.into_iter()).map(|key| (PLUGIN_WORK_KEYSPACE, key));
    drive(delete_rows(dropped.collect()), ctx).await?;
    Ok(count)
}

fn fill(status: &mut JobStatus, summary: Summary) {
    status.message = summary.message;
    status.bytes = summary.bytes;
    status.version = summary.version_id.map(|version| version.to_string());
    if let Some(code) = summary.status {
        answer(status, code, summary.reply.as_deref());
    }
    status.exit_code = summary.exit_code;
    status.report = summary.report;
}

/// The status, headers and body of a kept answer, stored as the broker keeps whole answers; a
/// reply that was too large to keep has no headers or body.
fn answer(status: &mut JobStatus, code: u16, reply: Option<&str>) {
    status.reply_status = Some(code);
    let Some(value) = reply.and_then(|reply| serde_json::from_str::<Value>(reply).ok()) else {
        return;
    };
    let headers = value.get("headers").cloned().unwrap_or(Value::Null);
    let headers: Vec<(String, String)> = serde_json::from_value(headers).unwrap_or_default();
    status.reply_headers = (headers.into_iter())
        .map(|(name, value)| Field { name, value })
        .collect();
    let body = value
        .get("body")
        .and_then(Value::as_str)
        .unwrap_or_default();
    status.reply_body = base64::engine::general_purpose::STANDARD.decode(body).ok();
}

fn kind_name(kind: WorkKind) -> &'static str {
    match kind {
        WorkKind::Copy => "copy",
        WorkKind::Download => "download",
        WorkKind::Upload => "upload",
        WorkKind::Recipe => "task",
    }
}

/// Starts or finds the work of `request` and answers its work id. The dedup key is the work id,
/// so a repeated submission with the same payload answers the same job.
pub(crate) async fn start_work(
    ctx: &DriverContext,
    scope: &RunScope,
    request: WorkRequest,
) -> Result<Ulid, HostError> {
    let now_ms = unix_timestamp_millis();
    let prepare = PrepareWork::new(scope, request, (now_ms, Ulid::generate()));
    let (account, spec) = match drive(prepare, ctx).await? {
        PreparedWork::Known(work_id) => return Ok(work_id),
        PreparedWork::Submit { account, spec } => (account, *spec),
    };
    let (install_id, work_id) = (spec.install_id, spec.work_id);
    let dedup = dedup_key(work_id);
    let job_id = mint_local_job(ctx, scope.realm_id, scope.local, Some(&dedup)).await;
    let job_id = job_id.map_err(submit_error)?;
    let submitted = SubmitJobSpec {
        payload: JobPayload::PluginWork(spec),
        created_by: account,
        owner_node_id: scope.local,
        dedup_key: Some(dedup),
        now_ms: unix_timestamp_millis(),
        retention_ms: RETENTION_MS,
        workspace_mode: WorkspaceMode::default(),
        workspace_bucket: None,
        active_cap: Some(WORK_ACTIVE),
    };
    let result = submit_local_job(ctx, submitted, job_id).await;
    let result = result.map_err(submit_error)?;
    let change = WorkChange::Dispatched(result.job_id);
    let recorded = drive(update_work((install_id, work_id), change), ctx).await;
    recorded.map_err(|error| unavailable(error.0))?;
    Ok(work_id)
}

/// The work's row stays reserved after any refusal, so the same call submits again later.
fn submit_error(error: SubmitJobError) -> HostError {
    match error {
        SubmitJobError::ActiveJobLimit { limit } => HostError::new(
            ErrorKind::Limit,
            format!("the install has {limit} unfinished work already"),
        ),
        other => unavailable(other.to_string()),
    }
}

#[cfg(test)]
#[path = "jobs_tests.rs"]
mod tests;
