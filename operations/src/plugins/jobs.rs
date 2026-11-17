//! Answers a run's calls about its install's jobs from their work rows: status, outputs and
//! cancel.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::plugin_installs::PluginInstall;
use aruna_plugins::broker::{ErrorKind, Field, HostError, JobStatus, ObjectInfo, ObjectPage};
use base64::Engine;
use serde_json::Value;
use ulid::Ulid;

use super::broker::short_path;
use super::check::unavailable;
use super::marks::is_marked;
use super::work::{
    SendBoundary, Summary, WorkKind, WorkRow, WorkState, read_work, reconcile, summary_of,
    work_state,
};
use crate::driver::DriverContext;
use crate::jobs::store::JobMutationError;
use crate::jobs::store::attempt::set_cancel_requested;
use crate::jobs::store::state::read_job_record;

/// Outputs of one page.
const PAGE: usize = 100;

/// The work row of job `id` of `install`; a row of an earlier install under the same id is not
/// this install's job.
pub async fn job_row(
    ctx: &DriverContext,
    install: &PluginInstall,
    id: &str,
) -> Result<WorkRow, HostError> {
    let missing = || HostError::new(ErrorKind::NotFound, format!("the install has no job {id}"));
    let work_id = Ulid::from_string(id).map_err(|_| missing())?;
    let row = read_work(ctx, install.install_id, work_id).await;
    let row = row.map_err(|error| unavailable(error.0))?;
    row.filter(|row| row.account == install.account)
        .ok_or_else(missing)
}

/// What job `id` did: from its kept summary once settled, else from its job record.
pub async fn job_status(
    ctx: &DriverContext,
    (install, local): (&PluginInstall, NodeId),
    id: &str,
) -> Result<JobStatus, HostError> {
    let row = job_row(ctx, install, id).await?;
    let record = match (&row.summary, row.job) {
        (None, Some(job)) => read_job_record(&ctx.storage_handle, job, None)
            .await
            .map_err(unavailable)?,
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
pub async fn job_outputs(
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
        let marked = is_marked(ctx, version).await;
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
    match set_cancel_requested(&ctx.storage_handle, job, now_ms).await {
        Ok(_) | Err(JobMutationError::NotFound) => Ok(()),
        Err(error) => Err(unavailable(error.to_string())),
    }
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

#[cfg(test)]
#[path = "jobs_tests.rs"]
mod tests;
