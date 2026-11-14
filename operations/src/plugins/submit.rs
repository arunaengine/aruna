//! Starts native work for a run on the receiver: the per call checks, the work row that names it
//! across calls, and the job, submitted only while the row's dispatch is unresolved.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{
    JobPayload, PluginWorkAction, PluginWorkSpec, RETENTION_MS, WorkspaceMode,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::{ErrorKind, HostError};
use aruna_plugins::permission::Permission;
use ulid::Ulid;

use super::broker::local_object;
use super::check::{RunScope, authorize_call, check_call, matching, unavailable};
use super::work::{
    Identity, WorkError, WorkKind, WorkRow, WorkState, dedup_key, dispatched, reserve,
};
use crate::driver::DriverContext;
use crate::jobs::service::{mint_local_job, submit_local_job};
use crate::jobs::submit::{SubmitJobError, SubmitJobSpec};

/// Unfinished in-process work one install may have at a time.
pub const WORK_ACTIVE: u32 = 8;

/// A copy a run asks for: object paths below the group's data on this node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyRequest {
    pub source: String,
    pub source_version: Option<Ulid>,
    pub target: String,
    /// The author's key, which names the work across runs.
    pub key: Option<String>,
    pub ordinal: u32,
}

/// Starts or finds the copy of `request` and answers its work id. A paused install starts nothing.
pub async fn start_copy(
    ctx: &DriverContext,
    scope: &RunScope,
    request: CopyRequest,
) -> Result<Ulid, HostError> {
    let copy = Permission::ObjectsCopy;
    let install = check_call(ctx, scope, Some((copy, &request.source))).await?;
    let patterns = matching(&install, copy, &request.target)?;
    authorize_call(ctx, &install, copy, &request.target, &patterns).await?;
    if install.paused {
        return Err(HostError::new(ErrorKind::Denied, "the install is paused"));
    }
    let place = (scope.realm_id, scope.local);
    local_object(ctx, place, &install, &request.source).await?;
    local_object(ctx, place, &install, &request.target).await?;
    let identity = match &request.key {
        Some(key) => Identity::Key(key.clone()),
        None => Identity::Call {
            run_id: scope.run.run_id,
            ordinal: request.ordinal,
        },
    };
    let version = request.source_version.map(|version| version.to_bytes());
    let parts: [&[u8]; 4] = [
        b"copy",
        request.source.as_bytes(),
        version.as_ref().map_or(&[], |bytes| &bytes[..]),
        request.target.as_bytes(),
    ];
    let now_ms = unix_timestamp_millis();
    let mut proposed = WorkRow::reserved(&install, WorkKind::Copy, scope.run.run_id, now_ms);
    proposed.digest = digest(&parts);
    proposed.version_id = Some(Ulid::generate());
    let (row, _) = match reserve(ctx, (&install, &identity), proposed).await {
        Ok(found) => found,
        Err(WorkError::Conflict) => {
            let message = "the key names a copy with other arguments";
            return Err(HostError::new(ErrorKind::Conflict, message));
        }
        Err(WorkError::Store(error)) => return Err(unavailable(error.0)),
    };
    if row.state != WorkState::Reserved {
        return Ok(row.work_id);
    }
    let action = PluginWorkAction::Copy {
        source: request.source,
        source_version: request.source_version,
        target: request.target,
        version_id: row.version_id.unwrap_or(row.work_id),
    };
    let spec = PluginWorkSpec {
        realm_id: scope.realm_id,
        install_id: install.install_id,
        work_id: row.work_id,
        group_id: install.group_id,
        action,
    };
    submit(ctx, scope, (&install.account, spec)).await
}

/// Submits the job of a reserved work and records it. The dedup key is the work id, so a
/// repeated submission with the same payload answers the same job.
async fn submit(
    ctx: &DriverContext,
    scope: &RunScope,
    (account, spec): (&aruna_core::UserId, PluginWorkSpec),
) -> Result<Ulid, HostError> {
    let (install_id, work_id) = (spec.install_id, spec.work_id);
    let dedup = dedup_key(work_id);
    let job_id = mint_local_job(ctx, scope.realm_id, scope.local, Some(&dedup)).await;
    let job_id = job_id.map_err(submit_error)?;
    let submitted = SubmitJobSpec {
        payload: JobPayload::PluginWork(spec),
        created_by: *account,
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
    let recorded = dispatched(ctx, (install_id, work_id), result.job_id).await;
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

/// BLAKE3 of length-prefixed parts, so no two argument lists share bytes.
fn digest(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

#[cfg(test)]
#[path = "submit_tests.rs"]
mod tests;
