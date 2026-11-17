//! Starts native work for a run on the receiver: the per call checks, the work row that names it
//! across calls, and the job, submitted only while the row's dispatch is unresolved.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::job::{
    JobPayload, PluginWorkAction, PluginWorkSpec, RETENTION_MS, UploadForm, WorkspaceMode,
};
use aruna_core::structs::identity::auth::Permission as Access;
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::{ErrorKind, HostError};
use aruna_plugins::permission::Permission;
use ulid::Ulid;

use super::broker::local_object;
use super::check::{
    RunScope, authorize_access, authorize_call, check_call, copy_grants, matching, unavailable,
};
use super::grants::InstallScope;
use super::broker::unmarked;
use super::rows::encode;
use super::work::{
    Identity, Reserve, RequestContext, WorkChange, WorkError, WorkKind, WorkRow, WorkState,
    dedup_key, reserve, update_work,
};
use crate::driver::{DriverContext, drive};
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
    /// The call's ordinal in its run; with the run it names the work.
    pub ordinal: u32,
}

/// Starts or finds the copy of `request` and answers its work id. A paused install starts nothing.
pub async fn start_copy(
    ctx: &DriverContext,
    scope: &RunScope,
    request: CopyRequest,
) -> Result<Ulid, HostError> {
    let install = check_call(ctx, scope, None).await?;
    copy_grants(ctx, &install, (&request.source, &request.target)).await?;
    if install.paused {
        return Err(HostError::new(ErrorKind::Denied, "the install is paused"));
    }
    let place = (scope.realm_id, scope.local);
    let (bucket, key) = local_object(ctx, place, &install, &request.source).await?;
    // Every attempt checks the version it reads again; a source that is missing now fails there.
    unmarked(ctx, (&bucket, &key), request.source_version).await?;
    local_object(ctx, place, &install, &request.target).await?;
    let identity = Identity::Call {
        run_id: scope.run.run_id,
        ordinal: request.ordinal,
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
    proposed.source = Some(request.source.clone());
    proposed.target = Some(request.target.clone());
    proposed.version_id = Some(Ulid::generate());
    let (row, _) = match reserved(ctx, (&install, &identity), proposed, None).await {
        Ok(found) => found,
        Err(WorkError::Conflict) => {
            let message = "this call started a copy with other arguments";
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

/// A download a run asks for: the request resolved when the call came, into an object path below
/// the group's data on this node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DownloadRequest {
    pub context: RequestContext,
    pub target: String,
    pub checksums: Vec<(ChecksumAlgorithm, Vec<u8>)>,
    pub max_bytes: Option<u64>,
    /// Whether the connection injects a secret; the version the download writes is marked then.
    pub secret: bool,
    /// The call's ordinal in its run; with the run it names the work.
    pub ordinal: u32,
}

/// Starts or finds the download of `request` and answers its work id. It needs the connection's
/// grant and the download grant on the target; a paused install starts nothing.
pub async fn start_download(
    ctx: &DriverContext,
    scope: &RunScope,
    request: DownloadRequest,
) -> Result<Ulid, HostError> {
    let install = check_call(ctx, scope, None).await?;
    connection_granted(ctx, (scope, &install), &request.context.connection).await?;
    let download = Permission::TransfersDownload;
    let patterns = matching(&install, download, &request.target)?;
    authorize_call(ctx, &install, download, &request.target, &patterns).await?;
    if install.paused {
        return Err(HostError::new(ErrorKind::Denied, "the install is paused"));
    }
    let place = (scope.realm_id, scope.local);
    let (bucket, key) = local_object(ctx, place, &install, &request.target).await?;
    let identity = Identity::Call {
        run_id: scope.run.run_id,
        ordinal: request.ordinal,
    };
    let stable = (
        &request.context,
        &request.target,
        &request.checksums,
        request.max_bytes,
    );
    let stable = encode(&stable).map_err(|error| unavailable(error.0))?;
    let now_ms = unix_timestamp_millis();
    let mut proposed = WorkRow::reserved(&install, WorkKind::Download, scope.run.run_id, now_ms);
    proposed.digest = digest(&[b"download", &stable]);
    proposed.target = Some(request.target.clone());
    proposed.context = Some(request.context);
    proposed.version_id = Some(Ulid::generate());
    let marked = request.secret.then_some((bucket.as_str(), key.as_str()));
    let (row, _) = match reserved(ctx, (&install, &identity), proposed, marked).await {
        Ok(found) => found,
        Err(WorkError::Conflict) => {
            let message = "this call started a download with other arguments";
            return Err(HostError::new(ErrorKind::Conflict, message));
        }
        Err(WorkError::Store(error)) => return Err(unavailable(error.0)),
    };
    if row.state != WorkState::Reserved {
        return Ok(row.work_id);
    }
    let action = PluginWorkAction::Download {
        target: request.target,
        version_id: row.version_id.unwrap_or(row.work_id),
        checksums: request.checksums,
        max_bytes: request.max_bytes,
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

/// An upload a run asks for: the request resolved when the call came, and the object it sends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadRequest {
    pub context: RequestContext,
    pub source: String,
    pub version: Option<Ulid>,
    pub form: Option<UploadForm>,
    /// The call's ordinal in its run; with the run it names the work.
    pub ordinal: u32,
}

/// Starts or finds the upload of `request` and answers its work id. It needs the connection's
/// grant and read on the source, whose version must not be marked; a paused install starts nothing.
pub async fn start_upload(
    ctx: &DriverContext,
    scope: &RunScope,
    request: UploadRequest,
) -> Result<Ulid, HostError> {
    let install = check_call(ctx, scope, None).await?;
    connection_granted(ctx, (scope, &install), &request.context.connection).await?;
    let upload = Permission::TransfersUpload;
    let patterns = matching(&install, upload, &request.source)?;
    let read = (upload, Access::READ);
    authorize_access(ctx, &install, read, &request.source, &patterns).await?;
    if install.paused {
        return Err(HostError::new(ErrorKind::Denied, "the install is paused"));
    }
    let place = (scope.realm_id, scope.local);
    let (bucket, key) = local_object(ctx, place, &install, &request.source).await?;
    // Every attempt checks the version it sends again; one marked meanwhile fails there.
    if unmarked(ctx, (&bucket, &key), request.version)
        .await?
        .is_none()
    {
        let message = format!("no object at {}", request.source);
        return Err(HostError::new(ErrorKind::NotFound, message));
    }
    let identity = Identity::Call {
        run_id: scope.run.run_id,
        ordinal: request.ordinal,
    };
    let stable = (
        &request.context,
        &request.source,
        request.version,
        &request.form,
    );
    let stable = encode(&stable).map_err(|error| unavailable(error.0))?;
    let now_ms = unix_timestamp_millis();
    let mut proposed = WorkRow::reserved(&install, WorkKind::Upload, scope.run.run_id, now_ms);
    proposed.digest = digest(&[b"upload", &stable]);
    proposed.source = Some(request.source.clone());
    proposed.context = Some(request.context);
    let (row, _) = match reserved(ctx, (&install, &identity), proposed, None).await {
        Ok(found) => found,
        Err(WorkError::Conflict) => {
            let message = "this call started an upload with other arguments";
            return Err(HostError::new(ErrorKind::Conflict, message));
        }
        Err(WorkError::Store(error)) => return Err(unavailable(error.0)),
    };
    if row.state != WorkState::Reserved {
        return Ok(row.work_id);
    }
    let action = PluginWorkAction::Upload {
        source: request.source,
        version: request.version,
        form: request.form,
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

/// Whether the install may send through connection `name`: its grant and its authorization.
async fn connection_granted(
    ctx: &DriverContext,
    (scope, install): (&RunScope, &PluginInstall),
    name: &str,
) -> Result<(), HostError> {
    let connection = InstallScope {
        realm_id: scope.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    let connection = connection.connection(name);
    let send = Permission::ConnectionsSend;
    let patterns = matching(install, send, &connection)?;
    authorize_call(ctx, install, send, &connection, &patterns).await
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
    let change = WorkChange::Dispatched(result.job_id);
    let recorded = drive(update_work((install_id, work_id), change), ctx).await;
    recorded.map_err(|error| unavailable(error.0))?;
    Ok(work_id)
}

/// Reserves the work's row; see [`reserve`].
async fn reserved(
    ctx: &DriverContext,
    named: (&PluginInstall, &Identity),
    proposed: WorkRow,
    marked: Option<(&str, &str)>,
) -> Result<(WorkRow, Reserve), WorkError> {
    drive(reserve(named, proposed, marked)?, ctx).await
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
pub(crate) mod tests;
