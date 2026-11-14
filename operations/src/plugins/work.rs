//! Native work of plugin installs on their receiver: one row per install and identity keeps the
//! work's generated ids, binding and state, so a repeated call never starts the work twice.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::keyspaces::{PLUGIN_RUNS_KEYSPACE, PLUGIN_WORK_KEYSPACE};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{RUN_RETENTION_MS, run_key};
use aruna_core::structs::execution::job::{
    JobId, JobRecord, JobResultPayload, JobState, OutputObject, PluginWorkSpec,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;
use crate::jobs::store::read_job_record;

/// Row kinds of the work keyspace: the work by install and identity, and its identity by work id.
pub const WORK_ROW: u8 = b'w';
pub const WORK_INDEX: u8 = b'i';

/// What names one work across calls: the author's key, or the run and call ordinal without one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Identity {
    Key(String),
    Call { run_id: Ulid, ordinal: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkKind {
    Copy,
    Download,
    Upload,
    Recipe,
}

/// What the work was approved under; a later manifest digest stops it before it runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub digest: String,
    pub revision: u64,
    pub recipe: Option<String>,
}

/// A transfer's request as resolved when the work started, without any secret, so the work
/// still runs after its run was pruned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContext {
    pub connection: String,
    /// The connection's base URL resolved from the run's saved settings.
    pub base: String,
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
}

/// `Reserved` until its job is known, then `Dispatched`, and `Settled` once the summary is kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkState {
    Reserved,
    Dispatched,
    Settled,
}

/// How far a sending attempt came; an attempt that finds `Sending` never sends again.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SendBoundary {
    Sending { attempt: u32 },
    Sent { status: u16 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinalState {
    Succeeded,
    Failed,
    Cancelled,
    /// The outcome cannot be known; nothing is tried again.
    Unknown,
}

/// What a settled work answers after its job record is gone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub state: FinalState,
    pub message: Option<String>,
    pub bytes: Option<u64>,
    pub version_id: Option<Ulid>,
    /// The answer status of an upload, and its reply as exposed when it was stored.
    pub status: Option<u16>,
    pub reply: Option<String>,
    pub exit_code: Option<i32>,
    pub outputs: Vec<OutputObject>,
    pub report: Option<String>,
}

/// One work of an install on its receiver.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkRow {
    /// The account of the install that wrote it; a reinstall under the same id has another one.
    pub account: UserId,
    pub work_id: Ulid,
    pub kind: WorkKind,
    /// BLAKE3 of the stable arguments, never of generated ids.
    pub digest: [u8; 32],
    pub binding: Binding,
    pub context: Option<RequestContext>,
    /// The run that started it; an audit reference only, the run may be pruned first.
    pub run_id: Ulid,
    /// The version a copy or download writes, minted once.
    pub version_id: Option<Ulid>,
    pub job: Option<JobId>,
    pub state: WorkState,
    pub boundary: Option<SendBoundary>,
    pub summary: Option<Summary>,
    pub notified: bool,
    pub cancel: bool,
    pub created_ms: u64,
    pub settled_ms: Option<u64>,
}

impl WorkRow {
    /// A new row of `install` in state `Reserved`.
    pub fn reserved(install: &PluginInstall, kind: WorkKind, run_id: Ulid, now_ms: u64) -> Self {
        Self {
            account: install.account,
            work_id: Ulid::generate(),
            kind,
            digest: [0; 32],
            binding: Binding {
                digest: install.digest.clone(),
                revision: install.revision,
                recipe: None,
            },
            context: None,
            run_id,
            version_id: None,
            job: None,
            state: WorkState::Reserved,
            boundary: None,
            summary: None,
            notified: false,
            cancel: false,
            created_ms: now_ms,
            settled_ms: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WorkError {
    #[error("the key names other work with other arguments")]
    Conflict,
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// How the row of a call was found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reserve {
    /// This call wrote the row.
    Fresh,
    /// An earlier call with the same arguments wrote it.
    Earlier,
}

pub fn row_key(install_id: Ulid, identity: &Identity) -> Result<Vec<u8>, StoreError> {
    Ok([&[WORK_ROW][..], &install_id.to_bytes(), &encode(identity)?].concat())
}

fn index_key(install_id: Ulid, work_id: Ulid) -> Vec<u8> {
    [
        &[WORK_INDEX][..],
        &install_id.to_bytes(),
        &work_id.to_bytes(),
    ]
    .concat()
}

/// Writes `proposed` for `identity` in one transaction, or adopts the row an earlier call with
/// the same digest wrote; another digest is a conflict. A former install's row is replaced.
pub async fn reserve(
    ctx: &DriverContext,
    (install, identity): (&PluginInstall, &Identity),
    proposed: WorkRow,
) -> Result<(WorkRow, Reserve), WorkError> {
    let key = row_key(install.install_id, identity)?;
    transact(ctx, async |txn: &Txn<'_>| {
        let stored = match txn.read(PLUGIN_WORK_KEYSPACE, &key).await? {
            Some(bytes) => Some(decode::<WorkRow>(&bytes)?),
            None => None,
        };
        match stored.filter(|stored| stored.account == install.account) {
            Some(stored) if stored.digest == proposed.digest => Ok((stored, Reserve::Earlier)),
            Some(_) => Err(WorkError::Conflict),
            None => {
                let index = index_key(install.install_id, proposed.work_id);
                txn.write(PLUGIN_WORK_KEYSPACE, vec![(index, key.clone())])
                    .await?;
                txn.write(
                    PLUGIN_WORK_KEYSPACE,
                    vec![(key.clone(), encode(&proposed)?)],
                )
                .await?;
                Ok((proposed, Reserve::Fresh))
            }
        }
    })
    .await
}

/// The row of work `work_id` of an install and its key, read in `txn`.
pub async fn read_in(
    txn: &Txn<'_>,
    install_id: Ulid,
    work_id: Ulid,
) -> Result<Option<(Vec<u8>, WorkRow)>, StoreError> {
    let index = index_key(install_id, work_id);
    let Some(key) = txn.read(PLUGIN_WORK_KEYSPACE, &index).await? else {
        return Ok(None);
    };
    let Some(bytes) = txn.read(PLUGIN_WORK_KEYSPACE, &key).await? else {
        return Ok(None);
    };
    let row: WorkRow = decode(&bytes)?;
    Ok((row.work_id == work_id).then_some((key, row)))
}

pub async fn read_work(
    ctx: &DriverContext,
    install_id: Ulid,
    work_id: Ulid,
) -> Result<Option<WorkRow>, StoreError> {
    let found = read_in(&Txn::direct(ctx), install_id, work_id).await?;
    Ok(found.map(|(_, row)| row))
}

/// Changes the row of work `work_id` in one transaction; `None` when it is gone.
pub async fn update_work(
    ctx: &DriverContext,
    (install_id, work_id): (Ulid, Ulid),
    change: impl FnOnce(&mut WorkRow),
) -> Result<Option<WorkRow>, StoreError> {
    transact(ctx, async |txn: &Txn<'_>| {
        let Some((key, mut row)) = read_in(txn, install_id, work_id).await? else {
            return Ok(None);
        };
        change(&mut row);
        txn.write(PLUGIN_WORK_KEYSPACE, vec![(key, encode(&row)?)])
            .await?;
        Ok(Some(row))
    })
    .await
}

/// Records the job a reserved work was submitted as; a later state keeps its job.
pub async fn dispatched(
    ctx: &DriverContext,
    (install_id, work_id): (Ulid, Ulid),
    job: JobId,
) -> Result<Option<WorkRow>, StoreError> {
    update_work(ctx, (install_id, work_id), |row| {
        if row.state == WorkState::Reserved {
            row.state = WorkState::Dispatched;
            row.job = Some(job);
        }
    })
    .await
}

/// The summary of a finished job, or `None` while it may still run. A job record that is gone
/// leaves the outcome unknown.
pub fn summary_of(record: Option<&JobRecord>) -> Option<Summary> {
    let Some(record) = record else {
        let mut gone = Summary::of(FinalState::Unknown);
        gone.message = Some("the job record is gone".into());
        return Some(gone);
    };
    let state = match record.state {
        JobState::Succeeded => FinalState::Succeeded,
        JobState::Failed => FinalState::Failed,
        JobState::Cancelled => FinalState::Cancelled,
        JobState::Indeterminate if record.locally_exhausted => FinalState::Unknown,
        _ => return None,
    };
    let mut summary = Summary::of(state);
    summary.message = (record.last_error.as_ref()).map(|error| error.message.clone());
    if let Some(JobResultPayload::PluginWork { bytes, version_id }) = &record.result {
        summary.bytes = Some(*bytes);
        summary.version_id = *version_id;
    }
    Some(summary)
}

impl Summary {
    fn of(state: FinalState) -> Self {
        Self {
            state,
            message: None,
            bytes: None,
            version_id: None,
            status: None,
            reply: None,
            exit_code: None,
            outputs: Vec::new(),
            report: None,
        }
    }
}

/// Keeps the summary of every dispatched work whose job finished, so its status outlives the
/// job record; answers how many settled.
pub async fn settle_work(ctx: &DriverContext, now_ms: u64) -> Result<usize, StoreError> {
    let txn = Txn::direct(ctx);
    let mut settled = 0;
    for (key, value) in txn.scan(PLUGIN_WORK_KEYSPACE, &[WORK_ROW]).await? {
        let row: WorkRow = decode(&value)?;
        let (WorkState::Dispatched, Some(job)) = (row.state, row.job) else {
            continue;
        };
        let record = read_job_record(&ctx.storage_handle, job, None).await;
        let record = record.map_err(StoreError)?;
        let Some(summary) = summary_of(record.as_ref()) else {
            continue;
        };
        let Some(install_id) = install_at(&key) else {
            continue;
        };
        let changed = update_work(ctx, (install_id, row.work_id), |row| {
            if row.state == WorkState::Dispatched {
                row.state = WorkState::Settled;
                row.summary = Some(summary);
                row.settled_ms = Some(now_ms);
            }
        });
        if changed.await?.is_some() {
            settled += 1;
        }
    }
    Ok(settled)
}

/// Deletes the unkeyed rows nobody can ask for again: settled, no cancel open, kept past the run
/// retention, and their run record pruned. Keyed rows stay while their install exists.
pub async fn prune_work(ctx: &DriverContext, now_ms: u64) -> Result<usize, StoreError> {
    let txn = Txn::direct(ctx);
    let mut dropped = Vec::new();
    for (key, value) in txn.scan(PLUGIN_WORK_KEYSPACE, &[WORK_ROW]).await? {
        let row: WorkRow = decode(&value)?;
        let (Some(install_id), Some(Identity::Call { run_id, .. })) =
            (install_at(&key), identity_at(&key))
        else {
            continue;
        };
        let aged = (row.settled_ms)
            .is_some_and(|settled| settled.saturating_add(RUN_RETENTION_MS) <= now_ms);
        if row.state != WorkState::Settled || row.cancel || !aged {
            continue;
        }
        let run = run_key(install_id, run_id);
        if txn.read(PLUGIN_RUNS_KEYSPACE, &run).await?.is_none() {
            dropped.push(index_key(install_id, row.work_id));
            dropped.push(key);
        }
    }
    let count = dropped.len() / 2;
    transact(ctx, async |txn: &Txn<'_>| {
        txn.delete(PLUGIN_WORK_KEYSPACE, dropped).await
    })
    .await?;
    Ok(count)
}

/// Whether a plugin work job must keep its record: its work row exists and is not settled yet.
pub async fn holds_job(ctx: &DriverContext, spec: &PluginWorkSpec) -> Result<bool, StoreError> {
    let row = read_work(ctx, spec.install_id, spec.work_id).await?;
    Ok(row.is_some_and(|row| row.state != WorkState::Settled))
}

fn install_at(key: &[u8]) -> Option<Ulid> {
    Some(Ulid::from_bytes(key.get(1..17)?.try_into().ok()?))
}

fn identity_at(key: &[u8]) -> Option<Identity> {
    decode(key.get(17..)?).ok()
}

#[cfg(test)]
#[path = "work_tests.rs"]
mod tests;
