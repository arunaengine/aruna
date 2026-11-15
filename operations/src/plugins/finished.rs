//! Delivers the end of each plugin job as a `job_finished` run, only after the run that started
//! the job succeeded, so the mapping that run wrote exists; the others are held.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::{PLUGIN_RUNS_KEYSPACE, PLUGIN_WORK_KEYSPACE};
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::{PluginRun, RunState, SourceEvent, SourceKind, run_key};
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use tracing::warn;
use ulid::Ulid;

use super::dispatch::event_run;
use super::intents::clear_undone;
use super::runs::{ADMISSIONS, RunError, admit_in, read_run};
use super::store::{StoreError, Txn, decode, encode, transact};
use super::work::{WORK_ROW, WorkKind, WorkRow, WorkState, read_in, work_state};
use crate::driver::DriverContext;

/// Admits a `job_finished` run for every settled work whose starting run succeeded and marks the
/// work notified in the same transaction; a manifest without the trigger is notified without a
/// run. Answers how many works were notified.
pub async fn notify_work(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
) -> Result<usize, RunError> {
    let rows = Txn::direct(ctx)
        .scan(PLUGIN_WORK_KEYSPACE, &[WORK_ROW])
        .await?;
    let mut notified = 0;
    for (key, value) in rows {
        let row: WorkRow = decode(&value)?;
        if row.state != WorkState::Settled || row.notified {
            continue;
        }
        let Some(install) = (config.installs.iter()).find(|install| {
            key.get(1..17) == Some(&install.install_id.to_bytes()[..])
                && install.account == row.account
                && install.receiver == local
                && install.state != InstallState::Removing
        }) else {
            continue;
        };
        let run = match read_run(ctx, install.install_id, row.run_id).await {
            Ok(run) => Some(run),
            // Runs stay while a completion waits; one that is gone anyway cannot be retried.
            Err(RunError::NotFound) => None,
            Err(error) => return Err(error),
        };
        if run.is_some_and(|run| run.state != RunState::Succeeded) {
            continue;
        }
        let finished = event_run(
            config,
            install,
            &finished_event(install, &row, now_ms),
            now_ms,
        );
        let _admitting = ADMISSIONS.lock().await;
        let marked = transact(ctx, async |txn: &Txn<'_>| {
            let Some((key, mut stored)) = read_in(txn, install.install_id, row.work_id).await?
            else {
                return Ok(false);
            };
            if stored.notified {
                return Ok(false);
            }
            if let Some(run) = &finished {
                admit_in(txn, run).await?;
            }
            stored.notified = true;
            let encoded = encode(&stored)?;
            txn.write(PLUGIN_WORK_KEYSPACE, vec![(key, encoded)])
                .await?;
            Ok::<_, RunError>(true)
        });
        match marked.await {
            Ok(true) => notified += 1,
            Ok(false) => {}
            // A full queue takes the completion at a later pass.
            Err(RunError::Full(_)) => {}
            Err(error) => {
                warn!(%error, work = %row.work_id, "Could not deliver a finished plugin job")
            }
        }
    }
    Ok(notified)
}

/// The run a settled, undelivered job waits for: one that ended without success, so an admin
/// retries it or drops the completion. `None` once delivered, or while the run may still succeed.
pub fn waits_for(row: &WorkRow, run: Option<RunState>) -> Option<Ulid> {
    let held = matches!(
        run,
        Some(RunState::Failed | RunState::Cancelled | RunState::NeedsAttention)
    );
    (row.state == WorkState::Settled && !row.notified && held).then_some(row.run_id)
}

/// Queues a failed or cancelled run again with its intents kept, so its calls reach the same
/// ordinals and its jobs the same ids; a call resolved as not done is made again. The audit row
/// commits with it.
pub async fn retry_run(
    ctx: &DriverContext,
    (install_id, run_id): (Ulid, Ulid),
    audit: &AdminAuditRecord,
    now_ms: u64,
) -> Result<PluginRun, RunError> {
    let _admitting = ADMISSIONS.lock().await;
    transact(ctx, async |txn: &Txn<'_>| {
        let row = txn
            .read(PLUGIN_RUNS_KEYSPACE, &run_key(install_id, run_id))
            .await?;
        let mut run = decode::<PluginRun>(&row.ok_or(RunError::NotFound)?)?;
        if !matches!(run.state, RunState::Failed | RunState::Cancelled) {
            return Err(RunError::NotRetried(run.state.as_str()));
        }
        (run.state, run.attempts, run.due_ms) = (RunState::Queued, 0, now_ms);
        (run.finished_ms, run.message, run.output) = (None, None, None);
        run.log.clear();
        admit_in(txn, &run).await?;
        clear_undone(txn, run_id).await?;
        audited(txn, audit).await?;
        Ok(run)
    })
    .await
}

/// Marks the held end of job `work_id` delivered without a run; the audit row commits with it.
pub async fn drop_completion(
    ctx: &DriverContext,
    (install_id, work_id): (Ulid, Ulid),
    audit: &AdminAuditRecord,
) -> Result<WorkRow, RunError> {
    transact(ctx, async |txn: &Txn<'_>| {
        let Some((key, mut row)) = read_in(txn, install_id, work_id).await? else {
            return Err(RunError::NotFound);
        };
        if row.state != WorkState::Settled || row.notified {
            return Err(RunError::NotHeld);
        }
        row.notified = true;
        txn.write(PLUGIN_WORK_KEYSPACE, vec![(key, encode(&row)?)])
            .await?;
        audited(txn, audit).await?;
        Ok(row)
    })
    .await
}

async fn audited(txn: &Txn<'_>, audit: &AdminAuditRecord) -> Result<(), RunError> {
    let entries = (audit.entries()).map_err(|error| StoreError(error.to_string()))?;
    for (space, key, value) in entries {
        txn.write(&space, vec![(key.to_vec(), value.to_vec())])
            .await?;
    }
    Ok(())
}

/// The event a `job_finished` run gets: the job's id, the object path it wrote or read, and
/// how it ended.
fn finished_event(install: &PluginInstall, row: &WorkRow, now_ms: u64) -> SourceEvent {
    let path = match row.kind {
        WorkKind::Copy | WorkKind::Download => row.target.clone(),
        WorkKind::Upload => row.source.clone(),
        WorkKind::Recipe => None,
    };
    SourceEvent {
        kind: SourceKind::JobFinished,
        group_id: install.group_id,
        path: path.unwrap_or_default(),
        resource: row.work_id.to_string(),
        revision: work_state(row, None).to_string(),
        source: format!("job_finished/{}", row.work_id),
        anchor_ms: now_ms,
        journaled_ms: now_ms,
        actor: None,
    }
}

/// The runs a completion still waits on: those of works not notified yet, which keep their
/// record until the completion is delivered or dropped.
pub async fn held_runs(ctx: &DriverContext) -> Result<Vec<Ulid>, StoreError> {
    let rows = Txn::direct(ctx)
        .scan(PLUGIN_WORK_KEYSPACE, &[WORK_ROW])
        .await?;
    let mut runs = Vec::new();
    for (_, value) in rows {
        let row: WorkRow = decode(&value)?;
        if !row.notified {
            runs.push(row.run_id);
        }
    }
    Ok(runs)
}

#[cfg(test)]
#[path = "finished_tests.rs"]
mod tests;
