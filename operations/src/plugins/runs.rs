//! Run records on an install's receiver: admission within the install's bounds, reads, pages,
//! and saving a run, which drops it from the queue once it ended.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::{PLUGIN_QUEUE_KEYSPACE, PLUGIN_RUNS_KEYSPACE};
use aruna_core::plugin_runs::{
    ATTENTION_ROW, PluginRun, RUN_RETENTION_MS, RUN_ROW, RUNS_QUEUED, RunState, WAITING_ROW,
    parse_queue_key, queue_key, run_key, run_prefix,
};
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use aruna_core::time::unix_timestamp_millis;
use tokio::sync::Mutex;
use ulid::Ulid;

use super::intents::{drop_intents, open_intent, resolve_intents};
use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;

/// Serializes admissions on this node, so no install passes its queue bound.
pub static ADMISSIONS: Mutex<()> = Mutex::const_new(());

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RunError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the install has {0} unfinished or unresolved runs, its limit")]
    Full(usize),
    #[error("no run with this id")]
    NotFound,
    #[error("the run is {0}, not waiting for a resolution")]
    NotWaiting(&'static str),
}

/// Stores a new queued run, unless its install already has `RUNS_QUEUED` unfinished ones.
pub async fn admit(ctx: &DriverContext, run: &PluginRun) -> Result<(), RunError> {
    let _admitting = ADMISSIONS.lock().await;
    transact(ctx, async |txn: &Txn<'_>| admit_in(txn, run).await).await
}

/// Admits the run in `txn`; the caller holds `ADMISSIONS`.
pub async fn admit_in(txn: &Txn<'_>, run: &PluginRun) -> Result<(), RunError> {
    let prefix = run.install_id.to_bytes();
    let queued = txn.scan(PLUGIN_QUEUE_KEYSPACE, &prefix).await?.len();
    if queued >= RUNS_QUEUED {
        return Err(RunError::Full(queued));
    }
    let queue = vec![(queue_key(run.install_id, run.run_id), Vec::new())];
    txn.write(PLUGIN_QUEUE_KEYSPACE, queue).await?;
    let row = vec![(run_key(run.install_id, run.run_id), encode(run)?)];
    txn.write(PLUGIN_RUNS_KEYSPACE, row).await?;
    Ok(())
}

pub async fn read_run(
    ctx: &DriverContext,
    install_id: Ulid,
    run_id: Ulid,
) -> Result<PluginRun, RunError> {
    let key = run_key(install_id, run_id);
    let row = Txn::direct(ctx).read(PLUGIN_RUNS_KEYSPACE, &key).await?;
    Ok(decode(&row.ok_or(RunError::NotFound)?)?)
}

/// At most `limit` runs of an install after the run `after`, oldest first, and the id of the
/// last one when more may follow.
pub async fn list_runs(
    ctx: &DriverContext,
    install_id: Ulid,
    after: Option<Ulid>,
    limit: usize,
) -> Result<(Vec<PluginRun>, Option<Ulid>), RunError> {
    let after = after.map(|run_id| run_key(install_id, run_id));
    let prefix = run_prefix(install_id);
    let (rows, next) = (Txn::direct(ctx))
        .page(PLUGIN_RUNS_KEYSPACE, &prefix, after, limit)
        .await?;
    let runs = (rows.iter())
        .map(|(_, value)| decode::<PluginRun>(value))
        .collect::<Result<Vec<_>, _>>()?;
    let next = next.and(runs.last().map(|run| run.run_id));
    Ok((runs, next))
}

/// The install and run ids of every unfinished run on this node; unresolved runs wait for a
/// person and are left out.
pub async fn unfinished(ctx: &DriverContext) -> Result<Vec<(Ulid, Ulid)>, RunError> {
    let rows = Txn::direct(ctx).scan(PLUGIN_QUEUE_KEYSPACE, &[]).await?;
    Ok((rows.iter())
        .filter(|(_, value)| ![ATTENTION_ROW, WAITING_ROW].contains(&value.as_slice()))
        .filter_map(|(key, _)| parse_queue_key(key))
        .collect())
}

/// Writes the run; a run that ended leaves the queue in the same transaction.
pub async fn save_run(ctx: &DriverContext, run: &PluginRun) -> Result<(), RunError> {
    transact(ctx, async |txn: &Txn<'_>| save_in(txn, run).await).await
}

pub async fn save_in(txn: &Txn<'_>, run: &PluginRun) -> Result<(), RunError> {
    let row = vec![(run_key(run.install_id, run.run_id), encode(run)?)];
    txn.write(PLUGIN_RUNS_KEYSPACE, row).await?;
    let queued = queue_key(run.install_id, run.run_id);
    match run.state {
        RunState::NeedsAttention => {
            let waiting = vec![(queued, ATTENTION_ROW.to_vec())];
            txn.write(PLUGIN_QUEUE_KEYSPACE, waiting).await?;
        }
        state if state.finished() => txn.delete(PLUGIN_QUEUE_KEYSPACE, vec![queued]).await?,
        _ => {}
    }
    Ok(())
}

/// Ends a run that needs attention as a person decided: `done` when its effects happened.
/// Its open intents record the decision and the audit row is written in the same transaction.
pub async fn resolve_run(
    ctx: &DriverContext,
    install_id: Ulid,
    run_id: Ulid,
    done: bool,
    audit: &AdminAuditRecord,
) -> Result<PluginRun, RunError> {
    transact(ctx, async |txn: &Txn<'_>| {
        let key = run_key(install_id, run_id);
        let row = txn.read(PLUGIN_RUNS_KEYSPACE, &key).await?;
        let mut run = decode::<PluginRun>(&row.ok_or(RunError::NotFound)?)?;
        if run.state != RunState::NeedsAttention {
            return Err(RunError::NotWaiting(run.state.as_str()));
        }
        let (state, outcome) = match done {
            true => (RunState::Succeeded, "done"),
            false => (RunState::Failed, "not done"),
        };
        let message = format!("{} resolved it as {outcome}", audit.actor);
        run.finish(state, Some(message), unix_timestamp_millis());
        save_in(txn, &run).await?;
        resolve_intents(txn, run_id, done).await?;
        let entries = audit
            .entries()
            .map_err(|error| StoreError(error.to_string()))?;
        for (space, key, value) in entries {
            txn.write(&space, vec![(key.to_vec(), value.to_vec())])
                .await?;
        }
        Ok(run)
    })
    .await
}

/// Removes runs that ended longer than `RUN_RETENTION_MS` ago, with their intents and stored
/// replies. A run that needs attention stays until a person resolves it.
pub async fn prune_runs(ctx: &DriverContext, now_ms: u64) -> Result<usize, RunError> {
    let rows = Txn::direct(ctx)
        .scan(PLUGIN_RUNS_KEYSPACE, &[RUN_ROW])
        .await?;
    let mut pruned = 0;
    for (_, value) in rows {
        let run = decode::<PluginRun>(&value)?;
        let expired = (run.finished_ms)
            .is_some_and(|finished| finished.saturating_add(RUN_RETENTION_MS) <= now_ms);
        if !expired || run.state == RunState::NeedsAttention {
            continue;
        }
        // A run whose calls may have had effects, or cannot be read, keeps its record.
        if !matches!(open_intent(ctx, run.run_id).await, Ok(None)) {
            continue;
        }
        transact(ctx, async |txn: &Txn<'_>| {
            let key = run_key(run.install_id, run.run_id);
            txn.delete(PLUGIN_RUNS_KEYSPACE, vec![key]).await?;
            drop_intents(txn, run.run_id).await?;
            Ok::<_, RunError>(())
        })
        .await?;
        pruned += 1;
    }
    Ok(pruned)
}

#[cfg(test)]
#[path = "runs_tests.rs"]
pub(crate) mod tests;
