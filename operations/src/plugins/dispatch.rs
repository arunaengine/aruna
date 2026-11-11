//! Starts runs on the receiver: by hand with a JSON input, and on `every` timers. A timer
//! queues its occurrence and advances its last due time in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::PLUGIN_RUNS_KEYSPACE;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::{MAX_INPUT, PluginRun, RunState, RunTrigger, timer_key};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_plugins::trigger::TriggerKind;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::install::{InstallError, check_admin, pick_version, read_config, read_manifest};
use super::receiver::lost_reason;
use super::runs::{ADMISSIONS, RunError, admit, admit_in};
use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error(transparent)]
    Install(#[from] InstallError),
    #[error("this node does not run the install; its receiver is {0}")]
    Elsewhere(NodeId),
    #[error("the install is blocked: {reason} ({receiver})")]
    Lost {
        receiver: NodeId,
        reason: &'static str,
    },
    #[error("the plugin has no {0} trigger")]
    NoTrigger(&'static str),
    #[error("the input is not a JSON value of at most 64 KiB")]
    Input,
    #[error(transparent)]
    Run(#[from] RunError),
}

/// The last due occurrence of an install's timer, and occurrences skipped at the queue bound.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerRow {
    pub last_due_ms: u64,
    pub skipped: u64,
}

/// The handler a trigger of the install's pinned version calls.
fn handler(
    config: &RealmConfigDocument,
    install: &PluginInstall,
    kind: TriggerKind,
) -> Option<String> {
    let plugin = pick_version(config, &install.plugin, Some(&install.digest)).ok()?;
    let manifest = read_manifest(&plugin).ok()?;
    manifest.triggers.get(&kind)?.handler.clone()
}

/// A queued run of the install's current revision, due at `due_ms`.
fn new_run(
    install: &PluginInstall,
    handler: String,
    trigger: RunTrigger,
    due_ms: u64,
) -> PluginRun {
    PluginRun {
        run_id: Ulid::generate(),
        install_id: install.install_id,
        revision: install.revision,
        digest: install.digest.clone(),
        handler,
        settings: install.settings.clone(),
        trigger,
        state: RunState::Queued,
        attempts: 0,
        due_ms,
        created_ms: due_ms,
        finished_ms: None,
        message: None,
        output: None,
        log: Vec::new(),
    }
}

/// Queues a manual run for a group admin. Only the install's receiver admits it; an install
/// over its queue bound refuses it.
pub async fn start_run(
    ctx: &DriverContext,
    auth: &AuthContext,
    realm_id: RealmId,
    local: NodeId,
    install_id: Ulid,
    input: &str,
    now_ms: u64,
) -> Result<PluginRun, StartError> {
    let config = read_config(ctx, realm_id).await?;
    let install = (config.installs.iter())
        .find(|install| install.install_id == install_id)
        .ok_or(InstallError::NotFound)?;
    check_admin(ctx, auth, install.group_id).await?;
    if let Some(reason) = lost_reason(&config, install.receiver) {
        let receiver = install.receiver;
        return Err(StartError::Lost { receiver, reason });
    }
    if install.receiver != local {
        return Err(StartError::Elsewhere(install.receiver));
    }
    if install.state != InstallState::Active {
        return Err(InstallError::State(install.state.as_str()).into());
    }
    let valid = serde_json::from_str::<serde_json::Value>(input).is_ok();
    if !valid || input.len() > MAX_INPUT {
        return Err(StartError::Input);
    }
    let handler =
        handler(&config, install, TriggerKind::Manual).ok_or(StartError::NoTrigger("manual"))?;
    let trigger = RunTrigger::Manual {
        by: auth.user_id,
        input: input.to_string(),
    };
    let run = new_run(install, handler, trigger, now_ms);
    admit(ctx, &run).await?;
    Ok(run)
}

/// Queues the due `every` occurrence of each active install this node receives. Missed
/// occurrences collapse into the latest; one over the queue bound is counted as skipped.
pub async fn enqueue_timers(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
) -> Result<usize, StoreError> {
    let mut queued = 0;
    for install in &config.installs {
        let lost = lost_reason(config, install.receiver).is_some();
        if install.receiver != local || install.state != InstallState::Active || lost {
            continue;
        }
        let Some(every) = (pick_version(config, &install.plugin, Some(&install.digest)).ok())
            .and_then(|plugin| read_manifest(&plugin).ok())
            .and_then(|manifest| manifest.triggers.get(&TriggerKind::Every).cloned())
        else {
            continue;
        };
        let (Some(handler), Some(secs)) = (every.handler.clone(), every.interval_secs()) else {
            continue;
        };
        if enqueue_timer(ctx, install, handler, secs * 1000, now_ms).await? {
            queued += 1;
        }
    }
    Ok(queued)
}

async fn enqueue_timer(
    ctx: &DriverContext,
    install: &PluginInstall,
    handler: String,
    interval_ms: u64,
    now_ms: u64,
) -> Result<bool, StoreError> {
    let _admitting = ADMISSIONS.lock().await;
    let key = timer_key(install.install_id);
    transact(ctx, async |txn: &Txn<'_>| {
        let mut row = match txn.read(PLUGIN_RUNS_KEYSPACE, &key).await? {
            Some(bytes) => decode::<TimerRow>(&bytes)?,
            None => TimerRow::default(),
        };
        // Occurrences count from the last activation, so a pause is never replayed.
        let base = row
            .last_due_ms
            .max(install.active_since_ms.unwrap_or(now_ms));
        if now_ms < base.saturating_add(interval_ms) {
            return Ok(false);
        }
        let occurrence = base + (now_ms - base) / interval_ms * interval_ms;
        let trigger = RunTrigger::Timer {
            occurrence_ms: occurrence,
        };
        let run = new_run(install, handler, trigger, now_ms);
        let admitted = match admit_in(txn, &run).await {
            Ok(()) => true,
            Err(RunError::Full(_)) => false,
            Err(RunError::Store(error)) => return Err(error),
            Err(RunError::NotFound) => false,
        };
        row.last_due_ms = occurrence;
        row.skipped += u64::from(!admitted);
        txn.write(PLUGIN_RUNS_KEYSPACE, vec![(key.clone(), encode(&row)?)])
            .await?;
        Ok(admitted)
    })
    .await
}

/// The timer row of an install, if its timer ran before.
pub async fn read_timer(
    ctx: &DriverContext,
    install_id: Ulid,
) -> Result<Option<TimerRow>, StoreError> {
    let row = (Txn::direct(ctx))
        .read(PLUGIN_RUNS_KEYSPACE, &timer_key(install_id))
        .await?;
    row.map(|bytes| decode(&bytes)).transpose()
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
