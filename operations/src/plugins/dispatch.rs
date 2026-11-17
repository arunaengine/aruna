//! Starts runs on the receiver: by hand with the manual form, and on `every` timers. A timer
//! queues its occurrence and advances its last due time in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::PLUGIN_RUNS_KEYSPACE;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::{
    MAX_INPUT, PluginRun, RunState, RunTrigger, SourceEvent, timer_key,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_plugins::trigger::{Trigger, TriggerKind};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::install::{InstallError, check_admin, pick_version, read_config, read_manifest};
use super::matcher::event_trigger;
use super::receiver::lost_reason;
use super::rows::{
    Answer, Ask, Plan, RowsOperation, StoreError, Turn, decode, done, encode, row as row_of, values,
};
use super::runs::{ADMISSIONS, Admission, RunError, admit};
use super::settings::{SettingError, check_fields};
use crate::driver::{DriverContext, drive};

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
    #[error("the input is not a JSON object of at most 64 KiB")]
    Input,
    #[error("the manual form: {0}")]
    Form(SettingError),
    #[error(transparent)]
    Run(#[from] RunError),
}

/// The last due occurrence of an install's timer, and occurrences skipped at the queue bound.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerRow {
    pub last_due_ms: u64,
    pub skipped: u64,
}

/// The trigger's name when the install's pinned version declares it; the guest picks its
/// function by this name.
fn handler(
    config: &RealmConfigDocument,
    install: &PluginInstall,
    kind: TriggerKind,
) -> Option<String> {
    trigger(config, install, kind).map(|_| kind.as_str().to_string())
}

/// The trigger of `kind` the install's pinned version declares.
fn trigger(
    config: &RealmConfigDocument,
    install: &PluginInstall,
    kind: TriggerKind,
) -> Option<Trigger> {
    let plugin = pick_version(config, &install.plugin, Some(&install.digest)).ok()?;
    read_manifest(&plugin).ok()?.triggers.remove(&kind)
}

/// A queued run of the install's current revision, due at `due_ms`.
pub(super) fn new_run(
    install: &PluginInstall,
    handler: String,
    trigger: RunTrigger,
    due_ms: u64,
) -> PluginRun {
    PluginRun {
        run_id: Ulid::generate(),
        install_id: install.install_id,
        group_id: install.group_id,
        plugin: install.plugin.clone(),
        version: install.version.clone(),
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

/// Queues a manual run for a group admin with the form's values, defaults filled in. Only the
/// install's receiver admits it; an install over its queue bound refuses it.
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
    let values: serde_json::Map<String, serde_json::Value> = (input.len() <= MAX_INPUT)
        .then(|| serde_json::from_str(input).ok())
        .flatten()
        .ok_or(StartError::Input)?;
    let manual = TriggerKind::Manual;
    let form = trigger(&config, install, manual).ok_or(StartError::NoTrigger("manual"))?;
    let checked = check_fields(&form.form, &values).map_err(StartError::Form)?;
    let input = serde_json::Value::Object(checked).to_string();
    if input.len() > MAX_INPUT {
        return Err(StartError::Input);
    }
    let trigger = RunTrigger::Manual {
        by: auth.user_id,
        input,
    };
    let run = new_run(install, manual.as_str().to_string(), trigger, now_ms);
    let _admitting = ADMISSIONS.lock().await;
    drive(admit(run.clone()), ctx).await?;
    Ok(run)
}

/// A run of the install's handler for the event's kind, due at `now_ms`, or `None` when the
/// manifest has no trigger of that kind.
pub fn event_run(
    config: &RealmConfigDocument,
    install: &PluginInstall,
    event: &SourceEvent,
    now_ms: u64,
) -> Option<PluginRun> {
    let handler = handler(config, install, event_trigger(event.kind))?;
    let trigger = RunTrigger::Event(event.clone());
    Some(new_run(install, handler, trigger, now_ms))
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
        let Some(secs) = every.interval_secs() else {
            continue;
        };
        let handler = TriggerKind::Every.as_str().to_string();
        let _admitting = ADMISSIONS.lock().await;
        let timer = enqueue_timer(install, handler, secs * 1000, now_ms);
        if drive(timer, ctx).await? {
            queued += 1;
        }
    }
    Ok(queued)
}

/// Queues the due occurrence of an install's timer, or counts it as skipped over the queue bound;
/// the caller holds `ADMISSIONS`.
fn enqueue_timer(
    install: &PluginInstall,
    handler: String,
    interval_ms: u64,
    now_ms: u64,
) -> RowsOperation<EnqueueTimer> {
    RowsOperation::transaction(EnqueueTimer {
        install: Box::new(install.clone()),
        handler,
        interval_ms,
        now_ms,
        key: timer_key(install.install_id),
        phase: TimerPhase::Reading,
    })
}

#[derive(Debug, PartialEq)]
pub struct EnqueueTimer {
    install: Box<PluginInstall>,
    handler: String,
    interval_ms: u64,
    now_ms: u64,
    key: Vec<u8>,
    phase: TimerPhase,
}

#[derive(Debug, PartialEq)]
enum TimerPhase {
    Reading,
    Admitting(TimerRow, Admission),
    Writing(bool),
}

impl Plan for EnqueueTimer {
    type Output = bool;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<bool>, StoreError> {
        Ok(Turn::Ask(Ask::Read(vec![(PLUGIN_RUNS_KEYSPACE, self.key.clone())])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<bool>, StoreError> {
        match std::mem::replace(&mut self.phase, TimerPhase::Writing(false)) {
            TimerPhase::Reading => {
                let [found] = values::<1>(answer)?;
                let mut row = match found {
                    Some(bytes) => decode::<TimerRow>(&bytes)?,
                    None => TimerRow::default(),
                };
                // Occurrences count from the last activation, so a pause is never replayed.
                let base = (row.last_due_ms).max(self.install.active_since_ms.unwrap_or(self.now_ms));
                if self.now_ms < base.saturating_add(self.interval_ms) {
                    return Ok(Turn::Finish(false));
                }
                let occurrence = base + (self.now_ms - base) / self.interval_ms * self.interval_ms;
                let trigger = RunTrigger::Timer {
                    occurrence_ms: occurrence,
                };
                let handler = std::mem::take(&mut self.handler);
                let run = new_run(&self.install, handler, trigger, self.now_ms);
                row.last_due_ms = occurrence;
                let admission = Admission::new(run);
                let ask = admission.ask();
                self.phase = TimerPhase::Admitting(row, admission);
                Ok(Turn::Ask(ask))
            }
            TimerPhase::Admitting(row, mut admission) => {
                let (admitted, rows) = match admission.take(answer) {
                    Ok(None) => {
                        let ask = admission.ask();
                        self.phase = TimerPhase::Admitting(row, admission);
                        return Ok(Turn::Ask(ask));
                    }
                    Ok(Some(rows)) => (true, rows),
                    Err(RunError::Store(error)) => return Err(error),
                    Err(_) => (false, Vec::new()),
                };
                let mut row = row;
                row.skipped += u64::from(!admitted);
                let mut rows = rows;
                rows.push(row_of(PLUGIN_RUNS_KEYSPACE, self.key.clone(), encode(&row)?));
                self.phase = TimerPhase::Writing(admitted);
                Ok(Turn::Ask(Ask::Write(rows)))
            }
            TimerPhase::Writing(admitted) => {
                done(answer)?;
                Ok(Turn::Finish(admitted))
            }
        }
    }
}

/// The timer row of an install, if its timer ran before.
pub fn read_timer(install_id: Ulid) -> RowsOperation<ReadTimer> {
    RowsOperation::direct(ReadTimer(timer_key(install_id)))
}

#[derive(Debug, PartialEq)]
pub struct ReadTimer(Vec<u8>);

impl Plan for ReadTimer {
    type Output = Option<TimerRow>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        Ok(Turn::Ask(Ask::Read(vec![(PLUGIN_RUNS_KEYSPACE, self.0.clone())])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        let [found] = values::<1>(answer)?;
        Ok(Turn::Finish(found.map(|bytes| decode(&bytes)).transpose()?))
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
