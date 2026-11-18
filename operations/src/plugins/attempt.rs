//! Decides the attempts of plugin runs on the receiver: which queued runs start, whether an
//! attempt may run, what its guest gets, and how its end is written.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::{PluginRun, RUNS_PARALLEL, RunState, RunTrigger};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_plugins::broker::ErrorKind;
use aruna_plugins::limits::Limits;
use aruna_plugins::permission::{Grant, Permission};
use aruna_plugins::{
    CancellationToken, Field, Invocation, Trigger, WatchEvent, WebhookDelivery, WebhookRequest,
};
use ulid::Ulid;

use super::check::{CallError, CheckCall, RunScope};
use super::grants::{parse_grants, patterns_for, short_path};
use super::install::{InstallError, config_answer, config_call};
use super::intents::open_intent;
use super::matcher::event_trigger;
use super::receiver::{eligible, lost_reason};
use super::rows::StoreError;
use super::runs::{Ended, RunError, conclude, read_run, save_run, unfinished};

const INSTALL_REMOVED: &str = "the install was removed";

/// The runs a claim started, and what stopped it early.
#[derive(Debug, PartialEq)]
pub struct ClaimedRuns {
    pub runs: Vec<PluginRun>,
    pub failure: Option<RunError>,
}

#[derive(Debug, PartialEq)]
enum ClaimPhase {
    Queue,
    Reading(Ulid),
    Saving(Box<PluginRun>),
    DropReading,
    DropIntents(Box<PluginRun>),
    DropSaving,
}

/// Marks the due runs of the active installs this node receives as running, at most
/// `RUNS_PARALLEL` per install. A `running` run no task runs starts again; a queued run whose
/// install is gone ends, unless its intents decide otherwise.
#[derive(Debug, PartialEq)]
pub struct ClaimRuns {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    active: BTreeMap<Ulid, Ulid>,
    now_ms: u64,
    queue: VecDeque<(Ulid, Ulid)>,
    claimed: Vec<PluginRun>,
    phase: ClaimPhase,
}

impl ClaimRuns {
    pub fn new(
        config: Arc<RealmConfigDocument>,
        local: NodeId,
        active: BTreeMap<Ulid, Ulid>,
        now_ms: u64,
    ) -> Self {
        Self {
            config,
            local,
            active,
            now_ms,
            queue: VecDeque::new(),
            claimed: Vec::new(),
            phase: ClaimPhase::Queue,
        }
    }

    /// Ends the claim; the runs already marked still start.
    fn end(&mut self, failure: Option<RunError>) -> Next<ClaimedRuns> {
        Next::Done(ClaimedRuns {
            runs: std::mem::take(&mut self.claimed),
            failure,
        })
    }

    fn advance(&mut self) -> Next<ClaimedRuns> {
        let here = eligible(&self.config, self.local);
        while let Some((install_id, run_id)) = self.queue.pop_front() {
            if self.active.contains_key(&run_id) {
                continue;
            }
            let install =
                (self.config.installs.iter()).find(|install| install.install_id == install_id);
            let Some(install) = install else {
                self.phase = ClaimPhase::DropReading;
                return Next::Call(IoCall::child(read_run(install_id, run_id)));
            };
            let busy = (self.active.values())
                .filter(|install| **install == install_id)
                .count();
            let ready = install.receiver == self.local && install.state == InstallState::Active;
            if !ready || !here || busy >= RUNS_PARALLEL {
                continue;
            }
            self.phase = ClaimPhase::Reading(install_id);
            return Next::Call(IoCall::child(read_run(install_id, run_id)));
        }
        self.end(None)
    }

    fn save(&mut self, run: &PluginRun) -> Result<IoCall, RunError> {
        Ok(IoCall::child(save_run(run)?))
    }

    fn step(&mut self, result: IoResult) -> Result<Next<ClaimedRuns>, RunError> {
        match std::mem::replace(&mut self.phase, ClaimPhase::Queue) {
            ClaimPhase::Queue => {
                self.queue = result.child::<Vec<(Ulid, Ulid)>, RunError>()??.into();
                Ok(self.advance())
            }
            ClaimPhase::Reading(install_id) => {
                let mut run = result.child::<PluginRun, RunError>()??;
                if run.due_ms > self.now_ms || run.state.finished() {
                    return Ok(self.advance());
                }
                run.state = RunState::Running;
                run.attempts += 1;
                let save = self.save(&run)?;
                self.active.insert(run.run_id, install_id);
                self.phase = ClaimPhase::Saving(Box::new(run));
                Ok(Next::Call(save))
            }
            ClaimPhase::Saving(run) => {
                result.child::<(), StoreError>()??;
                self.claimed.push(*run);
                Ok(self.advance())
            }
            ClaimPhase::DropReading => {
                let run = result.child::<PluginRun, RunError>()??;
                let intents = open_intent(run.run_id);
                self.phase = ClaimPhase::DropIntents(Box::new(run));
                Ok(Next::Call(IoCall::child(intents)))
            }
            ClaimPhase::DropIntents(mut run) => {
                let open = result.child::<Option<String>, StoreError>()?;
                let ended = Ended::Cancelled(INSTALL_REMOVED.to_string());
                conclude(&mut run, ended, false, open, self.now_ms);
                let save = self.save(&run)?;
                self.phase = ClaimPhase::DropSaving;
                Ok(Next::Call(save))
            }
            ClaimPhase::DropSaving => {
                result.child::<(), StoreError>()??;
                Ok(self.advance())
            }
        }
    }
}

impl Operation for ClaimRuns {
    type Output = ClaimedRuns;
    type Error = RunError;

    fn first(&mut self) -> Result<Next<ClaimedRuns>, RunError> {
        Ok(Next::Call(IoCall::child(unfinished())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ClaimedRuns>, RunError> {
        Ok(match self.step(result) {
            Ok(next) => next,
            Err(error) => self.end(Some(error)),
        })
    }
}

/// What an attempt that may run works with.
#[derive(Debug, PartialEq)]
pub struct AttemptStart {
    pub config: RealmConfigDocument,
    pub install: PluginInstall,
}

/// Reads the install of an attempt and checks an event run's path again, so a grant removed
/// after admission cancels the run before the guest sees the event.
#[derive(Debug, PartialEq)]
pub struct BeginAttempt {
    scope: RunScope,
    start: Option<AttemptStart>,
}

impl BeginAttempt {
    pub fn new((realm_id, local): (RealmId, NodeId), run: &PluginRun) -> Self {
        let run = run.clone();
        Self {
            scope: RunScope {
                realm_id,
                local,
                run,
            },
            start: None,
        }
    }

    fn started(&mut self, config: RealmConfigDocument) -> Result<Next<AttemptStart>, Ended> {
        let id = self.scope.run.install_id;
        let install = (config.installs.iter()).find(|install| install.install_id == id);
        let install = install.ok_or_else(|| Ended::Cancelled(INSTALL_REMOVED.to_string()))?;
        let start = AttemptStart {
            install: install.clone(),
            config,
        };
        let RunTrigger::Event(event) = &self.scope.run.trigger else {
            return Ok(Next::Done(start));
        };
        let needs = event_trigger(event.kind).needs();
        let target = needs.map(|permission| (permission, event.path.as_str()));
        let check = CheckCall::new(&self.scope, target);
        self.start = Some(start);
        Ok(Next::Call(IoCall::child(check)))
    }
}

impl Operation for BeginAttempt {
    type Output = AttemptStart;
    type Error = Ended;

    fn first(&mut self) -> Result<Next<AttemptStart>, Ended> {
        Ok(Next::Call(config_call(self.scope.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AttemptStart>, Ended> {
        let Some(start) = self.start.take() else {
            let config = config_answer(result);
            let config = config.map_err(|error: InstallError| Ended::Retry(error.to_string()))?;
            return self.started(config);
        };
        match result.child::<PluginInstall, CallError>()? {
            Ok(_) => Ok(Next::Done(start)),
            Err(CallError(error)) if error.kind == ErrorKind::Unavailable => {
                Err(Ended::Retry(error.message))
            }
            Err(CallError(error)) => Err(Ended::Cancelled(error.message)),
        }
    }
}

/// Writes how an attempt ended, after its intents were inspected.
#[derive(Debug, PartialEq)]
pub struct FinishRun {
    run: Box<PluginRun>,
    ended: Option<Ended>,
    changed: bool,
    now_ms: u64,
}

impl FinishRun {
    pub fn new(run: PluginRun, ended: Ended, changed: bool, now_ms: u64) -> Self {
        Self {
            run: Box::new(run),
            ended: Some(ended),
            changed,
            now_ms,
        }
    }
}

impl Operation for FinishRun {
    type Output = ();
    type Error = RunError;

    fn first(&mut self) -> Result<Next<()>, RunError> {
        Ok(Next::Call(IoCall::child(open_intent(self.run.run_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, RunError> {
        let Some(ended) = self.ended.take() else {
            result.child::<(), StoreError>()??;
            return Ok(Next::Done(()));
        };
        let open = result.child::<Option<String>, StoreError>()?;
        conclude(&mut self.run, ended, self.changed, open, self.now_ms);
        Ok(Next::Call(IoCall::child(save_run(&self.run)?)))
    }
}

/// Why the running guests of an install must stop: it was paused or removed, or this node can
/// no longer run installs. Other changes apply at the next call.
pub fn revocation(
    config: &RealmConfigDocument,
    local: NodeId,
    install_id: Ulid,
) -> Option<&'static str> {
    let install = (config.installs.iter()).find(|install| install.install_id == install_id);
    match install {
        None => Some(INSTALL_REMOVED),
        Some(install) if install.state == InstallState::Removing => Some(INSTALL_REMOVED),
        Some(install) => {
            (lost_reason(config, local)).or(install.paused.then_some("the install was paused"))
        }
    }
}

/// The handler call of a run: its saved invocation and the install's grants. An event's object on
/// this node is named by its short `bucket/key` path; `path` keeps the permission path.
pub fn invocation(
    run: &PluginRun,
    (install, local): (&PluginInstall, NodeId),
    limits: Limits,
    cancel: CancellationToken,
) -> Invocation {
    let grants = parse_grants(&install.grants);
    let permissions: BTreeSet<Permission> = (Permission::ALL.into_iter())
        .filter(|permission| !patterns_for(&grants, *permission).is_empty())
        .collect();
    // A service's grant path ends in the connection's name.
    let connections = (grants.get(&Grant::Services).into_iter().flatten())
        .filter_map(|path| path.rsplit('/').next().map(str::to_string))
        .collect();
    let trigger = match &run.trigger {
        RunTrigger::Manual { input, .. } => Trigger::Manual(input.clone()),
        RunTrigger::Timer { occurrence_ms } => Trigger::Timer(*occurrence_ms),
        RunTrigger::Event(event) => Trigger::Event(WatchEvent {
            kind: event.kind.name().into(),
            path: event.path.clone(),
            resource: short_path(&event.resource, (install, local)),
            revision: event.revision.clone(),
        }),
        RunTrigger::Webhook(hook) => Trigger::Webhook(WebhookDelivery {
            delivery_id: hook.delivery_id.clone(),
            request: WebhookRequest {
                method: hook.method.clone(),
                query: hook.query.clone(),
                headers: (hook.headers.iter())
                    .map(|(name, value)| Field {
                        name: name.clone(),
                        value: value.clone(),
                    })
                    .collect(),
                body: hook.body.clone(),
            },
        }),
    };
    Invocation {
        run: run.run_id.to_string(),
        install: run.install_id.to_string(),
        group: install.group_id.to_string(),
        settings: run.settings.clone(),
        trigger,
        permissions,
        connections,
        limits,
        cancel,
    }
}

impl From<IoError> for Ended {
    fn from(error: IoError) -> Self {
        Self::Retry(error.to_string())
    }
}

#[cfg(test)]
#[path = "attempt_tests.rs"]
mod tests;
