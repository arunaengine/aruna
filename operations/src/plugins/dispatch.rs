//! Starts runs on the receiver: by hand with the manual form, on `every` timers and from checked
//! webhooks. A timer queues its occurrence and advances its last due time in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::injection::verify_request;
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::plugin_installs::{BoundScope, InstallState, PluginInstall};
use aruna_core::plugin_runs::{HookRequest, MAX_INPUT, PluginRun, RunTrigger};
use aruna_core::plugins::{ReleaseRequest, Released};
use aruna_core::secrets::Delivery;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_plugins::manifest::{Manifest, SecretKind};
use aruna_plugins::trigger::TriggerKind;
use ulid::Ulid;

use super::bound::{CheckNodeBinding, NodeCheckFailure, delivery_clash, vault_kind};
use super::held::{AskRelease, HeldSecrets};
use super::hooks::{DROPPED, HookError};
use super::install::{InstallError, admin_answer, admin_call, pick_version, read_manifest};
use super::rate::take_hook;
use super::receiver::lost_reason;
use super::rows::StoreError;
use super::runs::{RunError, admit, declared_trigger, enqueue_timer, new_run};
use super::secret_use::{ReleaseFailure, SelectSecret, Selected};
use super::settings::{SettingError, check_fields};
use crate::document_repository::{GetConfigError, GetConfigOperation};

#[derive(Debug, PartialEq, thiserror::Error)]
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

impl From<IoError> for StartError {
    fn from(error: IoError) -> Self {
        Self::Install(error.into())
    }
}

#[derive(Debug, PartialEq)]
enum StartPhase {
    Config,
    Admin(Box<RealmConfigDocument>),
    Admitting(Box<PluginRun>),
}

/// Queues a manual run for a group admin with the form's values, defaults filled in. Only the
/// install's receiver admits it; an install over its queue bound refuses it.
#[derive(Debug, PartialEq)]
pub struct StartRun {
    auth: AuthContext,
    realm_id: RealmId,
    local: NodeId,
    install_id: Ulid,
    input: String,
    now_ms: u64,
    phase: StartPhase,
}

impl StartRun {
    pub fn new(
        auth: &AuthContext,
        (realm_id, local): (RealmId, NodeId),
        install_id: Ulid,
        input: &str,
        now_ms: u64,
    ) -> Self {
        Self {
            auth: auth.clone(),
            realm_id,
            local,
            install_id,
            input: input.to_string(),
            now_ms,
            phase: StartPhase::Config,
        }
    }

    fn install<'a>(
        &self,
        config: &'a RealmConfigDocument,
    ) -> Result<&'a PluginInstall, InstallError> {
        (config.installs.iter())
            .find(|install| install.install_id == self.install_id)
            .ok_or(InstallError::NotFound)
    }

    /// The run the form asks for, on a receiver that may start it.
    fn new_manual(&self, config: &RealmConfigDocument) -> Result<PluginRun, StartError> {
        let install = self.install(config)?;
        if let Some(reason) = lost_reason(config, install.receiver) {
            let receiver = install.receiver;
            return Err(StartError::Lost { receiver, reason });
        }
        if install.receiver != self.local {
            return Err(StartError::Elsewhere(install.receiver));
        }
        if install.state != InstallState::Active {
            return Err(InstallError::State(install.state.as_str()).into());
        }
        let values: serde_json::Map<String, serde_json::Value> = (self.input.len() <= MAX_INPUT)
            .then(|| serde_json::from_str(&self.input).ok())
            .flatten()
            .ok_or(StartError::Input)?;
        let manual = TriggerKind::Manual;
        let form = declared_trigger(config, install, manual);
        let form = form.ok_or(StartError::NoTrigger("manual"))?;
        let checked = check_fields(&form.form, &values).map_err(StartError::Form)?;
        let input = serde_json::Value::Object(checked).to_string();
        if input.len() > MAX_INPUT {
            return Err(StartError::Input);
        }
        let trigger = RunTrigger::Manual {
            by: self.auth.user_id,
            input,
        };
        let handler = manual.as_str().to_string();
        Ok(new_run(install, handler, trigger, self.now_ms))
    }
}

impl Operation for StartRun {
    type Output = PluginRun;
    type Error = StartError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginAdmissions)
    }

    fn first(&mut self) -> Result<Next<PluginRun>, StartError> {
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginRun>, StartError> {
        match std::mem::replace(&mut self.phase, StartPhase::Config) {
            StartPhase::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(InstallError::from)?;
                let group_id = self.install(&config)?.group_id;
                self.phase = StartPhase::Admin(Box::new(config));
                Ok(Next::Call(admin_call(&self.auth, group_id)))
            }
            StartPhase::Admin(config) => {
                admin_answer(result)?;
                let run = self.new_manual(&config)?;
                let queue = admit(run.clone());
                self.phase = StartPhase::Admitting(Box::new(run));
                Ok(Next::Call(IoCall::child(queue)))
            }
            StartPhase::Admitting(run) => {
                result.child::<(), RunError>()??;
                Ok(Next::Done(*run))
            }
        }
    }
}

/// Queues the due `every` occurrence of each active install this node receives. Missed
/// occurrences collapse into the latest; one over the queue bound is counted as skipped.
#[derive(Debug, PartialEq)]
pub(crate) struct EnqueueTimers {
    now_ms: u64,
    timers: VecDeque<(PluginInstall, u64)>,
    queued: usize,
}

impl EnqueueTimers {
    pub(crate) fn new(config: &RealmConfigDocument, local: NodeId, now_ms: u64) -> Self {
        let timers = (config.installs.iter())
            .filter(|install| {
                let lost = lost_reason(config, install.receiver).is_some();
                install.receiver == local && install.state == InstallState::Active && !lost
            })
            .filter_map(|install| {
                let every = declared_trigger(config, install, TriggerKind::Every)?;
                Some((install.clone(), every.interval_secs()?))
            })
            .collect();
        Self {
            now_ms,
            timers,
            queued: 0,
        }
    }

    fn advance(&mut self) -> Next<usize> {
        let Some((install, secs)) = self.timers.pop_front() else {
            return Next::Done(self.queued);
        };
        let handler = TriggerKind::Every.as_str().to_string();
        let timer = enqueue_timer(&install, handler, secs * 1000, self.now_ms);
        Next::Call(IoCall::child(timer))
    }
}

impl Operation for EnqueueTimers {
    type Output = usize;
    type Error = StoreError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginAdmissions)
    }

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        Ok(self.advance())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, StoreError> {
        self.queued += usize::from(result.child::<bool, StoreError>()??);
        Ok(self.advance())
    }
}

/// A webhook request that passed its check, ready to queue with `AdmitHook`.
#[derive(Debug, PartialEq)]
pub struct VerifiedHook {
    pub run: PluginRun,
    pub install_id: Ulid,
    pub delivery_id: Option<String>,
}

/// What a hook check keeps while its secret is released.
#[derive(Debug, PartialEq)]
struct HookCheck {
    config: Arc<RealmConfigDocument>,
    manifest: Box<Manifest>,
    slot: String,
}

/// Checks one webhook request for the install on this receiver: its install and rate, then the
/// request with its bound secret. Removes credential headers from what the run keeps.
#[derive(Debug, PartialEq)]
pub struct VerifyHook {
    realm_id: RealmId,
    local: NodeId,
    install_id: Ulid,
    request: Option<HookRequest>,
    now_ms: u64,
    check: Option<HookCheck>,
}

impl VerifyHook {
    pub fn new(
        (realm_id, local): (RealmId, NodeId),
        install_id: Ulid,
        request: HookRequest,
        now_ms: u64,
    ) -> Self {
        Self {
            realm_id,
            local,
            install_id,
            request: Some(request),
            now_ms,
            check: None,
        }
    }

    fn checked(&mut self, config: RealmConfigDocument) -> Result<Next<VerifiedHook>, HookError> {
        let unavailable = |why: String| HookError::Unavailable(why);
        let id = self.install_id;
        let install = (config.installs.iter()).find(|install| install.install_id == id);
        let install = install.ok_or(HookError::NotFound)?;
        let plugin = pick_version(&config, &install.plugin, Some(&install.digest));
        let manifest = plugin.and_then(|plugin| read_manifest(&plugin));
        let manifest = manifest.map_err(|error| unavailable(error.to_string()))?;
        let hook = manifest.triggers.get(&TriggerKind::Webhook);
        let hook = hook.ok_or(HookError::NotFound)?;
        let slot = hook.verify.clone().ok_or(HookError::NotFound)?;
        if let Some(reason) = lost_reason(&config, install.receiver) {
            return Err(HookError::Paused(reason));
        }
        if install.receiver != self.local {
            return Err(HookError::Elsewhere(install.receiver));
        }
        if install.state != InstallState::Active {
            return Err(HookError::Paused(install.state.as_str()));
        }
        if !take_hook(id, Instant::now()) {
            return Err(HookError::Rate);
        }
        let release = ReleaseRequest {
            install_id: id,
            slot: slot.clone(),
            delivery: Delivery::Verify,
            target: None,
            held: None,
            run_id: None,
        };
        let config = Arc::new(config);
        let ask = AskRelease::new(config.clone(), self.local, release);
        self.check = Some(HookCheck {
            config,
            manifest: Box::new(manifest),
            slot,
        });
        Ok(Next::Call(IoCall::child(ask)))
    }

    fn released(
        &mut self,
        check: HookCheck,
        released: Released,
    ) -> Result<Next<VerifiedHook>, HookError> {
        let unavailable = |why: String| HookError::Unavailable(why);
        let HookCheck {
            config,
            manifest,
            slot,
        } = check;
        let (released, fields) = (HeldSecrets::default().absorb(&slot, released))
            .map_err(|error| unavailable(error.to_string()))?;
        // A secret edit can make the checked header the delivery id; nothing starts then.
        if delivery_clash(&manifest, &slot, &released.settings) {
            let clash = "the delivery id header is the checked header".to_string();
            return Err(unavailable(clash));
        }
        let mut request = self.request.take().ok_or(IoError::Unfinished)?;
        let (kind, settings) = (&released.kind, &released.settings);
        let passed = verify_request(kind, settings, &fields, &mut request.headers, &request.body);
        let passed = passed.map_err(|error| unavailable(error.to_string()))?;
        (request.headers)
            .retain(|(name, _)| !DROPPED.contains(&name.to_ascii_lowercase().as_str()));
        if !passed {
            return Err(HookError::Unverified);
        }
        let hook = manifest.triggers.get(&TriggerKind::Webhook);
        let id_header = hook.and_then(|hook| hook.delivery_id.as_deref());
        request.delivery_id = (request.headers.iter())
            .find(|(name, _)| id_header.is_some_and(|id| name.eq_ignore_ascii_case(id)))
            .map(|(_, value)| value.clone())
            .filter(|value| !value.is_empty());
        let delivery_id = request.delivery_id.clone();
        let id = self.install_id;
        let install = (config.installs.iter()).find(|install| install.install_id == id);
        let install = install.ok_or(HookError::NotFound)?;
        let handler = TriggerKind::Webhook.as_str().to_string();
        let trigger = RunTrigger::Webhook(request);
        Ok(Next::Done(VerifiedHook {
            run: new_run(install, handler, trigger, self.now_ms),
            install_id: id,
            delivery_id,
        }))
    }
}

impl Operation for VerifyHook {
    type Output = VerifiedHook;
    type Error = HookError;

    fn first(&mut self) -> Result<Next<VerifiedHook>, HookError> {
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<VerifiedHook>, HookError> {
        let Some(check) = self.check.take() else {
            let config = result.child::<RealmConfigDocument, GetConfigError>()?;
            let config = config
                .map_err(|error| HookError::Unavailable(InstallError::from(error).to_string()))?;
            return self.checked(config);
        };
        let released = result.child::<Released, ReleaseFailure>()?;
        let released = released.map_err(|error| HookError::Unavailable(error.to_string()))?;
        self.released(check, released)
    }
}

/// Whether a relay from `local` passes `authorization` on: only when the install's verify secret
/// checks that header. A node secret is asked of the receiver; `None` when it cannot be told.
#[derive(Debug, PartialEq)]
pub struct HookAuthorization {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    install_id: Ulid,
    by_node: bool,
}

impl HookAuthorization {
    pub fn new(config: RealmConfigDocument, (local, install_id): (NodeId, Ulid)) -> Self {
        Self {
            config: Arc::new(config),
            local,
            install_id,
            by_node: false,
        }
    }

    fn checks(header: Option<String>) -> Next<Option<bool>> {
        let authorization =
            header.is_some_and(|header| header.eq_ignore_ascii_case("authorization"));
        Next::Done(Some(authorization))
    }
}

impl Operation for HookAuthorization {
    type Output = Option<bool>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Option<bool>>, IoError> {
        let id = self.install_id;
        let config = self.config.clone();
        let install = (config.installs.iter()).find(|install| install.install_id == id);
        let Some(install) = install else {
            return Ok(Next::Done(None));
        };
        let plugin = pick_version(&config, &install.plugin, Some(&install.digest));
        let Ok(manifest) = plugin.and_then(|plugin| read_manifest(&plugin)) else {
            return Ok(Next::Done(None));
        };
        let hook = manifest.triggers.get(&TriggerKind::Webhook);
        let Some(slot) = hook.and_then(|hook| hook.verify.as_ref()) else {
            return Ok(Next::Done(Some(false)));
        };
        let declared = manifest.secrets.get(slot).map(|declared| declared.kind);
        if declared == Some(SecretKind::Bearer) {
            return Ok(Next::Done(Some(true)));
        }
        let Some(binding) = install.secrets.get(slot) else {
            return Ok(Next::Done(Some(false)));
        };
        Ok(Next::Call(match binding.scope {
            BoundScope::Node => {
                let Some(kind) = declared.and_then(vault_kind) else {
                    return Ok(Next::Done(None));
                };
                self.by_node = true;
                let place = (self.local, install.receiver);
                IoCall::child(CheckNodeBinding::new(place, &binding.name, &kind))
            }
            BoundScope::Group | BoundScope::Realm => {
                IoCall::child(SelectSecret::new(config.clone(), install, binding))
            }
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<bool>>, IoError> {
        if self.by_node {
            return Ok(match result.child::<Option<String>, NodeCheckFailure>()? {
                Ok(header) => Self::checks(header),
                Err(_) => Next::Done(None),
            });
        }
        Ok(match result.child::<Option<Selected>, ReleaseFailure>()? {
            Ok(Some(selected)) => Self::checks(selected.settings.get("header").cloned()),
            _ => Next::Done(None),
        })
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
