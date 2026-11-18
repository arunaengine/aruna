//! Changes an active install: grants, version, pause, resume, removal. A role change first writes
//! an audited `preparing` record, so the role is never wider than an audited record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::plugin_installs::{InstallState, PluginInstall, Widening};
use aruna_core::plugins::PublishedPlugin;
use aruna_core::structs::identity::admin_audit::AdminAuditAction;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_plugins::manifest::Manifest;
use aruna_plugins::trigger::TriggerKind;
use serde_json::{Map, Value};
use tracing::info;
use ulid::Ulid;

use super::account::{AccountError, Teardown};
use super::bound::{BindError, BindPlace, BindSlots, BoundSlots};
use super::grants::{
    Grants, InstallScope, carry_grants, check_supported, held_covers, narrow_grants, parse_grants,
    patterns_for, stored_grants,
};
use super::install::{
    CheckHolder, InstallCaller, InstallError, SettleInstall, admin_answer, admin_call,
    config_answer, config_call, pick_version, read_manifest, scope_of, written,
};
use super::matcher::Target;
use super::records::{InstallEntryConfig, InstallEntryOperation, RecordError};
use super::rules::RulesFailure;
use super::settings::check_settings;
use crate::realm::mutate_placement::node_kind;

/// An install as the realm config holds it now, for a caller proven group admin.
#[derive(Debug, PartialEq)]
pub struct LoadedInstall {
    config: Arc<RealmConfigDocument>,
    current: PluginInstall,
}

/// Reads the install and refuses a caller who is not an admin of its group.
#[derive(Debug, PartialEq)]
pub struct LoadInstall {
    auth: AuthContext,
    realm_id: RealmId,
    install_id: Ulid,
    loaded: Option<Box<LoadedInstall>>,
}

impl LoadInstall {
    fn new(caller: &InstallCaller, install_id: Ulid) -> Self {
        Self {
            auth: caller.auth.clone(),
            realm_id: caller.actor.realm_id,
            install_id,
            loaded: None,
        }
    }
}

impl Operation for LoadInstall {
    type Output = LoadedInstall;
    type Error = InstallError;

    fn first(&mut self) -> Result<Next<LoadedInstall>, InstallError> {
        Ok(Next::Call(config_call(self.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LoadedInstall>, InstallError> {
        if let Some(loaded) = self.loaded.take() {
            admin_answer(result)?;
            return Ok(Next::Done(*loaded));
        }
        let config = config_answer(result)?;
        let current = (config.installs.iter())
            .find(|install| install.install_id == self.install_id)
            .cloned()
            .ok_or(InstallError::NotFound)?;
        let admin = admin_call(&self.auth, current.group_id);
        self.loaded = Some(Box::new(LoadedInstall {
            config: Arc::new(config),
            current,
        }));
        Ok(Next::Call(admin))
    }
}

fn load_call(caller: &InstallCaller, install_id: Ulid) -> IoCall {
    IoCall::child(LoadInstall::new(caller, install_id))
}

fn load_answer(result: IoResult) -> Result<LoadedInstall, InstallError> {
    result.child::<LoadedInstall, InstallError>()?
}

fn holder_answer(result: IoResult) -> Result<(), InstallError> {
    result.child::<(), InstallError>()?
}

fn settled_answer(result: IoResult) -> Result<PluginInstall, InstallError> {
    result.child::<PluginInstall, InstallError>()?
}

/// Writes the audited `preparing` record of a role change, then settles it.
#[derive(Debug, PartialEq)]
struct CommitChange {
    caller: InstallCaller,
    expected: u64,
    next: Option<PluginInstall>,
    action: AdminAuditAction,
}

impl CommitChange {
    fn call(
        caller: &InstallCaller,
        current: &PluginInstall,
        next: PluginInstall,
        action: AdminAuditAction,
    ) -> IoCall {
        IoCall::child(Self {
            caller: caller.clone(),
            expected: current.revision,
            next: Some(next),
            action,
        })
    }
}

impl Operation for CommitChange {
    type Output = PluginInstall;
    type Error = InstallError;

    fn first(&mut self) -> Result<Next<PluginInstall>, InstallError> {
        let next = self.next.as_ref().ok_or(IoError::Unfinished)?;
        let audit = self.caller.audit(self.action, next);
        let write = self.caller.write(Some(self.expected), next, Some(audit));
        Ok(Next::Call(write))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, InstallError> {
        let Some(next) = self.next.take() else {
            return Ok(Next::Done(settled_answer(result)?));
        };
        written(result)?;
        let settle = SettleInstall::new(self.caller.clone(), next, None);
        Ok(Next::Call(IoCall::child(settle)))
    }
}

/// A new grant set waiting for the holder check.
#[derive(Debug, PartialEq)]
struct GrantPlan {
    current: PluginInstall,
    manifest: Manifest,
    grants: Grants,
}

#[derive(Debug, PartialEq)]
enum GrantsPhase {
    Load,
    Holder(Box<GrantPlan>),
    Commit,
}

/// Replaces the grants: a permission left out gets all of the group, an empty list removes an
/// optional one. The caller must hold every grant. It also retries a failed earlier change.
#[derive(Debug, PartialEq)]
pub struct SetGrants {
    caller: InstallCaller,
    install_id: Ulid,
    choices: BTreeMap<String, Vec<String>>,
    phase: GrantsPhase,
}

impl SetGrants {
    pub fn new(
        caller: InstallCaller,
        install_id: Ulid,
        choices: &BTreeMap<String, Vec<String>>,
    ) -> Self {
        Self {
            caller,
            install_id,
            choices: choices.clone(),
            phase: GrantsPhase::Load,
        }
    }

    fn plan(&mut self, loaded: LoadedInstall) -> Result<Next<PluginInstall>, InstallError> {
        let LoadedInstall { config, current } = loaded;
        changeable(&current)?;
        let plugin = pick_version(&config, &current.plugin, Some(&current.digest))?;
        let manifest = read_manifest(&plugin)?;
        let scope = scope_of(self.caller.actor.realm_id, &current);
        let grants = narrow_grants(&manifest, &scope, &self.choices)?;
        let holder = CheckHolder::new(&self.caller.auth, (&scope, &grants), &config);
        self.phase = GrantsPhase::Holder(Box::new(GrantPlan {
            current,
            manifest,
            grants,
        }));
        Ok(Next::Call(IoCall::child(holder)))
    }
}

impl Operation for SetGrants {
    type Output = PluginInstall;
    type Error = InstallError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginInstall(self.install_id))
    }

    fn first(&mut self) -> Result<Next<PluginInstall>, InstallError> {
        Ok(Next::Call(load_call(&self.caller, self.install_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, InstallError> {
        match std::mem::replace(&mut self.phase, GrantsPhase::Commit) {
            GrantsPhase::Load => self.plan(load_answer(result)?),
            GrantsPhase::Holder(plan) => {
                holder_answer(result)?;
                let GrantPlan {
                    current,
                    manifest,
                    grants,
                } = *plan;
                let mut next = preparing(&current, &grants);
                next.granted_by = self.caller.actor.user_id;
                widen(&mut next, &current, (&manifest, &manifest), &self.caller);
                let action = AdminAuditAction::PluginGrantsChanged;
                let commit = CommitChange::call(&self.caller, &current, next, action);
                Ok(Next::Call(commit))
            }
            GrantsPhase::Commit => Ok(Next::Done(settled_answer(result)?)),
        }
    }
}

/// A version move waiting for its holder and secret checks.
#[derive(Debug, PartialEq)]
struct UpdatePlan {
    config: Arc<RealmConfigDocument>,
    current: PluginInstall,
    plugin: PublishedPlugin,
    manifest: Manifest,
    settings: String,
    kept: Grants,
    missing: Vec<String>,
}

#[derive(Debug, PartialEq)]
enum UpdatePhase {
    Load,
    Holder(Box<UpdatePlan>),
    Binding(Box<UpdatePlan>),
    Commit(Vec<String>),
}

/// Moves the install to another published version of the same plugin. It keeps the grants the
/// new version still requests, which the caller must hold now, and grants nothing new; it is
/// blocked while something it requires is not granted. Answers the record and what is missing.
#[derive(Debug, PartialEq)]
pub struct UpdateInstall {
    caller: InstallCaller,
    local: Option<NodeId>,
    install_id: Ulid,
    digest: String,
    settings: Option<Map<String, Value>>,
    phase: UpdatePhase,
}

impl UpdateInstall {
    pub fn new(
        (caller, local): (InstallCaller, Option<NodeId>),
        install_id: Ulid,
        digest: &str,
        settings: Option<&Map<String, Value>>,
    ) -> Self {
        Self {
            caller,
            local,
            install_id,
            digest: digest.to_string(),
            settings: settings.cloned(),
            phase: UpdatePhase::Load,
        }
    }

    fn plan(
        &mut self,
        loaded: LoadedInstall,
    ) -> Result<Next<(PluginInstall, Vec<String>)>, InstallError> {
        let LoadedInstall { config, current } = loaded;
        changeable(&current)?;
        let plugin = pick_version(&config, &current.plugin, Some(&self.digest))?;
        let manifest = read_manifest(&plugin)?;
        check_supported(&manifest)?;
        let stored: Map<String, Value> =
            serde_json::from_str(&current.settings).unwrap_or_default();
        let settings = check_settings(&manifest, self.settings.as_ref().unwrap_or(&stored))?;
        let scope: InstallScope = scope_of(self.caller.actor.realm_id, &current);
        let (kept, missing) = carry_grants(&parse_grants(&current.grants), &manifest, &scope);
        let holder = CheckHolder::new(&self.caller.auth, (&scope, &kept), &config);
        self.phase = UpdatePhase::Holder(Box::new(UpdatePlan {
            config,
            current,
            plugin,
            manifest,
            settings,
            kept,
            missing,
        }));
        Ok(Next::Call(IoCall::child(holder)))
    }

    /// Binds the new version's slots: a kept slot's binding is checked again where it lives, an
    /// added slot is bound to the group secret of its name, and a dropped slot's binding goes.
    fn rebind(&self, plan: &UpdatePlan) -> IoCall {
        let current = &plan.current;
        let kept: BTreeMap<String, String> = (current.secrets.iter())
            .filter(|(slot, _)| plan.manifest.secrets.contains_key(*slot))
            .map(|(slot, binding)| (slot.clone(), binding.to_string()))
            .collect();
        let place = BindPlace {
            group_id: current.group_id,
            local: self.local,
            receiver: Some(current.receiver),
        };
        IoCall::child(BindSlots::new(
            plan.config.clone(),
            place,
            &plan.manifest,
            &kept,
        ))
    }

    fn commit(
        &mut self,
        plan: UpdatePlan,
        bound: BoundSlots,
    ) -> Result<Next<(PluginInstall, Vec<String>)>, InstallError> {
        let UpdatePlan {
            config,
            current,
            plugin,
            manifest,
            settings,
            kept,
            missing,
        } = plan;
        let mut next = preparing(&current, &kept);
        next.secrets = bound.0;
        next.version = plugin.version;
        next.digest = plugin.digest;
        next.settings = settings;
        let before = pick_version(&config, &current.plugin, Some(&current.digest))?;
        let before = read_manifest(&before)?;
        widen(&mut next, &current, (&before, &manifest), &self.caller);
        let action = AdminAuditAction::PluginUpdated;
        self.phase = UpdatePhase::Commit(missing);
        let commit = CommitChange::call(&self.caller, &current, next, action);
        Ok(Next::Call(commit))
    }
}

impl Operation for UpdateInstall {
    type Output = (PluginInstall, Vec<String>);
    type Error = InstallError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginInstall(self.install_id))
    }

    fn first(&mut self) -> Result<Next<Self::Output>, InstallError> {
        Ok(Next::Call(load_call(&self.caller, self.install_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, InstallError> {
        match std::mem::replace(&mut self.phase, UpdatePhase::Load) {
            UpdatePhase::Load => self.plan(load_answer(result)?),
            UpdatePhase::Holder(plan) => {
                holder_answer(result)?;
                let rebind = self.rebind(&plan);
                self.phase = UpdatePhase::Binding(plan);
                Ok(Next::Call(rebind))
            }
            UpdatePhase::Binding(plan) => {
                let bound = result.child::<BoundSlots, BindError>()??;
                self.commit(*plan, bound)
            }
            UpdatePhase::Commit(missing) => Ok(Next::Done((settled_answer(result)?, missing))),
        }
    }
}

/// Pauses or resumes an install. A pause stops runs and keeps everything else; a blocked install
/// stays blocked, and paused. A resume makes it active only while everything it requires is
/// granted, else it stays blocked; events from the pause are not replayed.
#[derive(Debug, PartialEq)]
pub struct SetPaused {
    caller: InstallCaller,
    install_id: Ulid,
    paused: bool,
    written: Option<Box<PluginInstall>>,
}

impl SetPaused {
    pub fn new(caller: InstallCaller, install_id: Ulid, paused: bool) -> Self {
        Self {
            caller,
            install_id,
            paused,
            written: None,
        }
    }

    fn paused(current: &PluginInstall) -> Result<PluginInstall, InstallError> {
        let state = match current.state {
            InstallState::Active => InstallState::Paused,
            InstallState::Blocked if !current.paused => InstallState::Blocked,
            state => return Err(InstallError::State(state.as_str())),
        };
        let mut next = current.clone();
        next.paused = true;
        next.state = state;
        next.revision += 1;
        Ok(next)
    }

    fn resumed(&self, loaded: &LoadedInstall) -> Result<PluginInstall, InstallError> {
        let LoadedInstall { config, current } = loaded;
        let paused = matches!(current.state, InstallState::Paused | InstallState::Blocked);
        if !current.paused || !paused {
            return Err(InstallError::State(current.state.as_str()));
        }
        let plugin = pick_version(config, &current.plugin, Some(&current.digest))?;
        let manifest = read_manifest(&plugin)?;
        let scope = scope_of(self.caller.actor.realm_id, current);
        let (_, missing) = carry_grants(&parse_grants(&current.grants), &manifest, &scope);
        let mut next = current.clone();
        next.paused = false;
        next.state = match missing.is_empty() {
            true => InstallState::Active,
            false => InstallState::Blocked,
        };
        if next.state == InstallState::Active {
            next.active_since_ms = Some(self.caller.now_ms);
            next.activated_by = Some(self.caller.actor.node_id);
        }
        next.revision += 1;
        Ok(next)
    }
}

impl Operation for SetPaused {
    type Output = PluginInstall;
    type Error = InstallError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginInstall(self.install_id))
    }

    fn first(&mut self) -> Result<Next<PluginInstall>, InstallError> {
        Ok(Next::Call(load_call(&self.caller, self.install_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, InstallError> {
        if let Some(next) = self.written.take() {
            written(result)?;
            return Ok(Next::Done(*next));
        }
        let loaded = load_answer(result)?;
        let (next, action) = match self.paused {
            true => (
                Self::paused(&loaded.current)?,
                AdminAuditAction::PluginPaused,
            ),
            false => (self.resumed(&loaded)?, AdminAuditAction::PluginResumed),
        };
        let audit = self.caller.audit(action, &next);
        let expected = Some(loaded.current.revision);
        let write = self.caller.write(expected, &next, Some(audit));
        self.written = Some(Box::new(next));
        Ok(Next::Call(write))
    }
}

#[derive(Debug, PartialEq)]
enum RemovePhase {
    Load,
    Marked(Box<PluginInstall>),
    Teardown(Box<PluginInstall>),
    Rules(Box<PluginInstall>),
    Deleted(Box<PluginInstall>),
}

/// Marks the install `removing`, deactivates its account, removes its role, then deletes the
/// record. A retry after a failure resumes from `removing`. The account id stays in audit rows.
#[derive(Debug, PartialEq)]
pub struct RemoveInstall {
    caller: InstallCaller,
    install_id: Ulid,
    phase: RemovePhase,
}

impl RemoveInstall {
    pub fn new(caller: InstallCaller, install_id: Ulid) -> Self {
        Self {
            caller,
            install_id,
            phase: RemovePhase::Load,
        }
    }

    fn teardown(&mut self, removing: Box<PluginInstall>) -> Next<PluginInstall> {
        let teardown = Teardown::new(self.caller.change(&removing));
        self.phase = RemovePhase::Teardown(removing);
        Next::Call(IoCall::child(teardown))
    }

    fn delete(&self, removing: &PluginInstall) -> IoCall {
        let action = AdminAuditAction::PluginRemoved;
        IoCall::child(InstallEntryOperation::new(InstallEntryConfig {
            actor: self.caller.actor.clone(),
            auth_context: self.caller.auth.clone(),
            install_id: self.install_id,
            group_id: removing.group_id,
            expected: Some(removing.revision),
            install: None,
            audit: Some(self.caller.audit(action, removing)),
        }))
    }
}

impl Operation for RemoveInstall {
    type Output = PluginInstall;
    type Error = InstallError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginInstall(self.install_id))
    }

    fn first(&mut self) -> Result<Next<PluginInstall>, InstallError> {
        Ok(Next::Call(load_call(&self.caller, self.install_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, InstallError> {
        match std::mem::replace(&mut self.phase, RemovePhase::Load) {
            RemovePhase::Load => {
                let current = load_answer(result)?.current;
                if current.state == InstallState::Removing {
                    return Ok(self.teardown(Box::new(current)));
                }
                let mut next = current.clone();
                next.state = InstallState::Removing;
                next.revision += 1;
                let write = self.caller.write(Some(current.revision), &next, None);
                self.phase = RemovePhase::Marked(Box::new(next));
                Ok(Next::Call(write))
            }
            RemovePhase::Marked(removing) => {
                written(result)?;
                Ok(self.teardown(removing))
            }
            RemovePhase::Teardown(removing) => {
                let torn = result.child::<(), AccountError>()?;
                torn.map_err(|error| InstallError::Step {
                    step: "remove",
                    error: error.0,
                })?;
                let retire = self.caller.retire(&removing);
                self.phase = RemovePhase::Rules(removing);
                Ok(Next::Call(retire))
            }
            RemovePhase::Rules(removing) => {
                if let Err(error) = result.child::<(), RulesFailure>()? {
                    tracing::warn!(%error, "Could not remove a plugin install's use rules");
                }
                let delete = self.delete(&removing);
                self.phase = RemovePhase::Deleted(removing);
                Ok(Next::Call(delete))
            }
            RemovePhase::Deleted(removing) => {
                result.child::<Option<PluginInstall>, RecordError>()??;
                let (install_id, plugin) = (self.install_id, &removing.plugin);
                info!(%install_id, %plugin, "Removed a plugin install");
                Ok(Next::Done(*removing))
            }
        }
    }
}

/// Refuses changes while the install is removed or not installed yet. A `preparing` install
/// that was active before failed an earlier change, which a new change retries.
fn changeable(install: &PluginInstall) -> Result<(), InstallError> {
    match install.state {
        InstallState::Removing => Err(InstallError::State(install.state.as_str())),
        InstallState::Preparing if install.active_since_ms.is_none() => {
            Err(InstallError::State(install.state.as_str()))
        }
        _ => Ok(()),
    }
}

/// The next record of a role change: inactive, with the new grants, until it is settled.
fn preparing(current: &PluginInstall, grants: &Grants) -> PluginInstall {
    let mut next = current.clone();
    next.state = InstallState::Preparing;
    next.grants = stored_grants(grants);
    next.failed = None;
    next.revision += 1;
    next
}

/// The event coverage of an install: per event trigger kind, the grant patterns its permission
/// needs. A kind without a needed permission covers everything of the group.
pub fn coverage(manifest: &Manifest, grants: &Grants) -> BTreeMap<TriggerKind, BTreeSet<String>> {
    (manifest.triggers.keys())
        .filter(|kind| kind.needs().is_some())
        .map(|kind| {
            let needed = kind
                .needs()
                .map(|permission| patterns_for(grants, permission));
            (*kind, needed.unwrap_or_default())
        })
        .collect()
}

/// Records a widening when the next record matches events the current one did not: a new kind,
/// or a pattern the current grants of that kind did not hold.
fn widen(
    next: &mut PluginInstall,
    current: &PluginInstall,
    manifests: (&Manifest, &Manifest),
    caller: &InstallCaller,
) {
    let before = coverage(manifests.0, &parse_grants(&current.grants));
    let after = coverage(manifests.1, &parse_grants(&next.grants));
    let grown = (after.iter()).any(|(kind, patterns)| {
        let held = before.get(kind);
        (patterns.iter()).any(|pattern| held.is_none_or(|held| !held_covers(held, pattern)))
    });
    if grown && current.state == InstallState::Active {
        next.widened = Some(Widening {
            at_ms: caller.now_ms,
            by: caller.actor.node_id,
            extends: current.revision,
        });
    }
}

/// The active installs whose manifest declares event triggers, with their coverage.
pub fn targets(config: &RealmConfigDocument) -> Vec<Target> {
    (config.installs.iter())
        .filter(|install| install.state == InstallState::Active)
        .filter_map(|install| {
            let plugin = pick_version(config, &install.plugin, Some(&install.digest)).ok()?;
            let manifest = read_manifest(&plugin).ok()?;
            let coverage = coverage(&manifest, &parse_grants(&install.grants));
            (!coverage.is_empty()).then_some(Target {
                install_id: install.install_id,
                group_id: install.group_id,
                revision: install.revision,
                coverage,
                account: install.account,
            })
        })
        .collect()
}

/// The installs this node matches pending rows to, and whether it keeps its journal: a user
/// node keeps none, so its rows are deleted unmatched.
pub fn match_targets(config: &RealmConfigDocument, local: NodeId) -> (Vec<Target>, bool) {
    let keep = !matches!(node_kind(config, local), Some(RealmNodeKind::User { .. }));
    let targets = match keep {
        true => targets(config),
        false => Vec::new(),
    };
    (targets, keep)
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
