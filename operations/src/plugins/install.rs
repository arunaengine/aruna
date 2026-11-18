//! Installs a published plugin into a group: the record `preparing` first, then account and role,
//! activation last. Each step checks current state, so a retry with the same id resumes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::plugin_installs::{
    BoundScope, InstallState, PluginInstall, Prerequisite, SecretBinding,
};
use aruna_core::plugins::PublishedPlugin;
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission as Access};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use aruna_plugins::artifact::ArtifactDigest;
use aruna_plugins::manifest::Manifest;
use aruna_plugins::permission::{Grant, Permission};
use serde_json::{Map, Value};
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use super::account::{
    AccountChange, AccountError, CheckReady, Identity, MakeReady, ReadGroupAuth, StepError,
    Teardown,
};
use super::bound::{BindError, BindPlace, BindSlots, BoundSlots, vault_kind};
use super::grants::covers;
use super::grants::{
    GrantError, Grants, InstallScope, accesses, carry_grants, check_supported, narrow_grants,
    parse_grants, patterns_for, role_paths, stored_grants, writes,
};
use super::receiver::pick_receiver;
use super::records::{InstallEntryConfig, InstallEntryOperation, RecordError};
use super::rules::{EnsureRules, RetireRules, RulesFailure};
use super::settings::{SettingError, check_settings};
use crate::auth::enforcement::{group_from_path, policy_reads, read_policies};
use crate::auth::permission_rules::{PermissionRules, read_rules, rule_reads};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::users::service_account::check::{
    ServiceCheckConfig, ServiceCheckError, ServiceCheckOperation,
};

#[derive(Debug, Error, PartialEq)]
pub enum InstallError {
    #[error("caller may not administer this group's plugins")]
    Unauthorized,
    #[error("no published version of this plugin matches")]
    NotPublished,
    #[error("no install with this id")]
    NotFound,
    #[error("the published manifest cannot be read: {0}")]
    Manifest(String),
    #[error(transparent)]
    Grant(#[from] GrantError),
    #[error("settings: {0}")]
    Setting(#[from] SettingError),
    #[error("the caller does not hold {0} and cannot grant it")]
    NotHeld(String),
    #[error("an install with this id exists for another request")]
    Changed,
    #[error("the install is {0}")]
    State(&'static str),
    #[error(transparent)]
    Record(#[from] RecordError),
    #[error("the install stopped at step {step}: {error}")]
    Step { step: &'static str, error: String },
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    Binding(String),
}

impl From<IoError> for InstallError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

impl From<GetConfigError> for InstallError {
    fn from(error: GetConfigError) -> Self {
        Self::Unavailable(format!("realm config: {error:?}"))
    }
}

impl From<BindError> for InstallError {
    fn from(error: BindError) -> Self {
        match error {
            BindError::Invalid(why) => Self::Binding(why),
            BindError::Unavailable(why) => Self::Unavailable(why),
        }
    }
}

impl From<StepError> for InstallError {
    fn from(StepError { step, error }: StepError) -> Self {
        Self::Step { step, error }
    }
}

/// What a group admin asks to install.
#[derive(Clone, Debug, PartialEq)]
pub struct InstallRequest {
    pub plugin: String,
    pub group_id: GroupId,
    /// The published digest to pin; `None` takes the newest published version.
    pub version: Option<String>,
    pub settings: Map<String, Value>,
    /// Narrowed grants by permission; a permission left out gets all of the group.
    pub grants: BTreeMap<String, Vec<String>>,
    /// The id a preview answered; installing with it again resumes that install.
    pub install_id: Option<Ulid>,
    /// `scope:name` of the secret per slot; a slot left out uses the same name in the group.
    pub secrets: BTreeMap<String, String>,
}

/// A checked request: what the summary shows and the install then writes.
#[derive(Clone, Debug, PartialEq)]
pub struct Prepared {
    pub scope: InstallScope,
    pub plugin: PublishedPlugin,
    pub manifest: Manifest,
    pub settings: String,
    pub grants: Grants,
    /// The secret each slot uses, checked where it lives.
    pub secrets: BTreeMap<String, SecretBinding>,
    /// What the admin should know, such as rules only a realm admin can add.
    pub warnings: Vec<String>,
    /// Digest of the checked request; a retry with the same id must match it.
    pub request: String,
}

/// Who changes an install, and when the change was asked for.
#[derive(Clone, Debug, PartialEq)]
pub struct InstallCaller {
    pub actor: Actor,
    pub auth: AuthContext,
    pub now_ms: u64,
}

impl InstallCaller {
    pub fn new(actor: &Actor, auth: &AuthContext, now_ms: u64) -> Self {
        Self {
            actor: actor.clone(),
            auth: auth.clone(),
            now_ms,
        }
    }

    /// The change of `install`'s account and role by this caller.
    pub(crate) fn change(&self, install: &PluginInstall) -> AccountChange {
        let who = (&self.actor, &self.auth);
        AccountChange::of(who, install, self.now_ms / 1000)
    }

    /// Writes the record as the caller, expecting the revision it read.
    pub(crate) fn write(
        &self,
        expected: Option<u64>,
        install: &PluginInstall,
        audit: Option<AdminAuditRecord>,
    ) -> IoCall {
        IoCall::child(InstallEntryOperation::new(InstallEntryConfig {
            actor: self.actor.clone(),
            auth_context: self.auth.clone(),
            install_id: install.install_id,
            group_id: install.group_id,
            expected,
            install: Some(install.clone()),
            audit,
        }))
    }

    /// The audit row of `action` on `install` by this caller.
    pub(crate) fn audit(
        &self,
        action: AdminAuditAction,
        install: &PluginInstall,
    ) -> AdminAuditRecord {
        AdminAuditRecord::for_install(action, self.actor.user_id, install)
    }

    /// Removes the rules of a removing or abandoned install from its group secrets.
    pub(crate) fn retire(&self, install: &PluginInstall) -> IoCall {
        IoCall::child(RetireRules::new(&self.actor, install, self.now_ms))
    }
}

/// The answer of a [`InstallCaller::write`] call.
pub(crate) fn written(result: IoResult) -> Result<(), InstallError> {
    result.child::<Option<PluginInstall>, RecordError>()??;
    Ok(())
}

pub(crate) fn config_call(realm_id: RealmId) -> IoCall {
    IoCall::child(GetConfigOperation::new(realm_id))
}

pub(crate) fn config_answer(result: IoResult) -> Result<RealmConfigDocument, InstallError> {
    Ok(result.child::<RealmConfigDocument, GetConfigError>()??)
}

/// Asks whether the caller is an admin of the group.
pub(crate) fn admin_call(auth: &AuthContext, group_id: GroupId) -> IoCall {
    IoCall::child(ServiceCheckOperation::new(ServiceCheckConfig {
        auth_context: auth.clone(),
        group_id,
        target: None,
    }))
}

/// Refuses a caller who is not an admin of the group.
pub(crate) fn admin_answer(result: IoResult) -> Result<(), InstallError> {
    (result.child::<Option<User>, ServiceCheckError>()?)
        .map(drop)
        .map_err(|_| InstallError::Unauthorized)
}

fn find_install(config: &RealmConfigDocument, id: Ulid) -> Option<&PluginInstall> {
    (config.installs.iter()).find(|install| install.install_id == id)
}

/// A request whose version, settings and grants passed, before its secrets are bound.
#[derive(Debug, PartialEq)]
struct CheckedRequest {
    config: Arc<RealmConfigDocument>,
    plugin: PublishedPlugin,
    manifest: Manifest,
    settings: String,
    scope: InstallScope,
    grants: Grants,
}

#[derive(Debug, PartialEq)]
enum PreparePhase {
    Admin,
    Config,
    Holder(Box<CheckedRequest>),
    Binding(Box<CheckedRequest>),
}

/// Checks a request as a group admin and writes nothing; a preview answers with this. Without
/// a version, a retry of an existing install keeps its digest and a new install takes the newest.
#[derive(Debug, PartialEq)]
pub struct PrepareInstall {
    auth: AuthContext,
    realm_id: RealmId,
    local: Option<NodeId>,
    request: InstallRequest,
    phase: PreparePhase,
}

impl PrepareInstall {
    pub fn new(
        auth: &AuthContext,
        (realm_id, local): (RealmId, Option<NodeId>),
        request: &InstallRequest,
    ) -> Self {
        Self {
            auth: auth.clone(),
            realm_id,
            local,
            request: request.clone(),
            phase: PreparePhase::Admin,
        }
    }

    fn existing<'a>(&self, config: &'a RealmConfigDocument) -> Option<&'a PluginInstall> {
        (self.request.install_id).and_then(|id| find_install(config, id))
    }

    fn checked(&self, config: RealmConfigDocument) -> Result<CheckedRequest, InstallError> {
        let request = &self.request;
        let existing = self.existing(&config);
        let pinned = existing.map(|install| install.digest.as_str());
        let plugin = pick_version(
            &config,
            &request.plugin,
            request.version.as_deref().or(pinned),
        )?;
        let manifest = read_manifest(&plugin)?;
        check_supported(&manifest)?;
        let settings = check_settings(&manifest, &request.settings)?;
        let scope = InstallScope {
            realm_id: self.realm_id,
            group_id: request.group_id,
            install_id: request.install_id.unwrap_or_else(Ulid::generate),
        };
        let grants = narrow_grants(&manifest, &scope, &request.grants)?;
        Ok(CheckedRequest {
            config: Arc::new(config),
            plugin,
            manifest,
            settings,
            scope,
            grants,
        })
    }

    /// Binds the slots; node secrets are checked on the receiver, so it is chosen first.
    fn bind(&self, checked: &CheckedRequest) -> IoCall {
        let config = &checked.config;
        let group_id = self.request.group_id;
        let receiver = (self.existing(config).map(|install| install.receiver))
            .or_else(|| pick_receiver(config, self.realm_id, group_id));
        let place = BindPlace {
            group_id,
            local: self.local,
            receiver,
        };
        let requested = &self.request.secrets;
        let bind = BindSlots::new(config.clone(), place, &checked.manifest, requested);
        IoCall::child(bind)
    }

    fn prepared(&self, checked: CheckedRequest, bound: BoundSlots) -> Prepared {
        let CheckedRequest {
            plugin,
            manifest,
            settings,
            scope,
            grants,
            ..
        } = checked;
        let (secrets, warnings) = bound;
        let request = request_digest(&plugin.digest, &scope, &settings, (&grants, &secrets));
        Prepared {
            scope,
            plugin,
            manifest,
            settings,
            grants,
            secrets,
            warnings,
            request,
        }
    }
}

impl Operation for PrepareInstall {
    type Output = Prepared;
    type Error = InstallError;

    fn first(&mut self) -> Result<Next<Prepared>, InstallError> {
        Ok(Next::Call(admin_call(&self.auth, self.request.group_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Prepared>, InstallError> {
        match std::mem::replace(&mut self.phase, PreparePhase::Config) {
            PreparePhase::Admin => {
                admin_answer(result)?;
                Ok(Next::Call(config_call(self.realm_id)))
            }
            PreparePhase::Config => {
                let checked = self.checked(config_answer(result)?)?;
                let what = (&checked.scope, &checked.grants);
                let holder = CheckHolder::new(&self.auth, what, &checked.config);
                self.phase = PreparePhase::Holder(Box::new(checked));
                Ok(Next::Call(IoCall::child(holder)))
            }
            PreparePhase::Holder(checked) => {
                result.child::<(), InstallError>()??;
                let bind = self.bind(&checked);
                self.phase = PreparePhase::Binding(checked);
                Ok(Next::Call(bind))
            }
            PreparePhase::Binding(checked) => {
                let bound = result.child::<BoundSlots, BindError>()??;
                Ok(Next::Done(self.prepared(*checked, bound)))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum InstallPhase {
    Config,
    Ready(Box<PluginInstall>),
    Written(Box<PluginInstall>),
    Settling,
}

/// Installs a checked request, or resumes the install with its id. Answers once it is settled;
/// an existing settled install is checked again before it is answered.
#[derive(Debug, PartialEq)]
pub struct InstallPrepared {
    caller: InstallCaller,
    prepared: Box<Prepared>,
    phase: InstallPhase,
}

impl InstallPrepared {
    pub fn new(caller: InstallCaller, prepared: Prepared) -> Self {
        Self {
            caller,
            prepared: Box::new(prepared),
            phase: InstallPhase::Config,
        }
    }

    fn settle(&mut self, record: PluginInstall) -> Next<PluginInstall> {
        let audit = Some(AdminAuditAction::PluginInstalled);
        let settle = SettleInstall::new(self.caller.clone(), record, audit);
        self.phase = InstallPhase::Settling;
        Next::Call(IoCall::child(settle))
    }

    fn start(&mut self, config: RealmConfigDocument) -> Result<Next<PluginInstall>, InstallError> {
        let scope = self.prepared.scope;
        match find_install(&config, scope.install_id) {
            Some(existing) if existing.request != self.prepared.request => {
                Err(InstallError::Changed)
            }
            Some(existing) if existing.state == InstallState::Removing => {
                Err(InstallError::State("being removed"))
            }
            Some(existing) if existing.state != InstallState::Preparing => {
                let paths = role_paths(&parse_grants(&existing.grants));
                let check = CheckReady::new(scope, Identity::of(existing), paths);
                self.phase = InstallPhase::Ready(Box::new(existing.clone()));
                Ok(Next::Call(IoCall::child(check)))
            }
            Some(existing) => Ok(self.settle(existing.clone())),
            None => {
                let receiver = pick_receiver(&config, scope.realm_id, scope.group_id);
                let receiver = receiver.ok_or_else(|| {
                    let why = "no eligible server or management node holds the group's metadata";
                    InstallError::Unavailable(why.into())
                })?;
                let record = new_record(&self.caller.actor, &self.prepared, receiver);
                let write = self.caller.write(None, &record, None);
                self.phase = InstallPhase::Written(Box::new(record));
                Ok(Next::Call(write))
            }
        }
    }
}

impl Operation for InstallPrepared {
    type Output = PluginInstall;
    type Error = InstallError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginInstall(self.prepared.scope.install_id))
    }

    fn first(&mut self) -> Result<Next<PluginInstall>, InstallError> {
        Ok(Next::Call(config_call(self.caller.actor.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, InstallError> {
        match std::mem::replace(&mut self.phase, InstallPhase::Settling) {
            InstallPhase::Config => self.start(config_answer(result)?),
            InstallPhase::Ready(existing) => {
                let ready = result.child::<Vec<Prerequisite>, AccountError>()?;
                ready.map_err(|error| InstallError::Step {
                    step: "check",
                    error: error.0,
                })?;
                Ok(Next::Done(*existing))
            }
            InstallPhase::Written(record) => {
                written(result)?;
                Ok(self.settle(*record))
            }
            InstallPhase::Settling => {
                let settled = result.child::<PluginInstall, InstallError>()??;
                let (id, plugin) = (settled.install_id, &settled.plugin);
                info!(%id, %plugin, version = %settled.version, "Installed a plugin");
                Ok(Next::Done(settled))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum SettlePhase {
    Config,
    Account(Box<Manifest>),
    Rules(Box<Manifest>, Vec<Prerequisite>),
    Write(Box<PluginInstall>),
    Abandon(Box<InstallError>),
}

/// Makes a `preparing` record's account and role match it, then writes its settled state:
/// blocked while something required is not granted, else paused or active. A failure undoes
/// the account and role, but only while the record is still the revision this call wrote.
#[derive(Debug, PartialEq)]
pub(crate) struct SettleInstall {
    caller: InstallCaller,
    record: PluginInstall,
    audit: Option<AdminAuditAction>,
    phase: SettlePhase,
}

impl SettleInstall {
    pub(crate) fn new(
        caller: InstallCaller,
        record: PluginInstall,
        audit: Option<AdminAuditAction>,
    ) -> Self {
        Self {
            caller,
            record,
            audit,
            phase: SettlePhase::Config,
        }
    }

    fn abandon(&mut self, step: &'static str, error: String) -> Next<PluginInstall> {
        let failed = InstallError::Step { step, error };
        self.phase = SettlePhase::Abandon(Box::new(failed));
        let abandon = AbandonInstall::new(self.caller.clone(), &self.record, step);
        Next::Call(IoCall::child(abandon))
    }

    fn settled(&self, manifest: &Manifest, prerequisites: Vec<Prerequisite>) -> PluginInstall {
        let record = &self.record;
        let scope = scope_of(self.caller.actor.realm_id, record);
        let (_, missing) = carry_grants(&parse_grants(&record.grants), manifest, &scope);
        let mut next = record.clone();
        next.state = match (missing.is_empty(), record.paused) {
            (false, _) => InstallState::Blocked,
            (true, true) => InstallState::Paused,
            (true, false) => InstallState::Active,
        };
        if next.state == InstallState::Active && next.active_since_ms.is_none() {
            next.active_since_ms = Some(self.caller.now_ms);
            next.activated_by = Some(self.caller.actor.node_id);
        }
        next.failed = None;
        next.prerequisites = prerequisites;
        next.revision += 1;
        next
    }
}

impl Operation for SettleInstall {
    type Output = PluginInstall;
    type Error = InstallError;

    fn first(&mut self) -> Result<Next<PluginInstall>, InstallError> {
        Ok(Next::Call(config_call(self.caller.actor.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, InstallError> {
        match std::mem::replace(&mut self.phase, SettlePhase::Config) {
            SettlePhase::Config => {
                let config = config_answer(result)?;
                let record = &self.record;
                let plugin = pick_version(&config, &record.plugin, Some(&record.digest))?;
                let manifest = read_manifest(&plugin)?;
                let ready = MakeReady::new(self.caller.change(record), record);
                self.phase = SettlePhase::Account(Box::new(manifest));
                Ok(Next::Call(IoCall::child(ready)))
            }
            SettlePhase::Account(manifest) => {
                match result.child::<Vec<Prerequisite>, StepError>()? {
                    Err(StepError { step, error }) => Ok(self.abandon(step, error)),
                    Ok(prerequisites) => {
                        let (actor, now_ms) = (&self.caller.actor, self.caller.now_ms);
                        match EnsureRules::new(actor, &self.record, &manifest, now_ms) {
                            Err(error) => Ok(self.abandon("rules", error)),
                            Ok(rules) => {
                                self.phase = SettlePhase::Rules(manifest, prerequisites);
                                Ok(Next::Call(IoCall::child(rules)))
                            }
                        }
                    }
                }
            }
            SettlePhase::Rules(manifest, prerequisites) => {
                if let Err(error) = result.child::<(), RulesFailure>()? {
                    return Ok(self.abandon("rules", error.0));
                }
                let next = self.settled(&manifest, prerequisites);
                let audit = self.audit.map(|action| self.caller.audit(action, &next));
                let write = self.caller.write(Some(self.record.revision), &next, audit);
                self.phase = SettlePhase::Write(Box::new(next));
                Ok(Next::Call(write))
            }
            SettlePhase::Write(next) => {
                written(result)?;
                Ok(Next::Done(*next))
            }
            SettlePhase::Abandon(failed) => Err(*failed),
        }
    }
}

#[derive(Debug, PartialEq)]
enum AbandonPhase {
    Config,
    Teardown,
    Rules,
    Record,
}

/// Undoes a failed preparation and records the failed step, unless another change of the
/// install moved on meanwhile; the undo writes themselves refuse a record that moved on.
#[derive(Debug, PartialEq)]
struct AbandonInstall {
    caller: InstallCaller,
    record: PluginInstall,
    step: &'static str,
    phase: AbandonPhase,
}

impl AbandonInstall {
    fn new(caller: InstallCaller, record: &PluginInstall, step: &'static str) -> Self {
        Self {
            caller,
            record: record.clone(),
            step,
            phase: AbandonPhase::Config,
        }
    }
}

impl Operation for AbandonInstall {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(config_call(self.caller.actor.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        let id = self.record.install_id;
        match std::mem::replace(&mut self.phase, AbandonPhase::Record) {
            AbandonPhase::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.ok();
                let current = config.as_ref().and_then(|config| find_install(config, id));
                if current != Some(&self.record) {
                    let step = self.step;
                    warn!(%id, step, "A plugin install changed meanwhile; leaving it as it is");
                    return Ok(Next::Done(()));
                }
                self.phase = AbandonPhase::Teardown;
                let teardown = Teardown::new(self.caller.change(&self.record));
                Ok(Next::Call(IoCall::child(teardown)))
            }
            AbandonPhase::Teardown => {
                if let Err(error) = result.child::<(), AccountError>()? {
                    warn!(%id, %error, "Could not undo a failed plugin install step");
                }
                // Only an install that never became active loses its rules here.
                self.phase = AbandonPhase::Rules;
                Ok(Next::Call(self.caller.retire(&self.record)))
            }
            AbandonPhase::Rules => {
                if let Err(error) = result.child::<(), RulesFailure>()? {
                    warn!(%error, "Could not remove a plugin install's use rules");
                }
                let mut failed = self.record.clone();
                failed.failed = Some(self.step.to_string());
                failed.revision += 1;
                self.phase = AbandonPhase::Record;
                let expected = Some(self.record.revision);
                Ok(Next::Call(self.caller.write(expected, &failed, None)))
            }
            AbandonPhase::Record => {
                if let Err(error) = written(result) {
                    warn!(%id, %error, "Could not record the failed plugin install step");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum HolderPhase {
    Rules,
    Group,
    Policy,
}

/// The pattern under check, with the access the pending policy read decides.
#[derive(Debug, PartialEq)]
struct HeldPattern {
    permission: Permission,
    pattern: String,
    accesses: VecDeque<Access>,
    asked: Option<Access>,
}

/// Refuses grants the caller does not hold now: each action they allow, with its own access and
/// operation `plugin.<permission>`, against roles, denies below a subtree, restrictions and
/// policies. Any policy reading the path refuses every subtree, even one guarded off for plugins.
#[derive(Debug, PartialEq)]
pub(crate) struct CheckHolder {
    auth: AuthContext,
    scope: InstallScope,
    patterns: VecDeque<(Permission, String)>,
    path_policies: bool,
    rules: Option<PermissionRules>,
    current: Option<HeldPattern>,
    phase: HolderPhase,
}

impl CheckHolder {
    pub(crate) fn new(
        auth: &AuthContext,
        (scope, grants): (&InstallScope, &Grants),
        config: &RealmConfigDocument,
    ) -> Self {
        let patterns = (Permission::ALL.into_iter())
            .flat_map(|permission| {
                let patterns = patterns_for(grants, permission);
                patterns
                    .into_iter()
                    .map(move |pattern| (permission, pattern))
            })
            .collect();
        Self {
            auth: auth.clone(),
            scope: *scope,
            patterns,
            path_policies: config
                .request_policies
                .iter()
                .any(|policy| policy.reads_path()),
            rules: None,
            current: None,
            phase: HolderPhase::Rules,
        }
    }

    /// Takes up the next pattern, refusing a subtree any policy could decide by path.
    fn next_pattern(&mut self) -> Result<bool, InstallError> {
        let Some((permission, pattern)) = self.patterns.pop_front() else {
            return Ok(false);
        };
        if pattern.ends_with("**") && self.path_policies {
            let why = "a request policy reads the path, so grant exact paths";
            let what = format!("{permission} on {pattern} ({why})");
            return Err(InstallError::NotHeld(what));
        }
        self.current = Some(HeldPattern {
            permission,
            pattern,
            accesses: accesses(permission).to_vec().into(),
            asked: None,
        });
        Ok(true)
    }

    /// Decides the next access by role and reads the policies that decide it too.
    fn advance(&mut self) -> Result<Next<()>, InstallError> {
        loop {
            let rules = self.rules.as_ref().ok_or(IoError::Unfinished)?;
            let pending = self
                .current
                .as_mut()
                .filter(|held| !held.accesses.is_empty());
            let Some(held) = pending else {
                if !self.next_pattern()? {
                    return Ok(Next::Done(()));
                }
                continue;
            };
            let access = held.accesses.pop_front().ok_or(IoError::Unfinished)?;
            let holds = match held.pattern.strip_suffix("**") {
                Some(base) => rules.holds_below(base, &access),
                None => rules.allows(&held.pattern, &access),
            };
            if !holds {
                return Err(InstallError::NotHeld(what_held(held, &access)));
            }
            let group = group_from_path(&held.pattern);
            held.asked = Some(access);
            self.phase = HolderPhase::Policy;
            let reads = policy_reads(self.scope.realm_id, group);
            return Ok(Next::Call(IoCall::Read(reads)));
        }
    }

    fn policy_answer(&mut self, result: IoResult) -> Result<(), InstallError> {
        let held = self.current.as_mut().ok_or(IoError::Unfinished)?;
        let access = held.asked.take().ok_or(IoError::Unfinished)?;
        let extras = PolicyRequestExtras::operation(format!("plugin.{}", held.permission));
        let request = policy_request_with(&held.pattern, &access, Some(&self.auth), extras);
        let group = group_from_path(&held.pattern);
        (read_policies(self.scope.realm_id, group, result.values()?))
            .and_then(|policies| policies.evaluate(&request))
            .map_err(|error| {
                InstallError::NotHeld(format!("{} ({error})", what_held(held, &access)))
            })
    }
}

fn what_held(held: &HeldPattern, access: &Access) -> String {
    format!("{} ({access}) on {}", held.permission, held.pattern)
}

impl Operation for CheckHolder {
    type Output = ();
    type Error = InstallError;

    fn first(&mut self) -> Result<Next<()>, InstallError> {
        let reads = rule_reads(&self.scope.group());
        let reads = reads.map_err(|error| InstallError::Unavailable(error.to_string()))?;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, InstallError> {
        match self.phase {
            HolderPhase::Rules => {
                let rules = read_rules(&self.auth, result.values()?);
                let rules = rules.map_err(|error| InstallError::Unavailable(error.to_string()))?;
                self.rules = Some(rules);
                self.phase = HolderPhase::Group;
                Ok(Next::Call(IoCall::child(ReadGroupAuth(
                    self.scope.group_id,
                ))))
            }
            HolderPhase::Group => {
                let group = result.child::<Option<GroupAuthorizationDocument>, AccountError>()?;
                let group = group.map_err(|error| InstallError::Unavailable(error.0))?;
                let mut policies = group.iter().flat_map(|auth| auth.policies.iter());
                self.path_policies |= policies.any(|policy| policy.reads_path());
                self.advance()
            }
            HolderPhase::Policy => {
                self.policy_answer(result)?;
                self.advance()
            }
        }
    }
}

/// The record of a new install. The account and role ids are fresh, never taken from the
/// caller, so no existing user or role can be adopted or replaced.
fn new_record(actor: &Actor, prepared: &Prepared, receiver: NodeId) -> PluginInstall {
    let scope = &prepared.scope;
    PluginInstall {
        install_id: scope.install_id,
        group_id: scope.group_id,
        plugin: prepared.plugin.name.clone(),
        version: prepared.plugin.version.clone(),
        digest: prepared.plugin.digest.clone(),
        state: InstallState::Preparing,
        paused: false,
        settings: prepared.settings.clone(),
        grants: stored_grants(&prepared.grants),
        account: UserId::local(Ulid::generate(), scope.realm_id),
        role_id: Ulid::generate(),
        receiver,
        granted_by: actor.user_id,
        request: prepared.request.clone(),
        revision: 1,
        active_since_ms: None,
        failed: None,
        prerequisites: Vec::new(),
        activated_by: None,
        widened: None,
        secrets: prepared.secrets.clone(),
    }
}

pub(crate) fn scope_of(realm_id: RealmId, install: &PluginInstall) -> InstallScope {
    InstallScope {
        realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    }
}

/// The published version with `digest`, or the newest one of `name`.
pub fn pick_version(
    config: &RealmConfigDocument,
    name: &str,
    digest: Option<&str>,
) -> Result<PublishedPlugin, InstallError> {
    let mut versions = config.plugins.iter().filter(|plugin| plugin.name == name);
    let found = match digest {
        Some(digest) => versions.find(|plugin| plugin.digest == digest),
        None => versions.max_by_key(|plugin| (plugin.published_at_ms, plugin.digest.clone())),
    };
    found.cloned().ok_or(InstallError::NotPublished)
}

pub fn read_manifest(plugin: &PublishedPlugin) -> Result<Manifest, InstallError> {
    serde_json::from_str(&plugin.manifest)
        .map_err(|error| InstallError::Manifest(error.to_string()))
}

fn request_digest(
    digest: &str,
    scope: &InstallScope,
    settings: &str,
    (grants, secrets): (&Grants, &BTreeMap<String, SecretBinding>),
) -> String {
    let group = scope.group_id.to_string();
    let mut request = serde_json::json!([digest, group, settings, stored_grants(grants)]);
    // Bindings join the digest only when there are any, so earlier installs keep theirs.
    if let (false, Some(parts)) = (secrets.is_empty(), request.as_array_mut()) {
        let bound: BTreeMap<&String, String> = (secrets.iter())
            .map(|(slot, binding)| (slot, binding.to_string()))
            .collect();
        parts.push(serde_json::json!(bound));
    }
    ArtifactDigest::of(request.to_string().as_bytes()).to_string()
}

/// One grant line: `read`, or `write` when the plugin writes, and its granted patterns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionLine {
    pub permission: String,
    pub required: bool,
    pub granted: Vec<String>,
}

/// One declared service, its methods and whether the install may call it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionLine {
    pub name: String,
    pub url: String,
    pub methods: Vec<String>,
    pub required: bool,
    pub granted: bool,
    /// The plugin downloads, and the node adds a secret to requests of this service.
    pub stored: bool,
}

/// One secret slot: its kind, the secret it uses and whether the code reads the value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretLine {
    pub slot: String,
    pub kind: String,
    pub secret: String,
    pub value: bool,
}

/// What an admin approves: the version, the permissions and connections, and the triggers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub id: String,
    pub plugin: String,
    pub title: Option<String>,
    pub version: String,
    pub digest: String,
    pub group: String,
    pub permissions: Vec<PermissionLine>,
    pub connections: Vec<ConnectionLine>,
    pub triggers: Vec<String>,
    pub secrets: Vec<SecretLine>,
    pub warnings: Vec<String>,
}

/// The summary of a checked install request, before anything is written.
pub fn prepared_summary(prepared: &Prepared) -> Summary {
    let grants = stored_grants(&prepared.grants);
    let granted = (&grants, &prepared.secrets);
    summary_of(
        &prepared.manifest,
        &prepared.plugin.digest,
        &prepared.scope,
        granted,
    )
}

/// The summary of a stored install, from the record alone while the pinned manifest is missing.
pub fn installed_summary(config: &RealmConfigDocument, install: &PluginInstall) -> Summary {
    let manifest = pick_version(config, &install.plugin, Some(&install.digest))
        .ok()
        .and_then(|plugin| read_manifest(&plugin).ok());
    let scope = InstallScope {
        realm_id: config.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    match manifest {
        Some(manifest) => {
            let granted = (&install.grants, &install.secrets);
            summary_of(&manifest, &install.digest, &scope, granted)
        }
        None => bare_summary(install),
    }
}

/// The summary of `grants` against the manifest's requests.
fn summary_of(
    manifest: &Manifest,
    digest: &str,
    scope: &InstallScope,
    (grants, bound): (
        &BTreeMap<String, Vec<String>>,
        &BTreeMap<String, SecretBinding>,
    ),
) -> Summary {
    let granted = parse_grants(grants);
    let requested = [Grant::Read]
        .into_iter()
        .chain(writes(manifest).then_some(Grant::Write));
    let permissions = requested
        .map(|grant| PermissionLine {
            permission: grant.to_string(),
            required: true,
            granted: (granted.get(&grant).into_iter().flatten())
                .cloned()
                .collect(),
        })
        .collect();
    let sends = granted.get(&Grant::Services);
    let values = granted.get(&Grant::Secrets);
    let readable = |slot: &str| {
        let path = scope.secret(slot);
        values.is_some_and(|patterns| patterns.iter().any(|pattern| covers(pattern, &path)))
    };
    let downloads = (manifest.requested()).contains(&Permission::TransfersDownload);
    let connections = (manifest.connections.iter())
        .map(|(name, connection)| ConnectionLine {
            name: name.clone(),
            url: connection.url.clone(),
            methods: (connection.methods.iter())
                .map(|method| method.as_str().to_string())
                .collect(),
            required: !connection.optional,
            granted: sends.is_some_and(|paths| paths.contains(&scope.connection(name))),
            stored: downloads && connection.auth.is_some(),
        })
        .collect();
    Summary {
        id: scope.install_id.to_string(),
        plugin: manifest.name.clone(),
        title: manifest.title.clone(),
        version: manifest.version.clone(),
        digest: digest.to_string(),
        group: scope.group_id.to_string(),
        permissions,
        connections,
        triggers: manifest
            .triggers
            .keys()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        secrets: (bound.iter())
            .map(|(slot, binding)| SecretLine {
                slot: slot.clone(),
                kind: (manifest.secrets.get(slot))
                    .and_then(|declared| vault_kind(declared.kind))
                    .map_or_else(String::new, |kind| kind.as_str().to_string()),
                secret: binding.to_string(),
                value: readable(slot),
            })
            .collect(),
        warnings: (bound.iter())
            .filter_map(|(slot, binding)| {
                let shared = matches!(binding.scope, BoundScope::Realm | BoundScope::Node);
                shared
                    .then(|| format!("a realm admin adds the use rule of `{binding}` for `{slot}`"))
            })
            .collect(),
    }
}

/// A summary from the record alone.
fn bare_summary(install: &PluginInstall) -> Summary {
    let permissions = (install.grants.iter())
        .map(|(permission, granted)| PermissionLine {
            permission: permission.clone(),
            required: false,
            granted: granted.clone(),
        })
        .collect();
    Summary {
        id: install.install_id.to_string(),
        plugin: install.plugin.clone(),
        title: None,
        version: install.version.clone(),
        digest: install.digest.clone(),
        group: install.group_id.to_string(),
        permissions,
        connections: Vec::new(),
        triggers: Vec::new(),
        secrets: Vec::new(),
        warnings: Vec::new(),
    }
}

#[cfg(test)]
#[path = "install_tests.rs"]
pub(crate) mod tests;
