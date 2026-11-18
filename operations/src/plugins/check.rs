//! The check every plugin call passes on the receiver before a stored reply or a new effect.
//! Steps 0 to 3 decide on state read once for the call; step 4 authorizes as the account.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::PluginRun;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission as Access};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_plugins::broker::{ErrorKind, HostError};
use aruna_plugins::permission::Permission;
use ulid::Ulid;

use super::account::{read_user_call, read_user_result};
use super::grants::{InstallScope, accesses, covers, parse_grants, patterns_for};
use super::rows::StoreError;
use super::unavailable;
use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::enforcement::{group_from_path, policy_reads, read_policies};
use crate::auth::error::{AuthorizeError, check_error};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::document_repository::{GetConfigError, GetConfigOperation};

/// A host call's refusal or fault, as the operations behind the call report it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}", .0.message)]
pub struct CallError(pub HostError);

impl From<HostError> for CallError {
    fn from(error: HostError) -> Self {
        Self(error)
    }
}

impl From<CallError> for HostError {
    fn from(error: CallError) -> Self {
        error.0
    }
}

impl From<IoError> for CallError {
    fn from(error: IoError) -> Self {
        Self(unavailable(error.to_string()))
    }
}

impl From<StoreError> for CallError {
    fn from(error: StoreError) -> Self {
        Self(unavailable(error.0))
    }
}

/// What one run's calls are checked against: the run and the node that runs it.
#[derive(Clone, Debug, PartialEq)]
pub struct RunScope {
    pub realm_id: RealmId,
    pub local: NodeId,
    pub run: PluginRun,
}

/// Steps 0 to 2: this node runs the install, the account is the active account the record
/// names, and the record is active with the run's artifact.
pub fn check_run<'a>(
    scope: &RunScope,
    config: &'a RealmConfigDocument,
    account: Option<&User>,
) -> Result<&'a PluginInstall, HostError> {
    let run = (scope.run.install_id, scope.run.digest.as_str());
    check_install((config, scope.realm_id, scope.local), run, account)
}

/// Steps 0 to 2 for install `install_id` pinned at `digest`, run by `local`.
pub fn check_install<'a>(
    place: (&'a RealmConfigDocument, RealmId, NodeId),
    binding: (Ulid, &str),
    account: Option<&User>,
) -> Result<&'a PluginInstall, HostError> {
    let install = check_identity(place, binding, account)?;
    if install.state != InstallState::Active {
        return Err(denied(&format!(
            "the install is {}",
            install.state.as_str()
        )));
    }
    Ok(install)
}

/// The install whatever its state: this node runs it, its account is active and marked with
/// it, and it still pins `digest`.
pub fn check_identity<'a>(
    (config, realm_id, local): (&'a RealmConfigDocument, RealmId, NodeId),
    (install_id, digest): (Ulid, &str),
    account: Option<&User>,
) -> Result<&'a PluginInstall, HostError> {
    let install = (config.installs.iter())
        .find(|install| install.install_id == install_id)
        .ok_or_else(|| denied("the install is removed or conflicting"))?;
    if install.receiver != local {
        return Err(denied("this node does not run the install"));
    }
    let install_scope = InstallScope {
        realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    let active = account.is_some_and(|user| {
        user.user_id == install.account
            && super::account::owned(user, &install_scope)
            && !user.is_deactivated()
    });
    if !active {
        return Err(denied("the plugin account is not active"));
    }
    if install.digest != digest {
        return Err(denied("the install runs another version now"));
    }
    Ok(install)
}

/// Step 3: the granted patterns of `permission` that cover `target`.
pub fn matching(
    install: &PluginInstall,
    permission: Permission,
    target: &str,
) -> Result<BTreeSet<String>, HostError> {
    let grants = parse_grants(&install.grants);
    let patterns: BTreeSet<String> = (patterns_for(&grants, permission).into_iter())
        .filter(|pattern| covers(pattern, target))
        .collect();
    match patterns.is_empty() {
        true => Err(HostError::new(
            ErrorKind::NotGranted,
            format!("{permission} is not granted on {target}"),
        )),
        false => Ok(patterns),
    }
}

/// Refuses one authorization result: a fault of the check stays retryable.
fn refusal(error: AuthorizeError) -> CallError {
    CallError(match error {
        AuthorizeError::Storage(error) => unavailable(error.to_string()),
        AuthorizeError::CheckFailed(error) => unavailable(error),
        error => HostError::new(ErrorKind::Denied, error.to_string()),
    })
}

/// The account of an install restricted to `patterns` for `access`.
fn restricted(
    install: &PluginInstall,
    access: &Access,
    patterns: &BTreeSet<String>,
) -> AuthContext {
    let restrictions = (patterns.iter())
        .map(|pattern| PathRestriction {
            pattern: pattern.clone(),
            permission: access.clone(),
        })
        .collect();
    AuthContext {
        user_id: install.account,
        realm_id: install.account.realm_id,
        path_restrictions: Some(restrictions),
        session: None,
    }
}

/// Step 4: each of `accesses` on `target`, as the account restricted to `patterns`, against
/// roles, denies and policies with the operation `plugin.<permission>`.
#[derive(Debug, PartialEq)]
pub struct Authorize {
    account: PluginInstall,
    permission: Permission,
    target: String,
    patterns: BTreeSet<String>,
    accesses: VecDeque<Access>,
    current: Option<CheckPermissionsConfig>,
    rules_read: bool,
}

impl Authorize {
    /// The accesses `permission` needs on `target`.
    pub fn call(
        install: &PluginInstall,
        permission: Permission,
        target: &str,
        patterns: &BTreeSet<String>,
    ) -> Self {
        let accesses = accesses(permission).to_vec();
        Self::of(install, permission, accesses, target, patterns)
    }

    /// One access on `target` under the operation `plugin.<permission>`, such as READ on the
    /// source of a copy, whose target needs WRITE.
    pub fn access(
        install: &PluginInstall,
        (permission, access): (Permission, Access),
        target: &str,
        patterns: &BTreeSet<String>,
    ) -> Self {
        Self::of(install, permission, vec![access], target, patterns)
    }

    fn of(
        install: &PluginInstall,
        permission: Permission,
        accesses: Vec<Access>,
        target: &str,
        patterns: &BTreeSet<String>,
    ) -> Self {
        Self {
            account: install.clone(),
            permission,
            target: target.to_string(),
            patterns: patterns.clone(),
            accesses: accesses.into(),
            current: None,
            rules_read: false,
        }
    }

    fn realm(&self) -> RealmId {
        self.account.account.realm_id
    }

    /// Reads the rules of the next access, or ends when none is left.
    fn advance(&mut self) -> Result<Next<()>, CallError> {
        let Some(access) = self.accesses.pop_front() else {
            return Ok(Next::Done(()));
        };
        let config = CheckPermissionsConfig {
            auth_context: restricted(&self.account, &access, &self.patterns),
            path: self.target.clone(),
            required_permission: access,
        };
        let reads = config
            .reads()
            .map_err(|error| refusal(check_error(error)))?;
        self.current = Some(config);
        self.rules_read = false;
        Ok(Next::Call(IoCall::Read(reads)))
    }
}

impl Operation for Authorize {
    type Output = ();
    type Error = CallError;

    fn first(&mut self) -> Result<Next<()>, CallError> {
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CallError> {
        let config = self.current.as_ref().ok_or(IoError::Unfinished)?;
        let values = result.values()?;
        if !self.rules_read {
            let allowed = config.allowed(values);
            if !allowed.map_err(|error| refusal(check_error(error)))? {
                return Err(refusal(AuthorizeError::PermissionDenied));
            }
            self.rules_read = true;
            let reads = policy_reads(self.realm(), group_from_path(&self.target));
            return Ok(Next::Call(IoCall::Read(reads)));
        }
        let extras = PolicyRequestExtras::operation(format!("plugin.{}", self.permission));
        let auth = Some(&config.auth_context);
        let request = policy_request_with(&self.target, &config.required_permission, auth, extras);
        let group = group_from_path(&self.target);
        (read_policies(self.realm(), group, values))
            .and_then(|policies| policies.evaluate(&request))
            .map_err(|error| refusal(error.into()))?;
        self.advance()
    }
}

fn authorized(result: IoResult) -> Result<(), CallError> {
    result.child::<(), CallError>()?
}

/// The patterns a copy reads `source` and writes `target` under: read on the source and write
/// on the target, each authorized with the operation `plugin.objects.copy`.
#[derive(Debug, PartialEq)]
pub struct CopyGrants {
    install: PluginInstall,
    target: String,
    read: BTreeSet<String>,
    write: Option<BTreeSet<String>>,
    source: String,
}

impl CopyGrants {
    pub fn new(install: &PluginInstall, (source, target): (&str, &str)) -> Self {
        Self {
            install: install.clone(),
            target: target.to_string(),
            read: BTreeSet::new(),
            write: None,
            source: source.to_string(),
        }
    }
}

impl Operation for CopyGrants {
    type Output = BTreeSet<String>;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Self::Output>, CallError> {
        self.read = matching(&self.install, Permission::ObjectsRead, &self.source)?;
        let access = (Permission::ObjectsCopy, Access::READ);
        let check = Authorize::access(&self.install, access, &self.source, &self.read);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CallError> {
        authorized(result)?;
        if let Some(write) = self.write.take() {
            let read = std::mem::take(&mut self.read);
            return Ok(Next::Done(read.into_iter().chain(write).collect()));
        }
        let copy = Permission::ObjectsCopy;
        let write = matching(&self.install, copy, &self.target)?;
        let check = Authorize::access(&self.install, (copy, Access::WRITE), &self.target, &write);
        self.write = Some(write);
        Ok(Next::Call(IoCall::child(check)))
    }
}

#[derive(Debug, PartialEq)]
enum CheckPhase {
    Config,
    Account(Box<RealmConfigDocument>),
    Authorizing(Box<PluginInstall>),
}

/// Steps 0 to 4 for one call; `target` is the permission and resource a guarded call names.
/// State and log calls name none and pass steps 0 to 2 only.
#[derive(Debug, PartialEq)]
pub struct CheckCall {
    scope: RunScope,
    target: Option<(Permission, String)>,
    phase: CheckPhase,
}

impl CheckCall {
    pub fn new(scope: &RunScope, target: Option<(Permission, &str)>) -> Self {
        Self {
            scope: scope.clone(),
            target: target.map(|(permission, resource)| (permission, resource.to_string())),
            phase: CheckPhase::Config,
        }
    }

    /// Steps 0 to 2 with the account read, then step 3 and the authorization of step 4.
    fn decide(
        &mut self,
        config: &RealmConfigDocument,
        user: Option<User>,
    ) -> Result<Next<PluginInstall>, CallError> {
        let install = check_run(&self.scope, config, user.as_ref())?.clone();
        let Some((permission, resource)) = &self.target else {
            return Ok(Next::Done(install));
        };
        let patterns = matching(&install, *permission, resource)?;
        let check = Authorize::call(&install, *permission, resource, &patterns);
        self.phase = CheckPhase::Authorizing(Box::new(install));
        Ok(Next::Call(IoCall::child(check)))
    }
}

impl Operation for CheckCall {
    type Output = PluginInstall;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<PluginInstall>, CallError> {
        let read = GetConfigOperation::new(self.scope.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, CallError> {
        match std::mem::replace(&mut self.phase, CheckPhase::Config) {
            CheckPhase::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(config_unavailable)?;
                let install_id = self.scope.run.install_id;
                let account = (config.installs.iter())
                    .find(|install| install.install_id == install_id)
                    .map(|install| install.account);
                match account {
                    Some(account) => {
                        self.phase = CheckPhase::Account(Box::new(config));
                        Ok(Next::Call(read_user_call(account)))
                    }
                    None => self.decide(&config, None),
                }
            }
            CheckPhase::Account(config) => {
                let user = read_user_result(result).map_err(|error| unavailable(error.0))?;
                self.decide(&config, user)
            }
            CheckPhase::Authorizing(install) => {
                authorized(result)?;
                Ok(Next::Done(*install))
            }
        }
    }
}

/// A realm config that cannot be read, as a call sees it.
pub fn config_unavailable(error: GetConfigError) -> CallError {
    CallError(unavailable(format!("realm config: {error:?}")))
}

fn denied(message: &str) -> HostError {
    HostError::new(ErrorKind::Denied, message)
}

#[cfg(test)]
#[path = "check_tests.rs"]
mod tests;
