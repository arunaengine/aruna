//! The check every plugin call passes on the receiver before a stored reply or a new effect.
//! Steps 0 to 3 decide on state read once for the call; step 4 authorizes as the account.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::PluginRun;
use aruna_core::request_policy::{CompiledSets, RequestPolicy};
use aruna_core::structs::identity::auth::Permission as Access;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_core::types::{Key, KeySpace, Value};
use aruna_plugin_types::calls::{ErrorKind, HostError};
use aruna_plugin_types::permission::Permission;
use ulid::Ulid;

use super::rows::{StoreError, mapped_child};
use super::unavailable;
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::auth::install_access::{Authorize, granted};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::users::install_account::{owns_install, read_user_call, read_user_result};

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
        Self(unavailable(error.detail()))
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
pub(crate) fn check_run<'a>(
    scope: &RunScope,
    config: &'a RealmConfigDocument,
    account: Option<&User>,
) -> Result<&'a PluginInstall, HostError> {
    let run = (scope.run.install_id, scope.run.digest.as_str());
    check_install((config, scope.realm_id, scope.local), run, account)
}

/// Steps 0 to 2 for install `install_id` pinned at `digest`, run by `local`.
pub(crate) fn check_install<'a>(
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
pub(crate) fn check_identity<'a>(
    (config, _, local): (&'a RealmConfigDocument, RealmId, NodeId),
    (install_id, digest): (Ulid, &str),
    account: Option<&User>,
) -> Result<&'a PluginInstall, HostError> {
    let install = (config.installs.iter())
        .find(|install| install.install_id == install_id)
        .ok_or_else(|| denied("the install is removed or conflicting"))?;
    if install.receiver != local {
        return Err(denied("this node does not run the install"));
    }
    let active = account.is_some_and(|user| {
        user.user_id == install.account
            && owns_install(user, install.group_id, install.install_id)
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
    let patterns = granted(install, permission, target);
    match patterns.is_empty() {
        true => Err(HostError::new(
            ErrorKind::NotGranted,
            format!("{permission} is not granted on {target}"),
        )),
        false => Ok(patterns),
    }
}

/// The child that runs `check` and answers as a call: a fault of the check stays retryable.
pub(crate) fn authorize_call(check: Authorize) -> IoCall {
    mapped_child(check, |result| result.map_err(refusal))
}

/// Refuses one authorization result: a fault of the check stays retryable.
fn refusal(error: AuthorizeError) -> CallError {
    CallError(match error {
        AuthorizeError::Storage(error) => unavailable(error.to_string()),
        AuthorizeError::CheckFailed(error) => unavailable(error),
        error => HostError::new(ErrorKind::Denied, error.to_string()),
    })
}

fn authorized(result: IoResult) -> Result<(), CallError> {
    result.child::<(), CallError>()?
}

/// The rows a check read, by the keys it named.
type KeptRead = (Vec<(KeySpace, Key)>, Vec<Option<Value>>);

/// What the check in progress asked for, kept to remember its answer.
#[derive(Debug, PartialEq)]
enum Asked {
    Read(Vec<(KeySpace, Key)>),
    Compile(Vec<Vec<RequestPolicy>>),
    Other,
}

impl Asked {
    fn of(call: &IoCall) -> Self {
        match call {
            IoCall::Read(keys) => Self::Read(keys.clone()),
            IoCall::Effect(effect) => match &**effect {
                Effect::Storage(StorageEffect::CompilePolicies(sets)) => {
                    Self::Compile(sets.clone())
                }
                _ => Self::Other,
            },
            _ => Self::Other,
        }
    }
}

/// The next step of a shared check: a call to answer, or its decision as a call.
#[derive(Debug, PartialEq)]
pub(crate) enum Checked {
    Ask(IoCall),
    Done(Result<(), CallError>),
}

/// Runs authorizations in place and keeps the rows and compiled policies they read, so the
/// checks of one page read and compile them once. Each check still decides as [`Authorize`].
#[derive(Debug, Default, PartialEq)]
pub(crate) struct SharedChecks {
    check: Option<Authorize>,
    asked: Option<Asked>,
    reads: Vec<KeptRead>,
    compiled: Vec<(Vec<Vec<RequestPolicy>>, CompiledSets)>,
}

impl SharedChecks {
    /// Starts `check`, answered from what earlier checks read wherever it can be.
    pub(crate) fn start(&mut self, mut check: Authorize) -> Checked {
        let step = check.first();
        self.check = Some(check);
        self.step(step)
    }

    /// Hands the answer to the call of the last [`Checked::Ask`] to the check in progress.
    pub(crate) fn answer(&mut self, result: IoResult) -> Result<Checked, IoError> {
        let asked = self.asked.take().ok_or(IoError::Unfinished)?;
        let result = self.keep(asked, result);
        let check = self.check.as_mut().ok_or(IoError::Unfinished)?;
        let step = check.next(result);
        Ok(self.step(step))
    }

    fn step(&mut self, mut step: Result<Next<()>, AuthorizeError>) -> Checked {
        loop {
            let call = match step {
                Ok(Next::Call(call)) => call,
                Ok(Next::Done(())) => return self.decided(Ok(())),
                Ok(Next::Wait | Next::Then { .. }) => {
                    return self.decided(Err(IoError::Unfinished.into()));
                }
                Err(error) => return self.decided(Err(error)),
            };
            let Some(answer) = self.known(&call) else {
                self.asked = Some(Asked::of(&call));
                return Checked::Ask(call);
            };
            step = match self.check.as_mut() {
                Some(check) => check.next(answer),
                None => Err(IoError::Unfinished.into()),
            };
        }
    }

    fn decided(&mut self, result: Result<(), AuthorizeError>) -> Checked {
        self.check = None;
        Checked::Done(result.map_err(refusal))
    }

    /// The answer an earlier check already got for `call`.
    fn known(&self, call: &IoCall) -> Option<IoResult> {
        match call {
            IoCall::Read(keys) => (self.reads.iter())
                .find(|(read, _)| read == keys)
                .map(|(_, values)| IoResult::Values(values.clone())),
            IoCall::Effect(effect) => match &**effect {
                Effect::Storage(StorageEffect::CompilePolicies(sets)) => (self.compiled.iter())
                    .find(|(asked, _)| asked == sets)
                    .map(|(_, compiled)| {
                        let event = StorageEvent::PoliciesCompiled(compiled.clone());
                        IoResult::Event(Box::new(Event::Storage(event)))
                    }),
                _ => None,
            },
            _ => None,
        }
    }

    fn keep(&mut self, asked: Asked, result: IoResult) -> IoResult {
        match (asked, &result) {
            (Asked::Read(keys), IoResult::Values(values)) => {
                self.reads.push((keys, values.clone()));
            }
            (Asked::Compile(sets), IoResult::Event(event)) => {
                if let Event::Storage(StorageEvent::PoliciesCompiled(compiled)) = &**event {
                    self.compiled.push((sets, compiled.clone()));
                }
            }
            _ => {}
        }
        result
    }
}

/// The patterns a copy reads `source` and writes `target` under: read on the source and write
/// on the target, each authorized with the operation `plugin.objects.copy`.
#[derive(Debug, PartialEq)]
pub(crate) struct CopyGrants {
    install: PluginInstall,
    target: String,
    read: BTreeSet<String>,
    write: Option<BTreeSet<String>>,
    source: String,
}

impl CopyGrants {
    pub(crate) fn new(install: &PluginInstall, (source, target): (&str, &str)) -> Self {
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
        Ok(Next::Call(authorize_call(check)))
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
        Ok(Next::Call(authorize_call(check)))
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
pub(crate) struct CheckCall {
    scope: RunScope,
    target: Option<(Permission, String)>,
    phase: CheckPhase,
}

impl CheckCall {
    pub(crate) fn new(scope: &RunScope, target: Option<(Permission, &str)>) -> Self {
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
        Ok(Next::Call(authorize_call(check)))
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
                let user =
                    read_user_result(result).map_err(|error| unavailable(error.to_string()))?;
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
pub(crate) fn config_unavailable(error: GetConfigError) -> CallError {
    CallError(unavailable(format!("realm config: {error:?}")))
}

fn denied(message: &str) -> HostError {
    HostError::new(ErrorKind::Denied, message)
}

#[cfg(test)]
#[path = "check_tests.rs"]
pub(crate) mod tests;
