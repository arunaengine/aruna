//! Makes the service account and role of an install right: creates or reactivates the account,
//! sets its role from the grants, checks both, and undoes them on abort or removal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};

use aruna_core::UserId;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE, GROUP_KEYSPACE, USER_KEYSPACE,
};
use aruna_core::operation::{Operation, Runnable};
use aruna_core::plugin_installs::{InstallGuard, PluginInstall, Prerequisite};
use aruna_core::reducer::decode_reducer_state;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission as Access, Role};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_core::types::{GroupId, RoleId};

use super::grants::{InstallScope, READ_ROLE, WRITE_ROLE, parse_grants, role_name, role_paths};
use super::rows::{StoreError, mapped_child, read_call, values};
use crate::groups::add_role::{AddRoleConfig, AddRoleOperation};
use crate::groups::remove_role::{RemoveGroupConfig, RemoveGroupOperation};
use crate::users::account_status::{AccountStatusConfig, AccountStatusOperation, REALM_ADMIN_ROLE};
use crate::users::service_account::create::{CreateServiceConfig, CreateServiceOperation};

/// Why an account or role of an install could not be read, made right or checked.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct AccountError(pub String);

impl From<IoError> for AccountError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

impl From<StoreError> for AccountError {
    fn from(error: StoreError) -> Self {
        Self(error.0)
    }
}

fn refused(message: &str) -> AccountError {
    AccountError(message.to_string())
}

fn decoded<T, E: ToString>(
    row: Option<Vec<u8>>,
    decode: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<Option<T>, AccountError> {
    row.map(|bytes| decode(&bytes))
        .transpose()
        .map_err(|error| AccountError(error.to_string()))
}

fn user_row(user_id: UserId) -> (&'static str, Vec<u8>) {
    (USER_KEYSPACE, user_id.to_bytes().to_vec())
}

fn group_auth_row(group_id: GroupId) -> (&'static str, Vec<u8>) {
    (AUTH_KEYSPACE, group_id.to_bytes().to_vec())
}

fn realm_auth_row(realm_id: RealmId) -> (&'static str, Vec<u8>) {
    (AUTH_KEYSPACE, realm_id.as_bytes().to_vec())
}

fn state_row(target: &AdminDocumentTarget) -> (&'static str, Vec<u8>) {
    (DOCUMENT_STATE_KEYSPACE, reducer_state_key(target).to_vec())
}

fn stored_user(row: Option<Vec<u8>>) -> Result<Option<User>, AccountError> {
    decoded(row, User::from_bytes)
}

fn stored_auth(row: Option<Vec<u8>>) -> Result<Option<GroupAuthorizationDocument>, AccountError> {
    decoded(row, GroupAuthorizationDocument::from_bytes)
}

/// The call that reads an account, for an operation that decides with it.
pub(crate) fn read_user_call(user_id: UserId) -> IoCall {
    read_call(vec![user_row(user_id)])
}

/// The account a [`read_user_call`] answered.
pub(crate) fn read_user_result(result: IoResult) -> Result<Option<User>, AccountError> {
    let [row] = values::<1>(result)?;
    stored_user(row)
}

/// Runs `operation` as a child that answers only whether it worked and why not.
fn step<O, K>(operation: O) -> IoCall
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: Send + 'static,
    O::Error: std::fmt::Display + Send + 'static,
{
    mapped_child(operation, |result| {
        result.map(drop).map_err(|error| error.to_string())
    })
}

fn stepped(result: IoResult) -> Result<(), AccountError> {
    result.child::<(), String>()?.map_err(AccountError)
}

/// The account and role an install record names. Both are chosen by the node that writes the
/// record first, so a caller-chosen install id never names an existing user or role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Identity {
    pub account: UserId,
    pub role_id: RoleId,
}

impl Identity {
    pub(crate) fn of(install: &PluginInstall) -> Self {
        Self {
            account: install.account,
            role_id: install.role_id,
        }
    }
}

/// The guard that ties a prerequisite write to the record revision it was computed from.
pub(crate) fn guard_of(install: &PluginInstall) -> InstallGuard {
    InstallGuard::Revision {
        install_id: install.install_id,
        revision: install.revision,
    }
}

pub(crate) fn owned(user: &User, scope: &InstallScope) -> bool {
    user.service_group() == Some(scope.group_id) && user.install() == Some(scope.install_id)
}

/// Whether a role is the install's own: a plugin role held by the account alone, or by no one.
fn ours(role: &Role, identity: Identity) -> bool {
    let named = [READ_ROLE, WRITE_ROLE].contains(&role.name.as_str());
    named
        && role
            .assigned_users
            .iter()
            .all(|user| *user == identity.account)
}

/// Refuses an account that holds a realm role, or a role of the group other than the install's.
fn check_alone(
    identity: Identity,
    realm: Option<Vec<u8>>,
    group: Option<&GroupAuthorizationDocument>,
) -> Result<(), AccountError> {
    let realm = decoded(realm, RealmAuthorizationDocument::from_bytes)?;
    let holds = |role: &Role| role.assigned_users.contains(&identity.account);
    if realm.is_some_and(|realm| realm.roles.values().any(holds)) {
        return Err(refused("the account holds a realm role"));
    }
    let other = (group.iter().flat_map(|auth_doc| auth_doc.roles.iter()))
        .any(|(role_id, role)| *role_id != identity.role_id && holds(role));
    match other {
        true => Err(refused("the account holds another role of the group")),
        false => Ok(()),
    }
}

/// Who changes which install's account and role, under which guard.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AccountChange {
    pub actor: Actor,
    pub auth: AuthContext,
    pub scope: InstallScope,
    pub identity: Identity,
    pub guard: InstallGuard,
    /// When the change was asked for, as an account status change records it.
    pub now_secs: u64,
}

impl AccountChange {
    /// The change of `install`'s account and role by `actor`, tied to the record's revision.
    pub(crate) fn of(
        (actor, auth): (&Actor, &AuthContext),
        install: &PluginInstall,
        now_secs: u64,
    ) -> Self {
        Self {
            actor: actor.clone(),
            auth: auth.clone(),
            scope: InstallScope {
                realm_id: actor.realm_id,
                group_id: install.group_id,
                install_id: install.install_id,
            },
            identity: Identity::of(install),
            guard: guard_of(install),
            now_secs,
        }
    }

    fn status(&self, active: bool) -> AccountStatusOperation {
        AccountStatusOperation::new(AccountStatusConfig {
            actor: self.actor.clone(),
            auth_context: self.auth.clone(),
            target: self.identity.account,
            active,
            now: self.now_secs,
        })
        .guarded(self.guard)
        .by_install()
    }
}

/// Creates the account, or reactivates it. An account with another owner or marker, or one that
/// holds any role but the install's, fails the install.
#[derive(Debug, PartialEq)]
pub(crate) struct EnsureAccount {
    change: AccountChange,
    name: String,
    writing: bool,
}

impl EnsureAccount {
    pub(crate) fn new(change: AccountChange, name: String) -> Self {
        Self {
            change,
            name,
            writing: false,
        }
    }

    fn create(&self) -> CreateServiceOperation {
        let change = &self.change;
        CreateServiceOperation::new(CreateServiceConfig {
            actor: change.actor.clone(),
            auth_context: change.auth.clone(),
            group_id: change.scope.group_id,
            name: self.name.clone(),
            user_id: change.identity.account,
            install: Some(change.scope.install_id),
        })
        .guarded(change.guard)
    }
}

impl Operation for EnsureAccount {
    type Output = ();
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<()>, AccountError> {
        let scope = &self.change.scope;
        Ok(Next::Call(read_call(vec![
            realm_auth_row(scope.realm_id),
            group_auth_row(scope.group_id),
            user_row(self.change.identity.account),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AccountError> {
        if self.writing {
            stepped(result)?;
            return Ok(Next::Done(()));
        }
        let [realm, group, user] = values::<3>(result)?;
        let group = stored_auth(group)?;
        check_alone(self.change.identity, realm, group.as_ref())?;
        self.writing = true;
        match stored_user(user)? {
            None => Ok(Next::Call(step(self.create()))),
            Some(user) if !owned(&user, &self.change.scope) => {
                Err(refused("an account with this id belongs elsewhere"))
            }
            Some(user) if user.is_deactivated() => Ok(Next::Call(step(self.change.status(true)))),
            Some(_) => Ok(Next::Done(())),
        }
    }
}

/// Sets the install's role to the computed paths, also on every retry. A role with that id that
/// is not the install's own is never replaced.
#[derive(Debug, PartialEq)]
pub(crate) struct EnsureRole {
    change: AccountChange,
    paths: HashMap<String, Access>,
    writing: bool,
}

impl EnsureRole {
    pub(crate) fn new(change: AccountChange, paths: HashMap<String, Access>) -> Self {
        Self {
            change,
            paths,
            writing: false,
        }
    }
}

impl Operation for EnsureRole {
    type Output = ();
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<()>, AccountError> {
        let row = group_auth_row(self.change.scope.group_id);
        Ok(Next::Call(read_call(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AccountError> {
        if self.writing {
            stepped(result)?;
            return Ok(Next::Done(()));
        }
        let [row] = values::<1>(result)?;
        let auth_doc = stored_auth(row)?.ok_or_else(|| refused("the group is missing"))?;
        let identity = self.change.identity;
        if (auth_doc.roles.get(&identity.role_id)).is_some_and(|role| !ours(role, identity)) {
            return Err(refused("the role id belongs to another role"));
        }
        let role = Role {
            role_id: identity.role_id,
            name: role_name(&self.paths).to_string(),
            permissions: self.paths.clone(),
            assigned_users: HashSet::from([identity.account]),
        };
        let add = AddRoleOperation::new(AddRoleConfig {
            auth_context: self.change.auth.clone(),
            actor: self.change.actor.clone(),
            realm_id: self.change.scope.realm_id,
            group_id: self.change.scope.group_id,
            role,
        })
        .guarded(self.change.guard);
        self.writing = true;
        Ok(Next::Call(step(add)))
    }
}

/// Checks the account and role once more against current state and answers the writes the
/// install depends on. The install's role must be the account's only role.
#[derive(Debug, PartialEq)]
pub(crate) struct CheckReady {
    scope: InstallScope,
    identity: Identity,
    paths: HashMap<String, Access>,
}

impl CheckReady {
    pub(crate) fn new(
        scope: InstallScope,
        identity: Identity,
        paths: HashMap<String, Access>,
    ) -> Self {
        Self {
            scope,
            identity,
            paths,
        }
    }

    fn targets(&self) -> [AdminDocumentTarget; 2] {
        [
            AdminDocumentTarget::User {
                user_id: self.identity.account,
            },
            AdminDocumentTarget::Group {
                group_id: self.scope.group_id,
            },
        ]
    }
}

impl Operation for CheckReady {
    type Output = Vec<Prerequisite>;
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<Self::Output>, AccountError> {
        let [user_state, group_state] = self.targets().map(|target| state_row(&target));
        Ok(Next::Call(read_call(vec![
            user_row(self.identity.account),
            realm_auth_row(self.scope.realm_id),
            group_auth_row(self.scope.group_id),
            user_state,
            group_state,
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, AccountError> {
        let [user, realm, group, user_state, group_state] = values::<5>(result)?;
        let user = stored_user(user)?.ok_or_else(|| refused("the account is missing"))?;
        if !owned(&user, &self.scope) || user.is_deactivated() {
            return Err(refused(
                "the account is not an active account of this install",
            ));
        }
        let group = stored_auth(group)?;
        check_alone(self.identity, realm, group.as_ref())?;
        let auth_doc = group.ok_or_else(|| refused("the group is missing"))?;
        let role = (auth_doc.roles.get(&self.identity.role_id))
            .ok_or_else(|| refused("the role is missing"))?;
        let only = role.assigned_users == HashSet::from([self.identity.account]);
        if !ours(role, self.identity) || role.permissions != self.paths || !only {
            return Err(refused("the role does not match the grants"));
        }
        let mut prerequisites = Vec::new();
        for (target, row) in self.targets().into_iter().zip([user_state, group_state]) {
            let state = decoded(row, decode_reducer_state)?
                .ok_or_else(|| refused("a document state is missing"))?;
            prerequisites.push(Prerequisite {
                target,
                clock: state.clock,
            });
        }
        Ok(Next::Done(prerequisites))
    }
}

#[derive(Debug, PartialEq)]
enum TeardownPhase {
    ReadingUser,
    Deactivating,
    ReadingRole,
    Removing,
}

/// Undoes the account and role steps: deactivates the account and removes the install's own
/// role, each only if needed. Another role with the same id is left alone.
#[derive(Debug, PartialEq)]
pub(crate) struct Teardown {
    change: AccountChange,
    phase: TeardownPhase,
}

impl Teardown {
    pub(crate) fn new(change: AccountChange) -> Self {
        Self {
            change,
            phase: TeardownPhase::ReadingUser,
        }
    }

    fn read_role(&mut self) -> Next<()> {
        self.phase = TeardownPhase::ReadingRole;
        let row = group_auth_row(self.change.scope.group_id);
        Next::Call(read_call(vec![row]))
    }

    fn remove(&mut self) -> IoCall {
        self.phase = TeardownPhase::Removing;
        let change = &self.change;
        let remove = RemoveGroupOperation::new(RemoveGroupConfig {
            auth_context: change.auth.clone(),
            actor: change.actor.clone(),
            realm_id: change.scope.realm_id,
            group_id: change.scope.group_id,
            role_id: change.identity.role_id,
        })
        .guarded(change.guard);
        step(remove)
    }
}

impl Operation for Teardown {
    type Output = ();
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<()>, AccountError> {
        let account = self.change.identity.account;
        Ok(Next::Call(read_user_call(account)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AccountError> {
        match self.phase {
            TeardownPhase::ReadingUser => {
                let user = read_user_result(result)?;
                let scope = &self.change.scope;
                if user.is_some_and(|user| owned(&user, scope) && !user.is_deactivated()) {
                    self.phase = TeardownPhase::Deactivating;
                    return Ok(Next::Call(step(self.change.status(false))));
                }
                Ok(self.read_role())
            }
            TeardownPhase::Deactivating => {
                stepped(result)?;
                Ok(self.read_role())
            }
            TeardownPhase::ReadingRole => {
                let [row] = values::<1>(result)?;
                let auth_doc = stored_auth(row)?;
                let role_id = self.change.identity.role_id;
                let role = auth_doc.and_then(|auth_doc| auth_doc.roles.get(&role_id).cloned());
                match role.is_some_and(|role| ours(&role, self.change.identity)) {
                    true => Ok(Next::Call(self.remove())),
                    false => Ok(Next::Done(())),
                }
            }
            TeardownPhase::Removing => {
                stepped(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// `plugin <name> (<group>)`, with the group's display name when it is known.
#[derive(Debug, PartialEq)]
pub(crate) struct AccountName {
    plugin: String,
    group_id: GroupId,
}

impl AccountName {
    pub(crate) fn new(plugin: &str, group_id: GroupId) -> Self {
        Self {
            plugin: plugin.to_string(),
            group_id,
        }
    }

    fn named(&self, label: String) -> String {
        format!("plugin {} ({label})", self.plugin)
    }

    /// The name while the group cannot be read.
    fn unnamed(&self) -> String {
        self.named(self.group_id.to_string())
    }
}

impl Operation for AccountName {
    type Output = String;
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<String>, AccountError> {
        let row = (GROUP_KEYSPACE, self.group_id.to_bytes().to_vec());
        Ok(Next::Call(read_call(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, AccountError> {
        let [row] = values::<1>(result)?;
        let group = row.and_then(|bytes| Group::from_bytes(&bytes).ok());
        Ok(Next::Done(match group {
            Some(group) => self.named(group.display_name),
            None => self.unnamed(),
        }))
    }
}

/// A step of making an install's account and role right that failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{step}: {error}")]
pub struct StepError {
    pub step: &'static str,
    pub error: String,
}

impl StepError {
    fn at(step: &'static str) -> impl FnOnce(AccountError) -> Self {
        move |error| Self {
            step,
            error: error.0,
        }
    }
}

impl From<IoError> for StepError {
    fn from(error: IoError) -> Self {
        Self {
            step: "storage",
            error: error.to_string(),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ProvisionPhase {
    Naming,
    Account,
    Role,
}

/// The name, account and role steps both a new install and a repair run, in this order.
#[derive(Debug, PartialEq)]
struct Provision {
    change: AccountChange,
    plugin: String,
    paths: HashMap<String, Access>,
    phase: ProvisionPhase,
}

impl Provision {
    fn new(change: AccountChange, plugin: &str, paths: HashMap<String, Access>) -> Self {
        Self {
            change,
            plugin: plugin.to_string(),
            paths,
            phase: ProvisionPhase::Naming,
        }
    }

    fn naming(&self) -> AccountName {
        AccountName::new(&self.plugin, self.change.scope.group_id)
    }

    fn start(&self) -> IoCall {
        IoCall::child(self.naming())
    }

    /// The next step's call, or `None` once the role is written.
    fn advance(&mut self, result: IoResult) -> Result<Option<IoCall>, StepError> {
        match self.phase {
            ProvisionPhase::Naming => {
                let name = (result.child::<String, AccountError>()?)
                    .unwrap_or_else(|_| self.naming().unnamed());
                self.phase = ProvisionPhase::Account;
                let ensure = EnsureAccount::new(self.change.clone(), name);
                Ok(Some(IoCall::child(ensure)))
            }
            ProvisionPhase::Account => {
                (result.child::<(), AccountError>()?).map_err(StepError::at("account"))?;
                self.phase = ProvisionPhase::Role;
                let ensure = EnsureRole::new(self.change.clone(), self.paths.clone());
                Ok(Some(IoCall::child(ensure)))
            }
            ProvisionPhase::Role => {
                (result.child::<(), AccountError>()?).map_err(StepError::at("role"))?;
                Ok(None)
            }
        }
    }
}

/// The account, its role, then a check of both against current state, each write refused once
/// the record moved on. Answers the writes the install depends on.
#[derive(Debug, PartialEq)]
pub(crate) struct MakeReady {
    provision: Provision,
    checking: bool,
}

impl MakeReady {
    pub(crate) fn new(change: AccountChange, record: &PluginInstall) -> Self {
        let paths = role_paths(&parse_grants(&record.grants));
        Self {
            provision: Provision::new(change, &record.plugin, paths),
            checking: false,
        }
    }
}

impl Operation for MakeReady {
    type Output = Vec<Prerequisite>;
    type Error = StepError;

    fn first(&mut self) -> Result<Next<Self::Output>, StepError> {
        Ok(Next::Call(self.provision.start()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, StepError> {
        if self.checking {
            let ready = result.child::<Vec<Prerequisite>, AccountError>()?;
            return Ok(Next::Done(ready.map_err(StepError::at("check"))?));
        }
        if let Some(call) = self.provision.advance(result)? {
            return Ok(Next::Call(call));
        }
        self.checking = true;
        let change = &self.provision.change;
        let paths = self.provision.paths.clone();
        let check = CheckReady::new(change.scope, change.identity, paths);
        Ok(Next::Call(IoCall::child(check)))
    }
}

#[derive(Debug, PartialEq)]
enum RepairPhase {
    Checking,
    ReadingUser,
    Provisioning,
}

/// Makes one settled install's account and role match its record again; answers whether it
/// changed anything. An administrator's deactivation stands: repair restores only what the
/// install stopped.
#[derive(Debug, PartialEq)]
pub(crate) struct RepairAccount {
    provision: Provision,
    phase: RepairPhase,
}

impl RepairAccount {
    pub(crate) fn new(change: AccountChange, install: &PluginInstall) -> Self {
        let paths = role_paths(&parse_grants(&install.grants));
        Self {
            provision: Provision::new(change, &install.plugin, paths),
            phase: RepairPhase::Checking,
        }
    }
}

impl Operation for RepairAccount {
    type Output = bool;
    type Error = StepError;

    fn first(&mut self) -> Result<Next<bool>, StepError> {
        let change = &self.provision.change;
        let paths = self.provision.paths.clone();
        let check = CheckReady::new(change.scope, change.identity, paths);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, StepError> {
        match self.phase {
            RepairPhase::Checking => {
                if (result.child::<Vec<Prerequisite>, AccountError>()?).is_ok() {
                    return Ok(Next::Done(false));
                }
                self.phase = RepairPhase::ReadingUser;
                let account = self.provision.change.identity.account;
                Ok(Next::Call(read_user_call(account)))
            }
            RepairPhase::ReadingUser => {
                let user = read_user_result(result).map_err(StepError::at("account"))?;
                if user.is_some_and(|user| user.is_deactivated() && !user.stopped_by_install()) {
                    return Ok(Next::Done(false));
                }
                self.phase = RepairPhase::Provisioning;
                Ok(Next::Call(self.provision.start()))
            }
            RepairPhase::Provisioning => Ok(match self.provision.advance(result)? {
                Some(call) => Next::Call(call),
                None => Next::Done(true),
            }),
        }
    }
}

/// An account, read on its own.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadUser(pub UserId);

impl Operation for ReadUser {
    type Output = Option<User>;
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<Option<User>>, AccountError> {
        Ok(Next::Call(read_user_call(self.0)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<User>>, AccountError> {
        Ok(Next::Done(read_user_result(result)?))
    }
}

/// A group's authorization document, read on its own.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadGroupAuth(pub GroupId);

impl Operation for ReadGroupAuth {
    type Output = Option<GroupAuthorizationDocument>;
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<Self::Output>, AccountError> {
        Ok(Next::Call(read_call(vec![group_auth_row(self.0)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, AccountError> {
        let [row] = values::<1>(result)?;
        Ok(Next::Done(stored_auth(row)?))
    }
}

/// The realm administrator with the smallest id, so every node acts as the same one.
#[derive(Debug, PartialEq)]
pub(crate) struct RealmAdmin(pub RealmId);

impl Operation for RealmAdmin {
    type Output = Option<UserId>;
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<Option<UserId>>, AccountError> {
        Ok(Next::Call(read_call(vec![realm_auth_row(self.0)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<UserId>>, AccountError> {
        let [row] = values::<1>(result)?;
        let realm = decoded(row, RealmAuthorizationDocument::from_bytes)?;
        let admins = (realm.iter().flat_map(|realm| realm.roles.values()))
            .filter(|role| role.name == REALM_ADMIN_ROLE)
            .flat_map(|role| role.assigned_users.iter().copied());
        Ok(Next::Done(admins.min()))
    }
}

/// Whether this node holds every account and group write the install record names.
#[derive(Debug, PartialEq)]
pub(crate) struct PrerequisitesHeld(Vec<Prerequisite>);

impl PrerequisitesHeld {
    pub(crate) fn new(install: &PluginInstall) -> Self {
        Self(install.prerequisites.clone())
    }
}

impl Operation for PrerequisitesHeld {
    type Output = bool;
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<bool>, AccountError> {
        if self.0.is_empty() {
            return Ok(Next::Done(true));
        }
        let rows = self.0.iter().map(|needed| state_row(&needed.target));
        Ok(Next::Call(read_call(rows.collect())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, AccountError> {
        let rows = super::rows::all_values(result)?;
        for (needed, row) in self.0.iter().zip(rows) {
            let state = decoded(row, decode_reducer_state)?;
            if !state.is_some_and(|state| state.clock.covers(&needed.clock)) {
                return Ok(Next::Done(false));
            }
        }
        Ok(Next::Done(true))
    }
}

/// Whether the account acts for a plugin install; it changes only through that install.
#[derive(Debug, PartialEq)]
pub struct PluginAccount(pub UserId);

impl Operation for PluginAccount {
    type Output = bool;
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<bool>, AccountError> {
        Ok(Next::Call(read_user_call(self.0)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, AccountError> {
        let user = read_user_result(result)?;
        Ok(Next::Done(
            user.is_some_and(|user| user.install().is_some()),
        ))
    }
}

#[cfg(test)]
#[path = "account_tests.rs"]
mod tests;
