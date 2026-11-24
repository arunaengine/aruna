//! Keeps an applet's own account and role in line with its record: a live applet with grants
//! has an active account whose one role holds exactly the approved paths; otherwise neither.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};

use aruna_core::applet::record::Applet;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Role};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::RoleId;
use aruna_core::user::id::UserId;
use aruna_ops_identity::groups::add_role::{AddRoleConfig, AddRoleOperation};
use aruna_ops_identity::groups::remove_role::{RemoveGroupConfig, RemoveGroupOperation};
use aruna_ops_identity::users::account_status::{AccountStatusConfig, AccountStatusOperation};
use aruna_ops_identity::users::install_account::{read_user_call, read_user_result};
use aruna_ops_identity::users::service_account::create::{
    CreateServiceConfig, CreateServiceOperation,
};

use super::AppletError;
use super::change::{AppletChange, WriteChangeOperation};
use crate::plugins::account::step;

/// The account and role of an applet, named by its id so every retry names the same ones.
pub fn identity_ids(applet: &Applet, realm_id: RealmId) -> (UserId, RoleId) {
    (UserId::new(applet.applet_id, realm_id), applet.applet_id)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Task {
    Create,
    Activate,
    Deactivate,
    SetRole,
    RemoveRole,
    Record,
}

/// Makes the account and role match [`Applet::wants_account`], acting as the approver who
/// granted them, and records their ids. A step its approver may no longer take fails it.
#[derive(Debug, PartialEq)]
pub struct SyncIdentityOperation {
    applet: Applet,
    actor: Option<Actor>,
    account: UserId,
    role_id: RoleId,
    tasks: VecDeque<Task>,
    /// The account and role ids the record should show afterwards.
    target: (Option<UserId>, Option<RoleId>),
    now_secs: u64,
}

impl SyncIdentityOperation {
    pub fn new(applet: &Applet, (node_id, realm_id): (NodeId, RealmId)) -> Self {
        let (account, role_id) = identity_ids(applet, realm_id);
        let actor = applet.granter().map(|user_id| Actor {
            node_id,
            user_id,
            realm_id,
        });
        Self {
            applet: applet.clone(),
            actor,
            account,
            role_id,
            tasks: VecDeque::new(),
            target: (None, None),
            now_secs: 0,
        }
    }

    /// The steps from what the record and the stored account show now.
    fn plan(&mut self, active: Option<bool>) -> VecDeque<Task> {
        let wanted = self.applet.wants_account();
        let mut tasks = VecDeque::new();
        match (wanted, active) {
            (true, None) => tasks.push_back(Task::Create),
            (true, Some(false)) => tasks.push_back(Task::Activate),
            (false, Some(true)) => tasks.push_back(Task::Deactivate),
            _ => {}
        }
        match wanted {
            true => tasks.push_back(Task::SetRole),
            false if self.applet.role_id.is_some() => tasks.push_front(Task::RemoveRole),
            false => {}
        }
        self.target = match wanted {
            true => (Some(self.account), Some(self.role_id)),
            false => (active.map(|_| self.account), None),
        };
        if (self.applet.account, self.applet.role_id) != self.target {
            tasks.push_back(Task::Record);
        }
        tasks
    }

    fn call(&self, task: Task, actor: &Actor) -> IoCall {
        let auth = AuthContext::from(actor);
        let group_id = self.applet.group_id;
        let status = |active| {
            step(AccountStatusOperation::new(AccountStatusConfig {
                actor: actor.clone(),
                auth_context: auth.clone(),
                target: self.account,
                active,
                now: self.now_secs,
            }))
        };
        match task {
            Task::Create => step(CreateServiceOperation::new(CreateServiceConfig {
                actor: actor.clone(),
                auth_context: auth.clone(),
                group_id,
                name: format!("applet {}", self.applet.slug),
                user_id: self.account,
                install: None,
            })),
            Task::Activate => status(true),
            Task::Deactivate => status(false),
            Task::SetRole => {
                let realm_id = actor.realm_id;
                let permissions: HashMap<_, _> = (self.applet)
                    .role_paths(realm_id, self.applet.host_node)
                    .into_iter()
                    .collect();
                let role = Role {
                    role_id: self.role_id,
                    name: format!("applet {}", self.applet.slug),
                    permissions,
                    assigned_users: HashSet::from([self.account]),
                };
                let config = AddRoleConfig {
                    auth_context: auth.clone(),
                    actor: actor.clone(),
                    realm_id,
                    group_id,
                    role,
                };
                step(AddRoleOperation::new(config))
            }
            Task::RemoveRole => step(RemoveGroupOperation::new(RemoveGroupConfig {
                auth_context: auth.clone(),
                actor: actor.clone(),
                realm_id: actor.realm_id,
                group_id,
                role_id: self.role_id,
            })),
            Task::Record => {
                let (account, role_id) = self.target;
                let change = AppletChange::Identity { account, role_id };
                let applet_id = self.applet.applet_id;
                let write = WriteChangeOperation::node(applet_id, change, actor.user_id);
                IoCall::retry(write, &applet_id.to_bytes())
            }
        }
    }

    fn next_task(&mut self) -> Result<Next<Applet>, AppletError> {
        let Some(task) = self.tasks.front().copied() else {
            return Ok(Next::Done(self.applet.clone()));
        };
        let actor = (self.actor.clone()).ok_or_else(|| {
            AppletError::Conflict("no approver granted the applet's account".into())
        })?;
        Ok(Next::Call(self.call(task, &actor)))
    }
}

impl Operation for SyncIdentityOperation {
    type Output = Applet;
    type Error = AppletError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_secs = now_ms / 1000;
    }

    fn first(&mut self) -> Result<Next<Applet>, AppletError> {
        Ok(Next::Call(read_user_call(self.account)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AppletError> {
        let Some(done) = self.tasks.pop_front() else {
            let user = read_user_result(result)?;
            self.tasks = self.plan(user.map(|user| !user.is_deactivated()));
            return self.next_task();
        };
        match done {
            Task::Record => self.applet = result.child::<Applet, AppletError>()??,
            _ => result.child::<(), String>()?.map_err(|reason| {
                AppletError::Storage(format!("the applet's account was not changed: {reason}"))
            })?,
        }
        self.next_task()
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::applets::change::tests::{applet, version};
    use aruna_core::applet::record::{Lifecycle, VersionState};

    fn plan(applet: &Applet, active: Option<bool>) -> Vec<Task> {
        let realm = RealmId([2; 32]);
        let mut sync = SyncIdentityOperation::new(applet, (applet.host_node, realm));
        sync.plan(active).into_iter().collect()
    }

    // A live applet with grants gets an active account and its role; anything else loses both.
    #[test]
    fn plans_follow_record() {
        let mut applet = applet();
        assert!(plan(&applet, None).is_empty());
        applet.grants.read = vec!["/r/g/G/data/n/results/**".into()];
        let mut entry = version(1);
        entry.state = VersionState::Approved;
        entry.approved_by = Some(applet.created_by);
        applet.versions.push(entry);
        applet.live = Some(1);
        applet.lifecycle = Lifecycle::Live;
        assert_eq!(
            plan(&applet, None),
            vec![Task::Create, Task::SetRole, Task::Record]
        );
        let (account, role) = identity_ids(&applet, RealmId([2; 32]));
        (applet.account, applet.role_id) = (Some(account), Some(role));
        assert_eq!(plan(&applet, Some(true)), vec![Task::SetRole]);
        applet.lifecycle = Lifecycle::Paused;
        assert_eq!(
            plan(&applet, Some(true)),
            vec![Task::RemoveRole, Task::Deactivate, Task::Record]
        );
        applet.role_id = None;
        assert!(plan(&applet, Some(false)).is_empty());
        applet.lifecycle = Lifecycle::Live;
        assert_eq!(
            plan(&applet, Some(false)),
            vec![Task::Activate, Task::SetRole, Task::Record]
        );
    }
}
