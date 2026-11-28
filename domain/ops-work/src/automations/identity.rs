//! Keeps an automation's service account and role aligned with its approved scope and lifecycle.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};

use aruna_core::automation::record::Automation;
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

use super::AutoError;
use super::change::{AutoChange, WriteAutoOperation};
use crate::plugins::account::step;

/// Deterministic service account and role ids derived from `auto.automation_id`.
pub fn identity_ids(auto: &Automation, realm_id: RealmId) -> (UserId, RoleId) {
    (
        UserId::new(auto.automation_id, realm_id),
        auto.automation_id,
    )
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

/// Synchronizes an automation's service account and role using its human approver's authority.
#[derive(Debug, PartialEq)]
pub struct SyncIdentityOperation {
    auto: Automation,
    actor: Option<Actor>,
    account: UserId,
    role_id: RoleId,
    tasks: VecDeque<Task>,
    target: (Option<UserId>, Option<RoleId>),
    now_secs: u64,
}

impl SyncIdentityOperation {
    pub fn new(auto: &Automation, (node_id, realm_id): (NodeId, RealmId)) -> Self {
        let (account, role_id) = identity_ids(auto, realm_id);
        let actor = auto.granter().map(|user_id| Actor {
            node_id,
            user_id,
            realm_id,
        });
        Self {
            auto: auto.clone(),
            actor,
            account,
            role_id,
            tasks: VecDeque::new(),
            target: (None, None),
            now_secs: 0,
        }
    }

    fn plan(&mut self, active: Option<bool>) -> VecDeque<Task> {
        let wanted = self.auto.wants_account();
        let mut tasks = VecDeque::new();
        match (wanted, active) {
            (true, None) => tasks.push_back(Task::Create),
            (true, Some(false)) => tasks.push_back(Task::Activate),
            (false, Some(true)) => tasks.push_back(Task::Deactivate),
            _ => {}
        }
        match wanted {
            true => tasks.push_back(Task::SetRole),
            false if self.auto.role_id.is_some() => tasks.push_front(Task::RemoveRole),
            false => {}
        }
        self.target = match wanted {
            true => (Some(self.account), Some(self.role_id)),
            false => (active.map(|_| self.account), None),
        };
        if (self.auto.account, self.auto.role_id) != self.target
            || (wanted && self.auto.identity_synced_revision < self.auto.revision)
        {
            tasks.push_back(Task::Record);
        }
        tasks
    }

    fn call(&self, task: Task, actor: &Actor) -> IoCall {
        let auth = AuthContext::from(actor);
        let group_id = self.auto.group_id;
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
                name: format!("automation {}", self.auto.name),
                user_id: self.account,
                install: None,
            })),
            Task::Activate => status(true),
            Task::Deactivate => status(false),
            Task::SetRole => {
                let permissions: HashMap<_, _> = self
                    .auto
                    .role_paths(actor.realm_id, self.auto.host_node)
                    .into_iter()
                    .collect();
                let role = Role {
                    role_id: self.role_id,
                    name: format!("automation {}", self.auto.name),
                    permissions,
                    assigned_users: HashSet::from([self.account]),
                };
                step(AddRoleOperation::new(AddRoleConfig {
                    auth_context: auth,
                    actor: actor.clone(),
                    realm_id: actor.realm_id,
                    group_id,
                    role,
                }))
            }
            Task::RemoveRole => step(RemoveGroupOperation::new(RemoveGroupConfig {
                auth_context: auth,
                actor: actor.clone(),
                realm_id: actor.realm_id,
                group_id,
                role_id: self.role_id,
            })),
            Task::Record => {
                let (account, role_id) = self.target;
                let id = self.auto.automation_id;
                let write = WriteAutoOperation::node(
                    id,
                    AutoChange::Identity {
                        account,
                        role_id,
                        synced_revision: self.auto.revision,
                    },
                    actor.user_id,
                    actor.realm_id,
                );
                IoCall::retry(write, &id.to_bytes())
            }
        }
    }

    fn next_task(&mut self) -> Result<Next<Automation>, AutoError> {
        let Some(task) = self.tasks.front().copied() else {
            return Ok(Next::Done(self.auto.clone()));
        };
        let actor = self
            .actor
            .clone()
            .ok_or_else(|| AutoError::Conflict("no approver granted this automation".into()))?;
        Ok(Next::Call(self.call(task, &actor)))
    }
}

impl Operation for SyncIdentityOperation {
    type Output = Automation;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_secs = now_ms / 1000;
    }

    fn first(&mut self) -> Result<Next<Automation>, AutoError> {
        Ok(Next::Call(read_user_call(self.account)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Automation>, AutoError> {
        let Some(done) = self.tasks.pop_front() else {
            let user = read_user_result(result)?;
            self.tasks = self.plan(user.map(|user| !user.is_deactivated()));
            return self.next_task();
        };
        match done {
            Task::Record => self.auto = result.child::<Automation, AutoError>()??,
            _ => result.child::<(), String>()?.map_err(|reason| {
                AutoError::Storage(format!("automation account sync failed: {reason}"))
            })?,
        }
        self.next_task()
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
