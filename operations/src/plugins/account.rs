//! Makes the service account and role of an install right: creates or reactivates the account,
//! sets its role from the grants, checks both, and undoes them on abort or removal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};

use aruna_core::UserId;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE, GROUP_KEYSPACE, USER_KEYSPACE,
};
use aruna_core::plugin_installs::{InstallGuard, PluginInstall, Prerequisite};
use aruna_core::reducer::decode_reducer_state;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission as Access, Role};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::structs::identity::user::User;
use aruna_core::time::unix_timestamp_secs;
use aruna_core::types::{GroupId, RoleId};

use super::grants::{InstallScope, READ_ROLE, WRITE_ROLE, role_name};
use crate::driver::{DriverContext, drive};
use crate::groups::add_role::{AddRoleConfig, AddRoleOperation};
use crate::groups::remove_role::{RemoveGroupConfig, RemoveGroupOperation};
use crate::users::account_status::{AccountStatusConfig, AccountStatusOperation};
use crate::users::service_account::create::{CreateServiceConfig, CreateServiceOperation};

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
    InstallGuard {
        install_id: install.install_id,
        revision: install.revision,
    }
}

/// Creates the account, or reactivates it. An account with another owner or marker, or one
/// that holds any role but the install's, fails the install.
pub(crate) async fn ensure_account(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    scope: &InstallScope,
    identity: Identity,
    name: String,
    guard: InstallGuard,
) -> Result<(), String> {
    check_alone(ctx, scope, identity).await?;
    match read_user(ctx, identity.account).await? {
        None => {
            let create = CreateServiceOperation::new(CreateServiceConfig {
                actor: actor.clone(),
                auth_context: auth.clone(),
                group_id: scope.group_id,
                name,
                user_id: identity.account,
                install: Some(scope.install_id),
            })
            .guarded(guard);
            drive(create, ctx)
                .await
                .map(drop)
                .map_err(|error| error.to_string())
        }
        Some(user) if !owned(&user, scope) => {
            Err("an account with this id belongs elsewhere".into())
        }
        Some(user) if user.is_deactivated() => {
            set_active(ctx, actor, auth, identity.account, true, guard).await
        }
        Some(_) => Ok(()),
    }
}

/// Sets the install's role to the computed paths, also on every retry. A role with that id
/// that is not the install's own is never replaced.
pub(crate) async fn ensure_role(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    scope: &InstallScope,
    identity: Identity,
    paths: &HashMap<String, Access>,
    guard: InstallGuard,
) -> Result<(), String> {
    let auth_doc = read_auth(ctx, scope.group_id)
        .await?
        .ok_or("the group is missing")?;
    if (auth_doc.roles.get(&identity.role_id)).is_some_and(|role| !ours(role, identity)) {
        return Err("the role id belongs to another role".into());
    }
    let role = Role {
        role_id: identity.role_id,
        name: role_name(paths).to_string(),
        permissions: paths.clone(),
        assigned_users: HashSet::from([identity.account]),
    };
    let add = AddRoleOperation::new(AddRoleConfig {
        auth_context: auth.clone(),
        actor: actor.clone(),
        realm_id: scope.realm_id,
        group_id: scope.group_id,
        role,
    })
    .guarded(guard);
    drive(add, ctx)
        .await
        .map(drop)
        .map_err(|error| error.to_string())
}

/// Checks the account and role once more against current state and returns the writes the
/// install depends on. The install's role must be the account's only role.
pub(crate) async fn check_ready(
    ctx: &DriverContext,
    scope: &InstallScope,
    identity: Identity,
    paths: &HashMap<String, Access>,
) -> Result<Vec<Prerequisite>, String> {
    let user = (read_user(ctx, identity.account).await?).ok_or("the account is missing")?;
    if !owned(&user, scope) || user.is_deactivated() {
        return Err("the account is not an active account of this install".into());
    }
    check_alone(ctx, scope, identity).await?;
    let auth_doc = read_auth(ctx, scope.group_id)
        .await?
        .ok_or("the group is missing")?;
    let role = auth_doc
        .roles
        .get(&identity.role_id)
        .ok_or("the role is missing")?;
    let only = role.assigned_users == HashSet::from([identity.account]);
    if !ours(role, identity) || role.permissions != *paths || !only {
        return Err("the role does not match the grants".into());
    }
    let targets = [
        AdminDocumentTarget::User {
            user_id: identity.account,
        },
        AdminDocumentTarget::Group {
            group_id: scope.group_id,
        },
    ];
    let mut prerequisites = Vec::new();
    for target in targets {
        let key = reducer_state_key(&target).to_vec();
        let value = read_row(ctx, DOCUMENT_STATE_KEYSPACE, key).await?;
        let state = value.as_deref().map(decode_reducer_state).transpose();
        let state = state
            .map_err(|error| error.to_string())?
            .ok_or("a document state is missing")?;
        prerequisites.push(Prerequisite {
            target,
            clock: state.clock,
        });
    }
    Ok(prerequisites)
}

/// Undoes steps 2 and 3: deactivates the account and removes the install's own role, each only
/// if needed. Another role with the same id is left alone.
pub(crate) async fn teardown(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    scope: &InstallScope,
    identity: Identity,
    guard: InstallGuard,
) -> Result<(), String> {
    let user = read_user(ctx, identity.account).await?;
    if user.is_some_and(|user| owned(&user, scope) && !user.is_deactivated()) {
        set_active(ctx, actor, auth, identity.account, false, guard).await?;
    }
    let auth_doc = read_auth(ctx, scope.group_id).await?;
    let role = auth_doc.and_then(|auth_doc| auth_doc.roles.get(&identity.role_id).cloned());
    if role.is_some_and(|role| ours(&role, identity)) {
        let remove = RemoveGroupOperation::new(RemoveGroupConfig {
            auth_context: auth.clone(),
            actor: actor.clone(),
            realm_id: scope.realm_id,
            group_id: scope.group_id,
            role_id: identity.role_id,
        })
        .guarded(guard);
        drive(remove, ctx)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Refuses an account that holds a realm role, or a role of the group other than the install's.
async fn check_alone(
    ctx: &DriverContext,
    scope: &InstallScope,
    identity: Identity,
) -> Result<(), String> {
    let key = scope.realm_id.as_bytes().to_vec();
    let realm = read_row(ctx, AUTH_KEYSPACE, key).await?;
    let realm = (realm.map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes)))
        .transpose()
        .map_err(|error| error.to_string())?;
    let holds = |role: &Role| role.assigned_users.contains(&identity.account);
    if realm.is_some_and(|realm| realm.roles.values().any(holds)) {
        return Err("the account holds a realm role".into());
    }
    let auth_doc = read_auth(ctx, scope.group_id).await?;
    let other = (auth_doc.iter().flat_map(|auth_doc| auth_doc.roles.iter()))
        .any(|(role_id, role)| *role_id != identity.role_id && holds(role));
    match other {
        true => Err("the account holds another role of the group".into()),
        false => Ok(()),
    }
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

pub(crate) async fn set_active(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    account: UserId,
    active: bool,
    guard: InstallGuard,
) -> Result<(), String> {
    let status = AccountStatusOperation::new(AccountStatusConfig {
        actor: actor.clone(),
        auth_context: auth.clone(),
        target: account,
        active,
        now: unix_timestamp_secs(),
    })
    .guarded(guard);
    drive(status, ctx).await.map_err(|error| error.to_string())
}

pub(crate) fn owned(user: &User, scope: &InstallScope) -> bool {
    user.service_group() == Some(scope.group_id) && user.install() == Some(scope.install_id)
}

/// `plugin <name> (<group>)`, with the group's display name when it is known.
pub(crate) async fn account_name(ctx: &DriverContext, plugin: &str, group_id: GroupId) -> String {
    let group = read_row(ctx, GROUP_KEYSPACE, group_id.to_bytes().to_vec()).await;
    let group = group
        .ok()
        .flatten()
        .and_then(|bytes| Group::from_bytes(&bytes).ok());
    let label = group.map_or_else(|| group_id.to_string(), |group| group.display_name);
    format!("plugin {plugin} ({label})")
}

pub(crate) async fn read_user(
    ctx: &DriverContext,
    user_id: UserId,
) -> Result<Option<User>, String> {
    let value = read_row(ctx, USER_KEYSPACE, user_id.to_bytes().to_vec()).await?;
    value
        .map(|bytes| User::from_bytes(&bytes))
        .transpose()
        .map_err(|error| error.to_string())
}

pub(crate) async fn read_auth(
    ctx: &DriverContext,
    group_id: GroupId,
) -> Result<Option<GroupAuthorizationDocument>, String> {
    let value = read_row(ctx, AUTH_KEYSPACE, group_id.to_bytes().to_vec()).await?;
    (value.map(|bytes| GroupAuthorizationDocument::from_bytes(&bytes)))
        .transpose()
        .map_err(|error| error.to_string())
}

pub(crate) async fn read_row(
    ctx: &DriverContext,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, String> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.map(|v| v.to_vec())),
        other => Err(format!("{other:?}")),
    }
}

/// Whether the account acts for a plugin install; it changes only through that install.
pub async fn plugin_account(ctx: &DriverContext, user_id: UserId) -> Result<bool, String> {
    Ok(read_user(ctx, user_id)
        .await?
        .is_some_and(|user| user.install().is_some()))
}
