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
use aruna_core::plugin_installs::Prerequisite;
use aruna_core::reducer::decode_reducer_state;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission as Access, Role};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::user::User;
use aruna_core::time::unix_timestamp_secs;
use aruna_core::types::GroupId;

use super::grants::{InstallScope, role_name};
use crate::driver::{DriverContext, drive};
use crate::groups::add_role::{AddRoleConfig, AddRoleOperation};
use crate::groups::remove_role::{RemoveGroupConfig, RemoveGroupOperation};
use crate::users::account_status::{AccountStatusConfig, AccountStatusOperation};
use crate::users::service_account::create::{CreateServiceConfig, CreateServiceOperation};

/// Creates the account, or reactivates it; an account with this id but another owner or
/// marker fails the install.
pub(crate) async fn ensure_account(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    scope: &InstallScope,
    account: UserId,
    name: String,
) -> Result<(), String> {
    match read_user(ctx, account).await? {
        None => {
            let create = CreateServiceOperation::new(CreateServiceConfig {
                actor: actor.clone(),
                auth_context: auth.clone(),
                group_id: scope.group_id,
                name,
                user_id: account,
                install: Some(scope.install_id),
            });
            drive(create, ctx)
                .await
                .map(drop)
                .map_err(|error| error.to_string())
        }
        Some(user) if !owned(&user, scope) => {
            Err("an account with this id belongs elsewhere".into())
        }
        Some(user) if user.is_deactivated() => set_active(ctx, actor, auth, account, true).await,
        Some(_) => Ok(()),
    }
}

/// Sets the account's role to the computed paths, also on every retry.
pub(crate) async fn ensure_role(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    scope: &InstallScope,
    account: UserId,
    paths: &HashMap<String, Access>,
) -> Result<(), String> {
    let role = Role {
        role_id: scope.install_id,
        name: role_name(paths).to_string(),
        permissions: paths.clone(),
        assigned_users: HashSet::from([account]),
    };
    let add = AddRoleOperation::new(AddRoleConfig {
        auth_context: auth.clone(),
        actor: actor.clone(),
        realm_id: scope.realm_id,
        group_id: scope.group_id,
        role,
    });
    drive(add, ctx)
        .await
        .map(drop)
        .map_err(|error| error.to_string())
}

/// Checks the account and role once more against current state and returns the writes the
/// install depends on.
pub(crate) async fn check_ready(
    ctx: &DriverContext,
    scope: &InstallScope,
    account: UserId,
    paths: &HashMap<String, Access>,
) -> Result<Vec<Prerequisite>, String> {
    let user = read_user(ctx, account)
        .await?
        .ok_or("the account is missing")?;
    if !owned(&user, scope) || user.is_deactivated() {
        return Err("the account is not an active account of this install".into());
    }
    let auth_doc = read_auth(ctx, scope.group_id)
        .await?
        .ok_or("the group is missing")?;
    let role = auth_doc
        .roles
        .get(&scope.install_id)
        .ok_or("the role is missing")?;
    if role.permissions != *paths || !role.assigned_users.contains(&account) {
        return Err("the role does not match the grants".into());
    }
    let targets = [
        AdminDocumentTarget::User { user_id: account },
        AdminDocumentTarget::Group {
            group_id: scope.group_id,
        },
    ];
    let mut prerequisites = Vec::new();
    for target in targets {
        let value = read_row(
            ctx,
            DOCUMENT_STATE_KEYSPACE,
            reducer_state_key(&target).to_vec(),
        )
        .await?;
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

/// Undoes steps 2 and 3: deactivates the account and removes its role, each only if needed.
pub(crate) async fn teardown(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    scope: &InstallScope,
) -> Result<(), String> {
    let account = UserId::local(scope.install_id, scope.realm_id);
    let user = read_user(ctx, account).await?;
    if user.is_some_and(|user| owned(&user, scope) && !user.is_deactivated()) {
        set_active(ctx, actor, auth, account, false).await?;
    }
    let auth_doc = read_auth(ctx, scope.group_id).await?;
    if auth_doc.is_some_and(|auth_doc| auth_doc.roles.contains_key(&scope.install_id)) {
        let remove = RemoveGroupOperation::new(RemoveGroupConfig {
            auth_context: auth.clone(),
            actor: actor.clone(),
            realm_id: scope.realm_id,
            group_id: scope.group_id,
            role_id: scope.install_id,
        });
        drive(remove, ctx)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub(crate) async fn set_active(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    account: UserId,
    active: bool,
) -> Result<(), String> {
    let status = AccountStatusOperation::new(AccountStatusConfig {
        actor: actor.clone(),
        auth_context: auth.clone(),
        target: account,
        active,
        now: unix_timestamp_secs(),
    });
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
