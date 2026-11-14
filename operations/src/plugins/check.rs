//! The check every plugin call passes on the receiver before a stored reply or a new effect.
//! Steps 0 to 3 decide on state read once for the call; step 4 authorizes as the account.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::PluginRun;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_plugins::broker::{ErrorKind, HostError};
use aruna_plugins::permission::Permission;
use ulid::Ulid;

use super::account::read_user;
use super::grants::{InstallScope, accesses, parse_grants};
use super::install::read_config;
use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::DriverContext;

/// What one run's calls are checked against: the run and the node that runs it.
#[derive(Clone, Debug)]
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
    if install.state != InstallState::Active {
        return Err(denied(&format!(
            "the install is {}",
            install.state.as_str()
        )));
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
    let patterns: BTreeSet<String> = (grants.get(&permission).into_iter().flatten())
        .filter(|pattern| covers(pattern, target))
        .cloned()
        .collect();
    match patterns.is_empty() {
        true => Err(HostError::new(
            ErrorKind::NotGranted,
            format!("{permission} is not granted on {target}"),
        )),
        false => Ok(patterns),
    }
}

/// Whether a granted pattern, an exact path or a `<base>/**` subtree, covers `target`.
pub fn covers(pattern: &str, target: &str) -> bool {
    match pattern.strip_suffix("/**") {
        Some(base) => {
            target == base
                || target
                    .strip_prefix(base)
                    .is_some_and(|rest| rest.starts_with('/'))
        }
        None => pattern == target,
    }
}

/// Step 4: each access of `permission` on `target`, as the account restricted to `patterns`,
/// against roles, denies and policies with the operation `plugin.<permission>`.
pub async fn authorize_call(
    ctx: &DriverContext,
    install: &PluginInstall,
    permission: Permission,
    target: &str,
    patterns: &BTreeSet<String>,
) -> Result<(), HostError> {
    let realm_id = install.account.realm_id;
    for access in accesses(permission) {
        let restrictions = (patterns.iter())
            .map(|pattern| PathRestriction {
                pattern: pattern.clone(),
                permission: access.clone(),
            })
            .collect();
        let auth = AuthContext {
            user_id: install.account,
            realm_id,
            path_restrictions: Some(restrictions),
            session: None,
        };
        let extras = PolicyRequestExtras::operation(format!("plugin.{permission}"));
        match authorize(ctx, realm_id, &auth, target, access, extras).await {
            Ok(()) => {}
            Err(AuthorizeError::Storage(error)) => return Err(unavailable(error.to_string())),
            Err(AuthorizeError::CheckFailed(error)) => return Err(unavailable(error)),
            Err(error) => return Err(HostError::new(ErrorKind::Denied, error.to_string())),
        }
    }
    Ok(())
}

/// Steps 0 to 4 for one call; `target` is the permission and resource a guarded call names.
/// State and log calls name none and pass steps 0 to 2 only.
pub async fn check_call(
    ctx: &DriverContext,
    scope: &RunScope,
    target: Option<(Permission, &str)>,
) -> Result<PluginInstall, HostError> {
    let config =
        (read_config(ctx, scope.realm_id).await).map_err(|error| unavailable(error.to_string()))?;
    let account = (config.installs.iter())
        .find(|install| install.install_id == scope.run.install_id)
        .map(|install| install.account);
    let user = match account {
        Some(account) => read_user(ctx, account).await.map_err(unavailable)?,
        None => None,
    };
    let install = check_run(scope, &config, user.as_ref())?.clone();
    if let Some((permission, resource)) = target {
        let patterns = matching(&install, permission, resource)?;
        authorize_call(ctx, &install, permission, resource, &patterns).await?;
    }
    Ok(install)
}

fn denied(message: &str) -> HostError {
    HostError::new(ErrorKind::Denied, message)
}

pub(crate) fn unavailable(message: impl Into<String>) -> HostError {
    HostError::new(ErrorKind::Unavailable, message)
}

#[cfg(test)]
#[path = "check_tests.rs"]
mod tests;
