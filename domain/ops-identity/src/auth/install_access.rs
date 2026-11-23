//! Authorizes what a plugin install's account does: the patterns its grants hold for a
//! permission, and each access on a target as the account restricted to those patterns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::request_policy::{PolicyRequestExtras, policy_request_with};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission as Access};
use aruna_core::structs::identity::realm::RealmId;
use aruna_plugin_contract::permission::{Grant, Permission};

use super::check_permissions::CheckPermissionsConfig;
use super::enforcement::{compiled_policies, group_from_path, policy_reads, read_policies};
use super::error::{AuthorizeError, check_error};

/// Granted resource patterns: `read` and `write` as the installer scoped them, and the paths of
/// the services and readable secrets the manifest declares.
pub type Grants = BTreeMap<Grant, BTreeSet<String>>;

/// Grants read back from an install record; names this version does not know are dropped.
pub fn parse_grants(stored: &BTreeMap<String, Vec<String>>) -> Grants {
    (stored.iter())
        .filter_map(|(name, patterns)| {
            Some((name.parse().ok()?, patterns.iter().cloned().collect()))
        })
        .collect()
}

/// The granted patterns a call of `permission` may use.
pub fn patterns_for(grants: &Grants, permission: Permission) -> BTreeSet<String> {
    (permission.grants().iter())
        .filter_map(|grant| grants.get(grant))
        .flatten()
        .cloned()
        .collect()
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

/// The accesses a permission's actions need on its patterns, as the plan's table checks them.
pub fn accesses(permission: Permission) -> &'static [Access] {
    match permission {
        Permission::ObjectsCopy => &[Access::READ, Access::WRITE],
        Permission::MetadataWrite
        | Permission::MetadataCreate
        | Permission::TransfersDownload
        | Permission::JobsRun => &[Access::WRITE],
        Permission::MetadataRead
        | Permission::ObjectsRead
        | Permission::TransfersUpload
        | Permission::ConnectionsSend
        | Permission::SecretsValue => &[Access::READ],
    }
}

/// The patterns `install` holds for `permission` that cover `target`; none means not granted.
pub fn granted(install: &PluginInstall, permission: Permission, target: &str) -> BTreeSet<String> {
    let grants = parse_grants(&install.grants);
    (patterns_for(&grants, permission).into_iter())
        .filter(|pattern| covers(pattern, target))
        .collect()
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
    compiling: bool,
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
            compiling: false,
        }
    }

    fn realm(&self) -> RealmId {
        self.account.account.realm_id
    }

    /// Reads the rules of the next access, or ends when none is left.
    fn advance(&mut self) -> Result<Next<()>, AuthorizeError> {
        let Some(access) = self.accesses.pop_front() else {
            return Ok(Next::Done(()));
        };
        let config = CheckPermissionsConfig {
            auth_context: restricted(&self.account, &access, &self.patterns),
            path: self.target.clone(),
            required_permission: access,
        };
        let reads = config.reads().map_err(check_error)?;
        self.current = Some(config);
        self.rules_read = false;
        self.compiling = false;
        Ok(Next::Call(IoCall::Read(reads)))
    }
}

impl Operation for Authorize {
    type Output = ();
    type Error = AuthorizeError;

    fn first(&mut self) -> Result<Next<()>, AuthorizeError> {
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AuthorizeError> {
        let config = self.current.as_ref().ok_or(IoError::Unfinished)?;
        if self.compiling {
            let extras = PolicyRequestExtras::operation(format!("plugin.{}", self.permission));
            let auth = Some(&config.auth_context);
            let request =
                policy_request_with(&self.target, &config.required_permission, auth, extras);
            (compiled_policies(result)).and_then(|policies| policies.evaluate(&request))?;
            return self.advance();
        }
        let values = result.values()?;
        if !self.rules_read {
            let allowed = config.allowed(values);
            if !allowed.map_err(check_error)? {
                return Err(AuthorizeError::PermissionDenied);
            }
            self.rules_read = true;
            let reads = policy_reads(self.realm(), group_from_path(&self.target));
            return Ok(Next::Call(IoCall::Read(reads)));
        }
        let group = group_from_path(&self.target);
        let sets = read_policies(self.realm(), group, values)?;
        self.compiling = true;
        Ok(Next::Call(sets.compile()))
    }
}

#[cfg(test)]
#[path = "install_access_tests.rs"]
mod tests;
