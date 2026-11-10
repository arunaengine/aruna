//! Computes what an install may do from its manifest: the default grants, an admin's narrowing,
//! the role the account gets, and what an update keeps. Pure; the admin's rights are checked apart.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};

use aruna_core::structs::identity::auth::Permission as Access;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use aruna_plugins::manifest::Manifest;
use aruna_plugins::permission::Permission;
use aruna_plugins::trigger::TriggerKind;
use thiserror::Error;
use ulid::Ulid;

/// Granted resource patterns by permission.
pub type Grants = BTreeMap<Permission, BTreeSet<String>>;

pub const READ_ROLE: &str = "plugin-read";
pub const WRITE_ROLE: &str = "plugin-write";

#[derive(Debug, Error, PartialEq)]
pub enum GrantError {
    #[error("`{0}` is not a plugin permission")]
    Unknown(String),
    #[error("{0} is not requested by this plugin version")]
    NotRequested(Permission),
    #[error("{0} is required and cannot be removed")]
    Required(String),
    #[error("`{pattern}` is not a {permission} resource of this group")]
    Outside {
        permission: Permission,
        pattern: String,
    },
    #[error("connection `{0}` is not declared by this plugin version")]
    UnknownConnection(String),
    #[error("{0}")]
    Unsupported(&'static str),
}

/// Where one install lives: its realm, group and id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstallScope {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub install_id: Ulid,
}

impl InstallScope {
    /// The policy path below which a permission's resources live.
    pub fn root(&self, permission: Permission) -> String {
        let group = format!("/{}/g/{}", self.realm_id, self.group_id);
        match permission {
            Permission::MetadataRead | Permission::MetadataWrite => format!("{group}/meta"),
            Permission::ConnectionsSend => {
                format!("{group}/plugins/{}/connections", self.install_id)
            }
            _ => format!("{group}/data"),
        }
    }

    /// The generated grant path of one declared connection.
    pub fn connection(&self, name: &str) -> String {
        format!("{}/{name}", self.root(Permission::ConnectionsSend))
    }
}

/// Refuses what this version of Aruna cannot run yet: secrets, webhooks and container work.
pub fn check_supported(manifest: &Manifest) -> Result<(), GrantError> {
    if !manifest.secrets.is_empty() {
        return Err(GrantError::Unsupported(
            "plugins with secret slots cannot be installed yet",
        ));
    }
    if manifest.triggers.contains_key(&TriggerKind::Webhook) {
        return Err(GrantError::Unsupported(
            "plugins with webhooks cannot be installed yet",
        ));
    }
    let requested = manifest.requested();
    if requested.contains(&Permission::SecretsValue) || requested.contains(&Permission::JobsRun) {
        return Err(GrantError::Unsupported(
            "secrets.value and jobs.run cannot be granted yet",
        ));
    }
    Ok(())
}

/// Everything requested, on the whole group, and every declared connection.
pub fn default_grants(manifest: &Manifest, scope: &InstallScope) -> Grants {
    let mut grants: Grants = (manifest.requested().into_iter())
        .map(|permission| (permission, BTreeSet::from([whole(scope, permission)])))
        .collect();
    if !manifest.connections.is_empty() {
        let paths = manifest
            .connections
            .keys()
            .map(|name| scope.connection(name));
        grants.insert(Permission::ConnectionsSend, paths.collect());
    }
    grants
}

/// The default grants with the admin's choices applied. A listed permission keeps only the
/// listed patterns; an empty list removes an optional one. Connections are listed by name.
pub fn narrow_grants(
    manifest: &Manifest,
    scope: &InstallScope,
    choices: &BTreeMap<String, Vec<String>>,
) -> Result<Grants, GrantError> {
    let mut grants = default_grants(manifest, scope);
    for (name, chosen) in choices {
        let permission: Permission =
            (name.parse()).map_err(|_| GrantError::Unknown(name.clone()))?;
        let granted = match permission {
            Permission::ConnectionsSend => connection_grants(manifest, scope, chosen)?,
            _ => resource_grants(manifest, scope, permission, chosen)?,
        };
        match granted.is_empty() {
            true => grants.remove(&permission),
            false => grants.insert(permission, granted),
        };
    }
    Ok(grants)
}

fn resource_grants(
    manifest: &Manifest,
    scope: &InstallScope,
    permission: Permission,
    chosen: &[String],
) -> Result<BTreeSet<String>, GrantError> {
    if !manifest.requested().contains(&permission) {
        return Err(GrantError::NotRequested(permission));
    }
    if chosen.is_empty() && manifest.permissions.required.contains(&permission) {
        return Err(GrantError::Required(permission.to_string()));
    }
    let root = scope.root(permission);
    let outside = |pattern: &String| GrantError::Outside {
        permission,
        pattern: pattern.clone(),
    };
    (chosen.iter())
        .map(|pattern| {
            within(pattern, &root)
                .then(|| pattern.clone())
                .ok_or_else(|| outside(pattern))
        })
        .collect()
}

fn connection_grants(
    manifest: &Manifest,
    scope: &InstallScope,
    chosen: &[String],
) -> Result<BTreeSet<String>, GrantError> {
    if let Some(unknown) = chosen
        .iter()
        .find(|name| !manifest.connections.contains_key(*name))
    {
        return Err(GrantError::UnknownConnection(unknown.clone()));
    }
    let required = (manifest.connections.iter()).find(|(name, connection)| {
        !connection.optional && !chosen.iter().any(|chosen| chosen == *name)
    });
    if let Some((name, _)) = required {
        return Err(GrantError::Required(format!("connection `{name}`")));
    }
    Ok(chosen.iter().map(|name| scope.connection(name)).collect())
}

/// The whole subtree of a permission's resources in this group.
fn whole(scope: &InstallScope, permission: Permission) -> String {
    format!("{}/**", scope.root(permission))
}

/// Whether `pattern` names `root/**` or a path or subtree below `root`: clean segments, and
/// `**` only as the last one.
pub fn within(pattern: &str, root: &str) -> bool {
    let Some(rest) = pattern
        .strip_prefix(root)
        .and_then(|rest| rest.strip_prefix('/'))
    else {
        return false;
    };
    let segments: Vec<&str> = rest.split('/').collect();
    let last = segments.len() - 1;
    (segments.iter().enumerate()).all(|(index, segment)| {
        let glob = segment.contains(['*', '?', '[', ']', '{', '}', '\\']);
        match *segment {
            "**" => index == last,
            "" | "." | ".." => false,
            _ => !glob,
        }
    })
}

/// The role access a permission needs on its patterns.
fn access(permission: Permission) -> Access {
    match permission {
        Permission::MetadataWrite
        | Permission::ObjectsCopy
        | Permission::TransfersDownload
        | Permission::JobsRun => Access::WRITE,
        Permission::MetadataRead
        | Permission::ObjectsRead
        | Permission::TransfersUpload
        | Permission::ConnectionsSend
        | Permission::SecretsValue => Access::READ,
    }
}

/// The account role's paths: each granted pattern with the access its permissions need.
pub fn role_paths(grants: &Grants) -> HashMap<String, Access> {
    let mut paths = HashMap::new();
    for (permission, patterns) in grants {
        for pattern in patterns {
            let held = paths.entry(pattern.clone()).or_insert(Access::READ);
            if access(*permission) == Access::WRITE {
                *held = Access::WRITE;
            }
        }
    }
    paths
}

/// `plugin-write` when any path needs WRITE, else `plugin-read`.
pub fn role_name(paths: &HashMap<String, Access>) -> &'static str {
    match paths.values().any(|access| *access == Access::WRITE) {
        true => WRITE_ROLE,
        false => READ_ROLE,
    }
}

/// What an update to `manifest` keeps of `granted`: grants the new version still requests and
/// connections it still declares. It grants nothing new; the second value lists what the new
/// version requires but nobody granted yet.
pub fn carry_grants(
    granted: &Grants,
    manifest: &Manifest,
    scope: &InstallScope,
) -> (Grants, Vec<String>) {
    let requested = manifest.requested();
    let declared: BTreeSet<String> = manifest
        .connections
        .keys()
        .map(|n| scope.connection(n))
        .collect();
    let mut kept = Grants::new();
    for (permission, patterns) in granted {
        let patterns: BTreeSet<String> = match permission {
            Permission::ConnectionsSend => patterns.intersection(&declared).cloned().collect(),
            _ if requested.contains(permission) => patterns.clone(),
            _ => BTreeSet::new(),
        };
        if !patterns.is_empty() {
            kept.insert(*permission, patterns);
        }
    }
    let mut missing: Vec<String> = (manifest.permissions.required.iter())
        .filter(|permission| !kept.contains_key(*permission))
        .map(|permission| permission.to_string())
        .collect();
    let sends = kept.get(&Permission::ConnectionsSend);
    for (name, connection) in &manifest.connections {
        if !connection.optional
            && !sends.is_some_and(|paths| paths.contains(&scope.connection(name)))
        {
            missing.push(format!("connection `{name}`"));
        }
    }
    (kept, missing)
}

/// Grants as stored in the install record.
pub fn stored_grants(grants: &Grants) -> BTreeMap<String, Vec<String>> {
    (grants.iter())
        .map(|(permission, patterns)| (permission.to_string(), patterns.iter().cloned().collect()))
        .collect()
}

/// Grants read back from an install record; names this version does not know are dropped.
pub fn parse_grants(stored: &BTreeMap<String, Vec<String>>) -> Grants {
    (stored.iter())
        .filter_map(|(name, patterns)| {
            Some((name.parse().ok()?, patterns.iter().cloned().collect()))
        })
        .collect()
}

#[cfg(test)]
#[path = "grants_tests.rs"]
mod tests;
