//! Computes what an install may do from its manifest: the default grants, an admin's narrowing,
//! the role the account gets, and what an update keeps. Pure; the admin's rights are checked apart.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};

use aruna_core::structs::identity::auth::Permission as Access;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use aruna_plugins::manifest::Manifest;
use aruna_plugins::permission::{Grant, Permission};
use thiserror::Error;
use ulid::Ulid;

/// Granted resource patterns: `read` and `write` as the installer scoped them, and the paths of
/// the services and readable secrets the manifest declares.
pub type Grants = BTreeMap<Grant, BTreeSet<String>>;

pub const READ_ROLE: &str = "plugin-read";
pub const WRITE_ROLE: &str = "plugin-write";

#[derive(Debug, Error, PartialEq)]
pub enum GrantError {
    #[error("`{0}` is not a grant an installer chooses; choose `read` or `write`")]
    Unknown(String),
    #[error("{0} is not requested by this plugin version")]
    NotRequested(Grant),
    #[error("{0} is required by this plugin version")]
    Required(Grant),
    #[error("`{pattern}` is not a {grant} resource of this group")]
    Outside { grant: Grant, pattern: String },
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
    /// The group's policy path; `read` and `write` patterns lie in its `meta` or `data` subtree.
    pub fn group(&self) -> String {
        format!("/{}/g/{}", self.realm_id, self.group_id)
    }

    /// The generated grant path of one declared connection.
    pub fn connection(&self, name: &str) -> String {
        let group = self.group();
        format!("{group}/plugins/{}/connections/{name}", self.install_id)
    }

    /// The grant path of one secret slot's value.
    pub fn secret(&self, slot: &str) -> String {
        let group = self.group();
        format!("{group}/plugins/{}/secrets/{slot}", self.install_id)
    }
}

/// Refuses what this version of Aruna cannot run yet: container work.
pub fn check_supported(manifest: &Manifest) -> Result<(), GrantError> {
    if manifest.requested().contains(&Permission::JobsRun) {
        return Err(GrantError::Unsupported("jobs.run cannot be granted yet"));
    }
    Ok(())
}

/// Whether the manifest asks to change anything: a requested permission only `write` covers.
pub fn writes(manifest: &Manifest) -> bool {
    (manifest.requested().into_iter()).any(|permission| permission.grants() == [Grant::Write])
}

/// `read`, and `write` when requested, on the whole group, and every declared service and
/// readable secret.
pub fn default_grants(manifest: &Manifest, scope: &InstallScope) -> Grants {
    let whole = BTreeSet::from([format!("{}/**", scope.group())]);
    let mut grants = Grants::from([(Grant::Read, whole.clone())]);
    if writes(manifest) {
        grants.insert(Grant::Write, whole);
    }
    grants.extend(declared(manifest, scope));
    grants
}

/// The service and readable secret paths the manifest declares; nobody chooses them.
fn declared(manifest: &Manifest, scope: &InstallScope) -> Grants {
    let services: BTreeSet<String> = (manifest.connections.keys())
        .map(|name| scope.connection(name))
        .collect();
    let secrets: BTreeSet<String> = (manifest.secrets.iter())
        .filter(|(_, slot)| slot.readable)
        .map(|(name, _)| scope.secret(name))
        .collect();
    [(Grant::Services, services), (Grant::Secrets, secrets)]
        .into_iter()
        .filter(|(_, paths)| !paths.is_empty())
        .collect()
}

/// The default grants with the admin's choices applied: `read` or `write` keeps only the listed
/// patterns, and an empty list removes it.
pub fn narrow_grants(
    manifest: &Manifest,
    scope: &InstallScope,
    choices: &BTreeMap<String, Vec<String>>,
) -> Result<Grants, GrantError> {
    let mut grants = default_grants(manifest, scope);
    for (name, chosen) in choices {
        let grant = (name.parse::<Grant>().ok())
            .filter(|grant| grant.scoped())
            .ok_or_else(|| GrantError::Unknown(name.clone()))?;
        if grant == Grant::Write && !writes(manifest) {
            return Err(GrantError::NotRequested(grant));
        }
        let granted = resource_grants(scope, grant, chosen)?;
        match granted.is_empty() {
            true => grants.remove(&grant),
            false => grants.insert(grant, granted),
        };
    }
    match unmet(manifest, &grants).into_iter().next() {
        Some(grant) => Err(GrantError::Required(grant)),
        None => Ok(grants),
    }
}

/// The scoped grants a required permission of the manifest needs but `grants` lacks.
fn unmet(manifest: &Manifest, grants: &Grants) -> BTreeSet<Grant> {
    (manifest.permissions.required.iter())
        .filter(|permission| permission.grants().iter().all(|grant| grant.scoped()))
        .filter(|permission| patterns_for(grants, **permission).is_empty())
        .map(|permission| permission.grants()[0])
        .collect()
}

fn resource_grants(
    scope: &InstallScope,
    grant: Grant,
    chosen: &[String],
) -> Result<BTreeSet<String>, GrantError> {
    let group = scope.group();
    let inside = |pattern: &String| {
        pattern == &format!("{group}/**")
            || ["meta", "data"]
                .iter()
                .any(|tree| within(pattern, &format!("{group}/{tree}")))
    };
    (chosen.iter())
        .map(|pattern| match inside(pattern) {
            true => Ok(pattern.clone()),
            false => Err(GrantError::Outside {
                grant,
                pattern: pattern.clone(),
            }),
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

/// The access the role needs on a grant's patterns.
pub fn grant_access(grant: Grant) -> Access {
    match grant {
        Grant::Write => Access::WRITE,
        Grant::Read | Grant::Services | Grant::Secrets => Access::READ,
    }
}

/// The account role's paths: each granted pattern with the access its grant needs.
pub fn role_paths(grants: &Grants) -> HashMap<String, Access> {
    let mut paths = HashMap::new();
    for (grant, patterns) in grants {
        for pattern in patterns {
            let held = paths.entry(pattern.clone()).or_insert(Access::READ);
            if grant_access(*grant) == Access::WRITE {
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

/// What an update to `manifest` keeps of `granted`: `read`, `write` while the new version still
/// writes, and the services and secrets it declares. It grants nothing new; the second value
/// lists what the new version requires but nobody granted yet.
pub fn carry_grants(
    granted: &Grants,
    manifest: &Manifest,
    scope: &InstallScope,
) -> (Grants, Vec<String>) {
    let mut kept: Grants = (granted.iter())
        .filter(|(grant, _)| {
            **grant == Grant::Read || (**grant == Grant::Write && writes(manifest))
        })
        .map(|(grant, patterns)| (*grant, patterns.clone()))
        .collect();
    kept.extend(declared(manifest, scope));
    let missing = (unmet(manifest, &kept).iter())
        .map(|grant| grant.to_string())
        .collect();
    (kept, missing)
}

/// Grants as stored in the install record.
pub fn stored_grants(grants: &Grants) -> BTreeMap<String, Vec<String>> {
    (grants.iter())
        .map(|(grant, patterns)| (grant.to_string(), patterns.iter().cloned().collect()))
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
