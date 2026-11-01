//! Decides who may merge and what moves from the merged account to the surviving one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::MergeError;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{Permission, Role};
use aruna_core::structs::identity::user::User;
use aruna_core::types::RoleId;
use aruna_core::user::validation::{MAX_USER_ATTRIBUTES, is_reserved_attribute};
use std::collections::{BTreeSet, HashMap};

/// Refuses accounts that may not merge; `Ok(true)` when `merged` already merged into `survivor`.
pub(super) fn check_accounts(survivor: &User, merged: &User) -> Result<bool, MergeError> {
    if merged.merged_into() == Some(survivor.user_id) {
        return Ok(true);
    }
    if survivor.user_id == merged.user_id {
        return Err(MergeError::SameAccount);
    }
    if survivor.user_id.realm_id != merged.user_id.realm_id {
        return Err(MergeError::OtherRealm);
    }
    if survivor.is_deactivated() || merged.is_deactivated() {
        return Err(MergeError::Inactive);
    }
    if survivor.service_group().is_some() || merged.service_group().is_some() {
        return Err(MergeError::ServiceAccount);
    }
    Ok(false)
}

/// The roles `merged` holds, and those of them that grant `survivor` a path above its own level.
pub(super) fn handed_roles(
    roles: &HashMap<RoleId, Role>,
    survivor: UserId,
    merged: UserId,
) -> (Vec<RoleId>, Vec<RoleId>) {
    let mut own = HashMap::<&str, u8>::new();
    for role in roles.values() {
        if role.assigned_users.contains(&survivor) {
            for (path, permission) in &role.permissions {
                let best = own.entry(path.as_str()).or_default();
                *best = (*best).max(level(permission));
            }
        }
    }
    let mut held: Vec<RoleId> = roles
        .iter()
        .filter(|(_, role)| role.assigned_users.contains(&merged))
        .map(|(role_id, _)| *role_id)
        .collect();
    held.sort();
    let handed = held
        .iter()
        .copied()
        .filter(|role_id| {
            roles[role_id].permissions.iter().any(|(path, permission)| {
                level(permission) > own.get(path.as_str()).copied().unwrap_or_default()
            })
        })
        .collect();
    (handed, held)
}

fn level(permission: &Permission) -> u8 {
    match permission {
        Permission::DENY => 0,
        Permission::READ => 1,
        Permission::WRITE => 2,
    }
}

/// Attributes of `merged` that `survivor` lacks, in key order; reserved ones never move.
pub(super) fn copied_attributes(survivor: &User, merged: &User) -> Vec<(String, String)> {
    let mut copied: Vec<_> = merged
        .attributes
        .iter()
        .filter(|(key, _)| !is_reserved_attribute(key) && !survivor.attributes.contains_key(*key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    copied.sort();
    copied.truncate(MAX_USER_ATTRIBUTES.saturating_sub(survivor.attributes.len()));
    copied
}

/// The owners of one subject once it moved from `merged` to `survivor`.
pub(super) fn moved_claims(
    mut claims: BTreeSet<UserId>,
    survivor: UserId,
    merged: UserId,
) -> BTreeSet<UserId> {
    claims.remove(&merged);
    claims.insert(survivor);
    claims
}
