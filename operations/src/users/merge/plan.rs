//! Decides who may merge and what moves from the merged account to the surviving one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::MergeError;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{Permission, Role};
use aruna_core::structs::identity::user::User;
use aruna_core::types::RoleId;
use aruna_core::user::validation::{
    MAX_USER_ATTRIBUTES, counted_attributes, is_reserved_attribute,
};
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

/// Per document: the roles handed to the survivor, then all roles `merged` holds there.
type Handover = (Vec<RoleId>, Vec<RoleId>);

/// The roles `merged` holds in each document and those handed to `survivor`, realm first. A
/// role is handed when it grants a path above the survivor's level, and so is every deny of
/// `merged`, in any document, that overlaps a handed grant, since a deny wins over any grant.
/// A handed deny that also overlaps a grant of the survivor is refused, naming its role; the
/// survivor's grants count from every document, `others` being those `merged` has no role in.
pub(super) fn hand_over(
    documents: &[&HashMap<RoleId, Role>],
    others: &[&HashMap<RoleId, Role>],
    survivor: UserId,
    merged: UserId,
) -> Result<Vec<Handover>, MergeError> {
    let holds = |user: UserId| {
        documents
            .iter()
            .flat_map(|roles| roles.values())
            .filter(move |role| role.assigned_users.contains(&user))
    };
    let mut own = HashMap::<&str, u8>::new();
    for (path, permission) in holds(survivor).flat_map(|role| role.permissions.iter()) {
        let best = own.entry(path.as_str()).or_default();
        *best = (*best).max(level(permission));
    }
    let grants_more = |role: &Role| {
        role.permissions.iter().any(|(path, permission)| {
            level(permission) > own.get(path.as_str()).copied().unwrap_or_default()
        })
    };
    let mut plans: Vec<(Vec<RoleId>, Vec<RoleId>)> = documents
        .iter()
        .map(|roles| {
            let mut held: Vec<RoleId> = roles
                .iter()
                .filter(|(_, role)| role.assigned_users.contains(&merged))
                .map(|(role_id, _)| *role_id)
                .collect();
            held.sort();
            let handed = held
                .iter()
                .copied()
                .filter(|role_id| grants_more(&roles[role_id]))
                .collect();
            (handed, held)
        })
        .collect();
    let granted: Vec<&str> = documents
        .iter()
        .zip(&plans)
        .flat_map(|(roles, (handed, _))| handed.iter().map(|role_id| &roles[role_id]))
        .flat_map(|role| grant_paths(role))
        .collect();
    for (roles, (handed, held)) in documents.iter().zip(plans.iter_mut()) {
        for role_id in held.iter() {
            let denies = deny_paths(&roles[role_id]);
            if !handed.contains(role_id)
                && denies
                    .iter()
                    .any(|deny| granted.iter().any(|grant| overlaps(deny, grant)))
            {
                handed.push(*role_id);
            }
        }
        handed.sort();
    }
    let own_grants: Vec<&str> = documents
        .iter()
        .chain(others)
        .flat_map(|roles| roles.values())
        .filter(|role| role.assigned_users.contains(&survivor))
        .flat_map(grant_paths)
        .collect();
    for (roles, (handed, _)) in documents.iter().zip(&plans) {
        for role in handed.iter().map(|role_id| &roles[role_id]) {
            if deny_paths(role)
                .iter()
                .any(|deny| own_grants.iter().any(|grant| overlaps(deny, grant)))
            {
                return Err(MergeError::RoleConflict(role.name.clone()));
            }
        }
    }
    Ok(plans)
}

fn grant_paths(role: &Role) -> Vec<&str> {
    role.permissions
        .iter()
        .filter(|(_, permission)| **permission != Permission::DENY)
        .map(|(path, _)| path.as_str())
        .collect()
}

fn deny_paths(role: &Role) -> Vec<&str> {
    role.permissions
        .iter()
        .filter(|(_, permission)| **permission == Permission::DENY)
        .map(|(path, _)| path.as_str())
        .collect()
}

/// Whether two path patterns may match a common path, judged by their literal prefixes; it
/// errs towards an overlap, so a doubtful deny is kept or refused, never dropped.
fn overlaps(left: &str, right: &str) -> bool {
    let literal = |pattern: &'_ str| {
        pattern
            .split(['*', '?', '[', ']', '{', '}', '\\'])
            .next()
            .unwrap_or_default()
            .to_string()
    };
    let (left, right) = (literal(left), literal(right));
    left.starts_with(&right) || right.starts_with(&left)
}

/// Grant levels; a deny never counts as a grant and is handled apart.
fn level(permission: &Permission) -> u8 {
    match permission {
        Permission::DENY => 0,
        Permission::READ => 1,
        Permission::WRITE => 2,
    }
}

/// Attributes of `merged` that `survivor` lacks, in key order, as many as fit the survivor's
/// budget, and the keys of those that did not fit. Reserved ones never move.
pub(super) fn copied_attributes(
    survivor: &User,
    merged: &User,
) -> (Vec<(String, String)>, Vec<String>) {
    let mut copied: Vec<_> = merged
        .attributes
        .iter()
        .filter(|(key, _)| !is_reserved_attribute(key) && !survivor.attributes.contains_key(*key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    copied.sort();
    let room = MAX_USER_ATTRIBUTES.saturating_sub(counted_attributes(survivor.attributes.keys()));
    let skipped = copied
        .split_off(room.min(copied.len()))
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    (copied, skipped)
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
