//! Resolves the key holders of an encrypted bucket and whether they meet the recovery rule.
//! Holders are the creator, users with WRITE on the group admin path and explicit grants; a user
//! who loses admin rights stops being an implicit holder at once (D30).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::permission_path::permission_pattern_matches;
use crate::structs::identity::auth::{Permission, Role};
use crate::structs::identity::user::vault::UserKeyRecord;
use crate::structs::storage::encryption::{BucketHolder, HolderOrigin, SealedCopy};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// What the key directory answered for one user.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyLookup {
    Keys(Vec<UserKeyRecord>),
    /// Every reached holder answered without a key record.
    Missing,
    /// No holder of the directory answered.
    Unavailable,
}

/// The state of one holder for the active key generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HolderState {
    /// A sealed copy of the active generation exists.
    Ready,
    /// The user has a key, but no copy is sealed yet; the next unlock seals one.
    Pending,
    MissingKey,
    /// The key directory could not be read, so readiness is unknown.
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HolderEntry {
    pub user_id: UserId,
    pub origin: HolderOrigin,
    pub state: HolderState,
    /// The user's key record declares a recovery code; unknown when the directory failed.
    pub has_recovery: Option<bool>,
    /// The grant record, for explicit holders and holders that already got a copy.
    pub grant: Option<BucketHolder>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryState {
    Met,
    Degraded,
    /// Unresolved holders might still meet the rule.
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Recovery {
    pub state: RecoveryState,
    /// Distinct users with a ready copy; several keys of one user count once.
    pub ready_holders: usize,
    pub ready_with_recovery: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HolderReport {
    pub holders: Vec<HolderEntry>,
    /// False when a directory lookup failed, so the list is partial.
    pub complete: bool,
    pub unresolved: usize,
    pub recovery: Recovery,
}

impl HolderReport {
    /// The holder entry of `user_id`, if the user is a holder now.
    pub fn holder(&self, user_id: UserId) -> Option<&HolderEntry> {
        self.holders.iter().find(|holder| holder.user_id == user_id)
    }
}

/// Digest of a bucket's stored grants and copies with their contents. A removal names the
/// revision it was decided on, so a concurrent holder change refuses it.
pub fn holder_revision(grants: &[BucketHolder], copies: &[SealedCopy]) -> [u8; 32] {
    let grant_rows = grants
        .iter()
        .map(|grant| (grant.key(), grant.to_bytes().unwrap_or_default()));
    let copy_rows = copies
        .iter()
        .map(|copy| (copy.key(), copy.to_bytes().unwrap_or_default()));
    let mut rows: Vec<_> = grant_rows.chain(copy_rows).collect();
    rows.sort_unstable();
    let mut hasher = blake3::Hasher::new();
    for (key, value) in rows {
        for part in [key, value] {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(&part);
        }
    }
    *hasher.finalize().as_bytes()
}

/// `rows` from `holder_revision` with the resolved authority and recovery facts of `report`, so
/// a lost admin role or a changed key directory answer also refuses a stale removal.
pub fn revision_with_facts(
    rows: [u8; 32],
    creator: UserId,
    admins: &BTreeSet<UserId>,
    report: &HolderReport,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&rows);
    hasher.update(&creator.to_storage_key());
    for admin in admins {
        hasher.update(&admin.to_storage_key());
    }
    // Debug names are stable within one build, which is all a list-then-remove round needs.
    for holder in &report.holders {
        hasher.update(&holder.user_id.to_storage_key());
        let facts = format!(
            "{:?}{:?}{:?}",
            holder.origin, holder.state, holder.has_recovery
        );
        hasher.update(&(facts.len() as u64).to_be_bytes());
        hasher.update(facts.as_bytes());
    }
    hasher.update(format!("{:?}", report.recovery).as_bytes());
    *hasher.finalize().as_bytes()
}

/// Users with WRITE on `admin_path` through the realm and group roles; a matching deny of the
/// user wins. Public roles grant no admin rights.
pub fn admin_users<'a>(
    roles: impl IntoIterator<Item = &'a Role>,
    admin_path: &str,
) -> BTreeSet<UserId> {
    let mut decisions: BTreeMap<UserId, (bool, bool)> = BTreeMap::new();
    for role in roles {
        let patterns = role
            .permissions
            .iter()
            .filter(|(pattern, _)| permission_pattern_matches(pattern, admin_path));
        for (_, permission) in patterns {
            for user_id in role.assigned_users.iter().filter(|user| !user.is_nil()) {
                let (write, deny) = decisions.entry(*user_id).or_default();
                *write |= *permission == Permission::WRITE;
                *deny |= *permission == Permission::DENY;
            }
        }
    }
    decisions
        .into_iter()
        .filter(|(_, (write, deny))| *write && !*deny)
        .map(|(user_id, _)| user_id)
        .collect()
}

/// The holders of a bucket with their state for the active generation. `grants` are the
/// `bucket_holders` rows; a stored admin row whose user is no admin any more holds nothing.
pub fn resolve_holders(
    creator: UserId,
    admins: &BTreeSet<UserId>,
    grants: &[BucketHolder],
    lookups: &BTreeMap<UserId, KeyLookup>,
    copies: &[SealedCopy],
) -> HolderReport {
    let mut eligible: BTreeMap<UserId, (HolderOrigin, Option<BucketHolder>)> = BTreeMap::new();
    let mut add = |user_id, origin, grant| {
        eligible.entry(user_id).or_insert((origin, None)).1 = grant;
    };
    add(creator, HolderOrigin::Creator, None);
    for admin in admins {
        add(*admin, HolderOrigin::Admin, None);
    }
    for grant in grants
        .iter()
        .filter(|grant| grant.origin == HolderOrigin::Explicit)
    {
        add(grant.user_id, HolderOrigin::Explicit, Some(grant.clone()));
    }
    for grant in grants
        .iter()
        .filter(|grant| grant.origin != HolderOrigin::Explicit)
    {
        if let Some((_, slot)) = eligible.get_mut(&grant.user_id) {
            slot.get_or_insert_with(|| grant.clone());
        }
    }
    let holders: Vec<_> = eligible
        .into_iter()
        .map(|(user_id, (origin, grant))| {
            let own = |copy: &&SealedCopy| copy.user_id == user_id;
            let ready = copies.iter().any(|copy| own(&copy));
            let lookup = lookups.get(&user_id).unwrap_or(&KeyLookup::Unavailable);
            let (state, has_recovery) = match lookup {
                KeyLookup::Keys(keys) if !keys.is_empty() => {
                    let state = if ready {
                        HolderState::Ready
                    } else {
                        HolderState::Pending
                    };
                    // A ready holder recovers only through a key that has a copy of the bucket key.
                    let sealed = |key: &&UserKeyRecord| {
                        !ready
                            || copies
                                .iter()
                                .filter(own)
                                .any(|c| c.key_record == key.record_id)
                    };
                    (
                        state,
                        Some(keys.iter().filter(sealed).any(|key| key.has_recovery)),
                    )
                }
                KeyLookup::Keys(_) | KeyLookup::Missing => match ready {
                    true => (HolderState::Ready, Some(false)),
                    false => (HolderState::MissingKey, Some(false)),
                },
                KeyLookup::Unavailable => match ready {
                    true => (HolderState::Ready, None),
                    false => (HolderState::Unavailable, None),
                },
            };
            HolderEntry {
                user_id,
                origin,
                state,
                has_recovery,
                grant,
            }
        })
        .collect();
    let unresolved = holders
        .iter()
        .filter(|holder| holder.has_recovery.is_none())
        .count();
    let ready = holders
        .iter()
        .filter(|holder| holder.state == HolderState::Ready);
    let ready_holders = ready.clone().count();
    let ready_with_recovery = ready
        .filter(|holder| holder.has_recovery == Some(true))
        .count();
    let met = ready_holders >= 2 || (ready_holders == 1 && ready_with_recovery == 1);
    let state = match (met, unresolved) {
        (true, _) => RecoveryState::Met,
        (false, 0) => RecoveryState::Degraded,
        (false, _) => RecoveryState::Unknown,
    };
    HolderReport {
        holders,
        complete: unresolved == 0,
        unresolved,
        recovery: Recovery {
            state,
            ready_holders,
            ready_with_recovery,
        },
    }
}

#[cfg(test)]
#[path = "holders_tests.rs"]
mod tests;
