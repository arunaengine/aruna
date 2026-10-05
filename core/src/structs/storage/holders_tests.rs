//! Holder resolution: origins, admin loss, directory failures and the recovery rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::record::PlacementRef;
use crate::structs::storage::encryption::{BucketKeyRef, GrantState};
use crate::vault_format::key_fingerprint;
use std::collections::{HashMap, HashSet};
use ulid::Ulid;

const ADMIN_PATH: &str = "/realm/g/group/admin";

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn role(users: &[UserId], permissions: &[(&str, Permission)]) -> Role {
    Role {
        role_id: Ulid::generate(),
        name: "any name".to_string(),
        permissions: permissions
            .iter()
            .map(|(pattern, permission)| (pattern.to_string(), permission.clone()))
            .collect::<HashMap<_, _>>(),
        assigned_users: users.iter().copied().collect::<HashSet<_>>(),
    }
}

/// The key record id of a user's first key, which `copy` seals to.
fn record(user_id: UserId) -> Ulid {
    Ulid::from_bytes([user_id.user_ulid.to_bytes()[0]; 16])
}

fn keys(user_id: UserId, recovery: bool) -> KeyLookup {
    KeyLookup::Keys(vec![UserKeyRecord {
        user_id,
        record_id: record(user_id),
        key_id: "slot".to_string(),
        public_key: [7; 32],
        fingerprint: key_fingerprint(&[7; 32]),
        has_recovery: recovery,
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        placement: PlacementRef::NIL,
        created_at_ms: 1,
    }])
}

fn copy(user_id: UserId) -> SealedCopy {
    SealedCopy {
        key: BucketKeyRef::new(Ulid::from_bytes([9; 16]), 1),
        user_id,
        key_record: record(user_id),
        key_id: "slot".to_string(),
        enc: [0; 32],
        ciphertext: vec![0; 48],
        created_at_ms: 1,
    }
}

fn grant(user_id: UserId, origin: HolderOrigin) -> BucketHolder {
    BucketHolder {
        bucket_id: Ulid::from_bytes([9; 16]),
        user_id,
        origin,
        state: GrantState::Pending,
        granted_by: user(1),
        granted_at_ms: 1,
    }
}

#[test]
fn admins_need_write() {
    let roles = [
        // Admin by permission, whatever the role is called.
        role(
            &[user(2), user(5)],
            &[("/realm/g/group/**", Permission::WRITE)],
        ),
        role(&[user(3)], &[("/realm/g/group/admin/**", Permission::READ)]),
        role(&[user(4)], &[("/realm/g/group/data/**", Permission::WRITE)]),
        role(&[user(5)], &[("/realm/g/group/admin", Permission::DENY)]),
        role(
            &[UserId::nil(RealmId::from_bytes([1; 32]))],
            &[("/**", Permission::WRITE)],
        ),
    ];
    assert_eq!(admin_users(&roles, ADMIN_PATH), BTreeSet::from([user(2)]));
}

#[test]
fn origins_and_states() {
    let (creator, admin, explicit, former) = (user(1), user(2), user(3), user(4));
    let admins = BTreeSet::from([admin]);
    let grants = [
        grant(explicit, HolderOrigin::Explicit),
        // A stored admin row whose user lost admin rights grants nothing (D30).
        grant(former, HolderOrigin::Admin),
    ];
    let lookups = BTreeMap::from([
        (creator, keys(creator, false)),
        (admin, KeyLookup::Missing),
        (explicit, keys(explicit, true)),
        (former, keys(former, true)),
    ]);
    let report = resolve_holders(creator, &admins, &grants, &lookups, &[copy(creator)]);

    let states: Vec<_> = report
        .holders
        .iter()
        .map(|holder| (holder.user_id, holder.origin, holder.state))
        .collect();
    assert_eq!(
        states,
        vec![
            (creator, HolderOrigin::Creator, HolderState::Ready),
            (admin, HolderOrigin::Admin, HolderState::MissingKey),
            (explicit, HolderOrigin::Explicit, HolderState::Pending),
        ]
    );
    assert!(report.holder(former).is_none());
    assert!(report.complete);
    // One ready holder without a recovery code, and pending grants never count.
    assert_eq!(report.recovery.state, RecoveryState::Degraded);
    assert_eq!(report.recovery.ready_holders, 1);
}

#[test]
fn recovery_counts_users() {
    let (creator, admin) = (user(1), user(2));
    let admins = BTreeSet::from([admin]);
    let lookups = BTreeMap::from([(creator, keys(creator, false)), (admin, keys(admin, false))]);
    // Two keys of one user are still one holder.
    let one_user = [copy(creator), copy(creator)];
    let report = resolve_holders(creator, &admins, &[], &lookups, &one_user);
    assert_eq!(report.recovery.ready_holders, 1);
    assert_eq!(report.recovery.state, RecoveryState::Degraded);

    let two_users = [copy(creator), copy(admin)];
    let report = resolve_holders(creator, &admins, &[], &lookups, &two_users);
    assert_eq!(report.recovery.state, RecoveryState::Met);

    // One ready holder with a declared recovery code also meets the rule.
    let lookups = BTreeMap::from([(creator, keys(creator, true)), (admin, keys(admin, false))]);
    let report = resolve_holders(creator, &admins, &[], &lookups, &one_user);
    assert_eq!(report.recovery.state, RecoveryState::Met);
    assert_eq!(report.recovery.ready_with_recovery, 1);
}

#[test]
fn recovery_needs_copy() {
    let creator = user(1);
    let KeyLookup::Keys(mut records) = keys(creator, false) else {
        unreachable!()
    };
    // A second key declares a recovery code, but the bucket key is sealed only to the first.
    let mut second = records[0].clone();
    second.record_id = Ulid::generate();
    second.has_recovery = true;
    records.push(second);
    let lookups = BTreeMap::from([(creator, KeyLookup::Keys(records))]);
    let report = resolve_holders(creator, &BTreeSet::new(), &[], &lookups, &[copy(creator)]);
    assert_eq!(report.holder(creator).unwrap().has_recovery, Some(false));
    assert_eq!(report.recovery.state, RecoveryState::Degraded);
}

#[test]
fn unavailable_stays_unknown() {
    let (creator, admin) = (user(1), user(2));
    let admins = BTreeSet::from([admin]);
    let lookups = BTreeMap::from([(creator, keys(creator, false))]);
    let report = resolve_holders(creator, &admins, &[], &lookups, &[copy(creator)]);

    let entry = report.holder(admin).unwrap();
    assert_eq!(
        (entry.state, entry.has_recovery),
        (HolderState::Unavailable, None)
    );
    assert!(!report.complete);
    assert_eq!(report.unresolved, 1);
    // The unreached admin might complete the rule, so it is not reported as broken.
    assert_eq!(report.recovery.state, RecoveryState::Unknown);
}

#[test]
fn revision_tracks_rows() {
    let grants = [grant(user(3), HolderOrigin::Explicit)];
    let copies = [copy(user(1)), copy(user(3))];
    let revision = holder_revision(&grants, &copies);
    // Row order does not matter; any added or removed row does.
    let reordered = [copies[1].clone(), copies[0].clone()];
    assert_eq!(holder_revision(&grants, &reordered), revision);
    assert_ne!(holder_revision(&[], &copies), revision);
    assert_ne!(holder_revision(&grants, &copies[..1]), revision);
    // A changed row content changes the revision too.
    let mut ready = grants.clone();
    ready[0].state = GrantState::Ready;
    assert_ne!(holder_revision(&ready, &copies), revision);
}

#[test]
fn revision_tracks_facts() {
    let (creator, admin) = (user(1), user(2));
    let copies = [copy(creator), copy(admin)];
    let rows = holder_revision(&[], &copies);
    let lookups = BTreeMap::from([(creator, keys(creator, false)), (admin, keys(admin, false))]);
    let admins = BTreeSet::from([admin]);
    let report = resolve_holders(creator, &admins, &[], &lookups, &copies);
    let revision = revision_with_facts(rows, creator, &admins, &report);
    // The same rows with a lost admin role or another directory answer are another revision.
    let none = BTreeSet::new();
    let without = resolve_holders(creator, &none, &[], &lookups, &copies);
    assert_ne!(
        revision_with_facts(rows, creator, &none, &without),
        revision
    );
    let recovered = BTreeMap::from([(creator, keys(creator, true)), (admin, keys(admin, false))]);
    let report = resolve_holders(creator, &admins, &[], &recovered, &copies);
    assert_ne!(
        revision_with_facts(rows, creator, &admins, &report),
        revision
    );
}
