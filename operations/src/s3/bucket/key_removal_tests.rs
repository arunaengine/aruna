//! Holder removal: stale revisions, recovery confirmation, kept implicit copies and the audit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key_rows::authority_rows;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketKeyRef, EncryptionMode, GrantState};
use aruna_core::structs::storage::format::Compression;
use aruna_core::vault_format::key_fingerprint;
use std::time::SystemTime;

const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn keys(user_id: UserId) -> KeyLookup {
    KeyLookup::Keys(vec![UserKeyRecord {
        user_id,
        record_id: Ulid::generate(),
        key_id: "slot".to_string(),
        public_key: [7; 32],
        fingerprint: key_fingerprint(&[7; 32]),
        has_recovery: false,
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        placement: PlacementRef::NIL,
        created_at_ms: 1,
    }])
}

fn grant(user_id: UserId) -> BucketHolder {
    BucketHolder {
        bucket_id: BUCKET_ID,
        user_id,
        origin: HolderOrigin::Explicit,
        state: GrantState::Ready,
        granted_by: user(1),
        granted_at_ms: 1,
    }
}

fn copy(user_id: UserId, generation: u64) -> SealedCopy {
    SealedCopy {
        key: BucketKeyRef::new(BUCKET_ID, generation),
        user_id,
        key_record: Ulid::from_bytes([user_id.user_ulid.to_bytes()[0]; 16]),
        key_id: "slot".to_string(),
        enc: [0; 32],
        ciphertext: vec![0; 48],
        created_at_ms: 1,
    }
}

struct Case {
    target: UserId,
    admins: BTreeSet<UserId>,
    grants: Vec<BucketHolder>,
    copies: Vec<SealedCopy>,
    revision: Option<[u8; 32]>,
    confirm: bool,
    mode: EncryptionMode,
    keys: Vec<(u64, KeyState)>,
}

/// The active generation 2 of a vault-locked bucket.
const LOCKED: (EncryptionMode, u64, KeyState) = (EncryptionMode::VaultLocked, 2, KeyState::Active);

/// Runs a removal on a bucket of creator user(1) up to its delete batch.
fn run(case: Case) -> (RemoveHolderOperation, Effects) {
    let lookups = [user(1), user(2), user(3)].map(|user| (user, keys(user)));
    let revision = case.revision.unwrap_or_else(|| {
        // The list the caller saw: rows and facts of the active generation 2, if any.
        let active = case.mode != EncryptionMode::Off;
        let in_active: Vec<_> = case
            .copies
            .iter()
            .filter(|copy| active && copy.key.generation == 2)
            .cloned()
            .collect();
        let lookups = BTreeMap::from(lookups.clone());
        let report = resolve_holders(user(1), &case.admins, &case.grants, &lookups, &in_active);
        let rows = holder_revision(&case.grants, &case.copies);
        revision_with_facts(rows, user(1), &case.admins, &report)
    });
    let admins: Vec<_> = case.admins.iter().copied().collect();
    let mut operation = RemoveHolderOperation::new(RemovalInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        user_id: case.target,
        removed_by: user(1),
        revision,
        confirm_recovery: case.confirm,
        realm_id: RealmId::from_bytes([1; 32]),
        lookups: BTreeMap::from(lookups),
        now_ms: 9,
    });
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::from_bytes([9; 16]),
    }));
    let info = BucketInfo {
        group_id: Ulid::from_bytes([3; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user(1),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
    };
    let settings = BucketEncryption {
        mode: case.mode,
        bucket_id: Some(BUCKET_ID),
        key_generation: 2,
        ..Default::default()
    };
    let values = authority_rows(&info, Some(&settings), &admins);
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    let rows = |values: Vec<Vec<u8>>| {
        Event::Storage(StorageEvent::IterResult {
            values: values
                .into_iter()
                .map(|value| (Key::from(Vec::new()), value.into()))
                .collect(),
            next_start_after: None,
        })
    };
    let grants = case
        .grants
        .iter()
        .map(|grant| grant.to_bytes().unwrap())
        .collect();
    operation.step(rows(grants));
    let copies = case
        .copies
        .iter()
        .map(|copy| copy.to_bytes().unwrap())
        .collect();
    operation.step(rows(copies));
    let keys = case.keys.iter().map(|&(generation, state)| {
        let key = BucketKeyRef::new(BUCKET_ID, generation);
        let mut record = BucketKeyRecord::new(key, Ulid::generate(), [5; 32], 1);
        record.state = state;
        record.to_bytes().unwrap()
    });
    let effects = operation.step(rows(keys.collect()));
    (operation, effects)
}

fn deleted(effects: &Effects) -> Vec<String> {
    let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = effects.as_slice() else {
        panic!("expected the delete batch, got {effects:?}");
    };
    deletes.iter().map(|(space, _)| space.clone()).collect()
}

#[test]
fn removal_needs_confirmation() {
    // Creator and one explicit holder are ready: removing the grant leaves one holder without a
    // recovery code, which breaks the rule.
    let case = || Case {
        target: user(3),
        admins: BTreeSet::new(),
        grants: vec![grant(user(3))],
        copies: vec![copy(user(1), 2), copy(user(3), 2), copy(user(3), 1)],
        revision: None,
        confirm: false,
        mode: LOCKED.0,
        keys: vec![(LOCKED.1, LOCKED.2)],
    };
    let (operation, effects) = run(case());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(
        operation.finalize(),
        Err(RemovalError::RecoveryConfirmationRequired)
    );

    let (mut operation, effects) = run(Case {
        confirm: true,
        ..case()
    });
    // The grant and the user's copies of every generation go.
    assert_eq!(
        deleted(&effects),
        [BUCKET_HOLDER_KEYSPACE, KEY_COPY_KEYSPACE, KEY_COPY_KEYSPACE]
    );
    let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
        entries: Vec::new(),
    }));
    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected the audit record, got {effects:?}");
    };
    assert_eq!(key_space, BUCKET_AUDIT_KEYSPACE);
    let record = BucketAuditRecord::from_bytes(value).unwrap();
    assert_eq!(
        (record.action, record.actor),
        (AuditAction::HolderRemoval, Some(user(1)))
    );
    assert!(record.reason.is_some(), "a confirmed break is recorded");
}

#[test]
fn stale_revision_refused() {
    let (operation, _) = run(Case {
        target: user(3),
        admins: BTreeSet::new(),
        grants: vec![grant(user(3))],
        copies: vec![copy(user(1), 2)],
        revision: Some([0; 32]),
        confirm: true,
        mode: LOCKED.0,
        keys: vec![(LOCKED.1, LOCKED.2)],
    });
    assert_eq!(operation.finalize(), Err(RemovalError::StaleHolders));
}

#[test]
fn admin_keeps_copies() {
    // The explicit grant of a current admin goes, but the admin keeps their copies.
    let (operation, effects) = run(Case {
        target: user(2),
        admins: BTreeSet::from([user(2)]),
        grants: vec![grant(user(2))],
        copies: vec![copy(user(1), 2), copy(user(2), 2)],
        revision: None,
        confirm: false,
        mode: LOCKED.0,
        keys: vec![(LOCKED.1, LOCKED.2)],
    });
    assert_eq!(deleted(&effects), [BUCKET_HOLDER_KEYSPACE]);
    assert!(matches!(
        operation.output,
        Some(Ok(RemovalResult {
            deleted_copies: 0,
            ..
        }))
    ));
}

#[test]
fn retained_generation_protected() {
    // A decrypting change turned writes off, but generation 1 still holds archives.
    let case = |confirm| Case {
        target: user(3),
        admins: BTreeSet::new(),
        grants: vec![grant(user(3))],
        copies: vec![copy(user(1), 1), copy(user(3), 1)],
        revision: None,
        confirm,
        mode: EncryptionMode::Off,
        keys: vec![(1, KeyState::Retiring), (2, KeyState::Retired)],
    };
    let (operation, _) = run(case(false));
    assert_eq!(
        operation.finalize(),
        Err(RemovalError::RecoveryConfirmationRequired)
    );
    let (operation, effects) = run(case(true));
    assert_eq!(
        deleted(&effects),
        [BUCKET_HOLDER_KEYSPACE, KEY_COPY_KEYSPACE]
    );
    let Some(Ok(result)) = &operation.output else {
        panic!("expected a removal result");
    };
    assert_eq!(
        result.recovery.keys().copied().collect::<Vec<_>>(),
        [1],
        "a retired generation needs no recovery"
    );
}

#[test]
fn weak_recovery_protected() {
    // The creator has no copy; user(4) has no directory answer, so its recovery is unknown.
    for target in [user(3), user(4)] {
        let case = |confirm| Case {
            target,
            admins: BTreeSet::new(),
            grants: vec![grant(target)],
            copies: vec![copy(target, 2)],
            revision: None,
            confirm,
            mode: LOCKED.0,
            keys: vec![(LOCKED.1, LOCKED.2)],
        };
        let (operation, _) = run(case(false));
        assert_eq!(
            operation.finalize(),
            Err(RemovalError::RecoveryConfirmationRequired),
            "the last ready holder of {target:?} needs confirmation"
        );
        let (_, effects) = run(case(true));
        assert_eq!(
            deleted(&effects),
            [BUCKET_HOLDER_KEYSPACE, KEY_COPY_KEYSPACE]
        );
    }
}
