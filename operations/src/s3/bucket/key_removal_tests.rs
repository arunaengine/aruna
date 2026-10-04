//! Holder removal: stale revisions, recovery confirmation, kept implicit copies and the audit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketKeyRef, GrantState};
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
}

/// Runs a removal on a vault-locked bucket of creator user(1) up to its delete batch.
fn run(case: Case) -> (RemoveHolderOperation, Effects) {
    let revision = case
        .revision
        .unwrap_or_else(|| holder_revision(&case.grants, &case.copies));
    let lookups = [user(1), user(2), user(3)].map(|user| (user, keys(user)));
    let mut operation = RemoveHolderOperation::new(RemovalInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        user_id: case.target,
        removed_by: user(1),
        revision,
        confirm_recovery: case.confirm,
        admins: case.admins,
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
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(BUCKET_ID),
        key_generation: 2,
        ..Default::default()
    };
    let values = vec![
        (
            Key::from(b"bucket".to_vec()),
            Some(info.to_bytes().unwrap().into()),
        ),
        (
            Key::from(b"bucket".to_vec()),
            Some(settings.to_bytes().unwrap().into()),
        ),
    ];
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
    let effects = operation.step(rows(copies));
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
