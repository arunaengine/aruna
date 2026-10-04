//! Unlock: holder authority, the audit intent before activation and a discarded failed key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::compute::SecretBytes;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode, public_key_of};
use aruna_core::structs::storage::format::Compression;
use std::time::SystemTime;

const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn private() -> SharedSecret {
    SharedSecret::new(SecretBytes::new(vec![5; 32]))
}

/// Runs an unlock by `caller` with `admins` up to its prepare step.
fn prepared(caller: UserId, admins: BTreeSet<UserId>) -> (UnlockBucketOperation, Effects) {
    let key = BucketKeyRef::new(BUCKET_ID, 2);
    let input = UnlockInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        caller,
        key,
        duration: Some(Duration::from_secs(60)),
        admins,
        now_ms: 1_000,
    };
    let mut operation = UnlockBucketOperation::new(input, private());
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
        max_unlock_ms: Some(3_600_000),
        ..Default::default()
    };
    let row = |value: Option<Vec<u8>>| (Key::from(Vec::new()), value.map(Value::from));
    let values = vec![
        row(Some(info.to_bytes().unwrap())),
        row(Some(settings.to_bytes().unwrap())),
    ];
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    let public = public_key_of(private().bytes()).unwrap();
    let record = BucketKeyRecord::new(key, Ulid::from_bytes([6; 16]), public, 1);
    let values = vec![row(Some(record.to_bytes().unwrap())), row(None)];
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    (operation, effects)
}

fn ticket() -> KeyTicket {
    KeyTicket {
        key: BucketKeyRef::new(BUCKET_ID, 2),
        session_id: Ulid::from_bytes([8; 16]),
    }
}

fn audited(effects: &Effects) -> BucketAuditRecord {
    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected an audit write, got {effects:?}");
    };
    assert_eq!(key_space, BUCKET_AUDIT_KEYSPACE);
    BucketAuditRecord::from_bytes(value).unwrap()
}

#[test]
fn intent_before_activation() {
    let (mut operation, effects) = prepared(user(1), BTreeSet::new());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::PrepareKey { .. })]
    ));
    let effects = operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket: ticket() }));
    let intent = audited(&effects);
    assert_eq!(
        (intent.outcome, intent.deadline_ms),
        (AuditOutcome::Intent, Some(61_000))
    );
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    // Reads see the key only after the intent committed.
    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_bytes([9; 16]),
    }));
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ActivateKey { ticket: ticket() })]
    );
    let status = UnlockStatus {
        key: ticket().key,
        session_id: ticket().session_id,
        active: true,
        unlocked_at: SystemTime::UNIX_EPOCH,
        remaining: Some(Duration::from_secs(60)),
        max_remaining: Some(Duration::from_secs(3_600)),
    };
    let effects = operation.step(Event::Blob(BlobEvent::KeyActivated {
        status: status.clone(),
    }));
    assert_eq!(audited(&effects).outcome, AuditOutcome::Applied);
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    assert_eq!(operation.finalize(), Ok(status));
}

#[test]
fn failed_activation_discards() {
    let (mut operation, _) = prepared(user(1), BTreeSet::new());
    operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket: ticket() }));
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_bytes([9; 16]),
    }));
    let refused = BlobError::BucketKey(BucketKeyError::Capacity);
    let effects = operation.step(Event::Blob(BlobEvent::Error(refused)));
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::DiscardKey { ticket: ticket() })]
    );
    let effects = operation.step(Event::Blob(BlobEvent::KeyDiscarded { ticket: ticket() }));
    assert_eq!(audited(&effects).outcome, AuditOutcome::Failed);
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    assert!(matches!(operation.finalize(), Err(UnlockError::Blob(_))));
}

#[test]
fn former_admin_refused() {
    // Admin rights decide implicit authority at the moment of the unlock (D30).
    let (operation, effects) = prepared(user(2), BTreeSet::new());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(operation.finalize(), Err(UnlockError::NotHolder));
    let (_, effects) = prepared(user(2), BTreeSet::from([user(2)]));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::PrepareKey { .. })]
    ));
}
