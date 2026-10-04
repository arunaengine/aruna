//! Unlock: holder authority, the synced audit intent before activation and a discarded failed key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key_rows::authority_rows;
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
fn prepared(caller: UserId, admins: &[UserId]) -> (UnlockBucketOperation, Effects) {
    let (mut operation, effects) = checked(caller, admins);
    if !matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { .. })]
    ) {
        return (operation, effects);
    }
    let copy = (Key::from(Vec::new()), Value::from(vec![1]));
    let effects = operation.step(Event::Storage(StorageEvent::IterResult {
        values: vec![copy],
        next_start_after: None,
    }));
    (operation, effects)
}

/// Runs an unlock up to the read of the caller's sealed copy.
fn checked(caller: UserId, admins: &[UserId]) -> (UnlockBucketOperation, Effects) {
    let key = BucketKeyRef::new(BUCKET_ID, 2);
    let input = UnlockInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        caller,
        key,
        duration: Some(Duration::from_secs(60)),
        realm_id: RealmId::from_bytes([1; 32]),
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
    let values = authority_rows(&info, Some(&settings), admins);
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
    let (mut operation, effects) = prepared(user(1), &[]);
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
    // Reads see the key only after the intent committed and reached the disk.
    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_bytes([9; 16]),
    }));
    assert_eq!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::SyncAll)]
    );
    let effects = operation.step(Event::Storage(StorageEvent::SyncAllFinished));
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ActivateKey { ticket: ticket() })]
    );
    let status = UnlockStatus {
        key: ticket().key,
        session_id: ticket().session_id,
        active: true,
        unlocked_at: SystemTime::UNIX_EPOCH + Duration::from_secs(5),
        remaining: Some(Duration::from_secs(60)),
        max_remaining: Some(Duration::from_secs(3_600)),
    };
    let effects = operation.step(Event::Blob(BlobEvent::KeyActivated {
        status: status.clone(),
    }));
    // The timed lock of this session is armed for the remaining time.
    let key = lock_timer(&ticket());
    let arm = TaskEffect::ResetTimer {
        key: key.clone(),
        after: Duration::from_secs(60),
    };
    assert_eq!(effects.as_slice(), [Effect::Task(arm)]);
    let after = Duration::from_secs(60);
    let event = aruna_core::task::TaskEvent::TimerScheduled { key, after };
    let effects = operation.step(Event::Task(event));
    // The applied record states the deadline of the session as activated.
    let applied = audited(&effects);
    assert_eq!(
        (applied.outcome, applied.deadline_ms),
        (AuditOutcome::Applied, Some(65_000))
    );
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    assert_eq!(operation.finalize(), Ok(status));
}

#[test]
fn failed_activation_discards() {
    let (mut operation, _) = prepared(user(1), &[]);
    operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket: ticket() }));
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_bytes([9; 16]),
    }));
    operation.step(Event::Storage(StorageEvent::SyncAllFinished));
    let refused = BlobError::BucketKey(BucketKeyError::Capacity);
    let effects = operation.step(Event::Blob(BlobEvent::Error(refused)));
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::DiscardKey { ticket: ticket() })]
    );
    let effects = operation.step(Event::Blob(BlobEvent::KeyDiscarded { ticket: ticket() }));
    let outcome = audited(&effects);
    assert_eq!(outcome.outcome, AuditOutcome::Failed);
    // A failed outcome write is retried with the same record.
    let error = StorageError::Timeout;
    let effects = operation.step(Event::Storage(StorageEvent::Error { error }));
    assert_eq!(audited(&effects), outcome);
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    assert!(matches!(operation.finalize(), Err(UnlockError::Blob(_))));
}

#[test]
fn former_admin_refused() {
    // Admin rights decide implicit authority at the moment of the unlock (D30).
    let (operation, effects) = prepared(user(2), &[]);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(operation.finalize(), Err(UnlockError::NotHolder));
    let (_, effects) = prepared(user(2), &[user(2)]);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::PrepareKey { .. })]
    ));
}

#[test]
fn foreign_events_refused() {
    // A commit of another transaction proves nothing; the prepared key is discarded.
    let (mut operation, _) = prepared(user(1), &[]);
    operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket: ticket() }));
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_bytes([7; 16]),
    }));
    let discard = Effect::Blob(BlobEffect::DiscardKey { ticket: ticket() });
    assert!(effects.contains(&discard), "{effects:?}");
    assert!(matches!(
        operation.finalize(),
        Err(UnlockError::InvalidStateEvent { .. })
    ));

    // An activation answer for another key generation is not this session's.
    let (mut operation, _) = prepared(user(1), &[]);
    operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket: ticket() }));
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_bytes([9; 16]),
    }));
    operation.step(Event::Storage(StorageEvent::SyncAllFinished));
    let status = UnlockStatus {
        key: BucketKeyRef::new(BUCKET_ID, 1),
        session_id: ticket().session_id,
        active: true,
        unlocked_at: SystemTime::UNIX_EPOCH,
        remaining: None,
        max_remaining: None,
    };
    let effects = operation.step(Event::Blob(BlobEvent::KeyActivated { status }));
    assert!(effects.contains(&discard), "{effects:?}");
    assert!(matches!(
        operation.finalize(),
        Err(UnlockError::InvalidStateEvent { .. })
    ));
}

#[test]
fn unsynced_intent_discards() {
    let (mut operation, _) = prepared(user(1), &[]);
    operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket: ticket() }));
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_bytes([9; 16]),
    }));
    let error = StorageError::PersistError("disk full".to_string());
    let effects = operation.step(Event::Storage(StorageEvent::Error { error }));
    // The key never activates when its intent may be lost.
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::DiscardKey { ticket: ticket() })]
    );
    assert!(matches!(operation.finalize(), Err(UnlockError::Storage(_))));
}

#[test]
fn copyless_holder_refused() {
    let (mut operation, effects) = checked(user(1), &[]);
    let [
        Effect::Storage(StorageEffect::Iter {
            key_space, prefix, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected the copy read, got {effects:?}");
    };
    assert_eq!(key_space, KEY_COPY_KEYSPACE);
    let key = BucketKeyRef::new(BUCKET_ID, 2);
    assert_eq!(
        prefix.as_deref(),
        Some(&SealedCopy::user_prefix(key, user(1))[..])
    );
    let effects = operation.step(Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(operation.finalize(), Err(UnlockError::NoCopy));
}
