//! Startup opens node-managed keys from the node vault and reports keys it cannot open.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::compute::SecretBytes;
use aruna_core::effects::BlobEffect;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::encryption::{KeyTicket, UnlockStatus, public_key_of};
use std::time::SystemTime;

const MANAGED: Ulid = Ulid::from_bytes([1; 16]);

fn rows(values: Vec<Vec<u8>>) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values: values
            .into_iter()
            .map(|value| (Key::from(Vec::new()), value.into()))
            .collect(),
        next_start_after: None,
    })
}

fn settings(mode: EncryptionMode, bucket_id: Ulid) -> Vec<u8> {
    let settings = BucketEncryption {
        mode,
        bucket_id: Some(bucket_id),
        key_generation: 2,
        ..Default::default()
    };
    settings.to_bytes().unwrap()
}

fn record(generation: u64, state: KeyState) -> BucketKeyRecord {
    let public = public_key_of(&SecretBytes::new(vec![3; 32])).unwrap();
    let key = BucketKeyRef::new(MANAGED, generation);
    let mut record = BucketKeyRecord::new(key, Ulid::from_bytes([4; 16]), public, 1);
    record.state = state;
    record.vault_entry = Some(Ulid::from_bytes([generation as u8; 16]));
    record
}

/// Starts the operation and feeds the settings and key rows of one managed bucket.
fn scanned(records: &[BucketKeyRecord]) -> (OpenManagedOperation, Effects) {
    let mut operation = OpenManagedOperation::new();
    operation.start();
    let locked = settings(EncryptionMode::VaultLocked, Ulid::from_bytes([2; 16]));
    let effects = operation.step(rows(vec![
        settings(EncryptionMode::NodeManaged, MANAGED),
        locked,
        vec![0xff],
    ]));
    // Only the node-managed bucket is scanned for keys.
    let [
        Effect::Storage(StorageEffect::Iter {
            key_space, prefix, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected the key scan, got {effects:?}");
    };
    assert_eq!(key_space, BUCKET_KEY_KEYSPACE);
    assert_eq!(prefix.as_deref(), Some(&MANAGED.to_bytes()[..]));
    let values = records.iter().map(|record| record.to_bytes().unwrap());
    let effects = operation.step(rows(values.collect()));
    (operation, effects)
}

fn vault_read(effects: &Effects) -> VaultEntry {
    let [Effect::Storage(StorageEffect::VaultRead { entry, .. })] = effects.as_slice() else {
        panic!("expected a vault read, got {effects:?}");
    };
    *entry
}

#[test]
fn opens_managed_keys() {
    let (mut operation, effects) =
        scanned(&[record(1, KeyState::Retired), record(2, KeyState::Active)]);
    // A retired generation is not opened.
    let entry = vault_read(&effects);
    assert_eq!(entry.id, Ulid::from_bytes([2; 16]));
    let secret = Some(SecretBytes::new(vec![3; 32]));
    let effects = operation.step(Event::Storage(StorageEvent::VaultResult { entry, secret }));
    let [
        Effect::Blob(BlobEffect::PrepareKey {
            key, duration, max, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected the prepare step, got {effects:?}");
    };
    // A managed key stays open until lock or restart.
    assert_eq!(
        (*key, *duration, *max),
        (BucketKeyRef::new(MANAGED, 2), None, None)
    );
    let ticket = KeyTicket {
        key: *key,
        session_id: Ulid::from_bytes([5; 16]),
    };
    operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket }));
    let status = UnlockStatus {
        key: ticket.key,
        session_id: ticket.session_id,
        sequence: ulid::Ulid::from_parts(1, 1),
        deadline_ms: None,
        active: true,
        unlocked_at: SystemTime::UNIX_EPOCH,
        remaining: None,
        max_remaining: None,
    };
    let effects = operation.step(Event::Blob(BlobEvent::KeyActivated { status }));
    assert!(effects.is_empty());
    let keys = operation.finalize().unwrap();
    assert_eq!(keys.opened, [ticket.key]);
    assert!(keys.failed.is_empty());
    assert_eq!(keys.unreadable, 1);
}

#[test]
fn missing_secret_reported() {
    let (mut operation, effects) = scanned(&[record(2, KeyState::Active)]);
    let entry = vault_read(&effects);
    let effects = operation.step(Event::Storage(StorageEvent::VaultResult {
        entry,
        secret: None,
    }));
    assert!(effects.is_empty());
    let keys = operation.finalize().unwrap();
    assert!(keys.opened.is_empty());
    assert_eq!(keys.failed.len(), 1);
    assert_eq!(keys.failed[0].0, BucketKeyRef::new(MANAGED, 2));
}
