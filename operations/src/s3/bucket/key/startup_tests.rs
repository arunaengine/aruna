//! Startup opens node-managed keys from the node vault and reports keys it cannot open.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::compute::SecretBytes;
use aruna_core::structs::storage::encryption::{KeyTicket, public_key_of};
use std::time::SystemTime;

const MANAGED: Ulid = Ulid::from_bytes([1; 16]);

fn rows(values: Vec<Vec<u8>>) -> IoResult {
    IoResult::Page {
        rows: values
            .into_iter()
            .map(|value| (Key::from(Vec::new()), value.into()))
            .collect(),
        next: None,
    }
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
fn scanned(records: &[BucketKeyRecord]) -> (OpenManagedOperation, StartupTurn) {
    let mut operation = OpenManagedOperation::new();
    operation.first().unwrap();
    let locked = settings(EncryptionMode::VaultLocked, Ulid::from_bytes([2; 16]));
    let turn = operation.next(rows(vec![
        settings(EncryptionMode::NodeManaged, MANAGED),
        locked,
        vec![0xff],
    ]));
    // Only the node-managed bucket is scanned for keys.
    let Ok(Next::Call(IoCall::Page { space, prefix, .. })) = &turn else {
        panic!("expected the key scan, got {turn:?}");
    };
    assert_eq!(space, BUCKET_KEY_KEYSPACE);
    assert_eq!(prefix.as_ref(), &MANAGED.to_bytes()[..]);
    let values = records.iter().map(|record| record.to_bytes().unwrap());
    let turn = operation.next(rows(values.collect()));
    (operation, turn)
}

fn vault_answer(secret: Result<Option<SharedSecret>, VaultReadError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(secret))
}

fn secret() -> SharedSecret {
    SharedSecret::new(SecretBytes::new(vec![3; 32]))
}

fn status(key: BucketKeyRef) -> UnlockStatus {
    let ticket = KeyTicket {
        key,
        session_id: Ulid::from_bytes([5; 16]),
    };
    UnlockStatus {
        key: ticket.key,
        session_id: ticket.session_id,
        sequence: ulid::Ulid::from_parts(1, 1),
        deadline_ms: None,
        active: true,
        unlocked_at: SystemTime::UNIX_EPOCH,
        remaining: None,
        max_remaining: None,
    }
}

#[test]
fn opens_managed_keys() {
    let (mut operation, turn) =
        scanned(&[record(1, KeyState::Retired), record(2, KeyState::Active)]);
    // A retired generation is not opened.
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.current, Some(record(2, KeyState::Active)));
    let turn = operation.next(vault_answer(Ok(Some(secret()))));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.step, StartupStep::Install);
    let key = BucketKeyRef::new(MANAGED, 2);
    let installed = Ok::<_, InstallError>(status(key));
    let Ok(Next::Done(keys)) = operation.next(IoResult::Child(ChildOutcome::new(installed))) else {
        panic!("the only key was opened");
    };
    assert_eq!(keys.opened, [key]);
    assert!(keys.failed.is_empty());
    assert_eq!(keys.unreadable, 1);
}

#[test]
fn missing_secret_reported() {
    let (mut operation, _) = scanned(&[record(2, KeyState::Active)]);
    let Ok(Next::Done(keys)) = operation.next(vault_answer(Ok(None))) else {
        panic!("the only key was decided");
    };
    assert!(keys.opened.is_empty());
    assert_eq!(keys.failed.len(), 1);
    assert_eq!(keys.failed[0].0, BucketKeyRef::new(MANAGED, 2));
}

#[test]
fn failed_read_continues() {
    // A vault that cannot answer for one key leaves the next key to open.
    let (mut operation, _) = scanned(&[record(1, KeyState::Active), record(2, KeyState::Active)]);
    let turn = operation.next(vault_answer(Err(VaultReadError::Storage(
        StorageError::Timeout,
    ))));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.keys.failed.len(), 1);
}

#[test]
fn failed_install_reported() {
    let (mut operation, _) = scanned(&[record(2, KeyState::Active)]);
    operation.next(vault_answer(Ok(Some(secret())))).unwrap();
    let failed = Err::<UnlockStatus, _>(InstallError::NotFinished);
    let Ok(Next::Done(keys)) = operation.next(IoResult::Child(ChildOutcome::new(failed))) else {
        panic!("the only key was decided");
    };
    assert_eq!(keys.failed.len(), 1);
}

#[test]
fn vault_checks_entry() {
    let entry = VaultEntry::new(VaultPurpose::BucketKey, Ulid::from_bytes([1; 16]));
    let mut read = VaultKeyRead { entry };
    assert!(matches!(read.first(), Ok(Next::Call(IoCall::Effect(_)))));
    let other = VaultEntry::new(VaultPurpose::BucketKey, Ulid::from_bytes([2; 16]));
    let answer = Event::Storage(StorageEvent::VaultResult {
        entry: other,
        secret: None,
    });
    assert_eq!(
        read.next(IoResult::Event(Box::new(answer))),
        Err(VaultReadError::OtherEntry)
    );
}
