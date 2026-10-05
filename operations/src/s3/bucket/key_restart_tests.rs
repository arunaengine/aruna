//! The restart scan finds generations left unlocked by the last applied audit records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key_rows::authority_rows;
use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{BucketKeyRef, GrantState, HolderOrigin};
use aruna_core::structs::storage::format::Compression;
use std::time::SystemTime;

const LOCKED: Ulid = Ulid::from_bytes([1; 16]);
const GROUP: Ulid = Ulid::from_bytes([3; 16]);
const NOW: u64 = 10_000;

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn rows(values: Vec<(Vec<u8>, Vec<u8>)>) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values: values
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
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

fn audit(action: AuditAction, generation: u64) -> (Vec<u8>, Vec<u8>) {
    record(action, generation, AuditOutcome::Applied, None)
}

fn record(
    action: AuditAction,
    generation: u64,
    outcome: AuditOutcome,
    deadline_ms: Option<u64>,
) -> (Vec<u8>, Vec<u8>) {
    let record = BucketAuditRecord {
        event_id: Ulid::generate(),
        bucket_id: LOCKED,
        at_ms: 1,
        action,
        actor: None,
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        generation: Some(generation),
        deadline_ms,
        reason: None,
        outcome,
    };
    (record.key(), record.to_bytes().unwrap())
}

fn copy(user_id: UserId, generation: u64) -> (Vec<u8>, Vec<u8>) {
    let copy = SealedCopy {
        key: BucketKeyRef::new(LOCKED, generation),
        user_id,
        key_record: Ulid::from_bytes([9; 16]),
        key_id: "slot".to_string(),
        enc: [0; 32],
        ciphertext: vec![0; 48],
        created_at_ms: 1,
    };
    (copy.key(), copy.to_bytes().unwrap())
}

#[test]
fn finds_unlocked_generations() {
    let mut operation = RestartScanOperation::new(NOW, RealmId::from_bytes([1; 32]));
    operation.start();
    let effects = operation.step(rows(vec![
        (
            b"locked".to_vec(),
            settings(EncryptionMode::VaultLocked, LOCKED),
        ),
        (
            b"managed".to_vec(),
            settings(EncryptionMode::NodeManaged, GROUP),
        ),
    ]));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { key_space, .. })] if key_space == BUCKET_AUDIT_KEYSPACE
    ));
    // Generation 2 was locked after its unlock; generation 1 was still unlocked.
    operation.step(rows(vec![
        audit(AuditAction::Unlock, 1),
        audit(AuditAction::Unlock, 2),
        audit(AuditAction::Lock, 2),
    ]));
    let info = BucketInfo {
        group_id: GROUP,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user(1),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
    };
    let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
        key: Key::from(b"locked".to_vec()),
        value: Some(info.to_bytes().unwrap().into()),
    }));
    // Admin rights come from the current authorization documents: user(4) lost its role.
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { .. })]
    ));
    let settings = BucketEncryption::from_bytes(&settings(EncryptionMode::VaultLocked, LOCKED));
    let values = authority_rows(&info, Some(&settings.unwrap()), &[user(5)]);
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    let grant = BucketHolder {
        bucket_id: LOCKED,
        user_id: user(3),
        origin: HolderOrigin::Explicit,
        state: GrantState::Ready,
        granted_by: user(1),
        granted_at_ms: 1,
    };
    operation.step(rows(vec![(grant.key(), grant.to_bytes().unwrap())]));
    // A former admin keeps an old copy but is no holder any more and is not told.
    operation.step(rows(vec![copy(user(2), 1), copy(user(4), 1)]));
    let found = operation.finalize().unwrap();
    assert_eq!(
        found,
        [RestartedBucket {
            bucket: "locked".to_string(),
            bucket_id: LOCKED,
            group_id: GROUP,
            generations: vec![1],
            holders: vec![user(1), user(3), user(5)],
        }]
    );
}

/// The generations a scan of one vault-locked bucket with `trail` reports as unlocked.
fn reported(trail: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<u64> {
    let mut operation = RestartScanOperation::new(NOW, RealmId::from_bytes([1; 32]));
    operation.start();
    let locked = (
        b"locked".to_vec(),
        settings(EncryptionMode::VaultLocked, LOCKED),
    );
    operation.step(rows(vec![locked]));
    let effects = operation.step(rows(trail));
    if effects.is_empty() {
        assert_eq!(operation.finalize().unwrap(), []);
        return Vec::new();
    }
    let RestartScanOperation { current, .. } = operation;
    current.unwrap().generations
}

#[test]
fn lost_outcome_counts() {
    // A crash after activation left only the synced intent: the key may have been in use.
    let intent = record(AuditAction::Unlock, 1, AuditOutcome::Intent, None);
    assert_eq!(reported(vec![intent]), [1]);
    // A failed activation restores the state before its intent.
    let intent = record(AuditAction::Unlock, 1, AuditOutcome::Intent, None);
    let failed = record(AuditAction::Unlock, 1, AuditOutcome::Failed, None);
    assert!(reported(vec![intent, failed]).is_empty());
    // An outage lost the lock's audit: the last record is the unlock, so holders are told.
    assert_eq!(reported(vec![audit(AuditAction::Unlock, 2)]), [2]);
}

#[test]
fn expired_sessions_skipped() {
    // A timed session that ended before this start was not unlocked at the restart.
    let ended = record(AuditAction::Unlock, 1, AuditOutcome::Applied, Some(NOW - 1));
    assert!(reported(vec![ended]).is_empty());
    let running = record(AuditAction::Unlock, 1, AuditOutcome::Applied, Some(NOW + 1));
    assert_eq!(reported(vec![running]), [1]);
    // An extension moves the deadline that counts.
    let ended = record(AuditAction::Unlock, 1, AuditOutcome::Applied, Some(NOW - 1));
    let extended = record(AuditAction::Extend, 1, AuditOutcome::Applied, Some(NOW + 1));
    assert_eq!(reported(vec![ended, extended]), [1]);
}
