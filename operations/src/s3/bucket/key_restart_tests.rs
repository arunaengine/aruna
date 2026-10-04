//! The restart scan finds generations left unlocked by the last applied audit records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{BucketKeyRef, GrantState, HolderOrigin};
use aruna_core::structs::storage::format::Compression;
use std::time::SystemTime;

const LOCKED: Ulid = Ulid::from_bytes([1; 16]);
const GROUP: Ulid = Ulid::from_bytes([3; 16]);

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
    let record = BucketAuditRecord {
        event_id: Ulid::generate(),
        bucket_id: LOCKED,
        at_ms: 1,
        action,
        actor: None,
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        generation: Some(generation),
        deadline_ms: None,
        reason: None,
        outcome: AuditOutcome::Applied,
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
    let mut operation = RestartScanOperation::new();
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
    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: Key::from(b"locked".to_vec()),
        value: Some(info.to_bytes().unwrap().into()),
    }));
    let grant = BucketHolder {
        bucket_id: LOCKED,
        user_id: user(3),
        origin: HolderOrigin::Explicit,
        state: GrantState::Ready,
        granted_by: user(1),
        granted_at_ms: 1,
    };
    operation.step(rows(vec![(grant.key(), grant.to_bytes().unwrap())]));
    // Only holders of an unlocked generation are told.
    operation.step(rows(vec![copy(user(2), 1), copy(user(4), 2)]));
    let found = operation.finalize().unwrap();
    assert_eq!(
        found,
        [RestartedBucket {
            bucket: "locked".to_string(),
            bucket_id: LOCKED,
            group_id: GROUP,
            generations: vec![1],
            holders: vec![user(1), user(2), user(3)],
        }]
    );
}
