//! Enabling encryption step by step: holders, recovery, open uploads and stale generations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::format::Compression;
use std::time::SystemTime;

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn input(mode: EncryptionMode, lookups: BTreeMap<UserId, KeyLookup>) -> EnableInput {
    EnableInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        realm_id: RealmId::from_bytes([1; 32]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        mode,
        cipher: BlockCipher::Aes256Gcm,
        block_keys: BlockKeys::ContentDerived,
        max_unlock_ms: None,
        expected_generation: 0,
        admins: BTreeSet::from([user(2)]),
        lookups,
        now_ms: 5,
    }
}

fn bucket() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([3; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user(1),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
    }
}

/// Starts the operation and answers the bucket read with `settings`.
fn bucket_read(
    operation: &mut EnableEncryptionOperation,
    settings: Option<BucketEncryption>,
) -> Effects {
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::from_bytes([9; 16]),
    }));
    let settings = settings.map(|settings| settings.to_bytes().unwrap().into());
    let read = vec![
        (
            b"bucket".to_vec().into(),
            Some(bucket().to_bytes().unwrap().into()),
        ),
        (b"bucket".to_vec().into(), settings),
    ];
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: read,
    }))
}

#[test]
fn stale_generation_refused() {
    let mut operation =
        EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, BTreeMap::new()));
    let stale = BucketEncryption {
        storage_generation: 4,
        ..Default::default()
    };
    // The caller read generation 0, so a concurrent change refuses the enable.
    let effects = bucket_read(&mut operation, Some(stale));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert!(matches!(
        operation.finalize(),
        Err(EnableError::Key(BucketKeyError::StaleGeneration { .. }))
    ));
}
