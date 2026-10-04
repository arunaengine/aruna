// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key_rows::authority_rows;
use aruna_core::compute::SecretBytes;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::structs::storage::format::Compression;
use std::time::SystemTime;

const BUCKET_ID: Ulid = Ulid::from_bytes([7; 16]);

fn info() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([1; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::new(Ulid::from_bytes([2; 16]), RealmId::from_bytes([3; 32])),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Zstd { level: 3 },
    }
}

fn settings(mode: EncryptionMode) -> BucketEncryption {
    BucketEncryption {
        mode,
        bucket_id: Some(BUCKET_ID),
        key_generation: 1,
        storage_generation: 4,
        ..BucketEncryption::default()
    }
}

fn active(vault: bool) -> BucketKeyRecord {
    let key = BucketKeyRef::new(BUCKET_ID, 1);
    let mut record = BucketKeyRecord::new(key, Ulid::from_bytes([5; 16]), [9; 32], 1);
    record.vault_entry = vault.then_some(record.record_id);
    record
}

fn operation(change: KeyChange) -> ChangeEncryptionOperation {
    ChangeEncryptionOperation::new(ChangeInput {
        bucket: "b".to_string(),
        group_id: info().group_id,
        realm_id: info().created_by.realm_id,
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        change,
        expected_generation: 4,
        lookups: BTreeMap::new(),
        now_ms: 50,
    })
}

fn settings_change(mode: EncryptionMode) -> KeyChange {
    KeyChange::Settings {
        mode,
        cipher: BlockCipher::default(),
        block_keys: BlockKeys::default(),
    }
}

/// Answers the bucket, upload and active key reads; returns what the operation does next.
fn loaded(
    operation: &mut ChangeEncryptionOperation,
    mode: EncryptionMode,
    uploads: Vec<(Key, Value)>,
) -> Effects {
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: TxnId::default(),
    }));
    let current = settings(mode);
    let values = authority_rows(&info(), Some(&current), &[]);
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    let effects = operation.step(Event::Storage(StorageEvent::IterResult {
        values: uploads,
        next_start_after: None,
    }));
    if !matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ) {
        return effects;
    }
    let record = active(mode == EncryptionMode::NodeManaged);
    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: Some(record.to_bytes().unwrap().into()),
    }))
}

fn rows(effects: &Effects) -> Vec<(String, Key, Value)> {
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the row writes, got {effects:?}")
    };
    writes.clone()
}

fn row(rows: &[(String, Key, Value)], key_space: &str) -> Vec<u8> {
    let found = rows.iter().find(|(space, _, _)| space == key_space);
    found.map(|(_, _, value)| value.to_vec()).unwrap()
}

fn generated(operation: &mut ChangeEncryptionOperation) -> Effects {
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    }));
    let private = SecretBytes::new(vec![4; 32]);
    let public_key = aruna_core::structs::storage::encryption::public_key_of(&private).unwrap();
    operation.step(Event::Blob(BlobEvent::BucketKeyGenerated {
        public_key,
        private_key: SharedSecret::new(private),
    }))
}

#[test]
fn rotation_journals_generations() {
    let mut operation = operation(KeyChange::Rotate);
    let effects = loaded(&mut operation, EncryptionMode::NodeManaged, Vec::new());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { key_space, .. })] if key_space == BUCKET_HOLDER_KEYSPACE
    ));

    let rows = rows(&generated(&mut operation));

    let transition = EncryptionTransition::from_bytes(&row(&rows, TRANSITION_KEYSPACE)).unwrap();
    assert_eq!(transition.kind, TransitionKind::Rotate);
    assert_eq!(transition.source, Some(BucketKeyRef::new(BUCKET_ID, 1)));
    assert_eq!(transition.target.plan.unwrap().key.generation, 2);
    assert_eq!(transition.source_vault, active(true).vault_entry);
    let stored = BucketEncryption::from_bytes(&row(&rows, BUCKET_ENCRYPTION_KEYSPACE)).unwrap();
    assert_eq!((stored.key_generation, stored.storage_generation), (2, 5));
    let old = rows.iter().find(|(space, key, _)| {
        space == BUCKET_KEY_KEYSPACE && key.as_ref() == BucketKeyRef::new(BUCKET_ID, 1).key()
    });
    let old = BucketKeyRecord::from_bytes(&old.unwrap().2).unwrap();
    assert_eq!(old.state, KeyState::Retiring);

    // A node-managed rotation keeps a node copy of the new generation.
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::VaultWrite { .. })]
    ));
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Vec::new().into(),
    }));
    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: TxnId::default(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Task(TaskEffect::ShortenTimer { .. })]
    ));
    let result = operation.finalize().unwrap();
    assert_eq!(result.key.unwrap().0.key.generation, 2);
}

#[test]
fn locking_needs_recovery() {
    // Without ready holders a vault_locked generation would be lost, and the node copy is
    // never written for it.
    let mut operation = operation(settings_change(EncryptionMode::VaultLocked));
    loaded(&mut operation, EncryptionMode::NodeManaged, Vec::new());

    let effects = generated(&mut operation);

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(operation.finalize(), Err(ChangeError::RecoveryUnmet));
}

#[test]
fn decrypting_keeps_source() {
    let mut operation = operation(settings_change(EncryptionMode::Off));

    let rows = rows(&loaded(
        &mut operation,
        EncryptionMode::VaultLocked,
        Vec::new(),
    ));

    let stored = BucketEncryption::from_bytes(&row(&rows, BUCKET_ENCRYPTION_KEYSPACE)).unwrap();
    assert_eq!(stored.mode, EncryptionMode::Off);
    assert_eq!(stored.storage_generation, 5);
    let transition = EncryptionTransition::from_bytes(&row(&rows, TRANSITION_KEYSPACE)).unwrap();
    assert_eq!(transition.kind, TransitionKind::Decrypt);
    assert_eq!(transition.target.plan, None);
    assert_eq!(transition.target.compression, info().compression);
    let old = BucketKeyRecord::from_bytes(&row(&rows, BUCKET_KEY_KEYSPACE)).unwrap();
    assert_eq!(old.state, KeyState::Retiring);
}

#[test]
fn cipher_change_reencodes() {
    let mut operation = operation(KeyChange::Settings {
        mode: EncryptionMode::NodeManaged,
        cipher: BlockCipher::Aes256Gcm,
        block_keys: BlockKeys::default(),
    });

    let rows = rows(&loaded(
        &mut operation,
        EncryptionMode::NodeManaged,
        Vec::new(),
    ));

    let transition = EncryptionTransition::from_bytes(&row(&rows, TRANSITION_KEYSPACE)).unwrap();
    assert_eq!(transition.kind, TransitionKind::Reencode);
    let plan = transition.target.plan.unwrap();
    assert_eq!(
        (plan.key.generation, plan.cipher),
        (1, BlockCipher::Aes256Gcm)
    );
    let old = BucketKeyRecord::from_bytes(&row(&rows, BUCKET_KEY_KEYSPACE)).unwrap();
    assert_eq!(old.state, KeyState::Active);
}

#[test]
fn unlocking_adds_node_copy() {
    let mut operation = operation(settings_change(EncryptionMode::NodeManaged));
    let effects = loaded(&mut operation, EncryptionMode::VaultLocked, Vec::new());
    let key = BucketKeyRef::new(BUCKET_ID, 1);
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReadUnlockedKey { key })]
    );

    let private_key = SharedSecret::new(SecretBytes::new(vec![4; 32]));
    let rows = rows(&operation.step(Event::Blob(BlobEvent::UnlockedKeyRead { key, private_key })));

    assert!(
        rows.iter()
            .all(|(space, _, _)| space != TRANSITION_KEYSPACE)
    );
    let record = BucketKeyRecord::from_bytes(&row(&rows, BUCKET_KEY_KEYSPACE)).unwrap();
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::VaultWrite { entry, .. })]
            if Some(entry.id) == record.vault_entry
    ));
}

#[test]
fn locked_bucket_stays() {
    let mut operation = operation(settings_change(EncryptionMode::NodeManaged));
    loaded(&mut operation, EncryptionMode::VaultLocked, Vec::new());

    let locked = BucketKeyError::Locked(BUCKET_ID);
    operation.step(Event::Blob(BlobEvent::Error(locked.clone().into())));

    assert_eq!(
        operation.finalize(),
        Err(ChangeError::Blob(BlobError::BucketKey(locked)))
    );
}
