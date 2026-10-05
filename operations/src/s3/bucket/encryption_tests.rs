//! Enabling encryption step by step: holders, recovery, open uploads and stale generations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key_rows::authority_rows;
use aruna_core::compute::SecretBytes;
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, KEY_COPY_KEYSPACE};
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::public_key_of;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::vault_format::key_fingerprint;
use std::time::SystemTime;

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn keys(user_id: UserId, recovery: bool) -> KeyLookup {
    KeyLookup::Keys(vec![UserKeyRecord {
        user_id,
        record_id: Ulid::from_bytes([user_id.user_ulid.to_bytes()[0] + 100; 16]),
        key_id: "slot".to_string(),
        public_key: [7; 32],
        fingerprint: key_fingerprint(&[7; 32]),
        has_recovery: recovery,
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        placement: PlacementRef::NIL,
        created_at_ms: 1,
    }])
}

fn input(mode: EncryptionMode, lookups: BTreeMap<UserId, KeyLookup>) -> EnableInput {
    EnableInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        realm_id: RealmId::from_bytes([1; 32]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        caller: user(2),
        mode,
        cipher: BlockCipher::Aes256Gcm,
        block_keys: BlockKeys::ContentDerived,
        max_unlock_ms: None,
        expected_generation: 0,
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

/// Runs the operation up to the generated key, with no uploads and no stored settings.
fn generated(operation: &mut EnableEncryptionOperation) -> (Effects, [u8; 32]) {
    let txn_id = Ulid::from_bytes([9; 16]);
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    let values = authority_rows(&bucket(), None, &[user(2)]);
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    let effects = operation.step(Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::GenerateBucketKey)]
    ));
    let private = SecretBytes::new(vec![4; 32]);
    let public = public_key_of(&private).unwrap();
    let effects = operation.step(Event::Blob(BlobEvent::BucketKeyGenerated {
        public_key: public,
        private_key: SharedSecret::new(private),
    }));
    (effects, public)
}

fn sealed(effects: &Effects) -> Vec<SealedCopy> {
    let [Effect::Blob(BlobEffect::SealHolderCopies { key, holders, .. })] = effects.as_slice()
    else {
        panic!("expected sealing, got {effects:?}");
    };
    holders
        .iter()
        .map(|target| SealedCopy {
            key: *key,
            user_id: target.user_id,
            key_record: target.key_record,
            key_id: target.key_id.clone(),
            enc: [0; 32],
            ciphertext: vec![0; 48],
            created_at_ms: 5,
        })
        .collect()
}

#[test]
fn managed_enable_writes() {
    let lookups = BTreeMap::from([
        (user(1), keys(user(1), false)),
        (user(2), KeyLookup::Missing),
    ]);
    let mut operation = EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, lookups));
    let (effects, public) = generated(&mut operation);
    let copies = sealed(&effects);
    // Only the creator has a published key; the admin without one gets no copy.
    assert_eq!(
        copies.iter().map(|copy| copy.user_id).collect::<Vec<_>>(),
        vec![user(1)]
    );

    let effects = operation.step(Event::Blob(BlobEvent::CopiesSealed { copies }));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the settings write, got {effects:?}");
    };
    let spaces: Vec<_> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
    // The same batch starts the encrypt transition of copies stored in plain form.
    assert_eq!(
        spaces,
        [
            BUCKET_ENCRYPTION_KEYSPACE,
            BUCKET_KEY_KEYSPACE,
            KEY_COPY_KEYSPACE,
            aruna_core::keyspaces::TRANSITION_KEYSPACE,
            aruna_core::keyspaces::TRANSITION_QUEUE_KEYSPACE,
            aruna_core::keyspaces::BUCKET_AUDIT_KEYSPACE
        ]
    );
    // The enable records its mode change in the same batch.
    let audit = BucketAuditRecord::from_bytes(&writes[5].2).unwrap();
    assert_eq!(
        (audit.action, audit.actor, audit.outcome),
        (
            AuditAction::ModeChange,
            Some(user(2)),
            AuditOutcome::Applied
        )
    );
    let settings = BucketEncryption::from_bytes(&writes[0].2).unwrap();
    assert_eq!(
        (settings.mode, settings.key_generation),
        (EncryptionMode::NodeManaged, 1)
    );
    assert_eq!(
        (settings.storage_generation, settings.cipher),
        (1, BlockCipher::Aes256Gcm)
    );

    // The node copy of a managed bucket rides the same transaction.
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    let [Effect::Storage(StorageEffect::VaultWrite { entry, .. })] = effects.as_slice() else {
        panic!("expected the node copy, got {effects:?}");
    };
    assert_eq!(entry.purpose, VaultPurpose::BucketKey);
    let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Vec::new().into(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { .. })]
    ));
    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_bytes([9; 16]),
    }));
    // The transition runs at once after the commit.
    let wake = aruna_core::task::TaskEffect::ShortenTimer {
        key: aruna_core::task::TaskKey::MigrateCompression,
        after: std::time::Duration::ZERO,
    };
    assert_eq!(effects.as_slice(), [Effect::Task(wake)]);
    // The operation hands its only key handle on and keeps none.
    assert!(operation.private_key.is_none());
    let result = operation.finalize().unwrap();
    assert_eq!(result.key.public_key, public);
    assert_eq!(public_key_of(result.private_key.bytes()), Some(public));
}

#[test]
fn locked_needs_recovery() {
    // One ready holder without a recovery code does not meet the rule.
    let lookups = BTreeMap::from([
        (user(1), keys(user(1), false)),
        (user(2), KeyLookup::Unavailable),
    ]);
    let mut operation = EnableEncryptionOperation::new(input(EncryptionMode::VaultLocked, lookups));
    let (effects, _) = generated(&mut operation);
    let effects = operation.step(Event::Blob(BlobEvent::CopiesSealed {
        copies: sealed(&effects),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert!(operation.private_key.is_none());
    assert_eq!(operation.finalize(), Err(EnableError::RecoveryUnmet));

    let lookups = BTreeMap::from([
        (user(1), keys(user(1), true)),
        (user(2), KeyLookup::Missing),
    ]);
    let mut operation = EnableEncryptionOperation::new(input(EncryptionMode::VaultLocked, lookups));
    let (effects, _) = generated(&mut operation);
    let effects = operation.step(Event::Blob(BlobEvent::CopiesSealed {
        copies: sealed(&effects),
    }));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("a holder with a recovery code meets the rule, got {effects:?}");
    };
    // A vault-locked bucket keeps no node copy.
    let record = BucketKeyRecord::from_bytes(&writes[1].2).unwrap();
    assert_eq!(record.vault_entry, None);
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
    let values = authority_rows(&bucket(), settings.as_ref(), &[user(2)]);
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }))
}

#[test]
fn decryption_blocks_enable() {
    use aruna_core::keyspaces::{BUCKET_HOLDER_KEYSPACE, TRANSITION_KEYSPACE};
    use aruna_core::structs::storage::transition::{
        EncryptionTransition, TransitionKind, TransitionTarget,
    };
    // An `off` bucket that was encrypted keeps its id while the decryption still runs.
    let decrypted = BucketEncryption {
        bucket_id: Some(Ulid::from_bytes([6; 16])),
        key_generation: 1,
        ..Default::default()
    };
    let target = TransitionTarget {
        compression: Compression::Off,
        plan: None,
    };
    let source = BucketKeyRef::new(Ulid::from_bytes([6; 16]), 1);
    let kind = TransitionKind::Decrypt;
    let mut decrypt = EncryptionTransition::new(kind, Some(source), target, 1, 1);
    let read = |operation: &mut EnableEncryptionOperation, transition: &EncryptionTransition| {
        bucket_read(operation, Some(decrypted.clone()));
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })] if key_space == TRANSITION_KEYSPACE
        ));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(transition.to_bytes().unwrap().into()),
        }))
    };

    let mut operation =
        EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, BTreeMap::new()));
    let effects = read(&mut operation, &decrypt);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(operation.finalize(), Err(EnableError::TransitionRunning));

    // Once the decryption finished, the bucket enables again with its next generation.
    decrypt.finished_at_ms = Some(9);
    let mut operation =
        EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, BTreeMap::new()));
    let effects = read(&mut operation, &decrypt);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { key_space, .. })] if key_space == BUCKET_HOLDER_KEYSPACE
    ));
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

#[test]
fn open_uploads_conflict() {
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::multipart::MultipartUploadStatus;
    use std::collections::HashMap;

    let mut operation =
        EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, BTreeMap::new()));
    bucket_read(&mut operation, None);
    let upload = MultipartUpload {
        upload_id: Ulid::generate(),
        backend: BackendRef::node_default(),
        storage_class: None,
        bucket: "bucket".to_string(),
        key: "object".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        created_by: user(1),
        created_at: SystemTime::UNIX_EPOCH,
        status: MultipartUploadStatus::Open,
        checksum_hint: None,
        metadata: HashMap::new(),
        placement_policies: Vec::new(),
        subject_generation: 0,
        completing_since_ms: None,
        backend_upload: None,
        encryption: None,
    };
    let values = vec![(b"upload".to_vec().into(), upload.to_bytes().unwrap().into())];
    let effects = operation.step(Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after: None,
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(operation.finalize(), Err(EnableError::OpenUploads));
}

#[test]
fn revoked_enabler_refused() {
    // The route saw user(2) as admin, but the role was revoked before the enable's transaction.
    let lookups = BTreeMap::from([(user(1), keys(user(1), true))]);
    let mut operation = EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, lookups));
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::from_bytes([9; 16]),
    }));
    let values = authority_rows(&bucket(), None, &[]);
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert!(matches!(operation.finalize(), Err(EnableError::NotAdmin)));
}
