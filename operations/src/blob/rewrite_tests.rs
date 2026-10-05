//! Tests copy rewriting through encrypted bucket transitions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::encryption::{
    BlockCipher, BlockKeys, BucketKeyRef, EncryptionMode, SealPlan,
};
use aruna_core::structs::storage::format::{
    Compression, EncodingClass, PithosLayout, StoredFormat,
};
use aruna_core::structs::storage::transition::TransitionTarget;
use std::collections::HashMap;
use std::sync::Arc;
use ulid::Ulid;

const BUCKET_ID: Ulid = Ulid::from_bytes([1; 16]);

fn plan(generation: u64) -> SealPlan {
    SealPlan {
        key: BucketKeyRef::new(BUCKET_ID, generation),
        public_key: [2; 32],
        cipher: BlockCipher::default(),
        block_keys: BlockKeys::default(),
        storage_generation: 3,
    }
}

fn transition(
    kind: TransitionKind,
    source: Option<u64>,
    target: Option<u64>,
) -> EncryptionTransition {
    let target = TransitionTarget {
        compression: Compression::Off,
        plan: target.map(plan),
    };
    let source = source.map(|generation| BucketKeyRef::new(BUCKET_ID, generation));
    EncryptionTransition::new(kind, source, target, 3, 100)
}

fn settings(generation: u64, storage: u64) -> Vec<u8> {
    BucketEncryption {
        mode: EncryptionMode::NodeManaged,
        bucket_id: Some(BUCKET_ID),
        key_generation: generation,
        storage_generation: storage,
        ..BucketEncryption::default()
    }
    .to_bytes()
    .unwrap()
}

fn location(sealed: Option<u64>) -> BackendLocation {
    let format = sealed.map_or_else(StoredFormat::default, |generation| {
        let layout = PithosLayout {
            stored_size: 90,
            metadata_digest: [generation as u8; 32],
            storage_generation: 0,
        };
        StoredFormat::pithos(layout, BucketKeyRef::new(BUCKET_ID, generation))
    });
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/data".to_string(),
        storage_bucket: "store".to_string(),
        backend_path: format!("b/k_{}", Ulid::generate()),
        ulid: Ulid::generate(),
        format,
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 40,
        hashes: HashMap::from([(HASH_BLAKE3.to_string(), vec![4u8; 32])]),
    }
}

fn version(old: &BackendLocation) -> BlobVersion {
    BlobVersion::materialized(
        [4u8; 32],
        BackendRef::node_default(),
        old.format.encoding(),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    )
}

fn read_result(value: Option<Vec<u8>>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: value.map(Into::into),
    })
}

fn operation(transition: EncryptionTransition) -> RewriteVersionOperation {
    let key = VersionKey::new("b", "k", Ulid::from_bytes([2; 16]));
    RewriteVersionOperation::new(key, transition, SystemTime::UNIX_EPOCH)
}

fn lease(old: &BackendLocation) -> ReadLease {
    let key = old.format.bucket_key().unwrap();
    ReadLease::new(key, ArchiveKey::of(old), Ulid::generate(), Arc::new(()))
}

/// Reads version and location, and answers what the operation asks the adapter first.
fn located(operation: &mut RewriteVersionOperation, old: &BackendLocation) -> Effects {
    operation.start();
    operation.step(read_result(Some(version(old).to_bytes().unwrap())));
    operation.step(read_result(Some(old.to_bytes().unwrap())))
}

/// Answers the rewrite with `new` and the transaction reads with `settings`.
fn published(
    operation: &mut RewriteVersionOperation,
    old: &BackendLocation,
    new: BackendLocation,
    settings: Vec<u8>,
) -> Effects {
    let effects = operation.step(Event::Blob(BlobEvent::CopyRewritten { location: new }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction { .. })]
    ));
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: TxnId::default(),
    }));
    let record = operation.transition.to_bytes().unwrap();
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (Vec::new().into(), Some(settings.into())),
            (Vec::new().into(), Some(record.into())),
        ],
    }));
    if !matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ) {
        return effects;
    }
    operation.step(read_result(Some(version(old).to_bytes().unwrap())));
    operation.step(read_result(None))
}

#[test]
fn encrypts_plain_copy() {
    let mut operation = operation(transition(TransitionKind::Encrypt, None, Some(1)));
    let old = location(None);
    let effects = located(&mut operation, &old);
    let [
        Effect::Blob(BlobEffect::RewriteCopy {
            lease,
            target,
            grants_only,
            ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected a rewrite, got {effects:?}")
    };
    assert!(lease.is_none() && !grants_only);
    assert_eq!(target.encryption, Some(plan(1)));

    let effects = published(&mut operation, &old, location(Some(1)), settings(1, 3));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the row writes, got {effects:?}")
    };
    let spaces: Vec<&str> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
    assert_eq!(
        spaces,
        [
            BLOB_LOCATIONS_KEYSPACE,
            COPY_OWNER_KEYSPACE,
            BLOB_VERSIONS_KEYSPACE,
            TRANSITION_CLEANUP_KEYSPACE,
            BLOB_RECLAIM_KEYSPACE,
        ]
    );
    let stored = BlobVersion::from_bytes(&writes[2].2).unwrap();
    let encoding = stored.location_key().unwrap().encoding;
    assert_eq!(encoding, EncodingClass::Pithos { digest: [1; 32] });
}

#[test]
fn locked_source_waits() {
    let mut operation = operation(transition(TransitionKind::Decrypt, Some(1), None));
    let effects = located(&mut operation, &location(Some(1)));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::AdmitRead { key, .. })] if key.generation == 1
    ));

    let locked = BucketKeyError::Locked(BUCKET_ID);
    let effects = operation.step(Event::Blob(BlobEvent::Error(locked.into())));

    assert!(effects.is_empty());
    assert_eq!(operation.finalize(), Ok(RewriteOutcome::AwaitingKey));
}

#[test]
fn stale_target_discards() {
    // A newer change advanced the storage generation: the new copy is never published.
    let mut operation = operation(transition(TransitionKind::Encrypt, None, Some(1)));
    let old = location(None);
    located(&mut operation, &old);

    let effects = published(&mut operation, &old, location(Some(1)), settings(1, 4));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    operation.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: TxnId::default(),
    }));
    let id = operation.new.as_ref().unwrap().ulid;
    let effects = operation.step(Event::Blob(BlobEvent::ReservationReleased { id }));
    assert_eq!(effects.as_slice(), [schedule_cleanup_effect()]);
    assert_eq!(operation.finalize(), Ok(RewriteOutcome::Skipped));
}

#[test]
fn rotation_keeps_blocks() {
    let mut operation = operation(transition(TransitionKind::Rotate, Some(1), Some(2)));
    let old = location(Some(1));
    located(&mut operation, &old);
    let admitted = lease(&old);

    let effects = operation.step(Event::Blob(BlobEvent::ReadAdmitted { lease: lease(&old) }));

    let [
        Effect::Blob(BlobEffect::RewriteCopy {
            lease, grants_only, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected a rewrite, got {effects:?}")
    };
    assert!(*grants_only);
    assert_eq!(lease.as_ref().map(|lease| lease.key), Some(admitted.key));
    let effects = published(&mut operation, &old, location(Some(2)), settings(2, 3));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchWrite { .. })]
    ));
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    let owner = CopyOwner::new(ArchiveKey::of(&old), operation.version_key.clone());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Delete { key_space, key, .. })]
            if key_space == COPY_OWNER_KEYSPACE && key.as_ref() == owner.key().unwrap()
    ));
}

#[test]
fn rejects_wrong_event() {
    let mut operation = operation(transition(TransitionKind::Encrypt, None, Some(1)));
    located(&mut operation, &location(None));

    operation.step(read_result(None));

    assert!(matches!(
        operation.finalize(),
        Err(RewriteError::InvalidStateEvent { .. })
    ));
}
