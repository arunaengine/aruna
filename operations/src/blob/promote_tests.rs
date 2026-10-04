//! Steps pending content promotion through its effects without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::storage::blob::{BackendRef, VersionKey};
use aruna_core::structs::storage::encryption::ReadLease;
use aruna_core::structs::storage::format::{PithosLayout, StoredFormat};
use std::sync::Arc;
use std::time::SystemTime;
use ulid::Ulid;

const BLAKE3: [u8; 32] = [9; 32];

fn sealed() -> BackendLocation {
    let layout = PithosLayout {
        stored_size: 130,
        metadata_digest: [4; 32],
    };
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "path".to_string(),
        ulid: Ulid::from_bytes([3; 16]),
        format: StoredFormat::pithos(layout, BucketKeyRef::new(Ulid::from_bytes([5; 16]), 1)),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        staging: false,
        partial: false,
        blob_size: 100,
        hashes: HashMap::new(),
    }
}

fn operation() -> PromotePendingOperation {
    let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
    let archive = ArchiveKey::of(&sealed());
    PromotePendingOperation::new(archive, RealmId([1; 32]), node, RoCrateLimits::default())
}

fn read(value: Option<Vec<u8>>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: Key::from(Vec::new()),
        value: value.map(Value::from),
    })
}

fn location_read() -> Event {
    read(Some(sealed().to_bytes().unwrap()))
}

fn lease() -> ReadLease {
    let location = sealed();
    let key = location.format.bucket_key().unwrap();
    ReadLease::new(key, ArchiveKey::of(&location), Ulid::nil(), Arc::new(()))
}

fn hashed(size: u64) -> Event {
    let hashes = HashMap::from([(HASH_BLAKE3.to_string(), BLAKE3.to_vec())]);
    Event::Blob(BlobEvent::ArchiveHashed { hashes, size })
}

/// Steps through admission and hashing into the first transaction.
fn hashed_operation() -> PromotePendingOperation {
    let mut operation = operation();
    operation.start();
    operation.step(location_read());
    operation.step(Event::Blob(BlobEvent::ReadAdmitted { lease: lease() }));
    let effects = operation.step(hashed(100));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction { .. })]
    ));
    let txn_id = Ulid::from_bytes([7; 16]);
    operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    operation
}

#[test]
fn locked_key_waits() {
    let mut operation = operation();
    operation.start();
    let effects = operation.step(location_read());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::AdmitRead { .. })]
    ));
    let key = sealed().format.bucket_key().unwrap();
    let locked = BlobError::BucketKey(BucketKeyError::Locked(key.bucket_id));
    operation.step(Event::Blob(BlobEvent::Error(locked)));
    assert!(operation.is_complete());
    assert_eq!(operation.finalize(), Ok(Promotion::AwaitingKey(key)));
}

#[test]
fn wrong_size_fails() {
    let mut operation = operation();
    operation.start();
    operation.step(location_read());
    operation.step(Event::Blob(BlobEvent::ReadAdmitted { lease: lease() }));
    operation.step(hashed(99));
    assert_eq!(operation.finalize(), Err(PromoteError::BadHashes));
}

#[test]
fn changed_archive_aborts() {
    let mut operation = hashed_operation();
    let mut changed = sealed();
    changed.blob_size = 7;
    let effects = operation.step(read(Some(changed.to_bytes().unwrap())));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    operation.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: Ulid::nil(),
    }));
    assert_eq!(operation.finalize(), Ok(Promotion::Gone));
}

#[test]
fn promotes_pending_alias() {
    let mut operation = hashed_operation();
    let effects = operation.step(location_read());
    let [Effect::Storage(StorageEffect::Write { key, value, .. })] = effects.as_slice() else {
        panic!("the known location row comes first")
    };
    let location_key = BlobLocationKey::from_bytes(key).unwrap();
    assert_eq!(location_key.blake3_hash, BLAKE3);
    let known = BackendLocation::from_bytes(value).unwrap();
    assert_eq!(known.hashes.get(HASH_BLAKE3), Some(&BLAKE3.to_vec()));
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: key.clone(),
    }));

    let version_key = VersionKey::new("bucket", "key", Ulid::from_bytes([6; 16]));
    let owner = CopyOwner::new(ArchiveKey::of(&sealed()), version_key.clone());
    let owners = vec![(Key::from(owner.key().unwrap()), Value::from(Vec::new()))];
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: owners,
        next_start_after: None,
    }));
    let created = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(5);
    let pending =
        BlobVersion::pending(ArchiveKey::of(&sealed()), created, Default::default(), None);
    let version_row = Key::from(version_key.to_bytes().unwrap());
    let versions = vec![(
        version_row.clone(),
        Some(Value::from(pending.to_bytes().unwrap())),
    )];
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: versions,
    }));
    let group_id = Ulid::from_bytes([8; 16]);
    let info = BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
    };
    let buckets = vec![(
        Key::from(b"bucket".to_vec()),
        Some(Value::from(info.to_bytes().unwrap())),
    )];
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: buckets,
    }));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("one batch promotes the page")
    };
    assert_eq!(writes.len(), 2);
    let promoted = BlobVersion::from_bytes(&writes[0].2).unwrap();
    assert_eq!(promoted.created_at, created);
    assert_eq!(promoted.state.blob_hash(), Some(&BLAKE3));
    assert_eq!(writes[1].0, aruna_core::keyspaces::PATHS_INDEX_KEYSPACE);

    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Delete { key_space, .. })]
            if key_space == PENDING_LOCATION_KEYSPACE
    ));
    operation.step(Event::Storage(StorageEvent::DeleteResult {
        key: Key::from(Vec::new()),
    }));
    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::nil(),
    }));
    assert!(matches!(effects.as_slice(), [Effect::Net(_)]));
    let Effect::Net(aruna_core::effects::NetEffect::Dht(aruna_core::effects::DhtEffect::Put {
        key,
        ..
    })) = &effects[0]
    else {
        panic!("the content hash is registered in the DHT")
    };
    operation.step(Event::Net(aruna_core::events::NetEvent::Dht(
        aruna_core::events::DhtEvent::PutComplete {
            key: key.clone(),
            remote_attempt_count: 0,
            remote_store_count: 0,
        },
    )));
    assert_eq!(
        operation.finalize(),
        Ok(Promotion::Promoted {
            blake3: BLAKE3,
            versions: 1
        })
    );
}
