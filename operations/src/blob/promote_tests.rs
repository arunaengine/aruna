//! Steps pending content promotion through its effects without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::storage::abe::envelope_charge;
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
        storage_generation: 0,
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

/// The key record and the claimed hash of the archive, as the reread transaction reads them.
fn key_rows(record: &BucketKeyRecord, claim: Option<[u8; 32]>) -> Event {
    Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                Key::from(Vec::new()),
                Some(Value::from(record.to_bytes().unwrap())),
            ),
            (
                Key::from(Vec::new()),
                claim.map(|claim| Value::from(claim.to_vec())),
            ),
        ],
    })
}

fn reread_with(operation: &mut PromotePendingOperation, claim: Option<[u8; 32]>) -> Effects {
    let effects = operation.step(location_read());
    let [
        Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(_),
        }),
    ] = effects.as_slice()
    else {
        panic!("the key and the claim are read in the transaction")
    };
    assert_eq!(reads[0].0, BUCKET_KEY_KEYSPACE);
    assert_eq!(reads[1].0, PENDING_CLAIM_KEYSPACE);
    let key = sealed().format.bucket_key().unwrap();
    let record = BucketKeyRecord::new(key, Ulid::nil(), [1; 32], 0);
    operation.step(key_rows(&record, claim))
}

fn reread(operation: &mut PromotePendingOperation) -> Effects {
    reread_with(operation, None)
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

#[tokio::test]
async fn retirement_fences_promotion() {
    let directory = tempfile::tempdir().unwrap();
    let storage = aruna_storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    let key = sealed().format.bucket_key().unwrap();
    let mut record = BucketKeyRecord::new(key, Ulid::nil(), [1; 32], 0);
    record.state = KeyState::Retiring;
    storage
        .send_storage_effect(StorageEffect::Write {
            key_space: BUCKET_KEY_KEYSPACE.to_string(),
            key: key.key().into(),
            value: record.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        panic!("transaction missing")
    };
    let mut operation = hashed_operation();
    operation.txn_id = Some(txn_id);
    let mut effects = operation.step(location_read());
    let Some(Effect::Storage(read)) = effects.pop() else {
        panic!("key fence missing")
    };
    let mut effects = operation.step(storage.send_storage_effect(read).await);
    let Some(Effect::Storage(write)) = effects.pop() else {
        panic!("location write missing")
    };
    storage.send_storage_effect(write).await;
    record.state = KeyState::Retired;
    storage
        .send_storage_effect(StorageEffect::Write {
            key_space: BUCKET_KEY_KEYSPACE.to_string(),
            key: key.key().into(),
            value: record.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    let committed = storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await;
    assert!(matches!(
        committed,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict
        })
    ));
}

#[test]
fn retired_key_refused() {
    let mut operation = hashed_operation();
    operation.step(location_read());
    let key = sealed().format.bucket_key().unwrap();
    let mut record = BucketKeyRecord::new(key, Ulid::nil(), [1; 32], 0);
    record.state = KeyState::Retired;
    let effects = operation.step(key_rows(&record, None));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    operation.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: Ulid::nil(),
    }));
    assert_eq!(operation.finalize(), Err(PromoteError::NoKey));
}

#[test]
fn promotes_pending_alias() {
    let mut operation = hashed_operation();
    let effects = reread(&mut operation);
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
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: buckets,
    }));
    let effects = operation.step(no_registrations());
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
    // The final pass rescans from the first owner and finds nothing left pending.
    assert_eq!(effects.as_slice(), &verify_scan());
    let owners = vec![(Key::from(owner.key().unwrap()), Value::from(Vec::new()))];
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: owners,
        next_start_after: None,
    }));
    let versions = vec![(version_row, Some(writes[0].2.clone()))];
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: versions,
    }));
    let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = effects.as_slice() else {
        panic!("the pending row and its claim go together")
    };
    assert_eq!(deletes[0].0, PENDING_LOCATION_KEYSPACE);
    assert_eq!(deletes[1].0, PENDING_CLAIM_KEYSPACE);
    operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
        entries: Vec::new(),
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
            key: *key,
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

/// The verification scan: every owner, from the first, inside the open transaction.
fn verify_scan() -> [Effect; 1] {
    [Effect::Storage(StorageEffect::Iter {
        key_space: COPY_OWNER_KEYSPACE.to_string(),
        prefix: Some(CopyOwner::prefix(&ArchiveKey::of(&sealed())).into()),
        start: None,
        limit: PROMOTE_PAGE,
        txn_id: Some(Ulid::from_bytes([7; 16])),
    })]
}

fn bucket_rows() -> Vec<(Key, Option<Value>)> {
    let info = BucketInfo {
        group_id: Ulid::from_bytes([8; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
    };
    vec![(
        Key::from(b"bucket".to_vec()),
        Some(Value::from(info.to_bytes().unwrap())),
    )]
}

/// Promotes one owner page, `version_id` being the only owner, up to its batch write.
fn promote_one(operation: &mut PromotePendingOperation, version_id: [u8; 16]) -> Key {
    let version_key = VersionKey::new("bucket", "key", Ulid::from_bytes(version_id));
    let owner = CopyOwner::new(ArchiveKey::of(&sealed()), version_key.clone());
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: vec![(Key::from(owner.key().unwrap()), Value::from(Vec::new()))],
        next_start_after: None,
    }));
    let pending = BlobVersion::pending(
        ArchiveKey::of(&sealed()),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let row = Key::from(version_key.to_bytes().unwrap());
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![(row, Some(Value::from(pending.to_bytes().unwrap())))],
    }));
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: bucket_rows(),
    }));
    let effects = operation.step(no_registrations());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchWrite { .. })]
    ));
    Key::from(owner.key().unwrap())
}

#[test]
fn alias_behind_cursor() {
    let mut operation = hashed_operation();
    reread(&mut operation);
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    let first = promote_one(&mut operation, [6; 16]);
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert_eq!(effects.as_slice(), &verify_scan());

    // A copy committed an alias that sorts before the first owner, behind the cursor.
    let behind = VersionKey::new("bucket", "a", Ulid::from_bytes([1; 16]));
    let inserted = CopyOwner::new(ArchiveKey::of(&sealed()), behind.clone());
    let promoted = BlobVersion::materialized(
        BLAKE3,
        BackendRef::node_default(),
        sealed().format.encoding(),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let pending = BlobVersion::pending(
        ArchiveKey::of(&sealed()),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: vec![
            (Key::from(inserted.key().unwrap()), Value::from(Vec::new())),
            (first, Value::from(Vec::new())),
        ],
        next_start_after: None,
    }));
    let behind_row = Key::from(behind.to_bytes().unwrap());
    let first_row = Key::from(
        VersionKey::new("bucket", "key", Ulid::from_bytes([6; 16]))
            .to_bytes()
            .unwrap(),
    );
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                behind_row.clone(),
                Some(Value::from(pending.to_bytes().unwrap())),
            ),
            (first_row, Some(Value::from(promoted.to_bytes().unwrap()))),
        ],
    }));
    assert!(
        !matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { .. })]
        ),
        "the pending row must stay while an alias still pends"
    );
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: bucket_rows(),
    }));
    let effects = operation.step(no_registrations());
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("the alias behind the cursor is promoted in the final transaction")
    };
    assert_eq!(writes[0].1, behind_row);
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })]
            if deletes[0].0 == PENDING_LOCATION_KEYSPACE
    ));
}

#[test]
fn conflict_restarts_scan() {
    let mut operation = hashed_operation();
    reread(&mut operation);
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    let first = promote_one(&mut operation, [6; 16]);
    operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: vec![(first, Value::from(Vec::new()))],
        next_start_after: None,
    }));
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: Vec::new(),
    }));
    operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
        entries: Vec::new(),
    }));
    // An owner inserted into the scanned range makes the final commit conflict.
    let effects = operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction { .. })]
    ));
    assert!(!operation.is_complete());
}

/// The one version of the page has no placement registration and no envelope.
fn no_registrations() -> Event {
    Event::Storage(StorageEvent::BatchReadResult {
        values: vec![(Key::from(Vec::new()), None), (Key::from(Vec::new()), None)],
    })
}

#[test]
fn envelope_mapping_rebound() {
    // A promoted version's envelope mapping names the location key; completion charged its size.
    let mut operation = hashed_operation();
    reread(&mut operation);
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    let version = VersionKey::new("bucket", "key", Ulid::from_bytes([6; 16]));
    let owner = CopyOwner::new(ArchiveKey::of(&sealed()), version.clone());
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: vec![(Key::from(owner.key().unwrap()), Value::from(Vec::new()))],
        next_start_after: None,
    }));
    let pending = BlobVersion::pending(
        ArchiveKey::of(&sealed()),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let row = Key::from(version.to_bytes().unwrap());
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![(row.clone(), Some(Value::from(pending.to_bytes().unwrap())))],
    }));
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: bucket_rows(),
    }));
    let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice() else {
        panic!("the page reads its registrations and envelope ids")
    };
    assert_eq!(reads[1], (ABE_VERSION_KEYSPACE.to_string(), row));
    let id = Key::from(vec![2; 16]);
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (reads[0].1.clone(), None),
            (reads[1].1.clone(), Some(id.clone())),
        ],
    }));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("one batch promotes the page")
    };
    let (_, key, value) = writes
        .iter()
        .find(|(key_space, ..)| key_space == ABE_ARCHIVE_KEYSPACE)
        .expect("the envelope mapping is rewritten in the promotion transaction");
    assert_eq!(key, &id);
    let mut archive: EnvelopeArchive = postcard::from_bytes(value).unwrap();
    let location = sealed();
    let location_key = BlobLocationKey::new(BLAKE3, location.format.encoding(), location.backend);
    assert_eq!(archive.archive, ArchiveKey::of(&sealed()));
    assert_eq!(archive.location_key, location_key.to_bytes());
    archive.location_key.clear();
    let pending = postcard::to_allocvec(&archive).unwrap();
    assert_eq!(envelope_charge(&[], &pending), envelope_charge(&[], value));
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::Storage(StorageEffect::AddUsage { .. }))),
        "promotion adds no usage, got {effects:?}"
    );
}

#[test]
fn governed_alias_registration() {
    use crate::blob::managed_copy::{CopyRequest, validate_registration};
    use aruna_core::structs::storage::blob::{ManagedCopyKey, ManagedCopyState};

    let mut operation = hashed_operation();
    reread(&mut operation);
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Key::from(Vec::new()),
    }));
    let version = VersionKey::new("bucket", "key", Ulid::from_bytes([6; 16]));
    let owner = CopyOwner::new(ArchiveKey::of(&sealed()), version.clone());
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: vec![(Key::from(owner.key().unwrap()), Value::from(Vec::new()))],
        next_start_after: None,
    }));
    let pending = BlobVersion::pending(
        ArchiveKey::of(&sealed()),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let row = Key::from(version.to_bytes().unwrap());
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![(row, Some(Value::from(pending.to_bytes().unwrap())))],
    }));
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: bucket_rows(),
    }));
    let key = ManagedCopyKey::new(version.clone(), BackendRef::node_default());
    let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice() else {
        panic!("the page reads its registrations")
    };
    assert_eq!(reads[0].1, Key::from(key.to_bytes().unwrap()));
    let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
    let registered = ManagedCopyRecord::new(
        version,
        node,
        sealed(),
        Vec::new(),
        0,
        ManagedCopyState::Registered,
    )
    .unwrap();
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                Key::from(key.to_bytes().unwrap()),
                Some(Value::from(registered.to_bytes().unwrap())),
            ),
            (Key::from(Vec::new()), None),
        ],
    }));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("one batch promotes the page")
    };
    let (_, _, value) = writes
        .iter()
        .find(|(key_space, ..)| key_space == MANAGED_COPY_KEYSPACE)
        .expect("the registration is rewritten in the promotion transaction");
    // A read of the promoted version asks for its hash; the registration now matches it.
    let request = CopyRequest {
        key: &key,
        node_id: Some(node),
        blake3: Some(BLAKE3),
        refs: &[],
        subject_generation: None,
    };
    assert!(validate_registration(Some(value.as_ref()), &request).is_ok());
}

/// A received archive whose verified hash differs from its sender's claim registers nothing.
#[test]
fn claim_mismatch_unregistered() {
    let mut operation = hashed_operation();
    let effects = reread_with(&mut operation, Some([8; 32]));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    operation.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: Ulid::nil(),
    }));
    assert_eq!(
        operation.finalize(),
        Ok(Promotion::Mismatch { claimed: [8; 32] })
    );

    // A matching claim promotes as usual.
    let mut operation = hashed_operation();
    let effects = reread_with(&mut operation, Some(BLAKE3));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { key_space, .. })]
            if key_space == BLOB_LOCATIONS_KEYSPACE
    ));
}

mod received {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::object::copy::sealed::{SealedCopyInput, SealedCopyOperation};
    use crate::s3::object::copy::test::{full_context, seed_authority, seed_bucket};
    use aruna_core::UserId;
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, PATHS_INDEX_KEYSPACE};
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer, ResolvedBackend};
    use aruna_core::structs::storage::encryption::{SealPlan, public_key_of};
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    async fn put(context: &DriverContext, space: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: space.into(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn fixture() -> (tempfile::TempDir, DriverContext, SealPlan, UserId) {
        let (temp, context) = full_context().await;
        let realm = RealmId([1; 32]);
        let user = UserId::local(Ulid::generate(), realm);
        let group = Ulid::from_bytes([8; 16]);
        seed_bucket(&context, "bucket", group, user, Vec::new()).await;
        seed_authority(&context, realm, group, user).await;
        let private = SecretBytes::new(vec![9; 32]);
        let key = BucketKeyRef::new(Ulid::generate(), 1);
        let public = public_key_of(&private).unwrap();
        let record = BucketKeyRecord::new(key, Ulid::generate(), public, 0);
        put(
            &context,
            BUCKET_KEY_KEYSPACE,
            key.key(),
            record.to_bytes().unwrap(),
        )
        .await;
        let blob = context.blob_handle.as_ref().unwrap();
        let prepared = blob
            .send_blob_effect(BlobEffect::PrepareKey {
                key,
                public_key: public,
                private_key: SharedSecret::new(private),
                duration: None,
                max: None,
            })
            .await;
        let Event::Blob(BlobEvent::KeyPrepared { ticket }) = prepared else {
            panic!("key missing")
        };
        blob.send_blob_effect(BlobEffect::ActivateKey { ticket })
            .await;
        let plan = SealPlan {
            key,
            public_key: public,
            cipher: Default::default(),
            block_keys: Default::default(),
            storage_generation: 0,
        };
        (temp, context, plan, user)
    }

    async fn pending(
        context: &DriverContext,
        plan: SealPlan,
        user: UserId,
        name: &str,
        data: Vec<u8>,
        claimed: [u8; 32],
        invalid: bool,
    ) -> (BackendLocation, VersionKey) {
        let resolved = ResolvedBackend::node_default().with_encryption((!invalid).then_some(plan));
        let blob = BackendStream::new(futures_util::stream::iter([Ok::<_, std::io::Error>(
            bytes::Bytes::from(data),
        )]));
        let written = context
            .blob_handle
            .as_ref()
            .unwrap()
            .send_blob_effect(BlobEffect::Write {
                bucket: "bucket".into(),
                key: name.into(),
                resolved,
                created_by: user,
                blob,
                size: None,
            })
            .await;
        let Event::Blob(BlobEvent::WriteFinished { mut location }) = written else {
            panic!("write failed: {written:?}")
        };
        if invalid {
            location.format = StoredFormat::pithos(
                PithosLayout {
                    stored_size: location.blob_size,
                    metadata_digest: [0; 32],
                    storage_generation: 0,
                },
                plan.key,
            );
            location.ulid = Ulid::nil();
        }
        location.hashes.clear();
        let archive = ArchiveKey::of(&location);
        let version = VersionKey::new("bucket", name, Ulid::generate());
        let stored = BlobVersion::pending(archive.clone(), SystemTime::UNIX_EPOCH, user, None);
        let owner = CopyOwner::new(archive.clone(), version.clone());
        put(
            context,
            PENDING_LOCATION_KEYSPACE,
            archive.to_bytes(),
            location.to_bytes().unwrap(),
        )
        .await;
        put(
            context,
            PENDING_CLAIM_KEYSPACE,
            archive.to_bytes(),
            claimed.to_vec(),
        )
        .await;
        put(
            context,
            BLOB_VERSIONS_KEYSPACE,
            version.to_bytes().unwrap(),
            stored.to_bytes().unwrap(),
        )
        .await;
        put(
            context,
            COPY_OWNER_KEYSPACE,
            owner.key().unwrap(),
            Vec::new(),
        )
        .await;
        put(
            context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("bucket", name).to_bytes().unwrap(),
            CurrentVersionPointer::new(version.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await;
        (location, version)
    }

    fn origin(context: &DriverContext) -> (RealmId, NodeId) {
        (
            RealmId([1; 32]),
            context.net_handle.as_ref().unwrap().node_id(),
        )
    }

    async fn sweep(context: &DriverContext, key: BucketKeyRef) -> Result<usize, PromoteError> {
        promote_unlocked(context, key, origin(context), &RoCrateLimits::default()).await
    }

    async fn rejected(context: &DriverContext, archive: &ArchiveKey) {
        for space in [
            BLOB_VERSIONS_KEYSPACE,
            BLOB_HEAD_KEYSPACE,
            COPY_OWNER_KEYSPACE,
            BLOB_LOCATIONS_KEYSPACE,
            PATHS_INDEX_KEYSPACE,
        ] {
            let (rows, _) = crate::jobs::store::iter_prefix_page(
                &context.storage_handle,
                space,
                None,
                None,
                8,
                None,
            )
            .await
            .unwrap();
            assert!(rows.is_empty(), "rejected archive remains in {space}");
        }
        assert!(
            read_value(
                &context.storage_handle,
                PENDING_LOCATION_KEYSPACE,
                archive.to_bytes()
            )
            .await
            .unwrap()
            .is_some()
        );
        assert_eq!(
            read_value(
                &context.storage_handle,
                PENDING_CLAIM_KEYSPACE,
                archive.to_bytes()
            )
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
            &[0; 32]
        );
    }

    #[tokio::test]
    async fn mismatch_resweep_blocked() {
        let (_temp, context, plan, user) = fixture().await;
        let (location, _) = pending(
            &context,
            plan,
            user,
            "rejected",
            b"content".to_vec(),
            [0; 32],
            false,
        )
        .await;
        let archive = ArchiveKey::of(&location);
        assert_eq!(sweep(&context, plan.key).await.unwrap(), 0);
        rejected(&context, &archive).await;
        assert_eq!(sweep(&context, plan.key).await.unwrap(), 0);
        rejected(&context, &archive).await;
    }

    async fn storage_step(
        context: &DriverContext,
        operation: &mut RejectOwnersOperation,
        effects: Effects,
    ) -> Effects {
        assert_eq!(effects.len(), 1);
        let Some(Effect::Storage(effect)) = effects.into_iter().next() else {
            panic!("storage effect missing")
        };
        operation.step(context.storage_handle.send_storage_effect(effect).await)
    }

    #[tokio::test]
    async fn mismatch_alias_fenced() {
        let (_temp, context, plan, user) = fixture().await;
        let (location, version) = pending(
            &context,
            plan,
            user,
            "rejected",
            b"content".to_vec(),
            [0; 32],
            false,
        )
        .await;
        let archive = ArchiveKey::of(&location);
        let record =
            BlobQuarantineRecord::new([0; 32], archive.backend.clone(), "mismatch".into(), 0);
        let mut fence = RejectOwnersOperation::new(archive.clone(), &record).unwrap();
        let effects = fence.start();
        let effects = storage_step(&context, &mut fence, effects).await;
        let effects = storage_step(&context, &mut fence, effects).await;
        let alias = SealedCopyOperation::new(SealedCopyInput {
            bucket: "bucket".into(),
            source_key: version.key,
            source_version_id: version.version_id,
            location: location.clone(),
            source_policies: Vec::new(),
            size: location.blob_size,
            dest_key: "alias".into(),
            metadata: None,
            user_id: user,
            group_id: Ulid::from_bytes([8; 16]),
            realm_id: origin(&context).0,
            node_id: origin(&context).1,
            quota_ceiling: None,
        });
        drive(alias, &context).await.unwrap();
        let effects = storage_step(&context, &mut fence, effects).await;
        let effects = storage_step(&context, &mut fence, effects).await;
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        let mut effects = effects;
        while !fence.is_complete() {
            effects = storage_step(&context, &mut fence, effects).await;
        }
        assert_eq!(fence.finalize().unwrap().len(), 2);
        assert_eq!(
            drop_mismatch(&context, &archive, [0; 32], origin(&context))
                .await
                .unwrap(),
            2
        );
        assert_eq!(sweep(&context, plan.key).await.unwrap(), 0);
        rejected(&context, &archive).await;
    }

    struct Capture(tokio::sync::mpsc::UnboundedSender<aruna_net::streams::BiStream>);

    #[async_trait::async_trait]
    impl aruna_net::InboundEventHandler for Capture {
        async fn handle_incoming_stream(
            &self,
            _: aruna_core::alpn::Alpn,
            stream: aruna_net::streams::BiStream,
            _: NodeId,
        ) {
            self.0.send(stream).unwrap();
        }
    }

    async fn bao_bytes(context: &DriverContext, data: Vec<u8>) -> Vec<u8> {
        let net = context.net_handle.as_ref().unwrap();
        let (sender, mut incoming) = tokio::sync::mpsc::unbounded_channel();
        net.set_inbound_handler(Arc::new(Capture(sender)));
        let mut stream = net
            .open_stream(net.node_id(), aruna_core::alpn::Alpn::Bao)
            .await
            .unwrap();
        let inbound = incoming.recv().await.unwrap();
        let blob = context.blob_handle.as_ref().unwrap();
        let id = blob.store_connection(net.node_id(), inbound).await.unwrap();
        stream.0.write_all(&data).await.unwrap();
        stream.0.finish().unwrap();
        let event = blob
            .send_blob_effect(BlobEffect::ReceiveRead {
                stream_id: id,
                size: data.len() as u64,
                expected_blake3: *blake3::hash(&data).as_bytes(),
            })
            .await;
        let Event::Blob(BlobEvent::ReadFinished { blob, .. }) = event else {
            panic!("Bao read failed")
        };
        let chunks: Vec<_> = blob.0.collect().await;
        let verified: Vec<u8> = chunks
            .into_iter()
            .flat_map(|chunk| chunk.unwrap())
            .collect();
        assert_eq!(verified, data);
        verified
    }

    #[tokio::test]
    async fn corrupt_copy_continues() {
        rejected_copy(false).await;
    }

    #[tokio::test]
    async fn wrong_size_continues() {
        rejected_copy(true).await;
    }

    async fn rejected_copy(wrong_size: bool) {
        let (_temp, context, plan, user) = fixture().await;
        let (bytes, original) = if wrong_size {
            let stream = BackendStream::new(futures_util::stream::iter([Ok::<_, std::io::Error>(
                bytes::Bytes::from_static(b"content"),
            )]));
            let event = context
                .blob_handle
                .as_ref()
                .unwrap()
                .send_blob_effect(BlobEffect::Write {
                    bucket: "bucket".into(),
                    key: "source".into(),
                    resolved: ResolvedBackend::node_default().with_encryption(Some(plan)),
                    created_by: user,
                    blob: stream,
                    size: Some(7),
                })
                .await;
            let Event::Blob(BlobEvent::WriteFinished { location }) = event else {
                panic!("sealed write failed: {event:?}")
            };
            let bytes = std::fs::read(location.get_full_path().unwrap()).unwrap();
            (bytes, Some((location.format, location.blob_size)))
        } else {
            (vec![0; 64], None)
        };
        let invalid = bao_bytes(&context, bytes).await;
        let claimed = if wrong_size {
            *blake3::hash(b"content").as_bytes()
        } else {
            [0; 32]
        };
        let (mut bad, version) =
            pending(&context, plan, user, "invalid", invalid, claimed, true).await;
        if let Some((format, size)) = original {
            bad.format = format;
            bad.blob_size = size + 1;
            put(
                &context,
                PENDING_LOCATION_KEYSPACE,
                ArchiveKey::of(&bad).to_bytes(),
                bad.to_bytes().unwrap(),
            )
            .await;
        }
        let data = b"valid content".to_vec();
        let hash = *blake3::hash(&data).as_bytes();
        let (_, valid) = pending(&context, plan, user, "valid", data, hash, false).await;
        assert_eq!(sweep(&context, plan.key).await.unwrap(), 1);
        assert!(
            read_value(
                &context.storage_handle,
                BLOB_VERSIONS_KEYSPACE,
                version.to_bytes().unwrap()
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            read_value(
                &context.storage_handle,
                COPY_OWNER_KEYSPACE,
                CopyOwner::new(ArchiveKey::of(&bad), version).key().unwrap()
            )
            .await
            .unwrap()
            .is_none()
        );
        let stored = read_value(
            &context.storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            valid.to_bytes().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            BlobVersion::from_bytes(&stored).unwrap().state.blob_hash(),
            Some(&hash)
        );
        assert!(
            read_value(
                &context.storage_handle,
                PENDING_CLAIM_KEYSPACE,
                ArchiveKey::of(&bad).to_bytes()
            )
            .await
            .unwrap()
            .is_some()
        );
        let record = BlobQuarantineRecord::new(claimed, bad.backend, String::new(), 0);
        let value = read_value(
            &context.storage_handle,
            BLOB_QUARANTINE_KEYSPACE,
            record.key(),
        )
        .await
        .unwrap()
        .unwrap();
        if wrong_size {
            let record = BlobQuarantineRecord::from_bytes(&value).unwrap();
            assert!(record.reason.contains("holds 7 bytes, recorded 8"));
        }
    }

    #[tokio::test]
    async fn transient_copy_retained() {
        let (_temp, context, plan, user) = fixture().await;
        let data = b"content".to_vec();
        let hash = *blake3::hash(&data).as_bytes();
        let (location, version) = pending(&context, plan, user, "missing", data, hash, false).await;
        std::fs::remove_file(
            std::path::Path::new(&location.root).join(location.get_storage_path().unwrap()),
        )
        .unwrap();
        assert!(matches!(
            sweep(&context, plan.key).await,
            Err(PromoteError::Blob(BlobError::ReadError(_)))
        ));
        assert!(
            read_value(
                &context.storage_handle,
                BLOB_VERSIONS_KEYSPACE,
                version.to_bytes().unwrap()
            )
            .await
            .unwrap()
            .is_some()
        );
        assert!(
            read_value(
                &context.storage_handle,
                PENDING_CLAIM_KEYSPACE,
                ArchiveKey::of(&location).to_bytes()
            )
            .await
            .unwrap()
            .is_some()
        );
    }
}
