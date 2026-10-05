//! Same-bucket copies of sealed versions: shared archives, pending aliases and refusals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use aruna_core::keyspaces::COPY_OWNER_KEYSPACE;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::structs::storage::format::{PithosLayout, StoredFormat};
use aruna_storage::storage::{self, StorageHandle};
use tempfile::{TempDir, tempdir};

const SOURCE: &str = "sealed";

/// Storage only: no blob adapter exists, so any content read would fail the copy.
fn context() -> (TempDir, DriverContext) {
    let temp = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    (temp, context)
}

fn sealed_location() -> BackendLocation {
    let layout = PithosLayout {
        stored_size: 80,
        metadata_digest: [5u8; 32],
    };
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "objects".to_string(),
        backend_path: SOURCE.to_string(),
        ulid: Ulid::generate(),
        format: StoredFormat::pithos(layout, BucketKeyRef::new(Ulid::generate(), 1)),
        created_by: UserId::default(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 50,
        hashes: HashMap::new(),
    }
}

async fn put(storage: &StorageHandle, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    storage
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
}

async fn get(storage: &StorageHandle, key_space: &str, key: Vec<u8>) -> Option<Vec<u8>> {
    let event = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("unexpected storage event: {event:?}");
    };
    value.map(|value| value.to_vec())
}

async fn seed(storage: &StorageHandle, version_id: Ulid, version: &BlobVersion) {
    let key = VersionKey::new("bucket", SOURCE, version_id);
    put(
        storage,
        BLOB_VERSIONS_KEYSPACE,
        key.to_bytes().unwrap(),
        version.to_bytes().unwrap(),
    )
    .await;
}

fn input(location: &BackendLocation, version_id: Ulid) -> SealedCopyInput {
    SealedCopyInput {
        bucket: "bucket".to_string(),
        source_key: SOURCE.to_string(),
        source_version_id: version_id,
        location: location.clone(),
        source_policies: Vec::new(),
        size: location.blob_size,
        dest_key: "copy".to_string(),
        metadata: None,
        user_id: UserId::default(),
        group_id: Ulid::from_bytes([2; 16]),
        realm_id: RealmId::from_bytes([3; 32]),
        node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
        quota_ceiling: None,
    }
}

async fn copied(storage: &StorageHandle, version_id: Ulid) -> BlobVersion {
    let key = VersionKey::new("bucket", "copy", version_id)
        .to_bytes()
        .unwrap();
    let value = get(storage, BLOB_VERSIONS_KEYSPACE, key).await.unwrap();
    BlobVersion::from_bytes(&value).unwrap()
}

async fn owns(storage: &StorageHandle, archive: &ArchiveKey, version_id: Ulid) -> bool {
    let version = VersionKey::new("bucket", "copy", version_id);
    let key = CopyOwner::new(archive.clone(), version).key().unwrap();
    get(storage, COPY_OWNER_KEYSPACE, key).await.is_some()
}

#[tokio::test]
async fn pending_alias_shared() {
    // A pending source becomes a pending alias of the same archive, without any content read.
    let (_temp, context) = context();
    let location = sealed_location();
    let archive = ArchiveKey::of(&location);
    let source_id = Ulid::generate();
    let mut source = BlobVersion::pending(
        archive.clone(),
        SystemTime::UNIX_EPOCH,
        UserId::default(),
        None,
    );
    source.metadata = HashMap::from([("color".to_string(), "blue".to_string())]);
    seed(&context.storage_handle, source_id, &source).await;

    let result = drive(
        SealedCopyOperation::new(input(&location, source_id)),
        &context,
    )
    .await
    .unwrap();

    let version = copied(&context.storage_handle, result.version_id).await;
    assert_eq!(version.state.pending_archive(), Some(&archive));
    assert_eq!(version.metadata, source.metadata);
    assert!(owns(&context.storage_handle, &archive, result.version_id).await);
    let head = BlobHeadKey::new("bucket", "copy").to_bytes().unwrap();
    let head = get(&context.storage_handle, BLOB_HEAD_KEYSPACE, head)
        .await
        .unwrap();
    let pointer = CurrentVersionPointer::from_bytes(&head).unwrap();
    assert_eq!(pointer.version_id, result.version_id);
}

#[tokio::test]
async fn known_archive_shared() {
    // A known-hash source keeps its hash and archive; replacement metadata is applied.
    let (_temp, context) = context();
    let mut location = sealed_location();
    location.hashes.insert("blake3".to_string(), vec![9; 32]);
    let archive = ArchiveKey::of(&location);
    let source_id = Ulid::generate();
    let source = BlobVersion::materialized(
        [9; 32],
        location.backend.clone(),
        location.format.encoding(),
        SystemTime::UNIX_EPOCH,
        UserId::default(),
        None,
    );
    seed(&context.storage_handle, source_id, &source).await;
    let mut request = input(&location, source_id);
    request.metadata = Some(HashMap::from([("new".to_string(), "yes".to_string())]));

    let result = drive(SealedCopyOperation::new(request.clone()), &context)
        .await
        .unwrap();

    let version = copied(&context.storage_handle, result.version_id).await;
    assert_eq!(version.state.blob_hash(), Some(&[9; 32]));
    assert_eq!(Some(version.metadata), request.metadata);
    assert!(owns(&context.storage_handle, &archive, result.version_id).await);
}

#[tokio::test]
async fn changed_source_refused() {
    // A source version that no longer uses the described archive is never aliased.
    let (_temp, context) = context();
    let location = sealed_location();
    let other = ArchiveKey::new(Ulid::generate(), location.backend.clone());
    let source_id = Ulid::generate();
    let source = BlobVersion::pending(other, SystemTime::UNIX_EPOCH, UserId::default(), None);
    seed(&context.storage_handle, source_id, &source).await;

    let result = drive(
        SealedCopyOperation::new(input(&location, source_id)),
        &context,
    )
    .await;

    assert_eq!(result, Err(SealedCopyError::SourceChanged));
    let head = BlobHeadKey::new("bucket", "copy").to_bytes().unwrap();
    assert!(
        get(&context.storage_handle, BLOB_HEAD_KEYSPACE, head)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn materialized_archive_changed() {
    for changed_hash in [false, true] {
        let (_temp, context) = context();
        let mut captured = sealed_location();
        captured.hashes.insert("blake3".to_string(), vec![9; 32]);
        let mut current = captured.clone();
        current.ulid = Ulid::generate();
        if !changed_hash {
            let layout = PithosLayout {
                stored_size: 80,
                metadata_digest: [6; 32],
            };
            current.format = StoredFormat::pithos(layout, current.format.bucket_key().unwrap());
        }
        let source_id = Ulid::generate();
        let source = BlobVersion::materialized(
            [if changed_hash { 8 } else { 9 }; 32],
            current.backend,
            current.format.encoding(),
            SystemTime::UNIX_EPOCH,
            UserId::default(),
            None,
        );
        seed(&context.storage_handle, source_id, &source).await;
        let operation = SealedCopyOperation::new(input(&captured, source_id));
        let version = operation.version_id;
        assert_eq!(
            drive(operation, &context).await,
            Err(SealedCopyError::SourceChanged)
        );
        assert!(!owns(&context.storage_handle, &ArchiveKey::of(&captured), version).await);
        let head = BlobHeadKey::new("bucket", "copy").to_bytes().unwrap();
        assert!(
            get(&context.storage_handle, BLOB_HEAD_KEYSPACE, head)
                .await
                .is_none()
        );
    }
}

#[test]
fn copy_reads_no_content() {
    // Every effect is a storage effect, so a locked bucket copies the same way.
    let location = sealed_location();
    let source_id = Ulid::generate();
    let mut operation = SealedCopyOperation::new(input(&location, source_id));
    let missing = || {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: None,
        })
    };
    let mut effects = operation.start();
    // An ungoverned bucket needs no gate, so the transaction starts at once.
    effects.extend(operation.step(missing()));
    let txn_id = Ulid::from_bytes([1; 16]);
    effects.extend(operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id })));
    effects.extend(operation.step(missing()));
    effects.extend(
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Vec::<u8>::new().into(), None),
                (Vec::<u8>::new().into(), None),
            ],
        })),
    );
    let source = BlobVersion::pending(
        ArchiveKey::of(&location),
        SystemTime::UNIX_EPOCH,
        UserId::default(),
        None,
    );
    effects.extend(
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    Vec::<u8>::new().into(),
                    Some(source.to_bytes().unwrap().into()),
                ),
                (Vec::<u8>::new().into(), None),
            ],
        })),
    );
    assert!(
        effects
            .iter()
            .all(|effect| matches!(effect, Effect::Storage(_)))
    );
    assert!(matches!(
        effects.last(),
        Some(Effect::Storage(StorageEffect::Write { key_space, .. })) if key_space == BLOB_HEAD_KEYSPACE
    ));
}

#[tokio::test]
async fn governed_copy_registered() {
    // A governed source copies like a plain write: gated, stored with the bucket default and
    // the source refs, and registered on this node.
    use crate::blob::managed_copy::{CopyRequest, validate_registration};
    use crate::driver::gate_context;
    use crate::s3::object::copy::test::{admits, full_context, seed_bucket};
    use crate::tests::policy::{seed_gate, subject};
    use aruna_core::keyspaces::MANAGED_COPY_KEYSPACE;
    use aruna_core::structs::storage::blob::ManagedCopyKey;

    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([3; 32]);
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let (source_policy, bucket_policy) = (admits(node_id, 1), admits(node_id, 2));
    let (source_ref, bucket_ref) = (source_policy.policy_ref(), bucket_policy.policy_ref());
    let policies = [source_policy, bucket_policy];
    seed_gate(
        &context,
        realm_id,
        user_id,
        subject(node_id, "eu"),
        &policies,
    )
    .await;
    let mut request = input(&sealed_location(), Ulid::generate());
    request.realm_id = realm_id;
    request.node_id = node_id;
    request.source_policies = vec![source_ref];
    seed_bucket(
        &context,
        "bucket",
        request.group_id,
        user_id,
        vec![bucket_ref],
    )
    .await;
    let source = BlobVersion::pending(
        ArchiveKey::of(&request.location),
        SystemTime::UNIX_EPOCH,
        UserId::default(),
        None,
    )
    .with_policies(vec![source_ref])
    .unwrap();
    seed(&context.storage_handle, request.source_version_id, &source).await;
    let gate = gate_context(&context, realm_id, 1_000)
        .await
        .unwrap()
        .unwrap();

    let operation = SealedCopyOperation::new(request.clone()).with_gate(gate.clone());
    let result = drive(operation, &context).await.unwrap();

    let version = copied(&context.storage_handle, result.version_id).await;
    let refs = PlacementPolicyRef::canonical_set(&[source_ref, bucket_ref]).unwrap();
    assert_eq!(version.placement_policies, refs);
    let key = ManagedCopyKey::new(
        VersionKey::new("bucket", "copy", result.version_id),
        request.location.backend.clone(),
    );
    let row = get(
        &context.storage_handle,
        MANAGED_COPY_KEYSPACE,
        key.to_bytes().unwrap(),
    )
    .await;
    let registered = validate_registration(
        row.as_deref(),
        &CopyRequest {
            key: &key,
            node_id: Some(node_id),
            blake3: None,
            refs: &refs,
            subject_generation: Some(gate.subject.generation),
        },
    );
    assert_eq!(registered.unwrap().location, request.location);

    // Without a destination gate a governed copy fails closed.
    let refused = drive(SealedCopyOperation::new(request), &context).await;
    assert!(matches!(refused, Err(SealedCopyError::PolicyGate(_))));
}
