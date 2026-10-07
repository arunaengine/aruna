//! Envelopes of same-bucket copies: own envelope when unlocked, pending rows when locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::abe::copies::{CopyEnvelopeOperation, CopyOutcome};
use crate::abe::envelope::EnvelopeOperation;
use crate::s3::object::put::abe::envelope_write;
use aruna_core::compute::SecretBytes;
use aruna_core::keyspaces::{ABE_EPOCH_KEYSPACE, ABE_PARAMETERS_KEYSPACE};
use aruna_core::structs::storage::abe::{
    EnvelopeArchive, EnvelopePlan, copy_envelope, create_envelope, create_parameters,
};
use aruna_core::structs::storage::encryption::public_key_of;

struct Sealed {
    secret: SecretBytes,
    public: [u8; 32],
    location: BackendLocation,
}

async fn set_epoch(storage: &StorageHandle, location: &BackendLocation, epoch: u64) {
    let key = location.format.bucket_key().unwrap();
    let bucket = key.bucket_id.to_bytes().to_vec();
    put(
        storage,
        ABE_EPOCH_KEYSPACE,
        bucket,
        epoch.to_be_bytes().to_vec(),
    )
    .await;
}

/// Admits parameters at epoch 1 and seeds the source version with its complete envelope.
async fn sealed(storage: &StorageHandle) -> (Sealed, Ulid, ObjectEnvelope) {
    let location = sealed_location();
    let key = location.format.bucket_key().unwrap();
    let secret = SecretBytes::new(vec![9; 32]);
    let public = public_key_of(&secret).unwrap();
    let base = input(&location, Ulid::nil());
    let parameters = create_parameters(&secret, base.realm_id, base.node_id, key).unwrap();
    put(
        storage,
        ABE_PARAMETERS_KEYSPACE,
        key.key(),
        parameters.to_bytes().unwrap(),
    )
    .await;
    set_epoch(storage, &location, 1).await;
    let info = BucketInfo {
        group_id: base.group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
    };
    put(
        storage,
        S3_BUCKET_KEYSPACE,
        b"bucket".to_vec(),
        info.to_bytes().unwrap(),
    )
    .await;
    let source_id = Ulid::generate();
    let version = BlobVersion::pending(
        ArchiveKey::of(&location),
        SystemTime::UNIX_EPOCH,
        UserId::default(),
        None,
    );
    seed(storage, source_id, &version).await;
    let (envelope, _) = create_envelope(EnvelopePlan {
        parameters,
        epoch: 1,
        write_id: Ulid::generate(),
        object_key: SOURCE.to_string(),
        bucket_public: public,
    })
    .unwrap();
    let source = VersionKey::new("bucket", SOURCE, source_id);
    let rows = (&HashMap::new(), u64::MAX, None);
    let (Effect::Storage(effect), _) = envelope_write(&envelope, &source, &location, rows).unwrap()
    else {
        panic!("expected a storage write")
    };
    storage.send_storage_effect(effect).await;
    let sealed = Sealed {
        secret,
        public,
        location,
    };
    (sealed, source_id, envelope)
}

/// Runs storage effects on `storage` and answers copy envelopes as an unlocked or locked node.
/// `raise` raises the epoch while the envelope is made, before the publishing transaction.
async fn run<O: Operation>(
    mut operation: O,
    storage: &StorageHandle,
    unlocked: Option<&Sealed>,
    raise: bool,
) -> Result<O::Output, O::Error> {
    let mut effects: Vec<Effect> = operation.start().into_iter().collect();
    while !operation.is_complete() {
        let effect = effects.remove(0);
        let event = match effect {
            Effect::Storage(effect) => storage.send_storage_effect(effect).await,
            Effect::Blob(BlobEffect::Abe(effect)) => {
                let AbeEffect::Copy {
                    source,
                    epoch,
                    write_id,
                    object_key,
                } = *effect
                else {
                    panic!("unexpected ABE effect")
                };
                if raise && let Some(sealed) = unlocked {
                    set_epoch(storage, &sealed.location, epoch + 1).await;
                }
                Event::Blob(match unlocked {
                    Some(sealed) => {
                        let plan = EnvelopePlan {
                            parameters: source.context.parameters.clone(),
                            epoch,
                            write_id,
                            object_key,
                            bucket_public: sealed.public,
                        };
                        let envelope = copy_envelope(&source, &sealed.secret, plan).unwrap();
                        BlobEvent::Abe(Box::new(AbeEvent::Envelope(envelope)))
                    }
                    None => BlobEvent::Error(AbeError::Required.into()),
                })
            }
            effect => panic!("unexpected effect {effect:?}"),
        };
        effects.extend(operation.step(event));
    }
    operation.finalize()
}

fn copy_input(sealed: &Sealed, from: (&str, Ulid), dest: &str) -> SealedCopyInput {
    SealedCopyInput {
        source_key: from.0.to_string(),
        source_version_id: from.1,
        dest_key: dest.to_string(),
        ..input(&sealed.location, from.1)
    }
}

async fn envelope_of(
    storage: &StorageHandle,
    key: &str,
    version_id: Ulid,
) -> Result<(ObjectEnvelope, EnvelopeArchive), AbeError> {
    let operation = EnvelopeOperation::new("bucket".to_string(), key.to_string(), version_id);
    run(operation, storage, None, false).await
}

async fn pending_row(storage: &StorageHandle, key: &str, version_id: Ulid) -> Option<Vec<u8>> {
    let version = VersionKey::new("bucket", key, version_id);
    get(storage, ABE_COPY_KEYSPACE, version.to_bytes().unwrap()).await
}

async fn complete(
    storage: &StorageHandle,
    key: &str,
    version_id: Ulid,
    unlocked: Option<&Sealed>,
    raise: bool,
) -> Result<CopyOutcome, AbeError> {
    let row = pending_row(storage, key, version_id).await.unwrap();
    let pending = PendingCopy::from_bytes(&row).unwrap();
    let version = VersionKey::new("bucket", key, version_id);
    let operation = CopyEnvelopeOperation::new(version, row, pending);
    run(operation, storage, unlocked, raise).await
}

#[tokio::test]
async fn unlocked_copy_envelope() {
    // An unlocked copy gets its own envelope for its own path around the same object key.
    let (_temp, context) = context();
    let storage = &context.storage_handle;
    let (sealed, source_id, source) = sealed(storage).await;
    let copy = copy_input(&sealed, (SOURCE, source_id), "other/copy");
    let result = run(
        SealedCopyOperation::new(copy),
        storage,
        Some(&sealed),
        false,
    )
    .await
    .unwrap();
    let (envelope, archive) = envelope_of(storage, "other/copy", result.version_id)
        .await
        .unwrap();
    assert_eq!(envelope.context.object_key, "other/copy");
    assert_eq!(envelope.context.public_key, source.context.public_key);
    assert_ne!(envelope.context.write_id, source.context.write_id);
    assert_eq!(envelope.context.epoch, 1);
    assert_eq!(archive.archive, ArchiveKey::of(&sealed.location));
    assert!(
        pending_row(storage, "other/copy", result.version_id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn locked_copy_pending() {
    // A locked copy is pending; a locked completion keeps it, the next unlock envelopes it.
    let (_temp, context) = context();
    let storage = &context.storage_handle;
    let (sealed, source_id, source) = sealed(storage).await;
    let copy = copy_input(&sealed, (SOURCE, source_id), "copy");
    let result = run(SealedCopyOperation::new(copy), storage, None, false)
        .await
        .unwrap();
    let version_id = result.version_id;
    assert_eq!(
        envelope_of(storage, "copy", version_id).await,
        Err(AbeError::Pending)
    );
    let outcome = complete(storage, "copy", version_id, None, false).await;
    assert_eq!(outcome, Ok(CopyOutcome::Locked));
    let outcome = complete(storage, "copy", version_id, Some(&sealed), false).await;
    assert_eq!(outcome, Ok(CopyOutcome::Completed));
    let (envelope, _) = envelope_of(storage, "copy", version_id).await.unwrap();
    assert_eq!(envelope.context.object_key, "copy");
    assert_eq!(envelope.context.public_key, source.context.public_key);
    assert!(pending_row(storage, "copy", version_id).await.is_none());
}

#[tokio::test]
async fn pending_chain_completes() {
    // A to B to C while locked, A and B deleted: C still carries A's complete envelope.
    let (_temp, context) = context();
    let storage = &context.storage_handle;
    let (sealed, a, source) = sealed(storage).await;
    let copy = copy_input(&sealed, (SOURCE, a), "b");
    let b = run(SealedCopyOperation::new(copy), storage, None, false)
        .await
        .unwrap()
        .version_id;
    let copy = copy_input(&sealed, ("b", b), "c");
    let c = run(SealedCopyOperation::new(copy), storage, None, false)
        .await
        .unwrap()
        .version_id;
    let row = PendingCopy::from_bytes(&pending_row(storage, "c", c).await.unwrap()).unwrap();
    assert_eq!(row.source, source);
    let a_key = VersionKey::new("bucket", SOURCE, a).to_bytes().unwrap();
    let b_key = VersionKey::new("bucket", "b", b).to_bytes().unwrap();
    let id = source.context.write_id.to_bytes().to_vec();
    let deletes = vec![
        (BLOB_VERSIONS_KEYSPACE.to_string(), a_key.clone().into()),
        (ABE_VERSION_KEYSPACE.to_string(), a_key.into()),
        (ABE_ENVELOPE_KEYSPACE.to_string(), id.clone().into()),
        (
            aruna_core::keyspaces::ABE_ARCHIVE_KEYSPACE.to_string(),
            id.into(),
        ),
        (BLOB_VERSIONS_KEYSPACE.to_string(), b_key.clone().into()),
        (ABE_COPY_KEYSPACE.to_string(), b_key.into()),
    ];
    let effect = StorageEffect::BatchDelete {
        deletes,
        txn_id: None,
    };
    storage.send_storage_effect(effect).await;
    let outcome = complete(storage, "c", c, Some(&sealed), false).await;
    assert_eq!(outcome, Ok(CopyOutcome::Completed));
    let (envelope, _) = envelope_of(storage, "c", c).await.unwrap();
    assert_eq!(envelope.context.object_key, "c");
}

#[tokio::test]
async fn stale_epoch_refused() {
    // A raise between making the envelope and publishing it refuses the copy and the completion.
    let (_temp, context) = context();
    let storage = &context.storage_handle;
    let (sealed, source_id, _) = sealed(storage).await;
    let copy = copy_input(&sealed, (SOURCE, source_id), "copy");
    let refused = run(SealedCopyOperation::new(copy), storage, Some(&sealed), true).await;
    assert_eq!(
        refused,
        Err(SealedCopyError::Blob(BlobError::Abe(AbeError::Epoch)))
    );
    let head = BlobHeadKey::new("bucket", "copy").to_bytes().unwrap();
    assert!(get(storage, BLOB_HEAD_KEYSPACE, head).await.is_none());
    let copy = copy_input(&sealed, (SOURCE, source_id), "copy");
    let version_id = run(SealedCopyOperation::new(copy), storage, None, false)
        .await
        .unwrap()
        .version_id;
    let outcome = complete(storage, "copy", version_id, Some(&sealed), true).await;
    assert_eq!(outcome, Err(AbeError::Epoch));
    assert!(pending_row(storage, "copy", version_id).await.is_some());
}
