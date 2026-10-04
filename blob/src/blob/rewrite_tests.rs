//! Copies rewritten inside the adapter: sealed with a public key, opened under a lease, and
//! granted to a new key without new blocks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{setup_two_backends, stream_from_bytes, test_user_id};
use crate::blob::BlobHandler;
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::effects::BlobEffect;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::blob::{ArchiveKey, BackendLocation, ResolvedBackend};
use aruna_core::structs::storage::encryption::{
    BlockCipher, BlockKeys, BucketKeyError, BucketKeyRef, ReadLease, SealPlan, public_key_of,
};
use aruna_core::structs::storage::format::{StoredFormat, StoredLayout};
use futures::TryStreamExt;
use pithos_lib::archive::AccessKeys;
use pithos_lib::crypto::PrivateKey;
use std::time::Duration;
use ulid::Ulid;
use zeroize::Zeroizing;

const IDLE: Duration = Duration::from_secs(30);

/// A bucket key generation: its reference, raw private bytes and public key.
fn bucket_key(bucket_id: Ulid, generation: u64, seed: u8) -> (BucketKeyRef, [u8; 32], [u8; 32]) {
    let private = [seed; 32];
    let public = public_key_of(&SecretBytes::new(private.to_vec())).unwrap();
    (BucketKeyRef::new(bucket_id, generation), private, public)
}

fn sealing(key: BucketKeyRef, public_key: [u8; 32]) -> ResolvedBackend {
    let plan = SealPlan {
        key,
        public_key,
        cipher: BlockCipher::default(),
        block_keys: BlockKeys::default(),
        storage_generation: 1,
    };
    ResolvedBackend::node_default().with_encryption(Some(plan))
}

/// Unlocks `key` in the registry and admits a read of the archive at `location`.
async fn admitted(
    handler: &BlobHandler,
    key: BucketKeyRef,
    private: [u8; 32],
    public_key: [u8; 32],
    location: &BackendLocation,
) -> ReadLease {
    let prepare = BlobEffect::PrepareKey {
        key,
        public_key,
        private_key: SharedSecret::new(SecretBytes::new(private.to_vec())),
        duration: None,
        max: None,
    };
    let BlobEvent::KeyPrepared { ticket } = handler.unlock_effect(prepare) else {
        panic!("prepare failed")
    };
    let activated = handler.unlock_effect(BlobEffect::ActivateKey { ticket });
    assert!(matches!(activated, BlobEvent::KeyActivated { .. }));
    let archive = ArchiveKey::of(location);
    match handler.admit_read(key, archive).await {
        BlobEvent::ReadAdmitted { lease } => lease,
        other => panic!("admission failed: {other:?}"),
    }
}

/// Reads a sealed copy with the raw private key `private`.
async fn opened(
    handler: &BlobHandler,
    location: &BackendLocation,
    private: [u8; 32],
) -> Result<Vec<u8>, BlobError> {
    let StoredLayout::Pithos(layout) = &location.format.layout else {
        panic!("not a sealed copy: {location:?}")
    };
    let operator = handler.operator_from_location(location)?;
    let path = location.get_storage_path()?;
    let keys = AccessKeys::new().with_key(PrivateKey::from_raw(Zeroizing::new(private)));
    let range = 0..location.blob_size;
    let stream = crate::blob::pithos::read(operator, path, layout, keys, range, IDLE).await?;
    let chunks: Vec<_> = stream.try_collect().await?;
    Ok(chunks.concat())
}

async fn plain(handler: &BlobHandler, data: &[u8]) -> BackendLocation {
    let stream = stream_from_bytes(data);
    let backend = ResolvedBackend::node_default();
    let written = handler
        .write_blob("bucket", "object", backend, test_user_id(), stream)
        .await;
    let BlobEvent::WriteFinished { location } = written else {
        panic!("write failed: {written:?}")
    };
    location
}

async fn rewritten(
    handler: &BlobHandler,
    source: BackendLocation,
    lease: Option<ReadLease>,
    target: ResolvedBackend,
    grants_only: bool,
) -> BlobEvent {
    let rewrite = handler.rewrite_copy("bucket", "object", source, lease, target, grants_only);
    Box::pin(rewrite).await
}

#[tokio::test]
async fn seals_plain_copies() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = b"plain bytes for a sealed copy".repeat(1000);
    let source = plain(&handler, &data).await;
    let (key, private, public) = bucket_key(Ulid::generate(), 1, 3);

    let event = rewritten(&handler, source.clone(), None, sealing(key, public), false).await;

    let BlobEvent::CopyRewritten { location } = event else {
        panic!("rewrite failed: {event:?}")
    };
    assert_eq!(location.format.bucket_key(), Some(key));
    assert_eq!(location.get_blake3(), source.get_blake3());
    assert_eq!(location.blob_size, source.blob_size);
    assert_eq!(opened(&handler, &location, private).await.unwrap(), data);
}

#[tokio::test]
async fn opens_with_lease() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = b"sealed bytes made plain again".repeat(1000);
    let (key, private, public) = bucket_key(Ulid::generate(), 1, 4);
    let source = plain(&handler, &data).await;
    let event = rewritten(&handler, source, None, sealing(key, public), false).await;
    let BlobEvent::CopyRewritten { location: sealed } = event else {
        panic!("sealing failed: {event:?}")
    };

    // Without an admitted lease the sealed copy stays closed.
    let target = ResolvedBackend::node_default();
    let refused = rewritten(&handler, sealed.clone(), None, target.clone(), false).await;
    assert!(matches!(
        refused,
        BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))
    ));

    let lease = admitted(&handler, key, private, public, &sealed).await;
    let event = rewritten(&handler, sealed, Some(lease), target, false).await;

    let BlobEvent::CopyRewritten { location } = event else {
        panic!("rewrite failed: {event:?}")
    };
    assert_eq!(location.format, StoredFormat::default());
    let BlobEvent::ReadFinished { blob, .. } = handler.read_blob(location).await else {
        panic!("plain read failed")
    };
    let chunks: Vec<_> = blob.0.try_collect().await.unwrap();
    assert_eq!(chunks.concat(), data);
}

#[tokio::test]
async fn replaces_archive_grants() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = b"blocks stay, grants change".repeat(2000);
    let bucket_id = Ulid::generate();
    let (old, old_private, old_public) = bucket_key(bucket_id, 1, 5);
    let (new, new_private, new_public) = bucket_key(bucket_id, 2, 6);
    let source = plain(&handler, &data).await;
    let event = rewritten(&handler, source, None, sealing(old, old_public), false).await;
    let BlobEvent::CopyRewritten { location: sealed } = event else {
        panic!("sealing failed: {event:?}")
    };
    let lease = admitted(&handler, old, old_private, old_public, &sealed).await;

    let event = rewritten(
        &handler,
        sealed.clone(),
        Some(lease),
        sealing(new, new_public),
        true,
    );
    let BlobEvent::CopyRewritten { location } = event.await else {
        panic!("grant replacement failed")
    };

    assert_eq!(location.format.bucket_key(), Some(new));
    assert_ne!(location.backend_path, sealed.backend_path);
    assert_eq!(location.get_blake3(), sealed.get_blake3());
    assert_eq!(
        opened(&handler, &location, new_private).await.unwrap(),
        data
    );
    assert!(opened(&handler, &location, old_private).await.is_err());
    // The old archive is untouched until its copy is reclaimed.
    assert_eq!(opened(&handler, &sealed, old_private).await.unwrap(), data);
}
