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
    let rewrite = handler.rewrite_copy(
        "bucket",
        "object",
        source,
        lease,
        target,
        (grants_only, None),
    );
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
async fn reencodes_archive_format() {
    use pithos_lib::archive::{Archive, OpenOptions};
    use pithos_lib::source::MemorySource;
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = b"reencoded archive format".repeat(2000);
    for (cipher, block_keys) in [
        (BlockCipher::Aes256Gcm, BlockKeys::ContentDerived),
        (BlockCipher::ChaCha20Poly1305, BlockKeys::Unique),
        (BlockCipher::Aes256Gcm, BlockKeys::Unique),
    ] {
        let bucket_id = Ulid::generate();
        let (old, old_private, old_public) = bucket_key(bucket_id, 1, 5);
        let (new, new_private, new_public) = bucket_key(bucket_id, 2, 6);
        let source = plain(&handler, &data).await;
        let event = rewritten(&handler, source, None, sealing(old, old_public), false).await;
        let BlobEvent::CopyRewritten { location: sealed } = event else {
            panic!("sealing failed")
        };
        let lease = admitted(&handler, old, old_private, old_public, &sealed).await;
        let mut target = sealing(new, new_public);
        let plan = target.encryption.as_mut().unwrap();
        plan.cipher = cipher;
        plan.block_keys = block_keys;
        plan.storage_generation = 2;
        let event = rewritten(&handler, sealed, Some(lease), target, false).await;
        let BlobEvent::CopyRewritten { location } = event else {
            panic!("reencoding failed")
        };
        let operator = handler.operator_from_location(&location).unwrap();
        let bytes = operator
            .read(&location.get_storage_path().unwrap())
            .await
            .unwrap()
            .to_vec();
        let keys = AccessKeys::new().with_key(PrivateKey::from_raw(Zeroizing::new(new_private)));
        let archive = Archive::open(
            MemorySource::new(bytes),
            OpenOptions::default().with_access_keys(keys),
        )
        .unwrap();
        let blocks = archive
            .view()
            .plan_range(crate::blob::pithos::OBJECT_PATH, 0..location.blob_size)
            .unwrap();
        let mut count = 0;
        for block in blocks {
            // Pithos exposes decoded processing flags through the planned block's Debug output.
            let block = format!("{:?}", block.unwrap());
            assert!(block.contains(&format!(
                "aes_256_gcm: {}",
                cipher == BlockCipher::Aes256Gcm
            )));
            assert!(block.contains(&format!("unique_key: {}", block_keys == BlockKeys::Unique)));
            count += 1;
        }
        assert!(count > 0);
        assert_eq!(location.format.bucket_key(), Some(new));
        let StoredLayout::Pithos(layout) = &location.format.layout else {
            panic!("expected Pithos")
        };
        assert_eq!(layout.storage_generation, 2);
        assert_eq!(
            opened(&handler, &location, new_private).await.unwrap(),
            data
        );
        assert!(opened(&handler, &location, old_private).await.is_err());
    }
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

    let mut target = sealing(new, new_public);
    target.encryption.as_mut().unwrap().storage_generation = 2;
    let event = rewritten(&handler, sealed.clone(), Some(lease), target, true);
    let BlobEvent::CopyRewritten { location } = event.await else {
        panic!("grant replacement failed")
    };

    assert_eq!(location.format.bucket_key(), Some(new));
    let StoredLayout::Pithos(layout) = &location.format.layout else {
        panic!("expected Pithos")
    };
    assert_eq!(layout.storage_generation, 2);
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

#[tokio::test]
async fn retired_generations_free() {
    // Two settled rotations without restart: generation 3 needs the slot of retired generation 1.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let bucket_id = Ulid::generate();
    let (first, first_private, first_public) = bucket_key(bucket_id, 1, 7);
    let (second, second_private, second_public) = bucket_key(bucket_id, 2, 8);
    let (third, third_private, third_public) = bucket_key(bucket_id, 3, 9);
    let source = plain(&handler, b"retired generation").await;
    let event = rewritten(&handler, source, None, sealing(first, first_public), false).await;
    let BlobEvent::CopyRewritten { location: sealed } = event else {
        panic!("sealing failed: {event:?}")
    };
    let lease = admitted(&handler, first, first_private, first_public, &sealed).await;
    admitted(&handler, second, second_private, second_public, &sealed).await;
    let prepare = |key, private: [u8; 32], public_key| BlobEffect::PrepareKey {
        key,
        public_key,
        private_key: SharedSecret::new(SecretBytes::new(private.to_vec())),
        duration: None,
        max: None,
    };
    let full = handler.unlock_effect(prepare(third, third_private, third_public));
    assert!(matches!(
        full,
        BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Capacity))
    ));

    let BlobEvent::KeyStatus { generations } =
        handler.unlock_effect(BlobEffect::ReadKeyStatus { bucket_id })
    else {
        panic!("no key status")
    };
    for status in generations.iter().filter(|status| status.key == first) {
        let ticket = aruna_core::structs::storage::encryption::KeyTicket {
            key: first,
            session_id: status.session_id,
        };
        handler.unlock_effect(BlobEffect::DiscardKey { ticket });
    }

    let prepared = handler.unlock_effect(prepare(third, third_private, third_public));
    assert!(matches!(prepared, BlobEvent::KeyPrepared { .. }));
    // The admitted read of generation 1 still finishes with its own key.
    let target = ResolvedBackend::node_default();
    let event = rewritten(&handler, sealed, Some(lease), target, false).await;
    assert!(
        matches!(event, BlobEvent::CopyRewritten { .. }),
        "{event:?}"
    );
}

/// A connection of the handler to itself: its sending end and its receiving end.
async fn loopback(handler: &BlobHandler) -> (Ulid, Ulid) {
    let (sender, mut streams) = tokio::sync::mpsc::unbounded_channel();
    let capture = std::sync::Arc::new(super::StreamCapture(sender));
    handler.net.set_inbound_handler(capture);
    let local = handler.net.node_id();
    let BlobEvent::ConnectionEstablished { stream_id } = handler.open_connection(local).await
    else {
        panic!("no connection to the local node")
    };
    let accepted = tokio::time::timeout(IDLE, streams.recv()).await;
    let (_, inbound, peer) = accepted.unwrap().unwrap();
    let inbound_id = handler.add_connection(None, peer, inbound).await.unwrap();
    (stream_id, inbound_id)
}

/// Receives one replica on `inbound` for a destination that seals with `target`.
fn receiving(
    handler: &BlobHandler,
    inbound: Ulid,
    target: ResolvedBackend,
) -> tokio::task::JoinHandle<BlobEvent> {
    let handler = handler.clone();
    tokio::spawn(async move {
        let received = handler.handle_incoming_replication(None, inbound, (target, None), true);
        Box::pin(received).await
    })
}

/// A sealed copy of `data` under a source key, and an admitted lease for it.
async fn leased_source(
    handler: &BlobHandler,
    data: &[u8],
) -> (BackendLocation, ReadLease, [u8; 32]) {
    let (key, private, public) = bucket_key(Ulid::generate(), 1, 5);
    let source = plain(handler, data).await;
    let event = rewritten(handler, source, None, sealing(key, public), false).await;
    let BlobEvent::CopyRewritten { location } = event else {
        panic!("sealing failed: {event:?}")
    };
    let lease = admitted(handler, key, private, public, &location).await;
    (location, lease, private)
}

#[tokio::test]
async fn granted_copy_transfers() {
    // The source grants its archive to the target key; the target keeps the bytes as sent.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = b"granted on the way to another node".repeat(2000);
    let (sealed, lease, source_private) = leased_source(&handler, &data).await;
    let (target_key, target_private, target_public) = bucket_key(Ulid::generate(), 1, 6);
    let target = sealing(target_key, target_public);
    let plan = target.encryption.unwrap();
    let (sending, inbound) = loopback(&handler).await;

    let received = receiving(&handler, inbound, target);
    let ids = (Ulid::generate(), sending);
    let sent = handler
        .replicate_leased(ids, sealed, lease, Some((plan, None)))
        .await;
    assert!(
        matches!(sent, BlobEvent::ReplicationFinished { .. }),
        "{sent:?}"
    );

    let received = received.await.unwrap();
    let BlobEvent::ReplicationFinished { location } = received else {
        panic!("receive failed: {received:?}")
    };
    assert_eq!(location.format.bucket_key(), Some(target_key));
    assert!(location.hashes.is_empty());
    assert_eq!(location.blob_size, data.len() as u64);
    assert_eq!(
        opened(&handler, &location, target_private).await.unwrap(),
        data
    );
    assert!(opened(&handler, &location, source_private).await.is_err());
}

#[tokio::test]
async fn object_key_transfers() {
    // The target archive opens with the target's bucket and object keys, never the source's.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = b"granted to the target object key".repeat(2000);
    let (key, private, public) = bucket_key(Ulid::generate(), 1, 5);
    let (_, source_object, source_public) = bucket_key(Ulid::generate(), 1, 8);
    let source = plain(&handler, &data).await;
    let target = sealing(key, public);
    let grants = (false, Some(source_public));
    let rewrite = handler.rewrite_copy("bucket", "object", source, None, target, grants);
    let event = Box::pin(rewrite).await;
    let BlobEvent::CopyRewritten { location: sealed } = event else {
        panic!("sealing failed: {event:?}")
    };
    assert_eq!(
        opened(&handler, &sealed, source_object).await.unwrap(),
        data
    );
    let lease = admitted(&handler, key, private, public, &sealed).await;
    let (target_key, target_private, target_public) = bucket_key(Ulid::generate(), 1, 6);
    let (_, target_object, object_public) = bucket_key(Ulid::generate(), 1, 9);
    let target = sealing(target_key, target_public);
    let plan = target.encryption.unwrap();
    let (sending, inbound) = loopback(&handler).await;

    let received = receiving(&handler, inbound, target);
    let ids = (Ulid::generate(), sending);
    let regrant = Some((plan, Some(object_public)));
    let sent = handler.replicate_leased(ids, sealed, lease, regrant).await;
    assert!(
        matches!(sent, BlobEvent::ReplicationFinished { .. }),
        "{sent:?}"
    );
    let received = received.await.unwrap();
    let BlobEvent::ReplicationFinished { location } = received else {
        panic!("receive failed: {received:?}")
    };
    for allowed in [target_private, target_object] {
        assert_eq!(opened(&handler, &location, allowed).await.unwrap(), data);
    }
    for refused in [private, source_object] {
        assert!(opened(&handler, &location, refused).await.is_err());
    }
}

#[tokio::test]
async fn plaintext_object_sealed() {
    // Plaintext received for a target object key is sealed to it as well.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = b"plain source, target object key".repeat(2000);
    let source = plain(&handler, &data).await;
    let (key, private, public) = bucket_key(Ulid::generate(), 1, 7);
    let (_, object, object_public) = bucket_key(Ulid::generate(), 1, 10);
    let (sending, inbound) = loopback(&handler).await;

    let target = (sealing(key, public), Some(object_public));
    let receiver = handler.clone();
    let received = tokio::spawn(async move {
        Box::pin(receiver.handle_incoming_replication(None, inbound, target, true)).await
    });
    let sent = handler
        .replicate_blob(Ulid::generate(), sending, source, true)
        .await;
    assert!(
        matches!(sent, BlobEvent::ReplicationFinished { .. }),
        "{sent:?}"
    );
    let BlobEvent::ReplicationFinished { location } = received.await.unwrap() else {
        panic!("receive failed")
    };
    for allowed in [private, object] {
        assert_eq!(opened(&handler, &location, allowed).await.unwrap(), data);
    }
}

#[tokio::test]
async fn tampered_transfer_fails() {
    // Archive bytes changed in transit do not match the announced tree and are not kept.
    use crate::bao_tree::SendStreamWrapper;
    use crate::blob::control_plane::{read_replication_message, send_replication_message};
    use crate::messages::{MessageType, ReplicationMessage};
    use bao_tree::ByteRanges;
    use bao_tree::io::fsm::{CreateOutboard, encode_ranges_validated};
    use bao_tree::io::outboard::PreOrderOutboard;
    use bao_tree::io::round_up_to_chunks;
    use iroh_io::AsyncSliceReader;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = b"tampered while it travels".repeat(2000);
    let (sealed, lease, _) = leased_source(&handler, &data).await;
    let (target_key, _, target_public) = bucket_key(Ulid::generate(), 1, 6);
    let target = sealing(target_key, target_public);
    let plan = target.encryption.unwrap();
    let (mut reader, sent) = handler
        .regrant_reader(&sealed, lease, (&plan, None))
        .await
        .unwrap();
    let size = sent.stored_size();
    let genuine = reader.read_exact_at(0, size as usize).await.unwrap();
    let block = crate::blob::BAO_BLOCK_SIZE;
    let root = PreOrderOutboard::<bytes::BytesMut>::create(&mut genuine.clone(), block)
        .await
        .unwrap()
        .root;
    let mut changed = genuine.to_vec();
    changed[size as usize / 2] ^= 1;
    let changed = bytes::Bytes::from(changed);
    let mut forged = PreOrderOutboard::<bytes::BytesMut>::create(&mut changed.clone(), block)
        .await
        .unwrap();
    let (sending, inbound) = loopback(&handler).await;
    let received = receiving(&handler, inbound, target);

    // The genuine root is announced; the changed bytes follow with a tree of their own.
    let stream = handler.connection_handle(sending).await.unwrap();
    let mut stream = stream.lock().await;
    let id = Ulid::generate();
    let msg_type = MessageType::BaoTreeInfo {
        location: sent,
        root,
    };
    let init = ReplicationMessage { id, msg_type };
    send_replication_message(&mut stream.0, init, IDLE, "init")
        .await
        .unwrap();
    read_replication_message(&mut stream.1, IDLE, "ack")
        .await
        .unwrap();
    let ranges = round_up_to_chunks(&ByteRanges::from(0..size));
    let mut sender = SendStreamWrapper::new(&mut stream.0, IDLE);
    let _ = encode_ranges_validated(changed, &mut forged, &ranges, &mut sender).await;
    _ = stream.0.finish();
    drop(stream);

    let received = received.await.unwrap();
    // A cleanup error still rejects the copy and hands its location to later cleanup.
    assert!(
        matches!(
            received,
            BlobEvent::Error(
                BlobError::ReplicationFailed(_)
                    | BlobError::IntegrityCheckFailed(_)
                    | BlobError::WriteCleanup { .. }
            )
        ),
        "{received:?}"
    );
}

#[tokio::test]
async fn sealed_on_receipt() {
    // A plain source sends plaintext; an encrypting target seals it with its own plan.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = b"plain source, sealed target".repeat(2000);
    let source = plain(&handler, &data).await;
    let (key, private, public) = bucket_key(Ulid::generate(), 1, 7);
    let (sending, inbound) = loopback(&handler).await;

    let received = receiving(&handler, inbound, sealing(key, public));
    let sent = handler
        .replicate_blob(Ulid::generate(), sending, source.clone(), true)
        .await;
    assert!(
        matches!(sent, BlobEvent::ReplicationFinished { .. }),
        "{sent:?}"
    );

    let received = received.await.unwrap();
    let BlobEvent::ReplicationFinished { location } = received else {
        panic!("receive failed: {received:?}")
    };
    assert_eq!(location.format.bucket_key(), Some(key));
    assert_eq!(location.get_blake3(), source.get_blake3());
    assert_eq!(opened(&handler, &location, private).await.unwrap(), data);
}
