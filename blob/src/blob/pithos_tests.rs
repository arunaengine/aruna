//! Pithos copies through the writer and reader, and bucket keys through the blob adapter.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{failing_close, setup_two_backends, stream_from_bytes, test_user_id};
use crate::blob::pithos::{OBJECT_PATH, PithosWrite, read};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::BackendStream;
use aruna_core::structs::storage::blob::ResolvedBackend;
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::structs::storage::format::{Compression, PithosLayout, StoredFormat};
use bytes::Bytes;
use futures::TryStreamExt;
use opendal::Operator;
use pithos_lib::archive::{
    AccessKeys, ArchivePath, CdcConfig, Chunking, EntryMetadata, PieceEncoder, ProcessingOptions,
    compose,
};
use pithos_lib::crypto::PrivateKey;
use std::time::Duration;

const IDLE: Duration = Duration::from_secs(30);
const MIB: usize = 1 << 20;

/// Seeded bytes that neither compress nor repeat, so FastCDC cuts many distinct blocks.
fn content(len: usize) -> Vec<u8> {
    let mut seed = 7u64;
    (0..len)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect()
}

/// One sealed piece granted to `bucket`, composed into an archive.
fn sealed_archive(bucket: &PrivateKey, data: &[u8]) -> (Vec<u8>, PithosLayout) {
    let chunking = Chunking::ContentDefined(CdcConfig::new(1024, 4096, 16_384).unwrap());
    let processing = ProcessingOptions::new(true, 3).unwrap();
    let mut encoder = PieceEncoder::new(1, vec![bucket.public_key()], processing)
        .unwrap()
        .with_chunking(chunking)
        .unwrap();
    let mut stored = encoder.write(data).unwrap();
    stored.extend(encoder.flush().unwrap());
    let piece = encoder.finish().unwrap();
    let path = ArchivePath::new(OBJECT_PATH).unwrap();
    let composition = compose(path, EntryMetadata::new(0, 0, 0o644), &[piece]).unwrap();
    let archive = [
        composition.header().as_slice(),
        &stored,
        composition.directory(),
    ]
    .concat();
    let layout = PithosLayout {
        stored_size: archive.len() as u64,
        metadata_digest: composition.metadata_digest(),
    };
    (archive, layout)
}

fn empty_store() -> (tempfile::TempDir, Operator) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let operator = Operator::from_iter::<opendal::services::Fs>([("root".to_string(), root)])
        .unwrap()
        .finish();
    (dir, operator)
}

async fn stored(archive: &[u8]) -> (tempfile::TempDir, Operator) {
    let (dir, operator) = empty_store();
    operator
        .write("object.pith", archive.to_vec())
        .await
        .unwrap();
    (dir, operator)
}

async fn read_all(
    operator: &Operator,
    layout: &PithosLayout,
    key: &PrivateKey,
    range: std::ops::Range<u64>,
) -> Result<Vec<u8>, BlobError> {
    let keys = AccessKeys::new().with_key(key.duplicate());
    let path = "object.pith".to_string();
    let stream = read(operator.clone(), path, layout, keys, range, IDLE).await?;
    let chunks: Vec<_> = stream.try_collect().await?;
    Ok(chunks.concat())
}

#[tokio::test]
async fn reads_whole_ranges() {
    let bucket = PrivateKey::generate();
    let data = content(200_000);
    let (archive, layout) = sealed_archive(&bucket, &data);
    let (_dir, operator) = stored(&archive).await;

    let size = data.len() as u64;
    assert_eq!(
        read_all(&operator, &layout, &bucket, 0..size)
            .await
            .unwrap(),
        data
    );
    for range in [0..1, 4_000..70_000, 199_990..size, 5..5] {
        let expected = &data[range.start as usize..range.end as usize];
        let actual = read_all(&operator, &layout, &bucket, range.clone()).await;
        assert_eq!(actual.unwrap(), expected, "{range:?}");
    }
    let outside = read_all(&operator, &layout, &bucket, 0..size + 1).await;
    assert!(matches!(outside, Err(BlobError::ReadError(_))));
}

#[tokio::test]
async fn needs_granted_key() {
    let bucket = PrivateKey::generate();
    let (archive, layout) = sealed_archive(&bucket, &content(200_000));
    let (_dir, operator) = stored(&archive).await;

    let other = PrivateKey::generate();
    let result = read_all(&operator, &layout, &other, 0..10).await;
    assert!(matches!(result, Err(BlobError::ReadError(_))));
}

#[tokio::test]
async fn rejects_changed_archives() {
    let bucket = PrivateKey::generate();
    let (mut archive, layout) = sealed_archive(&bucket, &content(200_000));

    let (_dir, operator) = stored(&archive).await;
    let mut other = layout.clone();
    other.metadata_digest[0] ^= 1;
    let digest = read_all(&operator, &other, &bucket, 0..10).await;
    assert!(matches!(digest, Err(BlobError::IntegrityCheckFailed(_))));

    // The first block starts after the six-byte header and its four-byte marker.
    archive[20] ^= 1;
    let (_changed, operator) = stored(&archive).await;
    let block = read_all(&operator, &layout, &bucket, 0..10).await;
    assert!(matches!(block, Err(BlobError::IntegrityCheckFailed(_))));
}

#[tokio::test]
async fn refuses_unkeyed_reads() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let stream = stream_from_bytes(b"sealed bytes");
    let backend = ResolvedBackend::node_default();
    let written = handler
        .write_blob("bucket", "sealed.bin", backend, test_user_id(), stream)
        .await;
    let BlobEvent::WriteFinished { mut location } = written else {
        panic!("write failed: {written:?}")
    };
    let layout = PithosLayout {
        stored_size: 12,
        metadata_digest: [3; 32],
    };
    let key = BucketKeyRef::new(ulid::Ulid::from_bytes([4; 16]), 1);
    location.format = StoredFormat::pithos(layout, key);

    let whole = handler.read_blob(location.clone()).await;
    assert!(matches!(whole, BlobEvent::Error(BlobError::ReadError(_))));
    let range = handler.read_blob_range(location.clone(), 0..4).await;
    assert!(matches!(range, BlobEvent::Error(BlobError::ReadError(_))));
    let slice = handler.slice_reader(&location).await;
    assert!(matches!(slice, Err(BlobError::ReadError(_))));
}

/// Writes `data` into a fresh store and checks the reported size, hash and stored size.
async fn written(
    data: &[u8],
    key: &PrivateKey,
    compression: Compression,
) -> (tempfile::TempDir, Operator, PithosWrite) {
    let context = setup_two_backends().await;
    let (dir, operator) = empty_store();
    let stream = stream_from_bytes(data);
    let write = context
        .blob_handle
        .handler
        .write_pithos(
            &operator,
            "object.pith",
            key.public_key(),
            compression,
            stream,
        )
        .await
        .unwrap();
    assert_eq!(write.size, data.len() as u64);
    assert_eq!(write.content_hash, *blake3::hash(data).as_bytes());
    let stat = operator.stat("object.pith").await.unwrap();
    assert_eq!(stat.content_length(), write.layout.stored_size);
    (dir, operator, write)
}

#[tokio::test]
async fn writes_read_back() {
    let bucket = PrivateKey::generate();
    // Pithos probes the first 4 KiB of a block, so the compressible text comes first.
    let data = [b"aruna pithos ".repeat(16_000), content(200_000)].concat();
    let zstd = Compression::Zstd { level: 3 };
    let (_dir, operator, write) = written(&data, &bucket, zstd).await;
    assert!(write.layout.stored_size < write.size);

    let size = write.size;
    for range in [
        0..size,
        0..1,
        4_000..70_000,
        199_990..210_000,
        size - 1..size,
        5..5,
    ] {
        let expected = &data[range.start as usize..range.end as usize];
        let actual = read_all(&operator, &write.layout, &bucket, range.clone()).await;
        assert_eq!(actual.unwrap(), expected, "{range:?}");
    }
}

#[tokio::test]
async fn writes_empty_object() {
    let bucket = PrivateKey::generate();
    let (_dir, operator, write) = written(b"", &bucket, Compression::Off).await;
    let actual = read_all(&operator, &write.layout, &bucket, 0..0).await;
    assert!(actual.unwrap().is_empty());
}

#[tokio::test]
async fn writes_large_object() {
    // Larger than the 16 MiB FastCDC maximum, so the piece holds several blocks.
    let bucket = PrivateKey::generate();
    let data = content(40 * MIB);
    let (_dir, operator, write) = written(&data, &bucket, Compression::Off).await;

    let whole = read_all(&operator, &write.layout, &bucket, 0..write.size).await;
    assert!(whole.unwrap() == data);
    let range = (15 * MIB) as u64..(33 * MIB) as u64;
    let part = read_all(&operator, &write.layout, &bucket, range).await;
    assert!(part.unwrap() == data[15 * MIB..33 * MIB]);
}

#[tokio::test]
async fn aborts_failed_writes() {
    let context = setup_two_backends().await;
    let handler = &context.blob_handle.handler;
    let key = PrivateKey::generate().public_key();

    let (operator, aborts) = failing_close::operator_with_aborts();
    let stream = stream_from_bytes(b"payload");
    let closed = handler
        .write_pithos(&operator, "object.pith", key, Compression::Off, stream)
        .await;
    assert!(matches!(
        closed,
        Err(BlobError::WriteError(message)) if message.contains("injected finalization failure")
    ));
    assert_eq!(aborts.load(std::sync::atomic::Ordering::SeqCst), 1);

    // Filesystems cannot abort writers, so the partial object with its first block is deleted.
    let (_dir, operator) = empty_store();
    let chunks = [
        Ok(Bytes::from(content(17 * MIB))),
        Err(std::io::Error::other("gone")),
    ];
    let stream = BackendStream::new(futures::stream::iter(chunks));
    let failed = handler
        .write_pithos(&operator, "object.pith", key, Compression::Off, stream)
        .await;
    assert!(matches!(failed, Err(BlobError::StreamFailed(_))));
    assert!(!operator.exists("object.pith").await.unwrap());
}

#[tokio::test]
async fn reservations_keep_pending() {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::PENDING_LOCATION_KEYSPACE;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::blob::ArchiveKey;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    for owned in [true, false] {
        let stream = stream_from_bytes(b"sealed bytes");
        let backend = ResolvedBackend::node_default();
        let written = handler
            .write_blob("bucket", "sealed.bin", backend, test_user_id(), stream)
            .await;
        let BlobEvent::WriteFinished { mut location } = written else {
            panic!("write failed: {written:?}")
        };
        // The finalized marker of a pending archive names no content hash.
        location.hashes.remove(HASH_BLAKE3);
        let layout = PithosLayout {
            stored_size: 12,
            metadata_digest: [3; 32],
        };
        location.format =
            StoredFormat::pithos(layout, BucketKeyRef::new(ulid::Ulid::generate(), 1));
        handler.finalize_reservation(&location).await.unwrap();
        handler.clear_active(location.ulid);
        if owned {
            let event = context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: PENDING_LOCATION_KEYSPACE.to_string(),
                    key: ArchiveKey::of(&location).to_bytes().into(),
                    value: location.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
            assert!(matches!(
                event,
                Event::Storage(StorageEvent::WriteResult { .. })
            ));
        }

        assert!(
            handler
                .reconcile_reservation(location.clone())
                .await
                .unwrap()
        );
        assert!(!handler.marker_present(&location).await.unwrap());
        let operator = handler.operator_from_location(&location).unwrap();
        let path = location.get_storage_path().unwrap();
        assert_eq!(
            operator.exists(&path).await.unwrap(),
            owned,
            "owned: {owned}"
        );
    }
}

#[tokio::test]
async fn leases_pin_archives() {
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::structs::storage::blob::ArchiveKey;
    use aruna_core::structs::storage::encryption::public_key_of;

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let stream = stream_from_bytes(b"sealed bytes");
    let backend = ResolvedBackend::node_default();
    let written = handler
        .write_blob("bucket", "sealed.bin", backend, test_user_id(), stream)
        .await;
    let BlobEvent::WriteFinished { mut location } = written else {
        panic!("write failed: {written:?}")
    };
    let key = BucketKeyRef::new(ulid::Ulid::generate(), 1);
    let layout = PithosLayout {
        stored_size: 12,
        metadata_digest: [3; 32],
    };
    location.format = StoredFormat::pithos(layout, key);
    let prepare = BlobEffect::PrepareKey {
        key,
        public_key: public_key_of(&SecretBytes::new(vec![5; 32])).unwrap(),
        private_key: SharedSecret::new(SecretBytes::new(vec![5; 32])),
        duration: None,
        max: None,
    };
    let BlobEvent::KeyPrepared { ticket } = handler.unlock_effect(prepare) else {
        panic!("prepare failed")
    };
    let activated = handler.unlock_effect(BlobEffect::ActivateKey { ticket });
    assert!(matches!(activated, BlobEvent::KeyActivated { .. }));
    let admit = BlobEffect::AdmitRead {
        key,
        archive: ArchiveKey::of(&location),
    };
    let BlobEvent::ReadAdmitted { lease } = handler.unlock_effect(admit) else {
        panic!("admission failed")
    };

    // Locking stops new reads, but the admitted read still pins its archive.
    let lock = BlobEffect::LockKey {
        bucket_id: key.bucket_id,
        session: None,
    };
    assert!(
        matches!(handler.unlock_effect(lock), BlobEvent::KeyLocked { locked } if locked.len() == 1)
    );
    let refused = handler.delete_blob(location.clone()).await;
    assert!(matches!(
        refused,
        BlobEvent::Error(BlobError::DeleteError(_))
    ));
    drop(lease);
    assert_eq!(
        handler.delete_blob(location).await,
        BlobEvent::DeleteFinished
    );
}

#[tokio::test]
async fn keys_seal_through_adapter() {
    use aruna_core::compute::SecretBytes;
    use aruna_core::effects::BlobEffect;
    use aruna_core::events::Event;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::encryption::{CopyTarget, public_key_of};

    let context = setup_two_backends().await;
    let handle = &context.blob_handle;
    let Event::Blob(BlobEvent::BucketKeyGenerated {
        public_key,
        private_key,
    }) = handle.send_blob_effect(BlobEffect::GenerateBucketKey).await
    else {
        panic!("no key generated")
    };
    let holder = SecretBytes::new(vec![4; 32]);
    let target = CopyTarget {
        user_id: test_user_id(),
        key_record: ulid::Ulid::generate(),
        key_id: "slot".to_string(),
        public_key: public_key_of(&holder).unwrap(),
    };
    let seal = |public_key| BlobEffect::SealHolderCopies {
        key: BucketKeyRef::new(ulid::Ulid::generate(), 1),
        public_key,
        private_key: private_key.clone(),
        realm_id: RealmId::from_bytes([1; 32]),
        node_id: handle.handler.net.node_id(),
        holders: vec![target.clone()],
    };
    let sealed = handle.send_blob_effect(seal(public_key)).await;
    assert!(matches!(sealed, Event::Blob(BlobEvent::CopiesSealed { copies }) if copies.len() == 1));
    let wrong = handle.send_blob_effect(seal([9; 32])).await;
    assert!(matches!(
        wrong,
        Event::Blob(BlobEvent::Error(BlobError::BucketKey(_)))
    ));
}
