//! Pithos copies through the writer and reader, and bucket keys through the blob adapter.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{TestContext, failing_close, setup_two_backends, stream_from_bytes, test_user_id};
use crate::blob::pithos::{OBJECT_PATH, read};
use crate::hash::Hasher;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::BackendStream;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, ResolvedBackend};
use aruna_core::structs::storage::encryption::{BlockCipher, BlockKeys, BucketKeyRef, SealPlan};
use aruna_core::structs::storage::format::{Compression, PithosLayout, StoredFormat, StoredLayout};
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
    read_at(operator, "object.pith", layout, key, range).await
}

async fn read_at(
    operator: &Operator,
    path: &str,
    layout: &PithosLayout,
    key: &PrivateKey,
    range: std::ops::Range<u64>,
) -> Result<Vec<u8>, BlobError> {
    let keys = AccessKeys::new().with_key(key.duplicate());
    let path = path.to_string();
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

/// A seal plan for `bucket` with the given cipher and key mode.
fn plan(bucket: &PrivateKey, cipher: BlockCipher, block_keys: BlockKeys) -> SealPlan {
    SealPlan {
        key: BucketKeyRef::new(ulid::Ulid::from_bytes([4; 16]), 1),
        public_key: *bucket.public_key().as_bytes(),
        cipher,
        block_keys,
        storage_generation: 1,
    }
}

/// Writes `data` through `write_blob` and checks the size, the hashes and the stored format.
async fn written(
    data: &[u8],
    seal: SealPlan,
    compression: Compression,
) -> (TestContext, Operator, String, PithosLayout) {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let backend = ResolvedBackend::node_default()
        .with_compression(compression)
        .with_encryption(Some(seal));
    let stream = stream_from_bytes(data);
    let written = handler
        .write_blob("bucket", "sealed.bin", backend, test_user_id(), stream)
        .await;
    let BlobEvent::WriteFinished { location } = written else {
        panic!("write failed: {written:?}")
    };
    assert_eq!(location.blob_size, data.len() as u64);
    assert_eq!(location.hashes, Hasher::new_with_bytes(data).to_map());
    assert_eq!(location.format.bucket_key(), Some(seal.key));
    let StoredLayout::Pithos(layout) = location.format.layout.clone() else {
        panic!("not a Pithos copy: {:?}", location.format)
    };
    let operator = handler.operator_from_location(&location).unwrap();
    let path = location.get_storage_path().unwrap();
    let stat = operator.stat(&path).await.unwrap();
    assert_eq!(stat.content_length(), layout.stored_size);
    (context, operator, path, *layout)
}

#[tokio::test]
async fn writes_read_back() {
    let bucket = PrivateKey::generate();
    // Pithos probes the first 4 KiB of a block, so the compressible text comes first.
    let data = [b"aruna pithos ".repeat(16_000), content(200_000)].concat();
    let zstd = Compression::Zstd { level: 3 };
    let seal = plan(
        &bucket,
        BlockCipher::ChaCha20Poly1305,
        BlockKeys::ContentDerived,
    );
    let (_context, operator, path, layout) = written(&data, seal, zstd).await;
    assert!(layout.stored_size < data.len() as u64);

    let size = data.len() as u64;
    for range in [
        0..size,
        0..1,
        4_000..70_000,
        199_990..210_000,
        size - 1..size,
        5..5,
    ] {
        let expected = &data[range.start as usize..range.end as usize];
        let actual = read_at(&operator, &path, &layout, &bucket, range.clone()).await;
        assert_eq!(actual.unwrap(), expected, "{range:?}");
    }
}

#[tokio::test]
async fn writes_selected_cipher() {
    let bucket = PrivateKey::generate();
    let data = content(300_000);
    let seal = plan(&bucket, BlockCipher::Aes256Gcm, BlockKeys::Unique);
    let (_context, operator, path, layout) = written(&data, seal, Compression::Off).await;
    let size = data.len() as u64;
    let whole = read_at(&operator, &path, &layout, &bucket, 0..size).await;
    assert!(whole.unwrap() == data);
}

#[tokio::test]
async fn writes_empty_object() {
    let bucket = PrivateKey::generate();
    let seal = plan(&bucket, BlockCipher::ChaCha20Poly1305, BlockKeys::Unique);
    let (_context, operator, path, layout) = written(b"", seal, Compression::Off).await;
    let actual = read_at(&operator, &path, &layout, &bucket, 0..0).await;
    assert!(actual.unwrap().is_empty());
}

#[tokio::test]
async fn writes_large_object() {
    // Larger than the 16 MiB FastCDC maximum, so the piece holds several blocks.
    let bucket = PrivateKey::generate();
    let data = content(40 * MIB);
    let seal = plan(
        &bucket,
        BlockCipher::ChaCha20Poly1305,
        BlockKeys::ContentDerived,
    );
    let (_context, operator, path, layout) = written(&data, seal, Compression::Off).await;

    let size = data.len() as u64;
    let whole = read_at(&operator, &path, &layout, &bucket, 0..size).await;
    assert!(whole.unwrap() == data);
    let range = (15 * MIB) as u64..(33 * MIB) as u64;
    let part = read_at(&operator, &path, &layout, &bucket, range).await;
    assert!(part.unwrap() == data[15 * MIB..33 * MIB]);
}

#[tokio::test]
async fn aborts_failed_writes() {
    let context = setup_two_backends().await;
    let handler = &context.blob_handle.handler;
    let seal = plan(
        &PrivateKey::generate(),
        BlockCipher::ChaCha20Poly1305,
        BlockKeys::ContentDerived,
    );
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "sealed-bucket".to_string(),
        backend_path: format!("obj/{}", ulid::Ulid::generate()),
        ulid: ulid::Ulid::generate(),
        format: StoredFormat::default(),
        created_by: test_user_id(),
        created_at: std::time::SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 0,
        hashes: std::collections::HashMap::new(),
    };

    // An uncertain close keeps the reservation and names the Pithos copy for cleanup.
    let (operator, aborts) = failing_close::operator_with_aborts();
    let stream = stream_from_bytes(b"payload");
    let closed = handler
        .write_encoded(
            location.clone(),
            operator,
            stream,
            Compression::Off,
            (Some(seal), None),
            None,
        )
        .await;
    let BlobEvent::Error(BlobError::WriteCleanup { location: kept, .. }) = closed else {
        panic!("close failure must keep the location, got {closed:?}")
    };
    assert_eq!(kept.ulid, location.ulid);
    assert_eq!(kept.format.bucket_key(), Some(seal.key));
    assert_eq!(aborts.load(std::sync::atomic::Ordering::SeqCst), 1);

    // Filesystems cannot abort writers, so the partial object with its first block is deleted.
    let (_dir, operator) = empty_store();
    let path = location.get_storage_path().unwrap();
    let chunks = [
        Ok(Bytes::from(content(17 * MIB))),
        Err(std::io::Error::other("gone")),
    ];
    let stream = BackendStream::new(futures::stream::iter(chunks));
    let failed = handler
        .write_encoded(
            location,
            operator.clone(),
            stream,
            Compression::Off,
            (Some(seal), None),
            None,
        )
        .await;
    assert!(matches!(
        failed,
        BlobEvent::Error(BlobError::StreamFailed(_))
    ));
    assert!(!operator.exists(&path).await.unwrap());
}

#[tokio::test]
async fn rejects_other_shapes() {
    use pithos_lib::archive::{Archive, ArchiveWriter, OpenOptions, WriteOptions};
    use pithos_lib::source::MemorySource;

    let bucket = PrivateKey::generate();
    let options = WriteOptions::new(PrivateKey::generate(), vec![bucket.public_key()]);
    let mut writer = ArchiveWriter::create(Vec::new(), options).unwrap();
    let processing = ProcessingOptions::new(true, 0).unwrap();
    for name in [OBJECT_PATH, "other"] {
        let path = ArchivePath::new(name).unwrap();
        let metadata = EntryMetadata::new(0, 0, 0o644);
        writer
            .add_file(path, metadata, processing, None, &b"data"[..])
            .unwrap();
    }
    let archive = writer.finish().unwrap();
    let opened = Archive::open(MemorySource::new(archive.clone()), OpenOptions::default());
    let layout = PithosLayout {
        stored_size: archive.len() as u64,
        metadata_digest: opened.unwrap().metadata_digest(),
    };
    let (_dir, operator) = stored(&archive).await;
    let refused = read_all(&operator, &layout, &bucket, 0..4).await;
    assert!(matches!(refused, Err(BlobError::IntegrityCheckFailed(_))));
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
    let admitted = handler.admit_read(key, ArchiveKey::of(&location)).await;
    let BlobEvent::ReadAdmitted { lease } = admitted else {
        panic!("admission failed")
    };

    // Locking stops new reads, but the admitted read still pins its archive.
    let lock = BlobEffect::LockKey {
        bucket_id: key.bucket_id,
        session: None,
    };
    assert!(
        matches!(handler.unlock_effect(lock), BlobEvent::KeyLocked { locked, .. } if locked.len() == 1)
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
async fn adapter_seals_keys() {
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

#[tokio::test]
async fn seals_with_unlocked() {
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::key_seal::{SealedSecret, open_sealed};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::encryption::{
        BucketKeyError, CopyTarget, copy_info, public_key_of,
    };

    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let key = BucketKeyRef::new(ulid::Ulid::generate(), 1);
    let realm_id = RealmId::from_bytes([1; 32]);
    let node_id = handler.net.node_id();
    let holder = SecretBytes::new(vec![6; 32]);
    let target = CopyTarget {
        user_id: test_user_id(),
        key_record: ulid::Ulid::generate(),
        key_id: "slot".to_string(),
        public_key: public_key_of(&holder).unwrap(),
    };
    let seal = || handler.seal_unlocked(key, (realm_id, node_id), std::slice::from_ref(&target));
    // A locked generation seals nothing, so the grant stays pending.
    let locked = BlobError::BucketKey(BucketKeyError::Locked(key.bucket_id));
    assert_eq!(seal(), BlobEvent::Error(locked));

    let bucket_key = SecretBytes::new(vec![5; 32]);
    let prepare = BlobEffect::PrepareKey {
        key,
        public_key: public_key_of(&bucket_key).unwrap(),
        private_key: SharedSecret::new(SecretBytes::new(vec![5; 32])),
        duration: None,
        max: None,
    };
    let BlobEvent::KeyPrepared { ticket } = handler.unlock_effect(prepare) else {
        panic!("prepare failed")
    };
    handler.unlock_effect(BlobEffect::ActivateKey { ticket });
    let BlobEvent::CopiesSealed { copies } = seal() else {
        panic!("an unlocked key seals")
    };
    let info = copy_info(realm_id, node_id, key, target.user_id, target.key_record);
    let sealed = SealedSecret {
        enc: copies[0].enc,
        ciphertext: copies[0].ciphertext.clone(),
    };
    let opened = open_sealed(&[6; 32], &sealed, &info, &[]).unwrap();
    assert_eq!(opened.as_slice(), bucket_key.expose());
}

#[tokio::test]
async fn reads_with_lease() {
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::structs::storage::blob::ArchiveKey;

    let bucket = PrivateKey::from_raw(zeroize::Zeroizing::new([5; 32]));
    let seal = plan(
        &bucket,
        BlockCipher::ChaCha20Poly1305,
        BlockKeys::ContentDerived,
    );
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = content(300_000);
    let backend = ResolvedBackend::node_default().with_encryption(Some(seal));
    let stream = stream_from_bytes(&data);
    let written = handler
        .write_blob("bucket", "sealed.bin", backend, test_user_id(), stream)
        .await;
    let BlobEvent::WriteFinished { location } = written else {
        panic!("write failed: {written:?}")
    };
    let prepare = BlobEffect::PrepareKey {
        key: seal.key,
        public_key: seal.public_key,
        private_key: SharedSecret::new(SecretBytes::new(vec![5; 32])),
        duration: None,
        max: None,
    };
    let BlobEvent::KeyPrepared { ticket } = handler.unlock_effect(prepare) else {
        panic!("prepare failed")
    };
    handler.unlock_effect(BlobEffect::ActivateKey { ticket });
    let archive = ArchiveKey::of(&location);
    let collect = |event: BlobEvent| async move {
        let BlobEvent::ReadFinished { blob, stream_size } = event else {
            panic!("read failed: {event:?}")
        };
        let chunks: Vec<Bytes> = blob.try_collect().await.unwrap();
        (chunks.concat(), stream_size)
    };
    let BlobEvent::ReadAdmitted { lease } = handler.admit_read(seal.key, archive.clone()).await
    else {
        panic!("admission failed")
    };
    let whole = handler.read_sealed(location.clone(), None, lease).await;
    assert_eq!(collect(whole).await, (data.clone(), data.len() as u64));
    let BlobEvent::ReadAdmitted { lease } = handler.admit_read(seal.key, archive).await else {
        panic!("admission failed")
    };
    let part = handler
        .read_sealed(location.clone(), Some(1_000..9_000), lease)
        .await;
    assert_eq!(collect(part).await, (data[1_000..9_000].to_vec(), 8_000));

    // A lease of another archive never opens this copy.
    let other = ArchiveKey::new(ulid::Ulid::generate(), location.backend.clone());
    let BlobEvent::ReadAdmitted { lease } = handler.admit_read(seal.key, other).await else {
        panic!("admission failed")
    };
    let refused = handler.read_sealed(location, None, lease).await;
    assert!(matches!(refused, BlobEvent::Error(BlobError::BucketKey(_))));
}

#[tokio::test]
async fn reconcile_claims_archives() {
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::blob::ArchiveKey;

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
    // An abandoned archive without owners or hash, as an interrupted sealed write leaves it.
    location.hashes.remove(HASH_BLAKE3);
    let layout = PithosLayout {
        stored_size: 12,
        metadata_digest: [3; 32],
    };
    location.format = StoredFormat::pithos(layout, BucketKeyRef::new(ulid::Ulid::generate(), 1));
    handler.finalize_reservation(&location).await.unwrap();
    handler.clear_active(location.ulid);
    let operator = handler.operator_from_location(&location).unwrap();
    let path = location.get_storage_path().unwrap();

    // A pinned archive is kept; the reservation stays for a later pass.
    let pin = handler
        .unlocks
        .lock()
        .unwrap()
        .pin(ArchiveKey::of(&location))
        .unwrap();
    let refused = handler.reconcile_reservation(location.clone()).await;
    assert!(matches!(refused, Err(BlobError::DeleteError(_))));
    assert!(operator.exists(&path).await.unwrap());
    drop(pin);
    assert!(handler.reconcile_reservation(location).await.unwrap());
    assert!(!operator.exists(&path).await.unwrap());
}

#[tokio::test]
async fn lease_outlives_lock() {
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::structs::storage::blob::ArchiveKey;
    use aruna_core::structs::storage::encryption::BucketKeyError;

    let bucket = PrivateKey::from_raw(zeroize::Zeroizing::new([5; 32]));
    let seal = plan(
        &bucket,
        BlockCipher::ChaCha20Poly1305,
        BlockKeys::ContentDerived,
    );
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = content(300_000);
    let backend = ResolvedBackend::node_default().with_encryption(Some(seal));
    let stream = stream_from_bytes(&data);
    let written = handler
        .write_blob("bucket", "sealed.bin", backend, test_user_id(), stream)
        .await;
    let BlobEvent::WriteFinished { location } = written else {
        panic!("write failed: {written:?}")
    };
    let prepare = BlobEffect::PrepareKey {
        key: seal.key,
        public_key: seal.public_key,
        private_key: SharedSecret::new(SecretBytes::new(vec![5; 32])),
        duration: None,
        max: None,
    };
    let BlobEvent::KeyPrepared { ticket } = handler.unlock_effect(prepare) else {
        panic!("prepare failed")
    };
    handler.unlock_effect(BlobEffect::ActivateKey { ticket });
    let archive = ArchiveKey::of(&location);
    let BlobEvent::ReadAdmitted { lease } = handler.admit_read(seal.key, archive.clone()).await
    else {
        panic!("admission failed")
    };

    // The lock lands after admission; the admitted read still finishes with its own key.
    let lock = BlobEffect::LockKey {
        bucket_id: seal.key.bucket_id,
        session: None,
    };
    assert!(matches!(
        handler.unlock_effect(lock),
        BlobEvent::KeyLocked { .. }
    ));
    let BlobEvent::ReadFinished { blob, .. } = handler.read_sealed(location, None, lease).await
    else {
        panic!("the admitted read must finish")
    };
    let chunks: Vec<Bytes> = blob.try_collect().await.unwrap();
    assert!(chunks.concat() == data);

    let refused = handler.admit_read(seal.key, archive).await;
    let locked = BlobError::BucketKey(BucketKeyError::Locked(seal.key.bucket_id));
    assert_eq!(refused, BlobEvent::Error(locked));
}

#[tokio::test]
async fn declared_size_chunks() {
    use crate::blob::io::compose_chunk;
    use aruna_core::structs::storage::blob::Backend;

    let context = setup_two_backends().await;
    let handler = &context.blob_handle.handler;
    let seal = plan(
        &PrivateKey::generate(),
        BlockCipher::ChaCha20Poly1305,
        BlockKeys::ContentDerived,
    );
    // 150 GiB on a filesystem backend needs chunks of 16 MiB to stay within 10,000 parts.
    let declared = 150 * 1024u64.pow(3);
    let chunk = compose_chunk(&Backend::FileSystem, declared, true).unwrap();
    assert_eq!(chunk, 16 * MIB);
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "sealed-bucket".to_string(),
        backend_path: format!("obj/{}", ulid::Ulid::generate()),
        ulid: ulid::Ulid::generate(),
        format: StoredFormat::default(),
        created_by: test_user_id(),
        created_at: std::time::SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 0,
        hashes: std::collections::HashMap::new(),
    };
    let (operator, sizes) = failing_close::operator_with_sizes();
    let data = content(40 * MIB);
    handler
        .write_encoded(
            location,
            operator,
            stream_from_bytes(&data),
            Compression::Off,
            (Some(seal), None),
            Some(declared),
        )
        .await;

    let sizes = sizes.lock().unwrap();
    let (last, full) = sizes.split_last().unwrap();
    assert!(
        full.len() >= 2 && full.iter().all(|size| *size == chunk),
        "{sizes:?}"
    );
    assert!(*last <= chunk, "{sizes:?}");
}

#[tokio::test]
async fn serves_leased_plaintext() {
    use crate::blob::BAO_BLOCK_SIZE;
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::structs::storage::blob::ArchiveKey;
    use bao_tree::io::fsm::CreateOutboard;
    use bao_tree::io::outboard::PreOrderOutboard;
    use iroh_io::AsyncSliceReader;

    let bucket = PrivateKey::from_raw(zeroize::Zeroizing::new([5; 32]));
    let seal = plan(
        &bucket,
        BlockCipher::ChaCha20Poly1305,
        BlockKeys::ContentDerived,
    );
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = content(300_000);
    let backend = ResolvedBackend::node_default().with_encryption(Some(seal));
    let written = handler
        .write_blob(
            "bucket",
            "sealed.bin",
            backend,
            test_user_id(),
            stream_from_bytes(&data),
        )
        .await;
    let BlobEvent::WriteFinished { location } = written else {
        panic!("write failed: {written:?}")
    };

    // Replication never carries a sealed copy, unlocked or not.
    let replicated = handler
        .replicate_blob(
            ulid::Ulid::generate(),
            ulid::Ulid::generate(),
            location.clone(),
            false,
        )
        .await;
    assert!(matches!(
        replicated,
        BlobEvent::Error(BlobError::ReadError(_))
    ));

    let prepare = BlobEffect::PrepareKey {
        key: seal.key,
        public_key: seal.public_key,
        private_key: SharedSecret::new(SecretBytes::new(vec![5; 32])),
        duration: None,
        max: None,
    };
    let BlobEvent::KeyPrepared { ticket } = handler.unlock_effect(prepare) else {
        panic!("prepare failed")
    };
    handler.unlock_effect(BlobEffect::ActivateKey { ticket });
    let BlobEvent::ReadAdmitted { lease } = handler
        .admit_read(seal.key, ArchiveKey::of(&location))
        .await
    else {
        panic!("admission failed")
    };

    // The authorized reader gets the plaintext, and its bao root is the content address.
    let mut reader = handler.sealed_reader(&location, lease).await.unwrap();
    assert_eq!(reader.size().await.unwrap(), data.len() as u64);
    let part = reader.read_exact_at(1_000, 8_000).await.unwrap();
    assert!(part == data[1_000..9_000]);
    let outboard = PreOrderOutboard::<bytes::BytesMut>::create(&mut reader, BAO_BLOCK_SIZE)
        .await
        .unwrap();
    assert_eq!(outboard.root, blake3::hash(&data));
}

#[tokio::test]
async fn reads_reserve_budget() {
    use crate::blob::pithos::{budget_permits, working_set};
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::structs::storage::blob::ArchiveKey;
    use futures::FutureExt;

    let bucket = PrivateKey::from_raw(zeroize::Zeroizing::new([5; 32]));
    let seal = plan(
        &bucket,
        BlockCipher::ChaCha20Poly1305,
        BlockKeys::ContentDerived,
    );
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = content(100_000);
    let backend = ResolvedBackend::node_default().with_encryption(Some(seal));
    let written = handler
        .write_blob(
            "bucket",
            "sealed.bin",
            backend,
            test_user_id(),
            stream_from_bytes(&data),
        )
        .await;
    let BlobEvent::WriteFinished { location } = written else {
        panic!("write failed: {written:?}")
    };
    // The finished write returned its whole share.
    assert_eq!(handler.pithos_budget.available_permits(), budget_permits());
    let prepare = BlobEffect::PrepareKey {
        key: seal.key,
        public_key: seal.public_key,
        private_key: SharedSecret::new(SecretBytes::new(vec![5; 32])),
        duration: None,
        max: None,
    };
    let BlobEvent::KeyPrepared { ticket } = handler.unlock_effect(prepare) else {
        panic!("prepare failed")
    };
    handler.unlock_effect(BlobEffect::ActivateKey { ticket });
    let admit = || handler.admit_read(seal.key, ArchiveKey::of(&location));

    // A saturated budget holds a new open back until room is free again.
    let held = handler
        .pithos_budget
        .clone()
        .acquire_many_owned(budget_permits() as u32)
        .await
        .unwrap();
    let BlobEvent::ReadAdmitted { lease } = admit().await else {
        panic!("admission failed")
    };
    let mut waiting = Box::pin(handler.read_sealed(location.clone(), None, lease));
    assert!((&mut waiting).now_or_never().is_none());
    drop(held);
    let BlobEvent::ReadFinished { blob, .. } = waiting.await else {
        panic!("the read must start once the budget has room")
    };

    // The open keeps its share until the stream ends.
    let share = working_set(location.blob_size).div_ceil(1 << 20) as usize;
    assert_eq!(
        handler.pithos_budget.available_permits(),
        budget_permits() - share
    );
    let chunks: Vec<Bytes> = blob.try_collect().await.unwrap();
    assert!(chunks.concat() == data);
    assert_eq!(handler.pithos_budget.available_permits(), budget_permits());
}

#[tokio::test]
async fn rewrites_never_deadlock() {
    use crate::blob::pithos::{budget_permits, working_set};
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::structs::storage::blob::ArchiveKey;

    let source_key = PrivateKey::from_raw(zeroize::Zeroizing::new([5; 32]));
    let source = plan(
        &source_key,
        BlockCipher::ChaCha20Poly1305,
        BlockKeys::ContentDerived,
    );
    let mut target = plan(
        &PrivateKey::generate(),
        BlockCipher::Aes256Gcm,
        BlockKeys::Unique,
    );
    target.key = BucketKeyRef::new(ulid::Ulid::from_bytes([6; 16]), 2);
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let prepare = BlobEffect::PrepareKey {
        key: source.key,
        public_key: source.public_key,
        private_key: SharedSecret::new(SecretBytes::new(vec![5; 32])),
        duration: None,
        max: None,
    };
    let BlobEvent::KeyPrepared { ticket } = handler.unlock_effect(prepare) else {
        panic!("prepare failed")
    };
    handler.unlock_effect(BlobEffect::ActivateKey { ticket });

    let data = content(100_000);
    let mut copies = Vec::new();
    for index in 0..4 {
        let backend = ResolvedBackend::node_default().with_encryption(Some(source));
        let name = format!("sealed-{index}.bin");
        let written = handler
            .write_blob(
                "bucket",
                &name,
                backend,
                test_user_id(),
                stream_from_bytes(&data),
            )
            .await;
        let BlobEvent::WriteFinished { location } = written else {
            panic!("write failed: {written:?}")
        };
        copies.push(location);
    }

    // Only one rewrite's combined share is free, so they must run one after another.
    let share = (2 * working_set(data.len() as u64)).div_ceil(1 << 20) as usize;
    let held = handler
        .pithos_budget
        .clone()
        .acquire_many_owned((budget_permits() - share - 1) as u32)
        .await
        .unwrap();
    let mut rewrites = Vec::new();
    for location in copies {
        let BlobEvent::ReadAdmitted { lease } = handler
            .admit_read(source.key, ArchiveKey::of(&location))
            .await
        else {
            panic!("admission failed")
        };
        let handler = handler.clone();
        let target = ResolvedBackend::node_default().with_encryption(Some(target));
        rewrites.push(tokio::spawn(async move {
            handler
                .rewrite_copy(
                    "bucket",
                    "rewritten.bin",
                    location,
                    Some(lease),
                    target,
                    false,
                )
                .await
        }));
    }
    // A generous cap that only a hang reaches.
    let finished = tokio::time::timeout(Duration::from_secs(300), async {
        let mut events = Vec::new();
        for rewrite in rewrites {
            events.push(rewrite.await.unwrap());
        }
        events
    })
    .await
    .expect("saturated rewrites must not deadlock");
    for event in finished {
        let BlobEvent::CopyRewritten { location } = event else {
            panic!("rewrite failed: {event:?}")
        };
        assert_eq!(location.format.bucket_key(), Some(target.key));
    }
    drop(held);
    assert_eq!(handler.pithos_budget.available_permits(), budget_permits());
}

#[tokio::test]
async fn framed_reads_pin_copies() {
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::structs::storage::blob::ArchiveKey;
    use aruna_core::structs::storage::encryption::public_key_of;
    use futures::StreamExt;

    // A framed copy of an encrypting bucket, not converted yet.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = content(3 * MIB);
    let backend = ResolvedBackend::node_default().with_compression(Compression::Zstd { level: 3 });
    let written = handler
        .write_blob(
            "bucket",
            "framed.bin",
            backend,
            test_user_id(),
            stream_from_bytes(&data),
        )
        .await;
    let BlobEvent::WriteFinished { location } = written else {
        panic!("write failed: {written:?}")
    };
    assert!(matches!(location.format.layout, StoredLayout::Frames(_)));
    let key = BucketKeyRef::new(ulid::Ulid::generate(), 1);
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
    handler.unlock_effect(BlobEffect::ActivateKey { ticket });
    let BlobEvent::ReadAdmitted { lease } =
        handler.admit_read(key, ArchiveKey::of(&location)).await
    else {
        panic!("admission failed")
    };
    let BlobEvent::ReadFinished { mut blob, .. } = handler.read_blob(location.clone()).await else {
        panic!("the framed read must start")
    };
    let first = blob.next().await.unwrap().unwrap();

    // Conversion and reclaim delete through the same path; the admitted read keeps the copy.
    let refused = handler.delete_blob(location.clone()).await;
    assert!(matches!(
        refused,
        BlobEvent::Error(BlobError::DeleteError(_))
    ));
    let mut received = first.to_vec();
    while let Some(chunk) = blob.next().await {
        received.extend_from_slice(&chunk.unwrap());
    }
    assert!(received == data);
    drop(blob);
    drop(lease);
    assert_eq!(
        handler.delete_blob(location).await,
        BlobEvent::DeleteFinished
    );
}

#[tokio::test]
async fn over_budget_refused() {
    use crate::blob::pithos::{WORKING_SET, budget_permits, working_set};

    // A 5 TiB working set exceeds the node budget: refused, never clipped.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let refused = handler.reserve_pithos(working_set(5 << 40)).await;
    assert_eq!(
        refused.err(),
        Some(BlobError::SizeLimitExceeded { limit: WORKING_SET })
    );
    assert_eq!(handler.pithos_budget.available_permits(), budget_permits());
}

#[tokio::test]
async fn oversized_seal_refused() {
    use crate::blob::pithos::MAX_SIZE;

    // A sealed copy larger than every copy that can be re-encoded is refused before writing.
    let context = setup_two_backends().await;
    let handler = &context.blob_handle.handler;
    let seal = plan(
        &PrivateKey::generate(),
        BlockCipher::ChaCha20Poly1305,
        BlockKeys::ContentDerived,
    );
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "sealed-bucket".to_string(),
        backend_path: format!("obj/{}", ulid::Ulid::generate()),
        ulid: ulid::Ulid::generate(),
        format: StoredFormat::default(),
        created_by: test_user_id(),
        created_at: std::time::SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 0,
        hashes: std::collections::HashMap::new(),
    };
    let (_dir, operator) = empty_store();
    let refused = handler
        .write_encoded(
            location,
            operator,
            stream_from_bytes(b"data"),
            Compression::Off,
            (Some(seal), None),
            Some(MAX_SIZE + 1),
        )
        .await;
    assert_eq!(
        refused,
        BlobEvent::Error(BlobError::SizeLimitExceeded { limit: MAX_SIZE })
    );
}
