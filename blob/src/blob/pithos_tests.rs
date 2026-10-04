//! Pithos copies through the reader: ranges, metadata digests, keys and changed blocks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{setup_two_backends, stream_from_bytes, test_user_id};
use crate::blob::pithos::{OBJECT_PATH, read};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::blob::ResolvedBackend;
use aruna_core::structs::storage::format::{PithosLayout, StoredLayout};
use futures::TryStreamExt;
use opendal::Operator;
use pithos_lib::archive::{
    AccessKeys, ArchivePath, CdcConfig, Chunking, EntryMetadata, PieceEncoder, ProcessingOptions,
    compose,
};
use pithos_lib::crypto::PrivateKey;
use std::time::Duration;

const IDLE: Duration = Duration::from_secs(30);

/// Seeded bytes that neither compress nor repeat, so FastCDC cuts many distinct blocks.
fn content() -> Vec<u8> {
    let mut seed = 7u64;
    (0..200_000)
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

async fn stored(archive: &[u8]) -> (tempfile::TempDir, Operator) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let operator = Operator::from_iter::<opendal::services::Fs>([("root".to_string(), root)])
        .unwrap()
        .finish();
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
    let data = content();
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
    let (archive, layout) = sealed_archive(&bucket, &content());
    let (_dir, operator) = stored(&archive).await;

    let other = PrivateKey::generate();
    let result = read_all(&operator, &layout, &other, 0..10).await;
    assert!(matches!(result, Err(BlobError::ReadError(_))));
}

#[tokio::test]
async fn rejects_changed_archives() {
    let bucket = PrivateKey::generate();
    let (mut archive, layout) = sealed_archive(&bucket, &content());

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
    location.format.layout = StoredLayout::Pithos(Box::new(PithosLayout {
        stored_size: 12,
        metadata_digest: [3; 32],
    }));

    let whole = handler.read_blob(location.clone()).await;
    assert!(matches!(whole, BlobEvent::Error(BlobError::ReadError(_))));
    let range = handler.read_blob_range(location.clone(), 0..4).await;
    assert!(matches!(range, BlobEvent::Error(BlobError::ReadError(_))));
    let slice = handler.slice_reader(&location).await;
    assert!(matches!(slice, Err(BlobError::ReadError(_))));
}
