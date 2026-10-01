//! Compressed blobs through the handler: full reads, ranges and tampered frames.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{read_back, setup_two_backends, stream_from_bytes, test_user_id};
use crate::blob::BlobHandler;
use crate::codec::FRAME_SIZE;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::blob::{BackendLocation, ResolvedBackend};
use aruna_core::structs::storage::format::{Compression, StoredLayout};
use aruna_core::structs::storage::multipart::MultipartPartKey;
use futures::TryStreamExt;

/// Text frames, then seeded random frames that do not compress.
fn sample() -> Vec<u8> {
    let mut data: Vec<u8> = b"framed research data "
        .iter()
        .copied()
        .cycle()
        .take(5 * FRAME_SIZE as usize / 2)
        .collect();
    let mut seed = 11u64;
    data.extend((0..3 * FRAME_SIZE as usize / 2).map(|_| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed as u8
    }));
    data
}

fn zstd_backend() -> ResolvedBackend {
    ResolvedBackend::node_default().with_compression(Compression::Zstd { level: 3 })
}

async fn write(handler: &BlobHandler, data: &[u8]) -> BackendLocation {
    let event = handler
        .write_blob(
            "bucket",
            "framed.bin",
            zstd_backend(),
            test_user_id(),
            stream_from_bytes(data),
        )
        .await;
    let BlobEvent::WriteFinished { location } = event else {
        panic!("write failed: {event:?}")
    };
    location
}

async fn read_range(
    handler: &BlobHandler,
    location: BackendLocation,
    range: std::ops::Range<u64>,
) -> Result<Vec<u8>, BlobError> {
    let BlobEvent::ReadFinished { blob, stream_size } =
        handler.read_blob_range(location, range.clone()).await
    else {
        panic!("range read failed to start")
    };
    assert_eq!(stream_size, range.end - range.start);
    let chunks: Vec<bytes::Bytes> = blob
        .try_collect()
        .await
        .map_err(|error| BlobError::ReadError(error.to_string()))?;
    Ok(chunks.concat())
}

#[tokio::test]
async fn round_trips_compressed() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();

    for data in [sample(), b"tiny".to_vec(), Vec::new()] {
        let location = write(&handler, &data).await;

        assert!(matches!(location.format.layout, StoredLayout::Frames(_)));
        assert_eq!(location.blob_size, data.len() as u64);
        assert_eq!(
            location.get_blake3(),
            Some(blake3::hash(&data).as_bytes().as_slice())
        );
        assert_eq!(read_back(&handler, location).await, data);
    }
    let location = write(&handler, &sample()).await;
    assert!(location.stored_size() < location.blob_size);
}

#[tokio::test]
async fn ranges_map_frames() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = sample();
    let size = data.len() as u64;
    let location = write(&handler, &data).await;

    for range in [
        FRAME_SIZE - 7..FRAME_SIZE + 9,
        0..1,
        size - 3..size,
        FRAME_SIZE / 2..3 * FRAME_SIZE + 5,
    ] {
        let expected = &data[range.start as usize..range.end as usize];
        let read = read_range(&handler, location.clone(), range).await.unwrap();
        assert_eq!(read, expected);
    }
}

#[tokio::test]
async fn tampered_frame_fails() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = sample();
    let location = write(&handler, &data).await;
    let path = location.get_full_path().unwrap();
    let mut stored = std::fs::read(&path).unwrap();
    stored[10] ^= 1;
    std::fs::write(&path, stored).unwrap();

    let BlobEvent::ReadFinished { blob, .. } = handler.read_blob(location.clone()).await else {
        panic!("read failed to start")
    };
    let result: Result<Vec<bytes::Bytes>, _> = blob.try_collect().await;
    assert!(result.is_err());
    let range = read_range(&handler, location, 0..10).await;
    assert!(range.is_err());
}

#[tokio::test]
async fn reuses_cached_table() {
    // The first read caches the seek table, so damage to the stored table is not read again.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = sample();
    let location = write(&handler, &data).await;
    let range = FRAME_SIZE..FRAME_SIZE + 10;
    let expected = &data[range.start as usize..range.end as usize];
    let read = read_range(&handler, location.clone(), range.clone()).await;
    assert_eq!(read.unwrap(), expected);

    let path = location.get_full_path().unwrap();
    let mut stored = std::fs::read(&path).unwrap();
    let last = stored.len() - 1;
    stored[last] ^= 1;
    std::fs::write(&path, stored).unwrap();
    assert_eq!(
        read_range(&handler, location, range).await.unwrap(),
        expected
    );
}

#[tokio::test]
async fn compose_writes_frames() {
    // Parts stay raw; only the composed object is framed, hashed over original bytes.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = sample();
    let (first, second) = data.split_at(FRAME_SIZE as usize + 3);
    let upload = ulid::Ulid::generate();
    let mut parts = Vec::new();
    for (number, bytes) in [(1, first), (2, second)] {
        let event = handler
            .write_blob_part(
                MultipartPartKey::new(upload, number),
                zstd_backend(),
                test_user_id(),
                stream_from_bytes(bytes),
            )
            .await;
        let BlobEvent::WriteFinished { location } = event else {
            panic!("part write failed: {event:?}")
        };
        assert_eq!(location.format.layout, StoredLayout::Raw);
        parts.push(location);
    }

    let event = handler
        .compose_blob("bucket", "parts.bin", zstd_backend(), test_user_id(), parts)
        .await;
    let BlobEvent::WriteFinished { location } = event else {
        panic!("compose failed: {event:?}")
    };

    assert!(matches!(location.format.layout, StoredLayout::Frames(_)));
    assert_eq!(
        location.get_blake3(),
        Some(blake3::hash(&data).as_bytes().as_slice())
    );
    assert_eq!(read_back(&handler, location).await, data);
}

#[tokio::test]
async fn replica_streams_original() {
    // A bao transfer of a framed copy carries the original bytes and their hash.
    use bao_tree::io::fsm::CreateOutboard;
    use bao_tree::io::outboard::PreOrderOutboard;
    use iroh_io::AsyncSliceReader;
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let data = sample();
    let location = write(&handler, &data).await;

    let mut reader = handler.slice_reader(&location).await.unwrap();
    let outboard =
        PreOrderOutboard::<bytes::BytesMut>::create(&mut reader, crate::blob::BAO_BLOCK_SIZE)
            .await
            .unwrap();

    assert_eq!(outboard.root.as_bytes(), blake3::hash(&data).as_bytes());
    let offset = FRAME_SIZE - 100;
    let bytes = reader.read_exact_at(offset, 300).await.unwrap();
    assert_eq!(&bytes[..], &data[offset as usize..offset as usize + 300]);
    let end = reader.read_at(data.len() as u64 - 10, 100).await.unwrap();
    assert_eq!(&end[..], &data[data.len() - 10..]);
}
