//! Compressed uploads into a provider upload: padding, retried and staged parts, the table part.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{cold_backend, read_back, s3_env, setup_s3_mixed, stream_from_bytes, test_user_id};
use crate::blob::BlobHandler;
use crate::codec::PART_MIN;
use crate::hash::Hasher;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::blob::{BackendLocation, ResolvedBackend};
use aruna_core::structs::storage::format::{Compression, StoredLayout};
use aruna_core::structs::storage::multipart::{BackendUpload, MultipartPart, MultipartPartKey};
use futures::TryStreamExt;
use std::ops::Range;
use std::time::SystemTime;
use ulid::Ulid;

const MIB: usize = 1 << 20;

fn framed() -> ResolvedBackend {
    cold_backend().with_compression(Compression::Zstd { level: 3 })
}

/// Repeated text, which compresses to a few KiB per MiB.
fn text(len: usize, word: &[u8]) -> Vec<u8> {
    word.iter().copied().cycle().take(len).collect()
}

async fn open(handler: &BlobHandler, key: &str) -> BackendUpload {
    let event = handler
        .open_upload(Ulid::generate(), "bucket", key, framed(), test_user_id())
        .await;
    let BlobEvent::UploadOpened {
        backend_upload: Some(upload),
    } = event
    else {
        panic!("compressed upload did not open: {event:?}")
    };
    assert!(matches!(
        upload.location.format.layout,
        StoredLayout::FramedPart(_)
    ));
    upload
}

/// Writes a part as the runtime routes it: into the provider upload, or staged in a blob.
async fn put(
    handler: &BlobHandler,
    upload: &BackendUpload,
    number: u16,
    payload: &[u8],
) -> MultipartPart {
    let event = handler
        .write_part(
            upload.clone(),
            MultipartPartKey::new(upload.record_id, number),
            framed(),
            test_user_id(),
            Some(payload.len() as u64),
            stream_from_bytes(payload),
        )
        .await;
    let (location, backend_etag) = match event {
        BlobEvent::PartWritten {
            location,
            backend_etag,
        } => (location, Some(backend_etag)),
        BlobEvent::WriteFinished { location } => (location, None),
        other => panic!("part write failed: {other:?}"),
    };
    MultipartPart {
        part_number: number,
        location,
        created_at: SystemTime::now(),
        backend_etag,
        piece: None,
    }
}

async fn read_range(
    handler: &BlobHandler,
    location: &BackendLocation,
    range: Range<u64>,
) -> Vec<u8> {
    let event = handler.read_blob_range(location.clone(), range).await;
    let BlobEvent::ReadFinished { blob, .. } = event else {
        panic!("range read failed: {event:?}")
    };
    let chunks: Vec<bytes::Bytes> = blob.try_collect().await.unwrap();
    chunks.concat()
}

/// Checks the provider parts of the object: every part but the last holds at least 5 MiB.
async fn assert_part_sizes(
    handler: &BlobHandler,
    upload: &BackendUpload,
    location: &BackendLocation,
    numbers: &[i32],
) -> Vec<u64> {
    let native = handler.native_for(&upload.location).unwrap().unwrap();
    let path = upload.location.get_storage_path().unwrap();
    let (listed, sizes): (Vec<i32>, Vec<u64>) = native.part_sizes(&path).await.into_iter().unzip();
    assert_eq!(listed, numbers);
    assert_eq!(sizes.iter().sum::<u64>(), location.stored_size());
    let (_, others) = sizes.split_last().unwrap();
    assert!(others.iter().all(|size| *size >= PART_MIN), "{sizes:?}");
    sizes
}

/// Checks the object against `expected` in full and in ranges around each part border.
async fn assert_reads(
    handler: &BlobHandler,
    location: &BackendLocation,
    expected: &[u8],
    borders: &[u64],
) {
    assert!(matches!(location.format.layout, StoredLayout::Frames(_)));
    assert_eq!(location.blob_size, expected.len() as u64);
    assert_eq!(location.hashes, Hasher::new_with_bytes(expected).to_map());
    assert_eq!(read_back(handler, location.clone()).await, expected);
    let size = expected.len() as u64;
    for border in borders {
        for range in [
            border - 9..size.min(border + 9),
            border - 1..size,
            *border..border + 1,
        ] {
            let wanted = &expected[range.start as usize..range.end as usize];
            assert_eq!(read_range(handler, location, range).await, wanted);
        }
    }
}

#[tokio::test]
#[ignore = "requires a real S3 endpoint (ARUNA_TEST_S3_* variables)"]
async fn s3_framed_parts() {
    // Parts that compress well are padded; a retried part and a staged last part read back.
    let env = s3_env();
    let context = setup_s3_mixed(&env).await;
    let handler = context.blob_handle.handler.clone();
    let upload = open(&handler, "framed-parts.bin").await;
    let first = text(5 * MIB + 7, b"first part ");
    let second = text(6 * MIB, b"second part ");
    let retried = text(6 * MIB + 3, b"retried second part ");
    let tail = b"tail".to_vec();

    let one = put(&handler, &upload, 1, &first).await;
    let StoredLayout::FramedPart(layout) = &one.location.format.layout else {
        panic!("a streamed part records no frames")
    };
    assert!(layout.padding > 0);
    // A retry after a failed attempt writes the provider part again, with other frames.
    let failed = put(&handler, &upload, 2, &second).await;
    handler.release_claim(failed.location.ulid);
    let two = put(&handler, &upload, 2, &retried).await;
    assert!(two.location.partial);
    assert_ne!(two.location.format, failed.location.format);
    // Below 5 MiB a part can only be last, so it waits in a blob for the seek table.
    let three = put(&handler, &upload, 3, &tail).await;
    assert!(!three.location.partial);
    let listed = vec![one, two, three];

    let event = handler
        .complete_upload(upload.clone(), listed.clone())
        .await;
    let BlobEvent::WriteFinished { location } = event else {
        panic!("compressed completion failed: {event:?}")
    };
    let expected = [first.as_slice(), &retried, &tail].concat();
    let borders = [first.len() as u64, (first.len() + retried.len()) as u64];
    assert_reads(&handler, &location, &expected, &borders).await;
    assert_part_sizes(&handler, &upload, &location, &[1, 2, 3]).await;

    // A repeated completion whose first answer was lost finds the object by its seek table.
    let BlobEvent::WriteFinished { location: again } =
        handler.complete_upload(upload, listed).await
    else {
        panic!("repeated completion failed")
    };
    assert_eq!(again.format, location.format);
    assert_eq!(again.hashes, location.hashes);
}

#[tokio::test]
#[ignore = "requires a real S3 endpoint (ARUNA_TEST_S3_* variables)"]
async fn s3_framed_table() {
    // A streamed last part leaves the seek table to a part of its own. Replacements of
    // acknowledged parts wait in blobs; at completion both go into one provider part.
    let env = s3_env();
    let context = setup_s3_mixed(&env).await;
    let handler = context.blob_handle.handler.clone();
    let upload = open(&handler, "framed-table.bin").await;
    let first = text(6 * MIB, b"one ");
    let second = text(5 * MIB, b"two ");
    let replaced = text(5 * MIB + 1, b"replaced two ");
    let third = text(5 * MIB, b"three ");

    assert!(put(&handler, &upload, 1, &second).await.location.partial);
    let one = put(&handler, &upload, 1, &first).await;
    assert!(!one.location.partial);
    assert!(put(&handler, &upload, 2, &second).await.location.partial);
    let two = put(&handler, &upload, 2, &replaced).await;
    assert!(!two.location.partial);
    let three = put(&handler, &upload, 3, &third).await;
    assert!(three.location.partial);

    let event = handler
        .complete_upload(upload.clone(), vec![one, two, three])
        .await;
    let BlobEvent::WriteFinished { location } = event else {
        panic!("compressed completion failed: {event:?}")
    };
    let expected = [first.as_slice(), &replaced, &third].concat();
    let borders = [first.len() as u64, (first.len() + replaced.len()) as u64];
    assert_reads(&handler, &location, &expected, &borders).await;
    let sizes = assert_part_sizes(&handler, &upload, &location, &[1, 3, 10_000]).await;
    assert!(sizes[2] < MIB as u64);
}
