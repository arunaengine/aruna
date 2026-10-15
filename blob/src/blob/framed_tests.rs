//! Compressed uploads into a provider upload: padding, retried and staged parts, the table part.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{cold_backend, read_back, s3_env, setup_s3_mixed, stream_from_bytes, test_user_id};
use crate::blob::BlobHandler;
use crate::codec::PART_MIN;
use crate::hash::Hasher;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::blob::{BackendLocation, ResolvedBackend};
use aruna_core::structs::storage::format::{Compression, EncodingClass, StoredLayout};
use aruna_core::structs::storage::multipart::{BackendUpload, MultipartPart, MultipartPartKey};
use futures::TryStreamExt;
use std::ops::Range;
use std::time::SystemTime;
use ulid::Ulid;

const MIB: usize = 1 << 20;
const ZSTD: Compression = Compression::Zstd { level: 3 };

fn framed() -> ResolvedBackend {
    cold_backend().with_compression(ZSTD)
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
        .complete_upload(upload.clone(), listed.clone(), &[], ZSTD)
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
        handler.complete_upload(upload, listed, &[], ZSTD).await
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
        .complete_upload(upload.clone(), vec![one, two, three], &[], ZSTD)
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

#[tokio::test]
#[ignore = "requires a real S3 endpoint (ARUNA_TEST_S3_* variables)"]
async fn s3_decoded_compose() {
    // A framed object composed without compression streams in chunks, never as one buffer.
    let env = s3_env();
    let context = setup_s3_mixed(&env).await;
    let handler = context.blob_handle.handler.clone();
    let upload = open(&handler, "decoded.bin").await;
    let data = text(20 * MIB, b"decoded part ");
    let part = put(&handler, &upload, 1, &data).await;
    let BlobEvent::WriteFinished { location: framed } =
        handler.complete_upload(upload, vec![part], &[], ZSTD).await
    else {
        panic!("compressed completion failed")
    };

    let event = handler
        .compose_blob(
            "bucket",
            "raw.bin",
            cold_backend(),
            test_user_id(),
            vec![framed],
        )
        .await;
    let BlobEvent::WriteFinished { location } = event else {
        panic!("decoded compose failed: {event:?}")
    };
    assert_eq!(location.format.layout, StoredLayout::Raw);
    assert_eq!(read_back(&handler, location.clone()).await, data);
    let native = handler.native_for(&location).unwrap().unwrap();
    let parts = native
        .part_sizes(&location.get_storage_path().unwrap())
        .await;
    assert_eq!(parts.len(), 3, "{parts:?}");
}

/// Seeded xorshift bytes, which do not compress.
fn noise(len: usize, mut seed: u64) -> Vec<u8> {
    (0..len)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect()
}

/// Streams parts 1 to 3, then replaces parts 1 and 3 with staged blobs of `sizes`.
async fn staged_around(
    handler: &BlobHandler,
    upload: &BackendUpload,
    sizes: [usize; 2],
) -> [(MultipartPart, Vec<u8>); 3] {
    let mut streamed = Vec::new();
    for number in 1..=3 {
        let payload = noise(6 * MIB, u64::from(number));
        let part = put(handler, upload, number, &payload).await;
        assert!(part.location.partial);
        streamed.push((part, payload));
    }
    let (first, third) = (noise(sizes[0], 11), noise(sizes[1], 13));
    let one = put(handler, upload, 1, &first).await;
    let three = put(handler, upload, 3, &third).await;
    assert!(!one.location.partial && !three.location.partial);
    let (two, second) = streamed.swap_remove(1);
    [(one, first), (two, second), (three, third)]
}

#[tokio::test]
#[ignore = "requires a real S3 endpoint (ARUNA_TEST_S3_* variables)"]
async fn s3_framed_fallback() {
    // Staged part 1 needs two provider parts, but streamed part 2 holds the next number:
    // part 2 is assembled alone and composed with the staged parts into a new copy.
    let env = s3_env();
    let context = setup_s3_mixed(&env).await;
    let handler = context.blob_handle.handler.clone();
    let upload = open(&handler, "fallback.bin").await;
    let [(one, first), (two, second), (three, third)] =
        staged_around(&handler, &upload, [20 * MIB, 6 * MIB]).await;

    // The bucket level changed since the upload opened: the new copy takes the current one.
    let current = Compression::Zstd { level: 5 };
    let event = handler
        .complete_upload(upload.clone(), vec![one, two, three], &[2], current)
        .await;
    let BlobEvent::WriteFinished { location } = event else {
        panic!("fallback completion failed: {event:?}")
    };
    assert!(!location.same_object(&upload.location));
    assert_eq!(location.format.encoding(), EncodingClass::from(current));
    let expected = [first.as_slice(), &second, &third].concat();
    let borders = [first.len() as u64, (first.len() + second.len()) as u64];
    assert_reads(&handler, &location, &expected, &borders).await;
}

#[tokio::test]
#[ignore = "requires a real S3 endpoint (ARUNA_TEST_S3_* variables)"]
async fn s3_omitted_part() {
    // Staged parts 1 and 3 need two provider parts, but omitted part 2 holds the number
    // between them: they are composed instead, and part 2 stays for a later completion.
    let env = s3_env();
    let context = setup_s3_mixed(&env).await;
    let handler = context.blob_handle.handler.clone();
    let upload = open(&handler, "omitted.bin").await;
    let [(one, first), (two, second), (three, third)] =
        staged_around(&handler, &upload, [10 * MIB, 10 * MIB]).await;

    // Compression was turned off since the upload opened: the new copy keeps original bytes.
    let listed = vec![one.clone(), three.clone()];
    let off = Compression::Off;
    let event = handler
        .complete_upload(upload.clone(), listed, &[2], off)
        .await;
    let BlobEvent::WriteFinished { location } = event else {
        panic!("fallback completion failed: {event:?}")
    };
    assert!(!location.same_object(&upload.location));
    assert_eq!(location.format.layout, StoredLayout::Raw);
    let expected = [first.as_slice(), &third].concat();
    assert_eq!(location.hashes, Hasher::new_with_bytes(&expected).to_map());
    assert_eq!(read_back(&handler, location.clone()).await, expected);

    let event = handler
        .complete_upload(upload.clone(), vec![one, two, three], &[2], ZSTD)
        .await;
    let BlobEvent::WriteFinished { location } = event else {
        panic!("completion with the omitted part failed: {event:?}")
    };
    assert!(location.same_object(&upload.location));
    let expected = [first.as_slice(), &second, &third].concat();
    let borders = [first.len() as u64, (first.len() + second.len()) as u64];
    assert_reads(&handler, &location, &expected, &borders).await;
    assert_part_sizes(&handler, &upload, &location, &[1, 2, 3]).await;
}
