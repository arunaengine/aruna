//! Tests the local copy store: chunked copies, sync before an acknowledgement, the bound on
//! unsettled copies, the sweep, interrupted writes, copies across a restart and held memory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use aruna_core::UserId;
use aruna_core::errors::StorageError;
use aruna_core::plugins::PublishedPlugin;
use aruna_core::structs::identity::realm::RealmId;
use aruna_storage::{FjallStorage, StorageHandle};
use tempfile::{TempDir, tempdir};

const FIXTURE: &[u8] = include_bytes!("../../../plugins/tests/fixture.wasm");
/// A fixed clock for the copies these tests write.
const T0: u64 = 1_762_700_000_000;

fn context(storage_handle: StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

fn opened() -> (TempDir, DriverContext) {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    (dir, context(storage))
}

/// A storage that passes every effect to `real`, but fails the `nth` effect (counted from 0)
/// that `matching` selects.
fn faulty(real: StorageHandle, matching: fn(&StorageEffect) -> bool, nth: usize) -> StorageHandle {
    let (handle, receivers) = StorageHandle::new();
    let seen = Arc::new(AtomicUsize::new(0));
    for receiver in [receivers.foreground, receivers.bulk] {
        let (real, seen) = (real.clone(), seen.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            while let Ok((effect, response, ..)) = receiver.recv() {
                let failing = matching(&effect) && seen.fetch_add(1, Ordering::SeqCst) == nth;
                let event = match failing {
                    true => StorageEvent::Error {
                        error: StorageError::PersistError("injected failure".into()),
                    },
                    false => match runtime.block_on(real.send_storage_effect(effect)) {
                        Event::Storage(event) => event,
                        other => panic!("unexpected event: {other:?}"),
                    },
                };
                response.send(event);
            }
        });
    }
    handle
}

fn faulty_context(matching: fn(&StorageEffect) -> bool, nth: usize) -> (TempDir, DriverContext) {
    let dir = tempdir().unwrap();
    let real = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    (dir, context(faulty(real, matching, nth)))
}

fn syncs(effect: &StorageEffect) -> bool {
    matches!(effect, StorageEffect::SyncAll)
}

fn commits(effect: &StorageEffect) -> bool {
    matches!(effect, StorageEffect::CommitTransaction { .. })
}

/// The fixture with a custom section appended, so it spans three chunks.
fn padded() -> Vec<u8> {
    let name = b"padding";
    let mut payload = vec![name.len() as u8];
    payload.extend(name);
    payload.resize(2 * CHUNK + CHUNK / 2, 7);
    let mut bytes = FIXTURE.to_vec();
    bytes.push(0);
    let mut size = payload.len();
    while size >= 0x80 {
        bytes.push((size as u8 & 0x7f) | 0x80);
        size >>= 7;
    }
    bytes.push(size as u8);
    bytes.extend(payload);
    bytes
}

fn config(published: &[&[u8]]) -> RealmConfigDocument {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    for bytes in published {
        let manifest = check_artifact(bytes).unwrap();
        config.plugins.push(PublishedPlugin {
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            digest: ArtifactDigest::of(bytes).to_string(),
            size: bytes.len() as u64,
            manifest: serde_json::to_string(&manifest).unwrap(),
            published_by: UserId::nil(realm_id),
            published_at_ms: 1,
        });
    }
    config
}

fn digest(bytes: &[u8]) -> String {
    ArtifactDigest::of(bytes).to_string()
}

async fn local(ctx: &DriverContext, digest: &str) -> Option<Vec<u8>> {
    read_local(ctx, digest)
        .await
        .unwrap()
        .map(|copy| copy.bytes)
}

async fn rows(ctx: &DriverContext) -> usize {
    let rows = scan_all(&ctx.storage_handle, PLUGIN_ARTIFACT_KEYSPACE, None).await;
    rows.unwrap().len()
}

fn after_grace(time: u64) -> u64 {
    time + GRACE.as_millis() as u64 + 1
}

#[tokio::test]
async fn chunked_copy() {
    let (_dir, ctx) = opened();
    let config = config(&[]);
    let big = padded();
    let digest = digest(&big);
    assert!(check_artifact(&big).is_ok());
    assert_eq!(chunk_count(big.len() as u64), 3);
    let store = |chunk| store_chunk(&ctx, &config, chunk, T0);

    let mut short = chunk_of(&digest, &big, 0);
    short.bytes.0.pop();
    assert!(matches!(store(short).await, Err(ArtifactError::Mismatch)));
    let mut huge = chunk_of(&digest, &big, 0);
    huge.total = MAX_ARTIFACT as u64 + 1;
    assert!(matches!(store(huge).await, Err(ArtifactError::Mismatch)));

    assert!(!store(chunk_of(&digest, &big, 2)).await.unwrap());
    assert!(!store(chunk_of(&digest, &big, 0)).await.unwrap());
    let mut corrupt = chunk_of(&digest, &big, 1);
    corrupt.bytes.0[0] ^= 1;
    let refused = store(corrupt.clone()).await;
    assert!(
        matches!(refused, Err(ArtifactError::Mismatch)),
        "{refused:?}"
    );
    assert_eq!(local(&ctx, &digest).await, None);

    assert!(store(chunk_of(&digest, &big, 1)).await.unwrap());
    assert_eq!(local(&ctx, &digest).await, Some(big.clone()));
    assert!(store(corrupt).await.unwrap(), "a done copy stays");
    assert_eq!(local(&ctx, &digest).await, Some(big.clone()));
    let (total, first) = read_chunk(&ctx, &digest, 0).await.unwrap().unwrap();
    assert_eq!((total, first.len()), (big.len() as u64, CHUNK));
    assert_eq!(read_chunk(&ctx, &digest, 3).await.unwrap(), None);
    assert!(has_local(&ctx, &digest).await.unwrap());
}

#[tokio::test]
async fn sync_before_ack() {
    let (_dir, ctx) = faulty_context(syncs, 0);
    let config = config(&[]);
    let chunk = chunk_of(&digest(FIXTURE), FIXTURE, 0);
    let failed = store_chunk(&ctx, &config, chunk.clone(), T0).await;
    assert!(
        matches!(failed, Err(ArtifactError::Storage(_))),
        "{failed:?}"
    );
    // The done row exists, but only a resend whose sync succeeds acknowledges the copy.
    assert!(store_chunk(&ctx, &config, chunk, T0).await.unwrap());
}

#[tokio::test]
async fn bounds_unsettled() {
    let (_dir, ctx) = opened();
    let unpublished = config(&[]);
    let chunk = |n: u8| {
        let fake = format!("sha256:{}", format!("{n:02x}").repeat(32));
        let mut chunk = chunk_of(&fake, &[n; 16], 0);
        chunk.total = MAX_ARTIFACT as u64;
        chunk.bytes = ArtifactBytes(vec![n; CHUNK]);
        chunk
    };
    for n in 0..(MAX_UNSETTLED / MAX_ARTIFACT as u64) as u8 {
        assert!(!store_chunk(&ctx, &unpublished, chunk(n), T0).await.unwrap());
    }
    let refused = store_chunk(&ctx, &unpublished, chunk(200), T0).await;
    assert!(matches!(refused, Err(ArtifactError::Busy)), "{refused:?}");
    // A published copy is always welcome, however much is unsettled.
    let published = config(&[FIXTURE]);
    store_local(&ctx, &published, &digest(FIXTURE), FIXTURE, T0)
        .await
        .unwrap();
}

#[tokio::test]
async fn sweeps_abandoned() {
    let (_dir, ctx) = opened();
    let big = padded();
    let published = config(&[FIXTURE]);
    store_local(&ctx, &published, &digest(FIXTURE), FIXTURE, T0)
        .await
        .unwrap();
    store_local(&ctx, &published, &digest(&big), &big, T0)
        .await
        .unwrap();
    let mut unfinished = chunk_of(&digest(&big), &big, 0);
    unfinished.digest = format!("sha256:{}", "ee".repeat(32));
    assert!(!store_chunk(&ctx, &published, unfinished, T0).await.unwrap());

    assert_eq!(
        sweep_copies(&ctx, &published, T0).await.unwrap(),
        0,
        "recent copies stay"
    );
    assert_eq!(
        sweep_copies(&ctx, &published, after_grace(T0))
            .await
            .unwrap(),
        2
    );
    assert_eq!(local(&ctx, &digest(FIXTURE)).await, Some(FIXTURE.to_vec()));
    assert_eq!(local(&ctx, &digest(&big)).await, None);
    assert_eq!(
        rows(&ctx).await,
        2,
        "only the published copy's chunk and done row remain"
    );
}

#[tokio::test]
async fn resend_keeps_copy() {
    // A resend acknowledges an unpublished copy again, so its retention starts over.
    let (_dir, ctx) = opened();
    let unpublished = config(&[]);
    let chunk = chunk_of(&digest(FIXTURE), FIXTURE, 0);
    assert!(
        store_chunk(&ctx, &unpublished, chunk.clone(), T0)
            .await
            .unwrap()
    );
    let resent = after_grace(T0) - 2;
    assert!(
        store_chunk(&ctx, &unpublished, chunk, resent)
            .await
            .unwrap()
    );
    assert_eq!(
        sweep_copies(&ctx, &unpublished, after_grace(T0))
            .await
            .unwrap(),
        0
    );
    assert_eq!(local(&ctx, &digest(FIXTURE)).await, Some(FIXTURE.to_vec()));
    assert_eq!(
        sweep_copies(&ctx, &unpublished, after_grace(resent))
            .await
            .unwrap(),
        1
    );
    assert_eq!(rows(&ctx).await, 0);
}

#[tokio::test]
async fn interrupted_local_write() {
    // A local write that fails after its first chunk leaves a counted, sweepable unfinished copy.
    let (_dir, ctx) = faulty_context(commits, 1);
    let unpublished = config(&[]);
    let big = padded();
    let failed = store_local(&ctx, &unpublished, &digest(&big), &big, T0).await;
    assert!(
        matches!(failed, Err(ArtifactError::Storage(_))),
        "{failed:?}"
    );
    assert_eq!(local(&ctx, &digest(&big)).await, None);
    assert_eq!(rows(&ctx).await, 2, "the first chunk and its progress row");
    assert_eq!(
        sweep_copies(&ctx, &unpublished, after_grace(T0))
            .await
            .unwrap(),
        1
    );
    assert_eq!(rows(&ctx).await, 0);
}

#[tokio::test]
async fn interrupted_completion() {
    // A failed completion leaves the copy unfinished, never half complete; a resend completes it.
    let (_dir, ctx) = faulty_context(commits, 0);
    let unpublished = config(&[]);
    let chunk = chunk_of(&digest(FIXTURE), FIXTURE, 0);
    let failed = store_chunk(&ctx, &unpublished, chunk.clone(), T0).await;
    assert!(
        matches!(failed, Err(ArtifactError::Storage(_))),
        "{failed:?}"
    );
    assert!(!has_local(&ctx, &digest(FIXTURE)).await.unwrap());
    assert!(store_chunk(&ctx, &unpublished, chunk, T0).await.unwrap());
    assert_eq!(
        rows(&ctx).await,
        2,
        "the chunk and its done row, no progress row"
    );

    // A stale progress row next to a published done row only removes itself.
    let published = config(&[FIXTURE]);
    let progress = Progress::new(FIXTURE.len() as u64, T0).encode();
    write_rows(&ctx, vec![(progress_key(&digest(FIXTURE)), progress)])
        .await
        .unwrap();
    assert_eq!(
        sweep_copies(&ctx, &published, after_grace(T0))
            .await
            .unwrap(),
        0
    );
    assert_eq!(local(&ctx, &digest(FIXTURE)).await, Some(FIXTURE.to_vec()));
    assert_eq!(rows(&ctx).await, 2);
}

#[tokio::test]
async fn holds_memory() {
    // Assembled bytes carry their share of the node's memory budget until they are dropped.
    let (_dir, ctx) = opened();
    let big = padded();
    store_local(&ctx, &config(&[]), &digest(&big), &big, T0)
        .await
        .unwrap();
    let copy = read_local(&ctx, &digest(&big)).await.unwrap().unwrap();
    assert_eq!(copy.reserved_mib(), big.len().div_ceil(1 << 20) as u32);
    assert_eq!(copy.bytes, big);
}

#[tokio::test]
async fn survives_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let config = config(&[]);
    let big = padded();
    let digest = digest(&big);
    let ctx = context(FjallStorage::open(&path).unwrap());
    let first = store_chunk(&ctx, &config, chunk_of(&digest, &big, 0), T0).await;
    assert!(!first.unwrap());
    let second = store_chunk(&ctx, &config, chunk_of(&digest, &big, 1), T0).await;
    assert!(!second.unwrap());
    ctx.storage_handle.close().await;
    let ctx = context(FjallStorage::open(&path).unwrap());
    let last = store_chunk(&ctx, &config, chunk_of(&digest, &big, 2), T0).await;
    assert!(last.unwrap());
    assert_eq!(local(&ctx, &digest).await, Some(big));
}
