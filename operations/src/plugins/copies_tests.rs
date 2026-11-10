//! Tests the local copy store: chunked copies, sync before an acknowledgement, the bound on
//! unsettled copies, the sweep, and copies across a restart.
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

/// A storage that passes every effect to `real` but fails the first `failures` syncs.
fn failing_sync(real: StorageHandle, failures: usize) -> StorageHandle {
    let (handle, receivers) = StorageHandle::new();
    let left = Arc::new(AtomicUsize::new(failures));
    for receiver in [receivers.foreground, receivers.bulk] {
        let (real, left) = (real.clone(), left.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            while let Ok((effect, response, ..)) = receiver.recv() {
                let failing = matches!(effect, StorageEffect::SyncAll)
                    && left
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok();
                let event = match failing {
                    true => StorageEvent::Error {
                        error: StorageError::PersistError("sync failed".into()),
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

#[tokio::test]
async fn chunked_copy() {
    let (_dir, ctx) = opened();
    let config = config(&[]);
    let big = padded();
    let digest = digest(&big);
    assert!(check_artifact(&big).is_ok());
    assert_eq!(chunk_count(big.len() as u64), 3);

    let mut short = chunk_of(&digest, &big, 0);
    short.bytes.0.pop();
    assert!(matches!(
        store_chunk(&ctx, &config, short).await,
        Err(ArtifactError::Mismatch)
    ));
    let mut huge = chunk_of(&digest, &big, 0);
    huge.total = MAX_ARTIFACT as u64 + 1;
    assert!(matches!(
        store_chunk(&ctx, &config, huge).await,
        Err(ArtifactError::Mismatch)
    ));

    assert!(
        !store_chunk(&ctx, &config, chunk_of(&digest, &big, 2))
            .await
            .unwrap()
    );
    assert!(
        !store_chunk(&ctx, &config, chunk_of(&digest, &big, 0))
            .await
            .unwrap()
    );
    let mut corrupt = chunk_of(&digest, &big, 1);
    corrupt.bytes.0[0] ^= 1;
    let refused = store_chunk(&ctx, &config, corrupt.clone()).await;
    assert!(
        matches!(refused, Err(ArtifactError::Mismatch)),
        "{refused:?}"
    );
    assert_eq!(read_local(&ctx, &digest).await.unwrap(), None);

    assert!(
        store_chunk(&ctx, &config, chunk_of(&digest, &big, 1))
            .await
            .unwrap()
    );
    assert_eq!(read_local(&ctx, &digest).await.unwrap(), Some(big.clone()));
    assert!(
        store_chunk(&ctx, &config, corrupt).await.unwrap(),
        "a done copy stays"
    );
    assert_eq!(read_local(&ctx, &digest).await.unwrap(), Some(big.clone()));
    let (total, first) = read_chunk(&ctx, &digest, 0).await.unwrap().unwrap();
    assert_eq!((total, first.len()), (big.len() as u64, CHUNK));
    assert_eq!(read_chunk(&ctx, &digest, 3).await.unwrap(), None);
}

#[tokio::test]
async fn sync_before_ack() {
    let dir = tempdir().unwrap();
    let real = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = context(failing_sync(real, 1));
    let config = config(&[]);
    let digest = digest(FIXTURE);
    let chunk = chunk_of(&digest, FIXTURE, 0);
    let failed = store_chunk(&ctx, &config, chunk.clone()).await;
    assert!(
        matches!(failed, Err(ArtifactError::Storage(_))),
        "{failed:?}"
    );
    // The done row exists, but only a resend whose sync succeeds acknowledges the copy.
    assert!(store_chunk(&ctx, &config, chunk).await.unwrap());
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
        assert!(!store_chunk(&ctx, &unpublished, chunk(n)).await.unwrap());
    }
    let refused = store_chunk(&ctx, &unpublished, chunk(200)).await;
    assert!(matches!(refused, Err(ArtifactError::Busy)), "{refused:?}");
    // A published copy is always welcome, however much is unsettled.
    store_local(&ctx, &config(&[FIXTURE]), &digest(FIXTURE), FIXTURE)
        .await
        .unwrap();
}

#[tokio::test]
async fn sweeps_abandoned() {
    let (_dir, ctx) = opened();
    let big = padded();
    let published = config(&[FIXTURE]);
    store_local(&ctx, &published, &digest(FIXTURE), FIXTURE)
        .await
        .unwrap();
    store_local(&ctx, &published, &digest(&big), &big)
        .await
        .unwrap();
    let partial = chunk_of(&digest(&big), &big, 0);
    let other = format!("sha256:{}", "ee".repeat(32));
    let mut unfinished = partial.clone();
    unfinished.digest = other.clone();
    assert!(!store_chunk(&ctx, &published, unfinished).await.unwrap());

    let now = unix_timestamp_millis();
    assert_eq!(
        sweep_copies(&ctx, &published, now).await.unwrap(),
        0,
        "recent copies stay"
    );
    let later = now + GRACE.as_millis() as u64 + 1;
    assert_eq!(sweep_copies(&ctx, &published, later).await.unwrap(), 2);
    assert_eq!(
        read_local(&ctx, &digest(FIXTURE)).await.unwrap(),
        Some(FIXTURE.to_vec())
    );
    assert_eq!(read_local(&ctx, &digest(&big)).await.unwrap(), None);
    let rows = scan_all(&ctx.storage_handle, PLUGIN_ARTIFACT_KEYSPACE, None)
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "only the published copy's chunk and done row remain"
    );
}

#[tokio::test]
async fn survives_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let config = config(&[]);
    let big = padded();
    let digest = digest(&big);
    let ctx = context(FjallStorage::open(&path).unwrap());
    assert!(
        !store_chunk(&ctx, &config, chunk_of(&digest, &big, 0))
            .await
            .unwrap()
    );
    assert!(
        !store_chunk(&ctx, &config, chunk_of(&digest, &big, 1))
            .await
            .unwrap()
    );
    ctx.storage_handle.close().await;
    let ctx = context(FjallStorage::open(&path).unwrap());
    assert!(
        store_chunk(&ctx, &config, chunk_of(&digest, &big, 2))
            .await
            .unwrap()
    );
    assert_eq!(read_local(&ctx, &digest).await.unwrap(), Some(big));
}
