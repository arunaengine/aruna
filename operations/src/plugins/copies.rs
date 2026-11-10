//! Keeps artifact copies on this node: chunk rows and a completion row per complete copy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PLUGIN_ARTIFACT_KEYSPACE;
use aruna_core::plugins::MAX_ARTIFACT;
use aruna_plugins::artifact::{ArtifactDigest, check_artifact};

use super::ArtifactBytes;
use super::artifacts::ArtifactError;
use crate::driver::DriverContext;

/// Bytes of one artifact chunk; node messages stay far below their 16 MiB limit.
pub const CHUNK: usize = 8 << 20;

/// Reads this node's copy, if it is complete: its completion row names the length, and the
/// chunks below it hold the bytes.
pub async fn read_local(
    ctx: &DriverContext,
    digest: &str,
) -> Result<Option<Vec<u8>>, ArtifactError> {
    let Some(total) = read_total(ctx, digest).await? else {
        return Ok(None);
    };
    let mut bytes = Vec::with_capacity(total as usize);
    for index in 0..chunk_count(total) {
        let chunk = read_row(ctx, chunk_key(digest, index)).await?;
        bytes.extend(chunk.ok_or_else(|| storage("a chunk of a complete copy is missing"))?);
    }
    match bytes.len() as u64 == total {
        true => Ok(Some(bytes)),
        false => Err(storage("a complete copy has the wrong length")),
    }
}

/// Writes this node's whole copy, then its completion row, and syncs before anyone is told.
pub async fn store_local(
    ctx: &DriverContext,
    digest: &str,
    bytes: &[u8],
) -> Result<(), ArtifactError> {
    for (index, chunk) in bytes.chunks(CHUNK).enumerate() {
        write_row(ctx, chunk_key(digest, index as u32), chunk.to_vec()).await?;
    }
    finish_local(ctx, digest, bytes.len() as u64).await
}

/// Keeps one chunk of a copy another management node sends. Once every chunk is here and the
/// whole artifact checks out, the copy is completed; `true` means it is complete now.
pub async fn store_chunk(ctx: &DriverContext, chunk: Chunk) -> Result<bool, ArtifactError> {
    let count = chunk_count(chunk.total);
    let sized = chunk.total > 0 && chunk.total <= MAX_ARTIFACT as u64 && chunk.index < count;
    if !sized {
        return Err(ArtifactError::Mismatch);
    }
    let expected = (chunk.total - u64::from(chunk.index) * CHUNK as u64).min(CHUNK as u64);
    if chunk.bytes.0.len() as u64 != expected {
        return Err(ArtifactError::Mismatch);
    }
    if read_total(ctx, &chunk.digest).await?.is_some() {
        return Ok(true);
    }
    write_row(ctx, chunk_key(&chunk.digest, chunk.index), chunk.bytes.0).await?;
    let mut bytes = Vec::with_capacity(chunk.total as usize);
    for index in 0..count {
        match read_row(ctx, chunk_key(&chunk.digest, index)).await? {
            Some(part) => bytes.extend(part),
            None => return Ok(false),
        }
    }
    let whole =
        ArtifactDigest::of(&bytes).as_str() == chunk.digest && check_artifact(&bytes).is_ok();
    if !whole {
        return Err(ArtifactError::Mismatch);
    }
    finish_local(ctx, &chunk.digest, chunk.total).await?;
    Ok(true)
}

/// One piece of an artifact in a node message; chunk `index` covers bytes from `index * CHUNK`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Chunk {
    pub digest: String,
    pub index: u32,
    pub total: u64,
    pub bytes: ArtifactBytes,
}

/// The chunks an artifact of `total` bytes has.
pub fn chunk_count(total: u64) -> u32 {
    total.div_ceil(CHUNK as u64) as u32
}

/// The `index`-th chunk of `bytes`, ready to send.
pub fn chunk_of(digest: &str, bytes: &[u8], index: u32) -> Chunk {
    let start = (index as usize * CHUNK).min(bytes.len());
    let end = (start + CHUNK).min(bytes.len());
    Chunk {
        digest: digest.to_string(),
        index,
        total: bytes.len() as u64,
        bytes: ArtifactBytes(bytes[start..end].to_vec()),
    }
}

pub(super) fn chunk_key(digest: &str, index: u32) -> Vec<u8> {
    [digest.as_bytes(), b"/", &index.to_be_bytes()].concat()
}

/// The length of this node's copy, once it is complete.
pub(super) async fn read_total(
    ctx: &DriverContext,
    digest: &str,
) -> Result<Option<u64>, ArtifactError> {
    let Some(row) = read_row(ctx, digest.as_bytes().to_vec()).await? else {
        return Ok(None);
    };
    let row = row.try_into().map_err(|_| storage("a bad length row"))?;
    Ok(Some(u64::from_le_bytes(row)))
}

fn storage(why: &str) -> ArtifactError {
    ArtifactError::Storage(why.to_string())
}

async fn finish_local(ctx: &DriverContext, digest: &str, total: u64) -> Result<(), ArtifactError> {
    write_row(
        ctx,
        digest.as_bytes().to_vec(),
        total.to_le_bytes().to_vec(),
    )
    .await?;
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::SyncAll)
        .await
    {
        Event::Storage(StorageEvent::SyncAllFinished) => Ok(()),
        other => Err(ArtifactError::Storage(format!("{other:?}"))),
    }
}

pub(super) async fn read_row(
    ctx: &DriverContext,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, ArtifactError> {
    let read = StorageEffect::Read {
        key_space: PLUGIN_ARTIFACT_KEYSPACE.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.map(|v| v.to_vec())),
        other => Err(ArtifactError::Storage(format!("{other:?}"))),
    }
}

async fn write_row(ctx: &DriverContext, key: Vec<u8>, value: Vec<u8>) -> Result<(), ArtifactError> {
    let write = StorageEffect::Write {
        key_space: PLUGIN_ARTIFACT_KEYSPACE.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(ArtifactError::Storage(format!("{other:?}"))),
    }
}
