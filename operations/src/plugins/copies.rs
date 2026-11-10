//! Keeps artifact copies on this node: chunk rows, a progress row per unfinished copy and a done
//! row per complete one. Unsettled copies are bounded and reclaimed after a grace period.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::Duration;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PLUGIN_ARTIFACT_KEYSPACE;
use aruna_core::plugins::MAX_ARTIFACT;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::TxnId;
use aruna_plugins::artifact::{ArtifactDigest, check_artifact};
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

use super::ArtifactBytes;
use super::artifacts::ArtifactError;
use crate::driver::DriverContext;
use crate::storage_read::scan_all;

/// Bytes of one artifact chunk; node messages stay far below their 16 MiB limit.
pub const CHUNK: usize = 8 << 20;
/// Bytes of unfinished and unpublished copies one node keeps at most.
pub const MAX_UNSETTLED: u64 = 8 * MAX_ARTIFACT as u64;
/// How long an unfinished or unpublished copy stays untouched before it is reclaimed. It is
/// measured in this node's wall time, so a clock correction shortens or extends it.
pub const GRACE: Duration = Duration::from_secs(3600);
/// Memory one node spends at most on artifact uploads and assemblies, in MiB.
const MEMORY_MIB: u32 = 256;

/// Serializes the artifact writes of this node, so progress rows never lose a chunk.
static WRITES: Mutex<()> = Mutex::const_new(());
static MEMORY: Semaphore = Semaphore::const_new(MEMORY_MIB as usize);

/// Memory reserved for one artifact upload or assembly, released when dropped.
pub type MemoryPermit = SemaphorePermit<'static>;

/// Reserves memory for `bytes`, or `None` while the node spends its budget elsewhere.
pub fn reserve(bytes: usize) -> Option<MemoryPermit> {
    let mib = u32::try_from(bytes.div_ceil(1 << 20).max(1)).ok()?;
    MEMORY.try_acquire_many(mib).ok()
}

/// A whole artifact in memory together with the memory reserved for it; dropping it releases
/// the reservation, so whoever consumes the bytes holds it until done.
pub struct Assembled {
    pub bytes: Vec<u8>,
    memory: MemoryPermit,
}

impl Assembled {
    pub fn new(bytes: Vec<u8>, memory: MemoryPermit) -> Self {
        Self { bytes, memory }
    }

    /// MiB this artifact holds of the node's budget.
    pub fn reserved_mib(&self) -> u32 {
        self.memory.num_permits() as u32
    }
}

impl std::fmt::Debug for Assembled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Assembled({} bytes)", self.bytes.len())
    }
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

/// The exact length of chunk `index` of an artifact of `total` bytes.
pub fn chunk_len(total: u64, index: u32) -> u64 {
    total
        .saturating_sub(u64::from(index) * CHUNK as u64)
        .min(CHUNK as u64)
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

/// Whether a chunk fits its artifact: a size within the limit, an index and the exact length.
pub fn chunk_fits(chunk: &Chunk) -> bool {
    let sized = (1..=MAX_ARTIFACT as u64).contains(&chunk.total);
    let placed = chunk.index < chunk_count(chunk.total);
    sized && placed && chunk.bytes.0.len() as u64 == chunk_len(chunk.total, chunk.index)
}

/// Whether this node keeps a complete copy, without reading it.
pub async fn has_local(ctx: &DriverContext, digest: &str) -> Result<bool, ArtifactError> {
    Ok(read_done(ctx, digest).await?.is_some())
}

/// Reads this node's complete copy within the memory budget, or `None` without one.
pub async fn read_local(
    ctx: &DriverContext,
    digest: &str,
) -> Result<Option<Assembled>, ArtifactError> {
    let Some(done) = read_done(ctx, digest).await? else {
        return Ok(None);
    };
    let memory = reserve(done.total as usize).ok_or(ArtifactError::Busy)?;
    let bytes = assemble(ctx, digest, done.total).await?;
    Ok(Some(Assembled::new(bytes, memory)))
}

/// One chunk of this node's complete copy and the copy's length.
pub async fn read_chunk(
    ctx: &DriverContext,
    digest: &str,
    index: u32,
) -> Result<Option<(u64, Vec<u8>)>, ArtifactError> {
    let Some(done) = read_done(ctx, digest).await? else {
        return Ok(None);
    };
    if index >= chunk_count(done.total) {
        return Ok(None);
    }
    let bytes = read_row(ctx, chunk_key(digest, index)).await?;
    Ok(bytes.map(|bytes| (done.total, bytes)))
}

/// Writes this node's whole copy and syncs before anyone is told. Progress is kept with every
/// chunk, so an interrupted write is counted and swept like an unfinished copy from a peer.
pub async fn store_local(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    digest: &str,
    bytes: &[u8],
    now_ms: u64,
) -> Result<(), ArtifactError> {
    let _writes = WRITES.lock().await;
    if read_done(ctx, digest).await?.is_none() && read_progress(ctx, digest).await?.is_none() {
        admit(ctx, config, digest, bytes.len() as u64).await?;
    }
    let mut progress = Progress::new(bytes.len() as u64, now_ms);
    for (index, chunk) in bytes.chunks(CHUNK).enumerate() {
        progress.mark(index as u32, now_ms);
        let rows = vec![
            (chunk_key(digest, index as u32), chunk.to_vec()),
            (progress_key(digest), progress.encode()),
        ];
        // The first write drops an older done row, so a half-replaced copy is never complete.
        let deletes = match index {
            0 => vec![done_key(digest)],
            _ => Vec::new(),
        };
        apply(ctx, rows, deletes).await?;
    }
    finish(ctx, digest, bytes.len() as u64, now_ms).await
}

/// Keeps one chunk of a copy another management node sends. Once every chunk is here and the
/// whole artifact checks out, the copy is done; `true` means it is done and synced now. A
/// resend to a done copy restarts its retention before it is acknowledged.
pub async fn store_chunk(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    chunk: Chunk,
    now_ms: u64,
) -> Result<bool, ArtifactError> {
    if !chunk_fits(&chunk) {
        return Err(ArtifactError::Mismatch);
    }
    let _writes = WRITES.lock().await;
    if let Some(done) = read_done(ctx, &chunk.digest).await? {
        if done.total != chunk.total {
            return Err(ArtifactError::Mismatch);
        }
        let refreshed = Done::encode(done.total, now_ms);
        write_rows(ctx, vec![(done_key(&chunk.digest), refreshed)]).await?;
        sync(ctx).await?;
        return Ok(true);
    }
    let mut progress = match read_progress(ctx, &chunk.digest).await? {
        Some(progress) if progress.total != chunk.total => return Err(ArtifactError::Mismatch),
        Some(progress) => progress,
        None => {
            admit(ctx, config, &chunk.digest, chunk.total).await?;
            Progress::new(chunk.total, now_ms)
        }
    };
    progress.mark(chunk.index, now_ms);
    let rows = vec![
        (chunk_key(&chunk.digest, chunk.index), chunk.bytes.0),
        (progress_key(&chunk.digest), progress.encode()),
    ];
    write_rows(ctx, rows).await?;
    if !progress.complete() {
        return Ok(false);
    }
    let _memory = reserve(chunk.total as usize).ok_or(ArtifactError::Busy)?;
    let bytes = assemble(ctx, &chunk.digest, chunk.total).await?;
    let digest = ArtifactDigest::of(&bytes);
    if digest.as_str() != chunk.digest || check_artifact(&bytes).is_err() {
        return Err(ArtifactError::Mismatch);
    }
    finish(ctx, &chunk.digest, chunk.total, now_ms).await?;
    Ok(true)
}

/// Deletes unfinished copies and unpublished complete copies untouched for the grace period.
/// Published and pinned copies stay, and a stale progress row next to a done row only loses
/// itself. Returns how many copies it deleted.
pub async fn sweep_copies(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> Result<usize, ArtifactError> {
    let _writes = WRITES.lock().await;
    let grace = GRACE.as_millis() as u64;
    let kept = published(config);
    let done = scan_rows(ctx, b"d/", Done::decode).await?;
    let complete: BTreeSet<&str> = done.iter().map(|(digest, _)| digest.as_str()).collect();
    let mut deleted = 0;
    for (digest, progress) in scan_rows(ctx, b"p/", Progress::decode).await? {
        if progress.updated_ms.saturating_add(grace) >= now_ms {
            continue;
        }
        let mut keys = vec![progress_key(&digest)];
        if !complete.contains(digest.as_str()) {
            keys.extend((0..chunk_count(progress.total)).map(|i| chunk_key(&digest, i)));
            deleted += 1;
        }
        delete_rows(ctx, keys).await?;
    }
    for (digest, done) in &done {
        if kept.contains(digest.as_str()) || done.completed_ms.saturating_add(grace) >= now_ms {
            continue;
        }
        let mut keys: Vec<Vec<u8>> = (0..chunk_count(done.total))
            .map(|i| chunk_key(digest, i))
            .collect();
        keys.push(done_key(digest));
        delete_rows(ctx, keys).await?;
        deleted += 1;
    }
    Ok(deleted)
}

/// Refuses a new copy that would push the unfinished and unpublished bytes over the bound.
async fn admit(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    digest: &str,
    total: u64,
) -> Result<(), ArtifactError> {
    let published = published(config);
    if published.contains(digest) {
        return Ok(());
    }
    let unfinished = scan_rows(ctx, b"p/", Progress::decode).await?;
    let unpublished = (scan_rows(ctx, b"d/", Done::decode).await?.into_iter())
        .filter(|(digest, _)| !published.contains(digest.as_str()));
    let held: u64 = (unfinished.iter().map(|(_, progress)| progress.total))
        .chain(unpublished.map(|(_, done)| done.total))
        .sum();
    match held.saturating_add(total) <= MAX_UNSETTLED {
        true => Ok(()),
        false => Err(ArtifactError::Busy),
    }
}

/// Digests whose copies stay: published versions.
fn published(config: &RealmConfigDocument) -> BTreeSet<&str> {
    (config.plugins.iter())
        .map(|plugin| plugin.digest.as_str())
        .collect()
}

/// A complete copy: its length and when it was last confirmed, in this node's clock.
struct Done {
    total: u64,
    completed_ms: u64,
}

impl Done {
    fn encode(total: u64, completed_ms: u64) -> Vec<u8> {
        [total.to_le_bytes(), completed_ms.to_le_bytes()].concat()
    }

    fn decode(row: &[u8]) -> Option<Self> {
        let (total, completed_ms) = two_numbers(row)?;
        Some(Self {
            total,
            completed_ms,
        })
    }
}

/// An unfinished copy: its length, its last change and which chunks arrived.
struct Progress {
    total: u64,
    updated_ms: u64,
    received: Vec<u8>,
}

impl Progress {
    fn new(total: u64, now_ms: u64) -> Self {
        let bits = chunk_count(total).div_ceil(8) as usize;
        Self {
            total,
            updated_ms: now_ms,
            received: vec![0; bits],
        }
    }

    fn mark(&mut self, index: u32, now_ms: u64) {
        self.received[index as usize / 8] |= 1 << (index % 8);
        self.updated_ms = now_ms;
    }

    fn complete(&self) -> bool {
        (0..chunk_count(self.total)).all(|i| self.received[i as usize / 8] & (1 << (i % 8)) != 0)
    }

    fn encode(&self) -> Vec<u8> {
        [
            &self.total.to_le_bytes()[..],
            &self.updated_ms.to_le_bytes(),
            &self.received,
        ]
        .concat()
    }

    fn decode(row: &[u8]) -> Option<Self> {
        let (total, updated_ms) = two_numbers(row.get(..16)?)?;
        let received = row[16..].to_vec();
        let fits = (1..=MAX_ARTIFACT as u64).contains(&total)
            && received.len() == chunk_count(total).div_ceil(8) as usize;
        fits.then_some(Self {
            total,
            updated_ms,
            received,
        })
    }
}

fn two_numbers(row: &[u8]) -> Option<(u64, u64)> {
    let first = u64::from_le_bytes(row.get(..8)?.try_into().ok()?);
    let second = u64::from_le_bytes(row.get(8..16)?.try_into().ok()?);
    (row.len() == 16).then_some((first, second))
}

fn chunk_key(digest: &str, index: u32) -> Vec<u8> {
    [b"c/", digest.as_bytes(), b"/", &index.to_be_bytes()].concat()
}

fn done_key(digest: &str) -> Vec<u8> {
    [b"d/", digest.as_bytes()].concat()
}

fn progress_key(digest: &str) -> Vec<u8> {
    [b"p/", digest.as_bytes()].concat()
}

fn storage(why: &str) -> ArtifactError {
    ArtifactError::Storage(why.to_string())
}

async fn read_done(ctx: &DriverContext, digest: &str) -> Result<Option<Done>, ArtifactError> {
    match read_row(ctx, done_key(digest)).await? {
        Some(row) => Done::decode(&row)
            .map(Some)
            .ok_or_else(|| storage("a bad done row")),
        None => Ok(None),
    }
}

async fn read_progress(
    ctx: &DriverContext,
    digest: &str,
) -> Result<Option<Progress>, ArtifactError> {
    match read_row(ctx, progress_key(digest)).await? {
        Some(row) => Progress::decode(&row)
            .map(Some)
            .ok_or_else(|| storage("a bad progress row")),
        None => Ok(None),
    }
}

/// Reads every chunk of a copy in order; a missing chunk or wrong length is a storage fault.
async fn assemble(ctx: &DriverContext, digest: &str, total: u64) -> Result<Vec<u8>, ArtifactError> {
    let mut bytes = Vec::with_capacity(total as usize);
    for index in 0..chunk_count(total) {
        let chunk = read_row(ctx, chunk_key(digest, index)).await?;
        let chunk = chunk.ok_or_else(|| storage("a chunk of a copy is missing"))?;
        if chunk.len() as u64 != chunk_len(total, index) {
            return Err(storage("a chunk of a copy has the wrong length"));
        }
        bytes.extend(chunk);
    }
    Ok(bytes)
}

/// Writes the done row and drops the progress row in one transaction, then syncs: only then
/// may a copy count. A crash leaves either the unfinished copy or the complete one.
async fn finish(
    ctx: &DriverContext,
    digest: &str,
    total: u64,
    now_ms: u64,
) -> Result<(), ArtifactError> {
    let done = vec![(done_key(digest), Done::encode(total, now_ms))];
    apply(ctx, done, vec![progress_key(digest)]).await?;
    sync(ctx).await
}

async fn sync(ctx: &DriverContext) -> Result<(), ArtifactError> {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::SyncAll)
        .await
    {
        Event::Storage(StorageEvent::SyncAllFinished) => Ok(()),
        other => Err(ArtifactError::Storage(format!("{other:?}"))),
    }
}

/// Writes `rows` and deletes `keys` in one transaction: all of them or none.
async fn apply(
    ctx: &DriverContext,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    keys: Vec<Vec<u8>>,
) -> Result<(), ArtifactError> {
    let start = StorageEffect::StartTransaction { read: false };
    let txn_id = match ctx.storage_handle.send_storage_effect(start).await {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(ArtifactError::Storage(format!("{other:?}"))),
    };
    match apply_in(ctx, txn_id, rows, keys).await {
        Ok(()) => Ok(()),
        Err(error) => {
            let abort = StorageEffect::AbortTransaction { txn_id };
            ctx.storage_handle.send_storage_effect(abort).await;
            Err(error)
        }
    }
}

async fn apply_in(
    ctx: &DriverContext,
    txn_id: TxnId,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    keys: Vec<Vec<u8>>,
) -> Result<(), ArtifactError> {
    let writes = (rows.into_iter())
        .map(|(key, value)| {
            (
                PLUGIN_ARTIFACT_KEYSPACE.to_string(),
                key.into(),
                value.into(),
            )
        })
        .collect();
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: Some(txn_id),
    };
    match ctx.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => return Err(ArtifactError::Storage(format!("{other:?}"))),
    }
    if !keys.is_empty() {
        let deletes = (keys.into_iter())
            .map(|key| (PLUGIN_ARTIFACT_KEYSPACE.to_string(), key.into()))
            .collect();
        let delete = StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        };
        match ctx.storage_handle.send_storage_effect(delete).await {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            other => return Err(ArtifactError::Storage(format!("{other:?}"))),
        }
    }
    let commit = StorageEffect::CommitTransaction { txn_id };
    match ctx.storage_handle.send_storage_effect(commit).await {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        other => Err(ArtifactError::Storage(format!("{other:?}"))),
    }
}

/// The rows below `prefix`, keyed by digest, that decode; others are left for a later sweep.
async fn scan_rows<T>(
    ctx: &DriverContext,
    prefix: &[u8],
    decode: fn(&[u8]) -> Option<T>,
) -> Result<Vec<(String, T)>, ArtifactError> {
    let prefix_key = Some(prefix.to_vec().into());
    let rows = scan_all(&ctx.storage_handle, PLUGIN_ARTIFACT_KEYSPACE, prefix_key)
        .await
        .map_err(ArtifactError::Storage)?;
    Ok((rows.into_iter())
        .filter_map(|(key, value)| {
            let digest = std::str::from_utf8(key.get(prefix.len()..)?)
                .ok()?
                .to_string();
            Some((digest, decode(&value)?))
        })
        .collect())
}

async fn read_row(ctx: &DriverContext, key: Vec<u8>) -> Result<Option<Vec<u8>>, ArtifactError> {
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

async fn write_rows(
    ctx: &DriverContext,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), ArtifactError> {
    let writes = (rows.into_iter())
        .map(|(key, value)| {
            (
                PLUGIN_ARTIFACT_KEYSPACE.to_string(),
                key.into(),
                value.into(),
            )
        })
        .collect();
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        other => Err(ArtifactError::Storage(format!("{other:?}"))),
    }
}

async fn delete_rows(ctx: &DriverContext, keys: Vec<Vec<u8>>) -> Result<(), ArtifactError> {
    let deletes = (keys.into_iter())
        .map(|key| (PLUGIN_ARTIFACT_KEYSPACE.to_string(), key.into()))
        .collect();
    let delete = StorageEffect::BatchDelete {
        deletes,
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(delete).await {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        other => Err(ArtifactError::Storage(format!("{other:?}"))),
    }
}

#[cfg(test)]
#[path = "copies_tests.rs"]
mod tests;
