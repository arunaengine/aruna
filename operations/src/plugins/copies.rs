//! Keeps artifact copies on this node: serializes their writes and holds the memory that whole
//! copies take. The decisions are the operations in `copy_rows`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugins::Chunk;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

use super::artifacts::ArtifactError;
use super::copy_rows::{
    ChunkStored, CompleteCopy, CopyTotal, ReadChunk, ReadCopy, StoreChunk, StoreCopy, SweepCopies,
};
use crate::driver::{DriverContext, drive};

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

/// Whether this node keeps a complete copy, without reading it.
pub(crate) async fn has_local(ctx: &DriverContext, digest: &str) -> Result<bool, ArtifactError> {
    Ok(drive(CopyTotal::new(digest), ctx).await?.is_some())
}

/// Reads this node's complete copy within the memory budget, or `None` without one.
pub(crate) async fn read_local(
    ctx: &DriverContext,
    digest: &str,
) -> Result<Option<Assembled>, ArtifactError> {
    let Some(total) = drive(CopyTotal::new(digest), ctx).await? else {
        return Ok(None);
    };
    let memory = reserve(total as usize).ok_or(ArtifactError::Busy)?;
    let bytes = drive(ReadCopy::new(digest, total), ctx).await?;
    Ok(Some(Assembled::new(bytes, memory)))
}

/// One chunk of this node's complete copy and the copy's length.
pub(crate) async fn read_chunk(
    ctx: &DriverContext,
    digest: &str,
    index: u32,
) -> Result<Option<(u64, Vec<u8>)>, ArtifactError> {
    drive(ReadChunk::new(digest, index), ctx).await
}

/// Writes this node's whole copy and syncs before anyone is told.
pub(crate) async fn store_local(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    digest: &str,
    bytes: &[u8],
    now_ms: u64,
) -> Result<(), ArtifactError> {
    let _writes = WRITES.lock().await;
    drive(StoreCopy::new(config, digest, bytes, now_ms), ctx).await
}

/// Keeps one chunk of a copy another management node sends. Once every chunk is here and the
/// whole artifact checks out, the copy is done; `true` means it is done and synced now.
pub(crate) async fn store_chunk(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    chunk: Chunk,
    now_ms: u64,
) -> Result<bool, ArtifactError> {
    let _writes = WRITES.lock().await;
    let (digest, total) = (chunk.digest.clone(), chunk.total);
    match drive(StoreChunk::new(config, chunk, now_ms), ctx).await? {
        ChunkStored::Held => Ok(false),
        ChunkStored::Confirmed => Ok(true),
        ChunkStored::AllHere => {
            let _memory = reserve(total as usize).ok_or(ArtifactError::Busy)?;
            drive(CompleteCopy::new(digest, total, now_ms), ctx).await?;
            Ok(true)
        }
    }
}

/// Deletes the copies nobody settled within the grace period; answers how many.
pub(crate) async fn sweep_copies(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> Result<usize, ArtifactError> {
    let _writes = WRITES.lock().await;
    drive(SweepCopies::new(config, now_ms), ctx).await
}

#[cfg(test)]
#[path = "copies_tests.rs"]
mod tests;
