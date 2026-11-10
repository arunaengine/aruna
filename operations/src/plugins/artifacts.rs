//! Keeps published plugin artifacts on management nodes and fetches them by digest elsewhere.
//! Every copy that arrives is checked against its digest and the realm's published manifest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PLUGIN_ARTIFACT_KEYSPACE;
use aruna_core::plugins::{MAX_ARTIFACT, PublishedPlugin};
use aruna_core::realm_vault::FetchScope;
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_plugins::artifact::{ArtifactDigest, check_artifact};
use thiserror::Error;
use tracing::{debug, warn};

use super::ArtifactBytes;
use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::MetadataReadError;
use crate::realm::get_config::GetConfigOperation;
use crate::realm::mutate_placement::{is_management, node_kind};

/// Bytes of one artifact chunk; node messages stay far below their 16 MiB limit.
pub const CHUNK: usize = 8 << 20;
/// How often a management node looks for published artifacts it lacks.
const REPAIR_INTERVAL: Duration = Duration::from_secs(600);

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("no published plugin version has this digest")]
    NotPublished,
    #[error("the artifact does not match its published digest, size or manifest")]
    Mismatch,
    #[error("no management node handed out the artifact: {0}")]
    Unavailable(String),
    #[error("local artifact storage failed: {0}")]
    Storage(String),
}

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

fn chunk_key(digest: &str, index: u32) -> Vec<u8> {
    [digest.as_bytes(), b"/", &index.to_be_bytes()].concat()
}

/// The length of this node's copy, once it is complete.
async fn read_total(ctx: &DriverContext, digest: &str) -> Result<Option<u64>, ArtifactError> {
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

/// Whether `bytes` are exactly the artifact `plugin` publishes: digest, size and manifest.
pub fn check_published(bytes: &[u8], plugin: &PublishedPlugin) -> Result<(), ArtifactError> {
    let digest = ArtifactDigest::of(bytes);
    if digest.as_str() != plugin.digest || bytes.len() as u64 != plugin.size {
        return Err(ArtifactError::Mismatch);
    }
    let manifest = check_artifact(bytes).map_err(|_| ArtifactError::Mismatch)?;
    let json = serde_json::to_string(&manifest).map_err(|_| ArtifactError::Mismatch)?;
    match json == plugin.manifest {
        true => Ok(()),
        false => Err(ArtifactError::Mismatch),
    }
}

/// The published artifact with `digest`: this node's copy, else one from a management node,
/// checked and kept here.
pub async fn fetch_artifact(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    digest: &str,
) -> Result<Vec<u8>, ArtifactError> {
    let plugin = (config.plugins.iter())
        .find(|plugin| plugin.digest == digest)
        .ok_or(ArtifactError::NotPublished)?;
    if let Some(bytes) = read_local(ctx, digest).await?
        && check_published(&bytes, plugin).is_ok()
    {
        return Ok(bytes);
    }
    let metadata = (ctx.metadata_handle.as_ref())
        .ok_or_else(|| ArtifactError::Unavailable("metadata transport unavailable".into()))?;
    let local = ctx.net_handle.as_ref().map(|net| net.node_id());
    let mut last = "the realm has no other management node".to_string();
    for node_id in managers(config).filter(|node| Some(*node) != local) {
        match fetch_chunks(metadata, node_id, digest).await {
            Ok(bytes) => match check_published(&bytes, plugin) {
                Ok(()) => {
                    store_local(ctx, digest, &bytes).await?;
                    return Ok(bytes);
                }
                Err(error) => last = format!("{node_id}: {error}"),
            },
            Err(error) => last = format!("{node_id}: {error:?}"),
        }
    }
    Err(ArtifactError::Unavailable(last))
}

/// Every chunk of an artifact from one management node, in order and unchecked.
async fn fetch_chunks(
    metadata: &crate::metadata::MetadataHandle,
    node_id: NodeId,
    digest: &str,
) -> Result<Vec<u8>, MetadataReadError> {
    let first = metadata
        .request_artifact(node_id, digest.to_string(), 0)
        .await?;
    if first.total > MAX_ARTIFACT as u64 {
        return Err(MetadataReadError::Forbidden);
    }
    let mut bytes = first.bytes.0;
    for index in 1..chunk_count(first.total) {
        let chunk = metadata
            .request_artifact(node_id, digest.to_string(), index)
            .await?;
        let fits = bytes.len() + chunk.bytes.0.len() <= first.total as usize;
        if chunk.total != first.total || chunk.index != index || !fits {
            return Err(MetadataReadError::Forbidden);
        }
        bytes.extend(chunk.bytes.0);
    }
    Ok(bytes)
}

/// Management nodes in realm config order.
pub fn managers(config: &RealmConfigDocument) -> impl Iterator<Item = NodeId> + '_ {
    (config.nodes.iter())
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| node.node_id.parse::<NodeId>().ok())
}

/// Who may hand this node a copy to keep: only another management node, to a management node.
pub fn check_store(
    config: &RealmConfigDocument,
    local: NodeId,
    peer: NodeId,
) -> Result<(), MetadataReadError> {
    if !is_management(config, local) {
        return Err(MetadataReadError::Unavailable);
    }
    match is_management(config, peer) {
        true => Ok(()),
        false => Err(MetadataReadError::Forbidden),
    }
}

/// Who may fetch a copy from this node: infrastructure nodes, and only published digests.
pub fn check_fetch(
    config: &RealmConfigDocument,
    local: NodeId,
    peer: NodeId,
    digest: &str,
) -> Result<(), MetadataReadError> {
    if !is_management(config, local) {
        return Err(MetadataReadError::Unavailable);
    }
    if !node_kind(config, peer).is_some_and(|kind| FetchScope::Infrastructure.allows(&kind)) {
        return Err(MetadataReadError::Forbidden);
    }
    match config.plugins.iter().any(|plugin| plugin.digest == digest) {
        true => Ok(()),
        false => Err(MetadataReadError::NotFound),
    }
}

/// The management side of a copy before publishing: checks the peer, keeps the chunk, and
/// completes the copy once all chunks arrived and the whole artifact checks out. A stored
/// but never published copy only takes space.
pub(crate) async fn serve_store(
    ctx: &DriverContext,
    peer: NodeId,
    chunk: Chunk,
) -> Result<bool, MetadataReadError> {
    let (config, local) = local_config(ctx).await?;
    check_store(&config, local, peer)?;
    let digest = chunk.digest.clone();
    match store_chunk(ctx, chunk).await {
        Ok(complete) => {
            debug!(%peer, %digest, complete, "Stored a plugin artifact chunk");
            Ok(complete)
        }
        Err(ArtifactError::Mismatch) => Err(MetadataReadError::Forbidden),
        Err(_) => Err(MetadataReadError::Unavailable),
    }
}

/// The management side of a fetch: one chunk of a published artifact this node keeps.
pub(crate) async fn serve_fetch(
    ctx: &DriverContext,
    peer: NodeId,
    digest: String,
    index: u32,
) -> Result<Chunk, MetadataReadError> {
    let (config, local) = local_config(ctx).await?;
    check_fetch(&config, local, peer, &digest)?;
    let total = match read_total(ctx, &digest).await {
        Ok(Some(total)) if index < chunk_count(total) => total,
        Ok(Some(_)) => return Err(MetadataReadError::NotFound),
        _ => return Err(MetadataReadError::Unavailable),
    };
    match read_row(ctx, chunk_key(&digest, index)).await {
        Ok(Some(bytes)) => Ok(Chunk {
            digest,
            index,
            total,
            bytes: ArtifactBytes(bytes),
        }),
        _ => Err(MetadataReadError::Unavailable),
    }
}

async fn local_config(
    ctx: &DriverContext,
) -> Result<(RealmConfigDocument, NodeId), MetadataReadError> {
    let net = ctx
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let config = drive(GetConfigOperation::new(*net.realm_id()), ctx)
        .await
        .map_err(|_| MetadataReadError::Unavailable)?;
    Ok((config, net.node_id()))
}

/// Copies published artifacts this management node lacks from the other management nodes.
pub async fn repair_artifacts(ctx: &DriverContext) -> Result<usize, ArtifactError> {
    let (config, local) = local_config(ctx)
        .await
        .map_err(|_| ArtifactError::Unavailable("realm config unavailable".into()))?;
    if !is_management(&config, local) {
        return Ok(0);
    }
    let digests: BTreeSet<&str> = config.plugins.iter().map(|p| p.digest.as_str()).collect();
    let mut copied = 0;
    for digest in digests {
        if read_local(ctx, digest).await?.is_some() {
            continue;
        }
        match fetch_artifact(ctx, &config, digest).await {
            Ok(_) => copied += 1,
            Err(error) => warn!(%digest, %error, "A published plugin artifact is missing here"),
        }
    }
    Ok(copied)
}

/// Runs [`repair_artifacts`] every few minutes until shutdown.
pub fn spawn_repair(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(REPAIR_INTERVAL) => {}
            }
            if let Err(error) = repair_artifacts(context.as_ref()).await {
                warn!(%error, "Plugin artifact repair failed");
            }
        }
    });
}

#[cfg(test)]
#[path = "artifacts_tests.rs"]
mod tests;
