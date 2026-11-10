//! Keeps published plugin artifacts on management nodes and fetches them by digest elsewhere.
//! Every copy that arrives is checked against its digest and the realm's published manifest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::plugins::PublishedPlugin;
use aruna_core::realm_vault::FetchScope;
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::artifact::{ArtifactDigest, check_artifact};
use thiserror::Error;
use tracing::{debug, warn};

use super::ArtifactBytes;
use super::copies::{
    Assembled, Chunk, chunk_count, chunk_fits, has_local, read_chunk, read_local, reserve,
    store_chunk, store_local, sweep_copies,
};
use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::MetadataReadError;
use crate::realm::get_config::GetConfigOperation;
use crate::realm::mutate_placement::{is_management, node_kind};

/// How often a node sweeps its copies and a management node repairs missing ones.
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
    #[error("this node holds too many unsettled copies or uses its artifact memory; retry later")]
    Busy,
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
/// checked and kept here. The caller holds the bytes' memory reservation until it drops them.
pub async fn fetch_artifact(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    digest: &str,
) -> Result<Assembled, ArtifactError> {
    let plugin = (config.plugins.iter())
        .find(|plugin| plugin.digest == digest)
        .ok_or(ArtifactError::NotPublished)?;
    if let Some(local) = read_local(ctx, digest).await?
        && check_published(&local.bytes, plugin).is_ok()
    {
        return Ok(local);
    }
    let metadata = (ctx.metadata_handle.as_ref())
        .ok_or_else(|| ArtifactError::Unavailable("metadata transport unavailable".into()))?;
    let local = ctx.net_handle.as_ref().map(|net| net.node_id());
    let mut last = "the realm has no other management node".to_string();
    for node_id in managers(config).filter(|node| Some(*node) != local) {
        match fetch_chunks(metadata, node_id, digest).await {
            Ok(fetched) => match check_published(&fetched.bytes, plugin) {
                Ok(()) => {
                    store_local(ctx, config, digest, &fetched.bytes, now()).await?;
                    return Ok(fetched);
                }
                Err(error) => last = format!("{node_id}: {error}"),
            },
            Err(error) => last = format!("{node_id}: {error:?}"),
        }
    }
    Err(ArtifactError::Unavailable(last))
}

/// Every chunk of an artifact from one management node, in order, each placed and sized
/// exactly; the whole is checked by the caller. The memory it needs is reserved before it
/// arrives and travels with the bytes.
async fn fetch_chunks(
    metadata: &crate::metadata::MetadataHandle,
    node_id: NodeId,
    digest: &str,
) -> Result<Assembled, MetadataReadError> {
    let first = metadata
        .request_artifact(node_id, digest.to_string(), 0)
        .await?;
    let fits =
        |chunk: &Chunk, index| chunk.index == index && chunk.digest == digest && chunk_fits(chunk);
    if !fits(&first, 0) {
        return Err(MetadataReadError::Forbidden);
    }
    let memory = reserve(first.total as usize).ok_or(MetadataReadError::Unavailable)?;
    let mut bytes = Vec::with_capacity(first.total as usize);
    bytes.extend(first.bytes.0);
    for index in 1..chunk_count(first.total) {
        let chunk = metadata
            .request_artifact(node_id, digest.to_string(), index)
            .await?;
        if chunk.total != first.total || !fits(&chunk, index) {
            return Err(MetadataReadError::Forbidden);
        }
        bytes.extend(chunk.bytes.0);
    }
    Ok(Assembled::new(bytes, memory))
}

/// This node's wall time, which copy retention is measured in.
fn now() -> u64 {
    unix_timestamp_millis()
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
    match store_chunk(ctx, &config, chunk, now()).await {
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
    match read_chunk(ctx, &digest, index).await {
        Ok(Some((total, bytes))) => Ok(Chunk {
            digest,
            index,
            total,
            bytes: ArtifactBytes(bytes),
        }),
        Ok(None) => Err(MetadataReadError::NotFound),
        Err(_) => Err(MetadataReadError::Unavailable),
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
    let swept = sweep_copies(ctx, &config, now()).await?;
    if swept > 0 {
        debug!(swept, "Reclaimed abandoned plugin artifact copies");
    }
    if !is_management(&config, local) {
        return Ok(0);
    }
    let digests: BTreeSet<&str> = config.plugins.iter().map(|p| p.digest.as_str()).collect();
    let mut copied = 0;
    for digest in digests {
        match has_local(ctx, digest).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => {
                warn!(%digest, %error, "Could not look up a plugin artifact copy");
                continue;
            }
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
