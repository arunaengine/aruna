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

/// Reads this node's copy, if it has one.
pub async fn read_local(
    ctx: &DriverContext,
    digest: &str,
) -> Result<Option<Vec<u8>>, ArtifactError> {
    let read = StorageEffect::Read {
        key_space: PLUGIN_ARTIFACT_KEYSPACE.to_string(),
        key: digest.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.map(|v| v.to_vec())),
        other => Err(ArtifactError::Storage(format!("{other:?}"))),
    }
}

/// Writes this node's copy and syncs it to disk before anyone is told it is stored.
pub async fn store_local(
    ctx: &DriverContext,
    digest: &str,
    bytes: &[u8],
) -> Result<(), ArtifactError> {
    let write = StorageEffect::Write {
        key_space: PLUGIN_ARTIFACT_KEYSPACE.to_string(),
        key: digest.as_bytes().to_vec().into(),
        value: bytes.to_vec().into(),
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => return Err(ArtifactError::Storage(format!("{other:?}"))),
    }
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::SyncAll)
        .await
    {
        Event::Storage(StorageEvent::SyncAllFinished) => Ok(()),
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
        match metadata.request_artifact(node_id, digest.to_string()).await {
            Ok(ArtifactBytes(bytes)) => match check_published(&bytes, plugin) {
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

/// The management side of a copy before publishing: checks the peer and the artifact, then
/// keeps it durably. A stored but never published copy only takes space.
pub(crate) async fn serve_store(
    ctx: &DriverContext,
    peer: NodeId,
    artifact: ArtifactBytes,
) -> Result<(), MetadataReadError> {
    let (config, local) = local_config(ctx).await?;
    check_store(&config, local, peer)?;
    let bytes = artifact.0;
    if bytes.len() > MAX_ARTIFACT || check_artifact(&bytes).is_err() {
        return Err(MetadataReadError::Forbidden);
    }
    let digest = ArtifactDigest::of(&bytes);
    store_local(ctx, digest.as_str(), &bytes)
        .await
        .map_err(|_| MetadataReadError::Unavailable)?;
    debug!(%peer, %digest, "Stored a plugin artifact copy");
    Ok(())
}

/// The management side of a fetch by digest.
pub(crate) async fn serve_fetch(
    ctx: &DriverContext,
    peer: NodeId,
    digest: String,
) -> Result<ArtifactBytes, MetadataReadError> {
    let (config, local) = local_config(ctx).await?;
    check_fetch(&config, local, peer, &digest)?;
    match read_local(ctx, &digest).await {
        Ok(Some(bytes)) => Ok(ArtifactBytes(bytes)),
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

