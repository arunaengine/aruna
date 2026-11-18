//! Keeps published plugin artifacts on management nodes and fetches them by digest elsewhere.
//! Every copy that arrives is checked against its digest and the realm's published manifest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::plugins::{ArtifactBytes, Chunk, PublishedPlugin};
use aruna_core::realm_vault::FetchScope;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_plugins::artifact::{ArtifactDigest, check_artifact};
use thiserror::Error;
use tracing::{debug, warn};

use super::copies::{Assembled, MemoryPermit, reserve};
use super::copy_rows::{
    CopyTotal, KeepChunk, ReadChunk, ReadLocal, SweepCopies, chunk_count, chunk_fits,
};
use super::peer::{ask_peer, peer_answer};
use super::rows::StoreError;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::realm::mutate_placement::{is_management, node_kind};

#[derive(Debug, Error, PartialEq)]
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

impl From<IoError> for ArtifactError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<StoreError> for ArtifactError {
    fn from(error: StoreError) -> Self {
        Self::Storage(error.0)
    }
}

/// Whether `bytes` are exactly the artifact `plugin` publishes: digest, size and manifest.
pub(crate) fn check_published(bytes: &[u8], plugin: &PublishedPlugin) -> Result<(), ArtifactError> {
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

/// Management nodes in realm config order.
pub fn managers(config: &RealmConfigDocument) -> impl Iterator<Item = NodeId> + '_ {
    (config.nodes.iter())
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| node.node_id.parse::<NodeId>().ok())
}

/// A checked artifact and whether it came from another node, so this node should keep it.
#[derive(Debug, PartialEq)]
pub struct FetchedArtifact {
    pub copy: Assembled,
    pub fresh: bool,
}

/// The chunks of one artifact arriving from one management node, with their memory.
struct Arriving {
    node: NodeId,
    total: u64,
    bytes: Vec<u8>,
    memory: MemoryPermit,
}

impl std::fmt::Debug for Arriving {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Arriving({} of {} bytes)", self.bytes.len(), self.total)
    }
}

impl PartialEq for Arriving {
    fn eq(&self, other: &Self) -> bool {
        (self.node, self.total, &self.bytes) == (other.node, other.total, &other.bytes)
    }
}

/// The published artifact with `digest`: this node's copy, else one from a management node, each
/// chunk placed and sized exactly and the whole checked. The answer holds the bytes' memory.
#[derive(Debug, PartialEq)]
pub(crate) struct FetchArtifact {
    plugin: PublishedPlugin,
    peers: VecDeque<NodeId>,
    asked: Option<NodeId>,
    arriving: Option<Arriving>,
    last: String,
    local_read: bool,
}

impl FetchArtifact {
    pub(crate) fn new(
        config: &RealmConfigDocument,
        digest: &str,
        local: Option<NodeId>,
    ) -> Result<Self, ArtifactError> {
        let plugin = (config.plugins.iter())
            .find(|plugin| plugin.digest == digest)
            .ok_or(ArtifactError::NotPublished)?;
        Ok(Self {
            plugin: plugin.clone(),
            peers: managers(config)
                .filter(|node| Some(*node) != local)
                .collect(),
            asked: None,
            arriving: None,
            last: "the realm has no other management node".to_string(),
            local_read: false,
        })
    }

    fn ask(&self, node: NodeId, index: u32) -> Result<IoCall, ArtifactError> {
        let digest = self.plugin.digest.clone();
        let message = MetadataTransportMessage::FetchArtifact { digest, index };
        ask_peer(node, &message).ok_or_else(|| ArtifactError::Storage("encode".to_string()))
    }

    fn next_peer(&mut self) -> Result<Next<FetchedArtifact>, ArtifactError> {
        self.arriving = None;
        let Some(node) = self.peers.pop_front() else {
            return Err(ArtifactError::Unavailable(self.last.clone()));
        };
        self.asked = Some(node);
        Ok(Next::Call(self.ask(node, 0)?))
    }

    fn peer_failed(
        &mut self,
        node: NodeId,
        error: MetadataReadError,
    ) -> Result<Next<FetchedArtifact>, ArtifactError> {
        self.last = format!("{node}: {error:?}");
        self.next_peer()
    }

    /// Takes one chunk; a peer that sends a wrong one is left for the next.
    fn take_chunk(
        &mut self,
        node: NodeId,
        chunk: Chunk,
    ) -> Result<Next<FetchedArtifact>, ArtifactError> {
        let index = self.arriving.as_ref().map_or(0, |arriving| {
            (arriving.bytes.len() as u64).div_ceil(super::copy_rows::CHUNK as u64) as u32
        });
        let placed = chunk.index == index && chunk.digest == self.plugin.digest;
        let same = (self.arriving.as_ref()).is_none_or(|arriving| arriving.total == chunk.total);
        if !placed || !same || !chunk_fits(&chunk) {
            return self.peer_failed(node, MetadataReadError::Forbidden);
        }
        if self.arriving.is_none() {
            let Some(memory) = reserve(chunk.total as usize) else {
                return self.peer_failed(node, MetadataReadError::Unavailable);
            };
            self.arriving = Some(Arriving {
                node,
                total: chunk.total,
                bytes: Vec::with_capacity(chunk.total as usize),
                memory,
            });
        }
        let arriving = self.arriving.as_mut().ok_or(IoError::Unfinished)?;
        arriving.bytes.extend(chunk.bytes.0);
        if index + 1 < chunk_count(arriving.total) {
            return Ok(Next::Call(self.ask(node, index + 1)?));
        }
        let Arriving { bytes, memory, .. } = self.arriving.take().ok_or(IoError::Unfinished)?;
        match check_published(&bytes, &self.plugin) {
            Ok(()) => Ok(Next::Done(FetchedArtifact {
                copy: Assembled::new(bytes, memory),
                fresh: true,
            })),
            Err(error) => {
                self.last = format!("{node}: {error}");
                self.next_peer()
            }
        }
    }
}

impl Operation for FetchArtifact {
    type Output = FetchedArtifact;
    type Error = ArtifactError;

    fn first(&mut self) -> Result<Next<FetchedArtifact>, ArtifactError> {
        Ok(Next::Call(IoCall::child(ReadLocal::new(
            &self.plugin.digest,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<FetchedArtifact>, ArtifactError> {
        if !self.local_read {
            self.local_read = true;
            let local = result.child::<Option<Assembled>, ArtifactError>()??;
            if let Some(copy) = local
                && check_published(&copy.bytes, &self.plugin).is_ok()
            {
                let fresh = false;
                return Ok(Next::Done(FetchedArtifact { copy, fresh }));
            }
            return self.next_peer();
        }
        let node = self.asked.ok_or(IoError::Unfinished)?;
        match peer_answer(node, result)? {
            Some(MetadataTransportMessage::FetchedArtifact { result: Ok(chunk) }) => {
                self.take_chunk(node, chunk)
            }
            Some(MetadataTransportMessage::FetchedArtifact { result: Err(error) }) => {
                self.peer_failed(node, error)
            }
            _ => self.peer_failed(node, MetadataReadError::Unavailable),
        }
    }
}

/// Who may hand this node a copy to keep: only another management node, to a management node.
pub(crate) fn check_store(
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
pub(crate) fn check_fetch(
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

/// This node as a peer request sees it: its realm and id.
pub(crate) type ServingNode = (RealmId, NodeId);

/// The realm config a peer request is decided on.
pub(crate) fn served_config(result: IoResult) -> Result<RealmConfigDocument, MetadataReadError> {
    (result.child::<RealmConfigDocument, GetConfigError>()?)
        .map_err(|_| MetadataReadError::Unavailable)
}

/// The management side of a copy before publishing: checks the peer, keeps the chunk, and
/// completes the copy once all chunks arrived and the whole artifact checks out. A stored but
/// never published copy only takes space.
#[derive(Debug, PartialEq)]
pub(crate) struct ServeStore {
    node: ServingNode,
    peer: NodeId,
    chunk: Option<Chunk>,
    digest: String,
    now_ms: u64,
}

impl ServeStore {
    pub(crate) fn new(node: ServingNode, peer: NodeId, chunk: Chunk, now_ms: u64) -> Self {
        Self {
            node,
            peer,
            digest: chunk.digest.clone(),
            chunk: Some(chunk),
            now_ms,
        }
    }
}

impl Operation for ServeStore {
    type Output = bool;
    type Error = MetadataReadError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginCopies)
    }

    fn first(&mut self) -> Result<Next<bool>, MetadataReadError> {
        let read = GetConfigOperation::new(self.node.0);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, MetadataReadError> {
        let Some(chunk) = self.chunk.take() else {
            let (peer, digest) = (self.peer, &self.digest);
            return match result.child::<bool, ArtifactError>()? {
                Ok(complete) => {
                    debug!(%peer, %digest, complete, "Stored a plugin artifact chunk");
                    Ok(Next::Done(complete))
                }
                Err(ArtifactError::Mismatch) => Err(MetadataReadError::Forbidden),
                Err(_) => Err(MetadataReadError::Unavailable),
            };
        };
        let config = served_config(result)?;
        check_store(&config, self.node.1, self.peer)?;
        let keep = KeepChunk::new(&config, chunk, self.now_ms);
        Ok(Next::Call(IoCall::child(keep)))
    }
}

/// The management side of a fetch: one chunk of a published artifact this node keeps.
#[derive(Debug, PartialEq)]
pub(crate) struct ServeFetch {
    node: ServingNode,
    peer: NodeId,
    digest: String,
    index: u32,
    checked: bool,
}

impl ServeFetch {
    pub(crate) fn new(node: ServingNode, peer: NodeId, digest: String, index: u32) -> Self {
        Self {
            node,
            peer,
            digest,
            index,
            checked: false,
        }
    }
}

impl Operation for ServeFetch {
    type Output = Chunk;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<Chunk>, MetadataReadError> {
        let read = GetConfigOperation::new(self.node.0);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Chunk>, MetadataReadError> {
        if !self.checked {
            let config = served_config(result)?;
            check_fetch(&config, self.node.1, self.peer, &self.digest)?;
            self.checked = true;
            let read = ReadChunk::new(&self.digest, self.index);
            return Ok(Next::Call(IoCall::child(read)));
        }
        match result.child::<Option<(u64, Vec<u8>)>, ArtifactError>()? {
            Ok(Some((total, bytes))) => Ok(Next::Done(Chunk {
                digest: std::mem::take(&mut self.digest),
                index: self.index,
                total,
                bytes: ArtifactBytes(bytes),
            })),
            Ok(None) => Err(MetadataReadError::NotFound),
            Err(_) => Err(MetadataReadError::Unavailable),
        }
    }
}

#[derive(Debug, PartialEq)]
enum PlanPhase {
    Config,
    Sweeping(Box<RealmConfigDocument>),
    Looking,
}

/// What a repair pass does: reclaims abandoned copies, and on a management node answers the
/// config and the published digests this node lacks.
#[derive(Debug, PartialEq)]
pub(crate) struct RepairCopies {
    node: ServingNode,
    now_ms: u64,
    config: Option<RealmConfigDocument>,
    digests: VecDeque<String>,
    current: Option<String>,
    missing: Vec<String>,
    phase: PlanPhase,
}

impl RepairCopies {
    pub(crate) fn new(node: ServingNode, now_ms: u64) -> Self {
        Self {
            node,
            now_ms,
            config: None,
            digests: VecDeque::new(),
            current: None,
            missing: Vec::new(),
            phase: PlanPhase::Config,
        }
    }

    fn look_next(&mut self) -> Result<Next<RepairPlan>, ArtifactError> {
        let Some(digest) = self.digests.pop_front() else {
            let config = self.config.take().ok_or(IoError::Unfinished)?;
            return Ok(Next::Done(RepairPlan {
                config,
                missing: std::mem::take(&mut self.missing),
            }));
        };
        let look = CopyTotal::new(&digest);
        self.current = Some(digest);
        self.phase = PlanPhase::Looking;
        Ok(Next::Call(IoCall::child(look)))
    }
}

/// The realm config of a repair pass and the digests to fetch.
#[derive(Debug, PartialEq)]
pub(crate) struct RepairPlan {
    pub config: RealmConfigDocument,
    pub missing: Vec<String>,
}

impl Operation for RepairCopies {
    type Output = RepairPlan;
    type Error = ArtifactError;

    fn first(&mut self) -> Result<Next<RepairPlan>, ArtifactError> {
        let read = GetConfigOperation::new(self.node.0);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RepairPlan>, ArtifactError> {
        match std::mem::replace(&mut self.phase, PlanPhase::Looking) {
            PlanPhase::Config => {
                let config = (result.child::<RealmConfigDocument, GetConfigError>()?)
                    .map_err(|_| ArtifactError::Unavailable("realm config unavailable".into()))?;
                let sweep = SweepCopies::new(&config, self.now_ms);
                self.phase = PlanPhase::Sweeping(Box::new(config));
                Ok(Next::Call(IoCall::child(sweep)))
            }
            PlanPhase::Sweeping(config) => {
                let swept = result.child::<usize, ArtifactError>()??;
                if swept > 0 {
                    debug!(swept, "Reclaimed abandoned plugin artifact copies");
                }
                if is_management(&config, self.node.1) {
                    let digests: BTreeSet<&String> =
                        config.plugins.iter().map(|plugin| &plugin.digest).collect();
                    self.digests = digests.into_iter().cloned().collect();
                }
                self.config = Some(*config);
                self.look_next()
            }
            PlanPhase::Looking => {
                let digest = self.current.take().ok_or(IoError::Unfinished)?;
                match result.child::<Option<u64>, ArtifactError>()? {
                    Ok(Some(_)) => {}
                    Ok(None) => self.missing.push(digest),
                    Err(error) => {
                        warn!(%digest, %error, "Could not look up a plugin artifact copy");
                    }
                }
                self.look_next()
            }
        }
    }
}

#[cfg(test)]
#[path = "artifacts_tests.rs"]
mod tests;
