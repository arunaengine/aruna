//! The shard manifest wire format and its exchange over shard streams.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::ops::Range;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::document::{ShardManifest, ShardManifestEntry};
use aruna_core::storage_entries::sync_revision_key;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use irokle::Storage as _;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Instant, timeout};
use tracing::debug;

use crate::NetHandle;
use crate::streams::BiStream;

/// How long one read or write of a shard manifest exchange may take.
pub const SHARD_IO_TIMEOUT: Duration = Duration::from_secs(10);

/// A manifest request has only fixed-size identifiers and integer fields. This
/// small envelope leaves encoding headroom without permitting a large body to
/// be allocated before peer authorization.
pub const MAX_REQUEST_SIZE: usize = 128;

/// A shard manifest can carry one entry per document held in the shard; 16 MiB
/// bounds a large response while still refusing a hostile oversized frame.
pub const MAX_RESPONSE_SIZE: usize = 16 * 1024 * 1024;

/// New-holder request: give me your paged manifest for this shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub enum ShardTransportMessage {
    ManifestRequest {
        realm_id: RealmId,
        placement: PlacementRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ShardManifestPage {
    pub placement: PlacementRef,
    pub holder: NodeId,
    pub cursor: Vec<u8>,
    pub digest: [u8; 32],
    pub updated_at_ms: u64,
    pub entry_count: u64,
    pub entry_digest: [u8; 32],
    pub page_index: u32,
    pub last: bool,
    pub entries: Vec<ShardManifestEntry>,
}

/// Co-holder reply: a rejection (foreign realm, untrusted peer, or a shard the
/// responder does not hold), or one page of the assembled manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub enum ShardTransportResponse {
    Reject(String),
    ManifestPage(Box<ShardManifestPage>),
}

struct ManifestPagePlan {
    entry_count: u64,
    entry_digest: [u8; 32],
    ranges: Vec<Range<usize>>,
}

#[derive(Serialize)]
enum BorrowedTransportResponse<'a> {
    #[allow(dead_code)]
    Reject,
    ManifestPage(BorrowedManifestPage<'a>),
}

#[derive(Serialize)]
struct BorrowedManifestPage<'a> {
    placement: PlacementRef,
    holder: NodeId,
    cursor: &'a [u8],
    digest: [u8; 32],
    updated_at_ms: u64,
    entry_count: u64,
    entry_digest: [u8; 32],
    page_index: u32,
    last: bool,
    entries: &'a [ShardManifestEntry],
}

fn plan_manifest_pages(
    manifest: &ShardManifest,
    max_size: usize,
) -> Result<ManifestPagePlan, String> {
    let entry_count = u64::try_from(manifest.entries.len())
        .map_err(|_| "shard manifest entry count exceeds protocol range".to_string())?;
    let entry_digest = manifest_entry_digest(&manifest.entries);
    let entry_sizes = manifest
        .entries
        .iter()
        .map(serialized_size)
        .collect::<Result<Vec<_>, _>>()?;
    let empty_entries_size = serialized_size(&Vec::<ShardManifestEntry>::new())?;

    let mut ranges = Vec::new();
    let mut start = 0;
    loop {
        let page_index = u32::try_from(ranges.len())
            .map_err(|_| "shard manifest requires too many pages".to_string())?;
        let empty_page_size = serialized_size(&borrowed_page_response(
            manifest,
            entry_count,
            entry_digest,
            page_index,
            false,
            &[],
        ))?;
        let fixed_size = empty_page_size
            .checked_sub(empty_entries_size)
            .ok_or_else(|| "invalid shard manifest page encoding".to_string())?;
        if fixed_size
            .checked_add(encoded_len(0)?)
            .is_none_or(|size| size > max_size)
        {
            return Err("shard manifest page metadata exceeds maximum size".to_string());
        }

        let mut end = start;
        let mut entries_size = 0usize;
        while end < manifest.entries.len() {
            entries_size = entries_size
                .checked_add(entry_sizes[end])
                .ok_or_else(|| "shard manifest page size overflow".to_string())?;
            let page_len = end - start + 1;
            let candidate_size = fixed_size
                .checked_add(encoded_len(page_len)?)
                .and_then(|size| size.checked_add(entries_size))
                .ok_or_else(|| "shard manifest page size overflow".to_string())?;
            if candidate_size > max_size {
                break;
            }
            end += 1;
        }
        if end == start && start < manifest.entries.len() {
            return Err("shard manifest entry exceeds maximum page size".to_string());
        }

        ranges.push(start..end);
        start = end;
        if start == manifest.entries.len() {
            break;
        }
    }

    for (index, range) in ranges.iter().enumerate() {
        let page_index = u32::try_from(index)
            .map_err(|_| "shard manifest requires too many pages".to_string())?;
        let response = borrowed_page_response(
            manifest,
            entry_count,
            entry_digest,
            page_index,
            index + 1 == ranges.len(),
            &manifest.entries[range.clone()],
        );
        if serialized_size(&response)? > max_size {
            return Err("shard manifest page exceeds maximum size".to_string());
        }
    }

    Ok(ManifestPagePlan {
        entry_count,
        entry_digest,
        ranges,
    })
}

/// The encoded response frames of a planned manifest, one page each.
fn page_frames<'a>(
    manifest: &'a ShardManifest,
    plan: &'a ManifestPagePlan,
) -> impl Iterator<Item = Result<Vec<u8>, String>> + 'a {
    plan.ranges.iter().enumerate().map(|(index, range)| {
        let page_index = u32::try_from(index)
            .map_err(|_| "shard manifest requires too many pages".to_string())?;
        let response = borrowed_page_response(
            manifest,
            plan.entry_count,
            plan.entry_digest,
            page_index,
            index + 1 == plan.ranges.len(),
            &manifest.entries[range.clone()],
        );
        encode_frame(&response, MAX_RESPONSE_SIZE)
    })
}

/// Encodes one frame body, refusing one above `max_size`.
fn encode_frame<T: Serialize>(message: &T, max_size: usize) -> Result<Vec<u8>, String> {
    let bytes = postcard::to_allocvec(message).map_err(|error| error.to_string())?;
    if bytes.len() > max_size {
        return Err("shard message exceeds maximum size".to_string());
    }
    Ok(bytes)
}

fn borrowed_page_response<'a>(
    manifest: &'a ShardManifest,
    entry_count: u64,
    entry_digest: [u8; 32],
    page_index: u32,
    last: bool,
    entries: &'a [ShardManifestEntry],
) -> BorrowedTransportResponse<'a> {
    BorrowedTransportResponse::ManifestPage(BorrowedManifestPage {
        placement: manifest.placement,
        holder: manifest.holder,
        cursor: &manifest.cursor,
        digest: manifest.digest,
        updated_at_ms: manifest.updated_at_ms,
        entry_count,
        entry_digest,
        page_index,
        last,
        entries,
    })
}

fn serialized_size<T: Serialize + ?Sized>(value: &T) -> Result<usize, String> {
    postcard::serialize_with_flavor(value, postcard::ser_flavors::Size::default())
        .map_err(|error| error.to_string())
}

/// Partitions a manifest greedily and deterministically while measuring the
/// actual postcard representation used by response frames.
fn encoded_len(len: usize) -> Result<usize, String> {
    serialized_size(&len)
}

#[derive(Debug, PartialEq, Eq)]
struct ManifestMetadata {
    cursor: Vec<u8>,
    digest: [u8; 32],
    updated_at_ms: u64,
    entry_count: u64,
    entry_digest: [u8; 32],
}

struct ManifestAssembler {
    holder: NodeId,
    placement: PlacementRef,
    metadata: Option<ManifestMetadata>,
    entries: Vec<ShardManifestEntry>,
    next_page: u32,
    complete: bool,
}

impl ManifestAssembler {
    fn new(holder: NodeId, placement: PlacementRef) -> Self {
        Self {
            holder,
            placement,
            metadata: None,
            entries: Vec::new(),
            next_page: 0,
            complete: false,
        }
    }

    fn push(&mut self, page: ShardManifestPage) -> Result<Option<ShardManifest>, String> {
        if self.complete {
            return Err("received shard manifest page after final page".to_string());
        }
        if page.placement != self.placement {
            return Err("shard manifest page placement mismatch".to_string());
        }
        if page.holder != self.holder {
            return Err("shard manifest page holder mismatch".to_string());
        }
        if page.page_index != self.next_page {
            return Err(format!(
                "shard manifest page index mismatch: expected {}, received {}",
                self.next_page, page.page_index
            ));
        }
        if !page.last && page.entries.is_empty() {
            return Err("non-final shard manifest page is empty".to_string());
        }

        let metadata = ManifestMetadata {
            cursor: page.cursor,
            digest: page.digest,
            updated_at_ms: page.updated_at_ms,
            entry_count: page.entry_count,
            entry_digest: page.entry_digest,
        };
        if let Some(expected) = &self.metadata {
            if *expected != metadata {
                return Err("shard manifest page metadata mismatch".to_string());
            }
        } else {
            self.metadata = Some(metadata);
        }
        let Some(metadata) = self.metadata.as_ref() else {
            return Err("shard manifest page metadata is missing".to_string());
        };
        let received_count = u64::try_from(self.entries.len())
            .ok()
            .and_then(|count| u64::try_from(page.entries.len()).ok()?.checked_add(count))
            .ok_or_else(|| "shard manifest entry count overflow".to_string())?;
        if received_count > metadata.entry_count {
            return Err("shard manifest contains more entries than declared".to_string());
        }
        self.entries.extend(page.entries);

        if !page.last {
            self.next_page = self
                .next_page
                .checked_add(1)
                .ok_or_else(|| "shard manifest page index overflow".to_string())?;
            return Ok(None);
        }
        if received_count != metadata.entry_count {
            return Err("final shard manifest page has incomplete entry count".to_string());
        }
        if manifest_entry_digest(&self.entries) != metadata.entry_digest {
            return Err("shard manifest entry digest mismatch".to_string());
        }

        let Some(metadata) = self.metadata.take() else {
            return Err("shard manifest page metadata is missing".to_string());
        };
        self.complete = true;
        Ok(Some(ShardManifest {
            placement: self.placement,
            holder: self.holder,
            entries: std::mem::take(&mut self.entries),
            cursor: metadata.cursor,
            digest: metadata.digest,
            updated_at_ms: metadata.updated_at_ms,
        }))
    }
}

/// A digest of the entries that does not depend on their order.
pub fn manifest_entry_digest(entries: &[ShardManifestEntry]) -> [u8; 32] {
    let mut encoded_entries: Vec<Vec<u8>> = entries.iter().map(canonical_entry_bytes).collect();
    encoded_entries.sort();

    let mut hasher = blake3::Hasher::new();
    hasher.update(&(encoded_entries.len() as u64).to_be_bytes());
    for encoded in encoded_entries {
        hasher.update(&(encoded.len() as u64).to_be_bytes());
        hasher.update(&encoded);
    }
    *hasher.finalize().as_bytes()
}

fn canonical_entry_bytes(entry: &ShardManifestEntry) -> Vec<u8> {
    let target_key = sync_revision_key(&entry.target);
    let target_key = target_key.as_ref();
    let mut bytes = Vec::with_capacity(4 + target_key.len() + 8 + 16 + 32 + 8);
    bytes.extend_from_slice(&(target_key.len() as u32).to_be_bytes());
    bytes.extend_from_slice(target_key);
    bytes.extend_from_slice(&entry.revision.generation.to_be_bytes());
    bytes.extend_from_slice(&entry.revision.event_id.to_bytes());
    bytes.extend_from_slice(entry.revision.actor.as_bytes());
    bytes.extend_from_slice(&entry.revision.updated_at_ms.to_be_bytes());
    bytes
}

fn validate_manifest_response(
    manifest: ShardManifest,
    node_id: NodeId,
    placement: PlacementRef,
) -> Result<ShardManifest, String> {
    if manifest.placement != placement {
        return Err(format!(
            "shard manifest placement mismatch: requested {placement:?}, received {:?}",
            manifest.placement
        ));
    }
    if manifest.holder != node_id {
        return Err(format!(
            "shard manifest holder mismatch: requested {node_id}, received {}",
            manifest.holder
        ));
    }
    Ok(manifest)
}

impl NetHandle {
    /// Fetches a co-holder's paged manifest for one shard over the shard ALPN and assembles it.
    /// A `Reject` from the co-holder (foreign realm, untrusted peer, or a shard it does not
    /// hold) surfaces as an error.
    pub async fn fetch_shard_manifest(
        &self,
        node_id: NodeId,
        realm_id: RealmId,
        placement: PlacementRef,
    ) -> Result<ShardManifest, String> {
        let mut stream = self
            .open_stream(node_id, Alpn::Shard)
            .await
            .map_err(|error| error.to_string())?;
        let request = ShardTransportMessage::ManifestRequest {
            realm_id,
            placement,
        };
        let frame = encode_frame(&request, MAX_REQUEST_SIZE)?;
        timeout(SHARD_IO_TIMEOUT, write_frame(&mut stream, &frame))
            .await
            .map_err(|_| "timed out writing shard message".to_string())??;
        stream.0.finish().map_err(|error| error.to_string())?;

        let deadline = Instant::now() + SHARD_IO_TIMEOUT;
        let mut assembler = ManifestAssembler::new(node_id, placement);
        loop {
            let response = match read_response_before(&mut stream, deadline).await {
                Ok(response) => response,
                Err(error) => {
                    close_shard_stream(&mut stream).await;
                    return Err(error);
                }
            };
            match response {
                ShardTransportResponse::ManifestPage(page) => match assembler.push(*page) {
                    Ok(Some(manifest)) => {
                        close_shard_stream(&mut stream).await;
                        return validate_manifest_response(manifest, node_id, placement);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        close_shard_stream(&mut stream).await;
                        return Err(error);
                    }
                },
                ShardTransportResponse::Reject(reason) => {
                    close_shard_stream(&mut stream).await;
                    return Err(reason);
                }
            }
        }
    }

    /// The shard topic's fingerprint and encoded clock. Irokle returns empty facts for a
    /// genesis-less topic and a zero digest on storage errors, so callers check existence apart.
    pub fn shard_topic_facts(&self, topic: irokle::TopicId) -> ([u8; 32], Vec<u8>) {
        let node = self.document_sync_node();
        let digest = node
            .sync_fingerprint(topic)
            .map(|fingerprint| fingerprint.fingerprint)
            .unwrap_or([0u8; 32]);
        let cursor = node
            .storage()
            .actor_clock(&topic)
            .ok()
            .and_then(|clock| postcard::to_allocvec(&clock).ok())
            .unwrap_or_default();
        (digest, cursor)
    }

    /// Hashes the topic lineage and the exact operation at every required frontier
    /// position. Later writes can extend the topic, but cannot change this root.
    pub fn shard_frontier_root(
        &self,
        topic: irokle::TopicId,
        frontier: &irokle::ActorClock,
    ) -> Result<[u8; 32], String> {
        let node = self.document_sync_node();
        let storage = node.storage();
        let state = storage
            .topic_state(&topic)
            .map_err(|error| format!("failed to read shard topic state: {error}"))?
            .ok_or_else(|| "shard topic has no genesis".to_string())?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"aruna-shard-frontier-root-v1");
        hasher.update(state.genesis.as_bytes());
        hasher.update(&(frontier.iter().count() as u64).to_be_bytes());
        for (actor_id, actor_seq) in frontier.iter() {
            let op_id = storage
                .actor_index(&topic, actor_id, *actor_seq)
                .map_err(|error| format!("failed to read shard frontier operation: {error}"))?
                .ok_or_else(|| {
                    format!(
                        "shard frontier operation missing for actor {actor_id} sequence {actor_seq}"
                    )
                })?;
            hasher.update(actor_id.as_bytes());
            hasher.update(&actor_seq.to_be_bytes());
            hasher.update(op_id.as_bytes());
        }
        Ok(*hasher.finalize().as_bytes())
    }
}

async fn read_response_before(
    stream: &mut BiStream,
    deadline: Instant,
) -> Result<ShardTransportResponse, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    timeout(remaining, read_frame(stream, MAX_RESPONSE_SIZE))
        .await
        .map_err(|_| "timed out waiting for shard response".to_string())?
}

/// Ends both directions of a shard stream.
pub async fn close_shard_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}

/// Reads the request that opens an inbound shard stream.
pub async fn read_manifest_request(stream: &mut BiStream) -> Result<ShardTransportMessage, String> {
    let reading = "reading shard manifest request";
    with_deadline(
        SHARD_IO_TIMEOUT,
        reading,
        read_frame(stream, MAX_REQUEST_SIZE),
    )
    .await
}

/// Answers a request with the manifest, page by page, or with why it cannot be paged.
pub async fn write_manifest(stream: &mut BiStream, manifest: &ShardManifest) -> Result<(), String> {
    let writing = "writing shard manifest response";
    with_deadline(SHARD_IO_TIMEOUT, writing, write_pages(stream, manifest)).await
}

/// Answers a request with a refusal.
pub async fn write_rejection(stream: &mut BiStream, reason: String) -> Result<(), String> {
    let writing = "writing shard manifest response";
    with_deadline(SHARD_IO_TIMEOUT, writing, write_reject(stream, reason)).await
}

async fn write_pages(stream: &mut BiStream, manifest: &ShardManifest) -> Result<(), String> {
    let plan = match plan_manifest_pages(manifest, MAX_RESPONSE_SIZE) {
        Ok(plan) => plan,
        Err(error) => return write_reject(stream, error).await,
    };
    debug!(
        strategy = %manifest.placement.strategy_id,
        shard = manifest.placement.shard,
        entries = manifest.entries.len(),
        "Served shard manifest"
    );
    for frame in page_frames(manifest, &plan) {
        write_frame(stream, &frame?).await?;
    }
    Ok(())
}

async fn write_reject(stream: &mut BiStream, reason: String) -> Result<(), String> {
    let response = ShardTransportResponse::Reject(reason);
    let frame = encode_frame(&response, MAX_RESPONSE_SIZE)?;
    write_frame(stream, &frame).await
}

async fn with_deadline<T>(
    duration: Duration,
    operation: &'static str,
    future: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    timeout(duration, future)
        .await
        .unwrap_or_else(|_| Err(format!("timed out {operation}")))
}

async fn write_frame(stream: &mut BiStream, bytes: &[u8]) -> Result<(), String> {
    stream
        .0
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|error| error.to_string())?;
    stream
        .0
        .write_all(bytes)
        .await
        .map_err(|error| error.to_string())?;
    stream.0.flush().await.map_err(|error| error.to_string())
}

async fn read_frame<T: DeserializeOwned>(
    stream: &mut BiStream,
    max_size: usize,
) -> Result<T, String> {
    let mut len_buf = [0u8; 4];
    stream
        .1
        .read_exact(&mut len_buf)
        .await
        .map_err(|error| error.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_size {
        return Err("shard frame exceeds maximum size".to_string());
    }
    let mut bytes = vec![0u8; len];
    stream
        .1
        .read_exact(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    postcard::from_bytes(&bytes).map_err(|error| error.to_string())
}

#[cfg(test)]
#[path = "shard_manifest_tests.rs"]
mod tests;
