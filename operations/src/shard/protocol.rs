use std::ops::Range;

use aruna_core::NodeId;
use aruna_core::document::{ShardManifest, ShardManifestEntry};
use aruna_core::structs::{PlacementRef, RealmId};
use aruna_net::streams::BiStream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncWriteExt;

use crate::shard::manifest_entry_digest;

/// A manifest request has only fixed-size identifiers and integer fields. This
/// small envelope leaves encoding headroom without permitting a large body to
/// be allocated before peer authorization.
pub const SHARD_MAX_REQUEST_SIZE: usize = 128;

/// A shard manifest can carry one entry per document held in the shard; 16 MiB
/// bounds a large response while still refusing a hostile oversized frame.
pub const SHARD_MAX_RESPONSE_SIZE: usize = 16 * 1024 * 1024;

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

pub(crate) struct ManifestPagePlan {
    entry_count: u64,
    entry_digest: [u8; 32],
    ranges: Vec<Range<usize>>,
}

#[allow(dead_code)]
#[derive(Serialize)]
enum BorrowedShardTransportResponse<'a> {
    Reject,
    ManifestPage(BorrowedShardManifestPage<'a>),
}

#[derive(Serialize)]
struct BorrowedShardManifestPage<'a> {
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

pub(crate) fn plan_manifest_pages(
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

pub(crate) async fn write_manifest_pages(
    stream: &mut BiStream,
    manifest: &ShardManifest,
    plan: &ManifestPagePlan,
) -> Result<(), String> {
    for (index, range) in plan.ranges.iter().enumerate() {
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
        write_frame(stream, &response, SHARD_MAX_RESPONSE_SIZE).await?;
    }
    Ok(())
}

fn borrowed_page_response<'a>(
    manifest: &'a ShardManifest,
    entry_count: u64,
    entry_digest: [u8; 32],
    page_index: u32,
    last: bool,
    entries: &'a [ShardManifestEntry],
) -> BorrowedShardTransportResponse<'a> {
    BorrowedShardTransportResponse::ManifestPage(BorrowedShardManifestPage {
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
#[cfg(test)]
pub(crate) fn partition_manifest(
    manifest: &ShardManifest,
    max_size: usize,
) -> Result<Vec<ShardManifestPage>, String> {
    let plan = plan_manifest_pages(manifest, max_size)?;
    Ok(plan
        .ranges
        .iter()
        .enumerate()
        .map(|(index, range)| {
            manifest_page(
                manifest,
                plan.entry_count,
                plan.entry_digest,
                index as u32,
                index + 1 == plan.ranges.len(),
                manifest.entries[range.clone()].to_vec(),
            )
        })
        .collect())
}

#[cfg(test)]
fn manifest_page(
    manifest: &ShardManifest,
    entry_count: u64,
    entry_digest: [u8; 32],
    page_index: u32,
    last: bool,
    entries: Vec<ShardManifestEntry>,
) -> ShardManifestPage {
    ShardManifestPage {
        placement: manifest.placement,
        holder: manifest.holder,
        cursor: manifest.cursor.clone(),
        digest: manifest.digest,
        updated_at_ms: manifest.updated_at_ms,
        entry_count,
        entry_digest,
        page_index,
        last,
        entries,
    }
}

#[cfg(test)]
fn encoded_page_size(page: &ShardManifestPage) -> Result<usize, String> {
    postcard::to_allocvec(&ShardTransportResponse::ManifestPage(Box::new(
        page.clone(),
    )))
    .map(|bytes| bytes.len())
    .map_err(|error| error.to_string())
}

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

pub(crate) struct ManifestAssembler {
    holder: NodeId,
    placement: PlacementRef,
    metadata: Option<ManifestMetadata>,
    entries: Vec<ShardManifestEntry>,
    next_page: u32,
    complete: bool,
}

impl ManifestAssembler {
    pub(crate) fn new(holder: NodeId, placement: PlacementRef) -> Self {
        Self {
            holder,
            placement,
            metadata: None,
            entries: Vec::new(),
            next_page: 0,
            complete: false,
        }
    }

    pub(crate) fn push(
        &mut self,
        page: ShardManifestPage,
    ) -> Result<Option<ShardManifest>, String> {
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
        let metadata = self.metadata.as_ref().expect("manifest metadata set");
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

        self.complete = true;
        let metadata = self.metadata.take().expect("manifest metadata set");
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

pub async fn write_shard_request(
    stream: &mut BiStream,
    message: &ShardTransportMessage,
) -> Result<(), String> {
    write_frame(stream, message, SHARD_MAX_REQUEST_SIZE).await
}

pub async fn read_shard_request(stream: &mut BiStream) -> Result<ShardTransportMessage, String> {
    read_frame(stream, SHARD_MAX_REQUEST_SIZE).await
}

pub async fn write_shard_response(
    stream: &mut BiStream,
    message: &ShardTransportResponse,
) -> Result<(), String> {
    write_frame(stream, message, SHARD_MAX_RESPONSE_SIZE).await
}

pub async fn read_shard_response(stream: &mut BiStream) -> Result<ShardTransportResponse, String> {
    read_frame(stream, SHARD_MAX_RESPONSE_SIZE).await
}

async fn write_frame<T: Serialize>(
    stream: &mut BiStream,
    message: &T,
    max_size: usize,
) -> Result<(), String> {
    let bytes = postcard::to_allocvec(message).map_err(|err| err.to_string())?;
    if bytes.len() > max_size {
        return Err("shard message exceeds maximum size".to_string());
    }
    stream
        .0
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|err| err.to_string())?;
    stream
        .0
        .write_all(&bytes)
        .await
        .map_err(|err| err.to_string())?;
    stream.0.flush().await.map_err(|err| err.to_string())?;
    Ok(())
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
        .map_err(|err| err.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_size {
        return Err("shard frame exceeds maximum size".to_string());
    }
    let mut bytes = vec![0u8; len];
    stream
        .1
        .read_exact(&mut bytes)
        .await
        .map_err(|err| err.to_string())?;
    postcard::from_bytes(&bytes).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{
        DocumentSyncRevision, DocumentSyncTarget, ShardManifest, ShardManifestEntry,
    };
    use aruna_core::{NodeId, alpn::Alpn};
    use aruna_net::{DiscoveryMethod, InboundEventHandler, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use async_trait::async_trait;
    use std::{sync::Arc, time::Duration};
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use ulid::Ulid;

    #[derive(Debug)]
    struct StreamCapture(mpsc::UnboundedSender<BiStream>);

    #[async_trait]
    impl InboundEventHandler for StreamCapture {
        async fn handle_incoming_stream(&self, _alpn: Alpn, stream: BiStream, _node_id: NodeId) {
            self.0.send(stream).expect("capture shard stream");
        }
    }

    fn placement() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([9; 16]),
            epoch: 0,
            shard: 7,
        }
    }

    #[test]
    fn manifest_request_round_trips() {
        let mut placement = placement();
        placement.epoch = u64::MAX;
        placement.shard = u32::MAX;
        let message = ShardTransportMessage::ManifestRequest {
            realm_id: RealmId::from_bytes([2; 32]),
            placement,
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        assert!(bytes.len() <= SHARD_MAX_REQUEST_SIZE);
        assert_eq!(
            postcard::from_bytes::<ShardTransportMessage>(&bytes).unwrap(),
            message
        );
    }

    #[test]
    fn manifest_response_round_trips() {
        let holder = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let manifest = ShardManifest {
            placement: placement(),
            holder,
            entries: vec![ShardManifestEntry {
                target: DocumentSyncTarget::MetadataDocumentLifecycle {
                    document_id: Ulid::from_bytes([4; 16]),
                },
                revision: DocumentSyncRevision {
                    generation: 3,
                    event_id: Ulid::from_bytes([5; 16]),
                    actor: holder,
                    updated_at_ms: 42,
                },
            }],
            cursor: vec![1, 2, 3],
            digest: [7u8; 32],
            updated_at_ms: 99,
        };
        let reject = ShardTransportResponse::Reject("nope".to_string());
        let bytes = postcard::to_allocvec(&reject).unwrap();
        assert_eq!(bytes[0], 0, "reject variant index changed");
        assert_eq!(
            postcard::from_bytes::<ShardTransportResponse>(&bytes).unwrap(),
            reject
        );

        let page = partition_manifest(&manifest, SHARD_MAX_RESPONSE_SIZE)
            .unwrap()
            .remove(0);
        let response = ShardTransportResponse::ManifestPage(Box::new(page));
        let bytes = postcard::to_allocvec(&response).unwrap();
        assert_eq!(bytes[0], 1, "manifest page must follow reject");
        assert_eq!(
            postcard::from_bytes::<ShardTransportResponse>(&bytes).unwrap(),
            response
        );
    }

    #[test]
    fn tiny_budget_pages() {
        let holder = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let entries = (1..=4)
            .map(|seed| ShardManifestEntry {
                target: DocumentSyncTarget::MetadataDocumentLifecycle {
                    document_id: Ulid::from_bytes([seed; 16]),
                },
                revision: DocumentSyncRevision {
                    generation: u64::from(seed),
                    event_id: Ulid::from_bytes([seed + 4; 16]),
                    actor: holder,
                    updated_at_ms: u64::from(seed),
                },
            })
            .collect::<Vec<_>>();
        let manifest = ShardManifest {
            placement: placement(),
            holder,
            cursor: vec![1, 2, 3],
            digest: [7; 32],
            updated_at_ms: 99,
            entries,
        };
        let entry_digest = crate::shard::manifest_entry_digest(&manifest.entries);
        let sample_page = |entries: Vec<ShardManifestEntry>| ShardManifestPage {
            placement: manifest.placement,
            holder,
            cursor: manifest.cursor.clone(),
            digest: manifest.digest,
            updated_at_ms: manifest.updated_at_ms,
            entry_count: manifest.entries.len() as u64,
            entry_digest,
            page_index: 0,
            last: false,
            entries,
        };
        let budget = postcard::to_allocvec(&ShardTransportResponse::ManifestPage(Box::new(
            sample_page(vec![manifest.entries[0].clone()]),
        )))
        .unwrap()
        .len();
        assert!(
            postcard::to_allocvec(&ShardTransportResponse::ManifestPage(Box::new(
                sample_page(manifest.entries[..2].to_vec()),
            )))
            .unwrap()
            .len()
                > budget
        );

        let pages = partition_manifest(&manifest, budget).expect("manifest pages");
        assert_eq!(pages.len(), manifest.entries.len());
        for (index, page) in pages.iter().enumerate() {
            assert_eq!(page.page_index, index as u32);
            assert_eq!(page.last, index + 1 == pages.len());
            assert!(
                postcard::to_allocvec(&ShardTransportResponse::ManifestPage(Box::new(
                    page.clone(),
                )))
                .unwrap()
                .len()
                    <= budget
            );
        }

        let mut assembler = ManifestAssembler::new(holder, manifest.placement);
        let mut assembled = None;
        for page in pages {
            assembled = assembler.push(page).expect("valid page");
        }
        assert_eq!(assembled, Some(manifest));
    }

    #[test]
    fn invalid_pages_fail() {
        let holder = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let other_holder = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let entries = (1..=2)
            .map(|seed| ShardManifestEntry {
                target: DocumentSyncTarget::MetadataDocumentLifecycle {
                    document_id: Ulid::from_bytes([seed; 16]),
                },
                revision: DocumentSyncRevision {
                    generation: u64::from(seed),
                    event_id: Ulid::from_bytes([seed + 4; 16]),
                    actor: holder,
                    updated_at_ms: u64::from(seed),
                },
            })
            .collect::<Vec<_>>();
        let manifest = ShardManifest {
            placement: placement(),
            holder,
            cursor: vec![1, 2, 3],
            digest: [7; 32],
            updated_at_ms: 99,
            entries,
        };
        let entry_digest = manifest_entry_digest(&manifest.entries);
        let one_entry = manifest_page(
            &manifest,
            manifest.entries.len() as u64,
            entry_digest,
            0,
            false,
            vec![manifest.entries[0].clone()],
        );
        let pages = partition_manifest(&manifest, encoded_page_size(&one_entry).unwrap()).unwrap();
        assert_eq!(pages.len(), 2);
        let first = pages[0].clone();
        let second = pages[1].clone();
        let rejects_second = |changed: ShardManifestPage| {
            let mut assembler = ManifestAssembler::new(holder, manifest.placement);
            assert_eq!(assembler.push(first.clone()).unwrap(), None);
            assert!(assembler.push(changed).is_err());
        };

        let mut changed = second.clone();
        changed.placement.shard += 1;
        rejects_second(changed);
        let mut changed = second.clone();
        changed.holder = other_holder;
        rejects_second(changed);
        let mut changed = second.clone();
        changed.cursor.push(4);
        rejects_second(changed);
        let mut changed = second.clone();
        changed.digest[0] ^= 1;
        rejects_second(changed);
        let mut changed = second.clone();
        changed.updated_at_ms += 1;
        rejects_second(changed);
        let mut changed = second.clone();
        changed.entry_count += 1;
        rejects_second(changed);
        let mut changed = second.clone();
        changed.entry_digest[0] ^= 1;
        rejects_second(changed);
        let mut changed = second.clone();
        changed.page_index += 1;
        rejects_second(changed);
        let mut changed = second;
        changed.entries[0].revision.generation += 1;
        rejects_second(changed);

        let mut early_last = first;
        early_last.last = true;
        let mut assembler = ManifestAssembler::new(holder, manifest.placement);
        assert!(assembler.push(early_last).is_err());
    }

    #[tokio::test]
    async fn oversized_request_rejected() {
        let dir_a = tempdir().expect("client temp dir");
        let dir_b = tempdir().expect("server temp dir");
        let storage_a = FjallStorage::open(dir_a.path().to_str().expect("client temp path"))
            .expect("client storage");
        let storage_b = FjallStorage::open(dir_b.path().to_str().expect("server temp path"))
            .expect("server storage");
        let config = || NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind address"),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        };
        let client = NetHandle::new(config(), storage_a)
            .await
            .expect("client net handle");
        let server = NetHandle::new(config(), storage_b)
            .await
            .expect("server net handle");
        client.add_peer_addr(server.endpoint_addr()).await;
        server.add_peer_addr(client.endpoint_addr()).await;
        let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
        server.set_inbound_handler(Arc::new(StreamCapture(stream_tx)));

        let mut outbound = client
            .open_stream(server.node_id(), Alpn::Shard)
            .await
            .expect("open shard stream");
        outbound
            .0
            .write_all(&1024u32.to_be_bytes())
            .await
            .expect("write oversized request header");
        outbound.0.flush().await.expect("flush request header");
        let mut inbound = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
            .await
            .expect("receive shard stream promptly")
            .expect("capture remains open");

        let error =
            tokio::time::timeout(Duration::from_millis(250), read_shard_request(&mut inbound))
                .await
                .expect("oversized header must be rejected without reading its body")
                .expect_err("oversized request must be rejected");
        assert_eq!(error, "shard frame exceeds maximum size");

        client.shutdown().await;
        server.shutdown().await;
    }
}
