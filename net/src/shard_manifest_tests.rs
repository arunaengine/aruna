//! Tests the shard manifest frames, pages and response checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::streams::BiStream;
use crate::{DiscoveryMethod, InboundEventHandler, NetConfig, NetHandle, RelayMethod};
use aruna_core::document::{
    DocumentSyncRevision, DocumentTarget, ShardManifest, ShardManifestEntry,
};
use aruna_core::{NodeId, alpn::Alpn};
use aruna_storage::FjallStorage;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tempfile::tempdir;
use tokio::io::AsyncWriteExt;
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
        shard: 7,
    }
}

#[test]
fn request_round_trips() {
    let mut placement = placement();
    placement.shard = u32::MAX;
    let message = ShardTransportMessage::ManifestRequest {
        realm_id: RealmId::from_bytes([2; 32]),
        placement,
    };
    let bytes = postcard::to_allocvec(&message).unwrap();
    assert!(bytes.len() <= MAX_REQUEST_SIZE);
    assert_eq!(
        postcard::from_bytes::<ShardTransportMessage>(&bytes).unwrap(),
        message
    );
}

#[test]
fn response_round_trips() {
    let holder = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
    let manifest = ShardManifest {
        placement: placement(),
        holder,
        entries: vec![ShardManifestEntry {
            target: DocumentTarget::MetadataDocumentLifecycle {
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

    let page = partition_manifest(&manifest, MAX_RESPONSE_SIZE)
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
            target: DocumentTarget::MetadataDocumentLifecycle {
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
            target: DocumentTarget::MetadataDocumentLifecycle {
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

    let error = tokio::time::timeout(
        Duration::from_millis(250),
        read_frame::<ShardTransportMessage>(&mut inbound, MAX_REQUEST_SIZE),
    )
    .await
    .expect("oversized header must be rejected without reading its body")
    .expect_err("oversized request must be rejected");
    assert_eq!(error, "shard frame exceeds maximum size");

    client.shutdown().await;
    server.shutdown().await;
}

fn partition_manifest(
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

fn encoded_page_size(page: &ShardManifestPage) -> Result<usize, String> {
    postcard::to_allocvec(&ShardTransportResponse::ManifestPage(Box::new(
        page.clone(),
    )))
    .map(|bytes| bytes.len())
    .map_err(|error| error.to_string())
}

#[tokio::test]
async fn timeout_reports_operation() {
    let error = with_deadline(
        Duration::from_millis(1),
        "reading shard manifest request",
        std::future::pending::<Result<(), String>>(),
    )
    .await
    .expect_err("pending shard IO must time out");

    assert_eq!(error, "timed out reading shard manifest request");
}

mod response_checks {
    use super::*;

    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn placement(shard: u32) -> PlacementRef {
        PlacementRef {
            strategy_id: ulid::Ulid::from_bytes([9; 16]),
            shard,
        }
    }

    fn manifest(holder: NodeId, placement: PlacementRef) -> ShardManifest {
        ShardManifest {
            placement,
            holder,
            entries: Vec::new(),
            cursor: Vec::new(),
            digest: [0; 32],
            updated_at_ms: 0,
        }
    }

    #[test]
    fn response_rejects_placement() {
        let holder = node_id(1);
        let requested = placement(7);
        let received = manifest(holder, placement(8));

        let error = validate_manifest_response(received, holder, requested)
            .expect_err("wrong placement must be rejected");

        assert!(
            error.contains("placement mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn response_rejects_holder() {
        let requested_holder = node_id(1);
        let placement = placement(7);
        let received = manifest(node_id(2), placement);

        let error = validate_manifest_response(received, requested_holder, placement)
            .expect_err("wrong holder must be rejected");

        assert!(
            error.contains("holder mismatch"),
            "unexpected error: {error}"
        );
    }
}
