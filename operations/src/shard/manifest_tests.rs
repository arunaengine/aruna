//! Tests the shard manifest scan over pages and failures, and the order-free entry digest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::{DocumentSyncRevision, DocumentTarget};
use aruna_core::errors::StorageError;
use aruna_core::operation_check::{check_trace, rejects_events, rejects_finalize, sample_events};
use aruna_core::types::Value;
use ulid::Ulid;

use super::*;

fn placement() -> PlacementRef {
    PlacementRef {
        strategy_id: Ulid::from_bytes([9; 16]),
        shard: 3,
    }
}

fn entry(seed: u8) -> ShardManifestEntry {
    ShardManifestEntry {
        target: DocumentTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([seed; 16]),
        },
        revision: DocumentSyncRevision {
            generation: u64::from(seed),
            event_id: Ulid::from_bytes([seed; 16]),
            actor: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            updated_at_ms: u64::from(seed),
        },
    }
}

fn row(seed: u8) -> (Key, Value) {
    let value = postcard::to_allocvec(&entry(seed)).unwrap();
    (Key::from(vec![seed]), Value::from(value))
}

fn page(values: Vec<(Key, Value)>, next_start_after: Option<Key>) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after,
    })
}

#[test]
fn scans_every_page() {
    // The scan follows the cursor under the shard prefix and keeps every entry in order.
    let mut operation = ManifestScanOperation::new(&placement());
    let first = operation.page(None);
    let next = operation.page(Some(Key::from(vec![1u8])));
    check_trace(
        &mut operation,
        &[first],
        vec![
            (page(vec![row(1)], Some(Key::from(vec![1u8]))), vec![next]),
            (page(vec![row(2)], None), vec![]),
        ],
    );
    assert_eq!(operation.finalize(), Ok(vec![entry(1), entry(2)]));
}

#[test]
fn scan_failures_end() {
    // A storage error or an undecodable row ends the scan with the reason.
    let mut operation = ManifestScanOperation::new(&placement());
    operation.start();
    operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    }));
    assert!(operation.finalize().is_err());
    let mut operation = ManifestScanOperation::new(&placement());
    operation.start();
    let broken = (Key::from(vec![1u8]), Value::from(vec![0xff; 3]));
    assert!(operation.step(page(vec![broken], None)).is_empty());
    assert!(operation.finalize().is_err());
}

#[test]
fn rejects_unexpected_events() {
    // Before start every event ends the scan, without effects.
    let make = || ManifestScanOperation::new(&placement());
    let refusals = rejects_events(make, sample_events, |_| false);
    assert!(refusals.iter().all(|(_, effects, _)| effects.is_empty()));
}

#[test]
fn abort_and_finalize() {
    // The scan only reads: abort has nothing to clean up, and an early finalize fails.
    let mut operation = ManifestScanOperation::new(&placement());
    operation.start();
    assert!(operation.abort().is_empty());
    rejects_finalize(operation);
}

#[test]
fn digest_order_independent() {
    let actor = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
    let first = ShardManifestEntry {
        target: aruna_core::document::DocumentTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([1; 16]),
        },
        revision: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::from_bytes([2; 16]),
            actor,
            updated_at_ms: 3,
        },
    };
    let mut second = first.clone();
    second.target = aruna_core::document::DocumentTarget::MetadataDocumentLifecycle {
        document_id: Ulid::from_bytes([4; 16]),
    };
    let mut changed = second.clone();
    changed.revision.generation += 1;

    assert_eq!(
        manifest_entry_digest(&[first.clone(), second.clone()]),
        manifest_entry_digest(&[second.clone(), first.clone()])
    );
    assert_ne!(
        manifest_entry_digest(&[first, second]),
        manifest_entry_digest(&[changed])
    );
}
