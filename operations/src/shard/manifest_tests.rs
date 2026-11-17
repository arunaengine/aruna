//! Tests the shard manifest scan over pages and failures, and the order-free entry digest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::{DocumentSyncRevision, DocumentTarget};
use aruna_core::errors::StorageError;
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

fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
    IoResult::Page { rows, next }
}

fn call(after: Option<Key>) -> Next<Vec<ShardManifestEntry>> {
    Next::Call(IoCall::Page {
        space: SHARD_MANIFEST_KEYSPACE.to_string(),
        prefix: ByteView::from(shard_manifest_prefix(&placement())),
        after,
        limit: MANIFEST_SCAN_PAGE,
    })
}

#[test]
fn scans_every_page() {
    // The scan follows the cursor under the shard prefix and keeps every entry in order.
    let mut operation = ManifestScanOperation::new(&placement());
    assert_eq!(operation.first(), Ok(call(None)));
    let cursor = Key::from(vec![1u8]);
    let answer = operation.next(page(vec![row(1)], Some(cursor.clone())));
    assert_eq!(answer, Ok(call(Some(cursor))));
    let answer = operation.next(page(vec![row(2)], None));
    assert_eq!(answer, Ok(Next::Done(vec![entry(1), entry(2)])));
}

#[test]
fn scan_failures_end() {
    // An undecodable row or an answer that is not a page ends the scan with the reason.
    let mut operation = ManifestScanOperation::new(&placement());
    operation.first().unwrap();
    let broken = (Key::from(vec![1u8]), Value::from(vec![0xff; 3]));
    let answer = operation.next(page(vec![broken], None));
    assert!(matches!(answer, Err(ManifestScanError::Decode(_))));
    let mut operation = ManifestScanOperation::new(&placement());
    operation.first().unwrap();
    assert!(matches!(
        operation.next(IoResult::Done),
        Err(ManifestScanError::Io(IoError::Unexpected { .. }))
    ));
}

#[test]
fn runner_errors_carry() {
    // A storage failure the runner reports keeps its cause.
    let error = ManifestScanError::from(IoError::Storage(StorageError::Timeout));
    assert_eq!(
        error,
        ManifestScanError::Io(IoError::Storage(StorageError::Timeout))
    );
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
