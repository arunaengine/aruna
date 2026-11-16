//! Builds sync outbox keys and records and the effects that write and drain them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentOperation;
use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord};
use aruna_core::keyspaces::OUTBOX_INDEX_KEYSPACE;
use aruna_core::types::{Key, Value};
use byteview::ByteView;

// Size one drain for several full topic batches; every grouped peer receives every topic.
pub const OUTBOX_DRAIN_SIZE: usize = 4 * aruna_net::document_sync::BATCH_TOPIC_LIMIT;
pub(crate) const ADMIN_OUTBOX_PREFIX: &[u8] = b"document-sync-outbox-v1/admin-operation/";
const DELETE_OUTBOX_PREFIX: &[u8] = b"document-sync-outbox-v1/delete/";
const UPSERT_OUTBOX_PREFIX: &[u8] = b"document-sync-outbox-v1/upsert/";

pub(crate) fn outbox_stream_prefixes() -> [&'static [u8]; 3] {
    [
        ADMIN_OUTBOX_PREFIX,
        DELETE_OUTBOX_PREFIX,
        UPSERT_OUTBOX_PREFIX,
    ]
}

// Sort by kind and ULID; admin records also sort by origin sequence to preserve FIFO order.
pub fn outbox_prefix(event: &DocumentOutboxEvent) -> Key {
    let mut bytes = b"document-sync-outbox-v1/".to_vec();
    bytes.extend_from_slice(event.kind());
    bytes.push(b'/');
    ByteView::from(bytes)
}

pub fn admin_outbox_prefix(node_id: NodeId) -> Key {
    let mut bytes = ADMIN_OUTBOX_PREFIX.to_vec();
    bytes.extend_from_slice(node_id.as_bytes());
    ByteView::from(bytes)
}

pub fn outbox_key(record: &DocumentOutboxRecord) -> Key {
    let mut bytes = outbox_prefix(&record.event).to_vec();
    if let DocumentOutboxEvent::AdminOperation { event, .. } = &record.event {
        bytes.extend_from_slice(event.origin_node_id.as_bytes());
        bytes.extend_from_slice(&event.origin_seq.to_be_bytes());
    }
    bytes.extend_from_slice(&record.outbox_id.to_bytes());
    // Include the target so one event can enqueue distinct lifecycle and registry publishes.
    bytes.extend_from_slice(record.target.storage_key().as_ref());
    ByteView::from(bytes)
}

pub fn revocation_index_entry(record: &DocumentOutboxRecord) -> (String, Key, Value) {
    (
        OUTBOX_INDEX_KEYSPACE.to_string(),
        outbox_key(record),
        Value::from(vec![1u8]),
    )
}

pub(crate) fn revocation_index_key(record: &DocumentOutboxRecord) -> Option<Key> {
    let DocumentOutboxEvent::AdminOperation { event, .. } = &record.event else {
        return None;
    };
    matches!(&event.op, AdminDocumentOperation::ConfigTokenRevoked { .. })
        .then(|| outbox_key(record))
}
