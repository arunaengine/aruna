//! Builds sync outbox keys and records and the effects that write and drain them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentOperation;
use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::keyspaces::{OUTBOX_INDEX_KEYSPACE, SYNC_OUTBOX_KEYSPACE};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::time::unix_timestamp_secs;
use aruna_core::types::{Key, TxnId, Value};
use byteview::ByteView;
use ulid::Ulid;

// Size one drain for several full topic batches; every grouped peer receives every topic.
pub const OUTBOX_DRAIN_SIZE: usize = 4 * aruna_net::BATCH_TOPIC_LIMIT;
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

pub fn new_outbox_record(
    node_id: NodeId,
    target: DocumentTarget,
    peers: Vec<NodeId>,
    event: DocumentOutboxEvent,
    admin_placement: PlacementRef,
    allow_genesis: bool,
) -> DocumentOutboxRecord {
    new_identified_record(
        Ulid::generate(),
        node_id,
        target,
        peers,
        event,
        admin_placement,
        allow_genesis,
    )
}

/// `admin_placement` is only consulted for `AdminOperation` records (which carry
/// no envelope change); `Upsert`/`Delete` always take their ref from the event's
/// change so the record and its envelope can never diverge.
pub fn new_identified_record(
    outbox_id: Ulid,
    node_id: NodeId,
    target: DocumentTarget,
    mut peers: Vec<NodeId>,
    event: DocumentOutboxEvent,
    admin_placement: PlacementRef,
    allow_genesis: bool,
) -> DocumentOutboxRecord {
    crate::sync::shard_placement::sort_node_ids(&mut peers);
    let placement = match &event {
        DocumentOutboxEvent::Upsert { change, .. } | DocumentOutboxEvent::Delete { change } => {
            change.placement
        }
        DocumentOutboxEvent::AdminOperation { .. } => admin_placement,
    };
    DocumentOutboxRecord {
        outbox_id,
        node_id,
        target,
        peers,
        event,
        placement,
        // Unfenced by default; a fence-admitted writer stamps its generation.
        generation: 0,
        updated_at: unix_timestamp_secs(),
        allow_genesis,
    }
}

pub fn write_outbox_effect(record: &DocumentOutboxRecord) -> Result<Effect, postcard::Error> {
    write_transaction_effect(record, None)
}

#[cfg(debug_assertions)]
fn record_ledger(record: &DocumentOutboxRecord, key: &[u8], value: &[u8]) {
    use std::io::Write;
    use std::sync::{Mutex, OnceLock};

    let Some(path) = std::env::var_os("ARUNA_TEST_OUTBOX_LEDGER") else {
        return;
    };
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let Ok(_guard) = LOCK.get_or_init(|| Mutex::new(())).lock() else {
        return;
    };
    let key_hash = blake3::hash(key).to_hex();
    let value_hash = blake3::hash(value).to_hex();
    let line = format!(
        "target={:?} key_hash={key_hash} value_hash={value_hash}\n",
        record.target
    );
    let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(std::path::PathBuf::from(path))
    else {
        return;
    };
    let _ = file.write_all(line.as_bytes());
}

pub fn outbox_write_entry(
    record: &DocumentOutboxRecord,
) -> Result<(String, ByteView, ByteView), postcard::Error> {
    let key = outbox_key(record);
    let value = ByteView::from(postcard::to_allocvec(record)?);
    #[cfg(debug_assertions)]
    record_ledger(record, key.as_ref(), value.as_ref());
    Ok((SYNC_OUTBOX_KEYSPACE.to_string(), key, value))
}

pub fn write_transaction_effect(
    record: &DocumentOutboxRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, postcard::Error> {
    let (key_space, key, value) = outbox_write_entry(record)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
}

pub fn schedule_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainSyncOutbox,
        after: Duration::ZERO,
    })
}
