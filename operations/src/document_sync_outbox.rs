use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, TxnId};
use aruna_core::util::unix_timestamp_secs;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

const OUTBOX_RESTORE_PAGE_SIZE: usize = 256;

pub fn outbox_prefix(target: &DocumentSyncTarget, event: &DocumentSyncOutboxEvent) -> Key {
    let mut bytes = b"document-sync-outbox-v1/".to_vec();
    bytes.extend_from_slice(event.kind());
    bytes.push(b'/');
    bytes.extend_from_slice(target.irokle_topic_id().to_string().as_bytes());
    bytes.push(b'/');
    ByteView::from(bytes)
}

pub fn outbox_key(record: &DocumentSyncOutboxRecord) -> Key {
    let mut bytes = outbox_prefix(&record.target, &record.event).to_vec();
    bytes.extend_from_slice(&record.outbox_id.to_bytes());
    ByteView::from(bytes)
}

pub fn new_outbox_record(
    node_id: NodeId,
    target: DocumentSyncTarget,
    mut peers: Vec<NodeId>,
    event: DocumentSyncOutboxEvent,
) -> DocumentSyncOutboxRecord {
    crate::sync_placement::sort_node_ids(&mut peers);
    DocumentSyncOutboxRecord {
        outbox_id: Ulid::new(),
        node_id,
        target,
        peers,
        event,
        updated_at: unix_timestamp_secs(),
    }
}

pub fn write_outbox_effect(record: &DocumentSyncOutboxRecord) -> Result<Effect, postcard::Error> {
    write_outbox_effect_with_txn(record, None)
}

pub fn write_outbox_effect_with_txn(
    record: &DocumentSyncOutboxRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, postcard::Error> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
        key: outbox_key(record),
        value: ByteView::from(postcard::to_allocvec(record)?),
        txn_id,
    }))
}

pub fn schedule_outbox_drain_effect(record: &DocumentSyncOutboxRecord) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainDocumentSyncOutbox {
            prefix: outbox_prefix(&record.target, &record.event).to_vec(),
        },
        after: Duration::ZERO,
    })
}

pub async fn read_outbox_record(
    storage: &StorageHandle,
    key: &[u8],
) -> Result<Option<DocumentSyncOutboxRecord>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
            key: ByteView::from(key.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| postcard::from_bytes(&bytes).map_err(|error| error.to_string()))
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub async fn read_next_outbox_record(
    storage: &StorageHandle,
    prefix: &[u8],
) -> Result<Option<(Vec<u8>, DocumentSyncOutboxRecord, bool)>, String> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(prefix.to_vec())),
            start_after: None,
            limit: 2,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            let mut values = values.into_iter();
            let Some((key, value)) = values.next() else {
                return Ok(None);
            };
            let record = postcard::from_bytes(&value).map_err(|error| error.to_string())?;
            Ok(Some((key.to_vec(), record, values.next().is_some())))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub async fn delete_outbox_record(storage: &StorageHandle, key: &[u8]) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
            key: ByteView::from(key.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub async fn restore_document_sync_outbox_timers(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
) {
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start_after: start_after.take(),
                limit: OUTBOX_RESTORE_PAGE_SIZE,
                txn_id: None,
            })
            .await;

        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(error = %error, "Failed to scan document sync outbox");
                return;
            }
            other => {
                warn!(event = ?other, "Unexpected event while scanning document sync outbox");
                return;
            }
        };

        for (_, value) in values {
            let record = match postcard::from_bytes::<DocumentSyncOutboxRecord>(&value) {
                Ok(record) => record,
                Err(error) => {
                    warn!(error = %error, "Failed to decode document sync outbox record while restoring timers");
                    continue;
                }
            };
            let effect = TaskEffect::ResetTimer {
                key: TaskKey::DrainDocumentSyncOutbox {
                    prefix: outbox_prefix(&record.target, &record.event).to_vec(),
                },
                after: Duration::ZERO,
            };
            let event = task_handle.send_effect(Effect::Task(effect)).await;
            if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = event {
                warn!(message = %message, "Failed to restore document sync outbox timer");
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::RealmId;

    fn node(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn target() -> DocumentSyncTarget {
        DocumentSyncTarget::RealmConfig {
            realm_id: RealmId::from_bytes([7u8; 32]),
        }
    }

    #[test]
    fn outbox_prefix_is_deterministic_and_kind_scoped() {
        let target = target();
        let upsert = DocumentSyncOutboxEvent::Upsert { bytes: vec![1, 2] };
        let delete = DocumentSyncOutboxEvent::Delete;

        assert_eq!(
            outbox_prefix(&target, &upsert),
            outbox_prefix(&target, &upsert)
        );
        assert_ne!(
            outbox_prefix(&target, &upsert),
            outbox_prefix(&target, &delete)
        );
    }

    #[test]
    fn outbox_record_round_trips_and_deduplicates_peers() {
        let peer = node(3);
        let record = new_outbox_record(
            node(1),
            target(),
            vec![peer, peer],
            DocumentSyncOutboxEvent::Upsert { bytes: vec![4, 5] },
        );
        let bytes = postcard::to_allocvec(&record).expect("record serializes");
        let decoded: DocumentSyncOutboxRecord =
            postcard::from_bytes(&bytes).expect("record decodes");

        assert_eq!(decoded, record);
        assert_eq!(decoded.peers, vec![peer]);
    }

    #[test]
    fn outbox_key_is_unique_under_target_prefix() {
        let event = DocumentSyncOutboxEvent::Upsert { bytes: vec![1] };
        let left = new_outbox_record(node(1), target(), vec![node(2)], event.clone());
        let right = new_outbox_record(node(1), target(), vec![node(2)], event);
        let prefix = outbox_prefix(&left.target, &left.event);

        assert_ne!(outbox_key(&left), outbox_key(&right));
        assert!(outbox_key(&left).starts_with(prefix.as_ref()));
        assert!(outbox_key(&right).starts_with(prefix.as_ref()));
    }
}
