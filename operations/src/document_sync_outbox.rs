use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE;
use aruna_core::structs::PlacementRef;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, TxnId};
use aruna_core::util::unix_timestamp_secs;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

// Sized so one single-flight drain run fills several full document sync topic-batch
// streams per peer instead of paying the per-run scan/projection/fan-out
// setup for a half-filled one. Records group by peer set, and every peer in
// the set receives every topic in the group, so the cap scales with stream
// capacity rather than peer count.
pub const OUTBOX_DRAIN_BATCH_SIZE: usize =
    4 * aruna_net::document_sync::DOCUMENT_SYNC_BATCH_SYNC_TOPIC_LIMIT;

// Keys order by kind then outbox id (a ULID), with admin operations additionally
// ordered by origin sequence, so drains are FIFO instead of following the random
// blake3 topic id order; the topic stays in the value.
pub fn outbox_prefix(event: &DocumentSyncOutboxEvent) -> Key {
    let mut bytes = b"document-sync-outbox-v1/".to_vec();
    bytes.extend_from_slice(event.kind());
    bytes.push(b'/');
    ByteView::from(bytes)
}

pub fn outbox_key(record: &DocumentSyncOutboxRecord) -> Key {
    let mut bytes = outbox_prefix(&record.event).to_vec();
    if let DocumentSyncOutboxEvent::AdminOperation { event } = &record.event {
        bytes.extend_from_slice(event.origin_node_id.as_bytes());
        bytes.extend_from_slice(&event.origin_seq.to_be_bytes());
    }
    bytes.extend_from_slice(&record.outbox_id.to_bytes());
    ByteView::from(bytes)
}

pub fn new_outbox_record(
    node_id: NodeId,
    target: DocumentSyncTarget,
    peers: Vec<NodeId>,
    event: DocumentSyncOutboxEvent,
    admin_placement: PlacementRef,
    allow_genesis: bool,
) -> DocumentSyncOutboxRecord {
    new_outbox_record_with_id(
        Ulid::new(),
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
pub fn new_outbox_record_with_id(
    outbox_id: Ulid,
    node_id: NodeId,
    target: DocumentSyncTarget,
    mut peers: Vec<NodeId>,
    event: DocumentSyncOutboxEvent,
    admin_placement: PlacementRef,
    allow_genesis: bool,
) -> DocumentSyncOutboxRecord {
    crate::sync_placement::sort_node_ids(&mut peers);
    let placement = match &event {
        DocumentSyncOutboxEvent::Upsert { change, .. }
        | DocumentSyncOutboxEvent::Delete { change } => change.placement,
        DocumentSyncOutboxEvent::AdminOperation { .. } => admin_placement,
    };
    DocumentSyncOutboxRecord {
        outbox_id,
        node_id,
        target,
        peers,
        event,
        placement,
        updated_at: unix_timestamp_secs(),
        allow_genesis,
    }
}

pub fn write_outbox_effect(record: &DocumentSyncOutboxRecord) -> Result<Effect, postcard::Error> {
    write_outbox_effect_with_txn(record, None)
}

pub fn outbox_write_entry(
    record: &DocumentSyncOutboxRecord,
) -> Result<(String, ByteView, ByteView), postcard::Error> {
    Ok((
        DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
        outbox_key(record),
        ByteView::from(postcard::to_allocvec(record)?),
    ))
}

pub fn write_outbox_effect_with_txn(
    record: &DocumentSyncOutboxRecord,
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

pub fn schedule_outbox_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainDocumentSyncOutbox,
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

pub struct OutboxReadBatch {
    pub records: Vec<(Vec<u8>, DocumentSyncOutboxRecord)>,
    pub has_more: bool,
}

pub async fn read_outbox_records(
    storage: &StorageHandle,
    prefix: &[u8],
    limit: usize,
) -> Result<OutboxReadBatch, String> {
    let read_limit = limit.saturating_add(1);
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(prefix.to_vec())),
            start: None,
            limit: read_limit,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            let has_more = values.len() > limit;
            let mut records = Vec::with_capacity(values.len().min(limit));
            for (key, value) in values.into_iter().take(limit) {
                match postcard::from_bytes(&value) {
                    Ok(record) => records.push((key.to_vec(), record)),
                    Err(error) => {
                        let key = key.to_vec();
                        warn!(error = %error, key = ?key, "Deleting malformed document sync outbox record");
                        delete_outbox_records(storage, vec![key]).await?;
                    }
                }
            }
            Ok(OutboxReadBatch { records, has_more })
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub async fn delete_outbox_records(
    storage: &StorageHandle,
    keys: Vec<Vec<u8>>,
) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let deletes = keys
        .into_iter()
        .map(|key| {
            (
                DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
                ByteView::from(key),
            )
        })
        .collect();
    match storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub async fn restore_document_sync_outbox_timers(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
) {
    let event = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await;

    let has_records = match event {
        Event::Storage(StorageEvent::IterResult { values, .. }) => !values.is_empty(),
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan document sync outbox");
            return;
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning document sync outbox");
            return;
        }
    };

    if has_records {
        let event = task_handle
            .schedule_timer_if_idle(TaskKey::DrainDocumentSyncOutbox, Duration::ZERO)
            .await;
        if let TaskEvent::Error { message, .. } = event {
            warn!(message = %message, "Failed to restore document sync outbox timer");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
    };
    use aruna_core::document::{DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncRevision};
    use aruna_core::handle::Handle;
    use aruna_core::structs::{Actor, RealmId};
    use aruna_core::types::UserId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use tempfile::tempdir;

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

    fn user_admin_event(user_id: UserId, origin_seq: u64) -> DocumentSyncOutboxEvent {
        DocumentSyncOutboxEvent::AdminOperation {
            event: Box::new(AdminDocumentEvent {
                event_id: Ulid::from_parts(1, u128::from(origin_seq)),
                target: AdminDocumentTarget::User { user_id },
                origin_node_id: node(1),
                origin_seq,
                observed: AdminDocumentClock::default(),
                actor: Actor {
                    node_id: node(1),
                    user_id,
                    realm_id: user_id.realm_id,
                },
                op: AdminDocumentOperation::UserNameSet {
                    name: format!("user-{origin_seq}"),
                },
            }),
        }
    }

    fn placement(bucket: u32) -> aruna_core::structs::PlacementRef {
        aruna_core::structs::PlacementRef {
            strategy_id: Ulid::from_parts(42, 1),
            epoch: 0,
            bucket,
        }
    }

    fn change() -> DocumentSyncChange {
        DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_parts(8, 1),
                actor: node(1),
                updated_at_ms: 9,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: placement(5),
        }
    }

    fn delete_change() -> DocumentSyncChange {
        DocumentSyncChange {
            kind: DocumentSyncChangeKind::Delete,
            ..change()
        }
    }

    async fn write_raw_outbox_record(storage: &StorageHandle, key: Vec<u8>, value: Vec<u8>) {
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(value),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected outbox write event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn malformed_outbox_record_is_deleted() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let corrupt_key = b"000-corrupt-outbox".to_vec();
        write_raw_outbox_record(&storage, corrupt_key.clone(), vec![1, 2, 3]).await;

        let batch = read_outbox_records(&storage, &[], OUTBOX_DRAIN_BATCH_SIZE)
            .await
            .expect("outbox read succeeds");

        assert!(batch.records.is_empty());
        assert!(!batch.has_more);
        assert_eq!(
            read_outbox_record(&storage, &corrupt_key)
                .await
                .expect("corrupt record was deleted"),
            None
        );
    }

    #[tokio::test]
    async fn malformed_outbox_record_before_valid_is_deleted() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let corrupt_key = b"000-corrupt-outbox".to_vec();
        let valid = new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            DocumentSyncOutboxEvent::Upsert {
                bytes: vec![4, 5],
                change: change(),
            },
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        write_raw_outbox_record(&storage, corrupt_key.clone(), vec![1, 2, 3]).await;
        match storage
            .send_effect(write_outbox_effect(&valid).expect("valid outbox effect"))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected valid outbox write event: {other:?}"),
        }

        let batch = read_outbox_records(&storage, &[], OUTBOX_DRAIN_BATCH_SIZE)
            .await
            .expect("outbox read succeeds");

        assert_eq!(batch.records, vec![(outbox_key(&valid).to_vec(), valid)]);
        assert_eq!(
            read_outbox_record(&storage, &corrupt_key)
                .await
                .expect("corrupt record was deleted"),
            None
        );
    }

    #[test]
    fn outbox_prefix_is_deterministic_and_kind_scoped() {
        let upsert = DocumentSyncOutboxEvent::Upsert {
            bytes: vec![1, 2],
            change: change(),
        };
        let delete = DocumentSyncOutboxEvent::Delete {
            change: delete_change(),
        };

        assert_eq!(outbox_prefix(&upsert), outbox_prefix(&upsert));
        assert_ne!(outbox_prefix(&upsert), outbox_prefix(&delete));
    }

    #[test]
    fn outbox_record_round_trips_and_deduplicates_peers() {
        let peer = node(3);
        let record = new_outbox_record(
            node(1),
            target(),
            vec![peer, peer],
            DocumentSyncOutboxEvent::Upsert {
                bytes: vec![4, 5],
                change: change(),
            },
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        let bytes = postcard::to_allocvec(&record).expect("record serializes");
        let decoded: DocumentSyncOutboxRecord =
            postcard::from_bytes(&bytes).expect("record decodes");

        assert_eq!(decoded, record);
        assert_eq!(decoded.peers, vec![peer]);
    }

    #[test]
    fn outbox_record_upsert_round_trips_with_revision() {
        let record = new_outbox_record(
            node(1),
            target(),
            vec![node(3)],
            DocumentSyncOutboxEvent::Upsert {
                bytes: vec![4, 5],
                change: change(),
            },
            aruna_core::structs::PlacementRef::NIL,
            true,
        );
        let bytes = postcard::to_allocvec(&record).expect("record serializes");
        let decoded: DocumentSyncOutboxRecord =
            postcard::from_bytes(&bytes).expect("record decodes");

        assert_eq!(decoded, record);
        assert!(decoded.allow_genesis);
        // Upsert mirrors the envelope change's ref, never the admin fallback.
        assert_eq!(decoded.placement, change().placement);
        assert_ne!(decoded.placement, aruna_core::structs::PlacementRef::NIL);
    }

    #[test]
    fn outbox_record_delete_round_trips_with_revision() {
        let record = new_outbox_record(
            node(1),
            target(),
            vec![node(3)],
            DocumentSyncOutboxEvent::Delete {
                change: delete_change(),
            },
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        let bytes = postcard::to_allocvec(&record).expect("record serializes");
        let decoded: DocumentSyncOutboxRecord =
            postcard::from_bytes(&bytes).expect("record decodes");

        assert_eq!(decoded, record);
        assert_eq!(decoded.placement, delete_change().placement);
    }

    #[test]
    fn outbox_key_is_unique_under_kind_prefix() {
        let event = DocumentSyncOutboxEvent::Upsert {
            bytes: vec![1],
            change: change(),
        };
        let left = new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            event.clone(),
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        let right = new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            event,
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        let prefix = outbox_prefix(&left.event);

        assert_ne!(outbox_key(&left), outbox_key(&right));
        assert!(outbox_key(&left).starts_with(prefix.as_ref()));
        assert!(outbox_key(&right).starts_with(prefix.as_ref()));
    }

    #[test]
    fn outbox_keys_order_by_outbox_id_across_targets() {
        let event = DocumentSyncOutboxEvent::Upsert {
            bytes: vec![1],
            change: change(),
        };
        let mut older = new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            event.clone(),
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        older.outbox_id = Ulid::from_parts(1, 0);
        let mut newer = new_outbox_record(
            node(1),
            DocumentSyncTarget::RealmConfig {
                realm_id: RealmId::from_bytes([9u8; 32]),
            },
            vec![node(2)],
            event,
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        newer.outbox_id = Ulid::from_parts(2, 0);

        assert!(outbox_key(&older) < outbox_key(&newer));
    }

    #[test]
    fn admin_outbox_keys_order_by_origin_sequence() {
        let user_id = UserId::local(Ulid::from_parts(7, 1), RealmId::from_bytes([3; 32]));
        let target = DocumentSyncTarget::User { user_id };
        let mut earlier = new_outbox_record(
            node(1),
            target.clone(),
            Vec::new(),
            user_admin_event(user_id, 1),
            placement(9),
            false,
        );
        earlier.outbox_id = Ulid::from_parts(2, 2);
        let mut later = new_outbox_record(
            node(1),
            target,
            Vec::new(),
            user_admin_event(user_id, 2),
            placement(9),
            false,
        );
        later.outbox_id = Ulid::from_parts(1, 1);

        assert!(outbox_key(&earlier) < outbox_key(&later));
        // AdminOperation records take the supplied ref (no envelope change).
        assert_eq!(earlier.placement, placement(9));
    }

    #[test]
    fn outbox_key_is_byte_identical_regardless_of_placement() {
        let user_id = UserId::local(Ulid::from_parts(7, 1), RealmId::from_bytes([3; 32]));
        let event = user_admin_event(user_id, 1);
        let outbox_id = Ulid::from_parts(5, 5);
        let target = DocumentSyncTarget::User { user_id };
        let nil = new_outbox_record_with_id(
            outbox_id,
            node(1),
            target.clone(),
            Vec::new(),
            event.clone(),
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        let bucketed = new_outbox_record_with_id(
            outbox_id,
            node(1),
            target,
            Vec::new(),
            event,
            placement(17),
            false,
        );
        // The FIFO key layout must not depend on the placement field.
        assert_ne!(nil.placement, bucketed.placement);
        assert_eq!(outbox_key(&nil), outbox_key(&bucketed));
    }
}
