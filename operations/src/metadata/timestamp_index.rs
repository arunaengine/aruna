use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_UPDATED_INDEX_KEYSPACE;
use aruna_core::storage_entries::{metadata_updated_index_key, parse_metadata_updated_index_key};
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::get_metadata_document::load_metadata_record_by_document;
use crate::metadata::repository::StorageReadError;

/// Storage rows scanned per index batch while assembling one enumeration page.
const INDEX_SCAN_BATCH: usize = 256;

/// One page of registry records ordered by `updated_at_ms`, plus stale index
/// keys the caller may sweep.
pub struct UpdatedRecordsPage {
    pub records: Vec<MetadataRegistryRecord>,
    /// Exclusive cursor to resume after; `None` when the window is exhausted.
    pub next_after: Option<Key>,
    /// Index keys whose record moved to a newer datestamp or was deleted. Safe to
    /// delete in a sweep; skipping them keeps this read path read-only.
    pub stale_keys: Vec<Key>,
}

/// Enumerate registry records whose `updated_at_ms` is in `[from_ms, until_ms]`,
/// ascending, up to `limit` records. Local realm only (registry rows are
/// realm-complete on each node).
///
/// Lazy old-key cleanup means an index key can outlive its record's datestamp, so
/// every key is validated against the current record and mismatches are skipped.
/// This never under-lists: the current-datestamp key is always present.
pub async fn enumerate_updated(
    context: &DriverContext,
    from_ms: u64,
    until_ms: u64,
    after: Option<Key>,
    limit: usize,
) -> Result<UpdatedRecordsPage, StorageReadError> {
    let mut records = Vec::new();
    let mut stale_keys = Vec::new();
    let mut start = match after {
        Some(cursor) => IterStart::After(cursor),
        None => IterStart::At(metadata_updated_index_key(from_ms, Ulid::nil())),
    };

    loop {
        let event = context
            .storage_handle
            .send_effect(iter_effect(start.clone()))
            .await;
        let (entries, iter_next) = parse_iter(event)?;
        if entries.is_empty() {
            break;
        }

        for (key, _) in entries {
            let (updated_at_ms, document_id) = parse_metadata_updated_index_key(key.as_ref())
                .map_err(StorageReadError::Conversion)?;
            if updated_at_ms > until_ms {
                return Ok(UpdatedRecordsPage {
                    records,
                    next_after: None,
                    stale_keys,
                });
            }
            match load_metadata_record_by_document(context, document_id).await? {
                Some(record) if record.updated_at_ms == updated_at_ms => {
                    records.push(record);
                    if records.len() >= limit {
                        return Ok(UpdatedRecordsPage {
                            records,
                            next_after: Some(key),
                            stale_keys,
                        });
                    }
                }
                _ => stale_keys.push(key),
            }
        }

        match iter_next {
            Some(next) => start = IterStart::After(next),
            None => break,
        }
    }

    Ok(UpdatedRecordsPage {
        records,
        next_after: None,
        stale_keys,
    })
}

fn iter_effect(start: IterStart) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: METADATA_UPDATED_INDEX_KEYSPACE.to_string(),
        prefix: None,
        start: Some(start),
        limit: INDEX_SCAN_BATCH,
        txn_id: None,
    })
}

fn parse_iter(event: Event) -> Result<(Vec<(Key, Value)>, Option<Key>), StorageReadError> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(StorageError::ReadError)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::repository::create_records_and_outbox_write_entries;
    use aruna_core::NodeId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::{MetadataAuditOperation, MetadataAuditRecord, PlacementRef, RealmId};
    use aruna_storage::storage;
    use tempfile::tempdir;

    fn record(document_id: Ulid, updated_at_ms: u64) -> MetadataRegistryRecord {
        MetadataRegistryRecord {
            realm_id: RealmId([1; 32]),
            group_id: Ulid::from_bytes([2; 16]),
            document_id,
            document_path: format!("doc/{document_id}"),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: "/p".to_string(),
            placement: PlacementRef {
                strategy_id: Ulid::nil(),
                epoch: 0,
                shard: 0,
            },
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms,
            establishing_event_id: Ulid::from_bytes([8; 16]),
            last_event_id: Ulid::from_bytes([9; 16]),
        }
    }

    fn audit(record: &MetadataRegistryRecord) -> MetadataAuditRecord {
        MetadataAuditRecord {
            realm_id: record.realm_id,
            group_id: record.group_id,
            document_id: record.document_id,
            graph_iri: record.graph_iri.clone(),
            user_id: Default::default(),
            node_id: NodeId::from_bytes(&[1u8; 32]).expect("node id"),
            operation: MetadataAuditOperation::Create,
            occurred_at_ms: record.updated_at_ms,
            details: None,
        }
    }

    fn context() -> (DriverContext, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (context, dir)
    }

    async fn store(context: &DriverContext, record: &MetadataRegistryRecord) {
        let writes =
            create_records_and_outbox_write_entries(record, &audit(record), Ulid::generate(), None)
                .unwrap();
        let event = context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn enumerates_in_datestamp_order_within_window() {
        let (context, _dir) = context();
        let early = record(Ulid::from_bytes([10; 16]), 100);
        let mid = record(Ulid::from_bytes([11; 16]), 200);
        let late = record(Ulid::from_bytes([12; 16]), 300);
        store(&context, &late).await;
        store(&context, &early).await;
        store(&context, &mid).await;

        let page = enumerate_updated(&context, 150, 250, None, 10)
            .await
            .unwrap();
        let stamps: Vec<u64> = page.records.iter().map(|r| r.updated_at_ms).collect();
        assert_eq!(stamps, vec![200]);
        assert!(page.stale_keys.is_empty());
    }

    #[tokio::test]
    async fn stale_key_is_skipped_after_reindex() {
        // Writing the record twice leaves the first datestamp's index key stale.
        let (context, _dir) = context();
        let mut record = record(Ulid::from_bytes([20; 16]), 100);
        store(&context, &record).await;
        record.updated_at_ms = 500;
        store(&context, &record).await;

        let page = enumerate_updated(&context, 0, 1000, None, 10)
            .await
            .unwrap();
        let stamps: Vec<u64> = page.records.iter().map(|r| r.updated_at_ms).collect();
        assert_eq!(stamps, vec![500]);
        assert_eq!(page.stale_keys.len(), 1);
    }

    #[tokio::test]
    async fn pagination_resumes_after_cursor() {
        let (context, _dir) = context();
        for seed in 0..5u8 {
            store(
                &context,
                &record(Ulid::from_bytes([seed; 16]), 100 + seed as u64),
            )
            .await;
        }
        let first = enumerate_updated(&context, 0, 1000, None, 2).await.unwrap();
        assert_eq!(first.records.len(), 2);
        let cursor = first.next_after.clone();
        assert!(cursor.is_some());

        let second = enumerate_updated(&context, 0, 1000, cursor, 10)
            .await
            .unwrap();
        assert_eq!(second.records.len(), 3);
        assert!(second.next_after.is_none());
    }
}
