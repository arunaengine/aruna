//! Runs the timestamp index sweep and deletes index keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::types::Key;
use aruna_tasks::shutdown::Shutdown;
use tracing::warn;

use crate::driver::{DriverContext, drive};
use crate::metadata::repository::StorageReadError;
use crate::metadata::timestamp_sweep::{SWEEP_SCAN_LIMIT, TimestampSweepOperation};

const SWEEP_INTERVAL: Duration = Duration::from_secs(300);

/// Runs the sweep on the shutdown supervisor, resuming from the previous pass so
/// no single pass walks the whole index.
pub(crate) fn spawn_index_sweep(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        let mut cursor = None;
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(SWEEP_INTERVAL) => {}
            }
            let sweep = TimestampSweepOperation::new(cursor.take(), SWEEP_SCAN_LIMIT);
            match drive(sweep, &context).await {
                Ok(pass) => cursor = pass.next_after,
                Err(error) => warn!(error = ?error, "Metadata timestamp-index sweep failed"),
            }
        }
    });
}

/// Untransacted batch delete of index keys in one keyspace, returning how many
/// were dropped.
pub(crate) async fn delete_index_keys(
    context: &DriverContext,
    keyspace: &str,
    keys: Vec<Key>,
) -> Result<usize, StorageReadError> {
    if keys.is_empty() {
        return Ok(0);
    }
    let count = keys.len();
    let deletes = keys
        .into_iter()
        .map(|key| (keyspace.to_string(), key))
        .collect();
    let event = context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        }))
        .await;
    match event {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(count),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(
            aruna_core::errors::StorageError::WriteError("unexpected event".to_string()),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::repository::create_outbox_entries;
    use crate::metadata::updated_page::UpdatedPageOperation;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

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
                shard: 0,
            },
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms,
            establishing_event_id: Ulid::from_bytes([8; 16]),
            last_event_id: Ulid::from_bytes([9; 16]),
        }
    }

    fn context() -> (DriverContext, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        (context, dir)
    }

    async fn store(context: &DriverContext, record: &MetadataRegistryRecord) {
        let writes = create_outbox_entries(
            record,
            &crate::tests::metadata::create_audit(record),
            Ulid::generate(),
            None,
        )
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

    // enumerates in datestamp order within the window
    #[tokio::test]
    async fn enumerates_in_order() {
        let (context, _dir) = context();
        let early = record(Ulid::from_bytes([10; 16]), 100);
        let mid = record(Ulid::from_bytes([11; 16]), 200);
        let late = record(Ulid::from_bytes([12; 16]), 300);
        store(&context, &late).await;
        store(&context, &early).await;
        store(&context, &mid).await;

        let page = drive(UpdatedPageOperation::new(150, 250, None, 10), &context)
            .await
            .unwrap();
        let stamps: Vec<u64> = page.records.iter().map(|r| r.updated_at_ms).collect();
        assert_eq!(stamps, vec![200]);
        assert!(page.stale_keys.is_empty());
    }

    // a stale key is skipped after a reindex
    #[tokio::test]
    async fn stale_key_skipped() {
        // Writing the record twice leaves the first datestamp's index key stale.
        let (context, _dir) = context();
        let mut record = record(Ulid::from_bytes([20; 16]), 100);
        store(&context, &record).await;
        record.updated_at_ms = 500;
        store(&context, &record).await;

        let page = drive(UpdatedPageOperation::new(0, 1000, None, 10), &context)
            .await
            .unwrap();
        let stamps: Vec<u64> = page.records.iter().map(|r| r.updated_at_ms).collect();
        assert_eq!(stamps, vec![500]);
        assert_eq!(page.stale_keys.len(), 1);
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let (context, _dir) = context();
        let record = record(Ulid::from_bytes([30; 16]), 700);
        store(&context, &record).await;
        let event = context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::BatchDelete {
                deletes: aruna_core::storage_entries::registry_delete_entries(&record),
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchDeleteResult { .. })
        ));

        let page = drive(UpdatedPageOperation::new(0, u64::MAX, None, 10), &context)
            .await
            .unwrap();
        assert!(page.records.is_empty());
        // A leaked key would surface here as a stale key nothing can supersede.
        assert!(page.stale_keys.is_empty());
    }

    #[tokio::test]
    async fn sweep_deletes_stale() {
        let (context, _dir) = context();
        let mut moved = record(Ulid::from_bytes([40; 16]), 100);
        store(&context, &moved).await;
        moved.updated_at_ms = 900;
        store(&context, &moved).await;
        let current = record(Ulid::from_bytes([41; 16]), 200);
        store(&context, &current).await;

        let pass = drive(
            TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(pass.deleted, 1);
        assert!(pass.next_after.is_none());

        let page = drive(UpdatedPageOperation::new(0, u64::MAX, None, 10), &context)
            .await
            .unwrap();
        let stamps: Vec<u64> = page.records.iter().map(|r| r.updated_at_ms).collect();
        assert_eq!(stamps, vec![200, 900]);
        assert!(page.stale_keys.is_empty());
    }

    // A pass stops at its scan bound and hands back the cursor for the next one.
    #[tokio::test]
    async fn sweep_resumes_passes() {
        let (context, _dir) = context();
        for seed in 0..6u8 {
            let mut record = record(Ulid::from_bytes([50 + seed; 16]), 100 + seed as u64);
            store(&context, &record).await;
            record.updated_at_ms += 1_000;
            store(&context, &record).await;
        }

        let mut cursor = None;
        let mut deleted = 0usize;
        for _ in 0..64 {
            let pass = drive(TimestampSweepOperation::new(cursor.take(), 2), &context)
                .await
                .unwrap();
            deleted += pass.deleted;
            cursor = pass.next_after;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(deleted, 6);
        assert!(cursor.is_none());
    }

    // pagination resumes after the cursor
    #[tokio::test]
    async fn pagination_after_cursor() {
        let (context, _dir) = context();
        for seed in 0..5u8 {
            store(
                &context,
                &record(Ulid::from_bytes([seed; 16]), 100 + seed as u64),
            )
            .await;
        }
        let first = drive(UpdatedPageOperation::new(0, 1000, None, 2), &context)
            .await
            .unwrap();
        assert_eq!(first.records.len(), 2);
        let cursor = first.next_after.clone();
        assert!(cursor.is_some());

        let second = drive(UpdatedPageOperation::new(0, 1000, cursor, 10), &context)
            .await
            .unwrap();
        assert_eq!(second.records.len(), 3);
        assert!(second.next_after.is_none());
    }
}
