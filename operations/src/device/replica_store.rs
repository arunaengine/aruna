//! Reads and writes the device replica rows and measures the replica cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};

use super::replica::{
    CacheUsageOperation, ListReplicasOperation, ReplicaRecord, read_answer, replica_delete,
    replica_entry, replica_read,
};

/// One replica, or `None` for both an absent row and an unreadable one: every
/// caller treats an unreadable row as a document this device does not hold.
pub async fn read_replica(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Option<ReplicaRecord> {
    let answer = context
        .storage_handle
        .send_storage_effect(replica_read(document_id))
        .await;
    read_answer(&answer)
}

pub async fn store_replica(context: &Arc<DriverContext>, replica: &ReplicaRecord) -> bool {
    let Ok((key_space, key, value)) = replica_entry(replica) else {
        warn!(document_id = %replica.document_id, "Failed to encode a device replica");
        return false;
    };
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    )
}

pub async fn delete_replica(context: &Arc<DriverContext>, document_id: Ulid) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(replica_delete(document_id))
            .await,
        Event::Storage(StorageEvent::DeleteResult { .. })
    )
}

/// The bytes this device's replicas and their installed graphs take. A graph whose replica
/// row is gone still counts: nothing removes it locally yet. `None` means a scan failed.
pub async fn cache_usage(context: &Arc<DriverContext>) -> Option<u64> {
    drive(CacheUsageOperation::new(), context)
        .await
        .inspect_err(|error| warn!(error = %error, "Failed to measure the device cache"))
        .ok()
}

/// Every replica this device holds, in document order. `None` means the scan
/// itself failed, which a caller must not read as an empty ledger.
pub async fn list_replicas(context: &Arc<DriverContext>) -> Option<Vec<ReplicaRecord>> {
    drive(ListReplicasOperation::new(), context)
        .await
        .inspect_err(|error| warn!(error = %error, "Failed to scan the device replica ledger"))
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::replica::{ReplicaOrigin, replica_key, replica_size};
    use aruna_core::keyspaces::DEVICE_GRAPH_KEYSPACE;

    // A graph stays counted after its replica row goes: nothing removes it locally yet.
    #[tokio::test]
    async fn counts_installed_graphs() {
        let (_dir, context) = crate::tests::device::context().await;
        let context = Arc::new(context);
        let mut held = ReplicaRecord::new(
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            ReplicaOrigin::Realm,
        );
        held.displayed_jsonld = "x".repeat(100);
        assert!(store_replica(&context, &held).await);
        let size = StorageEffect::Write {
            key_space: DEVICE_GRAPH_KEYSPACE.to_string(),
            key: replica_key(held.document_id),
            value: 4_000u64.to_be_bytes().to_vec().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(size).await;
        assert_eq!(
            cache_usage(&context).await,
            Some(replica_size(&held) + 4_000)
        );
        assert!(delete_replica(&context, held.document_id).await);
        assert_eq!(cache_usage(&context).await, Some(4_000));
    }
}
