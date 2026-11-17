//! Reads and writes the device replica rows and the installed graph sizes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DEVICE_GRAPH_KEYSPACE, DEVICE_REPLICA_KEYSPACE};
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};

use super::replica::{
    CacheUsageOperation, ListReplicasOperation, ReplicaRecord, replica_entry, replica_key,
};

/// One replica, or `None` for both an absent row and an unreadable one: every
/// caller treats an unreadable row as a document this device does not hold.
pub async fn read_replica(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Option<ReplicaRecord> {
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(bytes), ..
    }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: DEVICE_REPLICA_KEYSPACE.to_string(),
            key: replica_key(document_id),
            txn_id: None,
        })
        .await
    else {
        return None;
    };
    ReplicaRecord::from_bytes(&bytes).ok()
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
            .send_storage_effect(StorageEffect::Delete {
                key_space: DEVICE_REPLICA_KEYSPACE.to_string(),
                key: replica_key(document_id),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::DeleteResult { .. })
    )
}

/// Records how many bytes the graph installed for one document takes.
pub async fn record_graph(context: &Arc<DriverContext>, document_id: Ulid, bytes: u64) {
    if let Event::Storage(StorageEvent::Error { error }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: DEVICE_GRAPH_KEYSPACE.to_string(),
            key: replica_key(document_id),
            value: ByteView::from(bytes.to_be_bytes().to_vec()),
            txn_id: None,
        })
        .await
    {
        warn!(%document_id, error = %error, "Failed to record a graph size");
    }
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
    use crate::device::replica::{ReplicaOrigin, replica_size};

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
        record_graph(&context, held.document_id, 4_000).await;
        assert_eq!(
            cache_usage(&context).await,
            Some(replica_size(&held) + 4_000)
        );
        assert!(delete_replica(&context, held.document_id).await);
        assert_eq!(cache_usage(&context).await, Some(4_000));
    }
}
