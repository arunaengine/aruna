//! Prunes stale connector reference rows in the background, one bounded page per transaction,
//! for connectors a dependency check queued. The queue row holds the position to continue at.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, CONNECTOR_PRUNE_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE,
};
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, KeySpace, TxnId, Value};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use crate::connectors::repository::references_connector;
use crate::driver::DriverContext;

/// Reference rows one pruning transaction checks.
const PRUNE_PAGE: usize = 128;
/// Queued connectors one run advances by a page each.
const PRUNE_CONNECTORS: usize = 16;
/// Connector id bytes ahead of the version key in a reference row.
const ROW_PREFIX: usize = 16;

/// Queues pruning of `connector_id`'s rows after `cursor`, the last row a check covered.
pub(crate) fn prune_queue_write(connector_id: Ulid, cursor: Key) -> (KeySpace, Key, Value) {
    (
        CONNECTOR_PRUNE_KEYSPACE.to_string(),
        ByteView::from(connector_id.to_bytes().to_vec()),
        cursor,
    )
}

pub(crate) fn schedule_prune() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::PruneConnectorReferences,
        after: Duration::ZERO,
    })
}

/// Re-arms the pruning task while queued work remains; the queue rows are the durable state.
pub async fn restore_prune_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    match queued(storage, 1).await {
        Ok(rows) if rows.is_empty() => {}
        Ok(_) => {
            if let Event::Task(TaskEvent::Error { message, .. }) =
                task_handle.send_effect(schedule_prune()).await
            {
                warn!(message = %message, "Failed to restore connector reference pruning");
            }
        }
        Err(error) => warn!(error = %error, "Failed to scan connector reference pruning"),
    }
}

/// Advances every queued connector by one page; `true` while queued work remains.
pub async fn prune_references(context: &DriverContext) -> Result<bool, String> {
    let rows = queued(&context.storage_handle, PRUNE_CONNECTORS).await?;
    let more = rows.len() == PRUNE_CONNECTORS;
    let mut remaining = false;
    for (connector, cursor) in rows {
        let Ok(connector_id) = <[u8; 16]>::try_from(connector.as_ref()).map(Ulid::from_bytes)
        else {
            delete_raw(context, connector).await?;
            continue;
        };
        remaining |= prune_page(context, connector_id, cursor).await?;
    }
    Ok(more || remaining)
}

async fn queued(storage: &StorageHandle, limit: usize) -> Result<Vec<(Key, Value)>, String> {
    let iter = StorageEffect::Iter {
        key_space: CONNECTOR_PRUNE_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit,
        txn_id: None,
    };
    match storage.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected pruning queue event: {other:?}")),
    }
}

/// Prunes one page of stale rows after `cursor`. A live reference ends the pruning: the next
/// check meets it at once. `true` while the connector stays queued.
async fn prune_page(context: &DriverContext, connector_id: Ulid, cursor: Value) -> Result<bool, String> {
    let txn_id = match send(context, StorageEffect::StartTransaction { read: false }).await? {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(format!("unexpected transaction event: {other:?}")),
    };
    let result = prune_rows(context, txn_id, connector_id, cursor).await;
    let commit = match result {
        Ok(_) => send(context, StorageEffect::CommitTransaction { txn_id }).await,
        Err(_) => send(context, StorageEffect::AbortTransaction { txn_id }).await,
    };
    let queued = result?;
    match commit? {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(queued),
        other => Err(format!("unexpected commit event: {other:?}")),
    }
}

async fn prune_rows(
    context: &DriverContext,
    txn_id: TxnId,
    connector_id: Ulid,
    cursor: Value,
) -> Result<bool, String> {
    let queue_key = ByteView::from(connector_id.to_bytes().to_vec());
    let iter = StorageEffect::Iter {
        key_space: CONNECTOR_REFERENCE_KEYSPACE.to_string(),
        prefix: Some(queue_key.clone()),
        start: (!cursor.is_empty()).then(|| IterStart::After(cursor)),
        limit: PRUNE_PAGE,
        txn_id: Some(txn_id),
    };
    let (rows, next) = match send(context, iter).await? {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => (values, next_start_after),
        other => return Err(format!("unexpected reference rows event: {other:?}")),
    };
    let reads = rows
        .iter()
        .map(|(row, _)| {
            let version_key = row.get(ROW_PREFIX..).unwrap_or_default();
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                ByteView::from(version_key.to_vec()),
            )
        })
        .collect();
    let versions = match send(context, StorageEffect::BatchRead { reads, txn_id: Some(txn_id) }).await? {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        other => return Err(format!("unexpected version read event: {other:?}")),
    };
    let mut stale = Vec::new();
    let mut live = false;
    for ((row, _), (_, value)) in rows.into_iter().zip(versions) {
        let version = value
            .map(|value| BlobVersion::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|error| error.to_string())?;
        if version.is_some_and(|version| references_connector(&version, connector_id)) {
            live = true;
            break;
        }
        stale.push((CONNECTOR_REFERENCE_KEYSPACE.to_string(), row));
    }
    let queued = !live && next.is_some();
    match next.filter(|_| queued) {
        Some(next) => {
            let (key_space, key, value) = prune_queue_write(connector_id, next);
            write(context, txn_id, key_space, key, value).await?;
        }
        None => stale.push((CONNECTOR_PRUNE_KEYSPACE.to_string(), queue_key)),
    }
    let deletes = StorageEffect::BatchDelete {
        deletes: stale,
        txn_id: Some(txn_id),
    };
    match send(context, deletes).await? {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(queued),
        other => Err(format!("unexpected prune delete event: {other:?}")),
    }
}

async fn write(
    context: &DriverContext,
    txn_id: TxnId,
    key_space: KeySpace,
    key: Key,
    value: Value,
) -> Result<(), String> {
    let write = StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id: Some(txn_id),
    };
    match send(context, write).await? {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected queue write event: {other:?}")),
    }
}

async fn delete_raw(context: &DriverContext, key: Key) -> Result<(), String> {
    let delete = StorageEffect::Delete {
        key_space: CONNECTOR_PRUNE_KEYSPACE.to_string(),
        key,
        txn_id: None,
    };
    send(context, delete).await.map(|_| ())
}

async fn send(context: &DriverContext, effect: StorageEffect) -> Result<Event, String> {
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        event => Ok(event),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::repository::{connector_reference_key, connector_reference_write};
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::storage::blob::VersionKey;
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn context(storage_handle: StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    async fn write_rows(context: &DriverContext, writes: Vec<(KeySpace, Key, Value)>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    async fn count(context: &DriverContext, key_space: &str) -> usize {
        let iter = StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: None,
            limit: 1000,
            txn_id: None,
        };
        match context.storage_handle.send_storage_effect(iter).await {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
            other => panic!("unexpected iteration event: {other:?}"),
        }
    }

    fn live_reference(connector_id: Ulid) -> BlobVersion {
        let binding = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::S3,
                public_config: HashMap::new(),
                source_path: "a".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(connector_id),
        };
        let metadata = SourceMetadata {
            content_length: 1,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let now = SystemTime::UNIX_EPOCH;
        BlobVersion::reference(binding, metadata, now, Default::default(), now)
    }

    // Queued stale rows are pruned page by page until the queue row is gone; a live reference
    // stays and ends the pruning.
    #[tokio::test]
    async fn background_prunes_stale() {
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = context(storage);
        let connector_id = Ulid::from_bytes([2; 16]);
        let mut writes: Vec<_> = (0..PRUNE_PAGE + 5)
            .map(|index| {
                let key = VersionKey::new("bucket", format!("gone-{index}"), Ulid::nil());
                (
                    CONNECTOR_REFERENCE_KEYSPACE.to_string(),
                    connector_reference_key(connector_id, &key).unwrap(),
                    ByteView::from(Vec::new()),
                )
            })
            .collect();
        writes.push(prune_queue_write(connector_id, ByteView::from(Vec::new())));
        write_rows(&context, writes).await;

        let mut runs = 0;
        while prune_references(&context).await.unwrap() {
            runs += 1;
            assert!(runs < 10, "pruning must finish");
        }
        assert_eq!(count(&context, CONNECTOR_REFERENCE_KEYSPACE).await, 0);
        assert_eq!(count(&context, CONNECTOR_PRUNE_KEYSPACE).await, 0);

        let version = live_reference(connector_id);
        let key = VersionKey::new("bucket", "live", Ulid::nil());
        let mut writes = vec![(
            BLOB_VERSIONS_KEYSPACE.to_string(),
            key.to_bytes().unwrap().into(),
            version.to_bytes().unwrap().into(),
        )];
        writes.extend(connector_reference_write(&version, &key).unwrap());
        writes.push(prune_queue_write(connector_id, ByteView::from(Vec::new())));
        write_rows(&context, writes).await;

        assert!(!prune_references(&context).await.unwrap());
        assert_eq!(count(&context, CONNECTOR_REFERENCE_KEYSPACE).await, 1);
        assert_eq!(count(&context, CONNECTOR_PRUNE_KEYSPACE).await, 0);
    }
}
