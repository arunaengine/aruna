//! Admin reads and reclamation for the durable sync-quarantine store (#338).
//!
//! Writes come from the replication path, which folds the evidence row and its
//! usage row into the same transaction as the topic cursor. This module owns the
//! other half: listing, inspection, acknowledgement, and bounded pruning of
//! acknowledged rows, each keeping the usage row consistent with the store.

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{SYNC_QUARANTINE_KEYSPACE, SYNC_QUARANTINE_USAGE_KEYSPACE};
use aruna_core::structs::{
    SYNC_QUARANTINE_USAGE_KEY, SyncQuarantineRecord, SyncQuarantineUsage, quarantine_row_entry,
    quarantine_usage_entry,
};
use aruna_core::types::{Key, TxnId, Value};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use thiserror::Error;

use crate::driver::DriverContext;

pub const QUARANTINE_PAGE_DEFAULT: usize = 50;
pub const QUARANTINE_PAGE_MAX: usize = 200;

#[derive(Debug, Error, PartialEq)]
pub enum QuarantineAdminError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected storage event: expected {expected}, got {got}")]
    Unexpected { expected: &'static str, got: String },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QuarantinePageRequest {
    /// Exclusive cursor: the last key of the previous page.
    pub start_after: Option<Vec<u8>>,
    /// Restrict the page to one sync topic, the key's leading component.
    pub topic: Option<Vec<u8>>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantinePage {
    pub records: Vec<SyncQuarantineRecord>,
    pub next_start_after: Option<Vec<u8>>,
    pub usage: SyncQuarantineUsage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantinePruneResult {
    pub pruned: usize,
    pub scanned: usize,
    pub next_start_after: Option<Vec<u8>>,
    pub usage: SyncQuarantineUsage,
}

fn bounded_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(QUARANTINE_PAGE_DEFAULT)
        .clamp(1, QUARANTINE_PAGE_MAX)
}

async fn iter_page(
    storage: &StorageHandle,
    request: &QuarantinePageRequest,
    limit: usize,
) -> Result<(Vec<(Key, Value)>, Option<Key>), QuarantineAdminError> {
    let start = request
        .start_after
        .as_deref()
        .map(|key| IterStart::After(ByteView::from(key)));
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: SYNC_QUARANTINE_KEYSPACE.to_string(),
            prefix: request.topic.as_deref().map(ByteView::from),
            start,
            limit,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        got => Err(QuarantineAdminError::Unexpected {
            expected: "storage iteration result",
            got: format!("{got:?}"),
        }),
    }
}

async fn read_usage_row(
    storage: &StorageHandle,
    txn_id: Option<TxnId>,
) -> Result<SyncQuarantineUsage, QuarantineAdminError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: SYNC_QUARANTINE_USAGE_KEYSPACE.to_string(),
            key: ByteView::from(SYNC_QUARANTINE_USAGE_KEY),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => match value {
            Some(bytes) => Ok(SyncQuarantineUsage::from_bytes(bytes.as_ref())?),
            None => Ok(SyncQuarantineUsage::default()),
        },
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        got => Err(QuarantineAdminError::Unexpected {
            expected: "storage read result",
            got: format!("{got:?}"),
        }),
    }
}

/// The stored value, so accounting uses the exact bytes this read observed.
async fn read_row_value(
    storage: &StorageHandle,
    key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<Value>, QuarantineAdminError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: SYNC_QUARANTINE_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        got => Err(QuarantineAdminError::Unexpected {
            expected: "storage read result",
            got: format!("{got:?}"),
        }),
    }
}

async fn read_row(
    storage: &StorageHandle,
    key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<SyncQuarantineRecord>, QuarantineAdminError> {
    read_row_value(storage, key, txn_id)
        .await?
        .map(|bytes| SyncQuarantineRecord::from_bytes(bytes.as_ref()))
        .transpose()
        .map_err(QuarantineAdminError::Conversion)
}

async fn start_txn(storage: &StorageHandle) -> Result<TxnId, QuarantineAdminError> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        got => Err(QuarantineAdminError::Unexpected {
            expected: "storage transaction started",
            got: format!("{got:?}"),
        }),
    }
}

async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

async fn commit_txn(storage: &StorageHandle, txn_id: TxnId) -> Result<(), QuarantineAdminError> {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        got => Err(QuarantineAdminError::Unexpected {
            expected: "storage transaction committed",
            got: format!("{got:?}"),
        }),
    }
}

async fn write_batch(
    storage: &StorageHandle,
    writes: Vec<(String, Key, Value)>,
    txn_id: TxnId,
) -> Result<(), QuarantineAdminError> {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        got => Err(QuarantineAdminError::Unexpected {
            expected: "storage batch write result",
            got: format!("{got:?}"),
        }),
    }
}

async fn delete_batch(
    storage: &StorageHandle,
    deletes: Vec<(String, Key)>,
    txn_id: TxnId,
) -> Result<(), QuarantineAdminError> {
    match storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        got => Err(QuarantineAdminError::Unexpected {
            expected: "storage batch delete result",
            got: format!("{got:?}"),
        }),
    }
}

pub async fn read_quarantine_usage(
    ctx: &DriverContext,
) -> Result<SyncQuarantineUsage, QuarantineAdminError> {
    read_usage_row(&ctx.storage_handle, None).await
}

/// One bounded page of quarantine evidence in key order
/// (`topic || actor || actor_seq`).
pub async fn list_quarantine_records(
    ctx: &DriverContext,
    request: QuarantinePageRequest,
) -> Result<QuarantinePage, QuarantineAdminError> {
    let (values, next_start_after) =
        iter_page(&ctx.storage_handle, &request, bounded_limit(request.limit)).await?;
    let records = values
        .into_iter()
        .map(|(_, value)| SyncQuarantineRecord::from_bytes(value.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(QuarantinePage {
        records,
        next_start_after: next_start_after.map(|key| key.as_ref().to_vec()),
        usage: read_usage_row(&ctx.storage_handle, None).await?,
    })
}

pub async fn read_quarantine_record(
    ctx: &DriverContext,
    key: &[u8],
) -> Result<Option<SyncQuarantineRecord>, QuarantineAdminError> {
    read_row(&ctx.storage_handle, key, None).await
}

/// Mark one row acknowledged. Idempotent: re-acknowledging an already
/// acknowledged row rewrites nothing and still reports the record.
pub async fn acknowledge_quarantine_row(
    ctx: &DriverContext,
    key: &[u8],
) -> Result<Option<SyncQuarantineRecord>, QuarantineAdminError> {
    let storage = &ctx.storage_handle;
    let txn_id = start_txn(storage).await?;
    let outcome = acknowledge_in_txn(storage, key, txn_id).await;
    match outcome {
        Ok(None) => {
            abort_txn(storage, txn_id).await;
            Ok(None)
        }
        Ok(Some(record)) => {
            commit_txn(storage, txn_id).await?;
            Ok(Some(record))
        }
        Err(error) => {
            abort_txn(storage, txn_id).await;
            Err(error)
        }
    }
}

async fn acknowledge_in_txn(
    storage: &StorageHandle,
    key: &[u8],
    txn_id: TxnId,
) -> Result<Option<SyncQuarantineRecord>, QuarantineAdminError> {
    let Some(mut record) = read_row(storage, key, Some(txn_id)).await? else {
        return Ok(None);
    };
    if record.acknowledged {
        return Ok(Some(record));
    }
    let before = record.to_bytes()?.len() as u64;
    record.acknowledged = true;
    let entry = quarantine_row_entry(&record)?;
    let after = entry.2.len() as u64;
    let mut usage = read_usage_row(storage, Some(txn_id)).await?;
    usage.bytes = usage.bytes.saturating_sub(before).saturating_add(after);
    write_batch(storage, vec![entry, quarantine_usage_entry(usage)?], txn_id).await?;
    Ok(Some(record))
}

/// Delete acknowledged rows in one bounded pass, decrementing the usage row in
/// the same transaction so reclaimed capacity survives a crash. Resumable: the
/// returned cursor continues the scan even when the pass deleted nothing.
///
/// The scan only proposes candidates. Every one is re-read and re-validated
/// inside the delete transaction, so a redelivery that replaced a selected row
/// with unacknowledged evidence between scan and commit is neither deleted nor
/// accounted from its stale value.
pub async fn prune_quarantine_records(
    ctx: &DriverContext,
    request: QuarantinePageRequest,
) -> Result<QuarantinePruneResult, QuarantineAdminError> {
    let storage = &ctx.storage_handle;
    let (values, next_start_after) =
        iter_page(storage, &request, bounded_limit(request.limit)).await?;
    let scanned = values.len();
    let mut candidates = Vec::new();
    for (key, value) in values {
        if SyncQuarantineRecord::from_bytes(value.as_ref())?.acknowledged {
            candidates.push(key);
        }
    }

    let next_start_after = next_start_after.map(|key| key.as_ref().to_vec());
    if candidates.is_empty() {
        return Ok(QuarantinePruneResult {
            pruned: 0,
            scanned,
            next_start_after,
            usage: read_usage_row(storage, None).await?,
        });
    }

    let txn_id = start_txn(storage).await?;
    match prune_in_txn(storage, candidates, txn_id).await {
        Ok((0, usage)) => {
            abort_txn(storage, txn_id).await;
            Ok(QuarantinePruneResult {
                pruned: 0,
                scanned,
                next_start_after,
                usage,
            })
        }
        Ok((pruned, usage)) => {
            commit_txn(storage, txn_id).await?;
            Ok(QuarantinePruneResult {
                pruned,
                scanned,
                next_start_after,
                usage,
            })
        }
        Err(error) => {
            abort_txn(storage, txn_id).await;
            Err(error)
        }
    }
}

async fn prune_in_txn(
    storage: &StorageHandle,
    candidates: Vec<Key>,
    txn_id: TxnId,
) -> Result<(usize, SyncQuarantineUsage), QuarantineAdminError> {
    let mut deletes = Vec::new();
    let mut released = 0u64;
    for key in candidates {
        let Some(value) = read_row_value(storage, key.as_ref(), Some(txn_id)).await? else {
            continue;
        };
        if !SyncQuarantineRecord::from_bytes(value.as_ref())?.acknowledged {
            continue;
        }
        released = released.saturating_add(value.len() as u64);
        deletes.push((SYNC_QUARANTINE_KEYSPACE.to_string(), key));
    }
    let mut usage = read_usage_row(storage, Some(txn_id)).await?;
    if deletes.is_empty() {
        return Ok((0, usage));
    }
    let pruned = deletes.len();
    usage.release(pruned as u64, released);
    delete_batch(storage, deletes, txn_id).await?;
    write_batch(storage, vec![quarantine_usage_entry(usage)?], txn_id).await?;
    Ok((pruned, usage))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncEvent, DocumentSyncRevision,
        DocumentSyncTarget,
    };
    use aruna_core::structs::{
        PlacementRef, RealmId, SyncQuarantineCapacity, SyncQuarantineEvidence,
        SyncQuarantineIdentity, SyncQuarantineInput, SyncQuarantineUsage, build_quarantine_entries,
    };
    use aruna_storage::storage;
    use ulid::Ulid;

    fn context() -> (DriverContext, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
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

    fn event(index: u8) -> DocumentSyncEvent {
        let node = NodeId::from_bytes(&[1u8; 32]).unwrap();
        DocumentSyncEvent::Upsert {
            event_id: Ulid::from_bytes([index; 16]),
            target: DocumentSyncTarget::RealmConfig {
                realm_id: RealmId([9; 32]),
            },
            bytes: vec![index; 8],
            change: DocumentSyncChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: Ulid::from_bytes([index; 16]),
                    actor: node,
                    updated_at_ms: 1,
                },
                kind: DocumentSyncChangeKind::Upsert,
                placement: PlacementRef::NIL,
            },
        }
    }

    fn identity(index: u8) -> SyncQuarantineIdentity {
        SyncQuarantineIdentity::from_parts([7; 32], [8; 32], u64::from(index) + 1)
    }

    async fn seed_rows(ctx: &DriverContext, count: u8) -> SyncQuarantineUsage {
        let mut usage = SyncQuarantineUsage::default();
        for index in 0..count {
            let write = build_quarantine_entries(
                SyncQuarantineInput {
                    identity: identity(index),
                    evidence: SyncQuarantineEvidence::from_event(&event(index)),
                    reason: "invalid",
                    quarantined_at_ms: 42,
                    replaced_bytes: None,
                },
                usage,
                SyncQuarantineCapacity::default(),
            )
            .unwrap();
            usage = write.usage;
            let txn_id = start_txn(&ctx.storage_handle).await.unwrap();
            write_batch(
                &ctx.storage_handle,
                vec![write.row, quarantine_usage_entry(usage).unwrap()],
                txn_id,
            )
            .await
            .unwrap();
            commit_txn(&ctx.storage_handle, txn_id).await.unwrap();
        }
        usage
    }

    fn row_key(index: u8) -> Vec<u8> {
        identity(index).storage_key()
    }

    #[tokio::test]
    async fn list_pages_records() {
        let (ctx, _dir) = context();
        let usage = seed_rows(&ctx, 5).await;
        let first = list_quarantine_records(
            &ctx,
            QuarantinePageRequest {
                topic: None,
                start_after: None,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(first.records.len(), 2);
        assert_eq!(first.usage, usage);
        assert_eq!(first.usage.records, 5);

        let second = list_quarantine_records(
            &ctx,
            QuarantinePageRequest {
                topic: None,
                start_after: first.next_start_after.clone(),
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(second.records.len(), 2);
        assert_ne!(second.records[0].event_id(), first.records[0].event_id());
    }

    #[tokio::test]
    async fn inspect_reads_record() {
        let (ctx, _dir) = context();
        seed_rows(&ctx, 2).await;
        let record = read_quarantine_record(&ctx, &row_key(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.decoded_event().unwrap(), event(1));
        assert!(
            read_quarantine_record(&ctx, &row_key(9))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn acknowledge_is_idempotent() {
        let (ctx, _dir) = context();
        let usage = seed_rows(&ctx, 2).await;
        assert!(
            acknowledge_quarantine_row(&ctx, &row_key(0))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            acknowledge_quarantine_row(&ctx, &row_key(0))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            acknowledge_quarantine_row(&ctx, &row_key(9))
                .await
                .unwrap()
                .is_none()
        );
        let stored = read_quarantine_record(&ctx, &row_key(0))
            .await
            .unwrap()
            .unwrap();
        assert!(stored.acknowledged);
        assert_eq!(read_quarantine_usage(&ctx).await.unwrap(), usage);
    }

    #[tokio::test]
    async fn prune_skips_unacknowledged() {
        let (ctx, _dir) = context();
        let usage = seed_rows(&ctx, 3).await;
        acknowledge_quarantine_row(&ctx, &row_key(1)).await.unwrap();

        let result = prune_quarantine_records(&ctx, QuarantinePageRequest::default())
            .await
            .unwrap();
        assert_eq!(result.pruned, 1);
        assert_eq!(result.scanned, 3);
        assert_eq!(result.usage.records, 2);
        assert!(result.usage.bytes < usage.bytes);
        assert_eq!(read_quarantine_usage(&ctx).await.unwrap(), result.usage);
        assert!(
            read_quarantine_record(&ctx, &row_key(1))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_quarantine_record(&ctx, &row_key(0))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn prune_is_resumable() {
        let (ctx, _dir) = context();
        seed_rows(&ctx, 4).await;
        for index in 0..4u8 {
            acknowledge_quarantine_row(&ctx, &row_key(index))
                .await
                .unwrap();
        }

        let mut cursor = None;
        let mut pruned = 0;
        loop {
            let result = prune_quarantine_records(
                &ctx,
                QuarantinePageRequest {
                    topic: None,
                    start_after: cursor,
                    limit: Some(1),
                },
            )
            .await
            .unwrap();
            assert!(result.scanned <= 1);
            pruned += result.pruned;
            cursor = result.next_start_after;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(pruned, 4);
        assert_eq!(
            read_quarantine_usage(&ctx).await.unwrap(),
            SyncQuarantineUsage::default()
        );
    }

    #[tokio::test]
    async fn prune_reclaims_capacity() {
        let (ctx, _dir) = context();
        let capacity = SyncQuarantineCapacity {
            max_records: 2,
            max_bytes: u64::MAX,
        };
        let usage = seed_rows(&ctx, 2).await;
        let error = build_quarantine_entries(
            SyncQuarantineInput {
                identity: identity(2),
                evidence: SyncQuarantineEvidence::from_event(&event(2)),
                reason: "invalid",
                quarantined_at_ms: 42,
                replaced_bytes: None,
            },
            usage,
            capacity,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            aruna_core::structs::SyncQuarantineError::AtCapacity { .. }
        ));

        acknowledge_quarantine_row(&ctx, &row_key(0)).await.unwrap();
        let reclaimed = prune_quarantine_records(&ctx, QuarantinePageRequest::default())
            .await
            .unwrap();
        assert!(
            build_quarantine_entries(
                SyncQuarantineInput {
                    identity: identity(2),
                    evidence: SyncQuarantineEvidence::from_event(&event(2)),
                    reason: "invalid",
                    quarantined_at_ms: 42,
                    replaced_bytes: None,
                },
                reclaimed.usage,
                capacity,
            )
            .is_ok()
        );
    }
}
