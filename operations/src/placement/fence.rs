//! Per-bucket write-admission fence.
//!
//! An empty outbox scan proves nothing on its own: a write that resolved the
//! bucket before the cutover can still commit its row afterwards, and the
//! departing holder has by then given up its publish authority. So a
//! holder-authoritative writer reads the bucket's fence inside the very
//! transaction that commits its domain mutation and outbox row. The departing
//! holder closes that fence durably before it drains, which conflicts every
//! predecessor-generation transaction that has not committed yet and leaves a
//! finite remainder to drain.

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PLACEMENT_WRITE_FENCE_KEYSPACE;
use aruna_core::storage_entries::placement_fence_key;
use aruna_core::structs::{PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::types::{Key, Value};
use aruna_storage::StorageHandle;
use byteview::ByteView;

/// Generation a write of `placement` is admitted at: the bucket's activation
/// epoch. `None` while the bucket has no usable activation, which is bootstrap
/// before the first candidate map, where no transition can be in flight.
pub fn write_generation(config: &RealmConfigDocument, placement: &PlacementRef) -> Option<u64> {
    if *placement == PlacementRef::NIL {
        return None;
    }
    config
        .activation(&placement.strategy_id, placement.shard)
        .map(|activation| activation.activation_epoch)
}

/// The bucket's fence read, addressed for a batch read inside a transaction.
pub fn fence_read(realm_id: &RealmId, placement: &PlacementRef) -> (String, Key) {
    (
        PLACEMENT_WRITE_FENCE_KEYSPACE.to_string(),
        placement_fence_key(realm_id, placement),
    )
}

/// The highest generation the stored fence value closes.
pub fn closed_generation(value: Option<&Value>) -> u64 {
    value
        .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_ref()).ok())
        .map_or(0, u64::from_be_bytes)
}

/// Whether a write that resolved holders at `generation` may still commit.
pub fn admits(value: Option<&Value>, generation: u64) -> bool {
    generation > closed_generation(value)
}

/// Durably closes `placement` through `generation`, so no later transaction
/// can be admitted at it and every uncommitted one conflicts. Monotone and
/// idempotent: a repeat after a crash re-closes the same generation.
pub async fn close(
    storage: &StorageHandle,
    realm_id: &RealmId,
    placement: &PlacementRef,
    generation: u64,
) -> Result<(), String> {
    let (key_space, key) = fence_read(realm_id, placement);
    let current = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.clone(),
            key: key.clone(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => closed_generation(value.as_ref()),
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("unexpected fence read result: {other:?}")),
    };
    if current >= generation {
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value: ByteView::from(generation.to_be_bytes().to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected fence write result: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::events::StorageEvent;
    use tempfile::tempdir;
    use ulid::Ulid;

    use super::*;

    fn placement() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([7; 16]),
            shard: 3,
        }
    }

    #[test]
    fn open_fence_admits() {
        assert!(admits(None, 1));
        let closed = Value::from(2u64.to_be_bytes().to_vec());
        assert_eq!(closed_generation(Some(&closed)), 2);
        assert!(!admits(Some(&closed), 1));
        assert!(!admits(Some(&closed), 2));
        assert!(admits(Some(&closed), 3));
        // A truncated value must never read as "closed at a high generation".
        assert_eq!(closed_generation(Some(&Value::from(vec![1u8]))), 0);
    }

    #[tokio::test]
    async fn close_conflicts_paused_write() {
        // A write that read the open fence and then paused must not commit
        // after the departing holder closed that generation.
        let directory = tempdir().unwrap();
        let storage =
            aruna_storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([9; 32]);
        let placement = placement();

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("the write transaction starts");
        };
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage
            .send_storage_effect(StorageEffect::Read {
                key_space: PLACEMENT_WRITE_FENCE_KEYSPACE.to_string(),
                key: placement_fence_key(&realm_id, &placement),
                txn_id: Some(txn_id),
            })
            .await
        else {
            panic!("the fence read answers");
        };
        assert!(admits(value.as_ref(), 1));

        close(&storage, &realm_id, &placement, 1)
            .await
            .expect("the departing holder closes the fence");

        let write = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
                key: ByteView::from(b"paused-row".to_vec()),
                value: ByteView::from(vec![1u8]),
                txn_id: Some(txn_id),
            })
            .await;
        assert!(matches!(
            write,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        let commit = storage
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;
        assert!(
            matches!(
                commit,
                Event::Storage(StorageEvent::Error {
                    error: aruna_core::errors::StorageError::TransactionConflict
                })
            ),
            "the paused predecessor write must conflict, got {commit:?}"
        );
    }

    #[tokio::test]
    async fn close_is_monotone() {
        let directory = tempdir().unwrap();
        let storage =
            aruna_storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([10; 32]);
        let placement = placement();
        let stored = || async {
            match storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: PLACEMENT_WRITE_FENCE_KEYSPACE.to_string(),
                    key: placement_fence_key(&realm_id, &placement),
                    txn_id: None,
                })
                .await
            {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    closed_generation(value.as_ref())
                }
                other => panic!("unexpected fence read: {other:?}"),
            }
        };

        close(&storage, &realm_id, &placement, 3).await.unwrap();
        assert_eq!(stored().await, 3);
        // A stale re-close never reopens the bucket.
        close(&storage, &realm_id, &placement, 2).await.unwrap();
        assert_eq!(stored().await, 3);
        close(&storage, &realm_id, &placement, 4).await.unwrap();
        assert_eq!(stored().await, 4);
    }
}
