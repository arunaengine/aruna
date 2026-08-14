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
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PLACEMENT_WRITE_FENCE_KEYSPACE;
use aruna_core::storage_entries::placement_fence_key;
use aruna_core::structs::{PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::types::{Key, Value};
use aruna_storage::StorageHandle;
use byteview::ByteView;

const CLOSE_ATTEMPTS: usize = 4;

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
    match value {
        None => 0,
        Some(bytes) => <[u8; 8]>::try_from(bytes.as_ref())
            .map(u64::from_be_bytes)
            .unwrap_or(u64::MAX),
    }
}

/// Whether a write that resolved holders at `generation` may still commit.
pub fn admits(value: Option<&Value>, generation: u64) -> bool {
    generation > closed_generation(value)
}

/// Buckets a write publishes onto, each with the activation generation it
/// resolved at. The write reads them inside its own transaction, so a
/// departing holder's close either rejects it or conflicts its commit.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteFence {
    buckets: Vec<(RealmId, PlacementRef, u64)>,
}

impl WriteFence {
    /// Adds the buckets `placements` resolves to in `config`. A bucket without
    /// a usable activation is skipped: that is bootstrap before the realm's
    /// first candidate map, where no transition can be in flight.
    pub fn add(
        &mut self,
        realm_id: RealmId,
        config: &RealmConfigDocument,
        placements: impl IntoIterator<Item = PlacementRef>,
    ) {
        for placement in placements {
            let Some(generation) = write_generation(config, &placement) else {
                continue;
            };
            if !self
                .buckets
                .iter()
                .any(|(realm, bucket, _)| *realm == realm_id && *bucket == placement)
            {
                self.buckets.push((realm_id, placement, generation));
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Fence reads to append to a batch read inside the write transaction.
    pub fn reads(&self) -> Vec<(String, Key)> {
        self.buckets
            .iter()
            .map(|(realm_id, placement, _)| fence_read(realm_id, placement))
            .collect()
    }

    /// Generation to stamp on a row publishing onto `placement`.
    pub fn generation(&self, realm_id: &RealmId, placement: &PlacementRef) -> u64 {
        self.buckets
            .iter()
            .find(|(realm, bucket, _)| realm == realm_id && bucket == placement)
            .map_or(0, |(_, _, generation)| *generation)
    }

    /// Whether every bucket still admits the write, given the fence values in
    /// `reads` order. A short or long answer is never admitted.
    pub fn admits(&self, values: &[(Key, Option<Value>)]) -> bool {
        values.len() == self.buckets.len()
            && self
                .buckets
                .iter()
                .zip(values)
                .all(|((_, _, generation), (_, value))| admits(value.as_ref(), *generation))
    }
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
    for _ in 0..CLOSE_ATTEMPTS {
        let txn_id = match storage
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => return Err(format!("unexpected fence transaction result: {other:?}")),
        };
        let current = match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.clone(),
                key: key.clone(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                closed_generation(value.as_ref())
            }
            Event::Storage(StorageEvent::Error { error }) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error.to_string());
            }
            other => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(format!("unexpected fence read result: {other:?}"));
            }
        };
        if current >= generation {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(());
        }
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.clone(),
                key: key.clone(),
                value: ByteView::from(generation.to_be_bytes().to_vec()),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error.to_string());
            }
            other => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(format!("unexpected fence write result: {other:?}"));
            }
        }
        match storage
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed })
                if committed == txn_id =>
            {
                return Ok(());
            }
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }) => continue,
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => return Err(format!("unexpected fence commit result: {other:?}")),
        }
    }
    Err(StorageError::TransactionConflict.to_string())
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
        // A malformed value must never reopen the bucket.
        let malformed = Value::from(vec![1u8]);
        assert_eq!(closed_generation(Some(&malformed)), u64::MAX);
        assert!(!admits(Some(&malformed), 1));
    }

    #[test]
    fn fence_reads_buckets() {
        // A repeated bucket reads once, and a short or long answer is never
        // admitted: the answer must line up with the reads it was asked for.
        let realm_id = RealmId::from_bytes([11; 32]);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config
            .strategies
            .push(aruna_core::structs::PlacementStrategy {
                strategy_id: placement().strategy_id,
                name: "default".to_string(),
                replica_count: Some(1),
                distinct_locations: false,
                affinity: Vec::new(),
                shard_count: 16,
            });
        config.snapshot_candidate_map();

        let mut fence = WriteFence::default();
        fence.add(
            realm_id,
            &config,
            [placement(), placement(), PlacementRef::NIL],
        );
        assert_eq!(fence.reads().len(), 1);
        assert_eq!(fence.generation(&realm_id, &placement()), 1);
        assert_eq!(fence.generation(&realm_id, &PlacementRef::NIL), 0);

        let key = fence.reads()[0].1.clone();
        assert!(fence.admits(&[(key.clone(), None)]));
        assert!(!fence.admits(&[]));
        assert!(!fence.admits(&[(key.clone(), None), (key.clone(), None)]));
        assert!(!fence.admits(&[(key, Some(Value::from(1u64.to_be_bytes().to_vec())))]));
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

        let (lower, higher) = tokio::join!(
            close(&storage, &realm_id, &placement, 5),
            close(&storage, &realm_id, &placement, 6),
        );
        lower.unwrap();
        higher.unwrap();
        assert_eq!(stored().await, 6);
    }
}
