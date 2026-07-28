use std::time::Duration;

use aruna_core::effects::{BlobEffect, DhtEffect, Effect, NetEffect, StorageEffect};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::DhtKeyId;
use aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE;
use aruna_core::structs::BlobCleanupWork;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::Key;
use tracing::warn;

use crate::driver::DriverContext;
use crate::jobs::store::iter_prefix_page;

pub const BLOB_CLEANUP_AFTER: Duration = Duration::from_secs(300);
pub const BLOB_CLEANUP_RETRY: Duration = Duration::from_secs(30);
const CLEANUP_PAGE_SIZE: usize = 128;

pub fn schedule_blob_cleanup_effect() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::DrainBlobCleanupQueue,
        after: Duration::ZERO,
    })
}

#[derive(Debug, Default)]
pub struct BlobCleanupOutcome {
    pub processed: usize,
    pub failed: usize,
    pub dropped: usize,
}

// Pages through the whole queue so persistently failing rows cannot starve the
// rows behind them, deleting each page's completed rows before the next fetch.
pub async fn process_cleanup_batch(context: &DriverContext) -> Result<BlobCleanupOutcome, String> {
    let mut outcome = BlobCleanupOutcome::default();
    let mut start_after = None;

    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            start_after,
            CLEANUP_PAGE_SIZE,
            None,
        )
        .await?;

        let mut done: Vec<(String, Key)> = Vec::new();
        for (key, value) in values {
            match BlobCleanupWork::from_bytes(&value) {
                // A row nobody can decode would wedge the drain forever.
                Err(error) => {
                    warn!(error = %error, "Dropping undecodable blob cleanup row");
                    done.push((BLOB_CLEANUP_KEYSPACE.to_string(), key));
                    outcome.dropped = outcome.dropped.saturating_add(1);
                }
                Ok(work) => {
                    if run_cleanup_work(context, work).await {
                        done.push((BLOB_CLEANUP_KEYSPACE.to_string(), key));
                        outcome.processed = outcome.processed.saturating_add(1);
                    } else {
                        outcome.failed = outcome.failed.saturating_add(1);
                    }
                }
            }
        }

        delete_cleanup_rows(context, done).await?;

        let Some(next) = next else {
            return Ok(outcome);
        };
        start_after = Some(next);
    }
}

async fn delete_cleanup_rows(
    context: &DriverContext,
    deletes: Vec<(String, Key)>,
) -> Result<(), String> {
    if deletes.is_empty() {
        return Ok(());
    }
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected cleanup delete event: {other:?}")),
    }
}

// Best-effort execution: a failed entry stays queued and retries next drain.
async fn run_cleanup_work(context: &DriverContext, work: BlobCleanupWork) -> bool {
    match work {
        BlobCleanupWork::DeleteBlob { location } => {
            let Some(blob_handle) = context.blob_handle.as_ref() else {
                return false;
            };
            match blob_handle
                .send_blob_effect(BlobEffect::Delete { location })
                .await
            {
                Event::Blob(BlobEvent::DeleteFinished) => true,
                event => {
                    warn!(?event, "Deferred blob delete failed");
                    false
                }
            }
        }
        BlobCleanupWork::RegisterDht {
            blake3,
            realm_id,
            ttl_ms,
        } => {
            let Some(net_handle) = context.net_handle.as_ref() else {
                return false;
            };
            let effect = Effect::Net(NetEffect::Dht(DhtEffect::Put {
                key: DhtKeyId::from_bytes(blake3),
                realm_id,
                value: Vec::new(),
                ttl: Duration::from_millis(ttl_ms),
            }));
            match net_handle.send_effect(effect).await {
                Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. })) => true,
                event => {
                    warn!(?event, "Deferred blob DHT registration failed");
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CLEANUP_PAGE_SIZE, process_cleanup_batch};
    use crate::driver::DriverContext;
    use crate::jobs::store::iter_prefix_page;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE;
    use aruna_core::structs::{BackendLocation, BlobCleanupWork};
    use aruna_core::types::UserId;
    use aruna_storage::storage::{FjallStorage, StorageHandle};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    fn setup_context() -> (TempDir, StorageHandle, DriverContext) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (dir, storage, context)
    }

    fn delete_work() -> Vec<u8> {
        let realm_id = aruna_core::structs::RealmId::from_bytes([3u8; 32]);
        BlobCleanupWork::DeleteBlob {
            location: BackendLocation {
                root: "root".to_string(),
                storage_bucket: "bucket".to_string(),
                backend_path: "bucket/object".to_string(),
                ulid: Ulid::generate(),
                compressed: false,
                encrypted: false,
                created_by: UserId::local(Ulid::generate(), realm_id),
                created_at: SystemTime::now(),
                staging: false,
                partial: false,
                blob_size: 0,
                hashes: HashMap::new(),
            },
        }
        .to_bytes()
        .unwrap()
    }

    async fn write_rows(storage: &StorageHandle, rows: Vec<Vec<u8>>) {
        let writes = rows
            .into_iter()
            .map(|value| {
                (
                    BLOB_CLEANUP_KEYSPACE.to_string(),
                    Ulid::generate().to_bytes().to_vec().into(),
                    value.into(),
                )
            })
            .collect();
        let event = storage
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

    async fn remaining_rows(storage: &StorageHandle) -> usize {
        let (values, _) = iter_prefix_page(storage, BLOB_CLEANUP_KEYSPACE, None, None, 4096, None)
            .await
            .unwrap();
        values.len()
    }

    #[tokio::test]
    async fn drops_poison_rows() {
        // An undecodable row must be deleted, and the rows after it still run.
        let (_dir, storage, context) = setup_context();
        write_rows(
            &storage,
            vec![b"not-postcard-at-all".to_vec(), delete_work()],
        )
        .await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.dropped, 1);
        // The delete row fails because this context has no blob handle.
        assert_eq!(outcome.failed, 1);
        assert_eq!(remaining_rows(&storage).await, 1);
    }

    #[tokio::test]
    async fn pages_past_failures() {
        // Rows behind a full page of poison must still be reached in one drain.
        let (_dir, storage, context) = setup_context();
        let rows = std::iter::repeat_n(b"broken".to_vec(), CLEANUP_PAGE_SIZE + 5).collect();
        write_rows(&storage, rows).await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.dropped, CLEANUP_PAGE_SIZE + 5);
        assert_eq!(remaining_rows(&storage).await, 0);
    }
}
