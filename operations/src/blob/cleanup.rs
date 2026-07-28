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
    pub has_more: bool,
}

pub async fn process_cleanup_batch(
    context: &DriverContext,
) -> Result<BlobCleanupOutcome, String> {
    let (values, next) = iter_prefix_page(
        &context.storage_handle,
        BLOB_CLEANUP_KEYSPACE,
        None,
        None,
        CLEANUP_PAGE_SIZE,
        None,
    )
    .await?;

    let mut outcome = BlobCleanupOutcome {
        has_more: next.is_some(),
        ..Default::default()
    };
    let mut done: Vec<(String, Key)> = Vec::new();
    for (key, value) in values {
        let work = BlobCleanupWork::from_bytes(&value).map_err(|error| error.to_string())?;
        if run_cleanup_work(context, work).await {
            done.push((BLOB_CLEANUP_KEYSPACE.to_string(), key));
            outcome.processed = outcome.processed.saturating_add(1);
        } else {
            outcome.failed = outcome.failed.saturating_add(1);
        }
    }

    if !done.is_empty() {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes: done,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => return Err(format!("unexpected cleanup delete event: {other:?}")),
        }
    }

    Ok(outcome)
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
