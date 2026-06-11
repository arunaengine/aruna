//! Periodic `queue.lag` gauges: depth and oldest-record age for the durable
//! work queues, emitted only while a queue is non-empty plus one final line
//! when it drains. Idle cost is one limit-1 storage probe per queue per tick.

use std::sync::{Arc, Weak};

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DOCUMENT_SYNC_OUTBOX_KEYSPACE, METADATA_MATERIALIZATION_JOB_KEYSPACE};
use aruna_core::telemetry::QUEUE_LAG_INTERVAL;
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use tokio::time::sleep;
use tracing::{info, warn};
use ulid::Ulid;

const QUEUE_SCAN_PAGE_SIZE: usize = 1_024;
const QUEUE_SCAN_PAGE_LIMIT: usize = 8;

pub fn spawn_queue_lag_monitor(context: &Arc<crate::driver::DriverContext>) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    runtime.spawn(queue_lag_loop(Arc::downgrade(context)));
}

async fn queue_lag_loop(context: Weak<crate::driver::DriverContext>) {
    let mut outbox_active = false;
    let mut materialization_active = false;
    let mut storage_active = false;
    loop {
        sleep(QUEUE_LAG_INTERVAL).await;
        let Some(context) = context.upgrade() else {
            return;
        };
        outbox_active = report_outbox_lag(&context.storage_handle, outbox_active).await;
        materialization_active =
            report_materialization_lag(&context.storage_handle, materialization_active).await;
        storage_active = report_storage_lag(&context.storage_handle, storage_active);
    }
}

async fn report_outbox_lag(storage: &StorageHandle, was_active: bool) -> bool {
    let mut depth = 0usize;
    let mut capped = false;
    let mut oldest_record_ms: Option<u64> = None;
    let mut start_after: Option<ByteView> = None;
    for page in 0..QUEUE_SCAN_PAGE_LIMIT {
        // The first probe uses limit 1 so an empty queue costs one key read.
        let limit = if page == 0 && !was_active {
            1
        } else {
            QUEUE_SCAN_PAGE_SIZE
        };
        let (keys, next) = match iter_page(
            storage,
            DOCUMENT_SYNC_OUTBOX_KEYSPACE,
            start_after.take(),
            limit,
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                warn!(error = %error, "Failed to probe document sync outbox lag");
                return was_active;
            }
        };
        depth += keys.len();
        for key in &keys {
            // Outbox keys end in the record's ULID, whose timestamp is the
            // enqueue time.
            if let Some(record_ms) = ulid_suffix_timestamp_ms(key) {
                oldest_record_ms =
                    Some(oldest_record_ms.map_or(record_ms, |oldest| oldest.min(record_ms)));
            }
        }
        match next {
            Some(next) if page + 1 < QUEUE_SCAN_PAGE_LIMIT => start_after = Some(next),
            Some(_) => {
                capped = true;
                break;
            }
            None => break,
        }
    }
    emit_queue_depth(
        "document_sync_outbox",
        depth,
        capped,
        oldest_record_ms
            .map(|record_ms| unix_timestamp_millis().saturating_sub(record_ms))
            .unwrap_or(0),
        was_active,
    )
}

async fn report_materialization_lag(storage: &StorageHandle, was_active: bool) -> bool {
    let now_ms = unix_timestamp_millis();
    let mut depth = 0usize;
    let mut due = 0usize;
    let mut capped = false;
    let mut oldest_due_ms: Option<u64> = None;
    let mut start_after: Option<ByteView> = None;
    for page in 0..QUEUE_SCAN_PAGE_LIMIT {
        let limit = if page == 0 && !was_active {
            1
        } else {
            QUEUE_SCAN_PAGE_SIZE
        };
        let (keys, next) = match iter_page(
            storage,
            METADATA_MATERIALIZATION_JOB_KEYSPACE,
            start_after.take(),
            limit,
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                warn!(error = %error, "Failed to probe metadata materialization queue lag");
                return was_active;
            }
        };
        depth += keys.len();
        for key in &keys {
            // Job keys are prefixed with the big-endian due timestamp.
            let Some(due_at_ms) = due_at_prefix_ms(key) else {
                continue;
            };
            if due_at_ms <= now_ms {
                due += 1;
                oldest_due_ms = Some(oldest_due_ms.map_or(due_at_ms, |old| old.min(due_at_ms)));
            }
        }
        match next {
            Some(next) if page + 1 < QUEUE_SCAN_PAGE_LIMIT => start_after = Some(next),
            Some(_) => {
                capped = true;
                break;
            }
            None => break,
        }
    }
    let active = depth > 0;
    if active || was_active {
        info!(
            event = "queue.lag",
            queue = "metadata_materialization",
            depth,
            due,
            pending = depth.saturating_sub(due),
            depth_capped = capped,
            oldest_due_age_ms = oldest_due_ms
                .map(|due_ms| now_ms.saturating_sub(due_ms))
                .unwrap_or(0),
            "Metadata materialization queue lag"
        );
    }
    active
}

fn report_storage_lag(storage: &StorageHandle, was_active: bool) -> bool {
    let in_flight = storage.in_flight();
    let active = in_flight > 0;
    if active || was_active {
        info!(
            event = "queue.lag",
            queue = "storage_effects",
            depth = in_flight,
            "Storage effect queue lag"
        );
    }
    active
}

fn emit_queue_depth(
    queue: &'static str,
    depth: usize,
    capped: bool,
    oldest_age_ms: u64,
    was_active: bool,
) -> bool {
    let active = depth > 0;
    if active || was_active {
        info!(
            event = "queue.lag",
            queue,
            depth,
            depth_capped = capped,
            oldest_age_ms,
            "Queue lag"
        );
    }
    active
}

async fn iter_page(
    storage: &StorageHandle,
    key_space: &str,
    start_after: Option<ByteView>,
    limit: usize,
) -> Result<(Vec<ByteView>, Option<ByteView>), String> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start_after,
            limit,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((
            values.into_iter().map(|(key, _)| key).collect(),
            next_start_after,
        )),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

fn ulid_suffix_timestamp_ms(key: &[u8]) -> Option<u64> {
    if key.len() < 16 {
        return None;
    }
    let bytes: [u8; 16] = key[key.len() - 16..].try_into().ok()?;
    Some(Ulid::from_bytes(bytes).timestamp_ms())
}

fn due_at_prefix_ms(key: &[u8]) -> Option<u64> {
    let bytes: [u8; 8] = key.get(..8)?.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_suffix_timestamp_round_trips() {
        let ulid = Ulid::from_parts(1_750_000_000_000, 42);
        let mut key = b"document-sync-outbox-v1/upsert/".to_vec();
        key.extend_from_slice(&ulid.to_bytes());
        assert_eq!(ulid_suffix_timestamp_ms(&key), Some(1_750_000_000_000));
        assert_eq!(ulid_suffix_timestamp_ms(b"short"), None);
    }

    #[test]
    fn due_at_prefix_parses_big_endian_timestamp() {
        let mut key = 1_234_567u64.to_be_bytes().to_vec();
        key.extend_from_slice(&[0u8; 32]);
        assert_eq!(due_at_prefix_ms(&key), Some(1_234_567));
        assert_eq!(due_at_prefix_ms(&[1, 2, 3]), None);
    }
}
