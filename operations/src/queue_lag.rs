//! Shared durable-queue lag sampling for tracing and Prometheus metrics.
//! `queue.lag` lines are emitted only while a traced queue is non-empty plus
//! one final line when it drains. Idle cost is one limit-1 storage probe per
//! durable queue per tick.

use std::future::Future;
use std::time::Duration;

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_REPLICATION_JOB_KEYSPACE, DOCUMENT_SYNC_OUTBOX_KEYSPACE,
    METADATA_MATERIALIZATION_JOB_KEYSPACE, REFERENCE_METADATA_REFRESH_JOB_KEYSPACE,
};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use tracing::{info, warn};
use ulid::Ulid;

const QUEUE_SCAN_PAGE_SIZE: usize = 1_024;
const QUEUE_SCAN_PAGE_LIMIT: usize = 8;
const QUEUE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Depth and age of a durable work queue at one probe instant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueueLagSnapshot {
    /// Number of records observed (bounded by the page scan cap).
    pub depth: usize,
    /// True when the scan hit its page cap before draining the queue.
    pub depth_capped: bool,
    /// Age in milliseconds of the oldest relevant record, or 0 when empty.
    pub oldest_age_ms: u64,
    /// Records already due for processing (materialization queue only).
    pub due: usize,
}

/// One cadence's independently-fallible durable-queue probe results.
pub struct DurableQueueLagSample {
    pub document_sync_outbox: Result<QueueLagSnapshot, String>,
    pub metadata_materialization: Result<QueueLagSnapshot, String>,
    pub blob_replication: Result<QueueLagSnapshot, String>,
    pub reference_metadata_refresh: Result<QueueLagSnapshot, String>,
}

/// Tracks active queues so tracing includes one final sample after each drain.
#[derive(Default)]
pub struct QueueLagReporter {
    outbox_active: bool,
    materialization_active: bool,
    storage_active: bool,
}

impl QueueLagReporter {
    /// Probes every durable queue once, emits the tracing projection, and
    /// returns the same sample for the Prometheus projection.
    pub async fn sample(&mut self, storage: &StorageHandle) -> DurableQueueLagSample {
        let document_sync_outbox =
            probe_with_timeout(probe_outbox_lag(storage, self.outbox_active)).await;
        let metadata_materialization = probe_with_timeout(probe_materialization_lag(
            storage,
            self.materialization_active,
        ))
        .await;
        // Preserve the tracing gauge's position immediately after its two
        // durable scans rather than including the remaining metric probes.
        let storage_in_flight = storage.in_flight();
        let blob_replication = probe_with_timeout(probe_queue_depth(
            storage,
            BLOB_REPLICATION_JOB_KEYSPACE,
            false,
        ))
        .await;
        let reference_metadata_refresh = probe_with_timeout(probe_queue_depth(
            storage,
            REFERENCE_METADATA_REFRESH_JOB_KEYSPACE,
            false,
        ))
        .await;
        let sample = DurableQueueLagSample {
            document_sync_outbox,
            metadata_materialization,
            blob_replication,
            reference_metadata_refresh,
        };
        self.outbox_active = report_outbox_lag(&sample.document_sync_outbox, self.outbox_active);
        self.materialization_active = report_materialization_lag(
            &sample.metadata_materialization,
            self.materialization_active,
        );
        self.storage_active = report_storage_lag(storage_in_flight, self.storage_active);
        sample
    }
}

fn report_outbox_lag(probe: &Result<QueueLagSnapshot, String>, was_active: bool) -> bool {
    let snapshot = match probe {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(error = %error, "Failed to probe document sync outbox lag");
            return was_active;
        }
    };
    emit_queue_depth("document_sync_outbox", snapshot, was_active)
}

fn report_materialization_lag(probe: &Result<QueueLagSnapshot, String>, was_active: bool) -> bool {
    let snapshot = match probe {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(error = %error, "Failed to probe metadata materialization queue lag");
            return was_active;
        }
    };
    let active = snapshot.depth > 0;
    if active || was_active {
        info!(
            event = "queue.lag",
            queue = "metadata_materialization",
            depth = snapshot.depth,
            due = snapshot.due,
            pending = snapshot.depth.saturating_sub(snapshot.due),
            depth_capped = snapshot.depth_capped,
            oldest_due_age_ms = snapshot.oldest_age_ms,
            "Metadata materialization queue lag"
        );
    }
    active
}

fn report_storage_lag(in_flight: u64, was_active: bool) -> bool {
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

async fn probe_with_timeout<F>(probe: F) -> Result<QueueLagSnapshot, String>
where
    F: Future<Output = Result<QueueLagSnapshot, String>>,
{
    match tokio::time::timeout(QUEUE_PROBE_TIMEOUT, probe).await {
        Ok(result) => result,
        Err(_) => Err("queue probe timed out".to_string()),
    }
}

fn emit_queue_depth(queue: &'static str, snapshot: &QueueLagSnapshot, was_active: bool) -> bool {
    let active = snapshot.depth > 0;
    if active || was_active {
        info!(
            event = "queue.lag",
            queue,
            depth = snapshot.depth,
            depth_capped = snapshot.depth_capped,
            oldest_age_ms = snapshot.oldest_age_ms,
            "Queue lag"
        );
    }
    active
}

/// Probe the document sync outbox depth and oldest-enqueue age. When
/// `assume_active` is false the first page reads a single key so an empty
/// queue costs one read; depth stays correct via subsequent pages.
pub async fn probe_outbox_lag(
    storage: &StorageHandle,
    assume_active: bool,
) -> Result<QueueLagSnapshot, String> {
    let mut depth = 0usize;
    let mut capped = false;
    let mut oldest_record_ms: Option<u64> = None;
    let mut start_after: Option<ByteView> = None;
    for page in 0..QUEUE_SCAN_PAGE_LIMIT {
        let limit = first_page_limit(page, assume_active);
        let (keys, next) = iter_page(
            storage,
            DOCUMENT_SYNC_OUTBOX_KEYSPACE,
            start_after.take(),
            limit,
        )
        .await?;
        depth += keys.len();
        for key in &keys {
            // Outbox keys end in the record's ULID, whose timestamp is the
            // enqueue time.
            if let Some(record_ms) = ulid_suffix_timestamp_ms(key) {
                oldest_record_ms =
                    Some(oldest_record_ms.map_or(record_ms, |oldest| oldest.min(record_ms)));
            }
        }
        if !advance(&mut start_after, next, page, &mut capped) {
            break;
        }
    }
    Ok(QueueLagSnapshot {
        depth,
        depth_capped: capped,
        oldest_age_ms: oldest_record_ms
            .map(|record_ms| unix_timestamp_millis().saturating_sub(record_ms))
            .unwrap_or(0),
        due: 0,
    })
}

/// Probe the metadata materialization queue depth, the number of jobs already
/// due, and the age of the oldest due job.
pub async fn probe_materialization_lag(
    storage: &StorageHandle,
    assume_active: bool,
) -> Result<QueueLagSnapshot, String> {
    let now_ms = unix_timestamp_millis();
    let mut depth = 0usize;
    let mut due = 0usize;
    let mut capped = false;
    let mut oldest_due_ms: Option<u64> = None;
    let mut start_after: Option<ByteView> = None;
    for page in 0..QUEUE_SCAN_PAGE_LIMIT {
        let limit = first_page_limit(page, assume_active);
        let (keys, next) = iter_page(
            storage,
            METADATA_MATERIALIZATION_JOB_KEYSPACE,
            start_after.take(),
            limit,
        )
        .await?;
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
        if !advance(&mut start_after, next, page, &mut capped) {
            break;
        }
    }
    Ok(QueueLagSnapshot {
        depth,
        depth_capped: capped,
        oldest_age_ms: oldest_due_ms
            .map(|due_ms| now_ms.saturating_sub(due_ms))
            .unwrap_or(0),
        due,
    })
}

/// Probe the depth of a keyspace-backed queue without record-age semantics.
pub async fn probe_queue_depth(
    storage: &StorageHandle,
    key_space: &str,
    assume_active: bool,
) -> Result<QueueLagSnapshot, String> {
    let mut depth = 0usize;
    let mut capped = false;
    let mut start_after: Option<ByteView> = None;
    for page in 0..QUEUE_SCAN_PAGE_LIMIT {
        let limit = first_page_limit(page, assume_active);
        let (keys, next) = iter_page(storage, key_space, start_after.take(), limit).await?;
        depth += keys.len();
        if !advance(&mut start_after, next, page, &mut capped) {
            break;
        }
    }
    Ok(QueueLagSnapshot {
        depth,
        depth_capped: capped,
        oldest_age_ms: 0,
        due: 0,
    })
}

fn first_page_limit(page: usize, assume_active: bool) -> usize {
    // The first probe uses limit 1 so an empty queue costs one key read.
    if page == 0 && !assume_active {
        1
    } else {
        QUEUE_SCAN_PAGE_SIZE
    }
}

/// Advances the paging cursor. Returns false when the scan should stop, setting
/// `capped` when it stopped because the page cap was reached mid-queue.
fn advance(
    start_after: &mut Option<ByteView>,
    next: Option<ByteView>,
    page: usize,
    capped: &mut bool,
) -> bool {
    match next {
        Some(next) if page + 1 < QUEUE_SCAN_PAGE_LIMIT => {
            *start_after = Some(next);
            true
        }
        Some(_) => {
            *capped = true;
            false
        }
        None => false,
    }
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
            start: start_after.map(IterStart::After),
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
    use aruna_storage::FjallStorage;

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

    async fn write_key(storage: &StorageHandle, key_space: &str, key: Vec<u8>) {
        let event = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(&b"1"[..]),
                txn_id: None,
            })
            .await;
        assert!(
            matches!(event, Event::Storage(StorageEvent::WriteResult { .. })),
            "unexpected write event: {event:?}"
        );
    }

    #[tokio::test]
    async fn probe_outbox_reports_depth_and_age() {
        let temp = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();

        let empty = probe_outbox_lag(&storage, false).await.unwrap();
        assert_eq!(empty, QueueLagSnapshot::default());

        let old = Ulid::from_parts(unix_timestamp_millis().saturating_sub(5_000), 1);
        let recent = Ulid::from_parts(unix_timestamp_millis(), 2);
        for ulid in [old, recent] {
            let mut key = b"upsert/".to_vec();
            key.extend_from_slice(&ulid.to_bytes());
            write_key(&storage, DOCUMENT_SYNC_OUTBOX_KEYSPACE, key).await;
        }

        let snapshot = probe_outbox_lag(&storage, false).await.unwrap();
        assert_eq!(snapshot.depth, 2);
        assert!(!snapshot.depth_capped);
        assert!(snapshot.oldest_age_ms >= 5_000, "{snapshot:?}");
    }

    #[tokio::test]
    async fn probe_queue_depth_counts_records() {
        let temp = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        for index in 0u32..3 {
            write_key(
                &storage,
                aruna_core::keyspaces::BLOB_REPLICATION_JOB_KEYSPACE,
                index.to_be_bytes().to_vec(),
            )
            .await;
        }
        let snapshot = probe_queue_depth(
            &storage,
            aruna_core::keyspaces::BLOB_REPLICATION_JOB_KEYSPACE,
            false,
        )
        .await
        .unwrap();
        assert_eq!(snapshot.depth, 3);
        assert_eq!(snapshot.oldest_age_ms, 0);
    }
}
