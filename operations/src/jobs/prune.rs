use std::time::Duration;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::keyspaces::JOB_SCHEDULE_INDEX_KEYSPACE;
use aruna_core::structs::{JOB_PRUNE_INDEX_PREFIX, parse_job_schedule_index_key};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, KeySpace};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;

use super::JOB_PRUNE_SCAN_PAGE_SIZE;
use super::store::{
    batch_delete, first_schedule_entry, iter_prefix_page, job_prune_delete_entries, read_job_record,
};
use crate::driver::DriverContext;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct JobPruneOutcome {
    pub pruned: usize,
    pub has_more: bool,
    pub next_due_after: Option<Duration>,
}

pub async fn process_job_prune_batch(context: &DriverContext) -> Result<JobPruneOutcome, String> {
    process_job_prune_batch_with_page_size(context, JOB_PRUNE_SCAN_PAGE_SIZE).await
}

pub(crate) async fn process_job_prune_batch_with_page_size(
    context: &DriverContext,
    page_size: usize,
) -> Result<JobPruneOutcome, String> {
    let storage = &context.storage_handle;
    let now_ms = unix_timestamp_millis();
    let deletion_cap = page_size.saturating_mul(4);
    let mut deletes: Vec<(KeySpace, Key)> = Vec::new();
    let mut pruned = 0usize;
    let mut processed = 0usize;
    let mut has_more = false;
    let mut next_due_after = None;
    let mut start_after = None;

    'scan: loop {
        let (values, next) = iter_prefix_page(
            storage,
            JOB_SCHEDULE_INDEX_KEYSPACE,
            Some(ByteView::from(JOB_PRUNE_INDEX_PREFIX.to_vec())),
            start_after.take(),
            page_size,
            None,
        )
        .await?;
        if values.is_empty() {
            break;
        }
        for (key, _) in values {
            let (expiry_ms, job_id) = match parse_job_schedule_index_key(key.as_ref()) {
                Ok(parsed) => parsed,
                Err(error) => {
                    warn!(error = %error, "Deleting malformed job prune index row");
                    deletes.push((JOB_SCHEDULE_INDEX_KEYSPACE.to_string(), key));
                    continue;
                }
            };
            if expiry_ms > now_ms {
                next_due_after = Some(Duration::from_millis(expiry_ms.saturating_sub(now_ms)));
                break 'scan;
            }
            match read_job_record(storage, job_id, None).await? {
                Some(record) => deletes.extend(job_prune_delete_entries(&record)),
                None => deletes.push((JOB_SCHEDULE_INDEX_KEYSPACE.to_string(), key)),
            }
            pruned = pruned.saturating_add(1);
            processed = processed.saturating_add(1);
            if processed >= deletion_cap {
                has_more = true;
                break 'scan;
            }
        }
        match next {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }

    batch_delete(storage, deletes, None).await?;
    Ok(JobPruneOutcome {
        pruned,
        has_more,
        next_due_after,
    })
}

/// ShortenTimer restore keyed off the earliest `prune/` entry.
pub async fn restore_job_prune_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    let after = match first_schedule_entry(storage, JOB_PRUNE_INDEX_PREFIX).await {
        Ok(Some((expiry_ms, _))) => {
            Duration::from_millis(expiry_ms.saturating_sub(unix_timestamp_millis()))
        }
        Ok(None) => return,
        Err(error) => {
            warn!(error = %error, "Failed to scan job prune index");
            return;
        }
    };
    let event = task_handle
        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::PruneJobs,
            after,
        }))
        .await;
    if let Event::Task(TaskEvent::Error { message, .. }) = event {
        warn!(message = %message, "Failed to restore job prune timer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::JOB_RETENTION_MS;
    use crate::jobs::store::insert_job;
    use aruna_core::keyspaces::{JOB_KEYSPACE, JOB_OWNER_INDEX_KEYSPACE};
    use aruna_core::structs::{JobId, JobPayload, JobRecord, JobState, RealmId};
    use aruna_core::types::{NodeId, UserId};
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn context(storage: StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    fn terminal_record(job_id: JobId, finished_at_ms: u64) -> JobRecord {
        let mut record = JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                cleanup_marker: None,
            },
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            1,
            1,
            None,
        );
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(finished_at_ms);
        record
    }

    async fn count(storage: &StorageHandle, key_space: &str) -> usize {
        iter_prefix_page(storage, key_space, None, None, 64, None)
            .await
            .unwrap()
            .0
            .len()
    }

    #[tokio::test]
    async fn expired_job_pruned() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let finished = unix_timestamp_millis().saturating_sub(JOB_RETENTION_MS + 1);
        insert_job(
            &storage,
            &terminal_record(JobId::from_bytes([1u8; 16]), finished),
        )
        .await
        .unwrap();

        let outcome = process_job_prune_batch(&context(storage.clone()))
            .await
            .unwrap();
        assert_eq!(outcome.pruned, 1);
        assert_eq!(count(&storage, JOB_KEYSPACE).await, 0);
        assert_eq!(count(&storage, JOB_OWNER_INDEX_KEYSPACE).await, 0);
        assert_eq!(count(&storage, JOB_SCHEDULE_INDEX_KEYSPACE).await, 0);
    }

    #[tokio::test]
    async fn unexpired_job_retained() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        insert_job(
            &storage,
            &terminal_record(JobId::from_bytes([2u8; 16]), unix_timestamp_millis()),
        )
        .await
        .unwrap();

        let outcome = process_job_prune_batch(&context(storage.clone()))
            .await
            .unwrap();
        assert_eq!(outcome.pruned, 0);
        assert!(outcome.next_due_after.is_some());
        assert_eq!(count(&storage, JOB_KEYSPACE).await, 1);
    }

    #[tokio::test]
    async fn restore_arms_timer() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        insert_job(
            &storage,
            &terminal_record(JobId::from_bytes([3u8; 16]), unix_timestamp_millis()),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        restore_job_prune_timer(&storage, &task_handle).await;

        let Event::Task(TaskEvent::TimerScheduled { .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::PruneJobs,
                after: Duration::from_secs(u32::MAX as u64),
            }))
            .await
        else {
            panic!("expected an armed prune timer to shorten");
        };
    }
}
