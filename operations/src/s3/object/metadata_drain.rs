//! Drains the reference metadata refresh queue: refreshes due references and retries failed ones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::handle::Handle;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, REFRESH_DUE_KEYSPACE, REFRESH_JOB_KEYSPACE};
use aruna_core::structs::execution::source_access::ObservationMatch;
use aruna_core::structs::storage::blob::{BlobVersion, BlobVersionState, VersionKey};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Key;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use super::metadata::{
    ReferenceRefresh, ReferenceRefreshError, ReferenceRefreshRecord, due_key, job_entries, job_key,
    split_due,
};
use crate::driver::{DriverContext, drive};
use crate::replication::queue::schedule_blob_drain;
use crate::s3::object::refresh_mint::{MintError, MintSuccessorOperation};
use crate::s3::object::refresh_observe::ObserveSourceOperation;
use crate::s3::usage::schedule_snapshot_publish;
use crate::tasks::queue_backoff::{due_after, min_due_at, retry_delay_ms};
use crate::tasks::task_persistence::persist_task_effect;

const REFRESH_SCAN_PAGE: usize = 512;

const REFRESH_BATCH_SIZE: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceDrainResult {
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

struct ReferenceRefreshScan {
    jobs: Vec<(Vec<u8>, ReferenceRefreshRecord)>,
    has_more_due: bool,
    next_due_ms: Option<u64>,
}

pub async fn refresh_reference_metadata(
    context: Arc<DriverContext>,
    refresh: ReferenceRefresh,
) -> Result<(), String> {
    refresh_with_context(context.as_ref(), refresh).await
}

pub(super) async fn refresh_with_context(
    context: &DriverContext,
    refresh: ReferenceRefresh,
) -> Result<(), String> {
    let version_key = VersionKey::new(&refresh.bucket, &refresh.key, refresh.version_id)
        .to_bytes()
        .map_err(|err| err.to_string())?;
    let txn_id = match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => {
            return Err(format!(
                "unexpected start transaction event: {}",
                other.label()
            ));
        }
    };

    let mut minted = false;
    let refreshed_value = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key.clone().into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let version = match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => version,
                Err(err) => {
                    abort_reference_refresh(context, txn_id).await;
                    return Err(err.to_string());
                }
            };
            let predecessor = version.clone();
            let BlobVersion {
                created_at,
                created_by,
                state,
                metadata,
                published_by,
                placement_policies,
                original_epoch,
                generation,
                identity,
                origin,
                hop,
            } = version;
            let BlobVersionState::Reference {
                source,
                cached_metadata,
                last_refresh,
                advance_count,
            } = state
            else {
                abort_reference_refresh(context, txn_id).await;
                return Ok(());
            };
            // Only a confirmed observation counts as a refresh; fields under the version id never
            // change, so a confirmed change becomes a successor and an unknown one is dropped.
            let Some(observation) = refresh.metadata.clone() else {
                abort_reference_refresh(context, txn_id).await;
                return Err("the refresh carries no observation".to_string());
            };
            let comparison = cached_metadata.compare(&observation);
            // A version whose recorded digests bind its bytes gets no successor without them.
            let bound = !cached_metadata.enforced_checksums().is_empty();
            if refresh.refreshed_at > last_refresh
                && comparison == ObservationMatch::Changed
                && !bound
            {
                let mint = MintSuccessorOperation::new(
                    txn_id,
                    refresh.clone(),
                    observation.clone(),
                    predecessor,
                );
                match drive(mint, context).await {
                    Ok(written) => minted = written,
                    // The key's range moved to another coordinator, which refreshes it there.
                    Err(MintError::Distribution(error)) if error.moved_away() => {
                        abort_reference_refresh(context, txn_id).await;
                        return Ok(());
                    }
                    Err(error) => {
                        abort_reference_refresh(context, txn_id).await;
                        return Err(error.to_string());
                    }
                }
            }
            if refresh.refreshed_at <= last_refresh || comparison != ObservationMatch::Same {
                None
            } else {
                Some(
                    match (BlobVersion {
                        published_by,
                        placement_policies,
                        original_epoch,
                        generation,
                        identity,
                        origin,
                        hop,
                        ..BlobVersion::reference(
                            source,
                            cached_metadata,
                            created_at,
                            created_by,
                            refresh.refreshed_at,
                        )
                        .with_metadata(metadata)
                        .with_advance_count(advance_count)
                    })
                    .to_bytes()
                    {
                        Ok(value) => value,
                        Err(err) => {
                            abort_reference_refresh(context, txn_id).await;
                            return Err(err.to_string());
                        }
                    },
                )
            }
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            abort_reference_refresh(context, txn_id).await;
            return Ok(());
        }
        Event::Storage(StorageEvent::Error { error }) => {
            abort_reference_refresh(context, txn_id).await;
            return Err(error.to_string());
        }
        other => {
            abort_reference_refresh(context, txn_id).await;
            return Err(format!("unexpected version read event: {}", other.label()));
        }
    };

    if let Some(refreshed_value) = refreshed_value {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: version_key.into(),
                value: refreshed_value.into(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                abort_reference_refresh(context, txn_id).await;
                return Err(error.to_string());
            }
            other => {
                abort_reference_refresh(context, txn_id).await;
                return Err(format!("unexpected version write event: {}", other.label()));
            }
        }
    }

    match context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
            if minted {
                // The dirty markers and the obligation committed above are the durable work.
                for effect in [schedule_snapshot_publish(), schedule_blob_drain()] {
                    if let Effect::Task(effect) = effect {
                        schedule_task(context, effect).await;
                    }
                }
            }
            Ok(())
        }
        Event::Storage(StorageEvent::Error { error }) => {
            abort_reference_refresh(context, txn_id).await;
            Err(error.to_string())
        }
        other => {
            abort_reference_refresh(context, txn_id).await;
            Err(format!("unexpected commit event: {}", other.label()))
        }
    }
}

/// Observes the source first when the refresh carries no observation; a failed observation
/// fails the job, which the queue retries later.
async fn observe_and_refresh(
    context: &DriverContext,
    mut refresh: ReferenceRefresh,
) -> Result<(), String> {
    if refresh.metadata.is_none() {
        let version_key = VersionKey::new(&refresh.bucket, &refresh.key, refresh.version_id);
        let observed = match drive(ObserveSourceOperation::new(version_key), context).await {
            Ok(observed) => observed,
            // Work for a version that is gone or no longer resolvable is finished, not retried.
            Err(error) if error.is_obsolete() => return Ok(()),
            Err(error) => return Err(error.to_string()),
        };
        refresh.metadata = Some(observed);
        refresh.refreshed_at = SystemTime::now().max(refresh.refreshed_at);
    }
    refresh_with_context(context, refresh).await
}

async fn schedule_task(context: &DriverContext, effect: TaskEffect) {
    if persist_task_effect(&context.storage_handle, &effect)
        .await
        .is_err()
    {
        return;
    }
    if let Some(task_handle) = &context.task_handle {
        let _ = task_handle.send_effect(Effect::Task(effect)).await;
    }
}

async fn abort_reference_refresh(context: &DriverContext, txn_id: Ulid) {
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

pub async fn restore_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    match next_timer(storage).await {
        Ok(None) => {}
        Ok(Some(after)) => {
            let event = task_handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::DrainRefreshQueue,
                    after,
                }))
                .await;
            if let Event::Task(TaskEvent::Error { message, .. }) = event {
                warn!(message = %message, "Failed to restore reference metadata refresh timer");
            }
        }
        Err(error) => warn!(error = ?error, "Failed to scan reference metadata refresh jobs"),
    }
}

#[cfg(test)]
pub(super) async fn jobs_exist(storage: &StorageHandle) -> Result<bool, ReferenceRefreshError> {
    Ok(!first_due(storage, 1).await?.0.is_empty())
}

async fn next_timer(storage: &StorageHandle) -> Result<Option<Duration>, ReferenceRefreshError> {
    let (rows, _) = first_due(storage, 1).await?;
    Ok(rows
        .first()
        .and_then(|(row, _)| split_due(row))
        .map(|(due_at_ms, _)| due_after(unix_timestamp_millis(), due_at_ms)))
}

pub async fn process_batch(
    context: &DriverContext,
) -> Result<ReferenceDrainResult, ReferenceRefreshError> {
    let batch_started = Instant::now();
    let now_ms = unix_timestamp_millis();
    let scan = scan_due_jobs(&context.storage_handle, now_ms, REFRESH_BATCH_SIZE).await?;
    let mut next_due_ms = scan.next_due_ms;
    let has_more_due = scan.has_more_due;
    let scan_elapsed = batch_started.elapsed();
    let job_count = scan.jobs.len();
    let oldest_lag_ms = scan
        .jobs
        .iter()
        .map(|(_, job)| now_ms.saturating_sub(job.due_at_ms))
        .max()
        .unwrap_or(0);

    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for (job_key, job) in scan.jobs {
        match observe_and_refresh(context, job.refresh.clone()).await {
            Ok(()) => {
                settle_job(&context.storage_handle, &job_key, &job, None).await?;
                succeeded = succeeded.saturating_add(1);
            }
            Err(error) => {
                let retry_due_at =
                    reschedule_job(&context.storage_handle, &job_key, &job, error).await?;
                next_due_ms = min_due_at(next_due_ms, retry_due_at);
                failed = failed.saturating_add(1);
            }
        }
    }

    if job_count > 0 {
        info!(
            event = "pipeline.reference_metadata_refresh.summary",
            jobs = job_count,
            succeeded,
            failed,
            scan_ms = duration_ms(scan_elapsed),
            total_ms = duration_ms(batch_started.elapsed()),
            oldest_lag_ms,
            has_more_due,
            "Reference metadata refresh batch summary"
        );
    }

    Ok(ReferenceDrainResult {
        processed: job_count,
        succeeded,
        failed,
        has_more_due,
        next_due_after: if has_more_due {
            None
        } else {
            next_due_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms))
        },
    })
}

/// The first `limit` due index rows and whether more follow.
async fn first_due(
    storage: &StorageHandle,
    limit: usize,
) -> Result<(Vec<(Key, ByteView)>, bool), ReferenceRefreshError> {
    let event = send(
        storage,
        StorageEffect::Iter {
            key_space: REFRESH_DUE_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit,
            txn_id: None,
        },
    )
    .await?;
    match event {
        StorageEvent::IterResult {
            values,
            next_start_after,
        } => Ok((values, next_start_after.is_some())),
        other => Err(ReferenceRefreshError::UnexpectedEvent(
            Event::Storage(other).label().to_string(),
        )),
    }
}

/// Walks one page of the due index in due order, with a point read per due row.
async fn scan_due_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<ReferenceRefreshScan, ReferenceRefreshError> {
    let (rows, more_rows) = first_due(storage, REFRESH_SCAN_PAGE).await?;
    let mut jobs = Vec::new();
    for (row, _) in rows {
        let Some((due_at_ms, key)) = split_due(&row) else {
            delete_rows(storage, vec![(REFRESH_DUE_KEYSPACE.to_string(), row)], None).await?;
            continue;
        };
        if due_at_ms > now_ms {
            return Ok(ReferenceRefreshScan {
                jobs,
                has_more_due: false,
                next_due_ms: Some(due_at_ms),
            });
        }
        if jobs.len() >= limit {
            return Ok(ReferenceRefreshScan {
                jobs,
                has_more_due: true,
                next_due_ms: None,
            });
        }
        let mut stale = vec![(REFRESH_DUE_KEYSPACE.to_string(), row.clone())];
        match read_job(storage, key, None).await? {
            Some(Ok(job)) if job.due_at_ms == due_at_ms => {
                jobs.push((key.to_vec(), job));
                continue;
            }
            Some(Ok(_)) | None => {}
            Some(Err(error)) => {
                warn!(error = %error, key = ?key, "Deleting malformed reference metadata refresh job");
                let key = ByteView::from(key.to_vec());
                stale.push((REFRESH_JOB_KEYSPACE.to_string(), key));
            }
        }
        delete_rows(storage, stale, None).await?;
    }
    Ok(ReferenceRefreshScan {
        jobs,
        has_more_due: more_rows,
        next_due_ms: None,
    })
}

async fn send(
    storage: &StorageHandle,
    effect: StorageEffect,
) -> Result<StorageEvent, ReferenceRefreshError> {
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        Event::Storage(event) => Ok(event),
        other => Err(ReferenceRefreshError::UnexpectedEvent(
            other.label().to_string(),
        )),
    }
}

async fn read_job(
    storage: &StorageHandle,
    key: &[u8],
    txn_id: Option<Ulid>,
) -> Result<Option<Result<ReferenceRefreshRecord, postcard::Error>>, ReferenceRefreshError> {
    let effect = StorageEffect::Read {
        key_space: REFRESH_JOB_KEYSPACE.to_string(),
        key: ByteView::from(key.to_vec()),
        txn_id,
    };
    match send(storage, effect).await? {
        StorageEvent::ReadResult { value, .. } => {
            Ok(value.map(|value| postcard::from_bytes(&value)))
        }
        other => Err(ReferenceRefreshError::UnexpectedEvent(
            Event::Storage(other).label().to_string(),
        )),
    }
}

async fn delete_rows(
    storage: &StorageHandle,
    deletes: Vec<(String, Key)>,
    txn_id: Option<Ulid>,
) -> Result<(), ReferenceRefreshError> {
    match send(storage, StorageEffect::BatchDelete { deletes, txn_id }).await? {
        StorageEvent::BatchDeleteResult { .. } => Ok(()),
        other => Err(ReferenceRefreshError::UnexpectedEvent(
            Event::Storage(other).label().to_string(),
        )),
    }
}

/// Removes a finished job, or replaces it with `next`, unless a newer request replaced it.
pub(super) async fn settle_job(
    storage: &StorageHandle,
    key: &[u8],
    job: &ReferenceRefreshRecord,
    next: Option<&ReferenceRefreshRecord>,
) -> Result<(), ReferenceRefreshError> {
    let txn_id = match send(storage, StorageEffect::StartTransaction { read: false }).await? {
        StorageEvent::TransactionStarted { txn_id } => txn_id,
        other => {
            return Err(ReferenceRefreshError::UnexpectedEvent(
                Event::Storage(other).label().to_string(),
            ));
        }
    };
    let result = async {
        if !matches!(read_job(storage, key, Some(txn_id)).await?, Some(Ok(stored)) if stored == *job)
        {
            return Ok(());
        }
        let mut deletes = vec![(
            REFRESH_DUE_KEYSPACE.to_string(),
            due_key(job.due_at_ms, key),
        )];
        let Some(next) = next else {
            let key = ByteView::from(key.to_vec());
            deletes.push((REFRESH_JOB_KEYSPACE.to_string(), key));
            return delete_rows(storage, deletes, Some(txn_id)).await;
        };
        delete_rows(storage, deletes, Some(txn_id)).await?;
        let writes = job_entries(next)?;
        match send(storage, StorageEffect::BatchWrite { writes, txn_id: Some(txn_id) }).await? {
            StorageEvent::BatchWriteResult { .. } => Ok(()),
            other => Err(ReferenceRefreshError::UnexpectedEvent(Event::Storage(other).label().to_string())),
        }
    }
    .await;
    let effect = match result {
        Ok(()) => StorageEffect::CommitTransaction { txn_id },
        Err(error) => {
            let _ = send(storage, StorageEffect::AbortTransaction { txn_id }).await;
            return Err(error);
        }
    };
    match send(storage, effect).await? {
        StorageEvent::TransactionCommitted { .. } => Ok(()),
        other => Err(ReferenceRefreshError::UnexpectedEvent(
            Event::Storage(other).label().to_string(),
        )),
    }
}

async fn reschedule_job(
    storage: &StorageHandle,
    key: &[u8],
    job: &ReferenceRefreshRecord,
    error: String,
) -> Result<u64, ReferenceRefreshError> {
    let attempts = job.attempts.saturating_add(1);
    let due_at_ms = unix_timestamp_millis().saturating_add(retry_delay_ms(attempts));
    let next_job = ReferenceRefreshRecord {
        refresh: job.refresh.clone(),
        due_at_ms,
        attempts,
        last_error: Some(error),
    };
    settle_job(storage, key, job, Some(&next_job)).await?;
    Ok(due_at_ms)
}
