//! Parks jobs that need locked bucket keys and wakes them after unlock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{KeyWait, key_wait_key, parse_wait_key};
use aruna_core::structs::storage::encryption::BucketKeyRef;

use super::*;

/// Wake rows handled per transaction page.
pub const WAKE_PAGE: usize = 64;

#[derive(Debug)]
pub enum AwaitOutcome {
    Parked(JobRecord),
    /// The job is settled or cancel-requested; the caller follows the normal path.
    Skipped,
}

/// Parks a claimed job until every key in `waits` is unlocked. The claim and schedule entry go;
/// checkpoints, captured inputs, attempts and the due time stay. No retry deadline is set.
pub async fn park_job(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    waits: Vec<KeyWait>,
) -> Result<AwaitOutcome, JobMutationError> {
    if waits.is_empty() {
        return Err(JobMutationError::Storage("park without key waits".into()));
    }
    let list = postcard::to_allocvec(&waits)
        .map(ByteView::from)
        .map_err(|error| JobMutationError::Storage(error.to_string()))?;
    for attempt in 0..MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(storage)
            .await
            .map_err(JobMutationError::Storage)?;
        let mut parked = false;
        let mut mutate = |record: &mut JobRecord| {
            parked = false;
            guard_token(record, token)?;
            if record.is_settled() || record.cancel_requested {
                return Ok(JobMutation::Skip);
            }
            // An attempt intent may already have reached an executor; it cannot wait here.
            if record.attempt_intent.is_some() {
                return Err(JobMutationError::IntentConflict);
            }
            record.state = JobState::AwaitingKey;
            record.claim = None;
            record.updated_at_ms = now_ms;
            parked = true;
            Ok(JobMutation::Persist)
        };
        let result = Box::pin(mutate_in_txn(storage, txn_id, job_id, &mut mutate, None)).await;
        let result = match result {
            Ok(record) if parked => {
                let mut writes = vec![(
                    KEY_WAIT_KEYSPACE.to_string(),
                    job_wait_key(job_id),
                    list.clone(),
                )];
                writes.extend(waits.iter().map(|wait| {
                    (
                        KEY_WAIT_KEYSPACE.to_string(),
                        key_wait_key(wait.key, job_id),
                        empty_value(),
                    )
                }));
                batch_write(storage, writes, Some(txn_id))
                    .await
                    .map(|()| record)
                    .map_err(JobMutationError::Storage)
            }
            other => other,
        };
        match result {
            Ok(record) => {
                match commit_write(storage, txn_id, attempt, "job park exhausted retries").await? {
                    CommitStep::Committed if parked => return Ok(AwaitOutcome::Parked(record)),
                    CommitStep::Committed => return Ok(AwaitOutcome::Skipped),
                    CommitStep::Retry => continue,
                }
            }
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(JobMutationError::Storage(
        "job park exhausted retries".to_string(),
    ))
}

/// The keys a parked job still waits for; empty when it is not parked.
pub async fn read_key_waits(
    storage: &StorageHandle,
    job_id: JobId,
) -> Result<Vec<KeyWait>, String> {
    read_state(storage, KEY_WAIT_KEYSPACE, job_wait_key(job_id), "key wait")
        .await
        .map(Option::unwrap_or_default)
}

/// Marks `key` available for one job. The job returns to `Queued`, due now, only when no
/// other wait remains; its run then rechecks source, authorization and generation.
pub async fn satisfy_key_wait(
    storage: &StorageHandle,
    job_id: JobId,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<bool, JobMutationError> {
    for attempt in 0..MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(storage)
            .await
            .map_err(JobMutationError::Storage)?;
        match Box::pin(satisfy_in_txn(storage, txn_id, job_id, key, now_ms)).await {
            Ok(woken) => {
                match commit_write(storage, txn_id, attempt, "key wake exhausted retries").await? {
                    CommitStep::Committed => return Ok(woken),
                    CommitStep::Retry => continue,
                }
            }
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(JobMutationError::Storage(
        "key wake exhausted retries".to_string(),
    ))
}

async fn satisfy_in_txn(
    storage: &StorageHandle,
    txn_id: TxnId,
    job_id: JobId,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<bool, JobMutationError> {
    let storage_error = |error: String| JobMutationError::Storage(error);
    batch_delete(
        storage,
        vec![(KEY_WAIT_KEYSPACE.to_string(), key_wait_key(key, job_id))],
        Some(txn_id),
    )
    .await
    .map_err(storage_error)?;
    let list = read_raw(
        storage,
        KEY_WAIT_KEYSPACE,
        job_wait_key(job_id),
        Some(txn_id),
    )
    .await
    .map_err(storage_error)?;
    let Some(list) = list else {
        return Ok(false);
    };
    let mut waits: Vec<KeyWait> =
        postcard::from_bytes(list.as_ref()).map_err(|error| storage_error(error.to_string()))?;
    waits.retain(|wait| wait.key != key);
    let remaining = !waits.is_empty();
    let mut woken = false;
    let mut mutate = |record: &mut JobRecord| {
        woken = false;
        if record.state != JobState::AwaitingKey || remaining {
            return Ok(JobMutation::Skip);
        }
        record.state = JobState::Queued;
        record.due_at_ms = now_ms;
        record.updated_at_ms = now_ms;
        woken = true;
        Ok(JobMutation::Persist)
    };
    match Box::pin(mutate_in_txn(storage, txn_id, job_id, &mut mutate, None)).await {
        Ok(_) | Err(JobMutationError::NotFound) => {}
        Err(error) => return Err(error),
    }
    if remaining {
        let value =
            postcard::to_allocvec(&waits).map_err(|error| storage_error(error.to_string()))?;
        batch_write(
            storage,
            vec![(
                KEY_WAIT_KEYSPACE.to_string(),
                job_wait_key(job_id),
                ByteView::from(value),
            )],
            Some(txn_id),
        )
        .await
        .map_err(storage_error)?;
    } else if !woken {
        // A list without a parked job is stale: the job was cancelled or pruned.
        batch_delete(
            storage,
            vec![(KEY_WAIT_KEYSPACE.to_string(), job_wait_key(job_id))],
            Some(txn_id),
        )
        .await
        .map_err(storage_error)?;
    }
    Ok(woken)
}

/// Wakes one page of jobs waiting for `key`. Handled rows are deleted, so callers repeat
/// until this reports no further page.
pub async fn wake_key_page(
    storage: &StorageHandle,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<(usize, bool), JobMutationError> {
    let prefix = ByteView::from([&b"k"[..], &key.key()].concat());
    let (rows, _) = iter_prefix_page(
        storage,
        KEY_WAIT_KEYSPACE,
        Some(prefix),
        None,
        WAKE_PAGE,
        None,
    )
    .await
    .map_err(JobMutationError::Storage)?;
    let mut woken = 0;
    for (row, _) in &rows {
        let Some((_, job_id)) = parse_wait_key(row.as_ref()) else {
            warn!("Deleting malformed key wait row");
            delete_raw(storage, KEY_WAIT_KEYSPACE, row.clone(), None)
                .await
                .map_err(JobMutationError::Storage)?;
            continue;
        };
        if satisfy_key_wait(storage, job_id, key, now_ms).await? {
            woken += 1;
        }
    }
    Ok((woken, rows.len() == WAKE_PAGE))
}

/// Wakes every job waiting for `key` in bounded pages; call after the key is installed.
pub async fn wake_key_waits(
    storage: &StorageHandle,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<usize, JobMutationError> {
    let mut total = 0;
    loop {
        let (woken, more) = wake_key_page(storage, key, now_ms).await?;
        total += woken;
        if !more {
            return Ok(total);
        }
    }
}

#[cfg(test)]
#[path = "key_wait_tests.rs"]
mod tests;
