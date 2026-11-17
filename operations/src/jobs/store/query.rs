//! Lists and pages stored job rows and runs the shared storage transaction helpers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::drive;

use super::list_jobs::ListJobsOperation;

/// Owner-scoped listing, newest first, with an opaque cursor and record filter; see
/// [`ListJobsOperation`].
pub async fn list_user_jobs(
    context: &DriverContext,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    filter: impl Fn(&JobRecord) -> bool + Send,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    drive(
        ListJobsOperation::new(user_id, cursor, limit, filter),
        context,
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn list_job_entries(
    context: &DriverContext,
    job_id: JobId,
    last_key: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<(Vec<u8>, Value)>, Option<Vec<u8>>), String> {
    let (values, next) = iter_prefix_page(
        &context.storage_handle,
        JOB_ENTRY_KEYSPACE,
        Some(job_entry_prefix(job_id)),
        last_key.map(|key| job_entry_key(job_id, &key)),
        limit,
        None,
    )
    .await?;
    let rows = values
        .into_iter()
        .map(|(key, value)| Ok((parse_entry_key(job_id, key.as_ref())?, value)))
        .collect::<Result<Vec<_>, ConversionError>>()
        .map_err(|error| error.to_string())?;
    let next = next
        .map(|key| parse_entry_key(job_id, key.as_ref()))
        .transpose()
        .map_err(|error| error.to_string())?;
    Ok((rows, next))
}

pub(crate) async fn job_entry_deletes(
    context: &DriverContext,
    job_id: JobId,
    limit: usize,
) -> Result<(JobDeletes, bool), String> {
    let (values, next) = iter_prefix_page(
        &context.storage_handle,
        JOB_ENTRY_KEYSPACE,
        Some(job_entry_prefix(job_id)),
        None,
        limit,
        None,
    )
    .await?;
    Ok((
        values
            .into_iter()
            .map(|(key, _)| (JOB_ENTRY_KEYSPACE.to_string(), key))
            .collect(),
        next.is_some(),
    ))
}

/// One bounded page of a staging job's queue, manifest and lane rows; `true` while more remain.
pub(crate) async fn staging_row_deletes(
    context: &DriverContext,
    job_id: JobId,
    limit: usize,
) -> Result<(JobDeletes, bool), String> {
    let prefix = ByteView::from(job_id.to_bytes().to_vec());
    let mut deletes = Vec::new();
    let mut more = false;
    for key_space in [
        aruna_core::keyspaces::STAGING_QUEUE_KEYSPACE,
        aruna_core::keyspaces::STAGING_MANIFEST_KEYSPACE,
        aruna_core::keyspaces::STAGING_STATE_KEYSPACE,
    ] {
        let room = limit.saturating_sub(deletes.len());
        if room == 0 {
            return Ok((deletes, true));
        }
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            key_space,
            Some(prefix.clone()),
            None,
            room,
            None,
        )
        .await?;
        more |= next.is_some();
        deletes.extend(
            values
                .into_iter()
                .map(|(key, _)| (key_space.to_string(), key)),
        );
    }
    Ok((deletes, more))
}

pub async fn find_dedup_job(
    context: &DriverContext,
    created_by: UserId,
    dedup_key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<JobId>, String> {
    Ok(find_dedup_plan(context, created_by, dedup_key, txn_id)
        .await?
        .map(|(job_id, _)| job_id))
}

pub async fn find_dedup_plan(
    context: &DriverContext,
    created_by: UserId,
    dedup_key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<(JobId, [u8; 32])>, String> {
    match read_raw(
        context,
        DEDUP_INDEX_KEYSPACE,
        dedup_index_key(created_by, dedup_key),
        txn_id,
    )
    .await?
    {
        Some(value) => parse_dedup_value(value.as_ref())
            .map(Some)
            .map_err(|error| error.to_string()),
        None => Ok(None),
    }
}

/// Earliest `(timestamp, job_id)` under a schedule-index prefix.
pub async fn first_schedule_entry(
    context: &DriverContext,
    prefix: &[u8],
) -> Result<Option<(u64, JobId)>, String> {
    let (values, _) = iter_prefix_page(
        &context.storage_handle,
        SCHEDULE_INDEX_KEYSPACE,
        Some(ByteView::from(prefix.to_vec())),
        None,
        1,
        None,
    )
    .await?;
    match values.into_iter().next() {
        Some((key, _)) => {
            match aruna_core::structs::execution::job::parse_schedule_key(key.as_ref()) {
                Ok(parsed) => Ok(Some(parsed)),
                Err(error) => {
                    warn!(error = %error, "Deleting malformed job schedule index row");
                    delete_raw(context, SCHEDULE_INDEX_KEYSPACE, key, None).await?;
                    Ok(None)
                }
            }
        }
        None => Ok(None),
    }
}

// --- low-level storage plumbing ------------------------------------------------

pub async fn insert_job(context: &DriverContext, record: &JobRecord) -> Result<(), String> {
    let writes = job_insert_entries(record).map_err(|error| error.to_string())?;
    batch_write(context, writes, None).await
}

pub(super) async fn start_write_txn(context: &DriverContext) -> Result<TxnId, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {}", other.label())),
    }
}

pub(super) enum CommitResult {
    Committed,
    Conflict,
    Failed(String),
}

pub(super) async fn commit_txn(context: &DriverContext, txn_id: TxnId) -> CommitResult {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => CommitResult::Committed,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => CommitResult::Conflict,
        Event::Storage(StorageEvent::Error { error }) => CommitResult::Failed(error.to_string()),
        other => CommitResult::Failed(format!("unexpected storage event: {}", other.label())),
    }
}

pub(crate) enum CommitStep {
    Committed,
    Retry,
}

/// Commits one write transaction, sleeping between bounded conflict retries.
pub(crate) async fn commit_write(
    context: &DriverContext,
    txn_id: TxnId,
    attempt: u32,
    exhausted: &str,
) -> Result<CommitStep, JobMutationError> {
    match commit_txn(context, txn_id).await {
        CommitResult::Committed => Ok(CommitStep::Committed),
        CommitResult::Conflict if attempt + 1 < MUTATE_MAX_ATTEMPTS => {
            tokio::time::sleep(std::time::Duration::from_millis(1 << attempt.min(6))).await;
            Ok(CommitStep::Retry)
        }
        CommitResult::Conflict => Err(JobMutationError::Storage(exhausted.to_string())),
        CommitResult::Failed(error) => Err(JobMutationError::Storage(error)),
    }
}

pub(super) async fn abort_txn(context: &DriverContext, txn_id: TxnId) {
    if let Event::Storage(StorageEvent::Error { error }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        warn!(error = %error, "Failed to abort job mutation transaction");
    }
}

pub(super) async fn read_raw(
    context: &DriverContext,
    key_space: &str,
    key: Key,
    txn_id: Option<TxnId>,
) -> Result<Option<Value>, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {}", other.label())),
    }
}

pub(super) async fn batch_write(
    context: &DriverContext,
    writes: JobWrites,
    txn_id: Option<TxnId>,
) -> Result<(), String> {
    if writes.is_empty() {
        return Ok(());
    }
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite { writes, txn_id })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {}", other.label())),
    }
}

pub(crate) async fn batch_delete(
    context: &DriverContext,
    deletes: JobDeletes,
    txn_id: Option<TxnId>,
) -> Result<(), String> {
    if deletes.is_empty() {
        return Ok(());
    }
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete { deletes, txn_id })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {}", other.label())),
    }
}

pub(super) async fn delete_raw(
    context: &DriverContext,
    key_space: &str,
    key: Key,
    txn_id: Option<TxnId>,
) -> Result<(), String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: key_space.to_string(),
            key,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {}", other.label())),
    }
}
