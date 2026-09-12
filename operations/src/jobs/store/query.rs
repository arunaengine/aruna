use super::*;

/// Owner-scoped listing, newest first, with an opaque cursor and record filter.
/// Scans for `limit` records plus one live lookahead.
pub async fn list_user_jobs(
    storage: &StorageHandle,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    filter: impl Fn(&JobRecord) -> bool,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    if limit == 0 {
        return Ok((Vec::new(), None));
    }
    let prefix = owner_index_prefix(user_id);
    let mut start_after = cursor.map(|cursor| {
        let mut key = user_id.to_storage_key();
        key.extend_from_slice(&cursor);
        ByteView::from(key)
    });
    let mut records = Vec::new();
    let mut page_cursor = None;
    loop {
        let (values, _) = iter_prefix_page(
            storage,
            JOB_OWNER_INDEX_KEYSPACE,
            Some(prefix.clone()),
            start_after,
            limit,
            None,
        )
        .await?;
        if values.is_empty() {
            return Ok((records, None));
        }
        let scanned = values.len();
        let mut resume = None;
        for (key, value) in values {
            let (_, created_at_ms, job_id) =
                parse_owner_key(key.as_ref()).map_err(|error| error.to_string())?;
            resume = Some(key);
            let record = if value.is_empty() {
                read_job_record(storage, job_id, None).await?
            } else {
                decode_job_record(value.as_ref()).ok()
            };
            if let Some(record) = record
                && !record.payload.is_internal()
                && filter(&record)
            {
                if records.len() == limit {
                    return Ok((records, page_cursor));
                }
                records.push(record);
                if records.len() == limit {
                    page_cursor = Some(job_owner_cursor(created_at_ms, job_id));
                }
            }
        }
        if scanned < limit {
            return Ok((records, None));
        }
        start_after = resume;
    }
}

pub async fn list_job_entries(
    storage: &StorageHandle,
    job_id: JobId,
    last_key: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<(Vec<u8>, Value)>, Option<Vec<u8>>), String> {
    let (values, next) = iter_prefix_page(
        storage,
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
    storage: &StorageHandle,
    job_id: JobId,
    limit: usize,
) -> Result<(JobDeletes, bool), String> {
    let (values, next) = iter_prefix_page(
        storage,
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

pub async fn find_dedup_job(
    storage: &StorageHandle,
    created_by: UserId,
    dedup_key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<JobId>, String> {
    Ok(find_dedup_plan(storage, created_by, dedup_key, txn_id)
        .await?
        .map(|(job_id, _)| job_id))
}

pub async fn find_dedup_plan(
    storage: &StorageHandle,
    created_by: UserId,
    dedup_key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<(JobId, [u8; 32])>, String> {
    match read_raw(
        storage,
        JOB_DEDUP_INDEX_KEYSPACE,
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

// --- scan helpers --------------------------------------------------------------

pub async fn iter_prefix_page(
    storage: &StorageHandle,
    key_space: &str,
    prefix: Option<Key>,
    start_after: Option<Key>,
    limit: usize,
    txn_id: Option<TxnId>,
) -> Result<(Vec<(Key, Value)>, Option<Key>), String> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix,
            start: start_after.map(IterStart::After),
            limit,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

/// Earliest `(timestamp, job_id)` under a schedule-index prefix.
pub async fn first_schedule_entry(
    storage: &StorageHandle,
    prefix: &[u8],
) -> Result<Option<(u64, JobId)>, String> {
    let (values, _) = iter_prefix_page(
        storage,
        JOB_SCHEDULE_INDEX_KEYSPACE,
        Some(ByteView::from(prefix.to_vec())),
        None,
        1,
        None,
    )
    .await?;
    match values.into_iter().next() {
        Some((key, _)) => match aruna_core::structs::parse_schedule_key(key.as_ref()) {
            Ok(parsed) => Ok(Some(parsed)),
            Err(error) => {
                warn!(error = %error, "Deleting malformed job schedule index row");
                delete_raw(storage, JOB_SCHEDULE_INDEX_KEYSPACE, key, None).await?;
                Ok(None)
            }
        },
        None => Ok(None),
    }
}

// --- low-level storage plumbing ------------------------------------------------

pub async fn insert_job(storage: &StorageHandle, record: &JobRecord) -> Result<(), String> {
    let writes = job_insert_entries(record).map_err(|error| error.to_string())?;
    batch_write(storage, writes, None).await
}

pub(super) async fn start_write_txn(storage: &StorageHandle) -> Result<TxnId, String> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub(super) enum CommitResult {
    Committed,
    Conflict,
    Failed(String),
}

pub(super) async fn commit_txn(storage: &StorageHandle, txn_id: TxnId) -> CommitResult {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => CommitResult::Committed,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => CommitResult::Conflict,
        Event::Storage(StorageEvent::Error { error }) => CommitResult::Failed(error.to_string()),
        other => CommitResult::Failed(format!("unexpected storage event: {other:?}")),
    }
}

pub(super) enum CommitStep {
    Committed,
    Retry,
}

/// Commits one write transaction, sleeping between bounded conflict retries.
pub(super) async fn commit_write(
    storage: &StorageHandle,
    txn_id: TxnId,
    attempt: u32,
    exhausted: &str,
) -> Result<CommitStep, JobMutationError> {
    match commit_txn(storage, txn_id).await {
        CommitResult::Committed => Ok(CommitStep::Committed),
        CommitResult::Conflict if attempt + 1 < JOB_MUTATE_MAX_ATTEMPTS => {
            tokio::time::sleep(std::time::Duration::from_millis(1 << attempt.min(6))).await;
            Ok(CommitStep::Retry)
        }
        CommitResult::Conflict => Err(JobMutationError::Storage(exhausted.to_string())),
        CommitResult::Failed(error) => Err(JobMutationError::Storage(error)),
    }
}

pub(super) async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) {
    if let Event::Storage(StorageEvent::Error { error }) = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        warn!(error = %error, "Failed to abort job mutation transaction");
    }
}

pub(super) async fn read_raw(
    storage: &StorageHandle,
    key_space: &str,
    key: Key,
    txn_id: Option<TxnId>,
) -> Result<Option<Value>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub(super) async fn batch_write(
    storage: &StorageHandle,
    writes: JobWrites,
    txn_id: Option<TxnId>,
) -> Result<(), String> {
    if writes.is_empty() {
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite { writes, txn_id })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub(crate) async fn batch_delete(
    storage: &StorageHandle,
    deletes: JobDeletes,
    txn_id: Option<TxnId>,
) -> Result<(), String> {
    if deletes.is_empty() {
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::BatchDelete { deletes, txn_id })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub(super) async fn delete_raw(
    storage: &StorageHandle,
    key_space: &str,
    key: Key,
    txn_id: Option<TxnId>,
) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: key_space.to_string(),
            key,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}
