use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    JOB_DEDUP_INDEX_KEYSPACE, JOB_KEYSPACE, JOB_OWNER_INDEX_KEYSPACE, JOB_SCHEDULE_INDEX_KEYSPACE,
};
use aruna_core::structs::{
    JobClaim, JobError, JobId, JobProgress, JobRecord, JobResultPayload, JobState,
    JobTransitionError, job_due_index_key, job_lease_index_key, job_owner_cursor,
    job_owner_index_key, job_owner_index_prefix, job_prune_index_key, job_record_key,
    parse_job_owner_index_key, validate_transition,
};
use aruna_core::types::{Key, KeySpace, NodeId, TxnId, UserId, Value};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::{JOB_LEASE_MS, JOB_MAX_ATTEMPTS, JOB_RETENTION_MS};
use crate::queue_backoff::queue_retry_after_ms;

type JobWrites = Vec<(KeySpace, Key, Value)>;
type JobDeletes = Vec<(KeySpace, Key)>;

/// Single decode chokepoint for persisted job records. Wrapping the record in a
/// version envelope later (#286) is a one-site change here.
pub fn decode_job_record(bytes: &[u8]) -> Result<JobRecord, ConversionError> {
    JobRecord::from_bytes(bytes)
}

#[derive(Debug, Error)]
pub enum JobMutationError {
    #[error("job not found")]
    NotFound,
    #[error("job claim token mismatch")]
    TokenMismatch,
    #[error(transparent)]
    IllegalTransition(#[from] JobTransitionError),
    #[error("{0}")]
    Storage(String),
}

/// The single schedule-index entry for a record follows its state: queued jobs
/// live under `due/`, claimed/running under `lease/`, terminal under `prune/`.
fn schedule_index_key_for(record: &JobRecord) -> Key {
    match record.state {
        JobState::Queued => job_due_index_key(record.due_at_ms, record.job_id),
        JobState::Claimed | JobState::Running => {
            let lease = record
                .claim
                .as_ref()
                .map(|claim| claim.lease_expires_at_ms)
                .unwrap_or(record.due_at_ms);
            job_lease_index_key(lease, record.job_id)
        }
        JobState::Succeeded | JobState::Failed | JobState::Cancelled => {
            let finished = record.finished_at_ms.unwrap_or(record.updated_at_ms);
            job_prune_index_key(finished.saturating_add(JOB_RETENTION_MS), record.job_id)
        }
    }
}

fn empty_value() -> Value {
    ByteView::from(Vec::new())
}

/// Storage writes for creating a fresh job: record + schedule index + owner index
/// (+ dedup index when a key is set). This is the composable primitive a producer
/// can splice into its own transaction; the ≤4 writes of the submit budget.
pub fn job_insert_entries(record: &JobRecord) -> Result<JobWrites, ConversionError> {
    let mut writes = vec![
        (
            JOB_KEYSPACE.to_string(),
            job_record_key(record.job_id),
            ByteView::from(record.to_bytes()?),
        ),
        (
            JOB_SCHEDULE_INDEX_KEYSPACE.to_string(),
            schedule_index_key_for(record),
            empty_value(),
        ),
        (
            JOB_OWNER_INDEX_KEYSPACE.to_string(),
            job_owner_index_key(record.created_by, record.created_at_ms, record.job_id),
            empty_value(),
        ),
    ];
    if let Some(dedup_key) = &record.dedup_key {
        writes.push((
            JOB_DEDUP_INDEX_KEYSPACE.to_string(),
            ByteView::from(dedup_key.clone()),
            ByteView::from(record.job_id.to_bytes().to_vec()),
        ));
    }
    Ok(writes)
}

/// Record + owner index + prune index deletes for a pruned terminal job. The
/// dedup entry was already removed when the job first went terminal.
pub fn job_prune_delete_entries(record: &JobRecord) -> JobDeletes {
    vec![
        (JOB_KEYSPACE.to_string(), job_record_key(record.job_id)),
        (
            JOB_OWNER_INDEX_KEYSPACE.to_string(),
            job_owner_index_key(record.created_by, record.created_at_ms, record.job_id),
        ),
        (
            JOB_SCHEDULE_INDEX_KEYSPACE.to_string(),
            schedule_index_key_for(record),
        ),
    ]
}

fn index_deltas(
    old: &JobRecord,
    new: &JobRecord,
) -> Result<(JobWrites, JobDeletes), ConversionError> {
    let mut writes = vec![(
        JOB_KEYSPACE.to_string(),
        job_record_key(new.job_id),
        ByteView::from(new.to_bytes()?),
    )];
    let mut deletes = Vec::new();

    let old_schedule = schedule_index_key_for(old);
    let new_schedule = schedule_index_key_for(new);
    if old_schedule != new_schedule {
        deletes.push((JOB_SCHEDULE_INDEX_KEYSPACE.to_string(), old_schedule));
    }
    writes.push((
        JOB_SCHEDULE_INDEX_KEYSPACE.to_string(),
        new_schedule,
        empty_value(),
    ));

    if let Some(dedup_key) = &old.dedup_key
        && !old.state.is_terminal()
        && new.state.is_terminal()
    {
        deletes.push((
            JOB_DEDUP_INDEX_KEYSPACE.to_string(),
            ByteView::from(dedup_key.clone()),
        ));
    }

    Ok((writes, deletes))
}

/// Outcome of a mutation closure: whether the caller wants the record persisted.
pub enum JobMutation {
    Persist,
    Skip,
}

fn guard_token(record: &JobRecord, token: Ulid) -> Result<(), JobMutationError> {
    match &record.claim {
        Some(claim) if claim.claim_token == token => Ok(()),
        _ => Err(JobMutationError::TokenMismatch),
    }
}

/// Read a job under a write transaction, apply `mutate`, and persist the record
/// plus its index deltas atomically. State changes are validated by the pure
/// transition guard so index and record can never disagree.
pub async fn mutate_job<F>(
    storage: &StorageHandle,
    job_id: JobId,
    mutate: F,
) -> Result<JobRecord, JobMutationError>
where
    F: FnOnce(&mut JobRecord) -> Result<JobMutation, JobMutationError>,
{
    let txn_id = start_write_txn(storage)
        .await
        .map_err(JobMutationError::Storage)?;

    let outcome = mutate_in_txn(storage, txn_id, job_id, mutate).await;
    match &outcome {
        Ok(_) => {
            if let Err(error) = commit_txn(storage, txn_id).await {
                return Err(JobMutationError::Storage(error));
            }
        }
        Err(_) => abort_txn(storage, txn_id).await,
    }
    outcome
}

async fn mutate_in_txn<F>(
    storage: &StorageHandle,
    txn_id: TxnId,
    job_id: JobId,
    mutate: F,
) -> Result<JobRecord, JobMutationError>
where
    F: FnOnce(&mut JobRecord) -> Result<JobMutation, JobMutationError>,
{
    let Some(mut record) = read_job_record(storage, job_id, Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)?
    else {
        return Err(JobMutationError::NotFound);
    };
    let old = record.clone();
    match mutate(&mut record)? {
        JobMutation::Skip => Ok(old),
        JobMutation::Persist => {
            if old.state != record.state {
                validate_transition(old.state, record.state)?;
            }
            let (writes, deletes) = index_deltas(&old, &record)
                .map_err(|error| JobMutationError::Storage(error.to_string()))?;
            batch_write(storage, writes, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            batch_delete(storage, deletes, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            Ok(record)
        }
    }
}

/// Read a job record, deleting and skipping a malformed row per the outbox precedent.
pub async fn read_job_record(
    storage: &StorageHandle,
    job_id: JobId,
    txn_id: Option<TxnId>,
) -> Result<Option<JobRecord>, String> {
    match read_raw(storage, JOB_KEYSPACE, job_record_key(job_id), txn_id).await? {
        Some(value) => match decode_job_record(&value) {
            Ok(record) => Ok(Some(record)),
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Deleting malformed job record");
                delete_raw(storage, JOB_KEYSPACE, job_record_key(job_id), None).await?;
                Ok(None)
            }
        },
        None => Ok(None),
    }
}

// --- claim / lease / transition operations -------------------------------------

pub enum ClaimOutcome {
    Claimed(JobRecord),
    CancelledFresh(JobRecord),
    NotEligible,
}

/// Claim a queued job for `holder_node_id`, or move a never-attempted
/// cancel-requested job straight to `Cancelled`.
pub async fn claim_job(
    storage: &StorageHandle,
    job_id: JobId,
    holder_node_id: NodeId,
    now_ms: u64,
) -> Result<ClaimOutcome, JobMutationError> {
    let record = mutate_job(storage, job_id, |record| {
        if record.state != JobState::Queued {
            return Ok(JobMutation::Skip);
        }
        record.updated_at_ms = now_ms;
        if record.cancel_requested && record.attempts == 0 {
            record.state = JobState::Cancelled;
            record.finished_at_ms = Some(now_ms);
            record.claim = None;
            return Ok(JobMutation::Persist);
        }
        record.state = JobState::Claimed;
        record.claim = Some(JobClaim {
            holder_node_id,
            claim_token: Ulid::new(),
            lease_expires_at_ms: now_ms.saturating_add(JOB_LEASE_MS),
        });
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(match record.state {
        JobState::Claimed => ClaimOutcome::Claimed(record),
        JobState::Cancelled => ClaimOutcome::CancelledFresh(record),
        _ => ClaimOutcome::NotEligible,
    })
}

pub async fn transition_to_running(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.state = JobState::Running;
        record.updated_at_ms = now_ms;
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_at_ms = now_ms.saturating_add(JOB_LEASE_MS);
        }
        Ok(JobMutation::Persist)
    })
    .await
}

pub struct RenewOutcome {
    pub cancel_requested: bool,
}

pub async fn renew_lease(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    progress: Option<JobProgress>,
) -> Result<RenewOutcome, JobMutationError> {
    let record = mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.updated_at_ms = now_ms;
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_at_ms = now_ms.saturating_add(JOB_LEASE_MS);
        }
        if let Some(progress) = progress {
            record.progress = progress;
        }
        Ok(JobMutation::Persist)
    })
    .await?;
    Ok(RenewOutcome {
        cancel_requested: record.cancel_requested,
    })
}

/// Persist an in-memory progress snapshot without touching the lease or state.
pub async fn flush_progress(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    progress: JobProgress,
    now_ms: u64,
) -> Result<RenewOutcome, JobMutationError> {
    let record = mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.progress = progress;
        record.updated_at_ms = now_ms;
        Ok(JobMutation::Persist)
    })
    .await?;
    Ok(RenewOutcome {
        cancel_requested: record.cancel_requested,
    })
}

pub async fn complete_job(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.progress = final_progress;
        record.result = Some(result);
        record.claim = None;
        Ok(JobMutation::Persist)
    })
    .await
}

pub async fn fail_job(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.state = JobState::Failed;
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.last_error = Some(error);
        record.claim = None;
        Ok(JobMutation::Persist)
    })
    .await
}

pub async fn cancel_running_job(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.state = JobState::Cancelled;
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.claim = None;
        Ok(JobMutation::Persist)
    })
    .await
}

pub enum RequeueOutcome {
    Requeued(JobRecord),
    Failed(JobRecord),
    Skipped,
}

/// Return a claimed/running job to the queue with `queue_retry_after_ms` backoff, or
/// move it to `Failed` once it has exhausted `JOB_MAX_ATTEMPTS`. `token` is `None`
/// for the lease-expiry sweep and startup recovery, which reclaim whatever holder is
/// recorded.
pub async fn requeue_job(
    storage: &StorageHandle,
    job_id: JobId,
    token: Option<Ulid>,
    now_ms: u64,
    error: Option<JobError>,
) -> Result<RequeueOutcome, JobMutationError> {
    let record = mutate_job(storage, job_id, |record| {
        if let Some(token) = token {
            guard_token(record, token)?;
        }
        if record.state.is_terminal() {
            return Ok(JobMutation::Skip);
        }
        if let Some(error) = error {
            record.last_error = Some(error);
        }
        record.attempts = record.attempts.saturating_add(1);
        record.updated_at_ms = now_ms;
        record.claim = None;
        if record.attempts >= JOB_MAX_ATTEMPTS {
            record.state = JobState::Failed;
            record.finished_at_ms = Some(now_ms);
        } else {
            record.state = JobState::Queued;
            record.due_at_ms = now_ms.saturating_add(queue_retry_after_ms(record.attempts));
        }
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(match record.state {
        JobState::Queued => RequeueOutcome::Requeued(record),
        JobState::Failed => RequeueOutcome::Failed(record),
        _ => RequeueOutcome::Skipped,
    })
}

pub enum CancelRequestOutcome {
    Flagged(JobRecord),
    AlreadyTerminal(JobRecord),
}

/// Set `cancel_requested` without changing state. Idempotent; a terminal job is a
/// no-op returning the current record.
pub async fn set_cancel_requested(
    storage: &StorageHandle,
    job_id: JobId,
    now_ms: u64,
) -> Result<CancelRequestOutcome, JobMutationError> {
    let record = mutate_job(storage, job_id, |record| {
        if record.state.is_terminal() || record.cancel_requested {
            return Ok(JobMutation::Skip);
        }
        record.cancel_requested = true;
        record.updated_at_ms = now_ms;
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(if record.state.is_terminal() {
        CancelRequestOutcome::AlreadyTerminal(record)
    } else {
        CancelRequestOutcome::Flagged(record)
    })
}

/// Owner-scoped listing, newest first, with an opaque 24-byte cursor and an
/// optional server-side state filter applied per page.
pub async fn list_jobs_for_user(
    storage: &StorageHandle,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    state_filter: Option<JobState>,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    let start_after = cursor.map(|cursor| {
        let mut key = user_id.to_storage_key();
        key.extend_from_slice(&cursor);
        ByteView::from(key)
    });
    let (values, _) = iter_prefix_page(
        storage,
        JOB_OWNER_INDEX_KEYSPACE,
        Some(job_owner_index_prefix(user_id)),
        start_after,
        limit.saturating_add(1),
        None,
    )
    .await?;

    let has_more = values.len() > limit;
    let mut records = Vec::new();
    let mut last_cursor = None;
    for (key, _) in values.into_iter().take(limit) {
        let (_, created_at_ms, job_id) =
            parse_job_owner_index_key(key.as_ref()).map_err(|error| error.to_string())?;
        last_cursor = Some(job_owner_cursor(created_at_ms, job_id));
        if let Some(record) = read_job_record(storage, job_id, None).await?
            && state_filter.is_none_or(|state| record.state == state)
        {
            records.push(record);
        }
    }
    Ok((records, if has_more { last_cursor } else { None }))
}

pub async fn find_dedup_job(
    storage: &StorageHandle,
    dedup_key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<JobId>, String> {
    match read_raw(
        storage,
        JOB_DEDUP_INDEX_KEYSPACE,
        ByteView::from(dedup_key.to_vec()),
        txn_id,
    )
    .await?
    {
        Some(value) => {
            let bytes: [u8; 16] = value
                .as_ref()
                .try_into()
                .map_err(|_| "malformed dedup index value".to_string())?;
            Ok(Some(JobId::from_bytes(bytes)))
        }
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

/// Head of a schedule-index prefix, i.e. the earliest `(timestamp, job_id)` still
/// enqueued under `due/`, `lease/`, or `prune/`.
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
        Some((key, _)) => match aruna_core::structs::parse_job_schedule_index_key(key.as_ref()) {
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

async fn start_write_txn(storage: &StorageHandle) -> Result<TxnId, String> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

async fn commit_txn(storage: &StorageHandle, txn_id: TxnId) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) {
    if let Event::Storage(StorageEvent::Error { error }) = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        warn!(error = %error, "Failed to abort job mutation transaction");
    }
}

async fn read_raw(
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

async fn batch_write(
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

async fn delete_raw(
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{
        JOB_LEASE_INDEX_PREFIX, JobPayload, RealmId, parse_job_schedule_index_key,
    };
    use aruna_core::types::UserId;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    fn temp_storage() -> (tempfile::TempDir, StorageHandle) {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        (dir, storage)
    }

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn queued_record(job_id: JobId) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                cleanup_marker: None,
            },
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            1_000,
            1_000,
            None,
        )
    }

    async fn schedule_keys(storage: &StorageHandle) -> Vec<Key> {
        let (values, _) =
            iter_prefix_page(storage, JOB_SCHEDULE_INDEX_KEYSPACE, None, None, 64, None)
                .await
                .expect("scan schedule index");
        values.into_iter().map(|(key, _)| key).collect()
    }

    #[tokio::test]
    async fn claim_sets_lease_and_moves_index_atomically() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([9u8; 16]);
        insert_job(&storage, &queued_record(job_id)).await.unwrap();

        let ClaimOutcome::Claimed(record) = claim_job(&storage, job_id, node_id(3), 5_000)
            .await
            .expect("claim succeeds")
        else {
            panic!("expected claimed outcome");
        };

        let claim = record.claim.expect("claim present");
        assert_eq!(record.state, JobState::Claimed);
        assert_eq!(claim.lease_expires_at_ms, 5_000 + JOB_LEASE_MS);

        let keys = schedule_keys(&storage).await;
        assert_eq!(keys.len(), 1, "exactly one schedule index entry");
        assert!(keys[0].starts_with(JOB_LEASE_INDEX_PREFIX));
        let (ts, parsed) = parse_job_schedule_index_key(keys[0].as_ref()).unwrap();
        assert_eq!(ts, 5_000 + JOB_LEASE_MS);
        assert_eq!(parsed, job_id);
    }

    #[tokio::test]
    async fn zombie_token_mismatch_is_rejected() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([1u8; 16]);
        insert_job(&storage, &queued_record(job_id)).await.unwrap();
        claim_job(&storage, job_id, node_id(3), 5_000)
            .await
            .unwrap();

        let err = complete_job(
            &storage,
            job_id,
            Ulid::new(),
            JobResultPayload::Probe { completed_steps: 1 },
            JobProgress::new("steps"),
            6_000,
        )
        .await
        .expect_err("stale token must be rejected");
        assert!(matches!(err, JobMutationError::TokenMismatch));

        let record = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.state,
            JobState::Claimed,
            "record unchanged after rejection"
        );
    }

    #[tokio::test]
    async fn requeue_increments_attempts_and_moves_back_to_due() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([4u8; 16]);
        insert_job(&storage, &queued_record(job_id)).await.unwrap();
        claim_job(&storage, job_id, node_id(3), 5_000)
            .await
            .unwrap();

        let RequeueOutcome::Requeued(record) = requeue_job(
            &storage,
            job_id,
            None,
            6_000,
            Some(JobError::retryable("lease expired")),
        )
        .await
        .unwrap() else {
            panic!("expected requeue");
        };
        assert_eq!(record.state, JobState::Queued);
        assert_eq!(record.attempts, 1);
        // queue_retry_after_ms(1) = 500ms.
        assert_eq!(record.due_at_ms, 6_500);
        assert!(record.claim.is_none());
    }

    #[tokio::test]
    async fn requeue_past_max_attempts_fails_terminally() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([6u8; 16]);
        let mut record = queued_record(job_id);
        record.attempts = JOB_MAX_ATTEMPTS - 1;
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: Ulid::new(),
            lease_expires_at_ms: 5_000,
        });
        insert_job(&storage, &record).await.unwrap();

        let RequeueOutcome::Failed(failed) = requeue_job(&storage, job_id, None, 6_000, None)
            .await
            .unwrap()
        else {
            panic!("expected terminal failure");
        };
        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.attempts, JOB_MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn cancel_before_claim_moves_fresh_job_to_cancelled() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([7u8; 16]);
        let mut record = queued_record(job_id);
        record.cancel_requested = true;
        insert_job(&storage, &record).await.unwrap();

        let ClaimOutcome::CancelledFresh(cancelled) =
            claim_job(&storage, job_id, node_id(3), 5_000)
                .await
                .unwrap()
        else {
            panic!("expected fresh cancellation");
        };
        assert_eq!(cancelled.state, JobState::Cancelled);
    }

    #[tokio::test]
    async fn cancel_request_on_terminal_is_noop() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([8u8; 16]);
        let mut record = queued_record(job_id);
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(2_000);
        insert_job(&storage, &record).await.unwrap();

        let outcome = set_cancel_requested(&storage, job_id, 9_000).await.unwrap();
        assert!(matches!(outcome, CancelRequestOutcome::AlreadyTerminal(_)));
        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert!(!stored.cancel_requested);
    }

    #[tokio::test]
    async fn terminal_transition_clears_dedup_index() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([5u8; 16]);
        let mut record = queued_record(job_id);
        record.dedup_key = Some(b"dedup".to_vec());
        record.state = JobState::Running;
        let token = Ulid::new();
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: token,
            lease_expires_at_ms: 5_000,
        });
        insert_job(&storage, &record).await.unwrap();
        assert_eq!(
            find_dedup_job(&storage, b"dedup", None).await.unwrap(),
            Some(job_id)
        );

        complete_job(
            &storage,
            job_id,
            token,
            JobResultPayload::Probe { completed_steps: 1 },
            JobProgress::new("steps"),
            6_000,
        )
        .await
        .unwrap();

        assert_eq!(
            find_dedup_job(&storage, b"dedup", None).await.unwrap(),
            None
        );
        let keys = schedule_keys(&storage).await;
        assert_eq!(keys.len(), 1);
        assert!(keys[0].starts_with(aruna_core::structs::JOB_PRUNE_INDEX_PREFIX));
    }

    #[tokio::test]
    async fn malformed_record_is_deleted_on_read() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([3u8; 16]);
        batch_write(
            &storage,
            vec![(
                JOB_KEYSPACE.to_string(),
                job_record_key(job_id),
                ByteView::from(vec![1, 2, 3]),
            )],
            None,
        )
        .await
        .unwrap();

        assert!(
            read_job_record(&storage, job_id, None)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_raw(&storage, JOB_KEYSPACE, job_record_key(job_id), None)
                .await
                .unwrap()
                .is_none()
        );
    }
}
