use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    JOB_DEDUP_INDEX_KEYSPACE, JOB_KEYSPACE, JOB_OWNER_INDEX_KEYSPACE, JOB_RUN_CRATE_KEYSPACE,
    JOB_SCHEDULE_INDEX_KEYSPACE,
};
use aruna_core::structs::{
    AttemptIntent, JobClaim, JobError, JobExecutionClass, JobId, JobPayload, JobProgress,
    JobRecord, JobResultPayload, JobState, JobTransitionError, RunCrateStatus, crate_job_id,
    encode_job_dedup_value, job_due_index_key, job_lease_index_key, job_owner_cursor,
    job_owner_index_key, job_owner_index_prefix, job_prune_index_key, job_record_key,
    job_run_crate_key, parse_job_dedup_value, parse_job_owner_index_key, run_crate_dedup_key,
    validate_transition,
};
use aruna_core::types::{Key, KeySpace, NodeId, TxnId, UserId, Value};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::{JOB_LEASE_MS, JOB_MAX_ATTEMPTS, JOB_MUTATE_MAX_ATTEMPTS, JOB_RETENTION_MS};
use crate::queue_backoff::queue_retry_after_ms;

type JobWrites = Vec<(KeySpace, Key, Value)>;
type JobDeletes = Vec<(KeySpace, Key)>;

/// Single decode chokepoint; wrappable in a version envelope later (#286).
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

/// Schedule-index key by state: queued -> due/, claimed/running -> lease/, terminal -> prune/.
fn schedule_index_key_for(record: &JobRecord) -> Key {
    match record.state {
        JobState::Queued => job_due_index_key(record.due_at_ms, record.job_id),
        JobState::Claimed
        | JobState::Preparing
        | JobState::Ready
        | JobState::Running
        | JobState::Cancelling
        | JobState::Indeterminate => {
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

pub(super) fn job_dedup_index_key(created_by: UserId, dedup_key: &[u8]) -> Key {
    let mut key = created_by.to_storage_key();
    key.extend_from_slice(dedup_key);
    ByteView::from(key)
}

fn empty_value() -> Value {
    ByteView::from(Vec::new())
}

/// Writes creating a fresh job (<=4 keys); composable into a producer transaction.
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
            job_dedup_index_key(record.created_by, dedup_key),
            ByteView::from(encode_job_dedup_value(
                record.job_id,
                record.plan_digest.unwrap_or_default(),
            )),
        ));
    }
    Ok(writes)
}

/// Deletes for a pruned terminal job (its dedup entry is already gone).
pub fn job_prune_delete_entries(record: &JobRecord) -> JobDeletes {
    vec![
        (JOB_KEYSPACE.to_string(), job_record_key(record.job_id)),
        (
            JOB_RUN_CRATE_KEYSPACE.to_string(),
            job_run_crate_key(record.job_id),
        ),
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

    // Dedup removal is guarded by job id in cleanup_dedup_entry, not here.
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

/// Read, mutate, and persist a job with its index deltas in one transaction, with
/// bounded OCC retry so a commit conflict re-reads and re-applies rather than losing.
pub async fn mutate_job<F>(
    storage: &StorageHandle,
    job_id: JobId,
    mut mutate: F,
) -> Result<JobRecord, JobMutationError>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError>,
{
    for attempt in 0..JOB_MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(storage)
            .await
            .map_err(JobMutationError::Storage)?;
        match mutate_in_txn(storage, txn_id, job_id, &mut mutate).await {
            Ok(record) => match commit_txn(storage, txn_id).await {
                CommitResult::Committed => return Ok(record),
                CommitResult::Conflict if attempt + 1 < JOB_MUTATE_MAX_ATTEMPTS => {
                    tokio::time::sleep(std::time::Duration::from_millis(1 << attempt.min(6))).await;
                }
                CommitResult::Conflict => {
                    return Err(JobMutationError::Storage(
                        "job mutation exhausted conflict retries".to_string(),
                    ));
                }
                CommitResult::Failed(error) => return Err(JobMutationError::Storage(error)),
            },
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(JobMutationError::Storage(
        "job mutation exhausted conflict retries".to_string(),
    ))
}

async fn mutate_in_txn<F>(
    storage: &StorageHandle,
    txn_id: TxnId,
    job_id: JobId,
    mutate: &mut F,
) -> Result<JobRecord, JobMutationError>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError>,
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
                validate_transition(old.execution_class, old.state, record.state)?;
            }
            let (writes, deletes) = index_deltas(&old, &record)
                .map_err(|error| JobMutationError::Storage(error.to_string()))?;
            batch_write(storage, writes, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            batch_delete(storage, deletes, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            if !old.state.is_terminal() && record.state.is_terminal() {
                insert_crate_obligation(storage, txn_id, &record).await?;
                mark_crate_failed(storage, txn_id, &record).await?;
            }
            cleanup_dedup_entry(storage, txn_id, &old, &record).await?;
            Ok(record)
        }
    }
}

async fn insert_crate_obligation(
    storage: &StorageHandle,
    txn_id: TxnId,
    record: &JobRecord,
) -> Result<(), JobMutationError> {
    if !matches!(&record.payload, JobPayload::Execution(_)) {
        return Ok(());
    }
    let now_ms = record.finished_at_ms.unwrap_or(record.updated_at_ms);
    let child = JobRecord::new(
        crate_job_id(record.job_id),
        JobPayload::WriteRunCrate {
            for_job: record.job_id,
        },
        record.created_by,
        record.owner_node_id,
        now_ms,
        now_ms,
        Some(run_crate_dedup_key(record.job_id)),
    );
    let mut writes =
        job_insert_entries(&child).map_err(|error| JobMutationError::Storage(error.to_string()))?;
    writes.push((
        JOB_RUN_CRATE_KEYSPACE.to_string(),
        job_run_crate_key(record.job_id),
        ByteView::from(
            RunCrateStatus::Pending
                .to_bytes()
                .map_err(|error| JobMutationError::Storage(error.to_string()))?,
        ),
    ));
    batch_write(storage, writes, Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)
}

async fn mark_crate_failed(
    storage: &StorageHandle,
    txn_id: TxnId,
    record: &JobRecord,
) -> Result<(), JobMutationError> {
    let JobPayload::WriteRunCrate { for_job } = &record.payload else {
        return Ok(());
    };
    if !matches!(record.state, JobState::Failed | JobState::Cancelled) {
        return Ok(());
    }
    let key = job_run_crate_key(*for_job);
    if let Some(value) = read_raw(storage, JOB_RUN_CRATE_KEYSPACE, key.clone(), Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)?
        && !matches!(
            RunCrateStatus::from_bytes(value.as_ref())
                .map_err(|error| JobMutationError::Storage(error.to_string()))?,
            RunCrateStatus::Pending
        )
    {
        return Ok(());
    }
    let status = RunCrateStatus::Failed {
        message: record
            .last_error
            .as_ref()
            .map(|error| error.message.clone())
            .unwrap_or_else(|| match record.state {
                JobState::Cancelled => "run-crate obligation cancelled".to_string(),
                _ => "run-crate retries exhausted".to_string(),
            }),
    };
    batch_write(
        storage,
        vec![(
            JOB_RUN_CRATE_KEYSPACE.to_string(),
            key,
            ByteView::from(
                status
                    .to_bytes()
                    .map_err(|error| JobMutationError::Storage(error.to_string()))?,
            ),
        )],
        Some(txn_id),
    )
    .await
    .map_err(JobMutationError::Storage)
}

/// Remove the dedup row only when it still references THIS job: a raced submit may
/// have repointed it at a newer job that must survive this one going terminal.
async fn cleanup_dedup_entry(
    storage: &StorageHandle,
    txn_id: TxnId,
    old: &JobRecord,
    new: &JobRecord,
) -> Result<(), JobMutationError> {
    let Some(dedup_key) = &old.dedup_key else {
        return Ok(());
    };
    if old.state.is_terminal() || !new.state.is_terminal() {
        return Ok(());
    }
    let key = job_dedup_index_key(old.created_by, dedup_key);
    let current = read_raw(storage, JOB_DEDUP_INDEX_KEYSPACE, key.clone(), Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)?;
    let still_ours = current
        .as_deref()
        .and_then(|bytes| parse_job_dedup_value(bytes).ok())
        .is_some_and(|(job_id, _)| job_id == old.job_id);
    if still_ours {
        delete_raw(storage, JOB_DEDUP_INDEX_KEYSPACE, key, Some(txn_id))
            .await
            .map_err(JobMutationError::Storage)?;
    }
    Ok(())
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

/// Claim a queued job, or cancel it directly if it never ran and is cancel-requested.
pub async fn claim_job(
    storage: &StorageHandle,
    job_id: JobId,
    holder_node_id: NodeId,
    now_ms: u64,
) -> Result<ClaimOutcome, JobMutationError> {
    let mut claimed_now = false;
    let mut cancelled_fresh = false;
    let record = mutate_job(storage, job_id, |record| {
        claimed_now = false;
        cancelled_fresh = false;
        if record.state != JobState::Queued {
            return Ok(JobMutation::Skip);
        }
        record.updated_at_ms = now_ms;
        if record.cancel_requested && !record.has_run && record.attempt_intent.is_none() {
            record.state = JobState::Cancelled;
            record.finished_at_ms = Some(now_ms);
            record.claim = None;
            cancelled_fresh = true;
            return Ok(JobMutation::Persist);
        }
        record.state = JobState::Claimed;
        record.claim = Some(JobClaim {
            holder_node_id,
            claim_token: Ulid::r#gen(),
            lease_expires_at_ms: now_ms.saturating_add(JOB_LEASE_MS),
        });
        claimed_now = true;
        Ok(JobMutation::Persist)
    })
    .await?;

    // Only a claim WE won returns a token; an already-Claimed job is NotEligible.
    Ok(if claimed_now {
        ClaimOutcome::Claimed(record)
    } else if cancelled_fresh {
        ClaimOutcome::CancelledFresh(record)
    } else {
        ClaimOutcome::NotEligible
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
        record.has_run = true;
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
        if let Some(progress) = &progress {
            record.progress = progress.clone();
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
        record.progress = progress.clone();
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
        record.progress = final_progress.clone();
        record.result = Some(result.clone());
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
        record.last_error = Some(error.clone());
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
    /// An external attempt lost its lease or the node restarted: requeuing would
    /// double-run the container, so the record is left untouched for reconciliation.
    NeedsReconcile(JobRecord),
    Skipped,
}

/// Re-queue with backoff, or fail once `JOB_MAX_ATTEMPTS` is spent. `token` is `None`
/// for the lease sweep and startup recovery. `require_expired_before` makes the sweep
/// re-check, in-txn, that the job still holds an expired lease: a revived renew is not
/// revoked, and a claim-less record (already requeued) is not charged a second attempt.
/// An external attempt that passes those checks is never requeued here; it returns
/// `NeedsReconcile` untouched.
pub async fn requeue_job(
    storage: &StorageHandle,
    job_id: JobId,
    token: Option<Ulid>,
    now_ms: u64,
    require_expired_before: Option<u64>,
    error: Option<JobError>,
) -> Result<RequeueOutcome, JobMutationError> {
    let mut persisted = false;
    let mut needs_reconcile = false;
    let record = mutate_job(storage, job_id, |record| {
        persisted = false;
        needs_reconcile = false;
        if let Some(token) = token {
            guard_token(record, token)?;
        }
        if record.state.is_terminal() {
            return Ok(JobMutation::Skip);
        }
        if let Some(now) = require_expired_before {
            let Some(claim) = &record.claim else {
                return Ok(JobMutation::Skip);
            };
            if claim.lease_expires_at_ms > now {
                return Ok(JobMutation::Skip);
            }
        }
        // Checked AFTER the expired-lease re-check: a live renewed attempt is a
        // plain Skip and must not be routed to the reconciler.
        if record.execution_class == JobExecutionClass::ExternalAttempt {
            needs_reconcile = true;
            return Ok(JobMutation::Skip);
        }
        if let Some(error) = error.clone() {
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
            record.progress = JobProgress::new(record.payload.progress_unit());
        }
        persisted = true;
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(if needs_reconcile {
        RequeueOutcome::NeedsReconcile(record)
    } else if !persisted {
        RequeueOutcome::Skipped
    } else if record.state == JobState::Failed {
        RequeueOutcome::Failed(record)
    } else {
        RequeueOutcome::Requeued(record)
    })
}

#[derive(Debug)]
pub enum ReleaseOutcome {
    Released(JobRecord),
    Skipped,
}

/// Hand a lease back without spending an attempt: clear the claim, re-queue, make it
/// due now. Guarded by `token`, so a job another node already took over is left alone.
/// Distinct from `requeue_job`, whose attempt increment is unconditional.
pub async fn release_job(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<ReleaseOutcome, JobMutationError> {
    let mut released = false;
    let record = mutate_job(storage, job_id, |record| {
        released = false;
        guard_token(record, token)?;
        if record.state.is_terminal() {
            return Ok(JobMutation::Skip);
        }
        record.state = JobState::Queued;
        record.claim = None;
        record.due_at_ms = now_ms;
        record.updated_at_ms = now_ms;
        released = true;
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(if released {
        ReleaseOutcome::Released(record)
    } else {
        ReleaseOutcome::Skipped
    })
}

/// Hand an external execution to reconciliation without re-queuing it.
/// Rotating the token fences the old supervisor; the expired replacement claim keeps
/// the unchanged state and attempt intent discoverable by the lease sweep.
pub async fn handoff_external_attempt(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<ReleaseOutcome, JobMutationError> {
    let mut released = false;
    let record = mutate_job(storage, job_id, |record| {
        released = false;
        guard_token(record, token)?;
        if record.execution_class != JobExecutionClass::ExternalAttempt
            || record.state.is_terminal()
        {
            return Ok(JobMutation::Skip);
        }
        if let Some(claim) = record.claim.as_mut() {
            claim.claim_token = Ulid::r#gen();
            claim.lease_expires_at_ms = now_ms;
        }
        record.updated_at_ms = now_ms;
        released = true;
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(if released {
        ReleaseOutcome::Released(record)
    } else {
        ReleaseOutcome::Skipped
    })
}

/// Write-ahead attempt intent: record the deterministic external identity BEFORE any
/// external submit so a lost attempt can be adopted by name. Token-fenced; no state
/// change.
pub async fn record_attempt_intent(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    intent: AttemptIntent,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.attempt_intent = Some(intent.clone());
        record.updated_at_ms = now_ms;
        Ok(JobMutation::Persist)
    })
    .await
}

/// Advance a claimed execution job to `Preparing`, renewing the lease. The
/// workspace is built and inputs staged in this phase.
pub async fn transition_to_preparing(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.state = JobState::Preparing;
        record.updated_at_ms = now_ms;
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_at_ms = now_ms.saturating_add(JOB_LEASE_MS);
        }
        Ok(JobMutation::Persist)
    })
    .await
}

/// Record the durable workspace bucket name on a claimed execution job; no state
/// change so `Preparing` can persist the bucket before flipping it `Active`.
pub async fn set_workspace_bucket(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    bucket: String,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.workspace_bucket = Some(bucket.clone());
        record.updated_at_ms = now_ms;
        Ok(JobMutation::Persist)
    })
    .await
}

/// `Preparing -> Ready`: workspace prepared and credentials minted.
pub async fn transition_to_ready(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.state = JobState::Ready;
        record.updated_at_ms = now_ms;
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_at_ms = now_ms.saturating_add(JOB_LEASE_MS);
        }
        Ok(JobMutation::Persist)
    })
    .await
}

/// Commit `Running` for an external attempt: from `Ready` after the backend
/// accepts the fenced attempt, or from `Indeterminate` when reconcile re-adopts a
/// still-running container.
pub async fn transition_external_to_running(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    started_at_ms: Option<u64>,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        if let Some(started_at_ms) = started_at_ms {
            record.started_at_ms.get_or_insert(started_at_ms);
        }
        record.state = JobState::Running;
        record.updated_at_ms = now_ms;
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_at_ms = now_ms.saturating_add(JOB_LEASE_MS);
        }
        Ok(JobMutation::Persist)
    })
    .await
}

/// Persist backend start evidence without changing execution state or timestamps.
pub async fn record_attempt_started(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    started_at_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        if record.started_at_ms.is_some() {
            return Ok(JobMutation::Skip);
        }
        record.started_at_ms = Some(started_at_ms);
        Ok(JobMutation::Persist)
    })
    .await
}

/// `Running/Indeterminate -> Cancelling`: a durable cancel intent precedes the
/// backend stop so no evidence is lost across a crash.
pub async fn transition_to_cancelling(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.state = JobState::Cancelling;
        record.updated_at_ms = now_ms;
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_at_ms = now_ms.saturating_add(JOB_LEASE_MS);
        }
        Ok(JobMutation::Persist)
    })
    .await
}

/// Park an ambiguous external attempt in `Indeterminate`, keeping the claim so the
/// lease sweep later re-routes it to reconciliation. Exits only on evidence.
pub async fn mark_indeterminate(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.state = JobState::Indeterminate;
        record.updated_at_ms = now_ms;
        record.last_error = Some(error.clone());
        Ok(JobMutation::Persist)
    })
    .await
}

/// Terminal `Failed` for an execution job, capturing the exit evidence in the
/// result so a failed run still yields a crate.
pub async fn fail_execution(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    error: JobError,
    result: JobResultPayload,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.state = JobState::Failed;
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.last_error = Some(error.clone());
        record.result = Some(result.clone());
        record.claim = None;
        Ok(JobMutation::Persist)
    })
    .await
}

/// Terminal `Cancelled` for an execution job (from `Cancelling`), recording the
/// exit evidence.
pub async fn cancel_execution(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.state = JobState::Cancelled;
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.result = Some(result.clone());
        record.claim = None;
        Ok(JobMutation::Persist)
    })
    .await
}

#[derive(Debug)]
pub enum AdoptOutcome {
    Adopted(JobRecord),
    /// Terminal, or the lease is still live: leave the current holder alone.
    Skipped,
}

/// Take over a lost external attempt with a fresh claim token so the reconciler
/// can supervise it. No token guard: the previous holder is provably dead (lease
/// swept or node restarted). State is preserved for the reconcile decision.
pub async fn adopt_external_attempt(
    storage: &StorageHandle,
    job_id: JobId,
    holder_node_id: NodeId,
    now_ms: u64,
) -> Result<AdoptOutcome, JobMutationError> {
    let mut adopted = false;
    let record = mutate_job(storage, job_id, |record| {
        adopted = false;
        if record.state.is_terminal() {
            return Ok(JobMutation::Skip);
        }
        // Re-check the lease INSIDE the transaction: a holder that renewed between the
        // sweep's read and here is still alive, and stealing its claim would put two
        // supervisors on one container.
        if let Some(claim) = &record.claim
            && claim.lease_expires_at_ms > now_ms
        {
            return Ok(JobMutation::Skip);
        }
        record.updated_at_ms = now_ms;
        record.claim = Some(JobClaim {
            holder_node_id,
            claim_token: Ulid::r#gen(),
            lease_expires_at_ms: now_ms.saturating_add(JOB_LEASE_MS),
        });
        adopted = true;
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(if adopted {
        AdoptOutcome::Adopted(record)
    } else {
        AdoptOutcome::Skipped
    })
}

/// Requeue an execution job that failed BEFORE its attempt was submitted (image
/// pull, prepare error): from `Preparing`/`Ready` back to `Queued` with backoff,
/// or terminal `Failed` once attempts are spent. Safe because no container exists.
pub async fn requeue_before_attempt(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    error: JobError,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        record.last_error = Some(error.clone());
        record.attempts = record.attempts.saturating_add(1);
        record.updated_at_ms = now_ms;
        record.claim = None;
        record.attempt_intent = None;
        if record.attempts >= JOB_MAX_ATTEMPTS {
            record.state = JobState::Failed;
            record.finished_at_ms = Some(now_ms);
        } else {
            record.state = JobState::Queued;
            record.due_at_ms = now_ms.saturating_add(queue_retry_after_ms(record.attempts));
        }
        Ok(JobMutation::Persist)
    })
    .await
}

/// Persist the run-crate obligation status in its side keyspace, leaving the
/// terminal parent record untouched.
pub async fn put_run_crate_status(
    storage: &StorageHandle,
    job_id: JobId,
    status: &aruna_core::structs::RunCrateStatus,
) -> Result<(), String> {
    let bytes = status.to_bytes().map_err(|error| error.to_string())?;
    batch_write(
        storage,
        vec![(
            JOB_RUN_CRATE_KEYSPACE.to_string(),
            aruna_core::structs::job_run_crate_key(job_id),
            ByteView::from(bytes),
        )],
        None,
    )
    .await
}

/// Read the run-crate obligation status for `job_id`, if recorded.
pub async fn read_run_crate_status(
    storage: &StorageHandle,
    job_id: JobId,
) -> Result<Option<aruna_core::structs::RunCrateStatus>, String> {
    match read_raw(
        storage,
        JOB_RUN_CRATE_KEYSPACE,
        aruna_core::structs::job_run_crate_key(job_id),
        None,
    )
    .await?
    {
        Some(value) => aruna_core::structs::RunCrateStatus::from_bytes(value.as_ref())
            .map(Some)
            .map_err(|error| error.to_string()),
        None => Ok(None),
    }
}

pub enum CancelRequestOutcome {
    Cancelled(JobRecord),
    Flagged(JobRecord),
    AlreadyTerminal(JobRecord),
}

/// Idempotently set `cancel_requested`; a terminal job is a no-op. A queued job that never
/// ran is terminalized right here: it owns no side effects to clean up, and leaving it for
/// the drain would strand it `Queued` for as long as the executor stays at capacity.
pub async fn set_cancel_requested(
    storage: &StorageHandle,
    job_id: JobId,
    now_ms: u64,
) -> Result<CancelRequestOutcome, JobMutationError> {
    let mut cancelled_now = false;
    let record = mutate_job(storage, job_id, |record| {
        cancelled_now = false;
        if record.state.is_terminal() {
            return Ok(JobMutation::Skip);
        }
        // `has_run`, not `attempts == 0`: a job interrupted by a shutdown hands its lease
        // back without spending an attempt, so attempts alone cannot prove it never ran.
        if record.state == JobState::Queued && !record.has_run && record.attempt_intent.is_none() {
            record.cancel_requested = true;
            record.state = JobState::Cancelled;
            record.finished_at_ms = Some(now_ms);
            record.updated_at_ms = now_ms;
            record.claim = None;
            cancelled_now = true;
            return Ok(JobMutation::Persist);
        }
        if record.cancel_requested {
            return Ok(JobMutation::Skip);
        }
        record.cancel_requested = true;
        record.updated_at_ms = now_ms;
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(if cancelled_now {
        CancelRequestOutcome::Cancelled(record)
    } else if record.state.is_terminal() {
        CancelRequestOutcome::AlreadyTerminal(record)
    } else {
        CancelRequestOutcome::Flagged(record)
    })
}

/// Owner-scoped listing, newest first, with an opaque cursor and optional state filter.
/// Scans across owner-index pages until `limit` records match the filter or the index is
/// exhausted, so a filtered page never returns empty with a dangling `next_cursor`.
pub async fn list_jobs_for_user(
    storage: &StorageHandle,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    state_filter: Option<JobState>,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    if limit == 0 {
        return Ok((Vec::new(), None));
    }
    let prefix = job_owner_index_prefix(user_id);
    let mut start_after = cursor.map(|cursor| {
        let mut key = user_id.to_storage_key();
        key.extend_from_slice(&cursor);
        ByteView::from(key)
    });
    let mut records = Vec::new();
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
        for (key, _) in values {
            let (_, created_at_ms, job_id) =
                parse_job_owner_index_key(key.as_ref()).map_err(|error| error.to_string())?;
            resume = Some(key);
            if let Some(record) = read_job_record(storage, job_id, None).await?
                && state_filter.is_none_or(|state| record.state == state)
            {
                records.push(record);
                if records.len() == limit {
                    let next = job_owner_cursor(created_at_ms, job_id);
                    let mut resume_key = user_id.to_storage_key();
                    resume_key.extend_from_slice(&next);
                    let (peek, _) = iter_prefix_page(
                        storage,
                        JOB_OWNER_INDEX_KEYSPACE,
                        Some(prefix.clone()),
                        Some(ByteView::from(resume_key)),
                        1,
                        None,
                    )
                    .await?;
                    return Ok((records, if peek.is_empty() { None } else { Some(next) }));
                }
            }
        }
        if scanned < limit {
            return Ok((records, None));
        }
        start_after = resume;
    }
}

pub async fn find_dedup_job(
    storage: &StorageHandle,
    created_by: UserId,
    dedup_key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<JobId>, String> {
    match read_raw(
        storage,
        JOB_DEDUP_INDEX_KEYSPACE,
        job_dedup_index_key(created_by, dedup_key),
        txn_id,
    )
    .await?
    {
        Some(value) => {
            let (job_id, _) =
                parse_job_dedup_value(value.as_ref()).map_err(|error| error.to_string())?;
            Ok(Some(job_id))
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

enum CommitResult {
    Committed,
    Conflict,
    Failed(String),
}

async fn commit_txn(storage: &StorageHandle, txn_id: TxnId) -> CommitResult {
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
                panic_at: None,
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

    // Stealing a claim whose lease is still live would put two supervisors on one
    // container. Only a genuinely expired lease may be adopted.
    #[tokio::test]
    async fn adopt_spares_live() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([0x77; 16]);
        let mut record = queued_record(job_id);
        record.execution_class = JobExecutionClass::ExternalAttempt;
        record.state = JobState::Running;
        let live_token = Ulid::r#gen();
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: live_token,
            lease_expires_at_ms: 10_000,
        });
        insert_job(&storage, &record).await.unwrap();

        // The holder renewed: its lease outlives `now`, so adoption must skip.
        let outcome = adopt_external_attempt(&storage, job_id, node_id(4), 9_000)
            .await
            .unwrap();
        assert!(matches!(outcome, AdoptOutcome::Skipped));
        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.claim.unwrap().claim_token, live_token);

        // Once the lease really has expired, the attempt is adopted with a fresh token.
        let AdoptOutcome::Adopted(adopted) =
            adopt_external_attempt(&storage, job_id, node_id(4), 11_000)
                .await
                .unwrap()
        else {
            panic!("expired lease must be adopted");
        };
        let claim = adopted.claim.unwrap();
        assert_ne!(claim.claim_token, live_token);
        assert_eq!(claim.holder_node_id, node_id(4));
    }

    #[tokio::test]
    async fn claim_moves_index() {
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

    // Perf budget: a claim is start + read + batch-write + batch-delete + commit.
    #[tokio::test]
    async fn claim_transition_bounded() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([2u8; 16]);
        insert_job(&storage, &queued_record(job_id)).await.unwrap();

        let before = storage.snapshot_metrics().requests_total;
        claim_job(&storage, job_id, node_id(3), 5_000)
            .await
            .unwrap();
        assert_eq!(storage.snapshot_metrics().requests_total - before, 5);
    }

    // The framework is opt-in: a write to any other keyspace never touches a job one.
    #[tokio::test]
    async fn non_job_write_untouched() {
        let (_dir, storage) = temp_storage();
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: "unrelated".to_string(),
                key: ByteView::from(vec![1]),
                value: ByteView::from(vec![2]),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }
        for key_space in [
            JOB_KEYSPACE,
            JOB_SCHEDULE_INDEX_KEYSPACE,
            JOB_OWNER_INDEX_KEYSPACE,
            JOB_DEDUP_INDEX_KEYSPACE,
        ] {
            let (values, _) = iter_prefix_page(&storage, key_space, None, None, 8, None)
                .await
                .unwrap();
            assert!(values.is_empty(), "{key_space} must stay empty");
        }
    }

    #[tokio::test]
    async fn zombie_rejected() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([1u8; 16]);
        insert_job(&storage, &queued_record(job_id)).await.unwrap();
        claim_job(&storage, job_id, node_id(3), 5_000)
            .await
            .unwrap();

        let err = complete_job(
            &storage,
            job_id,
            Ulid::r#gen(),
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
    async fn requeue_backs_off() {
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
            None,
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

    // Retried payload work starts over, so persisted progress must start over too.
    #[tokio::test]
    async fn retry_resets_progress() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([12u8; 16]);
        let mut record = queued_record(job_id);
        record.progress.current = 1;
        record.progress.total = Some(1);
        insert_job(&storage, &record).await.unwrap();
        claim_job(&storage, job_id, node_id(3), 5_000)
            .await
            .unwrap();

        let RequeueOutcome::Requeued(record) =
            requeue_job(&storage, job_id, None, 6_000, None, None)
                .await
                .unwrap()
        else {
            panic!("expected requeue");
        };
        assert_eq!(
            record.progress,
            JobProgress::new(record.payload.progress_unit())
        );
    }

    #[tokio::test]
    async fn requeue_exhausts() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([6u8; 16]);
        let mut record = queued_record(job_id);
        record.attempts = JOB_MAX_ATTEMPTS - 1;
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: Ulid::r#gen(),
            lease_expires_at_ms: 5_000,
        });
        insert_job(&storage, &record).await.unwrap();

        let RequeueOutcome::Failed(failed) = requeue_job(&storage, job_id, None, 6_000, None, None)
            .await
            .unwrap()
        else {
            panic!("expected terminal failure");
        };
        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.attempts, JOB_MAX_ATTEMPTS);
    }

    // A hand-back is not a failure: attempts stay put and the job is due immediately.
    #[tokio::test]
    async fn release_keeps_attempts() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([0xA1; 16]);
        let token = Ulid::r#gen();
        let mut record = running_record(job_id, token, 60_000);
        record.attempts = 2;
        insert_job(&storage, &record).await.unwrap();

        let ReleaseOutcome::Released(released) =
            release_job(&storage, job_id, token, 6_000).await.unwrap()
        else {
            panic!("expected release");
        };
        assert_eq!(released.state, JobState::Queued);
        assert_eq!(released.attempts, 2);
        assert_eq!(released.due_at_ms, 6_000);
        assert!(released.claim.is_none());
    }

    #[tokio::test]
    async fn release_guards_token() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([0xB2; 16]);
        insert_job(&storage, &running_record(job_id, Ulid::r#gen(), 60_000))
            .await
            .unwrap();

        let error = release_job(&storage, job_id, Ulid::r#gen(), 6_000)
            .await
            .expect_err("a foreign token must not release the lease");
        assert!(matches!(error, JobMutationError::TokenMismatch));
        let record = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, JobState::Running);
        assert!(record.claim.is_some());
    }

    #[tokio::test]
    async fn cancel_before_claim() {
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
    async fn cancel_terminal_noop() {
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
    async fn terminal_clears_dedup() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([5u8; 16]);
        let mut record = queued_record(job_id);
        record.dedup_key = Some(b"dedup".to_vec());
        record.state = JobState::Running;
        let token = Ulid::r#gen();
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: token,
            lease_expires_at_ms: 5_000,
        });
        insert_job(&storage, &record).await.unwrap();
        assert_eq!(
            find_dedup_job(&storage, record.created_by, b"dedup", None)
                .await
                .unwrap(),
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
            find_dedup_job(&storage, record.created_by, b"dedup", None)
                .await
                .unwrap(),
            None
        );
        let keys = schedule_keys(&storage).await;
        assert_eq!(keys.len(), 1);
        assert!(keys[0].starts_with(aruna_core::structs::JOB_PRUNE_INDEX_PREFIX));
    }

    #[tokio::test]
    async fn malformed_deleted() {
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

    fn running_record(job_id: JobId, token: Ulid, lease_expires: u64) -> JobRecord {
        let mut record = queued_record(job_id);
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: token,
            lease_expires_at_ms: lease_expires,
        });
        record
    }

    #[tokio::test]
    async fn revived_lease_kept() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([2u8; 16]);
        insert_job(&storage, &running_record(job_id, Ulid::r#gen(), 10_000))
            .await
            .unwrap();

        // Sweep at now=5_000 with the lease valid until 10_000 must not revoke it.
        let outcome = requeue_job(
            &storage,
            job_id,
            None,
            5_000,
            Some(5_000),
            Some(JobError::retryable("lease expired")),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, RequeueOutcome::Skipped));
        let record = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, JobState::Running);
    }

    // A sweep racing a completed requeue (record already Queued, claim gone) must not
    // charge a second attempt or overwrite last_error.
    #[tokio::test]
    async fn sweep_skips_requeued() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([11u8; 16]);
        let mut record = queued_record(job_id);
        record.attempts = 1;
        insert_job(&storage, &record).await.unwrap();

        let outcome = requeue_job(
            &storage,
            job_id,
            None,
            6_000,
            Some(6_000),
            Some(JobError::retryable("lease expired")),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, RequeueOutcome::Skipped));
        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Queued);
        assert_eq!(stored.attempts, 1);
        assert!(stored.last_error.is_none());
    }

    #[tokio::test]
    async fn reclaim_not_eligible() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([2u8; 16]);
        insert_job(&storage, &queued_record(job_id)).await.unwrap();
        claim_job(&storage, job_id, node_id(3), 5_000)
            .await
            .unwrap();

        // A second claim of an already-Claimed job must not hand back a token.
        let outcome = claim_job(&storage, job_id, node_id(4), 6_000)
            .await
            .unwrap();
        assert!(matches!(outcome, ClaimOutcome::NotEligible));
    }

    // A state filter must scan past non-matching pages, not return an empty page + cursor.
    #[tokio::test]
    async fn filter_scans_pages() {
        let (_dir, storage) = temp_storage();
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let make = |seq: u64, state: JobState| {
            let mut record = JobRecord::new(
                JobId(Ulid::from_parts(seq, 0)),
                JobPayload::Probe {
                    steps: 1,
                    step_sleep_ms: 0,
                    fail_at: None,
                    panic_at: None,
                    cleanup_marker: None,
                },
                owner,
                node_id(7),
                seq * 1000,
                seq * 1000,
                None,
            );
            record.state = state;
            record
        };
        for record in [
            make(3, JobState::Queued),
            make(2, JobState::Queued),
            make(1, JobState::Failed),
        ] {
            insert_job(&storage, &record).await.unwrap();
        }

        let (page, cursor) = list_jobs_for_user(&storage, owner, None, 1, Some(JobState::Failed))
            .await
            .unwrap();
        assert_eq!(page.len(), 1, "the older Failed job is found across pages");
        assert_eq!(page[0].state, JobState::Failed);
        assert!(
            cursor.is_none(),
            "no dangling cursor on an exhausted filter"
        );
    }

    #[tokio::test]
    async fn dedup_delete_guarded() {
        let (_dir, storage) = temp_storage();
        let job_a = JobId::from_bytes([1u8; 16]);
        let job_b = JobId::from_bytes([2u8; 16]);
        let token = Ulid::r#gen();
        let mut record = running_record(job_a, token, 10_000);
        record.dedup_key = Some(b"k".to_vec());
        insert_job(&storage, &record).await.unwrap();

        // A raced submit repoints the dedup row at job B.
        batch_write(
            &storage,
            vec![(
                JOB_DEDUP_INDEX_KEYSPACE.to_string(),
                job_dedup_index_key(record.created_by, b"k"),
                ByteView::from(encode_job_dedup_value(job_b, [3u8; 32])),
            )],
            None,
        )
        .await
        .unwrap();

        complete_job(
            &storage,
            job_a,
            token,
            JobResultPayload::Probe { completed_steps: 1 },
            JobProgress::new("steps"),
            6_000,
        )
        .await
        .unwrap();

        // A going terminal must not delete B's dedup row.
        assert_eq!(
            find_dedup_job(&storage, record.created_by, b"k", None)
                .await
                .unwrap(),
            Some(job_b)
        );
    }

    // An external attempt with an expired lease is never requeued; it is reconciled.
    #[tokio::test]
    async fn external_never_requeued() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([0xE0; 16]);
        let mut record = running_record(job_id, Ulid::r#gen(), 1);
        record.execution_class = JobExecutionClass::ExternalAttempt;
        insert_job(&storage, &record).await.unwrap();

        let outcome = requeue_job(&storage, job_id, None, 9_000, Some(9_000), None)
            .await
            .unwrap();
        assert!(matches!(outcome, RequeueOutcome::NeedsReconcile(_)));
        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Running, "not requeued");
        assert_eq!(stored.attempts, 0, "attempt count untouched");
        assert!(stored.claim.is_some(), "claim untouched");
    }

    #[tokio::test]
    async fn attempt_intent_persists() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([0xA0; 16]);
        let token = Ulid::r#gen();
        insert_job(&storage, &running_record(job_id, token, 10_000))
            .await
            .unwrap();

        let intent = AttemptIntent {
            attempt_no: 1,
            external_name: aruna_core::structs::attempt_external_name(job_id, 1),
            executor_kind: "docker".to_string(),
        };
        record_attempt_intent(&storage, job_id, token, intent.clone(), 6_000)
            .await
            .unwrap();

        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.attempt_intent, Some(intent));
    }

    // A live renewed external lease is a plain sweep Skip, never routed to reconcile.
    #[tokio::test]
    async fn external_live_skipped() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([0xE2; 16]);
        let mut record = running_record(job_id, Ulid::r#gen(), 10_000);
        record.execution_class = JobExecutionClass::ExternalAttempt;
        insert_job(&storage, &record).await.unwrap();

        let outcome = requeue_job(&storage, job_id, None, 5_000, Some(5_000), None)
            .await
            .unwrap();
        assert!(matches!(outcome, RequeueOutcome::Skipped));
        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Running);
    }
}
