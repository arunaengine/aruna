use aruna_core::compute::{AttemptRef, FenceContext};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    JOB_ACTIVE_USER_KEYSPACE, JOB_ARTIFACT_TOMBSTONE_KEYSPACE, JOB_ATTEMPT_CONTROL_KEYSPACE,
    JOB_DEDUP_INDEX_KEYSPACE, JOB_ENTRY_KEYSPACE, JOB_KEYSPACE, JOB_OUTPUT_RECORD_KEYSPACE,
    JOB_OWNER_INDEX_KEYSPACE, JOB_RUN_CRATE_KEYSPACE, JOB_SCHEDULE_INDEX_KEYSPACE,
    ROCRATE_JOB_STATE_KEYSPACE, S3_PURGE_CHECKPOINT_KEYSPACE, STAGING_JOB_STATE_KEYSPACE,
};
use aruna_core::structs::{
    ActiveJobKind, AttemptControl, AttemptIntent, GLOBAL_DEDUP_PREFIX, JobClaim, JobError,
    JobErrorKind, JobExecutionClass, JobId, JobPayload, JobProgress, JobRecord, JobRecordEnvelope,
    JobRecordError, JobResultPayload, JobState, JobTransitionError, RunCrateStatus,
    StoragePurgeCheckpoint, UserAccess, attempt_control_key, cleanup_dedup_key, cleanup_job_id,
    crate_dedup_key, crate_job_id, due_index_key, encode_dedup_value, job_active_key,
    job_entry_key, job_entry_prefix, job_owner_cursor, job_prune_key, job_record_key,
    lease_index_key, owner_index_key, owner_index_prefix, parse_dedup_value, parse_entry_key,
    parse_owner_key, rocrate_plan_key, run_crate_key, validate_transition, workspace_credential_id,
};
use aruna_core::types::{Key, KeySpace, NodeId, TxnId, UserId, Value};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::lifecycle::ids::session_of;
use super::{JOB_LEASE_MS, JOB_MAX_ATTEMPTS, JOB_MUTATE_MAX_ATTEMPTS};
use crate::tasks::queue_backoff::retry_delay_ms;

mod attempt;
mod query;
mod state;

pub use attempt::*;
pub use query::*;
pub use state::*;

pub(super) type JobWrites = Vec<(KeySpace, Key, Value)>;
pub(super) type JobDeletes = Vec<(KeySpace, Key)>;

#[derive(Debug, Serialize, Deserialize)]
struct ArtifactTombstone {
    owner: UserId,
    expires_at_ms: u64,
}

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
    #[error("attempt intent conflicts with the committed lineage")]
    IntentConflict,
    #[error("attempt epoch exhausted")]
    EpochExhausted,
    #[error("controller generation exhausted")]
    GenerationExhausted,
    #[error("attempt control record is missing")]
    MissingControl,
    #[error("job report is frozen")]
    ReportFrozen,
    #[error("attempt control epoch mismatch")]
    EpochMismatch,
    #[error("execution output record digest conflicts with the stored digest")]
    OutputRecordConflict,
    #[error(transparent)]
    IllegalTransition(#[from] JobTransitionError),
    #[error("execution outputs are not proven durable: {0}")]
    OutputsUnproven(#[from] JobRecordError),
    #[error("{0}")]
    Storage(String),
}

/// Schedule-index key by state: queued -> due/, claimed/running -> lease/, settled -> prune/.
fn job_schedule_key(record: &JobRecord) -> Key {
    // A locally exhausted job earns no further sweep: it is scheduled for pruning.
    if record.locally_exhausted {
        let finished = record.finished_at_ms.unwrap_or(record.updated_at_ms);
        return job_prune_key(finished.saturating_add(record.retention_ms), record.job_id);
    }
    match record.state {
        JobState::Queued => due_index_key(record.due_at_ms, record.job_id),
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
            lease_index_key(lease, record.job_id)
        }
        JobState::Succeeded | JobState::Failed | JobState::Cancelled => {
            let finished = record.finished_at_ms.unwrap_or(record.updated_at_ms);
            job_prune_key(finished.saturating_add(record.retention_ms), record.job_id)
        }
    }
}

/// Dedup index path. A user-scoped key is prefixed with the submitting user so a
/// caller cannot squat another's idempotency key; a `global/` key stays
/// unprefixed, so different users' submissions resolve to one job identity.
pub(super) fn dedup_index_key(created_by: UserId, dedup_key: &[u8]) -> Key {
    if dedup_key.starts_with(GLOBAL_DEDUP_PREFIX) {
        return ByteView::from(dedup_key.to_vec());
    }
    let mut key = created_by.to_storage_key();
    key.extend_from_slice(dedup_key);
    ByteView::from(key)
}

fn empty_value() -> Value {
    ByteView::from(Vec::new())
}

/// Writes creating a fresh job (<=5 keys); composable into a producer transaction.
pub fn job_insert_entries(record: &JobRecord) -> Result<JobWrites, ConversionError> {
    let mut writes = vec![
        (
            JOB_KEYSPACE.to_string(),
            job_record_key(record.job_id),
            ByteView::from(record.to_bytes()?),
        ),
        (
            JOB_SCHEDULE_INDEX_KEYSPACE.to_string(),
            job_schedule_key(record),
            empty_value(),
        ),
    ];
    if !record.payload.is_internal() {
        writes.push((
            JOB_OWNER_INDEX_KEYSPACE.to_string(),
            owner_index_key(record.created_by, record.created_at_ms, record.job_id),
            empty_value(),
        ));
    }
    // The row is what a submission counts in its own transaction; a node that
    // configures no ceiling still pays one key per job for it.
    if let Some(kind) = ActiveJobKind::of(&record.payload)
        && !record.state.is_terminal()
    {
        writes.push((
            JOB_ACTIVE_USER_KEYSPACE.to_string(),
            job_active_key(record.created_by, kind, record.job_id),
            empty_value(),
        ));
    }
    if let Some(dedup_key) = &record.dedup_key {
        writes.push((
            JOB_DEDUP_INDEX_KEYSPACE.to_string(),
            dedup_index_key(record.created_by, dedup_key),
            ByteView::from(encode_dedup_value(
                record.job_id,
                record.plan_digest.unwrap_or_default(),
            )),
        ));
    }
    Ok(writes)
}

/// Deletes for a pruned terminal job.
pub fn prune_delete_entries(record: &JobRecord) -> JobDeletes {
    let mut deletes = vec![
        (JOB_KEYSPACE.to_string(), job_record_key(record.job_id)),
        (
            JOB_RUN_CRATE_KEYSPACE.to_string(),
            run_crate_key(record.job_id),
        ),
        (
            JOB_OWNER_INDEX_KEYSPACE.to_string(),
            owner_index_key(record.created_by, record.created_at_ms, record.job_id),
        ),
        (
            JOB_SCHEDULE_INDEX_KEYSPACE.to_string(),
            job_schedule_key(record),
        ),
        (
            STAGING_JOB_STATE_KEYSPACE.to_string(),
            ByteView::from(record.job_id.to_bytes().to_vec()),
        ),
        (
            ROCRATE_JOB_STATE_KEYSPACE.to_string(),
            ByteView::from(record.job_id.to_bytes().to_vec()),
        ),
        (
            ROCRATE_JOB_STATE_KEYSPACE.to_string(),
            rocrate_plan_key(record.job_id),
        ),
    ];
    if let Some(kind) = ActiveJobKind::of(&record.payload) {
        deletes.push((
            JOB_ACTIVE_USER_KEYSPACE.to_string(),
            job_active_key(record.created_by, kind, record.job_id),
        ));
    }
    if record.payload.dedup_until_prune()
        && let Some(dedup_key) = &record.dedup_key
    {
        deletes.push((
            JOB_DEDUP_INDEX_KEYSPACE.to_string(),
            dedup_index_key(record.created_by, dedup_key),
        ));
    }
    // Epochs are handed out from 1; every used epoch left a control row and, on
    // a terminal success, the output record stored under the same key.
    for epoch in 1..record.next_attempt_epoch {
        deletes.push((
            JOB_ATTEMPT_CONTROL_KEYSPACE.to_string(),
            ByteView::from(attempt_control_key(record.job_id, epoch)),
        ));
        deletes.push((
            JOB_OUTPUT_RECORD_KEYSPACE.to_string(),
            ByteView::from(attempt_control_key(record.job_id, epoch)),
        ));
    }
    deletes
}

/// Row plus index changes of one job-state transition. Every writer of a job
/// row goes through this, so the schedule and active-user indexes never drift.
pub(super) fn index_deltas(
    old: &JobRecord,
    new: &JobRecord,
) -> Result<(JobWrites, JobDeletes), ConversionError> {
    let mut writes = vec![(
        JOB_KEYSPACE.to_string(),
        job_record_key(new.job_id),
        ByteView::from(new.to_bytes()?),
    )];
    let mut deletes = Vec::new();

    let old_schedule = job_schedule_key(old);
    let new_schedule = job_schedule_key(new);
    if old_schedule != new_schedule {
        deletes.push((JOB_SCHEDULE_INDEX_KEYSPACE.to_string(), old_schedule));
    }
    writes.push((
        JOB_SCHEDULE_INDEX_KEYSPACE.to_string(),
        new_schedule,
        empty_value(),
    ));
    if let Some(kind) = ActiveJobKind::of(&old.payload)
        && !old.is_settled()
        && new.is_settled()
    {
        deletes.push((
            JOB_ACTIVE_USER_KEYSPACE.to_string(),
            job_active_key(new.created_by, kind, new.job_id),
        ));
    }

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
    mutate: F,
) -> Result<JobRecord, JobMutationError>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError>,
{
    let mut mutate = mutate;
    for attempt in 0..JOB_MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(storage)
            .await
            .map_err(JobMutationError::Storage)?;
        match Box::pin(mutate_in_txn(storage, txn_id, job_id, &mut mutate, None)).await {
            Ok(record) => match commit_write(
                storage,
                txn_id,
                attempt,
                "job mutation exhausted conflict retries",
            )
            .await?
            {
                CommitStep::Committed => return Ok(record),
                CommitStep::Retry => continue,
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

/// Re-checks a mutated record against the attempt control read in the same
/// transaction. Only supplied when an invariant needs it, so ordinary mutations
/// pay no extra read.
type JobGuard<'a> =
    &'a mut (dyn FnMut(&JobRecord, Option<&AttemptControl>) -> Result<(), JobMutationError> + Send);

/// Same transaction as [`mutate_job`], with `guard` re-checking the mutated
/// record against the attempt control read in that transaction; a rejection
/// aborts before any write, so the invariant is atomic with the state change.
pub async fn mutate_job_guarded<F, G>(
    storage: &StorageHandle,
    job_id: JobId,
    mut mutate: F,
    mut guard: G,
) -> Result<JobRecord, JobMutationError>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError>,
    G: FnMut(&JobRecord, Option<&AttemptControl>) -> Result<(), JobMutationError> + Send,
{
    for attempt in 0..JOB_MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(storage)
            .await
            .map_err(JobMutationError::Storage)?;
        match Box::pin(mutate_in_txn(
            storage,
            txn_id,
            job_id,
            &mut mutate,
            Some(&mut guard),
        ))
        .await
        {
            Ok(record) => match commit_write(
                storage,
                txn_id,
                attempt,
                "job mutation exhausted conflict retries",
            )
            .await?
            {
                CommitStep::Committed => return Ok(record),
                CommitStep::Retry => continue,
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

/// Read and postcard-decode a state row, keeping a missing row distinct from a malformed one.
pub(super) async fn read_state<T: DeserializeOwned>(
    storage: &StorageHandle,
    key_space: &str,
    key: Key,
    label: &str,
) -> Result<Option<T>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => postcard::from_bytes(value.as_ref())
            .map(Some)
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected {label} event: {other:?}")),
    }
}

/// Postcard-encode `row` and persist it through the token-guarded checkpoint write.
pub(super) async fn put_state<T: Serialize>(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    key_space: &str,
    key: Key,
    row: &T,
) -> Result<(), JobMutationError> {
    let value = postcard::to_allocvec(row)
        .map(ByteView::from)
        .map_err(|error| JobMutationError::Storage(error.to_string()))?;
    put_job_checkpoint(storage, job_id, token, key_space, key, value).await
}

pub async fn put_purge_checkpoint(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    checkpoint: &StoragePurgeCheckpoint,
) -> Result<(), JobMutationError> {
    let value = checkpoint
        .to_bytes()
        .map(ByteView::from)
        .map_err(|error| JobMutationError::Storage(error.to_string()))?;
    put_job_checkpoint(
        storage,
        job_id,
        token,
        S3_PURGE_CHECKPOINT_KEYSPACE,
        ByteView::from(job_id.to_bytes().to_vec()),
        value,
    )
    .await
}

pub async fn read_purge_checkpoint(
    storage: &StorageHandle,
    job_id: JobId,
) -> Result<Option<StoragePurgeCheckpoint>, JobMutationError> {
    read_raw(
        storage,
        S3_PURGE_CHECKPOINT_KEYSPACE,
        ByteView::from(job_id.to_bytes().to_vec()),
        None,
    )
    .await
    .map_err(JobMutationError::Storage)?
    .map(|value| {
        StoragePurgeCheckpoint::from_bytes(value.as_ref())
            .map_err(|error| JobMutationError::Storage(error.to_string()))
    })
    .transpose()
}

async fn put_job_checkpoint(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    key_space: &str,
    key: Key,
    value: Value,
) -> Result<(), JobMutationError> {
    for attempt in 0..JOB_MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(storage)
            .await
            .map_err(JobMutationError::Storage)?;
        let result = async {
            let record = read_job_record(storage, job_id, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?
                .ok_or(JobMutationError::NotFound)?;
            guard_token(&record, token)?;
            batch_write(
                storage,
                vec![(key_space.to_string(), key.clone(), value.clone())],
                Some(txn_id),
            )
            .await
            .map_err(JobMutationError::Storage)
        }
        .await;
        match result {
            Ok(()) => match commit_write(
                storage,
                txn_id,
                attempt,
                "checkpoint write exhausted conflict retries",
            )
            .await?
            {
                CommitStep::Committed => return Ok(()),
                CommitStep::Retry => continue,
            },
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(JobMutationError::Storage(
        "checkpoint write exhausted conflict retries".to_string(),
    ))
}

pub async fn put_job_entry<T: Serialize>(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    entry_key: &[u8],
    row: &T,
) -> Result<(), JobMutationError> {
    let value = postcard::to_allocvec(row)
        .map(ByteView::from)
        .map_err(|error| JobMutationError::Storage(error.to_string()))?;
    for attempt in 0..JOB_MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(storage)
            .await
            .map_err(JobMutationError::Storage)?;
        let result = async {
            let record = read_job_record(storage, job_id, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?
                .ok_or(JobMutationError::NotFound)?;
            if record.is_settled() {
                return Err(JobMutationError::ReportFrozen);
            }
            guard_token(&record, token)?;
            batch_write(
                storage,
                vec![(
                    JOB_ENTRY_KEYSPACE.to_string(),
                    job_entry_key(job_id, entry_key),
                    value.clone(),
                )],
                Some(txn_id),
            )
            .await
            .map_err(JobMutationError::Storage)
        }
        .await;
        match result {
            Ok(()) => match commit_write(
                storage,
                txn_id,
                attempt,
                "job entry write exhausted conflict retries",
            )
            .await?
            {
                CommitStep::Committed => return Ok(()),
                CommitStep::Retry => continue,
            },
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(JobMutationError::Storage(
        "job entry write exhausted conflict retries".to_string(),
    ))
}

async fn mutate_in_txn<F>(
    storage: &StorageHandle,
    txn_id: TxnId,
    job_id: JobId,
    mutate: &mut F,
    guard: Option<JobGuard<'_>>,
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
            if let Some(guard) = guard {
                let control = match record.attempt_intent.as_ref() {
                    Some(intent) => {
                        read_attempt_control(storage, job_id, intent.attempt_epoch, Some(txn_id))
                            .await?
                    }
                    None => None,
                };
                guard(&record, control.as_ref())?;
            }
            if old.state != record.state {
                validate_transition(old.execution_class, old.state, record.state)?;
            }
            if !old.is_settled()
                && record.is_settled()
                && (record.payload.is_rocrate()
                    || matches!(&record.payload, JobPayload::Execution(spec) if session_of(spec).is_some()))
            {
                let digest = report_digest(storage, txn_id, record.job_id).await?;
                record.report_digest = Some(digest);
                match record.result.as_mut() {
                    Some(JobResultPayload::ImportRoCrate(result)) => {
                        result.report_digest = digest;
                    }
                    Some(JobResultPayload::ExportRoCrate(result)) => {
                        result.report_digest = digest;
                        if let Some(artifact) = result.artifact.as_mut() {
                            artifact.expires_at_ms = record
                                .finished_at_ms
                                .unwrap_or(record.updated_at_ms)
                                .saturating_add(record.retention_ms);
                        }
                    }
                    _ => {}
                }
            }
            let settled = !old.is_settled() && record.is_settled();
            let mut terminal_deletes = Vec::new();
            if settled && let JobPayload::StoragePurge(spec) = &record.payload {
                if let Some(delete) = crate::s3::purge_fence::owned_terminal_fence_delete(
                    storage,
                    txn_id,
                    record.job_id,
                    &spec.scope,
                )
                .await
                .map_err(|error| JobMutationError::Storage(error.to_string()))?
                {
                    terminal_deletes.push(delete);
                }
                terminal_deletes.push((
                    S3_PURGE_CHECKPOINT_KEYSPACE.to_string(),
                    ByteView::from(record.job_id.to_bytes().to_vec()),
                ));
            }
            let (writes, mut deletes) = index_deltas(&old, &record)
                .map_err(|error| JobMutationError::Storage(error.to_string()))?;
            deletes.extend(terminal_deletes);
            batch_write(storage, writes, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            batch_delete(storage, deletes, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            if settled {
                insert_crate_obligation(storage, txn_id, &record).await?;
                insert_cleanup_obligation(storage, txn_id, &record).await?;
                mark_crate_failed(storage, txn_id, &record).await?;
            }
            cleanup_dedup_entry(storage, txn_id, &old, &record).await?;
            Ok(record)
        }
    }
}

async fn report_digest(
    storage: &StorageHandle,
    txn_id: TxnId,
    job_id: JobId,
) -> Result<[u8; 32], JobMutationError> {
    let prefix = job_entry_prefix(job_id);
    let mut start_after = None;
    let mut hasher = blake3::Hasher::new();
    loop {
        let (values, next) = iter_prefix_page(
            storage,
            JOB_ENTRY_KEYSPACE,
            Some(prefix.clone()),
            start_after,
            512,
            Some(txn_id),
        )
        .await
        .map_err(JobMutationError::Storage)?;
        for (_, value) in values {
            hash_report_value(&mut hasher, value.as_ref());
        }
        let Some(next) = next else {
            return Ok(*hasher.finalize().as_bytes());
        };
        start_after = Some(next);
    }
}

fn hash_report_value(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

async fn insert_cleanup_obligation(
    storage: &StorageHandle,
    txn_id: TxnId,
    record: &JobRecord,
) -> Result<(), JobMutationError> {
    if !matches!(&record.payload, JobPayload::Execution(_)) {
        return Ok(());
    }
    let access_key = UserAccess::build_access_key(&workspace_credential_id(record.job_id))
        .map_err(|error| JobMutationError::Storage(error.to_string()))?;
    let now_ms = record.finished_at_ms.unwrap_or(record.updated_at_ms);
    let mut child = JobRecord::new(
        cleanup_job_id(record.job_id),
        JobPayload::TerminalCleanup {
            for_job: record.job_id,
            attempt: record.attempt_intent.clone(),
            access_key,
        },
        record.created_by,
        record.owner_node_id,
        now_ms,
        now_ms,
        Some(cleanup_dedup_key(record.job_id)),
    );
    child.retention_ms = record.retention_ms;
    let writes =
        job_insert_entries(&child).map_err(|error| JobMutationError::Storage(error.to_string()))?;
    batch_write(storage, writes, Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)
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
    let mut child = JobRecord::new(
        crate_job_id(record.job_id),
        JobPayload::WriteRunCrate {
            for_job: record.job_id,
        },
        record.created_by,
        record.owner_node_id,
        now_ms,
        now_ms,
        Some(crate_dedup_key(record.job_id)),
    );
    child.retention_ms = record.retention_ms;
    let mut writes =
        job_insert_entries(&child).map_err(|error| JobMutationError::Storage(error.to_string()))?;
    writes.push((
        JOB_RUN_CRATE_KEYSPACE.to_string(),
        run_crate_key(record.job_id),
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
    let key = run_crate_key(*for_job);
    if let Some(value) = read_raw(storage, JOB_RUN_CRATE_KEYSPACE, key.clone(), Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)?
        && !matches!(
            RunCrateStatus::from_bytes(value.as_ref())
                .map_err(|error| JobMutationError::Storage(error.to_string()))?,
            RunCrateStatus::Pending | RunCrateStatus::Minted { .. }
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

/// Remove a non-RO-Crate dedup row only when it still references THIS job.
/// RO-Crate dedup rows persist until their jobs are pruned.
async fn cleanup_dedup_entry(
    storage: &StorageHandle,
    txn_id: TxnId,
    old: &JobRecord,
    new: &JobRecord,
) -> Result<(), JobMutationError> {
    let Some(dedup_key) = &old.dedup_key else {
        return Ok(());
    };
    if old.payload.dedup_until_prune() || old.is_settled() || !new.is_settled() {
        return Ok(());
    }
    let key = dedup_index_key(old.created_by, dedup_key);
    let current = read_raw(storage, JOB_DEDUP_INDEX_KEYSPACE, key.clone(), Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)?;
    let still_ours = current
        .as_deref()
        .and_then(|bytes| parse_dedup_value(bytes).ok())
        .is_some_and(|(job_id, _)| job_id == old.job_id);
    if still_ours {
        delete_raw(storage, JOB_DEDUP_INDEX_KEYSPACE, key, Some(txn_id))
            .await
            .map_err(JobMutationError::Storage)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
