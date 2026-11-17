//! Builds the rows and index entries of one stored job record, shared by every writer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::keyspaces::{
    ACTIVE_USER_KEYSPACE, ATTEMPT_CONTROL_KEYSPACE, DEDUP_INDEX_KEYSPACE, JOB_CLAIM_KEYSPACE,
    JOB_INDEX_KEYSPACE, JOB_KEYSPACE, JOB_STATE_KEYSPACE, KEY_WAIT_KEYSPACE,
    LAUNCH_RECORD_KEYSPACE, OUTPUT_RECORD_KEYSPACE, RUN_CRATE_KEYSPACE, SCHEDULE_INDEX_KEYSPACE,
    STAGING_STATE_KEYSPACE,
};
use crate::types::{KeySpace, Value};

pub type JobWrites = Vec<(KeySpace, Key, Value)>;
pub type JobDeletes = Vec<(KeySpace, Key)>;

/// The claim row a page import guards on, so lease renewals and progress flushes,
/// which rewrite only the job record, never conflict with a page commit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClaimGuard {
    pub claim_token: Ulid,
    pub created_at_ms: u64,
}

impl ClaimGuard {
    pub fn of(record: &JobRecord) -> Option<Self> {
        let claim = record.claim.as_ref().filter(|_| !record.is_settled())?;
        Some(Self {
            claim_token: claim.claim_token,
            created_at_ms: record.created_at_ms,
        })
    }

    fn write(record: &JobRecord) -> Result<Option<(KeySpace, Key, Value)>, ConversionError> {
        let Some(guard) = Self::of(record) else {
            return Ok(None);
        };
        let value = ByteView::from(guard.to_bytes()?);
        Ok(Some((
            JOB_CLAIM_KEYSPACE.to_string(),
            job_record_key(record.job_id),
            value,
        )))
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Schedule-index key by state: queued -> due/, claimed/running -> lease/, settled -> prune/.
/// A job awaiting a key has none: only an unlock wakes it.
pub fn job_schedule_key(record: &JobRecord) -> Option<Key> {
    // A locally exhausted job earns no further sweep: it is scheduled for pruning.
    if record.locally_exhausted {
        let finished = record.finished_at_ms.unwrap_or(record.updated_at_ms);
        return Some(job_prune_key(
            finished.saturating_add(record.retention_ms),
            record.job_id,
        ));
    }
    Some(match record.state {
        JobState::AwaitingKey => return None,
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
                .map(|claim| claim.lease_expires_ms)
                .unwrap_or(record.due_at_ms);
            lease_index_key(lease, record.job_id)
        }
        JobState::Succeeded | JobState::Failed | JobState::Cancelled => {
            let finished = record.finished_at_ms.unwrap_or(record.updated_at_ms);
            job_prune_key(finished.saturating_add(record.retention_ms), record.job_id)
        }
    })
}

/// Dedup index path. A user-scoped key is prefixed with the submitting user so a
/// caller cannot squat another's idempotency key; a `global/` key stays
/// unprefixed, so different users' submissions resolve to one job identity.
pub fn dedup_index_key(created_by: UserId, dedup_key: &[u8]) -> Key {
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
    let mut writes = vec![(
        JOB_KEYSPACE.to_string(),
        job_record_key(record.job_id),
        ByteView::from(record.to_bytes()?),
    )];
    writes.extend(ClaimGuard::write(record)?);
    if let Some(schedule) = job_schedule_key(record) {
        writes.push((SCHEDULE_INDEX_KEYSPACE.to_string(), schedule, empty_value()));
    }
    if !record.payload.is_internal() {
        writes.push((
            JOB_INDEX_KEYSPACE.to_string(),
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
            ACTIVE_USER_KEYSPACE.to_string(),
            job_active_key(record.created_by, kind, record.job_id),
            empty_value(),
        ));
    }
    if let Some(dedup_key) = &record.dedup_key {
        writes.push((
            DEDUP_INDEX_KEYSPACE.to_string(),
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
            JOB_CLAIM_KEYSPACE.to_string(),
            job_record_key(record.job_id),
        ),
        (RUN_CRATE_KEYSPACE.to_string(), run_crate_key(record.job_id)),
        (
            JOB_INDEX_KEYSPACE.to_string(),
            owner_index_key(record.created_by, record.created_at_ms, record.job_id),
        ),
        (
            STAGING_STATE_KEYSPACE.to_string(),
            ByteView::from(record.job_id.to_bytes().to_vec()),
        ),
        (
            JOB_STATE_KEYSPACE.to_string(),
            ByteView::from(record.job_id.to_bytes().to_vec()),
        ),
        (
            JOB_STATE_KEYSPACE.to_string(),
            rocrate_plan_key(record.job_id),
        ),
    ];
    if let Some(schedule) = job_schedule_key(record) {
        deletes.push((SCHEDULE_INDEX_KEYSPACE.to_string(), schedule));
    }
    if let Some(kind) = ActiveJobKind::of(&record.payload) {
        deletes.push((
            ACTIVE_USER_KEYSPACE.to_string(),
            job_active_key(record.created_by, kind, record.job_id),
        ));
    }
    if record.payload.dedup_until_prune()
        && let Some(dedup_key) = &record.dedup_key
    {
        deletes.push((
            DEDUP_INDEX_KEYSPACE.to_string(),
            dedup_index_key(record.created_by, dedup_key),
        ));
    }
    // Epochs are handed out from 1; every used epoch left a control row and, on
    // a terminal success, the output record stored under the same key.
    for epoch in 1..record.next_attempt_epoch {
        deletes.push((
            ATTEMPT_CONTROL_KEYSPACE.to_string(),
            ByteView::from(attempt_control_key(record.job_id, epoch)),
        ));
        deletes.push((
            OUTPUT_RECORD_KEYSPACE.to_string(),
            ByteView::from(attempt_control_key(record.job_id, epoch)),
        ));
        deletes.push((
            LAUNCH_RECORD_KEYSPACE.to_string(),
            ByteView::from(attempt_control_key(record.job_id, epoch)),
        ));
    }
    deletes
}

/// Row plus index changes of one job-state transition. Every writer of a job
/// row goes through this, so the schedule and active-user indexes never drift.
pub fn index_deltas(
    old: &JobRecord,
    new: &JobRecord,
) -> Result<(JobWrites, JobDeletes), ConversionError> {
    let mut writes = vec![(
        JOB_KEYSPACE.to_string(),
        job_record_key(new.job_id),
        ByteView::from(new.to_bytes()?),
    )];
    let mut deletes = Vec::new();
    if ClaimGuard::of(old) != ClaimGuard::of(new) {
        match ClaimGuard::write(new)? {
            Some(write) => writes.push(write),
            None => deletes.push((JOB_CLAIM_KEYSPACE.to_string(), job_record_key(new.job_id))),
        }
    }

    let old_schedule = job_schedule_key(old);
    let new_schedule = job_schedule_key(new);
    if let Some(old_schedule) = old_schedule
        && Some(&old_schedule) != new_schedule.as_ref()
    {
        deletes.push((SCHEDULE_INDEX_KEYSPACE.to_string(), old_schedule));
    }
    if let Some(new_schedule) = new_schedule {
        writes.push((
            SCHEDULE_INDEX_KEYSPACE.to_string(),
            new_schedule,
            empty_value(),
        ));
    }
    // Leaving the parked state drops its wait list; stale wake rows are skipped on wake.
    if old.state == JobState::AwaitingKey && new.state != JobState::AwaitingKey {
        deletes.push((KEY_WAIT_KEYSPACE.to_string(), job_wait_key(new.job_id)));
    }
    if let Some(kind) = ActiveJobKind::of(&old.payload)
        && !old.is_settled()
        && new.is_settled()
    {
        deletes.push((
            ACTIVE_USER_KEYSPACE.to_string(),
            job_active_key(new.created_by, kind, new.job_id),
        ));
    }

    // Dedup removal is guarded by job id in cleanup_dedup_entry, not here.
    Ok((writes, deletes))
}
