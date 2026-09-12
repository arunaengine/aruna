use super::*;

/// Read a job record, retaining malformed rows for offline recovery.
pub async fn read_job_record(
    storage: &StorageHandle,
    job_id: JobId,
    txn_id: Option<TxnId>,
) -> Result<Option<JobRecord>, String> {
    match read_raw(storage, JOB_KEYSPACE, job_record_key(job_id), txn_id).await? {
        Some(value) => match decode_job_record(&value) {
            Ok(record) => Ok(Some(record)),
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Malformed job record retained");
                Ok(None)
            }
        },
        None => Ok(None),
    }
}

pub(crate) async fn preserve_artifact_tombstone(
    storage: &StorageHandle,
    job_id: JobId,
    owner: UserId,
    expires_at_ms: u64,
) -> Result<(), String> {
    let tombstone = ArtifactTombstone {
        owner,
        expires_at_ms,
    };
    batch_write(
        storage,
        vec![
            (
                JOB_ARTIFACT_TOMBSTONE_KEYSPACE.to_string(),
                artifact_tombstone_key(job_id),
                ByteView::from(
                    postcard::to_allocvec(&tombstone).map_err(|error| error.to_string())?,
                ),
            ),
            (
                JOB_SCHEDULE_INDEX_KEYSPACE.to_string(),
                job_prune_key(expires_at_ms, job_id),
                empty_value(),
            ),
        ],
        None,
    )
    .await
}

pub(crate) fn artifact_tombstone_key(job_id: JobId) -> Key {
    ByteView::from(job_id.to_bytes().to_vec())
}

pub(crate) async fn read_artifact_tombstone(
    storage: &StorageHandle,
    job_id: JobId,
    now_ms: u64,
) -> Result<Option<UserId>, String> {
    read_raw(
        storage,
        JOB_ARTIFACT_TOMBSTONE_KEYSPACE,
        artifact_tombstone_key(job_id),
        None,
    )
    .await?
    .map(|value| {
        postcard::from_bytes::<ArtifactTombstone>(value.as_ref()).map_err(|error| error.to_string())
    })
    .transpose()
    .map(|tombstone| {
        tombstone
            .filter(|tombstone| tombstone.expires_at_ms > now_ms)
            .map(|tombstone| tombstone.owner)
    })
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
            claim_token: Ulid::generate(),
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

pub enum ExecutionCompleteOutcome {
    Completed(JobRecord),
    CancelRequested(JobRecord),
}

/// Complete an execution only when cancellation has not already committed.
pub async fn complete_execution(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
) -> Result<ExecutionCompleteOutcome, JobMutationError> {
    let mut completed = false;
    let record = commit_success(
        storage,
        job_id,
        token,
        result,
        final_progress,
        now_ms,
        |record| match record.cancel_requested {
            true => JobMutation::Skip,
            false => {
                completed = true;
                JobMutation::Persist
            }
        },
    )
    .await?;

    Ok(if completed {
        ExecutionCompleteOutcome::Completed(record)
    } else {
        ExecutionCompleteOutcome::CancelRequested(record)
    })
}

/// Complete an execution that finished successfully after cancellation was
/// requested. It carries the same durable-output invariant as an ordinary
/// success, because it publishes the same terminal result.
pub async fn complete_cancelled(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    commit_success(
        storage,
        job_id,
        token,
        result,
        final_progress,
        now_ms,
        |_| JobMutation::Persist,
    )
    .await
}

/// The storage-level success invariant: `Succeeded` commits in the transaction
/// that reads the attempt control, and only when every output binds the active
/// ExecutionId, names a reserved version, and the exact output record is durable.
pub(super) async fn commit_success<G>(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
    mut gate: G,
) -> Result<JobRecord, JobMutationError>
where
    G: FnMut(&JobRecord) -> JobMutation,
{
    let proof = result.clone();
    mutate_job_guarded(
        storage,
        job_id,
        |record| {
            guard_token(record, token)?;
            if matches!(gate(record), JobMutation::Skip) {
                return Ok(JobMutation::Skip);
            }
            record.state = JobState::Succeeded;
            record.finished_at_ms = Some(now_ms);
            record.updated_at_ms = now_ms;
            record.progress = final_progress.clone();
            record.result = Some(result.clone());
            record.claim = None;
            Ok(JobMutation::Persist)
        },
        |record, control| match record.state {
            JobState::Succeeded => {
                let control = control.ok_or(JobMutationError::MissingControl)?;
                Ok(proof.proves_outputs(control)?)
            }
            _ => Ok(()),
        },
    )
    .await
}

/// The signed output record the attempt already stored, if any. It shares the
/// attempt-control key, so pruning the job removes both.
pub async fn read_output_record(
    storage: &StorageHandle,
    job_id: JobId,
    attempt_epoch: u64,
) -> Result<Option<JobRecordEnvelope>, JobMutationError> {
    let value = read_raw(
        storage,
        JOB_OUTPUT_RECORD_KEYSPACE,
        ByteView::from(attempt_control_key(job_id, attempt_epoch)),
        None,
    )
    .await
    .map_err(JobMutationError::Storage)?;
    value
        .map(|bytes| {
            postcard::from_bytes(bytes.as_ref())
                .map_err(|error| JobMutationError::Storage(error.to_string()))
        })
        .transpose()
}

/// Make this execution's signed output record durable and name its digest on
/// the attempt control in one transaction, so terminal success can never
/// observe the digest without the record it names.
pub async fn persist_output_record(
    storage: &StorageHandle,
    job_id: JobId,
    control: &AttemptControl,
    digest: [u8; 32],
    envelope: Vec<u8>,
) -> Result<(), JobMutationError> {
    let write = vec![(
        JOB_OUTPUT_RECORD_KEYSPACE.to_string(),
        ByteView::from(attempt_control_key(job_id, control.attempt_epoch)),
        ByteView::from(envelope),
    )];
    mutate_control_with(storage, job_id, write, |_, stored| {
        if stored.execution_id != control.execution_id {
            return Err(JobMutationError::EpochMismatch);
        }
        if let Some(existing) = stored.output_record {
            return if existing == digest {
                Ok(JobMutation::Skip)
            } else {
                Err(JobMutationError::OutputRecordConflict)
            };
        }
        stored.output_record = Some(digest);
        Ok(JobMutation::Persist)
    })
    .await?;
    Ok(())
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
    /// `JOB_MAX_ATTEMPTS` spent: terminal `Failed` on a permanent verdict, otherwise
    /// `Indeterminate` and locally exhausted.
    Exhausted(JobRecord),
    /// A submitted external attempt lost its lease or the node restarted: requeuing
    /// would double-run the container, so the record is left for reconciliation.
    NeedsReconcile(JobRecord),
    Skipped,
}

/// `JOB_MAX_ATTEMPTS` are spent. Only a permanent verdict terminalizes; a
/// retryable or absent one leaves the outcome `Indeterminate`, since exhausting
/// this node's attempts proves nothing. An execution keeps a result payload.
pub(super) fn exhaust_attempts(record: &mut JobRecord, now_ms: u64) {
    let permanent = record
        .last_error
        .as_ref()
        .is_some_and(|error| error.kind == JobErrorKind::Permanent);
    record.claim = None;
    if permanent {
        record.state = JobState::Failed;
        record.finished_at_ms = Some(now_ms);
    } else {
        record.state = JobState::Indeterminate;
        record.locally_exhausted = true;
    }
    if record.result.is_some() || !matches!(record.payload, JobPayload::Execution(_)) {
        return;
    }
    let result = JobResultPayload::Execution {
        exit_code: None,
        workspace_bucket: record.workspace_bucket.clone(),
        outputs: Vec::new(),
        stdout: String::new(),
        stderr: String::new(),
        output_digest: None,
    };
    record.result = Some(result);
}

/// Re-queue with backoff, or fail once `JOB_MAX_ATTEMPTS` is spent; `token` is
/// `None` for the sweep and recovery, and `require_expired_before` re-checks the
/// expired lease in-txn. External attempts return `NeedsReconcile` untouched.
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
        if record.is_settled() {
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
        if record.execution_class == JobExecutionClass::ExternalAttempt
            && record.attempt_intent.is_some()
        {
            needs_reconcile = true;
            return Ok(JobMutation::Skip);
        }
        if let Some(error) = error.clone() {
            record.last_error = Some(error);
        }
        record.attempts = record.attempts.saturating_add(1);
        record.updated_at_ms = now_ms;
        record.claim = None;
        if record.attempts >= JOB_MAX_ATTEMPTS
            && !matches!(&record.payload, JobPayload::TerminalCleanup { .. })
        {
            exhaust_attempts(record, now_ms);
        } else {
            record.state = JobState::Queued;
            record.due_at_ms = now_ms.saturating_add(retry_delay_ms(record.attempts));
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
    } else if record.is_settled() {
        RequeueOutcome::Exhausted(record)
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
        record.progress = JobProgress::new(record.payload.progress_unit());
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

/// Requeue a dependency-blocked job without spending an attempt. Unlike a
/// shutdown release, this records the reason and delays the next claim so a
/// lagging projection is not hot-polled.
pub async fn defer_job(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    retry_after_ms: u64,
    error: JobError,
) -> Result<ReleaseOutcome, JobMutationError> {
    let mut deferred = false;
    let record = mutate_job(storage, job_id, |record| {
        deferred = false;
        guard_token(record, token)?;
        if record.state.is_terminal() {
            return Ok(JobMutation::Skip);
        }
        record.state = JobState::Queued;
        record.claim = None;
        record.due_at_ms = now_ms.saturating_add(retry_after_ms);
        record.updated_at_ms = now_ms;
        record.last_error = Some(error.clone());
        record.progress = JobProgress::new(record.payload.progress_unit());
        deferred = true;
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(if deferred {
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
    let result = mutate_attempt_control(storage, job_id, |record, control| {
        released = false;
        guard_token(record, token)?;
        if record.execution_class != JobExecutionClass::ExternalAttempt
            || record.state.is_terminal()
        {
            return Ok(JobMutation::Skip);
        }
        if let Some(claim) = record.claim.as_mut() {
            claim.claim_token = Ulid::generate();
            claim.lease_expires_at_ms = now_ms;
        }
        bump_generation(control)?;
        control.bound_token = None;
        record.updated_at_ms = now_ms;
        released = true;
        Ok(JobMutation::Persist)
    })
    .await;

    match result {
        Ok((record, _)) => Ok(if released {
            ReleaseOutcome::Released(record)
        } else {
            ReleaseOutcome::Skipped
        }),
        // A job interrupted while still staging has no attempt intent; release
        // its lease instead of leaving it to the sweep's attempt charge.
        Err(JobMutationError::MissingControl) => {
            let record = read_job_record(storage, job_id, None)
                .await
                .map_err(JobMutationError::Storage)?;
            if record.is_some_and(|record| record.attempt_intent.is_none()) {
                release_job(storage, job_id, token, now_ms).await
            } else {
                Err(JobMutationError::MissingControl)
            }
        }
        Err(error) => Err(error),
    }
}

/// Write-ahead attempt intent: record the deterministic external identity BEFORE any
/// external submit so a lost attempt can be adopted by name. A committed cancellation
/// wins this gate, so no new attempt is submitted afterward.
#[derive(Debug)]
pub struct AttemptCommit {
    pub record: JobRecord,
    pub control: AttemptControl,
}

/// `execution_id` binds the fenced attempt to the physical execution a target
/// already receipted; `None` mints a fresh one for a purely local attempt.
pub async fn record_attempt_intent(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    draft: AttemptIntent,
    execution_id: Option<Ulid>,
    now_ms: u64,
) -> Result<AttemptCommit, JobMutationError> {
    for attempt in 0..JOB_MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(storage)
            .await
            .map_err(JobMutationError::Storage)?;
        let result = async {
            let Some(mut record) = read_job_record(storage, job_id, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?
            else {
                return Err(JobMutationError::NotFound);
            };
            guard_token(&record, token)?;
            if let Some(intent) = &record.attempt_intent {
                if intent.attempt_no != draft.attempt_no
                    || intent.external_name != draft.external_name
                    || intent.executor_kind != draft.executor_kind
                    || intent.pinned_image != draft.pinned_image
                {
                    return Err(JobMutationError::IntentConflict);
                }
                let control =
                    read_attempt_control(storage, job_id, intent.attempt_epoch, Some(txn_id))
                        .await?
                        .ok_or(JobMutationError::MissingControl)?;
                return Ok(AttemptCommit { record, control });
            }
            if record.cancel_requested {
                return Err(JobMutationError::IntentConflict);
            }
            let epoch = record.next_attempt_epoch;
            record.next_attempt_epoch = epoch
                .checked_add(1)
                .ok_or(JobMutationError::EpochExhausted)?;
            let intent = AttemptIntent {
                attempt_epoch: epoch,
                ..draft.clone()
            };
            let control = AttemptControl {
                attempt_epoch: epoch,
                execution_id: execution_id.unwrap_or_else(Ulid::generate),
                controller_generation: 1,
                bound_token: Some(token),
                tombstone_ref: None,
                output_commits: Vec::new(),
                output_record: None,
            };
            let old = record.clone();
            record.attempt_intent = Some(intent);
            record.updated_at_ms = now_ms;
            let (mut writes, deletes) = index_deltas(&old, &record)
                .map_err(|error| JobMutationError::Storage(error.to_string()))?;
            writes.push((
                JOB_ATTEMPT_CONTROL_KEYSPACE.to_string(),
                ByteView::from(attempt_control_key(job_id, epoch)),
                ByteView::from(
                    control
                        .to_bytes()
                        .map_err(|error| JobMutationError::Storage(error.to_string()))?,
                ),
            ));
            batch_write(storage, writes, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            batch_delete(storage, deletes, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            Ok(AttemptCommit { record, control })
        }
        .await;
        match result {
            Ok(commit) => match commit_write(
                storage,
                txn_id,
                attempt,
                "attempt commit exhausted conflict retries",
            )
            .await?
            {
                CommitStep::Committed => return Ok(commit),
                CommitStep::Retry => continue,
            },
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(JobMutationError::Storage(
        "attempt commit exhausted conflict retries".to_string(),
    ))
}
