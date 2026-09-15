use super::*;

pub async fn read_attempt_control(
    storage: &StorageHandle,
    job_id: JobId,
    attempt_epoch: u64,
    txn_id: Option<TxnId>,
) -> Result<Option<AttemptControl>, JobMutationError> {
    let value = read_raw(
        storage,
        JOB_ATTEMPT_CONTROL_KEYSPACE,
        ByteView::from(attempt_control_key(job_id, attempt_epoch)),
        txn_id,
    )
    .await
    .map_err(JobMutationError::Storage)?;
    value
        .map(|bytes| {
            AttemptControl::from_bytes(bytes.as_ref())
                .map_err(|error| JobMutationError::Storage(error.to_string()))
        })
        .transpose()
}

/// Write-ahead one VersionId per output destination in the attempt-control
/// transaction that already fences this physical execution. It commits before
/// any output write, so a replayed capture reuses the reserved identities.
pub async fn reserve_output_commits(
    storage: &StorageHandle,
    job_id: JobId,
    destinations: &[(NodeId, String, String)],
) -> Result<AttemptControl, JobMutationError> {
    let (_, control) = mutate_attempt_control(storage, job_id, |_, control| {
        match control.reserve_outputs(destinations, Ulid::generate) {
            true => Ok(JobMutation::Persist),
            false => Ok(JobMutation::Skip),
        }
    })
    .await?;
    Ok(control)
}

pub(super) async fn mutate_attempt_control<F>(
    storage: &StorageHandle,
    job_id: JobId,
    mutate: F,
) -> Result<(JobRecord, AttemptControl), JobMutationError>
where
    F: FnMut(&mut JobRecord, &mut AttemptControl) -> Result<JobMutation, JobMutationError>,
{
    mutate_control_with(storage, job_id, Vec::new(), mutate).await
}

/// Same transaction as [`mutate_attempt_control`], plus writes that must land
/// atomically with the control row.
pub(super) async fn mutate_control_with<F>(
    storage: &StorageHandle,
    job_id: JobId,
    extra: JobWrites,
    mut mutate: F,
) -> Result<(JobRecord, AttemptControl), JobMutationError>
where
    F: FnMut(&mut JobRecord, &mut AttemptControl) -> Result<JobMutation, JobMutationError>,
{
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
            let epoch = record
                .attempt_intent
                .as_ref()
                .map(|intent| intent.attempt_epoch)
                .ok_or(JobMutationError::MissingControl)?;
            let mut control = read_attempt_control(storage, job_id, epoch, Some(txn_id))
                .await?
                .ok_or(JobMutationError::MissingControl)?;
            if control.attempt_epoch != epoch {
                return Err(JobMutationError::EpochMismatch);
            }
            let old = record.clone();
            if matches!(mutate(&mut record, &mut control)?, JobMutation::Skip) {
                return Ok((old, control));
            }
            if old.state != record.state {
                validate_transition(old.execution_class, old.state, record.state)?;
            }
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
            writes.extend(extra.iter().cloned());
            batch_write(storage, writes, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            batch_delete(storage, deletes, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            Ok((record, control))
        }
        .await;
        match result {
            Ok(value) => match commit_write(
                storage,
                txn_id,
                attempt,
                "attempt mutation exhausted conflict retries",
            )
            .await?
            {
                CommitStep::Committed => return Ok(value),
                CommitStep::Retry => continue,
            },
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(JobMutationError::Storage(
        "attempt mutation exhausted conflict retries".to_string(),
    ))
}

pub(super) fn bump_generation(control: &mut AttemptControl) -> Result<(), JobMutationError> {
    control.controller_generation = control
        .controller_generation
        .checked_add(1)
        .ok_or(JobMutationError::GenerationExhausted)?;
    Ok(())
}

pub async fn authorize_cleanup(
    storage: &StorageHandle,
    job_id: JobId,
    intent: &AttemptIntent,
    token: Ulid,
) -> Result<FenceContext, JobMutationError> {
    let (_, control) = mutate_attempt_control(storage, job_id, |record, control| {
        if record
            .attempt_intent
            .as_ref()
            .is_none_or(|stored| stored.attempt_epoch != intent.attempt_epoch)
        {
            return Err(JobMutationError::EpochMismatch);
        }
        bump_generation(control)?;
        control.bound_token = Some(token);
        Ok(JobMutation::Persist)
    })
    .await?;
    Ok(FenceContext {
        attempt: AttemptRef::new(job_id.to_string().to_lowercase(), intent.attempt_no),
        attempt_epoch: intent.attempt_epoch,
        controller_generation: control.controller_generation,
    })
}

pub async fn record_attempt_tombstone(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    attempt_epoch: u64,
    tombstone_ref: String,
) -> Result<AttemptControl, JobMutationError> {
    let (_, control) = mutate_attempt_control(storage, job_id, |_, control| {
        if control.attempt_epoch != attempt_epoch {
            return Err(JobMutationError::EpochMismatch);
        }
        if control.bound_token != Some(token) {
            return Err(JobMutationError::TokenMismatch);
        }
        control.tombstone_ref = Some(tombstone_ref.clone());
        Ok(JobMutation::Persist)
    })
    .await?;
    Ok(control)
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

/// `Preparing -> Ready`: inputs staged and credentials minted.
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
pub async fn begin_external_running(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    started_at_ms: Option<u64>,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    mutate_job(storage, job_id, |record| {
        guard_token(record, token)?;
        if record.cancel_requested {
            return Ok(JobMutation::Skip);
        }
        // Running must always carry a start: it anchors the walltime cap, and a
        // backend that reports none still started when the controller saw it.
        record
            .started_at_ms
            .get_or_insert(started_at_ms.unwrap_or(now_ms));
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

#[derive(Debug)]
pub enum ParkOutcome {
    Parked(JobRecord),
    /// The park spent the last attempt, so no further attempt runs on this node.
    Exhausted(JobRecord),
}

/// Park an ambiguous external attempt in `Indeterminate`, keeping the claim so the
/// lease sweep later re-routes it to reconciliation. Exits only on evidence or the
/// attempt cap; adoption that resumes supervision is free, a failure terminalizes.
pub async fn mark_indeterminate(
    storage: &StorageHandle,
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> Result<ParkOutcome, JobMutationError> {
    let mut capped = false;
    let record = mutate_job(storage, job_id, |record| {
        capped = false;
        guard_token(record, token)?;
        record.last_error = Some(error.clone());
        record.attempts = record.attempts.saturating_add(1);
        record.updated_at_ms = now_ms;
        if record.attempts >= JOB_MAX_ATTEMPTS {
            exhaust_attempts(record, now_ms);
            capped = true;
        } else {
            record.state = JobState::Indeterminate;
        }
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(if capped {
        ParkOutcome::Exhausted(record)
    } else {
        ParkOutcome::Parked(record)
    })
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
    Adopted(JobRecord, AttemptControl),
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
    let (record, control) = mutate_attempt_control(storage, job_id, |record, control| {
        adopted = false;
        if record.state.is_terminal() {
            return Ok(JobMutation::Skip);
        }
        // Re-check the lease inside the transaction: a holder that renewed since the
        // sweep's read is alive, and stealing its claim would double-run the container.
        if let Some(claim) = &record.claim
            && claim.lease_expires_at_ms > now_ms
        {
            return Ok(JobMutation::Skip);
        }
        record.updated_at_ms = now_ms;
        let claim_token = Ulid::generate();
        record.claim = Some(JobClaim {
            holder_node_id,
            claim_token,
            lease_expires_at_ms: now_ms.saturating_add(JOB_LEASE_MS),
        });
        bump_generation(control)?;
        control.bound_token = Some(claim_token);
        adopted = true;
        Ok(JobMutation::Persist)
    })
    .await?;

    Ok(if adopted {
        AdoptOutcome::Adopted(record, control)
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
            exhaust_attempts(record, now_ms);
        } else {
            record.state = JobState::Queued;
            record.due_at_ms = now_ms.saturating_add(retry_delay_ms(record.attempts));
        }
        Ok(JobMutation::Persist)
    })
    .await
}

/// Persist the run-crate obligation status in its side keyspace, leaving the
/// terminal parent record untouched.
pub async fn put_crate_status(
    storage: &StorageHandle,
    job_id: JobId,
    status: &aruna_core::structs::RunCrateStatus,
) -> Result<(), String> {
    let bytes = status.to_bytes().map_err(|error| error.to_string())?;
    batch_write(
        storage,
        vec![(
            JOB_RUN_CRATE_KEYSPACE.to_string(),
            aruna_core::structs::run_crate_key(job_id),
            ByteView::from(bytes),
        )],
        None,
    )
    .await
}

/// Read the run-crate obligation status for `job_id`, if recorded.
pub async fn read_crate_status(
    storage: &StorageHandle,
    job_id: JobId,
) -> Result<Option<aruna_core::structs::RunCrateStatus>, String> {
    match read_raw(
        storage,
        JOB_RUN_CRATE_KEYSPACE,
        aruna_core::structs::run_crate_key(job_id),
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
        if record.is_settled() {
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
    } else if record.is_settled() {
        CancelRequestOutcome::AlreadyTerminal(record)
    } else {
        CancelRequestOutcome::Flagged(record)
    })
}
