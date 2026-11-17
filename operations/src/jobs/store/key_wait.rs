//! Parks jobs that need locked bucket keys and wakes them after unlock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{KeyWait, key_wait_key, parse_wait_key};
use aruna_core::structs::storage::encryption::BucketKeyRef;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};

use super::mutation::{MutateJobCalls, NoGuard, forward};
use super::settle_attempt;
use super::*;
use crate::driver::drive;

/// Wake rows handled per transaction page.
pub const WAKE_PAGE: usize = 64;

#[derive(Debug, PartialEq)]
pub enum AwaitOutcome {
    Parked(JobRecord),
    /// The job is settled or cancel-requested; the caller follows the normal path.
    Skipped,
}

/// Parks a claimed job until every key in `waits` is unlocked; see [`ParkJobOperation`].
pub async fn park_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    waits: Vec<KeyWait>,
) -> Result<AwaitOutcome, JobMutationError> {
    if waits.is_empty() {
        return Err(JobMutationError::Storage("park without key waits".into()));
    }
    let mut mutate = park_mutation(token, now_ms);
    let mut attempt = 0;
    loop {
        let result = drive(
            ParkJobOperation::new(job_id, &mut mutate, waits.clone()),
            context,
        )
        .await;
        match settle_attempt(result, attempt, "job park exhausted retries").await {
            Some(result) => return result,
            None => attempt += 1,
        }
    }
}

/// Parks the claimed job unless it settled or a cancel was requested meanwhile.
fn park_mutation(
    token: Ulid,
    now_ms: u64,
) -> impl FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Send {
    move |record: &mut JobRecord| {
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
        Ok(JobMutation::Persist)
    }
}

/// Parks a claimed job until every key in `waits` is unlocked. The claim and schedule entry go;
/// checkpoints, captured inputs, attempts and the due time stay. No retry deadline is set. The
/// wait rows are written in the same transaction as the parked job.
pub(crate) struct ParkJobOperation<F> {
    calls: MutateJobCalls<F, NoGuard>,
    waits: Vec<KeyWait>,
    parked: Option<JobRecord>,
}

impl<F> std::fmt::Debug for ParkJobOperation<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParkJobOperation")
            .field("calls", &self.calls)
            .finish_non_exhaustive()
    }
}

impl<F> PartialEq for ParkJobOperation<F> {
    fn eq(&self, other: &Self) -> bool {
        self.calls == other.calls && self.waits == other.waits && self.parked == other.parked
    }
}

impl<F> ParkJobOperation<F>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Send,
{
    pub(crate) fn new(job_id: JobId, mutate: F, waits: Vec<KeyWait>) -> Self {
        Self {
            calls: MutateJobCalls::new(job_id, mutate, None),
            waits,
            parked: None,
        }
    }

    fn wait_rows(&self, job_id: JobId) -> Result<JobWrites, JobMutationError> {
        let list = postcard::to_allocvec(&self.waits)
            .map_err(|error| JobMutationError::Storage(error.to_string()))?;
        let mut writes = vec![(
            KEY_WAIT_KEYSPACE.to_string(),
            job_wait_key(job_id),
            ByteView::from(list),
        )];
        writes.extend(self.waits.iter().map(|wait| {
            (
                KEY_WAIT_KEYSPACE.to_string(),
                key_wait_key(wait.key, job_id),
                Value::from(Vec::<u8>::new()),
            )
        }));
        Ok(writes)
    }
}

impl<F> Operation for ParkJobOperation<F>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Send,
{
    type Output = AwaitOutcome;
    type Error = JobMutationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<AwaitOutcome>, JobMutationError> {
        forward(self.calls.first()?)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AwaitOutcome>, JobMutationError> {
        if let Some(record) = self.parked.take() {
            done(result)?;
            return Ok(Next::Done(AwaitOutcome::Parked(record)));
        }
        let record = match self.calls.next(result)? {
            Next::Done(record) => record,
            other => return forward(other),
        };
        if !self.calls.persisted() {
            return Ok(Next::Done(AwaitOutcome::Skipped));
        }
        let writes = self.wait_rows(record.job_id)?;
        self.parked = Some(record);
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

/// The keys a parked job still waits for; empty when it is not parked.
pub async fn read_key_waits(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Vec<KeyWait>, String> {
    read_state(context, KEY_WAIT_KEYSPACE, job_wait_key(job_id), "key wait")
        .await
        .map(Option::unwrap_or_default)
}

/// Marks `key` available for one job; see [`SatisfyWaitOperation`].
pub async fn satisfy_key_wait(
    context: &DriverContext,
    job_id: JobId,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<bool, JobMutationError> {
    let mut attempt = 0;
    loop {
        let result = drive(SatisfyWaitOperation::new(job_id, key, now_ms), context).await;
        match settle_attempt(result, attempt, "key wake exhausted retries").await {
            Some(result) => return result,
            None => attempt += 1,
        }
    }
}

type WakeMutation = Box<dyn FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Send>;

#[derive(Debug, PartialEq)]
enum WaitStep {
    DeleteRow,
    ReadList,
    Mutate,
    Store,
}

/// Marks `key` available for one job. The job returns to `Queued`, due now, only when no
/// other wait remains; its run then rechecks source, authorization and generation. The answer
/// is whether the job woke.
#[derive(Debug, PartialEq)]
pub(crate) struct SatisfyWaitOperation {
    job_id: JobId,
    key: BucketKeyRef,
    now_ms: u64,
    waits: Vec<KeyWait>,
    calls: Option<MutateJobCalls<WakeMutation, NoGuard>>,
    woken: bool,
    step: WaitStep,
}

impl SatisfyWaitOperation {
    pub(crate) fn new(job_id: JobId, key: BucketKeyRef, now_ms: u64) -> Self {
        Self {
            job_id,
            key,
            now_ms,
            waits: Vec::new(),
            calls: None,
            woken: false,
            step: WaitStep::DeleteRow,
        }
    }

    /// Wakes the parked job when this was its last wait.
    fn mutate(&mut self, remaining: bool) -> Result<Next<bool>, JobMutationError> {
        let now_ms = self.now_ms;
        let mutate: WakeMutation = Box::new(move |record: &mut JobRecord| {
            if record.state != JobState::AwaitingKey || remaining {
                return Ok(JobMutation::Skip);
            }
            record.state = JobState::Queued;
            record.due_at_ms = now_ms;
            record.updated_at_ms = now_ms;
            Ok(JobMutation::Persist)
        });
        let mut calls = MutateJobCalls::new(self.job_id, mutate, None);
        let first = calls.first()?;
        self.calls = Some(calls);
        self.step = WaitStep::Mutate;
        forward(first)
    }

    /// Keeps the remaining waits, or drops a list that no longer belongs to a parked job.
    fn store(&mut self) -> Result<Next<bool>, JobMutationError> {
        self.step = WaitStep::Store;
        let list = (KEY_WAIT_KEYSPACE.to_string(), job_wait_key(self.job_id));
        if !self.waits.is_empty() {
            let value = postcard::to_allocvec(&self.waits)
                .map_err(|error| JobMutationError::Storage(error.to_string()))?;
            return Ok(Next::Call(IoCall::Write(vec![(
                list.0,
                list.1,
                ByteView::from(value),
            )])));
        }
        match self.woken {
            true => Ok(Next::Done(true)),
            // A list without a parked job is stale: the job was cancelled or pruned.
            false => Ok(Next::Call(IoCall::Delete(vec![list]))),
        }
    }
}

impl Operation for SatisfyWaitOperation {
    type Output = bool;
    type Error = JobMutationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<bool>, JobMutationError> {
        let row = (
            KEY_WAIT_KEYSPACE.to_string(),
            key_wait_key(self.key, self.job_id),
        );
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, JobMutationError> {
        match self.step {
            WaitStep::DeleteRow => {
                done(result)?;
                self.step = WaitStep::ReadList;
                let row = (KEY_WAIT_KEYSPACE.to_string(), job_wait_key(self.job_id));
                Ok(Next::Call(IoCall::Read(vec![row])))
            }
            WaitStep::ReadList => {
                let [list] = values::<1>(result)?;
                let Some(list) = list else {
                    return Ok(Next::Done(false));
                };
                let mut waits: Vec<KeyWait> = postcard::from_bytes(list.as_ref())
                    .map_err(|error| JobMutationError::Storage(error.to_string()))?;
                waits.retain(|wait| wait.key != self.key);
                self.waits = waits;
                self.mutate(!self.waits.is_empty())
            }
            WaitStep::Mutate => {
                let Some(calls) = self.calls.as_mut() else {
                    return Err(JobMutationError::Storage(
                        "key wake lost its mutation".into(),
                    ));
                };
                match calls.next(result) {
                    Ok(Next::Done(_)) => {
                        self.woken = calls.persisted();
                        self.store()
                    }
                    Ok(other) => forward(other),
                    Err(JobMutationError::NotFound) => self.store(),
                    Err(error) => Err(error),
                }
            }
            WaitStep::Store => {
                done(result)?;
                Ok(Next::Done(self.woken))
            }
        }
    }
}

/// Wakes one page of jobs waiting for `key`. Handled rows are deleted, so callers repeat
/// until this reports no further page.
pub async fn wake_key_page(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<(usize, bool), JobMutationError> {
    let prefix = ByteView::from([&b"k"[..], &key.key()].concat());
    let (rows, _) = iter_prefix_page(
        &context.storage_handle,
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
            delete_raw(context, KEY_WAIT_KEYSPACE, row.clone(), None)
                .await
                .map_err(JobMutationError::Storage)?;
            continue;
        };
        if satisfy_key_wait(context, job_id, key, now_ms).await? {
            woken += 1;
        }
    }
    Ok((woken, rows.len() == WAKE_PAGE))
}

/// Wakes every job waiting for `key` in bounded pages; call after the key is installed.
pub async fn wake_key_waits(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<usize, JobMutationError> {
    let mut total = 0;
    loop {
        let (woken, more) = wake_key_page(context, key, now_ms).await?;
        total += woken;
        if !more {
            return Ok(total);
        }
    }
}

#[cfg(test)]
#[path = "key_wait_tests.rs"]
mod tests;
