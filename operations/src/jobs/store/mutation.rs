//! Reads, changes and writes one job row with its index rows and terminal obligations as calls.
//! A parent folds the calls into its own transaction; [`MutateJobOperation`] runs them alone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};

use super::*;

/// The page size of the report rows a settled report digests.
const REPORT_PAGE: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Read,
    Guard,
    Digest,
    Fence,
    Write,
    Delete,
    Obligations,
    CrateRead,
    CrateWrite,
    DedupRead,
    DedupDelete,
}

/// The calls of one job mutation: read the row, apply `mutate`, check `guard` against the
/// attempt control, then write the row, its index changes and the obligations a settled job
/// leaves. `Skip` ends with the unchanged row and writes nothing.
pub(crate) struct MutateJobCalls<F, G> {
    job_id: JobId,
    mutate: F,
    guard: Option<G>,
    old: Option<JobRecord>,
    record: Option<JobRecord>,
    digest: blake3::Hasher,
    terminal_deletes: JobDeletes,
    step: Step,
}

impl<F, G> std::fmt::Debug for MutateJobCalls<F, G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutateJobCalls")
            .field("job_id", &self.job_id)
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

/// Two mutations are equal when they stand at the same place; closures are not compared.
impl<F, G> PartialEq for MutateJobCalls<F, G> {
    fn eq(&self, other: &Self) -> bool {
        self.job_id == other.job_id
            && self.old == other.old
            && self.record == other.record
            && self.terminal_deletes == other.terminal_deletes
            && self.step == other.step
    }
}

type Mutated = Result<Next<JobRecord>, JobMutationError>;

impl<F, G> MutateJobCalls<F, G>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError>,
    G: FnMut(&JobRecord, Option<&AttemptControl>) -> Result<(), JobMutationError>,
{
    pub(crate) fn new(job_id: JobId, mutate: F, guard: Option<G>) -> Self {
        Self {
            job_id,
            mutate,
            guard,
            old: None,
            record: None,
            digest: blake3::Hasher::new(),
            terminal_deletes: Vec::new(),
            step: Step::Read,
        }
    }

    /// Whether the mutation changed the row, rather than skipping it.
    pub(crate) fn persisted(&self) -> bool {
        self.old.is_some()
    }

    pub(crate) fn first(&mut self) -> Mutated {
        self.step = Step::Read;
        Ok(Next::Call(record_read(self.job_id)))
    }

    pub(crate) fn next(&mut self, result: IoResult) -> Mutated {
        match self.step {
            Step::Read => self.mutate_row(result),
            Step::Guard => {
                let [value] = values::<1>(result)?;
                let control = value
                    .map(|bytes| {
                        AttemptControl::from_bytes(bytes.as_ref())
                            .map_err(|error| JobMutationError::Storage(error.to_string()))
                    })
                    .transpose()?;
                let record = self.record.as_ref().ok_or_else(missing)?;
                if let Some(guard) = self.guard.as_mut() {
                    guard(record, control.as_ref())?;
                }
                self.check()
            }
            Step::Digest => {
                let IoResult::Page { rows, next } = result else {
                    return Err(unexpected(&result));
                };
                for (_, value) in rows {
                    hash_report_value(&mut self.digest, value.as_ref());
                }
                match next {
                    Some(after) => Ok(self.report_page(Some(after))),
                    None => {
                        self.keep_digest();
                        self.fence()
                    }
                }
            }
            Step::Fence => {
                let [value] = values::<1>(result)?;
                self.fenced(value.as_deref())?;
                self.write()
            }
            Step::Write => {
                done(result)?;
                let (_, mut deletes) = self.deltas()?;
                self.step = Step::Delete;
                deletes.extend(std::mem::take(&mut self.terminal_deletes));
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            Step::Delete => {
                done(result)?;
                self.obligations()
            }
            Step::Obligations => {
                done(result)?;
                self.crate_read()
            }
            Step::CrateRead => {
                let [value] = values::<1>(result)?;
                self.crate_write(value.as_deref())
            }
            Step::CrateWrite => {
                done(result)?;
                self.dedup_read()
            }
            Step::DedupRead => {
                let [value] = values::<1>(result)?;
                self.dedup_delete(value.as_deref())
            }
            Step::DedupDelete => {
                done(result)?;
                self.finish()
            }
        }
    }

    fn mutate_row(&mut self, result: IoResult) -> Mutated {
        let [value] = values::<1>(result)?;
        let Some(mut record) = read_record(self.job_id, value) else {
            return Err(JobMutationError::NotFound);
        };
        let old = record.clone();
        match (self.mutate)(&mut record)? {
            JobMutation::Skip => Ok(Next::Done(old)),
            JobMutation::Persist => {
                self.old = Some(old);
                let epoch = record
                    .attempt_intent
                    .as_ref()
                    .map(|intent| intent.attempt_epoch);
                self.record = Some(record);
                match (self.guard.is_some(), epoch) {
                    (true, Some(epoch)) => {
                        self.step = Step::Guard;
                        let key = ByteView::from(attempt_control_key(self.job_id, epoch));
                        let row = (ATTEMPT_CONTROL_KEYSPACE.to_string(), key);
                        Ok(Next::Call(IoCall::Read(vec![row])))
                    }
                    (true, None) => {
                        let record = self.record.as_ref().ok_or_else(missing)?;
                        if let Some(guard) = self.guard.as_mut() {
                            guard(record, None)?;
                        }
                        self.check()
                    }
                    (false, _) => self.check(),
                }
            }
        }
    }

    /// Checks the transition, and digests the report of a settled RO-Crate job or session.
    fn check(&mut self) -> Mutated {
        let old = self.old.as_ref().ok_or_else(missing)?;
        let record = self.record.as_mut().ok_or_else(missing)?;
        if old.state != record.state {
            validate_transition(old.execution_class, old.state, record.state)?;
        }
        // A source's pause belongs to a running attempt; every other state stores none.
        if record.state != JobState::Running {
            record.progress.paused_until = None;
        }
        let reported = record.payload.is_rocrate()
            || matches!(&record.payload, JobPayload::Execution(spec) if session_of(spec).is_some());
        if !old.is_settled() && record.is_settled() && reported {
            return Ok(self.report_page(None));
        }
        self.fence()
    }

    fn report_page(&mut self, after: Option<Key>) -> Next<JobRecord> {
        self.step = Step::Digest;
        Next::Call(IoCall::Page {
            space: JOB_ENTRY_KEYSPACE.to_string(),
            prefix: job_entry_prefix(self.job_id),
            after,
            limit: REPORT_PAGE,
        })
    }

    fn keep_digest(&mut self) {
        let digest = *std::mem::replace(&mut self.digest, blake3::Hasher::new())
            .finalize()
            .as_bytes();
        let Some(record) = self.record.as_mut() else {
            return;
        };
        record.report_digest = Some(digest);
        match record.result.as_mut() {
            Some(JobResultPayload::ImportRoCrate(result)) => result.report_digest = digest,
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

    fn settled(&self) -> bool {
        match (self.old.as_ref(), self.record.as_ref()) {
            (Some(old), Some(record)) => !old.is_settled() && record.is_settled(),
            _ => false,
        }
    }

    /// A settled purge job reads its bucket's fence, which it releases if it still owns it.
    fn fence(&mut self) -> Mutated {
        let record = self.record.as_ref().ok_or_else(missing)?;
        match record.payload.purge_spec().filter(|_| self.settled()) {
            Some(spec) => {
                self.step = Step::Fence;
                let bucket = crate::s3::purge_fence::fence_key(spec.scope.bucket());
                let row = (PURGE_FENCE_KEYSPACE.to_string(), bucket);
                Ok(Next::Call(IoCall::Read(vec![row])))
            }
            None => self.write(),
        }
    }

    fn fenced(&mut self, value: Option<&[u8]>) -> Result<(), JobMutationError> {
        let record = self.record.as_ref().ok_or_else(missing)?;
        let Some(spec) = record.payload.purge_spec() else {
            return Ok(());
        };
        let owned = crate::s3::fence_store::owned_fence_delete(value, record.job_id, &spec.scope)
            .map_err(|error| JobMutationError::Storage(error.to_string()))?;
        self.terminal_deletes.extend(owned);
        self.terminal_deletes.push((
            PURGE_CHECKPOINT_KEYSPACE.to_string(),
            ByteView::from(record.job_id.to_bytes().to_vec()),
        ));
        Ok(())
    }

    fn deltas(&self) -> Result<(JobWrites, JobDeletes), JobMutationError> {
        let old = self.old.as_ref().ok_or_else(missing)?;
        let record = self.record.as_ref().ok_or_else(missing)?;
        index_deltas(old, record).map_err(|error| JobMutationError::Storage(error.to_string()))
    }

    fn write(&mut self) -> Mutated {
        let (writes, _) = self.deltas()?;
        self.step = Step::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// A settled job queues its run crate and container cleanup in the same transaction.
    fn obligations(&mut self) -> Mutated {
        let mut writes = Vec::new();
        if self.settled() {
            let record = self.record.as_ref().ok_or_else(missing)?;
            writes.extend(crate_obligation(record)?);
            writes.extend(cleanup_obligation(record)?);
        }
        self.step = Step::Obligations;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// A failed or cancelled run-crate job reads the crate status it may mark failed.
    fn crate_read(&mut self) -> Mutated {
        let record = self.record.as_ref().ok_or_else(missing)?;
        let failed = matches!(record.state, JobState::Failed | JobState::Cancelled);
        match &record.payload {
            JobPayload::WriteRunCrate { for_job } if self.settled() && failed => {
                self.step = Step::CrateRead;
                let row = (RUN_CRATE_KEYSPACE.to_string(), run_crate_key(*for_job));
                Ok(Next::Call(IoCall::Read(vec![row])))
            }
            _ => self.dedup_read(),
        }
    }

    fn crate_write(&mut self, value: Option<&[u8]>) -> Mutated {
        let record = self.record.as_ref().ok_or_else(missing)?;
        let JobPayload::WriteRunCrate { for_job } = &record.payload else {
            return self.dedup_read();
        };
        if let Some(value) = value {
            let status = RunCrateStatus::from_bytes(value)
                .map_err(|error| JobMutationError::Storage(error.to_string()))?;
            if !matches!(
                status,
                RunCrateStatus::Pending | RunCrateStatus::Minted { .. }
            ) {
                return self.dedup_read();
            }
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
        let bytes = status
            .to_bytes()
            .map_err(|error| JobMutationError::Storage(error.to_string()))?;
        let row = (
            RUN_CRATE_KEYSPACE.to_string(),
            run_crate_key(*for_job),
            ByteView::from(bytes),
        );
        self.step = Step::CrateWrite;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    /// Remove a non-RO-Crate dedup row only when it still references THIS job.
    /// RO-Crate dedup rows persist until their jobs are pruned.
    fn dedup_read(&mut self) -> Mutated {
        let old = self.old.as_ref().ok_or_else(missing)?;
        let Some(dedup_key) = &old.dedup_key else {
            return self.finish();
        };
        if old.payload.dedup_until_prune() || !self.settled() {
            return self.finish();
        }
        let key = dedup_index_key(old.created_by, dedup_key);
        self.step = Step::DedupRead;
        Ok(Next::Call(IoCall::Read(vec![(
            DEDUP_INDEX_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn dedup_delete(&mut self, value: Option<&[u8]>) -> Mutated {
        let old = self.old.as_ref().ok_or_else(missing)?;
        let still_ours = value
            .and_then(|bytes| parse_dedup_value(bytes).ok())
            .is_some_and(|(job_id, _)| job_id == old.job_id);
        let Some(dedup_key) = old.dedup_key.as_ref().filter(|_| still_ours) else {
            return self.finish();
        };
        let key = dedup_index_key(old.created_by, dedup_key);
        self.step = Step::DedupDelete;
        Ok(Next::Call(IoCall::Delete(vec![(
            DEDUP_INDEX_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn finish(&mut self) -> Mutated {
        self.record.take().map(Next::Done).ok_or_else(missing)
    }
}

/// The run crate a settled execution owes: its writer job and a pending crate status.
fn crate_obligation(record: &JobRecord) -> Result<JobWrites, JobMutationError> {
    let JobPayload::Execution(spec) = &record.payload else {
        return Ok(Vec::new());
    };
    // A stopped notebook session keeps its report; it is no run to describe.
    if session_of(spec).is_some() {
        return Ok(Vec::new());
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
    let storage = |error: ConversionError| JobMutationError::Storage(error.to_string());
    let mut writes = job_insert_entries(&child).map_err(storage)?;
    let pending = RunCrateStatus::Pending.to_bytes().map_err(storage)?;
    writes.push((
        RUN_CRATE_KEYSPACE.to_string(),
        run_crate_key(record.job_id),
        ByteView::from(pending),
    ));
    Ok(writes)
}

/// The cleanup job of a settled execution, which revokes its workspace credential.
fn cleanup_obligation(record: &JobRecord) -> Result<JobWrites, JobMutationError> {
    if !matches!(&record.payload, JobPayload::Execution(_)) {
        return Ok(Vec::new());
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
    job_insert_entries(&child).map_err(|error| JobMutationError::Storage(error.to_string()))
}

/// The read of one job row.
pub(super) fn record_read(job_id: JobId) -> IoCall {
    IoCall::Read(vec![(JOB_KEYSPACE.to_string(), job_record_key(job_id))])
}

/// The job record of a row read; a malformed row reads as missing and is kept.
pub(super) fn read_record(job_id: JobId, value: Option<Value>) -> Option<JobRecord> {
    value.and_then(|bytes| match JobRecord::from_bytes(&bytes) {
        Ok(record) => Some(record),
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Malformed job record retained");
            None
        }
    })
}

fn missing() -> JobMutationError {
    JobMutationError::Storage("job mutation lost its record".to_string())
}

fn unexpected(result: &IoResult) -> JobMutationError {
    JobMutationError::Storage(format!(
        "unexpected job mutation result: {}",
        result.label()
    ))
}

/// One job mutation in its own write transaction. A lost conflict is retried by the caller with
/// a fresh run, so the closures run again on the current row.
pub(crate) struct MutateJobOperation<F, G>(MutateJobCalls<F, G>);

impl<F, G> std::fmt::Debug for MutateJobOperation<F, G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<F, G> PartialEq for MutateJobOperation<F, G> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<F, G> MutateJobOperation<F, G>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError>,
    G: FnMut(&JobRecord, Option<&AttemptControl>) -> Result<(), JobMutationError>,
{
    pub(crate) fn new(job_id: JobId, mutate: F, guard: Option<G>) -> Self {
        Self(MutateJobCalls::new(job_id, mutate, guard))
    }
}

impl<F, G> Operation for MutateJobOperation<F, G>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Send,
    G: FnMut(&JobRecord, Option<&AttemptControl>) -> Result<(), JobMutationError> + Send,
{
    type Output = JobRecord;
    type Error = JobMutationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Mutated {
        self.0.first()
    }

    fn next(&mut self, result: IoResult) -> Mutated {
        self.0.next(result)
    }
}

/// A mutation without a guard names this guard type.
pub(crate) type NoGuard = fn(&JobRecord, Option<&AttemptControl>) -> Result<(), JobMutationError>;

/// Forwards a call of folded job mutation calls; only calls and the outcome come from them.
pub(crate) fn forward<T>(next: Next<JobRecord>) -> Result<Next<T>, JobMutationError> {
    match next {
        Next::Call(call) => Ok(Next::Call(call)),
        other => Err(JobMutationError::Storage(format!(
            "job mutation calls ended early: {}",
            match other {
                Next::Wait => "Next::Wait",
                _ => "an outcome",
            }
        ))),
    }
}

/// Rows of an in-memory store, by key space and key.
#[cfg(test)]
pub(crate) type Rows = std::collections::BTreeMap<(String, Vec<u8>), Vec<u8>>;

/// Runs `operation` against `rows`, answering reads, pages, writes and deletes as storage would.
#[cfg(test)]
pub(crate) fn run_in_memory<O: Operation>(
    mut operation: O,
    rows: &mut Rows,
) -> Result<O::Output, O::Error> {
    let mut turn = operation.first();
    loop {
        let call = match turn? {
            Next::Done(output) | Next::Then { output, .. } => return Ok(output),
            Next::Wait => panic!("a store operation never waits"),
            Next::Call(call) => call,
        };
        let result = match call {
            IoCall::Read(reads) => IoResult::Values(
                reads
                    .into_iter()
                    .map(|(space, key)| {
                        let value = rows.get(&(space, key.to_vec()));
                        value.map(|value| Value::from(value.as_slice()))
                    })
                    .collect(),
            ),
            IoCall::Page {
                space,
                prefix,
                after,
                limit,
            } => {
                let page: Vec<(Key, Value)> = rows
                    .iter()
                    .filter(|((row_space, key), _)| *row_space == space && key.starts_with(&prefix))
                    .filter(|((_, key), _)| after.as_ref().is_none_or(|after| key[..] > after[..]))
                    .take(limit)
                    .map(|((_, key), value)| {
                        (Key::from(key.as_slice()), Value::from(value.as_slice()))
                    })
                    .collect();
                let next = (page.len() == limit)
                    .then(|| page.last().map(|(key, _)| key.clone()))
                    .flatten();
                IoResult::Page { rows: page, next }
            }
            IoCall::Write(writes) => {
                for (space, key, value) in writes {
                    rows.insert((space, key.to_vec()), value.to_vec());
                }
                IoResult::Done
            }
            IoCall::Delete(deletes) => {
                for (space, key) in deletes {
                    rows.remove(&(space, key.to_vec()));
                }
                IoResult::Done
            }
            other => panic!("unexpected store call {other:?}"),
        };
        turn = operation.next(result);
    }
}

/// The rows of one stored job, as an in-memory store.
#[cfg(test)]
pub(crate) fn seeded_rows(record: &JobRecord) -> Rows {
    job_insert_entries(record)
        .expect("job rows")
        .into_iter()
        .map(|(space, key, value)| ((space, key.to_vec()), value.to_vec()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;

    fn probe() -> JobRecord {
        JobRecord::new(
            JobId::from_bytes([4; 16]),
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32])),
            iroh::SecretKey::from_bytes(&[7; 32]).public(),
            1_000,
            1_000,
            None,
        )
    }

    fn stored(rows: &Rows, job_id: JobId) -> JobRecord {
        let row = rows
            .get(&(JOB_KEYSPACE.to_string(), job_record_key(job_id).to_vec()))
            .expect("job row");
        JobRecord::from_bytes(row).expect("job row decodes")
    }

    // A persisted change writes the job row in the same run; a skip writes nothing.
    #[test]
    fn writes_or_skips() {
        let record = probe();
        let mut rows = seeded_rows(&record);
        let mut later = |record: &mut JobRecord| {
            record.updated_at_ms = 2_000;
            Ok(JobMutation::Persist)
        };
        let operation = MutateJobOperation::new(record.job_id, &mut later, None::<NoGuard>);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        let changed = run_in_memory(operation, &mut rows).expect("mutation");
        assert_eq!(changed.updated_at_ms, 2_000);
        assert_eq!(stored(&rows, record.job_id).updated_at_ms, 2_000);

        let before = rows.clone();
        let mut skip = |_: &mut JobRecord| Ok(JobMutation::Skip);
        let operation = MutateJobOperation::new(record.job_id, &mut skip, None::<NoGuard>);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(changed));
        assert_eq!(rows, before);
    }

    // A missing row is reported, and a guard refusal ends the run before any write.
    #[test]
    fn refuses_before_writing() {
        let record = probe();
        let mut rows = Rows::new();
        let mut persist = |_: &mut JobRecord| Ok(JobMutation::Persist);
        let operation = MutateJobOperation::new(record.job_id, &mut persist, None::<NoGuard>);
        assert_eq!(
            run_in_memory(operation, &mut rows),
            Err(JobMutationError::NotFound)
        );

        let mut rows = seeded_rows(&record);
        let before = rows.clone();
        let mut later = |record: &mut JobRecord| {
            record.updated_at_ms = 2_000;
            Ok(JobMutation::Persist)
        };
        let mut guard = |_: &JobRecord, control: Option<&AttemptControl>| {
            assert!(
                control.is_none(),
                "a record without an intent has no control"
            );
            Err(JobMutationError::EpochMismatch)
        };
        let operation = MutateJobOperation::new(record.job_id, &mut later, Some(&mut guard));
        assert_eq!(
            run_in_memory(operation, &mut rows),
            Err(JobMutationError::EpochMismatch)
        );
        assert_eq!(rows, before);
    }
}
