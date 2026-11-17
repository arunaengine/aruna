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
        let row = (JOB_KEYSPACE.to_string(), job_record_key(self.job_id));
        Ok(Next::Call(IoCall::Read(vec![row])))
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
        let record = value.and_then(|bytes| match JobRecord::from_bytes(&bytes) {
            Ok(record) => Some(record),
            Err(error) => {
                warn!(job_id = %self.job_id, error = %error, "Malformed job record retained");
                None
            }
        });
        let Some(mut record) = record else {
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

