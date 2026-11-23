//! Requeues parked materialization jobs on a growing backoff unless their document moved on.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

pub(super) fn requeue_after_ms(parks: u32) -> u64 {
    let base = Duration::from_millis(REQUEUE_BASE_MS);
    let cap = Duration::from_millis(REQUEUE_MAX_MS);
    let delay = capped_backoff(base, cap, parks.saturating_sub(1), 0);
    u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Debug, PartialEq)]
pub(super) enum RequeueStage {
    Page,
    Malformed,
    Requeue(Ulid, Ulid),
}

/// Returns due dead letters to the queue and clears their terminal status, so a
/// node converges once the cause clears. Dead letters the document has moved
/// past are dropped instead. Answers the number of jobs requeued.
#[derive(Debug, PartialEq)]
pub struct RequeueParkedOperation {
    pub(super) now_ms: u64,
    pub(super) due_ms: u64,
    pub(super) rows: VecDeque<(Key, Value)>,
    pub(super) next: Option<Key>,
    pub(super) requeued: usize,
    pub(super) stage: RequeueStage,
}

impl RequeueParkedOperation {
    pub fn new() -> Self {
        Self {
            now_ms: 0,
            due_ms: 0,
            rows: VecDeque::new(),
            next: None,
            requeued: 0,
            stage: RequeueStage::Page,
        }
    }

    pub(super) fn page(&mut self, after: Option<Key>) -> MaterializationTurn<usize> {
        self.stage = RequeueStage::Page;
        Ok(Next::Call(IoCall::Page {
            space: DEAD_LETTER_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: REQUEUE_PAGE_SIZE,
        }))
    }

    pub(super) fn sweep(&mut self) -> MaterializationTurn<usize> {
        while let Some((key, value)) = self.rows.pop_front() {
            let Ok(dead_letter) = postcard::from_bytes::<DeadLetterRecord>(&value) else {
                warn!(key = ?key.to_vec(), "Deleting malformed metadata materialization dead letter");
                self.stage = RequeueStage::Malformed;
                let delete = (DEAD_LETTER_KEYSPACE.to_string(), key);
                return Ok(Next::Call(IoCall::Delete(vec![delete])));
            };
            if dead_letter.requeue_at_ms > self.due_ms {
                continue;
            }
            let job = &dead_letter.job;
            self.stage = RequeueStage::Requeue(job.document_id, job.event_id);
            let requeue = RequeueJobOperation::new(dead_letter);
            return Ok(Next::Call(IoCall::child(requeue)));
        }
        if let Some(after) = self.next.take() {
            return self.page(Some(after));
        }
        if self.requeued > 0 {
            info!(
                event = "materialization.deadletter.requeued",
                jobs = self.requeued,
                "Requeued parked metadata materialization jobs"
            );
        }
        Ok(Next::Done(self.requeued))
    }
}

impl Default for RequeueParkedOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for RequeueParkedOperation {
    type Output = usize;
    type Error = MetadataMaterializationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> MaterializationTurn<usize> {
        self.due_ms = self.now_ms;
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<usize> {
        match std::mem::replace(&mut self.stage, RequeueStage::Page) {
            RequeueStage::Page => {
                let (rows, next) = result.page()?;
                self.rows = rows.into();
                self.next = next;
            }
            RequeueStage::Malformed => result.done()?,
            RequeueStage::Requeue(document_id, event_id) => {
                match result.child::<bool, MetadataMaterializationError>()? {
                    Ok(true) => self.requeued = self.requeued.saturating_add(1),
                    Ok(false) => {}
                    // A racing finish aborts this requeue but must not stop the remaining sweep.
                    Err(MetadataMaterializationError::Storage(
                        StorageError::TransactionConflict,
                    )) => {
                        debug!(
                            event = "materialization.deadletter.contended",
                            document_id = %document_id,
                            event_id = %event_id,
                            "Deferring contended metadata materialization dead letter"
                        );
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        self.sweep()
    }
}

// The parked job's own status is Failed at its own event, so only a final status
// beyond that means the document moved on and requeueing would regress it.
pub(super) fn dead_letter_superseded(
    status: &MaterializationStatusRecord,
    job: &MetadataMaterializationRecord,
) -> bool {
    status_obsoletes_job(status, job)
        && (status.event_id > job.event_id || status.state == MaterializationState::Materialized)
}

#[derive(Debug, PartialEq)]
pub(super) enum ParkedStage {
    Read,
    Dropped,
    Written,
    Requeued,
}

/// The parked status is terminal for this event, so it must be cleared with the job rows or
/// the requeued job is pruned as obsolete on the next scan. The transactional status guard
/// conflicts with a newer event instead of restoring stale work.
#[derive(Debug, PartialEq)]
pub(super) struct RequeueJobOperation {
    pub(super) dead_letter: DeadLetterRecord,
    pub(super) now_ms: u64,
    pub(super) stage: ParkedStage,
}

impl RequeueJobOperation {
    pub(super) fn new(dead_letter: DeadLetterRecord) -> Self {
        Self {
            dead_letter,
            now_ms: 0,
            stage: ParkedStage::Read,
        }
    }

    pub(super) fn job(&self) -> MetadataMaterializationRecord {
        MetadataMaterializationRecord {
            document_id: self.dead_letter.job.document_id,
            event_id: self.dead_letter.job.event_id,
            due_at_ms: self.now_ms,
            attempts: 0,
            failures: MATERIALIZATION_MAX_FAILURES.saturating_sub(1),
            parks: self.dead_letter.parks,
        }
    }

    pub(super) fn drop_letter(
        &mut self,
        job: &MetadataMaterializationRecord,
    ) -> MaterializationTurn<bool> {
        self.stage = ParkedStage::Dropped;
        let key = dead_letter_key(job.document_id, job.event_id);
        Ok(Next::Call(IoCall::Delete(vec![(
            DEAD_LETTER_KEYSPACE.to_string(),
            key,
        )])))
    }

    pub(super) fn requeue(&mut self, result: IoResult) -> MaterializationTurn<bool> {
        let job = self.job();
        let [event, status] = values::<2>(result)?;
        let event = match logged_event(job.document_id, job.event_id, event) {
            Ok(event) => event,
            Err(MetadataMaterializationError::CreateEventMissing { .. }) => {
                return self.drop_letter(&job);
            }
            Err(error) => return Err(error),
        };
        if let Some(current) = decode_status(status)?
            && dead_letter_superseded(&current, &job)
        {
            info!(
                event = "materialization.deadletter.superseded",
                document_id = %job.document_id,
                event_id = %job.event_id,
                status_event_id = %current.event_id,
                "Dropping superseded metadata materialization dead letter"
            );
            return self.drop_letter(&job);
        }
        let status = MaterializationStatusRecord {
            failures: job.failures,
            ..MaterializationStatusRecord::pending(&event, self.now_ms)
        };
        self.stage = ParkedStage::Written;
        Ok(Next::Call(IoCall::Write(vec![
            materialization_status_entry(&status)?,
            materialization_job_entry(&job)?,
            document_job_entry(&job)?,
        ])))
    }
}

impl Operation for RequeueJobOperation {
    type Output = bool;
    type Error = MetadataMaterializationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> MaterializationTurn<bool> {
        let job = &self.dead_letter.job;
        Ok(Next::Call(IoCall::Read(vec![
            event_read(job.document_id, job.event_id),
            status_read(job.document_id),
        ])))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<bool> {
        match std::mem::replace(&mut self.stage, ParkedStage::Requeued) {
            ParkedStage::Read => self.requeue(result),
            ParkedStage::Dropped => {
                result.done()?;
                Ok(Next::Done(false))
            }
            ParkedStage::Written => {
                result.done()?;
                let job = self.job();
                self.stage = ParkedStage::Requeued;
                let key = dead_letter_key(job.document_id, job.event_id);
                Ok(Next::Call(IoCall::Delete(vec![(
                    DEAD_LETTER_KEYSPACE.to_string(),
                    key,
                )])))
            }
            ParkedStage::Requeued => {
                result.done()?;
                Ok(Next::Done(true))
            }
        }
    }
}
