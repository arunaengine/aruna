//! Builds the signed stop record of an execution this node ran, with its outbox entry, so the
//! terminal cleanup stores both in the transaction that records the attempt tombstone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, ExecutionSignEvent, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{FAMILY_OUTBOX_KEYSPACE, FAMILY_RECORD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::family::{
    ExecutionClaim, ExecutionReceipt, ExecutionStop, JobFamilyId, JobFamilyRecord, JobRecordBody,
    JobRecordEnvelope, JobRecordKind, StopEvidence,
};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::local::{JobError, RunLink};
use aruna_core::structs::execution::job::record_keys::{prune_rows, record_key};
use aruna_core::structs::execution::job::record_rows::{OutboxEntry, to_bytes};
use aruna_core::structs::execution::job::rows::JobWrites;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::Value;

use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::row_calls::read_run_link;

#[derive(Debug, PartialEq)]
enum StopStep {
    Link,
    Receipts(JobFamilyId),
    Stops(Box<(JobFamilyId, ExecutionReceipt)>),
    Updates(Box<(JobFamilyId, ExecutionReceipt)>),
    Sign,
}

type StopNext = Result<Next<JobWrites>, JobError>;

fn retryable(error: impl std::fmt::Display) -> JobError {
    JobError::retryable(format!("stop record failed: {error}"))
}

/// The stop record rows of the receipted execution of `for_job` on this node. Empty for a job
/// without such a receipt or when its stop is stored; retryable until its terminal update is.
#[derive(Debug, PartialEq)]
pub struct StopRecordOperation {
    for_job: JobId,
    evidence: StopEvidence,
    serving: (NodeId, RealmId),
    now_ms: u64,
    step: StopStep,
}

impl StopRecordOperation {
    pub fn new(for_job: JobId, evidence: StopEvidence, serving: (NodeId, RealmId)) -> Self {
        Self {
            for_job,
            evidence,
            serving,
            now_ms: 0,
            step: StopStep::Link,
        }
    }

    fn load(&mut self, step: StopStep, family: JobFamilyId, kind: JobRecordKind) -> StopNext {
        self.step = step;
        Ok(Next::Call(IoCall::child(LoadFamilyOperation::kind(
            family, kind,
        ))))
    }

    fn receipt(&self, records: Vec<JobRecordEnvelope>) -> Option<ExecutionReceipt> {
        records
            .into_iter()
            .find_map(|envelope| match envelope.record {
                JobFamilyRecord::Receipt(receipt)
                    if receipt.physical_job_id == self.for_job
                        && receipt.executor_node_id == self.serving.0 =>
                {
                    Some(*receipt)
                }
                _ => None,
            })
    }

    /// Asks for the signature of the stop that names the first terminal update.
    fn sign(&mut self, receipt: &ExecutionReceipt, updates: Vec<JobRecordEnvelope>) -> StopNext {
        let terminal = (updates.into_iter())
            .filter_map(|envelope| match envelope.record {
                JobFamilyRecord::Update(update)
                    if update.execution_id == receipt.execution_id
                        && update.state.is_terminal() =>
                {
                    Some(update)
                }
                _ => None,
            })
            .min_by_key(|update| update.sequence);
        // A stop names the terminal update, so it waits until that update is stored here.
        let Some(update) = terminal else {
            return Err(JobError::retryable("the terminal update is not stored yet"));
        };
        let stop = ExecutionStop {
            execution_id: receipt.execution_id,
            submission_id: receipt.submission_id,
            request_digest: receipt.request_digest,
            executor_node_id: self.serving.0,
            receipt_digest: receipt.digest().map_err(retryable)?,
            update_sequence: update.sequence,
            update_digest: update.digest().map_err(retryable)?,
            output_digest: (update.result.as_ref()).and_then(|result| result.output_digest),
            evidence: self.evidence,
            stopped_at_ms: self.now_ms,
        };
        let claim = ExecutionClaim {
            realm_id: self.serving.1,
            record: JobFamilyRecord::Stop(Box::new(stop)),
            publisher: self.serving.0,
        };
        self.step = StopStep::Sign;
        let effect = Effect::Net(NetEffect::ExecutionSign(Box::new(claim)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    /// The record, its outbox entry and the family's prune index rows, for one batch.
    fn rows(&self, envelope: &JobRecordEnvelope) -> StopNext {
        let entry = OutboxEntry {
            queued_at_ms: self.now_ms,
            delivered: Vec::new(),
            next_holder: 0,
            rejections: 0,
        };
        let key = record_key(&envelope.key());
        let record = Value::from(to_bytes(envelope).map_err(retryable)?.as_slice());
        let outbox = Value::from(to_bytes(&entry).map_err(retryable)?.as_slice());
        let mut rows = vec![
            (FAMILY_RECORD_KEYSPACE.to_string(), key.clone(), record),
            (FAMILY_OUTBOX_KEYSPACE.to_string(), key, outbox),
        ];
        rows.extend(prune_rows(&envelope.family(), self.now_ms));
        Ok(Next::Done(rows))
    }
}

impl Operation for StopRecordOperation {
    type Output = JobWrites;
    type Error = JobError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> StopNext {
        Ok(Next::Call(IoCall::child(read_run_link(self.for_job))))
    }

    fn next(&mut self, result: IoResult) -> StopNext {
        let loaded = |result: IoResult| {
            let records = result.child::<Vec<JobRecordEnvelope>, FamilyReadError>();
            records.map_err(retryable)?.map_err(retryable)
        };
        match std::mem::replace(&mut self.step, StopStep::Sign) {
            StopStep::Link => {
                let link = result.child::<Option<RunLink>, JobMutationError>();
                let link = link.map_err(retryable)?.map_err(retryable)?;
                match link.and_then(|link| link.family) {
                    Some(family) => {
                        let step = StopStep::Receipts(family);
                        self.load(step, family, JobRecordKind::Receipt)
                    }
                    None => Ok(Next::Done(Vec::new())),
                }
            }
            StopStep::Receipts(family) => match self.receipt(loaded(result)?) {
                Some(receipt) => {
                    let step = StopStep::Stops(Box::new((family, receipt)));
                    self.load(step, family, JobRecordKind::Stop)
                }
                None => Ok(Next::Done(Vec::new())),
            },
            StopStep::Stops(held) => {
                let execution_id = held.1.execution_id;
                let stored = loaded(result)?.into_iter().any(|envelope| {
                    matches!(&envelope.record, JobFamilyRecord::Stop(stop) if stop.execution_id == execution_id)
                });
                if stored {
                    return Ok(Next::Done(Vec::new()));
                }
                let family = held.0;
                self.load(StopStep::Updates(held), family, JobRecordKind::Update)
            }
            StopStep::Updates(held) => self.sign(&held.1, loaded(result)?),
            StopStep::Sign => {
                let expected = "Event::Net(NetEvent::ExecutionSign)";
                match result.event()? {
                    Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Signed(envelope))) => {
                        self.rows(&envelope)
                    }
                    Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Unavailable(error))) => {
                        Err(retryable(error))
                    }
                    other => Err(IoError::Unexpected {
                        expected,
                        got: other.label(),
                    }
                    .into()),
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "stop_record_tests.rs"]
mod tests;
