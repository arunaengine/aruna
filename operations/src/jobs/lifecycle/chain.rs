//! Resolves the receipt chain of one execution: its reservation, its alias family and its receipt.
//! An unresolved chain is reported as absent with a warning; the caller then publishes nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    ExecutionReceipt, JobFamilyId, JobFamilyRecord, JobId, JobRecordBody, JobRecordEnvelope,
    JobRecordKind,
};
use tracing::warn;
use ulid::Ulid;

use super::LifecycleError;
use super::alias::AliasFamilyOperation;
use super::held::HeldReservations;
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::route::JobRouteError;

/// The replicated identity of one physical execution, read back from the
/// receipt that authorized it. Without it there is no distributed chain to
/// extend and every publication is skipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionChain {
    pub family: JobFamilyId,
    pub execution_id: Ulid,
    pub executor_node_id: NodeId,
    pub spec_digest: [u8; 32],
    pub receipt_digest: [u8; 32],
    pub job_id: JobId,
}

/// Reads one execution chain. A read that fails inside a child, or finds nothing, ends with no
/// chain and a warning, as a purely local job does; a failed storage call fails the operation.
#[derive(Debug, PartialEq)]
pub struct ExecutionChainOperation {
    /// The local physical job, when the chain starts from its reservation.
    physical: Option<JobId>,
    /// The logical alias the family records are keyed by, once known.
    logical: Option<JobId>,
    execution_id: Option<Ulid>,
    family: Option<JobFamilyId>,
    phase: Phase,
}

/// The child read in progress; its calls are this operation's calls.
#[derive(Debug, PartialEq)]
enum Phase {
    Idle,
    Reservation(HeldReservations),
    Alias(AliasFamilyOperation),
    Receipt(LoadFamilyOperation),
}

type Step = Result<Next<Option<ExecutionChain>>, LifecycleError>;

/// A read this chain steps itself never ends with calls after a commit.
fn after_commit() -> LifecycleError {
    LifecycleError::unexpected(&"Chain", "a call or an outcome", "Next::Then")
}

impl ExecutionChainOperation {
    /// The chain of one local job, if a target admitted it here. A job with no
    /// reservation is purely local and has no chain at all.
    pub fn of_job(job_id: JobId) -> Self {
        Self::new(Some(job_id), None)
    }

    /// The chain of one local job's exact execution. The reservation is what maps
    /// the physical job row to the logical alias family records are keyed by.
    pub fn of_attempt(job_id: JobId, execution_id: Ulid) -> Self {
        Self::new(Some(job_id), Some(execution_id))
    }

    /// The chain of one exact execution id; `job_id` is the logical alias,
    /// never a local physical job id.
    pub fn of_execution(job_id: JobId, execution_id: Ulid) -> Self {
        Self {
            logical: Some(job_id),
            ..Self::new(None, Some(execution_id))
        }
    }

    fn new(physical: Option<JobId>, execution_id: Option<Ulid>) -> Self {
        Self {
            physical,
            logical: None,
            execution_id,
            family: None,
            phase: Phase::Idle,
        }
    }

    fn read_alias(&mut self, logical: JobId) -> Step {
        self.logical = Some(logical);
        let mut alias = AliasFamilyOperation::new(logical);
        let step = alias.first();
        self.phase = Phase::Alias(alias);
        self.after_alias(step)
    }

    fn after_reservation(
        &mut self,
        step: Result<Next<Vec<JobReservationRecord>>, LifecycleError>,
    ) -> Step {
        match step {
            Ok(Next::Done(held)) => self.reserved(Ok(held)),
            Ok(Next::Call(call)) => Ok(Next::Call(call)),
            Ok(Next::Wait) => Ok(Next::Wait),
            Ok(Next::Then { .. }) => Err(after_commit()),
            Err(error) => self.reserved(Err(error)),
        }
    }

    fn after_alias(&mut self, step: Result<Next<Option<JobFamilyId>>, JobRouteError>) -> Step {
        match step {
            Ok(Next::Done(family)) => self.aliased(Ok(family)),
            Ok(Next::Call(call)) => Ok(Next::Call(call)),
            Ok(Next::Wait) => Ok(Next::Wait),
            Ok(Next::Then { .. }) => Err(after_commit()),
            Err(error) => self.aliased(Err(error)),
        }
    }

    fn after_receipts(
        &mut self,
        step: Result<Next<Vec<JobRecordEnvelope>>, FamilyReadError>,
    ) -> Step {
        match step {
            Ok(Next::Done(records)) => self.receipted(Ok(records)),
            Ok(Next::Call(call)) => Ok(Next::Call(call)),
            Ok(Next::Wait) => Ok(Next::Wait),
            Ok(Next::Then { .. }) => Err(after_commit()),
            Err(error) => self.receipted(Err(error)),
        }
    }

    fn reserved(&mut self, held: Result<Vec<JobReservationRecord>, LifecycleError>) -> Step {
        self.phase = Phase::Idle;
        let Some(job_id) = self.physical else {
            return Ok(Next::Done(None));
        };
        let reservation = match held {
            Ok(held) => held.into_iter().next(),
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Execution reservation read failed");
                None
            }
        };
        let Some(reservation) = reservation else {
            return Ok(Next::Done(None));
        };
        match self.execution_id {
            Some(execution_id) if execution_id != reservation.execution_id => {
                warn!(
                    job_id = %job_id,
                    execution_id = %execution_id,
                    reserved = %reservation.execution_id,
                    "Attempt does not hold the reserved execution; the chain stays unresolved"
                );
                return Ok(Next::Done(None));
            }
            _ => self.execution_id = Some(reservation.execution_id),
        }
        self.read_alias(reservation.logical_job_id)
    }

    fn aliased(&mut self, family: Result<Option<JobFamilyId>, JobRouteError>) -> Step {
        self.phase = Phase::Idle;
        let job_id = self.logical;
        let family = match family {
            Ok(Some(family)) => family,
            Ok(None) => {
                warn!(
                    job_id = ?job_id,
                    execution_id = ?self.execution_id,
                    "No job family alias for this job; the execution chain is unresolvable"
                );
                return Ok(Next::Done(None));
            }
            Err(error) => {
                warn!(job_id = ?job_id, error = %error, "Job family alias read failed");
                return Ok(Next::Done(None));
            }
        };
        self.family = Some(family);
        let mut receipts = LoadFamilyOperation::kind(family, JobRecordKind::Receipt);
        let step = receipts.first();
        self.phase = Phase::Receipt(receipts);
        self.after_receipts(step)
    }

    fn receipted(&mut self, records: Result<Vec<JobRecordEnvelope>, FamilyReadError>) -> Step {
        self.phase = Phase::Idle;
        let (Some(family), Some(execution_id)) = (self.family, self.execution_id) else {
            return Ok(Next::Done(None));
        };
        let records = match records {
            Ok(records) => records,
            Err(error) => {
                warn!(error = %error, "Execution receipt read is incomplete");
                return Ok(Next::Done(None));
            }
        };
        let Some(receipt) = receipt_in(&records, execution_id) else {
            warn!(
                job_id = ?self.logical,
                execution_id = %execution_id,
                "No receipt for this execution in its family"
            );
            return Ok(Next::Done(None));
        };
        let digest = match receipt.digest() {
            Ok(digest) => digest,
            Err(error) => {
                warn!(error = %error, "Execution receipt digest failed");
                return Ok(Next::Done(None));
            }
        };
        Ok(Next::Done(Some(ExecutionChain {
            family,
            execution_id,
            executor_node_id: receipt.executor_node_id,
            spec_digest: receipt.spec_digest,
            receipt_digest: digest,
            job_id: receipt.job_id,
        })))
    }
}

/// The receipt one execution was admitted under, among a family's receipt records.
pub(crate) fn receipt_in(
    records: &[JobRecordEnvelope],
    execution_id: Ulid,
) -> Option<ExecutionReceipt> {
    records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Receipt(receipt) if receipt.execution_id == execution_id => {
            Some(receipt.as_ref().clone())
        }
        _ => None,
    })
}

impl Operation for ExecutionChainOperation {
    type Output = Option<ExecutionChain>;
    type Error = LifecycleError;

    fn first(&mut self) -> Step {
        match (self.physical, self.logical) {
            (Some(job_id), _) => {
                let mut held = HeldReservations::of_job(job_id);
                let step = held.first();
                self.phase = Phase::Reservation(held);
                self.after_reservation(step)
            }
            (None, Some(logical)) => self.read_alias(logical),
            (None, None) => Ok(Next::Done(None)),
        }
    }

    fn next(&mut self, result: IoResult) -> Step {
        match std::mem::replace(&mut self.phase, Phase::Idle) {
            Phase::Reservation(mut held) => {
                let step = held.next(result);
                self.phase = Phase::Reservation(held);
                self.after_reservation(step)
            }
            Phase::Alias(mut alias) => {
                let step = alias.next(result);
                self.phase = Phase::Alias(alias);
                self.after_alias(step)
            }
            Phase::Receipt(mut receipts) => {
                let step = receipts.next(result);
                self.phase = Phase::Receipt(receipts);
                self.after_receipts(step)
            }
            Phase::Idle => Err(IoError::Unexpected {
                expected: "no result",
                got: "a result",
            }
            .into()),
        }
    }
}

#[cfg(test)]
#[path = "chain_tests.rs"]
mod state_machine_tests;
