//! Resolves the receipt chain of one execution: its reservation, its alias family and its receipt.
//! An unresolved chain is reported as absent with a warning; the caller then publishes nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    ExecutionReceipt, JobFamilyId, JobFamilyRecord, JobId, JobRecordBody, JobRecordEnvelope,
    JobRecordKind,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use tracing::warn;
use ulid::Ulid;

use super::LifecycleError;
use super::alias::AliasFamilyOperation;
use super::held::HeldReservations;
use crate::jobs::JobRouteError;
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};

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

/// Reads one execution chain. Every read failure ends with no chain and a
/// warning, as a purely local job does; only an event that fits no phase fails.
#[derive(Debug, PartialEq)]
pub struct ExecutionChainOperation {
    /// The local physical job, when the chain starts from its reservation.
    physical: Option<JobId>,
    /// The logical alias the family records are keyed by, once known.
    logical: Option<JobId>,
    execution_id: Option<Ulid>,
    family: Option<JobFamilyId>,
    phase: Phase,
    outcome: Option<Result<Option<ExecutionChain>, LifecycleError>>,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Init,
    Reservation(HeldReservations),
    Alias(AliasFamilyOperation),
    Receipt(LoadFamilyOperation),
    Done,
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
            phase: Phase::Init,
            outcome: None,
        }
    }

    fn finish(&mut self, chain: Option<ExecutionChain>) -> Effects {
        self.outcome = Some(Ok(chain));
        self.phase = Phase::Done;
        smallvec![]
    }

    fn read_alias(&mut self, logical: JobId) -> Effects {
        self.logical = Some(logical);
        let mut alias = AliasFamilyOperation::new(logical);
        let effects = alias.start();
        self.phase = Phase::Alias(alias);
        effects
    }

    fn reserved(&mut self, held: Result<Vec<JobReservationRecord>, LifecycleError>) -> Effects {
        let Some(job_id) = self.physical else {
            return self.finish(None);
        };
        let reservation = match held {
            Ok(held) => held.into_iter().next(),
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Execution reservation read failed");
                None
            }
        };
        let Some(reservation) = reservation else {
            return self.finish(None);
        };
        match self.execution_id {
            Some(execution_id) if execution_id != reservation.execution_id => {
                warn!(
                    job_id = %job_id,
                    execution_id = %execution_id,
                    reserved = %reservation.execution_id,
                    "Attempt does not hold the reserved execution; the chain stays unresolved"
                );
                return self.finish(None);
            }
            _ => self.execution_id = Some(reservation.execution_id),
        }
        self.read_alias(reservation.logical_job_id)
    }

    fn aliased(&mut self, family: Result<Option<JobFamilyId>, JobRouteError>) -> Effects {
        let job_id = self.logical;
        let family = match family {
            Ok(Some(family)) => family,
            Ok(None) => {
                warn!(
                    job_id = ?job_id,
                    execution_id = ?self.execution_id,
                    "No job family alias for this job; the execution chain is unresolvable"
                );
                return self.finish(None);
            }
            Err(error) => {
                warn!(job_id = ?job_id, error = %error, "Job family alias read failed");
                return self.finish(None);
            }
        };
        self.family = Some(family);
        let mut receipts = LoadFamilyOperation::kind(family, JobRecordKind::Receipt);
        let effects = receipts.start();
        self.phase = Phase::Receipt(receipts);
        effects
    }

    fn receipted(&mut self, records: Result<Vec<JobRecordEnvelope>, FamilyReadError>) -> Effects {
        let (Some(family), Some(execution_id)) = (self.family, self.execution_id) else {
            return self.finish(None);
        };
        let records = match records {
            Ok(records) => records,
            Err(error) => {
                warn!(error = %error, "Execution receipt read is incomplete");
                return self.finish(None);
            }
        };
        let Some(receipt) = receipt_in(&records, execution_id) else {
            warn!(
                job_id = ?self.logical,
                execution_id = %execution_id,
                "No receipt for this execution in its family"
            );
            return self.finish(None);
        };
        let digest = match receipt.digest() {
            Ok(digest) => digest,
            Err(error) => {
                warn!(error = %error, "Execution receipt digest failed");
                return self.finish(None);
            }
        };
        self.finish(Some(ExecutionChain {
            family,
            execution_id,
            executor_node_id: receipt.executor_node_id,
            spec_digest: receipt.spec_digest,
            receipt_digest: digest,
            job_id: receipt.job_id,
        }))
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

/// Steps a child read: its effects while it runs, `None` once it ended.
fn advance<O: Operation>(child: &mut O, event: Event) -> Option<Effects> {
    let effects = child.step(event);
    (!child.is_complete()).then_some(effects)
}

impl Operation for ExecutionChainOperation {
    type Output = Option<ExecutionChain>;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        match self.physical {
            Some(job_id) => {
                let mut held = HeldReservations::of_job(job_id);
                let effects = held.start();
                self.phase = Phase::Reservation(held);
                effects
            }
            None => match self.logical {
                Some(logical) => self.read_alias(logical),
                None => self.finish(None),
            },
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        // Every phase reads pages; anything else fits none of them.
        let fits = matches!(
            event,
            Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
        );
        let phase = std::mem::replace(&mut self.phase, Phase::Done);
        if !fits || matches!(phase, Phase::Init | Phase::Done) {
            self.outcome = Some(Err(LifecycleError::UnexpectedEvent {
                state: "execution chain".to_string(),
                expected: "storage page",
                got: event.label(),
            }));
            return smallvec![];
        }
        match phase {
            Phase::Reservation(mut held) => match advance(&mut held, event) {
                Some(effects) => {
                    self.phase = Phase::Reservation(held);
                    effects
                }
                None => self.reserved(held.finalize()),
            },
            Phase::Alias(mut alias) => match advance(&mut alias, event) {
                Some(effects) => {
                    self.phase = Phase::Alias(alias);
                    effects
                }
                None => self.aliased(alias.finalize()),
            },
            Phase::Receipt(mut receipts) => match advance(&mut receipts, event) {
                Some(effects) => {
                    self.phase = Phase::Receipt(receipts);
                    effects
                }
                None => self.receipted(receipts.finalize()),
            },
            Phase::Init | Phase::Done => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.phase, Phase::Done)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "chain_tests.rs"]
mod state_machine_tests;
