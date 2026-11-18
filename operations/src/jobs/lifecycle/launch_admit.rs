//! Decides one launch offer at the execution target: spec, submitter, placement and fit are
//! checked, the receipt is signed and committed with its reservation; a replay answers with the
//! stored receipt and a family that already ran here is declined.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::{ExecutorCapability, ResourceEnvelope};
use aruna_core::effects::{
    Effect, JobRecordFrame, LaunchFrame, MAX_RECORD_HOLDERS, NetEffect, ReceiptFrame,
};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, ExecutionSignEvent, LaunchDecline, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::node_state::{NodeInfoError, ReadInfoOperation};
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::scheduling::PlannedInput;
use aruna_core::structs::execution::job::{
    ExecutionClaim, ExecutionReceipt, InputSource, JobFamilyId, JobFamilyRecord, JobId, JobPayload,
    JobRecord, JobRecordBody, JobRecordEnvelope, JobRecordKind, LaunchIntent, LogicalJobSpec,
    PhysicalExecutionState, WorkspaceMode,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use std::collections::BTreeMap;
use thiserror::Error;
use tracing::{debug, info, warn};
use ulid::Ulid;

use super::LifecycleError;
use super::fetch::{FetchFamilyOperation, FetchedFamily};
use super::ids::workspace_of;
use super::launch_checks::{CheckError, LaunchCheck, LaunchChecksOperation, Verdict};
use super::reservation::{ReserveExecutionConfig, ReserveExecutionOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::records::RecordStoreError;
use crate::jobs::records::admit::Admission;
use crate::jobs::records::append::{
    AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin,
};
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::records::pages::{PageReadError, PlannedInputsOperation, SpecInputsOperation};
use crate::jobs::records::reduce::reduce_family;
use crate::jobs::records::verify::FamilyView;
use crate::jobs::submit::mint_configured_job;

/// Wall-clock budget of the record fetch that pulls a missing stored spec.
const FETCH_DEADLINE: Duration = Duration::from_secs(10);
/// Reservation attempts one offer makes while no commit of its own became
/// durable: a lost race or a write storage never accepted.
pub(crate) const RESERVE_ATTEMPTS: u32 = 3;

/// An admitted receipt or the reason the offer was refused.
pub(crate) type LaunchDecision = Result<ReceiptFrame, LaunchDecline>;

/// The offer could not be decided at all, which is an availability answer and never a refusal
/// the scheduler should act on.
#[derive(Debug, Error, PartialEq)]
#[error("the launch offer stays undecided")]
pub(crate) struct LaunchUndecided;

impl From<IoError> for LaunchUndecided {
    fn from(_: IoError) -> Self {
        Self
    }
}

/// One decided offer, and the local run an admitted launch must cancel at once.
#[derive(Debug, PartialEq)]
pub(crate) struct LaunchAnswer {
    pub(crate) decision: LaunchDecision,
    pub(crate) cancel: Option<JobId>,
}

impl LaunchAnswer {
    fn declined(reason: LaunchDecline) -> Self {
        Self {
            decision: Err(reason),
            cancel: None,
        }
    }
}

/// Every record of one family, or nothing at all. An incomplete read leaves the offer
/// undecided: a receipt, a cancellation or the stored spec may be in the part that did not load.
fn family_read(result: IoResult) -> Result<Vec<JobRecordEnvelope>, LaunchUndecided> {
    result
        .child::<Vec<JobRecordEnvelope>, FamilyReadError>()?
        .map_err(|error| {
            warn!(error = %error, "Job family read is incomplete; launch stays undecided");
            LaunchUndecided
        })
}

/// Reads whether an admitted launch's family was cancelled meanwhile, so the answer can name the
/// physical run to cancel. An incomplete read leaves the offer undecided.
#[derive(Debug, PartialEq)]
pub(crate) struct SettleLaunchOperation {
    decision: Option<LaunchDecision>,
}

impl SettleLaunchOperation {
    pub(crate) fn new(decision: LaunchDecision) -> Self {
        Self {
            decision: Some(decision),
        }
    }
}

impl Operation for SettleLaunchOperation {
    type Output = LaunchAnswer;
    type Error = LaunchUndecided;

    fn first(&mut self) -> Result<Next<LaunchAnswer>, LaunchUndecided> {
        match self.decision.take().ok_or(LaunchUndecided)? {
            Ok(frame) => {
                let family = frame.envelope().family();
                self.decision = Some(Ok(frame));
                let read = LoadFamilyOperation::family(family);
                Ok(Next::Call(IoCall::child(read)))
            }
            Err(reason) => Ok(Next::Done(LaunchAnswer::declined(reason))),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LaunchAnswer>, LaunchUndecided> {
        let records = family_read(result)?;
        let decision = self.decision.take().ok_or(LaunchUndecided)?;
        let mut cancel = None;
        if let Ok(frame) = &decision {
            let family = frame.envelope().family();
            if cancelled(family, &records)
                && let JobFamilyRecord::Receipt(receipt) = &frame.envelope().record
            {
                cancel = Some(receipt.physical_job_id);
            }
        }
        Ok(Next::Done(LaunchAnswer { decision, cancel }))
    }
}

/// What one reservation attempt decided, and what it proves about the writes it
/// tried to commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitVerdict {
    /// Exact local capacity refused the execution.
    Capacity,
    /// Another admission committed first; its receipt answers this offer.
    Raced,
    /// Storage never accepted the write, so the reservation may be reattempted.
    Retry,
    /// The commit neither succeeded nor was refused; its writes may exist.
    Uncertain,
    /// This node cannot establish that it may admit here at all.
    Drained,
    /// A local fault that decides nothing about the offer.
    Faulted,
}

/// Classifies one reservation failure by what it means, never by convenience:
/// only a real capacity verdict may be reported as capacity, and only a lost
/// membership or missing realm config as a drain.
pub(crate) fn classify(error: &LifecycleError) -> CommitVerdict {
    match error {
        LifecycleError::Capacity => CommitVerdict::Capacity,
        LifecycleError::Storage(StorageError::TransactionConflict) => CommitVerdict::Raced,
        LifecycleError::Storage(storage) if storage.proves_no_commit() => CommitVerdict::Retry,
        LifecycleError::Storage(_) => CommitVerdict::Uncertain,
        LifecycleError::NotHolder | LifecycleError::RealmConfigMissing => CommitVerdict::Drained,
        LifecycleError::GroupWrite(_)
        | LifecycleError::Conversion(_)
        | LifecycleError::Codec(_)
        | LifecycleError::Record(_)
        | LifecycleError::Family(_)
        | LifecycleError::Store(_)
        | LifecycleError::IdempotencyConflict { .. }
        | LifecycleError::QuotaDenied(_)
        | LifecycleError::NotFinished
        | LifecycleError::UnexpectedEvent { .. } => CommitVerdict::Faulted,
    }
}

#[derive(Debug, PartialEq)]
enum CommitStep {
    Reserve,
    Receipts(CommitVerdict),
    Settle,
}

/// Commits one stored receipt with its reservation. A lost race and a write storage never
/// accepted are attempted again; an unknown commit outcome is reconciled once and never
/// reserved again, so its writes are not redone.
#[derive(Debug, PartialEq)]
pub(crate) struct CommitReceiptOperation {
    config: ReserveExecutionConfig,
    intent: LaunchIntent,
    attempt: u32,
    step: CommitStep,
}

type CommitNext = Result<Next<LaunchAnswer>, LaunchUndecided>;

impl CommitReceiptOperation {
    pub(crate) fn new(config: ReserveExecutionConfig, intent: LaunchIntent) -> Self {
        Self {
            config,
            intent,
            attempt: 0,
            step: CommitStep::Reserve,
        }
    }

    fn reserve(&mut self) -> CommitNext {
        if self.attempt >= RESERVE_ATTEMPTS {
            return Err(LaunchUndecided);
        }
        self.attempt += 1;
        self.step = CommitStep::Reserve;
        let reserve = ReserveExecutionOperation::new(self.config.clone());
        Ok(Next::Call(IoCall::child(reserve)))
    }

    fn settle(&mut self, decision: LaunchDecision) -> CommitNext {
        self.step = CommitStep::Settle;
        Ok(Next::Call(IoCall::child(SettleLaunchOperation::new(
            decision,
        ))))
    }

    /// The receipt already committed for this exact launch, whoever won the race.
    fn read_receipts(&mut self, verdict: CommitVerdict) -> CommitNext {
        self.step = CommitStep::Receipts(verdict);
        let family = self.config.receipt.envelope().family();
        let read = LoadFamilyOperation::kind(family, JobRecordKind::Receipt);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn reserved(&mut self, result: Result<Ulid, LifecycleError>) -> CommitNext {
        let error = match result {
            Ok(_) => {
                let frame = ReceiptFrame::new(self.config.receipt.envelope().clone())
                    .map_err(|_| LaunchDecline::Unauthorized);
                return self.settle(frame);
            }
            Err(error) => error,
        };
        match classify(&error) {
            CommitVerdict::Capacity => {
                Ok(Next::Done(LaunchAnswer::declined(LaunchDecline::Capacity)))
            }
            // A commit conflict is two admissions racing one launch, not a refusal: the
            // winner's receipt is searched before the reservation is attempted again.
            CommitVerdict::Raced => self.read_receipts(CommitVerdict::Raced),
            CommitVerdict::Retry => {
                debug!(
                    attempt = self.attempt,
                    "Storage refused the reservation write; retrying"
                );
                self.reserve()
            }
            CommitVerdict::Uncertain => {
                warn!(error = %error, "Execution commit outcome is unknown; reconciling");
                self.read_receipts(CommitVerdict::Uncertain)
            }
            CommitVerdict::Drained => {
                Ok(Next::Done(LaunchAnswer::declined(LaunchDecline::Draining)))
            }
            CommitVerdict::Faulted => {
                warn!(error = %error, "Execution reservation failed locally");
                Err(LaunchUndecided)
            }
        }
    }

    fn receipts(&mut self, verdict: CommitVerdict, result: IoResult) -> CommitNext {
        let records = result
            .child::<Vec<JobRecordEnvelope>, FamilyReadError>()?
            .inspect_err(|error| {
                warn!(error = %error, "Receipt read is incomplete after a reservation race");
            })
            .ok();
        match records.and_then(|records| existing_receipt(&records, &self.intent)) {
            Some(committed) => self.settle(committed),
            None if verdict == CommitVerdict::Raced => {
                debug!(
                    attempt = self.attempt,
                    "Execution reservation lost a race and retries"
                );
                self.reserve()
            }
            None => Err(LaunchUndecided),
        }
    }
}

impl Operation for CommitReceiptOperation {
    type Output = LaunchAnswer;
    type Error = LaunchUndecided;

    fn first(&mut self) -> CommitNext {
        self.reserve()
    }

    fn next(&mut self, result: IoResult) -> CommitNext {
        match self.step {
            CommitStep::Reserve => {
                let reserved = result.child::<Ulid, LifecycleError>()?;
                self.reserved(reserved)
            }
            CommitStep::Receipts(verdict) => self.receipts(verdict, result),
            CommitStep::Settle => {
                let answer = result.child::<LaunchAnswer, LaunchUndecided>()??;
                Ok(Next::Done(answer))
            }
        }
    }
}
