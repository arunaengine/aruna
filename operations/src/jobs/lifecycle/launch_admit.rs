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

#[derive(Debug, PartialEq)]
enum PullStep {
    Fetch,
    Append(Vec<JobRecordFrame>),
}

/// Pulls one bounded page of the family from its holders, so a target that has not synchronized
/// the spec, claim or budget yet can still admit the launch. Failures only leave it unsynced.
#[derive(Debug, PartialEq)]
struct PullFamilyOperation {
    fetch: Option<FetchFamilyOperation>,
    node: LaunchNode,
    origin: RecordOrigin,
    step: PullStep,
}

impl PullFamilyOperation {
    fn new(
        config: &RealmConfigDocument,
        family: JobFamilyId,
        scheduler: NodeId,
        node: &LaunchNode,
    ) -> Self {
        let fetch = config
            .family_placement(family.submission_id)
            .ok()
            .map(|placement| {
                let mut holders = resolve_shard_holders(config, &placement);
                holders.retain(|holder| *holder != scheduler);
                holders.truncate(MAX_RECORD_HOLDERS.saturating_sub(1));
                holders.insert(0, scheduler);
                let family = (family.submission_id, family.request_digest);
                FetchFamilyOperation::new(placement, holders, family, FETCH_DEADLINE)
            });
        Self {
            fetch,
            node: node.clone(),
            origin: RecordOrigin::Local,
            step: PullStep::Fetch,
        }
    }

    fn append_next(&mut self, mut frames: Vec<JobRecordFrame>) -> Result<Next<()>, IoError> {
        let Some(frame) = frames.pop() else {
            return Ok(Next::Done(()));
        };
        self.step = PullStep::Append(frames);
        let append = self.node.append(frame, self.origin);
        Ok(Next::Call(IoCall::child(append)))
    }
}

impl Operation for PullFamilyOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(match self.fetch.take() {
            Some(fetch) => Next::Call(IoCall::child(fetch)),
            None => Next::Done(()),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match std::mem::replace(&mut self.step, PullStep::Fetch) {
            PullStep::Fetch => {
                let Ok((source, mut frames)) = result.child::<FetchedFamily, LifecycleError>()?
                else {
                    return Ok(Next::Done(()));
                };
                self.origin = source.map_or(RecordOrigin::Local, RecordOrigin::Peer);
                // Taken from the end, so the fetched order stays the append order.
                frames.reverse();
                self.append_next(frames)
            }
            PullStep::Append(frames) => {
                let _ = result.child::<AppendOutcome, RecordStoreError>()?;
                self.append_next(frames)
            }
        }
    }
}

/// This node as a launch offer sees it. The limits are its advertisement for the offered
/// executor kind; `None` means that kind is not offered here.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LaunchNode {
    pub(crate) local: NodeId,
    pub(crate) realm_id: RealmId,
    pub(crate) now_ms: u64,
    pub(crate) execution_id: Ulid,
    pub(crate) limits: Option<ResourceEnvelope>,
}

impl LaunchNode {
    fn append(&self, record: JobRecordFrame, origin: RecordOrigin) -> AppendRecordOperation {
        AppendRecordOperation::new(AppendRecordConfig {
            realm_id: self.realm_id,
            local_node_id: self.local,
            record,
            local: None,
            origin,
            now_ms: self.now_ms,
        })
    }
}

#[derive(Debug, PartialEq)]
enum AdmitStep {
    Config,
    Records,
    Pull,
    RecordsAfterPull,
    SpecInputs,
    PlannedInputs(Box<LogicalJobSpec>),
    Append { retried: bool },
    PullBeforeAppend,
    Decide,
    Checks,
    Membership(Box<ExecutorCapability>),
    Sign(Box<PreparedReceipt>),
    Commit,
    Replay,
}

/// What the reservation needs besides the receipt being signed.
#[derive(Debug, PartialEq)]
struct PreparedReceipt {
    job_id: JobId,
    subject_generation: u64,
    subject_digest: [u8; 32],
    record: JobRecord,
}

/// Admits one offered launch, or answers why it was refused.
#[derive(Debug, PartialEq)]
pub(crate) struct AdmitLaunchOperation {
    launch: LaunchFrame,
    node: LaunchNode,
    config: Option<RealmConfigDocument>,
    intent: Option<LaunchIntent>,
    spec: Option<LogicalJobSpec>,
    planned: Option<LaunchIntent>,
    step: AdmitStep,
}

type AdmitNext = Result<Next<LaunchAnswer>, LaunchUndecided>;

impl AdmitLaunchOperation {
    pub(crate) fn new(launch: LaunchFrame, node: LaunchNode) -> Self {
        Self {
            launch,
            node,
            config: None,
            intent: None,
            spec: None,
            planned: None,
            step: AdmitStep::Config,
        }
    }

    fn family(&self) -> JobFamilyId {
        self.launch.envelope().family()
    }

    fn intent(&self) -> Result<&LaunchIntent, LaunchUndecided> {
        self.intent.as_ref().ok_or(LaunchUndecided)
    }

    fn config(&self) -> Result<&RealmConfigDocument, LaunchUndecided> {
        self.config.as_ref().ok_or(LaunchUndecided)
    }

    fn call(&mut self, step: AdmitStep, call: IoCall) -> AdmitNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn read_records(&mut self, step: AdmitStep) -> AdmitNext {
        let read = LoadFamilyOperation::family(self.family());
        self.call(step, IoCall::child(read))
    }

    fn pull(&mut self, step: AdmitStep) -> AdmitNext {
        let scheduler = self.intent()?.scheduler_node_id;
        let pull = PullFamilyOperation::new(self.config()?, self.family(), scheduler, &self.node);
        self.call(step, IoCall::child(pull))
    }

    fn configured(&mut self, config: RealmConfigDocument) -> AdmitNext {
        let envelope = self.launch.envelope();
        let JobFamilyRecord::Launch(intent) = &envelope.record else {
            return Ok(Next::Done(LaunchAnswer::declined(
                LaunchDecline::Unauthorized,
            )));
        };
        let intent = intent.as_ref().clone();
        if intent.target.node_id != self.node.local {
            // The offer names another node; this target has nothing to admit.
            return Ok(Next::Done(LaunchAnswer::declined(
                LaunchDecline::Unauthorized,
            )));
        }
        let view = FamilyView::resolve(&config, self.node.realm_id, envelope.family())
            .ok_or(LaunchUndecided)?;
        if !view.holds(intent.scheduler_node_id) {
            return Ok(Next::Done(LaunchAnswer::declined(LaunchDecline::NotHolder)));
        }
        self.config = Some(config);
        self.intent = Some(intent);
        self.read_records(AdmitStep::Records)
    }

    /// Paged inputs are read once here, so every check sees the full lists; the signed launch
    /// itself stays as offered, because its receipt binds that exact record.
    fn read_inputs(&mut self, spec: LogicalJobSpec) -> AdmitNext {
        let node = Some((self.node.local, self.node.realm_id));
        let fill = SpecInputsOperation::new(&spec, node);
        self.call(AdmitStep::SpecInputs, IoCall::child(fill))
    }

    /// The launch becomes a retained record before it can be receipted.
    fn append_launch(&mut self, retried: bool) -> AdmitNext {
        let frame =
            JobRecordFrame::new(self.launch.envelope().clone()).map_err(|_| LaunchUndecided);
        let origin = RecordOrigin::Peer(self.intent()?.scheduler_node_id);
        let append = self.node.append(frame?, origin);
        self.call(AdmitStep::Append { retried }, IoCall::child(append))
    }

    fn decide(&mut self, records: Vec<JobRecordEnvelope>) -> AdmitNext {
        let family = self.family();
        let intent = self.intent()?;
        // A replayed offer re-arms the wakeups the first acceptance may have lost,
        // so the receipt still replicates and the execution still starts.
        if let Some(decision) = existing_receipt(&records, intent) {
            self.step = AdmitStep::Replay;
            let settle = SettleLaunchOperation::new(decision);
            return Ok(Next::Call(IoCall::child(settle)));
        }
        if cancelled(family, &records) {
            return Ok(Next::Done(LaunchAnswer::declined(LaunchDecline::Cancelled)));
        }
        if already_running(family, &records, self.node.local) {
            return Ok(Next::Done(LaunchAnswer::declined(
                LaunchDecline::AlreadyRunning,
            )));
        }
        let check = LaunchCheck {
            config: self.config()?.clone(),
            local: self.node.local,
            planned: self.planned.clone().ok_or(LaunchUndecided)?,
            spec: self.spec.clone().ok_or(LaunchUndecided)?,
            now_ms: self.node.now_ms,
        };
        let checks = LaunchChecksOperation::new(check);
        self.call(AdmitStep::Checks, IoCall::child(checks))
    }

    fn checked(&mut self, capability: ExecutorCapability) -> AdmitNext {
        let spec = self.spec.as_ref().ok_or(LaunchUndecided)?;
        let Some(limits) = self.node.limits else {
            return Ok(Next::Done(LaunchAnswer::declined(LaunchDecline::Draining)));
        };
        if !limits.fits(&spec.resources) {
            return Ok(Next::Done(LaunchAnswer::declined(LaunchDecline::Capacity)));
        }
        let read = ReadInfoOperation::new(vec![self.node.local]);
        self.call(
            AdmitStep::Membership(Box::new(capability)),
            IoCall::child(read),
        )
    }

    /// Mints the local execution identity and asks for the receipt that authorizes it, leaving
    /// the reservation transaction as the only thing still to commit.
    fn sign(&mut self, capability: &ExecutorCapability, generation: u64) -> AdmitNext {
        let node = &self.node;
        let spec = self.spec.as_ref().ok_or(LaunchUndecided)?;
        let intent = self.intent()?;
        let planned = self.planned.as_ref().ok_or(LaunchUndecided)?;
        let limits = node.limits.ok_or(LaunchUndecided)?;
        let declined = |reason| Ok(Next::Done(LaunchAnswer::declined(reason)));
        let subject = node.execution_id.to_bytes();
        let Ok(job_id) = mint_configured_job(self.config()?, node.local, &subject) else {
            return declined(LaunchDecline::Draining);
        };
        let record = match materialize_local(spec, planned, job_id, node.local, node.now_ms) {
            Ok(record) => record,
            Err(reason) => return declined(reason),
        };
        let Ok(launch_digest) = JobRecordBody::digest(intent) else {
            return declined(LaunchDecline::Unauthorized);
        };
        let receipt = ExecutionReceipt {
            execution_id: node.execution_id,
            physical_job_id: job_id,
            launch_id: intent.launch_id,
            launch_digest,
            submission_id: spec.submission_id,
            request_digest: spec.request_digest,
            job_id: spec.job_id,
            executor_node_id: node.local,
            target: intent.target.clone(),
            spec_digest: spec.spec_digest,
            membership_generation: generation,
            subject_generation: capability.subject.generation,
            subject_digest: capability.subject_digest,
            accepted_at_ms: node.now_ms,
        };
        let claim = ExecutionClaim {
            realm_id: node.realm_id,
            record: JobFamilyRecord::Receipt(Box::new(receipt)),
            publisher: node.local,
        };
        let prepared = PreparedReceipt {
            job_id,
            subject_generation: capability.subject.generation,
            subject_digest: capability.subject_digest,
            record,
        };
        let effect = Effect::Net(NetEffect::ExecutionSign(Box::new(claim)));
        self.call(
            AdmitStep::Sign(Box::new(prepared)),
            IoCall::Effect(Box::new(effect)),
        )
    }

    fn commit(&mut self, prepared: PreparedReceipt, signed: IoResult) -> AdmitNext {
        let expected = "Event::Net(NetEvent::ExecutionSign)";
        let IoResult::Event(event) = signed else {
            let got = signed.label();
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let envelope = match *event {
            Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Signed(envelope))) => envelope,
            Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Unavailable(_))) => {
                return Ok(Next::Done(LaunchAnswer::declined(
                    LaunchDecline::Unauthorized,
                )));
            }
            _ => {
                let got = "another event";
                return Err(IoError::Unexpected { expected, got }.into());
            }
        };
        let Ok(frame) = JobRecordFrame::new(*envelope) else {
            return Ok(Next::Done(LaunchAnswer::declined(
                LaunchDecline::Unauthorized,
            )));
        };
        let node = &self.node;
        let spec = self.spec.as_ref().ok_or(LaunchUndecided)?;
        let intent = self.intent()?.clone();
        let config = ReserveExecutionConfig {
            realm_id: node.realm_id,
            local_node_id: node.local,
            envelope: node.limits.ok_or(LaunchUndecided)?,
            receipt: frame,
            launch: Box::new(intent.clone()),
            job_id: prepared.job_id,
            logical_job_id: spec.job_id,
            execution_id: node.execution_id,
            resources: spec.resources,
            subject_generation: prepared.subject_generation,
            subject_digest: prepared.subject_digest,
            record: Box::new(prepared.record),
            now_ms: node.now_ms,
        };
        let commit = CommitReceiptOperation::new(config, intent);
        self.call(AdmitStep::Commit, IoCall::child(commit))
    }
}

impl Operation for AdmitLaunchOperation {
    type Output = LaunchAnswer;
    type Error = LaunchUndecided;

    fn first(&mut self) -> AdmitNext {
        let read = GetConfigOperation::new(self.node.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> AdmitNext {
        match std::mem::replace(&mut self.step, AdmitStep::Commit) {
            AdmitStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                self.configured(config.map_err(|_| LaunchUndecided)?)
            }
            AdmitStep::Records => match spec_of(&family_read(result)?, self.intent()?) {
                Some(spec) => self.read_inputs(spec),
                None => self.pull(AdmitStep::Pull),
            },
            AdmitStep::Pull => {
                let _ = result.child::<(), IoError>()?;
                self.read_records(AdmitStep::RecordsAfterPull)
            }
            AdmitStep::RecordsAfterPull => {
                let spec = spec_of(&family_read(result)?, self.intent()?);
                self.read_inputs(spec.ok_or(LaunchUndecided)?)
            }
            AdmitStep::SpecInputs => {
                let spec = result.child::<LogicalJobSpec, PageReadError>()?;
                let spec = spec.map_err(|_| LaunchUndecided)?;
                let node = Some((self.node.local, self.node.realm_id));
                let read = PlannedInputsOperation::new(self.intent()?, node);
                self.call(
                    AdmitStep::PlannedInputs(Box::new(spec)),
                    IoCall::child(read),
                )
            }
            AdmitStep::PlannedInputs(spec) => {
                let inputs = result.child::<Vec<PlannedInput>, PageReadError>()?;
                let mut planned = self.intent()?.clone();
                planned.inputs = inputs.map_err(|_| LaunchUndecided)?;
                planned.input_pages = None;
                self.spec = Some(*spec);
                self.planned = Some(planned);
                self.append_launch(false)
            }
            AdmitStep::Append { retried } => {
                let appended = result.child::<AppendOutcome, RecordStoreError>()?;
                let kept = appended.is_ok_and(|outcome| {
                    matches!(
                        outcome.admission,
                        Admission::Authentic | Admission::Duplicate
                    )
                });
                match (kept, retried) {
                    (true, _) => self.read_records(AdmitStep::Decide),
                    // Missing family evidence is fetched before the offer stays undecided.
                    (false, false) => self.pull(AdmitStep::PullBeforeAppend),
                    (false, true) => Err(LaunchUndecided),
                }
            }
            AdmitStep::PullBeforeAppend => {
                let _ = result.child::<(), IoError>()?;
                self.append_launch(true)
            }
            AdmitStep::Decide => self.decide(family_read(result)?),
            AdmitStep::Checks => match result.child::<Verdict, CheckError>()? {
                Ok(Ok(capability)) => self.checked(capability),
                Ok(Err(reason)) => Ok(Next::Done(LaunchAnswer::declined(reason))),
                Err(_) => Err(LaunchUndecided),
            },
            AdmitStep::Membership(capability) => {
                let info = result.child::<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError>()?;
                let generation = info
                    .ok()
                    .and_then(|mut documents| documents.remove(&self.node.local))
                    .map(|document| document.epoch.membership_generation)
                    .unwrap_or_default();
                self.sign(&capability, generation)
            }
            AdmitStep::Sign(config) => self.commit(*config, result),
            AdmitStep::Commit => {
                let answer = result.child::<LaunchAnswer, LaunchUndecided>()??;
                if answer.decision.is_ok() {
                    info!(
                        job_id = %self.spec.as_ref().ok_or(LaunchUndecided)?.job_id,
                        execution_id = %self.node.execution_id,
                        "Target admitted a launch and stored its receipt"
                    );
                }
                Ok(Next::Done(answer))
            }
            AdmitStep::Replay => {
                let answer = result.child::<LaunchAnswer, LaunchUndecided>()??;
                Ok(Next::Done(answer))
            }
        }
    }
}

fn materialize_local(
    spec: &LogicalJobSpec,
    intent: &LaunchIntent,
    job_id: aruna_core::structs::execution::job::JobId,
    local: NodeId,
    now_ms: u64,
) -> Result<JobRecord, LaunchDecline> {
    let mut payload = spec.payload.clone();
    for input in &mut payload.inputs {
        let Some(pin) = intent
            .inputs
            .iter()
            .find(|pin| pin.destination_key == input.dest_key)
        else {
            return Err(LaunchDecline::Unauthorized);
        };
        let InputSource::S3 { version_id, .. } = &mut input.source;
        *version_id = Some(pin.version_id.to_string());
        // The plan picked which holder the bytes come from; no source means the
        // target already holds the compliant copy.
        input.source_node_id = pin.source_node_id.or(Some(local));
    }
    payload.resources.cpu_cores = Some(spec.resources.cpu_cores);
    payload.resources.ram_bytes = Some(spec.resources.ram_bytes);
    // A stored disk of zero is the absence of a request, not a zero-byte
    // ceiling: storing it as `Some(0)` is a spec every backend refuses.
    payload.resources.disk_bytes =
        (spec.resources.disk_bytes > 0).then_some(spec.resources.disk_bytes);
    payload.resources.max_walltime_ms = Some(spec.resources.max_walltime_ms);
    payload.resources.preemptible = spec.resources.preemptible;
    payload.executor_constraint = Some(intent.target.executor_kind.clone());
    let (mode, bucket) = workspace_of(&spec.payload);
    let (physical_mode, physical_bucket) = match mode {
        WorkspaceMode::Existing if local != spec.ingress_node_id => {
            return Err(LaunchDecline::Unauthorized);
        }
        WorkspaceMode::Existing => (mode, bucket),
        WorkspaceMode::None => (mode, None),
    };
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(payload),
        spec.created_by,
        local,
        now_ms,
        now_ms,
        None,
    );
    record.retention_ms = spec.retention_ms;
    record.workspace_mode = physical_mode;
    record.workspace_bucket = physical_bucket;
    record.captured_inputs = spec.captured_inputs.clone();
    Ok(record)
}

/// The stored spec the launch names, by its exact digest.
fn spec_of(records: &[JobRecordEnvelope], intent: &LaunchIntent) -> Option<LogicalJobSpec> {
    records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Spec(spec) if spec.spec_digest == intent.spec_digest => {
            Some(spec.as_ref().clone())
        }
        _ => None,
    })
}

/// The receipt this target already issued for the same launch id. A different
/// launch digest under one id is a conflict, never a second acceptance.
pub(crate) fn existing_receipt(
    records: &[JobRecordEnvelope],
    intent: &LaunchIntent,
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    let digest = aruna_core::structs::execution::job::JobRecordBody::digest(intent).ok()?;
    records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Receipt(receipt) if receipt.launch_id == intent.launch_id => {
            match receipt.launch_digest == digest {
                true => Some(
                    ReceiptFrame::new(envelope.clone()).map_err(|_| LaunchDecline::LaunchConflict),
                ),
                false => Some(Err(LaunchDecline::LaunchConflict)),
            }
        }
        _ => None,
    })
}

/// An execution of the same family this node already accepted. A second launch
/// is refused while that execution may finish, after success, and after permanent
/// failure, so one family never runs twice here. The refusal is retryable.
pub(crate) fn already_running(
    family: JobFamilyId,
    records: &[JobRecordEnvelope],
    local: NodeId,
) -> bool {
    let Ok(Some(projection)) = reduce_family(family, records) else {
        return false;
    };
    projection.executions.iter().any(|execution| {
        execution.executor_node_id == local
            && (!execution.state.is_terminal()
                || matches!(
                    execution.state,
                    PhysicalExecutionState::Succeeded | PhysicalExecutionState::Failed
                ))
    })
}

fn cancelled(family: JobFamilyId, records: &[JobRecordEnvelope]) -> bool {
    records.iter().any(|envelope| {
        matches!(&envelope.record, JobFamilyRecord::Cancel(_)) && envelope.family() == family
    })
}

#[cfg(test)]
mod tests {
    use crate::jobs::lifecycle::ids;
    use aruna_core::scheduling::PlannedInput;
    use aruna_core::structs::execution::job::{InputMode, InputSelection, JobId, WorkspaceOutput};

    use super::*;
    use crate::tests::records::{Family, REALM};
    use aruna_core::structs::placement::policy::PlacementSubject;

    fn limits() -> ResourceEnvelope {
        ResourceEnvelope {
            max_cpu_cores: Some(8),
            max_ram_bytes: Some(8_192),
            max_disk_bytes: Some(16_384),
            max_concurrent: Some(4),
        }
    }

    /// An offer whose checks have passed up to the point under test.
    fn prepared(family: &Family, limits: Option<ResourceEnvelope>) -> AdmitLaunchOperation {
        let spec = family.spec();
        let intent = family.launch(&spec, family.holder.public(), 0);
        let record = JobFamilyRecord::Launch(Box::new(intent.clone()));
        let frame = LaunchFrame::new(family.sign(&family.holder, record)).expect("bounded");
        let node = LaunchNode {
            local: family.target.public(),
            realm_id: REALM,
            now_ms: 4_000,
            execution_id: Ulid::from_parts(9, 9),
            limits,
        };
        let mut config = family.config.clone();
        config.seed_job_control(family.target.public(), 0);
        let mut operation = AdmitLaunchOperation::new(frame, node);
        operation.config = Some(config);
        operation.planned = Some(intent.clone());
        operation.intent = Some(intent);
        operation.spec = Some(spec);
        operation
    }

    fn capability(family: &Family) -> ExecutorCapability {
        let subject = PlacementSubject {
            node_id: family.target.public(),
            generation: 1,
            location: "eu".to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        };
        ExecutorCapability::new(family.target_id().executor_kind, subject).expect("subject")
    }

    fn declined(reason: LaunchDecline) -> AdmitNext {
        Ok(Next::Done(LaunchAnswer::declined(reason)))
    }

    // A cancelled family is declined, and one this node already ran stays refused.
    #[test]
    fn decide_refuses_family() {
        let family = Family::new([8; 32]);
        let cancel = JobFamilyRecord::Cancel(family.cancel(&family.spec()));
        let cancel = family.sign(&family.holder, cancel);
        let mut operation = prepared(&family, Some(limits()));
        assert_eq!(
            operation.decide(vec![cancel]),
            declined(LaunchDecline::Cancelled)
        );
    }

    // A replayed offer answers with the stored receipt instead of reserving again.
    #[test]
    fn replay_settles_receipt() {
        let family = Family::new([8; 32]);
        let mut operation = prepared(&family, Some(limits()));
        let intent = operation.intent.clone().expect("intent");
        let mut receipt = family.receipt(&intent, 1);
        receipt.launch_digest = JobRecordBody::digest(&intent).expect("digest");
        let record = JobFamilyRecord::Receipt(Box::new(receipt));
        let stored = family.sign(&family.target, record);
        assert!(matches!(
            operation.decide(vec![stored]),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(operation.step, AdmitStep::Replay);
    }

    // An executor kind not offered here drains, and a job over its limits has no capacity.
    #[test]
    fn limits_decide_fit() {
        let family = Family::new([8; 32]);
        let capability = capability(&family);
        let mut absent = prepared(&family, None);
        assert_eq!(
            absent.checked(capability.clone()),
            declined(LaunchDecline::Draining)
        );
        let small = ResourceEnvelope {
            max_cpu_cores: Some(0),
            ..limits()
        };
        let mut full = prepared(&family, Some(small));
        assert_eq!(full.checked(capability), declined(LaunchDecline::Capacity));
    }

    // The receipt names the minted run and the membership read, and is signed by this node.
    #[test]
    fn sign_builds_receipt() {
        let family = Family::new([8; 32]);
        let mut operation = prepared(&family, Some(limits()));
        let next = operation.sign(&capability(&family), 7);
        let Ok(Next::Call(IoCall::Effect(effect))) = next else {
            panic!("the receipt is signed: {next:?}");
        };
        let Effect::Net(NetEffect::ExecutionSign(claim)) = *effect else {
            panic!("expected an execution claim");
        };
        assert_eq!(claim.publisher, family.target.public());
        let JobFamilyRecord::Receipt(receipt) = claim.record else {
            panic!("expected a receipt");
        };
        assert_eq!(receipt.membership_generation, 7);
        assert_eq!(receipt.execution_id, Ulid::from_parts(9, 9));
        assert_eq!(receipt.accepted_at_ms, 4_000);
    }

    // A node without a job control binding cannot admit and answers as draining.
    #[test]
    fn unbound_node_drains() {
        let family = Family::new([8; 32]);
        let mut operation = prepared(&family, Some(limits()));
        operation.config = Some(family.config.clone());
        assert_eq!(
            operation.sign(&capability(&family), 0),
            declined(LaunchDecline::Draining)
        );
    }

    // A refused signature ends the offer; a signed receipt goes on to the commit.
    #[test]
    fn signature_decides_commit() {
        let family = Family::new([8; 32]);
        let signed = |event| IoResult::Event(Box::new(Event::Net(NetEvent::ExecutionSign(event))));
        let answer = |event| {
            let mut operation = prepared(&family, Some(limits()));
            let next = operation.sign(&capability(&family), 0);
            assert!(next.is_ok());
            let AdmitStep::Sign(prepared) =
                std::mem::replace(&mut operation.step, AdmitStep::Commit)
            else {
                panic!("the step waits for the signature");
            };
            operation.commit(*prepared, signed(event))
        };
        let refused = ExecutionSignEvent::Unavailable("no key".to_string());
        assert_eq!(answer(refused), declined(LaunchDecline::Unauthorized));
        let intent = family.launch(&family.spec(), family.holder.public(), 0);
        let receipt = JobFamilyRecord::Receipt(Box::new(family.receipt(&intent, 1)));
        let envelope = family.sign(&family.target, receipt);
        let good = ExecutionSignEvent::Signed(Box::new(envelope));
        assert!(matches!(answer(good), Ok(Next::Call(IoCall::Child(_)))));
    }

    #[test]
    fn materializes_captured_inputs() {
        // The physical row must execute the exact input, resources, backend, and retention stored.
        let family = Family::new([8u8; 32]);
        let mut spec = family.spec();
        spec.payload.inputs.push(InputSelection {
            source: InputSource::S3 {
                bucket: "source".to_string(),
                key: "reads.fastq".to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: "reads.fastq".to_string(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        });
        let mut launch = family.launch(&spec, family.holder.public(), 0);
        let version_id = Ulid::from_bytes([11u8; 16]);
        launch.inputs.push(PlannedInput {
            destination_key: "reads.fastq".to_string(),
            version_id,
            content: aruna_core::structs::execution::job::InputContent::Blake3([12u8; 32]),
            bytes: 3,
            policies: Vec::new(),
            source_node_id: None,
            transfer_ms: 0,
            known_link: true,
        });

        let record = materialize_local(
            &spec,
            &launch,
            JobId::from_bytes([10u8; 16]),
            family.target.public(),
            4_000,
        )
        .expect("launch materializes");
        let JobPayload::Execution(payload) = record.payload else {
            panic!("expected execution payload");
        };
        let InputSource::S3 {
            version_id: pinned, ..
        } = &payload.inputs[0].source;
        let expected_version = version_id.to_string();
        assert_eq!(pinned.as_deref(), Some(expected_version.as_str()));
        assert_eq!(payload.resources.cpu_cores, Some(spec.resources.cpu_cores));
        assert_eq!(payload.resources.ram_bytes, Some(spec.resources.ram_bytes));
        assert_eq!(
            payload.resources.disk_bytes,
            Some(spec.resources.disk_bytes)
        );
        assert_eq!(payload.executor_constraint, Some("docker".to_string()));
        assert_eq!(record.retention_ms, spec.retention_ms);
    }

    #[test]
    fn disk_stays_none() {
        // A request without a disk ceiling stores zero, which no backend accepts
        // as a container limit.
        let family = Family::new([8u8; 32]);
        let mut spec = family.spec();
        spec.resources.disk_bytes = 0;
        let launch = family.launch(&spec, family.holder.public(), 0);

        let record = materialize_local(
            &spec,
            &launch,
            JobId::from_bytes([10u8; 16]),
            family.target.public(),
            4_000,
        )
        .expect("launch materializes");
        let JobPayload::Execution(payload) = record.payload else {
            panic!("expected execution payload");
        };
        assert_eq!(payload.resources.disk_bytes, None);
    }

    #[test]
    fn remote_existing_rejected() {
        // Existing workspace contents are node-local until whole-bucket staging exists.
        let family = Family::new([8u8; 32]);
        let mut spec = family.spec();
        let ingress = family.holder.public();
        spec.ingress_node_id = ingress;
        spec.payload.workspace_outputs.push(WorkspaceOutput {
            container_path: "/out/result.txt".to_string(),
            dest_key: "results/result.txt".to_string(),
        });
        ids::store_workspace(
            &mut spec.payload,
            WorkspaceMode::Existing,
            Some("existing".to_string()),
        )
        .expect("workspace stored");
        spec.payload.resolve_outputs("existing", ingress);
        let launch = family.launch(&spec, family.holder.public(), 0);
        let physical_id = JobId::from_bytes([10u8; 16]);

        assert_eq!(
            materialize_local(&spec, &launch, physical_id, family.target.public(), 4_000),
            Err(LaunchDecline::Unauthorized)
        );
    }
}
