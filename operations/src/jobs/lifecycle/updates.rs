//! Publishes the monotonic execution chain, each update chained by digest from the receipt.
//! Only the fenced node may advance a chain, and success may name only a durable output record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::effects::{Effect, JobRecordFrame, NetEffect};
use aruna_core::events::{Event, ExecutionSignEvent, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::JOB_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::execution::job::{
    ExecutionClaim, ExecutionUpdate, JobErrorKind, JobFamilyRecord, JobRecord, JobRecordBody,
    JobRecordEnvelope, JobRecordKind, JobResultPayload, JobState, PhysicalExecutionResult,
    PhysicalExecutionState, ResultMessage, job_record_key,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskKey};
use tracing::{debug, warn};
use ulid::Ulid;

use super::LifecycleError;
use super::chain::{ExecutionChain, ExecutionChainOperation, receipt_in};
use super::held::HeldReservations;
use super::outbox::schedule_outbox_drain;
use super::reservation::ReleaseExecutionOperation;
use super::witness_arm::ArmFamilyOperation;
use crate::jobs::records::RecordStoreError;
use crate::jobs::records::admit::Admission;
use crate::jobs::records::append::{
    AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin,
};
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};

/// Spacing between retries of a terminal publication that stayed pending.
pub const SETTLE_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Asks the settle task to retry the terminal obligations still held here.
pub fn schedule_terminal_settle(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::SettleJobTerminals,
        after,
    })
}

/// The next sequence and predecessor digest of a chain proven contiguous from
/// `root`. `None` is a gap, a duplicate sequence, or a broken predecessor: the
/// chain cannot be extended without forging one of them.
fn chain_tip(updates: &[&ExecutionUpdate], root: [u8; 32]) -> Option<(u64, [u8; 32])> {
    let mut ordered: Vec<&ExecutionUpdate> = updates.to_vec();
    ordered.sort_by_key(|update| update.sequence);
    let mut previous = root;
    for (index, update) in ordered.iter().enumerate() {
        if update.sequence != index as u64 || update.previous_digest != previous {
            return None;
        }
        previous = update.digest().ok()?;
    }
    let next = ordered
        .last()
        .map_or(0, |update| update.sequence.saturating_add(1));
    Some((next, previous))
}

/// This node and the realm it serves, or `None` without a net transport.
type Serving = Option<(NodeId, RealmId)>;

#[derive(Debug, PartialEq)]
enum PublishState {
    Receipts,
    Updates,
    Sign,
    Append,
}

type PublishStep = Result<Next<bool>, LifecycleError>;

/// Publishes one state of the local execution. The sequence and the previous
/// digest are derived from the records already stored, so a replay of the same
/// state is idempotent and a lost publication never breaks the chain. Only the
/// receipted executor publishes; it signs the update through `NetEffect::ExecutionSign`.
#[derive(Debug, PartialEq)]
pub struct PublishStateOperation {
    chain: ExecutionChain,
    state: PhysicalExecutionState,
    result: Option<PhysicalExecutionResult>,
    observed_at_ms: u64,
    serving: Serving,
    now_ms: u64,
    root: [u8; 32],
    stage: PublishState,
}

impl PublishStateOperation {
    pub fn new(
        chain: ExecutionChain,
        (state, result, observed_at_ms): (
            PhysicalExecutionState,
            Option<PhysicalExecutionResult>,
            u64,
        ),
        serving: Serving,
        now_ms: u64,
    ) -> Self {
        Self {
            chain,
            state,
            result,
            observed_at_ms,
            serving,
            now_ms,
            root: [0; 32],
            stage: PublishState::Receipts,
        }
    }

    fn pending(&self, reason: &'static str) -> PublishStep {
        let chain = &self.chain;
        warn!(job_id = %chain.job_id, execution_id = %chain.execution_id, "{reason}");
        Ok(Next::Done(false))
    }

    /// The chain is extended only from evidence proven complete: the receipt
    /// that authorized it, plus every stored update of this execution.
    fn receipts(&mut self, result: IoResult) -> PublishStep {
        let records = result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()?;
        let records = records
            .inspect_err(|error| warn!(error = %error, "Execution receipt read is incomplete"))
            .ok();
        let receipt = records.and_then(|records| receipt_in(&records, self.chain.execution_id));
        let Some(receipt) = receipt else {
            return self.pending("Execution receipt is no longer readable; publication deferred");
        };
        let Ok(root) = receipt.digest() else {
            return self.pending("Execution receipt digest failed");
        };
        self.root = root;
        self.stage = PublishState::Updates;
        let updates = LoadFamilyOperation::kind(self.chain.family, JobRecordKind::Update);
        Ok(Next::Call(IoCall::child(updates)))
    }

    /// Asks this node to sign the next update of the chain, unless the state is stored already.
    fn sign(&mut self, result: IoResult, (local, realm_id): (NodeId, RealmId)) -> PublishStep {
        let records = match result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()? {
            Ok(records) => records,
            Err(error) => {
                warn!(error = %error, "Execution update read is incomplete; publication deferred");
                return Ok(Next::Done(false));
            }
        };
        let chain = self.chain;
        let mine: Vec<&ExecutionUpdate> = records
            .iter()
            .filter_map(|envelope| match &envelope.record {
                JobFamilyRecord::Update(update) if update.execution_id == chain.execution_id => {
                    Some(update.as_ref())
                }
                _ => None,
            })
            .collect();
        if mine.iter().any(|update| update.state == self.state) {
            return Ok(Next::Done(true));
        }
        let Some((sequence, previous)) = chain_tip(&mine, self.root) else {
            return self.pending("Execution update chain is not contiguous");
        };
        let update = ExecutionUpdate {
            execution_id: chain.execution_id,
            submission_id: chain.family.submission_id,
            request_digest: chain.family.request_digest,
            executor_node_id: local,
            sequence,
            previous_digest: previous,
            state: self.state,
            observed_at_ms: self.observed_at_ms,
            result: self.result.take(),
        };
        self.stage = PublishState::Sign;
        let claim = ExecutionClaim {
            realm_id,
            record: JobFamilyRecord::Update(Box::new(update)),
            publisher: local,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Net(
            NetEffect::ExecutionSign(Box::new(claim)),
        )))))
    }

    fn append(&mut self, result: IoResult, (local, realm_id): (NodeId, RealmId)) -> PublishStep {
        let IoResult::Event(event) = result else {
            return Err(IoError::Unexpected {
                expected: "Event::Net(NetEvent::ExecutionSign)",
                got: result.label(),
            }
            .into());
        };
        let envelope = match *event {
            Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Signed(envelope))) => envelope,
            Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Unavailable(reason))) => {
                warn!(reason = %reason, "Execution update signing failed");
                return Ok(Next::Done(false));
            }
            other => {
                return Err(IoError::Unexpected {
                    expected: "Event::Net(NetEvent::ExecutionSign)",
                    got: other.label(),
                }
                .into());
            }
        };
        let Ok(frame) = JobRecordFrame::new(*envelope) else {
            return self.pending("Execution update exceeds the record bounds");
        };
        self.stage = PublishState::Append;
        Ok(Next::Call(IoCall::child(AppendRecordOperation::new(
            AppendRecordConfig {
                realm_id,
                local_node_id: local,
                record: frame,
                local: None,
                origin: RecordOrigin::Local,
                now_ms: self.now_ms,
            },
        ))))
    }

    fn appended(&mut self, result: IoResult, (local, realm_id): (NodeId, RealmId)) -> PublishStep {
        let chain = self.chain;
        let outcome = match result.child::<AppendOutcome, RecordStoreError>()? {
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(
                    job_id = %chain.job_id,
                    execution_id = %chain.execution_id,
                    error = %error,
                    "Execution update append failed"
                );
                return Ok(Next::Done(false));
            }
        };
        if !matches!(
            outcome.admission,
            Admission::Authentic | Admission::Duplicate
        ) {
            warn!(
                job_id = %chain.job_id,
                execution_id = %chain.execution_id,
                admission = ?outcome.admission,
                "Execution update was not admitted"
            );
            return Ok(Next::Done(false));
        }
        debug!(state = self.state.name(), "Execution update published");
        let mut calls = Vec::new();
        // An infrastructure error ends this execution without deciding the
        // job, so the family must be planned again from here too.
        if self.state == PhysicalExecutionState::Error {
            let arm = ArmFamilyOperation::new(chain.family, local, realm_id, self.now_ms);
            calls.push(IoCall::child(arm));
        }
        calls.push(IoCall::Effect(Box::new(schedule_outbox_drain(
            Duration::ZERO,
        ))));
        // A duplicate is a retry of a stored state; only a new record is a change.
        if matches!(outcome.admission, Admission::Authentic) {
            let changed = Effect::Net(NetEffect::DashboardChanged);
            calls.push(IoCall::Effect(Box::new(changed)));
        }
        Ok(Next::Then {
            output: true,
            calls,
        })
    }
}

impl Operation for PublishStateOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn first(&mut self) -> PublishStep {
        let Some((local, _)) = self.serving else {
            return self.pending("Execution update needs a net handle; publication abandoned");
        };
        if self.chain.executor_node_id != local {
            return self.pending("Only the receipted executor may publish this execution");
        }
        let receipts = LoadFamilyOperation::kind(self.chain.family, JobRecordKind::Receipt);
        Ok(Next::Call(IoCall::child(receipts)))
    }

    fn next(&mut self, result: IoResult) -> PublishStep {
        let Some(serving) = self.serving else {
            return Ok(Next::Done(false));
        };
        match self.stage {
            PublishState::Receipts => self.receipts(result),
            PublishState::Updates => self.sign(result, serving),
            PublishState::Sign => self.append(result, serving),
            PublishState::Append => self.appended(result, serving),
        }
    }
}

#[derive(Debug, PartialEq)]
enum TerminalState {
    Held,
    Chain,
    Publish(Ulid),
    Release,
}

/// Publishes the terminal state of a receipted execution and releases its
/// reservation. Success names the digest of the output record stored before it,
/// so a success can never be projected without its exact outputs.
#[derive(Debug, PartialEq)]
pub struct PublishTerminalOperation {
    record: JobRecord,
    serving: Serving,
    now_ms: u64,
    stage: TerminalState,
}

impl PublishTerminalOperation {
    pub fn new(record: JobRecord, serving: Serving, now_ms: u64) -> Self {
        Self {
            record,
            serving,
            now_ms,
            stage: TerminalState::Held,
        }
    }

    fn deferred(&self, reason: &'static str) -> PublishStep {
        warn!(job_id = %self.record.job_id, "{reason}");
        Ok(Next::Done(false))
    }

    fn chain(&mut self, result: IoResult) -> PublishStep {
        let held = match result.child::<Vec<JobReservationRecord>, LifecycleError>()? {
            Ok(held) => held,
            Err(error) => {
                warn!(
                    job_id = %self.record.job_id,
                    error = %error,
                    "Execution reservation read failed; terminal publication deferred"
                );
                return Ok(Next::Done(false));
            }
        };
        // No reservation is a purely local execution: it owes nothing here.
        let Some(reservation) = held.into_iter().next() else {
            return Ok(Next::Done(true));
        };
        self.stage = TerminalState::Chain;
        let chain = ExecutionChainOperation::of_execution(
            reservation.logical_job_id,
            reservation.execution_id,
        );
        Ok(Next::Call(IoCall::child(chain)))
    }

    fn publish(&mut self, result: IoResult, state: PhysicalExecutionState) -> PublishStep {
        let Ok(Some(chain)) = result.child::<Option<ExecutionChain>, LifecycleError>()? else {
            return self.deferred("Terminal publication deferred: the receipt chain is unresolved");
        };
        let payload = self.record.result.as_ref();
        let (stdout, stderr) = log_tails(payload);
        let result = PhysicalExecutionResult {
            exit_code: exit_code(payload),
            output_digest: output_digest(payload),
            message: self
                .record
                .last_error
                .as_ref()
                .and_then(|error| ResultMessage::new(error.message.clone()).ok()),
            stdout,
            stderr,
        };
        let observed_at_ms = self
            .record
            .finished_at_ms
            .unwrap_or(self.record.updated_at_ms);
        self.stage = TerminalState::Publish(chain.execution_id);
        Ok(Next::Call(IoCall::child(PublishStateOperation::new(
            chain,
            (state, Some(result), observed_at_ms),
            self.serving,
            self.now_ms,
        ))))
    }
}

impl Operation for PublishTerminalOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn first(&mut self) -> PublishStep {
        if terminal_state(&self.record).is_none() {
            return Ok(Next::Done(true));
        }
        Ok(Next::Call(IoCall::child(HeldReservations::of_job(
            self.record.job_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> PublishStep {
        let Some(state) = terminal_state(&self.record) else {
            return Ok(Next::Done(true));
        };
        match self.stage {
            TerminalState::Held => self.chain(result),
            TerminalState::Chain => self.publish(result, state),
            TerminalState::Publish(execution_id) => {
                if !result.child::<bool, LifecycleError>()?.unwrap_or(false) {
                    return Ok(Next::Done(false));
                }
                self.stage = TerminalState::Release;
                let release = ReleaseExecutionOperation::new(execution_id);
                Ok(Next::Call(IoCall::child(release)))
            }
            TerminalState::Release => {
                if let Err(error) = result.child::<bool, LifecycleError>()? {
                    warn!(error = %error, "Execution reservation release failed");
                    return Ok(Next::Done(false));
                }
                Ok(Next::Done(true))
            }
        }
    }
}

/// Retries terminal publication and capacity release for every durable local
/// terminal execution that still has a reservation. `true` means at least one obligation
/// stayed pending, so the caller must run it again.
#[derive(Debug, PartialEq)]
pub struct SettleTerminalsOperation {
    serving: Serving,
    now_ms: u64,
    held: VecDeque<JobReservationRecord>,
    current: Option<JobReservationRecord>,
    /// Whether an obligation stayed pending, so the settle must run again.
    pending: bool,
    publishing: bool,
}

impl SettleTerminalsOperation {
    pub fn new(serving: Serving, now_ms: u64) -> Self {
        Self {
            serving,
            now_ms,
            held: VecDeque::new(),
            current: None,
            pending: false,
            publishing: false,
        }
    }

    /// Reads the job of the next held reservation, or ends once none is left.
    fn read_next(&mut self) -> PublishStep {
        let Some(reservation) = self.held.pop_front() else {
            return Ok(Next::Done(self.pending));
        };
        let row = (JOB_KEYSPACE.to_string(), job_record_key(reservation.job_id));
        self.current = Some(reservation);
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn settle(&mut self, result: IoResult) -> PublishStep {
        let [value] = values::<1>(result)?;
        let record = value.and_then(|value| match JobRecord::from_bytes(&value) {
            Ok(record) => Some(record),
            Err(error) => {
                warn!(error = %error, "Malformed job record retained");
                None
            }
        });
        let Some(record) = record.filter(JobRecord::is_settled) else {
            return self.read_next();
        };
        self.publishing = true;
        let publish = PublishTerminalOperation::new(record, self.serving, self.now_ms);
        Ok(Next::Call(IoCall::child(publish)))
    }

    fn published(&mut self, result: IoResult) -> PublishStep {
        self.publishing = false;
        if !result.child::<bool, LifecycleError>()?.unwrap_or(false) {
            if let Some(reservation) = &self.current {
                warn!(
                    job_id = %reservation.job_id,
                    execution_id = %reservation.execution_id,
                    "Terminal obligation remains pending; the reservation stays held"
                );
            }
            self.pending = true;
        }
        self.read_next()
    }
}

impl Operation for SettleTerminalsOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn first(&mut self) -> PublishStep {
        Ok(Next::Call(IoCall::child(HeldReservations::all())))
    }

    fn next(&mut self, result: IoResult) -> PublishStep {
        if self.publishing {
            return self.published(result);
        }
        if self.current.is_some() {
            return self.settle(result);
        }
        self.held = result
            .child::<Vec<JobReservationRecord>, LifecycleError>()??
            .into();
        self.read_next()
    }
}

fn terminal_state(record: &JobRecord) -> Option<PhysicalExecutionState> {
    match record.state {
        JobState::Succeeded => Some(PhysicalExecutionState::Succeeded),
        JobState::Failed
            if record
                .last_error
                .as_ref()
                .is_some_and(|error| error.kind == JobErrorKind::Permanent) =>
        {
            Some(PhysicalExecutionState::Failed)
        }
        JobState::Failed => Some(PhysicalExecutionState::Error),
        JobState::Cancelled => Some(PhysicalExecutionState::Cancelled),
        // Local exhaustion is an error the family may still resolve elsewhere.
        JobState::Indeterminate if record.locally_exhausted => Some(PhysicalExecutionState::Error),
        _ => None,
    }
}

fn exit_code(result: Option<&JobResultPayload>) -> Option<i32> {
    match result {
        Some(JobResultPayload::Execution { exit_code, .. }) => *exit_code,
        _ => None,
    }
}

fn output_digest(result: Option<&JobResultPayload>) -> Option<[u8; 32]> {
    match result {
        Some(JobResultPayload::Execution { output_digest, .. }) => *output_digest,
        _ => None,
    }
}

/// Bounded tails the replicated result carries, so every node answers a status
/// read with the logs instead of only the node that ran the container.
fn log_tails(result: Option<&JobResultPayload>) -> (Option<ResultMessage>, Option<ResultMessage>) {
    match result {
        Some(JobResultPayload::Execution { stdout, stderr, .. }) => {
            (ResultMessage::tail(stdout), ResultMessage::tail(stderr))
        }
        _ => (None, None),
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::tests::records::{Family, node, payload, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::{
        EffectiveResources, ExecutionReceipt, JobError, JobId, JobPayload,
    };
    use aruna_core::types::Value;

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn chain_of(family: &Family, receipt: &ExecutionReceipt) -> ExecutionChain {
        ExecutionChain {
            family: family.family(),
            execution_id: receipt.execution_id,
            executor_node_id: receipt.executor_node_id,
            spec_digest: receipt.spec_digest,
            receipt_digest: receipt.digest().expect("receipt digest"),
            job_id: receipt.job_id,
        }
    }

    fn serving(family: &Family) -> Serving {
        Some((family.target.public(), family.config.realm_id))
    }

    fn publish(family: &Family, receipt: &ExecutionReceipt) -> PublishStateOperation {
        let state = (PhysicalExecutionState::Running, None, 5);
        PublishStateOperation::new(chain_of(family, receipt), state, serving(family), 9)
    }

    /// A publication that read its receipt and is about to read the updates.
    fn receipted(family: &Family, receipt: &ExecutionReceipt) -> PublishStateOperation {
        let mut operation = publish(family, receipt);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let record = JobFamilyRecord::Receipt(Box::new(receipt.clone()));
        let receipts = vec![family.sign(&family.target, record)];
        let read = operation.next(child::<_, FamilyReadError>(Ok(receipts)));
        assert!(matches!(read, Ok(Next::Call(IoCall::Child(_)))));
        operation
    }

    fn updates(records: Vec<JobRecordEnvelope>) -> IoResult {
        child::<_, FamilyReadError>(Ok(records))
    }

    fn reservation() -> JobReservationRecord {
        JobReservationRecord {
            execution_id: Ulid::from_bytes([7; 16]),
            job_id: JobId::from_bytes([4; 16]),
            logical_job_id: JobId::from_bytes([5; 16]),
            resources: EffectiveResources {
                cpu_cores: 1,
                ram_bytes: 0,
                disk_bytes: 0,
                max_walltime_ms: 1_000,
                preemptible: false,
            },
            created_at_ms: 10,
            subject_generation: 0,
            subject_digest: [0; 32],
        }
    }

    // The executor signs the next update of its chain, appends it, then wakes the outbox and,
    // for a new record, the dashboard.
    #[test]
    fn publish_signs_next() {
        let family = Family::new([5u8; 32]);
        let receipt = receipt(&family);
        let mut operation = receipted(&family, &receipt);
        let Ok(Next::Call(IoCall::Effect(effect))) = operation.next(updates(Vec::new())) else {
            panic!("the next update is signed");
        };
        let Effect::Net(NetEffect::ExecutionSign(claim)) = *effect else {
            panic!("the next update is signed");
        };
        assert_eq!(claim.publisher, family.target.public());
        let JobFamilyRecord::Update(update) = &claim.record else {
            panic!("an update is signed");
        };
        let root = receipt.digest().expect("receipt digest");
        assert_eq!((update.sequence, update.previous_digest), (0, root));
        let envelope = family.sign(&family.target, claim.record.clone());
        let signed = Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Signed(
            Box::new(envelope.clone()),
        )));
        let append = operation.next(IoResult::Event(Box::new(signed)));
        assert!(matches!(append, Ok(Next::Call(IoCall::Child(_)))));
        let outcome = AppendOutcome {
            key: envelope.key(),
            digest: envelope.digest().expect("update digest"),
            admission: Admission::Authentic,
            admitted: 1,
            deferred: false,
        };
        let Ok(Next::Then {
            output: true,
            calls,
        }) = operation.next(child::<_, RecordStoreError>(Ok(outcome)))
        else {
            panic!("an admitted update ends the publication");
        };
        let changed = IoCall::Effect(Box::new(Effect::Net(NetEffect::DashboardChanged)));
        assert_eq!(calls.len(), 2);
        assert_eq!(calls.last(), Some(&changed));
    }

    // A stored state needs nothing, and a refused signature leaves the publication pending.
    #[test]
    fn publish_stored_state() {
        let family = Family::new([5u8; 32]);
        let receipt = receipt(&family);
        let root = receipt.digest().expect("receipt digest");
        let running = family.update(&receipt, 0, root, PhysicalExecutionState::Running, None);
        let stored = family.sign(&family.target, JobFamilyRecord::Update(Box::new(running)));
        let mut operation = receipted(&family, &receipt);
        assert_eq!(operation.next(updates(vec![stored])), Ok(Next::Done(true)));

        let mut operation = receipted(&family, &receipt);
        operation.next(updates(Vec::new())).expect("sign call");
        let refused = Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Unavailable(
            "no key".to_string(),
        )));
        let answer = operation.next(IoResult::Event(Box::new(refused)));
        assert_eq!(answer, Ok(Next::Done(false)));
    }

    // Only the receipted executor of a node with a net transport publishes.
    #[test]
    fn publish_needs_executor() {
        let family = Family::new([5u8; 32]);
        let receipt = receipt(&family);
        let state = (PhysicalExecutionState::Running, None, 5);
        let mut detached = PublishStateOperation::new(chain_of(&family, &receipt), state, None, 9);
        assert_eq!(detached.first(), Ok(Next::Done(false)));
        let other = Some((family.holder.public(), family.config.realm_id));
        let state = (PhysicalExecutionState::Running, None, 5);
        let mut foreign = PublishStateOperation::new(chain_of(&family, &receipt), state, other, 9);
        assert_eq!(foreign.first(), Ok(Next::Done(false)));
    }

    // A terminal job publishes its state through its chain, then releases its reservation; a
    // job without one owes nothing and a running job publishes nothing.
    #[test]
    fn terminal_publishes_releases() {
        let family = Family::new([5u8; 32]);
        let receipt = receipt(&family);
        let mut record = terminal(JobError::permanent("exit 1"));
        let mut operation = PublishTerminalOperation::new(record.clone(), serving(&family), 9);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let held = child::<_, LifecycleError>(Ok(vec![reservation()]));
        assert!(matches!(
            operation.next(held),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let chain = child::<_, LifecycleError>(Ok(Some(chain_of(&family, &receipt))));
        assert!(matches!(
            operation.next(chain),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let published = child::<_, LifecycleError>(Ok(true));
        assert!(matches!(
            operation.next(published),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let released = child::<_, LifecycleError>(Ok(true));
        assert_eq!(operation.next(released), Ok(Next::Done(true)));

        let mut local = PublishTerminalOperation::new(record.clone(), serving(&family), 9);
        local.first().expect("reservation read");
        let none = child::<Vec<JobReservationRecord>, LifecycleError>(Ok(Vec::new()));
        assert_eq!(local.next(none), Ok(Next::Done(true)));

        record.state = JobState::Running;
        let mut running = PublishTerminalOperation::new(record, serving(&family), 9);
        assert_eq!(running.first(), Ok(Next::Done(true)));
    }

    // An unresolved chain or a failed publication keeps the reservation held.
    #[test]
    fn terminal_stays_pending() {
        let family = Family::new([5u8; 32]);
        let record = terminal(JobError::permanent("exit 1"));
        let mut operation = PublishTerminalOperation::new(record.clone(), serving(&family), 9);
        operation.first().expect("reservation read");
        operation
            .next(child::<_, LifecycleError>(Ok(vec![reservation()])))
            .expect("chain read");
        let unresolved = child::<Option<ExecutionChain>, LifecycleError>(Ok(None));
        assert_eq!(operation.next(unresolved), Ok(Next::Done(false)));
    }

    // Each held reservation's settled job is published; a pending one makes the settle run
    // again, and a reservation without a job is skipped.
    #[test]
    fn settle_reports_pending() {
        let record = terminal(JobError::permanent("exit 1"));
        let mut operation = SettleTerminalsOperation::new(None, 9);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let held = child::<_, LifecycleError>(Ok(vec![reservation(), reservation()]));
        assert!(matches!(
            operation.next(held),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let bytes = Value::from(record.to_bytes().expect("job row").as_slice());
        let read = operation.next(IoResult::Values(vec![Some(bytes)]));
        assert!(matches!(read, Ok(Next::Call(IoCall::Child(_)))));
        let pending = operation.next(child::<_, LifecycleError>(Ok(false)));
        assert!(matches!(pending, Ok(Next::Call(IoCall::Read(_)))));
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Done(true))
        );

        let mut empty = SettleTerminalsOperation::new(None, 9);
        empty.first().expect("reservation read");
        let none = child::<Vec<JobReservationRecord>, LifecycleError>(Ok(Vec::new()));
        assert_eq!(empty.next(none), Ok(Next::Done(false)));
    }

    fn receipt(family: &Family) -> ExecutionReceipt {
        let spec = family.spec();
        family.receipt(&family.launch(&spec, family.holder.public(), 0), 1)
    }

    fn terminal(error: JobError) -> JobRecord {
        let mut record = JobRecord::new(
            JobId::from_bytes([4u8; 16]),
            JobPayload::Execution(payload()),
            user(),
            node(1),
            1_000,
            1_000,
            None,
        );
        record.state = JobState::Failed;
        record.last_error = Some(error);
        record
    }

    #[test]
    fn retryable_maps_error() {
        // Only an authenticated permanent, job-specific failure may replicate as
        // `failed`; a retryable one stays an infrastructure `error`.
        assert_eq!(
            terminal_state(&terminal(JobError::permanent(
                "container exited with code 1"
            ))),
            Some(PhysicalExecutionState::Failed)
        );
        assert_eq!(
            terminal_state(&terminal(JobError::retryable(
                "backend infrastructure failure"
            ))),
            Some(PhysicalExecutionState::Error)
        );
    }

    #[test]
    fn extends_valid_chain() {
        let family = Family::new([3u8; 32]);
        let receipt = receipt(&family);
        let root = receipt.digest().expect("receipt digest");
        let first = family.update(&receipt, 0, root, PhysicalExecutionState::Running, None);
        let tip = first.digest().expect("update digest");

        assert_eq!(chain_tip(&[], root), Some((0, root)));
        assert_eq!(chain_tip(&[&first], root), Some((1, tip)));
    }

    #[test]
    fn rejects_sequence_gap() {
        // The next sequence must come from the proven chain, never from how many
        // updates happened to load.
        let family = Family::new([3u8; 32]);
        let receipt = receipt(&family);
        let root = receipt.digest().expect("receipt digest");
        let first = family.update(&receipt, 0, root, PhysicalExecutionState::Running, None);
        let third = family.update(
            &receipt,
            2,
            first.digest().expect("update digest"),
            PhysicalExecutionState::Succeeded,
            None,
        );

        assert_eq!(chain_tip(&[&first, &third], root), None);
    }

    #[test]
    fn rejects_duplicate_sequence() {
        let family = Family::new([3u8; 32]);
        let receipt = receipt(&family);
        let root = receipt.digest().expect("receipt digest");
        let first = family.update(&receipt, 0, root, PhysicalExecutionState::Running, None);
        let twin = family.update(&receipt, 0, root, PhysicalExecutionState::Cancelled, None);

        assert_eq!(chain_tip(&[&first, &twin], root), None);
    }

    #[test]
    fn rejects_broken_predecessor() {
        let family = Family::new([3u8; 32]);
        let receipt = receipt(&family);
        let root = receipt.digest().expect("receipt digest");
        let first = family.update(&receipt, 0, root, PhysicalExecutionState::Running, None);
        let second = family.update(
            &receipt,
            1,
            [9u8; 32],
            PhysicalExecutionState::Succeeded,
            None,
        );

        assert_eq!(chain_tip(&[&first, &second], root), None);
        assert_eq!(chain_tip(&[&first], [9u8; 32]), None);
    }
}
