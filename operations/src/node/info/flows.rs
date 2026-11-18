//! Runs the flows that seed, refresh and revise this node's info document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::compute::ExecutorCapability;
use aruna_core::compute::quota::{
    ComputeDemandSnapshot, ComputeDepartureReport, ComputeReservationSnapshot,
    MAX_UNRESOLVED_EXECUTIONS, ResourceTotals,
};
use aruna_core::events::Event;
use aruna_core::heartbeat::{HeldHeartbeat, NodeHeartbeat, RunningJobs};
use aruna_core::keyspaces::COMPUTE_DEPARTURE_KEYSPACE;
use aruna_core::node_facts::NodeFacts;
use aruna_core::node_state::{
    DEPARTURE_KEY, DrainState, DrainStateError, DrainStateOperation, NodeInfoError,
    ReadInfoOperation, SetDrainOperation,
};
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::rows::{done, encoded_row};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::node_info::{
    AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization,
};
use aruna_core::types::GroupId;
use tracing::{info, warn};
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::demand::{DemandError, DemandSnapshotOperation, merge_view};
use crate::node::heartbeat::{HeartbeatError, SendHeartbeatOperation};
use crate::node::info::inputs::{
    InfoFlowError, RevisionInputs, RevisionInputsOperation, Telemetry, TelemetryOperation,
    advertises_executors, child_answer,
};
use crate::node::info::publish::{PublishInfoError, PublishInfoOperation};
use crate::node::info::write::{InfoChange, InfoWriteError, InfoWriteOperation};
use crate::placement::policy::reconcile::SubjectSync;
use crate::placement::policy::{
    SubjectScanError, SubjectScanMode, SubjectScanResult, SubjectSyncOperation, SubjectTrigger,
};

/// The runner's clock: the flow's start for its snapshots, the latest time for its revision.
#[derive(Debug, Default, PartialEq)]
struct FlowClock {
    started_ms: Option<u64>,
    latest_ms: u64,
}

impl FlowClock {
    fn stamp(&mut self, now_ms: u64) {
        self.started_ms.get_or_insert(now_ms);
        self.latest_ms = now_ms;
    }

    fn started(&self) -> u64 {
        self.started_ms.unwrap_or(self.latest_ms)
    }
}

fn unfinished() -> InfoFlowError {
    IoError::Unfinished.into()
}

fn config_call(realm_id: RealmId) -> IoCall {
    IoCall::child(GetConfigOperation::new(realm_id))
}

fn config_answer(result: IoResult) -> Result<RealmConfigDocument, InfoFlowError> {
    (result.child::<RealmConfigDocument, GetConfigError>()?).map_err(|error| {
        InfoFlowError::Step(format!(
            "failed to read realm config for node info: {error}"
        ))
    })
}

fn row_call(node_id: NodeId) -> IoCall {
    IoCall::child(ReadInfoOperation::new(vec![node_id]))
}

fn row_answer(
    result: IoResult,
    node_id: NodeId,
) -> Result<Option<NodeInfoDocument>, InfoFlowError> {
    let mut rows = child_answer::<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError>(result)?;
    Ok(rows.remove(&node_id))
}

fn inputs_call(
    realm_id: RealmId,
    stored: Option<&NodeInfoDocument>,
    now_ms: u64,
    facts: &NodeFacts,
) -> IoCall {
    let publisher_generation = stored
        .map(|document| document.epoch.publisher_generation.saturating_add(1))
        .unwrap_or(1);
    IoCall::child(RevisionInputsOperation::new(
        realm_id,
        publisher_generation,
        now_ms,
        facts.serving,
    ))
}

fn publish_call(realm_id: RealmId, node_id: NodeId, only_owed: bool) -> IoCall {
    IoCall::child(PublishInfoOperation::new(realm_id, node_id, only_owed))
}

/// Applies `change` to the stored row in its own transaction, rerun after a lost conflict.
fn revise_call(realm_id: RealmId, node_id: NodeId, change: InfoChange, now_ms: u64) -> IoCall {
    let build = move || InfoWriteOperation::revise(realm_id, node_id, change.clone(), now_ms);
    IoCall::Child(boxed_retry(build, node_id.as_bytes(), |result| {
        Event::Child(ChildOutcome::new(result))
    }))
}

#[derive(Debug, PartialEq)]
enum SeedStep {
    Config,
    Row,
    Inputs,
    Drain,
    Telemetry,
    Write,
    Publish,
}

/// Assembles this node's info document and persists it as owed to replication, which the next
/// tick performs; `publish` replicates it at once. The published drain is the operator's durable
/// flag or a departure, never a value carried forward from the previous document.
#[derive(Debug, PartialEq)]
pub struct SeedInfoOperation {
    realm_id: RealmId,
    node_id: NodeId,
    urls: NodeUrls,
    facts: NodeFacts,
    publish: bool,
    clock: FlowClock,
    config: Option<RealmConfigDocument>,
    leaving: bool,
    compute_draining: bool,
    inputs: Option<RevisionInputs>,
    step: SeedStep,
}

impl SeedInfoOperation {
    pub fn new(
        realm_id: RealmId,
        node_id: NodeId,
        urls: NodeUrls,
        facts: NodeFacts,
        publish: bool,
    ) -> Self {
        Self {
            realm_id,
            node_id,
            urls,
            facts,
            publish,
            clock: FlowClock::default(),
            config: None,
            leaving: false,
            compute_draining: false,
            inputs: None,
            step: SeedStep::Config,
        }
    }

    fn telemetry(&mut self) -> Result<Next<()>, InfoFlowError> {
        let config = self.config.take().ok_or_else(unfinished)?;
        let inputs = self.inputs.as_ref().ok_or_else(unfinished)?;
        self.step = SeedStep::Telemetry;
        Ok(Next::Call(IoCall::child(TelemetryOperation::new(
            config,
            self.node_id,
            self.facts.clone(),
            inputs.reservation.reserved,
            self.clock.started(),
        ))))
    }

    fn write(&mut self, telemetry: Telemetry) -> Result<Next<()>, InfoFlowError> {
        let inputs = self.inputs.take().ok_or_else(unfinished)?;
        let now_ms = self.clock.started();
        let document = NodeInfoDocument {
            node_id: self.node_id,
            executors: telemetry.executors,
            labels: telemetry.labels,
            urls: self.urls.clone(),
            // The first heartbeat reports the download slots.
            utilization: NodeUtilization {
                download_slots: None,
                ..telemetry.utilization
            },
            updated_at_ms: now_ms,
            epoch: inputs.epoch,
            compute_draining: self.compute_draining,
            leaving: self.leaving,
            demand: inputs.demand,
            reservation: inputs.reservation,
        };
        self.step = SeedStep::Write;
        let write = InfoWriteOperation::seed(self.realm_id, document);
        Ok(Next::Call(IoCall::child(write)))
    }
}

impl Operation for SeedInfoOperation {
    type Output = ();
    type Error = InfoFlowError;

    fn first(&mut self) -> Result<Next<()>, InfoFlowError> {
        Ok(Next::Call(config_call(self.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, InfoFlowError> {
        match self.step {
            SeedStep::Config => {
                self.config = Some(config_answer(result)?);
                self.step = SeedStep::Row;
                Ok(Next::Call(row_call(self.node_id)))
            }
            SeedStep::Row => {
                let stored = row_answer(result, self.node_id)?;
                self.leaving = stored.as_ref().is_some_and(|document| document.leaving);
                self.step = SeedStep::Inputs;
                let now_ms = self.clock.started();
                let call = inputs_call(self.realm_id, stored.as_ref(), now_ms, &self.facts);
                Ok(Next::Call(call))
            }
            SeedStep::Inputs => {
                self.inputs = Some(child_answer::<_, InfoFlowError>(result)?);
                self.compute_draining = self.leaving;
                if self.leaving {
                    return self.telemetry();
                }
                self.step = SeedStep::Drain;
                Ok(Next::Call(IoCall::child(DrainStateOperation::new())))
            }
            SeedStep::Drain => {
                let drain = child_answer::<DrainState, DrainStateError>(result)?;
                self.compute_draining = drain.operator_draining;
                self.telemetry()
            }
            SeedStep::Telemetry => self.write(child_answer::<_, InfoFlowError>(result)?),
            SeedStep::Write => {
                child_answer::<bool, InfoWriteError>(result)?;
                if !self.publish {
                    return Ok(Next::Done(()));
                }
                self.step = SeedStep::Publish;
                let publish = publish_call(self.realm_id, self.node_id, false);
                Ok(Next::Call(publish))
            }
            SeedStep::Publish => {
                child_answer::<(), PublishInfoError>(result)?;
                Ok(Next::Done(()))
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.clock.stamp(now_ms);
    }
}

#[derive(Debug, PartialEq)]
enum RefreshStep {
    Row,
    Config,
    Inputs,
    Telemetry,
    Revise,
    Publish,
    Stored,
    Send,
}

/// What the heartbeat carries from the revision it follows.
#[derive(Debug, PartialEq)]
struct HeartbeatParts {
    executors: Vec<ExecutorCapability>,
    utilization: NodeUtilization,
    reservation: ComputeReservationSnapshot,
    demand: ComputeDemandSnapshot,
}

/// Heartbeat: revises the persisted info document, publishes it while a committed revision is
/// still owed to replication, then pushes live telemetry to the sync peers. Snapshots run
/// outside the revision, so it never carries a stale drain backwards.
#[derive(Debug, PartialEq)]
pub struct RefreshInfoOperation {
    realm_id: RealmId,
    node_id: NodeId,
    sequence: u64,
    jobs: Option<RunningJobs>,
    facts: NodeFacts,
    clock: FlowClock,
    stored: Option<NodeInfoDocument>,
    config: Option<RealmConfigDocument>,
    eligible: bool,
    inputs: Option<RevisionInputs>,
    parts: Option<HeartbeatParts>,
    step: RefreshStep,
}

impl RefreshInfoOperation {
    pub fn new(
        realm_id: RealmId,
        node_id: NodeId,
        sequence: u64,
        jobs: Option<RunningJobs>,
        facts: NodeFacts,
    ) -> Self {
        Self {
            realm_id,
            node_id,
            sequence,
            jobs,
            facts,
            clock: FlowClock::default(),
            stored: None,
            config: None,
            eligible: false,
            inputs: None,
            parts: None,
            step: RefreshStep::Row,
        }
    }

    fn revise(&mut self, telemetry: Telemetry) -> Result<Next<()>, InfoFlowError> {
        let inputs = self.inputs.take().ok_or_else(unfinished)?;
        let Telemetry {
            executors,
            labels,
            utilization,
        } = telemetry;
        let change = InfoChange::Telemetry {
            executors: executors.clone(),
            labels,
            reservation: inputs.reservation,
            demand: inputs.demand.clone(),
            utilization,
        };
        self.parts = Some(HeartbeatParts {
            executors,
            utilization,
            reservation: inputs.reservation,
            demand: inputs.demand,
        });
        self.step = RefreshStep::Revise;
        let call = revise_call(self.realm_id, self.node_id, change, self.clock.latest_ms);
        Ok(Next::Call(call))
    }

    /// The heartbeat names the committed advertisement, so peers match it to the row they hold.
    fn send(&mut self, stored: NodeInfoDocument) -> Result<Next<()>, InfoFlowError> {
        let parts = self.parts.take().ok_or_else(unfinished)?;
        let mut availability: Vec<_> = (parts.executors.iter())
            .filter_map(|executor| Some((executor.kind.clone(), executor.availability?)))
            .collect();
        availability.sort_by(|left, right| left.0.cmp(&right.0));
        let epoch = AdvertisementEpoch {
            observed_at_ms: self.clock.started(),
            ..stored.epoch
        };
        let heartbeat = NodeHeartbeat {
            realm_id: self.realm_id,
            epoch,
            sequence: self.sequence,
            utilization: parts.utilization,
            availability,
            reservation: ComputeReservationSnapshot {
                epoch,
                ..parts.reservation
            },
            demand: ComputeDemandSnapshot {
                epoch,
                ..parts.demand
            },
            jobs: self.jobs.take(),
        };
        self.step = RefreshStep::Send;
        let send = SendHeartbeatOperation::new(heartbeat);
        Ok(Next::Call(IoCall::child(send)))
    }
}

impl Operation for RefreshInfoOperation {
    type Output = ();
    type Error = InfoFlowError;

    fn first(&mut self) -> Result<Next<()>, InfoFlowError> {
        Ok(Next::Call(row_call(self.node_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, InfoFlowError> {
        match self.step {
            RefreshStep::Row => {
                let Some(stored) = row_answer(result, self.node_id)? else {
                    return Ok(Next::Done(()));
                };
                self.stored = Some(stored);
                self.step = RefreshStep::Config;
                Ok(Next::Call(config_call(self.realm_id)))
            }
            RefreshStep::Config => {
                let config = config_answer(result)?;
                self.eligible = advertises_executors(&config, self.node_id);
                self.config = Some(config);
                self.step = RefreshStep::Inputs;
                let (stored, now_ms) = (self.stored.take(), self.clock.started());
                let call = inputs_call(self.realm_id, stored.as_ref(), now_ms, &self.facts);
                Ok(Next::Call(call))
            }
            RefreshStep::Inputs => {
                let inputs: RevisionInputs = child_answer::<_, InfoFlowError>(result)?;
                let config = self.config.take().ok_or_else(unfinished)?;
                let telemetry = TelemetryOperation::new(
                    config,
                    self.node_id,
                    self.facts.clone(),
                    inputs.reservation.reserved,
                    self.clock.started(),
                );
                self.inputs = Some(inputs);
                self.step = RefreshStep::Telemetry;
                Ok(Next::Call(IoCall::child(telemetry)))
            }
            RefreshStep::Telemetry => self.revise(child_answer::<_, InfoFlowError>(result)?),
            RefreshStep::Revise => {
                child_answer::<bool, InfoWriteError>(result)?;
                self.step = RefreshStep::Publish;
                Ok(Next::Call(publish_call(self.realm_id, self.node_id, true)))
            }
            RefreshStep::Publish => {
                child_answer::<(), PublishInfoError>(result)?;
                self.step = RefreshStep::Stored;
                Ok(Next::Call(row_call(self.node_id)))
            }
            RefreshStep::Stored => {
                let stored = row_answer(result, self.node_id)?;
                match stored {
                    Some(stored) if self.facts.serving.is_some() && self.eligible => {
                        self.send(stored)
                    }
                    _ => Ok(Next::Done(())),
                }
            }
            RefreshStep::Send => {
                child_answer::<(), HeartbeatError>(result)?;
                Ok(Next::Done(()))
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.clock.stamp(now_ms);
    }
}

/// The report of the executions a departing node still holds capacity for. Their reservations
/// stay in place: a departing node may neither erase them nor claim they ended.
fn departure_report(inputs: &RevisionInputs, now_ms: u64) -> ComputeDepartureReport {
    let mut unresolved: Vec<Ulid> = (inputs.held.iter())
        .map(|record| record.execution_id)
        .collect();
    unresolved.sort_unstable();
    let truncated = unresolved.len() > MAX_UNRESOLVED_EXECUTIONS;
    unresolved.truncate(MAX_UNRESOLVED_EXECUTIONS);
    ComputeDepartureReport {
        departed_at_ms: now_ms,
        membership_generation: inputs.epoch.membership_generation,
        unresolved,
        truncated,
    }
}

#[derive(Debug, PartialEq)]
enum DepartureStep {
    Row,
    Drain,
    Inputs,
    Report,
    Revise,
    Publish,
}

/// Applies an observed departure, or a return, to this node's compute plane. Departing stops
/// offers and admissions and records reserved executions as unresolved without blocking removal;
/// returning clears the flags. False when the state already matched or no row changed.
#[derive(Debug, PartialEq)]
pub struct DepartureStateOperation {
    realm_id: RealmId,
    node_id: NodeId,
    departing: bool,
    facts: NodeFacts,
    clock: FlowClock,
    stored: Option<NodeInfoDocument>,
    draining: bool,
    inputs: Option<RevisionInputs>,
    step: DepartureStep,
}

impl DepartureStateOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId, departing: bool, facts: NodeFacts) -> Self {
        Self {
            realm_id,
            node_id,
            departing,
            facts,
            clock: FlowClock::default(),
            stored: None,
            draining: departing,
            inputs: None,
            step: DepartureStep::Row,
        }
    }

    /// An operator drain outlives a placement observation: returning to the placement map must
    /// not undrain a node somebody drained deliberately.
    fn compare(&mut self) -> Result<Next<bool>, InfoFlowError> {
        let stored = self.stored.take().ok_or_else(unfinished)?;
        if stored.leaving == self.departing && stored.compute_draining == self.draining {
            return Ok(Next::Done(false));
        }
        self.step = DepartureStep::Inputs;
        let now_ms = self.clock.started();
        let call = inputs_call(self.realm_id, Some(&stored), now_ms, &self.facts);
        Ok(Next::Call(call))
    }

    fn revise(&mut self) -> Result<Next<bool>, InfoFlowError> {
        let inputs = self.inputs.as_ref().ok_or_else(unfinished)?;
        let change = InfoChange::Leaving {
            leaving: self.departing,
            reservation: inputs.reservation,
            demand: inputs.demand.clone(),
        };
        self.step = DepartureStep::Revise;
        let call = revise_call(self.realm_id, self.node_id, change, self.clock.latest_ms);
        Ok(Next::Call(call))
    }
}

impl Operation for DepartureStateOperation {
    type Output = bool;
    type Error = InfoFlowError;

    fn first(&mut self) -> Result<Next<bool>, InfoFlowError> {
        Ok(Next::Call(row_call(self.node_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, InfoFlowError> {
        match self.step {
            DepartureStep::Row => {
                let Some(stored) = row_answer(result, self.node_id)? else {
                    return Ok(Next::Done(false));
                };
                self.stored = Some(stored);
                if self.departing {
                    return self.compare();
                }
                self.step = DepartureStep::Drain;
                Ok(Next::Call(IoCall::child(DrainStateOperation::new())))
            }
            DepartureStep::Drain => {
                let drain = child_answer::<DrainState, DrainStateError>(result)?;
                self.draining = drain.operator_draining;
                self.compare()
            }
            DepartureStep::Inputs => {
                let inputs: RevisionInputs = child_answer::<_, InfoFlowError>(result)?;
                let report =
                    (self.departing).then(|| departure_report(&inputs, self.clock.started()));
                self.inputs = Some(inputs);
                let Some(report) = report else {
                    return self.revise();
                };
                self.step = DepartureStep::Report;
                let row = encoded_row(COMPUTE_DEPARTURE_KEYSPACE, DEPARTURE_KEY, &report)?;
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            DepartureStep::Report => {
                done(result)?;
                self.revise()
            }
            DepartureStep::Revise => {
                let revised = child_answer::<bool, InfoWriteError>(result)?;
                let inputs = self.inputs.take().ok_or_else(unfinished)?;
                info!(
                    leaving = self.departing,
                    compute_draining = self.draining,
                    membership_generation = inputs.epoch.membership_generation,
                    reserved = inputs.reservation.reserved.count,
                    "Compute departure state published"
                );
                if !revised {
                    return Ok(Next::Done(false));
                }
                self.step = DepartureStep::Publish;
                Ok(Next::Call(publish_call(self.realm_id, self.node_id, false)))
            }
            DepartureStep::Publish => {
                child_answer::<(), PublishInfoError>(result)?;
                Ok(Next::Done(true))
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.clock.stamp(now_ms);
    }
}

#[derive(Debug, PartialEq)]
enum DrainStep {
    Flag,
    Row,
    Inputs,
    Revise,
    Publish,
}

/// Sets or clears the operator's own compute drain and republishes the advertisement. A drained
/// node plans no new execution and never cancels work holding a receipt; departure state is
/// untouched. False means the durable flag already matched.
#[derive(Debug, PartialEq)]
pub struct OperatorDrainOperation {
    realm_id: RealmId,
    node_id: NodeId,
    draining: bool,
    facts: NodeFacts,
    clock: FlowClock,
    leaving: bool,
    reserved_count: u32,
    step: DrainStep,
}

impl OperatorDrainOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId, draining: bool, facts: NodeFacts) -> Self {
        Self {
            realm_id,
            node_id,
            draining,
            facts,
            clock: FlowClock::default(),
            leaving: false,
            reserved_count: 0,
            step: DrainStep::Flag,
        }
    }
}

impl Operation for OperatorDrainOperation {
    type Output = bool;
    type Error = InfoFlowError;

    fn first(&mut self) -> Result<Next<bool>, InfoFlowError> {
        Ok(Next::Call(IoCall::child(SetDrainOperation::new(
            self.draining,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, InfoFlowError> {
        match self.step {
            DrainStep::Flag => {
                if !child_answer::<bool, IoError>(result)? {
                    return Ok(Next::Done(false));
                }
                self.step = DrainStep::Row;
                Ok(Next::Call(row_call(self.node_id)))
            }
            DrainStep::Row => {
                let Some(stored) = row_answer(result, self.node_id)? else {
                    warn!(
                        operator_draining = self.draining,
                        "Operator compute drain recorded before this node advertises"
                    );
                    return Ok(Next::Done(true));
                };
                self.leaving = stored.leaving;
                self.step = DrainStep::Inputs;
                let now_ms = self.clock.started();
                let call = inputs_call(self.realm_id, Some(&stored), now_ms, &self.facts);
                Ok(Next::Call(call))
            }
            DrainStep::Inputs => {
                let inputs: RevisionInputs = child_answer::<_, InfoFlowError>(result)?;
                self.reserved_count = inputs.reservation.reserved.count;
                // The revision derives the published drain from the flag written above; a
                // departing node stays draining whatever that flag says.
                let change = InfoChange::Snapshots {
                    reservation: inputs.reservation,
                    demand: inputs.demand,
                };
                self.step = DrainStep::Revise;
                let call = revise_call(self.realm_id, self.node_id, change, self.clock.latest_ms);
                Ok(Next::Call(call))
            }
            DrainStep::Revise => {
                let revised = child_answer::<bool, InfoWriteError>(result)?;
                info!(
                    operator_draining = self.draining,
                    leaving = self.leaving,
                    reserved = self.reserved_count,
                    republished = revised,
                    "Operator compute drain published"
                );
                if !revised {
                    return Ok(Next::Done(true));
                }
                self.step = DrainStep::Publish;
                Ok(Next::Call(publish_call(self.realm_id, self.node_id, false)))
            }
            DrainStep::Publish => {
                child_answer::<(), PublishInfoError>(result)?;
                Ok(Next::Done(true))
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.clock.stamp(now_ms);
    }
}

/// Reacts to an observed placement change: a placed node revalidates its inventory, one draining
/// or no longer placed departs, blocking admission and recording copies unresolved. Compute
/// follows the departure state as best effort because departure cannot block on it.
#[derive(Debug, PartialEq)]
pub struct ObservePlacementOperation {
    realm_id: RealmId,
    node_id: NodeId,
    now_ms: u64,
    facts: NodeFacts,
    scanned: Option<SubjectScanResult>,
}

impl ObservePlacementOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId, now_ms: u64, facts: NodeFacts) -> Self {
        Self {
            realm_id,
            node_id,
            now_ms,
            facts,
            scanned: None,
        }
    }
}

impl Operation for ObservePlacementOperation {
    type Output = Option<SubjectScanResult>;
    type Error = SubjectScanError;

    fn first(&mut self) -> Result<Next<Self::Output>, SubjectScanError> {
        let trigger = SubjectTrigger::Observe;
        let observe = SubjectSyncOperation::new(self.realm_id, self.node_id, trigger, self.now_ms);
        Ok(Next::Call(IoCall::child(observe)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SubjectScanError> {
        if let Some(scanned) = self.scanned.take() {
            if let Err(error) = result.child::<bool, InfoFlowError>()? {
                warn!(%error, "failed to publish compute departure state");
            }
            return Ok(Next::Done(Some(scanned)));
        }
        let Some((mode, scanned)) = result.child::<SubjectSync, SubjectScanError>()?? else {
            return Ok(Next::Done(None));
        };
        self.scanned = Some(scanned);
        let departing = matches!(mode, SubjectScanMode::Depart);
        let departure = DepartureStateOperation::new(
            self.realm_id,
            self.node_id,
            departing,
            self.facts.clone(),
        );
        Ok(Next::Call(IoCall::child(departure)))
    }
}

/// Local nonterminal demand merged with the fresh heartbeats of the other sync-eligible members.
/// The bool flags an understated view: a truncated snapshot or a member without a fresh heartbeat.
#[derive(Debug, PartialEq)]
pub struct GroupDemandOperation {
    realm_id: RealmId,
    node_id: NodeId,
    group_id: GroupId,
    fresh: BTreeMap<NodeId, HeldHeartbeat>,
    members: Option<Vec<NodeId>>,
}

impl GroupDemandOperation {
    pub fn new(
        realm_id: RealmId,
        node_id: NodeId,
        group_id: GroupId,
        fresh: BTreeMap<NodeId, HeldHeartbeat>,
    ) -> Self {
        Self {
            realm_id,
            node_id,
            group_id,
            fresh,
            members: None,
        }
    }
}

impl Operation for GroupDemandOperation {
    type Output = (ResourceTotals, bool);
    type Error = InfoFlowError;

    fn first(&mut self) -> Result<Next<Self::Output>, InfoFlowError> {
        Ok(Next::Call(config_call(self.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, InfoFlowError> {
        let Some(members) = self.members.take() else {
            let members = (config_answer(result)?.sync_eligible_nodes())
                .map_err(|error| InfoFlowError::Step(error.to_string()))?;
            self.members = Some(members);
            let demand = DemandSnapshotOperation::new(AdvertisementEpoch::default());
            return Ok(Next::Call(IoCall::child(demand)));
        };
        let local = child_answer::<ComputeDemandSnapshot, DemandError>(result)?;
        Ok(Next::Done(merge_view(
            local,
            &members,
            self.node_id,
            &self.fresh,
            &self.group_id,
        )))
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::compute::quota::JobReservationRecord;

    use super::*;
    use crate::node::info::inputs::tests::{NOW, REALM, child, config, facts, node};

    fn urls() -> NodeUrls {
        NodeUrls {
            api: None,
            s3: None,
        }
    }

    fn epoch(publisher_generation: u64) -> AdvertisementEpoch {
        AdvertisementEpoch {
            membership_generation: 2,
            publisher_generation,
            observed_at_ms: NOW,
        }
    }

    fn utilization() -> NodeUtilization {
        NodeUtilization {
            storage_bytes_used: 5,
            documents_held: Some(1),
            load_permille: Some(10),
            heartbeat_at_ms: NOW,
            download_slots: None,
            storage_capacity_bytes: Some(20),
        }
    }

    fn document(leaving: bool) -> NodeInfoDocument {
        NodeInfoDocument {
            node_id: node(),
            executors: Vec::new(),
            labels: BTreeMap::new(),
            urls: urls(),
            utilization: utilization(),
            updated_at_ms: 1,
            epoch: epoch(3),
            compute_draining: leaving,
            leaving,
            demand: ComputeDemandSnapshot::default(),
            reservation: ComputeReservationSnapshot::default(),
        }
    }

    fn row(stored: Option<NodeInfoDocument>) -> IoResult {
        let rows: BTreeMap<_, _> = stored.into_iter().map(|row| (node(), row)).collect();
        child::<_, NodeInfoError>(Ok(rows))
    }

    fn inputs(held: Vec<JobReservationRecord>) -> RevisionInputs {
        RevisionInputs {
            epoch: epoch(4),
            reservation: ComputeReservationSnapshot {
                epoch: epoch(4),
                reserved: ResourceTotals::default(),
            },
            held,
            demand: ComputeDemandSnapshot::default(),
        }
    }

    fn telemetry() -> Telemetry {
        Telemetry {
            executors: Vec::new(),
            labels: BTreeMap::new(),
            utilization: utilization(),
        }
    }

    fn inputs_turn(stored: Option<&NodeInfoDocument>) -> Result<Next<bool>, InfoFlowError> {
        Ok(Next::Call(inputs_call(REALM, stored, NOW, &facts())))
    }

    fn telemetry_call() -> IoCall {
        IoCall::child(TelemetryOperation::new(
            config(),
            node(),
            facts(),
            ResourceTotals::default(),
            NOW,
        ))
    }

    fn drain(operator_draining: bool) -> IoResult {
        child::<_, DrainStateError>(Ok(DrainState {
            operator_draining,
            departure: None,
        }))
    }

    fn written(revised: bool) -> IoResult {
        child::<_, InfoWriteError>(Ok(revised))
    }

    fn published() -> IoResult {
        child::<(), PublishInfoError>(Ok(()))
    }

    fn leaving_change(leaving: bool) -> InfoChange {
        InfoChange::Leaving {
            leaving,
            reservation: inputs(Vec::new()).reservation,
            demand: ComputeDemandSnapshot::default(),
        }
    }

    fn seed(publish: bool) -> SeedInfoOperation {
        let mut operation = SeedInfoOperation::new(REALM, node(), urls(), facts(), publish);
        operation.stamp(NOW);
        operation
    }

    #[test]
    fn seed_writes_document() {
        let mut operation = seed(true);
        assert_eq!(operation.first(), Ok(Next::Call(config_call(REALM))));
        let answer = operation.next(child::<_, GetConfigError>(Ok(config())));
        assert_eq!(answer, Ok(Next::Call(row_call(node()))));
        let stored = document(false);
        let call = inputs_call(REALM, Some(&stored), NOW, &facts());
        assert_eq!(operation.next(row(Some(stored))), Ok(Next::Call(call)));
        let answer = operation.next(child::<_, InfoFlowError>(Ok(inputs(Vec::new()))));
        let read = IoCall::child(DrainStateOperation::new());
        assert_eq!(answer, Ok(Next::Call(read)));
        assert_eq!(
            operation.next(drain(true)),
            Ok(Next::Call(telemetry_call()))
        );
        let mut sampled = telemetry();
        sampled.utilization.download_slots = Some(Default::default());
        let answer = operation.next(child::<_, InfoFlowError>(Ok(sampled)));
        let document = NodeInfoDocument {
            updated_at_ms: NOW,
            epoch: epoch(4),
            compute_draining: true,
            reservation: inputs(Vec::new()).reservation,
            ..document(false)
        };
        let write = IoCall::child(InfoWriteOperation::seed(REALM, document));
        assert_eq!(answer, Ok(Next::Call(write)));
        let publish = publish_call(REALM, node(), false);
        assert_eq!(operation.next(written(true)), Ok(Next::Call(publish)));
        assert_eq!(operation.next(published()), Ok(Next::Done(())));
    }

    #[test]
    fn leaving_skips_drain() {
        let mut operation = seed(false);
        operation.config = Some(config());
        operation.step = SeedStep::Row;
        operation.next(row(Some(document(true)))).unwrap();
        let answer = operation.next(child::<_, InfoFlowError>(Ok(inputs(Vec::new()))));
        assert_eq!(answer, Ok(Next::Call(telemetry_call())));
        assert!(operation.compute_draining);
        operation.step = SeedStep::Write;
        assert_eq!(operation.next(written(true)), Ok(Next::Done(())));
    }

    #[test]
    fn missing_config_named() {
        let mut operation = seed(false);
        let failed = child::<RealmConfigDocument, _>(Err(GetConfigError::DocumentNotFound));
        let expected = "failed to read realm config for node info: document not found";
        assert_eq!(
            operation.next(failed),
            Err(InfoFlowError::Step(expected.to_string()))
        );
    }

    fn refresh() -> RefreshInfoOperation {
        let jobs = Some(RunningJobs::default());
        let mut operation = RefreshInfoOperation::new(REALM, node(), 8, jobs, facts());
        operation.stamp(NOW);
        operation
    }

    #[test]
    fn unseeded_refresh_ends() {
        let mut operation = refresh();
        assert_eq!(operation.first(), Ok(Next::Call(row_call(node()))));
        assert_eq!(operation.next(row(None)), Ok(Next::Done(())));
    }

    #[test]
    fn refresh_sends_heartbeat() {
        let mut operation = refresh();
        operation.stamp(NOW + 5);
        let stored = document(false);
        let answer = operation.next(row(Some(stored.clone())));
        assert_eq!(answer, Ok(Next::Call(config_call(REALM))));
        let call = inputs_call(REALM, Some(&stored), NOW, &facts());
        let answer = operation.next(child::<_, GetConfigError>(Ok(config())));
        assert_eq!(answer, Ok(Next::Call(call)));
        let answer = operation.next(child::<_, InfoFlowError>(Ok(inputs(Vec::new()))));
        assert_eq!(answer, Ok(Next::Call(telemetry_call())));
        let change = InfoChange::Telemetry {
            executors: Vec::new(),
            labels: BTreeMap::new(),
            reservation: inputs(Vec::new()).reservation,
            demand: ComputeDemandSnapshot::default(),
            utilization: utilization(),
        };
        // The revision takes the latest clock, the snapshots the start of the run.
        let revise = revise_call(REALM, node(), change, NOW + 5);
        let answer = operation.next(child::<_, InfoFlowError>(Ok(telemetry())));
        assert_eq!(answer, Ok(Next::Call(revise)));
        let publish = publish_call(REALM, node(), true);
        assert_eq!(operation.next(written(true)), Ok(Next::Call(publish)));
        assert_eq!(
            operation.next(published()),
            Ok(Next::Call(row_call(node())))
        );
        let sent_epoch = AdvertisementEpoch {
            observed_at_ms: NOW,
            ..stored.epoch
        };
        let heartbeat = NodeHeartbeat {
            realm_id: REALM,
            epoch: sent_epoch,
            sequence: 8,
            utilization: utilization(),
            availability: Vec::new(),
            reservation: ComputeReservationSnapshot {
                epoch: sent_epoch,
                reserved: ResourceTotals::default(),
            },
            demand: ComputeDemandSnapshot {
                epoch: sent_epoch,
                ..ComputeDemandSnapshot::default()
            },
            jobs: Some(RunningJobs::default()),
        };
        let send = IoCall::child(SendHeartbeatOperation::new(heartbeat));
        assert_eq!(operation.next(row(Some(stored))), Ok(Next::Call(send)));
        let answer = operation.next(child::<(), HeartbeatError>(Ok(())));
        assert_eq!(answer, Ok(Next::Done(())));
    }

    #[test]
    fn offline_refresh_stops() {
        // Without a net transport, or for a device, nothing is pushed.
        let mut offline = refresh();
        offline.facts.serving = None;
        offline.eligible = true;
        offline.step = RefreshStep::Stored;
        assert_eq!(offline.next(row(Some(document(false)))), Ok(Next::Done(())));
        let mut device = refresh();
        device.step = RefreshStep::Stored;
        assert_eq!(device.next(row(Some(document(false)))), Ok(Next::Done(())));
    }

    fn departure(departing: bool) -> DepartureStateOperation {
        let mut operation = DepartureStateOperation::new(REALM, node(), departing, facts());
        operation.stamp(NOW);
        operation
    }

    #[test]
    fn departure_records_report() {
        let mut operation = departure(true);
        assert_eq!(operation.first(), Ok(Next::Call(row_call(node()))));
        let stored = document(false);
        let answer = operation.next(row(Some(stored.clone())));
        assert_eq!(answer, inputs_turn(Some(&stored)));
        let answer = operation.next(child::<_, InfoFlowError>(Ok(inputs(Vec::new()))));
        let report = ComputeDepartureReport {
            departed_at_ms: NOW,
            membership_generation: 2,
            unresolved: Vec::new(),
            truncated: false,
        };
        let row_write = encoded_row(COMPUTE_DEPARTURE_KEYSPACE, DEPARTURE_KEY, &report).unwrap();
        assert_eq!(answer, Ok(Next::Call(IoCall::Write(vec![row_write]))));
        let revise = revise_call(REALM, node(), leaving_change(true), NOW);
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Call(revise)));
        let publish = publish_call(REALM, node(), false);
        assert_eq!(operation.next(written(true)), Ok(Next::Call(publish)));
        assert_eq!(operation.next(published()), Ok(Next::Done(true)));
    }

    #[test]
    fn return_keeps_drain() {
        // Returning reads the operator flag; a kept drain with no flag change ends at once.
        let mut operation = departure(false);
        operation.first().unwrap();
        let stored = NodeInfoDocument {
            compute_draining: true,
            ..document(false)
        };
        let read = IoCall::child(DrainStateOperation::new());
        assert_eq!(operation.next(row(Some(stored))), Ok(Next::Call(read)));
        assert_eq!(operation.next(drain(true)), Ok(Next::Done(false)));
    }

    #[test]
    fn return_skips_report() {
        let mut operation = departure(false);
        operation.stored = Some(document(true));
        operation.step = DepartureStep::Drain;
        operation.next(drain(false)).unwrap();
        let answer = operation.next(child::<_, InfoFlowError>(Ok(inputs(Vec::new()))));
        let revise = revise_call(REALM, node(), leaving_change(false), NOW);
        assert_eq!(answer, Ok(Next::Call(revise)));
        assert_eq!(operation.next(written(false)), Ok(Next::Done(false)));
    }

    #[test]
    fn report_stays_bounded() {
        let record = |seed: u128| JobReservationRecord {
            execution_id: Ulid::from(seed),
            ..reservation()
        };
        let count = MAX_UNRESOLVED_EXECUTIONS as u128 + 1;
        let held = (0..count).rev().map(record).collect();
        let report = departure_report(&inputs(held), NOW);
        assert!(report.truncated);
        assert_eq!(report.unresolved.len(), MAX_UNRESOLVED_EXECUTIONS);
        assert_eq!(report.unresolved[0], Ulid::from(0));
    }

    fn reservation() -> JobReservationRecord {
        let job_id = aruna_core::structs::execution::job::JobId::from_bytes([1; 16]);
        JobReservationRecord {
            execution_id: Ulid::nil(),
            job_id,
            logical_job_id: job_id,
            resources: aruna_core::structs::execution::job::EffectiveResources {
                cpu_cores: 1,
                ram_bytes: 1,
                disk_bytes: 0,
                max_walltime_ms: 1,
                preemptible: false,
            },
            created_at_ms: 1,
            subject_generation: 1,
            subject_digest: [0; 32],
        }
    }

    fn operator_drain() -> OperatorDrainOperation {
        let mut operation = OperatorDrainOperation::new(REALM, node(), true, facts());
        operation.stamp(NOW);
        operation
    }

    #[test]
    fn drain_republishes_revision() {
        let mut operation = operator_drain();
        let flag = IoCall::child(SetDrainOperation::new(true));
        assert_eq!(operation.first(), Ok(Next::Call(flag)));
        let answer = operation.next(child::<_, IoError>(Ok(true)));
        assert_eq!(answer, Ok(Next::Call(row_call(node()))));
        let stored = document(false);
        let answer = operation.next(row(Some(stored.clone())));
        assert_eq!(answer, inputs_turn(Some(&stored)));
        let change = InfoChange::Snapshots {
            reservation: inputs(Vec::new()).reservation,
            demand: ComputeDemandSnapshot::default(),
        };
        let revise = revise_call(REALM, node(), change, NOW);
        let answer = operation.next(child::<_, InfoFlowError>(Ok(inputs(Vec::new()))));
        assert_eq!(answer, Ok(Next::Call(revise)));
        let publish = publish_call(REALM, node(), false);
        assert_eq!(operation.next(written(true)), Ok(Next::Call(publish)));
        assert_eq!(operation.next(published()), Ok(Next::Done(true)));
    }

    #[test]
    fn matching_drain_ends() {
        let mut matching = operator_drain();
        let answer = matching.next(child::<_, IoError>(Ok(false)));
        assert_eq!(answer, Ok(Next::Done(false)));
        // A flag set before the first advertisement still counts as changed.
        let mut early = operator_drain();
        early.step = DrainStep::Row;
        assert_eq!(early.next(row(None)), Ok(Next::Done(true)));
    }

    #[test]
    fn observe_follows_mode() {
        let scan = SubjectScanResult::default();
        let mut operation = ObservePlacementOperation::new(REALM, node(), NOW, facts());
        let sync = IoCall::child(SubjectSyncOperation::new(
            REALM,
            node(),
            SubjectTrigger::Observe,
            NOW,
        ));
        assert_eq!(operation.first(), Ok(Next::Call(sync)));
        let synced: SubjectSync = Some((SubjectScanMode::Depart, scan.clone()));
        let answer = operation.next(child::<_, SubjectScanError>(Ok(synced)));
        let depart = DepartureStateOperation::new(REALM, node(), true, facts());
        assert_eq!(answer, Ok(Next::Call(IoCall::child(depart))));
        // A failed departure publication never fails the scan.
        let failed = child::<bool, _>(Err(InfoFlowError::Step("down".to_string())));
        assert_eq!(operation.next(failed), Ok(Next::Done(Some(scan))));
        let mut idle = ObservePlacementOperation::new(REALM, node(), NOW, facts());
        let answer = idle.next(child::<SubjectSync, SubjectScanError>(Ok(None)));
        assert_eq!(answer, Ok(Next::Done(None)));
    }

    #[test]
    fn group_demand_merges() {
        let group = Ulid::from_parts(1, 1);
        let mut operation = GroupDemandOperation::new(REALM, node(), group, BTreeMap::new());
        assert_eq!(operation.first(), Ok(Next::Call(config_call(REALM))));
        let demand = DemandSnapshotOperation::new(AdvertisementEpoch::default());
        let answer = operation.next(child::<_, GetConfigError>(Ok(config())));
        assert_eq!(answer, Ok(Next::Call(IoCall::child(demand))));
        let local = ComputeDemandSnapshot::default();
        let expected = merge_view(local.clone(), &[node()], node(), &BTreeMap::new(), &group);
        let answer = operation.next(child::<_, DemandError>(Ok(local)));
        assert_eq!(answer, Ok(Next::Done(expected)));
    }
}
