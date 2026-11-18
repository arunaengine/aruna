//! Gathers what a node info revision states: host facts, capacity snapshots and telemetry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;

use aruna_compute::registry::ExecutorOffer;
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::ExecutorCapability;
use aruna_core::compute::quota::{
    ComputeDemandSnapshot, ComputeReservationSnapshot, JobReservationRecord, ResourceTotals,
    availability,
};
use aruna_core::document::DocumentTarget;
use aruna_core::keyspaces::{
    METADATA_INDEX_KEYSPACE, NODE_SUBJECT_KEYSPACE, SYNC_REVISION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::placement::{build_view, held_buckets};
use aruna_core::rows::{PrefixPages, decode, read_row, values};
use aruna_core::storage_entries::sync_revision_key;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::node_info::{AdvertisementEpoch, NodeUtilization};
use aruna_core::structs::storage::usage::{USAGE_GLOBAL_KEY, UsageCounters};
use aruna_core::types::Key;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::facts::NodeFacts;
use crate::jobs::demand::{DemandError, DemandSnapshotOperation};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::held::HeldReservations;
use crate::jobs::lifecycle::updates::SettleTerminalsOperation;
use crate::metadata::repository::FILL_PAGE_SIZE;
use crate::node::info::write::revision_generation;
use crate::realm::mutate_placement::node_kind;
use crate::s3::usage::{LoadCountersError, LoadCountersOperation};

#[derive(Debug, Error, PartialEq)]
pub enum InfoFlowError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("{0}")]
    Step(String),
}

/// The successful answer of a typed child; its error keeps only its message.
pub(in crate::node) fn child_answer<T: 'static, E: Display + 'static>(
    result: IoResult,
) -> Result<T, InfoFlowError> {
    (result.child::<T, E>()?).map_err(|error| InfoFlowError::Step(error.to_string()))
}

/// Reads this node's placement subject record.
#[derive(Debug, PartialEq)]
struct SubjectRecordOperation;

impl Operation for SubjectRecordOperation {
    type Output = Option<NodeSubjectRecord>;
    type Error = InfoFlowError;

    fn first(&mut self) -> Result<Next<Self::Output>, InfoFlowError> {
        let read = read_row(NODE_SUBJECT_KEYSPACE, NODE_SUBJECT_KEY.to_vec());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, InfoFlowError> {
        let [value] = values::<1>(result)?;
        let record = value
            .map(|bytes| NodeSubjectRecord::from_bytes(bytes.as_ref()))
            .transpose()
            .map_err(|error| InfoFlowError::Step(error.to_string()))?;
        Ok(Next::Done(record))
    }
}

/// Whether this node may advertise execution targets at all. A User device runs
/// only its owner's local jobs, so it never offers the realm a target to
/// dispatch to, whatever compute the owner enabled on the machine.
pub(in crate::node) fn advertises_executors(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    node_kind(config, node_id).is_some_and(|kind| kind.is_sync_eligible())
}

/// This node's advertised execution targets at its placement subject (none without one), with
/// availability as static ceilings minus local reservations. Ranking only: exact admission stays
/// the target-side reservation.
fn advertised_executors(
    offers: &[ExecutorOffer],
    record: Option<&NodeSubjectRecord>,
    reserved: &ResourceTotals,
    now_ms: u64,
) -> Result<Vec<ExecutorCapability>, String> {
    let Some(record) = record else {
        return Ok(Vec::new());
    };
    let mut capabilities = (offers.iter())
        .map(|offer| offer.capability(&record.subject, record.policy_draining))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to build executor advertisements: {error}"))?;
    for capability in &mut capabilities {
        capability.availability = Some(availability(&capability.limits, reserved, now_ms));
    }
    Ok(capabilities)
}

fn placement_labels(
    config: &RealmConfigDocument,
    node_id: NodeId,
) -> Result<BTreeMap<String, String>, String> {
    build_view(config)
        .nodes
        .into_iter()
        .find(|node| node.node_id == node_id)
        .map(|node| node.labels)
        .ok_or_else(|| format!("node {node_id} is missing from the realm placement view"))
}

/// The `(strategy, shard)` buckets `node_id` holds across every strategy. A
/// document counts as held when its recorded placement bucket is in this set,
/// so everywhere-replicated registry rows are not each counted as local.
pub(in crate::node) fn held_placement_set(
    config: &RealmConfigDocument,
    node_id: NodeId,
) -> HashSet<(Ulid, u32)> {
    let mut held = HashSet::new();
    for strategy in &config.strategies {
        for shard in held_buckets(config, strategy, node_id) {
            held.insert((strategy.strategy_id, shard));
        }
    }
    held
}

/// Counts the registry documents in `held` buckets, one page at a time.
#[derive(Debug, PartialEq)]
pub struct HeldDocumentsOperation {
    held: HashSet<(Ulid, u32)>,
    pages: PrefixPages,
    count: u64,
}

impl HeldDocumentsOperation {
    pub(in crate::node) fn new(held: HashSet<(Ulid, u32)>) -> Self {
        let prefix = Key::from(Vec::new());
        Self {
            held,
            pages: PrefixPages::new(METADATA_INDEX_KEYSPACE, prefix, FILL_PAGE_SIZE),
            count: 0,
        }
    }
}

impl Operation for HeldDocumentsOperation {
    type Output = u64;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<u64>, IoError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, IoError> {
        for (_, value) in self.pages.take(result)? {
            let record: MetadataRegistryRecord = decode(&value)?;
            // NIL placements predate any strategy and are held by every local
            // node, matching holds_placement; count them alongside held buckets.
            if record.placement == PlacementRef::NIL
                || (self.held).contains(&(record.placement.strategy_id, record.placement.shard))
            {
                self.count += 1;
            }
        }
        if self.pages.done() {
            return Ok(Next::Done(self.count));
        }
        Ok(Next::Call(self.pages.call()))
    }
}

/// The epoch and capacity snapshots one advertisement revision states.
#[derive(Clone, Debug, PartialEq)]
pub struct RevisionInputs {
    pub epoch: AdvertisementEpoch,
    pub reservation: ComputeReservationSnapshot,
    /// The reservations the snapshot sums.
    pub held: Vec<JobReservationRecord>,
    pub demand: ComputeDemandSnapshot,
}

#[derive(Debug, PartialEq)]
enum InputStep {
    Generation,
    Settle,
    Held,
    Demand,
}

/// Reads the membership generation, settles finished executions, then snapshots this node's
/// exact reservations and its bounded logical demand.
#[derive(Debug, PartialEq)]
pub struct RevisionInputsOperation {
    realm_id: RealmId,
    publisher_generation: u64,
    now_ms: u64,
    serving: Option<(NodeId, RealmId)>,
    epoch: AdvertisementEpoch,
    reservation: ComputeReservationSnapshot,
    held: Vec<JobReservationRecord>,
    step: InputStep,
}

impl RevisionInputsOperation {
    pub(in crate::node) fn new(
        realm_id: RealmId,
        publisher_generation: u64,
        now_ms: u64,
        serving: Option<(NodeId, RealmId)>,
    ) -> Self {
        Self {
            realm_id,
            publisher_generation,
            now_ms,
            serving,
            epoch: AdvertisementEpoch::default(),
            reservation: ComputeReservationSnapshot::default(),
            held: Vec::new(),
            step: InputStep::Generation,
        }
    }
}

impl Operation for RevisionInputsOperation {
    type Output = RevisionInputs;
    type Error = InfoFlowError;

    fn first(&mut self) -> Result<Next<RevisionInputs>, InfoFlowError> {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        let read = read_row(SYNC_REVISION_KEYSPACE, sync_revision_key(&target));
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RevisionInputs>, InfoFlowError> {
        match self.step {
            InputStep::Generation => {
                let [revision] = values::<1>(result)?;
                // A rejoin observes a newer generation, so a restarted counter still supersedes.
                self.epoch = AdvertisementEpoch {
                    membership_generation: revision_generation(revision),
                    publisher_generation: self.publisher_generation,
                    observed_at_ms: self.now_ms,
                };
                self.step = InputStep::Settle;
                let settle = SettleTerminalsOperation::new(self.serving, self.now_ms);
                Ok(Next::Call(IoCall::child(settle)))
            }
            InputStep::Settle => {
                // A pending terminal obligation is retried by its own task; it must never
                // stop this node from publishing its info document.
                if let Err(error) = result.child::<bool, LifecycleError>()? {
                    warn!(%error, "Terminal job settlement failed during the node-info snapshot");
                }
                self.step = InputStep::Held;
                Ok(Next::Call(IoCall::child(HeldReservations::all())))
            }
            InputStep::Held => {
                self.held = child_answer::<_, LifecycleError>(result)?;
                let mut reserved = ResourceTotals::default();
                for record in &self.held {
                    reserved.add(&record.resources);
                }
                self.reservation = ComputeReservationSnapshot {
                    epoch: self.epoch,
                    reserved,
                };
                self.step = InputStep::Demand;
                let demand = DemandSnapshotOperation::new(self.epoch);
                Ok(Next::Call(IoCall::child(demand)))
            }
            InputStep::Demand => Ok(Next::Done(RevisionInputs {
                epoch: self.epoch,
                reservation: self.reservation,
                held: std::mem::take(&mut self.held),
                demand: child_answer::<_, DemandError>(result)?,
            })),
        }
    }
}

/// The advertised executors, labels and utilization of one revision.
#[derive(Clone, Debug, PartialEq)]
pub struct Telemetry {
    pub executors: Vec<ExecutorCapability>,
    pub labels: BTreeMap<String, String>,
    pub utilization: NodeUtilization,
}

#[derive(Debug, PartialEq)]
enum TelemetryStep {
    Subject,
    Counters,
    Documents,
}

/// Builds the telemetry of one revision from the host facts and two local reads. A failed
/// document count only warns, so a storage hiccup never fails the heartbeat.
#[derive(Debug, PartialEq)]
pub struct TelemetryOperation {
    config: RealmConfigDocument,
    node_id: NodeId,
    facts: NodeFacts,
    reserved: ResourceTotals,
    now_ms: u64,
    built: Option<(Vec<ExecutorCapability>, BTreeMap<String, String>)>,
    storage_bytes_used: u64,
    step: TelemetryStep,
}

impl TelemetryOperation {
    pub(in crate::node) fn new(
        config: RealmConfigDocument,
        node_id: NodeId,
        facts: NodeFacts,
        reserved: ResourceTotals,
        now_ms: u64,
    ) -> Self {
        Self {
            config,
            node_id,
            facts,
            reserved,
            now_ms,
            built: None,
            storage_bytes_used: 0,
            step: TelemetryStep::Subject,
        }
    }

    /// Builds what the facts decide, then reads the local usage counters.
    fn build(
        &mut self,
        record: Option<&NodeSubjectRecord>,
    ) -> Result<Next<Telemetry>, InfoFlowError> {
        let facts = &self.facts;
        let executors = advertised_executors(&facts.offers, record, &self.reserved, self.now_ms)
            .map_err(InfoFlowError::Step)?;
        // Class labels stay advertisement only and never enter the placement selector input.
        let mut labels =
            placement_labels(&self.config, self.node_id).map_err(InfoFlowError::Step)?;
        labels.extend(facts.class_labels.clone());
        self.built = Some((executors, labels));
        self.step = TelemetryStep::Counters;
        let counters = LoadCountersOperation::new(USAGE_GLOBAL_KEY.to_vec());
        Ok(Next::Call(IoCall::child(counters)))
    }
}

/// The held document count, or `None` with a warning when it could not be read.
fn documents_held(result: IoResult) -> Result<Option<u64>, IoError> {
    Ok(match result.child::<u64, IoError>()? {
        Ok(count) => Some(count),
        Err(error) => {
            warn!(%error, "failed to count documents held for node info");
            None
        }
    })
}

impl Operation for TelemetryOperation {
    type Output = Telemetry;
    type Error = InfoFlowError;

    fn first(&mut self) -> Result<Next<Telemetry>, InfoFlowError> {
        if self.facts.offers.is_empty() || !advertises_executors(&self.config, self.node_id) {
            return self.build(None);
        }
        Ok(Next::Call(IoCall::child(SubjectRecordOperation)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Telemetry>, InfoFlowError> {
        match self.step {
            TelemetryStep::Subject => {
                let record = child_answer::<Option<NodeSubjectRecord>, InfoFlowError>(result)?;
                self.build(record.as_ref())
            }
            TelemetryStep::Counters => {
                let counters: UsageCounters = child_answer::<_, LoadCountersError>(result)?;
                self.storage_bytes_used = counters.stored_bytes;
                self.step = TelemetryStep::Documents;
                let held = held_placement_set(&self.config, self.node_id);
                Ok(Next::Call(IoCall::child(HeldDocumentsOperation::new(held))))
            }
            TelemetryStep::Documents => {
                let documents_held = documents_held(result)?;
                let (executors, labels) = self.built.take().ok_or(IoError::Unfinished)?;
                Ok(Next::Done(Telemetry {
                    executors,
                    labels,
                    utilization: NodeUtilization {
                        storage_bytes_used: self.storage_bytes_used,
                        documents_held,
                        load_permille: self.facts.load_permille,
                        heartbeat_at_ms: self.now_ms,
                        download_slots: self.facts.download_slots,
                        storage_capacity_bytes: self.facts.storage_capacity,
                    },
                }))
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::LazyLock;

    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmNodeKind;

    use super::*;

    pub(in crate::node) const REALM: RealmId = RealmId([3; 32]);
    pub(in crate::node) const NOW: u64 = 9_000;

    pub(in crate::node) fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    pub(in crate::node) fn facts() -> NodeFacts {
        NodeFacts {
            offers: Vec::new(),
            class_labels: BTreeMap::from([("class".to_string(), "true".to_string())]),
            load_permille: Some(10),
            storage_capacity: Some(20),
            download_slots: None,
            serving: Some((node(), REALM)),
        }
    }

    /// One realm config per test run, as seeding placement mints new strategy ids.
    pub(in crate::node) fn config() -> RealmConfigDocument {
        static CONFIG: LazyLock<RealmConfigDocument> = LazyLock::new(|| {
            let mut config = RealmConfigDocument::default_for_realm(REALM, Vec::new());
            config.seed_default_placement();
            config.ensure_node(node(), RealmNodeKind::Server);
            config
        });
        CONFIG.clone()
    }

    pub(in crate::node) fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    #[test]
    fn inputs_snapshot_capacity() {
        let mut operation = RevisionInputsOperation::new(REALM, 4, NOW, facts().serving);
        let target = DocumentTarget::RealmConfig { realm_id: REALM };
        let read = read_row(SYNC_REVISION_KEYSPACE, sync_revision_key(&target));
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let settle = IoCall::child(SettleTerminalsOperation::new(facts().serving, NOW));
        let answer = operation.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Ok(Next::Call(settle)));
        // A failed settlement never stops the snapshot.
        let failed = child::<bool, _>(Err(LifecycleError::from(IoError::Unfinished)));
        let held = IoCall::child(HeldReservations::all());
        assert_eq!(operation.next(failed), Ok(Next::Call(held)));
        let reservations = child::<_, LifecycleError>(Ok(Vec::<JobReservationRecord>::new()));
        let epoch = AdvertisementEpoch {
            membership_generation: 0,
            publisher_generation: 4,
            observed_at_ms: NOW,
        };
        let demand = IoCall::child(DemandSnapshotOperation::new(epoch));
        assert_eq!(operation.next(reservations), Ok(Next::Call(demand)));
        let snapshot = ComputeDemandSnapshot::default();
        let answer = operation.next(child::<_, DemandError>(Ok(snapshot.clone())));
        let inputs = RevisionInputs {
            epoch,
            reservation: ComputeReservationSnapshot {
                epoch,
                reserved: ResourceTotals::default(),
            },
            held: Vec::new(),
            demand: snapshot,
        };
        assert_eq!(answer, Ok(Next::Done(inputs)));
    }

    #[test]
    fn inputs_keep_failures() {
        let mut operation = RevisionInputsOperation::new(REALM, 1, NOW, None);
        operation.step = InputStep::Held;
        let failed =
            child::<Vec<JobReservationRecord>, _>(Err(LifecycleError::from(IoError::Unfinished)));
        assert!(matches!(
            operation.next(failed),
            Err(InfoFlowError::Step(_))
        ));
    }

    fn telemetry(facts: NodeFacts) -> TelemetryOperation {
        TelemetryOperation::new(config(), node(), facts, ResourceTotals::default(), NOW)
    }

    #[test]
    fn telemetry_counts_usage() {
        let mut operation = telemetry(facts());
        let counters = IoCall::child(LoadCountersOperation::new(USAGE_GLOBAL_KEY.to_vec()));
        assert_eq!(operation.first(), Ok(Next::Call(counters)));
        let used = UsageCounters {
            stored_bytes: 7,
            ..UsageCounters::default()
        };
        let held = held_placement_set(&config(), node());
        let count = IoCall::child(HeldDocumentsOperation::new(held));
        let answer = operation.next(child::<_, LoadCountersError>(Ok(used)));
        assert_eq!(answer, Ok(Next::Call(count)));
        // A failed count degrades to an unknown value.
        let answer = operation.next(child::<u64, _>(Err(IoError::Unfinished)));
        let Ok(Next::Done(telemetry)) = answer else {
            panic!("unexpected answer {answer:?}");
        };
        assert_eq!(telemetry.utilization.storage_bytes_used, 7);
        assert_eq!(telemetry.utilization.documents_held, None);
        assert_eq!(telemetry.utilization.heartbeat_at_ms, NOW);
        assert_eq!(telemetry.utilization.load_permille, Some(10));
        assert_eq!(
            telemetry.labels.get("class").map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn offers_need_subject() {
        let mut facts = facts();
        let caps = aruna_compute::executor::BackendCaps::default();
        facts.offers = vec![ExecutorOffer::new("local".to_string(), caps)];
        let mut operation = telemetry(facts.clone());
        let read = IoCall::child(SubjectRecordOperation);
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        // A node without a subject offers nothing and goes on to its counters.
        let none = child::<Option<NodeSubjectRecord>, InfoFlowError>(Ok(None));
        let counters = IoCall::child(LoadCountersOperation::new(USAGE_GLOBAL_KEY.to_vec()));
        assert_eq!(operation.next(none), Ok(Next::Call(counters)));
        let failed = InfoFlowError::Step("no subject".to_string());
        let mut broken = telemetry(facts.clone());
        let answer = broken.next(child::<Option<NodeSubjectRecord>, _>(Err(failed)));
        assert_eq!(answer, Err(InfoFlowError::Step("no subject".to_string())));
        // A node that advertises nothing never reads the subject.
        let mut silent = telemetry(facts);
        silent.node_id = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let message = format!(
            "node {} is missing from the realm placement view",
            silent.node_id
        );
        assert_eq!(silent.first(), Err(InfoFlowError::Step(message)));
    }

    #[test]
    fn counts_held_pages() {
        let strategy_id = Ulid::from_parts(1, 1);
        let record = |shard: u32| MetadataRegistryRecord {
            realm_id: REALM,
            group_id: Ulid::nil(),
            document_id: Ulid::nil(),
            document_path: String::new(),
            graph_iri: String::new(),
            public: false,
            permission_path: String::new(),
            placement: PlacementRef { strategy_id, shard },
            holder_node_ids: Vec::new(),
            created_at_ms: 0,
            updated_at_ms: 0,
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        };
        let row = |shard| {
            let value = postcard::to_allocvec(&record(shard)).unwrap();
            (Key::from(vec![shard as u8]), value.into())
        };
        let held = HashSet::from([(strategy_id, 1)]);
        let mut operation = HeldDocumentsOperation::new(held);
        let page = |after| IoCall::Page {
            space: METADATA_INDEX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: FILL_PAGE_SIZE,
        };
        assert_eq!(operation.first(), Ok(Next::Call(page(None))));
        let next = Key::from(vec![2]);
        let first = IoResult::Page {
            rows: vec![row(1), row(2)],
            next: Some(next.clone()),
        };
        assert_eq!(operation.next(first), Ok(Next::Call(page(Some(next)))));
        let last = IoResult::Page {
            rows: vec![row(1)],
            next: None,
        };
        assert_eq!(operation.next(last), Ok(Next::Done(2)));
    }

    #[test]
    fn reads_subject_record() {
        let mut operation = SubjectRecordOperation;
        let read = read_row(NODE_SUBJECT_KEYSPACE, NODE_SUBJECT_KEY.to_vec());
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let answer = operation.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Ok(Next::Done(None)));
        let garbled = IoResult::Values(vec![Some(vec![0xff].into())]);
        assert!(matches!(
            operation.next(garbled),
            Err(InfoFlowError::Step(_))
        ));
    }
}
