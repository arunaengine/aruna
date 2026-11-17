//! Writes this node's info document in one transaction, as a seed or as a revision of the row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::compute::ExecutorCapability;
use aruna_core::compute::quota::{ComputeDemandSnapshot, ComputeReservationSnapshot};
use aruna_core::document::{DocumentChange, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{COMPUTE_DEPARTURE_KEYSPACE, NODE_INFO_KEYSPACE, SYNC_REVISION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::sync_revision_key;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::node_info::{
    AdvertisementEpoch, NodeInfoDocument, NodeUtilization, node_info_key,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;

use crate::node::drain_state::OPERATOR_DRAIN_KEY;

/// The newest advertisement revision not yet handed to replication. It is written in the same
/// transaction as that revision, so a failure after the commit only delays the publish.
pub(crate) const PUBLISH_OWED_KEY: &[u8] = b"publish_owed";

/// Node-info revisions one publisher retries after another committed first.
const ATTEMPTS: usize = 3;

#[derive(Debug, Error, PartialEq)]
pub enum InfoWriteError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("the node info document is invalid: {0}")]
    Document(String),
    #[error("node info seed conflicted with another revision")]
    SeedConflict,
    #[error("node info revision exhausted conflict retries")]
    Exhausted,
    #[error("unexpected event while {0}")]
    UnexpectedEvent(&'static str),
    #[error("the node info write has not finished")]
    Unfinished,
}

/// What one revision changes in the stored advertisement.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum InfoChange {
    /// A heartbeat: executors, labels, snapshots and telemetry.
    Telemetry {
        executors: Vec<ExecutorCapability>,
        labels: BTreeMap<String, String>,
        reservation: ComputeReservationSnapshot,
        demand: ComputeDemandSnapshot,
        utilization: NodeUtilization,
    },
    /// An observed departure or return.
    Leaving {
        leaving: bool,
        reservation: ComputeReservationSnapshot,
        demand: ComputeDemandSnapshot,
    },
    /// New snapshots only, after the operator drain flag changed.
    Snapshots {
        reservation: ComputeReservationSnapshot,
        demand: ComputeDemandSnapshot,
    },
}

impl InfoChange {
    fn apply(&self, document: &mut NodeInfoDocument) {
        match self {
            InfoChange::Telemetry {
                executors,
                labels,
                reservation,
                demand,
                utilization,
            } => {
                document.executors = executors.clone();
                document.labels = labels.clone();
                document.reservation = *reservation;
                document.demand = demand.clone();
                document.utilization = *utilization;
            }
            InfoChange::Leaving {
                leaving,
                reservation,
                demand,
            } => {
                document.leaving = *leaving;
                document.reservation = *reservation;
                document.demand = demand.clone();
            }
            InfoChange::Snapshots {
                reservation,
                demand,
            } => {
                document.reservation = *reservation;
                document.demand = demand.clone();
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum Write {
    /// Writes a whole document over whatever is stored.
    Seed(Box<NodeInfoDocument>),
    /// Applies a change to the stored row; nothing happens without a row.
    Revise { node_id: NodeId, change: InfoChange },
}

#[derive(Debug, PartialEq)]
enum Phase {
    Idle,
    Generation,
    Begin,
    Read(TxnId),
    Write(TxnId),
    Commit,
    Done(Result<bool, InfoWriteError>),
}

impl Phase {
    fn label(&self) -> &'static str {
        match self {
            Phase::Idle => "idle",
            Phase::Generation => "reading the membership generation",
            Phase::Begin => "starting the transaction",
            Phase::Read(_) => "reading the node info row",
            Phase::Write(_) => "writing the node info row",
            Phase::Commit => "committing the node info row",
            Phase::Done(_) => "done",
        }
    }
}

/// Writes the info row and its owed revision marker together. A revision re-reads the row and
/// the operator drain inside its transaction, so no publisher carries a stale drain forward,
/// and retries a commit conflict. True when a row was written.
#[derive(Debug, PartialEq)]
pub(crate) struct InfoWriteOperation {
    realm_id: RealmId,
    write: Write,
    now_ms: u64,
    generation: u64,
    attempts: usize,
    phase: Phase,
}

impl InfoWriteOperation {
    pub(crate) fn seed(realm_id: RealmId, document: NodeInfoDocument) -> Self {
        Self::new(realm_id, Write::Seed(Box::new(document)), 0)
    }

    pub(crate) fn revise(
        realm_id: RealmId,
        node_id: NodeId,
        change: InfoChange,
        now_ms: u64,
    ) -> Self {
        Self::new(realm_id, Write::Revise { node_id, change }, now_ms)
    }

    fn new(realm_id: RealmId, write: Write, now_ms: u64) -> Self {
        Self {
            realm_id,
            write,
            now_ms,
            generation: 0,
            attempts: 0,
            phase: Phase::Idle,
        }
    }

    fn begin(&mut self) -> Effects {
        self.attempts += 1;
        self.phase = Phase::Begin;
        smallvec![Effect::Storage(StorageEffect::StartTransaction { read: false })]
    }

    /// Ends the run and releases its open transaction; the runner still sends the abort.
    fn abort_with(&mut self, txn_id: TxnId, result: Result<bool, InfoWriteError>) -> Effects {
        self.phase = Phase::Done(result);
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn writes(document: &NodeInfoDocument, txn_id: TxnId) -> Result<Effects, InfoWriteError> {
        // `to_bytes` validates, so an advertisement that broke its own bounds is never stored.
        let row = document
            .to_bytes()
            .map_err(|error| InfoWriteError::Document(error.to_string()))?;
        let owed = document.epoch.publisher_generation.to_be_bytes().to_vec();
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    NODE_INFO_KEYSPACE.to_string(),
                    Key::from(node_info_key(document.node_id)),
                    Value::from(row),
                ),
                (
                    COMPUTE_DEPARTURE_KEYSPACE.to_string(),
                    Key::from(PUBLISH_OWED_KEY.to_vec()),
                    Value::from(owed),
                ),
            ],
            txn_id: Some(txn_id),
        })])
    }

    fn started(&mut self, txn_id: TxnId) -> Result<Effects, InfoWriteError> {
        let node_id = match &self.write {
            Write::Seed(document) => {
                self.phase = Phase::Write(txn_id);
                return Self::writes(document, txn_id);
            }
            Write::Revise { node_id, .. } => *node_id,
        };
        self.phase = Phase::Read(txn_id);
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (NODE_INFO_KEYSPACE.to_string(), Key::from(node_info_key(node_id))),
                (
                    COMPUTE_DEPARTURE_KEYSPACE.to_string(),
                    Key::from(OPERATOR_DRAIN_KEY.to_vec()),
                ),
            ],
            txn_id: Some(txn_id),
        })])
    }

    /// The revised row, or `None` when there is no row or nothing durable changed.
    fn revised(
        &self,
        values: &[(Key, Option<Value>)],
    ) -> Result<Option<NodeInfoDocument>, InfoWriteError> {
        let Write::Revise { change, .. } = &self.write else {
            return Err(InfoWriteError::UnexpectedEvent("revising a seed"));
        };
        let [(_, row), (_, drain)] = values else {
            return Err(InfoWriteError::UnexpectedEvent("reading the node info row"));
        };
        let Some(row) = row else {
            return Ok(None);
        };
        let mut document = postcard::from_bytes::<NodeInfoDocument>(row.as_ref())
            .map_err(|error| InfoWriteError::Document(error.to_string()))?;
        let stored = durable_view(&document);
        change.apply(&mut document);
        // The published drain is exactly the operator's durable flag or a departure.
        document.compute_draining = drain.is_some() || document.leaving;
        document.epoch.membership_generation = self.generation;
        if durable_view(&document) == stored {
            return Ok(None);
        }
        document.epoch = AdvertisementEpoch {
            membership_generation: self.generation,
            publisher_generation: document.epoch.publisher_generation.saturating_add(1),
            observed_at_ms: self.now_ms,
        };
        document.demand.epoch = document.epoch;
        document.reservation.epoch = document.epoch;
        document.updated_at_ms = self.now_ms;
        Ok(Some(document))
    }

    fn read(&mut self, txn_id: TxnId, values: &[(Key, Option<Value>)]) -> Effects {
        match self.revised(values) {
            Ok(Some(document)) => match Self::writes(&document, txn_id) {
                Ok(effects) => {
                    self.phase = Phase::Write(txn_id);
                    effects
                }
                Err(error) => self.abort_with(txn_id, Err(error)),
            },
            Ok(None) => self.abort_with(txn_id, Ok(false)),
            Err(error) => self.abort_with(txn_id, Err(error)),
        }
    }

    fn committed(&mut self, error: Option<StorageError>) -> Effects {
        let result = match error {
            None => Ok(true),
            Some(StorageError::TransactionConflict) => match self.write {
                Write::Seed(_) => Err(InfoWriteError::SeedConflict),
                Write::Revise { .. } if self.attempts < ATTEMPTS => return self.begin(),
                Write::Revise { .. } => Err(InfoWriteError::Exhausted),
            },
            Some(error) => Err(error.into()),
        };
        self.phase = Phase::Done(result);
        smallvec![]
    }

    fn advance(&mut self, event: StorageEvent) -> Effects {
        let unexpected = Err(InfoWriteError::UnexpectedEvent(self.phase.label()));
        match (std::mem::replace(&mut self.phase, Phase::Idle), event) {
            (Phase::Generation, StorageEvent::ReadResult { value, .. }) => {
                // An unreadable revision counts as generation zero, as a missing one does.
                self.generation = value
                    .and_then(|bytes| postcard::from_bytes::<DocumentChange>(bytes.as_ref()).ok())
                    .map_or(0, |change| change.current.generation);
                self.begin()
            }
            (Phase::Begin, StorageEvent::TransactionStarted { txn_id }) => {
                match self.started(txn_id) {
                    Ok(effects) => effects,
                    Err(error) => self.abort_with(txn_id, Err(error)),
                }
            }
            (Phase::Read(txn_id), StorageEvent::BatchReadResult { values }) => {
                self.read(txn_id, &values)
            }
            (Phase::Write(txn_id), StorageEvent::BatchWriteResult { .. }) => {
                self.phase = Phase::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (Phase::Commit, StorageEvent::TransactionCommitted { .. }) => self.committed(None),
            (Phase::Commit, StorageEvent::Error { error }) => self.committed(Some(error)),
            (Phase::Read(txn_id) | Phase::Write(txn_id), StorageEvent::Error { error }) => {
                self.abort_with(txn_id, Err(error.into()))
            }
            (Phase::Generation | Phase::Begin, StorageEvent::Error { error }) => {
                self.phase = Phase::Done(Err(error.into()));
                smallvec![]
            }
            (Phase::Read(txn_id) | Phase::Write(txn_id), _) => {
                self.abort_with(txn_id, unexpected)
            }
            _ => {
                self.phase = Phase::Done(unexpected);
                smallvec![]
            }
        }
    }
}

/// The advertisement without telemetry and publication stamps; only a change here publishes.
fn durable_view(document: &NodeInfoDocument) -> NodeInfoDocument {
    let mut view = document.clone();
    for executor in &mut view.executors {
        executor.availability = None;
    }
    view.utilization = NodeUtilization {
        storage_bytes_used: 0,
        documents_held: None,
        load_permille: None,
        heartbeat_at_ms: 0,
        download_slots: None,
        storage_capacity_bytes: None,
    };
    view.demand = ComputeDemandSnapshot::default();
    view.reservation = ComputeReservationSnapshot::default();
    view.updated_at_ms = 0;
    view.epoch.publisher_generation = 0;
    view.epoch.observed_at_ms = 0;
    view
}

impl Operation for InfoWriteOperation {
    type Output = bool;
    type Error = InfoWriteError;

    fn start(&mut self) -> Effects {
        if matches!(self.write, Write::Seed(_)) {
            return self.begin();
        }
        self.phase = Phase::Generation;
        let target = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: SYNC_REVISION_KEYSPACE.to_string(),
            key: sync_revision_key(&target),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(event) => self.advance(event),
            _ => {
                let unexpected = InfoWriteError::UnexpectedEvent(self.phase.label());
                match std::mem::replace(&mut self.phase, Phase::Idle) {
                    Phase::Read(txn_id) | Phase::Write(txn_id) => {
                        self.abort_with(txn_id, Err(unexpected))
                    }
                    _ => {
                        self.phase = Phase::Done(Err(unexpected));
                        smallvec![]
                    }
                }
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.phase, Phase::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.phase {
            Phase::Done(result) => result,
            _ => Err(InfoWriteError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.phase {
            Phase::Read(txn_id) | Phase::Write(txn_id) => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

