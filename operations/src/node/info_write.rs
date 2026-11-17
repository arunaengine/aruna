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
use aruna_core::keyspaces::{
    COMPUTE_DEPARTURE_KEYSPACE, NODE_INFO_KEYSPACE, SYNC_REVISION_KEYSPACE,
};
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

#[derive(Debug, Error, PartialEq)]
pub enum InfoWriteError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("the node info document is invalid: {0}")]
    Document(String),
    #[error("node info seed conflicted with another revision")]
    SeedConflict,
    #[error("node info revision lost a transaction conflict")]
    Conflict,
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
/// the operator drain inside its transaction, so no publisher carries a stale drain forward;
/// its lost conflict is retried by the runner. True when a row was written.
#[derive(Debug, PartialEq)]
pub(crate) struct InfoWriteOperation {
    realm_id: RealmId,
    write: Write,
    now_ms: u64,
    generation: u64,
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
            phase: Phase::Idle,
        }
    }

    fn begin(&mut self) -> Effects {
        self.phase = Phase::Begin;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
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
                (
                    NODE_INFO_KEYSPACE.to_string(),
                    Key::from(node_info_key(node_id))
                ),
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
                Write::Revise { .. } => Err(InfoWriteError::Conflict),
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
            (Phase::Read(txn_id) | Phase::Write(txn_id), _) => self.abort_with(txn_id, unexpected),
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

    fn is_conflict(error: &Self::Error) -> bool {
        *error == InfoWriteError::Conflict
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

#[cfg(test)]
mod state_machine_tests {
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::storage::node_info::NodeUrls;
    use ulid::Ulid;

    use super::*;

    const REALM: RealmId = RealmId([3; 32]);
    const TXN: TxnId = Ulid::from_parts(5, 5);
    const NOW: u64 = 9_000;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn document(publisher_generation: u64) -> NodeInfoDocument {
        NodeInfoDocument {
            node_id: node(),
            executors: Vec::new(),
            labels: BTreeMap::new(),
            urls: NodeUrls {
                api: None,
                s3: None,
            },
            utilization: NodeUtilization {
                storage_bytes_used: 0,
                documents_held: None,
                load_permille: None,
                heartbeat_at_ms: 0,
                download_slots: None,
                storage_capacity_bytes: None,
            },
            updated_at_ms: 1,
            epoch: AdvertisementEpoch {
                membership_generation: 0,
                publisher_generation,
                observed_at_ms: 1,
            },
            compute_draining: false,
            leaving: false,
            demand: ComputeDemandSnapshot::default(),
            reservation: ComputeReservationSnapshot::default(),
        }
    }

    fn storage(event: StorageEvent) -> Event {
        Event::Storage(event)
    }

    fn begin() -> Effect {
        Effect::Storage(StorageEffect::StartTransaction { read: false })
    }

    fn started() -> Event {
        storage(StorageEvent::TransactionStarted { txn_id: TXN })
    }

    fn commit() -> Effect {
        Effect::Storage(StorageEffect::CommitTransaction { txn_id: TXN })
    }

    fn rollback() -> Effect {
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN })
    }

    fn conflict() -> Event {
        storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        })
    }

    fn generation_read() -> Effect {
        Effect::Storage(StorageEffect::Read {
            key_space: SYNC_REVISION_KEYSPACE.to_string(),
            key: sync_revision_key(&DocumentTarget::RealmConfig { realm_id: REALM }),
            txn_id: None,
        })
    }

    fn row_read() -> Effect {
        Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    NODE_INFO_KEYSPACE.to_string(),
                    Key::from(node_info_key(node())),
                ),
                (
                    COMPUTE_DEPARTURE_KEYSPACE.to_string(),
                    Key::from(OPERATOR_DRAIN_KEY.to_vec()),
                ),
            ],
            txn_id: Some(TXN),
        })
    }

    fn rows(row: Option<&NodeInfoDocument>, drained: bool) -> Event {
        let row = row.map(|document| Value::from(document.to_bytes().unwrap()));
        let drain = drained.then(|| Value::from(vec![1]));
        storage(StorageEvent::BatchReadResult {
            values: vec![(Key::from(Vec::new()), row), (Key::from(Vec::new()), drain)],
        })
    }

    fn written() -> Event {
        storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        })
    }

    fn committed() -> Event {
        storage(StorageEvent::TransactionCommitted { txn_id: TXN })
    }

    fn leaving() -> InfoChange {
        InfoChange::Leaving {
            leaving: true,
            reservation: ComputeReservationSnapshot::default(),
            demand: ComputeDemandSnapshot::default(),
        }
    }

    fn revise(change: InfoChange) -> InfoWriteOperation {
        InfoWriteOperation::revise(REALM, node(), change, NOW)
    }

    /// A revision at the read step, generation zero.
    fn reading(change: InfoChange) -> InfoWriteOperation {
        let mut operation = revise(change);
        operation.start();
        operation.step(storage(StorageEvent::ReadResult {
            key: Key::from(Vec::new()),
            value: None,
        }));
        operation.step(started());
        operation
    }

    #[test]
    fn seed_writes_row() {
        let seeded = document(1);
        let mut operation = InfoWriteOperation::seed(REALM, seeded.clone());
        let writes = InfoWriteOperation::writes(&seeded, TXN).unwrap().to_vec();
        check_trace(
            &mut operation,
            &[begin()],
            vec![
                (started(), writes),
                (written(), vec![commit()]),
                (committed(), vec![]),
            ],
        );
        assert_eq!(operation.finalize(), Ok(true));

        let mut racing = InfoWriteOperation::seed(REALM, seeded);
        racing.start();
        racing.step(started());
        racing.step(written());
        racing.step(conflict());
        assert_eq!(racing.finalize(), Err(InfoWriteError::SeedConflict));
    }

    #[test]
    fn revision_stamps_epoch() {
        let mut operation = revise(leaving());
        let mut expected = document(4);
        expected.leaving = true;
        expected.compute_draining = true;
        expected.epoch = AdvertisementEpoch {
            membership_generation: 0,
            publisher_generation: 5,
            observed_at_ms: NOW,
        };
        expected.demand.epoch = expected.epoch;
        expected.reservation.epoch = expected.epoch;
        expected.updated_at_ms = NOW;
        let writes = InfoWriteOperation::writes(&expected, TXN).unwrap().to_vec();
        check_trace(
            &mut operation,
            &[generation_read()],
            vec![
                (
                    storage(StorageEvent::ReadResult {
                        key: Key::from(Vec::new()),
                        value: None,
                    }),
                    vec![begin()],
                ),
                (started(), vec![row_read()]),
                (rows(Some(&document(4)), false), writes),
                (written(), vec![commit()]),
                (committed(), vec![]),
            ],
        );
        assert_eq!(operation.finalize(), Ok(true));
    }

    #[test]
    fn unchanged_rows_abort() {
        // No row, or a change that alters nothing durable, writes nothing.
        let snapshots = InfoChange::Snapshots {
            reservation: ComputeReservationSnapshot::default(),
            demand: ComputeDemandSnapshot::default(),
        };
        for (row, change) in [(None, leaving()), (Some(document(2)), snapshots)] {
            let mut operation = reading(change);
            assert_eq!(
                operation.step(rows(row.as_ref(), false)).to_vec(),
                vec![rollback()]
            );
            assert_eq!(operation.finalize(), Ok(false));
        }
        // The operator's flag drains the advertisement although the change does not.
        let mut drained = reading(InfoChange::Snapshots {
            reservation: ComputeReservationSnapshot::default(),
            demand: ComputeDemandSnapshot::default(),
        });
        let effects = drained.step(rows(Some(&document(2)), true));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite { .. })]
        ));
    }

    #[test]
    fn conflict_left_to_runner() {
        // A lost revision conflict ends the run for the runner's retry; a seed conflict is final.
        let mut operation = reading(leaving());
        operation.step(rows(Some(&document(1)), false));
        operation.step(written());
        assert!(operation.step(conflict()).is_empty());
        let error = operation.finalize().unwrap_err();
        assert_eq!(error, InfoWriteError::Conflict);
        assert!(InfoWriteOperation::is_conflict(&error));
        assert!(!InfoWriteOperation::is_conflict(
            &InfoWriteError::SeedConflict
        ));
    }

    #[test]
    fn failures_abort_open() {
        let failed = || {
            storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })
        };
        let mut operation = reading(leaving());
        assert_eq!(operation.step(failed()).to_vec(), vec![rollback()]);
        assert_eq!(operation.finalize(), Err(StorageError::Timeout.into()));

        let mut garbled = reading(leaving());
        let row = storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(Vec::new()), Some(Value::from(vec![0xff; 3]))),
                (Key::from(Vec::new()), None),
            ],
        });
        assert_eq!(garbled.step(row).to_vec(), vec![rollback()]);
        assert!(matches!(
            garbled.finalize(),
            Err(InfoWriteError::Document(_))
        ));

        let mut unknown = reading(leaving());
        unknown.step(rows(Some(&document(1)), false));
        unknown.step(written());
        unknown.step(storage(StorageEvent::Error {
            error: StorageError::CommitFailed,
        }));
        assert_eq!(unknown.finalize(), Err(StorageError::CommitFailed.into()));
    }

    #[test]
    fn rejects_stray_events() {
        let open = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::BatchReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (label, effects, error) in rejects_events(|| reading(leaving()), sample_events, open) {
            assert_eq!(effects.to_vec(), vec![rollback()], "{label}");
            assert!(
                matches!(error, InfoWriteError::UnexpectedEvent(_)),
                "{label}"
            );
        }
        let fresh = || revise(leaving());
        assert_eq!(
            rejects_events(fresh, sample_events, |_| false).len(),
            sample_events().len()
        );
    }

    #[test]
    fn abort_releases_transaction() {
        let mut operation = revise(leaving());
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
        let mut open = reading(leaving());
        check_abort(&mut open, &[rollback()]);
        open.step(rows(Some(&document(1)), false));
        check_abort(&mut open, &[rollback()]);
        rejects_finalize(open);
    }
}
