//! Writes this node's info document in one transaction, as a seed or as a revision of the row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::ExecutorCapability;
use aruna_core::compute::quota::{ComputeDemandSnapshot, ComputeReservationSnapshot};
use aruna_core::document::{DocumentChange, DocumentTarget};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    COMPUTE_DEPARTURE_KEYSPACE, NODE_INFO_KEYSPACE, SYNC_REVISION_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::storage_entries::sync_revision_key;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::node_info::{
    AdvertisementEpoch, NodeInfoDocument, NodeUtilization, node_info_key,
};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;

use aruna_core::node::state::OPERATOR_DRAIN_KEY;

/// The newest advertisement revision not yet handed to replication. It is written in the same
/// transaction as that revision, so a failure after the commit only delays the publish.
pub(crate) const PUBLISH_OWED_KEY: &[u8] = b"publish_owed";

#[derive(Debug, Error, PartialEq)]
pub enum InfoWriteError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("the node info document is invalid: {0}")]
    Document(String),
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

#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase {
    Read,
    Write,
}

/// The stored row after a write, and whether this write changed it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InfoWritten {
    pub(crate) written: bool,
    pub(crate) document: Option<NodeInfoDocument>,
}

type Turn = Result<Next<InfoWritten>, InfoWriteError>;

/// Writes the info row and its owed revision marker in one transaction. A revision reads the
/// membership generation, the row and the operator drain in one read inside it, so no publisher
/// carries a stale drain forward; its lost conflict is retried by the runner.
#[derive(Debug, PartialEq)]
pub(crate) struct InfoWriteOperation {
    realm_id: RealmId,
    write: Write,
    now_ms: u64,
    generation: u64,
    written: Option<InfoWritten>,
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
            written: None,
            phase: Phase::Read,
        }
    }

    fn writes(document: &NodeInfoDocument) -> Result<Vec<(KeySpace, Key, Value)>, InfoWriteError> {
        // `to_bytes` validates, so an advertisement that broke its own bounds is never stored.
        let row = document
            .to_bytes()
            .map_err(|error| InfoWriteError::Document(error.to_string()))?;
        let owed = document.epoch.publisher_generation.to_be_bytes().to_vec();
        Ok(vec![
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
        ])
    }

    /// The realm config revision, the info row and the operator drain of the revised node.
    fn reads(&self) -> Vec<(KeySpace, Key)> {
        let node_id = match &self.write {
            Write::Seed(document) => document.node_id,
            Write::Revise { node_id, .. } => *node_id,
        };
        let target = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        vec![
            (
                SYNC_REVISION_KEYSPACE.to_string(),
                sync_revision_key(&target),
            ),
            (
                NODE_INFO_KEYSPACE.to_string(),
                Key::from(node_info_key(node_id)),
            ),
            (
                COMPUTE_DEPARTURE_KEYSPACE.to_string(),
                Key::from(OPERATOR_DRAIN_KEY.to_vec()),
            ),
        ]
    }

    /// The revised row, or the stored one unwritten when there is no row or nothing durable
    /// changed.
    fn revised(
        &self,
        row: Option<Value>,
        drain: Option<Value>,
    ) -> Result<InfoWritten, InfoWriteError> {
        let (Write::Revise { change, .. }, Some(row)) = (&self.write, row) else {
            return Ok(InfoWritten {
                written: false,
                document: None,
            });
        };
        let stored = postcard::from_bytes::<NodeInfoDocument>(row.as_ref())
            .map_err(|error| InfoWriteError::Document(error.to_string()))?;
        let mut document = stored.clone();
        change.apply(&mut document);
        // The published drain is exactly the operator's durable flag or a departure.
        document.compute_draining = drain.is_some() || document.leaving;
        document.epoch.membership_generation = self.generation;
        if durable_view(&document) == durable_view(&stored) {
            return Ok(InfoWritten {
                written: false,
                document: Some(stored),
            });
        }
        document.epoch = AdvertisementEpoch {
            membership_generation: self.generation,
            publisher_generation: document.epoch.publisher_generation.saturating_add(1),
            observed_at_ms: self.now_ms,
        };
        document.demand.epoch = document.epoch;
        document.reservation.epoch = document.epoch;
        document.updated_at_ms = self.now_ms;
        Ok(InfoWritten {
            written: true,
            document: Some(document),
        })
    }
}

/// The realm config generation of its stored revision row; an unreadable revision counts as
/// generation zero, as a missing one does.
pub(crate) fn revision_generation(revision: Option<Value>) -> u64 {
    revision
        .and_then(|bytes| postcard::from_bytes::<DocumentChange>(bytes.as_ref()).ok())
        .map_or(0, |change| change.current.generation)
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
    type Output = InfoWritten;
    type Error = InfoWriteError;

    fn first(&mut self) -> Turn {
        if let Write::Seed(document) = &self.write {
            let writes = Self::writes(document)?;
            self.written = Some(InfoWritten {
                written: true,
                document: Some(NodeInfoDocument::clone(document)),
            });
            self.phase = Phase::Write;
            return Ok(Next::Call(IoCall::Write(writes)));
        }
        Ok(Next::Call(IoCall::Read(self.reads())))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match (self.phase, result) {
            (Phase::Read, IoResult::Values(values)) => {
                let mut values = values.into_iter();
                self.generation = revision_generation(values.next().flatten());
                let (row, drain) = (values.next().flatten(), values.next().flatten());
                let revised = self.revised(row, drain)?;
                if !revised.written {
                    return Ok(Next::Done(revised));
                }
                let document = revised.document.as_ref().ok_or(IoError::Unfinished)?;
                let writes = Self::writes(document)?;
                self.written = Some(revised);
                self.phase = Phase::Write;
                Ok(Next::Call(IoCall::Write(writes)))
            }
            (Phase::Write, IoResult::Done) => {
                let written = self.written.take().ok_or(IoError::Unfinished)?;
                Ok(Next::Done(written))
            }
            _ => Err(IoError::Unexpected {
                expected: "the answer to the node info call",
                got: "another result",
            }
            .into()),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    /// Three attempts in all, as the publishers' own retry had.
    fn conflict_retries() -> u32 {
        2
    }
}

#[cfg(test)]
mod state_machine_tests {
    use aruna_core::structs::storage::node_info::NodeUrls;

    use super::*;

    const REALM: RealmId = RealmId([3; 32]);
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

    fn rows(row: Option<&NodeInfoDocument>, drained: bool) -> IoResult {
        let row = row.map(|document| Value::from(document.to_bytes().unwrap()));
        IoResult::Values(vec![None, row, drained.then(|| Value::from(vec![1]))])
    }

    fn unwritten(document: Option<NodeInfoDocument>) -> Turn {
        let written = false;
        Ok(Next::Done(InfoWritten { written, document }))
    }

    fn written(document: &NodeInfoDocument) -> Turn {
        let writes = InfoWriteOperation::writes(document).unwrap();
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn leaving() -> InfoChange {
        InfoChange::Leaving {
            leaving: true,
            reservation: ComputeReservationSnapshot::default(),
            demand: ComputeDemandSnapshot::default(),
        }
    }

    fn snapshots() -> InfoChange {
        InfoChange::Snapshots {
            reservation: ComputeReservationSnapshot::default(),
            demand: ComputeDemandSnapshot::default(),
        }
    }

    /// A revision at its one read; the revision row is absent, so generation zero.
    fn reading(change: InfoChange) -> InfoWriteOperation {
        let mut operation = InfoWriteOperation::revise(REALM, node(), change, NOW);
        let target = DocumentTarget::RealmConfig { realm_id: REALM };
        let revision = (
            SYNC_REVISION_KEYSPACE.to_string(),
            sync_revision_key(&target),
        );
        let first = operation.first();
        let Ok(Next::Call(IoCall::Read(reads))) = &first else {
            panic!("a revision reads first, got {first:?}");
        };
        assert_eq!((reads.len(), &reads[0]), (3, &revision));
        operation
    }

    #[test]
    fn seed_writes_row() {
        let seeded = document(1);
        let mut operation = InfoWriteOperation::seed(REALM, seeded.clone());
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        assert_eq!(operation.first(), written(&seeded));
        let done = InfoWritten {
            written: true,
            document: Some(seeded),
        };
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(done)));
    }

    #[test]
    fn revision_stamps_epoch() {
        let mut operation = reading(leaving());
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
        let answer = operation.next(rows(Some(&document(4)), false));
        assert_eq!(answer, written(&expected));
        let done = InfoWritten {
            written: true,
            document: Some(expected),
        };
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(done)));
    }

    #[test]
    fn unchanged_writes_nothing() {
        // No row, or a change that alters nothing durable, writes nothing and names the row.
        for (row, change) in [(None, leaving()), (Some(document(2)), snapshots())] {
            let mut operation = reading(change);
            let answer = operation.next(rows(row.as_ref(), false));
            assert_eq!(answer, unwritten(row));
        }
        // The operator's flag drains the advertisement although the change does not.
        let mut drained = reading(snapshots());
        let answer = drained.next(rows(Some(&document(2)), true));
        assert!(matches!(answer, Ok(Next::Call(IoCall::Write(_)))));
    }

    #[test]
    fn runner_retries_conflict() {
        // A lost conflict reruns the revision twice; other errors end it.
        let conflict = InfoWriteError::from(IoError::Conflict);
        assert!(InfoWriteOperation::is_conflict(&conflict));
        assert_eq!(InfoWriteOperation::conflict_retries(), 2);
        let document = InfoWriteError::Document(String::new());
        assert!(!InfoWriteOperation::is_conflict(&document));
    }

    #[test]
    fn refuses_bad_answers() {
        let mut garbled = reading(leaving());
        let row = IoResult::Values(vec![None, Some(Value::from(vec![0xff; 3])), None]);
        assert!(matches!(
            garbled.next(row),
            Err(InfoWriteError::Document(_))
        ));
        let mut early = reading(leaving());
        assert!(matches!(
            early.next(IoResult::Done),
            Err(InfoWriteError::Io(_))
        ));
    }
}
