//! Joins one holder's graph state into a device replica: installs the snapshot as an OR-Set,
//! records the graph size and stores the refreshed replica row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DEVICE_GRAPH_KEYSPACE;
use aruna_core::metadata::{
    MetadataClockRelation, MetadataEffect, MetadataEvent, compare_metadata_clocks,
};
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use super::replica::{ReplicaRecord, ReplicaState, replica_entry, replica_key};
use crate::metadata::protocol::GraphState;

#[derive(Debug, Error, PartialEq)]
pub enum InstallError {
    #[error("unexpected {0} while installing a graph state")]
    UnexpectedEvent(&'static str),
    #[error("installing a graph state did not finish")]
    NotFinished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallState {
    Init,
    Snapshot,
    Size,
    Row,
    Finish,
}

/// Installs `graph` into `replica`, synced at `now_ms`, and outputs whether the refreshed
/// replica row was stored. Local edits the holder has not seen keep their render.
#[derive(Debug, PartialEq)]
pub struct InstallGraphOperation {
    replica: ReplicaRecord,
    graph: Option<GraphState>,
    now_ms: u64,
    graph_bytes: u64,
    state: InstallState,
    output: Option<Result<bool, InstallError>>,
}

impl InstallGraphOperation {
    pub fn new(replica: ReplicaRecord, graph: GraphState, now_ms: u64) -> Self {
        Self {
            replica,
            graph: Some(graph),
            now_ms,
            graph_bytes: 0,
            state: InstallState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<bool, InstallError>) -> Effects {
        self.state = InstallState::Finish;
        self.output = Some(output);
        smallvec![]
    }

    /// Takes the holder's answer into the replica row, which is stored once the graph is in.
    fn join(&mut self, graph: GraphState) -> MetadataEffect {
        let replica = &mut self.replica;
        let clock = graph.snapshot.clock.clone();
        if !local_is_ahead(&replica.local_clock, &clock) {
            replica.displayed_jsonld = graph.displayed_jsonld;
            replica.dataset_digest = graph.dataset_digest;
        }
        replica.local_clock.merge(&clock);
        replica.realm_clock = clock;
        replica.last_synced_ms = Some(self.now_ms);
        replica.group_id = graph.record.group_id;
        replica.document_path = graph.record.document_path.clone();
        replica.graph_iri = graph.record.graph_iri.clone();
        replica.record = Some(Box::new(graph.record));
        replica.last_error = None;
        replica.findings = graph.findings;
        replica.state = match graph.findings {
            0 => ReplicaState::Synced,
            _ => ReplicaState::Invalid,
        };
        self.graph_bytes =
            postcard::to_allocvec(&graph.snapshot).map_or(0, |bytes| bytes.len() as u64);
        MetadataEffect::InstallSnapshot {
            graph_iri: replica.graph_iri.clone(),
            snapshot: Box::new(graph.snapshot),
        }
    }

    fn store_row(&mut self) -> Effects {
        let Ok((key_space, key, value)) = replica_entry(&self.replica) else {
            warn!(document_id = %self.replica.document_id, "Failed to encode a device replica");
            return self.finish(Ok(false));
        };
        self.state = InstallState::Row;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })]
    }
}

impl Operation for InstallGraphOperation {
    type Output = bool;
    type Error = InstallError;

    fn start(&mut self) -> Effects {
        let Some(graph) = self.graph.take() else {
            return self.finish(Ok(false));
        };
        let install = self.join(graph);
        self.state = InstallState::Snapshot;
        smallvec![Effect::Metadata(install)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let document_id = self.replica.document_id;
        match (self.state, event) {
            (InstallState::Snapshot, Event::Metadata(MetadataEvent::SnapshotInstalled { .. })) => {
                self.state = InstallState::Size;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: DEVICE_GRAPH_KEYSPACE.to_string(),
                    key: replica_key(document_id),
                    value: ByteView::from(self.graph_bytes.to_be_bytes().to_vec()),
                    txn_id: None,
                })]
            }
            (InstallState::Snapshot, event @ Event::Metadata(_)) => {
                let event = event.label();
                warn!(%document_id, event, "Could not install a graph snapshot");
                self.finish(Ok(false))
            }
            (InstallState::Size, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.store_row()
            }
            (InstallState::Size, Event::Storage(StorageEvent::Error { error })) => {
                warn!(%document_id, error = %error, "Failed to record a graph size");
                self.store_row()
            }
            (InstallState::Row, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.finish(Ok(true))
            }
            (InstallState::Row, Event::Storage(StorageEvent::Error { .. })) => {
                self.finish(Ok(false))
            }
            (_, event) => {
                let label = event.label();
                self.finish(Err(InstallError::UnexpectedEvent(label)))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == InstallState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(InstallError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn local_is_ahead(local: &craqle::VectorClock, remote: &craqle::VectorClock) -> bool {
    matches!(
        compare_metadata_clocks(local, remote),
        MetadataClockRelation::LocalAhead | MetadataClockRelation::Concurrent
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::replica::ReplicaOrigin;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use craqle::{ActorId, GraphId, GraphReplicaSnapshot, VectorClock};

    fn replica() -> ReplicaRecord {
        let record = crate::tests::device::record();
        let origin = ReplicaOrigin::Realm;
        ReplicaRecord::new(record.document_id, record.group_id, String::new(), origin)
    }

    fn graph(counter: u64) -> GraphState {
        let record = crate::tests::device::record();
        let mut clock = VectorClock::default();
        clock.advance(ActorId::from_bytes([1u8; 32]), counter);
        GraphState {
            snapshot: GraphReplicaSnapshot {
                graph: GraphId::new(&record.graph_iri),
                clock,
                quads: Vec::new(),
            },
            record,
            displayed_jsonld: "{}".to_string(),
            dataset_digest: Some([6; 32]),
            findings: 0,
        }
    }

    fn installed() -> Event {
        Event::Metadata(MetadataEvent::SnapshotInstalled {
            graph_iri: String::new(),
            applied: true,
        })
    }

    fn written() -> Event {
        Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        })
    }

    // The snapshot is installed, its size recorded and the refreshed row stored.
    #[test]
    fn installs_and_stores() {
        let mut install = InstallGraphOperation::new(replica(), graph(1), 9);
        let mut joined = InstallGraphOperation::new(replica(), graph(1), 9);
        let snapshot = joined.join(graph(1));
        let size = Effect::Storage(StorageEffect::Write {
            key_space: DEVICE_GRAPH_KEYSPACE.to_string(),
            key: replica_key(joined.replica.document_id),
            value: joined.graph_bytes.to_be_bytes().to_vec().into(),
            txn_id: None,
        });
        let (key_space, key, value) = replica_entry(&joined.replica).unwrap();
        let row = Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        });
        check_trace(
            &mut install,
            &[Effect::Metadata(snapshot)],
            vec![
                (installed(), vec![size]),
                (written(), vec![row]),
                (written(), Vec::new()),
            ],
        );
        assert_eq!(joined.replica.last_synced_ms, Some(9));
        assert_eq!(joined.replica.displayed_jsonld, "{}");
        assert_eq!(install.finalize(), Ok(true));
    }

    // Local edits ahead of the holder keep their render; a failed install stores nothing.
    #[test]
    fn local_edits_kept() {
        let mut ahead = replica();
        ahead.local_clock.advance(ActorId::from_bytes([1u8; 32]), 2);
        ahead.displayed_jsonld = "local".to_string();
        let mut install = InstallGraphOperation::new(ahead, graph(1), 9);
        install.start();
        assert_eq!(install.replica.displayed_jsonld, "local");
        let refused = Event::Metadata(MetadataEvent::ValidationResult {
            graph_iri: String::new(),
        });
        assert!(install.step(refused).is_empty());
        assert_eq!(install.finalize(), Ok(false));

        let mut install = InstallGraphOperation::new(replica(), graph(1), 9);
        install.start();
        install.step(installed());
        install.step(written());
        install.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(install.finalize(), Ok(false));
    }

    // Other events are refused; nothing is cleaned up and nothing finishes early.
    #[test]
    fn rejects_unexpected() {
        let fresh = || InstallGraphOperation::new(replica(), graph(1), 9);
        let all = sample_events().len();
        assert_eq!(rejects_events(fresh, sample_events, |_| false).len(), all);
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
    }

    #[test]
    fn detects_local_ahead() {
        let mut local = VectorClock::default();
        local.advance(ActorId::from_bytes([1u8; 32]), 2);
        let mut remote = VectorClock::default();
        remote.advance(ActorId::from_bytes([1u8; 32]), 1);
        assert!(local_is_ahead(&local, &remote));
        assert!(!local_is_ahead(&remote, &local));

        remote.advance(ActorId::from_bytes([2u8; 32]), 1);
        assert!(local_is_ahead(&local, &remote));
        assert!(!local_is_ahead(&local, &local));
    }
}
