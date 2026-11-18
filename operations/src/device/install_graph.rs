//! Joins one holder's graph state into a device replica: installs the snapshot as an OR-Set,
//! records the graph size and stores the refreshed replica row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::keyspaces::DEVICE_GRAPH_KEYSPACE;
use aruna_core::metadata::{
    MetadataClockRelation, MetadataEffect, MetadataEvent, compare_metadata_clocks,
};
use aruna_core::operation::Operation;
use aruna_core::rows::done;
use byteview::ByteView;
use thiserror::Error;
use tracing::warn;

use super::replica::{ReplicaRecord, ReplicaState, replica_entry, replica_key};
use crate::metadata::protocol::GraphState;

#[derive(Debug, Error, PartialEq)]
pub enum InstallError {
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Installs `graph` into `replica`, synced at `now_ms`, and outputs whether the refreshed
/// replica row was stored. Local edits the holder has not seen keep their render.
#[derive(Debug, PartialEq)]
pub struct InstallGraphOperation {
    replica: ReplicaRecord,
    graph: Option<GraphState>,
    now_ms: u64,
    graph_bytes: u64,
    /// Whether the snapshot is installed and the rows are being written.
    installed: bool,
}

impl InstallGraphOperation {
    pub fn new(replica: ReplicaRecord, graph: GraphState, now_ms: u64) -> Self {
        Self {
            replica,
            graph: Some(graph),
            now_ms,
            graph_bytes: 0,
            installed: false,
        }
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

    /// The graph size row and the refreshed replica row, written together.
    fn store_rows(&mut self) -> Next<bool> {
        let document_id = self.replica.document_id;
        let Ok(row) = replica_entry(&self.replica) else {
            warn!(%document_id, "Failed to encode a device replica");
            return Next::Done(false);
        };
        let size = (
            DEVICE_GRAPH_KEYSPACE.to_string(),
            replica_key(document_id),
            ByteView::from(self.graph_bytes.to_be_bytes().to_vec()),
        );
        self.installed = true;
        Next::Call(IoCall::Write(vec![size, row]))
    }
}

impl Operation for InstallGraphOperation {
    type Output = bool;
    type Error = InstallError;

    fn first(&mut self) -> Result<Next<bool>, InstallError> {
        let Some(graph) = self.graph.take() else {
            return Ok(Next::Done(false));
        };
        let install = self.join(graph);
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            install,
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, InstallError> {
        if self.installed {
            done(result)?;
            return Ok(Next::Done(true));
        }
        match result.event()? {
            Event::Metadata(MetadataEvent::SnapshotInstalled { .. }) => Ok(self.store_rows()),
            event => {
                let document_id = self.replica.document_id;
                let event = event.label();
                warn!(%document_id, event, "Could not install a graph snapshot");
                Ok(Next::Done(false))
            }
        }
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

    fn installed() -> IoResult {
        IoResult::Event(Box::new(Event::Metadata(
            MetadataEvent::SnapshotInstalled {
                graph_iri: String::new(),
                applied: true,
            },
        )))
    }

    // The snapshot is installed, then its size and the refreshed row are written together.
    #[test]
    fn installs_and_stores() {
        let mut install = InstallGraphOperation::new(replica(), graph(1), 9);
        let mut joined = InstallGraphOperation::new(replica(), graph(1), 9);
        let snapshot = joined.join(graph(1));
        let rows = joined.store_rows();
        assert_eq!(
            install.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
                snapshot
            )))))
        );
        assert_eq!(install.next(installed()), Ok(rows));
        assert_eq!(joined.replica.last_synced_ms, Some(9));
        assert_eq!(joined.replica.displayed_jsonld, "{}");
        assert_eq!(install.next(IoResult::Done), Ok(Next::Done(true)));
    }

    // Local edits ahead of the holder keep their render; a failed install stores nothing.
    #[test]
    fn local_edits_kept() {
        let mut ahead = replica();
        ahead.local_clock.advance(ActorId::from_bytes([1u8; 32]), 2);
        ahead.displayed_jsonld = "local".to_string();
        let mut install = InstallGraphOperation::new(ahead, graph(1), 9);
        install.first().unwrap();
        assert_eq!(install.replica.displayed_jsonld, "local");
        let refused = IoResult::Event(Box::new(Event::Metadata(MetadataEvent::ValidationResult {
            graph_iri: String::new(),
        })));
        assert_eq!(install.next(refused), Ok(Next::Done(false)));
        let mut install = InstallGraphOperation::new(replica(), graph(1), 9);
        install.first().unwrap();
        assert!(install.next(IoResult::Done).is_err());
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
