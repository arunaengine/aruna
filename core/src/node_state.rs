//! Reads this node's persisted state rows: node info documents and the compute drain state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use crate::NodeId;
use crate::compute::quota::ComputeDepartureReport;
use crate::effects::{Effect, StorageEffect};
use crate::errors::StorageError;
use crate::events::{Event, StorageEvent};
use crate::keyspaces::{COMPUTE_DEPARTURE_KEYSPACE, NODE_INFO_KEYSPACE};
use crate::operation::Operation;
use crate::structs::storage::node_info::{NodeInfoDocument, node_info_key};
use crate::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum NodeInfoError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("a node info document is unreadable: {0}")]
    Decode(String),
    #[error("a node info document breaks its bounds: {0}")]
    Invalid(String),
    #[error("unexpected event for the node info read")]
    UnexpectedEvent,
    #[error("the node info read has not finished")]
    Unfinished,
}

/// The info documents of `node_ids` that exist, keyed by node id.
#[derive(Debug, PartialEq)]
pub struct ReadInfoOperation {
    node_ids: Vec<NodeId>,
    started: bool,
    result: Option<Result<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError>>,
}

impl ReadInfoOperation {
    pub fn new(node_ids: Vec<NodeId>) -> Self {
        Self {
            node_ids,
            started: false,
            result: None,
        }
    }

    fn decide(&self, event: Event) -> Result<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError> {
        if !self.started {
            return Err(NodeInfoError::UnexpectedEvent);
        }
        let values = match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => values,
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            _ => return Err(NodeInfoError::UnexpectedEvent),
        };
        if values.len() != self.node_ids.len() {
            return Err(NodeInfoError::UnexpectedEvent);
        }
        let mut documents = BTreeMap::new();
        for (node_id, (_, value)) in self.node_ids.iter().zip(values) {
            let Some(bytes) = value else {
                continue;
            };
            let document = postcard::from_bytes::<NodeInfoDocument>(bytes.as_ref())
                .map_err(|error| NodeInfoError::Decode(error.to_string()))?;
            documents.insert(*node_id, document);
        }
        Ok(documents)
    }
}

impl Operation for ReadInfoOperation {
    type Output = BTreeMap<NodeId, NodeInfoDocument>;
    type Error = NodeInfoError;

    fn start(&mut self) -> Effects {
        self.started = true;
        if self.node_ids.is_empty() {
            self.result = Some(Ok(BTreeMap::new()));
            return smallvec![];
        }
        let reads = self
            .node_ids
            .iter()
            .map(|node_id| {
                (
                    NODE_INFO_KEYSPACE.to_string(),
                    Key::from(node_info_key(*node_id)),
                )
            })
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.result = Some(match self.result {
            Some(_) => Err(NodeInfoError::UnexpectedEvent),
            None => self.decide(event),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.result.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.result.unwrap_or(Err(NodeInfoError::Unfinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Installs node info documents fetched from the realm: one batch read, then one batch write of
/// those that differ from the stored rows. Nothing is written when one is unreadable or invalid.
#[derive(Debug, PartialEq)]
pub struct InstallInfoOperation {
    documents: Vec<NodeInfoDocument>,
    writing: bool,
    result: Option<Result<(), NodeInfoError>>,
}

impl InstallInfoOperation {
    pub fn new(documents: Vec<NodeInfoDocument>) -> Self {
        Self {
            documents,
            writing: false,
            result: None,
        }
    }

    fn writes(&self, values: Vec<(Key, Option<Value>)>) -> Result<Effects, NodeInfoError> {
        if values.len() != self.documents.len() {
            return Err(NodeInfoError::UnexpectedEvent);
        }
        let mut writes = Vec::new();
        for (document, (_, stored)) in self.documents.iter().zip(values) {
            let stored = stored
                .map(|bytes| postcard::from_bytes::<NodeInfoDocument>(bytes.as_ref()))
                .transpose()
                .map_err(|error| NodeInfoError::Decode(error.to_string()))?;
            if stored.as_ref() == Some(document) {
                continue;
            }
            // `to_bytes` validates, so a document that broke its bounds is never stored.
            let row = document
                .to_bytes()
                .map_err(|error| NodeInfoError::Invalid(error.to_string()))?;
            let key = Key::from(node_info_key(document.node_id));
            writes.push((NODE_INFO_KEYSPACE.to_string(), key, Value::from(row)));
        }
        if writes.is_empty() {
            return Ok(smallvec![]);
        }
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })])
    }
}

impl Operation for InstallInfoOperation {
    type Output = ();
    type Error = NodeInfoError;

    fn start(&mut self) -> Effects {
        if self.documents.is_empty() {
            self.result = Some(Ok(()));
            return smallvec![];
        }
        let reads = (self.documents.iter())
            .map(|document| {
                let key = Key::from(node_info_key(document.node_id));
                (NODE_INFO_KEYSPACE.to_string(), key)
            })
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.result.is_some() {
            self.result = Some(Err(NodeInfoError::UnexpectedEvent));
            return smallvec![];
        }
        let outcome = match (self.writing, event) {
            (false, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                match self.writes(values) {
                    Ok(effects) if effects.is_empty() => Some(Ok(())),
                    Ok(effects) => {
                        self.writing = true;
                        return effects;
                    }
                    Err(error) => Some(Err(error)),
                }
            }
            (true, Event::Storage(StorageEvent::BatchWriteResult { .. })) => Some(Ok(())),
            (_, Event::Storage(StorageEvent::Error { error })) => Some(Err(error.into())),
            _ => Some(Err(NodeInfoError::UnexpectedEvent)),
        };
        self.result = outcome;
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.result.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.result.unwrap_or(Err(NodeInfoError::Unfinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod info_tests {
    use crate::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use crate::structs::storage::node_info::{NodeUrls, NodeUtilization};
    use crate::types::Value;

    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn document(node_id: NodeId) -> NodeInfoDocument {
        NodeInfoDocument {
            node_id,
            executors: Vec::new(),
            labels: Default::default(),
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
            updated_at_ms: 0,
            epoch: Default::default(),
            compute_draining: false,
            leaving: false,
            demand: Default::default(),
            reservation: Default::default(),
        }
    }

    fn reads(node_ids: &[NodeId]) -> Effect {
        Effect::Storage(StorageEffect::BatchRead {
            reads: node_ids
                .iter()
                .map(|node_id| {
                    (
                        NODE_INFO_KEYSPACE.to_string(),
                        Key::from(node_info_key(*node_id)),
                    )
                })
                .collect(),
            txn_id: None,
        })
    }

    fn answer(values: Vec<Option<Vec<u8>>>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: values
                .into_iter()
                .map(|value| (Key::from(Vec::new()), value.map(Value::from)))
                .collect(),
        })
    }

    #[test]
    fn keeps_existing_documents() {
        let stored = postcard::to_allocvec(&document(node(2))).unwrap();
        let mut operation = ReadInfoOperation::new(vec![node(1), node(2)]);
        check_trace(
            &mut operation,
            &[reads(&[node(1), node(2)])],
            vec![(answer(vec![None, Some(stored)]), vec![])],
        );
        let documents = operation.finalize().unwrap();
        assert_eq!(documents, BTreeMap::from([(node(2), document(node(2)))]));
    }

    #[test]
    fn empty_reads_nothing() {
        let mut operation = ReadInfoOperation::new(Vec::new());
        check_trace(&mut operation, &[], vec![]);
        assert_eq!(operation.finalize(), Ok(BTreeMap::new()));
    }

    #[test]
    fn failures_end_read() {
        let mut failed = ReadInfoOperation::new(vec![node(1)]);
        failed.start();
        failed.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(failed.finalize(), Err(StorageError::Timeout.into()));

        let mut garbled = ReadInfoOperation::new(vec![node(1)]);
        garbled.start();
        garbled.step(answer(vec![Some(vec![0xff; 3])]));
        assert!(matches!(garbled.finalize(), Err(NodeInfoError::Decode(_))));

        let mut short = ReadInfoOperation::new(vec![node(1), node(2)]);
        short.start();
        short.step(answer(vec![None]));
        assert_eq!(short.finalize(), Err(NodeInfoError::UnexpectedEvent));
    }

    #[test]
    fn rejects_stray_events() {
        let started = || {
            let mut operation = ReadInfoOperation::new(vec![node(1)]);
            operation.start();
            operation
        };
        let expected = |event: &Event| matches!(event, Event::Storage(StorageEvent::Error { .. }));
        for (label, effects, error) in rejects_events(started, sample_events, expected) {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert_eq!(error, NodeInfoError::UnexpectedEvent, "{label}");
        }
        let fresh = || ReadInfoOperation::new(vec![node(1)]);
        let refused = rejects_events(fresh, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
        let mut answered = started();
        answered.step(answer(vec![None]));
        answered.step(answer(vec![None]));
        assert_eq!(answered.finalize(), Err(NodeInfoError::UnexpectedEvent));
    }

    #[test]
    fn abort_and_finalize() {
        let mut operation = ReadInfoOperation::new(vec![node(1)]);
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
        rejects_finalize(operation);
    }

    #[test]
    fn install_writes_changes() {
        let (same, changed) = (document(node(1)), document(node(2)));
        let mut stale = changed.clone();
        stale.updated_at_ms = 7;
        let mut operation = InstallInfoOperation::new(vec![same.clone(), changed.clone()]);
        let stored = vec![
            Some(postcard::to_allocvec(&same).unwrap()),
            Some(postcard::to_allocvec(&stale).unwrap()),
        ];
        let write = Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![(
                NODE_INFO_KEYSPACE.to_string(),
                Key::from(node_info_key(node(2))),
                Value::from(changed.to_bytes().unwrap()),
            )],
            txn_id: None,
        });
        let written = Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        });
        check_trace(
            &mut operation,
            &[reads(&[node(1), node(2)])],
            vec![(answer(stored), vec![write]), (written, vec![])],
        );
        assert_eq!(operation.finalize(), Ok(()));
        // Stored rows that match write nothing; an unreadable row stops the install.
        let mut unchanged = InstallInfoOperation::new(vec![same.clone()]);
        unchanged.start();
        let row = Some(postcard::to_allocvec(&same).unwrap());
        assert!(unchanged.step(answer(vec![row])).is_empty());
        assert_eq!(unchanged.finalize(), Ok(()));
        let mut garbled = InstallInfoOperation::new(vec![same]);
        garbled.start();
        garbled.step(answer(vec![Some(vec![0xff; 3])]));
        assert!(matches!(garbled.finalize(), Err(NodeInfoError::Decode(_))));
    }

    #[test]
    fn install_rejects_stray() {
        let started = || {
            let mut operation = InstallInfoOperation::new(vec![document(node(1))]);
            operation.start();
            operation
        };
        let expected = |event: &Event| matches!(event, Event::Storage(StorageEvent::Error { .. }));
        for (label, effects, error) in rejects_events(started, sample_events, expected) {
            assert!(effects.is_empty(), "{label}");
            assert_eq!(error, NodeInfoError::UnexpectedEvent, "{label}");
        }
        let mut open = started();
        check_abort(&mut open, &[]);
        rejects_finalize(open);
        let mut empty = InstallInfoOperation::new(Vec::new());
        assert!(empty.start().is_empty());
        assert_eq!(empty.finalize(), Ok(()));
    }
}

/// The single durable departure-report row of this node.
pub const DEPARTURE_KEY: &[u8] = b"departure";
/// The operator's own compute drain, kept apart from an observed departure so
/// returning to placement can never silently undrain a node an operator drained.
pub const OPERATOR_DRAIN_KEY: &[u8] = b"operator_drain";

#[derive(Debug, Error, PartialEq)]
pub enum DrainStateError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("the departure report is unreadable: {0}")]
    Decode(String),
    #[error("unexpected event for the drain state")]
    UnexpectedEvent,
    #[error("the drain state was not read")]
    Unfinished,
}

/// Whether an operator drains this node's compute plane, and the report of its last departure.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainState {
    pub operator_draining: bool,
    pub departure: Option<ComputeDepartureReport>,
}

/// Reads both drain rows in one batch read.
#[derive(Debug, Default, PartialEq)]
pub struct DrainStateOperation {
    started: bool,
    result: Option<Result<DrainState, DrainStateError>>,
}

impl DrainStateOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn decide(&self, event: Event) -> Result<DrainState, DrainStateError> {
        if !self.started {
            return Err(DrainStateError::UnexpectedEvent);
        }
        let values = match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => values,
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            _ => return Err(DrainStateError::UnexpectedEvent),
        };
        let [(_, drain), (_, departure)] = values.as_slice() else {
            return Err(DrainStateError::UnexpectedEvent);
        };
        let departure = departure
            .as_ref()
            .map(|bytes| postcard::from_bytes::<ComputeDepartureReport>(bytes.as_ref()))
            .transpose()
            .map_err(|error| DrainStateError::Decode(error.to_string()))?;
        Ok(DrainState {
            operator_draining: drain.is_some(),
            departure,
        })
    }
}

impl Operation for DrainStateOperation {
    type Output = DrainState;
    type Error = DrainStateError;

    fn start(&mut self) -> Effects {
        self.started = true;
        let read = |key: &[u8]| {
            (
                COMPUTE_DEPARTURE_KEYSPACE.to_string(),
                Key::from(key.to_vec()),
            )
        };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![read(OPERATOR_DRAIN_KEY), read(DEPARTURE_KEY)],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.result = Some(match self.result {
            Some(_) => Err(DrainStateError::UnexpectedEvent),
            None => self.decide(event),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.result.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.result.unwrap_or(Err(DrainStateError::Unfinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod drain_tests {
    use crate::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use crate::types::Value;
    use ulid::Ulid;

    use super::*;

    fn batch(drain: Option<Vec<u8>>, departure: Option<Vec<u8>>) -> Event {
        let row =
            |key: &[u8], value: Option<Vec<u8>>| (Key::from(key.to_vec()), value.map(Value::from));
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                row(OPERATOR_DRAIN_KEY, drain),
                row(DEPARTURE_KEY, departure),
            ],
        })
    }

    fn report() -> ComputeDepartureReport {
        ComputeDepartureReport {
            departed_at_ms: 5,
            membership_generation: 2,
            unresolved: vec![Ulid::from_parts(1, 1)],
            truncated: false,
        }
    }

    fn read_effect() -> Effect {
        let read = |key: &[u8]| {
            (
                COMPUTE_DEPARTURE_KEYSPACE.to_string(),
                Key::from(key.to_vec()),
            )
        };
        Effect::Storage(StorageEffect::BatchRead {
            reads: vec![read(OPERATOR_DRAIN_KEY), read(DEPARTURE_KEY)],
            txn_id: None,
        })
    }

    #[test]
    fn reads_both_rows() {
        let encoded = postcard::to_allocvec(&report()).unwrap();
        let cases = [
            (batch(None, None), DrainState::default()),
            (
                batch(Some(vec![1]), Some(encoded)),
                DrainState {
                    operator_draining: true,
                    departure: Some(report()),
                },
            ),
        ];
        for (event, expected) in cases {
            let mut operation = DrainStateOperation::new();
            check_trace(&mut operation, &[read_effect()], vec![(event, vec![])]);
            assert!(operation.is_complete());
            assert_eq!(operation.finalize(), Ok(expected));
        }
    }

    #[test]
    fn failures_end_reads() {
        let mut failed = DrainStateOperation::new();
        failed.start();
        failed.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(failed.finalize(), Err(StorageError::Timeout.into()));

        let mut garbled = DrainStateOperation::new();
        garbled.start();
        garbled.step(batch(None, Some(vec![0xff; 3])));
        assert!(matches!(
            garbled.finalize(),
            Err(DrainStateError::Decode(_))
        ));

        let mut short = DrainStateOperation::new();
        short.start();
        short.step(Event::Storage(StorageEvent::BatchReadResult {
            values: Vec::new(),
        }));
        assert_eq!(short.finalize(), Err(DrainStateError::UnexpectedEvent));
    }

    #[test]
    fn rejects_stray_events() {
        let started = || {
            let mut operation = DrainStateOperation::new();
            operation.start();
            operation
        };
        let expected = |event: &Event| matches!(event, Event::Storage(StorageEvent::Error { .. }));
        for (label, effects, error) in rejects_events(started, sample_events, expected) {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert_eq!(error, DrainStateError::UnexpectedEvent, "{label}");
        }
        // Before start and after the answer, every event is refused.
        let refused = rejects_events(DrainStateOperation::new, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
        let mut answered = started();
        answered.step(batch(None, None));
        answered.step(batch(None, None));
        assert_eq!(answered.finalize(), Err(DrainStateError::UnexpectedEvent));
    }

    #[test]
    fn abort_and_finalize() {
        let mut operation = DrainStateOperation::new();
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
        rejects_finalize(operation);
    }
}
