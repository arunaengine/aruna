//! Reads the persisted info documents of a set of nodes in one batch read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NODE_INFO_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::node_info::{NodeInfoDocument, node_info_key};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ReadInfoError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("a node info document is unreadable: {0}")]
    Decode(String),
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
    result: Option<Result<BTreeMap<NodeId, NodeInfoDocument>, ReadInfoError>>,
}

impl ReadInfoOperation {
    pub fn new(node_ids: Vec<NodeId>) -> Self {
        Self {
            node_ids,
            started: false,
            result: None,
        }
    }

    fn decide(&self, event: Event) -> Result<BTreeMap<NodeId, NodeInfoDocument>, ReadInfoError> {
        if !self.started {
            return Err(ReadInfoError::UnexpectedEvent);
        }
        let values = match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => values,
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            _ => return Err(ReadInfoError::UnexpectedEvent),
        };
        if values.len() != self.node_ids.len() {
            return Err(ReadInfoError::UnexpectedEvent);
        }
        let mut documents = BTreeMap::new();
        for (node_id, (_, value)) in self.node_ids.iter().zip(values) {
            let Some(bytes) = value else {
                continue;
            };
            let document = postcard::from_bytes::<NodeInfoDocument>(bytes.as_ref())
                .map_err(|error| ReadInfoError::Decode(error.to_string()))?;
            documents.insert(*node_id, document);
        }
        Ok(documents)
    }
}

impl Operation for ReadInfoOperation {
    type Output = BTreeMap<NodeId, NodeInfoDocument>;
    type Error = ReadInfoError;

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
            Some(_) => Err(ReadInfoError::UnexpectedEvent),
            None => self.decide(event),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.result.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.result.unwrap_or(Err(ReadInfoError::Unfinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::storage::node_info::{NodeUrls, NodeUtilization};
    use aruna_core::types::Value;

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
        assert!(matches!(garbled.finalize(), Err(ReadInfoError::Decode(_))));

        let mut short = ReadInfoOperation::new(vec![node(1), node(2)]);
        short.start();
        short.step(answer(vec![None]));
        assert_eq!(short.finalize(), Err(ReadInfoError::UnexpectedEvent));
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
            assert_eq!(error, ReadInfoError::UnexpectedEvent, "{label}");
        }
        let fresh = || ReadInfoOperation::new(vec![node(1)]);
        let refused = rejects_events(fresh, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
        let mut answered = started();
        answered.step(answer(vec![None]));
        answered.step(answer(vec![None]));
        assert_eq!(answered.finalize(), Err(ReadInfoError::UnexpectedEvent));
    }

    #[test]
    fn abort_and_finalize() {
        let mut operation = ReadInfoOperation::new(vec![node(1)]);
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
        rejects_finalize(operation);
    }
}
