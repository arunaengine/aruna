//! Reads this node's persisted state rows: node info documents and the compute drain state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use crate::NodeId;
use crate::calls::{IoCall, IoError, IoResult, Next};
use crate::compute::quota::ComputeDepartureReport;
use crate::keyspaces::{COMPUTE_DEPARTURE_KEYSPACE, NODE_INFO_KEYSPACE};
use crate::operation::Operation;
use crate::rows::{done, values};
use crate::structs::storage::node_info::{NodeInfoDocument, node_info_key};
use crate::types::{Key, KeySpace, Value};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum NodeInfoError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("a node info document is unreadable: {0}")]
    Decode(String),
    #[error("a node info document breaks its bounds: {0}")]
    Invalid(String),
}

/// The info document rows of `node_ids`, in this order.
fn info_reads(node_ids: impl Iterator<Item = NodeId>) -> Vec<(KeySpace, Key)> {
    node_ids
        .map(|node_id| {
            (
                NODE_INFO_KEYSPACE.to_string(),
                Key::from(node_info_key(node_id)),
            )
        })
        .collect()
}

/// The info documents of `node_ids` that exist, keyed by node id.
#[derive(Debug, PartialEq)]
pub struct ReadInfoOperation {
    node_ids: Vec<NodeId>,
}

impl ReadInfoOperation {
    pub fn new(node_ids: Vec<NodeId>) -> Self {
        Self { node_ids }
    }
}

impl Operation for ReadInfoOperation {
    type Output = BTreeMap<NodeId, NodeInfoDocument>;
    type Error = NodeInfoError;

    fn first(&mut self) -> Result<Next<Self::Output>, NodeInfoError> {
        let reads = info_reads(self.node_ids.iter().copied());
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, NodeInfoError> {
        let IoResult::Values(values) = result else {
            return Err(IoError::Unexpected {
                expected: "IoResult::Values",
                got: "another result",
            }
            .into());
        };
        let mut documents = BTreeMap::new();
        for (node_id, value) in self.node_ids.iter().zip(values) {
            let Some(bytes) = value else {
                continue;
            };
            let document = postcard::from_bytes::<NodeInfoDocument>(bytes.as_ref())
                .map_err(|error| NodeInfoError::Decode(error.to_string()))?;
            documents.insert(*node_id, document);
        }
        Ok(Next::Done(documents))
    }
}

/// Installs node info documents fetched from the realm: one batch read, then one batch write of
/// those that differ from the stored rows. Nothing is written when one is unreadable or invalid.
#[derive(Debug, PartialEq)]
pub struct InstallInfoOperation {
    documents: Vec<NodeInfoDocument>,
}

impl InstallInfoOperation {
    pub fn new(documents: Vec<NodeInfoDocument>) -> Self {
        Self { documents }
    }

    fn writes(
        &self,
        values: Vec<Option<Value>>,
    ) -> Result<Vec<(KeySpace, Key, Value)>, NodeInfoError> {
        let mut writes = Vec::new();
        for (document, stored) in self.documents.iter().zip(values) {
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
        Ok(writes)
    }
}

impl Operation for InstallInfoOperation {
    type Output = ();
    type Error = NodeInfoError;

    fn first(&mut self) -> Result<Next<()>, NodeInfoError> {
        let reads = info_reads(self.documents.iter().map(|document| document.node_id));
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, NodeInfoError> {
        match result {
            IoResult::Values(values) => Ok(Next::Call(IoCall::Write(self.writes(values)?))),
            IoResult::Done => Ok(Next::Done(())),
            _ => Err(IoError::Unexpected {
                expected: "IoResult::Values or IoResult::Done",
                got: "another result",
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod info_tests {
    use crate::structs::storage::node_info::{NodeUrls, NodeUtilization};

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

    fn reads(node_ids: &[NodeId]) -> Next<BTreeMap<NodeId, NodeInfoDocument>> {
        Next::Call(IoCall::Read(info_reads(node_ids.iter().copied())))
    }

    fn row(document: &NodeInfoDocument) -> Option<Value> {
        Some(Value::from(postcard::to_allocvec(document).unwrap()))
    }

    fn garbled() -> Option<Value> {
        Some(Value::from(vec![0xff; 3]))
    }

    #[test]
    fn keeps_existing_documents() {
        let mut operation = ReadInfoOperation::new(vec![node(1), node(2)]);
        assert_eq!(operation.first(), Ok(reads(&[node(1), node(2)])));
        let answer = IoResult::Values(vec![None, row(&document(node(2)))]);
        let documents = BTreeMap::from([(node(2), document(node(2)))]);
        assert_eq!(operation.next(answer), Ok(Next::Done(documents)));
        let mut empty = ReadInfoOperation::new(Vec::new());
        assert_eq!(empty.first(), Ok(Next::Call(IoCall::Read(Vec::new()))));
        let done = empty.next(IoResult::Values(Vec::new()));
        assert_eq!(done, Ok(Next::Done(BTreeMap::new())));
    }

    #[test]
    fn refuses_bad_answers() {
        let mut garbled_row = ReadInfoOperation::new(vec![node(1)]);
        let answer = garbled_row.next(IoResult::Values(vec![garbled()]));
        assert!(matches!(answer, Err(NodeInfoError::Decode(_))));
        let mut wrong = ReadInfoOperation::new(vec![node(1)]);
        assert!(matches!(
            wrong.next(IoResult::Done),
            Err(NodeInfoError::Io(_))
        ));
    }

    #[test]
    fn install_writes_changes() {
        let (same, changed) = (document(node(1)), document(node(2)));
        let mut stale = changed.clone();
        stale.updated_at_ms = 7;
        let mut operation = InstallInfoOperation::new(vec![same.clone(), changed.clone()]);
        let first = operation.first().unwrap();
        assert_eq!(
            first,
            Next::Call(IoCall::Read(info_reads([node(1), node(2)].into_iter())))
        );
        let write = vec![(
            NODE_INFO_KEYSPACE.to_string(),
            Key::from(node_info_key(node(2))),
            Value::from(changed.to_bytes().unwrap()),
        )];
        let answer = IoResult::Values(vec![row(&same), row(&stale)]);
        assert_eq!(operation.next(answer), Ok(Next::Call(IoCall::Write(write))));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
        // Stored rows that match write nothing; an unreadable row stops the install.
        let mut unchanged = InstallInfoOperation::new(vec![same.clone()]);
        let answer = unchanged.next(IoResult::Values(vec![row(&same)]));
        assert_eq!(answer, Ok(Next::Call(IoCall::Write(Vec::new()))));
        let mut stopped = InstallInfoOperation::new(vec![same]);
        let answer = stopped.next(IoResult::Values(vec![garbled()]));
        assert!(matches!(answer, Err(NodeInfoError::Decode(_))));
        assert!(matches!(
            stopped.next(IoResult::Woke),
            Err(NodeInfoError::Io(_))
        ));
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
    Io(#[from] IoError),
    #[error("the departure report is unreadable: {0}")]
    Decode(String),
}

/// Whether an operator drains this node's compute plane, and the report of its last departure.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainState {
    pub operator_draining: bool,
    pub departure: Option<ComputeDepartureReport>,
}

/// Reads both drain rows in one batch read.
#[derive(Debug, Default, PartialEq)]
pub struct DrainStateOperation;

impl DrainStateOperation {
    pub fn new() -> Self {
        Self
    }
}

/// The operator drain row, then the departure report row.
fn drain_reads() -> Vec<(KeySpace, Key)> {
    let read = |key: &[u8]| {
        (
            COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            Key::from(key.to_vec()),
        )
    };
    vec![read(OPERATOR_DRAIN_KEY), read(DEPARTURE_KEY)]
}

impl Operation for DrainStateOperation {
    type Output = DrainState;
    type Error = DrainStateError;

    fn first(&mut self) -> Result<Next<DrainState>, DrainStateError> {
        Ok(Next::Call(IoCall::Read(drain_reads())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DrainState>, DrainStateError> {
        let IoResult::Values(values) = result else {
            return Err(IoError::Unexpected {
                expected: "IoResult::Values",
                got: "another result",
            }
            .into());
        };
        let mut values = values.into_iter();
        let (drain, departure) = (values.next().flatten(), values.next().flatten());
        let departure = departure
            .map(|bytes| postcard::from_bytes::<ComputeDepartureReport>(bytes.as_ref()))
            .transpose()
            .map_err(|error| DrainStateError::Decode(error.to_string()))?;
        Ok(Next::Done(DrainState {
            operator_draining: drain.is_some(),
            departure,
        }))
    }
}

/// Sets or clears the operator drain flag. True when the flag changed; a matching flag writes
/// nothing.
#[derive(Debug, PartialEq)]
pub struct SetDrainOperation {
    draining: bool,
    writing: bool,
}

impl SetDrainOperation {
    pub fn new(draining: bool) -> Self {
        Self {
            draining,
            writing: false,
        }
    }
}

fn drain_row() -> (KeySpace, Key) {
    (
        COMPUTE_DEPARTURE_KEYSPACE.to_string(),
        Key::from(OPERATOR_DRAIN_KEY.to_vec()),
    )
}

impl Operation for SetDrainOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(IoCall::Read(vec![drain_row()])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        if self.writing {
            return done(result).map(|()| Next::Done(true));
        }
        let [flag] = values::<1>(result)?;
        if flag.is_some() == self.draining {
            return Ok(Next::Done(false));
        }
        self.writing = true;
        let call = match self.draining {
            true => {
                let (space, key) = drain_row();
                IoCall::Write(vec![(space, key, Value::from(vec![1u8]))])
            }
            false => IoCall::Delete(vec![drain_row()]),
        };
        Ok(Next::Call(call))
    }
}

#[cfg(test)]
mod drain_tests {
    use ulid::Ulid;

    use super::*;

    fn report() -> ComputeDepartureReport {
        ComputeDepartureReport {
            departed_at_ms: 5,
            membership_generation: 2,
            unresolved: vec![Ulid::from_parts(1, 1)],
            truncated: false,
        }
    }

    #[test]
    fn reads_both_rows() {
        let encoded = Value::from(postcard::to_allocvec(&report()).unwrap());
        let cases = [
            (vec![None, None], DrainState::default()),
            (
                vec![Some(Value::from(vec![1])), Some(encoded)],
                DrainState {
                    operator_draining: true,
                    departure: Some(report()),
                },
            ),
        ];
        for (values, expected) in cases {
            let mut operation = DrainStateOperation::new();
            let read = Next::Call(IoCall::Read(drain_reads()));
            assert_eq!(operation.first(), Ok(read));
            let answer = operation.next(IoResult::Values(values));
            assert_eq!(answer, Ok(Next::Done(expected)));
        }
    }

    #[test]
    fn refuses_bad_answers() {
        let mut garbled = DrainStateOperation::new();
        let values = vec![None, Some(Value::from(vec![0xff; 3]))];
        let answer = garbled.next(IoResult::Values(values));
        assert!(matches!(answer, Err(DrainStateError::Decode(_))));
        let mut wrong = DrainStateOperation::new();
        let answer = wrong.next(IoResult::Done);
        assert!(matches!(answer, Err(DrainStateError::Io(_))));
    }

    #[test]
    fn sets_flag_once() {
        // A changed flag is written or deleted; a matching one ends without a write.
        let flag = || IoResult::Values(vec![Some(Value::from(vec![1]))]);
        let unset = || IoResult::Values(vec![None]);
        let mut drain = SetDrainOperation::new(true);
        assert_eq!(drain.first(), Ok(Next::Call(IoCall::Read(vec![drain_row()]))));
        let (space, key) = drain_row();
        let write = IoCall::Write(vec![(space, key, Value::from(vec![1]))]);
        assert_eq!(drain.next(unset()), Ok(Next::Call(write)));
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(true)));
        let mut release = SetDrainOperation::new(false);
        let delete = IoCall::Delete(vec![drain_row()]);
        assert_eq!(release.next(flag()), Ok(Next::Call(delete)));
        assert_eq!(release.next(IoResult::Done), Ok(Next::Done(true)));
        assert_eq!(SetDrainOperation::new(true).next(flag()), Ok(Next::Done(false)));
        assert_eq!(SetDrainOperation::new(false).next(unset()), Ok(Next::Done(false)));
    }
}
