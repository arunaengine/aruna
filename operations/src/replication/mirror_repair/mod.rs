//! Queues and retries repair work that mirrors or deletes synced bucket relationships.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod drain;
mod ensure;
pub mod kick;
pub mod record;
pub mod remove;
pub mod request;
mod scan;
mod settle;
pub mod status;
#[cfg(test)]
mod tests;

use crate::replication::sync_relationship::SyncRelationshipError;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{MIRROR_REPAIR_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::SyncRelationship;
use aruna_core::structs::storage::mirror_repair::SyncMirrorRecord;
use aruna_core::types::{Key, Value};
use thiserror::Error;

pub const REPAIR_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SyncMirrorResult {
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, Error, PartialEq)]
pub enum SyncMirrorError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Relationship(#[from] SyncRelationshipError),
    #[error("{0}")]
    Mirror(String),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for SyncMirrorError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

fn repair_read(key: &Key) -> IoCall {
    read_row(MIRROR_REPAIR_KEYSPACE, key.clone())
}

fn outgoing_read(key: &Key) -> IoCall {
    read_row(RELATIONSHIP_OUT_KEYSPACE, key.clone())
}

/// The one value a single-row read answered.
fn one_value(result: IoResult) -> Result<Option<Value>, SyncMirrorError> {
    let [value] = values::<1>(result)?;
    Ok(value)
}

fn unexpected(expected: &'static str, result: &IoResult) -> SyncMirrorError {
    SyncMirrorError::Io(IoError::Unexpected {
        expected,
        got: result.label(),
    })
}

fn decode_record(
    key: &Key,
    value: Option<Value>,
) -> Result<Option<SyncMirrorRecord>, ConversionError> {
    value
        .map(|value| SyncMirrorRecord::from_bytes(key, &value))
        .transpose()
}

/// Where a mirror change runs, seen from the local node.
#[derive(Debug, PartialEq)]
enum MirrorPeer {
    Local,
    Remote(NodeId),
}

/// A mirror is created from its source node, locally when the target is local too.
fn create_peer(
    relationship: &SyncRelationship,
    local_node: NodeId,
) -> Result<MirrorPeer, SyncMirrorError> {
    if relationship.source.node_id != local_node {
        return Err(SyncMirrorError::Mirror(
            "sync mirror creation must run on the source node".to_string(),
        ));
    }
    if relationship.target.node_id == local_node {
        return Ok(MirrorPeer::Local);
    }
    Ok(MirrorPeer::Remote(relationship.target.node_id))
}

/// A mirror is deleted from either endpoint; a rollback runs only on the source.
fn delete_peer(
    relationship: &SyncRelationship,
    local_node: NodeId,
    rollback: bool,
) -> Result<MirrorPeer, SyncMirrorError> {
    let (source, target) = (relationship.source.node_id, relationship.target.node_id);
    if source == local_node && target == local_node {
        Ok(MirrorPeer::Local)
    } else if source == local_node {
        Ok(MirrorPeer::Remote(target))
    } else if target == local_node && !rollback {
        Ok(MirrorPeer::Remote(source))
    } else {
        Err(SyncMirrorError::Mirror(
            "sync mirror deletion must run on an endpoint node".to_string(),
        ))
    }
}

pub(crate) fn same_relationship(left: &SyncRelationship, right: &SyncRelationship) -> bool {
    left.id == right.id
        && left.source == right.source
        && left.target == right.target
        && left.mode == right.mode
        && left.reference_handling == right.reference_handling
        && left.reference_serving == right.reference_serving
        && left.replicate_deletes == right.replicate_deletes
        && left.created_by == right.created_by
        && left.created_at == right.created_at
}
