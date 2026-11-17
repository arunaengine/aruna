//! Queues and retries repair work that mirrors or deletes synced bucket relationships.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod drain;
mod kick;
mod record;
mod scan;
mod settle;
mod status;
#[cfg(test)]
mod tests;

pub use drain::{
    delete_sync_mirror, process_mirror_repairs, request_mirror_create, restore_mirror_timer,
};
pub use kick::KickRepairOperation;
pub use record::MirrorRecordOperation;
pub use status::StoreStatusOperation;

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{MIRROR_REPAIR_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::SyncRelationship;
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::sync::sync_relationship::SyncRelationshipError;

pub(crate) const RECONCILE_GRACE: Duration = Duration::from_secs(30);
pub const REPAIR_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncMirrorIntent {
    Reconcile,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncMirrorRecord {
    pub relationship: SyncRelationship,
    pub intent: SyncMirrorIntent,
    pub due_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
    /// A claimed rollback: the create never activated, so only the target mirror goes.
    pub rollback: bool,
}

impl SyncMirrorRecord {
    /// A fresh record staged at `now_ms`: a delete is due at once, a reconcile after the grace.
    fn new(relationship: SyncRelationship, intent: SyncMirrorIntent, now_ms: u64) -> Self {
        let due_at_ms = match intent {
            SyncMirrorIntent::Reconcile => {
                now_ms.saturating_add(RECONCILE_GRACE.as_millis() as u64)
            }
            SyncMirrorIntent::Delete => now_ms,
        };
        Self {
            relationship,
            intent,
            due_at_ms,
            attempts: 0,
            last_error: None,
            rollback: false,
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        self.relationship.validate()?;
        Ok(postcard::to_allocvec(self)?)
    }

    pub(crate) fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, ConversionError> {
        let record: Self = postcard::from_bytes(value)?;
        record.relationship.validate()?;
        if key != record.relationship.id.to_bytes() {
            return Err(ConversionError::FromStrError(
                "sync mirror repair key does not match payload".to_string(),
            ));
        }
        Ok(record)
    }
}

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

/// The row that stages a mirror delete for `relationship` at `now_ms`.
pub fn mirror_delete_entry(
    relationship: &SyncRelationship,
    now_ms: u64,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    repair_entry(SyncMirrorRecord::new(
        relationship.clone(),
        SyncMirrorIntent::Delete,
        now_ms,
    ))
}

fn repair_entry(record: SyncMirrorRecord) -> Result<(KeySpace, Key, Value), ConversionError> {
    let value = record.to_bytes()?;
    Ok((
        MIRROR_REPAIR_KEYSPACE.to_string(),
        repair_key(&record.relationship),
        ByteView::from(value),
    ))
}

/// The repair record key of a relationship: its id.
pub(crate) fn repair_key(relationship: &SyncRelationship) -> Key {
    ByteView::from(relationship.id.to_bytes().to_vec())
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
