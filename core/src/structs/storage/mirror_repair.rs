//! The repair rows that mirror or delete a synced bucket relationship on its target.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use byteview::ByteView;
use serde::{Deserialize, Serialize};

use crate::errors::ConversionError;
use crate::keyspaces::MIRROR_REPAIR_KEYSPACE;
use crate::structs::SyncRelationship;
use crate::types::{Key, KeySpace, Value};

pub const RECONCILE_GRACE: Duration = Duration::from_secs(30);

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
    pub fn new(relationship: SyncRelationship, intent: SyncMirrorIntent, now_ms: u64) -> Self {
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

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        self.relationship.validate()?;
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, ConversionError> {
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

pub fn repair_entry(record: SyncMirrorRecord) -> Result<(KeySpace, Key, Value), ConversionError> {
    let value = record.to_bytes()?;
    Ok((
        MIRROR_REPAIR_KEYSPACE.to_string(),
        repair_key(&record.relationship),
        ByteView::from(value),
    ))
}

/// The repair record key of a relationship: its id.
pub fn repair_key(relationship: &SyncRelationship) -> Key {
    ByteView::from(relationship.id.to_bytes().to_vec())
}
