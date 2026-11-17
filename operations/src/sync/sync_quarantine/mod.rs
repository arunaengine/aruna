//! Lists, inspects, acknowledges and prunes rows in the durable sync quarantine store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod acknowledge;
mod list;
mod prune;
mod read;

pub use acknowledge::AcknowledgeQuarantineOperation;
pub use list::ListQuarantineOperation;
pub use prune::PruneQuarantineOperation;
pub use read::ReadQuarantineOperation;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{QUARANTINE_USAGE_KEYSPACE, SYNC_QUARANTINE_KEYSPACE};
use aruna_core::structs::{QUARANTINE_USAGE_KEY, SyncQuarantineRecord, SyncQuarantineUsage};
use aruna_core::types::{TxnId, Value};
use byteview::ByteView;
use thiserror::Error;

const QUARANTINE_PAGE_DEFAULT: usize = 50;
const QUARANTINE_PAGE_MAX: usize = 200;

#[derive(Debug, Error, PartialEq)]
pub enum QuarantineAdminError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected storage event: expected {expected}, got {got}")]
    Unexpected { expected: &'static str, got: String },
    #[error("quarantine operation did not finish")]
    Unfinished,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QuarantinePageRequest {
    /// Exclusive cursor: the last key of the previous page.
    pub start_after: Option<Vec<u8>>,
    /// Restrict the page to one sync topic, the key's leading component.
    pub topic: Option<Vec<u8>>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantinePage {
    pub records: Vec<SyncQuarantineRecord>,
    pub next_start_after: Option<Vec<u8>>,
    pub usage: SyncQuarantineUsage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantinePruneResult {
    pub pruned: usize,
    pub scanned: usize,
    pub next_start_after: Option<Vec<u8>>,
    pub usage: SyncQuarantineUsage,
}

/// One bounded page of rows in key order (`topic || actor || actor_seq`).
fn iter_effect(request: &QuarantinePageRequest) -> Effect {
    let limit = request
        .limit
        .unwrap_or(QUARANTINE_PAGE_DEFAULT)
        .clamp(1, QUARANTINE_PAGE_MAX);
    Effect::Storage(StorageEffect::Iter {
        key_space: SYNC_QUARANTINE_KEYSPACE.to_string(),
        prefix: request.topic.as_deref().map(ByteView::from),
        start: request
            .start_after
            .as_deref()
            .map(|key| IterStart::After(ByteView::from(key))),
        limit,
        txn_id: None,
    })
}

fn usage_read(txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: QUARANTINE_USAGE_KEYSPACE.to_string(),
        key: ByteView::from(QUARANTINE_USAGE_KEY),
        txn_id,
    })
}

fn row_read(key: &[u8], txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: SYNC_QUARANTINE_KEYSPACE.to_string(),
        key: ByteView::from(key),
        txn_id,
    })
}

fn decode_usage(value: Option<Value>) -> Result<SyncQuarantineUsage, QuarantineAdminError> {
    match value {
        Some(bytes) => Ok(SyncQuarantineUsage::from_bytes(bytes.as_ref())?),
        None => Ok(SyncQuarantineUsage::default()),
    }
}

/// The error for an event a phase did not expect; storage errors keep their kind.
fn unexpected(expected: &'static str, event: Event) -> QuarantineAdminError {
    match event {
        Event::Storage(StorageEvent::Error { error }) => error.into(),
        got => QuarantineAdminError::Unexpected {
            expected,
            got: format!("{got:?}"),
        },
    }
}

#[cfg(test)]
mod tests;
