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

use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{QUARANTINE_USAGE_KEYSPACE, SYNC_QUARANTINE_KEYSPACE};
use aruna_core::rows::read_row;
use aruna_core::structs::{QUARANTINE_USAGE_KEY, SyncQuarantineRecord, SyncQuarantineUsage};
use aruna_core::types::{Key, Value};
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
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for QuarantineAdminError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::Io(other),
        }
    }
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
fn page_call(request: &QuarantinePageRequest) -> IoCall {
    let limit = request
        .limit
        .unwrap_or(QUARANTINE_PAGE_DEFAULT)
        .clamp(1, QUARANTINE_PAGE_MAX);
    IoCall::Page {
        space: SYNC_QUARANTINE_KEYSPACE.to_string(),
        prefix: ByteView::from(request.topic.clone().unwrap_or_default()),
        after: request.start_after.as_deref().map(ByteView::from),
        limit,
    }
}

/// The rows of the page that answered [`page_call`] and the key to continue after.
fn page_rows(result: IoResult) -> Result<(Vec<(Key, Value)>, Option<Vec<u8>>), IoError> {
    match result {
        IoResult::Page { rows, next } => Ok((rows, next.map(|key| key.as_ref().to_vec()))),
        other => Err(IoError::Unexpected {
            expected: "IoResult::Page",
            got: other.label(),
        }),
    }
}

fn usage_read() -> IoCall {
    read_row(
        QUARANTINE_USAGE_KEYSPACE,
        ByteView::from(QUARANTINE_USAGE_KEY),
    )
}

fn row_read(key: &[u8]) -> IoCall {
    read_row(SYNC_QUARANTINE_KEYSPACE, ByteView::from(key))
}

fn decode_usage(value: Option<Value>) -> Result<SyncQuarantineUsage, QuarantineAdminError> {
    match value {
        Some(bytes) => Ok(SyncQuarantineUsage::from_bytes(bytes.as_ref())?),
        None => Ok(SyncQuarantineUsage::default()),
    }
}

#[cfg(test)]
mod tests;
