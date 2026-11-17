//! Lists how files get into a bucket, from configuration only: syncs that copy into it and
//! storage folders registered into it. Outgoing syncs and member nodes are not sources.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Nothing here reads the bucket's objects, so the cost never grows with the bucket.

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{BACKEND_IMPORT_KEYSPACE, RELATIONSHIP_IN_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::{SyncMode, SyncRelationship, sync_relationship_prefix};
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use ulid::Ulid;

/// At most this many rows of each kind are read.
const MAX_ROWS: usize = 256;

/// One incoming path of a bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileSource {
    /// Another bucket's files are copied in by a sync.
    Sync {
        relationship_id: Ulid,
        source: String,
        mode: SyncMode,
    },
    /// A storage backend folder of the group is registered in place.
    Storage {
        import_id: Ulid,
        backend_id: Ulid,
        backend_prefix: String,
        key_prefix: String,
    },
}

#[derive(Debug, Error, PartialEq)]
pub enum SourcesError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("could not list bucket sources: {0}")]
    Io(IoError),
}

impl From<IoError> for SourcesError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

/// The incoming paths of `bucket`, owned by `group_id`, and whether every row was read.
#[derive(Debug, PartialEq)]
pub struct BucketSourcesOperation {
    group_id: GroupId,
    bucket: String,
    /// Whether the sync rows were read and the import rows are next.
    syncs_read: bool,
    sources: Vec<FileSource>,
    complete: bool,
}

impl BucketSourcesOperation {
    pub fn new(group_id: GroupId, bucket: &str) -> Self {
        Self {
            group_id,
            bucket: bucket.to_string(),
            syncs_read: false,
            sources: Vec::new(),
            complete: true,
        }
    }

    fn sync_rows(&mut self, values: Vec<(Key, Value)>) -> Result<(), SourcesError> {
        for (_, value) in values {
            let relationship = SyncRelationship::from_bytes(&value)?;
            self.sources.push(FileSource::Sync {
                relationship_id: relationship.id,
                source: relationship.source.to_string(),
                mode: relationship.mode,
            });
        }
        Ok(())
    }

    fn import_rows(&mut self, values: Vec<(Key, Value)>) -> Result<(), SourcesError> {
        for (_, value) in values {
            let import = BackendImport::from_bytes(&value)?;
            if import.bucket == self.bucket {
                self.sources.push(FileSource::Storage {
                    import_id: import.import_id,
                    backend_id: import.backend_id,
                    backend_prefix: import.backend_prefix,
                    key_prefix: import.key_prefix,
                });
            }
        }
        Ok(())
    }
}

impl Operation for BucketSourcesOperation {
    type Output = (Vec<FileSource>, bool);
    type Error = SourcesError;

    fn first(&mut self) -> Result<Next<Self::Output>, SourcesError> {
        let prefix = sync_relationship_prefix(&self.bucket);
        Ok(Next::Call(rows(RELATIONSHIP_IN_KEYSPACE, prefix)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SourcesError> {
        let (values, next) = result.page()?;
        self.complete &= next.is_none();
        if self.syncs_read {
            self.import_rows(values)?;
            let sources = std::mem::take(&mut self.sources);
            return Ok(Next::Done((sources, self.complete)));
        }
        self.sync_rows(values)?;
        self.syncs_read = true;
        let prefix = self.group_id.to_bytes().to_vec();
        Ok(Next::Call(rows(BACKEND_IMPORT_KEYSPACE, prefix)))
    }
}

/// One bounded read of the rows under `prefix`.
fn rows(key_space: &str, prefix: Vec<u8>) -> IoCall {
    IoCall::Page {
        space: key_space.to_string(),
        prefix: prefix.into(),
        after: None,
        limit: MAX_ROWS,
    }
}

#[cfg(test)]
#[path = "sources_tests.rs"]
mod tests;
