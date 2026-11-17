//! Lists how files get into a bucket, from configuration only: syncs that copy into it and
//! storage folders registered into it. Outgoing syncs and member nodes are not sources.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Nothing here reads the bucket's objects, so the cost never grows with the bucket.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BACKEND_IMPORT_KEYSPACE, RELATIONSHIP_IN_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::{SyncMode, SyncRelationship, sync_relationship_prefix};
use aruna_core::types::{Effects, GroupId, Key, Value};
use smallvec::smallvec;
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
    #[error("unexpected event while listing bucket sources: {0}")]
    UnexpectedEvent(String),
    #[error("the bucket source listing did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
enum SourcesState {
    Init,
    ReadSyncs,
    ReadImports,
    Finish,
    Error,
}

/// The incoming paths of `bucket`, owned by `group_id`, and whether every row was read.
#[derive(Debug, PartialEq)]
pub struct BucketSourcesOperation {
    group_id: GroupId,
    bucket: String,
    state: SourcesState,
    sources: Vec<FileSource>,
    complete: bool,
    error: Option<SourcesError>,
}

impl BucketSourcesOperation {
    pub fn new(group_id: GroupId, bucket: &str) -> Self {
        Self {
            group_id,
            bucket: bucket.to_string(),
            state: SourcesState::Init,
            sources: Vec::new(),
            complete: true,
            error: None,
        }
    }

    fn syncs_read(
        &mut self,
        values: Vec<(Key, Value)>,
        all: bool,
    ) -> Result<Effects, SourcesError> {
        self.complete &= all;
        for (_, value) in values {
            let relationship = SyncRelationship::from_bytes(&value)?;
            self.sources.push(FileSource::Sync {
                relationship_id: relationship.id,
                source: relationship.source.to_string(),
                mode: relationship.mode,
            });
        }
        self.state = SourcesState::ReadImports;
        Ok(smallvec![rows(
            BACKEND_IMPORT_KEYSPACE,
            self.group_id.to_bytes().to_vec()
        )])
    }

    fn imports_read(
        &mut self,
        values: Vec<(Key, Value)>,
        all: bool,
    ) -> Result<Effects, SourcesError> {
        self.complete &= all;
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
        self.state = SourcesState::Finish;
        Ok(smallvec![])
    }
}

impl Operation for BucketSourcesOperation {
    type Output = (Vec<FileSource>, bool);
    type Error = SourcesError;

    fn start(&mut self) -> Effects {
        self.state = SourcesState::ReadSyncs;
        smallvec![rows(
            RELATIONSHIP_IN_KEYSPACE,
            sync_relationship_prefix(&self.bucket)
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        let result = match (&self.state, event) {
            (
                SourcesState::ReadSyncs | SourcesState::ReadImports,
                Event::Storage(StorageEvent::Error { error }),
            ) => Err(error.into()),
            (
                SourcesState::ReadSyncs,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.syncs_read(values, next_start_after.is_none()),
            (
                SourcesState::ReadImports,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.imports_read(values, next_start_after.is_none()),
            (_, event) => Err(SourcesError::UnexpectedEvent(format!("{event:?}"))),
        };
        result.unwrap_or_else(|error| {
            self.state = SourcesState::Error;
            self.error = Some(error);
            smallvec![]
        })
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SourcesState::Finish | SourcesState::Error)
    }

    fn finalize(self) -> Result<Self::Output, SourcesError> {
        match (self.state, self.error) {
            (SourcesState::Finish, _) => Ok((self.sources, self.complete)),
            (_, Some(error)) => Err(error),
            _ => Err(SourcesError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// One bounded read of the rows under `prefix`.
fn rows(key_space: &str, prefix: Vec<u8>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: Some(prefix.into()),
        start: None,
        limit: MAX_ROWS,
        txn_id: None,
    })
}

#[cfg(test)]
#[path = "sources_tests.rs"]
mod tests;
