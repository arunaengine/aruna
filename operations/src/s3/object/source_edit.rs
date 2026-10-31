//! Adds sources to or removes sources from the reference versions under a key prefix, one page
//! of version rows per call. Rows change in place: version ids, observations and heads stay.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::{SourceMapping, SourceMirror, VersionSourceBinding};
use aruna_core::structs::storage::blob::{BlobVersion, BlobVersionState, VersionKey};
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::connector_reference_write;
use crate::staging::descriptor::build_source_descriptor;

#[derive(Clone, Debug, PartialEq)]
pub enum SourceChange {
    /// Adds the connector's path for each source path the mapping covers.
    Add {
        mapping: SourceMapping,
        connector: SourceConnector,
    },
    /// Removes the sources below `from`, of `connector_id` when given; never the last one.
    Remove {
        connector_id: Option<Ulid>,
        from: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct SourceEditInput {
    pub node_id: NodeId,
    pub bucket: String,
    /// Keys starting with this prefix, or exactly this key when `exact`.
    pub prefix: String,
    pub exact: bool,
    pub change: SourceChange,
    /// The version row a previous page ended at.
    pub after: Option<Vec<u8>>,
    /// Version rows one page reads.
    pub page: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceEditResult {
    /// Version rows the page read.
    pub scanned: u64,
    pub changed: u64,
    /// Reference versions keeping a source the change would remove, as it is their last.
    pub kept_last: u64,
    /// Reference versions keeping their first source, as no checksums prove the next one.
    pub kept_unproven: u64,
    /// Where the next page starts; `None` once every row was read.
    pub next: Option<Vec<u8>>,
}

#[derive(Debug, Error, PartialEq)]
pub enum SourceEditError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: {received}")]
    UnexpectedEvent {
        state: SourceEditState,
        received: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceEditState {
    Init,
    Started,
    Scan,
    Write,
    Commit,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct SourceEditOperation {
    input: SourceEditInput,
    state: SourceEditState,
    txn_id: Option<TxnId>,
    result: SourceEditResult,
    output: Option<Result<SourceEditResult, SourceEditError>>,
}

impl SourceEditOperation {
    pub fn new(input: SourceEditInput) -> Self {
        Self {
            input,
            state: SourceEditState::Init,
            txn_id: None,
            result: SourceEditResult::default(),
            output: None,
        }
    }

    fn fail(&mut self, error: SourceEditError) -> Effects {
        self.state = SourceEditState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn scan(&mut self) -> Result<Effects, SourceEditError> {
        let input = &self.input;
        let prefix = match input.exact {
            true => VersionKey::object_prefix(&input.bucket, &input.prefix)?,
            false => VersionKey::bucket_prefix(&input.bucket)?,
        };
        self.state = SourceEditState::Scan;
        Ok(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: (input.after.clone()).map(|after| IterStart::After(after.into())),
            limit: input.page,
            txn_id: self.txn_id,
        })])
    }

    fn edit_page(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Effects, SourceEditError> {
        self.result.scanned = values.len() as u64;
        self.result.next = next.map(|key| key.to_vec());
        let mut writes: Vec<(KeySpace, Key, Value)> = Vec::new();
        for (key, value) in values {
            let version_key = VersionKey::from_bytes(&key)?;
            if !version_key.key.starts_with(&self.input.prefix) {
                continue;
            }
            let mut version = BlobVersion::from_bytes(&value)?;
            let BlobVersionState::Reference {
                source,
                cached_metadata,
                ..
            } = &mut version.state
            else {
                continue;
            };
            let edited = match &self.input.change {
                SourceChange::Add { mapping, connector } => added(
                    source,
                    cached_metadata,
                    (mapping, connector),
                    self.input.node_id,
                ),
                SourceChange::Remove { connector_id, from } => {
                    match removed(source, cached_metadata, *connector_id, from) {
                        Removal::Removed => true,
                        Removal::Last => {
                            self.result.kept_last += 1;
                            false
                        }
                        Removal::Unproven => {
                            self.result.kept_unproven += 1;
                            false
                        }
                        Removal::None => false,
                    }
                }
            };
            if !edited {
                continue;
            }
            self.result.changed += 1;
            writes.extend(connector_reference_write(&version, &version_key)?);
            writes.push((
                BLOB_VERSIONS_KEYSPACE.to_string(),
                key,
                version.to_bytes()?.into(),
            ));
        }
        let Some(txn_id) = self.txn_id else {
            return Err(StorageError::ReadError("no source edit transaction".to_string()).into());
        };
        if writes.is_empty() {
            self.state = SourceEditState::Commit;
            return Ok(smallvec![Effect::Storage(
                StorageEffect::CommitTransaction { txn_id }
            )]);
        }
        self.state = SourceEditState::Write;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn step_inner(&mut self, event: Event) -> Result<Effects, SourceEditError> {
        match (self.state, event) {
            (
                SourceEditState::Started,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.scan()
            }
            (
                SourceEditState::Scan,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.edit_page(values, next_start_after),
            (SourceEditState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return Err(
                        StorageError::WriteError("no source edit transaction".into()).into(),
                    );
                };
                self.state = SourceEditState::Commit;
                Ok(smallvec![Effect::Storage(
                    StorageEffect::CommitTransaction { txn_id }
                )])
            }
            (
                SourceEditState::Commit,
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.state = SourceEditState::Finish;
                self.output = Some(Ok(self.result.clone()));
                Ok(smallvec![])
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (state, event) => Err(SourceEditError::UnexpectedEvent {
                state,
                received: crate::staging::describe_event(&event),
            }),
        }
    }
}

/// Adds the mapped path of the first source the mapping covers, unless the binding has it.
fn added(
    source: &mut VersionSourceBinding,
    observed: &SourceMetadata,
    (mapping, connector): (&SourceMapping, &SourceConnector),
    node_id: NodeId,
) -> bool {
    let paths = (0..source.source_count()).filter_map(|slot| source.source_at(slot));
    let Some(path) = paths
        .map(|single| single.descriptor.source_path)
        .find_map(|path| mapping.map(&path))
    else {
        return false;
    };
    let has = (0..source.source_count())
        .filter_map(|slot| source.source_at(slot))
        .any(|single| {
            single.connector_id == Some(connector.connector_id)
                && single.descriptor.source_path == path
        });
    if has {
        return false;
    }
    let mut descriptor = build_source_descriptor(connector, observed, path, Some(node_id));
    // A version id names one server's copy, never another's.
    descriptor.version_selector = None;
    source.mirrors.push(SourceMirror {
        descriptor,
        connector_id: Some(connector.connector_id),
    });
    true
}

#[derive(Debug, Eq, PartialEq)]
enum Removal {
    Removed,
    /// Every source matched, so the binding keeps them all.
    Last,
    /// The first source matched, and no checksums prove the bytes of the next one.
    Unproven,
    None,
}

/// Removes the matching sources. When the first one goes, the next becomes first and the
/// recorded ETag and time, which were the first one's, are dropped; only recorded checksums
/// can then prove its bytes, so without them the first source stays.
fn removed(
    source: &mut VersionSourceBinding,
    observed: &mut SourceMetadata,
    connector_id: Option<Ulid>,
    from: &str,
) -> Removal {
    let matches = |single: &VersionSourceBinding| {
        connector_id.is_none_or(|id| single.connector_id == Some(id))
            && single.descriptor.source_path.starts_with(from)
    };
    let sources: Vec<_> = (0..source.source_count())
        .filter_map(|slot| source.source_at(slot))
        .collect();
    let kept: Vec<_> = sources.iter().filter(|single| !matches(single)).collect();
    if kept.len() == sources.len() {
        return Removal::None;
    }
    let Some(first) = kept.first() else {
        return Removal::Last;
    };
    let mirrors = (kept[1..].iter())
        .map(|single| SourceMirror {
            descriptor: single.descriptor.clone(),
            connector_id: single.connector_id,
        })
        .collect();
    let first_removed = matches(&sources[0]);
    if first_removed && observed.enforced_checksums().is_empty() {
        return Removal::Unproven;
    }
    *source = VersionSourceBinding {
        mirrors,
        ..(*first).clone()
    };
    if first_removed {
        observed.etag = None;
        observed.last_modified = None;
    }
    Removal::Removed
}

impl Operation for SourceEditOperation {
    type Output = SourceEditResult;
    type Error = SourceEditError;

    fn start(&mut self) -> Effects {
        self.state = SourceEditState::Started;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.step_inner(event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SourceEditState::Finish | SourceEditState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SourceEditError::UnexpectedEvent {
            state: self.state,
            received: "operation did not finish".to_string(),
        }))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.take().map_or_else(Effects::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[cfg(test)]
#[path = "source_edit_tests.rs"]
mod tests;
