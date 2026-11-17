//! Adds sources to or removes sources from the reference versions under a key prefix, one page
//! of version rows per call. Rows change in place: version ids, observations and heads stay.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE, SOURCE_INDEX_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::{SourceMapping, SourceMirror, VersionSourceBinding};
use aruna_core::structs::execution::table::mirror_path;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::{
    BlobVersion, BlobVersionState, BucketIdentity, BucketInfo, VersionKey,
};
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::permission_rules::PermissionRules;
use crate::connectors::descriptor::build_source_descriptor;
use crate::connectors::repository::{connector_reference_write, source_connector_key};

#[derive(Clone, Debug, PartialEq)]
pub enum SourceChange {
    /// Adds the connector's path for each source path the mapping covers, when the caller's
    /// `rules` allow READ on it below `read_root`, the connector's permission path.
    Add {
        mapping: SourceMapping,
        connector: SourceConnector,
        rules: PermissionRules,
        read_root: String,
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
    /// The bucket as authorized; a bucket recreated under the name since then is not edited.
    pub expected_bucket: BucketIdentity,
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
    /// Reference versions left alone, as the caller may not read their new source or its
    /// path is not canonical.
    pub denied: u64,
    /// Where the next page starts; `None` once every row was read.
    pub next: Option<Vec<u8>>,
}

#[derive(Debug, Error, PartialEq)]
pub enum SourceEditError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("the bucket changed since the edit was authorized")]
    BucketChanged,
    #[error("the connector changed since the edit was authorized")]
    ConnectorChanged,
    #[error("the source edit failed: {0}")]
    Io(IoError),
}

impl From<IoError> for SourceEditError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::Io(other),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceEditState {
    Guards,
    Scan,
    Write,
}

type Step = Result<Next<SourceEditResult>, SourceEditError>;

#[derive(Debug, PartialEq)]
pub struct SourceEditOperation {
    input: SourceEditInput,
    state: SourceEditState,
    result: SourceEditResult,
}

impl SourceEditOperation {
    pub fn new(input: SourceEditInput) -> Self {
        Self {
            input,
            state: SourceEditState::Guards,
            result: SourceEditResult::default(),
        }
    }

    /// Reads the bucket and the new source's connector in the edit transaction.
    fn read_guards(&mut self) -> Step {
        let bucket = ByteView::from(self.input.bucket.as_bytes().to_vec());
        let mut reads = vec![(S3_BUCKET_KEYSPACE.to_string(), bucket)];
        if let SourceChange::Add { connector, .. } = &self.input.change {
            let key = source_connector_key(connector.group_id, connector.connector_id);
            reads.push((SOURCE_INDEX_KEYSPACE.to_string(), key));
        }
        self.state = SourceEditState::Guards;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn check_guards(&mut self, values: Vec<Option<Value>>) -> Result<(), SourceEditError> {
        let mut values = values.into_iter();
        let bucket = values.next().flatten();
        let bucket = bucket.map(|value| BucketInfo::from_bytes(value.as_ref()));
        match bucket.transpose()? {
            Some(bucket)
                if bucket.identity() == self.input.expected_bucket
                    && bucket.distribution.is_none() => {}
            _ => return Err(SourceEditError::BucketChanged),
        }
        if let SourceChange::Add { connector, .. } = &self.input.change {
            let found = values.next().flatten();
            let found = found.map(|value| SourceConnector::from_bytes(value.as_ref()));
            if found.transpose()?.as_ref() != Some(connector) {
                return Err(SourceEditError::ConnectorChanged);
            }
        }
        Ok(())
    }

    fn scan(&mut self) -> Step {
        let input = &self.input;
        let prefix = match input.exact {
            true => VersionKey::object_prefix(&input.bucket, &input.prefix)?,
            false => VersionKey::bucket_prefix(&input.bucket)?,
        };
        let after = (input.after.clone()).map(Key::from);
        let limit = input.page;
        self.state = SourceEditState::Scan;
        Ok(Next::Call(IoCall::Page {
            space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after,
            limit,
        }))
    }

    fn edit_page(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Step {
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
                SourceChange::Add {
                    mapping,
                    connector,
                    rules,
                    read_root,
                } => {
                    let readable = |path: &str| {
                        rules.allows(&format!("{read_root}/{path}"), &Permission::READ)
                    };
                    let rule = (mapping, connector);
                    match added(source, cached_metadata, rule, self.input.node_id, readable) {
                        Addition::Added => true,
                        Addition::Denied => {
                            self.result.denied += 1;
                            false
                        }
                        Addition::None => false,
                    }
                }
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
        if writes.is_empty() {
            return Ok(Next::Done(self.result.clone()));
        }
        self.state = SourceEditState::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Addition {
    Added,
    /// The caller may not read the new source, or its path is not canonical.
    Denied,
    None,
}

/// Adds the mapped path of the first source the mapping covers, unless the binding has it or
/// `readable` refuses it.
fn added(
    source: &mut VersionSourceBinding,
    observed: &SourceMetadata,
    (mapping, connector): (&SourceMapping, &SourceConnector),
    node_id: NodeId,
    readable: impl Fn(&str) -> bool,
) -> Addition {
    let paths = (0..source.source_count()).filter_map(|slot| source.source_at(slot));
    let Some(mapped) = paths
        .map(|single| single.descriptor.source_path)
        .find_map(|path| mapping.map(&path))
    else {
        return Addition::None;
    };
    // The path the READ check names is the one stored and read.
    let Ok(path) = mirror_path(&mapped, None) else {
        return Addition::Denied;
    };
    let has = (0..source.source_count())
        .filter_map(|slot| source.source_at(slot))
        .any(|single| {
            single.connector_id == Some(connector.connector_id)
                && single.descriptor.source_path == path
        });
    if has {
        return Addition::None;
    }
    if !readable(&path) {
        return Addition::Denied;
    }
    let mut descriptor = build_source_descriptor(connector, observed, path, Some(node_id));
    // A version id names one server's copy, never another's.
    descriptor.version_selector = None;
    source.mirrors.push(SourceMirror {
        descriptor,
        connector_id: Some(connector.connector_id),
    });
    Addition::Added
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

    fn first(&mut self) -> Step {
        self.read_guards()
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            SourceEditState::Guards => {
                self.check_guards(result.values()?)?;
                self.scan()
            }
            SourceEditState::Scan => {
                let (values, next) = result.page()?;
                self.edit_page(values, next)
            }
            SourceEditState::Write => {
                result.done()?;
                Ok(Next::Done(self.result.clone()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
#[path = "edit_tests.rs"]
mod tests;
