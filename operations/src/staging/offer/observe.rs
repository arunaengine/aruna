//! Records one walked file of an offered directory as a reference version, and marks a file
//! that vanished as deleted, each in its own transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_runs::{SourceEvent, journaled};
use aruna_core::rows::{read_row, value};
use aruna_core::structs::execution::offered_directory::{
    FileStat, OFFERED_DIRECTORY_BUCKET, weak_fingerprint,
};
use aruna_core::structs::execution::source_access::{SourceEntry, SourceMetadata};
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

use crate::blob::records::{HeadAliasContext, build_transition_effects, write_version_effect};
use crate::s3::usage::{RowChange, UsageCounterUpdate};

use super::super::offered_directory::{ObservationScope, OfferedDirectoryError};

type OfferError = OfferedDirectoryError;

/// The binding a served object carries: the offered bucket and the file's path inside it. The
/// root is deliberately absent.
fn offered_binding(bucket: &str, path: &str, node_id: NodeId) -> VersionSourceBinding {
    VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::LocalDirectory,
            public_config: HashMap::from([(
                OFFERED_DIRECTORY_BUCKET.to_string(),
                bucket.to_string(),
            )]),
            source_path: path.to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: Some(node_id),
            namespace: None,
            root: None,
        },
        connector_id: None,
        mirrors: Vec::new(),
    }
}

/// The weak identity one listed file carries. A source that reports the full stat is
/// fingerprinted by it; size and mtime alone never stand in for reading the bytes.
pub(in crate::staging) fn entry_fingerprint(entry: &SourceEntry) -> String {
    let size = entry.size.unwrap_or_default();
    let stat = (entry.stat).unwrap_or_else(|| FileStat::partial(size, entry.modified));
    weak_fingerprint(&stat)
}

fn entry_metadata(entry: &SourceEntry) -> SourceMetadata {
    SourceMetadata {
        content_length: entry.size.unwrap_or_default(),
        content_type: None,
        etag: Some(entry_fingerprint(entry)),
        last_modified: entry.modified,
        source_version: None,
        digests: Default::default(),
    }
}

/// The row writes of one version change, then its usage update.
#[derive(Debug, PartialEq)]
struct VersionChange {
    calls: VecDeque<IoCall>,
    usage: UsageCounterUpdate,
    counting: bool,
}

impl VersionChange {
    /// The head transition to `version_id`, then `write`, then `usage`.
    fn new(
        scope: &ObservationScope,
        key: &str,
        (pointer, version_id): (Option<&CurrentVersionPointer>, Ulid),
        write: Effect,
        usage: UsageCounterUpdate,
    ) -> Result<Self, OfferError> {
        let next = CurrentVersionPointer::next_for(pointer, version_id)?;
        let alias = HeadAliasContext::new(
            scope.realm_id,
            scope.group_id,
            scope.node_id,
            &scope.bucket,
            key,
        );
        let transition = build_transition_effects(&alias, Some(next), None, None)?;
        let calls = (transition.into_iter()).chain([write]).map(IoCall::from);
        Ok(Self {
            calls: calls.collect(),
            usage,
            counting: false,
        })
    }

    fn next_call(&mut self) -> Option<IoCall> {
        if let Some(call) = self.calls.pop_front() {
            return Some(call);
        }
        if self.usage.is_noop() {
            return None;
        }
        self.counting = true;
        Some(self.usage.start_calls().into_call())
    }

    /// The call after `result`, or `None` once the change is staged.
    fn advance(&mut self, result: IoResult) -> Result<Option<IoCall>, OfferError> {
        if self.counting {
            let call = self.usage.step_calls(result)?;
            return Ok(call.map(|call| call.into_call()));
        }
        match result.event()? {
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            Event::Storage(_) => {}
            _ => return Err(StorageError::WriteError("unexpected event".to_string()).into()),
        }
        Ok(self.next_call())
    }
}

/// The head pointer and current version of one object, read in order.
#[derive(Debug, Default, PartialEq)]
struct CurrentRows {
    pointer: Option<CurrentVersionPointer>,
    pointer_read: bool,
}

impl CurrentRows {
    fn first(bucket: &str, key: &str) -> Result<IoCall, OfferError> {
        let head = BlobHeadKey::new(bucket, key).to_bytes()?;
        Ok(read_row(BLOB_HEAD_KEYSPACE, head))
    }

    /// Takes the pointer row and answers the read of its version, when there is one.
    fn pointer(
        &mut self,
        bucket: &str,
        key: &str,
        result: IoResult,
    ) -> Result<Option<IoCall>, OfferError> {
        let row = value(result)?;
        self.pointer = row
            .map(|row| CurrentVersionPointer::from_bytes(&row))
            .transpose()?;
        self.pointer_read = true;
        let Some(pointer) = self.pointer.as_ref() else {
            return Ok(None);
        };
        let version = VersionKey::new(bucket, key, pointer.version_id).to_bytes()?;
        Ok(Some(read_row(BLOB_VERSIONS_KEYSPACE, version)))
    }

    fn version(result: IoResult) -> Result<Option<BlobVersion>, OfferError> {
        let row = value(result)?;
        Ok(row.map(|row| BlobVersion::from_bytes(&row)).transpose()?)
    }
}

/// Records one file. An unchanged file keeps its version, so offering a directory again does
/// not churn the identities realm nodes already reference. Outputs the file's version.
#[derive(Debug, PartialEq)]
pub(in crate::staging) struct ObserveFileOperation {
    scope: ObservationScope,
    path: String,
    binding: VersionSourceBinding,
    metadata: SourceMetadata,
    rows: CurrentRows,
    version_id: Option<Ulid>,
    change: Option<VersionChange>,
}

type ObserveTurn = Result<Next<Ulid>, OfferError>;

impl ObserveFileOperation {
    pub(in crate::staging) fn new(scope: ObservationScope, entry: &SourceEntry) -> Self {
        Self {
            binding: offered_binding(&scope.bucket, &entry.path, scope.node_id),
            metadata: entry_metadata(entry),
            path: entry.path.clone(),
            scope,
            rows: CurrentRows::default(),
            version_id: None,
            change: None,
        }
    }

    fn unchanged(&self, existing: Option<&BlobVersion>) -> bool {
        matches!(
            existing,
            Some(BlobVersion {
                state: BlobVersionState::Reference { source, cached_metadata, .. },
                ..
            }) if *source == self.binding
                && cached_metadata.observation_fingerprint()
                    == self.metadata.observation_fingerprint()
        )
    }

    fn decide(&mut self, existing: Option<BlobVersion>) -> ObserveTurn {
        let pointer = self.rows.pointer.as_ref();
        if let Some(pointer) = pointer.filter(|_| self.unchanged(existing.as_ref())) {
            return Ok(Next::Done(pointer.version_id));
        }
        let scope = &self.scope;
        let version_id = Ulid::generate();
        let now = SystemTime::now();
        let uploaded = SourceEvent::data_uploaded(
            (scope.realm_id, scope.group_id, scope.node_id),
            (&scope.bucket, &self.path),
            version_id,
            unix_timestamp_millis(),
        );
        let version = BlobVersion::reference(
            self.binding.clone(),
            self.metadata.clone(),
            now,
            scope.user_id,
            now,
        );
        let key = VersionKey::new(&scope.bucket, &self.path, version_id);
        let write = journaled(write_version_effect(&key, &version, None)?, &uploaded)?;
        // Charge the current observation and release the observation it replaces.
        let live = existing.filter(|version| !version.is_deleted());
        let released = (live.as_ref())
            .and_then(|version| version.offered_size(&scope.bucket, &self.path))
            .unwrap_or(0);
        let delta = UsageDelta {
            objects: i128::from(u8::from(live.is_none())),
            referenced_bytes: i128::from(self.metadata.content_length) - i128::from(released),
            ..Default::default()
        };
        let usage = UsageCounterUpdate::for_group(scope.group_id, delta)
            .in_bucket(&scope.bucket, RowChange::AddedVersion);
        let mut change =
            VersionChange::new(scope, &self.path, (pointer, version_id), write, usage)?;
        let call = change.next_call().ok_or(IoError::Unfinished)?;
        self.version_id = Some(version_id);
        self.change = Some(change);
        Ok(Next::Call(call))
    }
}

impl Operation for ObserveFileOperation {
    type Output = Ulid;
    type Error = OfferError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ObserveTurn {
        let call = CurrentRows::first(&self.scope.bucket, &self.path)?;
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> ObserveTurn {
        if let Some(change) = self.change.as_mut() {
            return match change.advance(result)? {
                Some(call) => Ok(Next::Call(call)),
                None => Ok(Next::Done(self.version_id.ok_or(IoError::Unfinished)?)),
            };
        }
        if self.rows.pointer_read {
            let existing = CurrentRows::version(result)?;
            return self.decide(existing);
        }
        match self.rows.pointer(&self.scope.bucket, &self.path, result)? {
            Some(call) => Ok(Next::Call(call)),
            None => self.decide(None),
        }
    }
}

/// Writes a delete marker over one observation; `true` when it removed a live object.
#[derive(Debug, PartialEq)]
pub(in crate::staging) struct TombstoneOperation {
    scope: ObservationScope,
    key: String,
    rows: CurrentRows,
    change: Option<VersionChange>,
}

type TombstoneTurn = Result<Next<bool>, OfferError>;

impl TombstoneOperation {
    pub(in crate::staging) fn new(scope: ObservationScope, key: String) -> Self {
        Self {
            scope,
            key,
            rows: CurrentRows::default(),
            change: None,
        }
    }

    fn decide(&mut self, existing: Option<BlobVersion>) -> TombstoneTurn {
        let Some(live) = existing.filter(|version| !version.is_deleted()) else {
            return Ok(Next::Done(false));
        };
        let scope = &self.scope;
        let version_id = Ulid::generate();
        let marker = BlobVersion::deleted(SystemTime::now(), scope.user_id);
        let key = VersionKey::new(&scope.bucket, &self.key, version_id);
        let write = write_version_effect(&key, &marker, None)?;
        let size = live.offered_size(&scope.bucket, &self.key).unwrap_or(0);
        let delta = UsageDelta {
            objects: -1,
            referenced_bytes: -i128::from(size),
            ..Default::default()
        };
        let usage = UsageCounterUpdate::for_group(scope.group_id, delta)
            .in_bucket(&scope.bucket, RowChange::AddedMarker);
        let pointer = self.rows.pointer.as_ref();
        let mut change = VersionChange::new(scope, &self.key, (pointer, version_id), write, usage)?;
        let call = change.next_call().ok_or(IoError::Unfinished)?;
        self.change = Some(change);
        Ok(Next::Call(call))
    }
}

impl Operation for TombstoneOperation {
    type Output = bool;
    type Error = OfferError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> TombstoneTurn {
        let call = CurrentRows::first(&self.scope.bucket, &self.key)?;
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> TombstoneTurn {
        if let Some(change) = self.change.as_mut() {
            return match change.advance(result)? {
                Some(call) => Ok(Next::Call(call)),
                None => Ok(Next::Done(true)),
            };
        }
        if self.rows.pointer_read {
            let existing = CurrentRows::version(result)?;
            return self.decide(existing);
        }
        match self.rows.pointer(&self.scope.bucket, &self.key, result)? {
            Some(call) => Ok(Next::Call(call)),
            None => self.decide(None),
        }
    }
}

#[cfg(test)]
#[path = "observe_tests.rs"]
mod tests;
