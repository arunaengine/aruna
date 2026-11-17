//! Checks one current version for stored copies that cannot be served.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BROKEN_VERSION_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobVersion, BlobVersionState, VersionKey,
};
use aruna_core::structs::storage::broken::{BrokenReason, BrokenVersion};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::Key;
use thiserror::Error;

use crate::groups::backends::backend_key;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckState {
    ReadVersion,
    ReadLocation,
    ReadStorage,
    Stat,
    Record,
}

/// What a check found for one version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckOutcome {
    Healthy,
    Broken(BrokenReason),
    /// Nothing stored to check: no version, no copy, or an unfinished copy.
    Skipped,
}

#[derive(Debug, Error, PartialEq)]
pub enum CheckVersionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("version check failed: {0}")]
    Io(IoError),
}

impl From<IoError> for CheckVersionError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

type Step = Result<Next<CheckOutcome>, CheckVersionError>;

/// Checks one version's copy: its location row, its group storage and one existence probe.
/// A broken version is recorded with the version record it saw; any other answer clears it.
#[derive(Debug, PartialEq)]
pub struct CheckVersionOperation {
    version_key: VersionKey,
    now_ms: u64,
    state: CheckState,
    version: Option<BlobVersion>,
    location: Option<BackendLocation>,
    outcome: Option<CheckOutcome>,
}

impl CheckVersionOperation {
    pub fn new(version_key: VersionKey, now_ms: u64) -> Self {
        Self {
            version_key,
            now_ms,
            state: CheckState::ReadVersion,
            version: None,
            location: None,
            outcome: None,
        }
    }

    /// Writes or clears the broken row, then ends with `outcome`.
    fn record(&mut self, outcome: CheckOutcome) -> Step {
        let key = Key::from(self.version_key.to_bytes()?);
        let call = match (outcome, self.version.clone()) {
            (CheckOutcome::Broken(reason), Some(version)) => {
                let row = BrokenVersion {
                    reason,
                    version,
                    found_at_ms: self.now_ms,
                };
                let value = row.to_bytes()?.into();
                IoCall::Write(vec![(BROKEN_VERSION_KEYSPACE.to_string(), key, value)])
            }
            _ => IoCall::Delete(vec![(BROKEN_VERSION_KEYSPACE.to_string(), key)]),
        };
        self.outcome = Some(outcome);
        self.state = CheckState::Record;
        Ok(Next::Call(call))
    }

    fn handle_version(&mut self, value: Option<&[u8]>) -> Step {
        let version = value.map(BlobVersion::from_bytes).transpose()?;
        let key = version.as_ref().and_then(|version| match &version.state {
            BlobVersionState::Materialized { .. } => version.location_key(),
            _ => None,
        });
        self.version = version;
        let Some(key) = key else {
            return self.record(CheckOutcome::Skipped);
        };
        self.state = CheckState::ReadLocation;
        Ok(Next::Call(read_row(
            BLOB_LOCATIONS_KEYSPACE,
            key.to_bytes(),
        )))
    }

    fn handle_location(&mut self, value: Option<&[u8]>) -> Step {
        let Some(location) = value.map(BackendLocation::from_bytes).transpose()? else {
            return self.record(CheckOutcome::Broken(BrokenReason::MissingRecord));
        };
        if location.staging || location.partial {
            return self.record(CheckOutcome::Skipped);
        }
        let storage = match &location.backend {
            BackendRef::Group(backend_id) => Some(backend_key(*backend_id)),
            BackendRef::Node(_) => None,
        };
        let Some(key) = storage else {
            return Ok(self.stat(location));
        };
        self.location = Some(location);
        self.state = CheckState::ReadStorage;
        Ok(Next::Call(read_row(STORAGE_BACKEND_KEYSPACE, key)))
    }

    fn handle_storage(&mut self, value: Option<&[u8]>) -> Step {
        let storage = value.map(GroupStorage::from_bytes).transpose()?;
        match (storage, self.location.take()) {
            (Some(storage), Some(location)) if !storage.disabled => Ok(self.stat(location)),
            _ => self.record(CheckOutcome::Broken(BrokenReason::StorageRemoved)),
        }
    }

    fn stat(&mut self, location: BackendLocation) -> Next<CheckOutcome> {
        self.state = CheckState::Stat;
        let stat = Effect::Blob(BlobEffect::StatCopy { location });
        Next::Call(IoCall::Effect(Box::new(stat)))
    }
}

impl Operation for CheckVersionOperation {
    type Output = CheckOutcome;
    type Error = CheckVersionError;

    fn first(&mut self) -> Step {
        let key = self.version_key.to_bytes()?;
        Ok(Next::Call(read_row(BLOB_VERSIONS_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            CheckState::ReadVersion => self.handle_version(result.value()?.as_deref()),
            CheckState::ReadLocation => self.handle_location(result.value()?.as_deref()),
            CheckState::ReadStorage => self.handle_storage(result.value()?.as_deref()),
            CheckState::Stat => match result.event()? {
                Event::Blob(BlobEvent::CopyStat { exists: true }) => {
                    self.record(CheckOutcome::Healthy)
                }
                Event::Blob(BlobEvent::CopyStat { exists: false }) => {
                    self.record(CheckOutcome::Broken(BrokenReason::MissingBlob))
                }
                Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
                other => Err(IoError::Unexpected {
                    expected: "BlobEvent::CopyStat",
                    got: other.label(),
                }
                .into()),
            },
            CheckState::Record => {
                result.done()?;
                let outcome = self.outcome.take().ok_or(IoError::Unfinished)?;
                Ok(Next::Done(outcome))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::format::{EncodingClass, StoredFormat};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn read(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    fn stat(exists: bool) -> IoResult {
        IoResult::Event(Box::new(Event::Blob(BlobEvent::CopyStat { exists })))
    }

    fn version(backend: BackendRef) -> BlobVersion {
        let created = SystemTime::UNIX_EPOCH;
        BlobVersion::materialized(
            [4; 32],
            backend,
            EncodingClass::Raw,
            created,
            Default::default(),
            None,
        )
    }

    fn location(backend: BackendRef) -> BackendLocation {
        BackendLocation {
            backend,
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "store".to_string(),
            backend_path: "b/k".to_string(),
            ulid: Ulid::from_bytes([3; 16]),
            format: StoredFormat::default(),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), vec![4u8; 32])]),
        }
    }

    /// Drives a check of a version on `backend` up to the answer after its location read.
    fn located(backend: BackendRef) -> (CheckVersionOperation, Step) {
        let key = VersionKey::new("b", "k", Ulid::from_bytes([2; 16]));
        let mut operation = CheckVersionOperation::new(key, 9);
        let _ = operation.first();
        let _ = operation.next(read(Some(version(backend.clone()).to_bytes().unwrap())));
        let turn = operation.next(read(Some(location(backend).to_bytes().unwrap())));
        (operation, turn)
    }

    /// Answers the broken row write or delete and returns the outcome and the effect.
    fn recorded(mut operation: CheckVersionOperation, turn: Step) -> (CheckOutcome, bool) {
        let written = match turn {
            Ok(Next::Call(IoCall::Write(_))) => true,
            Ok(Next::Call(IoCall::Delete(_))) => false,
            other => panic!("expected the broken row call, got {other:?}"),
        };
        match operation.next(IoResult::Done) {
            Ok(Next::Done(outcome)) => (outcome, written),
            other => panic!("expected the outcome, got {other:?}"),
        }
    }

    #[test]
    fn missing_blob_recorded() {
        let (mut operation, turn) = located(BackendRef::node_default());
        assert!(matches!(
            &turn,
            Ok(Next::Call(IoCall::Effect(effect)))
                if matches!(effect.as_ref(), Effect::Blob(BlobEffect::StatCopy { .. }))
        ));
        let turn = operation.next(stat(false));
        let outcome = recorded(operation, turn);
        assert_eq!(
            outcome,
            (CheckOutcome::Broken(BrokenReason::MissingBlob), true)
        );
    }

    #[test]
    fn healthy_copy_cleared() {
        let (mut operation, _) = located(BackendRef::node_default());
        let turn = operation.next(stat(true));
        assert_eq!(recorded(operation, turn), (CheckOutcome::Healthy, false));
    }

    #[test]
    fn removed_storage_recorded() {
        // A removed group storage is broken without asking the storage.
        let (mut operation, turn) = located(BackendRef::Group(Ulid::from_bytes([5; 16])));
        let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
            panic!("expected the storage record read, got {turn:?}")
        };
        assert_eq!(reads[0].0, STORAGE_BACKEND_KEYSPACE);
        let turn = operation.next(read(None));
        let outcome = recorded(operation, turn);
        assert_eq!(
            outcome,
            (CheckOutcome::Broken(BrokenReason::StorageRemoved), true)
        );
    }

    #[test]
    fn missing_record_recorded() {
        let key = VersionKey::new("b", "k", Ulid::from_bytes([2; 16]));
        let mut operation = CheckVersionOperation::new(key, 9);
        let _ = operation.first();
        let stored = version(BackendRef::node_default()).to_bytes().unwrap();
        let _ = operation.next(read(Some(stored)));
        let turn = operation.next(read(None));
        let outcome = recorded(operation, turn);
        assert_eq!(
            outcome,
            (CheckOutcome::Broken(BrokenReason::MissingRecord), true)
        );
    }
}
