//! Checks one current version for stored copies that cannot be served.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BROKEN_VERSION_KEYSPACE, STORAGE_BACKEND_KEYSPACE};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobVersion, BlobVersionState, VersionKey,
};
use aruna_core::structs::storage::broken::{BrokenReason, BrokenVersion};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

use crate::blob::records::{blob_location_read, read_version_effect};
use crate::groups::backends::backend_key;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckState {
    Init,
    ReadVersion,
    ReadLocation,
    ReadStorage,
    Stat,
    Record,
    Finish,
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
    #[error("version check did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: {received}")]
    InvalidStateEvent {
        state: &'static str,
        received: &'static str,
    },
}

/// Checks one version's copy: its location row, its group storage and one existence probe.
/// A broken version is recorded with the version record it saw; any other answer clears it.
#[derive(Debug, PartialEq)]
pub struct CheckVersionOperation {
    version_key: VersionKey,
    now_ms: u64,
    state: CheckState,
    version: Option<BlobVersion>,
    location: Option<BackendLocation>,
    output: Option<Result<CheckOutcome, CheckVersionError>>,
}

impl CheckVersionOperation {
    pub fn new(version_key: VersionKey, now_ms: u64) -> Self {
        Self {
            version_key,
            now_ms,
            state: CheckState::Init,
            version: None,
            location: None,
            output: None,
        }
    }

    fn fail(&mut self, error: CheckVersionError) -> Effects {
        self.state = CheckState::Finish;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, state: &'static str, received: Event) -> Effects {
        let received = received.label();
        self.fail(CheckVersionError::InvalidStateEvent { state, received })
    }

    /// Writes or clears the broken row, then ends with `outcome`.
    fn record(&mut self, outcome: CheckOutcome) -> Effects {
        let key = match self.version_key.to_bytes() {
            Ok(key) => Key::from(key),
            Err(error) => return self.fail(error.into()),
        };
        let effect = match (outcome, self.version.clone()) {
            (CheckOutcome::Broken(reason), Some(version)) => {
                let row = BrokenVersion {
                    reason,
                    version,
                    found_at_ms: self.now_ms,
                };
                match row.to_bytes() {
                    Ok(value) => StorageEffect::Write {
                        key_space: BROKEN_VERSION_KEYSPACE.to_string(),
                        key,
                        value: value.into(),
                        txn_id: None,
                    },
                    Err(error) => return self.fail(error.into()),
                }
            }
            _ => StorageEffect::Delete {
                key_space: BROKEN_VERSION_KEYSPACE.to_string(),
                key,
                txn_id: None,
            },
        };
        self.output = Some(Ok(outcome));
        self.state = CheckState::Record;
        smallvec![Effect::Storage(effect)]
    }

    fn handle_version(&mut self, value: Option<&[u8]>) -> Effects {
        let version = match value.map(BlobVersion::from_bytes).transpose() {
            Ok(version) => version,
            Err(error) => return self.fail(error.into()),
        };
        let key = version.as_ref().and_then(|version| match &version.state {
            BlobVersionState::Materialized { .. } => version.location_key(),
            _ => None,
        });
        self.version = version;
        let Some(key) = key else {
            return self.record(CheckOutcome::Skipped);
        };
        self.state = CheckState::ReadLocation;
        smallvec![blob_location_read(&key, None)]
    }

    fn handle_location(&mut self, value: Option<&[u8]>) -> Effects {
        let location = match value.map(BackendLocation::from_bytes).transpose() {
            Ok(Some(location)) => location,
            Ok(None) => return self.record(CheckOutcome::Broken(BrokenReason::MissingRecord)),
            Err(error) => return self.fail(error.into()),
        };
        if location.staging || location.partial {
            return self.record(CheckOutcome::Skipped);
        }
        let storage = match &location.backend {
            BackendRef::Group(backend_id) => Some(backend_key(*backend_id)),
            BackendRef::Node(_) => None,
        };
        self.location = Some(location);
        let Some(key) = storage else {
            return self.stat();
        };
        self.state = CheckState::ReadStorage;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })]
    }

    fn handle_storage(&mut self, value: Option<&[u8]>) -> Effects {
        match value.map(GroupStorage::from_bytes).transpose() {
            Ok(Some(storage)) if !storage.disabled => self.stat(),
            Ok(_) => self.record(CheckOutcome::Broken(BrokenReason::StorageRemoved)),
            Err(error) => self.fail(error.into()),
        }
    }

    fn stat(&mut self) -> Effects {
        let Some(location) = self.location.clone() else {
            return self.fail(CheckVersionError::NotFinished);
        };
        self.state = CheckState::Stat;
        smallvec![Effect::Blob(BlobEffect::StatCopy { location })]
    }
}

impl EffectOperation for CheckVersionOperation {
    type Output = CheckOutcome;
    type Error = CheckVersionError;

    fn start(&mut self) -> Effects {
        match read_version_effect(&self.version_key, None) {
            Ok(effect) => {
                self.state = CheckState::ReadVersion;
                smallvec![effect]
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (CheckState::ReadVersion, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.handle_version(value.as_deref())
            }
            (CheckState::ReadLocation, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.handle_location(value.as_deref())
            }
            (CheckState::ReadStorage, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.handle_storage(value.as_deref())
            }
            (CheckState::Stat, Event::Blob(BlobEvent::CopyStat { exists })) => match exists {
                true => self.record(CheckOutcome::Healthy),
                false => self.record(CheckOutcome::Broken(BrokenReason::MissingBlob)),
            },
            (CheckState::Stat, Event::Blob(BlobEvent::Error(error))) => self.fail(error.into()),
            (
                CheckState::Record,
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
                ),
            ) => {
                self.state = CheckState::Finish;
                smallvec![]
            }
            (CheckState::Init, _) => self.start(),
            (_, event) => self.unexpected("Check", event),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == CheckState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            CheckState::Finish => self.output.unwrap_or(Err(CheckVersionError::NotFinished)),
            _ => Err(CheckVersionError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
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

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        })
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
    fn located(backend: BackendRef) -> (CheckVersionOperation, Effects) {
        let key = VersionKey::new("b", "k", Ulid::from_bytes([2; 16]));
        let mut operation = CheckVersionOperation::new(key, 9);
        operation.start();
        operation.step(read(Some(version(backend.clone()).to_bytes().unwrap())));
        let effects = operation.step(read(Some(location(backend).to_bytes().unwrap())));
        (operation, effects)
    }

    /// Answers the broken row write or delete and returns the outcome and the effect.
    fn recorded(mut operation: CheckVersionOperation, effects: Effects) -> (CheckOutcome, bool) {
        let [Effect::Storage(effect)] = effects.as_slice() else {
            panic!("expected the broken row effect, got {effects:?}")
        };
        let written = matches!(effect, StorageEffect::Write { .. });
        let answer = match written {
            true => StorageEvent::WriteResult {
                key: Vec::new().into(),
            },
            false => StorageEvent::DeleteResult {
                key: Vec::new().into(),
            },
        };
        operation.step(Event::Storage(answer));
        (operation.finalize().unwrap(), written)
    }

    #[test]
    fn missing_blob_recorded() {
        let (mut operation, effects) = located(BackendRef::node_default());
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::StatCopy { .. })]
        ));
        let effects = operation.step(Event::Blob(BlobEvent::CopyStat { exists: false }));
        let outcome = recorded(operation, effects);
        assert_eq!(
            outcome,
            (CheckOutcome::Broken(BrokenReason::MissingBlob), true)
        );
    }

    #[test]
    fn healthy_copy_cleared() {
        let (mut operation, _) = located(BackendRef::node_default());
        let effects = operation.step(Event::Blob(BlobEvent::CopyStat { exists: true }));
        assert_eq!(recorded(operation, effects), (CheckOutcome::Healthy, false));
    }

    #[test]
    fn removed_storage_recorded() {
        // A removed group storage is broken without asking the storage.
        let (mut operation, effects) = located(BackendRef::Group(Ulid::from_bytes([5; 16])));
        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected the storage record read, got {effects:?}")
        };
        assert_eq!(key_space, STORAGE_BACKEND_KEYSPACE);
        let effects = operation.step(read(None));
        let outcome = recorded(operation, effects);
        assert_eq!(
            outcome,
            (CheckOutcome::Broken(BrokenReason::StorageRemoved), true)
        );
    }

    #[test]
    fn missing_record_recorded() {
        let key = VersionKey::new("b", "k", Ulid::from_bytes([2; 16]));
        let mut operation = CheckVersionOperation::new(key, 9);
        operation.start();
        operation.step(read(Some(
            version(BackendRef::node_default()).to_bytes().unwrap(),
        )));
        let effects = operation.step(read(None));
        let outcome = recorded(operation, effects);
        assert_eq!(
            outcome,
            (CheckOutcome::Broken(BrokenReason::MissingRecord), true)
        );
    }
}
