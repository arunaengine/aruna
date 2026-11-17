//! Finds the snapshot a retried staging already wrote: the head version of the key, when it
//! holds the same source binding and a stored copy of the source's length.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    CurrentVersionPointer, VersionKey,
};
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum SnapshotReuseError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReuseState {
    Init,
    Head,
    Version,
    Location,
    Finish,
    Error,
}

/// Outputs the stored copy and version id the retried staging may answer with, if any.
#[derive(Debug, PartialEq)]
pub struct SnapshotReuseOperation {
    bucket: String,
    key: String,
    source: VersionSourceBinding,
    content_length: u64,
    state: ReuseState,
    version_id: Option<Ulid>,
    output: Option<Result<Option<(BackendLocation, Ulid)>, SnapshotReuseError>>,
}

impl SnapshotReuseOperation {
    pub fn new(
        bucket: String,
        key: String,
        source: VersionSourceBinding,
        content_length: u64,
    ) -> Self {
        Self {
            bucket,
            key,
            source,
            content_length,
            state: ReuseState::Init,
            version_id: None,
            output: None,
        }
    }

    fn finish(
        &mut self,
        output: Result<Option<(BackendLocation, Ulid)>, SnapshotReuseError>,
    ) -> Effects {
        self.state = match output {
            Ok(_) => ReuseState::Finish,
            Err(_) => ReuseState::Error,
        };
        self.output = Some(output);
        smallvec![]
    }

    fn read(&mut self, state: ReuseState, key_space: &str, key: Key) -> Effects {
        self.state = state;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })]
    }

    /// The next read, or the answer once a row is missing or does not match.
    fn found(
        &mut self,
        value: Option<Value>,
    ) -> Result<Result<Effects, Option<(BackendLocation, Ulid)>>, SnapshotReuseError> {
        let Some(value) = value else {
            return Ok(Err(None));
        };
        match self.state {
            ReuseState::Head => {
                let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
                self.version_id = Some(pointer.version_id);
                let row =
                    VersionKey::new(&self.bucket, &self.key, pointer.version_id).to_bytes()?;
                Ok(Ok(self.read(
                    ReuseState::Version,
                    BLOB_VERSIONS_KEYSPACE,
                    row.into(),
                )))
            }
            ReuseState::Version => {
                let version = BlobVersion::from_bytes(value.as_ref())?;
                let BlobVersionState::Materialized {
                    blob_hash,
                    backend,
                    encoding,
                    source: Some(source),
                } = version.state
                else {
                    return Ok(Err(None));
                };
                if source != self.source {
                    return Ok(Err(None));
                }
                let key = BlobLocationKey::new(blob_hash, encoding, backend).to_bytes();
                Ok(Ok(self.read(
                    ReuseState::Location,
                    BLOB_LOCATIONS_KEYSPACE,
                    key.into(),
                )))
            }
            _ => {
                let location = BackendLocation::from_bytes(value.as_ref())?;
                let version_id = self.version_id.ok_or(StorageError::InvalidEffect)?;
                let kept =
                    (location.blob_size == self.content_length).then_some((location, version_id));
                Ok(Err(kept))
            }
        }
    }
}

impl EffectOperation for SnapshotReuseOperation {
    type Output = Option<(BackendLocation, Ulid)>;
    type Error = SnapshotReuseError;

    fn start(&mut self) -> Effects {
        match BlobHeadKey::new(&self.bucket, &self.key).to_bytes() {
            Ok(head) => self.read(ReuseState::Head, BLOB_HEAD_KEYSPACE, head.into()),
            Err(error) => self.finish(Err(error.into())),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let reading = matches!(
            self.state,
            ReuseState::Head | ReuseState::Version | ReuseState::Location
        );
        let value = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) if reading => value,
            Event::Storage(StorageEvent::Error { error }) if reading => {
                return self.finish(Err(error.into()));
            }
            event => {
                let unexpected = format!("unexpected {}", event.label());
                return self.finish(Err(StorageError::ReadError(unexpected).into()));
            }
        };
        match self.found(value) {
            Ok(Ok(effects)) => effects,
            Ok(Err(found)) => self.finish(Ok(found)),
            Err(error) => self.finish(Err(error)),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReuseState::Finish | ReuseState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(StorageError::ReadError(
            "the snapshot lookup did not finish".to_string(),
        )
        .into()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::staging::{sample_connector, sample_metadata};
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::execution::staging::StagingStrategy;

    fn operation() -> SnapshotReuseOperation {
        let source = crate::connectors::descriptor::build_source_binding(
            StagingStrategy::Snapshot,
            &sample_connector(),
            &sample_metadata(),
            "folder/file.txt".to_string(),
            None,
            None,
        );
        SnapshotReuseOperation::new("bucket".into(), "key".into(), source, 42)
    }

    fn head_read() -> Effect {
        Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new("bucket", "key").to_bytes().unwrap().into(),
            txn_id: None,
        })
    }

    // A key without a head has no snapshot to reuse.
    #[test]
    fn missing_head_ignored() {
        let mut reuse = operation();
        let missing = Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        });
        check_trace(&mut reuse, &[head_read()], vec![(missing, Vec::new())]);
        assert_eq!(reuse.finalize(), Ok(None));
    }

    // A failed read fails the lookup; other events are refused and nothing finishes early.
    #[test]
    fn rejects_unexpected() {
        let mut reuse = operation();
        reuse.start();
        reuse.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(reuse.finalize(), Err(StorageError::Timeout.into()));
        let all = sample_events().len();
        assert_eq!(
            rejects_events(operation, sample_events, |_| false).len(),
            all
        );
        rejects_finalize(operation());
        assert!(operation().abort().is_empty());
    }
}
