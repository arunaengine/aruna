//! Reads the version an object key names: the given one, or the current head.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
};
use aruna_core::types::{Effects, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

/// The version an object resolved to, and its record when one is stored.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectVersion {
    pub version_id: Option<Ulid>,
    pub version: Option<BlobVersion>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ObjectVersionError {
    #[error("the object key does not encode: {0}")]
    Key(ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Decode(#[from] ConversionError),
    #[error("unexpected event while {state}: got {got}")]
    UnexpectedEvent {
        state: &'static str,
        got: &'static str,
    },
    #[error("the object version read did not finish")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum VersionState {
    Init,
    Head,
    Version,
    Done,
}

impl VersionState {
    fn name(&self) -> &'static str {
        match self {
            Self::Init => "starting",
            Self::Head => "reading the object head",
            Self::Version => "reading the object version",
            Self::Done => "finished",
        }
    }
}

/// Resolves `version_id`, or the head of `bucket/key` when none is given, and
/// reads that version's record. An object without a head resolves to nothing.
#[derive(Debug, PartialEq)]
pub struct ObjectVersionOperation {
    bucket: String,
    key: String,
    version_id: Option<Ulid>,
    state: VersionState,
    output: Option<Result<ObjectVersion, ObjectVersionError>>,
}

impl ObjectVersionOperation {
    pub fn new(bucket: &str, key: &str, version_id: Option<Ulid>) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            state: VersionState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<ObjectVersion, ObjectVersionError>) -> Effects {
        self.state = VersionState::Done;
        self.output = Some(output);
        smallvec![]
    }

    fn read(&mut self, state: VersionState, key_space: &str, key: Vec<u8>) -> Effects {
        self.state = state;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn read_version(&mut self, version_id: Ulid) -> Effects {
        self.version_id = Some(version_id);
        match VersionKey::new(&self.bucket, &self.key, version_id).to_bytes() {
            Ok(row) => self.read(VersionState::Version, BLOB_VERSIONS_KEYSPACE, row),
            Err(error) => self.finish(Err(ObjectVersionError::Key(error))),
        }
    }

    fn head_read(&mut self, value: Option<Value>) -> Effects {
        let Some(value) = value else {
            return self.finish(Ok(ObjectVersion {
                version_id: None,
                version: None,
            }));
        };
        match CurrentVersionPointer::from_bytes(&value) {
            Ok(pointer) => self.read_version(pointer.version_id),
            Err(error) => self.finish(Err(error.into())),
        }
    }

    fn version_read(&mut self, value: Option<Value>) -> Effects {
        let output = value
            .map(|value| BlobVersion::from_bytes(&value))
            .transpose()
            .map(|version| ObjectVersion {
                version_id: self.version_id,
                version,
            })
            .map_err(ObjectVersionError::from);
        self.finish(output)
    }
}

impl Operation for ObjectVersionOperation {
    type Output = ObjectVersion;
    type Error = ObjectVersionError;

    fn start(&mut self) -> Effects {
        if let Some(version_id) = self.version_id {
            return self.read_version(version_id);
        }
        match BlobHeadKey::new(&self.bucket, &self.key).to_bytes() {
            Ok(head) => self.read(VersionState::Head, BLOB_HEAD_KEYSPACE, head),
            Err(error) => self.finish(Err(ObjectVersionError::Key(error))),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let reading = matches!(self.state, VersionState::Head | VersionState::Version);
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. })
                if self.state == VersionState::Head =>
            {
                self.head_read(value)
            }
            Event::Storage(StorageEvent::ReadResult { value, .. })
                if self.state == VersionState::Version =>
            {
                self.version_read(value)
            }
            Event::Storage(StorageEvent::Error { error }) if reading => {
                self.finish(Err(error.into()))
            }
            event => {
                let error = ObjectVersionError::UnexpectedEvent {
                    state: self.state.name(),
                    got: event.label(),
                };
                self.finish(Err(error))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == VersionState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ObjectVersionError::Unfinished))
    }

    // Both reads run outside a transaction, so nothing is left to clean up.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
