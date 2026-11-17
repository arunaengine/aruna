//! Refuses a write addressed to an offered bucket or to the bucket of a folder being unbound.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The write path calls it, so the read-only rule lives with the registration, not in a handler.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{OFFERED_DIRECTORY_KEYSPACE, SYNCED_FOLDER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::offered_directory::OfferedDirectory;
use aruna_core::structs::{FolderState, SyncedFolder};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::device::sync::repository::bucket_folder;

#[derive(Debug, Error, PartialEq)]
pub enum BucketGuardError {
    #[error("bucket `{0}` is an offered directory and is read-only")]
    ReadOnly(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GuardState {
    Init,
    Offered,
    Folder,
    Finish,
    Error,
}

/// Checks that `bucket` takes writes: neither an offered directory nor the bucket of a synced
/// folder that is being unbound.
#[derive(Debug, PartialEq)]
pub struct BucketGuardOperation {
    bucket: String,
    state: GuardState,
    output: Option<Result<(), BucketGuardError>>,
}

impl BucketGuardOperation {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            state: GuardState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<(), BucketGuardError>) -> Effects {
        self.state = match output {
            Ok(()) => GuardState::Finish,
            Err(_) => GuardState::Error,
        };
        self.output = Some(output);
        smallvec![]
    }

    fn read_only(&mut self) -> Effects {
        let bucket = self.bucket.clone();
        self.finish(Err(BucketGuardError::ReadOnly(bucket)))
    }

    fn read(key_space: &str, key: Vec<u8>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }
}

impl Operation for BucketGuardOperation {
    type Output = ();
    type Error = BucketGuardError;

    fn start(&mut self) -> Effects {
        self.state = GuardState::Offered;
        Self::read(OFFERED_DIRECTORY_KEYSPACE, self.bucket.as_bytes().to_vec())
    }

    fn step(&mut self, event: Event) -> Effects {
        let value = match (self.state, event) {
            (
                GuardState::Offered | GuardState::Folder,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => value,
            (
                GuardState::Offered | GuardState::Folder,
                Event::Storage(StorageEvent::Error { error }),
            ) => {
                return self.finish(Err(error.into()));
            }
            (state, event) => {
                let label = event.label();
                let error = StorageError::ReadError(format!("unexpected {label} in {state:?}"));
                return self.finish(Err(error.into()));
            }
        };
        match self.state {
            GuardState::Offered => {
                let offered = (value.map(|value| OfferedDirectory::from_bytes(&value))).transpose();
                match offered {
                    Ok(Some(_)) => self.read_only(),
                    Ok(None) => match bucket_folder(&self.bucket) {
                        Some(folder_id) => {
                            self.state = GuardState::Folder;
                            Self::read(SYNCED_FOLDER_KEYSPACE, folder_id.to_bytes().to_vec())
                        }
                        None => self.finish(Ok(())),
                    },
                    Err(error) => self.finish(Err(error.into())),
                }
            }
            _ => {
                let folder = (value.map(|value| SyncedFolder::from_bytes(&value))).transpose();
                match folder {
                    Ok(Some(folder)) if folder.state == FolderState::Deleting => self.read_only(),
                    Ok(_) => self.finish(Ok(())),
                    Err(error) => self.finish(Err(error.into())),
                }
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GuardState::Finish | GuardState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(StorageError::ReadError(
            "the bucket write guard did not finish".to_string(),
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
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use ulid::Ulid;

    fn read_result(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        })
    }

    // An ordinary bucket that is neither offered nor a folder bucket takes writes.
    #[test]
    fn plain_bucket_writes() {
        let mut operation = BucketGuardOperation::new("plain");
        let offered = BucketGuardOperation::read(OFFERED_DIRECTORY_KEYSPACE, b"plain".to_vec());
        check_trace(
            &mut operation,
            &offered,
            vec![(read_result(None), Vec::new())],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    // A folder bucket is checked against its folder; a missing folder row takes writes.
    #[test]
    fn folder_bucket_checked() {
        let folder_id = Ulid::from_bytes([3; 16]);
        let bucket = crate::device::sync::repository::folder_bucket(folder_id);
        let mut operation = BucketGuardOperation::new(bucket.clone());
        let offered = BucketGuardOperation::read(OFFERED_DIRECTORY_KEYSPACE, bucket.into_bytes());
        let folder =
            BucketGuardOperation::read(SYNCED_FOLDER_KEYSPACE, folder_id.to_bytes().to_vec());
        check_trace(
            &mut operation,
            &offered,
            vec![
                (read_result(None), folder.into_vec()),
                (read_result(None), Vec::new()),
            ],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    // Failed reads and undecodable rows refuse the write; other events are refused as well.
    #[test]
    fn failures_refuse() {
        let mut operation = BucketGuardOperation::new("plain");
        operation.start();
        operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(operation.finalize(), Err(StorageError::Timeout.into()));

        let mut operation = BucketGuardOperation::new("plain");
        operation.start();
        operation.step(read_result(Some(vec![0xFF])));
        assert!(matches!(
            operation.finalize(),
            Err(BucketGuardError::Conversion(_))
        ));

        let fresh = || BucketGuardOperation::new("plain");
        let all = sample_events().len();
        assert_eq!(rejects_events(fresh, sample_events, |_| false).len(), all);
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
    }
}
