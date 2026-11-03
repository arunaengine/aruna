//! Claims an object key for one share upload, so concurrent uploads of the same name pick
//! different keys before any bytes are written.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The claim only steers naming; the create-only write is what keeps a file from being replaced.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SHARE_NAME_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Key, TxnId};
use smallvec::smallvec;

use crate::shares::lifecycle::ShareError;

fn name_key(bucket: &str, key: &str) -> Key {
    [bucket.as_bytes(), &[0], key.as_bytes()].concat().into()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClaimState {
    Init,
    Begin,
    Read(TxnId),
    Write(TxnId),
    Commit,
    Abort,
    Done(bool),
    Failed,
}

/// Claims `bucket`/`key` until `until` (Unix seconds); `false` when a live claim holds it.
#[derive(Debug, PartialEq)]
pub struct ClaimNameOperation {
    bucket: String,
    key: String,
    now: u64,
    until: u64,
    state: ClaimState,
    error: Option<ShareError>,
}

impl ClaimNameOperation {
    pub fn new(bucket: &str, key: &str, now: u64, until: u64) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            now,
            until,
            state: ClaimState::Init,
            error: None,
        }
    }

    fn fail(&mut self, error: ShareError) -> Effects {
        self.state = ClaimState::Failed;
        self.error = Some(error);
        smallvec![]
    }

    fn row(&self) -> Key {
        name_key(&self.bucket, &self.key)
    }
}

impl Operation for ClaimNameOperation {
    type Output = bool;
    type Error = ShareError;

    fn start(&mut self) -> Effects {
        self.state = ClaimState::Begin;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (ClaimState::Begin, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.state = ClaimState::Read(txn_id);
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: SHARE_NAME_KEYSPACE.to_string(),
                    key: self.row(),
                    txn_id: Some(txn_id),
                })]
            }
            (ClaimState::Read(txn_id), Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let held = value
                    .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_ref()).ok())
                    .is_some_and(|until| u64::from_be_bytes(until) > self.now);
                if held {
                    self.state = ClaimState::Abort;
                    return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
                }
                self.state = ClaimState::Write(txn_id);
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: SHARE_NAME_KEYSPACE.to_string(),
                    key: self.row(),
                    value: self.until.to_be_bytes().to_vec().into(),
                    txn_id: Some(txn_id),
                })]
            }
            (ClaimState::Write(txn_id), Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.state = ClaimState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (ClaimState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.state = ClaimState::Done(true);
                smallvec![]
            }
            // A concurrent claim of the same name won the commit.
            (
                ClaimState::Commit,
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }),
            )
            | (ClaimState::Abort, Event::Storage(StorageEvent::TransactionAborted { .. })) => {
                self.state = ClaimState::Done(false);
                smallvec![]
            }
            (ClaimState::Read(txn_id) | ClaimState::Write(txn_id), event) => {
                let error = match event {
                    Event::Storage(StorageEvent::Error { error }) => error.into(),
                    _ => ShareError::UnexpectedEvent("claim"),
                };
                self.state = ClaimState::Failed;
                self.error = Some(error);
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            _ => self.fail(ShareError::UnexpectedEvent("claim")),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ClaimState::Done(_) | ClaimState::Failed)
    }

    fn finalize(self) -> Result<bool, ShareError> {
        match (self.state, self.error) {
            (ClaimState::Done(claimed), _) => Ok(claimed),
            (_, Some(error)) => Err(error),
            _ => Err(ShareError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            ClaimState::Read(txn_id) | ClaimState::Write(txn_id) => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

/// Drops the claim on `bucket`/`key` once its upload ended.
#[derive(Debug, PartialEq)]
pub struct ReleaseNameOperation {
    row: Key,
    output: Option<Result<(), ShareError>>,
}

impl ReleaseNameOperation {
    pub fn new(bucket: &str, key: &str) -> Self {
        Self {
            row: name_key(bucket, key),
            output: None,
        }
    }
}

impl Operation for ReleaseNameOperation {
    type Output = ();
    type Error = ShareError;

    fn start(&mut self) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: SHARE_NAME_KEYSPACE.to_string(),
            key: self.row.clone(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(match event {
            Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            _ => Err(ShareError::UnexpectedEvent("release")),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<(), ShareError> {
        self.output.unwrap_or(Err(ShareError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_storage::storage::FjallStorage;

    #[tokio::test]
    async fn claims_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let claim = |now| ClaimNameOperation::new("data", "in/a.txt", now, 200);
        assert!(drive(claim(100), &context).await.unwrap());
        assert!(!drive(claim(150), &context).await.unwrap());
        // An expired claim from a stopped upload frees the name again.
        assert!(drive(claim(250), &context).await.unwrap());
        drive(ReleaseNameOperation::new("data", "in/a.txt"), &context)
            .await
            .unwrap();
        assert!(drive(claim(150), &context).await.unwrap());
        let other = ClaimNameOperation::new("data", "in/b.txt", 100, 200);
        assert!(drive(other, &context).await.unwrap());
    }
}
