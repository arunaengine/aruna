//! Starts this node's check of a bucket's current versions for copies that cannot be served.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    S3_BUCKET_KEYSPACE, STORAGE_CHECK_KEYSPACE, STORAGE_CHECK_QUEUE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::broken::StorageCheck;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum StartCheckError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("the storage check did not start")]
    NotFinished,
    #[error("unexpected event: {0:?}")]
    Unexpected(Box<Event>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartCheckState {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Finish,
}

/// Replaces the bucket's check record with a fresh one and wakes the migration task.
#[derive(Debug, PartialEq)]
pub struct StartCheckOperation {
    bucket: String,
    group_id: GroupId,
    record: StorageCheck,
    state: StartCheckState,
    txn_id: Option<TxnId>,
    output: Option<Result<StorageCheck, StartCheckError>>,
}

impl StartCheckOperation {
    pub fn new(bucket: String, group_id: GroupId, now_ms: u64) -> Self {
        Self {
            bucket,
            group_id,
            record: StorageCheck::new(now_ms),
            state: StartCheckState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn fail(&mut self, error: StartCheckError) -> Effects {
        self.state = StartCheckState::Finish;
        self.output = Some(Err(error));
        self.abort()
    }

    fn write(&mut self, value: Option<&[u8]>) -> Effects {
        match value.map(BucketInfo::from_bytes).transpose() {
            Ok(Some(info)) if info.group_id == self.group_id => {}
            Ok(_) => return self.fail(StartCheckError::NoSuchBucket),
            Err(error) => return self.fail(error.into()),
        }
        let value = match self.record.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        self.state = StartCheckState::Write;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (STORAGE_CHECK_KEYSPACE.to_string(), self.key(), value.into()),
                (
                    STORAGE_CHECK_QUEUE_KEYSPACE.to_string(),
                    self.key(),
                    Vec::<u8>::new().into()
                ),
            ],
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for StartCheckOperation {
    type Output = StorageCheck;
    type Error = StartCheckError;

    fn start(&mut self) -> Effects {
        self.state = StartCheckState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (
                StartCheckState::Start,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = StartCheckState::Read;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.key(),
                    txn_id: Some(txn_id),
                })]
            }
            (StartCheckState::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.write(value.as_deref())
            }
            (StartCheckState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(StartCheckError::NotFinished);
                };
                self.state = StartCheckState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (
                StartCheckState::Commit,
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.state = StartCheckState::Finish;
                self.output = Some(Ok(self.record.clone()));
                smallvec![Effect::Task(TaskEffect::ShortenTimer {
                    key: TaskKey::MigrateCompression,
                    after: Duration::ZERO,
                })]
            }
            (_, event) => self.fail(StartCheckError::Unexpected(Box::new(event))),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == StartCheckState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            StartCheckState::Finish => self.output.unwrap_or(Err(StartCheckError::NotFinished)),
            _ => Err(StartCheckError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}
