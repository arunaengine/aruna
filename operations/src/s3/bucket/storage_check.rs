//! Starts this node's check of a bucket's current versions for copies that cannot be served.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{CHECK_QUEUE_KEYSPACE, S3_BUCKET_KEYSPACE, STORAGE_CHECK_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::broken::StorageCheck;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{GroupId, Key};
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
    #[error("the storage check did not start: {0}")]
    Io(IoError),
}

impl From<IoError> for StartCheckError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

type Step = Result<Next<StorageCheck>, StartCheckError>;

/// Replaces the bucket's check record with a fresh one and wakes the migration task once the
/// record committed.
#[derive(Debug, PartialEq)]
pub struct StartCheckOperation {
    bucket: String,
    group_id: GroupId,
    record: StorageCheck,
    written: bool,
}

impl StartCheckOperation {
    pub fn new(bucket: String, group_id: GroupId, now_ms: u64) -> Self {
        Self {
            bucket,
            group_id,
            record: StorageCheck::new(now_ms),
            written: false,
        }
    }

    fn key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn write(&mut self, value: Option<&[u8]>) -> Step {
        match value.map(BucketInfo::from_bytes).transpose()? {
            Some(info) if info.group_id == self.group_id => {}
            _ => return Err(StartCheckError::NoSuchBucket),
        }
        let value = self.record.to_bytes()?;
        self.written = true;
        Ok(Next::Call(IoCall::Write(vec![
            (STORAGE_CHECK_KEYSPACE.to_string(), self.key(), value.into()),
            (
                CHECK_QUEUE_KEYSPACE.to_string(),
                self.key(),
                Vec::<u8>::new().into(),
            ),
        ])))
    }
}

/// The wake of the migration task, sent after the commit so the task sees the record.
fn wake_migration() -> IoCall {
    let wake = TaskEffect::ShortenTimer {
        key: TaskKey::MigrateCompression,
        after: Duration::ZERO,
    };
    IoCall::Effect(Box::new(Effect::Task(wake)))
}

impl Operation for StartCheckOperation {
    type Output = StorageCheck;
    type Error = StartCheckError;

    fn first(&mut self) -> Step {
        Ok(Next::Call(read_row(S3_BUCKET_KEYSPACE, self.key())))
    }

    fn next(&mut self, result: IoResult) -> Step {
        if !self.written {
            return self.write(result.value()?.as_deref());
        }
        result.done()?;
        Ok(Next::Then {
            output: self.record.clone(),
            calls: vec![wake_migration()],
        })
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn group() -> GroupId {
        Ulid::from_parts(1, 1)
    }

    fn bucket(group_id: GroupId) -> IoResult {
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: None,
        };
        IoResult::Values(vec![Some(info.to_bytes().unwrap().into())])
    }

    #[test]
    fn wakes_after_commit() {
        let mut operation = StartCheckOperation::new("b".to_string(), group(), 5);
        assert!(operation.transaction());
        let read = read_row(S3_BUCKET_KEYSPACE, b"b".as_slice());
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let Ok(Next::Call(IoCall::Write(writes))) = operation.next(bucket(group())) else {
            panic!("expected the check record write");
        };
        let spaces: Vec<_> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
        assert_eq!(spaces, [STORAGE_CHECK_KEYSPACE, CHECK_QUEUE_KEYSPACE]);
        let then = Next::Then {
            output: StorageCheck::new(5),
            calls: vec![wake_migration()],
        };
        assert_eq!(operation.next(IoResult::Done), Ok(then));
    }

    #[test]
    fn foreign_bucket_refused() {
        let mut operation = StartCheckOperation::new("b".to_string(), group(), 5);
        let other = bucket(Ulid::from_parts(2, 2));
        assert_eq!(operation.next(other), Err(StartCheckError::NoSuchBucket));
        let mut missing = StartCheckOperation::new("b".to_string(), group(), 5);
        let none = IoResult::Values(vec![None]);
        assert_eq!(missing.next(none), Err(StartCheckError::NoSuchBucket));
    }
}
