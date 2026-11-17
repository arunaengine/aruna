//! Reads one group storage backend record and lists a group's backends page by page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::RecordReadError;
use super::{backend_key, index_prefix};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{BACKEND_INDEX_KEYSPACE, STORAGE_BACKEND_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::{GroupId, Key};
use thiserror::Error;
use ulid::Ulid;

const LIST_PAGE_SIZE: usize = 128;

#[derive(Debug, Error, PartialEq)]
pub enum GroupQueryError {
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("query never completed")]
    Incomplete,
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Reads one backend record. The caller checks that the record's group matches
/// the authorized one; the key itself carries no group.
#[derive(Debug, PartialEq)]
pub struct GetBackendOperation {
    backend_id: Ulid,
}

impl GetBackendOperation {
    pub fn new(backend_id: Ulid) -> Self {
        Self { backend_id }
    }
}

impl Operation for GetBackendOperation {
    type Output = Option<GroupStorage>;
    type Error = GroupQueryError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        let row = (
            STORAGE_BACKEND_KEYSPACE.to_string(),
            backend_key(self.backend_id),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let IoResult::Values(values) = result else {
            return Err(RecordReadError::Unexpected.into());
        };
        let record = (values.into_iter().next().flatten())
            .map(|value| GroupStorage::from_bytes(value.as_ref()))
            .transpose()
            .map_err(RecordReadError::Conversion)?;
        Ok(Next::Done(record))
    }
}

/// Lists a group's backends from the group-prefixed index, so one tenant's
/// listing never walks another's records.
#[derive(Debug, PartialEq)]
pub struct ListBackendsOperation {
    group_id: GroupId,
    found: Vec<GroupStorage>,
}

impl ListBackendsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            found: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> Next<Vec<GroupStorage>> {
        Next::Call(IoCall::Page {
            space: BACKEND_INDEX_KEYSPACE.to_string(),
            prefix: index_prefix(self.group_id),
            after,
            limit: LIST_PAGE_SIZE,
        })
    }
}

impl Operation for ListBackendsOperation {
    type Output = Vec<GroupStorage>;
    type Error = GroupQueryError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let IoResult::Page { rows, next } = result else {
            return Err(RecordReadError::Unexpected.into());
        };
        for (_, value) in rows {
            let record =
                GroupStorage::from_bytes(value.as_ref()).map_err(RecordReadError::Conversion)?;
            if record.group_id == self.group_id {
                self.found.push(record);
            }
        }
        Ok(match next {
            Some(after) => self.page(Some(after)),
            None => Next::Done(std::mem::take(&mut self.found)),
        })
    }
}

/// Why a bucket check has no answer: the storage's refusal, or a runner failure.
#[derive(Debug, Error, PartialEq)]
pub enum BucketCheckError {
    #[error(transparent)]
    Blob(BlobError),
    #[error(transparent)]
    Io(#[from] IoError),
}

impl BucketCheckError {
    /// The message a caller shows for the failed check.
    pub fn reason(&self) -> String {
        match self {
            Self::Blob(error) => error.reason(),
            Self::Io(error) => error.to_string(),
        }
    }
}

/// Whether `bucket` exists on the storage without a bucket `backend_id`, and for an existing
/// bucket whether Aruna may write and delete there. Nothing is created.
#[derive(Debug, PartialEq)]
pub struct BucketCheckOperation {
    backend_id: Ulid,
    bucket: String,
}

impl BucketCheckOperation {
    pub fn new(backend_id: Ulid, bucket: &str) -> Self {
        Self {
            backend_id,
            bucket: bucket.to_string(),
        }
    }
}

impl Operation for BucketCheckOperation {
    type Output = (bool, Option<bool>);
    type Error = BucketCheckError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        let head = Effect::Blob(BlobEffect::HeadBucket {
            backend: BackendRef::Group(self.backend_id),
            bucket: self.bucket.clone(),
        });
        Ok(Next::Call(IoCall::Effect(Box::new(head))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let IoResult::Event(event) = result else {
            return Err(BucketCheckError::Blob(BlobError::UnknownBackend(
                "unexpected bucket check answer".to_string(),
            )));
        };
        match *event {
            Event::Blob(BlobEvent::BucketExists { exists, writable }) => {
                Ok(Next::Done((exists, writable)))
            }
            // Kept from the direct handle call: a node without blob access says so this way.
            Event::Blob(BlobEvent::Error(BlobError::HandleMissing)) => {
                let missing = BlobError::UnknownBackend("blob handle unavailable".to_string());
                Err(BucketCheckError::Blob(missing))
            }
            Event::Blob(BlobEvent::Error(error)) => Err(BucketCheckError::Blob(error)),
            other => Err(BucketCheckError::Blob(BlobError::UnknownBackend(format!(
                "unexpected bucket check answer: {}",
                other.label()
            )))),
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::super::RecordReadError;
    use super::super::{backend_key, index_key, index_prefix};
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn record(group_id: Ulid) -> GroupStorage {
        GroupStorage {
            backend_id: Ulid::from_bytes([9u8; 16]),
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::B2,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled: false,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        }
    }

    #[test]
    fn reads_one_record() {
        // One read of the backend row; a missing row is none.
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let stored = record(Ulid::from_bytes([1u8; 16]));
        let mut operation = GetBackendOperation::new(backend_id);
        let row = (
            STORAGE_BACKEND_KEYSPACE.to_string(),
            backend_key(backend_id),
        );
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(vec![row]))));
        let answer = IoResult::Values(vec![Some(stored.to_bytes().unwrap().into())]);
        assert_eq!(operation.next(answer), Ok(Next::Done(Some(stored))));
        let mut operation = GetBackendOperation::new(backend_id);
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
    }

    #[test]
    fn scopes_by_group() {
        // The index prefix keeps another tenant's records out, and a stray record is dropped.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mine = record(group_id);
        let other = record(Ulid::from_bytes([2u8; 16]));
        let mut operation = ListBackendsOperation::new(group_id);
        let page = |after| {
            Next::Call(IoCall::Page {
                space: BACKEND_INDEX_KEYSPACE.to_string(),
                prefix: index_prefix(group_id),
                after,
                limit: LIST_PAGE_SIZE,
            })
        };
        assert_eq!(operation.first(), Ok(page(None)));
        let cursor = index_key(group_id, mine.backend_id);
        let rows = vec![(cursor.clone(), mine.to_bytes().unwrap().into())];
        let answer = operation.next(IoResult::Page {
            rows,
            next: Some(cursor.clone()),
        });
        assert_eq!(answer, Ok(page(Some(cursor))));
        let rows = vec![(Key::from(vec![1u8]), other.to_bytes().unwrap().into())];
        let answer = operation.next(IoResult::Page { rows, next: None });
        assert_eq!(answer, Ok(Next::Done(vec![mine])));
    }

    #[test]
    fn query_failures_end() {
        // An undecodable row, an answer of the wrong kind or a runner failure ends the query.
        let mut operation = GetBackendOperation::new(Ulid::from_bytes([9u8; 16]));
        operation.first().unwrap();
        let broken = IoResult::Values(vec![Some(vec![0xff; 3].into())]);
        assert!(matches!(
            operation.next(broken),
            Err(GroupQueryError::Read(RecordReadError::Conversion(_)))
        ));
        let mut operation = ListBackendsOperation::new(Ulid::from_bytes([1u8; 16]));
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Done),
            Err(GroupQueryError::Read(RecordReadError::Unexpected))
        );
        let error = GroupQueryError::from(IoError::Storage(StorageError::Timeout));
        assert_eq!(
            error,
            GroupQueryError::Io(IoError::Storage(StorageError::Timeout))
        );
    }

    fn bucket_check() -> BucketCheckOperation {
        BucketCheckOperation::new(Ulid::from_bytes([9u8; 16]), "data")
    }

    #[test]
    fn checks_one_bucket() {
        // One head request on the group storage; its answer is the result.
        let mut operation = bucket_check();
        let head = Effect::Blob(BlobEffect::HeadBucket {
            backend: BackendRef::Group(Ulid::from_bytes([9u8; 16])),
            bucket: "data".to_string(),
        });
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(head))))
        );
        let answer = Event::Blob(BlobEvent::BucketExists {
            exists: true,
            writable: Some(false),
        });
        let answer = operation.next(IoResult::Event(Box::new(answer)));
        assert_eq!(answer, Ok(Next::Done((true, Some(false)))));
    }

    #[test]
    fn bucket_errors_returned() {
        // A storage error is returned as is; a node without blob access names that.
        let down = || BlobError::ConnectionFailed("down".to_string());
        let mut operation = bucket_check();
        operation.first().unwrap();
        let error = Event::Blob(BlobEvent::Error(down()));
        let answer = operation.next(IoResult::Event(Box::new(error)));
        assert_eq!(answer, Err(BucketCheckError::Blob(down())));
        let missing = Event::Blob(BlobEvent::Error(BlobError::HandleMissing));
        let mut operation = bucket_check();
        operation.first().unwrap();
        let unknown = BlobError::UnknownBackend("blob handle unavailable".to_string());
        assert_eq!(
            operation.next(IoResult::Event(Box::new(missing))),
            Err(BucketCheckError::Blob(unknown))
        );
    }

    #[test]
    fn bucket_rejects_answers() {
        // An answer that is not a blob event fails the check with a reason.
        let mut operation = bucket_check();
        operation.first().unwrap();
        let error = operation.next(IoResult::Done).unwrap_err();
        assert!(error.reason().contains("unexpected bucket check answer"));
    }
}
