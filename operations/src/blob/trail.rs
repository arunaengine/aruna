//! Drops the delete trail of a removed bucket or group, a page at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::DELETE_TRAIL_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::delete_audit::bucket_prefix;
use aruna_core::types::Key;
use thiserror::Error;
use ulid::Ulid;

const TRAIL_PAGE: usize = 500;

#[derive(Debug, Error, PartialEq)]
pub enum TrailError {
    #[error("could not drop trail rows: {0}")]
    Storage(#[from] StorageError),
    #[error("unexpected answer while dropping trail rows: {0}")]
    Io(IoError),
}

impl From<IoError> for TrailError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

/// Deletes the trail rows of one incarnation of `bucket`, or of the whole group without one.
/// A bucket created again under the name has its own rows, which stay. An interrupted pass
/// leaves the rest for the next one.
#[derive(Debug, PartialEq)]
pub(crate) struct DropTrailOperation {
    prefix: Key,
    /// Whether the page being deleted was the last one; `None` while a page is read.
    last: Option<bool>,
}

impl DropTrailOperation {
    pub(crate) fn new(group_id: Ulid, bucket: Option<&str>, incarnation: u128) -> Self {
        let prefix = match bucket {
            Some(bucket) => bucket_prefix(group_id, bucket, incarnation).into(),
            None => group_id.to_bytes().to_vec().into(),
        };
        Self { prefix, last: None }
    }

    /// Always reads from the prefix start, since the previous page is gone by then.
    fn read_page(&mut self) -> Next<()> {
        self.last = None;
        Next::Call(IoCall::Page {
            space: DELETE_TRAIL_KEYSPACE.to_string(),
            prefix: self.prefix.clone(),
            after: None,
            limit: TRAIL_PAGE,
        })
    }
}

impl Operation for DropTrailOperation {
    type Output = ();
    type Error = TrailError;

    fn first(&mut self) -> Result<Next<()>, TrailError> {
        Ok(self.read_page())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, TrailError> {
        if let Some(last) = self.last {
            result.done()?;
            return Ok(match last {
                true => Next::Done(()),
                false => self.read_page(),
            });
        }
        let (rows, next) = result.page()?;
        self.last = Some(next.is_none());
        let deletes = (rows.into_iter())
            .map(|(key, _)| (DELETE_TRAIL_KEYSPACE.to_string(), key))
            .collect();
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::types::Value;

    fn group() -> Ulid {
        Ulid::from_parts(1, 1)
    }

    fn read() -> Next<()> {
        Next::Call(IoCall::Page {
            space: DELETE_TRAIL_KEYSPACE.to_string(),
            prefix: bucket_prefix(group(), "b", 1).into(),
            after: None,
            limit: TRAIL_PAGE,
        })
    }

    fn page(keys: &[&[u8]], next: Option<&[u8]>) -> IoResult {
        let rows: Vec<(Key, Value)> = (keys.iter())
            .map(|key| (key.to_vec().into(), Vec::new().into()))
            .collect();
        let next = next.map(|key| key.to_vec().into());
        IoResult::Page { rows, next }
    }

    fn delete(keys: &[&[u8]]) -> Next<()> {
        Next::Call(IoCall::Delete(
            (keys.iter())
                .map(|key| (DELETE_TRAIL_KEYSPACE.to_string(), key.to_vec().into()))
                .collect(),
        ))
    }

    #[test]
    fn drops_trail_pages() {
        let mut operation = DropTrailOperation::new(group(), Some("b"), 1);
        assert_eq!(operation.first(), Ok(read()));
        assert_eq!(
            operation.next(page(&[b"a"], Some(b"a"))),
            Ok(delete(&[b"a"]))
        );
        assert_eq!(operation.next(IoResult::Done), Ok(read()));
        assert_eq!(operation.next(page(&[b"b"], None)), Ok(delete(&[b"b"])));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn group_trail_errors() {
        let mut operation = DropTrailOperation::new(group(), None, 0);
        let Ok(Next::Call(IoCall::Page { prefix, .. })) = operation.first() else {
            panic!("expected a trail page");
        };
        assert_eq!(prefix, Key::from(group().to_bytes().to_vec()));
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(TrailError::Io(IoError::Unexpected { .. }))
        ));
        let storage = TrailError::from(IoError::Storage(StorageError::Timeout));
        assert_eq!(storage, TrailError::Storage(StorageError::Timeout));
    }
}
