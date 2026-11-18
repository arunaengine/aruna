//! Marks the encrypted buckets of a group, or of the node, due for an epoch raise after a change
//! that lost a READ scope, inside that change's transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, GROUP_ENCRYPTED_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::abe_access::{due_rows, indexed_buckets};
use aruna_core::types::{GroupId, Key};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub enum EpochDueError {
    #[error("encryption storage is unavailable")]
    Storage,
}

impl From<IoError> for EpochDueError {
    fn from(_: IoError) -> Self {
        Self::Storage
    }
}

/// The calls that mark a raise due in every encrypted bucket of a group, or of the node without a
/// group. A parent folds them into the transaction of the change that lost a READ scope, so the
/// marks commit with it. A group without encrypted buckets costs one index scan and nothing else.
#[derive(Debug, PartialEq)]
pub struct DueMarks {
    group_id: Option<GroupId>,
    now: u64,
    /// The indexed buckets, once the index is scanned.
    buckets: Option<Vec<String>>,
    managed: Vec<String>,
}

impl DueMarks {
    pub fn new(group_id: Option<GroupId>, now: u64) -> Self {
        Self {
            group_id,
            now,
            buckets: None,
            managed: Vec::new(),
        }
    }

    /// The scan of the encrypted bucket index.
    pub fn first(&self) -> IoCall {
        let prefix = self.group_id.map(|group| group.to_bytes().to_vec());
        IoCall::Page {
            space: GROUP_ENCRYPTED_KEYSPACE.to_string(),
            prefix: Key::from(prefix.unwrap_or_default()),
            after: None,
            limit: usize::MAX,
        }
    }

    /// The call after `result`, or `None` when the marks are done. Without `write` it only finds
    /// the node managed buckets to raise.
    pub fn next(&mut self, result: IoResult, write: bool) -> Result<Option<IoCall>, EpochDueError> {
        if self.buckets.is_none() {
            let IoResult::Page { rows, .. } = result else {
                return Err(EpochDueError::Storage);
            };
            let buckets = indexed_buckets(&rows);
            let reads = (buckets.iter())
                .map(|bucket| {
                    let key = Key::from(bucket.as_bytes().to_vec());
                    (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key)
                })
                .collect::<Vec<_>>();
            self.buckets = Some(buckets);
            return Ok((!reads.is_empty()).then_some(IoCall::Read(reads)));
        }
        let values = match result {
            IoResult::Values(values) => values,
            IoResult::Done => return Ok(None),
            _ => return Err(EpochDueError::Storage),
        };
        let buckets = self.buckets.as_deref().unwrap_or_default();
        let settings = (buckets.iter().zip(values))
            .map(|(bucket, value)| (Key::from(bucket.as_bytes().to_vec()), value))
            .collect();
        let (writes, managed) = due_rows(buckets, settings, self.now);
        self.managed = managed;
        Ok((write && !writes.is_empty()).then_some(IoCall::Write(writes)))
    }

    /// The node managed buckets whose raise was marked.
    pub fn managed(&mut self) -> Vec<String> {
        std::mem::take(&mut self.managed)
    }
}

/// Lists the node managed buckets of a group, or of the node, that a raise would mark, and
/// writes nothing.
#[derive(Debug, PartialEq)]
pub struct EpochDueOperation {
    marks: DueMarks,
}

impl EpochDueOperation {
    pub fn new(group_id: Option<GroupId>, now: u64) -> Self {
        Self {
            marks: DueMarks::new(group_id, now),
        }
    }
}

impl Operation for EpochDueOperation {
    type Output = Vec<String>;
    type Error = EpochDueError;

    fn first(&mut self) -> Result<Next<Vec<String>>, EpochDueError> {
        Ok(Next::Call(self.marks.first()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<String>>, EpochDueError> {
        Ok(match self.marks.next(result, false)? {
            Some(call) => Next::Call(call),
            None => Next::Done(self.marks.managed()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_early_write() {
        // A write answer before the index scan fails the marks.
        let mut marks = DueMarks::new(None, 1);
        assert!(matches!(marks.first(), IoCall::Page { .. }));
        assert_eq!(
            marks.next(IoResult::Done, true),
            Err(EpochDueError::Storage)
        );
    }

    #[test]
    fn empty_index_ends() {
        // A scope without encrypted buckets ends after the scan, so listing writes nothing.
        let mut operation = EpochDueOperation::new(None, 1);
        operation.first().unwrap();
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(operation.next(page), Ok(Next::Done(Vec::new())));
    }
}
