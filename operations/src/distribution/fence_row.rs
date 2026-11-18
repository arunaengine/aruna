//! Changes a set's local fence row in one transaction. A write that read the row before
//! conflicts with this commit, and a conflicted change is driven again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::DISTRIBUTION_FENCE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::done;
use aruna_core::structs::storage::distribution::fence::{LocalFence, RangeFence};
use aruna_core::types::Key;
use thiserror::Error;
use ulid::Ulid;

/// One change of a fence row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum FenceChange {
    /// Closes the set to incoming publications.
    Close,
    /// Fences every range for the deletion with this identity and closes the set.
    Delete([u8; 32]),
    /// Fences one range and keeps how many byte copies it had then.
    Range { fence: RangeFence, copies: u32 },
    /// Closes the uploads of one fenced range.
    Uploads(Ulid),
}

impl FenceChange {
    /// Applies the change and says whether it changed anything.
    fn apply(&self, local: &mut LocalFence) -> bool {
        match self {
            FenceChange::Close => !std::mem::replace(&mut local.closed, true),
            FenceChange::Delete(identity) => {
                let changed = local.deletion != Some(*identity) || !local.closed;
                local.deletion = Some(*identity);
                local.closed = true;
                changed
            }
            FenceChange::Range { fence, copies } => {
                let fenced = local.fence_range(fence.clone());
                if fenced {
                    local.copies.retain(|(id, _)| *id != fence.range_id);
                    local.copies.push((fence.range_id, *copies));
                }
                fenced
            }
            FenceChange::Uploads(range_id) => local.close_uploads(*range_id),
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub(super) enum FenceError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("{0}")]
    Failed(String),
}

/// Applies one change to the fence row of `set_id` in one transaction and outputs the row as
/// it is afterwards. A conflict with a write that read the row is driven again.
#[derive(Debug, PartialEq)]
pub(super) struct UpdateFenceOperation {
    set_id: Ulid,
    change: FenceChange,
    /// The changed row, once it is being written.
    written: Option<LocalFence>,
}

impl UpdateFenceOperation {
    pub(super) fn new(set_id: Ulid, change: FenceChange) -> Self {
        Self {
            set_id,
            change,
            written: None,
        }
    }

    fn key(&self) -> Key {
        LocalFence::key(self.set_id).into()
    }
}

impl Operation for UpdateFenceOperation {
    type Output = LocalFence;
    type Error = FenceError;

    fn first(&mut self) -> Result<Next<LocalFence>, FenceError> {
        let row = (DISTRIBUTION_FENCE_KEYSPACE.to_string(), self.key());
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LocalFence>, FenceError> {
        if let Some(fence) = self.written.take() {
            done(result)?;
            return Ok(Next::Done(fence));
        }
        let value = result.values()?.into_iter().next().flatten();
        let failed =
            |error: aruna_core::errors::ConversionError| FenceError::Failed(error.to_string());
        let mut fence = LocalFence::decode(value.as_deref()).map_err(failed)?;
        if !self.change.apply(&mut fence) {
            return Ok(Next::Done(fence));
        }
        let value = fence.to_bytes().map_err(failed)?;
        let row = (
            DISTRIBUTION_FENCE_KEYSPACE.to_string(),
            self.key(),
            value.into(),
        );
        self.written = Some(fence);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(value: Option<&LocalFence>) -> IoResult {
        IoResult::Values(vec![value.map(|fence| fence.to_bytes().unwrap().into())])
    }

    fn set() -> Ulid {
        Ulid::from_bytes([1; 16])
    }

    // A change is written in the transaction; the row is answered as it is afterwards.
    #[test]
    fn closes_open_set() {
        let closed = LocalFence {
            closed: true,
            ..LocalFence::default()
        };
        let mut update = UpdateFenceOperation::new(set(), FenceChange::Close);
        assert_eq!(
            update.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        let key: Key = LocalFence::key(set()).into();
        let row = (DISTRIBUTION_FENCE_KEYSPACE.to_string(), key.clone());
        assert_eq!(update.first(), Ok(Next::Call(IoCall::Read(vec![row]))));
        let value = closed.to_bytes().unwrap().into();
        let write = (DISTRIBUTION_FENCE_KEYSPACE.to_string(), key, value);
        assert_eq!(
            update.next(read(None)),
            Ok(Next::Call(IoCall::Write(vec![write])))
        );
        assert_eq!(update.next(IoResult::Done), Ok(Next::Done(closed)));
    }

    // An unchanged row is answered without a write.
    #[test]
    fn unchanged_row_kept() {
        let identity = [4; 32];
        let deleted = LocalFence {
            deletion: Some(identity),
            closed: true,
            ..LocalFence::default()
        };
        let mut update = UpdateFenceOperation::new(set(), FenceChange::Delete(identity));
        update.first().unwrap();
        assert_eq!(update.next(read(Some(&deleted))), Ok(Next::Done(deleted)));
    }

    // The runner's conflict is a conflict to drive again; a failed decode is not.
    #[test]
    fn conflict_retried() {
        assert!(UpdateFenceOperation::is_conflict(&IoError::Conflict.into()));
        let failed = FenceError::Failed("the fence row does not decode".to_string());
        assert!(!UpdateFenceOperation::is_conflict(&failed));
        let mut update = UpdateFenceOperation::new(set(), FenceChange::Uploads(set()));
        update.first().unwrap();
        let garbage = IoResult::Values(vec![Some(vec![0xFF].into())]);
        assert!(matches!(update.next(garbage), Err(FenceError::Failed(_))));
    }
}
