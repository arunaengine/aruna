//! Claims an object key for one share upload, so concurrent uploads of the same name pick
//! different keys before any bytes are written.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The claim only steers naming; the create-only write is what keeps a file from being replaced.

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::SHARE_NAME_KEYSPACE;
use aruna_core::operation::{AfterConflict, Operation, TransactionMode};
use aruna_core::rows;
use aruna_core::types::Key;

use crate::shares::lifecycle::ShareError;

fn name_key(bucket: &str, key: &str) -> Key {
    [bucket.as_bytes(), &[0], key.as_bytes()].concat().into()
}

/// Claims `bucket`/`key` until `until` (Unix seconds); `false` when a live claim holds it. Run it
/// with a conflict retry: a lost conflict means a concurrent claim won, so it answers `false`.
#[derive(Debug, PartialEq)]
pub struct ClaimNameOperation {
    row: Key,
    now: u64,
    until: u64,
    writing: bool,
}

impl ClaimNameOperation {
    pub fn new(bucket: &str, key: &str, now: u64, until: u64) -> Self {
        Self {
            row: name_key(bucket, key),
            now,
            until,
            writing: false,
        }
    }
}

impl Operation for ClaimNameOperation {
    type Output = bool;
    type Error = ShareError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<bool>, ShareError> {
        Ok(Next::Call(rows::read_row(
            SHARE_NAME_KEYSPACE,
            self.row.clone(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, ShareError> {
        if self.writing {
            rows::done(result)?;
            return Ok(Next::Done(true));
        }
        let held = rows::value(result)?
            .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_ref()).ok())
            .is_some_and(|until| u64::from_be_bytes(until) > self.now);
        if held {
            return Ok(Next::Done(false));
        }
        self.writing = true;
        Ok(Next::Call(IoCall::Write(vec![(
            SHARE_NAME_KEYSPACE.to_string(),
            self.row.clone(),
            self.until.to_be_bytes().to_vec().into(),
        )])))
    }

    fn on_conflict(&mut self, _attempt: u32) -> AfterConflict<bool> {
        AfterConflict::Finish(false)
    }
}

/// Drops the claim on `bucket`/`key` once its upload ended.
#[derive(Debug, PartialEq)]
pub struct ReleaseNameOperation {
    row: Key,
}

impl ReleaseNameOperation {
    pub fn new(bucket: &str, key: &str) -> Self {
        Self {
            row: name_key(bucket, key),
        }
    }
}

impl Operation for ReleaseNameOperation {
    type Output = ();
    type Error = ShareError;

    fn first(&mut self) -> Result<Next<()>, ShareError> {
        let row = (SHARE_NAME_KEYSPACE.to_string(), self.row.clone());
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ShareError> {
        rows::done(result)?;
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive, drive_retry};
    use aruna_storage::storage::FjallStorage;

    fn context(dir: &tempfile::TempDir) -> DriverContext {
        DriverContext {
            storage_handle: FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    /// Reads the name row and answers it with `stored`.
    fn answered(stored: Option<u64>) -> (ClaimNameOperation, Result<Next<bool>, ShareError>) {
        let mut operation = ClaimNameOperation::new("data", "in/a.txt", 100, 200);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        let row = (
            SHARE_NAME_KEYSPACE.to_string(),
            name_key("data", "in/a.txt"),
        );
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(vec![row]))));
        let value = stored.map(|until| until.to_be_bytes().to_vec().into());
        let turn = operation.next(IoResult::Values(vec![value]));
        (operation, turn)
    }

    #[test]
    fn free_name_written() {
        // A free or expired name is claimed until the given time.
        for stored in [None, Some(100), Some(50)] {
            let (mut operation, turn) = answered(stored);
            let row = (
                SHARE_NAME_KEYSPACE.to_string(),
                name_key("data", "in/a.txt"),
                200u64.to_be_bytes().to_vec().into(),
            );
            assert_eq!(turn, Ok(Next::Call(IoCall::Write(vec![row]))));
            assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
        }
    }

    #[test]
    fn live_claim_refuses() {
        let (_, turn) = answered(Some(101));
        assert_eq!(turn, Ok(Next::Done(false)));
    }

    #[test]
    fn lost_conflict_refuses() {
        // The concurrent claim that won the commit holds the name; nothing is retried.
        let mut operation = ClaimNameOperation::new("data", "in/a.txt", 100, 200);
        assert_eq!(operation.on_conflict(1), AfterConflict::Finish(false));
        let lost = ShareError::from(aruna_core::calls::IoError::Conflict);
        assert!(ClaimNameOperation::is_conflict(&lost));
    }

    #[test]
    fn release_deletes_row() {
        let mut operation = ReleaseNameOperation::new("data", "in/a.txt");
        let row = (
            SHARE_NAME_KEYSPACE.to_string(),
            name_key("data", "in/a.txt"),
        );
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Delete(vec![row]))));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[tokio::test]
    async fn claims_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let context = context(&dir);
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

    #[tokio::test]
    async fn one_claim_wins() {
        // Concurrent claims of one name leave exactly one winner and no error.
        let dir = tempfile::tempdir().unwrap();
        let context = context(&dir);
        let build = || ClaimNameOperation::new("data", "in/a.txt", 100, 200);
        let runs: Vec<_> = (0..8)
            .map(|_| drive_retry(build, &context, b"claim"))
            .collect();
        let claimed = futures_util::future::join_all(runs).await;
        let won = claimed.into_iter().filter(|run| *run == Ok(true)).count();
        assert_eq!(won, 1);
    }
}
