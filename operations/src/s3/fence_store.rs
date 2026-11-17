//! Checks, takes and releases the per bucket write fence of a permanent purge in storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::PURGE_FENCE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::storage::storage_purge::{StoragePurgeFence, StoragePurgeScope};
use aruna_core::types::{Key, Value};

use crate::s3::purge_fence::{PurgeFenceError, check_decoded_fence, fence_key};

/// The fence row of `bucket`.
fn fence_read(bucket: &str) -> IoCall {
    IoCall::Read(vec![(PURGE_FENCE_KEYSPACE.to_string(), fence_key(bucket))])
}

fn decode(value: Option<Value>) -> Result<Option<StoragePurgeFence>, PurgeFenceError> {
    (value.map(|value| StoragePurgeFence::from_bytes(value.as_ref())))
        .transpose()
        .map_err(|_| PurgeFenceError::Invalid)
}

/// Cheap rejection before a writer moves payload bytes. Mutating operations still read the
/// fence again in their commit transaction to close the race with a fence acquired later.
#[derive(Debug, PartialEq)]
pub struct FenceCheckOperation {
    bucket: String,
    key: String,
}

impl FenceCheckOperation {
    pub fn new(bucket: &str, key: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }
    }
}

impl Operation for FenceCheckOperation {
    type Output = ();
    type Error = PurgeFenceError;

    fn first(&mut self) -> Result<Next<()>, PurgeFenceError> {
        Ok(Next::Call(fence_read(&self.bucket)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeFenceError> {
        let fence = decode(result.value()?)?;
        check_decoded_fence(fence.as_ref(), &self.bucket, &self.key)?;
        Ok(Next::Done(()))
    }
}

/// Takes the write fence of `scope` for `job_id`, or confirms that the job already holds it.
/// A commit conflict is retried by the runner (`drive_retry`).
#[derive(Debug, PartialEq)]
pub struct AcquireFenceOperation {
    job_id: JobId,
    scope: StoragePurgeScope,
    writing: bool,
}

impl AcquireFenceOperation {
    pub fn new(job_id: JobId, scope: StoragePurgeScope) -> Self {
        Self {
            job_id,
            scope,
            writing: false,
        }
    }

    fn fence_read(&mut self, value: Option<Value>) -> Result<Next<()>, PurgeFenceError> {
        if let Some(existing) = decode(value)? {
            // Holding the fence already is success; the transaction wrote nothing.
            return match (existing.job_id == self.job_id, existing.scope == self.scope) {
                (false, _) => Err(PurgeFenceError::Busy),
                (true, true) => Ok(Next::Done(())),
                (true, false) => Err(PurgeFenceError::Invalid),
            };
        }
        let fence = StoragePurgeFence {
            job_id: self.job_id,
            scope: self.scope.clone(),
        };
        let value = fence.to_bytes().map_err(|_| PurgeFenceError::Invalid)?;
        let key: Key = fence_key(self.scope.bucket());
        self.writing = true;
        let row = (PURGE_FENCE_KEYSPACE.to_string(), key, value.into());
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for AcquireFenceOperation {
    type Output = ();
    type Error = PurgeFenceError;

    fn first(&mut self) -> Result<Next<()>, PurgeFenceError> {
        Ok(Next::Call(fence_read(self.scope.bucket())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeFenceError> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(()));
        }
        self.fence_read(result.value()?)
    }

    fn transaction(&self) -> bool {
        true
    }
}

/// The delete of the bucket fence row, read in a job-terminal transaction, when the terminal
/// job still owns it. A cancelled waiter must never clear the fence of the purge that won.
pub fn owned_fence_delete(
    value: Option<&[u8]>,
    job_id: JobId,
    scope: &StoragePurgeScope,
) -> Result<Option<(String, Key)>, PurgeFenceError> {
    let fence = value
        .map(|value| StoragePurgeFence::from_bytes(value).map_err(|_| PurgeFenceError::Invalid))
        .transpose()?;
    match fence {
        Some(fence) if fence.job_id == job_id && fence.scope == *scope => Ok(Some((
            PURGE_FENCE_KEYSPACE.to_string(),
            fence_key(scope.bucket()),
        ))),
        Some(fence) if fence.job_id == job_id => Err(PurgeFenceError::Invalid),
        Some(_) | None => Ok(None),
    }
}

#[cfg(test)]
#[path = "fence_store_tests.rs"]
mod tests;
