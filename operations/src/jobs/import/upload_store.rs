//! Reads, writes and deletes the stored RO-Crate upload records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::ROCRATE_UPLOAD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{done, values};
use aruna_core::structs::execution::job::{JobId, RoCrateUploadRecord, upload_key};
use byteview::ByteView;
use ulid::Ulid;

/// Why an upload record could not be read, written or deleted.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct UploadStoreError(pub String);

impl From<IoError> for UploadStoreError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

fn upload_row(upload_id: Ulid) -> (String, aruna_core::types::Key) {
    (ROCRATE_UPLOAD_KEYSPACE.to_string(), upload_key(upload_id))
}

fn decoded(
    value: Option<aruna_core::types::Value>,
) -> Result<Option<RoCrateUploadRecord>, UploadStoreError> {
    value
        .map(|value| postcard::from_bytes(value.as_ref()))
        .transpose()
        .map_err(|error| UploadStoreError(error.to_string()))
}

/// Reads one upload record; a missing record stays distinct from a malformed one.
#[derive(Debug, PartialEq)]
pub struct ReadUploadOperation {
    upload_id: Ulid,
}

impl ReadUploadOperation {
    pub fn new(upload_id: Ulid) -> Self {
        Self { upload_id }
    }
}

impl Operation for ReadUploadOperation {
    type Output = Option<RoCrateUploadRecord>;
    type Error = UploadStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, UploadStoreError> {
        Ok(Next::Call(IoCall::Read(vec![upload_row(self.upload_id)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, UploadStoreError> {
        let [value] = values::<1>(result)?;
        Ok(Next::Done(decoded(value)?))
    }
}

/// Stores one upload record.
#[derive(Debug, PartialEq)]
pub struct WriteUploadOperation {
    record: RoCrateUploadRecord,
}

impl WriteUploadOperation {
    pub fn new(record: RoCrateUploadRecord) -> Self {
        Self { record }
    }
}

impl Operation for WriteUploadOperation {
    type Output = ();
    type Error = UploadStoreError;

    fn first(&mut self) -> Result<Next<()>, UploadStoreError> {
        let value = postcard::to_allocvec(&self.record)
            .map_err(|error| UploadStoreError(error.to_string()))?;
        let (space, key) = upload_row(self.record.upload_id);
        let row = (space, key, ByteView::from(value));
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, UploadStoreError> {
        done(result)?;
        Ok(Next::Done(()))
    }
}

/// Deletes the upload record `claimed_by` holds; a record that is gone needs nothing.
#[derive(Debug, PartialEq)]
pub struct DeleteUploadOperation {
    upload_id: Ulid,
    claimed_by: JobId,
    deleting: bool,
}

impl DeleteUploadOperation {
    pub fn new(upload_id: Ulid, claimed_by: JobId) -> Self {
        Self {
            upload_id,
            claimed_by,
            deleting: false,
        }
    }
}

impl Operation for DeleteUploadOperation {
    type Output = ();
    type Error = UploadStoreError;

    fn first(&mut self) -> Result<Next<()>, UploadStoreError> {
        Ok(Next::Call(IoCall::Read(vec![upload_row(self.upload_id)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, UploadStoreError> {
        if self.deleting {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let [value] = values::<1>(result)?;
        let Some(record) = decoded(value)? else {
            return Ok(Next::Done(()));
        };
        if record.claimed_by != Some(self.claimed_by) {
            return Err(UploadStoreError(
                "upload cleanup claim does not match the job".to_string(),
            ));
        }
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(vec![upload_row(self.upload_id)])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job() -> JobId {
        JobId::from_bytes([1; 16])
    }

    fn upload() -> Ulid {
        Ulid::from_bytes([2; 16])
    }

    // A missing upload reads as none; a malformed one fails.
    #[test]
    fn read_decodes_record() {
        let mut read = ReadUploadOperation::new(upload());
        assert_eq!(
            read.first(),
            Ok(Next::Call(IoCall::Read(vec![upload_row(upload())])))
        );
        assert_eq!(
            read.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
        let broken = IoResult::Values(vec![Some(ByteView::from(vec![0xFF]))]);
        assert!(read.next(broken).is_err());
    }

    // An upload that is gone needs no delete.
    #[test]
    fn delete_missing_ends() {
        let mut delete = DeleteUploadOperation::new(upload(), job());
        delete.first().unwrap();
        assert_eq!(
            delete.next(IoResult::Values(vec![None])),
            Ok(Next::Done(()))
        );
    }
}
