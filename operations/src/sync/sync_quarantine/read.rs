//! Reads one quarantine row by its storage key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::SyncQuarantineRecord;

use super::{QuarantineAdminError, row_read};

/// One quarantine row, or none when the key holds no row.
#[derive(Debug, PartialEq)]
pub struct ReadQuarantineOperation {
    key: Vec<u8>,
}

impl ReadQuarantineOperation {
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
    }
}

impl Operation for ReadQuarantineOperation {
    type Output = Option<SyncQuarantineRecord>;
    type Error = QuarantineAdminError;

    fn first(&mut self) -> Result<Next<Self::Output>, QuarantineAdminError> {
        Ok(Next::Call(row_read(&self.key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, QuarantineAdminError> {
        let [value] = values::<1>(result)?;
        let record = value
            .map(|bytes| SyncQuarantineRecord::from_bytes(bytes.as_ref()))
            .transpose()?;
        Ok(Next::Done(record))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{record, record_bytes};
    use super::*;
    use aruna_core::calls::IoError;
    use aruna_core::types::Value;

    fn read(value: Option<Value>) -> IoResult {
        IoResult::Values(vec![value])
    }

    #[test]
    fn reads_row() {
        let mut operation = ReadQuarantineOperation::new(vec![4]);
        assert_eq!(operation.first(), Ok(Next::Call(row_read(&[4]))));
        let stored = read(Some(record_bytes(&record(1, true))));
        assert_eq!(
            operation.next(stored),
            Ok(Next::Done(Some(record(1, true))))
        );
        assert_eq!(operation.next(read(None)), Ok(Next::Done(None)));
    }

    #[test]
    fn refuses_bad_results() {
        let mut operation = ReadQuarantineOperation::new(vec![4]);
        assert!(matches!(
            operation.next(read(Some(vec![0xff].into()))),
            Err(QuarantineAdminError::Conversion(_))
        ));
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(QuarantineAdminError::Io(IoError::Unexpected { .. }))
        ));
        let failed = IoError::Storage(aruna_core::errors::StorageError::Timeout);
        assert_eq!(
            QuarantineAdminError::from(failed),
            QuarantineAdminError::Storage(aruna_core::errors::StorageError::Timeout)
        );
    }
}
