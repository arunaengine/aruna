//! Marks one quarantine row acknowledged and moves the usage bytes in the same transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::{done, values};
use aruna_core::structs::{SyncQuarantineRecord, quarantine_row_entry, quarantine_usage_entry};
use aruna_core::types::{Key, KeySpace, Value};

use super::{QuarantineAdminError, decode_usage, row_read, usage_read};

#[derive(Debug, PartialEq)]
enum Phase {
    Row,
    /// The acknowledged row to write and the size of the row it replaces.
    Usage {
        record: SyncQuarantineRecord,
        entry: (KeySpace, Key, Value),
        before: u64,
    },
    Writing(SyncQuarantineRecord),
}

/// Marks one row acknowledged. Idempotent: an acknowledged row is reported again and
/// nothing is rewritten; a missing row reports none.
#[derive(Debug, PartialEq)]
pub struct AcknowledgeQuarantineOperation {
    key: Vec<u8>,
    phase: Phase,
}

impl AcknowledgeQuarantineOperation {
    pub fn new(key: Vec<u8>) -> Self {
        Self {
            key,
            phase: Phase::Row,
        }
    }

    fn row(
        &mut self,
        value: Option<Value>,
    ) -> Result<Next<Option<SyncQuarantineRecord>>, QuarantineAdminError> {
        let Some(value) = value else {
            return Ok(Next::Done(None));
        };
        let mut record = SyncQuarantineRecord::from_bytes(value.as_ref())?;
        if record.acknowledged {
            return Ok(Next::Done(Some(record)));
        }
        record.acknowledged = true;
        let entry = quarantine_row_entry(&record)?;
        // The bytes this transaction read, not a re-encoding of them.
        let before = value.len() as u64;
        self.phase = Phase::Usage {
            record,
            entry,
            before,
        };
        Ok(Next::Call(usage_read()))
    }
}

impl Operation for AcknowledgeQuarantineOperation {
    type Output = Option<SyncQuarantineRecord>;
    type Error = QuarantineAdminError;

    fn first(&mut self) -> Result<Next<Self::Output>, QuarantineAdminError> {
        Ok(Next::Call(row_read(&self.key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, QuarantineAdminError> {
        match std::mem::replace(&mut self.phase, Phase::Row) {
            Phase::Row => {
                let [value] = values::<1>(result)?;
                self.row(value)
            }
            Phase::Usage {
                record,
                entry,
                before,
            } => {
                let [value] = values::<1>(result)?;
                let mut usage = decode_usage(value)?;
                let after = entry.2.len() as u64;
                usage.bytes = usage.bytes.saturating_sub(before).saturating_add(after);
                let writes = vec![entry, quarantine_usage_entry(usage)?];
                self.phase = Phase::Writing(record);
                Ok(Next::Call(IoCall::Write(writes)))
            }
            Phase::Writing(record) => {
                done(result)?;
                Ok(Next::Done(Some(record)))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{record, record_bytes, usage_bytes};
    use super::*;
    use aruna_core::calls::IoError;
    use aruna_core::structs::SyncQuarantineUsage;

    fn read(value: Option<Value>) -> IoResult {
        IoResult::Values(vec![value])
    }

    fn reading_row() -> AcknowledgeQuarantineOperation {
        let mut operation = AcknowledgeQuarantineOperation::new(vec![4]);
        assert!(operation.transaction());
        assert_eq!(operation.first(), Ok(Next::Call(row_read(&[4]))));
        operation
    }

    #[test]
    fn acknowledge_moves_usage() {
        let stored = record_bytes(&record(1, false));
        let entry = quarantine_row_entry(&record(1, true)).unwrap();
        let usage = SyncQuarantineUsage {
            records: 1,
            bytes: stored.len() as u64,
        };
        let moved = SyncQuarantineUsage {
            records: 1,
            bytes: entry.2.len() as u64,
        };
        let mut operation = reading_row();
        assert_eq!(
            operation.next(read(Some(stored))),
            Ok(Next::Call(usage_read()))
        );
        let writes = vec![entry, quarantine_usage_entry(moved).unwrap()];
        assert_eq!(
            operation.next(read(Some(usage_bytes(usage)))),
            Ok(Next::Call(IoCall::Write(writes)))
        );
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Done(Some(record(1, true))))
        );
    }

    #[test]
    fn keeps_settled_rows() {
        let acknowledged = read(Some(record_bytes(&record(1, true))));
        assert_eq!(
            reading_row().next(acknowledged),
            Ok(Next::Done(Some(record(1, true))))
        );
        assert_eq!(reading_row().next(read(None)), Ok(Next::Done(None)));
    }

    #[test]
    fn refuses_bad_results() {
        assert!(matches!(
            reading_row().next(read(Some(vec![0xff].into()))),
            Err(QuarantineAdminError::Conversion(_))
        ));
        assert!(matches!(
            reading_row().next(IoResult::Done),
            Err(QuarantineAdminError::Io(IoError::Unexpected { .. }))
        ));
        assert_eq!(
            QuarantineAdminError::from(IoError::Conflict),
            QuarantineAdminError::Storage(aruna_core::errors::StorageError::TransactionConflict)
        );
    }
}
