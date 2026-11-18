//! Deletes acknowledged quarantine rows of one page and releases their usage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::SYNC_QUARANTINE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::structs::{SyncQuarantineRecord, SyncQuarantineUsage, quarantine_usage_entry};
use aruna_core::types::{Key, KeySpace, Value};

use super::{
    QuarantineAdminError, QuarantinePageRequest, QuarantinePruneResult, decode_usage, page_call,
    page_rows, usage_read,
};

#[derive(Debug, PartialEq)]
enum Phase {
    Scanning,
    /// The acknowledged rows of the page and their stored bytes.
    Usage {
        deletes: Vec<(KeySpace, Key)>,
        released: u64,
    },
    Deleting(SyncQuarantineUsage),
    Writing(SyncQuarantineUsage),
}

/// Deletes the acknowledged rows of one page and releases their usage in one transaction, so a
/// row replaced by redelivered evidence meanwhile fails the commit instead of being deleted.
/// Run it with `drive_retry`: a conflict reruns the page.
#[derive(Debug, PartialEq)]
pub struct PruneQuarantineOperation {
    request: QuarantinePageRequest,
    scanned: usize,
    pruned: usize,
    next_start_after: Option<Vec<u8>>,
    phase: Phase,
}

impl PruneQuarantineOperation {
    pub fn new(request: QuarantinePageRequest) -> Self {
        Self {
            request,
            scanned: 0,
            pruned: 0,
            next_start_after: None,
            phase: Phase::Scanning,
        }
    }

    fn scan(&mut self, rows: Vec<(Key, Value)>) -> Result<Phase, QuarantineAdminError> {
        self.scanned = rows.len();
        let mut deletes = Vec::new();
        let mut released = 0u64;
        for (key, value) in rows {
            if SyncQuarantineRecord::from_bytes(value.as_ref())?.acknowledged {
                released = released.saturating_add(value.len() as u64);
                deletes.push((SYNC_QUARANTINE_KEYSPACE.to_string(), key));
            }
        }
        Ok(Phase::Usage { deletes, released })
    }

    fn result(&mut self, usage: SyncQuarantineUsage) -> Next<QuarantinePruneResult> {
        Next::Done(QuarantinePruneResult {
            pruned: self.pruned,
            scanned: self.scanned,
            next_start_after: self.next_start_after.take(),
            usage,
        })
    }
}

impl Operation for PruneQuarantineOperation {
    type Output = QuarantinePruneResult;
    type Error = QuarantineAdminError;

    fn first(&mut self) -> Result<Next<QuarantinePruneResult>, QuarantineAdminError> {
        Ok(Next::Call(page_call(&self.request)))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<QuarantinePruneResult>, QuarantineAdminError> {
        match std::mem::replace(&mut self.phase, Phase::Scanning) {
            Phase::Scanning => {
                let (rows, next) = page_rows(result)?;
                self.next_start_after = next;
                self.phase = self.scan(rows)?;
                Ok(Next::Call(usage_read()))
            }
            Phase::Usage { deletes, released } => {
                let [value] = values::<1>(result)?;
                let mut usage = decode_usage(value)?;
                if deletes.is_empty() {
                    return Ok(self.result(usage));
                }
                self.pruned = deletes.len();
                usage.release(self.pruned as u64, released);
                self.phase = Phase::Deleting(usage);
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            Phase::Deleting(usage) => {
                done(result)?;
                self.phase = Phase::Writing(usage);
                Ok(Next::Call(IoCall::Write(vec![quarantine_usage_entry(
                    usage,
                )?])))
            }
            Phase::Writing(usage) => {
                done(result)?;
                Ok(self.result(usage))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{record, record_bytes, usage_bytes};
    use super::*;
    use aruna_core::calls::IoError;

    fn key(seed: u8) -> Key {
        vec![seed].into()
    }

    fn page() -> IoResult {
        IoResult::Page {
            rows: vec![
                (key(1), record_bytes(&record(1, true))),
                (key(2), record_bytes(&record(2, false))),
            ],
            next: Some(key(2)),
        }
    }

    fn read(value: Option<Value>) -> IoResult {
        IoResult::Values(vec![value])
    }

    fn scanning() -> PruneQuarantineOperation {
        let mut operation = PruneQuarantineOperation::new(QuarantinePageRequest::default());
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        assert_eq!(
            operation.first(),
            Ok(Next::Call(page_call(&QuarantinePageRequest::default())))
        );
        operation
    }

    #[test]
    fn prunes_acknowledged_rows() {
        let row = record_bytes(&record(1, true));
        let usage = SyncQuarantineUsage {
            records: 2,
            bytes: 100 + row.len() as u64,
        };
        let released = SyncQuarantineUsage {
            records: 1,
            bytes: 100,
        };
        let mut operation = scanning();
        assert_eq!(operation.next(page()), Ok(Next::Call(usage_read())));
        let deletes = vec![(SYNC_QUARANTINE_KEYSPACE.to_string(), key(1))];
        assert_eq!(
            operation.next(read(Some(usage_bytes(usage)))),
            Ok(Next::Call(IoCall::Delete(deletes)))
        );
        let writes = vec![quarantine_usage_entry(released).unwrap()];
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Write(writes)))
        );
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Done(QuarantinePruneResult {
                pruned: 1,
                scanned: 2,
                next_start_after: Some(vec![2]),
                usage: released,
            }))
        );
    }

    #[test]
    fn reads_usage_only() {
        let mut operation = scanning();
        let unacknowledged = IoResult::Page {
            rows: vec![(key(2), record_bytes(&record(2, false)))],
            next: None,
        };
        assert_eq!(operation.next(unacknowledged), Ok(Next::Call(usage_read())));
        let Ok(Next::Done(result)) = operation.next(read(None)) else {
            panic!("prune finishes");
        };
        assert_eq!((result.pruned, result.scanned), (0, 1));
        assert_eq!(result.usage, SyncQuarantineUsage::default());
    }

    #[test]
    fn refuses_bad_results() {
        let corrupt = IoResult::Page {
            rows: vec![(key(1), vec![0xff].into())],
            next: None,
        };
        assert!(matches!(
            scanning().next(corrupt),
            Err(QuarantineAdminError::Conversion(_))
        ));
        assert!(matches!(
            scanning().next(IoResult::Done),
            Err(QuarantineAdminError::Io(IoError::Unexpected { .. }))
        ));
        let mut deleting = scanning();
        deleting.next(page()).unwrap();
        deleting.next(read(None)).unwrap();
        assert!(matches!(
            deleting.next(read(None)),
            Err(QuarantineAdminError::Io(IoError::Unexpected { .. }))
        ));
    }
}
