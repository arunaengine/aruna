//! Scans the mirror repair records for the due batch and the next due time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::MIRROR_REPAIR_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, done};
use aruna_core::types::{Key, Value};
use tracing::warn;

use super::{SyncMirrorError, SyncMirrorRecord};

const REPAIR_PAGE_SIZE: usize = 128;
const REPAIR_BATCH_SIZE: usize = 64;

/// The due records of one pass, oldest first, and when the earliest later one is due.
#[derive(Debug, Default, PartialEq)]
pub(super) struct RepairScan {
    pub(super) records: Vec<SyncMirrorRecord>,
    pub(super) has_more_due: bool,
    pub(super) next_due_ms: Option<u64>,
}

/// Reads every repair record page by page and drops malformed rows. Records due at the run's
/// time are kept, at most one batch; the rest only move the next due time.
#[derive(Debug, PartialEq)]
pub(super) struct ScanRepairsOperation {
    now_ms: u64,
    scan: RepairScan,
    due: Vec<(Key, SyncMirrorRecord)>,
    pages: PrefixPages,
    /// Set while the malformed rows of a page are dropped.
    dropping: bool,
}

impl ScanRepairsOperation {
    pub(super) fn new() -> Self {
        Self {
            now_ms: 0,
            scan: RepairScan::default(),
            due: Vec::new(),
            pages: PrefixPages::new(MIRROR_REPAIR_KEYSPACE, Vec::<u8>::new(), REPAIR_PAGE_SIZE),
            dropping: false,
        }
    }

    fn rows(&mut self, rows: Vec<(Key, Value)>) -> Next<RepairScan> {
        let mut malformed = Vec::new();
        for (key, value) in rows {
            let record = match SyncMirrorRecord::from_bytes(&key, &value) {
                Ok(record) => record,
                Err(error) => {
                    warn!(%error, "Dropping malformed sync mirror repair record");
                    malformed.push((MIRROR_REPAIR_KEYSPACE.to_string(), key));
                    continue;
                }
            };
            if record.due_at_ms <= self.now_ms {
                self.due.push((key, record));
            } else {
                let due = record.due_at_ms;
                self.scan.next_due_ms = Some(self.scan.next_due_ms.map_or(due, |at| at.min(due)));
            }
        }
        if !malformed.is_empty() {
            self.dropping = true;
            return Next::Call(IoCall::Delete(malformed));
        }
        self.advance()
    }

    fn advance(&mut self) -> Next<RepairScan> {
        if !self.pages.done() {
            return Next::Call(self.pages.call());
        }
        let mut due = std::mem::take(&mut self.due);
        due.sort_by(|(left_key, left), (right_key, right)| {
            let left: (u64, &[u8]) = (left.due_at_ms, left_key.as_ref());
            let right: (u64, &[u8]) = (right.due_at_ms, right_key.as_ref());
            left.cmp(&right)
        });
        let mut scan = std::mem::take(&mut self.scan);
        scan.has_more_due = due.len() > REPAIR_BATCH_SIZE;
        due.truncate(REPAIR_BATCH_SIZE);
        scan.records = due.into_iter().map(|(_, record)| record).collect();
        Next::Done(scan)
    }
}

impl Operation for ScanRepairsOperation {
    type Output = RepairScan;
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<RepairScan>, SyncMirrorError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RepairScan>, SyncMirrorError> {
        if std::mem::take(&mut self.dropping) {
            done(result)?;
            return Ok(self.advance());
        }
        let rows = self.pages.take(result)?;
        Ok(self.rows(rows))
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::super::{SyncMirrorIntent, repair_key};
    use super::*;
    use aruna_core::calls::IoError;
    use byteview::ByteView;
    use ulid::Ulid;

    fn record(seed: u8, due_at_ms: u64) -> (Key, Value) {
        let mut relationship = relationship();
        relationship.id = Ulid::from_bytes([seed; 16]);
        let record = SyncMirrorRecord {
            due_at_ms,
            ..SyncMirrorRecord::new(relationship, SyncMirrorIntent::Reconcile, 0)
        };
        (
            repair_key(&record.relationship),
            ByteView::from(record.to_bytes().unwrap()),
        )
    }

    fn page(after: Option<Key>) -> Next<RepairScan> {
        Next::Call(IoCall::Page {
            space: MIRROR_REPAIR_KEYSPACE.to_string(),
            prefix: ByteView::from(Vec::new()),
            after,
            limit: REPAIR_PAGE_SIZE,
        })
    }

    fn listing() -> ScanRepairsOperation {
        let mut operation = ScanRepairsOperation::new();
        operation.stamp(50);
        assert_eq!(operation.first(), Ok(page(None)));
        operation
    }

    fn done(next: Result<Next<RepairScan>, SyncMirrorError>) -> RepairScan {
        match next {
            Ok(Next::Done(scan)) => scan,
            other => panic!("expected the scan to finish: {other:?}"),
        }
    }

    #[test]
    fn scans_pages_ordered() {
        let (late, early, future) = (record(1, 40), record(2, 10), record(3, 90));
        let mut operation = listing();
        let first = IoResult::Page {
            rows: vec![late, future.clone()],
            next: Some(future.0.clone()),
        };
        assert_eq!(operation.next(first), Ok(page(Some(future.0))));
        let last = IoResult::Page {
            rows: vec![early],
            next: None,
        };
        let scan = done(operation.next(last));
        let due: Vec<_> = scan.records.iter().map(|record| record.due_at_ms).collect();
        assert_eq!(due, vec![10, 40]);
        assert_eq!(scan.next_due_ms, Some(90));
        assert!(!scan.has_more_due);
    }

    #[test]
    fn drops_malformed_rows() {
        let bad: (Key, Value) = (ByteView::from(vec![7]), ByteView::from(vec![0xff]));
        let mut operation = listing();
        let page = IoResult::Page {
            rows: vec![bad.clone(), record(1, 1)],
            next: None,
        };
        assert_eq!(
            operation.next(page),
            Ok(Next::Call(IoCall::Delete(vec![(
                MIRROR_REPAIR_KEYSPACE.to_string(),
                bad.0
            )])))
        );
        assert_eq!(done(operation.next(IoResult::Done)).records.len(), 1);
    }

    #[test]
    fn bounds_the_batch() {
        let rows = (0..=REPAIR_BATCH_SIZE as u8)
            .map(|seed| record(seed + 1, 1))
            .collect();
        let scan = done(listing().next(IoResult::Page { rows, next: None }));
        assert_eq!(scan.records.len(), REPAIR_BATCH_SIZE);
        assert!(scan.has_more_due);
    }

    #[test]
    fn refuses_other_results() {
        assert!(matches!(
            listing().next(IoResult::Done),
            Err(SyncMirrorError::Io(IoError::Unexpected { .. }))
        ));
    }
}
