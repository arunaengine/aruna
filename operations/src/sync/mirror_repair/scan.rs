//! Scans the mirror repair records for the due batch and the next due time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::MIRROR_REPAIR_KEYSPACE;
use aruna_core::operation::EffectOperation;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
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

#[derive(Debug, PartialEq)]
enum ScanState {
    Ready,
    Listing,
    /// Malformed rows of the last page are being dropped; the next page starts after it.
    Dropping(Option<Key>),
    Done(Result<(), SyncMirrorError>),
}

/// Reads every repair record page by page and drops malformed rows. Records due at `now_ms`
/// are kept, at most one batch; the rest only move the next due time.
#[derive(Debug, PartialEq)]
pub(super) struct ScanRepairsOperation {
    now_ms: u64,
    scan: RepairScan,
    due: Vec<(Key, SyncMirrorRecord)>,
    state: ScanState,
}

impl ScanRepairsOperation {
    pub(super) fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            scan: RepairScan::default(),
            due: Vec::new(),
            state: ScanState::Ready,
        }
    }

    fn list(&mut self, start_after: Option<Key>) -> Effects {
        self.state = ScanState::Listing;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: REPAIR_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn page(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        let mut malformed = Vec::new();
        for (key, value) in values {
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
            self.state = ScanState::Dropping(next);
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: malformed,
                txn_id: None,
            })];
        }
        self.advance(next)
    }

    fn advance(&mut self, next: Option<Key>) -> Effects {
        if next.is_some() {
            return self.list(next);
        }
        let mut due = std::mem::take(&mut self.due);
        due.sort_by(|(left_key, left), (right_key, right)| {
            let left: (u64, &[u8]) = (left.due_at_ms, left_key.as_ref());
            let right: (u64, &[u8]) = (right.due_at_ms, right_key.as_ref());
            left.cmp(&right)
        });
        self.scan.has_more_due = due.len() > REPAIR_BATCH_SIZE;
        due.truncate(REPAIR_BATCH_SIZE);
        self.scan.records = due.into_iter().map(|(_, record)| record).collect();
        self.state = ScanState::Done(Ok(()));
        Effects::new()
    }
}

impl EffectOperation for ScanRepairsOperation {
    type Output = RepairScan;
    type Error = SyncMirrorError;

    fn start(&mut self) -> Effects {
        match self.state {
            ScanState::Ready => self.list(None),
            _ => Effects::new(),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::replace(&mut self.state, ScanState::Ready);
        match (state, event) {
            (
                ScanState::Listing,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.page(values, next_start_after),
            (ScanState::Dropping(next), Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.advance(next)
            }
            (_, event) => {
                self.state = ScanState::Done(Err(SyncMirrorError::from_event(event)));
                Effects::new()
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ScanState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            ScanState::Done(Ok(())) => Ok(self.scan),
            ScanState::Done(Err(error)) => Err(error),
            _ => Err(SyncMirrorError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.state = ScanState::Done(Err(SyncMirrorError::Unfinished));
        Effects::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::super::{SyncMirrorIntent, repair_key};
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use byteview::ByteView;
    use ulid::Ulid;

    fn record(seed: u8, due_at_ms: u64) -> (Key, Value) {
        let mut relationship = relationship();
        relationship.id = Ulid::from_bytes([seed; 16]);
        let record = SyncMirrorRecord {
            due_at_ms,
            ..SyncMirrorRecord::new(relationship, SyncMirrorIntent::Reconcile)
        };
        (
            repair_key(&record.relationship),
            ByteView::from(record.to_bytes().unwrap()),
        )
    }

    fn page(values: Vec<(Key, Value)>, next: Option<Key>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: next,
        })
    }

    fn iter(start_after: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: REPAIR_PAGE_SIZE,
            txn_id: None,
        })
    }

    fn listing() -> ScanRepairsOperation {
        let mut operation = ScanRepairsOperation::new(50);
        operation.start();
        operation
    }

    #[test]
    fn scans_pages_ordered() {
        let (late, early, future) = (record(1, 40), record(2, 10), record(3, 90));
        let mut operation = ScanRepairsOperation::new(50);
        check_trace(
            &mut operation,
            &[iter(None)],
            vec![
                (
                    page(vec![late.clone(), future.clone()], Some(future.0.clone())),
                    vec![iter(Some(future.0.clone()))],
                ),
                (page(vec![early.clone()], None), vec![]),
            ],
        );
        let scan = operation.finalize().expect("scan finishes");
        let due: Vec<_> = scan.records.iter().map(|record| record.due_at_ms).collect();
        assert_eq!(due, vec![10, 40]);
        assert_eq!(scan.next_due_ms, Some(90));
        assert!(!scan.has_more_due);
    }

    #[test]
    fn drops_malformed_rows() {
        let bad: (Key, Value) = (vec![7].into(), vec![0xff].into());
        let mut operation = listing();
        assert_eq!(
            operation
                .step(page(vec![bad.clone(), record(1, 1)], None))
                .as_slice(),
            &[Effect::Storage(StorageEffect::BatchDelete {
                deletes: vec![(MIRROR_REPAIR_KEYSPACE.to_string(), bad.0)],
                txn_id: None,
            })]
        );
        let dropped = Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        });
        assert!(operation.step(dropped).is_empty());
        assert_eq!(
            operation.finalize().expect("scan finishes").records.len(),
            1
        );
    }

    #[test]
    fn bounds_the_batch() {
        let values = (0..=REPAIR_BATCH_SIZE as u8)
            .map(|seed| record(seed + 1, 1))
            .collect();
        let mut operation = listing();
        operation.step(page(values, None));
        let scan = operation.finalize().expect("scan finishes");
        assert_eq!(scan.records.len(), REPAIR_BATCH_SIZE);
        assert!(scan.has_more_due);
    }

    #[test]
    fn storage_errors_fail() {
        let mut operation = listing();
        operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(
            operation.finalize(),
            Err(SyncMirrorError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn rejects_foreign_events() {
        let refusals = rejects_events(listing, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refusals.is_empty());
        for (label, effects, error) in refusals {
            assert!(effects.is_empty(), "{label}");
            assert!(matches!(error, SyncMirrorError::Unexpected(_)), "{label}");
        }
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(ScanRepairsOperation::new(1));
        rejects_finalize(listing());
        let mut operation = listing();
        check_abort(&mut operation, &[]);
        assert_eq!(operation.finalize(), Err(SyncMirrorError::Unfinished));
    }
}
