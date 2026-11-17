//! Deletes acknowledged quarantine rows of one page and releases their usage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SYNC_QUARANTINE_KEYSPACE;
use aruna_core::operation::EffectOperation;
use aruna_core::structs::{SyncQuarantineRecord, SyncQuarantineUsage, quarantine_usage_entry};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;

use super::{
    QuarantineAdminError, QuarantinePageRequest, QuarantinePruneResult, decode_usage, iter_effect,
    unexpected, usage_read,
};

#[derive(Debug, PartialEq)]
enum PruneState {
    Ready(QuarantinePageRequest),
    Scanning,
    /// No candidate on the page: only the usage is read, outside a transaction.
    ReadingTotals,
    Starting(Vec<Key>),
    ReadingRows,
    ReadingUsage {
        deletes: Vec<(String, Key)>,
        released: u64,
    },
    Deleting(SyncQuarantineUsage),
    Writing(SyncQuarantineUsage),
    Committing(SyncQuarantineUsage),
    Done(Result<SyncQuarantineUsage, QuarantineAdminError>),
}

/// Deletes acknowledged rows in one bounded, resumable pass and releases their usage in the
/// same transaction. The scan only proposes candidates; each is read again inside the
/// transaction, so redelivered evidence is neither deleted nor released.
#[derive(Debug, PartialEq)]
pub struct PruneQuarantineOperation {
    txn_id: Option<TxnId>,
    scanned: usize,
    pruned: usize,
    next_start_after: Option<Vec<u8>>,
    state: PruneState,
}

impl PruneQuarantineOperation {
    pub fn new(request: QuarantinePageRequest) -> Self {
        Self {
            txn_id: None,
            scanned: 0,
            pruned: 0,
            next_start_after: None,
            state: PruneState::Ready(request),
        }
    }

    /// Ends the run and aborts the open transaction.
    fn finish(&mut self, result: Result<SyncQuarantineUsage, QuarantineAdminError>) -> Effects {
        self.state = PruneState::Done(result);
        abort_effects(&mut self.txn_id)
    }

    fn scan(&mut self, values: Vec<(Key, Value)>) -> Effects {
        self.scanned = values.len();
        let mut candidates = Vec::new();
        for (key, value) in values {
            match SyncQuarantineRecord::from_bytes(value.as_ref()) {
                Ok(record) if record.acknowledged => candidates.push(key),
                Ok(_) => {}
                Err(error) => return self.finish(Err(error.into())),
            }
        }
        if candidates.is_empty() {
            self.state = PruneState::ReadingTotals;
            return smallvec![usage_read(None)];
        }
        self.state = PruneState::Starting(candidates);
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn revalidate(&mut self, values: Vec<(Key, Option<Value>)>, txn_id: TxnId) -> Effects {
        let mut deletes = Vec::new();
        let mut released = 0u64;
        for (key, value) in values {
            let Some(value) = value else {
                continue;
            };
            match SyncQuarantineRecord::from_bytes(value.as_ref()) {
                Ok(record) if record.acknowledged => {
                    released = released.saturating_add(value.len() as u64);
                    deletes.push((SYNC_QUARANTINE_KEYSPACE.to_string(), key));
                }
                Ok(_) => {}
                Err(error) => return self.finish(Err(error.into())),
            }
        }
        self.state = PruneState::ReadingUsage { deletes, released };
        smallvec![usage_read(Some(txn_id))]
    }

    fn release(
        &mut self,
        value: Option<Value>,
        deletes: Vec<(String, Key)>,
        released: u64,
        txn_id: TxnId,
    ) -> Effects {
        let mut usage = match decode_usage(value) {
            Ok(usage) => usage,
            Err(error) => return self.finish(Err(error)),
        };
        if deletes.is_empty() {
            return self.finish(Ok(usage));
        }
        self.pruned = deletes.len();
        usage.release(self.pruned as u64, released);
        self.state = PruneState::Deleting(usage);
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })]
    }
}

impl EffectOperation for PruneQuarantineOperation {
    type Output = QuarantinePruneResult;
    type Error = QuarantineAdminError;

    fn start(&mut self) -> Effects {
        let PruneState::Ready(request) = &self.state else {
            return Effects::new();
        };
        let effect = iter_effect(request);
        self.state = PruneState::Scanning;
        smallvec![effect]
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::replace(&mut self.state, PruneState::Scanning);
        match (state, self.txn_id, event) {
            (
                PruneState::Scanning,
                None,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                self.next_start_after = next_start_after.map(|key| key.as_ref().to_vec());
                self.scan(values)
            }
            (
                PruneState::ReadingTotals,
                None,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.finish(decode_usage(value)),
            (
                PruneState::Starting(candidates),
                None,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = PruneState::ReadingRows;
                let reads = candidates
                    .into_iter()
                    .map(|key| (SYNC_QUARANTINE_KEYSPACE.to_string(), key))
                    .collect();
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: Some(txn_id),
                })]
            }
            (
                PruneState::ReadingRows,
                Some(txn_id),
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.revalidate(values, txn_id),
            (
                PruneState::ReadingUsage { deletes, released },
                Some(txn_id),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.release(value, deletes, released, txn_id),
            (
                PruneState::Deleting(usage),
                Some(txn_id),
                Event::Storage(StorageEvent::BatchDeleteResult { .. }),
            ) => match quarantine_usage_entry(usage) {
                Ok(entry) => {
                    self.state = PruneState::Writing(usage);
                    smallvec![Effect::Storage(StorageEffect::BatchWrite {
                        writes: vec![entry],
                        txn_id: Some(txn_id),
                    })]
                }
                Err(error) => self.finish(Err(error.into())),
            },
            (
                PruneState::Writing(usage),
                Some(txn_id),
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => {
                self.state = PruneState::Committing(usage);
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (
                PruneState::Committing(usage),
                Some(_),
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.finish(Ok(usage))
            }
            (PruneState::Committing(_), _, event) => {
                // A failed commit leaves nothing to abort.
                self.txn_id = None;
                self.finish(Err(unexpected("storage transaction committed", event)))
            }
            (_, _, event) => self.finish(Err(unexpected("storage result", event))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PruneState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            PruneState::Done(Ok(usage)) => Ok(QuarantinePruneResult {
                pruned: self.pruned,
                scanned: self.scanned,
                next_start_after: self.next_start_after,
                usage,
            }),
            PruneState::Done(Err(error)) => Err(error),
            _ => Err(QuarantineAdminError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.finish(Err(QuarantineAdminError::Unfinished))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{record, record_bytes, usage_bytes};
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use ulid::Ulid;

    const TXN: TxnId = Ulid(6);

    fn key(seed: u8) -> Key {
        vec![seed].into()
    }

    fn page() -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: vec![
                (key(1), record_bytes(&record(1, true))),
                (key(2), record_bytes(&record(2, false))),
            ],
            next_start_after: Some(key(2)),
        })
    }

    fn read_result(value: Option<Value>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value,
        })
    }

    fn start_effect() -> Effect {
        Effect::Storage(StorageEffect::StartTransaction { read: false })
    }

    fn abort() -> Effect {
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN })
    }

    fn reading_rows() -> PruneQuarantineOperation {
        let mut operation = PruneQuarantineOperation::new(QuarantinePageRequest::default());
        operation.start();
        operation.step(page());
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        operation
    }

    fn rows(acknowledged: bool) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(key(1), Some(record_bytes(&record(1, acknowledged))))],
        })
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
        let mut operation = PruneQuarantineOperation::new(QuarantinePageRequest::default());
        check_trace(
            &mut operation,
            &[iter_effect(&QuarantinePageRequest::default())],
            vec![
                (page(), vec![start_effect()]),
                (
                    Event::Storage(StorageEvent::TransactionStarted { txn_id: TXN }),
                    vec![Effect::Storage(StorageEffect::BatchRead {
                        reads: vec![(SYNC_QUARANTINE_KEYSPACE.to_string(), key(1))],
                        txn_id: Some(TXN),
                    })],
                ),
                (rows(true), vec![usage_read(Some(TXN))]),
                (
                    read_result(Some(usage_bytes(usage))),
                    vec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes: vec![(SYNC_QUARANTINE_KEYSPACE.to_string(), key(1))],
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    Event::Storage(StorageEvent::BatchDeleteResult {
                        entries: Vec::new(),
                    }),
                    vec![Effect::Storage(StorageEffect::BatchWrite {
                        writes: vec![quarantine_usage_entry(released).unwrap()],
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    Event::Storage(StorageEvent::BatchWriteResult {
                        entries: Vec::new(),
                    }),
                    vec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: TXN,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::TransactionCommitted { txn_id: TXN }),
                    vec![],
                ),
            ],
        );
        assert_eq!(
            operation.finalize(),
            Ok(QuarantinePruneResult {
                pruned: 1,
                scanned: 2,
                next_start_after: Some(vec![2]),
                usage: released,
            })
        );
    }

    #[test]
    fn reads_usage_only() {
        let mut operation = PruneQuarantineOperation::new(QuarantinePageRequest::default());
        operation.start();
        let unacknowledged = Event::Storage(StorageEvent::IterResult {
            values: vec![(key(2), record_bytes(&record(2, false)))],
            next_start_after: None,
        });
        assert_eq!(
            operation.step(unacknowledged).as_slice(),
            &[usage_read(None)]
        );
        operation.step(read_result(None));
        let result = operation.finalize().expect("prune finishes");
        assert_eq!((result.pruned, result.scanned), (0, 1));
    }

    /// Redelivered evidence replaced the scanned row: the transaction reads it unacknowledged,
    /// so nothing is deleted or released and the transaction is aborted.
    #[test]
    fn prune_revalidates_candidates() {
        let usage = SyncQuarantineUsage {
            records: 2,
            bytes: 300,
        };
        let mut operation = reading_rows();
        assert_eq!(
            operation.step(rows(false)).as_slice(),
            &[usage_read(Some(TXN))]
        );
        assert_eq!(
            operation
                .step(read_result(Some(usage_bytes(usage))))
                .as_slice(),
            &[abort()]
        );
        let result = operation.finalize().expect("prune finishes");
        assert_eq!((result.pruned, result.usage), (0, usage));
    }

    #[test]
    fn errors_abort_transaction() {
        let mut operation = reading_rows();
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert_eq!(operation.step(failed).as_slice(), &[abort()]);
        assert_eq!(
            operation.finalize(),
            Err(QuarantineAdminError::Storage(StorageError::Timeout))
        );
        let mut corrupt = reading_rows();
        let bad = Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(key(1), Some(vec![0xff].into()))],
        });
        assert_eq!(corrupt.step(bad).as_slice(), &[abort()]);
        assert!(matches!(
            corrupt.finalize(),
            Err(QuarantineAdminError::Conversion(_))
        ));
    }

    #[test]
    fn rejects_foreign_events() {
        let refusals = rejects_events(reading_rows, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::BatchReadResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refusals.is_empty());
        for (label, effects, error) in refusals {
            assert_eq!(effects.as_slice(), &[abort()], "{label}");
            assert!(
                matches!(error, QuarantineAdminError::Unexpected { .. }),
                "{label}"
            );
        }
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(PruneQuarantineOperation::new(
            QuarantinePageRequest::default(),
        ));
        rejects_finalize(reading_rows());
        let mut operation = reading_rows();
        check_abort(&mut operation, &[abort()]);
        assert_eq!(operation.finalize(), Err(QuarantineAdminError::Unfinished));
    }
}
