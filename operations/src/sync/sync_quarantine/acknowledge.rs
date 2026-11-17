//! Marks one quarantine row acknowledged and moves the usage bytes in the same transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::{SyncQuarantineRecord, quarantine_row_entry, quarantine_usage_entry};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;

use super::{QuarantineAdminError, decode_usage, row_read, unexpected, usage_read};

#[derive(Debug, PartialEq)]
enum AcknowledgeState {
    Ready,
    Starting,
    ReadingRow,
    ReadingUsage {
        record: SyncQuarantineRecord,
        entry: (String, Key, Value),
        before: u64,
    },
    Writing(SyncQuarantineRecord),
    Committing(SyncQuarantineRecord),
    Done(Result<Option<SyncQuarantineRecord>, QuarantineAdminError>),
}

/// Marks one row acknowledged. Idempotent: an acknowledged row is reported again and
/// nothing is rewritten; a missing row reports none.
#[derive(Debug, PartialEq)]
pub struct AcknowledgeQuarantineOperation {
    key: Vec<u8>,
    txn_id: Option<TxnId>,
    state: AcknowledgeState,
}

impl AcknowledgeQuarantineOperation {
    pub fn new(key: Vec<u8>) -> Self {
        Self {
            key,
            txn_id: None,
            state: AcknowledgeState::Ready,
        }
    }

    /// Ends the run and aborts the open transaction.
    fn finish(
        &mut self,
        result: Result<Option<SyncQuarantineRecord>, QuarantineAdminError>,
    ) -> Effects {
        self.state = AcknowledgeState::Done(result);
        abort_effects(&mut self.txn_id)
    }

    fn commit(&mut self, record: SyncQuarantineRecord, txn_id: TxnId) -> Effects {
        self.state = AcknowledgeState::Committing(record);
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn read_row(&mut self, value: Option<Value>, txn_id: TxnId) -> Effects {
        let Some(value) = value else {
            return self.finish(Ok(None));
        };
        let mut record = match SyncQuarantineRecord::from_bytes(value.as_ref()) {
            Ok(record) => record,
            Err(error) => return self.finish(Err(error.into())),
        };
        if record.acknowledged {
            return self.commit(record, txn_id);
        }
        // The bytes this transaction read, not a re-encoding of them.
        let before = value.len() as u64;
        record.acknowledged = true;
        match quarantine_row_entry(&record) {
            Ok(entry) => {
                self.state = AcknowledgeState::ReadingUsage {
                    record,
                    entry,
                    before,
                };
                smallvec![usage_read(Some(txn_id))]
            }
            Err(error) => self.finish(Err(error.into())),
        }
    }
}

impl EffectOperation for AcknowledgeQuarantineOperation {
    type Output = Option<SyncQuarantineRecord>;
    type Error = QuarantineAdminError;

    fn start(&mut self) -> Effects {
        if self.state != AcknowledgeState::Ready {
            return Effects::new();
        }
        self.state = AcknowledgeState::Starting;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::replace(&mut self.state, AcknowledgeState::Ready);
        match (state, self.txn_id, event) {
            (
                AcknowledgeState::Starting,
                None,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = AcknowledgeState::ReadingRow;
                smallvec![row_read(&self.key, Some(txn_id))]
            }
            (
                AcknowledgeState::ReadingRow,
                Some(txn_id),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.read_row(value, txn_id),
            (
                AcknowledgeState::ReadingUsage {
                    record,
                    entry,
                    before,
                },
                Some(txn_id),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => {
                let mut usage = match decode_usage(value) {
                    Ok(usage) => usage,
                    Err(error) => return self.finish(Err(error)),
                };
                let after = entry.2.len() as u64;
                usage.bytes = usage.bytes.saturating_sub(before).saturating_add(after);
                let usage_entry = match quarantine_usage_entry(usage) {
                    Ok(usage_entry) => usage_entry,
                    Err(error) => return self.finish(Err(error.into())),
                };
                self.state = AcknowledgeState::Writing(record);
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes: vec![entry, usage_entry],
                    txn_id: Some(txn_id),
                })]
            }
            (
                AcknowledgeState::Writing(record),
                Some(txn_id),
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => self.commit(record, txn_id),
            (
                AcknowledgeState::Committing(record),
                Some(_),
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.finish(Ok(Some(record)))
            }
            (AcknowledgeState::Committing(_), _, event) => {
                // A failed commit leaves nothing to abort.
                self.txn_id = None;
                self.finish(Err(unexpected("storage transaction committed", event)))
            }
            (_, _, event) => self.finish(Err(unexpected("storage result", event))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, AcknowledgeState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            AcknowledgeState::Done(result) => result,
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
    use aruna_core::structs::SyncQuarantineUsage;
    use ulid::Ulid;

    const TXN: TxnId = Ulid(5);

    fn started() -> Event {
        Event::Storage(StorageEvent::TransactionStarted { txn_id: TXN })
    }

    fn read_result(value: Option<Value>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value,
        })
    }

    fn commit() -> Effect {
        Effect::Storage(StorageEffect::CommitTransaction { txn_id: TXN })
    }

    fn abort() -> Effect {
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN })
    }

    fn reading_row() -> AcknowledgeQuarantineOperation {
        let mut operation = AcknowledgeQuarantineOperation::new(vec![4]);
        operation.start();
        operation.step(started());
        operation
    }

    #[test]
    fn acknowledge_moves_usage() {
        let stored = record_bytes(&record(1, false));
        let entry = quarantine_row_entry(&record(1, true)).expect("row entry");
        let usage = SyncQuarantineUsage {
            records: 1,
            bytes: stored.len() as u64,
        };
        let moved = SyncQuarantineUsage {
            records: 1,
            bytes: entry.2.len() as u64,
        };
        let mut operation = AcknowledgeQuarantineOperation::new(vec![4]);
        check_trace(
            &mut operation,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })],
            vec![
                (started(), vec![row_read(&[4], Some(TXN))]),
                (read_result(Some(stored)), vec![usage_read(Some(TXN))]),
                (
                    read_result(Some(usage_bytes(usage))),
                    vec![Effect::Storage(StorageEffect::BatchWrite {
                        writes: vec![entry, quarantine_usage_entry(moved).unwrap()],
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    Event::Storage(StorageEvent::BatchWriteResult {
                        entries: Vec::new(),
                    }),
                    vec![commit()],
                ),
                (
                    Event::Storage(StorageEvent::TransactionCommitted { txn_id: TXN }),
                    vec![],
                ),
            ],
        );
        assert_eq!(operation.finalize(), Ok(Some(record(1, true))));
    }

    #[test]
    fn acknowledged_row_commits() {
        let mut operation = reading_row();
        assert_eq!(
            operation
                .step(read_result(Some(record_bytes(&record(1, true)))))
                .as_slice(),
            &[commit()]
        );
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TXN,
        }));
        assert_eq!(operation.finalize(), Ok(Some(record(1, true))));
    }

    #[test]
    fn missing_row_aborts() {
        let mut operation = reading_row();
        assert_eq!(operation.step(read_result(None)).as_slice(), &[abort()]);
        assert_eq!(operation.finalize(), Ok(None));
    }

    #[test]
    fn errors_abort_transaction() {
        let mut operation = reading_row();
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert_eq!(operation.step(failed).as_slice(), &[abort()]);
        assert_eq!(
            operation.finalize(),
            Err(QuarantineAdminError::Storage(StorageError::Timeout))
        );
        let mut corrupt = reading_row();
        assert_eq!(
            corrupt
                .step(read_result(Some(vec![0xff].into())))
                .as_slice(),
            &[abort()]
        );
        let mut committing = reading_row();
        committing.step(read_result(Some(record_bytes(&record(1, true)))));
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::CommitFailed,
        });
        assert!(committing.step(failed).is_empty());
        assert!(committing.finalize().is_err());
    }

    #[test]
    fn rejects_foreign_events() {
        let refusals = rejects_events(reading_row, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
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
        let mut early = AcknowledgeQuarantineOperation::new(vec![4]);
        assert!(early.step(read_result(None)).is_empty());
        assert!(early.finalize().is_err());
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(AcknowledgeQuarantineOperation::new(vec![4]));
        rejects_finalize(reading_row());
        let mut operation = reading_row();
        check_abort(&mut operation, &[abort()]);
        assert_eq!(operation.finalize(), Err(QuarantineAdminError::Unfinished));
        let mut fresh = AcknowledgeQuarantineOperation::new(vec![4]);
        check_abort(&mut fresh, &[]);
    }
}
