//! Guards a version transaction on rows it must read as given, and records its version there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

/// Rows one version transaction must still read as given, and keys it records its version id
/// under. A concurrent writer of a guard fails the commit, so its retry finds the guard taken.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowFence {
    guards: Vec<(String, Key, Option<Value>)>,
    receipts: Vec<(String, Key)>,
    step: FenceStep,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FenceStep {
    Unread,
    Reading,
    Checked,
    Writing,
    Done,
}

/// Why a fenced transaction may not go on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FenceError {
    /// A guard no longer reads as given.
    Taken,
    Storage(StorageError),
    /// The event does not answer the effect the fence sent.
    Unexpected,
}

impl RowFence {
    pub(crate) fn new(
        guards: Vec<(String, Key, Option<Value>)>,
        receipts: Vec<(String, Key)>,
    ) -> Self {
        Self {
            guards,
            receipts,
            step: FenceStep::Unread,
        }
    }

    /// The next effect in transaction `txn_id`, or `None` once the fence is done.
    pub(crate) fn next(&mut self, txn_id: Ulid, version_id: Ulid) -> Option<Effect> {
        if self.step == FenceStep::Unread {
            self.step = FenceStep::Checked;
            if !self.guards.is_empty() {
                self.step = FenceStep::Reading;
                let reads = self.guards.iter();
                let reads = reads.map(|(space, key, _)| (space.clone(), key.clone()));
                return Some(Effect::Storage(StorageEffect::BatchRead {
                    reads: reads.collect(),
                    txn_id: Some(txn_id),
                }));
            }
        }
        if self.step == FenceStep::Checked {
            self.step = FenceStep::Done;
            if !self.receipts.is_empty() {
                self.step = FenceStep::Writing;
                let value: Value = version_id.to_bytes().to_vec().into();
                let writes = self.receipts.iter();
                let writes = writes.map(|(space, key)| (space.clone(), key.clone(), value.clone()));
                return Some(Effect::Storage(StorageEffect::BatchWrite {
                    writes: writes.collect(),
                    txn_id: Some(txn_id),
                }));
            }
        }
        None
    }

    /// Takes the answer to the effect `next` returned.
    pub(crate) fn answer(&mut self, event: Event) -> Result<(), FenceError> {
        match (self.step, event) {
            (FenceStep::Reading, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let expected = self.guards.iter().map(|(_, _, value)| value);
                let seen = values.iter().map(|(_, value)| value);
                if values.len() != self.guards.len() || !seen.eq(expected) {
                    return Err(FenceError::Taken);
                }
                self.step = FenceStep::Checked;
                Ok(())
            }
            (FenceStep::Writing, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.step = FenceStep::Done;
                Ok(())
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(FenceError::Storage(error)),
            _ => Err(FenceError::Unexpected),
        }
    }

    /// A restarted transaction reads the guards again.
    pub(crate) fn restart(&mut self) {
        self.step = FenceStep::Unread;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence() -> RowFence {
        let key: Key = b"receipt".to_vec().into();
        RowFence::new(
            vec![("receipts".to_string(), key.clone(), None)],
            vec![("receipts".to_string(), key)],
        )
    }

    fn read(value: Option<Value>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(b"receipt".to_vec().into(), value)],
        })
    }

    #[test]
    fn records_free_guard() {
        // An absent guard lets the transaction record its version id under the receipt.
        let (txn_id, version_id) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let mut fence = fence();
        assert!(matches!(
            fence.next(txn_id, version_id),
            Some(Effect::Storage(StorageEffect::BatchRead { txn_id: Some(id), .. })) if id == txn_id
        ));
        assert_eq!(fence.answer(read(None)), Ok(()));
        let Some(Effect::Storage(StorageEffect::BatchWrite { writes, .. })) =
            fence.next(txn_id, version_id)
        else {
            panic!("the receipt is written in the transaction");
        };
        assert_eq!(writes[0].2.as_ref(), version_id.to_bytes().as_slice());
        let written = Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        });
        assert_eq!(fence.answer(written), Ok(()));
        assert_eq!(fence.next(txn_id, version_id), None);
    }

    #[test]
    fn refuses_taken_guard() {
        // A restarted transaction reads again and finds the receipt of the commit that won.
        let (txn_id, version_id) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let mut fence = fence();
        fence.next(txn_id, version_id);
        assert_eq!(fence.answer(read(None)), Ok(()));
        fence.next(txn_id, version_id);
        fence.restart();
        assert!(matches!(
            fence.next(txn_id, version_id),
            Some(Effect::Storage(StorageEffect::BatchRead { .. }))
        ));
        let winner: Value = Ulid::from_parts(3, 3).to_bytes().to_vec().into();
        assert_eq!(fence.answer(read(Some(winner))), Err(FenceError::Taken));
    }

    #[test]
    fn rejects_stray_events() {
        // Only the answer to the effect the fence sent moves it on.
        let mut fence = fence();
        let written = Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        });
        assert_eq!(fence.answer(written), Err(FenceError::Unexpected));
        fence.next(Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        });
        assert_eq!(
            fence.answer(failed),
            Err(FenceError::Storage(StorageError::TransactionConflict))
        );
    }
}
