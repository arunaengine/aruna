//! Guards a version transaction on rows it must read as given, and records its version there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::errors::StorageError;
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

    /// The next call in the caller's transaction, or `None` once the fence is done.
    pub(crate) fn next_call(&mut self, version_id: Ulid) -> Option<IoCall> {
        if self.step == FenceStep::Unread {
            self.step = FenceStep::Checked;
            if !self.guards.is_empty() {
                self.step = FenceStep::Reading;
                let reads = self.guards.iter();
                let reads = reads.map(|(space, key, _)| (space.clone(), key.clone()));
                return Some(IoCall::Read(reads.collect()));
            }
        }
        if self.step == FenceStep::Checked {
            self.step = FenceStep::Done;
            if !self.receipts.is_empty() {
                self.step = FenceStep::Writing;
                let value: Value = version_id.to_bytes().to_vec().into();
                let writes = self.receipts.iter();
                let writes = writes.map(|(space, key)| (space.clone(), key.clone(), value.clone()));
                return Some(IoCall::Write(writes.collect()));
            }
        }
        None
    }

    /// Takes the result of the call `next_call` returned.
    pub(crate) fn answer_call(&mut self, result: IoResult) -> Result<(), FenceError> {
        let storage = |error: IoError| FenceError::Storage(error.into());
        match self.step {
            FenceStep::Reading => {
                let values = result.values().map_err(storage)?;
                let expected = self.guards.iter().map(|(_, _, value)| value);
                if values.len() != self.guards.len() || !values.iter().eq(expected) {
                    return Err(FenceError::Taken);
                }
                self.step = FenceStep::Checked;
                Ok(())
            }
            FenceStep::Writing => {
                result.done().map_err(storage)?;
                self.step = FenceStep::Done;
                Ok(())
            }
            FenceStep::Unread | FenceStep::Checked | FenceStep::Done => Err(FenceError::Unexpected),
        }
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

    fn read(value: Option<Value>) -> IoResult {
        IoResult::Values(vec![value])
    }

    #[test]
    fn records_free_guard() {
        // An absent guard lets the transaction record its version id under the receipt.
        let version_id = Ulid::from_parts(2, 2);
        let mut fence = fence();
        assert!(matches!(fence.next_call(version_id), Some(IoCall::Read(_))));
        assert_eq!(fence.answer_call(read(None)), Ok(()));
        let Some(IoCall::Write(writes)) = fence.next_call(version_id) else {
            panic!("the receipt is written in the transaction");
        };
        assert_eq!(writes[0].2.as_ref(), version_id.to_bytes().as_slice());
        assert_eq!(fence.answer_call(IoResult::Done), Ok(()));
        assert_eq!(fence.next_call(version_id), None);
    }

    #[test]
    fn refuses_taken_guard() {
        // A transaction run again reads the receipt of the commit that won.
        let version_id = Ulid::from_parts(2, 2);
        let mut fence = fence();
        fence.next_call(version_id);
        let winner: Value = Ulid::from_parts(3, 3).to_bytes().to_vec().into();
        assert_eq!(
            fence.answer_call(read(Some(winner))),
            Err(FenceError::Taken)
        );
    }

    #[test]
    fn rejects_stray_results() {
        // Only the answer to the call the fence made moves it on.
        let mut fence = fence();
        assert_eq!(
            fence.answer_call(IoResult::Done),
            Err(FenceError::Unexpected)
        );
        fence.next_call(Ulid::from_parts(2, 2));
        assert!(matches!(
            fence.answer_call(IoResult::Done),
            Err(FenceError::Storage(_))
        ));
    }
}
