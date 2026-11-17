//! Reads one quarantine row by its storage key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::SyncQuarantineRecord;
use aruna_core::types::Effects;
use smallvec::smallvec;

use super::{QuarantineAdminError, row_read, unexpected};

#[derive(Debug, PartialEq)]
enum ReadState {
    Ready(Vec<u8>),
    Reading,
    Done(Result<Option<SyncQuarantineRecord>, QuarantineAdminError>),
}

/// One quarantine row, or none when the key holds no row.
#[derive(Debug, PartialEq)]
pub struct ReadQuarantineOperation {
    state: ReadState,
}

impl ReadQuarantineOperation {
    pub fn new(key: Vec<u8>) -> Self {
        Self {
            state: ReadState::Ready(key),
        }
    }
}

impl EffectOperation for ReadQuarantineOperation {
    type Output = Option<SyncQuarantineRecord>;
    type Error = QuarantineAdminError;

    fn start(&mut self) -> Effects {
        let ReadState::Ready(key) = &self.state else {
            return Effects::new();
        };
        let effect = row_read(key, None);
        self.state = ReadState::Reading;
        smallvec![effect]
    }

    fn step(&mut self, event: Event) -> Effects {
        let result = match (&self.state, event) {
            (ReadState::Reading, Event::Storage(StorageEvent::ReadResult { value, .. })) => value
                .map(|bytes| SyncQuarantineRecord::from_bytes(bytes.as_ref()))
                .transpose()
                .map_err(QuarantineAdminError::Conversion),
            (_, event) => Err(unexpected("storage read result", event)),
        };
        self.state = ReadState::Done(result);
        Effects::new()
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            ReadState::Done(result) => result,
            _ => Err(QuarantineAdminError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.state = ReadState::Done(Err(QuarantineAdminError::Unfinished));
        Effects::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{record, record_bytes};
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn reading() -> ReadQuarantineOperation {
        let mut operation = ReadQuarantineOperation::new(vec![4]);
        operation.start();
        operation
    }

    fn read_result(value: Option<aruna_core::types::Value>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: vec![4].into(),
            value,
        })
    }

    #[test]
    fn reads_row() {
        let mut operation = ReadQuarantineOperation::new(vec![4]);
        check_trace(
            &mut operation,
            &[row_read(&[4], None)],
            vec![(read_result(Some(record_bytes(&record(1, true)))), vec![])],
        );
        assert_eq!(operation.finalize(), Ok(Some(record(1, true))));
        let mut missing = reading();
        missing.step(read_result(None));
        assert_eq!(missing.finalize(), Ok(None));
    }

    #[test]
    fn storage_errors_fail() {
        let mut operation = reading();
        operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(
            operation.finalize(),
            Err(QuarantineAdminError::Storage(StorageError::Timeout))
        );
        let mut corrupt = reading();
        corrupt.step(read_result(Some(vec![0xff].into())));
        assert!(matches!(
            corrupt.finalize(),
            Err(QuarantineAdminError::Conversion(_))
        ));
    }

    #[test]
    fn rejects_foreign_events() {
        let refusals = rejects_events(reading, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refusals.is_empty());
        for (label, effects, error) in refusals {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(
                matches!(error, QuarantineAdminError::Unexpected { .. }),
                "{label}"
            );
        }
        let mut early = ReadQuarantineOperation::new(vec![4]);
        early.step(read_result(None));
        assert!(early.finalize().is_err());
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(reading());
        let mut operation = reading();
        check_abort(&mut operation, &[]);
        assert_eq!(operation.finalize(), Err(QuarantineAdminError::Unfinished));
    }
}
