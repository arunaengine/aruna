//! Scans one key prefix in pages up to a row limit, reporting whether rows were cut.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;

/// Storage rows read per scan page.
pub(super) const PREFLIGHT_PAGE_SIZE: usize = 128;

/// The rows found, at most the limit, and whether more rows matched.
pub type ScannedRows = (Vec<(Key, Value)>, bool);

#[derive(Debug, Error, PartialEq)]
pub enum PrefixScanError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("unexpected event while scanning: got {0}")]
    UnexpectedEvent(&'static str),
    #[error("the prefix scan did not finish")]
    Unfinished,
}

/// Reads rows under `prefix` from `from` on, untransacted, and stops once more than
/// `limit` rows are seen: the result keeps `limit` rows and marks the scan as cut.
#[derive(Debug, PartialEq)]
pub struct PrefixScanOperation {
    key_space: String,
    prefix: Key,
    start: Option<IterStart>,
    limit: usize,
    rows: Vec<(Key, Value)>,
    scanning: bool,
    output: Option<Result<ScannedRows, PrefixScanError>>,
}

impl PrefixScanOperation {
    pub(super) fn new(key_space: &str, (prefix, from): (Key, Option<Key>), limit: usize) -> Self {
        Self {
            key_space: key_space.to_string(),
            prefix,
            start: from.map(IterStart::At),
            limit,
            rows: Vec::new(),
            scanning: false,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<ScannedRows, PrefixScanError>) -> Effects {
        self.scanning = false;
        self.output = Some(output);
        smallvec![]
    }

    fn scan(&mut self) -> Effects {
        self.scanning = true;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: self.key_space.clone(),
            prefix: Some(self.prefix.clone()),
            start: self.start.take(),
            limit: PREFLIGHT_PAGE_SIZE.min(self.limit + 1 - self.rows.len()),
            txn_id: None,
        })]
    }

    fn scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        self.rows.extend(values);
        if self.rows.len() > self.limit {
            let mut rows = std::mem::take(&mut self.rows);
            rows.truncate(self.limit);
            return self.finish(Ok((rows, true)));
        }
        match next {
            Some(next) => {
                self.start = Some(IterStart::After(next));
                self.scan()
            }
            None => {
                let rows = std::mem::take(&mut self.rows);
                self.finish(Ok((rows, false)))
            }
        }
    }
}

impl Operation for PrefixScanOperation {
    type Output = ScannedRows;
    type Error = PrefixScanError;

    fn start(&mut self) -> Effects {
        self.scan()
    }

    fn step(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) if self.scanning => self.scanned(values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) if self.scanning => {
                self.finish(Err(error.into()))
            }
            other => self.finish(Err(PrefixScanError::UnexpectedEvent(other.label()))),
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PrefixScanError::Unfinished))
    }

    // Untransacted reads leave nothing to clean up.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    const SPACE: &str = "rows";

    fn key(seed: u8) -> Key {
        vec![7, seed].into()
    }

    fn row(seed: u8) -> (Key, Value) {
        (key(seed), vec![seed].into())
    }

    fn prefix() -> Key {
        vec![7].into()
    }

    fn scan_effect(start: Option<IterStart>, limit: usize) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: SPACE.to_string(),
            prefix: Some(prefix()),
            start,
            limit,
            txn_id: None,
        })
    }

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: rows,
            next_start_after: next,
        })
    }

    fn scanning(limit: usize) -> PrefixScanOperation {
        let mut operation = PrefixScanOperation::new(SPACE, (prefix(), None), limit);
        operation.start();
        operation
    }

    #[test]
    fn scans_all_pages() {
        // The scan starts at `from`, follows the cursor and asks only for what the limit allows.
        let mut operation = PrefixScanOperation::new(SPACE, (prefix(), Some(key(1))), 5);
        check_trace(
            &mut operation,
            &[scan_effect(Some(IterStart::At(key(1))), 6)],
            vec![
                (
                    page(vec![row(1), row(2)], Some(key(2))),
                    vec![scan_effect(Some(IterStart::After(key(2))), 4)],
                ),
                (page(vec![row(3)], None), vec![]),
            ],
        );
        assert_eq!(
            operation.finalize(),
            Ok((vec![row(1), row(2), row(3)], false))
        );
    }

    #[test]
    fn cut_at_limit() {
        // One row past the limit marks the scan as cut and is dropped.
        let mut operation = scanning(2);
        assert!(
            operation
                .step(page(vec![row(1), row(2), row(3)], Some(key(3))))
                .is_empty()
        );
        assert_eq!(operation.finalize(), Ok((vec![row(1), row(2)], true)));
    }

    #[test]
    fn large_limit_pages() {
        let mut operation = PrefixScanOperation::new(SPACE, (prefix(), None), 10_000);
        assert_eq!(
            operation.start().as_slice(),
            [scan_effect(None, PREFLIGHT_PAGE_SIZE)]
        );
    }

    #[test]
    fn storage_errors_fail() {
        let mut operation = scanning(2);
        let error = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert!(operation.step(error).is_empty());
        assert_eq!(
            operation.finalize(),
            Err(PrefixScanError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn rejects_other_events() {
        let accepted = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (_, effects, error) in rejects_events(|| scanning(2), sample_events, accepted) {
            assert!(effects.is_empty());
            assert!(matches!(error, PrefixScanError::UnexpectedEvent(_)));
        }
        let mut idle = PrefixScanOperation::new(SPACE, (prefix(), None), 2);
        idle.step(page(Vec::new(), None));
        assert!(matches!(
            idle.finalize(),
            Err(PrefixScanError::UnexpectedEvent(_))
        ));
    }

    #[test]
    fn abort_cleans_nothing() {
        let mut operation = PrefixScanOperation::new(SPACE, (prefix(), None), 2);
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
    }

    #[test]
    fn finalize_needs_result() {
        rejects_finalize(PrefixScanOperation::new(SPACE, (prefix(), None), 2));
        rejects_finalize(scanning(2));
    }
}
