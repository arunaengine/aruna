//! Scans one key prefix in pages up to a row limit, reporting whether rows were cut.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::operation::Operation;
use aruna_core::types::{Key, Value};
use thiserror::Error;

use crate::metadata::call_results::page_rows;

/// Storage rows read per scan page.
pub(super) const PREFLIGHT_PAGE_SIZE: usize = 128;

/// The rows found, at most the limit, and whether more rows matched.
pub type ScannedRows = (Vec<(Key, Value)>, bool);

#[derive(Debug, Error, PartialEq)]
pub enum PrefixScanError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for PrefixScanError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::Io(other),
        }
    }
}

/// Reads rows under `prefix` from `from` on, untransacted, and stops once more than
/// `limit` rows are seen: the result keeps `limit` rows and marks the scan as cut.
#[derive(Debug, PartialEq)]
pub struct PrefixScanOperation {
    key_space: String,
    prefix: Key,
    from: Option<Key>,
    limit: usize,
    rows: Vec<(Key, Value)>,
}

impl PrefixScanOperation {
    pub(super) fn new(key_space: &str, (prefix, from): (Key, Option<Key>), limit: usize) -> Self {
        Self {
            key_space: key_space.to_string(),
            prefix,
            from,
            limit,
            rows: Vec::new(),
        }
    }

    fn page_limit(&self) -> usize {
        PREFLIGHT_PAGE_SIZE.min(self.limit + 1 - self.rows.len())
    }

    fn page(&self, after: Option<Key>) -> IoCall {
        IoCall::Page {
            space: self.key_space.clone(),
            prefix: self.prefix.clone(),
            after,
            limit: self.page_limit(),
        }
    }
}

impl Operation for PrefixScanOperation {
    type Output = ScannedRows;
    type Error = PrefixScanError;

    fn first(&mut self) -> Result<Next<ScannedRows>, PrefixScanError> {
        let Some(from) = self.from.take() else {
            return Ok(Next::Call(self.page(None)));
        };
        // A page call only continues after a key; starting at one needs the iteration effect.
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(
            StorageEffect::Iter {
                key_space: self.key_space.clone(),
                prefix: Some(self.prefix.clone()),
                start: Some(IterStart::At(from)),
                limit: self.page_limit(),
                txn_id: None,
            },
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ScannedRows>, PrefixScanError> {
        let (values, next) = page_rows(result)?;
        self.rows.extend(values);
        if self.rows.len() > self.limit {
            let mut rows = std::mem::take(&mut self.rows);
            rows.truncate(self.limit);
            return Ok(Next::Done((rows, true)));
        }
        match next {
            Some(next) => Ok(Next::Call(self.page(Some(next)))),
            None => Ok(Next::Done((std::mem::take(&mut self.rows), false))),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::events::{Event, StorageEvent};

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

    fn page_call(after: Option<Key>, limit: usize) -> Result<Next<ScannedRows>, PrefixScanError> {
        Ok(Next::Call(IoCall::Page {
            space: SPACE.to_string(),
            prefix: prefix(),
            after,
            limit,
        }))
    }

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
    }

    fn scanning(limit: usize) -> PrefixScanOperation {
        let mut operation = PrefixScanOperation::new(SPACE, (prefix(), None), limit);
        operation.first().unwrap();
        operation
    }

    #[test]
    fn scans_all_pages() {
        // The scan starts at `from`, follows the cursor and asks only for what the limit allows.
        let mut operation = PrefixScanOperation::new(SPACE, (prefix(), Some(key(1))), 5);
        let start = StorageEffect::Iter {
            key_space: SPACE.to_string(),
            prefix: Some(prefix()),
            start: Some(IterStart::At(key(1))),
            limit: 6,
            txn_id: None,
        };
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(start)))))
        );
        let first = StorageEvent::IterResult {
            values: vec![row(1), row(2)],
            next_start_after: Some(key(2)),
        };
        assert_eq!(
            operation.next(IoResult::Event(Box::new(Event::Storage(first)))),
            page_call(Some(key(2)), 4)
        );
        assert_eq!(
            operation.next(page(vec![row(3)], None)),
            Ok(Next::Done((vec![row(1), row(2), row(3)], false)))
        );
    }

    #[test]
    fn cut_at_limit() {
        // One row past the limit marks the scan as cut and is dropped.
        let mut operation = PrefixScanOperation::new(SPACE, (prefix(), None), 2);
        assert_eq!(operation.first(), page_call(None, 3));
        assert_eq!(
            operation.next(page(vec![row(1), row(2), row(3)], Some(key(3)))),
            Ok(Next::Done((vec![row(1), row(2)], true)))
        );
    }

    #[test]
    fn large_limit_pages() {
        let mut operation = PrefixScanOperation::new(SPACE, (prefix(), None), 10_000);
        assert_eq!(operation.first(), page_call(None, PREFLIGHT_PAGE_SIZE));
    }

    #[test]
    fn errors_fail() {
        let error = IoResult::Event(Box::new(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })));
        assert_eq!(
            scanning(2).next(error),
            Err(PrefixScanError::Storage(StorageError::Timeout))
        );
        assert!(matches!(
            scanning(2).next(IoResult::Done),
            Err(PrefixScanError::Io(IoError::Unexpected { .. }))
        ));
        assert_eq!(
            PrefixScanError::from(IoError::Aborted),
            PrefixScanError::Io(IoError::Aborted)
        );
    }
}
