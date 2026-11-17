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
