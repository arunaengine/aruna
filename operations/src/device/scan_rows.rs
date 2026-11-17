//! Reads every row of a device keyspace, or of one prefix of it, page by page. A failed page
//! ends the scan with the rows read so far: the device views report what they can read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::EffectOperation;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error, PartialEq)]
pub enum ScanRowsError {
    #[error("unexpected {0} while scanning device rows")]
    UnexpectedEvent(&'static str),
    #[error("scanning device rows did not finish")]
    NotFinished,
}

/// Outputs the values of every row in `key_space` under `prefix`, in key order.
#[derive(Debug, PartialEq)]
pub struct ScanRowsOperation {
    key_space: String,
    prefix: Option<Key>,
    limit: usize,
    started: bool,
    rows: Vec<Value>,
    output: Option<Result<Vec<Value>, ScanRowsError>>,
}

impl ScanRowsOperation {
    pub fn new(key_space: &str, prefix: Option<Key>, limit: usize) -> Self {
        Self {
            key_space: key_space.to_string(),
            prefix,
            limit,
            started: false,
            rows: Vec::new(),
            output: None,
        }
    }

    fn page(&self, start_after: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: self.key_space.clone(),
            prefix: self.prefix.clone(),
            start: start_after.map(IterStart::After),
            limit: self.limit,
            txn_id: None,
        })]
    }

    fn finish(&mut self, output: Result<Vec<Value>, ScanRowsError>) -> Effects {
        self.output = Some(output);
        smallvec![]
    }
}

impl EffectOperation for ScanRowsOperation {
    type Output = Vec<Value>;
    type Error = ScanRowsError;

    fn start(&mut self) -> Effects {
        self.started = true;
        self.page(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) if self.started => {
                self.rows.extend(values.into_iter().map(|(_, value)| value));
                match next_start_after {
                    Some(next) => self.page(Some(next)),
                    None => {
                        let rows = std::mem::take(&mut self.rows);
                        self.finish(Ok(rows))
                    }
                }
            }
            Event::Storage(StorageEvent::Error { error }) if self.started => {
                warn!(key_space = %self.key_space, error = %error, "A device row scan stopped early");
                let rows = std::mem::take(&mut self.rows);
                self.finish(Ok(rows))
            }
            event => {
                let label = event.label();
                self.finish(Err(ScanRowsError::UnexpectedEvent(label)))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ScanRowsError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn scan() -> ScanRowsOperation {
        ScanRowsOperation::new("rows", Some(b"p".to_vec().into()), 2)
    }

    fn page(values: &[&[u8]], next: Option<&[u8]>) -> Event {
        let values = values
            .iter()
            .map(|value| (Key::from(value.to_vec()), Value::from(value.to_vec())));
        Event::Storage(StorageEvent::IterResult {
            values: values.collect(),
            next_start_after: next.map(|key| key.to_vec().into()),
        })
    }

    // Pages are read until the cursor ends, under the prefix.
    #[test]
    fn reads_every_page() {
        let mut rows = scan();
        let first = rows.page(None);
        let second = rows.page(Some(b"b".to_vec().into()));
        check_trace(
            &mut rows,
            &first,
            vec![
                (page(&[b"a", b"b"], Some(b"b")), second.into_vec()),
                (page(&[b"c"], None), Vec::new()),
            ],
        );
        let values: Vec<Value> = [b"a", b"b", b"c"]
            .iter()
            .map(|value| value.to_vec().into())
            .collect();
        assert_eq!(rows.finalize(), Ok(values));
    }

    // A failed page keeps the rows read so far; other events are refused.
    #[test]
    fn partial_rows_kept() {
        let mut rows = scan();
        rows.start();
        rows.step(page(&[b"a"], Some(b"a")));
        rows.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(rows.finalize(), Ok(vec![b"a".to_vec().into()]));

        let all = sample_events().len();
        assert_eq!(rejects_events(scan, sample_events, |_| false).len(), all);
        rejects_finalize(scan());
        assert!(scan().abort().is_empty());
    }
}
