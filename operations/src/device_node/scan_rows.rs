//! Reads every row of a device keyspace, or of one prefix of it, page by page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::types::{Key, Value};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ScanRowsError {
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Outputs the values of every row in `key_space` under `prefix`, in key order.
#[derive(Debug, PartialEq)]
pub struct ScanRowsOperation {
    pages: PrefixPages,
    rows: Vec<Value>,
}

impl ScanRowsOperation {
    pub fn new(key_space: &str, prefix: Option<Key>, limit: usize) -> Self {
        let prefix = prefix.unwrap_or_else(|| Key::from(Vec::new()));
        Self {
            pages: PrefixPages::new(key_space, prefix, limit),
            rows: Vec::new(),
        }
    }
}

impl Operation for ScanRowsOperation {
    type Output = Vec<Value>;
    type Error = ScanRowsError;

    fn first(&mut self) -> Result<Next<Vec<Value>>, ScanRowsError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Value>>, ScanRowsError> {
        let rows = self.pages.take(result)?;
        self.rows.extend(rows.into_iter().map(|(_, value)| value));
        Ok(match self.pages.done() {
            false => Next::Call(self.pages.call()),
            true => Next::Done(std::mem::take(&mut self.rows)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::IoCall;

    fn scan() -> ScanRowsOperation {
        ScanRowsOperation::new("rows", Some(b"p".to_vec().into()), 2)
    }

    fn page(values: &[&[u8]], next: Option<&[u8]>) -> IoResult {
        let rows = values
            .iter()
            .map(|value| (Key::from(value.to_vec()), Value::from(value.to_vec())));
        IoResult::Page {
            rows: rows.collect(),
            next: next.map(|key| key.to_vec().into()),
        }
    }

    // Pages are read until the cursor ends, under the prefix.
    #[test]
    fn reads_every_page() {
        let mut rows = scan();
        let first = IoCall::Page {
            space: "rows".to_string(),
            prefix: b"p".to_vec().into(),
            after: None,
            limit: 2,
        };
        assert_eq!(rows.first(), Ok(Next::Call(first)));
        let second = Next::Call(IoCall::Page {
            space: "rows".to_string(),
            prefix: b"p".to_vec().into(),
            after: Some(b"b".to_vec().into()),
            limit: 2,
        });
        let first_page = page(&[b"a".as_slice(), b"b".as_slice()], Some(b"b".as_slice()));
        assert_eq!(rows.next(first_page), Ok(second));
        let values: Vec<Value> = [b"a", b"b", b"c"]
            .iter()
            .map(|value| value.to_vec().into())
            .collect();
        assert_eq!(
            rows.next(page(&[b"c".as_slice()], None)),
            Ok(Next::Done(values))
        );
    }

    // A result that answers no page fails the scan.
    #[test]
    fn other_result_fails() {
        let mut rows = scan();
        rows.first().unwrap();
        assert!(rows.next(IoResult::Done).is_err());
    }
}
