//! Shared pieces of the plugin row operations: rows and their codec, storage calls built from
//! plain bytes, answers read back as plain bytes, a paged scan, and plain reads and writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::operation::{Operation, Runnable, TransactionMode, boxed_suboperation};
use aruna_core::rows;
use aruna_core::types::{Key, KeySpace, Value};

/// A storage failure, with the answer that was not expected.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("plugin run storage failed: {0}")]
pub struct StoreError(pub String);

impl From<IoError> for StoreError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;

pub fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    postcard::to_allocvec(value).map_err(|error| StoreError(error.to_string()))
}

pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StoreError> {
    Ok(rows::decode(bytes)?)
}

/// Runs `operation` as a child whose outcome `map` reduces to what the parent decides with.
pub(crate) fn mapped_child<O, K, T, E>(
    operation: O,
    map: impl FnOnce(Result<O::Output, O::Error>) -> Result<T, E> + Send + 'static,
) -> IoCall
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: Send + 'static,
    O::Error: Send + 'static,
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoCall::Child(boxed_suboperation(operation, move |result| {
        Event::Child(ChildOutcome::new(map(result)))
    }))
}

/// A read of these rows, in this order.
pub fn read_call(keys: Vec<(&'static str, Vec<u8>)>) -> IoCall {
    IoCall::Read(spaced(keys))
}

/// A delete of these rows.
pub fn delete_call(keys: Vec<(&'static str, Vec<u8>)>) -> IoCall {
    IoCall::Delete(spaced(keys))
}

/// At most `limit` rows below `prefix` after the key `after`.
pub fn page_call(
    space: &'static str,
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    limit: usize,
) -> IoCall {
    IoCall::Page {
        space: space.to_string(),
        prefix: prefix.into(),
        after: after.map(Into::into),
        limit,
    }
}

fn spaced(keys: Vec<(&'static str, Vec<u8>)>) -> Vec<(KeySpace, Key)> {
    (keys.into_iter())
        .map(|(space, key)| (space.to_string(), key.into()))
        .collect()
}

/// The rows of a page and the key to continue after.
pub fn page_of(result: IoResult) -> Result<(Rows, Option<Vec<u8>>), StoreError> {
    match result {
        IoResult::Page { rows, next } => Ok((
            (rows.into_iter())
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect(),
            next.map(|key| key.to_vec()),
        )),
        other => Err(unexpected(other)),
    }
}

/// The values of a read, in the order of its rows.
pub fn all_values(result: IoResult) -> Result<Vec<Option<Vec<u8>>>, StoreError> {
    match result {
        IoResult::Values(values) => Ok((values.into_iter())
            .map(|value| value.map(|value| value.to_vec()))
            .collect()),
        other => Err(unexpected(other)),
    }
}

/// One row to write: key space, key and value.
pub type Row = (KeySpace, Key, Value);

pub fn row(space: &str, key: Vec<u8>, value: Vec<u8>) -> Row {
    (space.to_string(), key.into(), value.into())
}

/// Rows a scan reads per page.
const SCAN_PAGE: usize = 1_000;

/// A sub-plan that reads every row below a prefix, page by page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scan {
    space: &'static str,
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    rows: Rows,
}

impl Scan {
    pub fn new(space: &'static str, prefix: Vec<u8>) -> Self {
        Self {
            space,
            prefix,
            after: None,
            rows: Vec::new(),
        }
    }

    /// The request for the next page.
    pub fn ask(&self) -> IoCall {
        page_call(
            self.space,
            self.prefix.clone(),
            self.after.clone(),
            SCAN_PAGE,
        )
    }

    /// Takes one page; returns every row once the last page arrived.
    pub fn take(&mut self, answer: IoResult) -> Result<Option<Rows>, StoreError> {
        let (rows, next) = page_of(answer)?;
        self.rows.extend(rows);
        match next {
            Some(key) => {
                self.after = Some(key);
                Ok(None)
            }
            None => Ok(Some(std::mem::take(&mut self.rows))),
        }
    }
}

/// The values of these rows, read on their own.
pub fn read_rows(reads: Vec<(&'static str, Vec<u8>)>) -> ReadRows {
    ReadRows(Some(read_call(reads)))
}

#[derive(Debug, PartialEq)]
pub struct ReadRows(Option<IoCall>);

impl Operation for ReadRows {
    type Output = Vec<Option<Vec<u8>>>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        let read = self
            .0
            .take()
            .ok_or_else(|| StoreError("read twice".into()))?;
        Ok(Next::Call(read))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Done(all_values(answer)?))
    }
}

/// Every row a scan reads, each page on its own.
pub(crate) fn scan_rows(scan: Scan) -> ScanRows {
    ScanRows(scan)
}

#[derive(Debug, PartialEq)]
pub struct ScanRows(Scan);

impl Operation for ScanRows {
    type Output = Rows;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Rows>, StoreError> {
        Ok(Next::Call(self.0.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Rows>, StoreError> {
        Ok(match self.0.take(answer)? {
            Some(rows) => Next::Done(rows),
            None => Next::Call(self.0.ask()),
        })
    }
}

/// Writes rows, then deletes rows, in one transaction.
pub fn writes(rows: Vec<Row>, deletes: Vec<(&'static str, Vec<u8>)>) -> Writes {
    Writes {
        rows: Some(rows),
        deletes: Some(deletes),
    }
}

#[derive(Debug, PartialEq)]
pub struct Writes {
    rows: Option<Vec<Row>>,
    deletes: Option<Vec<(&'static str, Vec<u8>)>>,
}

impl Operation for Writes {
    type Output = ();
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, StoreError> {
        let rows = self.rows.take().unwrap_or_default();
        Ok(Next::Call(IoCall::Write(rows)))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<()>, StoreError> {
        done(answer)?;
        Ok(match self.deletes.take() {
            Some(deletes) => Next::Call(delete_call(deletes)),
            None => Next::Done(()),
        })
    }
}

/// Persists every pending write of this node with `SyncAll` durability.
pub fn sync_all() -> Change {
    Change(Some(IoCall::Sync))
}

/// Writes rows, each batch on its own.
pub fn write_rows(rows: Vec<Row>) -> Change {
    Change(Some(IoCall::Write(rows)))
}

/// Deletes rows, each batch on its own.
pub(crate) fn delete_rows(keys: Vec<(&'static str, Vec<u8>)>) -> Change {
    Change(Some(delete_call(keys)))
}

/// One write or delete without a decision.
#[derive(Debug, PartialEq)]
pub struct Change(Option<IoCall>);

impl Operation for Change {
    type Output = ();
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<()>, StoreError> {
        let ask = self
            .0
            .take()
            .ok_or_else(|| StoreError("no change".into()))?;
        Ok(Next::Call(ask))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<()>, StoreError> {
        done(answer)?;
        Ok(Next::Done(()))
    }
}

/// The `N` values of a read of `N` rows.
pub fn values<const N: usize>(result: IoResult) -> Result<[Option<Vec<u8>>; N], StoreError> {
    let values = rows::values::<N>(result)?;
    Ok(values.map(|value| value.map(|value| value.to_vec())))
}

/// Checks that a write or delete was answered.
pub fn done(result: IoResult) -> Result<(), StoreError> {
    Ok(rows::done(result)?)
}

/// An answer the operation did not ask for.
pub fn unexpected(result: IoResult) -> StoreError {
    StoreError(format!("unexpected answer {}", result.label()))
}

#[cfg(test)]
#[path = "rows_tests.rs"]
pub(crate) mod tests;
