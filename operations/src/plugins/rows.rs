//! Runs a pure plan of plugin row reads and writes as one operation, in one transaction or not.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect, abort_effects};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;

use super::store::{Rows, StoreError};

/// One storage request of a plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ask {
    /// The values of these rows, in this order; an empty list is answered without storage.
    Read(Vec<(&'static str, Vec<u8>)>),
    /// At most `limit` rows below `prefix` after the key `after`.
    Page {
        space: &'static str,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
    },
    /// Writes these rows; an empty list is answered without storage.
    Write(Vec<Row>),
    /// Deletes these rows; an empty list is answered without storage.
    Delete(Vec<(&'static str, Vec<u8>)>),
}

/// The answer to an [`Ask`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    Values(Vec<Option<Vec<u8>>>),
    /// The rows of a page, and the key to continue after when more may follow.
    Page(Rows, Option<Vec<u8>>),
    Done,
}

/// What a plan does next: ask storage, or end with its outcome.
#[derive(Debug, PartialEq, Eq)]
pub enum Turn<T> {
    Ask(Ask),
    Finish(T),
}

/// The decisions of one use case over plugin rows, without I/O. The operation that runs it
/// starts, commits and aborts the transaction and checks every storage event.
pub trait Plan: Send + std::fmt::Debug + PartialEq {
    type Output: Send + std::fmt::Debug;
    type Error: Send + std::fmt::Debug + From<StoreError>;

    /// The first request, or the outcome when the plan needs no storage.
    fn first(&mut self) -> Result<Turn<Self::Output>, Self::Error>;
    /// The next request after `answer`, or the outcome.
    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, Self::Error>;
}

/// Runs a [`Plan`]: in one write transaction that commits its outcome and aborts on an error,
/// or with each request on its own.
#[derive(Debug)]
pub struct RowsOperation<P: Plan> {
    plan: P,
    transaction: bool,
    txn_id: Option<TxnId>,
    state: State<P::Output, P::Error>,
}

#[derive(Debug)]
enum State<O, E> {
    Created,
    /// Waits for the transaction that the first request needs.
    Starting(Ask),
    Waiting(Wait),
    Committing(O),
    Finished(Result<O, E>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wait {
    Values(usize),
    Page,
    Written,
    Deleted,
}

impl<P: Plan> PartialEq for RowsOperation<P> {
    fn eq(&self, other: &Self) -> bool {
        self.plan == other.plan
            && self.transaction == other.transaction
            && self.txn_id == other.txn_id
            && std::mem::discriminant(&self.state) == std::mem::discriminant(&other.state)
    }
}

impl<P: Plan> RowsOperation<P> {
    /// Runs `plan` in one write transaction.
    pub fn transaction(plan: P) -> Self {
        Self::new(plan, true)
    }

    /// Runs `plan` with each request on its own.
    pub fn direct(plan: P) -> Self {
        Self::new(plan, false)
    }

    fn new(plan: P, transaction: bool) -> Self {
        Self {
            plan,
            transaction,
            txn_id: None,
            state: State::Created,
        }
    }

    fn advance(&mut self, mut turn: Result<Turn<P::Output>, P::Error>) -> Effects {
        loop {
            let ask = match turn {
                Err(error) => return self.fail(error),
                Ok(Turn::Finish(output)) => return self.finish(output),
                Ok(Turn::Ask(ask)) => ask,
            };
            if let Some(answer) = empty_answer(&ask) {
                turn = self.plan.next(answer);
                continue;
            }
            if self.transaction && self.txn_id.is_none() {
                self.state = State::Starting(ask);
                let start = StorageEffect::StartTransaction { read: false };
                return smallvec![Effect::Storage(start)];
            }
            return self.request(ask);
        }
    }

    fn request(&mut self, ask: Ask) -> Effects {
        let txn_id = self.txn_id;
        let (wait, effect) = match ask {
            Ask::Read(reads) => (
                Wait::Values(reads.len()),
                StorageEffect::BatchRead {
                    reads: (reads.into_iter())
                        .map(|(space, key)| (space.to_string(), key.into()))
                        .collect(),
                    txn_id,
                },
            ),
            Ask::Page {
                space,
                prefix,
                after,
                limit,
            } => (
                Wait::Page,
                StorageEffect::Iter {
                    key_space: space.to_string(),
                    prefix: Some(prefix.into()),
                    start: after.map(|key| IterStart::After(key.into())),
                    limit,
                    txn_id,
                },
            ),
            Ask::Write(rows) => (
                Wait::Written,
                StorageEffect::BatchWrite {
                    writes: rows,
                    txn_id,
                },
            ),
            Ask::Delete(keys) => (
                Wait::Deleted,
                StorageEffect::BatchDelete {
                    deletes: (keys.into_iter())
                        .map(|(space, key)| (space.to_string(), key.into()))
                        .collect(),
                    txn_id,
                },
            ),
        };
        self.state = State::Waiting(wait);
        smallvec![Effect::Storage(effect)]
    }

    fn finish(&mut self, output: P::Output) -> Effects {
        match self.txn_id {
            Some(txn_id) => {
                self.state = State::Committing(output);
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            None => {
                self.state = State::Finished(Ok(output));
                smallvec![]
            }
        }
    }

    /// Ends with `error`; an open transaction is aborted unless its commit was sent.
    fn fail(&mut self, error: impl Into<P::Error>) -> Effects {
        let committing = matches!(self.state, State::Committing(_));
        self.state = State::Finished(Err(error.into()));
        match committing {
            true => smallvec![],
            false => abort_effects(&mut self.txn_id),
        }
    }
}

impl<P: Plan> Operation for RowsOperation<P> {
    type Output = P::Output;
    type Error = P::Error;

    fn start(&mut self) -> Effects {
        let turn = self.plan.first();
        self.advance(turn)
    }

    fn step(&mut self, event: Event) -> Effects {
        let Event::Storage(event) = event else {
            return self.fail(StoreError(format!("{event:?}")));
        };
        match (std::mem::replace(&mut self.state, State::Created), event) {
            (State::Starting(ask), StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                self.request(ask)
            }
            (State::Waiting(wait), event) => match answer(wait, event) {
                Ok(answer) => {
                    let turn = self.plan.next(answer);
                    self.advance(turn)
                }
                Err(error) => self.fail(error),
            },
            (State::Committing(output), StorageEvent::TransactionCommitted { txn_id })
                if Some(txn_id) == self.txn_id =>
            {
                self.txn_id = None;
                self.state = State::Finished(Ok(output));
                smallvec![]
            }
            (state, event) => {
                self.state = state;
                self.fail(StoreError(format!("{event:?}")))
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Finished(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            State::Finished(result) => result,
            _ => Err(StoreError("the plugin row operation did not finish".into()).into()),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            State::Committing(_) | State::Finished(_) => smallvec![],
            _ => abort_effects(&mut self.txn_id),
        }
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
    pub fn ask(&self) -> Ask {
        Ask::Page {
            space: self.space,
            prefix: self.prefix.clone(),
            after: self.after.clone(),
            limit: SCAN_PAGE,
        }
    }

    /// Takes one page; returns every row once the last page arrived.
    pub fn take(&mut self, answer: Answer) -> Result<Option<Rows>, StoreError> {
        let Answer::Page(rows, next) = answer else {
            return Err(unexpected(answer));
        };
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
pub fn read_rows(reads: Vec<(&'static str, Vec<u8>)>) -> RowsOperation<ReadRows> {
    RowsOperation::direct(ReadRows(Some(Ask::Read(reads))))
}

#[derive(Debug, PartialEq)]
pub struct ReadRows(Option<Ask>);

impl Plan for ReadRows {
    type Output = Vec<Option<Vec<u8>>>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        let read = self.0.take().ok_or_else(|| StoreError("read twice".into()))?;
        Ok(Turn::Ask(read))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        match answer {
            Answer::Values(values) => Ok(Turn::Finish(values)),
            other => Err(unexpected(other)),
        }
    }
}

/// Every row a scan reads, each page on its own.
pub fn scan_rows(scan: Scan) -> RowsOperation<ScanRows> {
    RowsOperation::direct(ScanRows(scan))
}

#[derive(Debug, PartialEq)]
pub struct ScanRows(Scan);

impl Plan for ScanRows {
    type Output = Rows;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Rows>, StoreError> {
        Ok(Turn::Ask(self.0.ask()))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Rows>, StoreError> {
        Ok(match self.0.take(answer)? {
            Some(rows) => Turn::Finish(rows),
            None => Turn::Ask(self.0.ask()),
        })
    }
}

/// At most `limit` rows below `prefix` after the key `after`, and the key to continue after.
pub fn page_rows(
    space: &'static str,
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    limit: usize,
) -> RowsOperation<PageRows> {
    RowsOperation::direct(PageRows(Some(Ask::Page {
        space,
        prefix,
        after,
        limit,
    })))
}

#[derive(Debug, PartialEq)]
pub struct PageRows(Option<Ask>);

impl Plan for PageRows {
    type Output = (Rows, Option<Vec<u8>>);
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        let page = self.0.take().ok_or_else(|| StoreError("paged twice".into()))?;
        Ok(Turn::Ask(page))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        match answer {
            Answer::Page(rows, next) => Ok(Turn::Finish((rows, next))),
            other => Err(unexpected(other)),
        }
    }
}

/// Writes rows, each batch on its own.
pub fn write_rows(rows: Vec<Row>) -> RowsOperation<Change> {
    RowsOperation::direct(Change(Some(Ask::Write(rows))))
}

/// Deletes rows, each batch on its own.
pub fn delete_rows(keys: Vec<(&'static str, Vec<u8>)>) -> RowsOperation<Change> {
    RowsOperation::direct(Change(Some(Ask::Delete(keys))))
}

/// One write or delete without a decision.
#[derive(Debug, PartialEq)]
pub struct Change(Option<Ask>);

impl Plan for Change {
    type Output = ();
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<()>, StoreError> {
        let ask = self.0.take().ok_or_else(|| StoreError("no change".into()))?;
        Ok(Turn::Ask(ask))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<()>, StoreError> {
        match answer {
            Answer::Done => Ok(Turn::Finish(())),
            other => Err(unexpected(other)),
        }
    }
}

/// The `N` values of a read of `N` rows.
pub fn values<const N: usize>(answer: Answer) -> Result<[Option<Vec<u8>>; N], StoreError> {
    match answer {
        Answer::Values(values) => <[_; N]>::try_from(values)
            .map_err(|values| StoreError(format!("{} values for {N} rows", values.len()))),
        other => Err(unexpected(other)),
    }
}

/// Checks that a write or delete was answered.
pub fn done(answer: Answer) -> Result<(), StoreError> {
    match answer {
        Answer::Done => Ok(()),
        other => Err(unexpected(other)),
    }
}

/// An answer the plan did not ask for.
pub fn unexpected(answer: Answer) -> StoreError {
    StoreError(format!("unexpected answer {answer:?}"))
}

/// The answer to an empty read, write or delete, which needs no storage.
fn empty_answer(ask: &Ask) -> Option<Answer> {
    match ask {
        Ask::Read(reads) if reads.is_empty() => Some(Answer::Values(Vec::new())),
        Ask::Write(rows) if rows.is_empty() => Some(Answer::Done),
        Ask::Delete(keys) if keys.is_empty() => Some(Answer::Done),
        _ => None,
    }
}

fn answer(wait: Wait, event: StorageEvent) -> Result<Answer, StoreError> {
    match (wait, event) {
        (Wait::Values(count), StorageEvent::BatchReadResult { values }) if values.len() == count => {
            Ok(Answer::Values(
                (values.into_iter())
                    .map(|(_, value)| value.map(|value| value.to_vec()))
                    .collect(),
            ))
        }
        (
            Wait::Page,
            StorageEvent::IterResult {
                values,
                next_start_after,
            },
        ) => Ok(Answer::Page(
            (values.into_iter())
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect(),
            next_start_after.map(|key| key.to_vec()),
        )),
        (Wait::Written, StorageEvent::BatchWriteResult { .. }) => Ok(Answer::Done),
        (Wait::Deleted, StorageEvent::BatchDeleteResult { .. }) => Ok(Answer::Done),
        (_, event) => Err(StoreError(format!("{event:?}"))),
    }
}

#[cfg(test)]
#[path = "rows_tests.rs"]
mod tests;
