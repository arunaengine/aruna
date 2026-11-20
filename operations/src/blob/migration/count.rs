//! Starts and advances this node's saved count of a bucket's current versions against its rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{COUNT_QUEUE_KEYSPACE, COVERAGE_COUNT_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::placement::coverage::CoverageCount;
use aruna_core::types::{Key, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::drain::MIGRATION_CONTINUE;
use super::shared::MigrationError;
use crate::s3::bucket::storage_check::wake_migration;
use crate::s3::policy::coverage::{
    COVERAGE_PAGE_LIMIT, CoverageError, CoverageInput, CoverageReport, CoverageScope,
    PolicyCoverageOperation,
};

/// Queued buckets one run of the migration task looks at.
const BUCKET_PAGE: usize = 16;
/// Coverage pages one bucket counts in one run, so other buckets and jobs of the task go on.
const COUNT_PAGES: usize = 16;

#[derive(Debug, Error, PartialEq)]
pub enum StartCountError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("the coverage count did not start: {0}")]
    Io(IoError),
}

impl From<IoError> for StartCountError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

fn count_key(bucket: &str) -> Key {
    bucket.as_bytes().to_vec().into()
}

fn decode_count(value: Option<Value>) -> Result<Option<CoverageCount>, ConversionError> {
    (value.as_deref())
        .map(CoverageCount::from_bytes)
        .transpose()
}

/// Replaces the bucket's saved count with one started at the run's time and wakes the migration
/// task once the record committed. The count joins `run`, or a run it mints when none is given.
#[derive(Debug, PartialEq)]
pub struct StartCountOperation {
    bucket: String,
    record: CoverageCount,
    ids: IdMint,
    written: bool,
}

impl StartCountOperation {
    pub fn new(bucket: String, run: Option<Ulid>) -> Self {
        Self {
            bucket,
            record: CoverageCount {
                run,
                ..CoverageCount::new(0)
            },
            ids: IdMint::default(),
            written: false,
        }
    }
}

/// The positions of node counts from another run than the one a distributed count reports: the
/// run its start gave, else the newest any node holds. A node without a count is in every run.
pub fn other_runs(started: Option<Ulid>, runs: &[Option<Ulid>]) -> Vec<usize> {
    let Some(current) = started.or_else(|| runs.iter().flatten().max().copied()) else {
        return Vec::new();
    };
    (runs.iter().enumerate())
        .filter(|(_, run)| run.is_some_and(|run| run != current))
        .map(|(index, _)| index)
        .collect()
}

impl Operation for StartCountOperation {
    type Output = CoverageCount;
    type Error = StartCountError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn stamp(&mut self, now_ms: u64) {
        self.record.started_at_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<CoverageCount>, StartCountError> {
        let started = self.record.started_at_ms;
        self.record.run = Some(self.record.run.unwrap_or_else(|| self.ids.mint(started)));
        Ok(Next::Call(read_row(
            S3_BUCKET_KEYSPACE,
            count_key(&self.bucket),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CoverageCount>, StartCountError> {
        if self.written {
            result.done()?;
            return Ok(Next::Then {
                output: self.record.clone(),
                calls: vec![wake_migration()],
            });
        }
        if result.value()?.is_none() {
            return Err(StartCountError::NoSuchBucket);
        }
        let key = count_key(&self.bucket);
        let value = self.record.to_bytes()?;
        self.written = true;
        Ok(Next::Call(IoCall::Write(vec![
            (
                COVERAGE_COUNT_KEYSPACE.to_string(),
                key.clone(),
                value.into(),
            ),
            (
                COUNT_QUEUE_KEYSPACE.to_string(),
                key,
                Vec::<u8>::new().into(),
            ),
        ])))
    }
}

/// Reads the saved count of `bucket` on this node.
#[derive(Debug, PartialEq)]
pub struct ReadCountOperation {
    bucket: String,
}

impl ReadCountOperation {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
        }
    }
}

impl Operation for ReadCountOperation {
    type Output = Option<CoverageCount>;
    type Error = StartCountError;

    fn first(&mut self) -> Result<Next<Option<CoverageCount>>, StartCountError> {
        let read = read_row(COVERAGE_COUNT_KEYSPACE, count_key(&self.bucket));
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<CoverageCount>>, StartCountError> {
        Ok(Next::Done(decode_count(result.value()?)?))
    }
}

/// Adds one page to the count. Rules changed since the first page start the count over, so
/// every number compares against one generation.
fn add_page(record: CoverageCount, report: &CoverageReport, now_ms: u64) -> CoverageCount {
    let fresh = record.cursor.is_none() && record.observed == 0;
    let mut record = match fresh || record.generation == report.generation {
        true => record,
        false => {
            return CoverageCount {
                run: record.run,
                ..CoverageCount::new(record.started_at_ms)
            };
        }
    };
    record.generation = report.generation;
    record.observed += report.observed as u64;
    record.deleted += report.deleted as u64;
    record.gaps += report.gaps.len() as u64;
    record.registered += report.copies.registered as u64;
    record.quarantined += report.copies.quarantined as u64;
    record.absent += report.copies.absent as u64;
    record.reference_only += report.copies.reference_only as u64;
    record.cursor = report.cursor.as_ref().map(|cursor| cursor.to_vec());
    if report.complete {
        record.finished_at_ms = Some(now_ms);
    }
    record
}

#[derive(Debug, PartialEq)]
enum CountsStep {
    Queue,
    Record(String),
    Page,
}

/// Advances the unfinished counts at the head of the queue. Outputs when the task must run again.
#[derive(Debug, PartialEq)]
pub struct ProcessCountsOperation {
    step: CountsStep,
    buckets: VecDeque<Key>,
    next: Option<Duration>,
}

type CountsTurn = Result<Next<Option<Duration>>, MigrationError>;

impl ProcessCountsOperation {
    pub fn new() -> Self {
        Self {
            step: CountsStep::Queue,
            buckets: VecDeque::new(),
            next: None,
        }
    }

    fn next_bucket(&mut self) -> CountsTurn {
        let Some(key) = self.buckets.pop_front() else {
            return Ok(Next::Done(self.next));
        };
        let bucket = String::from_utf8(key.to_vec()).map_err(MigrationError::failed)?;
        let read = read_row(COVERAGE_COUNT_KEYSPACE, count_key(&bucket));
        self.step = CountsStep::Record(bucket);
        Ok(Next::Call(read))
    }
}

impl Default for ProcessCountsOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for ProcessCountsOperation {
    type Output = Option<Duration>;
    type Error = MigrationError;

    fn first(&mut self) -> CountsTurn {
        Ok(Next::Call(IoCall::Page {
            space: COUNT_QUEUE_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: BUCKET_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> CountsTurn {
        match std::mem::replace(&mut self.step, CountsStep::Page) {
            CountsStep::Queue => {
                let (rows, more) = result.page()?;
                self.next = more.map(|_| MIGRATION_CONTINUE);
                self.buckets = rows.into_iter().map(|(key, _)| key).collect();
                self.next_bucket()
            }
            CountsStep::Record(bucket) => match decode_count(result.value()?)? {
                Some(record) if record.finished_at_ms.is_none() => {
                    let pages = CountPagesOperation::new(bucket, record);
                    Ok(Next::Call(IoCall::child(pages)))
                }
                _ => self.next_bucket(),
            },
            // A set that is moving or unreadable now is counted again on the next run.
            CountsStep::Page => {
                match result.child::<bool, MigrationError>()? {
                    Ok(false) => {}
                    Ok(true) => self.next = Some(MIGRATION_CONTINUE),
                    Err(error) => {
                        warn!(%error, "Failed to count the coverage of a bucket");
                        self.next = Some(MIGRATION_CONTINUE);
                    }
                }
                self.next_bucket()
            }
        }
    }
}

/// Counts up to [`COUNT_PAGES`] coverage pages of one bucket and stores the progress. Outputs
/// whether more remain.
#[derive(Debug, PartialEq)]
struct CountPagesOperation {
    bucket: String,
    record: CoverageCount,
    pages: usize,
    now_ms: u64,
    stored: bool,
}

impl CountPagesOperation {
    fn new(bucket: String, record: CoverageCount) -> Self {
        Self {
            bucket,
            record,
            pages: 0,
            now_ms: 0,
            stored: false,
        }
    }

    fn page(&mut self) -> Next<bool> {
        self.pages += 1;
        let input = CoverageInput {
            bucket: self.bucket.clone(),
            scope: CoverageScope::CurrentHeads,
            start_after: self.record.cursor.clone().map(Into::into),
            limit: COVERAGE_PAGE_LIMIT,
            auth_context: None,
        };
        Next::Call(IoCall::child(PolicyCoverageOperation::new(input)))
    }

    fn store(&mut self) -> Next<bool> {
        self.stored = true;
        let store = StoreCountOperation::new(&self.bucket, self.record.clone());
        Next::Call(IoCall::child(store))
    }
}

impl Operation for CountPagesOperation {
    type Output = bool;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<bool>, MigrationError> {
        Ok(self.page())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, MigrationError> {
        if self.stored {
            result.child::<(), MigrationError>()??;
            return Ok(Next::Done(self.record.finished_at_ms.is_none()));
        }
        match result.child::<CoverageReport, CoverageError>()? {
            Ok(report) => {
                let record = std::mem::take(&mut self.record);
                self.record = add_page(record, &report, self.now_ms);
            }
            // A deleted bucket leaves the queue with what was counted.
            Err(CoverageError::NoSuchBucket) => self.record.finished_at_ms = Some(self.now_ms),
            Err(error) => return Err(MigrationError::failed(error)),
        }
        match self.record.finished_at_ms.is_none() && self.pages < COUNT_PAGES {
            true => Ok(self.page()),
            false => Ok(self.store()),
        }
    }
}

#[derive(Debug, PartialEq)]
enum StoreStep {
    Current,
    Written,
    Left,
}

/// Stores the progress unless a newer count replaced it; a finished count leaves the queue.
#[derive(Debug, PartialEq)]
struct StoreCountOperation {
    key: Key,
    record: CoverageCount,
    step: StoreStep,
}

impl StoreCountOperation {
    fn new(bucket: &str, record: CoverageCount) -> Self {
        Self {
            key: count_key(bucket),
            record,
            step: StoreStep::Current,
        }
    }
}

impl Operation for StoreCountOperation {
    type Output = ();
    type Error = MigrationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, MigrationError> {
        let read = read_row(COVERAGE_COUNT_KEYSPACE, self.key.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MigrationError> {
        match self.step {
            StoreStep::Current => {
                let stored = decode_count(result.value()?)?;
                let started = self.record.started_at_ms;
                if stored.is_none_or(|stored| stored.started_at_ms != started) {
                    return Ok(Next::Done(()));
                }
                self.step = StoreStep::Written;
                let value = self.record.to_bytes()?.into();
                let row = (COVERAGE_COUNT_KEYSPACE.to_string(), self.key.clone(), value);
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            StoreStep::Written => {
                result.done()?;
                if self.record.finished_at_ms.is_none() {
                    return Ok(Next::Done(()));
                }
                self.step = StoreStep::Left;
                let row = (COUNT_QUEUE_KEYSPACE.to_string(), self.key.clone());
                Ok(Next::Call(IoCall::Delete(vec![row])))
            }
            StoreStep::Left => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
#[path = "count_tests.rs"]
mod tests;
