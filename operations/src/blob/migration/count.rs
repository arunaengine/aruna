//! Starts and advances this node's saved count of a bucket's current versions against its rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cmp::Ordering;
use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent};
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{COUNT_QUEUE_KEYSPACE, COVERAGE_COUNT_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::placement::coverage::{CountRelay, CoverageCount};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::DistributionBinding;
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
    #[error("a coverage count run ordered after this one holds the bucket")]
    Superseded,
    #[error("this node did not sign the count relay: {0}")]
    Unsigned(String),
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

/// Replaces the bucket's saved count with one of `run`, or of a run it mints, and wakes the
/// migration task once it committed. Run ids order the starts: a saved count of a later run stays
/// and refuses the start, one of the same run answers as it is.
#[derive(Debug, PartialEq)]
pub struct StartCountOperation {
    bucket: String,
    record: CoverageCount,
    ids: IdMint,
    binding: Option<DistributionBinding>,
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
            binding: None,
            written: false,
        }
    }

    /// Starts only while the bucket carries `binding`'s set, member and incarnation, read in the
    /// write transaction; a bucket made again under the name answers `NoSuchBucket`.
    pub fn bound_to(mut self, binding: DistributionBinding) -> Self {
        self.binding = Some(binding);
        self
    }

    fn binds(&self, bucket: &[u8]) -> Result<bool, ConversionError> {
        let Some(member) = self.binding.as_ref() else {
            return Ok(true);
        };
        let info = BucketInfo::from_bytes(bucket)?;
        Ok(info.distribution.as_ref().is_some_and(|binding| {
            (binding.set_id, binding.member_id, binding.incarnation)
                == (member.set_id, member.member_id, member.incarnation)
        }))
    }
}

/// The run a distributed count reports: the one asked for, else the one every coordinator with a
/// count holds. Without an asked run, different runs leave no choice; the error names them.
pub fn current_run(wanted: Option<Ulid>, runs: &[Option<Ulid>]) -> Result<Option<Ulid>, Vec<Ulid>> {
    if wanted.is_some() {
        return Ok(wanted);
    }
    let seen: BTreeSet<Ulid> = runs.iter().flatten().copied().collect();
    match seen.len() {
        0 | 1 => Ok(seen.first().copied()),
        _ => Err(seen.into_iter().collect()),
    }
}

/// The positions of coordinator counts outside `current`; once a run is known, a coordinator
/// without a count missed it.
pub fn other_runs(current: Option<Ulid>, runs: &[Option<Ulid>]) -> Vec<usize> {
    let Some(current) = current else {
        return Vec::new();
    };
    (runs.iter().enumerate())
        .filter(|(_, run)| **run != Some(current))
        .map(|(index, _)| index)
        .collect()
}

/// Mints the run a distributed count starts on every coordinator, before the first one starts.
#[derive(Debug, Default, PartialEq)]
pub struct MintRunOperation {
    ids: IdMint,
    now_ms: u64,
}

impl Operation for MintRunOperation {
    type Output = Ulid;
    type Error = StartCountError;

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Ulid>, StartCountError> {
        Ok(Next::Done(self.ids.mint(self.now_ms)))
    }

    fn next(&mut self, _result: IoResult) -> Result<Next<Ulid>, StartCountError> {
        Err(StartCountError::Io(IoError::Unfinished))
    }
}

/// Signs this node's start of a count run for the other members of a distributed bucket.
#[derive(Debug, PartialEq)]
pub struct SignRelayOperation {
    relay: Option<CountRelay>,
}

impl SignRelayOperation {
    pub fn new(relay: CountRelay) -> Self {
        Self { relay: Some(relay) }
    }
}

impl Operation for SignRelayOperation {
    type Output = iroh::Signature;
    type Error = StartCountError;

    fn first(&mut self) -> Result<Next<iroh::Signature>, StartCountError> {
        let relay = (self.relay.take()).ok_or(StartCountError::Io(IoError::Unfinished))?;
        let sign = Effect::Net(NetEffect::SignCountRelay(Box::new(relay)));
        Ok(Next::Call(IoCall::Effect(Box::new(sign))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<iroh::Signature>, StartCountError> {
        match result.event()? {
            Event::Net(NetEvent::CountRelaySigned(signed)) => {
                Ok(Next::Done(signed.map_err(StartCountError::Unsigned)?))
            }
            other => Err(StartCountError::Unsigned(format!(
                "unexpected signing answer {}",
                other.label()
            ))),
        }
    }
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
        let key = count_key(&self.bucket);
        Ok(Next::Call(IoCall::Read(vec![
            (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
            (COVERAGE_COUNT_KEYSPACE.to_string(), key),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CoverageCount>, StartCountError> {
        if self.written {
            result.done()?;
            return Ok(Next::Then {
                output: self.record.clone(),
                calls: vec![wake_migration()],
            });
        }
        let [bucket, stored] = <[_; 2]>::try_from(result.values()?).map_err(|_| {
            StartCountError::Io(IoError::Unexpected {
                expected: "the bucket and its count",
                got: "another number of rows",
            })
        })?;
        let Some(bucket) = bucket else {
            return Err(StartCountError::NoSuchBucket);
        };
        if !self.binds(&bucket)? {
            return Err(StartCountError::NoSuchBucket);
        }
        if let Some(stored) = decode_count(stored)? {
            match stored.run.cmp(&self.record.run) {
                Ordering::Greater => return Err(StartCountError::Superseded),
                Ordering::Equal => return Ok(Next::Done(stored)),
                Ordering::Less => {}
            }
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

/// Reads the saved count of `bucket` on this node; when `run` is given, a count of another run
/// reads as none.
#[derive(Debug, PartialEq)]
pub struct ReadCountOperation {
    bucket: String,
    run: Option<Ulid>,
}

impl ReadCountOperation {
    pub fn new(bucket: &str, run: Option<Ulid>) -> Self {
        Self {
            bucket: bucket.to_string(),
            run,
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
        let count = decode_count(result.value()?)?;
        let wanted = |count: &CoverageCount| self.run.is_none_or(|run| count.run == Some(run));
        Ok(Next::Done(count.filter(wanted)))
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
                let current = (self.record.started_at_ms, self.record.run);
                if stored.is_none_or(|stored| (stored.started_at_ms, stored.run) != current) {
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
