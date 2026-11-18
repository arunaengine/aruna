//! Refills a range's key sample with a paged pass over the inventory head rows, resuming from
//! a durable cursor that is kept apart from the account row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    INVENTORY_KEYSPACE, KEY_SAMPLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, read_row, value};
use aruna_core::structs::storage::distribution::accounting::{
    Charge, Cursor, RangeAccount, RangeCounters, SAMPLE_SIZE, SampleEntry, account_key, refill_key,
    sample_count_key, sample_prefix,
};
use aruna_core::structs::storage::distribution::inventory::{Entry, Position, range_interval};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::call_results::page_rows;

/// Page transactions one run retries after conflicts before it pauses.
const MAX_CONFLICTS: usize = 16;
/// Entries concurrent writers may add beyond the sample size before a refill trims them.
const SAMPLE_SLACK: usize = 64;

#[derive(Debug, Error, PartialEq)]
pub enum RefillError {
    #[error("range {0} has no account row")]
    RangeMissing(Ulid),
    #[error("range {0} was split; its children are refilled instead")]
    RangeRetired(Ulid),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Io(#[from] IoError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefillOutcome {
    Done,
    /// The page or conflict budget ran out; the durable cursor resumes the pass.
    Paused,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefillConfig {
    pub set_id: Ulid,
    pub range_id: Ulid,
    pub page_rows: usize,
    pub page_bytes: usize,
    pub max_pages: usize,
}

fn cursor_key(config: &RefillConfig) -> Key {
    refill_key(config.set_id, config.range_id).into()
}

fn read_cursor(config: &RefillConfig) -> IoCall {
    read_row(KEY_SAMPLE_KEYSPACE, cursor_key(config))
}

fn decode_cursor(stored: Option<Value>) -> Result<Cursor, RefillError> {
    match stored {
        Some(stored) => Ok(postcard::from_bytes(&stored).map_err(ConversionError::from)?),
        None => Ok(Cursor::Start),
    }
}

/// The inventory rows one page walks.
#[derive(Clone, Debug, PartialEq)]
struct PageSpan {
    start: IterStart,
    upper: Vec<u8>,
    /// The last row the probe saw, where the page ends unless the probe reached the range's end.
    probe_end: Option<Vec<u8>>,
    terminal: bool,
}

impl PageSpan {
    fn read(&self, end: IterEnd, config: &RefillConfig) -> IoCall {
        let range = StorageEffect::IterRange {
            key_space: INVENTORY_KEYSPACE.to_string(),
            start: self.start.clone(),
            end,
            limit: config.page_rows,
            max_bytes: config.page_bytes,
            txn_id: None,
        };
        IoCall::Effect(Box::new(Effect::Storage(range)))
    }

    /// The read of the page inside its transaction, bounded by what the probe saw.
    fn page_read(&self, config: &RefillConfig) -> IoCall {
        let end = match (&self.probe_end, self.terminal) {
            (Some(probe_end), false) => IterEnd::Through(probe_end.clone().into()),
            _ => IterEnd::Before(self.upper.clone().into()),
        };
        self.read(end, config)
    }
}

/// How one page transaction ended.
#[derive(Clone, Debug, PartialEq)]
enum PageEnd {
    /// The page committed and the pass goes on at this cursor.
    Committed(Cursor),
    /// Another pass moved the durable cursor; nothing was changed.
    Stale,
}

/// What the pending call of a page reads or writes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PageStep {
    Cursor,
    Rows,
    Sample,
    Count,
    Write,
    Delete,
    AddCount,
}

/// Merges one page of head keys into the sample and moves the cursor, in one transaction.
#[derive(Debug, PartialEq)]
struct RefillPageOperation {
    config: RefillConfig,
    cursor: Cursor,
    span: PageSpan,
    step: PageStep,
    /// The head keys this page found, the sample it read, and the cursor it ends at.
    candidates: Vec<SampleEntry>,
    sample: Vec<SampleEntry>,
    deletes: Vec<(String, Key)>,
    delta: i128,
    /// The sample read stopped before its end, so the count is not corrected yet.
    truncated: bool,
    next_cursor: Cursor,
}

impl RefillPageOperation {
    fn new(config: RefillConfig, cursor: Cursor, span: PageSpan) -> Self {
        Self {
            config,
            cursor,
            span,
            step: PageStep::Cursor,
            candidates: Vec::new(),
            sample: Vec::new(),
            deletes: Vec::new(),
            delta: 0,
            truncated: false,
            next_cursor: Cursor::Start,
        }
    }

    fn call(&mut self, step: PageStep, call: IoCall) -> Result<Next<PageEnd>, RefillError> {
        self.step = step;
        Ok(Next::Call(call))
    }

    /// Takes the head keys of the page as candidates and reads the current sample.
    fn collect(
        &mut self,
        values: Vec<(Key, Value)>,
        truncated: Option<Key>,
    ) -> Result<Next<PageEnd>, RefillError> {
        self.candidates.clear();
        for (key, _) in &values {
            let position = Position::decode(key)?;
            if position.entry == Entry::Head {
                self.candidates.push(SampleEntry::of_key(&position.key));
            }
        }
        let probe_end = self.span.probe_end.clone();
        self.next_cursor = match (truncated, self.span.terminal, probe_end) {
            (Some(last), _, _) => Cursor::At(last.to_vec()),
            (None, false, Some(probe_end)) => Cursor::At(probe_end),
            (None, _, _) => Cursor::End,
        };
        let (set_id, range_id) = (self.config.set_id, self.config.range_id);
        let sample = IoCall::Page {
            space: KEY_SAMPLE_KEYSPACE.to_string(),
            prefix: sample_prefix(set_id, range_id).into(),
            after: None,
            limit: usize::from(SAMPLE_SIZE) + self.config.page_rows + SAMPLE_SLACK,
        };
        self.call(PageStep::Sample, sample)
    }

    /// Keeps the lowest entries of the sample and the candidates, and corrects the count row
    /// to the kept size.
    fn write(&mut self, stored: Option<Value>) -> Result<Next<PageEnd>, RefillError> {
        let (set_id, range_id) = (self.config.set_id, self.config.range_id);
        let current = std::mem::take(&mut self.sample);
        let mut kept: Vec<SampleEntry> = current.clone();
        kept.extend(std::mem::take(&mut self.candidates));
        kept.sort_by(|left, right| (left.priority, &left.key).cmp(&(right.priority, &right.key)));
        kept.dedup();
        kept.truncate(usize::from(SAMPLE_SIZE));
        let mut writes: Vec<(String, Key, Value)> = (kept.iter())
            .filter(|entry| !current.contains(entry))
            .map(|entry| {
                let key = entry.row_key(set_id, range_id);
                (
                    KEY_SAMPLE_KEYSPACE.to_string(),
                    key.into(),
                    Value::from(vec![]),
                )
            })
            .collect();
        self.deletes = (current.iter())
            .filter(|entry| !kept.contains(entry))
            .map(|entry| {
                (
                    KEY_SAMPLE_KEYSPACE.to_string(),
                    entry.row_key(set_id, range_id).into(),
                )
            })
            .collect();
        let counted = stored
            .map(|value| RangeCounters::from_bytes(&value))
            .transpose()?;
        let size = i128::try_from(kept.len()).unwrap_or(i128::MAX);
        self.delta = match self.truncated {
            true => 0,
            false => size - counted.unwrap_or_default().sampled,
        };
        let cursor = postcard::to_allocvec(&self.next_cursor).map_err(ConversionError::from)?;
        writes.push((
            KEY_SAMPLE_KEYSPACE.to_string(),
            cursor_key(&self.config),
            cursor.into(),
        ));
        self.call(PageStep::Write, IoCall::Write(writes))
    }

    fn committed(&mut self) -> Next<PageEnd> {
        Next::Done(PageEnd::Committed(std::mem::replace(
            &mut self.next_cursor,
            Cursor::Start,
        )))
    }

    fn count(&mut self) -> Result<Next<PageEnd>, RefillError> {
        if self.delta == 0 {
            return Ok(self.committed());
        }
        let delta = RangeCounters {
            sampled: std::mem::take(&mut self.delta),
            ..RangeCounters::default()
        };
        let key = sample_count_key(self.config.set_id, self.config.range_id);
        let add = IoCall::AddCounters {
            space: RANGE_COUNTER_KEYSPACE.to_string(),
            deltas: vec![(key.into(), delta)],
        };
        self.call(PageStep::AddCount, add)
    }
}

impl Operation for RefillPageOperation {
    type Output = PageEnd;
    type Error = RefillError;

    fn first(&mut self) -> Result<Next<PageEnd>, RefillError> {
        Ok(Next::Call(read_cursor(&self.config)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PageEnd>, RefillError> {
        match self.step {
            PageStep::Cursor => {
                let stored = decode_cursor(value(result)?)?;
                // A finished pass starts over, so its end marker still matches a fresh start.
                let restarted = stored == Cursor::End && self.cursor == Cursor::Start;
                if stored != self.cursor && !restarted {
                    return Ok(Next::Done(PageEnd::Stale));
                }
                let read = self.span.page_read(&self.config);
                self.call(PageStep::Rows, read)
            }
            PageStep::Rows => {
                let (rows, truncated) = page_rows(result)?;
                self.collect(rows, truncated)
            }
            PageStep::Sample => {
                let (rows, more) = result.page()?;
                // A truncated read trims what it saw and repeats the page before it counts.
                self.truncated = more.is_some();
                if self.truncated {
                    self.next_cursor = self.cursor.clone();
                }
                self.sample = (rows.iter())
                    .filter_map(|(key, _)| SampleEntry::decode(key))
                    .collect();
                let key = sample_count_key(self.config.set_id, self.config.range_id);
                self.call(PageStep::Count, read_row(RANGE_COUNTER_KEYSPACE, key))
            }
            PageStep::Count => self.write(value(result)?),
            PageStep::Write => {
                done(result)?;
                let deletes = std::mem::take(&mut self.deletes);
                self.call(PageStep::Delete, IoCall::Delete(deletes))
            }
            PageStep::Delete => {
                done(result)?;
                self.count()
            }
            PageStep::AddCount => {
                done(result)?;
                Ok(self.committed())
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// What the pending call of a refill run answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Account,
    Cursor,
    Probe,
    Page,
}

/// Walks the range's head rows page by page: each page is probed outside a transaction, then
/// merged by its own [`RefillPageOperation`].
#[derive(Debug, PartialEq)]
pub struct RefillOperation {
    config: RefillConfig,
    state: State,
    bounds: Option<(Vec<u8>, Vec<u8>)>,
    cursor: Cursor,
    start: Option<IterStart>,
    pages: usize,
    conflicts: usize,
}

impl RefillOperation {
    pub fn new(config: RefillConfig) -> Self {
        Self {
            config,
            state: State::Account,
            bounds: None,
            cursor: Cursor::Start,
            start: None,
            pages: 0,
            conflicts: 0,
        }
    }

    fn call(&mut self, state: State, call: IoCall) -> Result<Next<RefillOutcome>, RefillError> {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn span(&self, probe_end: Option<Vec<u8>>, terminal: bool) -> Option<PageSpan> {
        Some(PageSpan {
            start: self.start.clone()?,
            upper: self.bounds.as_ref()?.1.clone(),
            probe_end,
            terminal,
        })
    }

    fn probe(&mut self) -> Result<Next<RefillOutcome>, RefillError> {
        if self.pages >= self.config.max_pages {
            return Ok(Next::Done(RefillOutcome::Paused));
        }
        let lower = self.bounds.as_ref().map(|(lower, _)| lower.clone());
        self.start = match (&self.cursor, lower) {
            (Cursor::Start, Some(lower)) => Some(IterStart::At(lower.into())),
            (Cursor::At(position), Some(_)) => Some(IterStart::After(position.clone().into())),
            (Cursor::End, _) | (_, None) => return Ok(Next::Done(RefillOutcome::Done)),
        };
        let Some(span) = self.span(None, true) else {
            return Ok(Next::Done(RefillOutcome::Done));
        };
        let end = IterEnd::Before(span.upper.clone().into());
        let read = span.read(end, &self.config);
        self.call(State::Probe, read)
    }

    /// Starts over from the durable cursor, until the conflict budget runs out.
    fn restart(&mut self) -> Result<Next<RefillOutcome>, RefillError> {
        self.conflicts += 1;
        if self.conflicts > MAX_CONFLICTS {
            return Ok(Next::Done(RefillOutcome::Paused));
        }
        let read = read_cursor(&self.config);
        self.call(State::Cursor, read)
    }
}

impl Operation for RefillOperation {
    type Output = RefillOutcome;
    type Error = RefillError;

    fn first(&mut self) -> Result<Next<RefillOutcome>, RefillError> {
        let key = account_key(self.config.set_id, self.config.range_id);
        Ok(Next::Call(read_row(RANGE_ACCOUNT_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RefillOutcome>, RefillError> {
        let range_id = self.config.range_id;
        match self.state {
            State::Account => {
                let stored = value(result)?.ok_or(RefillError::RangeMissing(range_id))?;
                let account = RangeAccount::from_bytes(&stored)?;
                if matches!(account.charge, Charge::Retired { .. }) {
                    return Err(RefillError::RangeRetired(range_id));
                }
                let upper = account.upper.as_deref();
                let bounds = range_interval(self.config.set_id, &account.lower, upper);
                self.bounds = Some(bounds);
                let read = read_cursor(&self.config);
                self.call(State::Cursor, read)
            }
            State::Cursor => {
                self.cursor = decode_cursor(value(result)?)?;
                // A finished pass starts over: the sample is refilled because it ran low.
                if self.cursor == Cursor::End {
                    self.cursor = Cursor::Start;
                }
                self.probe()
            }
            State::Probe => {
                let (rows, more) = page_rows(result)?;
                let probe_end = rows.last().map(|(key, _)| key.to_vec());
                let Some(span) = self.span(probe_end, more.is_none()) else {
                    return Ok(Next::Done(RefillOutcome::Done));
                };
                let (config, cursor) = (self.config.clone(), self.cursor.clone());
                let page = RefillPageOperation::new(config, cursor, span);
                self.call(State::Page, IoCall::child(page))
            }
            State::Page => match result.child::<PageEnd, RefillError>()? {
                Ok(PageEnd::Committed(cursor)) => {
                    self.pages += 1;
                    self.cursor = cursor;
                    match self.cursor {
                        Cursor::End => Ok(Next::Done(RefillOutcome::Done)),
                        _ => self.probe(),
                    }
                }
                Ok(PageEnd::Stale) | Err(RefillError::Io(IoError::Conflict)) => self.restart(),
                Err(error) => Err(error),
            },
        }
    }
}

#[cfg(test)]
#[path = "refill_tests.rs"]
mod tests;
