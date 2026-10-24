//! Refills a range's key sample with a paged pass over the inventory head rows, resuming from
//! a durable cursor that is kept apart from the account row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{INVENTORY_KEYSPACE, KEY_SAMPLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, Cursor, RangeAccount, SampleSlot, account_key, refill_key, sample_key,
};
use aruna_core::structs::storage::distribution::inventory::{Entry, Position, range_interval};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

/// Page transactions one run retries after conflicts before it pauses.
const MAX_CONFLICTS: usize = 16;

#[derive(Debug, Error, PartialEq)]
pub enum RefillError {
    #[error("range {0} has no account row")]
    RangeMissing(Ulid),
    #[error("range {0} was split; its children are refilled instead")]
    RangeRetired(Ulid),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("the refill stopped before it finished")]
    Stopped,
    #[error("refill received unexpected event: {0:?}")]
    UnexpectedEvent(Box<Event>),
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum State {
    Init,
    ReadAccount,
    ReadCursor,
    Probe,
    PageTxn,
    PageCursor,
    PageRead,
    ReadSlots,
    Write,
    Commit,
    Abort,
    Done,
}

#[derive(Debug, PartialEq)]
pub struct RefillOperation {
    config: RefillConfig,
    state: State,
    bounds: Option<(Vec<u8>, Vec<u8>)>,
    cursor: Cursor,
    probe_end: Option<Vec<u8>>,
    terminal: bool,
    /// The best candidate of each slot this page found, and the cursor it ends at.
    candidates: BTreeMap<u16, SampleSlot>,
    next_cursor: Cursor,
    txn_id: Option<TxnId>,
    pages: usize,
    conflicts: usize,
    outcome: Option<Result<RefillOutcome, RefillError>>,
}

impl RefillOperation {
    pub fn new(config: RefillConfig) -> Self {
        Self {
            config,
            state: State::Init,
            bounds: None,
            cursor: Cursor::Start,
            probe_end: None,
            terminal: false,
            candidates: BTreeMap::new(),
            next_cursor: Cursor::Start,
            txn_id: None,
            pages: 0,
            conflicts: 0,
            outcome: None,
        }
    }

    fn cursor_key(&self) -> Key {
        refill_key(self.config.set_id, self.config.range_id).into()
    }

    fn finish(&mut self, outcome: Result<RefillOutcome, RefillError>) -> Effects {
        self.state = State::Done;
        self.outcome = Some(outcome);
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    fn read_cursor(&mut self, txn_id: Option<TxnId>) -> Effects {
        self.state = match txn_id {
            Some(_) => State::PageCursor,
            None => State::ReadCursor,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: KEY_SAMPLE_KEYSPACE.to_string(),
            key: self.cursor_key(),
            txn_id,
        })]
    }

    fn page_read(&self, txn_id: Option<TxnId>) -> Option<Effect> {
        let (lower, upper) = self.bounds.clone()?;
        let start = match &self.cursor {
            Cursor::Start => IterStart::At(lower.into()),
            Cursor::At(position) => IterStart::After(position.clone().into()),
            Cursor::End => return None,
        };
        let end = match (&self.probe_end, txn_id, self.terminal) {
            (Some(probe_end), Some(_), false) => IterEnd::Through(probe_end.clone().into()),
            _ => IterEnd::Before(upper.into()),
        };
        Some(Effect::Storage(StorageEffect::IterRange {
            key_space: INVENTORY_KEYSPACE.to_string(),
            start,
            end,
            limit: self.config.page_rows,
            max_bytes: self.config.page_bytes,
            txn_id,
        }))
    }

    fn probe(&mut self) -> Effects {
        if self.pages >= self.config.max_pages {
            return self.finish(Ok(RefillOutcome::Paused));
        }
        self.state = State::Probe;
        match self.page_read(None) {
            Some(effect) => smallvec![effect],
            None => self.finish(Ok(RefillOutcome::Done)),
        }
    }

    /// Keeps the best head key of each slot the page reaches and reads those slots.
    fn collect(
        &mut self,
        values: Vec<(Key, Value)>,
        truncated: Option<Key>,
    ) -> Result<Effects, RefillError> {
        self.candidates.clear();
        for (key, _) in &values {
            let position = Position::decode(key)?;
            if position.entry != Entry::Head {
                continue;
            }
            let (slot, entry) = SampleSlot::of_key(&position.key);
            if entry.displaces(self.candidates.get(&slot)) {
                self.candidates.insert(slot, entry);
            }
        }
        self.next_cursor = match (truncated, self.terminal, self.probe_end.clone()) {
            (Some(last), _, _) => Cursor::At(last.to_vec()),
            (None, false, Some(probe_end)) => Cursor::At(probe_end),
            (None, _, _) => Cursor::End,
        };
        let (set_id, range_id) = (self.config.set_id, self.config.range_id);
        let reads = (self.candidates.keys())
            .map(|slot| {
                (
                    KEY_SAMPLE_KEYSPACE.to_string(),
                    sample_key(set_id, range_id, *slot).into(),
                )
            })
            .collect();
        self.state = State::ReadSlots;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })])
    }

    fn write(&mut self, values: Vec<(Key, Option<Value>)>) -> Result<Effects, RefillError> {
        let mut writes: Vec<(String, Key, Value)> = Vec::new();
        let candidates = std::mem::take(&mut self.candidates);
        for ((key, stored), (_, entry)) in values.into_iter().zip(candidates) {
            let occupant = stored
                .map(|value| SampleSlot::from_bytes(&value))
                .transpose()?;
            if entry.displaces(occupant.as_ref()) {
                writes.push((
                    KEY_SAMPLE_KEYSPACE.to_string(),
                    key,
                    entry.to_bytes()?.into(),
                ));
            }
        }
        let cursor = postcard::to_allocvec(&self.next_cursor).map_err(ConversionError::from)?;
        writes.push((
            KEY_SAMPLE_KEYSPACE.to_string(),
            self.cursor_key(),
            cursor.into(),
        ));
        self.state = State::Write;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })])
    }

    fn handle(&mut self, event: Event) -> Result<Effects, RefillError> {
        let range_id = self.config.range_id;
        match (&self.state, event) {
            (State::ReadAccount, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let value = value.ok_or(RefillError::RangeMissing(range_id))?;
                let account = RangeAccount::from_bytes(&value)?;
                if matches!(account.charge, Charge::Retired { .. }) {
                    return Err(RefillError::RangeRetired(range_id));
                }
                let set_id = self.config.set_id;
                self.bounds = Some(range_interval(
                    set_id,
                    &account.lower,
                    account.upper.as_deref(),
                ));
                Ok(self.read_cursor(None))
            }
            (State::ReadCursor, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.cursor = match value {
                    Some(value) => postcard::from_bytes(&value).map_err(ConversionError::from)?,
                    None => Cursor::Start,
                };
                // A finished pass starts over: the sample is refilled because it ran low.
                if self.cursor == Cursor::End {
                    self.cursor = Cursor::Start;
                }
                Ok(self.probe())
            }
            (
                State::Probe,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                self.terminal = next_start_after.is_none();
                self.probe_end = values.last().map(|(key, _)| key.to_vec());
                self.state = State::PageTxn;
                Ok(smallvec![Effect::Storage(
                    StorageEffect::StartTransaction { read: false }
                )])
            }
            (State::PageTxn, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                Ok(self.read_cursor(Some(txn_id)))
            }
            (State::PageCursor, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let stored = match value {
                    Some(value) => postcard::from_bytes(&value).map_err(ConversionError::from)?,
                    None => Cursor::Start,
                };
                if stored != self.cursor && !(stored == Cursor::End && self.cursor == Cursor::Start)
                {
                    return Ok(self.restart());
                }
                self.state = State::PageRead;
                match self.page_read(self.txn_id) {
                    Some(effect) => Ok(smallvec![effect]),
                    None => Ok(self.finish(Ok(RefillOutcome::Done))),
                }
            }
            (
                State::PageRead,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.collect(values, next_start_after),
            (State::ReadSlots, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.write(values)
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.state = State::Commit;
                let txn_id = self.txn_id.unwrap_or_default();
                Ok(smallvec![Effect::Storage(
                    StorageEffect::CommitTransaction { txn_id }
                )])
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.pages += 1;
                self.cursor = std::mem::replace(&mut self.next_cursor, Cursor::Start);
                match self.cursor {
                    Cursor::End => Ok(self.finish(Ok(RefillOutcome::Done))),
                    _ => Ok(self.probe()),
                }
            }
            (
                State::Commit,
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }),
            ) => {
                self.txn_id = None;
                Ok(self.restart())
            }
            (State::Abort, Event::Storage(_)) => {
                self.txn_id = None;
                Ok(self.read_cursor(None))
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, event) => Err(RefillError::UnexpectedEvent(Box::new(event))),
        }
    }

    fn restart(&mut self) -> Effects {
        self.conflicts += 1;
        if self.conflicts > MAX_CONFLICTS {
            return self.finish(Ok(RefillOutcome::Paused));
        }
        match self.txn_id {
            Some(txn_id) => {
                self.state = State::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => self.read_cursor(None),
        }
    }
}

impl Operation for RefillOperation {
    type Output = RefillOutcome;
    type Error = RefillError;

    fn start(&mut self) -> Effects {
        self.state = State::ReadAccount;
        let key = account_key(self.config.set_id, self.config.range_id);
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: RANGE_ACCOUNT_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state == State::Done {
            return smallvec![];
        }
        match self.handle(event) {
            Ok(effects) => effects,
            Err(error) => self.finish(Err(error)),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Done
    }

    fn finalize(self) -> Result<RefillOutcome, RefillError> {
        self.outcome.unwrap_or(Err(RefillError::Stopped))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "refill_tests.rs"]
mod tests;
