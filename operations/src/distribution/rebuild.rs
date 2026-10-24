//! Rebuilds the counters of one range into a new generation with bounded page transactions
//! over the inventory and upload inventory, then activates it or hands a split off.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    INVENTORY_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE, UPLOAD_INVENTORY_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, Cursor, RangeAccount, RangeCounters, Rebuild, account_key, generation_key,
};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, HeadEntry, Position, VersionEntry, range_interval,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::handoff::{
    HandoffError, account_row, activation_reads, decode_account, plan_activation,
};

/// Page transactions one run retries after conflicts before it pauses.
const MAX_CONFLICTS: usize = 16;

#[derive(Debug, Error, PartialEq)]
pub enum RebuildError {
    #[error(transparent)]
    Handoff(#[from] HandoffError),
    #[error("range {range_id} has an inconsistent account: {reason}")]
    InvalidAccount {
        range_id: Ulid,
        reason: &'static str,
    },
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("rebuild received unexpected event: {0:?}")]
    UnexpectedEvent(Box<Event>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RebuildOutcome {
    /// The new generation is active, or the split was handed off.
    Done,
    /// This child is complete and keeps counting until its sibling finishes the handoff.
    Waiting,
    /// The page or conflict budget of this run ran out; the durable cursor resumes it.
    Paused,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildConfig {
    pub set_id: Ulid,
    pub range_id: Ulid,
    pub page_rows: usize,
    pub page_bytes: usize,
    /// Page transactions this run commits at most.
    pub max_pages: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Walk {
    Versions,
    Uploads,
}

#[derive(Clone, Debug, PartialEq)]
enum State {
    Init,
    ReadAccount,
    BeginTxn,
    BeginRead,
    Probe,
    PageTxn,
    PageAccount,
    PageRead,
    PageCount,
    PageWrite,
    ActivateTxn,
    ActivateRead,
    ActivateWrite,
    ActivateDelete,
    Commit,
    Abort,
    Done,
}

/// What the open transaction commits: the next account rows and the deletes that go along.
#[derive(Clone, Debug, Default, PartialEq)]
struct Plan {
    writes: Vec<(String, Key, Value)>,
    deletes: Vec<(String, Key)>,
    account: Option<RangeAccount>,
    /// The outcome the commit finishes the run with; `None` continues the walk.
    finish: Option<RebuildOutcome>,
    /// The transaction commits one page of the walk.
    page: bool,
}

#[derive(Debug, PartialEq)]
pub struct RebuildOperation {
    config: RebuildConfig,
    state: State,
    account: Option<RangeAccount>,
    walk: Walk,
    probe_end: Option<Vec<u8>>,
    terminal: bool,
    txn_id: Option<TxnId>,
    plan: Plan,
    pages: usize,
    conflicts: usize,
    outcome: Option<Result<RebuildOutcome, RebuildError>>,
}

/// What one walked row contributes to the new generation.
fn contribution(walk: Walk, key: &[u8], value: &[u8]) -> Result<RangeCounters, ConversionError> {
    if walk == Walk::Uploads {
        return Ok(RangeCounters::of_upload(1));
    }
    Ok(match Position::decode(key)?.entry {
        Entry::Head => RangeCounters::of_head(&HeadEntry::from_bytes(value)?),
        Entry::Version { epoch, .. } => {
            RangeCounters::of_version(epoch, &VersionEntry::from_bytes(value)?)
        }
        Entry::Upload(_) => RangeCounters::default(),
    })
}

impl RebuildOperation {
    pub fn new(config: RebuildConfig) -> Self {
        Self {
            config,
            state: State::Init,
            account: None,
            walk: Walk::Versions,
            probe_end: None,
            terminal: false,
            txn_id: None,
            plan: Plan::default(),
            pages: 0,
            conflicts: 0,
            outcome: None,
        }
    }

    fn key(&self) -> Key {
        account_key(self.config.set_id, self.config.range_id).into()
    }

    fn finish(&mut self, outcome: Result<RebuildOutcome, RebuildError>) -> Effects {
        self.state = State::Done;
        self.outcome = Some(outcome);
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    fn fail(&mut self, error: impl Into<RebuildError>) -> Effects {
        self.finish(Err(error.into()))
    }

    fn read_account(&mut self) -> Effects {
        self.state = State::ReadAccount;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: RANGE_ACCOUNT_KEYSPACE.to_string(),
            key: self.key(),
            txn_id: None,
        })]
    }

    fn start_txn(&mut self, state: State) -> Effects {
        self.state = state;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// Decides the next step from the last committed account row.
    fn advance(&mut self) -> Effects {
        let Some(account) = self.account.clone() else {
            return self.read_account();
        };
        let range_id = self.config.range_id;
        let Some(rebuild) = account.rebuild.as_ref() else {
            return match account.charge {
                Charge::Active(_) => self.start_txn(State::BeginTxn),
                Charge::Retired { .. } => self.fail(HandoffError::RangeRetired(range_id)),
                Charge::Child { .. } => self.fail(RebuildError::InvalidAccount {
                    range_id,
                    reason: "a split child has no rebuild",
                }),
            };
        };
        if rebuild.is_done() {
            return self.start_txn(State::ActivateTxn);
        }
        if self.pages >= self.config.max_pages {
            return self.finish(Ok(RebuildOutcome::Paused));
        }
        self.walk = match rebuild.versions {
            Cursor::End => Walk::Uploads,
            _ => Walk::Versions,
        };
        self.state = State::Probe;
        let (start, end) = self.bounds(&account);
        smallvec![self.page_read(start, end, None)]
    }

    fn cursor<'a>(&self, account: &'a RangeAccount) -> Option<&'a Cursor> {
        let rebuild = account.rebuild.as_ref()?;
        Some(match self.walk {
            Walk::Versions => &rebuild.versions,
            Walk::Uploads => &rebuild.uploads,
        })
    }

    /// Where the walk continues and the exclusive end of the range.
    fn bounds(&self, account: &RangeAccount) -> (IterStart, Vec<u8>) {
        let (lower, upper) =
            range_interval(self.config.set_id, &account.lower, account.upper.as_deref());
        let start = match self.cursor(account) {
            Some(Cursor::At(position)) => IterStart::After(position.clone().into()),
            _ => IterStart::At(lower.into()),
        };
        (start, upper)
    }

    fn page_read(&self, start: IterStart, end: Vec<u8>, txn_id: Option<TxnId>) -> Effect {
        let end = match (&self.probe_end, txn_id, self.terminal) {
            (Some(probe_end), Some(_), false) => IterEnd::Through(probe_end.clone().into()),
            _ => IterEnd::Before(end.into()),
        };
        let key_space = match self.walk {
            Walk::Versions => INVENTORY_KEYSPACE,
            Walk::Uploads => UPLOAD_INVENTORY_KEYSPACE,
        };
        Effect::Storage(StorageEffect::IterRange {
            key_space: key_space.to_string(),
            start,
            end,
            limit: self.config.page_rows,
            max_bytes: self.config.page_bytes,
            txn_id,
        })
    }

    /// Counts one page read inside the transaction and plans the cursor it ends at.
    fn plan_page(
        &mut self,
        values: Vec<(Key, Value)>,
        truncated: Option<Key>,
    ) -> Result<Effects, RebuildError> {
        let (Some(txn_id), Some(mut account)) = (self.txn_id, self.account.clone()) else {
            return Err(RebuildError::InvalidAccount {
                range_id: self.config.range_id,
                reason: "a page needs an open transaction",
            });
        };
        let mut total = RangeCounters::default();
        for (key, value) in &values {
            total = total.merge(contribution(self.walk, key, value)?);
        }
        let cursor = match (truncated, self.terminal, self.probe_end.clone()) {
            (Some(last), _, _) => Cursor::At(last.to_vec()),
            (None, true, _) | (None, false, None) => Cursor::End,
            (None, false, Some(probe_end)) => Cursor::At(probe_end),
        };
        let rebuild = account
            .rebuild
            .as_mut()
            .ok_or(RebuildError::InvalidAccount {
                range_id: self.config.range_id,
                reason: "the rebuild ended during a page",
            })?;
        let generation = rebuild.generation;
        match self.walk {
            Walk::Versions => rebuild.versions = cursor,
            Walk::Uploads => rebuild.uploads = cursor,
        }
        let (set_id, range_id) = (self.config.set_id, self.config.range_id);
        self.plan = Plan {
            writes: vec![account_row(set_id, range_id, &account)?],
            account: Some(account),
            page: true,
            ..Plan::default()
        };
        if total.is_zero() {
            return Ok(self.write_plan(txn_id));
        }
        self.state = State::PageCount;
        let row = generation_key(set_id, range_id, generation);
        Ok(smallvec![Effect::Storage(StorageEffect::AddCounters {
            key_space: RANGE_COUNTER_KEYSPACE.to_string(),
            deltas: vec![(row.into(), total)],
            txn_id,
        })])
    }

    fn write_plan(&mut self, txn_id: TxnId) -> Effects {
        if self.plan.writes.is_empty() {
            return self.delete_plan(txn_id);
        }
        self.state = match self.state {
            State::ActivateRead => State::ActivateWrite,
            _ => State::PageWrite,
        };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: std::mem::take(&mut self.plan.writes),
            txn_id: Some(txn_id),
        })]
    }

    fn delete_plan(&mut self, txn_id: TxnId) -> Effects {
        if self.plan.deletes.is_empty() {
            self.state = State::Commit;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }
        self.state = State::ActivateDelete;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: std::mem::take(&mut self.plan.deletes),
            txn_id: Some(txn_id),
        })]
    }

    fn handle(&mut self, event: Event) -> Result<Effects, RebuildError> {
        let range_id = self.config.range_id;
        let set_id = self.config.set_id;
        match (&self.state, event) {
            (State::ReadAccount, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.account = Some(decode_account(value.as_ref(), range_id)?);
                Ok(self.advance())
            }
            (
                State::BeginTxn | State::PageTxn | State::ActivateTxn,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                let reads = match (&self.state, self.account.as_ref()) {
                    (State::ActivateTxn, Some(account)) => {
                        activation_reads(set_id, range_id, account)
                    }
                    _ => vec![(RANGE_ACCOUNT_KEYSPACE.to_string(), self.key())],
                };
                self.state = match self.state {
                    State::BeginTxn => State::BeginRead,
                    State::PageTxn => State::PageAccount,
                    _ => State::ActivateRead,
                };
                Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: Some(txn_id),
                })])
            }
            (State::BeginRead, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let current =
                    decode_account(values.first().and_then(|(_, v)| v.as_ref()), range_id)?;
                if Some(&current) != self.account.as_ref() {
                    return Ok(self.restart());
                }
                let Charge::Active(generation) = current.charge else {
                    return Ok(self.restart());
                };
                let next = RangeAccount {
                    rebuild: Some(Rebuild::new(generation.saturating_add(1))),
                    ..current
                };
                self.plan = Plan {
                    writes: vec![account_row(set_id, range_id, &next)?],
                    account: Some(next),
                    ..Plan::default()
                };
                Ok(self.write_plan(self.txn_id.unwrap_or_default()))
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
                Ok(self.start_txn(State::PageTxn))
            }
            (State::PageAccount, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let current =
                    decode_account(values.first().and_then(|(_, v)| v.as_ref()), range_id)?;
                if Some(&current) != self.account.as_ref() {
                    return Ok(self.restart());
                }
                self.state = State::PageRead;
                let (start, end) = self.bounds(&current);
                Ok(smallvec![self.page_read(start, end, self.txn_id)])
            }
            (
                State::PageRead,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.plan_page(values, next_start_after),
            (State::PageCount, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                Ok(self.write_plan(self.txn_id.unwrap_or_default()))
            }
            (State::ActivateRead, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let current =
                    decode_account(values.first().and_then(|(_, v)| v.as_ref()), range_id)?;
                if Some(&current) != self.account.as_ref() {
                    return Ok(self.restart());
                }
                let values = values.into_iter().map(|(_, value)| value).collect();
                match plan_activation(set_id, range_id, values)? {
                    Some(activation) => {
                        self.plan = Plan {
                            writes: activation.writes,
                            deletes: activation.deletes,
                            finish: Some(RebuildOutcome::Done),
                            ..Plan::default()
                        };
                        Ok(self.write_plan(self.txn_id.unwrap_or_default()))
                    }
                    None => Ok(self.finish(Ok(RebuildOutcome::Waiting))),
                }
            }
            (
                State::PageWrite | State::ActivateWrite,
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => Ok(self.delete_plan(self.txn_id.unwrap_or_default())),
            (State::ActivateDelete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                Ok(self.delete_plan(self.txn_id.unwrap_or_default()))
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                if let Some(outcome) = self.plan.finish.take() {
                    return Ok(self.finish(Ok(outcome)));
                }
                if self.plan.page {
                    self.pages += 1;
                }
                self.account = self.plan.account.take();
                Ok(self.advance())
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
                Ok(self.read_account())
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, event) => Err(RebuildError::UnexpectedEvent(Box::new(event))),
        }
    }

    /// Drops the open transaction, if any, and starts over from the committed account.
    fn restart(&mut self) -> Effects {
        self.conflicts += 1;
        self.plan = Plan::default();
        if self.conflicts > MAX_CONFLICTS {
            return self.finish(Ok(RebuildOutcome::Paused));
        }
        self.account = None;
        match self.txn_id {
            Some(txn_id) => {
                self.state = State::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => self.read_account(),
        }
    }
}

impl Operation for RebuildOperation {
    type Output = RebuildOutcome;
    type Error = RebuildError;

    fn start(&mut self) -> Effects {
        self.read_account()
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state == State::Done {
            return smallvec![];
        }
        match self.handle(event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Done
    }

    fn finalize(self) -> Result<RebuildOutcome, RebuildError> {
        self.outcome.unwrap_or(Err(RebuildError::InvalidAccount {
            range_id: self.config.range_id,
            reason: "the rebuild stopped before it finished",
        }))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

