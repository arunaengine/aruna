//! Rebuilds the counters of one range into a new generation with bounded page transactions
//! over the inventory and upload inventory, then activates it or hands a split off.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    INVENTORY_KEYSPACE, NODE_STATS_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE,
    UPLOAD_INVENTORY_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done, read_row, value};
use aruna_core::structs::storage::distribution::accounting::{
    Charge, CounterOverflow, Cursor, RangeAccount, RangeCounters, Rebuild, account_key,
    generation_key,
};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, HeadEntry, Position, VersionEntry, range_interval,
};
use aruna_core::structs::storage::usage::dirty_share_key;
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::metadata::call_results::page_rows;

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
    Overflow(#[from] CounterOverflow),
    #[error(transparent)]
    Io(#[from] IoError),
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
    /// The group whose replica share an activation may lower, marked in the activation's commit;
    /// `None` only for a run without page budget, which never activates.
    pub group_id: Option<GroupId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Walk {
    Versions,
    Uploads,
}

/// What the open transaction commits: the next account rows and the deletes that go along.
#[derive(Clone, Debug, Default, PartialEq)]
struct Plan {
    writes: Vec<(String, Key, Value)>,
    deletes: Vec<(String, Key)>,
    counters: Vec<(Key, RangeCounters)>,
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

fn walked(walk: Walk, rebuild: &Rebuild) -> &Cursor {
    match walk {
        Walk::Versions => &rebuild.versions,
        Walk::Uploads => &rebuild.uploads,
    }
}

/// One page read of `walk`, from its cursor in `account` up to `end`.
fn walk_read(config: &RebuildConfig, walk: Walk, account: &RangeAccount, end: IterEnd) -> IoCall {
    let (lower, _) = range_interval(config.set_id, &account.lower, account.upper.as_deref());
    let cursor = account.rebuild.as_ref().map(|rebuild| walked(walk, rebuild));
    let start = match cursor {
        Some(Cursor::At(position)) => IterStart::After(position.clone().into()),
        _ => IterStart::At(lower.into()),
    };
    let key_space = match walk {
        Walk::Versions => INVENTORY_KEYSPACE,
        Walk::Uploads => UPLOAD_INVENTORY_KEYSPACE,
    };
    let range = StorageEffect::IterRange {
        key_space: key_space.to_string(),
        start,
        end,
        limit: config.page_rows,
        max_bytes: config.page_bytes,
        txn_id: None,
    };
    IoCall::Effect(Box::new(Effect::Storage(range)))
}

fn range_end(config: &RebuildConfig, account: &RangeAccount) -> Vec<u8> {
    range_interval(config.set_id, &account.lower, account.upper.as_deref()).1
}

/// What one transaction of a rebuild does.
#[derive(Clone, Debug, PartialEq)]
enum StepKind {
    /// Records the rebuild of the next generation.
    Begin,
    /// Counts one page of `walk`, ending at what the probe saw.
    Page {
        walk: Walk,
        probe_end: Option<Vec<u8>>,
        terminal: bool,
    },
    /// Activates the finished generation or hands the split off.
    Activate,
}

/// How one transaction of a rebuild ended.
#[derive(Clone, Debug, PartialEq)]
enum StepEnd {
    /// The account moved on; `page` tells whether a page of the budget was spent.
    Advanced { account: RangeAccount, page: bool },
    Finished(RebuildOutcome),
    /// The committed account differs from the one this step was planned against.
    Stale,
}

/// What the pending call of a step answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StepState {
    Read,
    PageRows,
    Write,
    Delete,
    Count,
}

/// One rebuild transaction: re-reads the account, plans from it and commits the plan.
#[derive(Debug, PartialEq)]
struct RebuildStepOperation {
    config: RebuildConfig,
    account: RangeAccount,
    kind: StepKind,
    state: StepState,
    plan: Plan,
    end: Option<StepEnd>,
}

impl RebuildStepOperation {
    fn new(config: RebuildConfig, account: RangeAccount, kind: StepKind) -> Self {
        Self {
            config,
            account,
            kind,
            state: StepState::Read,
            plan: Plan::default(),
            end: None,
        }
    }

    fn invalid(&self, reason: &'static str) -> RebuildError {
        RebuildError::InvalidAccount {
            range_id: self.config.range_id,
            reason,
        }
    }

    fn call(&mut self, state: StepState, call: IoCall) -> Result<Next<StepEnd>, RebuildError> {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn write_plan(&mut self) -> Result<Next<StepEnd>, RebuildError> {
        let writes = std::mem::take(&mut self.plan.writes);
        self.call(StepState::Write, IoCall::Write(writes))
    }

    fn count_plan(&mut self) -> Result<Next<StepEnd>, RebuildError> {
        let deltas = std::mem::take(&mut self.plan.counters);
        if deltas.is_empty() {
            return self.finish();
        }
        let add = IoCall::AddCounters {
            space: RANGE_COUNTER_KEYSPACE.to_string(),
            deltas,
        };
        self.call(StepState::Count, add)
    }

    fn finish(&mut self) -> Result<Next<StepEnd>, RebuildError> {
        let end = self.end.take();
        Ok(Next::Done(
            end.ok_or(self.invalid("the rebuild step planned nothing"))?,
        ))
    }

    fn planned(&mut self, values: Vec<Option<Value>>) -> Result<Next<StepEnd>, RebuildError> {
        let (set_id, range_id) = (self.config.set_id, self.config.range_id);
        let current = decode_account(values.first().and_then(Option::as_ref), range_id)?;
        if current != self.account {
            return Ok(Next::Done(StepEnd::Stale));
        }
        match self.kind.clone() {
            StepKind::Begin => {
                let Charge::Active(generation) = current.charge else {
                    return Ok(Next::Done(StepEnd::Stale));
                };
                let next = RangeAccount {
                    rebuild: Some(Rebuild::new(generation.saturating_add(1))),
                    ..current
                };
                self.plan.writes = vec![account_row(set_id, range_id, &next)?];
                self.end = Some(StepEnd::Advanced {
                    account: next,
                    page: false,
                });
                self.write_plan()
            }
            StepKind::Page {
                walk,
                probe_end,
                terminal,
            } => {
                let end = match (probe_end, terminal) {
                    (Some(probe_end), false) => IterEnd::Through(probe_end.into()),
                    _ => IterEnd::Before(range_end(&self.config, &current).into()),
                };
                let read = walk_read(&self.config, walk, &current, end);
                self.call(StepState::PageRows, read)
            }
            StepKind::Activate => match plan_activation(set_id, range_id, values)? {
                Some(mut activation) => {
                    if let Some(group) = self.config.group_id {
                        let key = dirty_share_key(group).into();
                        let generation = Ulid::generate().to_bytes().to_vec().into();
                        let marker = (NODE_STATS_KEYSPACE.to_string(), key, generation);
                        activation.writes.push(marker);
                    }
                    self.plan = Plan {
                        writes: activation.writes,
                        deletes: activation.deletes,
                        counters: activation.counters,
                    };
                    self.end = Some(StepEnd::Finished(RebuildOutcome::Done));
                    self.write_plan()
                }
                None => Ok(Next::Done(StepEnd::Finished(RebuildOutcome::Waiting))),
            },
        }
    }

    /// Counts one page read inside the transaction and plans the cursor it ends at.
    fn plan_page(
        &mut self,
        values: Vec<(Key, Value)>,
        truncated: Option<Key>,
    ) -> Result<Next<StepEnd>, RebuildError> {
        let StepKind::Page {
            walk,
            probe_end,
            terminal,
        } = self.kind.clone()
        else {
            return Err(self.invalid("only a page step reads a page"));
        };
        let mut total = RangeCounters::default();
        for (key, value) in &values {
            total = total.merge(contribution(walk, key, value)?)?;
        }
        let cursor = match (truncated, terminal, probe_end) {
            (Some(last), _, _) => Cursor::At(last.to_vec()),
            (None, true, _) | (None, false, None) => Cursor::End,
            (None, false, Some(probe_end)) => Cursor::At(probe_end),
        };
        let mut account = self.account.clone();
        let rebuild = (account.rebuild.as_mut())
            .ok_or(self.invalid("the rebuild ended during a page"))?;
        let generation = rebuild.generation;
        match walk {
            Walk::Versions => rebuild.versions = cursor,
            Walk::Uploads => rebuild.uploads = cursor,
        }
        let (set_id, range_id) = (self.config.set_id, self.config.range_id);
        self.plan.writes = vec![account_row(set_id, range_id, &account)?];
        if !total.is_zero() {
            let row = generation_key(set_id, range_id, generation);
            self.plan.counters = vec![(row.into(), total)];
        }
        self.end = Some(StepEnd::Advanced {
            account,
            page: true,
        });
        self.write_plan()
    }
}

impl Operation for RebuildStepOperation {
    type Output = StepEnd;
    type Error = RebuildError;

    fn first(&mut self) -> Result<Next<StepEnd>, RebuildError> {
        let (set_id, range_id) = (self.config.set_id, self.config.range_id);
        let reads = match self.kind {
            StepKind::Activate => activation_reads(set_id, range_id, &self.account),
            _ => {
                let key = account_key(set_id, range_id).into();
                vec![(RANGE_ACCOUNT_KEYSPACE.to_string(), key)]
            }
        };
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StepEnd>, RebuildError> {
        match self.state {
            StepState::Read => self.planned(result.values()?),
            StepState::PageRows => {
                let (rows, truncated) = page_rows(result)?;
                self.plan_page(rows, truncated)
            }
            StepState::Write => {
                done(result)?;
                let deletes = std::mem::take(&mut self.plan.deletes);
                self.call(StepState::Delete, IoCall::Delete(deletes))
            }
            StepState::Delete => {
                done(result)?;
                self.count_plan()
            }
            StepState::Count => {
                done(result)?;
                self.finish()
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// What the pending call of a rebuild run answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Account,
    Probe(Walk),
    Step,
}

/// Rebuilds a range's counters: decides each step from the last committed account and runs it
/// as its own [`RebuildStepOperation`].
#[derive(Debug, PartialEq)]
pub struct RebuildOperation {
    config: RebuildConfig,
    state: State,
    account: Option<RangeAccount>,
    pages: usize,
    conflicts: usize,
}

impl RebuildOperation {
    pub fn new(config: RebuildConfig) -> Self {
        Self {
            config,
            state: State::Account,
            account: None,
            pages: 0,
            conflicts: 0,
        }
    }

    fn read_account(&mut self) -> Next<RebuildOutcome> {
        self.state = State::Account;
        let key = account_key(self.config.set_id, self.config.range_id);
        Next::Call(read_row(RANGE_ACCOUNT_KEYSPACE, key))
    }

    fn step(&mut self, account: RangeAccount, kind: StepKind) -> Next<RebuildOutcome> {
        self.state = State::Step;
        let step = RebuildStepOperation::new(self.config.clone(), account, kind);
        Next::Call(IoCall::child(step))
    }

    /// Decides the next step from the last committed account row.
    fn advance(&mut self, account: RangeAccount) -> Result<Next<RebuildOutcome>, RebuildError> {
        let range_id = self.config.range_id;
        self.account = Some(account.clone());
        let Some(rebuild) = account.rebuild.as_ref() else {
            return match account.charge {
                Charge::Active(_) => Ok(self.step(account, StepKind::Begin)),
                Charge::Retired { .. } => Err(HandoffError::RangeRetired(range_id).into()),
                Charge::Child { .. } => Err(RebuildError::InvalidAccount {
                    range_id,
                    reason: "a split child has no rebuild",
                }),
            };
        };
        if rebuild.is_done() {
            return Ok(self.step(account, StepKind::Activate));
        }
        if self.pages >= self.config.max_pages {
            return Ok(Next::Done(RebuildOutcome::Paused));
        }
        let walk = match rebuild.versions {
            Cursor::End => Walk::Uploads,
            _ => Walk::Versions,
        };
        self.state = State::Probe(walk);
        let end = IterEnd::Before(range_end(&self.config, &account).into());
        Ok(Next::Call(walk_read(&self.config, walk, &account, end)))
    }

    /// Starts over from the committed account, until the conflict budget runs out.
    fn restart(&mut self) -> Next<RebuildOutcome> {
        self.conflicts += 1;
        if self.conflicts > MAX_CONFLICTS {
            return Next::Done(RebuildOutcome::Paused);
        }
        self.account = None;
        self.read_account()
    }
}

impl Operation for RebuildOperation {
    type Output = RebuildOutcome;
    type Error = RebuildError;

    fn first(&mut self) -> Result<Next<RebuildOutcome>, RebuildError> {
        Ok(self.read_account())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RebuildOutcome>, RebuildError> {
        match self.state {
            State::Account => {
                let stored = value(result)?;
                let account = decode_account(stored.as_ref(), self.config.range_id)?;
                self.advance(account)
            }
            State::Probe(walk) => {
                let (rows, more) = page_rows(result)?;
                let kind = StepKind::Page {
                    walk,
                    probe_end: rows.last().map(|(key, _)| key.to_vec()),
                    terminal: more.is_none(),
                };
                let account = self.account.clone().ok_or(RebuildError::InvalidAccount {
                    range_id: self.config.range_id,
                    reason: "a page needs a read account",
                })?;
                Ok(self.step(account, kind))
            }
            State::Step => match result.child::<StepEnd, RebuildError>()? {
                Ok(StepEnd::Advanced { account, page }) => {
                    self.pages += usize::from(page);
                    self.advance(account)
                }
                Ok(StepEnd::Finished(outcome)) => Ok(Next::Done(outcome)),
                Ok(StepEnd::Stale) | Err(RebuildError::Io(IoError::Conflict)) => {
                    Ok(self.restart())
                }
                Err(error) => Err(error),
            },
        }
    }
}

/// The set, range and account of one range account row; `None` for a malformed row.
pub(super) fn decode(key: &[u8], value: &[u8]) -> Option<(Ulid, Ulid, RangeAccount)> {
    let set_id = Ulid::from_bytes(key.get(..16)?.try_into().ok()?);
    let range_id = Ulid::from_bytes(key.get(16..32)?.try_into().ok()?);
    let account = RangeAccount::from_bytes(value).ok()?;
    (key.len() == 32).then_some((set_id, range_id, account))
}

#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub struct BeginRebuildsError(String);

impl From<IoError> for BeginRebuildsError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

/// Begins a counter rebuild of every active range account without one, a page at a time, and
/// outputs how many began. A rebuild that does not begin is logged and skipped.
#[derive(Debug, PartialEq)]
pub struct BeginRebuildsOperation {
    walk: (usize, usize),
    pages: PrefixPages,
    /// Accounts of the page still to begin, the next one last.
    pending: Vec<(Ulid, Ulid)>,
    /// The account whose rebuild is running.
    current: Option<(Ulid, Ulid)>,
    begun: usize,
}

impl BeginRebuildsOperation {
    /// Pages of `account_page` accounts; each rebuild records walk pages of `walk` rows and bytes.
    pub(super) fn new(account_page: usize, walk: (usize, usize)) -> Self {
        Self {
            walk,
            pages: PrefixPages::new(RANGE_ACCOUNT_KEYSPACE, Vec::new(), account_page),
            pending: Vec::new(),
            current: None,
            begun: 0,
        }
    }

    fn read_page(&mut self, rows: Vec<(Key, Value)>) {
        // Reversed, so the accounts are popped in key order.
        for (key, value) in rows.into_iter().rev() {
            let Some((set_id, range_id, account)) = decode(&key, &value) else {
                continue;
            };
            if account.rebuild.is_none() && matches!(account.charge, Charge::Active(_)) {
                self.pending.push((set_id, range_id));
            }
        }
    }

    fn next_rebuild(&mut self) -> Next<usize> {
        let Some((set_id, range_id)) = self.pending.pop() else {
            return match self.pages.done() {
                true => Next::Done(self.begun),
                false => Next::Call(self.pages.call()),
            };
        };
        // No page budget: the run only records the new generation's rebuild.
        let config = RebuildConfig {
            set_id,
            range_id,
            page_rows: self.walk.0,
            page_bytes: self.walk.1,
            max_pages: 0,
            group_id: None,
        };
        self.current = Some((set_id, range_id));
        Next::Call(IoCall::child(RebuildOperation::new(config)))
    }
}

impl Operation for BeginRebuildsOperation {
    type Output = usize;
    type Error = BeginRebuildsError;

    fn first(&mut self) -> Result<Next<usize>, BeginRebuildsError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, BeginRebuildsError> {
        match self.current.take() {
            Some((set_id, range_id)) => {
                match result.child::<RebuildOutcome, RebuildError>()? {
                    Ok(_) => self.begun += 1,
                    Err(error) => {
                        warn!(%set_id, %range_id, %error, "A range counter rebuild did not begin")
                    }
                }
            }
            None => {
                let rows = self.pages.take(result)?;
                self.read_page(rows);
            }
        }
        Ok(self.next_rebuild())
    }
}

#[cfg(test)]
#[path = "rebuild_tests.rs"]
mod tests;
