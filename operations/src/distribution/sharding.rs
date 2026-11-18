//! Tends the key ranges this node holds: split accounting, rebuilds of split children until
//! handoff, and a coordinator's split of a range whose counters pass the bucket's thresholds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Every pass is bounded and resumes.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    DISTRIBUTION_KEYSPACE, KEY_SAMPLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, RangeAccount, RangeCounters, SAMPLE_SIZE, SampleEntry, generation_key, needs_refill,
    sample_prefix,
};
use aruna_core::structs::storage::distribution::{
    DistributionMember, DistributionSnapshot, DistributionView, KeyRange, Statement, StatementClaim,
};
use aruna_core::types::{Key, Value};
use tracing::{info, warn};
use ulid::Ulid;

use super::balance::{BalanceInputs, BalanceOperation, COOL_DOWN, balance_inputs};
use super::here::NodeRealm;
use super::imports::{ContinueImportsOperation, ImportError};
use super::inventory::{StagedCall, outside_answer};
use super::lifecycle::{LifecycleCursors, LifecyclePass, TendLifecycleOperation};
pub use super::rebuild::BeginRebuildsError;
use super::rebuild::{
    BeginRebuildsOperation, RebuildConfig, RebuildError, RebuildOperation, RebuildOutcome, decode,
};
use super::refill::{RefillConfig, RefillError, RefillOperation, RefillOutcome};
use super::refresh::{RefreshCursor, RefreshError, RefreshPass, RefreshRangesOperation};
use super::split::{SplitConfig, SplitError, SplitOperation};
use super::submit::{StateOperation, Stated};
use crate::driver::DriverContext;
use crate::s3::usage::{MarkDirtyError, MarkDirtyOperation};

/// How long a pass that found nothing to do waits before it looks again.
const MAINTAIN_AFTER: Duration = Duration::from_secs(60);
/// Account rows one page reads.
const ACCOUNT_PAGE: usize = 64;
/// Account pages one pass reads before it yields to the next timer run.
const MAX_PAGES: usize = 16;
const WALK_ROWS: usize = 512;
const WALK_BYTES: usize = 4 * 1024 * 1024;
/// Page transactions one rebuild or refill run commits.
const WALK_PAGES: usize = 8;

/// Where the passes go on; a restart begins again at the start.
static CURSORS: Mutex<Option<MaintainCursors>> = Mutex::new(None);

/// What range maintenance keeps between its passes.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MaintainCursors {
    /// The account row after which the next pass continues.
    accounts: Option<Vec<u8>>,
    refresh: Option<RefreshCursor>,
    lifecycle: LifecycleCursors,
    /// When this node last proposed a balancing transfer.
    balanced_at: Option<Instant>,
}

/// One finished pass: when the next should run, and where it goes on.
#[derive(Debug, PartialEq)]
pub(crate) struct MaintainPass {
    after: Duration,
    cursors: MaintainCursors,
}

impl MaintainPass {
    /// Keeps the cursors for the next pass and returns when it should run.
    pub(crate) fn finish(self) -> Duration {
        if let Ok(mut kept) = CURSORS.lock() {
            *kept = Some(self.cursors);
        }
        self.after
    }
}

/// One bounded pass over the range accounts and this node's other range work, resuming where
/// the last finished pass stopped.
pub(crate) fn maintain_ranges(context: &DriverContext) -> MaintainRangesOperation {
    let kept = CURSORS.lock().ok().and_then(|cursors| cursors.clone());
    MaintainRangesOperation::new(
        context.node_and_realm(),
        kept.unwrap_or_default(),
        balance_inputs(context),
    )
}

/// How long to wait after a pass the runner could not finish.
pub(crate) fn maintain_retry() -> Duration {
    MAINTAIN_AFTER
}

/// What tending one range left behind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Tended {
    Idle,
    /// Work remains; the next pass comes at once.
    Busy,
}

/// What the pending call of a maintenance pass answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintainStep {
    Lifecycle,
    Accounts,
    Record,
    Tend,
    Balance,
    Imports,
    Refresh,
}

/// See [`maintain_ranges`].
#[derive(Debug, PartialEq)]
pub(crate) struct MaintainRangesOperation {
    ids: NodeRealm,
    cursors: MaintainCursors,
    inputs: BalanceInputs,
    step: MaintainStep,
    pages: usize,
    more: bool,
    busy: bool,
    rows: VecDeque<(Vec<u8>, Value)>,
    records: BTreeMap<Ulid, Option<DistributionSnapshot>>,
    /// The account waiting for its set's record.
    current: Option<(Ulid, Ulid, RangeAccount)>,
    balancing: VecDeque<DistributionSnapshot>,
    lifecycle_poll: Option<Duration>,
    after: Duration,
    refresh_began: Option<Vec<u8>>,
}

type Maintaining = Result<Next<MaintainPass>, IoError>;

impl MaintainRangesOperation {
    fn new(ids: NodeRealm, cursors: MaintainCursors, inputs: BalanceInputs) -> Self {
        Self {
            ids,
            cursors,
            inputs,
            step: MaintainStep::Lifecycle,
            pages: 0,
            more: false,
            busy: false,
            rows: VecDeque::new(),
            records: BTreeMap::new(),
            current: None,
            balancing: VecDeque::new(),
            lifecycle_poll: None,
            after: MAINTAIN_AFTER,
            refresh_began: None,
        }
    }

    fn node(&self) -> Result<(NodeId, RealmId), IoError> {
        self.ids.ok_or(IoError::Unfinished)
    }

    fn read_accounts(&mut self) -> Maintaining {
        let page = IoCall::Page {
            space: RANGE_ACCOUNT_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.cursors.accounts.clone().map(Key::from),
            limit: ACCOUNT_PAGE,
        };
        self.step = MaintainStep::Accounts;
        Ok(Next::Call(StagedCall::Outside(page).into_call()))
    }

    fn accounts_read(&mut self, result: IoResult) -> Maintaining {
        match outside_answer(result) {
            Ok(IoResult::Page { rows, next }) => {
                self.more = next.is_some();
                self.rows = (rows.into_iter())
                    .map(|(key, value)| (key.to_vec(), value))
                    .collect();
                self.next_row()
            }
            Ok(other) => self.accounts_failed(&other.label()),
            Err(error) => self.accounts_failed(&error),
        }
    }

    fn accounts_failed(&mut self, error: &dyn std::fmt::Display) -> Maintaining {
        warn!(%error, "Range maintenance could not read the range accounts");
        self.after = MAINTAIN_AFTER;
        self.continue_imports()
    }

    /// Tends the next account of the page, reading its set's record once.
    fn next_row(&mut self) -> Maintaining {
        while let Some((key, value)) = self.rows.pop_front() {
            self.cursors.accounts = Some(key.clone());
            let Some((set_id, range_id, account)) = decode(&key, &value) else {
                continue;
            };
            self.current = Some((set_id, range_id, account));
            if !self.records.contains_key(&set_id) {
                let read = read_row(DISTRIBUTION_KEYSPACE, set_id.to_bytes().to_vec());
                self.step = MaintainStep::Record;
                return Ok(Next::Call(StagedCall::Outside(read).into_call()));
            }
            if let Some(tend) = self.tend_current()? {
                return Ok(tend);
            }
        }
        self.pages += 1;
        if !self.more {
            self.cursors.accounts = None;
        } else if self.pages < MAX_PAGES {
            return self.read_accounts();
        }
        self.accounts_done()
    }

    /// The call that tends the current account, when its set has a usable record.
    fn tend_current(&mut self) -> Result<Option<Next<MaintainPass>>, IoError> {
        let (set_id, range_id, account) = self.current.take().ok_or(IoError::Unfinished)?;
        let Some(Some(snapshot)) = self.records.get(&set_id) else {
            return Ok(None);
        };
        let (local, realm_id) = self.node()?;
        let tend = TendRangeOperation::new(realm_id, local, snapshot.clone(), range_id, account);
        self.step = MaintainStep::Tend;
        Ok(Some(Next::Call(IoCall::child(tend))))
    }

    fn accounts_done(&mut self) -> Maintaining {
        self.after = match self.busy || self.cursors.accounts.is_some() {
            true => Duration::ZERO,
            false => MAINTAIN_AFTER,
        };
        self.balancing = self.records.values().flatten().cloned().collect();
        self.next_balance()
    }

    fn next_balance(&mut self) -> Maintaining {
        let cooling = (self.cursors.balanced_at).is_some_and(|at| at.elapsed() < COOL_DOWN);
        match self.balancing.pop_front() {
            Some(snapshot) if !cooling => {
                let (local, realm_id) = self.node()?;
                let ids = (local, realm_id);
                let balance = BalanceOperation::new(ids, snapshot, self.inputs.clone());
                self.step = MaintainStep::Balance;
                Ok(Next::Call(IoCall::child(balance)))
            }
            _ => self.continue_imports(),
        }
    }

    /// Imports handed over with a range start once this node coordinates it.
    fn continue_imports(&mut self) -> Maintaining {
        let (local, _) = self.node()?;
        self.step = MaintainStep::Imports;
        let imports = ContinueImportsOperation::new(local);
        Ok(Next::Call(IoCall::child(imports)))
    }

    /// Coordinators walk their ranges for the periodic source refresh, a few pages per pass.
    fn refresh(&mut self) -> Maintaining {
        let (local, realm_id) = self.node()?;
        let cursor = self.cursors.refresh.take().unwrap_or_default();
        self.refresh_began = cursor.record.clone();
        let pass = RefreshRangesOperation::new(realm_id, local, SystemTime::now(), cursor);
        self.step = MaintainStep::Refresh;
        Ok(Next::Call(IoCall::child(pass)))
    }

    fn finished(&mut self) -> Maintaining {
        let poll = self.lifecycle_poll;
        Ok(Next::Done(MaintainPass {
            after: poll.map_or(self.after, |poll| poll.min(self.after)),
            cursors: std::mem::take(&mut self.cursors),
        }))
    }
}

impl Operation for MaintainRangesOperation {
    type Output = MaintainPass;
    type Error = IoError;

    fn first(&mut self) -> Maintaining {
        let Some((local, realm_id)) = self.ids else {
            return self.finished();
        };
        let cursors = std::mem::take(&mut self.cursors.lifecycle);
        let tend = TendLifecycleOperation::new(realm_id, local, cursors);
        Ok(Next::Call(IoCall::child(tend)))
    }

    fn next(&mut self, result: IoResult) -> Maintaining {
        match self.step {
            MaintainStep::Lifecycle => {
                let pass = result.child::<LifecyclePass, IoError>()??;
                self.lifecycle_poll = pass.poll;
                self.cursors.lifecycle = pass.cursors;
                self.read_accounts()
            }
            MaintainStep::Accounts => self.accounts_read(result),
            MaintainStep::Record => {
                let (set_id, ..) = self.current.as_ref().ok_or(IoError::Unfinished)?;
                let set_id = *set_id;
                let stored = (outside_answer(result).and_then(|read| read.value())).ok();
                self.records.insert(set_id, usable_record(stored.flatten()));
                match self.tend_current()? {
                    Some(tend) => Ok(tend),
                    None => self.next_row(),
                }
            }
            MaintainStep::Tend => {
                if result.child::<Tended, IoError>()?? == Tended::Busy {
                    self.busy = true;
                }
                self.next_row()
            }
            MaintainStep::Balance => {
                if result.child::<bool, IoError>()?? {
                    self.cursors.balanced_at = Some(Instant::now());
                }
                self.next_balance()
            }
            MaintainStep::Imports => {
                if let Err(error) = result.child::<(), ImportError>()? {
                    warn!(%error, "Handed over imports could not start");
                }
                self.refresh()
            }
            MaintainStep::Refresh => {
                let (busy, cursor) = match result.child::<RefreshPass, RefreshError>()? {
                    Ok(pass) => (pass.busy, pass.cursor),
                    Err(error) => {
                        warn!(%error, "The periodic source refresh could not run");
                        // A runner failure names no cursor; the next pass resumes where this
                        // one began.
                        let record = error.record.or(self.refresh_began.take());
                        let sweep = None;
                        (false, RefreshCursor { record, sweep })
                    }
                };
                if busy {
                    self.after = Duration::ZERO;
                }
                self.cursors.refresh = Some(cursor);
                self.finished()
            }
        }
    }
}

/// The set's head from its stored record; a conflicted set takes no configuration changes,
/// splits included.
fn usable_record(stored: Option<Value>) -> Option<DistributionSnapshot> {
    let view = DistributionView::from_bytes(&stored?).ok()?;
    (!view.conflicted()).then_some(view.head.snapshot)
}

/// Begins a counter rebuild of every active range account without one, so range counters are
/// confirmed again after a node-wide usage rebuild started. The range maintenance runs the
/// rebuilds; until they activate, the counters report incomplete.
pub fn begin_range_rebuilds() -> BeginRebuildsOperation {
    BeginRebuildsOperation::new(ACCOUNT_PAGE, (WALK_ROWS, WALK_BYTES))
}

/// What the pending call of tending one range answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TendStep {
    Split,
    Rebuild,
    Refill,
    Republish(Tended),
    Counters,
    Sample,
    State,
}

/// Tends one range account this node holds, with the record of its set.
#[derive(Debug, PartialEq)]
pub(super) struct TendRangeOperation {
    realm_id: RealmId,
    local: NodeId,
    snapshot: DistributionSnapshot,
    range_id: Ulid,
    account: RangeAccount,
    step: TendStep,
    objects: i128,
    split_key: Option<String>,
}

type Tending = Result<Next<Tended>, IoError>;

impl TendRangeOperation {
    pub(super) fn new(
        realm_id: RealmId,
        local: NodeId,
        snapshot: DistributionSnapshot,
        range_id: Ulid,
        account: RangeAccount,
    ) -> Self {
        Self {
            realm_id,
            local,
            snapshot,
            range_id,
            account,
            step: TendStep::Rebuild,
            objects: 0,
            split_key: None,
        }
    }

    fn set_id(&self) -> Ulid {
        self.snapshot.set_id()
    }

    /// A finished accounting transition moves the charged rows a replica share reads.
    fn republish(&mut self, then: Tended) -> Tending {
        let group = self.snapshot.config.owner_group;
        let mark = MarkDirtyOperation::new([group], Ulid::generate());
        self.step = TendStep::Republish(then);
        Ok(Next::Call(IoCall::child(mark)))
    }

    fn rebuild(&mut self) -> Tending {
        let config = RebuildConfig {
            set_id: self.set_id(),
            range_id: self.range_id,
            page_rows: WALK_ROWS,
            page_bytes: WALK_BYTES,
            max_pages: WALK_PAGES,
            group_id: Some(self.snapshot.config.owner_group),
        };
        self.step = TendStep::Rebuild;
        Ok(Next::Call(IoCall::child(RebuildOperation::new(config))))
    }

    fn refill(&mut self) -> Tending {
        let config = RefillConfig {
            set_id: self.set_id(),
            range_id: self.range_id,
            page_rows: WALK_ROWS,
            page_bytes: WALK_BYTES,
            max_pages: WALK_PAGES,
        };
        self.step = TendStep::Refill;
        Ok(Next::Call(IoCall::child(RefillOperation::new(config))))
    }

    /// The range this node coordinates and may split now, with the member it signs as.
    fn splittable(&self) -> Option<(&DistributionMember, &KeyRange)> {
        let snapshot = &self.snapshot;
        let settings = &snapshot.config.settings;
        if settings.split_objects.is_none() && settings.split_bytes.is_none() {
            return None;
        }
        let member =
            (snapshot.config.members.iter()).find(|member| member.node_id == self.local)?;
        let range =
            (snapshot.config.ranges.iter()).find(|range| range.range_id == self.range_id)?;
        let moving =
            (snapshot.lifecycle.transfers.iter()).any(|open| open.range_id == range.range_id);
        let filling = (snapshot.lifecycle.fills.iter()).any(|fill| fill.range_id == range.range_id);
        let open =
            snapshot.serving() && snapshot.lifecycle.deletion.is_none() && !moving && !filling;
        (open && range.coordinator == member.member_id).then_some((member, range))
    }

    /// Reads the range's counters when it may split at all.
    fn check_thresholds(&mut self) -> Tending {
        let Some((_, range)) = self.splittable() else {
            return Ok(Next::Done(Tended::Idle));
        };
        let Charge::Active(generation) = self.account.charge else {
            return Ok(Next::Done(Tended::Idle));
        };
        let row = generation_key(self.set_id(), range.range_id, generation);
        let read = read_row(RANGE_COUNTER_KEYSPACE, row);
        self.step = TendStep::Counters;
        Ok(Next::Call(StagedCall::Outside(read).into_call()))
    }

    /// Goes on to the key sample once the counters pass a threshold.
    fn counted(&mut self, counters: RangeCounters) -> Tending {
        let settings = &self.snapshot.config.settings;
        let above =
            |limit: Option<u64>, value: i128| limit.is_some_and(|limit| value > i128::from(limit));
        if !above(settings.split_objects, counters.objects)
            && !above(settings.split_bytes, counters.current_bytes)
        {
            return Ok(Next::Done(Tended::Idle));
        }
        self.objects = counters.objects;
        let sample = RangeSampleOperation::new(self.set_id(), self.range_id);
        self.step = TendStep::Sample;
        Ok(Next::Call(IoCall::child(sample)))
    }

    /// States the split at a key from the sample; a thin sample is refilled first.
    fn sampled(&mut self, keys: Option<Vec<String>>) -> Tending {
        let keys = match keys {
            Some(keys) if !needs_refill(keys.len(), self.objects) => keys,
            _ => return self.refill(),
        };
        let Some((member, range)) = self.splittable() else {
            return Ok(Next::Done(Tended::Idle));
        };
        let Some(split_key) = split_point(&keys, range) else {
            return Ok(Next::Done(Tended::Idle));
        };
        let snapshot = &self.snapshot;
        let set_id = snapshot.set_id();
        let statement = Statement::split(
            self.realm_id,
            set_id,
            member,
            snapshot.number,
            range,
            &split_key,
        );
        let claim = StatementClaim {
            signer: self.local,
            statement,
        };
        let ids = Some((self.local, self.realm_id));
        let state = StateOperation::new(ids, snapshot.root.publisher, claim);
        self.split_key = Some(split_key);
        self.step = TendStep::State;
        Ok(Next::Call(IoCall::child(state)))
    }

    fn stated(&mut self, stated: Stated) -> Next<Tended> {
        let (set_id, range_id) = (self.set_id(), self.range_id);
        let split_key = self.split_key.take().unwrap_or_default();
        match stated {
            Stated::Taken => {
                info!(%set_id, %range_id, %split_key, "Split a distributed bucket range");
            }
            Stated::Refused(refusal) => {
                warn!(%set_id, %range_id, %refusal, "The publisher did not take a range split");
            }
            Stated::Unsigned(reason) => warn!(%reason, "A range split could not be signed"),
        }
        Next::Done(Tended::Idle)
    }
}

impl Operation for TendRangeOperation {
    type Output = Tended;
    type Error = IoError;

    fn first(&mut self) -> Tending {
        match (&self.account.charge, self.account.rebuild.is_some()) {
            (Charge::Retired { .. }, _) => Ok(Next::Done(Tended::Idle)),
            (Charge::Active(_), false) => match self.snapshot.retired_range(self.range_id) {
                // Splits this node's accounting the way the record split the range.
                Some(retired) => {
                    let split = SplitOperation::new(SplitConfig {
                        set_id: self.snapshot.set_id(),
                        range_id: self.range_id,
                        split_key: retired.split_key.clone(),
                        left: retired.lower_id,
                        right: retired.upper_id,
                    });
                    self.step = TendStep::Split;
                    Ok(Next::Call(IoCall::child(split)))
                }
                None => self.check_thresholds(),
            },
            (Charge::Active(_) | Charge::Child { .. }, _) => self.rebuild(),
        }
    }

    fn next(&mut self, result: IoResult) -> Tending {
        let (set_id, range_id) = (self.set_id(), self.range_id);
        match self.step {
            TendStep::Split => match result.child::<(), SplitError>()? {
                Ok(()) => self.republish(Tended::Busy),
                // Concurrent writers left more sample entries than one split reads.
                Err(SplitError::Refused(_, "the key sample needs a refill")) => self.refill(),
                Err(error) => {
                    warn!(%set_id, %range_id, %error, "A range split could not be applied");
                    Ok(Next::Done(Tended::Idle))
                }
            },
            TendStep::Rebuild => match result.child::<RebuildOutcome, RebuildError>()? {
                Ok(RebuildOutcome::Paused) => Ok(Next::Done(Tended::Busy)),
                Ok(RebuildOutcome::Done) => self.republish(Tended::Idle),
                Ok(RebuildOutcome::Waiting) => Ok(Next::Done(Tended::Idle)),
                Err(error) => {
                    warn!(%set_id, %range_id, %error, "A range rebuild failed");
                    Ok(Next::Done(Tended::Idle))
                }
            },
            TendStep::Refill => match result.child::<RefillOutcome, RefillError>()? {
                Ok(_) => Ok(Next::Done(Tended::Busy)),
                Err(error) => {
                    warn!(%set_id, %range_id, %error, "A key sample refill failed");
                    Ok(Next::Done(Tended::Idle))
                }
            },
            TendStep::Republish(then) => {
                if let Err(error) = result.child::<(), MarkDirtyError>()? {
                    warn!(%set_id, %error, "The replica share was not marked for a publish");
                }
                Ok(Next::Done(then))
            }
            TendStep::Counters => {
                let stored = outside_answer(result).and_then(|read| read.value());
                let counters = (stored.ok().flatten())
                    .and_then(|value| RangeCounters::from_bytes(&value).ok())
                    .unwrap_or_default();
                self.counted(counters)
            }
            TendStep::Sample => {
                let keys = result.child::<Option<Vec<String>>, IoError>()??;
                self.sampled(keys)
            }
            TendStep::State => Ok(self.stated(result.child::<Stated, IoError>()??)),
        }
    }
}

/// The keys of this node's sample of a range, in key order; `None` when it cannot be read.
#[derive(Debug, PartialEq)]
pub(crate) struct RangeSampleOperation {
    set_id: Ulid,
    range_id: Ulid,
}

impl RangeSampleOperation {
    pub(crate) fn new(set_id: Ulid, range_id: Ulid) -> Self {
        Self { set_id, range_id }
    }
}

impl Operation for RangeSampleOperation {
    type Output = Option<Vec<String>>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Option<Vec<String>>>, IoError> {
        let page = IoCall::Page {
            space: KEY_SAMPLE_KEYSPACE.to_string(),
            prefix: sample_prefix(self.set_id, self.range_id).into(),
            after: None,
            limit: usize::from(SAMPLE_SIZE) * 2,
        };
        Ok(Next::Call(StagedCall::Outside(page).into_call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Vec<String>>>, IoError> {
        let Ok(IoResult::Page { rows, .. }) = outside_answer(result) else {
            return Ok(Next::Done(None));
        };
        let mut keys: Vec<String> = (rows.iter())
            .filter_map(|(key, _)| SampleEntry::decode(key).map(|entry| entry.key))
            .collect();
        keys.sort();
        Ok(Next::Done(Some(keys)))
    }
}

/// A split key near the sample's median at a folder boundary: the deepest folder of the
/// median key that leaves sampled keys on both sides inside the range, else the median.
pub(crate) fn split_point(keys: &[String], range: &KeyRange) -> Option<String> {
    let median = keys.get(keys.len() / 2)?;
    let folders = median
        .match_indices('/')
        .map(|(index, _)| &median[..=index]);
    let candidates: Vec<&str> = folders.rev().chain([median.as_str()]).collect();
    candidates
        .into_iter()
        .find(|candidate| {
            range.lower.as_str() < *candidate
                && range
                    .upper
                    .as_deref()
                    .is_none_or(|upper| *candidate < upper)
                && keys.iter().any(|key| key.as_str() < *candidate)
                && keys.iter().any(|key| key.as_str() >= *candidate)
        })
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::{begin_range_rebuilds, split_point};
    use crate::distribution::rebuild::{RebuildConfig, RebuildOperation, RebuildOutcome};
    use crate::distribution::test_store::{
        account, apply, context, counters, exact, install, open, write,
    };
    use crate::driver::drive;
    use aruna_core::structs::storage::distribution::KeyRange;
    use aruna_core::structs::storage::distribution::accounting::{Charge, generation_key};
    use ulid::Ulid;

    fn range(lower: &str, upper: Option<&str>) -> KeyRange {
        KeyRange {
            range_id: Ulid::from_bytes([1; 16]),
            lower: lower.to_string(),
            upper: upper.map(str::to_string),
            lineage: None,
            coordinator: Ulid::from_bytes([2; 16]),
            epoch: 1,
            replicas: Vec::new(),
        }
    }

    fn keys(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn splits_at_folders() {
        let sample = keys(&["a/1", "b/x/1", "b/x/2", "c/1", "d/1"]);
        assert_eq!(
            split_point(&sample, &range("", None)).as_deref(),
            Some("b/x/")
        );
        let inside = keys(&["b/x/1", "b/x/2", "b/x/3"]);
        assert_eq!(
            split_point(&inside, &range("b/x/", None)).as_deref(),
            Some("b/x/2"),
            "a folder at the lower bound leaves the left child empty"
        );
    }

    #[tokio::test]
    async fn rebuilds_begin_once() {
        let (_dir, handle) = open();
        let set = Ulid::from_bytes([6; 16]);
        install(
            &handle,
            set,
            &[super::super::test_store::range(1, "", None)],
        )
        .await;
        assert!(apply(&handle, write(set, "a", 1, 5)).await.unwrap());
        let context = context(&handle);
        assert_eq!(drive(begin_range_rebuilds(), &context).await.unwrap(), 1);
        let range_id = Ulid::from_bytes([1; 16]);
        let begun = account(&handle, set, range_id).await.unwrap();
        assert!(begun.rebuild.is_some() && !begun.is_complete());
        // A running rebuild is left alone.
        assert_eq!(drive(begin_range_rebuilds(), &context).await.unwrap(), 0);
        let config = RebuildConfig {
            set_id: set,
            range_id,
            page_rows: 8,
            page_bytes: 1 << 20,
            max_pages: 100,
            group_id: None,
        };
        let outcome = drive(RebuildOperation::new(config), &context).await;
        assert_eq!(outcome.unwrap(), RebuildOutcome::Done);
        let done = account(&handle, set, range_id).await.unwrap();
        let Charge::Active(generation) = done.charge else {
            panic!("the range stays active");
        };
        let row = counters(&handle, generation_key(set, range_id, generation)).await;
        assert_eq!(row, exact(1, 5));
    }

    #[tokio::test]
    async fn split_marks_group() {
        // A split of this node's accounting moves the charged rows, so the group's replica
        // share is published again.
        use aruna_core::keyspaces::NODE_STATS_KEYSPACE;
        use aruna_core::structs::storage::distribution::split::RetiredRange;
        use aruna_core::structs::storage::usage::dirty_group_key;
        let (_dir, handle) = open();
        let (snapshot, _) = crate::distribution::test_record::record(true);
        let set = snapshot.set_id();
        let range_id = Ulid::from_bytes([1; 16]);
        install(
            &handle,
            set,
            &[super::super::test_store::range(1, "", None)],
        )
        .await;
        for (key, seed) in [("a", 1), ("x", 2)] {
            assert!(apply(&handle, write(set, key, seed, 5)).await.unwrap());
        }
        let context = std::sync::Arc::new(context(&handle));
        let account = account(&handle, set, range_id).await.unwrap();
        let mut snapshot = snapshot;
        snapshot.retired.push(RetiredRange {
            range_id,
            lineage: None,
            split_key: "m".to_string(),
            lower_id: Ulid::from_bytes([2; 16]),
            upper_id: Ulid::from_bytes([3; 16]),
        });
        let tend = super::TendRangeOperation::new(
            crate::distribution::test_record::realm(),
            crate::distribution::test_record::secret(1).public(),
            snapshot.clone(),
            range_id,
            account,
        );
        assert_eq!(drive(tend, &context).await, Ok(super::Tended::Busy));
        let marker = super::super::test_store::get(
            &handle,
            NODE_STATS_KEYSPACE,
            dirty_group_key(snapshot.config.owner_group),
        );
        assert!(marker.await.is_some());
    }

    #[test]
    fn split_needs_sides() {
        assert_eq!(split_point(&keys(&["a"]), &range("", None)), None);
        assert_eq!(split_point(&[], &range("", None)), None);
        let sample = keys(&["m/1", "m/2", "n/1"]);
        assert_eq!(split_point(&sample, &range("", Some("m/2"))), None);
    }
}
