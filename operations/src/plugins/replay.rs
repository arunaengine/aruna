//! Replays kept journal rows for an install whose event coverage grew on this node: a new or
//! resumed install from its activation, a widened one from its widening, on the recording clock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{EVENT_JOURNAL_KEYSPACE, PLUGIN_REPLAYS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{
    EVENTS_QUEUED, GapReason, JOURNAL_RETENTION_MS, JOURNAL_ROW, PENDING_ROW, SourceEvent,
    SourceKind, journal_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use aruna_plugins::trigger::TriggerKind;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::clock::{AskClock, ClockReading, SKEW_MARGIN_MS, local_start};
use super::grants::held_covers;
use super::lifecycle::targets;
use super::matcher::{Counting, Outbox, Target};
use super::rows::{
    Row, Rows, Scan, StoreError, all_values, decode, delete_call, done, encode, page_call, page_of,
    read_call, row as row_of, unexpected, values,
};
use crate::realm::mutate_placement::node_kind;

pub(crate) const COVERAGE_ROW: u8 = b'c';
const REPLAY_ROW: u8 = b'r';

type Coverage = BTreeMap<TriggerKind, BTreeSet<String>>;

/// What this node matched for an install, at which revision and activation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageRow {
    pub revision: u64,
    pub active_since_ms: Option<u64>,
    pub coverage: Coverage,
    /// Why the last replay started at the earliest kept row.
    pub note: Option<String>,
}

/// A replay in progress: the coverage it adds to `old`, the last row it read, and the first row
/// the matcher reads itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayRow {
    pub group_id: GroupId,
    pub account: UserId,
    pub next: CoverageRow,
    pub old: Coverage,
    pub after: Vec<u8>,
    pub until: Vec<u8>,
}

pub(crate) fn row_key(kind: u8, install_id: Ulid) -> Vec<u8> {
    [&[kind][..], &install_id.to_bytes()].concat()
}

/// The coverage this node held before, the boundary the grown part starts at, and the node
/// whose clock it uses; `None` when nothing grew since the coverage row.
pub(crate) fn growth(
    install: &PluginInstall,
    row: Option<&CoverageRow>,
    coverage: &Coverage,
) -> Option<(Coverage, u64, Option<NodeId>)> {
    let since = install.active_since_ms?;
    let Some(row) = row.filter(|row| row.active_since_ms == install.active_since_ms) else {
        return Some((Coverage::new(), since, install.activated_by));
    };
    let grown = (coverage.iter()).any(|(kind, patterns)| {
        let held = row.coverage.get(kind);
        (patterns.iter()).any(|pattern| held.is_none_or(|held| !held_covers(held, pattern)))
    });
    if row.revision == install.revision || !grown {
        return None;
    }
    // A skipped revision may have grown the coverage earlier than the last widening.
    match &install.widened {
        Some(widened) if widened.extends == row.revision => {
            Some((row.coverage.clone(), widened.at_ms, Some(widened.by)))
        }
        _ => Some((row.coverage.clone(), since, install.activated_by)),
    }
}

/// The coverage row and the replay row of an install.
pub(crate) fn replay_rows(install_id: Ulid) -> ReplayRows {
    ReplayRows(install_id)
}

#[derive(Debug, PartialEq)]
pub struct ReplayRows(Ulid);

impl Operation for ReplayRows {
    type Output = (Option<CoverageRow>, Option<ReplayRow>);
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(read_call(vec![
            (PLUGIN_REPLAYS_KEYSPACE, row_key(COVERAGE_ROW, self.0)),
            (PLUGIN_REPLAYS_KEYSPACE, row_key(REPLAY_ROW, self.0)),
        ])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        let [coverage, task] = values::<2>(answer)?;
        let coverage = coverage.map(|bytes| decode(&bytes)).transpose()?;
        let task = task.map(|bytes| decode(&bytes)).transpose()?;
        Ok(Next::Done((coverage, task)))
    }
}

/// Writes the replay row of a grown coverage, counting a gap per grown kind when the journal was
/// already pruned past the start.
#[derive(Debug, PartialEq)]
pub struct StartReplay {
    install_id: Ulid,
    task: Box<ReplayRow>,
    now_ms: u64,
    phase: StartPhase,
}

#[derive(Debug, PartialEq)]
enum StartPhase {
    Gaps(Box<Outbox>),
    Pending(Vec<Row>),
    Writing,
}

impl StartReplay {
    pub(crate) fn new(
        target: Target,
        (mut next, old): (CoverageRow, Coverage),
        (start, by): (Option<u64>, Option<NodeId>),
        now_ms: u64,
    ) -> Self {
        let kept_from = now_ms.saturating_sub(JOURNAL_RETENTION_MS);
        let mut outbox = Outbox::new(now_ms, BTreeMap::new());
        if start.is_some_and(|start| start < kept_from) {
            for kind in [
                SourceKind::DatasetCreated,
                SourceKind::DataUploaded,
                SourceKind::DatasetUpdated,
                SourceKind::SyncCompleted,
                SourceKind::SyncFailed,
            ] {
                let trigger = super::matcher::event_trigger(kind);
                if target.coverage.contains_key(&trigger)
                    && old.get(&trigger) != target.coverage.get(&trigger)
                {
                    // The journal no longer shows how many events the pruned part held.
                    let range = (start.unwrap_or_default(), kept_from);
                    let id = target.install_id;
                    outbox.lost_range(id, (kind, GapReason::Pruned), range);
                }
            }
        }
        if start.is_none() {
            let node = by.map_or_else(|| "unknown".to_string(), |node| node.to_string());
            let note =
                format!("the clock of node {node} was not readable; replayed every kept event");
            next.note = Some(note);
        }
        let task = ReplayRow {
            group_id: target.group_id,
            account: target.account,
            next,
            old,
            after: journal_key(JOURNAL_ROW, start.unwrap_or_default(), Ulid::nil()),
            until: Vec::new(),
        };
        Self {
            install_id: target.install_id,
            task: Box::new(task),
            now_ms,
            phase: StartPhase::Gaps(Box::new(outbox)),
        }
    }
}

impl Operation for StartReplay {
    type Output = ();
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<()>, StoreError> {
        match &self.phase {
            StartPhase::Gaps(outbox) => Ok(Next::Call(outbox.gap_ask())),
            _ => Err(StoreError("started twice".into())),
        }
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<()>, StoreError> {
        match std::mem::replace(&mut self.phase, StartPhase::Writing) {
            StartPhase::Gaps(outbox) => {
                let values = all_values(answer)?;
                self.phase = StartPhase::Pending(outbox.rows(values)?);
                Ok(Next::Call(page_call(
                    EVENT_JOURNAL_KEYSPACE,
                    vec![PENDING_ROW],
                    None,
                    1,
                )))
            }
            StartPhase::Pending(mut rows) => {
                let (pending, _) = page_of(answer)?;
                self.task.until = match pending.first() {
                    Some((key, _)) => [&[JOURNAL_ROW][..], &key[1..]].concat(),
                    None => journal_key(JOURNAL_ROW, self.now_ms.saturating_add(1), Ulid::nil()),
                };
                let key = row_key(REPLAY_ROW, self.install_id);
                rows.push(row_of(PLUGIN_REPLAYS_KEYSPACE, key, encode(&*self.task)?));
                Ok(Next::Call(IoCall::Write(rows)))
            }
            StartPhase::Writing => {
                done(answer)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// Every replay key, for [`replay_page`].
pub(crate) fn replay_scan() -> Scan {
    Scan::new(PLUGIN_REPLAYS_KEYSPACE, vec![REPLAY_ROW])
}

/// One page of `page` rows of the replay at `key` in one transaction: forward rows for the newly
/// covered events it reads, then the replay row continued, or the coverage row written once the
/// replay reaches `until`. Answers the rows read and whether the replay ended.
pub(crate) fn replay_page(key: Vec<u8>, now_ms: u64, page: usize) -> ReplayPage {
    ReplayPage {
        key,
        now_ms,
        page,
        phase: PagePhase::Task,
    }
}

#[derive(Debug, PartialEq)]
pub struct ReplayPage {
    key: Vec<u8>,
    now_ms: u64,
    page: usize,
    phase: PagePhase,
}

#[derive(Debug, PartialEq)]
enum PagePhase {
    Task,
    Journal(Box<ReplayRow>),
    Counting(Box<ReplayRow>, Rows, bool, Counting),
    Gaps(Box<Outbox>, Vec<Row>, (usize, bool)),
    Writing((usize, bool)),
    Deleting((usize, bool)),
}

impl ReplayPage {
    fn install_id(&self) -> Ulid {
        Ulid::from_bytes(self.key[1..].try_into().unwrap_or_default())
    }

    /// The outbox of the page's matches, then the read of its gap rows.
    fn matched(
        &mut self,
        mut task: ReplayRow,
        (rows, more): (Rows, bool),
        queued: BTreeMap<Ulid, usize>,
    ) -> Result<Next<(usize, bool)>, StoreError> {
        let install_id = self.install_id();
        let target = |coverage: &Coverage| Target {
            install_id,
            group_id: task.group_id,
            revision: task.next.revision,
            coverage: coverage.clone(),
            account: task.account,
        };
        let (new, old) = (target(&task.next.coverage), target(&task.old));
        let mut outbox = Outbox::new(self.now_ms, queued);
        let mut read = 0;
        for (row, value) in rows.iter().take_while(|(row, _)| *row < task.until) {
            read += 1;
            task.after = row.clone();
            let Ok(event) = SourceEvent::from_bytes(value) else {
                continue;
            };
            if new.matches(&event) && !old.matches(&event) {
                let stale = event.journaled_ms < self.now_ms.saturating_sub(JOURNAL_RETENTION_MS);
                let install = (install_id, task.next.revision);
                outbox.offer(install, row, &event, stale, EVENTS_QUEUED)?;
            }
        }
        let ended = !more || read < rows.len();
        let written = match ended {
            true => {
                let coverage = row_key(COVERAGE_ROW, install_id);
                row_of(PLUGIN_REPLAYS_KEYSPACE, coverage, encode(&task.next)?)
            }
            false => row_of(PLUGIN_REPLAYS_KEYSPACE, self.key.clone(), encode(&task)?),
        };
        let ask = outbox.gap_ask();
        self.phase = PagePhase::Gaps(Box::new(outbox), vec![written], (read, ended));
        Ok(Next::Call(ask))
    }

    /// Whether any row of the page is a newly covered event, so the install's queue is counted.
    fn offers(&self, task: &ReplayRow, rows: &Rows) -> bool {
        let install_id = self.install_id();
        let target = |coverage: &Coverage| Target {
            install_id,
            group_id: task.group_id,
            revision: task.next.revision,
            coverage: coverage.clone(),
            account: task.account,
        };
        let (new, old) = (target(&task.next.coverage), target(&task.old));
        (rows.iter().take_while(|(row, _)| *row < task.until))
            .filter_map(|(_, value)| SourceEvent::from_bytes(value).ok())
            .any(|event| new.matches(&event) && !old.matches(&event))
    }
}

impl Operation for ReplayPage {
    type Output = (usize, bool);
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(read_call(vec![(
            PLUGIN_REPLAYS_KEYSPACE,
            self.key.clone(),
        )])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        match std::mem::replace(&mut self.phase, PagePhase::Writing((0, true))) {
            PagePhase::Task => {
                let [task] = values::<1>(answer)?;
                let Some(bytes) = task else {
                    return Ok(Next::Done((0, true)));
                };
                let task: ReplayRow = decode(&bytes)?;
                let page = page_call(
                    EVENT_JOURNAL_KEYSPACE,
                    vec![JOURNAL_ROW],
                    Some(task.after.clone()),
                    self.page,
                );
                self.phase = PagePhase::Journal(Box::new(task));
                Ok(Next::Call(page))
            }
            PagePhase::Journal(task) => {
                let (rows, next) = page_of(answer)?;
                let more = next.is_some();
                if !self.offers(&task, &rows) {
                    return self.matched(*task, (rows, more), BTreeMap::new());
                }
                let counting = Counting::new(BTreeSet::from([self.install_id()]), EVENTS_QUEUED);
                let ask = counting.ask().ok_or_else(|| unexpected(IoResult::Done))?;
                self.phase = PagePhase::Counting(task, rows, more, counting);
                Ok(Next::Call(ask))
            }
            PagePhase::Counting(task, rows, more, mut counting) => {
                counting.take(answer)?;
                self.matched(*task, (rows, more), counting.counted())
            }
            PagePhase::Gaps(outbox, mut written, outcome) => {
                let values = all_values(answer)?;
                let mut rows = outbox.rows(values)?;
                rows.append(&mut written);
                self.phase = PagePhase::Writing(outcome);
                Ok(Next::Call(IoCall::Write(rows)))
            }
            PagePhase::Writing(outcome) => {
                done(answer)?;
                if !outcome.1 {
                    return Ok(Next::Done(outcome));
                }
                self.phase = PagePhase::Deleting(outcome);
                Ok(Next::Call(delete_call(vec![(
                    PLUGIN_REPLAYS_KEYSPACE,
                    self.key.clone(),
                )])))
            }
            PagePhase::Deleting(outcome) => {
                done(answer)?;
                Ok(Next::Done(outcome))
            }
        }
    }
}

/// What one install's coverage needs now.
#[derive(Debug, PartialEq)]
pub(crate) enum ReplayPlan {
    Nothing,
    /// A replay of the grown part starts; the node's clock is `by`'s unless it is this one.
    Start {
        target: Box<Target>,
        next: Box<CoverageRow>,
        old: Coverage,
        boundary: u64,
        by: Option<NodeId>,
    },
}

/// Decides from the coverage row whether the install's coverage grew here; a record that moved on
/// without growth only updates the row.
#[derive(Debug, PartialEq)]
pub(crate) struct PlanReplay {
    install: PluginInstall,
    target: Option<Target>,
    writing: bool,
}

impl PlanReplay {
    pub(crate) fn new(install: PluginInstall, target: Target) -> Self {
        Self {
            install,
            target: Some(target),
            writing: false,
        }
    }
}

impl Operation for PlanReplay {
    type Output = ReplayPlan;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<ReplayPlan>, StoreError> {
        Ok(Next::Call(IoCall::child(replay_rows(
            self.install.install_id,
        ))))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<ReplayPlan>, StoreError> {
        if self.writing {
            done(answer)?;
            return Ok(Next::Done(ReplayPlan::Nothing));
        }
        let rows = answer.child::<(Option<CoverageRow>, Option<ReplayRow>), StoreError>()??;
        let (row, task) = rows;
        let target = self.target.take().ok_or(IoError::Unfinished)?;
        let next = CoverageRow {
            revision: self.install.revision,
            active_since_ms: self.install.active_since_ms,
            coverage: target.coverage.clone(),
            note: None,
        };
        if task.is_some_and(|task| task.next.revision == next.revision) {
            return Ok(Next::Done(ReplayPlan::Nothing));
        }
        let Some((old, boundary, by)) = growth(&self.install, row.as_ref(), &target.coverage)
        else {
            if !row.is_some_and(|row| row.revision != next.revision) {
                return Ok(Next::Done(ReplayPlan::Nothing));
            }
            self.writing = true;
            let key = row_key(COVERAGE_ROW, self.install.install_id);
            let coverage = row_of(PLUGIN_REPLAYS_KEYSPACE, key, encode(&next)?);
            return Ok(Next::Call(IoCall::Write(vec![coverage])));
        };
        Ok(Next::Done(ReplayPlan::Start {
            target: Box::new(target),
            next: Box::new(next),
            old,
            boundary,
            by,
        }))
    }
}

/// Continues every replay, at most `pages` pages of `page` rows each, a page per transaction;
/// answers the rows read.
#[derive(Debug, PartialEq)]
pub(crate) struct RunReplays {
    now_ms: u64,
    page: usize,
    pages: usize,
    scan: Option<Scan>,
    keys: VecDeque<Vec<u8>>,
    current: Option<(Vec<u8>, usize)>,
    read: usize,
}

impl RunReplays {
    pub(crate) fn new(now_ms: u64, (page, pages): (usize, usize)) -> Self {
        Self {
            now_ms,
            page,
            pages,
            scan: Some(replay_scan()),
            keys: VecDeque::new(),
            current: None,
            read: 0,
        }
    }

    fn page_of(&self, key: Vec<u8>) -> IoCall {
        IoCall::child(replay_page(key, self.now_ms, self.page))
    }

    /// The next page of the next replay, or the end.
    fn next_replay(&mut self) -> Next<usize> {
        while let Some(key) = self.keys.pop_front() {
            if self.pages > 0 {
                self.current = Some((key.clone(), 1));
                return Next::Call(self.page_of(key));
            }
        }
        Next::Done(self.read)
    }
}

impl Operation for RunReplays {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        let scan = self.scan.as_ref().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(scan.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, StoreError> {
        if let Some(scan) = self.scan.as_mut() {
            let Some(rows) = scan.take(answer)? else {
                return Ok(Next::Call(scan.ask()));
            };
            self.scan = None;
            self.keys = rows.into_iter().map(|(key, _)| key).collect();
            return Ok(self.next_replay());
        }
        let (rows, ended) = answer.child::<(usize, bool), StoreError>()??;
        self.read += rows;
        let (key, done_pages) = self.current.take().ok_or(IoError::Unfinished)?;
        if ended || done_pages >= self.pages {
            return Ok(self.next_replay());
        }
        self.current = Some((key.clone(), done_pages + 1));
        Ok(Next::Call(self.page_of(key)))
    }
}

/// A replay about to start, waiting for the time it starts from.
#[derive(Debug, PartialEq)]
struct PlannedReplay {
    target: Box<Target>,
    next: Box<CoverageRow>,
    old: Coverage,
}

#[derive(Debug, PartialEq)]
enum PlanPhase {
    Plan,
    Clock(PlannedReplay, u64, NodeId, Option<u64>),
    Start,
}

/// Starts a replay for each active install whose coverage grew here since its coverage row. The
/// activation time on another node's clock is mapped with one round trip to it. Answers how many
/// replays started; a user node keeps no journal and starts none.
#[derive(Debug, PartialEq)]
pub(crate) struct PlanReplays {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    now_ms: u64,
    clock_ms: Option<u64>,
    targets: VecDeque<Target>,
    started: usize,
    phase: PlanPhase,
}

impl PlanReplays {
    pub(crate) fn new(config: Arc<RealmConfigDocument>, local: NodeId, now_ms: u64) -> Self {
        Self {
            targets: targets(&config).into(),
            config,
            local,
            now_ms,
            clock_ms: None,
            started: 0,
            phase: PlanPhase::Plan,
        }
    }

    fn advance(&mut self) -> Next<usize> {
        while let Some(target) = self.targets.pop_front() {
            let id = target.install_id;
            let install = (self.config.installs.iter()).find(|install| install.install_id == id);
            if let Some(install) = install {
                self.phase = PlanPhase::Plan;
                let plan = PlanReplay::new(install.clone(), target);
                return Next::Call(IoCall::child(plan));
            }
        }
        Next::Done(self.started)
    }

    fn start(
        &mut self,
        plan: PlannedReplay,
        start: Option<u64>,
        by: Option<NodeId>,
    ) -> Next<usize> {
        let PlannedReplay { target, next, old } = plan;
        let replay = StartReplay::new(*target, (*next, old), (start, by), self.now_ms);
        self.phase = PlanPhase::Start;
        Next::Call(IoCall::child(replay))
    }

    fn planned(&mut self, plan: ReplayPlan) -> Next<usize> {
        let ReplayPlan::Start {
            target,
            next,
            old,
            boundary,
            by,
        } = plan
        else {
            return self.advance();
        };
        let plan = PlannedReplay { target, next, old };
        match by {
            Some(node) if node == self.local => {
                let start = boundary.saturating_sub(SKEW_MARGIN_MS);
                self.start(plan, Some(start), by)
            }
            Some(node) => {
                self.phase = PlanPhase::Clock(plan, boundary, node, self.clock_ms);
                Next::Call(IoCall::child(AskClock(node)))
            }
            None => self.start(plan, None, None),
        }
    }
}

impl Operation for PlanReplays {
    type Output = usize;
    type Error = StoreError;

    fn stamp(&mut self, now_ms: u64) {
        self.clock_ms = Some(now_ms);
    }

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        let user = matches!(
            node_kind(&self.config, self.local),
            Some(RealmNodeKind::User { .. })
        );
        Ok(match user {
            true => Next::Done(0),
            false => self.advance(),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, StoreError> {
        match std::mem::replace(&mut self.phase, PlanPhase::Plan) {
            PlanPhase::Plan => {
                let plan = result.child::<ReplayPlan, StoreError>()??;
                Ok(self.planned(plan))
            }
            PlanPhase::Clock(plan, boundary, node, sent_ms) => {
                let remote = result.child::<Option<u64>, IoError>()?;
                let reading = (sent_ms.zip(remote.ok().flatten()).zip(self.clock_ms)).map(
                    |((sent_ms, remote_ms), received_ms)| ClockReading {
                        sent_ms,
                        remote_ms,
                        received_ms,
                    },
                );
                Ok(self.start(plan, local_start(boundary, reading), Some(node)))
            }
            PlanPhase::Start => {
                result.child::<(), StoreError>()??;
                self.started += 1;
                Ok(self.advance())
            }
        }
    }
}

#[cfg(test)]
#[path = "replay_tests.rs"]
mod tests;
