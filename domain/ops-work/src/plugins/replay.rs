//! Replays kept journal rows for an install whose event coverage grew on this node: a new or
//! resumed install from its activation, a widened one from its widening, on the recording clock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{EVENT_JOURNAL_KEYSPACE, PLUGIN_REPLAYS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{
    EVENTS_QUEUED, GapReason, JOURNAL_RETENTION_MS, JOURNAL_ROW, PENDING_ROW, SourceEvent,
    SourceKind, journal_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::types::GroupId;
use aruna_core::user::id::UserId;
use aruna_plugin_contract::trigger::TriggerKind;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::clock::{AskClock, ClockReading, SKEW_MARGIN_MS, local_start};
use super::grants::held_covers;
use super::lifecycle::targets;
use super::matcher::{Counting, Outbox, Target};
use super::rows::{
    Row, Rows, StoreError, all_values, decode, delete_call, done, encode, page_call, page_of,
    read_call, row as row_of, unexpected, values,
};
use aruna_ops_identity::realm::mutate_placement::node_kind;

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
    pub fn new(
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
                let Some(trigger) = super::matcher::event_trigger(kind) else {
                    continue;
                };
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
            _ => Err(StoreError::Message("started twice".into())),
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
    Counting(Box<ReplayRow>, CoveredPage, Counting),
    Gaps(Box<Outbox>, Vec<Row>, (usize, bool)),
    Writing((usize, bool)),
    Deleting((usize, bool)),
}

/// A journal page cut at the replay's end, each row decoded once: how many rows it read, the
/// last of them, its newly covered events and whether the replay ended.
#[derive(Debug, PartialEq)]
struct CoveredPage {
    read: usize,
    last: Option<Vec<u8>>,
    events: Vec<(Vec<u8>, SourceEvent)>,
    ended: bool,
}

impl ReplayPage {
    fn install_id(&self) -> Ulid {
        Ulid::from_bytes(self.key[1..].try_into().unwrap_or_default())
    }

    /// The page's rows before the replay's end, and the events among them the grown coverage
    /// covers and the old one did not.
    fn covered(&self, task: &ReplayRow, (rows, more): (Rows, bool)) -> CoveredPage {
        let install_id = self.install_id();
        let target = |coverage: &Coverage| Target {
            install_id,
            group_id: task.group_id,
            revision: task.next.revision,
            coverage: coverage.clone(),
            account: task.account,
        };
        let (new, old) = (target(&task.next.coverage), target(&task.old));
        let total = rows.len();
        let (mut read, mut last, mut events) = (0, None, Vec::new());
        for (row, value) in rows.into_iter().take_while(|(row, _)| *row < task.until) {
            read += 1;
            if let Ok(event) = SourceEvent::from_bytes(&value)
                && new.matches(&event)
                && !old.matches(&event)
            {
                events.push((row.clone(), event));
            }
            last = Some(row);
        }
        let ended = !more || read < total;
        CoveredPage {
            read,
            last,
            events,
            ended,
        }
    }

    /// The outbox of the page's newly covered events, then the read of its gap rows.
    fn matched(
        &mut self,
        mut task: ReplayRow,
        page: CoveredPage,
        queued: BTreeMap<Ulid, usize>,
    ) -> Result<Next<(usize, bool)>, StoreError> {
        let install_id = self.install_id();
        let mut outbox = Outbox::new(self.now_ms, queued);
        let install = (install_id, task.next.revision);
        let stale_ms = self.now_ms.saturating_sub(JOURNAL_RETENTION_MS);
        for (row, event) in &page.events {
            let stale = event.journaled_ms < stale_ms;
            outbox.offer(install, row, event, stale, EVENTS_QUEUED)?;
        }
        if let Some(last) = page.last {
            task.after = last;
        }
        let written = match page.ended {
            true => {
                let coverage = row_key(COVERAGE_ROW, install_id);
                row_of(PLUGIN_REPLAYS_KEYSPACE, coverage, encode(&task.next)?)
            }
            false => row_of(PLUGIN_REPLAYS_KEYSPACE, self.key.clone(), encode(&task)?),
        };
        let outcome = (page.read, page.ended);
        if !outbox.counts_gaps() {
            let mut rows = outbox.rows(Vec::new())?;
            rows.push(written);
            self.phase = PagePhase::Writing(outcome);
            return Ok(Next::Call(IoCall::Write(rows)));
        }
        let ask = outbox.gap_ask();
        self.phase = PagePhase::Gaps(Box::new(outbox), vec![written], outcome);
        Ok(Next::Call(ask))
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
                let page = self.covered(&task, (rows, next.is_some()));
                // Only a page with newly covered events counts the install's queue.
                if page.events.is_empty() {
                    return self.matched(*task, page, BTreeMap::new());
                }
                let counting = Counting::new(BTreeSet::from([self.install_id()]), EVENTS_QUEUED);
                let ask = counting.ask().ok_or_else(|| unexpected(IoResult::Done))?;
                self.phase = PagePhase::Counting(task, page, counting);
                Ok(Next::Call(ask))
            }
            PagePhase::Counting(task, page, mut counting) => {
                counting.take(answer)?;
                self.matched(*task, page, counting.counted())
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

/// What one replay pass did: the rows read, whether a replay has more, and the replay key the
/// next pass starts after.
#[derive(Debug, Default, PartialEq)]
pub struct Replayed {
    pub read: usize,
    pub more: bool,
    pub after: Option<Vec<u8>>,
    /// Replays whose page failed; each keeps its row and comes again after the others.
    pub failed: usize,
}

/// Continues the replays in key order after `after`, one journal page of `page` rows each, a
/// page per transaction, and wraps once to the first replay. Every replay key page and journal
/// page costs one of `pages`; while pages remain and a replay has more, another pass starts.
#[derive(Debug, PartialEq)]
pub struct RunReplays {
    now_ms: u64,
    page: usize,
    pages: usize,
    /// The key this pass started after; after wrapping the pass stops at it.
    start: Option<Vec<u8>>,
    wrapped: bool,
    /// The key the next key page starts after, while this sweep has one.
    scan: Option<Option<Vec<u8>>>,
    scanning: bool,
    keys: VecDeque<Vec<u8>>,
    current: Option<Vec<u8>>,
    last: Option<Vec<u8>>,
    read: usize,
    pending: bool,
    failed: usize,
}

impl RunReplays {
    pub fn new(now_ms: u64, (page, pages): (usize, usize), after: Option<Vec<u8>>) -> Self {
        Self {
            now_ms,
            page,
            pages,
            start: after.clone(),
            wrapped: false,
            scan: Some(after),
            scanning: false,
            keys: VecDeque::new(),
            current: None,
            last: None,
            read: 0,
            pending: false,
            failed: 0,
        }
    }

    fn page_of(&self, key: Vec<u8>) -> IoCall {
        IoCall::child(replay_page(key, self.now_ms, self.page))
    }

    /// The end of the pass; a failure keeps the position past the failed replay and asks for
    /// the ordinary delay instead of the next tick at once.
    fn done(&mut self, more: bool) -> Next<Replayed> {
        let failed = self.failed;
        let after = if more || failed > 0 {
            self.last.take()
        } else {
            None
        };
        Next::Done(Replayed {
            read: self.read,
            more: more && failed == 0,
            after,
            failed,
        })
    }

    /// The next key page or journal page, or the end once the pages are spent.
    fn advance(&mut self) -> Next<Replayed> {
        loop {
            if let Some(key) = self.keys.pop_front() {
                if self.wrapped && self.start.as_ref().is_some_and(|start| key > *start) {
                    // The rest of this sweep was visited before the wrap.
                    self.keys.clear();
                    self.scan = None;
                    continue;
                }
                if self.pages == 0 {
                    return self.done(true);
                }
                self.pages -= 1;
                self.current = Some(key.clone());
                return Next::Call(self.page_of(key));
            }
            if let Some(after) = self.scan.clone() {
                if self.pages == 0 {
                    return self.done(true);
                }
                self.pages -= 1;
                self.scanning = true;
                let limit = self.pages.max(1);
                let keys = page_call(PLUGIN_REPLAYS_KEYSPACE, vec![REPLAY_ROW], after, limit);
                return Next::Call(keys);
            }
            if !self.wrapped && self.start.is_some() {
                self.wrapped = true;
                self.scan = Some(None);
                continue;
            }
            if self.pending && self.failed == 0 && self.pages > 0 {
                (self.pending, self.start, self.wrapped) = (false, None, false);
                self.scan = Some(None);
                continue;
            }
            return self.done(self.pending);
        }
    }
}

impl Operation for RunReplays {
    type Output = Replayed;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Replayed>, StoreError> {
        Ok(self.advance())
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Replayed>, StoreError> {
        if self.scanning {
            // Only the keys of each page stay; the replay rows are read again page by page.
            self.scanning = false;
            let (rows, next) = page_of(answer)?;
            self.keys.extend(rows.into_iter().map(|(key, _)| key));
            self.scan = next.map(Some);
            return Ok(self.advance());
        }
        let key = self.current.take().ok_or(IoError::Unfinished)?;
        match answer.child::<(usize, bool), StoreError>()? {
            Ok((rows, ended)) => {
                self.read += rows;
                self.pending |= !ended;
            }
            // The failed replay keeps its row; the pass goes on with the next install.
            Err(_) => self.failed += 1,
        }
        self.last = Some(key);
        Ok(self.advance())
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
pub struct PlanReplays {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    now_ms: u64,
    clock_ms: Option<u64>,
    targets: VecDeque<Target>,
    started: usize,
    phase: PlanPhase,
}

impl PlanReplays {
    pub fn new(config: Arc<RealmConfigDocument>, local: NodeId, now_ms: u64) -> Self {
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
