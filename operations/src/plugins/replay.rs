//! Replays kept journal rows for an install whose event coverage grew on this node: a new or
//! resumed install from its activation, a widened one from its widening, on the recording clock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::keyspaces::{EVENT_JOURNAL_KEYSPACE, PLUGIN_REPLAYS_KEYSPACE};
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

use super::clock::{ClockReading, SKEW_MARGIN_MS, local_start};
use super::lifecycle::held_covers;
use super::matcher::{Counting, Outbox, Target, targets};
use super::rows::{
    Answer, Ask, Plan, Row, RowsOperation, Scan, Turn, done, row as row_of, unexpected, values,
    write_rows,
};
use super::store::{Rows, StoreError, decode, encode};
use crate::driver::{DriverContext, drive};
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
fn growth(
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

/// Starts a replay for each active install whose coverage grew here since its coverage row;
/// `probe` reads another node's clock. Returns how many replays started.
pub async fn plan_replays<F: Future<Output = Option<ClockReading>>>(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
    probe: impl Fn(NodeId) -> F,
) -> Result<usize, StoreError> {
    if matches!(node_kind(config, local), Some(RealmNodeKind::User { .. })) {
        return Ok(0);
    }
    let mut started = 0;
    for target in targets(config) {
        let id = target.install_id;
        let Some(install) = config.installs.iter().find(|i| i.install_id == id) else {
            continue;
        };
        let (row, task) = drive(replay_rows(id), ctx).await?;
        let next = CoverageRow {
            revision: install.revision,
            active_since_ms: install.active_since_ms,
            coverage: target.coverage.clone(),
            note: None,
        };
        if task.is_some_and(|task| task.next.revision == next.revision) {
            continue;
        }
        let Some((old, boundary, by)) = growth(install, row.as_ref(), &target.coverage) else {
            if row
                .as_ref()
                .is_some_and(|row| row.revision != next.revision)
            {
                let coverage = row_of(PLUGIN_REPLAYS_KEYSPACE, row_key(COVERAGE_ROW, id), encode(&next)?);
                drive(write_rows(vec![coverage]), ctx).await?;
            }
            continue;
        };
        let start = match by {
            Some(node) if node == local => Some(boundary.saturating_sub(SKEW_MARGIN_MS)),
            Some(node) => local_start(boundary, probe(node).await),
            None => None,
        };
        let replay = StartReplay::new(target, (next, old), (start, by), now_ms);
        drive(RowsOperation::direct(replay), ctx).await?;
        started += 1;
    }
    Ok(started)
}

/// The coverage row and the replay row of an install.
fn replay_rows(install_id: Ulid) -> RowsOperation<ReplayRows> {
    RowsOperation::direct(ReplayRows(install_id))
}

#[derive(Debug, PartialEq)]
pub struct ReplayRows(Ulid);

impl Plan for ReplayRows {
    type Output = (Option<CoverageRow>, Option<ReplayRow>);
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        Ok(Turn::Ask(Ask::Read(vec![
            (PLUGIN_REPLAYS_KEYSPACE, row_key(COVERAGE_ROW, self.0)),
            (PLUGIN_REPLAYS_KEYSPACE, row_key(REPLAY_ROW, self.0)),
        ])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        let [coverage, task] = values::<2>(answer)?;
        let coverage = coverage.map(|bytes| decode(&bytes)).transpose()?;
        let task = task.map(|bytes| decode(&bytes)).transpose()?;
        Ok(Turn::Finish((coverage, task)))
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
    fn new(
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
                let trigger = super::dispatch::event_trigger(kind);
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
            let note = format!("the clock of node {node} was not readable; replayed every kept event");
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

impl Plan for StartReplay {
    type Output = ();
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<()>, StoreError> {
        match &self.phase {
            StartPhase::Gaps(outbox) => Ok(Turn::Ask(outbox.gap_ask())),
            _ => Err(StoreError("started twice".into())),
        }
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<()>, StoreError> {
        match std::mem::replace(&mut self.phase, StartPhase::Writing) {
            StartPhase::Gaps(outbox) => {
                let Answer::Values(values) = answer else {
                    return Err(unexpected(answer));
                };
                self.phase = StartPhase::Pending(outbox.rows(values)?);
                Ok(Turn::Ask(Ask::Page {
                    space: EVENT_JOURNAL_KEYSPACE,
                    prefix: vec![PENDING_ROW],
                    after: None,
                    limit: 1,
                }))
            }
            StartPhase::Pending(mut rows) => {
                let Answer::Page(pending, _) = answer else {
                    return Err(unexpected(answer));
                };
                self.task.until = match pending.first() {
                    Some((key, _)) => [&[JOURNAL_ROW][..], &key[1..]].concat(),
                    None => journal_key(JOURNAL_ROW, self.now_ms.saturating_add(1), Ulid::nil()),
                };
                let key = row_key(REPLAY_ROW, self.install_id);
                rows.push(row_of(PLUGIN_REPLAYS_KEYSPACE, key, encode(&*self.task)?));
                Ok(Turn::Ask(Ask::Write(rows)))
            }
            StartPhase::Writing => {
                done(answer)?;
                Ok(Turn::Finish(()))
            }
        }
    }
}

/// Every replay key, for [`replay_page`].
pub fn replay_scan() -> Scan {
    Scan::new(PLUGIN_REPLAYS_KEYSPACE, vec![REPLAY_ROW])
}

/// One page of `page` rows of the replay at `key` in one transaction: forward rows for the newly
/// covered events it reads, then the replay row continued, or the coverage row written once the
/// replay reaches `until`. Answers the rows read and whether the replay ended.
pub fn replay_page(key: Vec<u8>, now_ms: u64, page: usize) -> RowsOperation<ReplayPage> {
    RowsOperation::transaction(ReplayPage {
        key,
        now_ms,
        page,
        phase: PagePhase::Task,
    })
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
    ) -> Result<Turn<(usize, bool)>, StoreError> {
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
        Ok(Turn::Ask(ask))
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

impl Plan for ReplayPage {
    type Output = (usize, bool);
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        Ok(Turn::Ask(Ask::Read(vec![(PLUGIN_REPLAYS_KEYSPACE, self.key.clone())])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        match std::mem::replace(&mut self.phase, PagePhase::Writing((0, true))) {
            PagePhase::Task => {
                let [task] = values::<1>(answer)?;
                let Some(bytes) = task else {
                    return Ok(Turn::Finish((0, true)));
                };
                let task: ReplayRow = decode(&bytes)?;
                let page = Ask::Page {
                    space: EVENT_JOURNAL_KEYSPACE,
                    prefix: vec![JOURNAL_ROW],
                    after: Some(task.after.clone()),
                    limit: self.page,
                };
                self.phase = PagePhase::Journal(Box::new(task));
                Ok(Turn::Ask(page))
            }
            PagePhase::Journal(task) => {
                let Answer::Page(rows, next) = answer else {
                    return Err(unexpected(answer));
                };
                let more = next.is_some();
                if !self.offers(&task, &rows) {
                    return self.matched(*task, (rows, more), BTreeMap::new());
                }
                let counting = Counting::new(BTreeSet::from([self.install_id()]), EVENTS_QUEUED);
                let ask = counting.ask().ok_or_else(|| unexpected(Answer::Done))?;
                self.phase = PagePhase::Counting(task, rows, more, counting);
                Ok(Turn::Ask(ask))
            }
            PagePhase::Counting(task, rows, more, mut counting) => {
                counting.take(answer)?;
                self.matched(*task, (rows, more), counting.counted())
            }
            PagePhase::Gaps(outbox, mut written, outcome) => {
                let Answer::Values(values) = answer else {
                    return Err(unexpected(answer));
                };
                let mut rows = outbox.rows(values)?;
                rows.append(&mut written);
                self.phase = PagePhase::Writing(outcome);
                Ok(Turn::Ask(Ask::Write(rows)))
            }
            PagePhase::Writing(outcome) => {
                done(answer)?;
                if !outcome.1 {
                    return Ok(Turn::Finish(outcome));
                }
                self.phase = PagePhase::Deleting(outcome);
                Ok(Turn::Ask(Ask::Delete(vec![(PLUGIN_REPLAYS_KEYSPACE, self.key.clone())])))
            }
            PagePhase::Deleting(outcome) => {
                done(answer)?;
                Ok(Turn::Finish(outcome))
            }
        }
    }
}

#[cfg(test)]
#[path = "replay_tests.rs"]
mod tests;
