//! Matches this node's pending journal rows to the active installs with event triggers and
//! queues forward rows for their receivers, or counts a gap where a row cannot be queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::keyspaces::{EVENT_JOURNAL_KEYSPACE, PLUGIN_GAPS_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE};
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::{
    EVENTS_QUEUED, ForwardRow, GapReason, GapRow, JOURNAL_RETENTION_MS, JOURNAL_ROW, PENDING_ROW,
    SourceEvent, SourceKind, gap_key, outbox_key, source_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use aruna_plugins::trigger::TriggerKind;
use ulid::Ulid;

use super::check::covers;
use super::dispatch::event_trigger;
use super::grants::parse_grants;
use super::install::{pick_version, read_manifest};
use super::journal::JOURNAL_PAGE;
use super::lifecycle::coverage;
use super::rows::{Answer, Ask, Plan, Row, RowsOperation, Turn, done, row as row_of, unexpected};
use super::store::{Rows, StoreError, decode, encode};
use crate::realm::mutate_placement::node_kind;

/// An active install with event triggers, as the matcher sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub(crate) install_id: Ulid,
    pub(crate) group_id: GroupId,
    pub(crate) revision: u64,
    pub(crate) coverage: BTreeMap<TriggerKind, BTreeSet<String>>,
    /// The install's account; datasets it wrote start no run of the install.
    pub(crate) account: UserId,
}

impl Target {
    /// Whether the install's trigger of the event's kind covers the event's path, and the
    /// event is not a dataset write of the install itself.
    pub(crate) fn matches(&self, event: &SourceEvent) -> bool {
        let dataset = matches!(
            event.kind,
            SourceKind::DatasetCreated | SourceKind::DatasetUpdated
        );
        self.group_id == event.group_id
            && !(dataset && event.actor == Some(self.account))
            && (self.coverage.get(&event_trigger(event.kind)))
                .is_some_and(|patterns| patterns.iter().any(|pattern| covers(pattern, &event.path)))
    }
}

/// The active installs whose manifest declares event triggers, with their coverage.
pub fn targets(config: &RealmConfigDocument) -> Vec<Target> {
    (config.installs.iter())
        .filter(|install| install.state == InstallState::Active)
        .filter_map(|install| {
            let plugin = pick_version(config, &install.plugin, Some(&install.digest)).ok()?;
            let manifest = read_manifest(&plugin).ok()?;
            let coverage = coverage(&manifest, &parse_grants(&install.grants));
            (!coverage.is_empty()).then_some(Target {
                install_id: install.install_id,
                group_id: install.group_id,
                revision: install.revision,
                coverage,
                account: install.account,
            })
        })
        .collect()
}

/// The installs this node matches pending rows to, and whether it keeps its journal: a user
/// node keeps none, so its rows are deleted unmatched.
pub fn match_targets(config: &RealmConfigDocument, local: NodeId) -> (Vec<Target>, bool) {
    let keep = !matches!(node_kind(config, local), Some(RealmNodeKind::User { .. }));
    let targets = match keep {
        true => targets(config),
        false => Vec::new(),
    };
    (targets, keep)
}

/// One page of pending rows in one transaction, matched with at most `bound` forward rows per
/// install: the forward rows and gaps of the targets each row matches, and the pending rows
/// deleted. Answers how many rows were read; a full page of `JOURNAL_PAGE` means more may follow.
pub fn match_page(
    (targets, keep): (&[Target], bool),
    now_ms: u64,
    bound: usize,
) -> RowsOperation<MatchPage> {
    RowsOperation::transaction(MatchPage {
        targets: targets.to_vec(),
        keep,
        now_ms,
        bound,
        phase: MatchPhase::Reading,
    })
}

#[derive(Debug, PartialEq)]
pub struct MatchPage {
    targets: Vec<Target>,
    keep: bool,
    now_ms: u64,
    bound: usize,
    phase: MatchPhase,
}

#[derive(Debug, PartialEq)]
enum MatchPhase {
    Reading,
    Counting(Rows, Counting),
    Gaps(Box<Outbox>, Vec<(&'static str, Vec<u8>)>, usize),
    Writing(Vec<(&'static str, Vec<u8>)>, usize),
    Deleting(usize),
}

impl MatchPage {
    /// The outbox of the page's matches once every touched install is counted.
    fn matched(
        &mut self,
        rows: Rows,
        queued: BTreeMap<Ulid, usize>,
    ) -> Result<Turn<usize>, StoreError> {
        let stale = self.now_ms.saturating_sub(JOURNAL_RETENTION_MS);
        let (mut done, mut outbox) = (Vec::new(), Outbox::new(self.now_ms, queued));
        let read = rows.len();
        for (key, value) in &rows {
            done.push((EVENT_JOURNAL_KEYSPACE, key.clone()));
            let event = SourceEvent::from_bytes(value);
            if !self.keep {
                done.push((EVENT_JOURNAL_KEYSPACE, [&[JOURNAL_ROW][..], &key[1..]].concat()));
                let source = event.ok().map(|event| source_key(&event.source));
                done.extend(source.map(|key| (EVENT_JOURNAL_KEYSPACE, key)));
                continue;
            }
            let Ok(event) = event else {
                continue;
            };
            for target in self.targets.iter().filter(|target| target.matches(&event)) {
                let install = (target.install_id, target.revision);
                let stale = event.journaled_ms < stale;
                if let Err(error) = outbox.offer(install, key, &event, stale, self.bound) {
                    return Err(error);
                }
            }
        }
        let ask = outbox.gap_ask();
        self.phase = MatchPhase::Gaps(Box::new(outbox), done, read);
        Ok(Turn::Ask(ask))
    }
}

impl Plan for MatchPage {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<usize>, StoreError> {
        Ok(Turn::Ask(Ask::Page {
            space: EVENT_JOURNAL_KEYSPACE,
            prefix: vec![PENDING_ROW],
            after: None,
            limit: JOURNAL_PAGE,
        }))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<usize>, StoreError> {
        match std::mem::replace(&mut self.phase, MatchPhase::Deleting(0)) {
            MatchPhase::Reading => {
                let Answer::Page(rows, _) = answer else {
                    return Err(unexpected(answer));
                };
                if rows.is_empty() {
                    return Ok(Turn::Finish(0));
                }
                let installs = (rows.iter())
                    .filter_map(|(_, value)| SourceEvent::from_bytes(value).ok())
                    .flat_map(|event| {
                        let targets = self.targets.iter();
                        let matched = targets.filter(move |target| target.matches(&event));
                        matched.map(|target| target.install_id).collect::<Vec<_>>()
                    })
                    .filter(|_| self.keep)
                    .collect();
                let counting = Counting::new(installs, self.bound);
                match counting.ask() {
                    Some(ask) => {
                        self.phase = MatchPhase::Counting(rows, counting);
                        Ok(Turn::Ask(ask))
                    }
                    None => self.matched(rows, BTreeMap::new()),
                }
            }
            MatchPhase::Counting(rows, mut counting) => {
                counting.take(answer)?;
                match counting.ask() {
                    Some(ask) => {
                        self.phase = MatchPhase::Counting(rows, counting);
                        Ok(Turn::Ask(ask))
                    }
                    None => self.matched(rows, counting.counted()),
                }
            }
            MatchPhase::Gaps(outbox, done, read) => {
                let Answer::Values(values) = answer else {
                    return Err(unexpected(answer));
                };
                self.phase = MatchPhase::Writing(done, read);
                Ok(Turn::Ask(Ask::Write(outbox.rows(values)?)))
            }
            MatchPhase::Writing(done_keys, read) => {
                done(answer)?;
                self.phase = MatchPhase::Deleting(read);
                Ok(Turn::Ask(Ask::Delete(done_keys)))
            }
            MatchPhase::Deleting(read) => {
                done(answer)?;
                Ok(Turn::Finish(read))
            }
        }
    }
}

/// A sub-plan that counts the queued forward rows of each install, up to a bound.
#[derive(Debug, PartialEq)]
pub(crate) struct Counting {
    left: Vec<Ulid>,
    bound: usize,
    counted: BTreeMap<Ulid, usize>,
}

impl Counting {
    pub(crate) fn new(installs: BTreeSet<Ulid>, bound: usize) -> Self {
        Self {
            left: installs.into_iter().rev().collect(),
            bound,
            counted: BTreeMap::new(),
        }
    }

    /// The page of the next install to count, or `None` once every install is counted.
    pub(crate) fn ask(&self) -> Option<Ask> {
        let install_id = self.left.last()?;
        Some(Ask::Page {
            space: PLUGIN_OUTBOX_KEYSPACE,
            prefix: install_id.to_bytes().to_vec(),
            after: None,
            limit: self.bound,
        })
    }

    pub(crate) fn take(&mut self, answer: Answer) -> Result<(), StoreError> {
        let Answer::Page(rows, _) = answer else {
            return Err(unexpected(answer));
        };
        let install_id = self.left.pop().ok_or_else(|| unexpected(Answer::Done))?;
        self.counted.insert(install_id, rows.len());
        Ok(())
    }

    pub(crate) fn counted(self) -> BTreeMap<Ulid, usize> {
        self.counted
    }
}

/// The forward rows and gap counts of one transaction, with each install's queued count. Gaps
/// are counted once their stored rows are read, see [`Outbox::gap_ask`].
#[derive(Debug, PartialEq)]
pub(crate) struct Outbox {
    now_ms: u64,
    forward: Vec<Row>,
    queued: BTreeMap<Ulid, usize>,
    gaps: Vec<(Vec<u8>, SourceKind, GapReason, Lost)>,
}

/// What a gap counts: one event with its anchor, or a range of unknown size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lost {
    Event(u64),
    Range(u64, u64),
}

impl Outbox {
    /// An outbox over the queued counts of the installs it offers rows to.
    pub(crate) fn new(now_ms: u64, queued: BTreeMap<Ulid, usize>) -> Self {
        Self {
            now_ms,
            forward: Vec::new(),
            queued,
            gaps: Vec::new(),
        }
    }

    /// Queues the event of the journal row `key` for the install at `revision`, or counts a gap
    /// when the row is stale or the install has `bound` rows queued.
    pub(crate) fn offer(
        &mut self,
        (install_id, revision): (Ulid, u64),
        key: &[u8],
        event: &SourceEvent,
        stale: bool,
        bound: usize,
    ) -> Result<(), StoreError> {
        let count = self.queued.get(&install_id).copied().unwrap_or_default();
        let reason = match () {
            () if stale => GapReason::Pruned,
            () if count >= bound => GapReason::Full,
            () => {
                let row = ForwardRow {
                    install_id,
                    revision,
                    event: event.clone(),
                    queued_ms: self.now_ms,
                };
                let key = outbox_key(install_id, key);
                self.forward.push(row_of(PLUGIN_OUTBOX_KEYSPACE, key, encode(&row)?));
                self.queued.insert(install_id, count + 1);
                return Ok(());
            }
        };
        self.gap(install_id, (event.kind, event.anchor_ms), reason);
        Ok(())
    }

    /// Counts one undelivered event of `kind` with the anchor `anchor_ms`.
    pub(crate) fn gap(
        &mut self,
        install_id: Ulid,
        (kind, anchor_ms): (SourceKind, u64),
        reason: GapReason,
    ) {
        let key = gap_key(install_id, kind, reason);
        self.gaps.push((key, kind, reason, Lost::Event(anchor_ms)));
    }

    /// Counts a lost range of events of unknown size between two times.
    pub(crate) fn lost_range(
        &mut self,
        install_id: Ulid,
        (kind, reason): (SourceKind, GapReason),
        (from_ms, to_ms): (u64, u64),
    ) {
        let key = gap_key(install_id, kind, reason);
        self.gaps.push((key, kind, reason, Lost::Range(from_ms, to_ms)));
    }

    /// The read of every gap row the outbox counts in, each once.
    pub(crate) fn gap_ask(&self) -> Ask {
        let keys: BTreeSet<_> = self.gaps.iter().map(|(key, ..)| key.clone()).collect();
        Ask::Read(keys.into_iter().map(|key| (PLUGIN_GAPS_KEYSPACE, key)).collect())
    }

    /// The forward rows and the counted gap rows, from the stored gap rows `values` that
    /// [`Outbox::gap_ask`] read.
    pub(crate) fn rows(self, values: Vec<Option<Vec<u8>>>) -> Result<Vec<Row>, StoreError> {
        let keys: BTreeSet<_> = self.gaps.iter().map(|(key, ..)| key.clone()).collect();
        if keys.len() != values.len() {
            let counts = (values.len(), keys.len());
            return Err(StoreError(format!("{} gap rows for {} keys", counts.0, counts.1)));
        }
        let mut stored = BTreeMap::new();
        for (key, value) in keys.into_iter().zip(values) {
            stored.insert(key, value.map(|bytes| decode::<GapRow>(&bytes)).transpose()?);
        }
        let mut gaps: BTreeMap<Vec<u8>, GapRow> = BTreeMap::new();
        for (key, kind, reason, lost) in self.gaps {
            let kept = stored.get_mut(&key).and_then(Option::take);
            let row = (gaps.entry(key)).or_insert_with(|| kept.unwrap_or(GapRow::new(kind, reason)));
            match lost {
                Lost::Event(anchor_ms) => row.add(anchor_ms),
                Lost::Range(from_ms, to_ms) => row.add_range(from_ms, to_ms),
            }
        }
        let mut rows = self.forward;
        for (key, row) in gaps {
            rows.push(row_of(PLUGIN_GAPS_KEYSPACE, key, encode(&row)?));
        }
        Ok(rows)
    }
}

#[cfg(test)]
#[path = "matcher_tests.rs"]
pub(crate) mod tests;
