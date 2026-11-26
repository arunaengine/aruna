//! Matches this node's pending journal rows to the active installs with event triggers and
//! queues forward rows for their receivers, or counts a gap where a row cannot be queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{EVENT_JOURNAL_KEYSPACE, PLUGIN_GAPS_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_runs::{
    AUTO_PENDING_ROW, ForwardRow, GapReason, GapRow, JOURNAL_RETENTION_MS, JOURNAL_ROW,
    PENDING_ROW, SourceEvent, SourceKind, gap_key, outbox_key, source_key,
};
use aruna_core::types::GroupId;
use aruna_core::user::id::UserId;
use aruna_plugin_contract::trigger::TriggerKind;
use ulid::Ulid;

use super::journal::JOURNAL_PAGE;
use super::rows::{
    Row, Rows, StoreError, all_values, decode, delete_call, done, encode, page_call, page_of,
    read_call, row as row_of, unexpected,
};
use aruna_ops_identity::auth::install_access::covers;

/// An active install with event triggers, as the matcher sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub install_id: Ulid,
    pub group_id: GroupId,
    pub revision: u64,
    pub coverage: BTreeMap<TriggerKind, BTreeSet<String>>,
    /// The install's account; datasets it wrote start no run of the install.
    pub account: UserId,
}

impl Target {
    /// Whether the install's trigger of the event's kind covers the event's path, and the
    /// event is not a dataset or object write of the install itself.
    pub fn matches(&self, event: &SourceEvent) -> bool {
        let Some(trigger) = event_trigger(event.kind) else {
            return false;
        };
        let own = matches!(
            event.kind,
            SourceKind::DatasetCreated | SourceKind::DatasetUpdated | SourceKind::DataUploaded
        );
        self.group_id == event.group_id
            && !(own && event.actor == Some(self.account))
            && (self.coverage.get(&trigger))
                .is_some_and(|patterns| patterns.iter().any(|pattern| covers(pattern, &event.path)))
    }
}

/// The manifest trigger a journaled event kind starts, when plugins subscribe to it.
pub(crate) fn event_trigger(kind: SourceKind) -> Option<TriggerKind> {
    Some(match kind {
        SourceKind::DatasetCreated => TriggerKind::DatasetCreated,
        SourceKind::DataUploaded => TriggerKind::DataUploaded,
        SourceKind::DatasetUpdated => TriggerKind::DatasetUpdated,
        SourceKind::SyncCompleted => TriggerKind::SyncCompleted,
        SourceKind::SyncFailed => TriggerKind::SyncFailed,
        SourceKind::JobFinished => TriggerKind::JobFinished,
        SourceKind::AppletSubmission | SourceKind::RunFinished => return None,
    })
}

/// The pending rows `keys` in one transaction, each queued for the installs it matches with at
/// most `bound` forward rows per install or counted as a gap, then deleted. Answers how many were
/// still there; only these keys join the read set, so a row journaled meanwhile never conflicts.
pub(crate) fn match_page(
    (targets, keep): (&[Target], bool),
    now_ms: u64,
    bound: usize,
    keys: Vec<Vec<u8>>,
) -> MatchPage {
    MatchPage {
        targets: targets.to_vec(),
        keep,
        now_ms,
        bound,
        keys,
        phase: MatchPhase::Reading,
    }
}

#[derive(Debug, PartialEq)]
pub struct MatchPage {
    targets: Vec<Target>,
    keep: bool,
    now_ms: u64,
    bound: usize,
    keys: Vec<Vec<u8>>,
    phase: MatchPhase,
}

type DoneKeys = Vec<(&'static str, Vec<u8>)>;

#[derive(Debug, PartialEq)]
enum MatchPhase {
    Reading,
    Counting(Vec<PendingRow>, Counting),
    Gaps(Box<Outbox>, DoneKeys, usize),
    Writing(DoneKeys, usize),
    Deleting(usize),
}

/// A pending row decoded once: its event, when it decodes, and the installs it matches.
#[derive(Debug, PartialEq)]
struct PendingRow {
    key: Vec<u8>,
    event: Option<SourceEvent>,
    installs: Vec<(Ulid, u64)>,
}

impl MatchPage {
    /// Decodes each row and matches it to the targets; a user node keeps no journal, so it
    /// matches nothing.
    fn pending(&self, rows: Rows) -> Vec<PendingRow> {
        let row = |(key, value): (Vec<u8>, Vec<u8>)| {
            let event = SourceEvent::from_bytes(&value).ok();
            let matched = (event.as_ref()).filter(|_| self.keep).map(|event| {
                let targets = self.targets.iter().filter(|target| target.matches(event));
                let installs = targets.map(|target| (target.install_id, target.revision));
                installs.collect()
            });
            let installs = matched.unwrap_or_default();
            PendingRow {
                key,
                event,
                installs,
            }
        };
        rows.into_iter().map(row).collect()
    }

    /// The outbox of the page's matches once every touched install is counted.
    fn matched(
        &mut self,
        rows: Vec<PendingRow>,
        queued: BTreeMap<Ulid, usize>,
    ) -> Result<Next<usize>, StoreError> {
        let stale = self.now_ms.saturating_sub(JOURNAL_RETENTION_MS);
        let (mut done, mut outbox) = (Vec::new(), Outbox::new(self.now_ms, queued));
        let read = rows.len();
        for PendingRow {
            key,
            event,
            installs,
        } in rows
        {
            if !self.keep {
                let kept = [&[JOURNAL_ROW][..], &key[1..]].concat();
                let has_auto = event.as_ref().is_some_and(|e| {
                    matches!(
                        e.kind,
                        SourceKind::DataUploaded
                            | SourceKind::AppletSubmission
                            | SourceKind::RunFinished
                    )
                });
                let auto = has_auto.then(|| [&[AUTO_PENDING_ROW][..], &key[1..]].concat());
                let source = event.map(|event| source_key(&event.source));
                done.push((EVENT_JOURNAL_KEYSPACE, key));
                done.push((EVENT_JOURNAL_KEYSPACE, kept));
                done.extend(auto.map(|key| (EVENT_JOURNAL_KEYSPACE, key)));
                done.extend(source.map(|key| (EVENT_JOURNAL_KEYSPACE, key)));
                continue;
            }
            if let Some(event) = &event {
                for install in installs {
                    let stale = event.journaled_ms < stale;
                    outbox.offer(install, &key, event, stale, self.bound)?;
                }
            }
            done.push((EVENT_JOURNAL_KEYSPACE, key));
        }
        if !outbox.counts_gaps() {
            return self.write(outbox.rows(Vec::new())?, done, read);
        }
        let ask = outbox.gap_ask();
        self.phase = MatchPhase::Gaps(Box::new(outbox), done, read);
        Ok(Next::Call(ask))
    }

    /// Writes the page's forward and gap rows, then deletes its done rows; nothing to write
    /// goes straight to the delete.
    fn write(
        &mut self,
        rows: Vec<Row>,
        done: DoneKeys,
        read: usize,
    ) -> Result<Next<usize>, StoreError> {
        if rows.is_empty() {
            self.phase = MatchPhase::Deleting(read);
            return Ok(Next::Call(delete_call(done)));
        }
        self.phase = MatchPhase::Writing(done, read);
        Ok(Next::Call(IoCall::Write(rows)))
    }
}

impl Operation for MatchPage {
    type Output = usize;
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        let keys = (self.keys.iter()).map(|key| (EVENT_JOURNAL_KEYSPACE, key.clone()));
        Ok(Next::Call(read_call(keys.collect())))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, StoreError> {
        match std::mem::replace(&mut self.phase, MatchPhase::Deleting(0)) {
            MatchPhase::Reading => {
                // A row another page matched meanwhile is gone; it is not matched twice.
                let keys = std::mem::take(&mut self.keys).into_iter();
                let rows: Rows = (keys.zip(all_values(answer)?))
                    .filter_map(|(key, value)| Some((key, value?)))
                    .collect();
                if rows.is_empty() {
                    return Ok(Next::Done(0));
                }
                let rows = self.pending(rows);
                let installs = (rows.iter())
                    .flat_map(|row| row.installs.iter().map(|(install_id, _)| *install_id))
                    .collect();
                let counting = Counting::new(installs, self.bound);
                match counting.ask() {
                    Some(ask) => {
                        self.phase = MatchPhase::Counting(rows, counting);
                        Ok(Next::Call(ask))
                    }
                    None => self.matched(rows, BTreeMap::new()),
                }
            }
            MatchPhase::Counting(rows, mut counting) => {
                counting.take(answer)?;
                match counting.ask() {
                    Some(ask) => {
                        self.phase = MatchPhase::Counting(rows, counting);
                        Ok(Next::Call(ask))
                    }
                    None => self.matched(rows, counting.counted()),
                }
            }
            MatchPhase::Gaps(outbox, done, read) => {
                let values = all_values(answer)?;
                self.write(outbox.rows(values)?, done, read)
            }
            MatchPhase::Writing(done_keys, read) => {
                done(answer)?;
                self.phase = MatchPhase::Deleting(read);
                Ok(Next::Call(delete_call(done_keys)))
            }
            MatchPhase::Deleting(read) => {
                done(answer)?;
                Ok(Next::Done(read))
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
    pub(crate) fn ask(&self) -> Option<IoCall> {
        let install_id = self.left.last()?;
        Some(page_call(
            PLUGIN_OUTBOX_KEYSPACE,
            install_id.to_bytes().to_vec(),
            None,
            self.bound,
        ))
    }

    pub(crate) fn take(&mut self, answer: IoResult) -> Result<(), StoreError> {
        let (rows, _) = page_of(answer)?;
        let install_id = self.left.pop().ok_or_else(|| unexpected(IoResult::Done))?;
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
                self.forward
                    .push(row_of(PLUGIN_OUTBOX_KEYSPACE, key, encode(&row)?));
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
        self.gaps
            .push((key, kind, reason, Lost::Range(from_ms, to_ms)));
    }

    /// Whether the outbox counts any gap, so [`Outbox::gap_ask`] reads a row.
    pub(crate) fn counts_gaps(&self) -> bool {
        !self.gaps.is_empty()
    }

    /// The read of every gap row the outbox counts in, each once.
    pub(crate) fn gap_ask(&self) -> IoCall {
        let keys: BTreeSet<_> = self.gaps.iter().map(|(key, ..)| key.clone()).collect();
        read_call(
            keys.into_iter()
                .map(|key| (PLUGIN_GAPS_KEYSPACE, key))
                .collect(),
        )
    }

    /// The forward rows and the counted gap rows, from the stored gap rows `values` that
    /// [`Outbox::gap_ask`] read.
    pub(crate) fn rows(self, values: Vec<Option<Vec<u8>>>) -> Result<Vec<Row>, StoreError> {
        let keys: BTreeSet<_> = self.gaps.iter().map(|(key, ..)| key.clone()).collect();
        if keys.len() != values.len() {
            let counts = (values.len(), keys.len());
            return Err(StoreError::Message(format!(
                "{} gap rows for {} keys",
                counts.0, counts.1
            )));
        }
        let mut stored = BTreeMap::new();
        for (key, value) in keys.into_iter().zip(values) {
            stored.insert(
                key,
                value.map(|bytes| decode::<GapRow>(&bytes)).transpose()?,
            );
        }
        let mut gaps: BTreeMap<Vec<u8>, GapRow> = BTreeMap::new();
        for (key, kind, reason, lost) in self.gaps {
            let kept = stored.get_mut(&key).and_then(Option::take);
            let row =
                (gaps.entry(key)).or_insert_with(|| kept.unwrap_or(GapRow::new(kind, reason)));
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

/// Matches every pending journal row with at most `bound` forward rows per install, a page per
/// transaction; answers how many rows were read.
#[derive(Debug, PartialEq)]
pub struct MatchJournal {
    targets: Vec<Target>,
    keep: bool,
    now_ms: u64,
    bound: usize,
    read: usize,
    /// The size of the scanned page while its match runs; `None` while scanning.
    scanned: Option<usize>,
}

impl MatchJournal {
    pub fn new((targets, keep): (Vec<Target>, bool), now_ms: u64, bound: usize) -> Self {
        Self {
            targets,
            keep,
            now_ms,
            bound,
            read: 0,
            scanned: None,
        }
    }

    /// Scans the next pending page outside any transaction, so rows journaled meanwhile never
    /// conflict the match.
    fn scan(&mut self) -> IoCall {
        self.scanned = None;
        page_call(
            EVENT_JOURNAL_KEYSPACE,
            vec![PENDING_ROW],
            None,
            JOURNAL_PAGE,
        )
    }

    /// Matches the scanned keys; a page that loses a conflict runs again after a backoff.
    fn page(&self, keys: Vec<Vec<u8>>) -> IoCall {
        let (targets, keep, now_ms, bound) =
            (self.targets.clone(), self.keep, self.now_ms, self.bound);
        let build = move || match_page((&targets[..], keep), now_ms, bound, keys.clone());
        IoCall::retry_with(build, &now_ms.to_le_bytes())
    }
}

impl Operation for MatchJournal {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        Ok(Next::Call(self.scan()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, StoreError> {
        let Some(scanned) = self.scanned else {
            let (rows, _) = page_of(answer)?;
            if rows.is_empty() {
                return Ok(Next::Done(self.read));
            }
            self.scanned = Some(rows.len());
            let keys = rows.into_iter().map(|(key, _)| key).collect();
            return Ok(Next::Call(self.page(keys)));
        };
        self.read += answer.child::<usize, StoreError>()??;
        Ok(match scanned < JOURNAL_PAGE {
            true => Next::Done(self.read),
            false => Next::Call(self.scan()),
        })
    }
}

#[cfg(test)]
#[path = "matcher_tests.rs"]
pub(crate) mod tests;
