//! Decides forwarded plugin events on the install's receiver, once per install, trigger and
//! source id: a run is queued, the event waits for its install record, or it is denied.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use aruna_core::keyspaces::{
    PLUGIN_GAPS_KEYSPACE, PLUGIN_QUEUE_KEYSPACE, PLUGIN_REPLAYS_KEYSPACE, PLUGIN_SEEN_KEYSPACE,
    PLUGIN_WAITING_KEYSPACE,
};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{
    DEDUP_RETENTION_MS, Decisions, EventBatch, ForwardRow, GapReason, GapRow, PluginRun,
    RECEIVER_GAP, RUNS_QUEUED, SourceKind, WAITING_ROW, queue_key, reported_key,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugins::broker::ErrorKind;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::account::{AccountError, PrerequisitesHeld};
use super::check::{CallError, CheckCall, RunScope};
use super::matcher::event_trigger;
use super::replay::{COVERAGE_ROW, CoverageRow, row_key};
use super::rows::{
    Row, Rows, Scan, StoreError, all_values, decode, delete_call, done, encode, page_call, page_of,
    read_call, row as row_of, unexpected, values,
};
use super::runs::{Admission, RunError, event_run};
use aruna_core::plugin_runs::Outcome;

/// The decision a resend of an event gets, with its anchor for the dedup retention.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeenRow {
    pub outcome: Outcome,
    pub anchor_ms: u64,
    pub decided_ms: u64,
}

/// An event that waits on this receiver for its install record or a prerequisite write.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingRow {
    pub row: ForwardRow,
    pub received_ms: u64,
}

/// Whether an event can be decided now, and how.
#[derive(Debug, PartialEq)]
pub(crate) enum Verdict {
    /// The install record or a prerequisite write is not here yet.
    Unready,
    Run(Box<PluginRun>),
    Denied(String),
    /// The check could not read what it needs; nothing is decided.
    Unavailable,
}

/// Key of an event's dedup record: install, trigger kind, then the source id.
pub(crate) fn seen_key(install_id: Ulid, kind: SourceKind, source: &str) -> Vec<u8> {
    let mut key = install_id.to_bytes().to_vec();
    key.push(kind as u8);
    key.extend_from_slice(source.as_bytes());
    key
}

/// The outcome an earlier decision recorded for the event.
pub(crate) fn seen_outcome(row: &ForwardRow) -> SeenOutcome {
    let seen = seen_key(row.install_id, row.event.kind, &row.event.source);
    SeenOutcome(seen)
}

#[derive(Debug, PartialEq)]
pub struct SeenOutcome(Vec<u8>);

impl Operation for SeenOutcome {
    type Output = Option<Outcome>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(read_call(vec![(
            PLUGIN_SEEN_KEYSPACE,
            self.0.clone(),
        )])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        let [found] = values::<1>(answer)?;
        let seen = found.map(|bytes| decode::<SeenRow>(&bytes)).transpose()?;
        Ok(Next::Done(seen.map(|seen| seen.outcome)))
    }
}

/// Counts an event that came too late in its own gap row.
#[derive(Debug, PartialEq)]
pub struct LateEvent(pub(crate) GapCount);

impl Operation for LateEvent {
    type Output = ();
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, StoreError> {
        Ok(Next::Call(self.0.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<()>, StoreError> {
        match self.0.take(answer)? {
            Some(row) => Ok(Next::Call(IoCall::Write(vec![row]))),
            None => Ok(Next::Done(())),
        }
    }
}

/// Records the event's outcome under `verdict` in one transaction: a queued run, a held queue
/// slot, or a denial. A full queue and an unavailable check are busy and write nothing.
pub(crate) fn decide_event(
    verdict: Verdict,
    row: &ForwardRow,
    now_ms: u64,
) -> Result<DecideEvent, StoreError> {
    let phase = match verdict {
        Verdict::Unavailable => DecidePhase::Busy,
        Verdict::Run(run) => DecidePhase::Admitting(Admission::new(*run)),
        Verdict::Unready => DecidePhase::Holding(Hold::new(row, now_ms)?),
        Verdict::Denied(reason) => DecidePhase::Denying(reason),
    };
    Ok(DecideEvent {
        row: Box::new(row.clone()),
        now_ms,
        phase,
    })
}

#[derive(Debug, PartialEq)]
pub struct DecideEvent {
    row: Box<ForwardRow>,
    now_ms: u64,
    phase: DecidePhase,
}

#[derive(Debug, PartialEq)]
enum DecidePhase {
    Busy,
    Admitting(Admission),
    Holding(Hold),
    Denying(String),
    /// The rows of this outcome are being written.
    Writing(Outcome),
}

impl DecideEvent {
    fn decided(
        &mut self,
        outcome: Outcome,
        mut rows: Vec<Row>,
    ) -> Result<Next<Outcome>, StoreError> {
        rows.push(seen_row(&self.row, &outcome, self.now_ms)?);
        self.phase = DecidePhase::Writing(outcome);
        Ok(Next::Call(IoCall::Write(rows)))
    }
}

impl Operation for DecideEvent {
    type Output = Outcome;
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Outcome>, StoreError> {
        match std::mem::replace(&mut self.phase, DecidePhase::Busy) {
            DecidePhase::Busy => Ok(Next::Done(Outcome::Busy)),
            DecidePhase::Admitting(admission) => {
                let ask = admission.ask();
                self.phase = DecidePhase::Admitting(admission);
                Ok(Next::Call(ask))
            }
            DecidePhase::Holding(hold) => {
                let ask = hold.ask();
                self.phase = DecidePhase::Holding(hold);
                Ok(Next::Call(ask))
            }
            DecidePhase::Denying(reason) => self.decided(Outcome::Denied(reason), Vec::new()),
            DecidePhase::Writing(_) => Err(StoreError("decided twice".into())),
        }
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Outcome>, StoreError> {
        match std::mem::replace(&mut self.phase, DecidePhase::Busy) {
            DecidePhase::Admitting(mut admission) => match admission.take(answer) {
                Ok(None) => {
                    let ask = admission.ask();
                    self.phase = DecidePhase::Admitting(admission);
                    Ok(Next::Call(ask))
                }
                Ok(Some(rows)) => self.decided(Outcome::Queued, rows),
                Err(RunError::Full(_)) => Ok(Next::Done(Outcome::Busy)),
                Err(RunError::Store(error)) => Err(error),
                Err(error) => Err(StoreError(error.to_string())),
            },
            DecidePhase::Holding(mut hold) => match hold.take(answer)? {
                Slot::More(ask) => {
                    self.phase = DecidePhase::Holding(hold);
                    Ok(Next::Call(ask))
                }
                Slot::Full => Ok(Next::Done(Outcome::Busy)),
                Slot::Taken(rows) => self.decided(Outcome::Waiting, rows),
            },
            DecidePhase::Writing(outcome) => {
                done(answer)?;
                Ok(Next::Done(outcome))
            }
            DecidePhase::Busy | DecidePhase::Denying(_) => Err(unexpected(answer)),
        }
    }
}

/// A sub-plan that counts an event this receiver did not run in its own gap row of `reason`.
#[derive(Debug, PartialEq)]
pub struct GapCount {
    key: Vec<u8>,
    kind: SourceKind,
    reason: GapReason,
    anchor_ms: u64,
    counted: bool,
}

impl GapCount {
    pub fn new(local: NodeId, row: &ForwardRow, reason: GapReason) -> Self {
        let (install_id, kind) = (row.install_id, row.event.kind);
        Self {
            key: reported_key(local, install_id, kind, reason),
            kind,
            reason,
            anchor_ms: row.event.anchor_ms,
            counted: false,
        }
    }

    pub fn ask(&self) -> IoCall {
        read_call(vec![(PLUGIN_GAPS_KEYSPACE, self.key.clone())])
    }

    /// The counted gap row after its read, or `None` once its write was answered.
    pub fn take(&mut self, answer: IoResult) -> Result<Option<Row>, StoreError> {
        if self.counted {
            return done(answer).map(|()| None);
        }
        let [found] = values::<1>(answer)?;
        let mut gap = match found {
            Some(bytes) => decode(&bytes)?,
            None => GapRow::new(self.kind, self.reason),
        };
        gap.add(self.anchor_ms);
        self.counted = true;
        Ok(Some(row_of(
            PLUGIN_GAPS_KEYSPACE,
            self.key.clone(),
            encode(&gap)?,
        )))
    }
}

/// Deletes the dedup records past their retention; returns how many.
pub(crate) fn prune_seen(now_ms: u64) -> PruneSeen {
    PruneSeen {
        now_ms,
        after: None,
        deleting: None,
        pruned: 0,
    }
}

#[derive(Debug, PartialEq)]
pub struct PruneSeen {
    now_ms: u64,
    after: Option<Vec<u8>>,
    /// The key to continue after once the deletes of a page are answered; `Some(None)` ends.
    deleting: Option<Option<Vec<u8>>>,
    pruned: usize,
}

impl PruneSeen {
    fn page(&self) -> IoCall {
        page_call(PLUGIN_SEEN_KEYSPACE, Vec::new(), self.after.clone(), 1_000)
    }
}

impl Operation for PruneSeen {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        Ok(Next::Call(self.page()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, StoreError> {
        if let Some(next) = self.deleting.take() {
            done(answer)?;
            return Ok(match next {
                Some(key) => {
                    self.after = Some(key);
                    Next::Call(self.page())
                }
                None => Next::Done(self.pruned),
            });
        }
        let (rows, next) = page_of(answer)?;
        let old: Vec<_> = (rows.into_iter())
            .filter(|(_, value)| {
                decode::<SeenRow>(value).is_ok_and(|seen| {
                    self.now_ms > seen.anchor_ms.saturating_add(DEDUP_RETENTION_MS)
                })
            })
            .map(|(key, _)| (PLUGIN_SEEN_KEYSPACE, key))
            .collect();
        self.pruned += old.len();
        self.deleting = Some(next);
        Ok(Next::Call(delete_call(old)))
    }
}

/// The event's dedup record with its outcome.
pub(crate) fn seen_row(
    row: &ForwardRow,
    outcome: &Outcome,
    now_ms: u64,
) -> Result<Row, StoreError> {
    let key = seen_key(row.install_id, row.event.kind, &row.event.source);
    let record = SeenRow {
        outcome: outcome.clone(),
        anchor_ms: row.event.anchor_ms,
        decided_ms: now_ms,
    };
    Ok(row_of(PLUGIN_SEEN_KEYSPACE, key, encode(&record)?))
}

/// A sub-plan that takes a queue slot for a waiting event, unless the install's queue is full.
#[derive(Debug, PartialEq)]
pub struct Hold {
    key: Vec<u8>,
    waiting: Vec<u8>,
    queued: Scan,
}

/// What a hold needs next, or whether it took the slot.
#[derive(Debug, PartialEq)]
pub enum Slot {
    More(IoCall),
    Full,
    /// The rows of the slot and its waiting event.
    Taken(Vec<Row>),
}

impl Hold {
    pub fn new(row: &ForwardRow, now_ms: u64) -> Result<Self, StoreError> {
        let waiting = WaitingRow {
            row: row.clone(),
            received_ms: now_ms,
        };
        Ok(Self {
            key: queue_key(row.install_id, Ulid::generate()),
            waiting: encode(&waiting)?,
            queued: Scan::new(PLUGIN_QUEUE_KEYSPACE, row.install_id.to_bytes().to_vec()),
        })
    }

    pub fn ask(&self) -> IoCall {
        self.queued.ask()
    }

    pub fn take(&mut self, answer: IoResult) -> Result<Slot, StoreError> {
        let Some(queued) = self.queued.take(answer)? else {
            return Ok(Slot::More(self.queued.ask()));
        };
        if queued.len() >= RUNS_QUEUED {
            return Ok(Slot::Full);
        }
        Ok(Slot::Taken(vec![
            row_of(
                PLUGIN_QUEUE_KEYSPACE,
                self.key.clone(),
                WAITING_ROW.to_vec(),
            ),
            row_of(
                PLUGIN_WAITING_KEYSPACE,
                self.key.clone(),
                std::mem::take(&mut self.waiting),
            ),
        ]))
    }
}

/// Keeps each report from `sender` that is newer than the one kept; answers the sequence kept.
pub(crate) fn keep_reports(sender: NodeId, reports: Vec<(Ulid, GapRow)>) -> KeepReports {
    KeepReports {
        sender,
        reports,
        acked: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct KeepReports {
    sender: NodeId,
    reports: Vec<(Ulid, GapRow)>,
    acked: Option<Vec<(Ulid, SourceKind, GapReason, u64)>>,
}

impl Operation for KeepReports {
    type Output = Vec<(Ulid, SourceKind, GapReason, u64)>;
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        let reads = (self.reports.iter())
            .map(|(install_id, report)| {
                let key = reported_key(self.sender, *install_id, report.kind, report.reason);
                (PLUGIN_GAPS_KEYSPACE, key)
            })
            .collect();
        Ok(Next::Call(read_call(reads)))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        if let Some(acked) = self.acked.take() {
            done(answer)?;
            return Ok(Next::Done(acked));
        }
        let kept = all_values(answer)?;
        let (mut acked, mut writes) = (Vec::new(), Vec::new());
        for ((install_id, report), kept) in std::mem::take(&mut self.reports).into_iter().zip(kept)
        {
            let kept = kept.map(|bytes| decode::<GapRow>(&bytes)).transpose()?;
            let sequence = kept.as_ref().map_or(0, |kept| kept.sequence);
            let (kind, reason) = (report.kind, report.reason);
            acked.push((install_id, kind, reason, sequence.max(report.sequence)));
            if report.sequence > sequence {
                let key = reported_key(self.sender, install_id, kind, reason);
                writes.push(row_of(PLUGIN_GAPS_KEYSPACE, key, encode(&report)?));
            }
        }
        self.acked = Some(acked);
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

/// What the receiver holds about an install's event delivery.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Delivery {
    pub waiting: usize,
    pub oldest_ms: Option<u64>,
    /// Undelivered events by kind and reason, summed over the nodes that counted them.
    pub gaps: Vec<GapRow>,
    /// Why the last replay here started at the earliest kept event.
    pub note: Option<String>,
}

/// The install's waiting events, gap totals and replay note on this receiver.
pub fn event_delivery(install_id: Ulid) -> EventDelivery {
    let prefix = install_id.to_bytes().to_vec();
    EventDelivery {
        install_id,
        phase: DeliveryPhase::Waiting(Scan::new(PLUGIN_WAITING_KEYSPACE, prefix)),
        delivery: Delivery::default(),
    }
}

#[derive(Debug, PartialEq)]
pub struct EventDelivery {
    install_id: Ulid,
    phase: DeliveryPhase,
    delivery: Delivery,
}

#[derive(Debug, PartialEq)]
enum DeliveryPhase {
    Waiting(Scan),
    Gaps(Scan),
    Coverage,
}

impl Operation for EventDelivery {
    type Output = Delivery;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Delivery>, StoreError> {
        match &self.phase {
            DeliveryPhase::Waiting(scan) => Ok(Next::Call(scan.ask())),
            _ => Err(StoreError("read twice".into())),
        }
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Delivery>, StoreError> {
        match std::mem::replace(&mut self.phase, DeliveryPhase::Coverage) {
            DeliveryPhase::Waiting(mut scan) => {
                let Some(waiting) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = DeliveryPhase::Waiting(scan);
                    return Ok(Next::Call(ask));
                };
                let received = (waiting.iter())
                    .filter_map(|(_, value)| decode::<WaitingRow>(value).ok())
                    .map(|waiting| waiting.received_ms);
                self.delivery.oldest_ms = received.min();
                self.delivery.waiting = waiting.len();
                let scan = Scan::new(PLUGIN_GAPS_KEYSPACE, vec![RECEIVER_GAP]);
                let ask = scan.ask();
                self.phase = DeliveryPhase::Gaps(scan);
                Ok(Next::Call(ask))
            }
            DeliveryPhase::Gaps(mut scan) => {
                let Some(gaps) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = DeliveryPhase::Gaps(scan);
                    return Ok(Next::Call(ask));
                };
                self.delivery.gaps = gap_totals(self.install_id, gaps)?;
                let coverage = row_key(COVERAGE_ROW, self.install_id);
                Ok(Next::Call(read_call(vec![(
                    PLUGIN_REPLAYS_KEYSPACE,
                    coverage,
                )])))
            }
            DeliveryPhase::Coverage => {
                let [coverage] = values::<1>(answer)?;
                let coverage = coverage.and_then(|bytes| decode::<CoverageRow>(&bytes).ok());
                self.delivery.note = coverage.and_then(|row| row.note);
                Ok(Next::Done(std::mem::take(&mut self.delivery)))
            }
        }
    }
}

/// The undelivered events of an install among the receiver's gap `rows`, summed by kind and
/// reason over the nodes that counted them.
fn gap_totals(install_id: Ulid, rows: Rows) -> Result<Vec<GapRow>, StoreError> {
    let prefix = install_id.to_bytes();
    let mut totals: BTreeMap<(SourceKind, GapReason), GapRow> = BTreeMap::new();
    for (key, value) in rows {
        if key.get(33..49) != Some(&prefix[..]) {
            continue;
        }
        let row: GapRow = decode(&value)?;
        let total = (totals.entry((row.kind, row.reason)))
            .or_insert_with(|| GapRow::new(row.kind, row.reason));
        total.count += row.count;
        total.unknown |= row.unknown;
        total.earliest_ms = total.earliest_ms.min(row.earliest_ms);
        total.latest_ms = total.latest_ms.max(row.latest_ms);
    }
    Ok(totals.into_values().collect())
}

#[derive(Debug, PartialEq)]
enum JudgePhase {
    Prerequisites,
    Checking(Box<PluginRun>),
}

/// Whether the local install record and its prerequisite writes are here, then steps 0 to 4 for
/// the event's path as a new run of the install would pass them.
#[derive(Debug, PartialEq)]
pub(crate) struct Judge {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    row: ForwardRow,
    now_ms: u64,
    phase: JudgePhase,
}

impl Judge {
    pub(crate) fn new(
        config: Arc<RealmConfigDocument>,
        local: NodeId,
        row: ForwardRow,
        now_ms: u64,
    ) -> Self {
        Self {
            config,
            local,
            row,
            now_ms,
            phase: JudgePhase::Prerequisites,
        }
    }

    fn install(&self) -> Option<&PluginInstall> {
        let found = (self.config.installs.iter()).find(|i| i.install_id == self.row.install_id);
        found.filter(|install| install.revision >= self.row.revision)
    }

    fn check(&mut self) -> Result<Next<Verdict>, StoreError> {
        let Some(install) = self.install() else {
            return Ok(Next::Done(Verdict::Unready));
        };
        let event = &self.row.event;
        let Some(run) = event_run(&self.config, install, event, self.now_ms) else {
            let refused = "the install has no trigger of this kind".to_string();
            return Ok(Next::Done(Verdict::Denied(refused)));
        };
        let scope = RunScope {
            realm_id: self.config.realm_id,
            local: self.local,
            run: run.clone(),
        };
        let needs = event_trigger(event.kind).needs();
        let target = needs.map(|permission| (permission, event.path.as_str()));
        let check = IoCall::child(CheckCall::new(&scope, target));
        self.phase = JudgePhase::Checking(Box::new(run));
        Ok(Next::Call(check))
    }
}

impl Operation for Judge {
    type Output = Verdict;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Verdict>, StoreError> {
        Ok(match self.install() {
            Some(install) => Next::Call(IoCall::child(PrerequisitesHeld::new(install))),
            None => Next::Done(Verdict::Unready),
        })
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Verdict>, StoreError> {
        match std::mem::replace(&mut self.phase, JudgePhase::Prerequisites) {
            JudgePhase::Prerequisites => {
                let held = answer.child::<bool, AccountError>()?;
                match held.map_err(|error| StoreError(error.0))? {
                    true => self.check(),
                    false => Ok(Next::Done(Verdict::Unready)),
                }
            }
            JudgePhase::Checking(run) => {
                let checked = answer.child::<PluginInstall, CallError>()?;
                Ok(Next::Done(match checked {
                    Ok(_) => Verdict::Run(run),
                    Err(CallError(error)) if error.kind == ErrorKind::Unavailable => {
                        Verdict::Unavailable
                    }
                    Err(CallError(error)) => Verdict::Denied(error.message),
                }))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum RowPhase {
    Seen,
    Late,
    Judging,
    Deciding,
}

/// One event's outcome. Busy writes nothing, so the sender keeps the row and sends it again.
#[derive(Debug, PartialEq)]
pub(crate) struct DecideRow {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    row: ForwardRow,
    now_ms: u64,
    phase: RowPhase,
}

impl DecideRow {
    fn new(config: Arc<RealmConfigDocument>, local: NodeId, row: ForwardRow, now_ms: u64) -> Self {
        Self {
            config,
            local,
            row,
            now_ms,
            phase: RowPhase::Seen,
        }
    }
}

impl Operation for DecideRow {
    type Output = Outcome;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Outcome>, StoreError> {
        Ok(Next::Call(IoCall::child(seen_outcome(&self.row))))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Outcome>, StoreError> {
        match self.phase {
            RowPhase::Seen => {
                if let Some(outcome) = answer.child::<Option<Outcome>, StoreError>()?? {
                    return Ok(Next::Done(outcome));
                }
                // Its dedup record would be gone, so a run now could repeat one already made.
                let kept_until = self.row.event.anchor_ms.saturating_add(DEDUP_RETENTION_MS);
                if self.now_ms > kept_until {
                    self.phase = RowPhase::Late;
                    let late = GapCount::new(self.local, &self.row, GapReason::Late);
                    return Ok(Next::Call(IoCall::child(LateEvent(late))));
                }
                self.phase = RowPhase::Judging;
                let config = self.config.clone();
                let judge = Judge::new(config, self.local, self.row.clone(), self.now_ms);
                Ok(Next::Call(IoCall::child(judge)))
            }
            RowPhase::Late => {
                answer.child::<(), StoreError>()??;
                Ok(Next::Done(Outcome::Expired))
            }
            RowPhase::Judging => {
                let verdict = answer.child::<Verdict, StoreError>()??;
                self.phase = RowPhase::Deciding;
                let decide = decide_event(verdict, &self.row, self.now_ms)?;
                Ok(Next::Call(IoCall::child(decide)))
            }
            RowPhase::Deciding => Ok(Next::Done(answer.child::<Outcome, StoreError>()??)),
        }
    }
}

/// Decides each event of a batch from `sender` and keeps its newest gap reports. The caller holds
/// `ADMISSIONS` while it runs.
#[derive(Debug, PartialEq)]
pub(crate) struct DecideBatch {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    sender: NodeId,
    now_ms: u64,
    events: VecDeque<ForwardRow>,
    gaps: Vec<(Ulid, GapRow)>,
    current: Option<(Ulid, String)>,
    decisions: Decisions,
}

impl DecideBatch {
    pub(crate) fn new(
        config: Arc<RealmConfigDocument>,
        (local, sender): (NodeId, NodeId),
        batch: EventBatch,
        now_ms: u64,
    ) -> Self {
        Self {
            config,
            local,
            sender,
            now_ms,
            events: batch.events.into(),
            gaps: batch.gaps,
            current: None,
            decisions: Decisions::default(),
        }
    }

    fn advance(&mut self) -> IoCall {
        let Some(row) = self.events.pop_front() else {
            self.current = None;
            let gaps = std::mem::take(&mut self.gaps);
            return IoCall::child(keep_reports(self.sender, gaps));
        };
        self.current = Some((row.install_id, row.event.source.clone()));
        let config = self.config.clone();
        IoCall::child(DecideRow::new(config, self.local, row, self.now_ms))
    }
}

impl Operation for DecideBatch {
    type Output = Decisions;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Decisions>, StoreError> {
        Ok(Next::Call(self.advance()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Decisions>, StoreError> {
        let Some((install_id, source)) = self.current.take() else {
            let kept = answer.child::<Vec<(Ulid, SourceKind, GapReason, u64)>, StoreError>()??;
            self.decisions.gaps = kept;
            return Ok(Next::Done(std::mem::take(&mut self.decisions)));
        };
        let outcome = answer.child::<Outcome, StoreError>()??;
        self.decisions.events.push((install_id, source, outcome));
        Ok(Next::Call(self.advance()))
    }
}

#[cfg(test)]
#[path = "decide_tests.rs"]
mod tests;
