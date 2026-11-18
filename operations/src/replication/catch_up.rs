//! Works on the sets that name catching-up members or new range copies, within the drain's page
//! budget; a coordinator whose walks all ended states the catch-up to the record's publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent, StatementSignEvent};
use aruna_core::keyspaces::{BOOTSTRAP_KEYSPACE, CATCH_UP_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::bootstrap::{BootstrapCursor, bootstrap_key};
use aruna_core::structs::storage::distribution::snapshot::{DistributionView, RangeFill};
use aruna_core::structs::storage::distribution::{
    DistributionMember, DistributionSnapshot, KeyRange, Statement, StatementClaim,
};
use aruna_core::types::{Key, Value};
use tracing::{debug, warn};
use ulid::Ulid;

use super::bootstrap::{
    BootstrapError, BootstrapPage, ClearCursorsOperation, PAGE_ROWS, PageConfig, PageProgress,
    ReadCursorOperation,
};
use super::members::{ReadRecordOperation, RecordError};
use super::queue::{BlobQueueError, schedule_blob_drain};
use crate::distribution::forward::PublishRefusal;
use crate::distribution::lifecycle::SettleMarkerOperation;
use crate::distribution::purge_steps::PurgeError;
use crate::distribution::submit::SubmitOperation;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// Sets and pages one drain works on at most.
const SET_BATCH: usize = 16;
const PAGE_BUDGET: usize = 8;

/// Rows one drain deletes at most while it clears the cursors of a settled set.
const CLEAR_BATCH: usize = 1024;

/// How soon the drain looks again while a bootstrap waits for its jobs.
pub const BOOTSTRAP_POLL: Duration = Duration::from_secs(5);

/// The set after which the next drain continues; a restart begins at the first set.
static SET_CURSOR: Mutex<Option<Vec<u8>>> = Mutex::new(None);

/// The range each walk of a set and member continues at, so later ranges get their turn.
static RANGE_TURNS: Mutex<Option<HashMap<(Ulid, Ulid), usize>>> = Mutex::new(None);

type Turn<T> = Result<Next<T>, BlobQueueError>;
/// What a walk leaves: its outcome and the page budget still unused.
type Walked = (Outcome, usize);

fn failed(error: impl std::fmt::Display) -> BlobQueueError {
    BlobQueueError::UnexpectedEvent(error.to_string())
}

fn catch_up_page(after: Option<Vec<u8>>, limit: usize) -> IoCall {
    IoCall::Page {
        space: CATCH_UP_KEYSPACE.to_string(),
        prefix: Key::from(Vec::new()),
        after: after.map(Into::into),
        limit,
    }
}

/// The range index a walk continues at; with `next` it records where the next walk starts.
fn turn(walk: (Ulid, Ulid), next: Option<usize>) -> usize {
    let Ok(mut turns) = RANGE_TURNS.lock() else {
        return 0;
    };
    let turns = turns.get_or_insert_with(HashMap::new);
    match next {
        Some(index) => {
            turns.insert(walk, index);
            index
        }
        None => turns.get(&walk).copied().unwrap_or(0),
    }
}

/// Whether any set still waits for its bootstrap here.
#[derive(Debug, PartialEq)]
pub struct BootstrapWaitingOperation;

impl Operation for BootstrapWaitingOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(catch_up_page(None, 1)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        Ok(Next::Done(!result.page()?.0.is_empty()))
    }
}

/// Wakes the replication drain when a set waits for its bootstrap.
#[derive(Debug, Default, PartialEq)]
pub struct WakeBootstrapOperation {
    waking: bool,
}

impl Operation for WakeBootstrapOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::child(BootstrapWaitingOperation)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.waking {
            result.event()?;
            return Ok(Next::Done(()));
        }
        if !result.child::<bool, IoError>()?.unwrap_or(false) {
            return Ok(Next::Done(()));
        }
        self.waking = true;
        Ok(Next::Call(schedule_blob_drain().into()))
    }
}

#[derive(Debug, PartialEq)]
enum DrainStep {
    Page,
    Dropped,
    Walk(Key, Value, Ulid),
    Settled,
}

/// Works on the sets that name catching-up members, within the drain's page budget. Outputs
/// whether some bootstrap still waits, so the drain looks again soon.
#[derive(Debug, PartialEq)]
pub struct ProcessBootstrapOperation {
    serving: Option<(NodeId, RealmId)>,
    budget: usize,
    waiting: bool,
    rows: VecDeque<(Key, Value)>,
    step: DrainStep,
}

impl ProcessBootstrapOperation {
    pub fn new(serving: Option<(NodeId, RealmId)>) -> Self {
        Self {
            serving,
            budget: PAGE_BUDGET,
            waiting: false,
            rows: VecDeque::new(),
            step: DrainStep::Page,
        }
    }

    fn next_set(&mut self) -> Turn<bool> {
        let Some((key, seen)) = self.rows.pop_front() else {
            return Ok(Next::Done(self.waiting));
        };
        let set_id = <[u8; 16]>::try_from(&key[..]).map(Ulid::from_bytes);
        let (Ok(set_id), Some((local, realm_id))) = (set_id, self.serving) else {
            self.step = DrainStep::Dropped;
            let row = (CATCH_UP_KEYSPACE.to_string(), key);
            return Ok(Next::Call(IoCall::Delete(vec![row])));
        };
        let walk = SetWalk {
            realm_id,
            local,
            set_id,
        };
        let run = SetWalkOperation::new(walk, self.serving, self.budget);
        self.step = DrainStep::Walk(key, seen, set_id);
        Ok(Next::Call(IoCall::child(run)))
    }
}

impl Operation for ProcessBootstrapOperation {
    type Output = bool;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<bool> {
        if self.serving.is_none() {
            return Ok(Next::Done(false));
        }
        // Sets after the last batch come next, so waiting sets never starve later ones.
        let after = SET_CURSOR
            .lock()
            .map(|cursor| cursor.clone())
            .unwrap_or(None);
        Ok(Next::Call(catch_up_page(after, SET_BATCH)))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match std::mem::replace(&mut self.step, DrainStep::Dropped) {
            DrainStep::Page => {
                let (rows, _) = result.page()?;
                let next = (rows.len() == SET_BATCH)
                    .then(|| rows.last().map(|(key, _)| key.to_vec()))
                    .flatten();
                // A pass that stopped short of the end looks again soon.
                self.waiting |= next.is_some();
                if let Ok(mut cursor) = SET_CURSOR.lock() {
                    *cursor = next;
                }
                self.rows = rows.into();
                self.next_set()
            }
            DrainStep::Dropped => {
                result.done()?;
                self.next_set()
            }
            DrainStep::Walk(key, seen, set_id) => {
                match result.child::<Walked, BlobQueueError>()? {
                    Ok((outcome, budget)) => {
                        self.budget = budget;
                        match outcome {
                            Outcome::Settled => {
                                let settle = SettleMarkerOperation::new(
                                    CATCH_UP_KEYSPACE,
                                    key.to_vec(),
                                    seen.to_vec(),
                                );
                                self.step = DrainStep::Settled;
                                return Ok(Next::Call(IoCall::child(settle)));
                            }
                            Outcome::Waiting => self.waiting = true,
                            Outcome::Stated => {}
                        }
                    }
                    Err(error) => {
                        warn!(%set_id, %error, "Distributed bucket bootstrap failed; retrying");
                        self.waiting = true;
                    }
                }
                self.next_set()
            }
            DrainStep::Settled => {
                (result.child::<(), PurgeError>()?).map_err(failed)?;
                self.next_set()
            }
        }
    }
}

/// What a drain left of one set's bootstraps on this node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Outcome {
    /// Nothing is left to do; the set's row goes.
    Settled,
    /// A walk waits for its jobs or the drain's budget; the drain looks again soon.
    Waiting,
    /// The catch-up was stated; the record that makes the member ready settles the set.
    Stated,
}

impl Outcome {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Waiting, _) | (_, Self::Waiting) => Self::Waiting,
            (Self::Stated, _) | (_, Self::Stated) => Self::Stated,
            (Self::Settled, Self::Settled) => Self::Settled,
        }
    }
}

/// One set's bootstraps as seen by this node.
#[derive(Clone, Copy, Debug, PartialEq)]
struct SetWalk {
    realm_id: RealmId,
    local: NodeId,
    set_id: Ulid,
}

/// The record and the member of this node a set's walks run under.
#[derive(Debug, PartialEq)]
struct WalkScope {
    walk: SetWalk,
    serving: Option<(NodeId, RealmId)>,
    snapshot: DistributionSnapshot,
    local: DistributionMember,
}

#[derive(Debug, PartialEq)]
enum SetStep {
    Record,
    Bucket,
    Departed,
    Clear,
    Part,
}

/// Runs the walks of one set: the members catching up and the new range copies.
#[derive(Debug, PartialEq)]
struct SetWalkOperation {
    walk: SetWalk,
    serving: Option<(NodeId, RealmId)>,
    budget: usize,
    step: SetStep,
    scope: Option<Arc<WalkScope>>,
    targets: VecDeque<DistributionMember>,
    fills: VecDeque<RangeFill>,
    outcome: Outcome,
}

impl SetWalkOperation {
    fn new(walk: SetWalk, serving: Option<(NodeId, RealmId)>, budget: usize) -> Self {
        Self {
            walk,
            serving,
            budget,
            step: SetStep::Record,
            scope: None,
            targets: VecDeque::new(),
            fills: VecDeque::new(),
            outcome: Outcome::Settled,
        }
    }

    /// Deletes the set's cursors in bounded batches; settled once none is left.
    fn clear(&mut self) -> Turn<Walked> {
        self.step = SetStep::Clear;
        let clear = ClearCursorsOperation::new(self.walk.set_id, None, CLEAR_BATCH);
        Ok(Next::Call(IoCall::child(clear)))
    }

    fn record_read(&mut self, view: Option<DistributionView>) -> Turn<Walked> {
        let Some(view) = view else {
            return Ok(Next::Done((Outcome::Settled, self.budget)));
        };
        let snapshot = view.head.snapshot;
        let members = &snapshot.config.members;
        let Some(local) = members
            .iter()
            .find(|m| m.node_id == self.walk.local)
            .cloned()
        else {
            return self.clear();
        };
        let targets: VecDeque<_> = snapshot.catching_up().into_iter().cloned().collect();
        let deleting = snapshot.lifecycle.deletion.is_some();
        let fills: VecDeque<_> = (snapshot.lifecycle.fills.iter())
            .filter(|_| !deleting)
            .filter(|fill| {
                (snapshot.config.ranges.iter()).any(|range| {
                    range.range_id == fill.range_id && range.coordinator == local.member_id
                })
            })
            .cloned()
            .collect();
        if targets.is_empty() && fills.is_empty() {
            return self.clear();
        }
        // Only the admitted incarnation of this member's bucket publishes.
        let bucket = snapshot.config.bucket.clone();
        (self.targets, self.fills) = (targets, fills);
        self.scope = Some(Arc::new(WalkScope {
            walk: self.walk,
            serving: self.serving,
            snapshot,
            local,
        }));
        self.step = SetStep::Bucket;
        Ok(Next::Call(IoCall::child(GetBucketOperation::new(bucket))))
    }

    fn bucket_read(&mut self, info: Result<BucketInfo, GetBucketError>) -> Turn<Walked> {
        let scope = self.scope.as_ref().ok_or(IoError::Unfinished)?;
        let local = &scope.local;
        let bound = (info.ok().and_then(|info| info.distribution)).is_some_and(|binding| {
            binding.member_id == local.member_id && local.incarnation == Some(binding.incarnation)
        });
        if !bound {
            return self.clear();
        }
        // The cursors of members no longer walked, such as one that left, go in one batch.
        let walked = (self.targets.iter().map(|target| target.member_id))
            .chain(self.fills.iter().map(|fill| fill.member_id))
            .collect();
        let clear = ClearCursorsOperation::new(self.walk.set_id, Some(walked), CLEAR_BATCH);
        self.step = SetStep::Departed;
        Ok(Next::Call(IoCall::child(clear)))
    }

    fn next_part(&mut self) -> Turn<Walked> {
        let scope = self.scope.clone().ok_or(IoError::Unfinished)?;
        self.step = SetStep::Part;
        if let Some(target) = self.targets.pop_front() {
            let walk = WalkMemberOperation::new(scope, target, self.budget);
            return Ok(Next::Call(IoCall::child(walk)));
        }
        if let Some(fill) = self.fills.pop_front() {
            let fill = FillRangeOperation::new(scope, fill, self.budget);
            return Ok(Next::Call(IoCall::child(fill)));
        }
        Ok(Next::Done((self.outcome, self.budget)))
    }
}

impl Operation for SetWalkOperation {
    type Output = Walked;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<Walked> {
        let read = ReadRecordOperation::new(self.walk.set_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Walked> {
        match self.step {
            SetStep::Record => {
                let view = result.child::<Option<DistributionView>, RecordError>()?;
                self.record_read(view.map_err(failed)?)
            }
            SetStep::Bucket => self.bucket_read(result.child()?),
            SetStep::Departed => {
                (result.child::<bool, BootstrapError>()?).map_err(failed)?;
                self.next_part()
            }
            SetStep::Clear => {
                let all_read = (result.child::<bool, BootstrapError>()?).map_err(failed)?;
                let outcome = match all_read {
                    true => Outcome::Settled,
                    false => Outcome::Waiting,
                };
                Ok(Next::Done((outcome, self.budget)))
            }
            SetStep::Part => {
                let (outcome, budget) = result.child::<Walked, BlobQueueError>()??;
                self.outcome = self.outcome.and(outcome);
                self.budget = budget;
                self.next_part()
            }
        }
    }
}

fn read_cursor(key: Vec<u8>) -> IoCall {
    IoCall::child(ReadCursorOperation::new(key))
}

fn cursor_answer(result: IoResult) -> Result<Option<BootstrapCursor>, BlobQueueError> {
    (result.child::<Option<BootstrapCursor>, BootstrapError>()?).map_err(failed)
}

fn page_answer(result: IoResult) -> Result<PageProgress, BlobQueueError> {
    (result.child::<PageProgress, BootstrapError>()?).map_err(failed)
}

#[derive(Debug, PartialEq)]
enum FillStep {
    Cursor,
    Page,
    Hand,
}

/// Walks one range for its new copy; when the walk ended, states the copy filled.
#[derive(Debug, PartialEq)]
struct FillRangeOperation {
    scope: Arc<WalkScope>,
    fill: RangeFill,
    budget: usize,
    now_ms: u64,
    step: FillStep,
}

impl FillRangeOperation {
    fn new(scope: Arc<WalkScope>, fill: RangeFill, budget: usize) -> Self {
        Self {
            scope,
            fill,
            budget,
            now_ms: 0,
            step: FillStep::Cursor,
        }
    }

    fn range(&self) -> Option<&KeyRange> {
        let ranges = &self.scope.snapshot.config.ranges;
        ranges
            .iter()
            .find(|range| range.range_id == self.fill.range_id)
    }

    fn hand(&mut self, watermark: u64) -> Turn<Walked> {
        let scope = self.scope.clone();
        let range = self.range().ok_or(IoError::Unfinished)?;
        let walk = scope.walk;
        let statement = Statement::copy_filled(
            walk.realm_id,
            walk.set_id,
            &scope.local,
            &self.fill,
            (range.epoch, watermark),
        );
        let hand = HandStatementOperation::new(scope.clone(), statement, self.fill.member_id);
        self.step = FillStep::Hand;
        Ok(Next::Call(IoCall::child(hand)))
    }
}

impl Operation for FillRangeOperation {
    type Output = Walked;
    type Error = BlobQueueError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Walked> {
        let snapshot = &self.scope.snapshot;
        let (Some(range), Some(_)) = (self.range(), snapshot.member(self.fill.member_id)) else {
            return Ok(Next::Done((Outcome::Settled, self.budget)));
        };
        // Until its node opened the range, the copy has nowhere to receive versions.
        if !self.fill.admitted {
            return Ok(Next::Done((Outcome::Waiting, self.budget)));
        }
        let key = bootstrap_key(self.scope.walk.set_id, self.fill.member_id, range.range_id);
        Ok(Next::Call(read_cursor(key)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Walked> {
        match self.step {
            FillStep::Cursor => {
                let intent = self.fill.intent();
                let cursor = cursor_answer(result)?.filter(|cursor| cursor.intent == intent);
                if let Some(cursor) = cursor.filter(|cursor| cursor.done) {
                    return self.hand(cursor.watermark);
                }
                if self.budget == 0 {
                    return Ok(Next::Done((Outcome::Waiting, 0)));
                }
                self.budget -= 1;
                let scope = &self.scope;
                let snapshot = &scope.snapshot;
                let target = (snapshot.member(self.fill.member_id)).ok_or(IoError::Unfinished)?;
                let range = self.range().ok_or(IoError::Unfinished)?;
                let page = BootstrapPage::new(PageConfig {
                    realm_id: scope.walk.realm_id,
                    bucket: snapshot.config.bucket.clone(),
                    set_id: scope.walk.set_id,
                    member_id: self.fill.member_id,
                    node_id: target.node_id,
                    intent,
                    range: range.clone(),
                    now_ms: self.now_ms,
                    rows: PAGE_ROWS,
                });
                self.step = FillStep::Page;
                Ok(Next::Call(IoCall::child(page)))
            }
            FillStep::Page => match page_answer(result)? {
                PageProgress::Done { watermark } => self.hand(watermark),
                PageProgress::Waiting | PageProgress::Queued(_) => {
                    Ok(Next::Done((Outcome::Waiting, self.budget)))
                }
            },
            FillStep::Hand => {
                let outcome = result.child::<Outcome, BlobQueueError>()??;
                Ok(Next::Done((outcome, self.budget)))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum MemberStep {
    Cursor,
    Seed,
    Page,
    Hand,
}

/// Walks the ranges this node publishes to a member; when every walk ended, states the
/// catch-up.
#[derive(Debug, PartialEq)]
struct WalkMemberOperation {
    scope: Arc<WalkScope>,
    target: DistributionMember,
    budget: usize,
    now_ms: u64,
    ranges: Vec<KeyRange>,
    order: VecDeque<usize>,
    current: usize,
    watermark: u64,
    /// A range still waits for its jobs, so nothing is stated yet.
    blocked: bool,
    step: MemberStep,
}

impl WalkMemberOperation {
    fn new(scope: Arc<WalkScope>, target: DistributionMember, budget: usize) -> Self {
        let local = scope.local.member_id;
        let ranges: Vec<KeyRange> = (scope.snapshot)
            .catch_up_ranges(local, target.member_id)
            .into_iter()
            .cloned()
            .collect();
        Self {
            scope,
            target,
            budget,
            now_ms: 0,
            ranges,
            order: VecDeque::new(),
            current: 0,
            watermark: u64::MAX,
            blocked: false,
            step: MemberStep::Cursor,
        }
    }

    fn turns(&self) -> (Ulid, Ulid) {
        (self.scope.walk.set_id, self.target.member_id)
    }

    fn waiting(&self) -> Turn<Walked> {
        Ok(Next::Done((Outcome::Waiting, self.budget)))
    }

    fn next_range(&mut self) -> Turn<Walked> {
        let Some(index) = self.order.pop_front() else {
            return match self.blocked {
                true => self.waiting(),
                false => self.state(),
            };
        };
        self.current = index;
        self.step = MemberStep::Cursor;
        let range_id = self.ranges[index].range_id;
        let key = bootstrap_key(self.scope.walk.set_id, self.target.member_id, range_id);
        Ok(Next::Call(read_cursor(key)))
    }

    fn cursor_known(&mut self, cursor: Option<BootstrapCursor>) -> Turn<Walked> {
        if let Some(cursor) = cursor.filter(|cursor| cursor.done) {
            self.watermark = self.watermark.min(cursor.watermark);
            return self.next_range();
        }
        if self.budget == 0 {
            // The next drain starts at this range; nothing is stated before all are done.
            turn(self.turns(), Some(self.current));
            return self.waiting();
        }
        self.budget -= 1;
        let scope = &self.scope;
        let page = BootstrapPage::new(PageConfig {
            realm_id: scope.walk.realm_id,
            bucket: scope.snapshot.config.bucket.clone(),
            set_id: scope.walk.set_id,
            member_id: self.target.member_id,
            node_id: self.target.node_id,
            intent: self.target.intent,
            range: self.ranges[self.current].clone(),
            now_ms: self.now_ms,
            rows: PAGE_ROWS,
        });
        self.step = MemberStep::Page;
        Ok(Next::Call(IoCall::child(page)))
    }

    /// Hands the signed catch-up to the publisher; the next record makes the member ready.
    fn state(&mut self) -> Turn<Walked> {
        let scope = self.scope.clone();
        let (local, target) = (&scope.local, &self.target);
        let cover = (scope.snapshot).catch_up_cover(local.member_id, target.member_id);
        let walk = scope.walk;
        let statement = Statement::caught_up(
            walk.realm_id,
            walk.set_id,
            local,
            target,
            cover,
            self.watermark,
        );
        let hand = HandStatementOperation::new(scope.clone(), statement, target.member_id);
        self.step = MemberStep::Hand;
        Ok(Next::Call(IoCall::child(hand)))
    }
}

impl Operation for WalkMemberOperation {
    type Output = Walked;
    type Error = BlobQueueError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    /// Signs a statement about the walks for `member_id` and hands it to the publisher.
    async fn hand(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        statement: Statement,
        member_id: Ulid,
    ) -> Result<Outcome, String> {
        let claim = StatementClaim {
            signer: self.local,
            statement,
        };
        let signed = match context.net_handle.as_ref().map_or_else(
            || StatementSignEvent::Unavailable("net transport unavailable".to_string()),
            |net| net.sign_statement(claim),
        ) {
            StatementSignEvent::Signed(signed) => *signed,
            StatementSignEvent::Unavailable(reason) => return Err(reason),
        };
        let publisher = snapshot.root.publisher;
        let submit = SubmitOperation::new(context.node_and_realm(), publisher, signed);
        match drive(submit, context).await {
            Ok(()) => Ok(Outcome::Stated),
            Err(refusal) => {
                debug!(set_id = %self.set_id, %member_id, %refusal, "Bootstrap statement not taken yet");
                Ok(Outcome::Waiting)
            }
        }
    }

    /// A split child of a walked range continues from its ancestor's cursor instead of walking
    /// again from the start; the derived cursor is stored for the page that follows. Only a
    /// child without a cursor of its own is seeded, so an ancestor never resets its progress.
    async fn seed_child(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        target: &DistributionMember,
        range: &KeyRange,
    ) -> Result<Option<BootstrapCursor>, String> {
        let own = bootstrap_key(self.set_id, target.member_id, range.range_id);
        if read_cursor(context, own.clone()).await?.is_some() {
            // A cursor of a former intent starts over in the page, never from an ancestor.
            return Ok(None);
        }
        let mut ancestor = range.lineage;
        for _ in 0..=snapshot.retired.len() {
            let Some(id) = ancestor else {
                return Ok(None);
            };
            let key = bootstrap_key(self.set_id, target.member_id, id);
            let parent = (read_cursor(context, key).await?).filter(|c| c.intent == target.intent);
            if let Some(parent) = parent {
                let child = parent.for_child(self.set_id, range);
                let bytes = child.to_bytes().map_err(|error| error.to_string())?;
                let write = StorageEffect::Write {
                    key_space: BOOTSTRAP_KEYSPACE.to_string(),
                    key: own.into(),
                    value: bytes.into(),
                    txn_id: None,
                };
                return match context.storage_handle.send_storage_effect(write).await {
                    Event::Storage(StorageEvent::WriteResult { .. }) => Ok(Some(child)),
                    other => Err(format!("a child cursor was not stored: {}", other.label())),
                };
            }
            ancestor = snapshot
                .retired_range(id)
                .and_then(|retired| retired.lineage);
        }
        Ok(None)
    }

    /// Deletes the cursors of members that are no longer walked, such as a member that left,
    /// in one bounded batch per drain.
    async fn clear_departed(&self, context: &DriverContext, walked: &[Ulid]) -> Result<(), String> {
        let clear = ClearCursorsOperation::new(self.set_id, Some(walked.to_vec()), CLEAR_BATCH);
        let cleared = drive(clear, context).await;
        cleared.map(|_| ()).map_err(|error| error.to_string())
    }

    /// Deletes the set's cursors in bounded batches; settled once none is left.
    async fn clear(&self, context: &DriverContext) -> Result<Outcome, String> {
        let clear = ClearCursorsOperation::new(self.set_id, None, CLEAR_BATCH);
        let all_read = drive(clear, context)
            .await
            .map_err(|error| error.to_string())?;
        Ok(match all_read {
            true => Outcome::Settled,
            false => Outcome::Waiting,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_store::{context, get, open};

    /// Member 1 coordinates both shards; member 2 keeps a copy of each and catches up.
    fn catching_up() -> DistributionSnapshot {
        use crate::distribution::test_record::{member, replicated};
        use aruna_core::structs::storage::distribution::MemberState;
        let (mut snapshot, _) = replicated(true, 1);
        snapshot.config.ranges[1].coordinator = member(1);
        snapshot.config.ranges[1].replicas = vec![member(2)];
        let target = (snapshot.config.members.iter_mut()).find(|m| m.member_id == member(2));
        target.unwrap().state = MemberState::CatchingUp;
        snapshot
    }

    fn walker(snapshot: &DistributionSnapshot) -> SetWalk {
        SetWalk {
            realm_id: RealmId::from_bytes([9; 32]),
            local: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            set_id: snapshot.set_id(),
        }
    }

    #[tokio::test]
    async fn ranges_take_turns() {
        // A drain whose budget ends before a range starts the next drain at that range.
        use crate::distribution::test_record::member;
        let (_dir, handle) = open();
        let context = context(&handle);
        let snapshot = catching_up();
        let walk = walker(&snapshot);
        let local = snapshot.member(member(1)).unwrap();
        let target = snapshot.member(member(2)).unwrap();
        let second = &snapshot.config.ranges[1];
        let key = bootstrap_key(walk.set_id, target.member_id, second.range_id);
        for _ in 0..2 {
            let mut budget = 1;
            let outcome = walk
                .walk(&context, &snapshot, local, target, &mut budget)
                .await;
            assert_eq!(outcome.unwrap(), Outcome::Waiting);
        }
        assert!(
            get(&handle, BOOTSTRAP_KEYSPACE, key).await.is_some(),
            "the second range had its turn"
        );
    }

    #[tokio::test]
    async fn child_keeps_cursor() {
        // A split child with a cursor of its own is never seeded from its ancestor again.
        use crate::distribution::test_record::member;
        let (_dir, handle) = open();
        let context = context(&handle);
        let snapshot = catching_up();
        let walk = walker(&snapshot);
        let target = snapshot.member(member(2)).unwrap();
        let parent = snapshot.config.ranges[0].clone();
        let child = KeyRange {
            range_id: Ulid::from_bytes([42; 16]),
            lineage: Some(parent.range_id),
            ..parent.clone()
        };
        let store = |range_id: Ulid, cursor: BootstrapCursor| {
            let key = bootstrap_key(walk.set_id, target.member_id, range_id);
            let write = StorageEffect::Write {
                key_space: BOOTSTRAP_KEYSPACE.to_string(),
                key: key.into(),
                value: cursor.to_bytes().unwrap().into(),
                txn_id: None,
            };
            let handle = handle.clone();
            async move { handle.send_storage_effect(write).await }
        };
        store(parent.range_id, BootstrapCursor::start(target.intent, 3)).await;
        let seeded = walk.seed_child(&context, &snapshot, target, &child).await;
        assert_eq!(seeded.unwrap().map(|c| c.watermark), Some(3));
        let mut progressed = BootstrapCursor::start(target.intent, 3);
        progressed.done = true;
        store(child.range_id, progressed.clone()).await;
        let again = walk.seed_child(&context, &snapshot, target, &child).await;
        assert_eq!(again.unwrap(), None);
        let key = bootstrap_key(walk.set_id, target.member_id, child.range_id);
        let kept = get(&handle, BOOTSTRAP_KEYSPACE, key).await.unwrap();
        assert_eq!(BootstrapCursor::from_bytes(&kept).unwrap(), progressed);
    }
}
