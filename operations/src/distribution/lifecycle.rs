//! Does this node's part of its sets' open lifecycle work: deletion fences and purges, transfer
//! fences and completions, reclaims of ranges it gave away and bucket release when it leaves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A transfer away drains its uploads before its fence is stated; a deletion purges the member
// bucket once it commits. Every pass is bounded and resumes from durable state.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_LIFECYCLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
    RANGE_COUNTER_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::rows::{read_row, value};
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, RangeAccount, RangeCounters, account_key, generation_key,
};
use aruna_core::structs::storage::distribution::fence::{LocalFence, RangeFence};
use aruna_core::structs::storage::distribution::lifecycle::MAX_CURSOR_BYTES;
use aruna_core::structs::storage::distribution::snapshot::OpenTransfer;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionMember, DistributionSnapshot, DistributionView, KeyRange,
    MemberState, MetadataCoverage, Statement, StatementClaim, StatementKind,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use ulid::Ulid;

use super::fence_row::{FenceChange, FenceError, UpdateFenceOperation};
use super::imports::{
    FencedImportsOperation, FetchImportsOperation, ImportError, KeepResumeOperation,
};
use super::inventory::{StagedCall, outside_answer};
use super::purge_steps::{
    ClearSetOperation, DeleteRowsOperation, DropResumeOperation, PurgeError, PurgeRangeOperation,
    PurgeRestOperation, PurgeTarget, Purged, RangeUploadsOperation, ReadRowOperation,
};
use super::roots::{RootsCheckOperation, RootsError, RootsOutcome, Sweep};
use super::status_view::{ReadViewOperation, StatusError};
use super::submit::{StateOperation, Stated};
use crate::replication::members::{OwesFanoutOperation, RecordError};
use crate::s3::bucket::delete::{DeleteBucketError, DeleteBucketOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::usage::{MarkDirtyOperation, dirty_rows};

/// Sets one pass looks at, and purge pages it spends at most.
const SET_BATCH: usize = 16;
const PURGE_PAGES: usize = 32;
/// How soon the next pass comes while lifecycle work waits.
const LIFECYCLE_POLL: Duration = Duration::from_secs(5);
/// How long uploads opened before a transfer fence may still complete before they are aborted.
const UPLOAD_GRACE_MS: u64 = 5 * 60 * 1000;

/// What a range held when its former coordinator fenced it; the new coordinator completes
/// once its own counters of the range reach it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Watermark {
    pub versions: i128,
    pub delete_markers: i128,
    pub max_epoch: u64,
}

impl Watermark {
    fn of(counters: &RangeCounters) -> Self {
        Self {
            versions: counters.versions,
            delete_markers: counters.delete_markers,
            max_epoch: counters.max_epoch.0,
        }
    }

    /// Whether counters of the receiving copy hold at least what the fenced range held.
    pub fn reached_by(&self, counters: &RangeCounters) -> bool {
        counters.versions >= self.versions
            && counters.delete_markers >= self.delete_markers
            && counters.max_epoch.0 >= self.max_epoch
    }

    /// Whether every version of the range is older than `epoch`, so the new coordinator's
    /// writes at `epoch` outrank them.
    pub fn below(&self, epoch: u64) -> bool {
        self.max_epoch < epoch
    }
}

/// What one set left to do on this node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Left {
    Settled,
    Waiting,
}

impl Left {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Settled, Self::Settled) => Self::Settled,
            _ => Self::Waiting,
        }
    }
}

fn failed(error: impl std::fmt::Display) -> PurgeError {
    PurgeError(error.to_string())
}

/// What lifecycle passes keep between them: the set marker after which the next continues, and
/// where the open source root sweep of each transfer goes on.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct LifecycleCursors {
    marker: Option<Vec<u8>>,
    sweeps: BTreeMap<Ulid, Sweep>,
}

/// One finished pass: when to look again, if work waits, and the cursors for the next.
#[derive(Debug, PartialEq)]
pub(super) struct LifecyclePass {
    pub(super) poll: Option<Duration>,
    pub(super) cursors: LifecycleCursors,
}

/// What the pending call of a lifecycle pass answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PassStep {
    Scan,
    DropMarker,
    Set,
    Settle,
}

/// One bounded pass over the sets with open lifecycle work. It reads the next batch of sets
/// after the last one looked at, so sets that keep waiting never starve later ones; a short
/// batch makes the next pass start over.
#[derive(Debug, PartialEq)]
pub(super) struct TendLifecycleOperation {
    realm_id: RealmId,
    local: NodeId,
    cursors: LifecycleCursors,
    step: PassStep,
    rows: VecDeque<(Vec<u8>, Vec<u8>)>,
    current: Option<(Vec<u8>, Vec<u8>, Ulid)>,
    budget: usize,
    waiting: bool,
}

type Passing = Result<Next<LifecyclePass>, IoError>;

impl TendLifecycleOperation {
    pub(super) fn new(realm_id: RealmId, local: NodeId, cursors: LifecycleCursors) -> Self {
        Self {
            realm_id,
            local,
            cursors,
            step: PassStep::Scan,
            rows: VecDeque::new(),
            current: None,
            budget: PURGE_PAGES,
            waiting: false,
        }
    }

    fn finish(&mut self, poll: bool) -> Passing {
        Ok(Next::Done(LifecyclePass {
            poll: poll.then_some(LIFECYCLE_POLL),
            cursors: std::mem::take(&mut self.cursors),
        }))
    }

    fn scanned(&mut self, result: IoResult) -> Passing {
        let rows = match outside_answer(result) {
            Ok(IoResult::Page { rows, .. }) => rows,
            Ok(other) => return self.scan_failed(&other.label()),
            Err(error) => return self.scan_failed(&error),
        };
        self.rows = (rows.into_iter())
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect();
        self.cursors.marker = (self.rows.len() == SET_BATCH)
            .then(|| self.rows.back().map(|(key, _)| key.clone()))
            .flatten();
        self.next_set()
    }

    fn scan_failed(&mut self, error: &dyn std::fmt::Display) -> Passing {
        warn!(%error, "Lifecycle work could not read its sets");
        self.finish(true)
    }

    fn next_set(&mut self) -> Passing {
        let Some((key, seen)) = self.rows.pop_front() else {
            return self.finish(self.waiting);
        };
        let Ok(set_id) = <[u8; 16]>::try_from(key.as_slice()).map(Ulid::from_bytes) else {
            let deletes = vec![marker(&key)];
            self.step = PassStep::DropMarker;
            return Ok(Next::Call(IoCall::child(DeleteRowsOperation { deletes })));
        };
        let sweeps = self.cursors.sweeps.clone();
        let ids = (self.realm_id, self.local);
        let work = SetWorkOperation::new(ids, set_id, self.budget, sweeps);
        self.current = Some((key, seen, set_id));
        self.step = PassStep::Set;
        Ok(Next::Call(IoCall::child(work)))
    }

    fn set_worked(&mut self, result: Result<Worked, PurgeError>) -> Passing {
        let (key, seen, set_id) = self.current.take().ok_or(IoError::Unfinished)?;
        let worked = match result {
            Ok(worked) => worked,
            Err(error) => {
                warn!(%set_id, %error, "Distributed bucket lifecycle work failed; retrying");
                self.waiting = true;
                return self.next_set();
            }
        };
        self.budget = worked.budget;
        for (transfer_id, sweep) in worked.sweeps {
            match sweep {
                Some(sweep) => self.cursors.sweeps.insert(transfer_id, sweep),
                None => self.cursors.sweeps.remove(&transfer_id),
            };
        }
        if worked.left == Left::Waiting {
            self.waiting = true;
            return self.next_set();
        }
        let space = DISTRIBUTION_LIFECYCLE_KEYSPACE;
        let settle = SettleMarkerOperation::new(space, key, seen);
        self.current = Some((Vec::new(), Vec::new(), set_id));
        self.step = PassStep::Settle;
        Ok(Next::Call(IoCall::child(settle)))
    }
}

impl Operation for TendLifecycleOperation {
    type Output = LifecyclePass;
    type Error = IoError;

    fn first(&mut self) -> Passing {
        let page = IoCall::Page {
            space: DISTRIBUTION_LIFECYCLE_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.cursors.marker.clone().map(Key::from),
            limit: SET_BATCH,
        };
        Ok(Next::Call(StagedCall::Outside(page).into_call()))
    }

    fn next(&mut self, result: IoResult) -> Passing {
        match self.step {
            PassStep::Scan => self.scanned(result),
            PassStep::DropMarker => {
                result.child::<(), PurgeError>()?.ok();
                self.next_set()
            }
            PassStep::Set => self.set_worked(result.child::<Worked, PurgeError>()?),
            PassStep::Settle => {
                let (.., set_id) = self.current.take().ok_or(IoError::Unfinished)?;
                if let Err(error) = result.child::<(), PurgeError>()? {
                    warn!(%set_id, %error, "A settled lifecycle row stays until the next pass");
                }
                self.next_set()
            }
        }
    }
}

fn marker(key: &[u8]) -> (String, Key) {
    (
        DISTRIBUTION_LIFECYCLE_KEYSPACE.to_string(),
        key.to_vec().into(),
    )
}

/// Deletes a set's marker row only while it still holds `seen`: a record installed since the
/// pass read it wrote a newer revision, and its work is looked at again.
#[derive(Debug, PartialEq)]
pub(crate) struct SettleMarkerOperation {
    key_space: String,
    key: Vec<u8>,
    seen: Vec<u8>,
    deleting: bool,
}

impl SettleMarkerOperation {
    pub(crate) fn new(key_space: &str, key: Vec<u8>, seen: Vec<u8>) -> Self {
        Self {
            key_space: key_space.to_string(),
            key,
            seen,
            deleting: false,
        }
    }

    fn row(&self) -> (String, Key) {
        (self.key_space.clone(), self.key.clone().into())
    }
}

impl Operation for SettleMarkerOperation {
    type Output = ();
    type Error = PurgeError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        Ok(Next::Call(IoCall::Read(vec![self.row()])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        if self.deleting {
            result.done()?;
            return Ok(Next::Done(()));
        }
        if value(result)?.as_deref() != Some(self.seen.as_slice()) {
            return Ok(Next::Done(()));
        }
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(vec![self.row()])))
    }
}

/// What working on one set left: its state, the purge budget, and changed transfer sweeps.
#[derive(Debug, PartialEq)]
pub(super) struct Worked {
    left: Left,
    budget: usize,
    sweeps: Vec<(Ulid, Option<Sweep>)>,
}

/// This node's view of one set it works on.
#[derive(Clone, Debug, PartialEq)]
struct SetScope {
    realm_id: RealmId,
    local: NodeId,
    set_id: Ulid,
    snapshot: DistributionSnapshot,
    member: DistributionMember,
    /// The local bucket's binding, while the bucket is this member's.
    binding: Option<DistributionBinding>,
}

impl SetScope {
    fn bucket(&self) -> &str {
        &self.snapshot.config.bucket
    }

    fn target(&self, binding: &DistributionBinding, range: &KeyRange) -> PurgeTarget {
        PurgeTarget {
            realm_id: self.realm_id,
            node_id: self.local,
            bucket: self.bucket().to_string(),
            group_id: self.snapshot.config.owner_group,
            binding: binding.clone(),
            range_id: range.range_id,
            lower: range.lower.clone(),
            upper: range.upper.clone(),
        }
    }

    /// Changes the set's fence row; a lost conflict runs it again.
    fn fence(&self, change: FenceChange) -> IoCall {
        let set_id = self.set_id;
        let build = move || UpdateFenceOperation::new(set_id, change.clone());
        let child = boxed_retry(build, &set_id.to_bytes(), |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }

    /// Clears what this node keeps of the set besides its bucket, within `pages`.
    fn clear(
        &self,
        keep_fence: bool,
        clearing: Option<DistributionBinding>,
        pages: usize,
    ) -> IoCall {
        let rows = (self.set_id, self.bucket().to_string(), clearing);
        IoCall::child(ClearSetOperation::new(rows, keep_fence, pages))
    }

    /// Signs a lifecycle statement with this node's key and hands it to the publisher.
    fn state(&self, subject: &[u8], kind: StatementKind) -> IoCall {
        let statement =
            Statement::lifecycle(self.realm_id, self.set_id, &self.member, subject, kind);
        let claim = StatementClaim {
            signer: self.local,
            statement,
        };
        let ids = Some((self.local, self.realm_id));
        IoCall::child(StateOperation::new(
            ids,
            self.snapshot.root.publisher,
            claim,
        ))
    }

    /// An unreachable publisher keeps the fence; the statement is sent again next pass.
    fn stated(&self, result: IoResult) -> Result<(), PurgeError> {
        match result.child::<Stated, IoError>()?? {
            Stated::Taken => Ok(()),
            Stated::Unsigned(reason) => Err(PurgeError(reason)),
            Stated::Refused(refusal) => {
                let set_id = self.set_id;
                debug!(%set_id, %refusal, "A lifecycle statement was not taken yet");
                Ok(())
            }
        }
    }
}

fn fence_of(result: IoResult) -> Result<LocalFence, PurgeError> {
    result.child::<LocalFence, FenceError>()?.map_err(failed)
}

fn cleared(result: IoResult) -> Result<(Left, usize), PurgeError> {
    let (purged, budget) = result.child::<(Purged, usize), PurgeError>()??;
    let left = match purged {
        Purged::Done => Left::Settled,
        Purged::More => Left::Waiting,
    };
    Ok((left, budget))
}

fn worked(left: Left, budget: usize) -> Next<Worked> {
    Next::Done(Worked {
        left,
        budget,
        sweeps: Vec::new(),
    })
}

type Working = Result<Next<Worked>, PurgeError>;

/// What the pending call of one set's work answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SetStep {
    View,
    Bucket,
    Part,
}

/// One part of a set's work, in the order it runs.
#[derive(Clone, Debug, PartialEq)]
enum Part {
    Leave,
    HandOver(OpenTransfer),
    TakeOver(OpenTransfer),
    Reclaim,
}

/// One set's lifecycle work on this node.
#[derive(Debug, PartialEq)]
struct SetWorkOperation {
    realm_id: RealmId,
    local: NodeId,
    set_id: Ulid,
    budget: usize,
    sweeps: BTreeMap<Ulid, Sweep>,
    step: SetStep,
    view: Option<DistributionView>,
    scope: Option<SetScope>,
    parts: VecDeque<Part>,
    left: Left,
    changed: Vec<(Ulid, Option<Sweep>)>,
}

impl SetWorkOperation {
    fn new(
        (realm_id, local): (RealmId, NodeId),
        set_id: Ulid,
        budget: usize,
        sweeps: BTreeMap<Ulid, Sweep>,
    ) -> Self {
        Self {
            realm_id,
            local,
            set_id,
            budget,
            sweeps,
            step: SetStep::View,
            view: None,
            scope: None,
            parts: VecDeque::new(),
            left: Left::Settled,
            changed: Vec::new(),
        }
    }

    fn settled(&self) -> Next<Worked> {
        worked(Left::Settled, self.budget)
    }

    fn viewed(&mut self, view: Option<DistributionView>) -> Working {
        let Some(view) = view else {
            return Ok(self.settled());
        };
        let snapshot = &view.head.snapshot;
        if !(snapshot.config.members.iter()).any(|m| m.node_id == self.local) {
            return Ok(self.settled());
        }
        let bucket = GetBucketOperation::new(snapshot.config.bucket.clone());
        self.view = Some(view);
        self.step = SetStep::Bucket;
        Ok(Next::Call(IoCall::child(bucket)))
    }

    /// Chooses the work from the record and the local bucket's binding to this set.
    fn bound(&mut self, bound: Option<DistributionBinding>) -> Working {
        let view = self.view.as_ref().ok_or(IoError::Unfinished)?;
        let conflicted = view.conflicted();
        let snapshot = view.head.snapshot.clone();
        let member = (snapshot.config.members.iter())
            .find(|m| m.node_id == self.local)
            .cloned()
            .ok_or(IoError::Unfinished)?;
        let departing = bound.clone().filter(|b| replaced(&snapshot, &member, b));
        let binding = bound.filter(|binding| owns(self.set_id, &member, binding));
        let scope = SetScope {
            realm_id: self.realm_id,
            local: self.local,
            set_id: self.set_id,
            snapshot,
            member,
            binding,
        };
        self.step = SetStep::Part;
        if let Some(left) = departing {
            // A replaced member's departure finishes before the new member's work.
            let depart = DepartOperation::new(scope, left, self.budget);
            return Ok(Next::Call(IoCall::child(depart)));
        }
        if scope.snapshot.lifecycle.deletion.is_some() {
            let delete = SetDeleteOperation::new(scope, self.budget);
            return Ok(Next::Call(IoCall::child(delete)));
        }
        if scope.member.state == MemberState::Leaving {
            self.parts.push_back(Part::Leave);
        }
        let member_id = scope.member.member_id;
        for open in &scope.snapshot.lifecycle.transfers {
            if open.from == member_id {
                self.parts.push_back(Part::HandOver(open.clone()));
            } else if open.to == member_id {
                self.parts.push_back(Part::TakeOver(open.clone()));
            }
        }
        if !conflicted {
            self.parts.push_back(Part::Reclaim);
        }
        self.scope = Some(scope);
        self.next_part()
    }

    fn next_part(&mut self) -> Working {
        let Some(part) = self.parts.pop_front() else {
            return Ok(Next::Done(Worked {
                left: self.left,
                budget: self.budget,
                sweeps: std::mem::take(&mut self.changed),
            }));
        };
        let scope = self.scope.clone().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(match part {
            Part::Leave => IoCall::child(LeaveOperation::new(scope, self.budget)),
            Part::HandOver(open) => IoCall::child(HandOverOperation::new(scope, open, self.budget)),
            Part::TakeOver(open) => {
                let sweep = self.sweeps.get(&open.transfer_id).cloned();
                IoCall::child(TakeOverOperation::new(scope, open, self.budget, sweep))
            }
            Part::Reclaim => IoCall::child(ReclaimOperation::new(scope, self.budget)),
        }))
    }
}

impl Operation for SetWorkOperation {
    type Output = Worked;
    type Error = PurgeError;

    fn first(&mut self) -> Working {
        let read = ReadViewOperation {
            set_id: self.set_id,
        };
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Working {
        match self.step {
            SetStep::View => {
                let view = result.child::<Option<DistributionView>, StatusError>()?;
                self.viewed(view.map_err(failed)?)
            }
            SetStep::Bucket => {
                let info = result.child::<BucketInfo, GetBucketError>()?;
                let binding = set_binding(self.set_id, info).map_err(PurgeError)?;
                self.bound(binding)
            }
            SetStep::Part => {
                let done = result.child::<Worked, PurgeError>()??;
                self.left = self.left.and(done.left);
                self.budget = done.budget;
                self.changed.extend(done.sweeps);
                self.next_part()
            }
        }
    }
}

/// What the pending call of a departure answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DepartStep {
    Fence,
    Clear,
    Unbind,
}

/// Releases the bucket of a member a join replaced: fences it and clears the set's rows, then
/// unbinds it and drops the fence in one transaction.
#[derive(Debug, PartialEq)]
struct DepartOperation {
    scope: SetScope,
    left: DistributionBinding,
    budget: usize,
    step: DepartStep,
}

impl DepartOperation {
    fn new(scope: SetScope, left: DistributionBinding, budget: usize) -> Self {
        Self {
            scope,
            left,
            budget,
            step: DepartStep::Fence,
        }
    }
}

impl Operation for DepartOperation {
    type Output = Worked;
    type Error = PurgeError;

    fn first(&mut self) -> Working {
        Ok(Next::Call(self.scope.fence(FenceChange::Close)))
    }

    fn next(&mut self, result: IoResult) -> Working {
        match self.step {
            DepartStep::Fence => {
                fence_of(result)?;
                self.step = DepartStep::Clear;
                let clear = self.scope.clear(true, Some(self.left.clone()), self.budget);
                Ok(Next::Call(clear))
            }
            DepartStep::Clear => {
                let (left, budget) = cleared(result)?;
                self.budget = budget;
                if left == Left::Waiting {
                    return Ok(worked(Left::Waiting, budget));
                }
                self.step = DepartStep::Unbind;
                let unbind = UnbindOperation::new(self.scope.bucket(), self.left.clone(), true);
                Ok(Next::Call(IoCall::child(unbind)))
            }
            // The new member's work follows in the next pass.
            DepartStep::Unbind => {
                result.child::<(), PurgeError>()??;
                Ok(worked(Left::Waiting, self.budget))
            }
        }
    }
}

/// What the pending call of a set deletion answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeleteStep {
    Fence,
    State,
    Purge,
    Bucket,
    Clear,
    Rest,
}

/// Fences the whole set, states it as a coordinator, and purges once it is committed.
#[derive(Debug, PartialEq)]
struct SetDeleteOperation {
    scope: SetScope,
    budget: usize,
    step: DeleteStep,
    next_range: usize,
}

impl SetDeleteOperation {
    fn new(scope: SetScope, budget: usize) -> Self {
        Self {
            scope,
            budget,
            step: DeleteStep::Fence,
            next_range: 0,
        }
    }

    fn clear(&mut self) -> Working {
        // The member bucket is gone or replaced: nothing of the set is left here.
        self.step = DeleteStep::Clear;
        Ok(Next::Call(self.scope.clear(true, None, self.budget)))
    }

    fn fenced(&mut self) -> Working {
        let snapshot = &self.scope.snapshot;
        let deletion = (snapshot.lifecycle.deletion.as_ref()).ok_or(IoError::Unfinished)?;
        let member_id = self.scope.member.member_id;
        if deletion.committed {
            return match self.scope.binding.is_some() {
                true => self.purge_next(),
                false => self.clear(),
            };
        }
        let coordinates = (snapshot.config.ranges.iter()).any(|r| r.coordinator == member_id);
        if !coordinates || deletion.fenced.contains(&member_id) {
            return Ok(worked(Left::Waiting, self.budget));
        }
        let identity = deletion.identity(self.scope.set_id);
        let kind = StatementKind::DeleteFenced { deletion: identity };
        self.step = DeleteStep::State;
        Ok(Next::Call(self.scope.state(&identity, kind)))
    }

    fn purge_next(&mut self) -> Working {
        let binding = self.scope.binding.clone().ok_or(IoError::Unfinished)?;
        let snapshot = &self.scope.snapshot;
        if let Some(range) = snapshot.config.ranges.get(self.next_range) {
            let target = self.scope.target(&binding, range);
            self.next_range += 1;
            self.step = DeleteStep::Purge;
            let purge = PurgeRangeOperation::new(target, self.budget);
            return Ok(Next::Call(IoCall::child(purge)));
        }
        let origin = Some((self.scope.local, self.scope.realm_id));
        let delete = DeleteBucketOperation::new(self.scope.bucket().to_string())
            .bound_to(binding)
            .purge_notices(origin);
        self.step = DeleteStep::Bucket;
        Ok(Next::Call(IoCall::child(delete)))
    }
}

impl Operation for SetDeleteOperation {
    type Output = Worked;
    type Error = PurgeError;

    fn first(&mut self) -> Working {
        let deletion = self.scope.snapshot.lifecycle.deletion.as_ref();
        let deletion = deletion.ok_or(IoError::Unfinished)?;
        let identity = deletion.identity(self.scope.set_id);
        Ok(Next::Call(self.scope.fence(FenceChange::Delete(identity))))
    }

    fn next(&mut self, result: IoResult) -> Working {
        match self.step {
            DeleteStep::Fence => {
                fence_of(result)?;
                self.fenced()
            }
            DeleteStep::State => {
                self.scope.stated(result)?;
                Ok(worked(Left::Waiting, self.budget))
            }
            DeleteStep::Purge => {
                let (left, budget) = cleared(result)?;
                self.budget = budget;
                match left {
                    Left::Waiting => Ok(worked(Left::Waiting, budget)),
                    Left::Settled => self.purge_next(),
                }
            }
            DeleteStep::Bucket => match result.child::<(), DeleteBucketError>()? {
                Ok(()) | Err(DeleteBucketError::NotFound) => self.clear(),
                Err(error) => {
                    // Versions the inventory did not name block the delete; their walk follows.
                    let set_id = self.scope.set_id;
                    debug!(%set_id, %error, "The member bucket is not empty yet");
                    let binding = self.scope.binding.as_ref().ok_or(IoError::Unfinished)?;
                    let Some(range) = self.scope.snapshot.config.ranges.first() else {
                        return Ok(worked(Left::Waiting, self.budget));
                    };
                    let rest = PurgeRestOperation::new(self.scope.target(binding, range));
                    self.step = DeleteStep::Rest;
                    Ok(Next::Call(IoCall::child(rest)))
                }
            },
            DeleteStep::Clear => {
                let (left, budget) = cleared(result)?;
                Ok(worked(left, budget))
            }
            DeleteStep::Rest => {
                result.child::<(), PurgeError>()??;
                Ok(worked(Left::Waiting, self.budget))
            }
        }
    }
}

/// What the pending call of a member's leave answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LeaveStep {
    Fence,
    Unbind,
    Clear,
}

/// A leaving member refuses publications, then releases its bucket as an ordinary one.
#[derive(Debug, PartialEq)]
struct LeaveOperation {
    scope: SetScope,
    budget: usize,
    step: LeaveStep,
}

impl LeaveOperation {
    fn new(scope: SetScope, budget: usize) -> Self {
        Self {
            scope,
            budget,
            step: LeaveStep::Fence,
        }
    }

    /// The released bucket checks no fence; a later join of this node starts afresh.
    fn clear(&mut self) -> Working {
        self.step = LeaveStep::Clear;
        Ok(Next::Call(self.scope.clear(false, None, self.budget)))
    }
}

impl Operation for LeaveOperation {
    type Output = Worked;
    type Error = PurgeError;

    fn first(&mut self) -> Working {
        match self.scope.binding {
            Some(_) => Ok(Next::Call(self.scope.fence(FenceChange::Close))),
            None => self.clear(),
        }
    }

    fn next(&mut self, result: IoResult) -> Working {
        match self.step {
            LeaveStep::Fence => {
                fence_of(result)?;
                let binding = self.scope.binding.clone().ok_or(IoError::Unfinished)?;
                let unbind = UnbindOperation::new(self.scope.bucket(), binding, false);
                self.step = LeaveStep::Unbind;
                Ok(Next::Call(IoCall::child(unbind)))
            }
            LeaveStep::Unbind => {
                result.child::<(), PurgeError>()??;
                self.clear()
            }
            LeaveStep::Clear => {
                let (left, budget) = cleared(result)?;
                Ok(worked(left, budget))
            }
        }
    }
}

/// What the pending call of a transfer hand-over answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandStep {
    FenceRow,
    Keep,
    Fence,
    Uploads,
    Close,
    Abort,
    Owes,
    Counters,
    Imports,
    State,
}

/// The former coordinator's side: fence the range, let its open uploads complete for a bounded
/// grace period, abort the rest, then state the fence.
#[derive(Debug, PartialEq)]
struct HandOverOperation {
    scope: SetScope,
    open: OpenTransfer,
    budget: usize,
    range: Option<KeyRange>,
    step: HandStep,
    watermark: Vec<u8>,
}

impl HandOverOperation {
    fn new(scope: SetScope, open: OpenTransfer, budget: usize) -> Self {
        let range = (scope.snapshot.config.ranges.iter())
            .find(|range| range.range_id == open.range_id)
            .cloned();
        Self {
            scope,
            open,
            budget,
            range,
            step: HandStep::FenceRow,
            watermark: Vec::new(),
        }
    }

    fn waiting(&self) -> Working {
        Ok(worked(Left::Waiting, self.budget))
    }

    fn range(&self) -> Result<&KeyRange, IoError> {
        self.range.as_ref().ok_or(IoError::Unfinished)
    }

    fn target(&self) -> Result<Option<PurgeTarget>, IoError> {
        let range = self.range()?;
        let binding = self.scope.binding.as_ref();
        Ok(binding.map(|binding| self.scope.target(binding, range)))
    }

    /// Fences the range and keeps the byte copies it has now; this node's copy goes only once
    /// it has them again.
    fn fence(&mut self) -> Working {
        let range = self.range()?;
        let fence = RangeFence {
            range_id: range.range_id,
            transfer_id: self.open.transfer_id,
            epoch: range.epoch,
            since_ms: unix_timestamp_millis(),
            uploads_open: true,
        };
        let copies = self.scope.snapshot.byte_holders(range, None).len();
        let copies = u32::try_from(copies).unwrap_or(u32::MAX);
        let change = FenceChange::Range { fence, copies };
        self.step = HandStep::Fence;
        Ok(Next::Call(self.scope.fence(change)))
    }

    fn fenced(&mut self, fence: LocalFence) -> Working {
        // Keep the set until the range is reclaimed after the activation.
        if self.open.fenced.is_some() {
            return self.waiting();
        }
        let Some(target) = self.target()? else {
            return self.waiting();
        };
        let range_id = self.range()?.range_id;
        let Some(grace) = fence.range(range_id).filter(|fenced| fenced.uploads_open) else {
            return self.abort(target);
        };
        let ends = grace.since_ms.saturating_add(UPLOAD_GRACE_MS);
        if unix_timestamp_millis() < ends {
            self.step = HandStep::Uploads;
            let look = RangeUploadsOperation::new(target, false);
            return Ok(Next::Call(IoCall::child(look)));
        }
        self.close()
    }

    /// Closing conflicts every completion still in flight, so none commits after it.
    fn close(&mut self) -> Working {
        let change = FenceChange::Uploads(self.range()?.range_id);
        self.step = HandStep::Close;
        Ok(Next::Call(self.scope.fence(change)))
    }

    fn abort(&mut self, target: PurgeTarget) -> Working {
        self.step = HandStep::Abort;
        let abort = RangeUploadsOperation::new(target, true);
        Ok(Next::Call(IoCall::child(abort)))
    }

    /// The new coordinator learns nothing of what this node still owes other members.
    fn owes(&mut self) -> Working {
        let range = self.range()?.clone();
        let owes = OwesFanoutOperation::new(self.scope.bucket(), self.scope.set_id, range);
        self.step = HandStep::Owes;
        Ok(Next::Call(IoCall::child(owes)))
    }

    /// States the fence with the watermark and the imports that go on at the new coordinator;
    /// refresh rounds restart.
    fn state(&mut self, jobs: Vec<JobId>) -> Working {
        let import_cursor = match jobs.is_empty() {
            true => None,
            false => Some(postcard::to_allocvec(&jobs).map_err(failed)?),
        };
        if import_cursor
            .as_ref()
            .is_some_and(|jobs| jobs.len() > MAX_CURSOR_BYTES)
        {
            return self.waiting();
        }
        let range = self.range()?;
        let kind = StatementKind::TransferFenced {
            transfer_id: self.open.transfer_id,
            range_id: range.range_id,
            epoch: range.epoch,
            next_epoch: self.open.epoch,
            watermark: std::mem::take(&mut self.watermark),
            import_cursor,
            refresh_cursor: None,
            open_uploads: 0,
        };
        let subject = self.open.transfer_id.to_bytes();
        let state = self.scope.state(&subject, kind);
        self.step = HandStep::State;
        Ok(Next::Call(state))
    }
}

impl Operation for HandOverOperation {
    type Output = Worked;
    type Error = PurgeError;

    fn first(&mut self) -> Working {
        if self.range.is_none() {
            return Ok(worked(Left::Settled, self.budget));
        }
        let key = LocalFence::key(self.scope.set_id);
        let read = ReadRowOperation {
            key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
            key,
        };
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Working {
        match self.step {
            HandStep::FenceRow => {
                let stored = result.child::<Option<Value>, PurgeError>()??;
                let fence = LocalFence::decode(stored.as_deref()).map_err(failed)?;
                let fenced = fence.range(self.range()?.range_id);
                // Where each open import stands is kept before the fence, for the new
                // coordinator.
                if fenced.is_none_or(|fenced| fenced.transfer_id != self.open.transfer_id) {
                    let keep = KeepResumeOperation::new(self.scope.bucket(), self.open.transfer_id);
                    self.step = HandStep::Keep;
                    return Ok(Next::Call(IoCall::child(keep)));
                }
                self.fence()
            }
            HandStep::Keep => {
                result.child::<(), ImportError>()?.map_err(failed)?;
                self.fence()
            }
            HandStep::Fence => self.fenced(fence_of(result)?),
            HandStep::Uploads => match result.child::<bool, PurgeError>()?? {
                true => self.waiting(),
                false => self.close(),
            },
            HandStep::Close => {
                fence_of(result)?;
                let target = self.target()?.ok_or(IoError::Unfinished)?;
                self.abort(target)
            }
            HandStep::Abort => match result.child::<bool, PurgeError>()?? {
                true => self.waiting(),
                false => self.owes(),
            },
            HandStep::Owes => match result.child::<bool, RecordError>()?.map_err(failed)? {
                true => self.waiting(),
                false => {
                    let read =
                        RangeCountersOperation::new(self.scope.set_id, self.range()?.range_id);
                    self.step = HandStep::Counters;
                    Ok(Next::Call(IoCall::child(read)))
                }
            },
            HandStep::Counters => {
                let Some(counters) = result.child::<Option<RangeCounters>, PurgeError>()?? else {
                    return self.waiting();
                };
                self.watermark =
                    postcard::to_allocvec(&Watermark::of(&counters)).map_err(failed)?;
                let imports =
                    FencedImportsOperation::new(self.scope.bucket(), self.open.transfer_id);
                self.step = HandStep::Imports;
                Ok(Next::Call(IoCall::child(imports)))
            }
            HandStep::Imports => {
                let jobs = result.child::<Vec<JobId>, ImportError>()?.map_err(failed)?;
                self.state(jobs)
            }
            HandStep::State => {
                self.scope.stated(result)?;
                self.waiting()
            }
        }
    }
}

/// What the pending call of a transfer take-over answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TakeStep {
    Counters,
    Roots,
    Imports,
    State,
}

/// The new coordinator's side: complete once its copy reached the fenced watermark and every
/// source root it holds references of is checked.
#[derive(Debug, PartialEq)]
struct TakeOverOperation {
    scope: SetScope,
    open: OpenTransfer,
    budget: usize,
    sweep: Option<Sweep>,
    step: TakeStep,
    fenced: Option<([u8; 32], Vec<u8>, Option<Vec<u8>>)>,
    changed: Vec<(Ulid, Option<Sweep>)>,
}

impl TakeOverOperation {
    fn new(scope: SetScope, open: OpenTransfer, budget: usize, sweep: Option<Sweep>) -> Self {
        Self {
            scope,
            open,
            budget,
            sweep,
            step: TakeStep::Counters,
            fenced: None,
            changed: Vec::new(),
        }
    }

    fn waiting(&mut self) -> Working {
        Ok(Next::Done(Worked {
            left: Left::Waiting,
            budget: self.budget,
            sweeps: std::mem::take(&mut self.changed),
        }))
    }

    /// The watermark and the imports the former coordinator's fence statement names.
    fn fenced_parts(&self) -> Option<([u8; 32], Vec<u8>, Option<Vec<u8>>)> {
        let (Some(fenced), None) = (self.open.fenced, self.open.completed) else {
            return None;
        };
        let statements = self.scope.snapshot.statements.iter();
        let signed = statements
            .into_iter()
            .find(|signed| signed.statement.digest() == fenced)?;
        match &signed.statement.kind {
            StatementKind::TransferFenced {
                watermark,
                import_cursor,
                ..
            } => Some((fenced, watermark.clone(), import_cursor.clone())),
            _ => None,
        }
    }

    fn counted(&mut self, counters: Option<RangeCounters>) -> Working {
        let (_, bytes, _) = self.fenced.as_ref().ok_or(IoError::Unfinished)?;
        let wanted: Watermark = postcard::from_bytes(bytes).map_err(failed)?;
        let Some(counters) = counters else {
            return self.waiting();
        };
        if !wanted.reached_by(&counters) || self.scope.binding.is_none() {
            return self.waiting();
        }
        // A holder's version at the new epoch or above would outrank the new coordinator.
        let epoch = self.open.epoch;
        if !wanted.below(epoch) || !Watermark::of(&counters).below(epoch) {
            let (set_id, transfer_id) = (self.scope.set_id, self.open.transfer_id);
            warn!(%set_id, %transfer_id, "A transfer epoch is not above the range's versions");
            return self.waiting();
        }
        // Every source root of what it applied must be readable here before it takes over.
        let group_id = self.scope.snapshot.config.owner_group;
        let check = RootsCheckOperation::new(self.scope.set_id, group_id, self.sweep.take());
        self.step = TakeStep::Roots;
        Ok(Next::Call(IoCall::child(check)))
    }

    fn checked(&mut self, outcome: RootsOutcome) -> Working {
        let transfer_id = self.open.transfer_id;
        match &outcome {
            RootsOutcome::Continue(sweep) => self.changed.push((transfer_id, Some(sweep.clone()))),
            RootsOutcome::Done(_) => self.changed.push((transfer_id, None)),
        }
        if outcome != RootsOutcome::Done(true) {
            return self.waiting();
        }
        let (_, _, imports) = self.fenced.as_ref().ok_or(IoError::Unfinished)?;
        let Some(imports) = imports else {
            return self.complete();
        };
        // The former coordinator's open imports are fetched now, while it still holds them,
        // and run once this node coordinates.
        let jobs: Vec<JobId> = postcard::from_bytes(imports).map_err(failed)?;
        let members = self.scope.snapshot.config.members.iter();
        let from = members.into_iter().find(|m| m.member_id == self.open.from);
        let Some(from) = from.map(|member| member.node_id) else {
            return self.waiting();
        };
        let range = (self.scope.set_id, self.open.range_id, transfer_id);
        let fetch = FetchImportsOperation::new(range, self.scope.local, from, jobs);
        self.step = TakeStep::Imports;
        Ok(Next::Call(IoCall::child(fetch)))
    }

    fn complete(&mut self) -> Working {
        let (fenced, watermark, _) = self.fenced.take().ok_or(IoError::Unfinished)?;
        let kind = StatementKind::TransferComplete {
            transfer_id: self.open.transfer_id,
            range_id: self.open.range_id,
            fenced,
            watermark,
        };
        let subject = self.open.transfer_id.to_bytes();
        self.step = TakeStep::State;
        Ok(Next::Call(self.scope.state(&subject, kind)))
    }
}

impl Operation for TakeOverOperation {
    type Output = Worked;
    type Error = PurgeError;

    fn first(&mut self) -> Working {
        self.fenced = self.fenced_parts();
        if self.fenced.is_none() {
            return self.waiting();
        }
        let read = RangeCountersOperation::new(self.scope.set_id, self.open.range_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Working {
        match self.step {
            TakeStep::Counters => {
                self.counted(result.child::<Option<RangeCounters>, PurgeError>()??)
            }
            TakeStep::Roots => {
                let outcome = result.child::<RootsOutcome, RootsError>()?;
                self.checked(outcome.map_err(failed)?)
            }
            TakeStep::Imports => match result.child::<bool, ImportError>()?.map_err(failed)? {
                true => self.complete(),
                false => self.waiting(),
            },
            TakeStep::State => {
                self.scope.stated(result)?;
                self.waiting()
            }
        }
    }
}

/// What the pending call of a reclaim answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReclaimStep {
    Fence,
    Drop,
    Purge,
}

/// Purges a range this node fenced for a transfer, or each range split from it, once the record
/// no longer lists it as a byte holder, the range is ready and has at least as many ready byte
/// copies without this node as it had at the fence.
#[derive(Debug, PartialEq)]
struct ReclaimOperation {
    scope: SetScope,
    budget: usize,
    step: ReclaimStep,
    closed: VecDeque<Ulid>,
    ranges: VecDeque<(KeyRange, bool)>,
    left: Left,
}

impl ReclaimOperation {
    fn new(scope: SetScope, budget: usize) -> Self {
        Self {
            scope,
            budget,
            step: ReclaimStep::Fence,
            closed: VecDeque::new(),
            ranges: VecDeque::new(),
            left: Left::Settled,
        }
    }

    /// Imports kept for a transfer away are dropped once it is no longer open.
    fn next_drop(&mut self) -> Working {
        match self.closed.pop_front() {
            Some(transfer_id) => {
                self.step = ReclaimStep::Drop;
                let drop = DropResumeOperation::new(transfer_id);
                Ok(Next::Call(IoCall::child(drop)))
            }
            None => self.next_range(),
        }
    }

    fn next_range(&mut self) -> Working {
        let binding = self.scope.binding.clone().ok_or(IoError::Unfinished)?;
        while let Some((range, ready)) = self.ranges.pop_front() {
            if !ready {
                self.left = Left::Waiting;
                continue;
            }
            let target = self.scope.target(&binding, &range);
            self.step = ReclaimStep::Purge;
            let purge = PurgeRangeOperation::new(target, self.budget);
            return Ok(Next::Call(IoCall::child(purge)));
        }
        Ok(worked(self.left, self.budget))
    }
}

impl Operation for ReclaimOperation {
    type Output = Worked;
    type Error = PurgeError;

    fn first(&mut self) -> Working {
        if self.scope.binding.is_none() {
            return Ok(worked(Left::Settled, self.budget));
        }
        let read = ReadRowOperation {
            key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
            key: LocalFence::key(self.scope.set_id),
        };
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Working {
        match self.step {
            ReclaimStep::Fence => {
                let stored = result.child::<Option<Value>, PurgeError>()??;
                let fence = LocalFence::decode(stored.as_deref()).map_err(failed)?;
                let snapshot = &self.scope.snapshot;
                let transfers = &snapshot.lifecycle.transfers;
                self.closed = (fence.ranges.iter())
                    .map(|fenced| fenced.transfer_id)
                    .filter(|id| !transfers.iter().any(|t| t.transfer_id == *id))
                    .collect();
                self.ranges = given_away(snapshot, &fence, &self.scope.member)
                    .into_iter()
                    .map(|(range, ready)| (range.clone(), ready))
                    .collect();
                self.next_drop()
            }
            ReclaimStep::Drop => {
                result.child::<(), PurgeError>()??;
                self.next_drop()
            }
            ReclaimStep::Purge => {
                let (left, budget) = cleared(result)?;
                self.left = self.left.and(left);
                self.budget = budget;
                self.next_range()
            }
        }
    }
}

/// The ranges `member` fenced and gave away, split children included, and whether each keeps
/// its copies without it: not moving, listed members ready, as many byte holders as at the
/// fence. An `all` coverage member keeps its records, so it gives nothing away.
fn given_away<'a>(
    snapshot: &'a DistributionSnapshot,
    fence: &LocalFence,
    member: &DistributionMember,
) -> Vec<(&'a KeyRange, bool)> {
    if member.coverage == MetadataCoverage::All {
        return Vec::new();
    }
    let mut given = Vec::new();
    for fenced in &fence.ranges {
        let copies = usize::try_from(fence.copies_of(fenced.range_id)).unwrap_or(usize::MAX);
        let ranges = (snapshot.config.ranges.iter())
            .filter(|range| snapshot.descends(range, fenced.range_id))
            .filter(|range| {
                range.coordinator != member.member_id && !range.replicas.contains(&member.member_id)
            });
        for range in ranges {
            let moving =
                (snapshot.lifecycle.transfers.iter()).any(|t| t.range_id == range.range_id);
            let ready = (snapshot
                .byte_holders(range, Some(member.member_id))
                .into_iter())
            .filter(|holder| holder.state == MemberState::Ready)
            .count();
            given.push((
                range,
                !moving && snapshot.range_ready(range) && ready >= copies,
            ));
        }
    }
    given
}

/// Whether `binding` names a former member of this node that a join replaced.
fn replaced(
    snapshot: &DistributionSnapshot,
    member: &DistributionMember,
    binding: &DistributionBinding,
) -> bool {
    binding.member_id != member.member_id
        && snapshot.tombstoned(binding.member_id, binding.incarnation)
}

/// Whether `binding` names the bucket of `member`: same set and member at its admitted
/// incarnation. A member that never admitted owns the bucket it created, whatever incarnation.
fn owns(set_id: Ulid, member: &DistributionMember, binding: &DistributionBinding) -> bool {
    binding.set_id == set_id
        && binding.member_id == member.member_id
        && (member.incarnation).is_none_or(|admitted| admitted == binding.incarnation)
}

/// The bucket's binding to the set from a bucket read; only a missing bucket means it is
/// gone, any other failure keeps the set's state for the next pass.
fn set_binding(
    set_id: Ulid,
    read: Result<BucketInfo, GetBucketError>,
) -> Result<Option<DistributionBinding>, String> {
    match read {
        Ok(info) => Ok(info.distribution.filter(|bound| bound.set_id == set_id)),
        Err(GetBucketError::NotFound) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// The active counters this node keeps for a range, while its account is not split or rebuilt.
#[derive(Debug, PartialEq)]
pub(super) struct RangeCountersOperation {
    set_id: Ulid,
    range_id: Ulid,
    reading_counters: bool,
}

impl RangeCountersOperation {
    pub(super) fn new(set_id: Ulid, range_id: Ulid) -> Self {
        Self {
            set_id,
            range_id,
            reading_counters: false,
        }
    }
}

impl Operation for RangeCountersOperation {
    type Output = Option<RangeCounters>;
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<Option<RangeCounters>>, PurgeError> {
        let key = account_key(self.set_id, self.range_id);
        Ok(Next::Call(read_row(RANGE_ACCOUNT_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<RangeCounters>>, PurgeError> {
        let stored = value(result)?;
        if self.reading_counters {
            let counters = stored.map(|value| RangeCounters::from_bytes(&value));
            let counters = counters.transpose().map_err(failed)?;
            return Ok(Next::Done(Some(counters.unwrap_or_default())));
        }
        let Some(account) = stored else {
            return Ok(Next::Done(None));
        };
        let account = RangeAccount::from_bytes(&account).map_err(failed)?;
        let (Charge::Active(generation), None) = (account.charge, account.rebuild) else {
            return Ok(Next::Done(None));
        };
        self.reading_counters = true;
        let row = generation_key(self.set_id, self.range_id, generation);
        Ok(Next::Call(read_row(RANGE_COUNTER_KEYSPACE, row)))
    }
}

/// Releases the member bucket as an ordinary bucket, only while it carries `binding`; with
/// `drop_fence` the set's fence row goes in the same transaction.
#[derive(Debug, PartialEq)]
struct UnbindOperation {
    bucket: String,
    binding: DistributionBinding,
    drop_fence: bool,
    writes: usize,
}

impl UnbindOperation {
    fn new(bucket: &str, binding: DistributionBinding, drop_fence: bool) -> Self {
        Self {
            bucket: bucket.to_string(),
            binding,
            drop_fence,
            writes: 0,
        }
    }

    fn key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    /// Whether the stored bucket still carries this binding.
    fn same(&self, info: &BucketInfo) -> bool {
        let wanted = &self.binding;
        info.distribution.as_ref().is_some_and(|bound| {
            (bound.set_id, bound.member_id, bound.incarnation)
                == (wanted.set_id, wanted.member_id, wanted.incarnation)
        })
    }

    /// The bucket without its binding; its group's published replica share must go too.
    fn release(&mut self, mut info: BucketInfo) -> Result<Next<()>, PurgeError> {
        info.distribution = None;
        let bucket = info.to_bytes().map_err(failed)?;
        let mut writes = vec![(S3_BUCKET_KEYSPACE.to_string(), self.key(), bucket.into())];
        writes.extend(dirty_rows([info.group_id], Ulid::generate()));
        self.writes = 1;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for UnbindOperation {
    type Output = ();
    type Error = PurgeError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        let row = (S3_BUCKET_KEYSPACE.to_string(), self.key());
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        // Scheduling the publish is best effort; the committed markers wait for it.
        let wake = || Next::Then {
            output: (),
            calls: vec![IoCall::child(MarkDirtyOperation::wake())],
        };
        match self.writes {
            0 => {
                let stored = value(result)?;
                let info = stored.and_then(|value| BucketInfo::from_bytes(&value).ok());
                match info.filter(|info| self.same(info)) {
                    Some(info) => self.release(info),
                    None => Ok(Next::Done(())),
                }
            }
            1 if self.drop_fence => {
                result.done()?;
                self.writes = 2;
                let key = LocalFence::key(self.binding.set_id).into();
                let row = (DISTRIBUTION_FENCE_KEYSPACE.to_string(), key);
                Ok(Next::Call(IoCall::Delete(vec![row])))
            }
            _ => {
                result.done()?;
                Ok(wake())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::storage::distribution::OriginalEpoch;
    use std::sync::Arc;

    #[test]
    fn unadmitted_bucket_deleted() {
        // A bucket created for a member that was never admitted is still deleted with the set.
        let (snapshot, _) = crate::distribution::test_record::record(true);
        let mut member = snapshot.config.members[0].clone();
        let (set_id, member_id) = (snapshot.set_id(), member.member_id);
        let bound = |incarnation| DistributionBinding {
            set_id,
            member_id,
            incarnation,
            revision: snapshot.revision(),
        };
        assert!(owns(set_id, &member, &bound(5)));
        assert!(
            !owns(set_id, &member, &bound(6)),
            "a replaced bucket is left alone"
        );
        member.incarnation = None;
        assert!(owns(set_id, &member, &bound(6)));
        let other = DistributionBinding {
            member_id: Ulid::from_bytes([77; 16]),
            ..bound(6)
        };
        assert!(!owns(set_id, &member, &other));
    }

    #[tokio::test]
    async fn release_marks_group() {
        // A leaving member's bucket turns ordinary, so its group's replica share is published
        // again in the same commit.
        use crate::distribution::test_store::{context, get, open};
        use aruna_core::keyspaces::NODE_STATS_KEYSPACE;
        use aruna_core::structs::storage::usage::{DIRTY_GLOBAL_KEY, dirty_group_key};
        let (_dir, handle) = open();
        let context = context(&handle);
        let (snapshot, _) = crate::distribution::test_record::record(true);
        let binding = crate::distribution::test_record::binding(&snapshot, 1, 5);
        let group_id = Ulid::from_bytes([2; 16]);
        let info = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: Some(binding.clone()),
        };
        let write = StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: b"spire".to_vec().into(),
            value: info.to_bytes().unwrap().into(),
            txn_id: None,
        };
        handle.send_storage_effect(write).await;

        let unbind = UnbindOperation::new("spire", binding.clone(), false);
        drive(unbind, &context).await.unwrap();

        let stored = get(&handle, S3_BUCKET_KEYSPACE, b"spire".to_vec()).await;
        assert!(
            BucketInfo::from_bytes(&stored.unwrap())
                .unwrap()
                .distribution
                .is_none()
        );
        let marked = get(&handle, NODE_STATS_KEYSPACE, dirty_group_key(group_id)).await;
        assert!(marked.is_some(), "the group's share is published again");
        assert!(
            get(&handle, NODE_STATS_KEYSPACE, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
    }

    #[test]
    fn reclaim_awaits_copies() {
        // A range given away is purged only once it has its copies again, split children too.
        use crate::distribution::test_record::member;
        use aruna_core::structs::storage::distribution::fence::RangeFence;
        let (mut snapshot, _) = crate::distribution::test_record::replicated(true, 1);
        let parent = snapshot.config.ranges[0].range_id;
        let child = &mut snapshot.config.ranges[0];
        (child.range_id, child.lineage) = (Ulid::from_bytes([60; 16]), Some(parent));
        (child.coordinator, child.replicas) = (member(2), Vec::new());
        let former = snapshot.member(member(1)).unwrap().clone();
        let mut fence = LocalFence::default();
        fence.fence_range(RangeFence {
            range_id: parent,
            transfer_id: Ulid::from_bytes([61; 16]),
            epoch: 1,
            since_ms: 0,
            uploads_open: false,
        });
        fence.copies.push((parent, 2));
        let given = given_away(&snapshot, &fence, &former);
        let child_id = Ulid::from_bytes([60; 16]);
        assert_eq!(
            given.len(),
            1,
            "the split child of the fenced range is found"
        );
        assert_eq!((given[0].0.range_id, given[0].1), (child_id, false));
        fence.copies = vec![(parent, 1)];
        assert!(
            given_away(&snapshot, &fence, &former)[0].1,
            "one copy is enough now"
        );
        let mut all = former.clone();
        all.coverage = MetadataCoverage::All;
        assert!(given_away(&snapshot, &fence, &all).is_empty());
    }

    #[tokio::test]
    async fn replaced_member_departs() {
        // A join replaced this node's member 3 before its bucket was released.
        use crate::distribution::test_record::{realm, replicated, secret, store_record};
        use crate::distribution::test_store::{context, get, open};
        use aruna_core::structs::storage::distribution::snapshot::Tombstone;
        use aruna_core::structs::storage::distribution::{DistributionView, SignedSnapshot};
        let (mut snapshot, _) = replicated(true, 1);
        let set_id = snapshot.set_id();
        snapshot.tombstones.push(Tombstone {
            member_id: Ulid::from_bytes([3; 16]),
            incarnation: 5,
        });
        let view = DistributionView {
            head: SignedSnapshot::sign(snapshot.clone(), realm(), &secret(1)).unwrap(),
            conflict: None,
        };
        let (_dir, handle) = open();
        store_record(&handle, set_id, view.to_bytes().unwrap()).await;
        let left = DistributionBinding {
            member_id: Ulid::from_bytes([3; 16]),
            ..crate::distribution::test_record::binding(&snapshot, 2, 5)
        };
        let info = BucketInfo {
            group_id: snapshot.config.owner_group,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: Some(left),
        };
        let account = account_key(set_id, snapshot.config.ranges[0].range_id);
        for (space, key, value) in [
            (
                S3_BUCKET_KEYSPACE,
                b"spire".to_vec(),
                info.to_bytes().unwrap(),
            ),
            (RANGE_ACCOUNT_KEYSPACE, account.clone(), vec![1]),
        ] {
            let write = StorageEffect::Write {
                key_space: space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            };
            handle.send_storage_effect(write).await;
        }
        let context = Arc::new(context(&handle));
        let ids = (realm(), secret(2).public());
        let work = SetWorkOperation::new(ids, set_id, 64, BTreeMap::new());
        let worked = drive(work, &context).await.unwrap();
        assert_eq!(worked.left, Left::Waiting);
        let stored = get(&handle, S3_BUCKET_KEYSPACE, b"spire".to_vec())
            .await
            .unwrap();
        assert_eq!(BucketInfo::from_bytes(&stored).unwrap().distribution, None);
        assert_eq!(get(&handle, RANGE_ACCOUNT_KEYSPACE, account).await, None);
        let fence = get(
            &handle,
            DISTRIBUTION_FENCE_KEYSPACE,
            LocalFence::key(set_id),
        )
        .await;
        assert_eq!(fence, None, "the new member starts without the old fence");
    }

    #[tokio::test]
    async fn drops_settled_imports() {
        // A former coordinator drops the imports it kept for a transfer no longer open only.
        use crate::distribution::imports::KeepResumeOperation;
        use crate::distribution::test_record::store_record;
        use crate::distribution::test_record::{binding, member, realm, replicated, secret};
        use crate::distribution::test_store::{context, get, open};
        use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
        use aruna_core::structs::storage::distribution::fence::RangeFence;
        use aruna_core::structs::storage::distribution::{
            DistributionView, SignedSnapshot, TransferRole,
        };
        let (mut snapshot, _) = replicated(true, 1);
        let set_id = snapshot.set_id();
        let ranges = [0, 1].map(|index| snapshot.config.ranges[index].range_id);
        let (settled, open_id) = (Ulid::from_bytes([61; 16]), Ulid::from_bytes([62; 16]));
        snapshot.lifecycle.transfers.push(OpenTransfer {
            transfer_id: open_id,
            range_id: ranges[1],
            from: member(2),
            to: member(1),
            epoch: 2,
            role: TransferRole::Replica,
            fenced: None,
            completed: None,
        });
        let view = DistributionView {
            head: SignedSnapshot::sign(snapshot.clone(), realm(), &secret(1)).unwrap(),
            conflict: None,
        };
        let (_dir, handle) = open();
        store_record(&handle, set_id, view.to_bytes().unwrap()).await;
        let info = BucketInfo {
            group_id: snapshot.config.owner_group,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: Some(binding(&snapshot, 1, 5)),
        };
        let mut fence = LocalFence::default();
        for (range_id, transfer_id) in [(ranges[0], settled), (ranges[1], open_id)] {
            fence.fence_range(RangeFence {
                range_id,
                transfer_id,
                epoch: 1,
                since_ms: 0,
                uploads_open: false,
            });
        }
        for (space, key, value) in [
            (S3_BUCKET_KEYSPACE, b"spire".to_vec(), info.to_bytes()),
            (
                DISTRIBUTION_FENCE_KEYSPACE,
                LocalFence::key(set_id),
                fence.to_bytes(),
            ),
        ] {
            let write = StorageEffect::Write {
                key_space: space.to_string(),
                key: key.into(),
                value: value.unwrap().into(),
                txn_id: None,
            };
            handle.send_storage_effect(write).await;
        }
        let context = Arc::new(context(&handle));
        for transfer_id in [settled, open_id] {
            let keep = KeepResumeOperation::new("spire", transfer_id);
            drive(keep, &context).await.unwrap();
        }
        let ids = (realm(), secret(1).public());
        let work = SetWorkOperation::new(ids, set_id, 64, BTreeMap::new());
        let worked = drive(work, &context).await.unwrap();
        assert_eq!(worked.left, Left::Waiting);
        let jobs = |id: Ulid| [b"import_jobs/".as_slice(), &id.to_bytes()].concat();
        let kept = get(&handle, NODE_STATE_KEYSPACE, jobs(settled)).await;
        assert_eq!(kept, None, "a transfer no longer open keeps no imports");
        let kept = get(&handle, NODE_STATE_KEYSPACE, jobs(open_id)).await;
        assert!(kept.is_some(), "an open transfer keeps its imports");
    }

    #[tokio::test]
    async fn settling_keeps_newer() {
        // A marker rewritten by a newer record after the pass read it stays.
        let (_dir, storage_handle) = crate::distribution::test_store::open();
        let context = crate::driver::DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let space = DISTRIBUTION_LIFECYCLE_KEYSPACE;
        let key = [1u8; 16];
        let put = |revision: u64| StorageEffect::Write {
            key_space: space.to_string(),
            key: key.to_vec().into(),
            value: revision.to_be_bytes().to_vec().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(put(8)).await;
        let settle = |seen: u64| {
            SettleMarkerOperation::new(space, key.to_vec(), seen.to_be_bytes().to_vec())
        };
        let read = || ReadRowOperation {
            key_space: space.to_string(),
            key: key.to_vec(),
        };
        drive(settle(7), &context).await.unwrap();
        let kept = drive(read(), &context).await.unwrap();
        assert!(kept.is_some(), "a newer marker stays");
        drive(settle(8), &context).await.unwrap();
        assert_eq!(drive(read(), &context).await.unwrap(), None);
    }

    #[test]
    fn read_failure_retries() {
        // Only a missing bucket clears the set; a failed read retries instead.
        let set_id = Ulid::from_bytes([1; 16]);
        let gone = set_binding(set_id, Err(GetBucketError::NotFound));
        assert_eq!(gone, Ok(None));
        let failed = set_binding(set_id, Err(GetBucketError::Incomplete));
        assert!(failed.is_err(), "a failed read is not an absent bucket");
    }

    #[test]
    fn completion_awaits_counts() {
        // The new coordinator completes only once its copy holds what the fenced range held.
        let fenced = RangeCounters {
            versions: 5,
            delete_markers: 2,
            max_epoch: OriginalEpoch(3),
            ..RangeCounters::default()
        };
        let watermark = Watermark::of(&fenced);
        assert!(watermark.reached_by(&fenced));
        let behind = RangeCounters {
            delete_markers: 1,
            ..fenced
        };
        assert!(!watermark.reached_by(&behind));
        let older = RangeCounters {
            max_epoch: OriginalEpoch(2),
            ..fenced
        };
        assert!(!watermark.reached_by(&older));
        assert!(watermark.below(4) && !watermark.below(3));
        let bytes = postcard::to_allocvec(&watermark).unwrap();
        assert_eq!(
            postcard::from_bytes::<Watermark>(&bytes).unwrap(),
            watermark
        );
    }
}
