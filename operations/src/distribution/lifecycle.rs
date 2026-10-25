//! Does this node's part of its sets' open lifecycle work: deletion fences and purges, transfer
//! fences and completions, reclaims of ranges it gave away and bucket release when it leaves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A transfer away drains its uploads before its fence is stated; a deletion purges the member
// bucket once it commits. Every pass is bounded and resumes from durable state.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StatementSignEvent, StorageEvent};
use aruna_core::keyspaces::{
    DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_LIFECYCLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
    RANGE_COUNTER_KEYSPACE, S3_BUCKET_KEYSPACE,
};
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
    DistributionBinding, DistributionMember, DistributionSnapshot, KeyRange, MemberState,
    MetadataCoverage, Statement, StatementClaim, StatementKind,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Key;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use ulid::Ulid;

use super::create::submit;
use super::imports::{keep_imports, open_imports};
use super::purge::{
    PurgeTarget, Purged, abort_range_uploads, batch_delete, clear_set_rows, has_uploads,
    purge_range, purge_rest, read,
};
use super::roots::roots_checked;
use super::sign_statement;
use super::status::read_view;
use crate::driver::{DriverContext, drive};
use crate::replication::members::owes_fanout;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// Sets one pass looks at, and purge pages it spends at most.
const SET_BATCH: usize = 16;
const PURGE_PAGES: usize = 32;
/// How soon the next pass comes while lifecycle work waits.
pub const LIFECYCLE_POLL: Duration = Duration::from_secs(5);
const FENCE_ATTEMPTS: usize = 4;
/// How long uploads opened before a transfer fence may still complete before they are aborted.
const UPLOAD_GRACE_MS: u64 = 5 * 60 * 1000;
/// The set marker after which the next pass continues; a restart begins at the first set.
static MARKER_CURSOR: Mutex<Option<Vec<u8>>> = Mutex::new(None);

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

/// One bounded pass over the sets with open lifecycle work. Returns when to look again.
pub async fn tend_lifecycle(context: &Arc<DriverContext>) -> Option<Duration> {
    let net = context.net_handle.as_ref()?;
    let (realm_id, local) = (*net.realm_id(), net.node_id());
    let rows = match scan_markers(context).await {
        Ok(rows) => rows,
        Err(error) => {
            warn!(%error, "Lifecycle work could not read its sets");
            return Some(LIFECYCLE_POLL);
        }
    };
    let mut waiting = false;
    let mut budget = PURGE_PAGES;
    for (key, seen) in rows {
        let Ok(set_id) = <[u8; 16]>::try_from(key.as_slice()).map(Ulid::from_bytes) else {
            let _ = batch_delete(context, vec![marker(&key)]).await;
            continue;
        };
        let set = SetWork {
            realm_id,
            local,
            set_id,
        };
        match set.run(context, &mut budget).await {
            Ok(Left::Settled) => {
                let space = DISTRIBUTION_LIFECYCLE_KEYSPACE;
                if let Err(error) = settle_marker(context, space, &key, &seen).await {
                    warn!(%set_id, %error, "A settled lifecycle row stays until the next pass");
                }
            }
            Ok(Left::Waiting) => waiting = true,
            Err(error) => {
                warn!(%set_id, %error, "Distributed bucket lifecycle work failed; retrying");
                waiting = true;
            }
        }
    }
    waiting.then_some(LIFECYCLE_POLL)
}

fn marker(key: &[u8]) -> (String, Key) {
    (
        DISTRIBUTION_LIFECYCLE_KEYSPACE.to_string(),
        key.to_vec().into(),
    )
}

/// Reads the next batch of sets after the last one looked at, so sets that keep waiting never
/// starve later ones; a short batch makes the next pass start over.
async fn scan_markers(context: &DriverContext) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
    let after = MARKER_CURSOR
        .lock()
        .map(|cursor| cursor.clone())
        .unwrap_or(None);
    let iter = StorageEffect::Iter {
        key_space: DISTRIBUTION_LIFECYCLE_KEYSPACE.to_string(),
        prefix: None,
        start: after.map(|key| IterStart::After(key.into())),
        limit: SET_BATCH,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            let rows: Vec<(Vec<u8>, Vec<u8>)> = (values.into_iter())
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect();
            let next = (rows.len() == SET_BATCH)
                .then(|| rows.last().map(|(key, _)| key.clone()))
                .flatten();
            if let Ok(mut cursor) = MARKER_CURSOR.lock() {
                *cursor = next;
            }
            Ok(rows)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected scan answer {other:?}")),
    }
}

/// Deletes a set's marker row only while it still holds `seen`: a record installed since the
/// pass read it wrote a newer revision, and its work is looked at again.
pub(crate) async fn settle_marker(
    context: &DriverContext,
    key_space: &str,
    key: &[u8],
    seen: &[u8],
) -> Result<(), String> {
    let storage = &context.storage_handle;
    let started = storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await;
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = started else {
        return Err(format!("the marker transaction did not start: {started:?}"));
    };
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.to_vec().into(),
        txn_id: Some(txn_id),
    };
    let current = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(format!("the marker was not read: {other:?}"));
        }
    };
    if current.as_deref() != Some(seen) {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return Ok(());
    }
    let effects = [
        StorageEffect::BatchDelete {
            deletes: vec![(key_space.to_string(), key.to_vec().into())],
            txn_id: Some(txn_id),
        },
        StorageEffect::CommitTransaction { txn_id },
    ];
    for effect in effects {
        if let Event::Storage(StorageEvent::Error { error }) =
            storage.send_storage_effect(effect).await
        {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(error.to_string());
        }
    }
    Ok(())
}

/// One set's lifecycle work on this node.
struct SetWork {
    realm_id: RealmId,
    local: NodeId,
    set_id: Ulid,
}

/// This node's member of a set and its local bucket binding, while the bucket is bound.
struct Local<'a> {
    member: &'a DistributionMember,
    binding: Option<DistributionBinding>,
}

impl SetWork {
    async fn run(&self, context: &Arc<DriverContext>, budget: &mut usize) -> Result<Left, String> {
        let Some(view) = read_view(context, self.set_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(Left::Settled);
        };
        let snapshot = &view.head.snapshot;
        let Some(member) = (snapshot.config.members.iter()).find(|m| m.node_id == self.local)
        else {
            return Ok(Left::Settled);
        };
        let bound = self.binding(context, snapshot).await?;
        if let Some(left) = bound
            .as_ref()
            .filter(|binding| replaced(snapshot, member, binding))
        {
            // A replaced member's departure finishes before the new member's work.
            return self.depart(context, snapshot, left, budget).await;
        }
        let binding = bound.filter(|binding| owns(self.set_id, member, binding));
        let local = Local { member, binding };
        if snapshot.lifecycle.deletion.is_some() {
            return self.delete(context, snapshot, &local, budget).await;
        }
        let mut left = Left::Settled;
        if member.state == MemberState::Leaving {
            left = left.and(self.leave(context, snapshot, &local, budget).await?);
        }
        for open in &snapshot.lifecycle.transfers {
            if open.from == member.member_id {
                left = left.and(self.hand_over(context, snapshot, &local, open).await?);
            } else if open.to == member.member_id {
                left = left.and(self.take_over(context, snapshot, &local, open).await?);
            }
        }
        if !view.conflicted() {
            left = left.and(self.reclaim(context, snapshot, &local, budget).await?);
        }
        Ok(left)
    }

    /// The local bucket's binding to this set.
    async fn binding(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
    ) -> Result<Option<DistributionBinding>, String> {
        let bucket = GetBucketOperation::new(snapshot.config.bucket.clone());
        set_binding(self.set_id, drive(bucket, context).await)
    }

    /// Releases the bucket of a member a join replaced: fences it and clears the set's rows,
    /// then unbinds it and drops the fence in one transaction.
    async fn depart(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        binding: &DistributionBinding,
        budget: &mut usize,
    ) -> Result<Left, String> {
        update_fence(context, self.set_id, |fence| {
            !std::mem::replace(&mut fence.closed, true)
        })
        .await?;
        let clearing = (snapshot.config.bucket.as_str(), true, Some(binding));
        if self.clear_set(context, clearing, budget).await? == Left::Waiting {
            return Ok(Left::Waiting);
        }
        unbind(context, &snapshot.config.bucket, binding, true).await?;
        // The new member's work follows in the next pass.
        Ok(Left::Waiting)
    }

    /// Fences the whole set, states it as a coordinator, and purges once it is committed.
    async fn delete(
        &self,
        context: &Arc<DriverContext>,
        snapshot: &DistributionSnapshot,
        local: &Local<'_>,
        budget: &mut usize,
    ) -> Result<Left, String> {
        let Some(deletion) = snapshot.lifecycle.deletion.as_ref() else {
            return Ok(Left::Settled);
        };
        let identity = deletion.identity(self.set_id);
        update_fence(context, self.set_id, |fence| {
            let changed = fence.deletion != Some(identity) || !fence.closed;
            fence.deletion = Some(identity);
            fence.closed = true;
            changed
        })
        .await?;
        let member_id = local.member.member_id;
        let coordinates = snapshot
            .config
            .ranges
            .iter()
            .any(|r| r.coordinator == member_id);
        if !deletion.committed {
            if coordinates && !deletion.fenced.contains(&member_id) {
                let kind = StatementKind::DeleteFenced { deletion: identity };
                self.state(context, snapshot, local.member, &identity, kind)
                    .await?;
            }
            return Ok(Left::Waiting);
        }
        let Some(binding) = local.binding.clone() else {
            // The member bucket is gone or replaced: nothing of the set is left here.
            return self
                .clear_set(context, (&snapshot.config.bucket, true, None), budget)
                .await;
        };
        for range in &snapshot.config.ranges {
            let target = self.target(snapshot, &binding, range);
            if purge_range(context, &target, budget).await? == Purged::More {
                return Ok(Left::Waiting);
            }
        }
        let delete =
            crate::s3::bucket::delete::DeleteBucketOperation::new(snapshot.config.bucket.clone())
                .bound_to(binding.clone());
        match drive(delete, context.as_ref()).await {
            Ok(()) | Err(crate::s3::bucket::delete::DeleteBucketError::NotFound) => {
                self.clear_set(context, (&snapshot.config.bucket, true, None), budget)
                    .await
            }
            Err(error) => {
                // Versions the inventory did not name block the delete; their walk follows.
                debug!(set_id = %self.set_id, %error, "The member bucket is not empty yet");
                if let Some(range) = snapshot.config.ranges.first() {
                    purge_rest(context, &self.target(snapshot, &binding, range)).await?;
                }
                Ok(Left::Waiting)
            }
        }
    }

    fn target(
        &self,
        snapshot: &DistributionSnapshot,
        binding: &DistributionBinding,
        range: &KeyRange,
    ) -> PurgeTarget {
        PurgeTarget {
            realm_id: self.realm_id,
            node_id: self.local,
            bucket: snapshot.config.bucket.clone(),
            group_id: snapshot.config.owner_group,
            binding: binding.clone(),
            range_id: range.range_id,
            lower: range.lower.clone(),
            upper: range.upper.clone(),
        }
    }

    /// Deletes what this node keeps of the set besides its bucket: inventory, accounting,
    /// bootstrap, purge cursors and kept copies, a bounded number of pages per pass.
    async fn clear_set(
        &self,
        context: &DriverContext,
        (bucket, keep_fence, clearing): (&str, bool, Option<&DistributionBinding>),
        budget: &mut usize,
    ) -> Result<Left, String> {
        let rows = (self.set_id, bucket, clearing);
        match clear_set_rows(context, rows, keep_fence, budget).await? {
            Purged::Done => Ok(Left::Settled),
            Purged::More => Ok(Left::Waiting),
        }
    }

    /// A leaving member refuses publications, then releases its bucket as an ordinary one.
    async fn leave(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        local: &Local<'_>,
        budget: &mut usize,
    ) -> Result<Left, String> {
        if let Some(binding) = &local.binding {
            update_fence(context, self.set_id, |fence| {
                !std::mem::replace(&mut fence.closed, true)
            })
            .await?;
            unbind(context, &snapshot.config.bucket, binding, false).await?;
        }
        // The released bucket checks no fence; a later join of this node starts afresh.
        self.clear_set(context, (&snapshot.config.bucket, false, None), budget)
            .await
    }

    /// The former coordinator's side: fence the range, let its open uploads complete for a
    /// bounded grace period, abort the rest, then state the fence.
    async fn hand_over(
        &self,
        context: &Arc<DriverContext>,
        snapshot: &DistributionSnapshot,
        local: &Local<'_>,
        open: &OpenTransfer,
    ) -> Result<Left, String> {
        let Some(range) = (snapshot.config.ranges.iter()).find(|r| r.range_id == open.range_id)
        else {
            return Ok(Left::Settled);
        };
        let fence = RangeFence {
            range_id: range.range_id,
            transfer_id: open.transfer_id,
            epoch: range.epoch,
            since_ms: unix_timestamp_millis(),
            uploads_open: true,
        };
        // The byte copies the range has now; this node's copy goes only once it has them again.
        let copies = snapshot.byte_holders(range, None).len();
        let copies = u32::try_from(copies).unwrap_or(u32::MAX);
        let fenced = update_fence(context, self.set_id, |local| {
            let fenced = local.fence_range(fence.clone());
            if fenced {
                local.copies.retain(|(id, _)| *id != range.range_id);
                local.copies.push((range.range_id, copies));
            }
            fenced
        })
        .await?;
        if open.fenced.is_some() {
            // Keep the set until the range is reclaimed after the activation.
            return Ok(Left::Waiting);
        }
        let Some(binding) = local.binding.clone() else {
            return Ok(Left::Waiting);
        };
        let target = self.target(snapshot, &binding, range);
        if let Some(grace) = fenced.range(range.range_id).filter(|f| f.uploads_open) {
            let ends = grace.since_ms.saturating_add(UPLOAD_GRACE_MS);
            if unix_timestamp_millis() < ends && has_uploads(context, &target).await? {
                return Ok(Left::Waiting);
            }
            // Closing conflicts every completion still in flight, so none commits after it.
            update_fence(context, self.set_id, |local| {
                local.close_uploads(range.range_id)
            })
            .await?;
        }
        if abort_range_uploads(context, &target).await? {
            return Ok(Left::Waiting);
        }
        // The new coordinator learns nothing of what this node still owes other members.
        if owes_fanout(context, &snapshot.config.bucket, self.set_id, range).await? {
            return Ok(Left::Waiting);
        }
        let Some(counters) = range_counters(context, self.set_id, range.range_id).await? else {
            return Ok(Left::Waiting);
        };
        let watermark =
            postcard::to_allocvec(&Watermark::of(&counters)).map_err(|error| error.to_string())?;
        // Open imports of the bucket go on at the new coordinator; refresh rounds start anew.
        let jobs = open_imports(context, &snapshot.config.bucket).await?;
        let import_cursor = match jobs.is_empty() {
            true => None,
            false => Some(postcard::to_allocvec(&jobs).map_err(|error| error.to_string())?),
        };
        if import_cursor
            .as_ref()
            .is_some_and(|jobs| jobs.len() > MAX_CURSOR_BYTES)
        {
            return Ok(Left::Waiting);
        }
        let kind = StatementKind::TransferFenced {
            transfer_id: open.transfer_id,
            range_id: range.range_id,
            epoch: range.epoch,
            next_epoch: open.epoch,
            watermark,
            import_cursor,
            refresh_cursor: None,
            open_uploads: 0,
        };
        let subject = open.transfer_id.to_bytes();
        self.state(context, snapshot, local.member, &subject, kind)
            .await?;
        Ok(Left::Waiting)
    }

    /// The new coordinator's side: complete once its copy reached the fenced watermark and
    /// every source root it holds references of is checked.
    async fn take_over(
        &self,
        context: &Arc<DriverContext>,
        snapshot: &DistributionSnapshot,
        local: &Local<'_>,
        open: &OpenTransfer,
    ) -> Result<Left, String> {
        let (Some(fenced), None) = (open.fenced, open.completed) else {
            return Ok(Left::Waiting);
        };
        let fenced_parts = (snapshot.statements.iter())
            .find(|signed| signed.statement.digest() == fenced)
            .and_then(|signed| match &signed.statement.kind {
                StatementKind::TransferFenced {
                    watermark,
                    import_cursor,
                    ..
                } => Some((watermark.clone(), import_cursor.clone())),
                _ => None,
            });
        let Some((bytes, imports)) = fenced_parts else {
            return Ok(Left::Waiting);
        };
        let wanted: Watermark = postcard::from_bytes(&bytes).map_err(|error| error.to_string())?;
        let Some(counters) = range_counters(context, self.set_id, open.range_id).await? else {
            return Ok(Left::Waiting);
        };
        if !wanted.reached_by(&counters) || local.binding.is_none() {
            return Ok(Left::Waiting);
        }
        // A holder's version at the new epoch or above would outrank the new coordinator.
        if !wanted.below(open.epoch) || !Watermark::of(&counters).below(open.epoch) {
            warn!(set_id = %self.set_id, transfer_id = %open.transfer_id, "A transfer epoch is not above its range's versions");
            return Ok(Left::Waiting);
        }
        // Every source root of what it applied must be readable here before it takes over.
        let group_id = snapshot.config.owner_group;
        if !roots_checked(context, self.set_id, group_id, open.transfer_id).await? {
            return Ok(Left::Waiting);
        }
        // The former coordinator's open imports are kept to run once this node coordinates.
        if let Some(imports) = imports {
            let jobs: Vec<JobId> = postcard::from_bytes(&imports).map_err(|e| e.to_string())?;
            let range = (self.set_id, open.range_id);
            let from = (snapshot.config.members.iter()).find(|m| m.member_id == open.from);
            let Some(from) = from.map(|member| member.node_id) else {
                return Ok(Left::Waiting);
            };
            keep_imports(context, range, from, &jobs).await?;
        }
        let kind = StatementKind::TransferComplete {
            transfer_id: open.transfer_id,
            range_id: open.range_id,
            fenced,
            watermark: bytes,
        };
        let subject = open.transfer_id.to_bytes();
        self.state(context, snapshot, local.member, &subject, kind)
            .await?;
        Ok(Left::Waiting)
    }

    /// Purges a range this node fenced for a transfer, or each range split from it, once the
    /// record no longer lists it as a byte holder, the range is ready and has at least as many
    /// ready byte copies without this node as it had at the fence.
    async fn reclaim(
        &self,
        context: &Arc<DriverContext>,
        snapshot: &DistributionSnapshot,
        local: &Local<'_>,
        budget: &mut usize,
    ) -> Result<Left, String> {
        let (Some(binding), member) = (local.binding.clone(), local.member) else {
            return Ok(Left::Settled);
        };
        let stored = read(
            context,
            DISTRIBUTION_FENCE_KEYSPACE,
            LocalFence::key(self.set_id),
        )
        .await?;
        let fence = LocalFence::decode(stored.as_deref()).map_err(|error| error.to_string())?;
        let mut left = Left::Settled;
        for (range, ready) in given_away(snapshot, &fence, member) {
            if !ready {
                left = Left::Waiting;
                continue;
            }
            let target = self.target(snapshot, &binding, range);
            if purge_range(context, &target, budget).await? == Purged::More {
                left = Left::Waiting;
            }
        }
        Ok(left)
    }

    /// Signs a lifecycle statement with this node's key and hands it to the publisher.
    async fn state(
        &self,
        context: &Arc<DriverContext>,
        snapshot: &DistributionSnapshot,
        member: &DistributionMember,
        subject: &[u8],
        kind: StatementKind,
    ) -> Result<(), String> {
        let statement = Statement::lifecycle(self.realm_id, self.set_id, member, subject, kind);
        let claim = StatementClaim {
            signer: self.local,
            statement,
        };
        let signed = match sign_statement(context, claim) {
            StatementSignEvent::Signed(signed) => *signed,
            StatementSignEvent::Unavailable(reason) => return Err(reason),
        };
        if let Err(refusal) = submit(context, snapshot.root.publisher, self.local, signed).await {
            // An unreachable publisher keeps the fence; the statement is sent again next pass.
            debug!(set_id = %self.set_id, %refusal, "A lifecycle statement was not taken yet");
        }
        Ok(())
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
pub(super) async fn range_counters(
    context: &DriverContext,
    set_id: Ulid,
    range_id: Ulid,
) -> Result<Option<RangeCounters>, String> {
    let Some(account) = read(
        context,
        RANGE_ACCOUNT_KEYSPACE,
        account_key(set_id, range_id),
    )
    .await?
    else {
        return Ok(None);
    };
    let account = RangeAccount::from_bytes(&account).map_err(|error| error.to_string())?;
    let (Charge::Active(generation), None) = (account.charge, account.rebuild) else {
        return Ok(None);
    };
    let row = generation_key(set_id, range_id, generation);
    let counters = read(context, RANGE_COUNTER_KEYSPACE, row).await?;
    (counters.map(|value| RangeCounters::from_bytes(&value)))
        .transpose()
        .map(|counters| Some(counters.unwrap_or_default()))
        .map_err(|error| error.to_string())
}

/// Changes the set's fence row in one transaction; `change` says whether it changed anything.
/// A write that read the row before conflicts with this commit.
pub(super) async fn update_fence(
    context: &DriverContext,
    set_id: Ulid,
    change: impl Fn(&mut LocalFence) -> bool,
) -> Result<LocalFence, String> {
    let storage = &context.storage_handle;
    let key: Key = LocalFence::key(set_id).into();
    for _ in 0..FENCE_ATTEMPTS {
        let started = storage
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await;
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = started else {
            return Err(format!("the fence transaction did not start: {started:?}"));
        };
        let read = StorageEffect::Read {
            key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: Some(txn_id),
        };
        let value = match storage.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => {
                storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(format!("the fence row was not read: {other:?}"));
            }
        };
        let mut fence = LocalFence::decode(value.as_deref()).map_err(|error| error.to_string())?;
        if !change(&mut fence) {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(fence);
        }
        let value = fence.to_bytes().map_err(|error| error.to_string())?;
        let write = StorageEffect::Write {
            key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
            key: key.clone(),
            value: value.into(),
            txn_id: Some(txn_id),
        };
        if !matches!(
            storage.send_storage_effect(write).await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ) {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err("the fence row was not written".to_string());
        }
        match storage
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => return Ok(fence),
            Event::Storage(StorageEvent::Error { error }) if error.proves_no_commit() => continue,
            other => return Err(format!("the fence row was not committed: {other:?}")),
        }
    }
    Err("the fence row kept changing concurrently".to_string())
}

/// Releases the member bucket as an ordinary bucket, only while it carries `binding`; with
/// `drop_fence` the set's fence row goes in the same transaction.
async fn unbind(
    context: &DriverContext,
    bucket: &str,
    binding: &DistributionBinding,
    drop_fence: bool,
) -> Result<(), String> {
    let storage = &context.storage_handle;
    let started = storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await;
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = started else {
        return Err(format!("the release did not start: {started:?}"));
    };
    let key: Key = bucket.as_bytes().to_vec().into();
    let read = StorageEffect::Read {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: Some(txn_id),
    };
    let abort = StorageEffect::AbortTransaction { txn_id };
    let info = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            value.and_then(|value| BucketInfo::from_bytes(value.as_ref()).ok())
        }
        other => {
            storage.send_storage_effect(abort).await;
            return Err(format!("the bucket was not read: {other:?}"));
        }
    };
    let same = |info: &BucketInfo| {
        info.distribution.as_ref().is_some_and(|bound| {
            (bound.set_id, bound.member_id, bound.incarnation)
                == (binding.set_id, binding.member_id, binding.incarnation)
        })
    };
    let Some(mut info) = info.filter(same) else {
        storage.send_storage_effect(abort).await;
        return Ok(());
    };
    info.distribution = None;
    let value = info.to_bytes().map_err(|error| error.to_string())?;
    let write = StorageEffect::Write {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key,
        value: value.into(),
        txn_id: Some(txn_id),
    };
    storage.send_storage_effect(write).await;
    if drop_fence {
        let delete = StorageEffect::Delete {
            key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
            key: LocalFence::key(binding.set_id).into(),
            txn_id: Some(txn_id),
        };
        storage.send_storage_effect(delete).await;
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        other => Err(format!("the bucket was not released: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::distribution::OriginalEpoch;

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
        let work = SetWork {
            realm_id: realm(),
            local: secret(2).public(),
            set_id,
        };
        assert_eq!(work.run(&context, &mut 64).await, Ok(Left::Waiting));
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
    async fn settling_keeps_newer() {
        // A marker rewritten by a newer record after the pass read it stays.
        let (_dir, storage_handle) = crate::distribution::test_store::open();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
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
        let seen = 7u64.to_be_bytes();
        settle_marker(&context, space, &key, &seen).await.unwrap();
        let kept = read(&context, space, key.to_vec()).await.unwrap();
        assert!(kept.is_some(), "a newer marker stays");
        settle_marker(&context, space, &key, &8u64.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(read(&context, space, key.to_vec()).await.unwrap(), None);
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
