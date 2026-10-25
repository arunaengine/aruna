//! Does this node's part of the open lifecycle work of its sets: it fences a set for deletion,
//! states the fence and purges its member bucket once the deletion commits; it fences a range
//! it transfers away, drains its uploads and states the fence; it completes a transfer it
//! receives; it reclaims a range it gave away and releases its bucket when it leaves. Every
//! pass is bounded and resumes from durable state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StatementSignEvent, StorageEvent};
use aruna_core::keyspaces::{
    DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_LIFECYCLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
    RANGE_COUNTER_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, RangeAccount, RangeCounters, account_key, generation_key,
};
use aruna_core::structs::storage::distribution::fence::{LocalFence, RangeFence};
use aruna_core::structs::storage::distribution::snapshot::OpenTransfer;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionMember, DistributionSnapshot, KeyRange, MemberState,
    Statement, StatementClaim, StatementKind,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Key;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use ulid::Ulid;

use super::create::submit;
use super::purge::{
    PurgeTarget, Purged, abort_range_uploads, batch_delete, clear_set_rows, purge_range,
    purge_rest, read,
};
use super::sign_statement;
use super::status::read_view;
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::get::GetBucketOperation;

/// Sets one pass looks at, and purge pages it spends at most.
const SET_BATCH: usize = 16;
const PURGE_PAGES: usize = 32;
/// How soon the next pass comes while lifecycle work waits.
pub const LIFECYCLE_POLL: Duration = Duration::from_secs(5);
const FENCE_ATTEMPTS: usize = 4;
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
    for key in rows {
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
                if let Err(error) = batch_delete(context, vec![marker(&key)]).await {
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
async fn scan_markers(context: &DriverContext) -> Result<Vec<Vec<u8>>, String> {
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
            let keys: Vec<Vec<u8>> = values.into_iter().map(|(key, _)| key.to_vec()).collect();
            let next = (keys.len() == SET_BATCH)
                .then(|| keys.last().cloned())
                .flatten();
            if let Ok(mut cursor) = MARKER_CURSOR.lock() {
                *cursor = next;
            }
            Ok(keys)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected scan answer {other:?}")),
    }
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
        let binding = self.binding(context, snapshot, member).await?;
        let local = Local { member, binding };
        if snapshot.lifecycle.deletion.is_some() {
            return self.delete(context, snapshot, &local, budget).await;
        }
        let mut left = Left::Settled;
        if member.state == MemberState::Leaving {
            left = left.and(self.leave(context, snapshot, &local).await?);
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

    /// The local bucket's binding, only while it is this member's bucket.
    async fn binding(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        member: &DistributionMember,
    ) -> Result<Option<DistributionBinding>, String> {
        let bucket = GetBucketOperation::new(snapshot.config.bucket.clone());
        let Ok(info) = drive(bucket, context).await else {
            return Ok(None);
        };
        Ok(info
            .distribution
            .filter(|binding| owns(self.set_id, member, binding)))
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
            return self.clear_set(context, true).await;
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
                self.clear_set(context, true).await
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
    /// bootstrap, purge cursors and kept copies, in bounded batches.
    async fn clear_set(&self, context: &DriverContext, keep_fence: bool) -> Result<Left, String> {
        clear_set_rows(context, self.set_id, keep_fence).await?;
        Ok(Left::Settled)
    }

    /// A leaving member refuses publications, then releases its bucket as an ordinary one.
    async fn leave(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        local: &Local<'_>,
    ) -> Result<Left, String> {
        if let Some(binding) = &local.binding {
            update_fence(context, self.set_id, |fence| {
                !std::mem::replace(&mut fence.closed, true)
            })
            .await?;
            unbind(context, &snapshot.config.bucket, binding).await?;
        }
        // The released bucket checks no fence; a later join of this node starts afresh.
        self.clear_set(context, false).await
    }

    /// The former coordinator's side: fence the range, abort its uploads, state the fence.
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
        };
        update_fence(context, self.set_id, |local| {
            local.fence_range(fence.clone())
        })
        .await?;
        if open.fenced.is_some() {
            // Keep the set until the range is reclaimed after the activation.
            return Ok(Left::Waiting);
        }
        let Some(binding) = local.binding.clone() else {
            return Ok(Left::Waiting);
        };
        if abort_range_uploads(context, &self.target(snapshot, &binding, range)).await? {
            return Ok(Left::Waiting);
        }
        let Some(counters) = range_counters(context, self.set_id, range.range_id).await? else {
            return Ok(Left::Waiting);
        };
        let watermark =
            postcard::to_allocvec(&Watermark::of(&counters)).map_err(|error| error.to_string())?;
        // Imports and refreshes keep per-job checkpoints, not per-range cursors (see report).
        let kind = StatementKind::TransferFenced {
            transfer_id: open.transfer_id,
            range_id: range.range_id,
            epoch: range.epoch,
            next_epoch: open.epoch,
            watermark,
            import_cursor: None,
            refresh_cursor: None,
            open_uploads: 0,
        };
        let subject = open.transfer_id.to_bytes();
        self.state(context, snapshot, local.member, &subject, kind)
            .await?;
        Ok(Left::Waiting)
    }

    /// The new coordinator's side: complete once its copy reached the fenced watermark.
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
        let watermark = (snapshot.statements.iter())
            .find(|signed| signed.statement.digest() == fenced)
            .and_then(|signed| match &signed.statement.kind {
                StatementKind::TransferFenced { watermark, .. } => Some(watermark.clone()),
                _ => None,
            });
        let Some(bytes) = watermark else {
            return Ok(Left::Waiting);
        };
        let wanted: Watermark = postcard::from_bytes(&bytes).map_err(|error| error.to_string())?;
        let reached = range_counters(context, self.set_id, open.range_id)
            .await?
            .is_some_and(|counters| wanted.reached_by(&counters));
        if !reached || local.binding.is_none() {
            return Ok(Left::Waiting);
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

    /// Purges a range this node fenced for a transfer once the record no longer has it hold
    /// the range at all and the range's coordinator and every listed replica are ready.
    async fn reclaim(
        &self,
        context: &Arc<DriverContext>,
        snapshot: &DistributionSnapshot,
        local: &Local<'_>,
        budget: &mut usize,
    ) -> Result<Left, String> {
        let (Some(binding), member_id) = (local.binding.clone(), local.member.member_id) else {
            return Ok(Left::Settled);
        };
        let stored = read(
            context,
            DISTRIBUTION_FENCE_KEYSPACE,
            LocalFence::key(self.set_id),
        )
        .await?;
        let fence = LocalFence::decode(stored.as_deref()).map_err(|error| error.to_string())?;
        let held = snapshot.held_ranges(member_id);
        let mut left = Left::Settled;
        for fenced in &fence.ranges {
            let Some(range) =
                (snapshot.config.ranges.iter()).find(|r| r.range_id == fenced.range_id)
            else {
                continue;
            };
            if held.iter().any(|r| r.range_id == range.range_id) {
                continue;
            }
            let moving = snapshot
                .lifecycle
                .transfers
                .iter()
                .any(|t| t.range_id == range.range_id);
            // The copies stay until the range keeps every listed replica without this node.
            if moving || !snapshot.range_ready(range) {
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

/// Whether `binding` names the bucket of `member`: same set and member at its admitted
/// incarnation. A member that never admitted owns the bucket it created, whatever incarnation.
fn owns(set_id: Ulid, member: &DistributionMember, binding: &DistributionBinding) -> bool {
    binding.set_id == set_id
        && binding.member_id == member.member_id
        && (member.incarnation).is_none_or(|admitted| admitted == binding.incarnation)
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

/// Releases the member bucket as an ordinary bucket, only while it carries `binding`.
async fn unbind(
    context: &DriverContext,
    bucket: &str,
    binding: &DistributionBinding,
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
    fn created_member_owns_its_bucket() {
        // A bucket created for a member that was never admitted is still deleted with the set.
        let (snapshot, _) = crate::distribution::test_record::record(true);
        let mut member = snapshot.config.members[0].clone();
        let set_id = snapshot.set_id();
        let bound = |incarnation| DistributionBinding {
            set_id,
            member_id: member.member_id,
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
    fn completion_waits_for_every_count() {
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
        let bytes = postcard::to_allocvec(&watermark).unwrap();
        assert_eq!(
            postcard::from_bytes::<Watermark>(&bytes).unwrap(),
            watermark
        );
    }
}
