//! Works on the sets that name catching-up members or new range copies, within the drain's page
//! budget; a coordinator whose walks all ended states the catch-up to the record's publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StatementSignEvent, StorageEvent};
use aruna_core::keyspaces::{BOOTSTRAP_KEYSPACE, CATCH_UP_KEYSPACE};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::bootstrap::{BootstrapCursor, bootstrap_key};
use aruna_core::structs::storage::distribution::snapshot::RangeFill;
use aruna_core::structs::storage::distribution::{
    DistributionMember, DistributionSnapshot, KeyRange, Statement, StatementClaim,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, Value};
use aruna_storage::StorageHandle;
use tracing::{debug, warn};
use ulid::Ulid;

use super::bootstrap::{
    BootstrapPage, ClearCursorsOperation, PAGE_ROWS, PageConfig, PageProgress, ReadCursorOperation,
};
use super::members::ReadRecordOperation;
use super::queue::schedule_blob_drain;
use crate::distribution::create::submit;
use crate::distribution::lifecycle::settle_marker;
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::get::GetBucketOperation;

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

/// Rows of one keyspace under `prefix` after `after`, at most `limit`.
async fn scan(
    context: &DriverContext,
    key_space: &str,
    (prefix, after): (Option<Vec<u8>>, Option<Vec<u8>>),
    limit: usize,
) -> Result<Vec<(Vec<u8>, Value)>, String> {
    let iter = StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: prefix.map(Key::from),
        start: after.map(|key| IterStart::After(key.into())),
        limit,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values
            .into_iter()
            .map(|(key, value)| (key.to_vec(), value))
            .collect()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected scan answer {}", other.label())),
    }
}

async fn delete(context: &DriverContext, deletes: Vec<(String, Key)>) -> Result<(), String> {
    if deletes.is_empty() {
        return Ok(());
    }
    let effect = StorageEffect::BatchDelete {
        deletes,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected delete answer {}", other.label())),
    }
}

/// One stored cursor, read on its own key.
async fn read_cursor(
    context: &DriverContext,
    key: Vec<u8>,
) -> Result<Option<BootstrapCursor>, String> {
    let read = drive(ReadCursorOperation::new(key), context).await;
    read.map_err(|error| error.to_string())
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

/// Wakes the replication drain when a set waits for its bootstrap.
pub async fn wake_bootstrap(context: &DriverContext) {
    if let Some(task) = context.task_handle.as_ref()
        && bootstrap_waiting(&context.storage_handle).await
    {
        task.send_effect(schedule_blob_drain()).await;
    }
}

/// Whether any set still waits for its bootstrap here.
pub async fn bootstrap_waiting(storage: &StorageHandle) -> bool {
    let iter = StorageEffect::Iter {
        key_space: CATCH_UP_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit: 1,
        txn_id: None,
    };
    matches!(
        storage.send_storage_effect(iter).await,
        Event::Storage(StorageEvent::IterResult { values, .. }) if !values.is_empty()
    )
}

/// Works on the sets that name catching-up members, within the drain's page budget. Returns
/// whether some bootstrap still waits, so the drain looks again soon.
pub async fn process_bootstrap(context: &DriverContext) -> Result<bool, String> {
    let Some(net) = context.net_handle.as_ref() else {
        return Ok(false);
    };
    let (realm_id, local) = (*net.realm_id(), net.node_id());
    let mut budget = PAGE_BUDGET;
    let mut waiting = false;
    // Sets after the last batch come next, so waiting sets never starve later ones.
    let after = SET_CURSOR
        .lock()
        .map(|cursor| cursor.clone())
        .unwrap_or(None);
    let rows = scan(context, CATCH_UP_KEYSPACE, (None, after), SET_BATCH).await?;
    let next = (rows.len() == SET_BATCH)
        .then(|| rows.last().map(|(key, _)| key.clone()))
        .flatten();
    if let Ok(mut cursor) = SET_CURSOR.lock() {
        *cursor = next.clone();
    }
    // A pass that stopped short of the end looks again soon.
    waiting |= next.is_some();
    for (key, seen) in rows {
        let Ok(set_id) = <[u8; 16]>::try_from(key.as_slice()).map(Ulid::from_bytes) else {
            delete(context, vec![(CATCH_UP_KEYSPACE.to_string(), key.into())]).await?;
            continue;
        };
        let walk = SetWalk {
            realm_id,
            local,
            set_id,
        };
        match walk.run(context, &mut budget).await {
            Ok(Outcome::Settled) => settle_marker(context, CATCH_UP_KEYSPACE, &key, &seen).await?,
            Ok(Outcome::Waiting) => waiting = true,
            Ok(Outcome::Stated) => {}
            Err(error) => {
                warn!(%set_id, %error, "Distributed bucket bootstrap failed; retrying");
                waiting = true;
            }
        }
    }
    Ok(waiting)
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
struct SetWalk {
    realm_id: RealmId,
    local: NodeId,
    set_id: Ulid,
}

impl SetWalk {
    async fn run(&self, context: &DriverContext, budget: &mut usize) -> Result<Outcome, String> {
        let read = drive(ReadRecordOperation::new(self.set_id), context).await;
        let Some(view) = read.map_err(|error| error.to_string())? else {
            return Ok(Outcome::Settled);
        };
        let snapshot = view.head.snapshot;
        let targets = snapshot.catching_up();
        let local = snapshot
            .config
            .members
            .iter()
            .find(|m| m.node_id == self.local);
        let Some(local) = local else {
            return self.clear(context).await;
        };
        let deleting = snapshot.lifecycle.deletion.is_some();
        let fills: Vec<&RangeFill> = (snapshot.lifecycle.fills.iter())
            .filter(|_| !deleting)
            .filter(|fill| {
                (snapshot.config.ranges.iter()).any(|range| {
                    range.range_id == fill.range_id && range.coordinator == local.member_id
                })
            })
            .collect();
        if targets.is_empty() && fills.is_empty() {
            return self.clear(context).await;
        }
        // Only the admitted incarnation of this member's bucket publishes.
        let info = drive(
            GetBucketOperation::new(snapshot.config.bucket.clone()),
            context,
        )
        .await;
        let bound = info
            .ok()
            .and_then(|info| info.distribution)
            .is_some_and(|binding| {
                binding.member_id == local.member_id
                    && local.incarnation == Some(binding.incarnation)
            });
        if !bound {
            return self.clear(context).await;
        }
        let mut walked: Vec<Ulid> = targets.iter().map(|target| target.member_id).collect();
        walked.extend(fills.iter().map(|fill| fill.member_id));
        self.clear_departed(context, &walked).await?;
        let mut outcome = Outcome::Settled;
        for target in targets {
            let walked = self.walk(context, &snapshot, local, target, budget).await?;
            outcome = outcome.and(walked);
        }
        for fill in fills {
            let filled = self.fill(context, &snapshot, local, fill, budget).await?;
            outcome = outcome.and(filled);
        }
        Ok(outcome)
    }

    /// Walks one range for its new copy; when the walk ended, states the copy filled.
    async fn fill(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        local: &DistributionMember,
        fill: &RangeFill,
        budget: &mut usize,
    ) -> Result<Outcome, String> {
        let range = (snapshot.config.ranges.iter()).find(|range| range.range_id == fill.range_id);
        let (Some(range), Some(target)) = (range, snapshot.member(fill.member_id)) else {
            return Ok(Outcome::Settled);
        };
        // Until its node opened the range, the copy has nowhere to receive versions.
        if !fill.admitted {
            return Ok(Outcome::Waiting);
        }
        let intent = fill.intent();
        let key = bootstrap_key(self.set_id, fill.member_id, range.range_id);
        let cursor = (read_cursor(context, key).await?).filter(|c| c.intent == intent);
        let watermark = match cursor {
            Some(cursor) if cursor.done => cursor.watermark,
            _ if *budget == 0 => return Ok(Outcome::Waiting),
            _ => {
                *budget -= 1;
                let page = BootstrapPage::new(PageConfig {
                    realm_id: self.realm_id,
                    bucket: snapshot.config.bucket.clone(),
                    set_id: self.set_id,
                    member_id: fill.member_id,
                    node_id: target.node_id,
                    intent,
                    range: range.clone(),
                    now_ms: unix_timestamp_millis(),
                    rows: PAGE_ROWS,
                });
                match drive(page, context).await.map_err(|e| e.to_string())? {
                    PageProgress::Done { watermark } => watermark,
                    PageProgress::Waiting | PageProgress::Queued(_) => return Ok(Outcome::Waiting),
                }
            }
        };
        let statement = Statement::copy_filled(
            self.realm_id,
            self.set_id,
            local,
            fill,
            (range.epoch, watermark),
        );
        self.hand(context, snapshot, statement, fill.member_id)
            .await
    }

    /// Walks the ranges this node publishes to `target`; when every walk ended, states the
    /// catch-up.
    async fn walk(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        local: &DistributionMember,
        target: &DistributionMember,
        budget: &mut usize,
    ) -> Result<Outcome, String> {
        let ranges = snapshot.catch_up_ranges(local.member_id, target.member_id);
        if ranges.is_empty() {
            return Ok(Outcome::Settled);
        }
        let turns = (self.set_id, target.member_id);
        let first = turn(turns, None) % ranges.len();
        let mut watermark = u64::MAX;
        let mut done = true;
        let order = (first..ranges.len()).chain(0..first);
        for index in order {
            let range = ranges[index];
            let key = bootstrap_key(self.set_id, target.member_id, range.range_id);
            let own = (read_cursor(context, key).await?).filter(|c| c.intent == target.intent);
            let cursor = match own {
                Some(cursor) => Some(cursor),
                None => self.seed_child(context, snapshot, target, range).await?,
            };
            if let Some(cursor) = cursor
                && cursor.done
            {
                watermark = watermark.min(cursor.watermark);
                continue;
            }
            if *budget == 0 {
                // The next drain starts at this range; nothing is stated before all are done.
                turn(turns, Some(index));
                done = false;
                break;
            }
            *budget -= 1;
            let page = BootstrapPage::new(PageConfig {
                realm_id: self.realm_id,
                bucket: snapshot.config.bucket.clone(),
                set_id: self.set_id,
                member_id: target.member_id,
                node_id: target.node_id,
                intent: target.intent,
                range: range.clone(),
                now_ms: unix_timestamp_millis(),
                rows: PAGE_ROWS,
            });
            match drive(page, context)
                .await
                .map_err(|error| error.to_string())?
            {
                PageProgress::Done { watermark: ended } => watermark = watermark.min(ended),
                PageProgress::Waiting | PageProgress::Queued(_) => done = false,
            }
        }
        if !done {
            return Ok(Outcome::Waiting);
        }
        self.state(context, snapshot, local, target, watermark)
            .await
    }

    /// Hands the signed catch-up to the publisher; the next record makes the member ready.
    async fn state(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        local: &DistributionMember,
        target: &DistributionMember,
        watermark: u64,
    ) -> Result<Outcome, String> {
        let cover = snapshot.catch_up_cover(local.member_id, target.member_id);
        let statement =
            Statement::caught_up(self.realm_id, self.set_id, local, target, cover, watermark);
        self.hand(context, snapshot, statement, target.member_id)
            .await
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
        let shared = Arc::new(context.clone());
        match submit(&shared, publisher, self.local, signed).await {
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
