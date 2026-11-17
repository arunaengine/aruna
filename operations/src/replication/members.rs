//! Publishes a distributed bucket's versions from the coordinator of their range to the members
//! that hold it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A queued job names only the member; what it publishes comes from the record this node
// holds when it sends, so an ended coordinatorship publishes nothing.

use aruna_core::structs::storage::replication_queue::MemberTarget;
use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    DISTRIBUTION_KEYSPACE, REPLICATION_OBLIGATION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, DistributionView, KeyRange, Publication,
};
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use aruna_core::structs::storage::replication_queue::ReplicationMode;
use aruna_core::structs::storage::replication_queue::{
    BlobJobRecord, LiveObligationRecord, queued_prefixes,
};
use aruna_core::structs::storage::replication_queue::{ReplicateScopeInput, ReplicateScopeTarget};

/// Queue rows one check reads per page, and pages it reads before it counts the queue as owing.
const FANOUT_PAGE: usize = 1024;
const FANOUT_PAGES: usize = 16;
/// Queue pages one copy lag count reads.
const LAG_PAGES: usize = 4;

/// What this member publishes of `key` to `target` now, and the target's node: `None` once
/// this member does not coordinate the key's range, or the target no longer holds it.
pub(crate) fn member_publication(
    binding: &DistributionBinding,
    snapshot: &DistributionSnapshot,
    key: &str,
    target: &MemberTarget,
) -> Option<(Publication, NodeId)> {
    let local = snapshot.member(binding.member_id)?;
    if binding.set_id != target.set_id || local.incarnation != Some(binding.incarnation) {
        return None;
    }
    let (range, deliveries) = snapshot.key_deliveries(binding.member_id, key)?;
    let delivery = deliveries
        .iter()
        .find(|delivery| delivery.member_id == target.member_id)?;
    Some((snapshot.publication(range, delivery), delivery.node_id))
}

/// What a coordinator still owes one member of one range.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CopyLag {
    /// Versions queued for the member.
    pub pending: u64,
    /// When the oldest of them was written, in Unix milliseconds.
    pub oldest_ms: Option<u64>,
}

/// The publications of one committed version to the members holding its key's range, while
/// this member coordinates it. Delete markers are always delivered inside a set.
pub(crate) fn member_jobs(
    binding: &DistributionBinding,
    snapshot: &DistributionSnapshot,
    obligation: &LiveObligationRecord,
    now_ms: u64,
) -> Vec<BlobJobRecord> {
    let admitted = snapshot
        .member(binding.member_id)
        .is_some_and(|member| member.incarnation == Some(binding.incarnation));
    let deliveries = match snapshot.key_deliveries(binding.member_id, &obligation.key) {
        Some((_, deliveries)) if admitted => deliveries,
        _ => return Vec::new(),
    };
    deliveries
        .into_iter()
        .map(|delivery| {
            let input = ReplicateScopeInput {
                bucket: obligation.bucket.clone(),
                target: ReplicateScopeTarget::Version {
                    key: obligation.key.clone(),
                    version_id: obligation.version_id,
                },
                target_node_id: delivery.node_id,
                auth_context: obligation.auth_context.clone(),
                replicate_delete_markers: true,
                mode: ReplicationMode::Live,
            };
            BlobJobRecord::new(input, Some(obligation.delete_marker), now_ms).with_member(
                MemberTarget {
                    set_id: binding.set_id,
                    member_id: delivery.member_id,
                },
            )
        })
        .collect()
}

#[derive(Debug, Error, PartialEq)]
pub enum RecordError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event while reading the set record: {0}")]
    UnexpectedEvent(String),
    #[error("the set record read did not finish")]
    NotFinished,
}

fn record_read(set_id: Ulid) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: DISTRIBUTION_KEYSPACE.to_string(),
        key: set_id.to_bytes().to_vec().into(),
        txn_id: None,
    })
}

fn decode_record(value: Option<Value>) -> Result<Option<DistributionView>, RecordError> {
    Ok(value
        .map(|value| DistributionView::from_bytes(value.as_ref()))
        .transpose()?)
}

/// This node's record of a set, if it holds one.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadRecordOperation {
    set_id: Ulid,
    output: Option<Result<Option<DistributionView>, RecordError>>,
}

impl ReadRecordOperation {
    pub(crate) fn new(set_id: Ulid) -> Self {
        Self {
            set_id,
            output: None,
        }
    }
}

impl Operation for ReadRecordOperation {
    type Output = Option<DistributionView>;
    type Error = RecordError;

    fn start(&mut self) -> Effects {
        smallvec![record_read(self.set_id)]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(match (&self.output, event) {
            (None, Event::Storage(StorageEvent::ReadResult { value, .. })) => decode_record(value),
            (None, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, event) => Err(RecordError::UnexpectedEvent(format!("{event:?}"))),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, RecordError> {
        self.output.unwrap_or(Err(RecordError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// A local bucket's binding and this node's record of its set.
pub(crate) type BoundRecord = (DistributionBinding, DistributionSnapshot);

#[derive(Debug, PartialEq)]
enum BoundState {
    Init,
    ReadBucket,
    ReadRecord(Box<DistributionBinding>),
    Finish,
    Error,
}

/// The binding of a local bucket and this node's record of its set; `None` for an ordinary
/// bucket, a missing bucket or a record that has not arrived.
#[derive(Debug, PartialEq)]
pub(crate) struct BoundRecordOperation {
    bucket: String,
    state: BoundState,
    output: Option<Result<Option<BoundRecord>, RecordError>>,
}

impl BoundRecordOperation {
    pub(crate) fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            state: BoundState::Init,
            output: None,
        }
    }

    fn done(&mut self, result: Result<Option<BoundRecord>, RecordError>) -> Effects {
        self.state = match result {
            Ok(_) => BoundState::Finish,
            Err(_) => BoundState::Error,
        };
        self.output = Some(result);
        smallvec![]
    }

    fn bucket_read(&mut self, value: Option<Value>) -> Effects {
        let info = match value
            .map(|value| BucketInfo::from_bytes(&value))
            .transpose()
        {
            Ok(info) => info,
            Err(error) => return self.done(Err(error.into())),
        };
        let Some(binding) = info.and_then(|info| info.distribution) else {
            return self.done(Ok(None));
        };
        let read = record_read(binding.set_id);
        self.state = BoundState::ReadRecord(Box::new(binding));
        smallvec![read]
    }
}

impl Operation for BoundRecordOperation {
    type Output = Option<BoundRecord>;
    type Error = RecordError;

    fn start(&mut self) -> Effects {
        self.state = BoundState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::replace(&mut self.state, BoundState::Error), event) {
            (
                BoundState::ReadBucket | BoundState::ReadRecord(_),
                Event::Storage(StorageEvent::Error { error }),
            ) => self.done(Err(error.into())),
            (BoundState::ReadBucket, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.bucket_read(value)
            }
            (
                BoundState::ReadRecord(binding),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => {
                let record = decode_record(value)
                    .map(|view| view.map(|view| (*binding, view.head.snapshot)));
                self.done(record)
            }
            (_, event) => self.done(Err(RecordError::UnexpectedEvent(format!("{event:?}")))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, BoundState::Finish | BoundState::Error)
    }

    fn finalize(self) -> Result<Self::Output, RecordError> {
        self.output.unwrap_or(Err(RecordError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Reads the member job queue and the live obligations of a bucket page by page, at most
/// `limit` pages of each.
#[derive(Debug, PartialEq)]
struct QueueWalk {
    queues: [(&'static str, Vec<u8>); 2],
    limit: usize,
    index: usize,
    pages: usize,
    start: Option<Key>,
}

/// What a queue walk does after one page.
enum WalkStep {
    Read(Effect),
    /// The current queue still has rows behind its last page, but its page budget is spent.
    Cut,
    /// Every queue was read to its end.
    Done,
}

impl QueueWalk {
    fn new(bucket: &str, limit: usize) -> Result<Self, ConversionError> {
        let [live, _, obligations] = queued_prefixes(bucket)?;
        Ok(Self {
            queues: [live, obligations],
            limit,
            index: 0,
            pages: 0,
            start: None,
        })
    }

    fn key_space(&self) -> &'static str {
        self.queues[self.index.min(1)].0
    }

    fn read(&self) -> Effect {
        let (key_space, prefix) = &self.queues[self.index.min(1)];
        Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(prefix.clone().into()),
            start: self.start.clone().map(IterStart::After),
            limit: FANOUT_PAGE,
            txn_id: None,
        })
    }

    /// Moves on after a page whose last row is `last`; `more` when rows remain behind it.
    fn advance(&mut self, last: Option<Key>, more: bool) -> WalkStep {
        self.pages += 1;
        match last {
            Some(last) if more && self.pages < self.limit => {
                self.start = Some(last);
                WalkStep::Read(self.read())
            }
            _ if more => WalkStep::Cut,
            _ => self.next_queue(),
        }
    }

    /// Starts the next queue, or reports that every queue was read.
    fn next_queue(&mut self) -> WalkStep {
        self.index += 1;
        self.pages = 0;
        self.start = None;
        match self.index < self.queues.len() {
            true => WalkStep::Read(self.read()),
            false => WalkStep::Done,
        }
    }
}

/// Whether this node still owes a publication of a key in `range` of the set: a queued member
/// job or a live obligation that has not become jobs yet. A queue longer than one check reads
/// counts as owing, so the caller looks again later.
#[derive(Debug, PartialEq)]
pub(crate) struct OwesFanoutOperation {
    set_id: Ulid,
    range: KeyRange,
    walk: Option<QueueWalk>,
    output: Option<Result<bool, RecordError>>,
}

impl OwesFanoutOperation {
    pub(crate) fn new(bucket: &str, set_id: Ulid, range: KeyRange) -> Self {
        let (walk, output) = match QueueWalk::new(bucket, FANOUT_PAGES) {
            Ok(walk) => (Some(walk), None),
            Err(error) => (None, Some(Err(error.into()))),
        };
        Self {
            set_id,
            range,
            walk,
            output,
        }
    }

    fn owed(&self, key_space: &str, value: &[u8]) -> bool {
        if key_space == REPLICATION_OBLIGATION_KEYSPACE {
            return LiveObligationRecord::from_bytes(value)
                .is_ok_and(|obligation| self.range.contains(&obligation.key));
        }
        BlobJobRecord::from_bytes(value).is_ok_and(|job| {
            let key = match &job.input.target {
                ReplicateScopeTarget::Version { key, .. } => Some(key),
                _ => None,
            };
            let member = (job.distribution.as_ref()).is_some_and(|t| t.set_id == self.set_id);
            member && key.is_some_and(|key| self.range.contains(key))
        })
    }

    fn page_read(&mut self, values: Vec<(Key, Value)>, more: bool) -> Effects {
        let Some(key_space) = self.walk.as_ref().map(QueueWalk::key_space) else {
            return self.finish(Err(RecordError::NotFinished));
        };
        if values.iter().any(|(_, value)| self.owed(key_space, value)) {
            return self.finish(Ok(true));
        }
        let last = values.last().map(|(key, _)| key.clone());
        match self.walk.as_mut().map(|walk| walk.advance(last, more)) {
            Some(WalkStep::Read(effect)) => smallvec![effect],
            // A queue longer than the check reads counts as owing.
            Some(WalkStep::Cut) => self.finish(Ok(true)),
            Some(WalkStep::Done) => self.finish(Ok(false)),
            None => self.finish(Err(RecordError::NotFinished)),
        }
    }

    fn finish(&mut self, result: Result<bool, RecordError>) -> Effects {
        self.output = Some(result);
        smallvec![]
    }
}

impl Operation for OwesFanoutOperation {
    type Output = bool;
    type Error = RecordError;

    fn start(&mut self) -> Effects {
        self.walk.iter().map(QueueWalk::read).collect()
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.output, event) {
            (
                None,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.page_read(values, next_start_after.is_some()),
            (None, Event::Storage(StorageEvent::Error { error })) => self.finish(Err(error.into())),
            (_, event) => self.finish(Err(RecordError::UnexpectedEvent(format!("{event:?}")))),
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<bool, RecordError> {
        self.output.unwrap_or(Err(RecordError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Copy lag per range this node coordinates and per receiving member.
pub type LagMap = BTreeMap<(Ulid, Ulid), CopyLag>;

/// The versions `coordinator` still owes, by range it coordinates and receiving member, from at
/// most `LAG_PAGES` pages of each queue; false when a queue held more, so counts are lower bounds.
#[derive(Debug, PartialEq)]
pub struct CopyLagOperation {
    snapshot: DistributionSnapshot,
    coordinator: Ulid,
    walk: Option<QueueWalk>,
    lag: LagMap,
    counted: bool,
    output: Option<Result<(LagMap, bool), RecordError>>,
}

impl CopyLagOperation {
    pub fn new(snapshot: DistributionSnapshot, coordinator: Ulid) -> Self {
        let (walk, output) = match QueueWalk::new(&snapshot.config.bucket, LAG_PAGES) {
            Ok(walk) => (Some(walk), None),
            Err(error) => (None, Some(Err(error.into()))),
        };
        Self {
            snapshot,
            coordinator,
            walk,
            lag: LagMap::new(),
            counted: true,
            output,
        }
    }

    fn add(&mut self, range_id: Ulid, member_id: Ulid, written_ms: u64) {
        let entry = self.lag.entry((range_id, member_id)).or_default();
        entry.pending += 1;
        entry.oldest_ms = Some(entry.oldest_ms.map_or(written_ms, |ms| ms.min(written_ms)));
    }

    fn count(&mut self, key_space: &str, value: &[u8]) {
        if key_space == REPLICATION_OBLIGATION_KEYSPACE {
            let Ok(owed) = LiveObligationRecord::from_bytes(value) else {
                return;
            };
            // An obligation whose member jobs are queued is counted through them.
            if owed.continuation.as_ref().is_some_and(|c| c.members_done) {
                return;
            }
            let Some((range, deliveries)) =
                self.snapshot.key_deliveries(self.coordinator, &owed.key)
            else {
                return;
            };
            let (range_id, written_ms) = (range.range_id, owed.version_id.timestamp_ms());
            for delivery in deliveries {
                self.add(range_id, delivery.member_id, written_ms);
            }
            return;
        }
        let Ok(job) = BlobJobRecord::from_bytes(value) else {
            return;
        };
        let (Some(target), ReplicateScopeTarget::Version { key, .. }) =
            (&job.distribution, &job.input.target)
        else {
            return;
        };
        let set_id = self.snapshot.set_id();
        let range = (self.snapshot.range_for(key))
            .filter(|range| range.coordinator == self.coordinator)
            .filter(|_| target.set_id == set_id)
            .map(|range| range.range_id);
        if let Some(range_id) = range {
            self.add(range_id, target.member_id, job.owed_since_ms());
        }
    }

    fn page_read(&mut self, values: Vec<(Key, Value)>, more: bool) -> Effects {
        let Some(key_space) = self.walk.as_ref().map(QueueWalk::key_space) else {
            return self.finish(Err(RecordError::NotFinished));
        };
        for (_, value) in &values {
            self.count(key_space, value);
        }
        let last = values.last().map(|(key, _)| key.clone());
        let Some(walk) = self.walk.as_mut() else {
            return self.finish(Err(RecordError::NotFinished));
        };
        let mut step = walk.advance(last, more);
        if let WalkStep::Cut = step {
            // The counts become lower bounds; the next queue is still read.
            self.counted = false;
            step = walk.next_queue();
        }
        match step {
            WalkStep::Read(effect) => smallvec![effect],
            WalkStep::Cut | WalkStep::Done => {
                let lag = std::mem::take(&mut self.lag);
                let counted = self.counted;
                self.finish(Ok((lag, counted)))
            }
        }
    }

    fn finish(&mut self, result: Result<(LagMap, bool), RecordError>) -> Effects {
        self.output = Some(result);
        smallvec![]
    }
}

impl Operation for CopyLagOperation {
    type Output = (LagMap, bool);
    type Error = RecordError;

    fn start(&mut self) -> Effects {
        self.walk.iter().map(QueueWalk::read).collect()
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.output, event) {
            (
                None,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.page_read(values, next_start_after.is_some()),
            (None, Event::Storage(StorageEvent::Error { error })) => self.finish(Err(error.into())),
            (_, event) => self.finish(Err(RecordError::UnexpectedEvent(format!("{event:?}")))),
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<(LagMap, bool), RecordError> {
        self.output.unwrap_or(Err(RecordError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_record::{binding, member, replicated};
    use crate::driver::{DriverContext, drive};

    #[test]
    fn publishes_while_coordinating() {
        let (snapshot, _) = replicated(true, 1);
        let (coordinator, replica) = (binding(&snapshot, 1, 5), binding(&snapshot, 2, 5));
        let target = MemberTarget {
            set_id: snapshot.set_id(),
            member_id: member(2),
        };
        let (publication, node_id) =
            member_publication(&coordinator, &snapshot, "a", &target).unwrap();
        assert!(publication.bytes, "a copy replica keeps the bytes");
        assert_eq!(node_id, snapshot.member(member(2)).unwrap().node_id);
        assert!(
            member_publication(&replica, &snapshot, "a", &target).is_none(),
            "a replica publishes nothing"
        );
        assert!(member_publication(&coordinator, &snapshot, "x", &target).is_none());
        let mut replaced = coordinator.clone();
        replaced.incarnation += 1;
        assert!(member_publication(&replaced, &snapshot, "a", &target).is_none());
    }

    #[tokio::test]
    async fn owed_fanout_waits() {
        // A queued member job or obligation of a key in the range keeps it owing.
        use crate::distribution::test_store::{open, range};
        use aruna_core::structs::identity::auth::AuthContext;
        use aruna_core::types::{Effects, Key, Value};
        use smallvec::smallvec;
        use thiserror::Error;
        let (_dir, storage_handle) = open();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let (snapshot, _) = replicated(true, 1);
        let set_id = snapshot.set_id();
        let (lower, upper) = (range(1, "", Some("m")), range(2, "m", None));
        let owes = |range: KeyRange| {
            let context = &context;
            async move {
                let owes = OwesFanoutOperation::new("spire", set_id, range);
                drive(owes, context).await.unwrap()
            }
        };
        assert!(!owes(lower.clone()).await);
        let realm_id = snapshot.statements[0].statement.realm_id;
        let obligation = LiveObligationRecord::new(
            snapshot.member(member(1)).unwrap().node_id,
            AuthContext::anonymous(realm_id),
            "spire".to_string(),
            "a".to_string(),
            Ulid::from_bytes([3; 16]),
            false,
        );
        let write = |(key_space, key, value): (String, Key, Value)| {
            let effect = StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            };
            let context = &context;
            async move { context.storage_handle.send_storage_effect(effect).await }
        };
        let entry =
            aruna_core::structs::storage::replication_queue::live_obligation_entry(&obligation)
                .unwrap();
        write(entry.clone()).await;
        assert!(owes(lower.clone()).await, "an obligation is still owed");
        assert!(!owes(upper.clone()).await, "another range owes nothing");
        let delete = StorageEffect::Delete {
            key_space: entry.0,
            key: entry.1,
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(delete).await;
        let jobs = member_jobs(&binding(&snapshot, 1, 5), &snapshot, &obligation, 9);
        let job = &jobs[0];
        let key = aruna_core::structs::storage::replication_queue::blob_job_key(job).unwrap();
        let value = job.to_bytes().unwrap().into();
        let job_space = aruna_core::keyspaces::REPLICATION_JOB_KEYSPACE.to_string();
        write((job_space, key, value)).await;
        assert!(owes(lower).await, "a queued member job is still owed");
        assert!(!owes(upper).await);
    }

    #[tokio::test]
    async fn counts_copy_lag() {
        // Obligations count for every member until their jobs are queued, then the jobs count.
        use crate::distribution::test_store::open;
        use aruna_core::keyspaces::REPLICATION_JOB_KEYSPACE;
        use aruna_core::structs::identity::auth::AuthContext;
        use aruna_core::structs::storage::replication_queue::{
            LiveReplicationContinuation, blob_job_key, live_obligation_entry,
        };
        let (_dir, storage_handle) = open();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let (snapshot, _) = replicated(true, 1);
        let realm_id = snapshot.statements[0].statement.realm_id;
        let owed = |key: &str, written_ms: u64| {
            LiveObligationRecord::new(
                snapshot.member(member(1)).unwrap().node_id,
                AuthContext::anonymous(realm_id),
                "spire".to_string(),
                key.to_string(),
                Ulid::from_parts(written_ms, 1),
                false,
            )
        };
        let mut rows = vec![live_obligation_entry(&owed("a", 2_000)).unwrap()];
        let mut queued = owed("b", 1_000);
        for job in member_jobs(&binding(&snapshot, 1, 5), &snapshot, &queued, 9) {
            let key = blob_job_key(&job).unwrap();
            rows.push((
                REPLICATION_JOB_KEYSPACE.to_string(),
                key,
                job.to_bytes().unwrap().into(),
            ));
        }
        queued.continuation = Some(LiveReplicationContinuation {
            members_done: true,
            ..LiveReplicationContinuation::default()
        });
        rows.push(live_obligation_entry(&queued).unwrap());
        let write = StorageEffect::BatchWrite {
            writes: rows,
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(write).await;

        let lower = snapshot.config.ranges[0].range_id;
        let (lag, counted) = drive(CopyLagOperation::new(snapshot.clone(), member(1)), &context)
            .await
            .unwrap();
        assert!(counted);
        let expected = CopyLag {
            pending: 2,
            oldest_ms: Some(1_000),
        };
        assert_eq!(lag, BTreeMap::from([((lower, member(2)), expected)]));
        let (lag, _) = drive(CopyLagOperation::new(snapshot.clone(), member(2)), &context)
            .await
            .unwrap();
        assert!(
            lag.is_empty(),
            "the other coordinator owes nothing of the lower range"
        );
    }

    #[test]
    fn fans_out_markers() {
        use aruna_core::structs::identity::auth::AuthContext;
        let (snapshot, _) = replicated(true, 1);
        let realm_id = snapshot.statements[0].statement.realm_id;
        let obligation = LiveObligationRecord::new(
            snapshot.member(member(1)).unwrap().node_id,
            AuthContext::anonymous(realm_id),
            "spire".to_string(),
            "a".to_string(),
            Ulid::from_bytes([3; 16]),
            true,
        );
        let jobs = member_jobs(&binding(&snapshot, 1, 5), &snapshot, &obligation, 9);
        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.source_delete_marker, Some(true));
        assert!(
            job.input.replicate_delete_markers,
            "markers ignore replicate_deletes"
        );
        assert_eq!(
            job.distribution,
            Some(MemberTarget {
                set_id: snapshot.set_id(),
                member_id: member(2),
            })
        );
        assert!(member_jobs(&binding(&snapshot, 2, 5), &snapshot, &obligation, 9).is_empty());
    }

    mod transitions {
        use super::*;
        use aruna_core::operation_check::{
            check_abort, rejects_events, rejects_finalize, sample_events,
        };

        fn page(rows: usize, more: bool) -> Event {
            Event::Storage(StorageEvent::IterResult {
                values: (0..rows)
                    .map(|index| (vec![index as u8].into(), Vec::new().into()))
                    .collect(),
                next_start_after: more.then(|| vec![0xff].into()),
            })
        }

        fn timeout() -> Event {
            Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })
        }

        #[test]
        fn record_reads_contract() {
            let set_id = Ulid::from_parts(1, 1);
            let mut missing = ReadRecordOperation::new(set_id);
            assert_eq!(missing.start().as_slice(), [record_read(set_id)]);
            missing.step(Event::Storage(StorageEvent::ReadResult {
                key: Vec::new().into(),
                value: None,
            }));
            assert_eq!(missing.finalize(), Ok(None));
            // A plain or missing bucket has no record; a storage error fails.
            let mut plain = BoundRecordOperation::new("plain");
            plain.start();
            assert!(
                plain
                    .step(Event::Storage(StorageEvent::ReadResult {
                        key: Vec::new().into(),
                        value: None,
                    }))
                    .is_empty()
            );
            assert_eq!(plain.finalize(), Ok(None));
            let mut failed = BoundRecordOperation::new("b");
            failed.start();
            failed.step(timeout());
            assert_eq!(
                failed.finalize(),
                Err(RecordError::Storage(StorageError::Timeout))
            );
            let reading = || {
                let mut operation = BoundRecordOperation::new("b");
                operation.start();
                operation
            };
            let refused = rejects_events(reading, sample_events, |event| {
                matches!(event, Event::Storage(StorageEvent::ReadResult { .. }))
            });
            assert_eq!(refused.len(), sample_events().len() - 1);
            let mut open = reading();
            check_abort(&mut open, &[]);
            rejects_finalize(open);
        }

        #[test]
        fn fanout_budget_owes() {
            // Empty queues owe nothing; a queue longer than the page budget counts as owing.
            let range = crate::distribution::test_store::range(1, "", None);
            let set_id = Ulid::from_parts(1, 1);
            let mut empty = OwesFanoutOperation::new("spire", set_id, range.clone());
            assert_eq!(empty.start().len(), 1);
            assert_eq!(
                empty.step(page(0, false)).len(),
                1,
                "the obligations follow"
            );
            assert!(empty.step(page(0, false)).is_empty());
            assert_eq!(empty.finalize(), Ok(false));
            let mut long = OwesFanoutOperation::new("spire", set_id, range);
            long.start();
            for _ in 1..FANOUT_PAGES {
                assert_eq!(long.step(page(1, true)).len(), 1);
            }
            assert!(long.step(page(1, true)).is_empty());
            assert_eq!(long.finalize(), Ok(true));
        }

        #[test]
        fn lag_cut_continues() {
            // A cut queue makes the counts lower bounds, and the next queue is still read.
            let (snapshot, _) = replicated(true, 1);
            let mut operation = CopyLagOperation::new(snapshot, member(1));
            operation.start();
            for _ in 1..LAG_PAGES {
                operation.step(page(1, true));
            }
            assert_eq!(operation.step(page(1, true)).len(), 1);
            assert!(operation.step(page(0, false)).is_empty());
            let (lag, counted) = operation.finalize().unwrap();
            assert!(lag.is_empty() && !counted);
            let mut failed = CopyLagOperation::new(replicated(true, 1).0, member(1));
            failed.start();
            failed.step(timeout());
            assert!(failed.finalize().is_err());
        }
    }
}
