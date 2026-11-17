//! Runs the periodic source refresh of distributed buckets: a range's coordinator queues its
//! reference heads for observation, a bounded page per pass from a durable round cursor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DISTRIBUTION_KEYSPACE, NODE_STATE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, KeyRange, WriteRoute,
};
use aruna_core::types::{Effects, Key, Value};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::guard::{authority_reads, authority_rows, write_target};
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::metadata::{QueueRefreshOperation, ReferenceRefresh};

/// Heads one range's page reads.
const PAGE: usize = 256;
/// Range pages one pass reads at most.
const PAGES_PER_PASS: usize = 16;
/// Distribution records one pass looks at; the next pass continues after them.
const SETS_PER_PASS: usize = 64;
const ROUND_PREFIX: &[u8] = b"refresh_round/";

/// One range's refresh round on this coordinator.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RefreshRound {
    /// The last key queued in this round; `None` before its first page.
    pub after: Option<String>,
    pub started_ms: u64,
    /// The round reached the range's end; the next starts `refresh_secs` after this one.
    pub finished: bool,
}

impl RefreshRound {
    /// The round to walk now: this one while it runs, a new one once it is due again.
    fn due(self, now_ms: u64, refresh_secs: u64) -> Option<Self> {
        let next_ms = self
            .started_ms
            .saturating_add(refresh_secs.saturating_mul(1000));
        match (self.finished, now_ms >= next_ms) {
            (false, _) => Some(self),
            (true, true) => Some(Self {
                started_ms: now_ms,
                ..Self::default()
            }),
            (true, false) => None,
        }
    }
}

fn round_key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    [ROUND_PREFIX, &set_id.to_bytes(), &range_id.to_bytes()].concat()
}

/// Where the next pass goes on: the record after which it reads, and the set and lower key of
/// the range after which a set's sweep continues.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RefreshCursor {
    pub record: Option<Vec<u8>>,
    pub sweep: Option<(Ulid, String)>,
}

/// What one pass did: whether a round has more pages, and where the next pass goes on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RefreshPass {
    pub busy: bool,
    pub cursor: RefreshCursor,
}

/// A failed pass, with the record cursor the next pass resumes at.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("the periodic source refresh failed: {reason}")]
pub(crate) struct RefreshError {
    pub reason: String,
    pub record: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Init,
    Records,
    Bucket,
    Authority,
    Round,
    Heads,
    Version,
    Queue,
    WriteRound,
    Finish,
    Error,
}

/// The record whose ranges the pass walks.
#[derive(Debug, PartialEq)]
struct SetWalk {
    record_key: Vec<u8>,
    set_id: Ulid,
    bucket: String,
    refresh_secs: u64,
    binding: Option<DistributionBinding>,
    after: Option<String>,
}

/// The range page being walked.
#[derive(Debug, PartialEq)]
struct RangeWalk {
    range: KeyRange,
    key: Vec<u8>,
    walked: RefreshRound,
    heads: VecDeque<(Key, Value)>,
    head: Option<(String, Ulid)>,
}

/// One bounded pass over the distributed buckets with a refresh interval. `now` is the time
/// rounds start at and refreshes are stamped with.
#[derive(Debug, PartialEq)]
pub(crate) struct RefreshRangesOperation {
    realm_id: RealmId,
    node_id: NodeId,
    now: SystemTime,
    now_ms: u64,
    after: Option<Vec<u8>>,
    record: Option<Vec<u8>>,
    sweep: Option<(Ulid, String)>,
    state: State,
    records: VecDeque<(Key, Value)>,
    set: Option<SetWalk>,
    ranges: VecDeque<KeyRange>,
    last: Option<String>,
    walk: Option<RangeWalk>,
    bucket: Option<GetBucketOperation>,
    queue: Option<QueueRefreshOperation>,
    pages: usize,
    busy: bool,
    done: Option<Vec<u8>>,
    output: Option<Result<RefreshPass, RefreshError>>,
}

impl RefreshRangesOperation {
    pub(crate) fn new(
        realm_id: RealmId,
        node_id: NodeId,
        now: SystemTime,
        cursor: RefreshCursor,
    ) -> Self {
        let now_ms = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
            });
        Self {
            realm_id,
            node_id,
            now,
            now_ms,
            after: cursor.record.clone(),
            record: cursor.record.clone(),
            sweep: cursor.sweep,
            state: State::Init,
            records: VecDeque::new(),
            set: None,
            ranges: VecDeque::new(),
            last: None,
            walk: None,
            bucket: None,
            queue: None,
            pages: 0,
            busy: false,
            done: cursor.record,
            output: None,
        }
    }

    fn finish(&mut self, busy: bool, sweep: Option<(Ulid, String)>) -> Effects {
        let cursor = RefreshCursor {
            record: self.record.clone(),
            sweep,
        };
        self.state = State::Finish;
        self.output = Some(Ok(RefreshPass { busy, cursor }));
        smallvec![]
    }

    fn fail(&mut self, reason: impl ToString) -> Effects {
        self.state = State::Error;
        self.output = Some(Err(RefreshError {
            reason: reason.to_string(),
            record: self.record.clone(),
        }));
        smallvec![]
    }

    fn next_record(&mut self) -> Effects {
        while let Some((record_key, value)) = self.records.pop_front() {
            let Ok(view) = DistributionView::from_bytes(&value) else {
                continue;
            };
            let snapshot = view.snapshot();
            let Some(refresh_secs) = snapshot.config.settings.refresh_secs else {
                continue;
            };
            self.set = Some(SetWalk {
                record_key: record_key.to_vec(),
                set_id: snapshot.set_id(),
                bucket: snapshot.config.bucket.clone(),
                refresh_secs,
                binding: None,
                after: None,
            });
            let mut bucket = GetBucketOperation::new(snapshot.config.bucket.clone());
            let effects = bucket.start();
            self.state = State::Bucket;
            return self.bucket_step(bucket, effects);
        }
        let busy = self.busy;
        self.finish(busy, None)
    }

    /// Keeps a running bucket read; a finished one decides whether the set is walked here.
    fn bucket_step(&mut self, bucket: GetBucketOperation, effects: Effects) -> Effects {
        if !bucket.is_complete() {
            self.bucket = Some(bucket);
            return effects;
        }
        let Some(set) = self.set.as_mut() else {
            return self.fail("no set is walked");
        };
        let binding = (bucket.finalize().ok())
            .and_then(|info| info.distribution)
            .filter(|binding| binding.set_id == set.set_id);
        let Some(binding) = binding else {
            return self.next_record();
        };
        set.after = (self.sweep.take())
            .filter(|(set_id, _)| *set_id == binding.set_id)
            .map(|(_, lower)| lower);
        let reads = authority_reads(binding.set_id);
        set.binding = Some(binding);
        self.state = State::Authority;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    /// Walks the next range the set's sweep visits, while the pass has pages left.
    fn next_range(&mut self) -> Effects {
        let Some(set) = self.set.as_ref() else {
            return self.fail("no set is walked");
        };
        let Some(range) = self.ranges.pop_front() else {
            self.done = Some(set.record_key.clone());
            return self.next_record();
        };
        // A pass that runs out of pages comes back to this set, after its last range.
        if self.pages >= PAGES_PER_PASS {
            self.record = self.done.clone();
            let sweep = self.last.take().map(|lower| (set.set_id, lower));
            return self.finish(true, sweep);
        }
        self.last = Some(range.lower.clone());
        let key = round_key(set.set_id, range.range_id);
        self.walk = Some(RangeWalk {
            range,
            key: key.clone(),
            walked: RefreshRound::default(),
            heads: VecDeque::new(),
            head: None,
        });
        self.state = State::Round;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    /// Reads the next page of the range's heads for a due round.
    fn walk_page(&mut self, round: RefreshRound) -> Effects {
        let (Some(set), Some(walk)) = (self.set.as_ref(), self.walk.as_mut()) else {
            return self.fail("no range is walked");
        };
        let bucket = &set.bucket;
        let start = match &round.after {
            Some(after) => {
                (BlobHeadKey::new(bucket, after).to_bytes()).map(|key| IterStart::After(key.into()))
            }
            None => (BlobHeadKey::new(bucket, &walk.range.lower).to_bytes())
                .map(|key| IterStart::At(key.into())),
        };
        let page = start.and_then(|start| Ok((BlobHeadKey::bucket_prefix(bucket)?, start)));
        let (prefix, start) = match page {
            Ok(page) => page,
            Err(error) => return self.fail(error),
        };
        walk.walked = round;
        self.state = State::Heads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: Some(start),
            limit: PAGE,
            txn_id: None,
        })]
    }

    /// Reads the version of the next head of the page, or writes the round once the page ends.
    fn next_head(&mut self) -> Effects {
        let (Some(set), Some(walk)) = (self.set.as_ref(), self.walk.as_mut()) else {
            return self.fail("no range is walked");
        };
        let Some((key, value)) = walk.heads.pop_front() else {
            return self.write_round();
        };
        let found = BlobHeadKey::from_bytes(&key).and_then(|head| {
            let pointer = CurrentVersionPointer::from_bytes(&value)?;
            let row = VersionKey::new(&set.bucket, &head.key, pointer.version_id).to_bytes()?;
            Ok((head.key, pointer.version_id, row))
        });
        let (name, version_id, row) = match found {
            Ok(found) => found,
            Err(error) => return self.fail(error),
        };
        if !walk.range.contains(&name) {
            walk.walked.finished = true;
            return self.write_round();
        }
        walk.head = Some((name, version_id));
        self.state = State::Version;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: row.into(),
            txn_id: None,
        })]
    }

    /// Notes the head as walked and moves to the next one.
    fn walked_head(&mut self) -> Effects {
        if let Some(walk) = self.walk.as_mut()
            && let Some((name, _)) = walk.head.take()
        {
            walk.walked.after = Some(name);
        }
        self.next_head()
    }

    fn write_round(&mut self) -> Effects {
        let Some(walk) = self.walk.as_ref() else {
            return self.fail("no range is walked");
        };
        let value = match postcard::to_allocvec(&walk.walked) {
            Ok(value) => value,
            Err(error) => return self.fail(error),
        };
        self.state = State::WriteRound;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: walk.key.clone().into(),
            value: value.into(),
            txn_id: None,
        })]
    }

    /// Keeps a running refresh queue write; a finished one moves to the next head.
    fn queue_step(&mut self, queue: QueueRefreshOperation, effects: Effects) -> Effects {
        if !queue.is_complete() {
            self.queue = Some(queue);
            return effects;
        }
        match queue.finalize() {
            Ok(_) => self.walked_head(),
            Err(error) => self.fail(error),
        }
    }

    fn refresh(&self, version: &BlobVersion) -> Option<ReferenceRefresh> {
        let (set, walk) = (self.set.as_ref()?, self.walk.as_ref()?);
        let (key, version_id) = walk.head.clone()?;
        Some(ReferenceRefresh {
            bucket: set.bucket.clone(),
            key,
            version_id,
            metadata: None,
            refreshed_at: self.now,
            node_id: self.node_id,
            auth_context: AuthContext {
                user_id: version.created_by,
                realm_id: self.realm_id,
                path_restrictions: None,
                session: None,
            },
        })
    }
}

impl Operation for RefreshRangesOperation {
    type Output = RefreshPass;
    type Error = RefreshError;

    fn start(&mut self) -> Effects {
        self.state = State::Records;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: DISTRIBUTION_KEYSPACE.to_string(),
            prefix: None,
            start: self
                .after
                .clone()
                .map(|after| IterStart::After(after.into())),
            limit: SETS_PER_PASS,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (
                State::Records,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                self.record = next_start_after.map(|next| next.to_vec());
                self.records = values.into();
                self.next_record()
            }
            (State::Bucket, event) => match self.bucket.take() {
                Some(mut bucket) => {
                    let effects = bucket.step(event);
                    self.bucket_step(bucket, effects)
                }
                None => self.fail("no bucket is read"),
            },
            (State::Authority, event) => {
                let rows = authority_rows(event).map_err(|error| error.to_string());
                let Some(set) = self.set.as_ref() else {
                    return self.fail("no set is walked");
                };
                let ranges = rows.and_then(|(record, fence)| {
                    coordinated(set.binding.as_ref(), record.as_deref(), fence.as_deref())
                });
                match ranges {
                    Ok(ranges) => {
                        self.ranges = after_range(ranges, set.after.as_deref()).into();
                        self.last = None;
                        self.next_range()
                    }
                    Err(reason) => self.fail(reason),
                }
            }
            (State::Round, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let stored = (value.as_deref())
                    .map(postcard::from_bytes::<RefreshRound>)
                    .transpose();
                let round = match stored {
                    Ok(round) => round.unwrap_or(RefreshRound {
                        started_ms: self.now_ms,
                        ..RefreshRound::default()
                    }),
                    Err(error) => return self.fail(error),
                };
                let refresh_secs = self.set.as_ref().map_or(0, |set| set.refresh_secs);
                match round.due(self.now_ms, refresh_secs) {
                    Some(round) => self.walk_page(round),
                    None => self.next_range(),
                }
            }
            (
                State::Heads,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                let Some(walk) = self.walk.as_mut() else {
                    return self.fail("no range is walked");
                };
                walk.walked.finished = next_start_after.is_none();
                walk.heads = values.into();
                self.next_head()
            }
            (State::Version, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let version = match (value.as_deref()).map(BlobVersion::from_bytes).transpose() {
                    Ok(version) => version,
                    Err(error) => return self.fail(error),
                };
                // The worker observes the source; only a confirmed change mints a successor.
                let refresh = version
                    .filter(|version| matches!(version.state, BlobVersionState::Reference { .. }))
                    .and_then(|version| self.refresh(&version));
                let Some(refresh) = refresh else {
                    return self.walked_head();
                };
                let mut queue = QueueRefreshOperation::new(refresh);
                let effects = queue.start();
                self.state = State::Queue;
                self.queue_step(queue, effects)
            }
            (State::Queue, event) => match self.queue.take() {
                Some(mut queue) => {
                    let effects = queue.step(event);
                    self.queue_step(queue, effects)
                }
                None => self.fail("no refresh is queued"),
            },
            (State::WriteRound, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.pages += 1;
                self.busy |= self.walk.take().is_some_and(|walk| !walk.walked.finished);
                self.next_range()
            }
            (state, event) => self.fail(format!("unexpected event in state {state:?}: {event:?}")),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Finish | State::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RefreshError {
            reason: "the pass did not finish".to_string(),
            record: self.record,
        }))
    }

    fn abort(&mut self) -> Effects {
        if let Some(queue) = self.queue.as_mut() {
            return queue.abort();
        }
        match self.bucket.as_mut() {
            Some(bucket) => bucket.abort(),
            None => smallvec![],
        }
    }
}

/// The ranges a sweep still visits, in key order: those whose lower key follows `after`.
fn after_range(mut ranges: Vec<KeyRange>, after: Option<&str>) -> Vec<KeyRange> {
    ranges.retain(|range| after.is_none_or(|after| range.lower.as_str() > after));
    ranges.sort_by(|left, right| left.lower.cmp(&right.lower));
    ranges
}

/// The ranges this member coordinates now; a range it fenced for a transfer is left out.
fn coordinated(
    binding: Option<&DistributionBinding>,
    record: Option<&[u8]>,
    fence: Option<&[u8]>,
) -> Result<Vec<KeyRange>, String> {
    let Some(binding) = binding else {
        return Err("no binding is walked".to_string());
    };
    let Some(view) = (record.map(DistributionView::from_bytes).transpose())
        .map_err(|error| error.to_string())?
    else {
        return Ok(Vec::new());
    };
    let ranges = view.snapshot().config.ranges.iter().filter(|range| {
        let target = write_target(binding, &range.lower, record, fence);
        matches!(target, Ok(WriteRoute::Local { .. }))
    });
    Ok(ranges.cloned().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    // A pass that ran out of pages goes on after its last range and ends with the set's last.
    #[test]
    fn passes_resume_ranges() {
        let range = |seed: u8| KeyRange {
            range_id: Ulid::from_bytes([seed; 16]),
            lower: ["", "g", "p"][usize::from(seed - 1)].to_string(),
            upper: None,
            lineage: None,
            coordinator: Ulid::nil(),
            epoch: 1,
            replicas: Vec::new(),
        };
        let ids = |ranges: Vec<KeyRange>| -> Vec<u8> {
            ranges.iter().map(|r| r.range_id.to_bytes()[0]).collect()
        };
        let all = vec![range(3), range(1), range(2)];
        assert_eq!(ids(after_range(all.clone(), None)), [1, 2, 3]);
        assert_eq!(ids(after_range(all.clone(), Some("g"))), [3]);
        assert_eq!(ids(after_range(all.clone(), Some("p"))), [0u8; 0]);
        assert_eq!(
            ids(after_range(all, Some("h"))),
            [3],
            "a range split since goes on by key"
        );
    }

    // A running round goes on; a finished one waits for the interval, then starts over.
    #[test]
    fn rounds_wait_interval() {
        let running = RefreshRound {
            after: Some("a".to_string()),
            started_ms: 1_000,
            finished: false,
        };
        assert_eq!(running.clone().due(1_500, 60), Some(running));
        let finished = RefreshRound {
            after: Some("z".to_string()),
            started_ms: 1_000,
            finished: true,
        };
        assert_eq!(finished.clone().due(30_000, 60), None);
        let next = finished.due(61_000, 60).unwrap();
        assert_eq!(
            (next.after, next.started_ms, next.finished),
            (None, 61_000, false)
        );
    }
}
