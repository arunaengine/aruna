//! Runs the periodic source refresh of distributed buckets: a range's coordinator queues its
//! reference heads for observation, a bounded page per pass from a durable round cursor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DISTRIBUTION_KEYSPACE, NODE_STATE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{done, read_row, values};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, KeyRange, WriteRoute,
};
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use super::guard::{authority_reads, write_target};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::metadata::{
    QueueRefreshOperation, QueueRefreshResult, ReferenceRefresh, ReferenceRefreshError,
};

/// Heads one range's page reads.
const PAGE: usize = 256;
/// Range pages one pass reads at most.
const PAGES_PER_PASS: usize = 16;
/// Distribution records one pass looks at; the next pass continues after them.
const SETS_PER_PASS: usize = 64;
const ROUND_PREFIX: &[u8] = b"refresh_round/";

/// One range's refresh round on this coordinator.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct RefreshRound {
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

/// A failure of the runner itself carries no cursor; the caller resumes where the pass began.
impl From<IoError> for RefreshError {
    fn from(error: IoError) -> Self {
        Self {
            reason: error.to_string(),
            record: None,
        }
    }
}

type Step = Result<Next<RefreshPass>, String>;

/// What the pending call of a pass reads or writes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Records,
    Bucket,
    Authority,
    Round,
    Heads,
    Version,
    Queue,
    WriteRound,
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
    pages: usize,
    busy: bool,
    done: Option<Vec<u8>>,
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
            state: State::Records,
            records: VecDeque::new(),
            set: None,
            ranges: VecDeque::new(),
            last: None,
            walk: None,
            pages: 0,
            busy: false,
            done: cursor.record,
        }
    }

    fn finish(&mut self, busy: bool, sweep: Option<(Ulid, String)>) -> Step {
        let cursor = RefreshCursor {
            record: self.record.clone(),
            sweep,
        };
        Ok(Next::Done(RefreshPass { busy, cursor }))
    }

    fn call(&mut self, state: State, call: IoCall) -> Step {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn next_record(&mut self) -> Step {
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
            let bucket = GetBucketOperation::new(snapshot.config.bucket.clone());
            return self.call(State::Bucket, IoCall::child(bucket));
        }
        let busy = self.busy;
        self.finish(busy, None)
    }

    /// A read bucket decides whether the set is walked here; an unreadable one is skipped.
    fn bucket_read(&mut self, bucket: Result<BucketInfo, GetBucketError>) -> Step {
        let Some(set) = self.set.as_mut() else {
            return Err("no set is walked".to_string());
        };
        let binding = (bucket.ok())
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
        self.call(State::Authority, IoCall::Read(reads))
    }

    /// Walks the next range the set's sweep visits, while the pass has pages left.
    fn next_range(&mut self) -> Step {
        let Some(set) = self.set.as_ref() else {
            return Err("no set is walked".to_string());
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
        self.call(State::Round, read_row(NODE_STATE_KEYSPACE, key))
    }

    /// Reads the next page of the range's heads for a due round.
    fn walk_page(&mut self, round: RefreshRound) -> Step {
        let (Some(set), Some(walk)) = (self.set.as_ref(), self.walk.as_mut()) else {
            return Err("no range is walked".to_string());
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
        let (prefix, start) = page.map_err(|error| error.to_string())?;
        walk.walked = round;
        let iter = StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: Some(start),
            limit: PAGE,
            txn_id: None,
        };
        self.call(
            State::Heads,
            IoCall::Effect(Box::new(Effect::Storage(iter))),
        )
    }

    /// Reads the version of the next head of the page, or writes the round once the page ends.
    fn next_head(&mut self) -> Step {
        let (Some(set), Some(walk)) = (self.set.as_ref(), self.walk.as_mut()) else {
            return Err("no range is walked".to_string());
        };
        let Some((key, value)) = walk.heads.pop_front() else {
            return self.write_round();
        };
        let found = BlobHeadKey::from_bytes(&key).and_then(|head| {
            let pointer = CurrentVersionPointer::from_bytes(&value)?;
            let row = VersionKey::new(&set.bucket, &head.key, pointer.version_id).to_bytes()?;
            Ok((head.key, pointer.version_id, row))
        });
        let (name, version_id, row) = found.map_err(|error| error.to_string())?;
        if !walk.range.contains(&name) {
            walk.walked.finished = true;
            return self.write_round();
        }
        walk.head = Some((name, version_id));
        self.call(State::Version, read_row(BLOB_VERSIONS_KEYSPACE, row))
    }

    /// Notes the head as walked and moves to the next one.
    fn walked_head(&mut self) -> Step {
        if let Some(walk) = self.walk.as_mut()
            && let Some((name, _)) = walk.head.take()
        {
            walk.walked.after = Some(name);
        }
        self.next_head()
    }

    fn write_round(&mut self) -> Step {
        let Some(walk) = self.walk.as_ref() else {
            return Err("no range is walked".to_string());
        };
        let value = postcard::to_allocvec(&walk.walked).map_err(|error| error.to_string())?;
        let row = (
            NODE_STATE_KEYSPACE.to_string(),
            walk.key.clone().into(),
            value.into(),
        );
        self.call(State::WriteRound, IoCall::Write(vec![row]))
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

impl RefreshRangesOperation {
    fn advance(&mut self, result: IoResult) -> Result<Next<RefreshPass>, String> {
        let text = |error: IoError| error.to_string();
        match self.state {
            State::Records => {
                let (values, next) = result.page().map_err(text)?;
                self.record = next.map(|next| next.to_vec());
                self.records = values.into();
                self.next_record()
            }
            State::Bucket => {
                let bucket = result.child::<BucketInfo, GetBucketError>().map_err(text)?;
                self.bucket_read(bucket)
            }
            State::Authority => {
                let [record, fence] = values::<2>(result).map_err(text)?;
                let Some(set) = self.set.as_ref() else {
                    return Err("no set is walked".to_string());
                };
                let ranges =
                    coordinated(set.binding.as_ref(), record.as_deref(), fence.as_deref())?;
                self.ranges = after_range(ranges, set.after.as_deref()).into();
                self.last = None;
                self.next_range()
            }
            State::Round => {
                let [value] = values::<1>(result).map_err(text)?;
                let stored = (value.as_deref()).map(postcard::from_bytes::<RefreshRound>);
                let round = stored.transpose().map_err(|error| error.to_string())?;
                let round = round.unwrap_or(RefreshRound {
                    started_ms: self.now_ms,
                    ..RefreshRound::default()
                });
                let refresh_secs = self.set.as_ref().map_or(0, |set| set.refresh_secs);
                match round.due(self.now_ms, refresh_secs) {
                    Some(round) => self.walk_page(round),
                    None => self.next_range(),
                }
            }
            State::Heads => {
                let event = result.event().map_err(text)?;
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return Err(format!("unexpected head page answer {}", event.label()));
                };
                let Some(walk) = self.walk.as_mut() else {
                    return Err("no range is walked".to_string());
                };
                walk.walked.finished = next_start_after.is_none();
                walk.heads = values.into();
                self.next_head()
            }
            State::Version => {
                let [value] = values::<1>(result).map_err(text)?;
                let version = (value.as_deref()).map(BlobVersion::from_bytes).transpose();
                let version = version.map_err(|error| error.to_string())?;
                // The worker observes the source; only a confirmed change mints a successor.
                let refresh = version
                    .filter(|version| matches!(version.state, BlobVersionState::Reference { .. }))
                    .and_then(|version| self.refresh(&version));
                match refresh {
                    Some(refresh) => {
                        let queue = QueueRefreshOperation::new(refresh);
                        self.call(State::Queue, IoCall::child(queue))
                    }
                    None => self.walked_head(),
                }
            }
            State::Queue => {
                let queued = result.child::<QueueRefreshResult, ReferenceRefreshError>();
                queued.map_err(text)?.map_err(|error| error.to_string())?;
                self.walked_head()
            }
            State::WriteRound => {
                done(result).map_err(text)?;
                self.pages += 1;
                self.busy |= self.walk.take().is_some_and(|walk| !walk.walked.finished);
                self.next_range()
            }
        }
    }
}

impl Operation for RefreshRangesOperation {
    type Output = RefreshPass;
    type Error = RefreshError;

    fn first(&mut self) -> Result<Next<RefreshPass>, RefreshError> {
        Ok(Next::Call(IoCall::Page {
            space: DISTRIBUTION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.clone().map(Key::from),
            limit: SETS_PER_PASS,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RefreshPass>, RefreshError> {
        self.advance(result).map_err(|reason| RefreshError {
            reason,
            record: self.record.clone(),
        })
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
    use crate::distribution::test_record::with_settings;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::distribution::DistributionSettings;

    fn pass(cursor: RefreshCursor) -> RefreshRangesOperation {
        let realm_id = RealmId::from_bytes([1; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(100);
        RefreshRangesOperation::new(realm_id, node_id, now, cursor)
    }

    fn records(values: Vec<(Vec<u8>, Vec<u8>)>, next: Option<Vec<u8>>) -> IoResult {
        IoResult::Page {
            rows: (values.into_iter())
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            next: next.map(Key::from),
        }
    }

    // Records without a refresh interval are skipped; the record cursor moves past the page.
    #[test]
    fn skips_unrefreshed_sets() {
        let (_, plain) = with_settings(true, 0, DistributionSettings::default());
        let mut operation = pass(RefreshCursor {
            record: Some(vec![1]),
            sweep: None,
        });
        let page = IoCall::Page {
            space: DISTRIBUTION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: Some(Key::from(vec![1])),
            limit: SETS_PER_PASS,
        };
        assert_eq!(operation.first(), Ok(Next::Call(page)));
        let read = records(vec![(vec![2], plain), (vec![3], vec![0xFF])], Some(vec![3]));
        let cursor = RefreshCursor {
            record: Some(vec![3]),
            sweep: None,
        };
        let pass = RefreshPass {
            busy: false,
            cursor,
        };
        assert_eq!(operation.next(read), Ok(Next::Done(pass)));
    }

    // A set whose bucket is not readable here is skipped, and the pass goes on.
    #[test]
    fn unreadable_bucket_skipped() {
        let settings = DistributionSettings {
            refresh_secs: Some(60),
            ..DistributionSettings::default()
        };
        let (_, refreshed) = with_settings(true, 0, settings);
        let mut operation = pass(RefreshCursor::default());
        operation.first().unwrap();
        let bucket = operation.next(records(vec![(vec![2], refreshed)], None));
        assert!(matches!(bucket, Ok(Next::Call(IoCall::Child(_)))));
        let failed: Result<BucketInfo, GetBucketError> = Err(GetBucketError::NotFound);
        let Ok(Next::Done(pass)) = operation.next(IoResult::Child(ChildOutcome::new(failed)))
        else {
            panic!("expected the pass to end");
        };
        assert_eq!((pass.busy, pass.cursor), (false, RefreshCursor::default()));
    }

    // A failure inside the pass keeps the record cursor it reached; a runner failure has none.
    #[test]
    fn failure_keeps_cursor() {
        let mut operation = pass(RefreshCursor {
            record: Some(vec![1]),
            sweep: Some((Ulid::nil(), "a".to_string())),
        });
        operation.first().unwrap();
        let error = operation.next(IoResult::Done).unwrap_err();
        assert_eq!(error.record, Some(vec![1]));
        assert_eq!(RefreshError::from(IoError::Aborted).record, None);
    }
}
