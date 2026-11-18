//! Searches a distributed bucket only through the key indexes of the ranges this member
//! coordinates, with a bounded number of examined candidates and concurrent ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::LazyLock;
use std::time::{Duration, UNIX_EPOCH};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::key_index::{IndexEffect, IndexEvent, KeyPage, KeyQuery, MetaTerms};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, SEARCH_INDEX_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::table::{query_terms, search_tokens};
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer};
use aruna_core::structs::storage::distribution::inventory::Position;
use aruna_core::structs::storage::distribution::search::IndexState;
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionView, KeyRange};
use aruna_core::types::Key;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use ulid::Ulid;

use super::{
    BuildTurn, Candidate, Checked, HeadCandidate, HitBuilder, HitWalk, ObjectKeyMatch,
    SearchNodeHit, SearchNodePage, SearchObjectsError, SearchObjectsInput, SearchTurn, WalkTurn,
};
use crate::distribution::feed_walks::{
    FeedError, IndexProgress, RangeProgressOperation, state_key,
};
use crate::replication::members::{ReadRecordOperation, RecordError};
use std::collections::BTreeMap;

/// Index candidates one node page examines at most, visible or not.
pub(super) const EXAMINE_BUDGET: usize = 1_000;
/// Range indexes one page queries at once.
const RANGE_CONCURRENCY: usize = 4;

/// A range's page of a search, and the key a building index covers through.
type RangeFound = (KeyPage, Option<String>);

#[derive(Debug, PartialEq)]
enum RangeStep {
    Record,
    States(Vec<Ulid>, KeyQuery),
    Search(Vec<(Ulid, Option<Option<String>>)>),
    Through(Ulid, String),
    ThroughCheck(Ulid, String, VecDeque<Candidate>, Option<Candidate>),
    Keys,
    Walk,
    Progress,
}

/// One page of a distributed bucket. Ranges are read in key order and continue by key; a
/// missing or building index is reported as incomplete, with its progress, and never replaced
/// by a scan. A building index answers for the keys up to its cursor and is reported partial.
#[derive(Debug, PartialEq)]
pub(super) struct RangeSearchOperation {
    input: SearchObjectsInput,
    binding: DistributionBinding,
    budget: usize,
    indexed: bool,
    bucket: String,
    window: Option<(Bound<String>, Bound<String>)>,
    layout: Option<[u8; 32]>,
    chunks: VecDeque<Vec<Ulid>>,
    found: VecDeque<(Ulid, Option<RangeFound>)>,
    current: Option<KeyPage>,
    walk: HitWalk,
    unready: Vec<(Ulid, Option<Option<String>>)>,
    certain: bool,
    examined: usize,
    incomplete: usize,
    remote: usize,
    done_page: Option<SearchNodePage>,
    now_ms: u64,
    step: RangeStep,
}

type RangeTurn = SearchTurn<SearchNodePage>;

impl RangeSearchOperation {
    pub(super) fn new(
        input: SearchObjectsInput,
        binding: DistributionBinding,
        budget: usize,
        indexed: bool,
    ) -> Self {
        Self {
            bucket: input.bucket.clone().unwrap_or_default(),
            input,
            binding,
            budget: budget.max(1),
            indexed,
            window: None,
            layout: None,
            chunks: VecDeque::new(),
            found: VecDeque::new(),
            current: None,
            walk: HitWalk::default(),
            unready: Vec::new(),
            certain: true,
            examined: 0,
            incomplete: 0,
            remote: 0,
            done_page: None,
            now_ms: 0,
            step: RangeStep::Record,
        }
    }

    fn record_read(&mut self, view: DistributionView) -> RangeTurn {
        let ranges = &view.head.snapshot.config.ranges;
        self.layout = Some(layout_digest(ranges));
        let after = match self.input.start_after.as_deref() {
            Some(position) => Some(BlobHeadKey::from_bytes(&open_position(position)?)?.key),
            None => None,
        };
        let (start, end) = window(&self.input, after);
        let mut local = Vec::new();
        for range in ranges {
            if !overlaps(range, &start, &end) {
                continue;
            }
            match range.coordinator == self.binding.member_id {
                true => local.push(range.range_id),
                false => self.remote += 1,
            }
        }
        self.chunks = local
            .chunks(RANGE_CONCURRENCY)
            .map(<[Ulid]>::to_vec)
            .collect();
        self.window = Some((start, end));
        self.next_chunk()
    }

    fn query(&self) -> Result<KeyQuery, SearchObjectsError> {
        let input = &self.input;
        let (start, end) = self.window.clone().ok_or(SearchObjectsError::Unavailable)?;
        let names = (input.key_match == ObjectKeyMatch::Substring).then(|| input.query.clone());
        let meta = (input.key_match == ObjectKeyMatch::Metadata).then(|| {
            let (text, exact) = query_terms(&input.query);
            MetaTerms {
                text: search_tokens(&text).into_iter().collect(),
                exact: exact.into_iter().collect(),
            }
        });
        Ok(KeyQuery {
            start,
            end,
            names,
            meta,
            limit: self.budget.saturating_sub(self.examined).max(1),
        })
    }

    fn next_chunk(&mut self) -> RangeTurn {
        let Some(chunk) = self.chunks.pop_front() else {
            return self.finish(None);
        };
        let set_id = self.binding.set_id;
        let reads = (chunk.iter())
            .map(|range_id| {
                let key = Key::from(state_key(set_id, *range_id));
                (SEARCH_INDEX_KEYSPACE.to_string(), key)
            })
            .collect();
        self.step = RangeStep::States(chunk, self.query()?);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Queries the searchable indexes of the chunk at once. A building index is searched up to
    /// its cursor; a range without a searchable index is passed on as not found.
    fn search_chunk(
        &mut self,
        chunk: Vec<Ulid>,
        query: KeyQuery,
        states: Vec<Option<aruna_core::types::Value>>,
    ) -> RangeTurn {
        let set_id = self.binding.set_id;
        let (mut members, mut slots) = (Vec::new(), Vec::new());
        for (range_id, state) in chunk.into_iter().zip(states) {
            let state = state.and_then(|value| IndexState::from_bytes(value.as_ref()).ok());
            let Some((generation, through, query)) = searchable(state, &query) else {
                slots.push((range_id, None));
                continue;
            };
            if !self.indexed {
                slots.push((range_id, None));
                continue;
            }
            let search = IndexEffect::Search {
                set_id,
                range_id,
                generation,
                query,
            };
            members.push(Effect::Index(Box::new(search)));
            slots.push((range_id, Some(through)));
        }
        if members.is_empty() {
            self.found = slots.into_iter().map(|(id, _)| (id, None)).collect();
            return self.next_range();
        }
        self.step = RangeStep::Search(slots);
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: RANGE_CONCURRENCY,
            form: GroupForm::All { deadline: None },
        })))
    }

    fn searched(
        &mut self,
        slots: Vec<(Ulid, Option<Option<String>>)>,
        result: IoResult,
    ) -> RangeTurn {
        let IoResult::Group(GroupEvent::Finished { results }) = result else {
            return Err(SearchObjectsError::Unavailable);
        };
        let mut results = results.into_iter();
        for (range_id, slot) in slots {
            let found = slot.and_then(|through| {
                let GroupResult::Done(event) = results.next()? else {
                    return None;
                };
                let Event::Index(IndexEvent::Searched(page)) = *event else {
                    return None;
                };
                Some((*page, through))
            });
            self.found.push_back((range_id, found));
        }
        self.next_range()
    }

    fn next_range(&mut self) -> RangeTurn {
        loop {
            let Some((range_id, found)) = self.found.pop_front() else {
                return self.next_chunk();
            };
            let Some((page, through)) = found else {
                self.unready.push((range_id, None));
                self.incomplete += 1;
                continue;
            };
            // A key removed from the index after `as_of` leaves no trace, so a commit since
            // then makes the changed keys of this page unknown.
            self.certain &= page.committed_at.is_some_and(|at| at < self.input.as_of);
            self.current = Some(page);
            // Checked once wherever the query ends, so neither coverage nor timing shows the
            // cursor.
            return match through {
                Some(through) => {
                    let read = self.resolve(std::slice::from_ref(&through));
                    self.step = RangeStep::Through(range_id, through);
                    Ok(Next::Call(read))
                }
                None => self.read_keys(),
            };
        }
    }

    fn resolve(&self, keys: &[String]) -> IoCall {
        IoCall::child(ResolveKeysOperation::new(&self.input, &self.bucket, keys))
    }

    /// Whether the caller may read a building index's cursor, one candidate after another.
    fn check_through(
        &mut self,
        (range_id, through): (Ulid, String),
        mut pending: VecDeque<Candidate>,
    ) -> RangeTurn {
        let Some(candidate) = pending.pop_front() else {
            return self.through_known(range_id, through, false);
        };
        let call = self.walk.check.begin(&self.input, &candidate.hit);
        self.step = RangeStep::ThroughCheck(range_id, through, pending, Some(candidate));
        Ok(Next::Call(call))
    }

    fn through_known(&mut self, range_id: Ulid, through: String, shown: bool) -> RangeTurn {
        let (_, end) = self
            .window
            .as_ref()
            .ok_or(SearchObjectsError::Unavailable)?;
        if !shown || !ends_by(end, &through) {
            self.unready
                .push((range_id, Some(shown.then_some(through))));
            self.incomplete += 1;
        }
        self.read_keys()
    }

    fn read_keys(&mut self) -> RangeTurn {
        let page = self
            .current
            .as_mut()
            .ok_or(SearchObjectsError::Unavailable)?;
        self.examined += page.examined;
        let (key_match, query) = (self.input.key_match, &self.input.query);
        let mut keys = std::mem::take(&mut page.keys);
        keys.retain(|key| key_match.matches(key, query));
        if keys.is_empty() {
            return self.range_done();
        }
        let read = self.resolve(&keys);
        self.step = RangeStep::Keys;
        Ok(Next::Call(read))
    }

    fn walked(&mut self, turn: WalkTurn) -> RangeTurn {
        match turn {
            WalkTurn::Call(call) => {
                self.step = RangeStep::Walk;
                Ok(Next::Call(call))
            }
            WalkTurn::Drained => self.range_done(),
            WalkTurn::Full(next) => self.finish(next),
        }
    }

    /// Zero-hit and denied pages stop at the last examined key too, sealed so the caller never
    /// learns a key it may not read.
    fn range_done(&mut self) -> RangeTurn {
        let page = self.current.take().ok_or(SearchObjectsError::Unavailable)?;
        let Some(last) = page.last else {
            return self.next_range();
        };
        let position = BlobHeadKey::new(self.bucket.as_str(), last).to_bytes()?;
        self.finish(Some(seal_position(&position)?))
    }

    fn finish(&mut self, next: Option<Vec<u8>>) -> RangeTurn {
        let hits = std::mem::take(&mut self.walk.visible);
        let changed = std::mem::take(&mut self.walk.changed);
        let counts = (self.incomplete, self.remote);
        let mut found = page(hits, changed, self.certain, next, counts, self.now_ms);
        found.layout = (self.layout).map(|layout| (self.binding.set_id, layout));
        found.partial_ranges = (self.unready.iter())
            .filter_map(|(range_id, partial)| Some((*range_id, partial.clone()?)))
            .collect();
        if !self.indexed || self.unready.is_empty() {
            return Ok(Next::Done(found));
        }
        self.done_page = Some(found);
        self.step = RangeStep::Progress;
        let progress = RangeProgressOperation::new(&self.bucket);
        Ok(Next::Call(IoCall::child(progress)))
    }
}

/// The generation, covered key and query a stored index state allows searching with.
fn searchable(
    state: Option<IndexState>,
    query: &KeyQuery,
) -> Option<(u64, Option<String>, KeyQuery)> {
    let mut query = query.clone();
    match state? {
        IndexState::Ready { generation } => Some((generation, None, query)),
        IndexState::Building {
            generation,
            cursor: Some(cursor),
        } => {
            let through = Position::decode(&cursor).ok()?.key;
            if !ends_by(&query.end, &through) {
                query.end = Bound::Included(through.clone());
            }
            Some((generation, Some(through), query))
        }
        IndexState::Building { cursor: None, .. } => None,
    }
}

impl Operation for RangeSearchOperation {
    type Output = SearchNodePage;
    type Error = SearchObjectsError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> RangeTurn {
        let read = ReadRecordOperation::new(self.binding.set_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> RangeTurn {
        match std::mem::replace(&mut self.step, RangeStep::Keys) {
            RangeStep::Record => {
                let record = result.child::<Option<DistributionView>, RecordError>()?;
                let record = record.map_err(|error| SearchObjectsError::Storage(error.to_string()));
                match record? {
                    Some(view) => self.record_read(view),
                    // Without the record the ranges are unknown, so nothing is covered.
                    None => {
                        let empty = (Vec::new(), Vec::new());
                        let found = page(empty.0, empty.1, true, None, (1, 0), self.now_ms);
                        Ok(Next::Done(found))
                    }
                }
            }
            RangeStep::States(chunk, query) => self.search_chunk(chunk, query, result.values()?),
            RangeStep::Search(slots) => self.searched(slots, result),
            RangeStep::Through(range_id, through) => {
                let found = result.child::<Vec<Candidate>, SearchObjectsError>()??;
                self.check_through((range_id, through), found.into())
            }
            RangeStep::ThroughCheck(range_id, through, pending, candidate) => {
                let candidate = candidate.ok_or(SearchObjectsError::Unavailable)?;
                let input = &self.input;
                match self.walk.check.answer(input, &candidate.hit, result)? {
                    Checked::Call(call) => {
                        self.step =
                            RangeStep::ThroughCheck(range_id, through, pending, Some(candidate));
                        Ok(Next::Call(call))
                    }
                    Checked::Decided(true) => self.through_known(range_id, through, true),
                    Checked::Decided(false) => self.check_through((range_id, through), pending),
                }
            }
            RangeStep::Keys => {
                let found = result.child::<Vec<Candidate>, SearchObjectsError>()??;
                self.walk.add(found);
                let turn = self.walk.advance(&self.input);
                self.walked(turn)
            }
            RangeStep::Walk => {
                let turn = self.walk.answer(&self.input, result)?;
                self.walked(turn)
            }
            RangeStep::Progress => {
                // Progress is a hint, so a failed read leaves it out.
                let read = result.child::<BTreeMap<Ulid, IndexProgress>, FeedError>()?;
                let progress = read.unwrap_or_default();
                let mut found = self
                    .done_page
                    .take()
                    .ok_or(SearchObjectsError::Unavailable)?;
                found.index_progress = (self.unready.iter())
                    .filter_map(|(range_id, _)| Some((*range_id, *progress.get(range_id)?)))
                    .collect();
                Ok(Next::Done(found))
            }
        }
    }
}

/// The hits of the current heads of `keys`, in their order, from one read snapshot; keys without
/// a live head are left out, so a lagging index never returns a deleted object.
#[derive(Debug, PartialEq)]
struct ResolveKeysOperation {
    bucket: String,
    keys: Vec<String>,
    heads: Vec<(BlobHeadKey, Vec<u8>)>,
    hits: HitBuilder,
    building: bool,
}

impl ResolveKeysOperation {
    fn new(input: &SearchObjectsInput, bucket: &str, keys: &[String]) -> Self {
        Self {
            bucket: bucket.to_string(),
            keys: keys.to_vec(),
            heads: Vec::new(),
            hits: HitBuilder::new(input.clone()),
            building: false,
        }
    }

    fn built(turn: BuildTurn) -> SearchTurn<Vec<Candidate>> {
        Ok(match turn {
            BuildTurn::Call(call) => Next::Call(call),
            BuildTurn::Done(candidates) => Next::Done(candidates),
        })
    }
}

impl Operation for ResolveKeysOperation {
    type Output = Vec<Candidate>;
    type Error = SearchObjectsError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> SearchTurn<Vec<Candidate>> {
        let mut reads = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            let head = BlobHeadKey::new(self.bucket.as_str(), key.as_str());
            let bytes = head.to_bytes()?;
            reads.push((BLOB_HEAD_KEYSPACE.to_string(), Key::from(bytes.clone())));
            self.heads.push((head, bytes));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> SearchTurn<Vec<Candidate>> {
        if self.building {
            return Self::built(self.hits.answer(result)?);
        }
        let mut candidates = Vec::new();
        let heads = std::mem::take(&mut self.heads);
        for ((head, cursor_key), value) in heads.into_iter().zip(result.values()?) {
            let Some(value) = value else {
                continue;
            };
            let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
            candidates.push(HeadCandidate {
                head,
                version_id: pointer.version_id,
                cursor_key,
            });
        }
        self.building = true;
        // The index leaves very common metadata prefixes out, so versions are matched again.
        Self::built(self.hits.start(candidates)?)
    }
}

/// Starts a sealed position; no head key encoding starts with it, as bucket names are short.
const SEALED: u8 = 0xFF;

/// This process's key for sealed positions; a restart makes older sealed cursors invalid.
static POSITION_KEY: LazyLock<[u8; 32]> = LazyLock::new(rand::random);

/// A head key position encrypted and authenticated with this node's key, so a cursor carries
/// no key name; only this node opens it again.
pub(crate) fn seal_position(position: &[u8]) -> Result<Vec<u8>, SearchObjectsError> {
    let nonce: [u8; 24] = rand::random();
    let cipher = XChaCha20Poly1305::new((&*POSITION_KEY).into());
    let sealed = (cipher.encrypt(&XNonce::from(nonce), position))
        .map_err(|_| SearchObjectsError::Storage("search position not sealed".to_string()))?;
    Ok([&[SEALED][..], &nonce, &sealed].concat())
}

/// The head key of a position: a sealed one is opened, a plain one is passed on.
pub(crate) fn open_position(position: &[u8]) -> Result<Vec<u8>, SearchObjectsError> {
    let Some(sealed) = position.strip_prefix(&[SEALED]) else {
        return Ok(position.to_vec());
    };
    let (nonce, sealed) = sealed
        .split_at_checked(24)
        .ok_or(SearchObjectsError::Position)?;
    let nonce: [u8; 24] = nonce.try_into().map_err(|_| SearchObjectsError::Position)?;
    let cipher = XChaCha20Poly1305::new((&*POSITION_KEY).into());
    (cipher.decrypt(&XNonce::from(nonce), sealed)).map_err(|_| SearchObjectsError::Position)
}

/// Whether `position` is a sealed one, which only its node can check.
pub fn sealed_position(position: &[u8]) -> bool {
    position.first() == Some(&SEALED)
}

/// The keys a page may return: after the cursor and, for a prefix, inside the prefix.
fn window(input: &SearchObjectsInput, after: Option<String>) -> (Bound<String>, Bound<String>) {
    let after = after.map_or(Bound::Unbounded, Bound::Excluded);
    if input.key_match != ObjectKeyMatch::Prefix {
        return (after, Bound::Unbounded);
    }
    let end = prefix_end(&input.query).map_or(Bound::Unbounded, Bound::Excluded);
    let start = match after {
        Bound::Excluded(key) if key >= input.query => Bound::Excluded(key),
        _ => Bound::Included(input.query.clone()),
    };
    (start, end)
}

/// The smallest key after every key that starts with `prefix`; `None` if there is none.
fn prefix_end(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let next = match last as u32 + 1 {
            0xD800 => Some('\u{E000}'),
            code => char::from_u32(code),
        };
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// Whether a query ending at `end` stays within `through`, the last key a building index holds
/// every head up to.
fn ends_by(end: &Bound<String>, through: &str) -> bool {
    matches!(end, Bound::Included(end) | Bound::Excluded(end) if end.as_str() <= through)
}

fn overlaps(range: &KeyRange, start: &Bound<String>, end: &Bound<String>) -> bool {
    let after_start = match start {
        Bound::Included(key) | Bound::Excluded(key) => range
            .upper
            .as_deref()
            .is_none_or(|upper| key.as_str() < upper),
        Bound::Unbounded => true,
    };
    let before_end = match end {
        Bound::Included(key) => range.lower.as_str() <= key.as_str(),
        Bound::Excluded(key) => range.lower.as_str() < key.as_str(),
        Bound::Unbounded => true,
    };
    after_start && before_end
}

fn page(
    hits: Vec<SearchNodeHit>,
    changed: Vec<Vec<u8>>,
    certain: bool,
    next_start_after: Option<Vec<u8>>,
    (incomplete_ranges, remote_ranges): (usize, usize),
    now_ms: u64,
) -> SearchNodePage {
    SearchNodePage {
        hits,
        next_start_after,
        observed_at: UNIX_EPOCH + Duration::from_millis(now_ms),
        incomplete_ranges,
        remote_ranges,
        index_progress: Vec::new(),
        partial_ranges: Vec::new(),
        skipped_buckets: Vec::new(),
        changed: certain.then_some(changed),
        layout: None,
    }
}

/// Which member searches which keys: the ranges' bounds, coordinators and epochs.
fn layout_digest(ranges: &[KeyRange]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for range in ranges {
        let bounds = (range.range_id, &range.lower, &range.upper);
        let owner = (range.coordinator, range.epoch);
        hasher.update(&postcard::to_allocvec(&(bounds, owner)).unwrap_or_default());
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
#[path = "index_search_tests.rs"]
mod tests;
