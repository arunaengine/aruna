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
            certain &= found.committed_at.is_some_and(|at| at < input.as_of);
            // Checked once wherever the query ends, so neither coverage nor timing shows the cursor.
            if let Some(through) = through {
                let shown = readable(context, input, &bucket, &mut evaluators, &through).await?;
                if !shown || !ends_by(&end, &through) {
                    unready.push((*range_id, Some(shown.then_some(through))));
                    incomplete += 1;
                }
            }
            examined += found.examined;
            let (mut keys, last) = (found.keys, found.last);
            keys.retain(|key| input.key_match.matches(key, &input.query));
            for candidate in resolve_keys(context, input, &bucket, &keys).await? {
                if !authorize(context, input, &mut evaluators, &candidate.hit).await? {
                    continue;
                }
                if candidate.changed {
                    changed.push(candidate.hit.cursor_key);
                    if changed.len() >= CHANGED_PAGE_CAP {
                        let next = changed.last().cloned();
                        return Ok(page(visible, changed, certain, next, incomplete, remote));
                    }
                    continue;
                }
                visible.push(candidate.hit);
                if visible.len() > limit {
                    let next = visible.get(limit - 1).map(|hit| hit.cursor_key.clone());
                    visible.truncate(limit);
                    changed_until(&mut changed, next.as_deref());
                    return Ok(page(visible, changed, certain, next, incomplete, remote));
                }
            }
            if let Some(last) = last {
                // Zero-hit and denied pages stop here too and continue after the last key,
                // sealed so the caller never learns a key it may not read.
                let position = BlobHeadKey::new(bucket.as_str(), last).to_bytes()?;
                return Ok(page(
                    visible,
                    changed,
                    certain,
                    Some(seal_position(&position)?),
                    incomplete,
                    remote,
                ));
            }
        }
    }
    Ok(page(visible, changed, certain, None, incomplete, remote))
}

/// Queries the indexes of `chunk` at once; `None` for a range without a searchable one. A
/// building index is searched up to its cursor and returned with the key it covers through.
async fn search_chunk(
    context: &DriverContext,
    search: &RangeSearch,
    chunk: &[Ulid],
    query: KeyQuery,
) -> Result<Vec<Option<(KeyPage, Option<String>)>>, SearchObjectsError> {
    let set_id = search.binding.set_id;
    let reads = (chunk.iter())
        .map(|range_id| {
            let key = state_key(set_id, *range_id);
            (SEARCH_INDEX_KEYSPACE.to_string(), Key::from(key))
        })
        .collect();
    let txn_id = start_read(context).await?;
    let states = batch_read(context, reads, txn_id).await;
    let states = finish_read(context, txn_id, states).await?;
    let searches = chunk.iter().zip(states).map(|(range_id, (_, state))| {
        let state = state.and_then(|value| IndexState::from_bytes(value.as_ref()).ok());
        let (indexes, mut query) = (search.indexes.clone(), query.clone());
        async move {
            let (generation, through) = match state? {
                IndexState::Ready { generation } => (generation, None),
                IndexState::Building {
                    generation,
                    cursor: Some(cursor),
                } => {
                    let through = Position::decode(&cursor).ok()?.key;
                    if !ends_by(&query.end, &through) {
                        query.end = Bound::Included(through.clone());
                    }
                    (generation, Some(through))
                }
                IndexState::Building { cursor: None, .. } => return None,
            };
            let found = indexes?.search(set_id, *range_id, generation, query).await;
            Some((found.ok()?, through))
        }
    });
    Ok(join_all(searches).await)
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
    incomplete_ranges: usize,
    remote_ranges: usize,
) -> SearchNodePage {
    SearchNodePage {
        hits,
        next_start_after,
        observed_at: std::time::SystemTime::now(),
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

/// The hits of the current heads of `keys`, in their order; keys without a live head are
/// left out, so a lagging index never returns a deleted object.
async fn resolve_keys(
    context: &DriverContext,
    input: &SearchObjectsInput,
    bucket: &str,
    keys: &[String],
) -> Result<Vec<Candidate>, SearchObjectsError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let txn_id = start_read(context).await?;
    let result = resolve_heads(context, input, bucket, keys, txn_id).await;
    finish_read(context, txn_id, result).await
}

async fn resolve_heads(
    context: &DriverContext,
    input: &SearchObjectsInput,
    bucket: &str,
    keys: &[String],
    txn_id: TxnId,
) -> Result<Vec<Candidate>, SearchObjectsError> {
    let heads: Vec<_> = (keys.iter())
        .map(|key| BlobHeadKey::new(bucket, key.as_str()))
        .collect();
    let reads = (heads.iter())
        .map(|head| {
            let key = head.to_bytes()?;
            Ok((BLOB_HEAD_KEYSPACE.to_string(), Key::from(key)))
        })
        .collect::<Result<Vec<_>, SearchObjectsError>>()?;
    let values = batch_read(context, reads, txn_id).await?;
    let mut candidates = Vec::new();
    for (head, (key, value)) in heads.into_iter().zip(values) {
        let Some(value) = value else {
            continue;
        };
        let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
        candidates.push(HeadCandidate {
            head,
            version_id: pointer.version_id,
            cursor_key: key.to_vec(),
        });
    }
    let mut live = load_live(context, input, candidates, txn_id).await?;
    // The index leaves very common metadata prefixes out, so versions are matched again here.
    retain_matches(input, &mut live);
    build_hits(context, input, live, txn_id).await
}

#[cfg(test)]
#[path = "index_search_tests.rs"]
mod tests;
