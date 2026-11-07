//! Searches a distributed bucket only through the key indexes of the ranges this member
//! coordinates, with a bounded number of examined candidates and concurrent ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::{Arc, LazyLock};

use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, SEARCH_INDEX_KEYSPACE};
use aruna_core::structs::execution::table::{query_terms, search_tokens};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer};
use aruna_core::structs::storage::distribution::inventory::Position;
use aruna_core::structs::storage::distribution::search::IndexState;
use aruna_core::structs::storage::distribution::{DistributionBinding, KeyRange};
use aruna_core::types::{GroupId, Key, TxnId};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use futures_util::future::join_all;
use ulid::Ulid;

use super::{
    CHANGED_PAGE_CAP, Candidate, HeadCandidate, ObjectKeyMatch, SEARCH_MAX_LIMIT, SearchNodeHit,
    SearchNodePage, SearchObjectsError, SearchObjectsInput, authorize, batch_read, build_hits,
    changed_until, finish_read, load_live, retain_matches, start_read,
};
use crate::auth::request_policy::PolicyEvaluator;
use crate::distribution::index_feed::{range_progress, state_key};
use crate::distribution::key_index::{KeyPage, KeyQuery, MetaTerms, RangeIndexes};
use crate::driver::DriverContext;
use crate::replication::members::read_record;

/// Index candidates one node page examines at most, visible or not.
pub(super) const EXAMINE_BUDGET: usize = 1_000;
/// Range indexes one page queries at once.
const RANGE_CONCURRENCY: usize = 4;

pub(super) struct RangeSearch {
    pub(super) binding: DistributionBinding,
    pub(super) indexes: Option<Arc<RangeIndexes>>,
    pub(super) budget: usize,
}

/// One page of a distributed bucket. Ranges are read in key order and continue by key; a
/// missing or building index is reported as incomplete, with its progress, and never replaced
/// by a scan. A building index answers for the keys up to its cursor and is reported partial.
pub(super) async fn search_ranges(
    context: &DriverContext,
    input: &SearchObjectsInput,
    search: RangeSearch,
) -> Result<SearchNodePage, SearchObjectsError> {
    let indexes = search.indexes.clone();
    let record = read_record(context, search.binding.set_id).await;
    let Some(view) = record.map_err(SearchObjectsError::Storage)? else {
        // Without the record the ranges are unknown, so nothing is covered.
        return Ok(page(Vec::new(), Vec::new(), true, None, 1, 0));
    };
    let ranges = &view.head.snapshot.config.ranges;
    let mut unready = Vec::new();
    let mut page = range_page(context, input, search, ranges, &mut unready).await?;
    page.layout = Some(layout_digest(ranges));
    page.partial_ranges = (unready.iter())
        .filter_map(|(range_id, partial)| Some((*range_id, partial.clone()?)))
        .collect();
    if let (Some(indexes), false) = (indexes, unready.is_empty()) {
        let bucket = input.bucket.as_deref().unwrap_or_default();
        // Progress is a hint, so a failed read leaves it out.
        let progress = (range_progress(context, &indexes, bucket).await).unwrap_or_default();
        page.index_progress = (unready.into_iter())
            .filter_map(|(range_id, _)| Some((range_id, *progress.get(&range_id)?)))
            .collect();
    }
    Ok(page)
}

/// Whether the caller may read `key`, a building index's cursor.
async fn readable(
    context: &DriverContext,
    input: &SearchObjectsInput,
    bucket: &str,
    evaluators: &mut HashMap<(RealmId, GroupId), PolicyEvaluator>,
    key: &str,
) -> Result<bool, SearchObjectsError> {
    for candidate in resolve_keys(context, input, bucket, &[key.to_string()]).await? {
        if authorize(context, input, evaluators, &candidate.hit).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Adds each incomplete range to `unready`, a partial one with the cursor key it may show.
async fn range_page(
    context: &DriverContext,
    input: &SearchObjectsInput,
    search: RangeSearch,
    ranges: &[KeyRange],
    unready: &mut Vec<(Ulid, Option<Option<String>>)>,
) -> Result<SearchNodePage, SearchObjectsError> {
    let bucket = input.bucket.clone().unwrap_or_default();
    let limit = input.limit.clamp(1, SEARCH_MAX_LIMIT);
    let budget = search.budget.max(1);
    let after = match input.start_after.as_deref() {
        Some(position) => Some(BlobHeadKey::from_bytes(&open_position(position)?)?.key),
        None => None,
    };
    let (start, end) = window(input, after);
    let mut local = Vec::new();
    let mut remote = 0;
    for range in ranges {
        if !overlaps(range, &start, &end) {
            continue;
        }
        match range.coordinator == search.binding.member_id {
            true => local.push(range.range_id),
            false => remote += 1,
        }
    }

    let names = (input.key_match == ObjectKeyMatch::Substring).then(|| input.query.clone());
    let meta = (input.key_match == ObjectKeyMatch::Metadata).then(|| {
        let (text, exact) = query_terms(&input.query);
        let text = search_tokens(&text).into_iter().collect();
        MetaTerms {
            text,
            exact: exact.into_iter().collect(),
        }
    });
    let mut evaluators = HashMap::new();
    let mut visible = Vec::with_capacity(limit + 1);
    let mut changed = Vec::new();
    let mut certain = true;
    let (mut examined, mut incomplete) = (0, 0);
    for chunk in local.chunks(RANGE_CONCURRENCY) {
        let query = KeyQuery {
            start: start.clone(),
            end: end.clone(),
            names: names.clone(),
            meta: meta.clone(),
            limit: budget.saturating_sub(examined).max(1),
        };
        let found = search_chunk(context, &search, chunk, query).await?;
        for (range_id, found) in chunk.iter().zip(found) {
            let Some((found, through)) = found else {
                unready.push((*range_id, None));
                incomplete += 1;
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
