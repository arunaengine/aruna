//! Searches a distributed bucket only through the key indexes of the ranges this member
//! coordinates, with a bounded number of examined candidates and concurrent ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::{Arc, LazyLock};

use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, SEARCH_INDEX_KEYSPACE};
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer};
use aruna_core::structs::storage::distribution::search::IndexState;
use aruna_core::structs::storage::distribution::{DistributionBinding, KeyRange};
use aruna_core::types::{Key, TxnId};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use futures_util::future::join_all;
use ulid::Ulid;

use super::{
    HeadCandidate, ObjectKeyMatch, SEARCH_MAX_LIMIT, SearchNodeHit, SearchNodePage,
    SearchObjectsError, SearchObjectsInput, authorize, batch_read, build_hits, finish_read,
    load_live, start_read,
};
use crate::distribution::index_feed::state_key;
use crate::distribution::key_index::{KeyPage, KeyQuery, RangeIndexes};
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
/// missing or building index is reported as incomplete and never replaced by a scan.
pub(super) async fn search_ranges(
    context: &DriverContext,
    input: &SearchObjectsInput,
    search: RangeSearch,
) -> Result<SearchNodePage, SearchObjectsError> {
    let bucket = input.bucket.clone().unwrap_or_default();
    let limit = input.limit.clamp(1, SEARCH_MAX_LIMIT);
    let budget = search.budget.max(1);
    let set_id = search.binding.set_id;
    let record = read_record(context, set_id).await;
    let Some(view) = record.map_err(SearchObjectsError::Storage)? else {
        // Without the record the ranges are unknown, so nothing is covered.
        return Ok(page(Vec::new(), None, 1, 0));
    };
    let after = match input.start_after.as_deref() {
        Some(position) => Some(BlobHeadKey::from_bytes(&open_position(position)?)?.key),
        None => None,
    };
    let (start, end) = window(input, after);
    let mut local = Vec::new();
    let mut remote = 0;
    for range in &view.head.snapshot.config.ranges {
        if !overlaps(range, &start, &end) {
            continue;
        }
        match range.coordinator == search.binding.member_id {
            true => local.push(range.range_id),
            false => remote += 1,
        }
    }

    let names = (input.key_match == ObjectKeyMatch::Substring).then(|| input.query.clone());
    let mut evaluators = HashMap::new();
    let mut visible = Vec::with_capacity(limit + 1);
    let (mut examined, mut incomplete) = (0, 0);
    for chunk in local.chunks(RANGE_CONCURRENCY) {
        let query = KeyQuery {
            start: start.clone(),
            end: end.clone(),
            names: names.clone(),
            limit: budget.saturating_sub(examined).max(1),
        };
        let found = search_chunk(context, &search, chunk, query).await?;
        for found in found {
            let Some(found) = found else {
                incomplete += 1;
                continue;
            };
            examined += found.examined;
            let (mut keys, last) = (found.keys, found.last);
            keys.retain(|key| input.key_match.matches(key, &input.query));
            for candidate in resolve_keys(context, input, &bucket, &keys).await? {
                if !authorize(context, input, &mut evaluators, &candidate).await? {
                    continue;
                }
                visible.push(candidate);
                if visible.len() > limit {
                    let next = visible.get(limit - 1).map(|hit| hit.cursor_key.clone());
                    visible.truncate(limit);
                    return Ok(page(visible, next, incomplete, remote));
                }
            }
            if let Some(last) = last {
                // Zero-hit and denied pages stop here too and continue after the last key,
                // sealed so the caller never learns a key it may not read.
                let position = BlobHeadKey::new(bucket.as_str(), last).to_bytes()?;
                return Ok(page(
                    visible,
                    Some(seal_position(&position)?),
                    incomplete,
                    remote,
                ));
            }
        }
    }
    Ok(page(visible, None, incomplete, remote))
}

/// Queries the ready indexes of `chunk` at once; `None` for a range without one.
async fn search_chunk(
    context: &DriverContext,
    search: &RangeSearch,
    chunk: &[Ulid],
    query: KeyQuery,
) -> Result<Vec<Option<KeyPage>>, SearchObjectsError> {
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
        let (indexes, query) = (search.indexes.clone(), query.clone());
        async move {
            let (Some(indexes), Some(IndexState::Ready { generation })) = (indexes, state) else {
                return None;
            };
            indexes
                .search(set_id, *range_id, generation, query)
                .await
                .ok()
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
pub(super) fn seal_position(position: &[u8]) -> Result<Vec<u8>, SearchObjectsError> {
    let nonce: [u8; 24] = rand::random();
    let cipher = XChaCha20Poly1305::new((&*POSITION_KEY).into());
    let sealed = (cipher.encrypt(&XNonce::from(nonce), position))
        .map_err(|_| SearchObjectsError::Storage("search position not sealed".to_string()))?;
    Ok([&[SEALED][..], &nonce, &sealed].concat())
}

/// The head key of a position: a sealed one is opened, a plain one is passed on.
pub(super) fn open_position(position: &[u8]) -> Result<Vec<u8>, SearchObjectsError> {
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
    }
}

/// The hits of the current heads of `keys`, in their order; keys without a live head are
/// left out, so a lagging index never returns a deleted object.
async fn resolve_keys(
    context: &DriverContext,
    input: &SearchObjectsInput,
    bucket: &str,
    keys: &[String],
) -> Result<Vec<SearchNodeHit>, SearchObjectsError> {
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
) -> Result<Vec<SearchNodeHit>, SearchObjectsError> {
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
    let live = load_live(context, input, candidates, txn_id).await?;
    build_hits(context, input, live, txn_id).await
}

#[cfg(test)]
#[path = "index_search_tests.rs"]
mod tests;
