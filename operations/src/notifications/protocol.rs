//! Bounds the watch event batches, scans, expansions and retries of the notification transport.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::structs::execution::notification_watch::{WatchEventRetry, watch_retry_key};
use aruna_core::structs::identity::realm::RealmId;

pub const EVENT_BATCH_SIZE: usize = 128;
pub const SUBSCRIPTION_SCAN_CAP: usize = 1024;
pub const DIRTY_REALM_CAP: usize = 4;
pub const RETRY_BATCH_CAP: usize = 4;
pub const RETRY_BYTES_CAP: usize = 1024 * 1024;
pub const EXPANSION_WORK_CAP: usize = 4096;
pub const EXPANSION_CANDIDATE_CAP: usize = 1024;
pub const EXPANSION_RECORD_CAP: usize = 1024;
/// Subscription rows read per expansion page.
pub(crate) const WATCH_PAGE_LIMIT: usize = 256;

pub(crate) fn valid_retry(retry: &WatchEventRetry, realm_id: RealmId, key: &[u8]) -> bool {
    let Some(first) = retry.events.first() else {
        return false;
    };
    let Some(cursor) = retry.cursor.as_ref() else {
        return false;
    };
    retry.events.len() <= EVENT_BATCH_SIZE
        && retry
            .events
            .iter()
            .all(|event| event.realm_id == realm_id && !event.event_id.is_nil())
        && cursor.len() == 64
        && cursor.starts_with(UserId::storage_prefix(realm_id).as_ref())
        && key == watch_retry_key(realm_id, first.event_id).as_slice()
}

pub(crate) fn page_limit(events: usize) -> Result<usize, String> {
    if events == 0 {
        return Err("watch event batch is empty".to_string());
    }
    let mut limit = WATCH_PAGE_LIMIT;
    for cap in [
        EXPANSION_WORK_CAP,
        EXPANSION_CANDIDATE_CAP,
        EXPANSION_RECORD_CAP,
    ] {
        limit = limit.min(cap / events);
    }
    (limit > 0)
        .then_some(limit)
        .ok_or_else(|| "watch event batch exceeds expansion caps".to_string())
}

pub(crate) fn expansion_budget(events: usize, subscriptions: usize) -> Result<usize, String> {
    let work = events
        .checked_mul(subscriptions)
        .ok_or_else(|| "watch expansion work budget overflow".to_string())?;
    if work > EXPANSION_WORK_CAP {
        return Err(format!(
            "watch expansion work {} exceeds cap {}",
            work, EXPANSION_WORK_CAP
        ));
    }
    Ok(work)
}

pub(crate) fn add_limit(
    total: usize,
    added: usize,
    limit: usize,
    label: &str,
) -> Result<usize, String> {
    let next = total
        .checked_add(added)
        .ok_or_else(|| format!("watch expansion {label} budget overflow"))?;
    if next > limit {
        return Err(format!(
            "watch expansion {label} count {next} exceeds cap {limit}"
        ));
    }
    Ok(next)
}
