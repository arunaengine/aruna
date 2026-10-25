//! Runs the periodic source refresh of distributed buckets: a range's coordinator queues its
//! reference heads for observation, a bounded page per pass from a durable round cursor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Mutex;
use std::time::SystemTime;

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DISTRIBUTION_KEYSPACE, NODE_STATE_KEYSPACE,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, KeyRange, WriteRoute,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::guard::{authority_reads, authority_rows, write_target};
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::metadata::{QueueRefreshOperation, ReferenceRefresh};

/// Heads one range's page reads.
const PAGE: usize = 256;
/// Range pages one pass reads at most.
const PAGES_PER_PASS: usize = 16;
/// Distribution records one pass looks at; the next pass continues after them.
const SETS_PER_PASS: usize = 64;
const ROUND_PREFIX: &[u8] = b"refresh_round/";

/// The record after which the next pass continues; a restart begins again at the start.
static CURSOR: Mutex<Option<Vec<u8>>> = Mutex::new(None);
/// The set and range after which the set's next pass goes on, so later ranges get pages too.
static RANGE_CURSOR: Mutex<Option<(Ulid, Ulid)>> = Mutex::new(None);

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

/// One bounded pass over the distributed buckets with a refresh interval. Returns whether a
/// round has more pages, so the next pass should come soon.
pub(crate) async fn refresh_ranges(context: &DriverContext) -> Result<bool, String> {
    let Some(net) = context.net_handle.as_ref() else {
        return Ok(false);
    };
    let realm_id = *net.realm_id();
    let after = CURSOR.lock().ok().and_then(|cursor| cursor.clone());
    let start = after.clone().map(|after| IterStart::After(after.into()));
    let (records, next) = page(context, DISTRIBUTION_KEYSPACE, None, start, SETS_PER_PASS).await?;
    let resume = |position: Option<Vec<u8>>| {
        if let Ok(mut cursor) = CURSOR.lock() {
            *cursor = position;
        }
    };
    resume(next.map(|next| next.to_vec()));
    let (mut pages, mut busy, mut done) = (0usize, false, after);
    for (record_key, value) in records {
        let Ok(view) = DistributionView::from_bytes(&value) else {
            continue;
        };
        let snapshot = view.snapshot();
        let Some(refresh_secs) = snapshot.config.settings.refresh_secs else {
            continue;
        };
        let bucket = &snapshot.config.bucket;
        let Ok(info) = drive(GetBucketOperation::new(bucket.clone()), context).await else {
            continue;
        };
        let Some(binding) = info.distribution.filter(|b| b.set_id == snapshot.set_id()) else {
            continue;
        };
        let now_ms = unix_timestamp_millis();
        let after = (RANGE_CURSOR.lock().ok().and_then(|cursor| *cursor))
            .filter(|(set_id, _)| *set_id == binding.set_id)
            .map(|(_, range_id)| range_id);
        let mut last = None;
        for range in after_range(coordinated(context, &binding).await?, after) {
            // A pass that runs out of pages comes back to this set, after its last paged range.
            if pages >= PAGES_PER_PASS {
                resume(done);
                if let (Ok(mut cursor), Some(range_id)) = (RANGE_CURSOR.lock(), last) {
                    *cursor = Some((binding.set_id, range_id));
                }
                return Ok(true);
            }
            let key = round_key(binding.set_id, range.range_id);
            let first = RefreshRound {
                started_ms: now_ms,
                ..RefreshRound::default()
            };
            let round = read_round(context, &key).await?.unwrap_or(first);
            let Some(round) = round.due(now_ms, refresh_secs) else {
                continue;
            };
            let walked = walk_page(context, (bucket, &range), round, realm_id).await?;
            pages += 1;
            last = Some(range.range_id);
            busy |= !walked.finished;
            write_round(context, key, &walked).await?;
        }
        done = Some(record_key.to_vec());
    }
    Ok(busy)
}

/// The ranges in the order a pass visits them: those after `after` first, then the rest.
fn after_range(mut ranges: Vec<KeyRange>, after: Option<Ulid>) -> Vec<KeyRange> {
    let position = after.and_then(|after| ranges.iter().position(|r| r.range_id == after));
    if let Some(position) = position {
        ranges.rotate_left(position + 1);
    }
    ranges
}

/// The ranges this member coordinates now; a range it fenced for a transfer is left out.
async fn coordinated(
    context: &DriverContext,
    binding: &DistributionBinding,
) -> Result<Vec<KeyRange>, String> {
    let read = StorageEffect::BatchRead {
        reads: authority_reads(binding.set_id),
        txn_id: None,
    };
    let event = context.storage_handle.send_storage_effect(read).await;
    let (record, fence) = authority_rows(event).map_err(|error| error.to_string())?;
    let Some(view) = (record.as_deref())
        .map(DistributionView::from_bytes)
        .transpose()
        .map_err(|error| error.to_string())?
    else {
        return Ok(Vec::new());
    };
    let ranges = view.snapshot().config.ranges.iter().filter(|range| {
        let target = write_target(binding, &range.lower, record.as_deref(), fence.as_deref());
        matches!(target, Ok(WriteRoute::Local { .. }))
    });
    Ok(ranges.cloned().collect())
}

/// Queues the reference heads of one page of the range and returns the round after it.
async fn walk_page(
    context: &DriverContext,
    (bucket, range): (&str, &KeyRange),
    round: RefreshRound,
    realm_id: aruna_core::structs::identity::realm::RealmId,
) -> Result<RefreshRound, String> {
    let error = |error: aruna_core::errors::ConversionError| error.to_string();
    let prefix = BlobHeadKey::bucket_prefix(bucket).map_err(error)?;
    let start = match &round.after {
        Some(after) => IterStart::After(
            BlobHeadKey::new(bucket, after)
                .to_bytes()
                .map_err(error)?
                .into(),
        ),
        None => IterStart::At(
            BlobHeadKey::new(bucket, &range.lower)
                .to_bytes()
                .map_err(error)?
                .into(),
        ),
    };
    let (heads, more) = page(context, BLOB_HEAD_KEYSPACE, Some(prefix), Some(start), PAGE).await?;
    let mut next = RefreshRound {
        finished: more.is_none(),
        ..round
    };
    let node_id = context.net_handle.as_ref().map(|net| net.node_id());
    for (key, value) in heads {
        let head = BlobHeadKey::from_bytes(&key).map_err(error)?;
        if !range.contains(&head.key) {
            next.finished = true;
            break;
        }
        let pointer = CurrentVersionPointer::from_bytes(&value).map_err(error)?;
        let row = VersionKey::new(bucket, &head.key, pointer.version_id)
            .to_bytes()
            .map_err(error)?;
        let version = read_value(context, BLOB_VERSIONS_KEYSPACE, row.into()).await?;
        let version = (version.as_deref())
            .map(BlobVersion::from_bytes)
            .transpose()
            .map_err(error)?;
        if let (Some(version), Some(node_id)) = (version, node_id)
            && matches!(version.state, BlobVersionState::Reference { .. })
        {
            // The worker observes the source; only a confirmed change mints a successor.
            let refresh = ReferenceRefresh {
                bucket: bucket.to_string(),
                key: head.key.clone(),
                version_id: pointer.version_id,
                metadata: None,
                refreshed_at: SystemTime::now(),
                node_id,
                auth_context: AuthContext {
                    user_id: version.created_by,
                    realm_id,
                    path_restrictions: None,
                    session: None,
                },
            };
            drive(QueueRefreshOperation::new(refresh), context)
                .await
                .map_err(|error| error.to_string())?;
        }
        next.after = Some(head.key);
    }
    Ok(next)
}

async fn read_round(context: &DriverContext, key: &[u8]) -> Result<Option<RefreshRound>, String> {
    let value = read_value(context, NODE_STATE_KEYSPACE, key.to_vec().into()).await?;
    (value.as_deref())
        .map(postcard::from_bytes)
        .transpose()
        .map_err(|error| error.to_string())
}

async fn write_round(
    context: &DriverContext,
    key: Vec<u8>,
    round: &RefreshRound,
) -> Result<(), String> {
    let value = postcard::to_allocvec(round).map_err(|error| error.to_string())?;
    let write = StorageEffect::Write {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("refresh round not written: {other:?}")),
    }
}

async fn read_value(
    context: &DriverContext,
    key_space: &str,
    key: Key,
) -> Result<Option<Value>, String> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        other => Err(format!("unexpected read answer {other:?}")),
    }
}

/// One page of `key_space` and whether more rows may follow.
async fn page(
    context: &DriverContext,
    key_space: &str,
    prefix: Option<Vec<u8>>,
    start: Option<IterStart>,
    limit: usize,
) -> Result<(Vec<(Key, Value)>, Option<Key>), String> {
    let iter = StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: prefix.map(Into::into),
        start,
        limit,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        other => Err(format!("unexpected page answer {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A pass that ran out of pages goes on after its last paged range, then wraps around.
    #[test]
    fn passes_resume_ranges() {
        let range = |seed: u8| KeyRange {
            range_id: Ulid::from_bytes([seed; 16]),
            lower: String::new(),
            upper: None,
            lineage: None,
            coordinator: Ulid::nil(),
            epoch: 1,
            replicas: Vec::new(),
        };
        let ids = |ranges: Vec<KeyRange>| -> Vec<u8> {
            ranges.iter().map(|r| r.range_id.to_bytes()[0]).collect()
        };
        let all = vec![range(1), range(2), range(3)];
        assert_eq!(ids(after_range(all.clone(), None)), [1, 2, 3]);
        let after = Some(Ulid::from_bytes([2; 16]));
        assert_eq!(ids(after_range(all.clone(), after)), [3, 1, 2]);
        let gone = Some(Ulid::from_bytes([9; 16]));
        assert_eq!(
            ids(after_range(all, gone)),
            [1, 2, 3],
            "a gone range starts over"
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
