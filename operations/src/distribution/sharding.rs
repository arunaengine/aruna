//! Tends the key ranges this node holds: runs the accounting of a split the record installed,
//! rebuilds split children until their handoff, and lets a coordinator split a range whose
//! maintained counters pass the bucket's thresholds. Every pass is bounded and resumes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StatementSignEvent, StorageEvent};
use aruna_core::keyspaces::{
    DISTRIBUTION_KEYSPACE, KEY_SAMPLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, RangeAccount, RangeCounters, SAMPLE_SIZE, SampleEntry, generation_key, needs_refill,
    sample_prefix,
};
use aruna_core::structs::storage::distribution::{
    DistributionMember, DistributionSnapshot, DistributionView, KeyRange, Statement, StatementClaim,
};
use aruna_core::types::{Key, Value};
use tracing::{info, warn};
use ulid::Ulid;

use super::create::submit;
use super::rebuild::{RebuildConfig, RebuildOperation, RebuildOutcome};
use super::refill::{RefillConfig, RefillOperation};
use super::sign_statement;
use super::split::{SplitConfig, SplitError, SplitOperation};
use crate::driver::{DriverContext, drive};

/// How long a pass that found nothing to do waits before it looks again.
pub const MAINTAIN_AFTER: Duration = Duration::from_secs(60);
/// Account rows one page reads.
const ACCOUNT_PAGE: usize = 64;
/// Account pages one pass reads before it yields to the next timer run.
const MAX_PAGES: usize = 16;
const WALK_ROWS: usize = 512;
const WALK_BYTES: usize = 4 * 1024 * 1024;
/// Page transactions one rebuild or refill run commits.
const WALK_PAGES: usize = 8;

/// The account row after which the next pass continues; a restart begins again at the start.
static CURSOR: Mutex<Option<Vec<u8>>> = Mutex::new(None);

/// What tending one range left behind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Tended {
    Idle,
    /// Work remains; the next pass comes at once.
    Busy,
}

/// One bounded pass over the range accounts. Returns when the next pass should run.
pub async fn maintain_ranges(context: &Arc<DriverContext>) -> Duration {
    let Some(net) = context.net_handle.as_ref() else {
        return MAINTAIN_AFTER;
    };
    let (realm_id, local) = (*net.realm_id(), net.node_id());
    let mut start = CURSOR.lock().map(|cursor| cursor.clone()).unwrap_or(None);
    let mut records: HashMap<Ulid, Option<DistributionSnapshot>> = HashMap::new();
    let mut busy = false;
    for _ in 0..MAX_PAGES {
        let (rows, more) = match accounts(context, start.clone()).await {
            Ok(page) => page,
            Err(error) => {
                warn!(%error, "Range maintenance could not read the range accounts");
                return MAINTAIN_AFTER;
            }
        };
        for (key, value) in rows {
            start = Some(key.clone());
            let Some((set_id, range_id, account)) = decode(&key, &value) else {
                continue;
            };
            if !records.contains_key(&set_id) {
                let record = read_record(context, set_id).await;
                records.insert(set_id, record);
            }
            let Some(Some(snapshot)) = records.get(&set_id) else {
                continue;
            };
            let range = Tending {
                realm_id,
                local,
                snapshot,
                range_id,
                account: &account,
            };
            if range.tend(context).await == Tended::Busy {
                busy = true;
            }
        }
        if !more {
            start = None;
            break;
        }
    }
    let more = start.is_some();
    if let Ok(mut cursor) = CURSOR.lock() {
        *cursor = start;
    }
    match busy || more {
        true => Duration::ZERO,
        false => MAINTAIN_AFTER,
    }
}

fn decode(key: &[u8], value: &[u8]) -> Option<(Ulid, Ulid, RangeAccount)> {
    let set_id = Ulid::from_bytes(key.get(..16)?.try_into().ok()?);
    let range_id = Ulid::from_bytes(key.get(16..32)?.try_into().ok()?);
    let account = RangeAccount::from_bytes(value).ok()?;
    (key.len() == 32).then_some((set_id, range_id, account))
}

async fn accounts(
    context: &DriverContext,
    start: Option<Vec<u8>>,
) -> Result<(Vec<(Vec<u8>, Value)>, bool), String> {
    let iter = StorageEffect::Iter {
        key_space: RANGE_ACCOUNT_KEYSPACE.to_string(),
        prefix: None,
        start: start.map(|key| IterStart::After(Key::from(key))),
        limit: ACCOUNT_PAGE,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((
            values
                .into_iter()
                .map(|(key, value)| (key.to_vec(), value))
                .collect(),
            next_start_after.is_some(),
        )),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected answer {other:?}")),
    }
}

async fn read(context: &DriverContext, key_space: &str, key: Vec<u8>) -> Option<Value> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        _ => None,
    }
}

async fn read_record(context: &DriverContext, set_id: Ulid) -> Option<DistributionSnapshot> {
    let bytes = read(context, DISTRIBUTION_KEYSPACE, set_id.to_bytes().to_vec()).await?;
    let view = DistributionView::from_bytes(&bytes).ok()?;
    // A conflicted set takes no configuration changes, splits included.
    (!view.conflicted()).then(|| view.head.snapshot)
}

/// One range account this node holds, with the record of its set.
struct Tending<'a> {
    realm_id: RealmId,
    local: NodeId,
    snapshot: &'a DistributionSnapshot,
    range_id: Ulid,
    account: &'a RangeAccount,
}

impl Tending<'_> {
    fn set_id(&self) -> Ulid {
        self.snapshot.set_id()
    }

    async fn tend(&self, context: &Arc<DriverContext>) -> Tended {
        match (&self.account.charge, self.account.rebuild.is_some()) {
            (Charge::Retired { .. }, _) => Tended::Idle,
            (Charge::Active(_), false) => match self.snapshot.retired_range(self.range_id) {
                Some(retired) => {
                    let config = SplitConfig {
                        set_id: self.set_id(),
                        range_id: self.range_id,
                        split_key: retired.split_key.clone(),
                        left: retired.lower_id,
                        right: retired.upper_id,
                    };
                    self.split_accounting(context, config).await
                }
                None => self.check_thresholds(context).await,
            },
            (Charge::Active(_) | Charge::Child { .. }, _) => self.rebuild(context).await,
        }
    }

    /// Splits this node's accounting the way the record split the range.
    async fn split_accounting(&self, context: &Arc<DriverContext>, config: SplitConfig) -> Tended {
        match drive(SplitOperation::new(config), context.as_ref()).await {
            Ok(()) => Tended::Busy,
            // Concurrent writers left more sample entries than one split reads.
            Err(SplitError::Refused(_, "the key sample needs a refill")) => {
                self.refill(context).await
            }
            Err(error) => {
                warn!(set_id = %self.set_id(), range_id = %self.range_id, %error, "A range split could not be accounted");
                Tended::Idle
            }
        }
    }

    async fn rebuild(&self, context: &Arc<DriverContext>) -> Tended {
        let config = RebuildConfig {
            set_id: self.set_id(),
            range_id: self.range_id,
            page_rows: WALK_ROWS,
            page_bytes: WALK_BYTES,
            max_pages: WALK_PAGES,
        };
        match drive(RebuildOperation::new(config), context.as_ref()).await {
            Ok(RebuildOutcome::Paused) => Tended::Busy,
            Ok(RebuildOutcome::Done | RebuildOutcome::Waiting) => Tended::Idle,
            Err(error) => {
                warn!(set_id = %self.set_id(), range_id = %self.range_id, %error, "A range rebuild failed");
                Tended::Idle
            }
        }
    }

    async fn refill(&self, context: &Arc<DriverContext>) -> Tended {
        let config = RefillConfig {
            set_id: self.set_id(),
            range_id: self.range_id,
            page_rows: WALK_ROWS,
            page_bytes: WALK_BYTES,
            max_pages: WALK_PAGES,
        };
        match drive(RefillOperation::new(config), context.as_ref()).await {
            Ok(_) => Tended::Busy,
            Err(error) => {
                warn!(set_id = %self.set_id(), range_id = %self.range_id, %error, "A key sample refill failed");
                Tended::Idle
            }
        }
    }

    /// The range this node coordinates and may split now, with the member it signs as.
    fn splittable(&self) -> Option<(&DistributionMember, &KeyRange)> {
        let snapshot = self.snapshot;
        let settings = &snapshot.config.settings;
        if settings.split_objects.is_none() && settings.split_bytes.is_none() {
            return None;
        }
        let member =
            (snapshot.config.members.iter()).find(|member| member.node_id == self.local)?;
        let range =
            (snapshot.config.ranges.iter()).find(|range| range.range_id == self.range_id)?;
        let moving =
            (snapshot.lifecycle.transfers.iter()).any(|open| open.range_id == range.range_id);
        let open = snapshot.serving() && snapshot.lifecycle.deletion.is_none() && !moving;
        (open && range.coordinator == member.member_id).then_some((member, range))
    }

    /// Splits the range when its counters pass a threshold, at a key from its sample.
    async fn check_thresholds(&self, context: &Arc<DriverContext>) -> Tended {
        let Some((member, range)) = self.splittable() else {
            return Tended::Idle;
        };
        let Charge::Active(generation) = self.account.charge else {
            return Tended::Idle;
        };
        let row = generation_key(self.set_id(), range.range_id, generation);
        let counters = read(context, RANGE_COUNTER_KEYSPACE, row)
            .await
            .and_then(|value| RangeCounters::from_bytes(&value).ok())
            .unwrap_or_default();
        let settings = &self.snapshot.config.settings;
        let above =
            |limit: Option<u64>, value: i128| limit.is_some_and(|limit| value > i128::from(limit));
        if !above(settings.split_objects, counters.objects)
            && !above(settings.split_bytes, counters.current_bytes)
        {
            return Tended::Idle;
        }
        let keys = match self.sample(context).await {
            Some(keys) if !needs_refill(keys.len(), counters.objects) => keys,
            _ => return self.refill(context).await,
        };
        let Some(split_key) = split_point(&keys, range) else {
            return Tended::Idle;
        };
        let statement = Statement::split(
            self.realm_id,
            self.set_id(),
            member,
            self.snapshot.number,
            range,
            &split_key,
        );
        let claim = StatementClaim {
            signer: self.local,
            statement,
        };
        let signed = match sign_statement(context, claim) {
            StatementSignEvent::Signed(signed) => *signed,
            StatementSignEvent::Unavailable(reason) => {
                warn!(%reason, "A range split could not be signed");
                return Tended::Idle;
            }
        };
        let publisher = self.snapshot.root.publisher;
        match submit(context, publisher, self.local, signed).await {
            Ok(()) => {
                info!(set_id = %self.set_id(), range_id = %range.range_id, %split_key, "Split a distributed bucket range");
                Tended::Idle
            }
            Err(refusal) => {
                warn!(set_id = %self.set_id(), range_id = %range.range_id, %refusal, "The publisher did not take a range split");
                Tended::Idle
            }
        }
    }

    /// The keys of the range's sample, in key order; `None` when it cannot be read.
    async fn sample(&self, context: &DriverContext) -> Option<Vec<String>> {
        let iter = StorageEffect::Iter {
            key_space: KEY_SAMPLE_KEYSPACE.to_string(),
            prefix: Some(sample_prefix(self.set_id(), self.range_id).into()),
            start: None,
            limit: usize::from(SAMPLE_SIZE) * 2,
            txn_id: None,
        };
        let Event::Storage(StorageEvent::IterResult { values, .. }) =
            context.storage_handle.send_storage_effect(iter).await
        else {
            return None;
        };
        let mut keys: Vec<String> = values
            .iter()
            .filter_map(|(key, _)| SampleEntry::decode(key).map(|entry| entry.key))
            .collect();
        keys.sort();
        Some(keys)
    }
}

/// A split key near the sample's median at a folder boundary: the deepest folder of the
/// median key that leaves sampled keys on both sides inside the range, else the median.
pub(crate) fn split_point(keys: &[String], range: &KeyRange) -> Option<String> {
    let median = keys.get(keys.len() / 2)?;
    let folders = median
        .match_indices('/')
        .map(|(index, _)| &median[..=index]);
    let candidates: Vec<&str> = folders.rev().chain([median.as_str()]).collect();
    candidates
        .into_iter()
        .find(|candidate| {
            range.lower.as_str() < *candidate
                && range
                    .upper
                    .as_deref()
                    .is_none_or(|upper| *candidate < upper)
                && keys.iter().any(|key| key.as_str() < *candidate)
                && keys.iter().any(|key| key.as_str() >= *candidate)
        })
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::split_point;
    use aruna_core::structs::storage::distribution::KeyRange;
    use ulid::Ulid;

    fn range(lower: &str, upper: Option<&str>) -> KeyRange {
        KeyRange {
            range_id: Ulid::from_bytes([1; 16]),
            lower: lower.to_string(),
            upper: upper.map(str::to_string),
            lineage: None,
            coordinator: Ulid::from_bytes([2; 16]),
            epoch: 1,
            replicas: Vec::new(),
        }
    }

    fn keys(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn splits_at_folders() {
        let sample = keys(&["a/1", "b/x/1", "b/x/2", "c/1", "d/1"]);
        assert_eq!(
            split_point(&sample, &range("", None)).as_deref(),
            Some("b/x/")
        );
        let inside = keys(&["b/x/1", "b/x/2", "b/x/3"]);
        assert_eq!(
            split_point(&inside, &range("b/x/", None)).as_deref(),
            Some("b/x/2"),
            "a folder at the lower bound leaves the left child empty"
        );
    }

    #[test]
    fn split_needs_both_sides() {
        assert_eq!(split_point(&keys(&["a"]), &range("", None)), None);
        assert_eq!(split_point(&[], &range("", None)), None);
        let sample = keys(&["m/1", "m/2", "n/1"]);
        assert_eq!(split_point(&sample, &range("", Some("m/2"))), None);
    }
}
