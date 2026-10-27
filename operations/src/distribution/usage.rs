//! Sums a distributed bucket's maintained range counters on this member: one account and at
//! most two counter rows per held range, never a walk over its objects.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, CounterOverflow, RangeAccount, RangeCounters, account_key, generation_key, pending_key,
};
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionSnapshot};
use aruna_core::structs::storage::usage::UsageCounters;
use aruna_core::types::{GroupId, Key, TxnId, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::node::usage_stats::usage_complete;
use crate::replication::members::bound_record;
use crate::storage_read::scan_all;

#[derive(Debug, Error, PartialEq)]
pub enum UsageError {
    #[error("{0}")]
    Record(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Overflow(#[from] CounterOverflow),
}

/// The counted part of a distributed bucket on this member.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RangeUsage {
    pub counters: RangeCounters,
    /// Ranges of the set.
    pub ranges: usize,
    /// Ranges whose records this member holds and whose counters were summed.
    pub counted: usize,
    /// Every range is held here and its counters are exact (active, not rebuilding, and no
    /// node-wide usage rebuild is running).
    pub complete: bool,
}

/// The counter rows charged for the given accounts: an active range's generation row, a split
/// child's own pending row, and its parent's frozen row once per parent.
pub fn charged_rows(set_id: Ulid, accounts: &[(Ulid, RangeAccount)]) -> Vec<Vec<u8>> {
    let mut parents = BTreeSet::new();
    let mut rows = Vec::new();
    for (range_id, account) in accounts {
        match account.charge {
            Charge::Active(generation) => rows.push(generation_key(set_id, *range_id, generation)),
            Charge::Child { parent, frozen, .. } => {
                rows.push(pending_key(set_id, *range_id));
                if parents.insert(parent) {
                    rows.push(generation_key(set_id, parent, frozen));
                }
            }
            // A retired parent stays charged through its children.
            Charge::Retired { .. } => {}
        }
    }
    rows
}

/// Adds the read counter rows; a missing row counts as zero.
pub fn sum_rows(values: &[Option<Value>]) -> Result<RangeCounters, UsageError> {
    let mut total = RangeCounters::default();
    for value in values.iter().flatten() {
        total = total.merge(RangeCounters::from_bytes(value)?)?;
    }
    Ok(total)
}

async fn batch_read(
    context: &DriverContext,
    reads: Vec<(String, Key)>,
    txn_id: TxnId,
) -> Result<Vec<Option<Value>>, UsageError> {
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: Some(txn_id),
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(StorageError::ReadError(format!("unexpected answer {other:?}")).into()),
    }
}

/// The charged counter rows of one range this member holds, so a member that does not hold the
/// range can add them without counting a shared parent row twice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeldRange {
    pub range_id: Ulid,
    /// The range's counters are exact: active, not rebuilding, and no node-wide usage rebuild.
    pub complete: bool,
    /// This member coordinates the range, so its counters include every acknowledged write; a
    /// copy's counters may lag behind its coordinator's.
    pub coordinated: bool,
    /// Counter row keys with their values; a split child names its parent's frozen row.
    pub rows: Vec<(Vec<u8>, RangeCounters)>,
}

/// The local distributed bucket's binding, record and the charged rows of every range this
/// member holds; `None` for an ordinary bucket or a member whose record has not arrived.
pub async fn held_ranges(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<(DistributionBinding, DistributionSnapshot, Vec<HeldRange>)>, UsageError> {
    let Some((binding, snapshot)) = bound_record(context, bucket)
        .await
        .map_err(UsageError::Record)?
    else {
        return Ok(None);
    };
    // Accounts and the counter rows they name are read from one snapshot, so a generation
    // switch in between never pairs an account with another generation's row.
    let txn_id = match (context.storage_handle)
        .send_storage_effect(StorageEffect::StartTransaction { read: true })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(StorageError::ReadError(format!("unexpected answer {other:?}")).into());
        }
    };
    let rows = charged_ranges(context, &binding, &snapshot, txn_id).await;
    (context.storage_handle)
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
    let (accounts, values) = rows?;
    let set_id = binding.set_id;
    // A node-wide usage rebuild also leaves range counters unconfirmed.
    let confirmed = usage_complete(context).await?;
    let ranges = (accounts.iter())
        .map(|(range_id, account)| HeldRange {
            range_id: *range_id,
            complete: confirmed && account.is_complete(),
            coordinated: (snapshot.config.ranges.iter())
                .any(|range| range.range_id == *range_id && range.coordinator == binding.member_id),
            rows: charged_rows(set_id, &[(*range_id, account.clone())])
                .into_iter()
                .map(|key| {
                    let counters = values.get(&key).copied().unwrap_or_default();
                    (key, counters)
                })
                .collect(),
        })
        .collect();
    Ok(Some((binding, snapshot, ranges)))
}

/// The accounts of the ranges this member holds and the counter rows they charge, read in
/// `txn_id`.
async fn charged_ranges(
    context: &DriverContext,
    binding: &DistributionBinding,
    snapshot: &DistributionSnapshot,
    txn_id: TxnId,
) -> Result<(Vec<(Ulid, RangeAccount)>, BTreeMap<Vec<u8>, RangeCounters>), UsageError> {
    let set_id = binding.set_id;
    let held: Vec<Ulid> = snapshot
        .held_ranges(binding.member_id)
        .iter()
        .map(|range| range.range_id)
        .collect();
    let reads = held
        .iter()
        .map(|range| {
            (
                RANGE_ACCOUNT_KEYSPACE.to_string(),
                account_key(set_id, *range).into(),
            )
        })
        .collect();
    let mut accounts = Vec::with_capacity(held.len());
    for (range_id, value) in held.iter().zip(batch_read(context, reads, txn_id).await?) {
        if let Some(value) = value {
            accounts.push((*range_id, RangeAccount::from_bytes(&value)?));
        }
    }
    let keys: BTreeSet<Vec<u8>> = charged_rows(set_id, &accounts).into_iter().collect();
    let reads = (keys.iter())
        .map(|key| (RANGE_COUNTER_KEYSPACE.to_string(), key.clone().into()))
        .collect();
    let mut values = BTreeMap::new();
    for (key, value) in keys
        .into_iter()
        .zip(batch_read(context, reads, txn_id).await?)
    {
        values.insert(key, sum_rows(&[value])?);
    }
    Ok((accounts, values))
}

/// Adds the rows of `ranges`, each distinct row once.
pub fn sum_held<'a>(
    ranges: impl IntoIterator<Item = &'a HeldRange>,
) -> Result<RangeCounters, UsageError> {
    let mut rows = BTreeMap::new();
    for range in ranges {
        for (key, counters) in &range.rows {
            rows.entry(key.clone()).or_insert(*counters);
        }
    }
    let mut total = RangeCounters::default();
    for counters in rows.into_values() {
        total = total.merge(counters)?;
    }
    Ok(total)
}

/// What this member holds of `bucket`'s set as copies: every range with an account here, one
/// being purged included, less the ranges it coordinates. Zero for an ordinary bucket.
pub async fn replica_share(
    context: &DriverContext,
    bucket: &str,
) -> Result<RangeCounters, UsageError> {
    let Some((binding, snapshot)) = bound_record(context, bucket)
        .await
        .map_err(UsageError::Record)?
    else {
        return Ok(RangeCounters::default());
    };
    let txn_id = match (context.storage_handle)
        .send_storage_effect(StorageEffect::StartTransaction { read: true })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(StorageError::ReadError(format!("unexpected answer {other:?}")).into());
        }
    };
    let share = copied_rows(context, (&binding, &snapshot), txn_id).await;
    (context.storage_handle)
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
    share
}

/// The charged rows of every local account of the set less those of the coordinated ranges, so
/// a split parent's row shared with a coordinated child is not subtracted.
async fn copied_rows(
    context: &DriverContext,
    (binding, snapshot): (&DistributionBinding, &DistributionSnapshot),
    txn_id: TxnId,
) -> Result<RangeCounters, UsageError> {
    let set_id = binding.set_id;
    let accounts = local_accounts(context, set_id, txn_id).await?;
    let coordinated = |range_id: &Ulid| {
        (snapshot.config.ranges.iter())
            .any(|range| range.range_id == *range_id && range.coordinator == binding.member_id)
    };
    let own: Vec<_> = (accounts.iter())
        .filter(|(range_id, _)| coordinated(range_id))
        .cloned()
        .collect();
    let (held, own) = (charged_rows(set_id, &accounts), charged_rows(set_id, &own));
    let keys: BTreeSet<Vec<u8>> = held.iter().chain(&own).cloned().collect();
    let reads = (keys.iter())
        .map(|key| (RANGE_COUNTER_KEYSPACE.to_string(), key.clone().into()))
        .collect();
    let stored: BTreeMap<_, _> = keys
        .into_iter()
        .zip(batch_read(context, reads, txn_id).await?)
        .collect();
    let sum = |rows: &[Vec<u8>]| {
        let values: Vec<_> = rows.iter().map(|key| stored[key].clone()).collect();
        sum_rows(&values)
    };
    Ok(sum(&held)?.merge(sum(&own)?.negate()?)?)
}

/// Every range account this node keeps for the set, read in `txn_id`.
async fn local_accounts(
    context: &DriverContext,
    set_id: Ulid,
    txn_id: TxnId,
) -> Result<Vec<(Ulid, RangeAccount)>, UsageError> {
    let mut accounts = Vec::new();
    let mut start = None;
    loop {
        let iter = StorageEffect::Iter {
            key_space: RANGE_ACCOUNT_KEYSPACE.to_string(),
            prefix: Some(set_id.to_bytes().to_vec().into()),
            start: start.map(IterStart::After),
            limit: 1_000,
            txn_id: Some(txn_id),
        };
        let (values, next) = match context.storage_handle.send_storage_effect(iter).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(StorageError::ReadError(format!("unexpected answer {other:?}")).into());
            }
        };
        for (key, value) in values {
            let Some(range_id) = key.get(16..).and_then(|id| <[u8; 16]>::try_from(id).ok()) else {
                continue;
            };
            accounts.push((
                Ulid::from_bytes(range_id),
                RangeAccount::from_bytes(&value)?,
            ));
        }
        match next {
            Some(next) => start = Some(next),
            None => return Ok(accounts),
        }
    }
}

/// Each group's replica share on this node, summed over its local distributed buckets. A bucket
/// whose share cannot be read is left out, so its copies count as owned until the next run.
pub async fn replica_shares(
    context: &DriverContext,
) -> Result<BTreeMap<GroupId, UsageCounters>, UsageError> {
    let buckets = scan_all(&context.storage_handle, S3_BUCKET_KEYSPACE, None)
        .await
        .map_err(UsageError::Record)?;
    let mut shares: BTreeMap<GroupId, UsageCounters> = BTreeMap::new();
    for (key, value) in buckets {
        let info = BucketInfo::from_bytes(value.as_ref())?;
        if info.distribution.is_none() {
            continue;
        }
        let bucket = String::from_utf8_lossy(key.as_ref()).into_owned();
        let share = match replica_share(context, &bucket).await {
            Ok(share) => share,
            Err(error) => {
                warn!(bucket = %bucket, error = %error, "replica share unavailable");
                continue;
            }
        };
        let count = |value: i128| u64::try_from(value.max(0)).unwrap_or(u64::MAX);
        let counters = UsageCounters {
            objects: count(share.objects),
            logical_bytes: count(share.logical_bytes),
            referenced_bytes: count(share.referenced_bytes),
            ..UsageCounters::default()
        };
        let total = shares.entry(info.group_id).or_default();
        total
            .add(&counters)
            .map_err(|_| CounterOverflow("replicas"))?;
    }
    Ok(shares)
}

/// The usage of a local distributed bucket from the range counters this member holds; `None`
/// for an ordinary bucket or a member whose record has not arrived.
pub async fn range_usage(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<RangeUsage>, UsageError> {
    let Some((_, snapshot, held)) = held_ranges(context, bucket).await? else {
        return Ok(None);
    };
    let ranges = snapshot.config.ranges.len();
    Ok(Some(RangeUsage {
        counters: sum_held(&held)?,
        ranges,
        counted: held.len(),
        complete: held.iter().all(|range| range.complete) && held.len() == ranges,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(charge: Charge) -> RangeAccount {
        RangeAccount {
            charge,
            ..RangeAccount::fresh(String::new(), None)
        }
    }

    #[test]
    fn charges_parent_once() {
        let set = Ulid::from_bytes([1; 16]);
        let (parent, left, right, other) = (
            Ulid::from_bytes([2; 16]),
            Ulid::from_bytes([3; 16]),
            Ulid::from_bytes([4; 16]),
            Ulid::from_bytes([5; 16]),
        );
        let child = |sibling| {
            account(Charge::Child {
                parent,
                sibling,
                frozen: 7,
            })
        };
        let accounts = [
            (left, child(right)),
            (right, child(left)),
            (other, account(Charge::Active(2))),
        ];
        let rows = charged_rows(set, &accounts);
        assert_eq!(
            rows,
            vec![
                pending_key(set, left),
                generation_key(set, parent, 7),
                pending_key(set, right),
                generation_key(set, other, 2),
            ]
        );
    }

    #[test]
    fn rows_count_once() {
        let counters = |objects| RangeCounters {
            objects,
            ..RangeCounters::default()
        };
        let parent = (b"parent".to_vec(), counters(10));
        let child = |id: u8, pending| HeldRange {
            range_id: Ulid::from_bytes([id; 16]),
            complete: false,
            coordinated: true,
            rows: vec![(vec![id], counters(pending)), parent.clone()],
        };
        let total = sum_held(&[child(1, 2), child(2, -1)]).unwrap();
        assert_eq!(total.objects, 11, "the shared parent row is added once");
    }

    #[tokio::test]
    async fn reads_held_counters() {
        // The charged rows of the held ranges come with their accounts, read in one snapshot.
        use crate::distribution::test_record::{binding, record, store_record};
        use crate::distribution::test_store::{context, open};
        use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
        use aruna_core::structs::storage::blob::BucketInfo;
        let (_dir, handle) = open();
        let context = context(&handle);
        let (snapshot, bytes) = record(true);
        let set_id = snapshot.set_id();
        store_record(&handle, set_id, bytes).await;
        let info = BucketInfo {
            group_id: snapshot.config.owner_group,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: Some(binding(&snapshot, 1, 5)),
        };
        let range_id = snapshot.config.ranges[0].range_id;
        let counted = RangeCounters {
            objects: 4,
            ..RangeCounters::default()
        };
        let writes = vec![
            (
                S3_BUCKET_KEYSPACE.to_string(),
                b"spire".to_vec().into(),
                info.to_bytes().unwrap().into(),
            ),
            (
                RANGE_ACCOUNT_KEYSPACE.to_string(),
                account_key(set_id, range_id).into(),
                account(Charge::Active(3)).to_bytes().unwrap().into(),
            ),
            (
                RANGE_COUNTER_KEYSPACE.to_string(),
                generation_key(set_id, range_id, 3).into(),
                counted.to_bytes().unwrap().into(),
            ),
        ];
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        handle.send_storage_effect(write).await;
        let (_, _, held) = held_ranges(&context, "spire").await.unwrap().unwrap();
        assert_eq!(held.len(), 1, "member 1 holds the lower shard only");
        assert_eq!(sum_held(&held).unwrap().objects, 4);
        assert!(held[0].complete);
        assert!(held[0].coordinated, "member 1 coordinates the lower shard");
    }

    /// Stores the bound bucket "spire" of member 1, its record and these accounts and counters.
    async fn spire(
        handle: &aruna_storage::storage::StorageHandle,
        accounts: Vec<(Ulid, Charge)>,
        counters: Vec<(Vec<u8>, RangeCounters)>,
    ) -> DistributionSnapshot {
        use crate::distribution::test_record::{binding, replicated, store_record};
        use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
        let (snapshot, bytes) = replicated(true, 1);
        let set_id = snapshot.set_id();
        store_record(handle, set_id, bytes).await;
        let info = BucketInfo {
            group_id: snapshot.config.owner_group,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: Some(binding(&snapshot, 1, 5)),
        };
        let row = |space: &str, key: Vec<u8>, value: Vec<u8>| {
            (space.to_string(), key.into(), value.into())
        };
        let mut writes = vec![row(
            S3_BUCKET_KEYSPACE,
            b"spire".to_vec(),
            info.to_bytes().unwrap(),
        )];
        for (range_id, charge) in accounts {
            let value = account(charge).to_bytes().unwrap();
            writes.push(row(
                RANGE_ACCOUNT_KEYSPACE,
                account_key(set_id, range_id),
                value,
            ));
        }
        for (key, value) in counters {
            writes.push(row(RANGE_COUNTER_KEYSPACE, key, value.to_bytes().unwrap()));
        }
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        handle.send_storage_effect(write).await;
        snapshot
    }

    #[tokio::test]
    async fn shares_copied_ranges() {
        // Member 1 coordinates the lower shard and holds the upper one and a range it purges
        // as copies; only the copies form its replica share.
        use crate::distribution::test_store::{context, open};
        let (_dir, handle) = open();
        let context = context(&handle);
        let (snapshot, _) = crate::distribution::test_record::replicated(true, 1);
        let set_id = snapshot.set_id();
        let (lower, upper) = (
            snapshot.config.ranges[0].range_id,
            snapshot.config.ranges[1].range_id,
        );
        let purged = Ulid::from_bytes([77; 16]);
        let counted = |objects, logical_bytes, referenced_bytes| RangeCounters {
            objects,
            logical_bytes,
            referenced_bytes,
            ..RangeCounters::default()
        };
        let accounts = vec![
            (lower, Charge::Active(3)),
            (upper, Charge::Active(2)),
            (purged, Charge::Active(1)),
        ];
        let counters = vec![
            (generation_key(set_id, lower, 3), counted(4, 40, 0)),
            (generation_key(set_id, upper, 2), counted(1, 10, 7)),
            (generation_key(set_id, purged, 1), counted(2, 5, 0)),
        ];
        let snapshot = spire(&handle, accounts, counters).await;

        assert_eq!(
            replica_share(&context, "spire").await.unwrap(),
            counted(3, 15, 7)
        );
        let shares = replica_shares(&context).await.unwrap();
        let expected = UsageCounters {
            objects: 3,
            logical_bytes: 15,
            referenced_bytes: 7,
            ..UsageCounters::default()
        };
        assert_eq!(
            shares,
            BTreeMap::from([(snapshot.config.owner_group, expected)])
        );
    }

    #[tokio::test]
    async fn shared_parent_stays() {
        // Split children share their parent's frozen row; a coordinated child keeps it.
        use crate::distribution::test_store::{context, open};
        let (_dir, handle) = open();
        let context = context(&handle);
        let (snapshot, _) = crate::distribution::test_record::replicated(true, 1);
        let set_id = snapshot.set_id();
        let (lower, upper) = (
            snapshot.config.ranges[0].range_id,
            snapshot.config.ranges[1].range_id,
        );
        let parent = Ulid::from_bytes([78; 16]);
        let child = |sibling| Charge::Child {
            parent,
            sibling,
            frozen: 7,
        };
        let objects = |objects| RangeCounters {
            objects,
            ..RangeCounters::default()
        };
        let accounts = vec![(lower, child(upper)), (upper, child(lower))];
        let counters = vec![
            (generation_key(set_id, parent, 7), objects(10)),
            (pending_key(set_id, lower), objects(2)),
            (pending_key(set_id, upper), objects(-1)),
        ];
        spire(&handle, accounts, counters).await;

        assert_eq!(replica_share(&context, "spire").await.unwrap(), objects(-1));
        let shares = replica_shares(&context).await.unwrap();
        assert!(
            shares.values().all(|share| share.objects == 0),
            "a negative share reads as zero"
        );
    }

    #[test]
    fn sums_signed_rows() {
        let row = |objects| {
            let counters = RangeCounters {
                objects,
                ..RangeCounters::default()
            };
            Some(Value::from(counters.to_bytes().unwrap()))
        };
        let total = sum_rows(&[row(5), None, row(-2)]).unwrap();
        assert_eq!(total.objects, 3, "a pending row may subtract");
    }
}
