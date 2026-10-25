//! Sums a distributed bucket's maintained range counters on this member: one account and at
//! most two counter rows per held range, never a walk over its objects.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::effects::StorageEffect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE};
use aruna_core::structs::storage::distribution::accounting::{
    Charge, CounterOverflow, RangeAccount, RangeCounters, account_key, generation_key, pending_key,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::node::usage_stats::usage_complete;
use crate::replication::members::bound_record;

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
) -> Result<Vec<Option<Value>>, UsageError> {
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(StorageError::ReadError(format!("unexpected answer {other:?}")).into()),
    }
}

/// The usage of a local distributed bucket from its range counters; `None` for an ordinary
/// bucket or a member whose record has not arrived.
pub async fn range_usage(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<RangeUsage>, UsageError> {
    let Some((binding, snapshot)) = bound_record(context, bucket)
        .await
        .map_err(UsageError::Record)?
    else {
        return Ok(None);
    };
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
    let mut exact = true;
    for (range_id, value) in held.iter().zip(batch_read(context, reads).await?) {
        let Some(value) = value else {
            exact = false;
            continue;
        };
        let account = RangeAccount::from_bytes(&value)?;
        exact &= account.is_complete();
        accounts.push((*range_id, account));
    }
    let reads = charged_rows(set_id, &accounts)
        .into_iter()
        .map(|key| (RANGE_COUNTER_KEYSPACE.to_string(), key.into()))
        .collect();
    let counters = sum_rows(&batch_read(context, reads).await?)?;
    let ranges = snapshot.config.ranges.len();
    // A node-wide usage rebuild also leaves range counters unconfirmed.
    exact &= usage_complete(context).await?;
    Ok(Some(RangeUsage {
        counters,
        ranges,
        counted: accounts.len(),
        complete: exact && accounts.len() == ranges,
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
