//! Reads the range accounts this member holds and the counter rows they charge, from one read
//! snapshot, so a generation switch never pairs an account with another generation's row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::storage::distribution::accounting::{
    Charge, CounterOverflow, RangeAccount, RangeCounters, account_key, generation_key, pending_key,
};
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionSnapshot};
use aruna_core::types::Value;
use thiserror::Error;
use ulid::Ulid;

use crate::replication::members::{BoundRecord, BoundRecordOperation, RecordError};
use crate::s3::usage::UsageCompleteOperation;

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

impl From<IoError> for UsageError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
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
pub(super) fn sum_rows(values: &[Option<Value>]) -> Result<RangeCounters, UsageError> {
    let mut total = RangeCounters::default();
    for value in values.iter().flatten() {
        total = total.merge(RangeCounters::from_bytes(value)?)?;
    }
    Ok(total)
}

/// The accounts of the held ranges, and the counters of every row they charge.
pub(super) type ChargedRanges = (Vec<(Ulid, RangeAccount)>, BTreeMap<Vec<u8>, RangeCounters>);

/// Reads the accounts of `held` ranges of `set_id` and the counter rows they charge in one
/// transaction, so both reads see the same snapshot.
#[derive(Debug, PartialEq)]
pub(super) struct ChargedRangesOperation {
    set_id: Ulid,
    held: Vec<Ulid>,
    accounts: Vec<(Ulid, RangeAccount)>,
    /// The counter rows read, once the accounts are read.
    keys: Option<Vec<Vec<u8>>>,
}

impl ChargedRangesOperation {
    pub(super) fn new(set_id: Ulid, held: Vec<Ulid>) -> Self {
        Self {
            set_id,
            held,
            accounts: Vec::new(),
            keys: None,
        }
    }

    fn read_accounts(&mut self, values: Vec<Option<Value>>) -> Result<IoCall, UsageError> {
        for (range_id, value) in self.held.iter().zip(values) {
            if let Some(value) = value {
                self.accounts
                    .push((*range_id, RangeAccount::from_bytes(&value)?));
            }
        }
        let keys: BTreeSet<Vec<u8>> = charged_rows(self.set_id, &self.accounts)
            .into_iter()
            .collect();
        let keys: Vec<Vec<u8>> = keys.into_iter().collect();
        let reads = (keys.iter())
            .map(|key| (RANGE_COUNTER_KEYSPACE.to_string(), key.clone().into()))
            .collect();
        self.keys = Some(keys);
        Ok(IoCall::Read(reads))
    }

    fn read_counters(
        &mut self,
        keys: Vec<Vec<u8>>,
        values: Vec<Option<Value>>,
    ) -> Result<ChargedRanges, UsageError> {
        let mut counters = BTreeMap::new();
        for (key, value) in keys.into_iter().zip(values) {
            counters.insert(key, sum_rows(&[value])?);
        }
        Ok((std::mem::take(&mut self.accounts), counters))
    }
}

impl Operation for ChargedRangesOperation {
    type Output = ChargedRanges;
    type Error = UsageError;

    fn first(&mut self) -> Result<Next<ChargedRanges>, UsageError> {
        let reads = (self.held.iter())
            .map(|range| {
                let key = account_key(self.set_id, *range).into();
                (RANGE_ACCOUNT_KEYSPACE.to_string(), key)
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ChargedRanges>, UsageError> {
        let values = result.values()?;
        match self.keys.take() {
            None => Ok(Next::Call(self.read_accounts(values)?)),
            Some(keys) => Ok(Next::Done(self.read_counters(keys, values)?)),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
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

/// The local bucket's binding and record and the charged rows of every range it holds.
pub type HeldRanges = (DistributionBinding, DistributionSnapshot, Vec<HeldRange>);

/// What the pending child of [`HeldRangesOperation`] answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeldStep {
    Record,
    Charged,
    Complete,
}

/// Reads the local distributed bucket's binding, record and the charged rows of every range
/// this member holds; `None` for an ordinary bucket or a member whose record has not arrived.
#[derive(Debug, PartialEq)]
pub struct HeldRangesOperation {
    bucket: String,
    step: HeldStep,
    bound: Option<BoundRecord>,
    charged: Option<ChargedRanges>,
}

impl HeldRangesOperation {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            step: HeldStep::Record,
            bound: None,
            charged: None,
        }
    }

    fn held(&mut self, confirmed: bool) -> Result<Next<Option<HeldRanges>>, UsageError> {
        let (binding, snapshot) = self.bound.take().ok_or(not_read())?;
        let (accounts, values) = self.charged.take().ok_or(not_read())?;
        let set_id = binding.set_id;
        let ranges = (accounts.iter())
            .map(|(range_id, account)| HeldRange {
                range_id: *range_id,
                // A node-wide usage rebuild also leaves range counters unconfirmed.
                complete: confirmed && account.is_complete(),
                coordinated: (snapshot.config.ranges.iter()).any(|range| {
                    range.range_id == *range_id && range.coordinator == binding.member_id
                }),
                rows: charged_rows(set_id, &[(*range_id, account.clone())])
                    .into_iter()
                    .map(|key| {
                        let counters = values.get(&key).copied().unwrap_or_default();
                        (key, counters)
                    })
                    .collect(),
            })
            .collect();
        Ok(Next::Done(Some((binding, snapshot, ranges))))
    }
}

fn not_read() -> UsageError {
    UsageError::Record("the held ranges were not read".to_string())
}

impl Operation for HeldRangesOperation {
    type Output = Option<HeldRanges>;
    type Error = UsageError;

    fn first(&mut self) -> Result<Next<Option<HeldRanges>>, UsageError> {
        let record = BoundRecordOperation::new(&self.bucket);
        Ok(Next::Call(IoCall::child(record)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<HeldRanges>>, UsageError> {
        match self.step {
            HeldStep::Record => {
                let record = result.child::<Option<BoundRecord>, RecordError>()?;
                let record = record.map_err(|error| UsageError::Record(error.to_string()))?;
                let Some((binding, snapshot)) = record else {
                    return Ok(Next::Done(None));
                };
                let held = (snapshot.held_ranges(binding.member_id).iter())
                    .map(|range| range.range_id)
                    .collect();
                let charged = ChargedRangesOperation::new(binding.set_id, held);
                self.bound = Some((binding, snapshot));
                self.step = HeldStep::Charged;
                Ok(Next::Call(IoCall::child(charged)))
            }
            HeldStep::Charged => {
                self.charged = Some(result.child::<ChargedRanges, UsageError>()??);
                self.step = HeldStep::Complete;
                Ok(Next::Call(IoCall::child(UsageCompleteOperation::new())))
            }
            HeldStep::Complete => {
                let confirmed = result.child::<bool, StorageError>()??;
                self.held(confirmed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_record::{binding, record};

    fn set() -> Ulid {
        Ulid::from_bytes([1; 16])
    }

    fn range() -> Ulid {
        Ulid::from_bytes([2; 16])
    }

    fn account() -> RangeAccount {
        RangeAccount {
            charge: Charge::Active(3),
            ..RangeAccount::fresh(String::new(), None)
        }
    }

    // The held accounts and the counter rows they charge are read in the transaction.
    #[test]
    fn reads_held_rows() {
        let row = generation_key(set(), range(), 3);
        let mut charged = ChargedRangesOperation::new(set(), vec![range(), Ulid::nil()]);
        assert_eq!(
            charged.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        let accounts = IoCall::Read(vec![
            (
                RANGE_ACCOUNT_KEYSPACE.to_string(),
                account_key(set(), range()).into(),
            ),
            (
                RANGE_ACCOUNT_KEYSPACE.to_string(),
                account_key(set(), Ulid::nil()).into(),
            ),
        ]);
        assert_eq!(charged.first(), Ok(Next::Call(accounts)));
        let found = IoResult::Values(vec![Some(account().to_bytes().unwrap().into()), None]);
        let counters = IoCall::Read(vec![(
            RANGE_COUNTER_KEYSPACE.to_string(),
            row.clone().into(),
        )]);
        assert_eq!(charged.next(found), Ok(Next::Call(counters)));
        let rows = BTreeMap::from([(row, RangeCounters::default())]);
        let expected = (vec![(range(), account())], rows);
        assert_eq!(
            charged.next(IoResult::Values(vec![None])),
            Ok(Next::Done(expected))
        );
    }

    // An undecodable account fails, and so does a result that answers no read.
    #[test]
    fn failures_fail() {
        let mut charged = ChargedRangesOperation::new(set(), vec![range()]);
        charged.first().unwrap();
        let garbage = IoResult::Values(vec![Some(vec![0xFF].into())]);
        assert!(matches!(
            charged.next(garbage),
            Err(UsageError::Conversion(_))
        ));
        let mut charged = ChargedRangesOperation::new(set(), vec![range()]);
        charged.first().unwrap();
        assert!(charged.next(IoResult::Done).is_err());
        assert_eq!(
            UsageError::from(IoError::Conflict),
            StorageError::TransactionConflict.into()
        );
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    fn asks_child<T>(next: Result<Next<T>, UsageError>) -> bool {
        matches!(next, Ok(Next::Call(IoCall::Child(_))))
    }

    /// Runs the read of member 1's ranges to its end, charging its lower range.
    fn held_with(confirmed: bool) -> (Ulid, Vec<u8>, Option<HeldRanges>) {
        let (snapshot, _) = record(true);
        let range_id = snapshot.config.ranges[0].range_id;
        let row = generation_key(snapshot.set_id(), range_id, 3);
        let bound = (binding(&snapshot, 1, 5), snapshot);
        let counters = RangeCounters {
            objects: 2,
            ..RangeCounters::default()
        };
        let mut held = HeldRangesOperation::new("spire");
        assert!(asks_child(held.first()));
        let found = Ok::<_, RecordError>(Some(bound));
        assert!(asks_child(held.next(child(found))));
        let charged: ChargedRanges = (
            vec![(range_id, account())],
            BTreeMap::from([(row.clone(), counters)]),
        );
        assert!(asks_child(held.next(child(Ok::<_, UsageError>(charged)))));
        let Ok(Next::Done(output)) = held.next(child(Ok::<_, StorageError>(confirmed))) else {
            panic!("expected the held ranges");
        };
        (range_id, row, output)
    }

    // Each held range carries its charged rows, and whether this member coordinates it.
    #[test]
    fn held_range_rows() {
        let (range_id, row, output) = held_with(true);
        let (_, _, ranges) = output.unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].range_id, range_id);
        assert_eq!(ranges[0].complete, account().is_complete());
        assert!(ranges[0].coordinated);
        assert_eq!(ranges[0].rows[0].0, row);
        assert_eq!(ranges[0].rows[0].1.objects, 2);
    }

    // Unconfirmed node usage leaves every range incomplete.
    #[test]
    fn unconfirmed_usage_incomplete() {
        let (_, _, output) = held_with(false);
        assert!(!output.unwrap().2[0].complete);
    }

    // An ordinary bucket holds nothing; a record failure fails the read.
    #[test]
    fn held_needs_record() {
        let mut ordinary = HeldRangesOperation::new("spire");
        ordinary.first().unwrap();
        let absent = Ok::<Option<BoundRecord>, RecordError>(None);
        assert_eq!(ordinary.next(child(absent)), Ok(Next::Done(None)));
        let mut failed = HeldRangesOperation::new("spire");
        failed.first().unwrap();
        let error = Err::<Option<BoundRecord>, _>(RecordError::from(IoError::Unfinished));
        assert!(matches!(
            failed.next(child(error)),
            Err(UsageError::Record(_))
        ));
    }
}
