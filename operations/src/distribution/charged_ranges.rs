//! Reads the range accounts this member holds and the counter rows they charge, from one read
//! snapshot, so a generation switch never pairs an account with another generation's row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, CounterOverflow, RangeAccount, RangeCounters, account_key, generation_key, pending_key,
};
use aruna_core::types::Value;
use thiserror::Error;
use ulid::Ulid;

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

pub(super) fn unexpected(event: &Event) -> UsageError {
    StorageError::ReadError(format!("unexpected answer {}", event.label())).into()
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

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(charged.transaction());
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
}
