//! Reads the range accounts this member holds and the counter rows they charge, from one read
//! snapshot, so a generation switch never pairs an account with another generation's row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, CounterOverflow, RangeAccount, RangeCounters, account_key, generation_key, pending_key,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChargeState {
    Init,
    Start,
    Accounts,
    Counters,
    Abort,
    Finish,
}

/// Reads the accounts of `held` ranges of `set_id` and the counter rows they charge in one read
/// transaction, which it aborts again once both reads answered.
#[derive(Debug, PartialEq)]
pub(super) struct ChargedRangesOperation {
    set_id: Ulid,
    held: Vec<Ulid>,
    state: ChargeState,
    txn_id: Option<TxnId>,
    accounts: Vec<(Ulid, RangeAccount)>,
    keys: Vec<Vec<u8>>,
    output: Option<Result<ChargedRanges, UsageError>>,
}

impl ChargedRangesOperation {
    pub(super) fn new(set_id: Ulid, held: Vec<Ulid>) -> Self {
        Self {
            set_id,
            held,
            state: ChargeState::Init,
            txn_id: None,
            accounts: Vec::new(),
            keys: Vec::new(),
            output: None,
        }
    }

    fn batch_read(&mut self, state: ChargeState, reads: Vec<(String, Key)>) -> Effects {
        self.state = state;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    /// Ends with `output`, aborting the read transaction first when one is open.
    fn finish(&mut self, output: Result<ChargedRanges, UsageError>) -> Effects {
        self.output = Some(output);
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = ChargeState::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.state = ChargeState::Finish;
                smallvec![]
            }
        }
    }

    fn read_accounts(&mut self, values: Vec<Option<Value>>) -> Result<Effects, UsageError> {
        for (range_id, value) in self.held.iter().zip(values) {
            if let Some(value) = value {
                self.accounts
                    .push((*range_id, RangeAccount::from_bytes(&value)?));
            }
        }
        let keys: BTreeSet<Vec<u8>> = charged_rows(self.set_id, &self.accounts)
            .into_iter()
            .collect();
        self.keys = keys.into_iter().collect();
        let reads = (self.keys.iter())
            .map(|key| (RANGE_COUNTER_KEYSPACE.to_string(), key.clone().into()))
            .collect();
        Ok(self.batch_read(ChargeState::Counters, reads))
    }

    fn read_counters(&mut self, values: Vec<Option<Value>>) -> Result<ChargedRanges, UsageError> {
        let mut counters = BTreeMap::new();
        for (key, value) in std::mem::take(&mut self.keys).into_iter().zip(values) {
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

    fn start(&mut self) -> Effects {
        self.state = ChargeState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (ChargeState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                let reads = (self.held.iter())
                    .map(|range| {
                        let key = account_key(self.set_id, *range).into();
                        (RANGE_ACCOUNT_KEYSPACE.to_string(), key)
                    })
                    .collect();
                self.batch_read(ChargeState::Accounts, reads)
            }
            (ChargeState::Accounts, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let values = values.into_iter().map(|(_, value)| value).collect();
                match self.read_accounts(values) {
                    Ok(effects) => effects,
                    Err(error) => self.finish(Err(error)),
                }
            }
            (ChargeState::Counters, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let values = values.into_iter().map(|(_, value)| value).collect();
                let output = self.read_counters(values);
                self.finish(output)
            }
            // The read transaction holds nothing to keep, so a failed abort changes nothing.
            (ChargeState::Abort, _) => {
                self.state = ChargeState::Finish;
                smallvec![]
            }
            (
                ChargeState::Start | ChargeState::Accounts | ChargeState::Counters,
                Event::Storage(StorageEvent::Error { error }),
            ) => self.finish(Err(error.into())),
            (_, event) => {
                let error = unexpected(&event);
                self.finish(Err(error))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == ChargeState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let unfinished = StorageError::ReadError("the charged ranges were not read".to_string());
        self.output.unwrap_or(Err(unfinished.into()))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    const TXN: TxnId = Ulid::from_bytes([9; 16]);

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

    fn batch(reads: Vec<(&str, Vec<u8>)>) -> Effect {
        let reads = reads.into_iter();
        Effect::Storage(StorageEffect::BatchRead {
            reads: reads
                .map(|(space, key)| (space.to_string(), key.into()))
                .collect(),
            txn_id: Some(TXN),
        })
    }

    fn answer(values: Vec<Option<Vec<u8>>>) -> Event {
        let values = values.into_iter();
        Event::Storage(StorageEvent::BatchReadResult {
            values: values
                .map(|value| (Vec::new().into(), value.map(Into::into)))
                .collect(),
        })
    }

    fn started() -> Event {
        Event::Storage(StorageEvent::TransactionStarted { txn_id: TXN })
    }

    fn abort() -> Effect {
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN })
    }

    // The held accounts and their counter rows are read in one transaction, then it is aborted.
    #[test]
    fn reads_held_rows() {
        let counters = RangeCounters::default();
        let row = generation_key(set(), range(), 3);
        let mut charged = ChargedRangesOperation::new(set(), vec![range(), Ulid::nil()]);
        let accounts = batch(vec![
            (RANGE_ACCOUNT_KEYSPACE, account_key(set(), range())),
            (RANGE_ACCOUNT_KEYSPACE, account_key(set(), Ulid::nil())),
        ]);
        check_trace(
            &mut charged,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: true,
            })],
            vec![
                (started(), vec![accounts]),
                (
                    answer(vec![Some(account().to_bytes().unwrap()), None]),
                    vec![batch(vec![(RANGE_COUNTER_KEYSPACE, row.clone())])],
                ),
                (answer(vec![None]), vec![abort()]),
                (
                    Event::Storage(StorageEvent::TransactionAborted { txn_id: TXN }),
                    Vec::new(),
                ),
            ],
        );
        let rows = BTreeMap::from([(row, counters)]);
        assert_eq!(charged.finalize(), Ok((vec![(range(), account())], rows)));
    }

    // A failed read aborts the transaction and fails; other events are refused.
    #[test]
    fn failed_read_aborts() {
        let mut charged = ChargedRangesOperation::new(set(), vec![range()]);
        charged.start();
        charged.step(started());
        check_abort(
            &mut ChargedRangesOperation {
                txn_id: Some(TXN),
                ..ChargedRangesOperation::new(set(), Vec::new())
            },
            &[abort()],
        );
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert_eq!(charged.step(failed).into_vec(), vec![abort()]);
        let aborted = Event::Storage(StorageEvent::TransactionAborted { txn_id: TXN });
        assert!(charged.step(aborted).is_empty());
        assert_eq!(charged.finalize(), Err(StorageError::Timeout.into()));

        let fresh = || ChargedRangesOperation::new(set(), vec![range()]);
        let all = sample_events().len();
        assert_eq!(rejects_events(fresh, sample_events, |_| false).len(), all);
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
    }
}
