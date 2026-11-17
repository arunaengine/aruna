//! Reads each group's replica share on this node in the caller's read transaction: the charged
//! rows of every local range account of a bound bucket's set, less those of coordinated ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    DISTRIBUTION_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::accounting::{
    CounterOverflow, RangeAccount, RangeCounters,
};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, DistributionView,
};
use aruna_core::structs::storage::usage::UsageCounters;
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use ulid::Ulid;

use super::charged_ranges::{UsageError, charged_rows, sum_rows, unexpected};

/// Rows one scan page reads.
const SCAN_PAGE: usize = 1_000;

/// One group's replica share on this node, and the record number of each set it covers.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GroupShare {
    pub replicas: UsageCounters,
    pub revisions: BTreeMap<Ulid, u64>,
}

/// A bound bucket's share and the set and record number it was computed under.
pub type BindingShare = (RangeCounters, Option<(Ulid, u64)>);

fn scan(key_space: &str, prefix: Option<Key>, after: Option<Key>, txn_id: TxnId) -> Effects {
    smallvec![Effect::Storage(StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix,
        start: after.map(IterStart::After),
        limit: SCAN_PAGE,
        txn_id: Some(txn_id),
    })]
}

fn batch_read(reads: Vec<(String, Key)>, txn_id: TxnId) -> Effects {
    smallvec![Effect::Storage(StorageEffect::BatchRead {
        reads,
        txn_id: Some(txn_id),
    })]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShareState {
    Init,
    Set,
    Accounts,
    Counters,
    Finish,
}

/// Reads one bound bucket's share in `txn_id`: zero without a set record here.
#[derive(Debug, PartialEq)]
pub struct BindingShareOperation {
    binding: DistributionBinding,
    txn_id: TxnId,
    state: ShareState,
    snapshot: Option<DistributionSnapshot>,
    accounts: Vec<(Ulid, RangeAccount)>,
    keys: Vec<Vec<u8>>,
    output: Option<Result<BindingShare, UsageError>>,
}

impl BindingShareOperation {
    pub fn new(binding: DistributionBinding, txn_id: TxnId) -> Self {
        Self {
            binding,
            txn_id,
            state: ShareState::Init,
            snapshot: None,
            accounts: Vec::new(),
            keys: Vec::new(),
            output: None,
        }
    }

    fn finish(&mut self, output: Result<BindingShare, UsageError>) -> Effects {
        self.state = ShareState::Finish;
        self.output = Some(output);
        smallvec![]
    }

    fn accounts_page(&mut self, after: Option<Key>) -> Effects {
        self.state = ShareState::Accounts;
        let prefix = Some(self.binding.set_id.to_bytes().to_vec().into());
        scan(RANGE_ACCOUNT_KEYSPACE, prefix, after, self.txn_id)
    }

    fn read_set(&mut self, mut values: Vec<(Key, Option<Value>)>) -> Result<Effects, UsageError> {
        let Some((_, Some(value))) = values.pop() else {
            return Ok(self.finish(Ok((RangeCounters::default(), None))));
        };
        self.snapshot = Some(DistributionView::from_bytes(&value)?.head.snapshot);
        Ok(self.accounts_page(None))
    }

    fn read_accounts(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Effects, UsageError> {
        for (key, value) in values {
            let Some(range_id) = key.get(16..).and_then(|id| <[u8; 16]>::try_from(id).ok()) else {
                continue;
            };
            let account = RangeAccount::from_bytes(&value)?;
            self.accounts.push((Ulid::from_bytes(range_id), account));
        }
        if let Some(next) = next {
            return Ok(self.accounts_page(Some(next)));
        }
        let (held, own) = self.charged();
        let keys: BTreeSet<Vec<u8>> = held.into_iter().chain(own).collect();
        self.keys = keys.into_iter().collect();
        let reads = (self.keys.iter())
            .map(|key| (RANGE_COUNTER_KEYSPACE.to_string(), key.clone().into()))
            .collect();
        self.state = ShareState::Counters;
        Ok(batch_read(reads, self.txn_id))
    }

    /// The charged rows of every local account, and those of the coordinated ranges. A split
    /// parent this node has not split its accounting for yet counts with its children.
    fn charged(&self) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let set_id = self.binding.set_id;
        let member_id = self.binding.member_id;
        let coordinated = |range_id: &Ulid| {
            self.snapshot.as_ref().is_some_and(|snapshot| {
                (snapshot.config.ranges.iter()).any(|range| {
                    range.coordinator == member_id && snapshot.descends(range, *range_id)
                })
            })
        };
        let own: Vec<_> = (self.accounts.iter())
            .filter(|(range_id, _)| coordinated(range_id))
            .cloned()
            .collect();
        (
            charged_rows(set_id, &self.accounts),
            charged_rows(set_id, &own),
        )
    }

    fn read_counters(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
    ) -> Result<BindingShare, UsageError> {
        let (held, own) = self.charged();
        let stored: BTreeMap<_, _> = (std::mem::take(&mut self.keys).into_iter())
            .zip(values.into_iter().map(|(_, value)| value))
            .collect();
        let sum = |rows: &[Vec<u8>]| {
            let values: Vec<_> = rows
                .iter()
                .map(|key| stored.get(key).cloned().flatten())
                .collect();
            sum_rows(&values)
        };
        let share = sum(&held)?.merge(sum(&own)?.negate()?)?;
        let number = self.snapshot.as_ref().map(|snapshot| snapshot.number);
        Ok((share, number.map(|number| (self.binding.set_id, number))))
    }
}

impl EffectOperation for BindingShareOperation {
    type Output = BindingShare;
    type Error = UsageError;

    fn start(&mut self) -> Effects {
        self.state = ShareState::Set;
        let set = self.binding.set_id.to_bytes().to_vec().into();
        batch_read(vec![(DISTRIBUTION_KEYSPACE.to_string(), set)], self.txn_id)
    }

    fn step(&mut self, event: Event) -> Effects {
        let next = match (self.state, event) {
            (ShareState::Set, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.read_set(values)
            }
            (
                ShareState::Accounts,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.read_accounts(values, next_start_after),
            (ShareState::Counters, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let share = self.read_counters(values);
                return self.finish(share);
            }
            (
                ShareState::Set | ShareState::Accounts | ShareState::Counters,
                Event::Storage(StorageEvent::Error { error }),
            ) => Err(error.into()),
            (_, event) => Err(unexpected(&event)),
        };
        match next {
            Ok(effects) => effects,
            Err(error) => self.finish(Err(error)),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == ShareState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let unfinished = aruna_core::errors::StorageError::ReadError(
            "the binding share was not read".to_string(),
        );
        self.output.unwrap_or(Err(unfinished.into()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Sums each group's replica share over its local distributed buckets, read in `txn_id`. A share
/// that cannot be read fails the whole run, so the publish retries.
#[derive(Debug, PartialEq)]
pub struct ReplicaSharesOperation {
    txn_id: TxnId,
    started: bool,
    /// Bound buckets still to read, the next one last.
    pending: Vec<(GroupId, DistributionBinding)>,
    current: Option<(GroupId, BindingShareOperation)>,
    shares: BTreeMap<GroupId, GroupShare>,
    output: Option<Result<BTreeMap<GroupId, GroupShare>, UsageError>>,
}

impl ReplicaSharesOperation {
    pub fn new(txn_id: TxnId) -> Self {
        Self {
            txn_id,
            started: false,
            pending: Vec::new(),
            current: None,
            shares: BTreeMap::new(),
            output: None,
        }
    }

    fn finish(&mut self, output: Result<BTreeMap<GroupId, GroupShare>, UsageError>) -> Effects {
        self.output = Some(output);
        smallvec![]
    }

    fn read_buckets(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Effects, UsageError> {
        for (_, value) in values {
            let info = BucketInfo::from_bytes(value.as_ref())?;
            if let Some(binding) = info.distribution {
                self.pending.push((info.group_id, binding));
            }
        }
        if let Some(next) = next {
            return Ok(scan(S3_BUCKET_KEYSPACE, None, Some(next), self.txn_id));
        }
        self.pending.reverse();
        Ok(self.next_binding())
    }

    fn next_binding(&mut self) -> Effects {
        let Some((group_id, binding)) = self.pending.pop() else {
            let shares = std::mem::take(&mut self.shares);
            return self.finish(Ok(shares));
        };
        let mut share = BindingShareOperation::new(binding, self.txn_id);
        let effects = share.start();
        self.current = Some((group_id, share));
        effects
    }

    fn add_share(
        &mut self,
        group_id: GroupId,
        (share, revision): BindingShare,
    ) -> Result<(), UsageError> {
        let count = |value: i128| u64::try_from(value.max(0)).unwrap_or(u64::MAX);
        let counters = UsageCounters {
            objects: count(share.objects),
            logical_bytes: count(share.logical_bytes),
            referenced_bytes: count(share.referenced_bytes),
            ..UsageCounters::default()
        };
        let total = self.shares.entry(group_id).or_default();
        (total.replicas)
            .add(&counters)
            .map_err(|_| CounterOverflow("replicas"))?;
        total.revisions.extend(revision);
        Ok(())
    }
}

impl EffectOperation for ReplicaSharesOperation {
    type Output = BTreeMap<GroupId, GroupShare>;
    type Error = UsageError;

    fn start(&mut self) -> Effects {
        self.started = true;
        scan(S3_BUCKET_KEYSPACE, None, None, self.txn_id)
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Some((group_id, mut share)) = self.current.take() {
            let effects = share.step(event);
            if !share.is_complete() {
                self.current = Some((group_id, share));
                return effects;
            }
            return match share
                .finalize()
                .and_then(|share| self.add_share(group_id, share))
            {
                Ok(()) => self.next_binding(),
                Err(error) => self.finish(Err(error)),
            };
        }
        let next = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) if self.started && self.output.is_none() => {
                self.read_buckets(values, next_start_after)
            }
            Event::Storage(StorageEvent::Error { error }) if self.started => Err(error.into()),
            event => Err(unexpected(&event)),
        };
        match next {
            Ok(effects) => effects,
            Err(error) => self.finish(Err(error)),
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let unfinished = aruna_core::errors::StorageError::ReadError(
            "the replica shares were not read".to_string(),
        );
        self.output.unwrap_or(Err(unfinished.into()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_record::{binding, record};
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };

    const TXN: TxnId = Ulid::from_bytes([9; 16]);

    fn share() -> BindingShareOperation {
        let (snapshot, _) = record(true);
        BindingShareOperation::new(binding(&snapshot, 1, 1), TXN)
    }

    fn read(values: Vec<Option<Vec<u8>>>) -> Event {
        let values = values.into_iter();
        Event::Storage(StorageEvent::BatchReadResult {
            values: (values.map(|value| (Vec::new().into(), value.map(Into::into)))).collect(),
        })
    }

    fn empty_page() -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        })
    }

    // A set without local accounts shares nothing but names its record number.
    #[test]
    fn reads_set_accounts() {
        let (snapshot, bytes) = record(true);
        let set_id = snapshot.set_id();
        let mut operation = share();
        let start = batch_read(
            vec![(
                DISTRIBUTION_KEYSPACE.to_string(),
                set_id.to_bytes().to_vec().into(),
            )],
            TXN,
        );
        let prefix = Some(set_id.to_bytes().to_vec().into());
        let accounts = scan(RANGE_ACCOUNT_KEYSPACE, prefix, None, TXN);
        check_trace(
            &mut operation,
            &start,
            vec![
                (read(vec![Some(bytes)]), accounts.into_vec()),
                (empty_page(), batch_read(Vec::new(), TXN).into_vec()),
                (read(Vec::new()), Vec::new()),
            ],
        );
        let expected = (RangeCounters::default(), Some((set_id, snapshot.number)));
        assert_eq!(operation.finalize(), Ok(expected));
    }

    // A set this node has no record of shares nothing; a failed read fails.
    #[test]
    fn missing_set_empty() {
        let mut operation = share();
        operation.start();
        assert!(operation.step(read(vec![None])).is_empty());
        assert_eq!(operation.finalize(), Ok((RangeCounters::default(), None)));
        let mut operation = share();
        operation.start();
        operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(operation.finalize(), Err(StorageError::Timeout.into()));
    }

    // A node without bound buckets has no shares.
    #[test]
    fn empty_without_buckets() {
        let mut operation = ReplicaSharesOperation::new(TXN);
        let buckets = scan(S3_BUCKET_KEYSPACE, None, None, TXN);
        check_trace(&mut operation, &buckets, vec![(empty_page(), Vec::new())]);
        assert_eq!(operation.finalize(), Ok(BTreeMap::new()));
    }

    // Unstarted reads refuse every event, abort nothing and never finish early.
    #[test]
    fn shares_reject_unexpected() {
        let shares = || ReplicaSharesOperation::new(TXN);
        let all = sample_events().len();
        assert_eq!(rejects_events(share, sample_events, |_| false).len(), all);
        assert_eq!(rejects_events(shares, sample_events, |_| false).len(), all);
        rejects_finalize(share());
        rejects_finalize(shares());
        assert!(share().abort().is_empty());
        assert!(shares().abort().is_empty());
    }
}
