//! Reads each group's replica share on this node in the caller's read transaction: the charged
//! rows of every local range account of a bound bucket's set, less those of coordinated ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::keyspaces::{
    DISTRIBUTION_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::accounting::{
    CounterOverflow, RangeAccount, RangeCounters,
};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, DistributionView,
};
use aruna_core::structs::storage::usage::UsageCounters;
use aruna_core::types::{GroupId, Key, TxnId, Value};
use ulid::Ulid;

use super::charged_ranges::{UsageError, charged_rows, sum_rows};
use crate::metadata::call_results::{page_rows, read_value, read_values};

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

/// One page of a scan in the caller's transaction.
fn scan(key_space: &str, prefix: Option<Key>, after: Option<Key>, txn_id: TxnId) -> IoCall {
    IoCall::Effect(Box::new(Effect::Storage(StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix,
        start: after.map(IterStart::After),
        limit: SCAN_PAGE,
        txn_id: Some(txn_id),
    })))
}

/// A read in the caller's transaction.
fn batch_read(reads: Vec<(String, Key)>, txn_id: TxnId) -> IoCall {
    IoCall::Effect(Box::new(Effect::Storage(StorageEffect::BatchRead {
        reads,
        txn_id: Some(txn_id),
    })))
}

/// What the pending call of a share read answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShareState {
    Set,
    Accounts,
    Counters,
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
}

impl BindingShareOperation {
    pub fn new(binding: DistributionBinding, txn_id: TxnId) -> Self {
        Self {
            binding,
            txn_id,
            state: ShareState::Set,
            snapshot: None,
            accounts: Vec::new(),
            keys: Vec::new(),
        }
    }

    fn accounts_page(&mut self, after: Option<Key>) -> Next<BindingShare> {
        self.state = ShareState::Accounts;
        let prefix = Some(self.binding.set_id.to_bytes().to_vec().into());
        Next::Call(scan(RANGE_ACCOUNT_KEYSPACE, prefix, after, self.txn_id))
    }

    fn read_set(&mut self, value: Option<Value>) -> Result<Next<BindingShare>, UsageError> {
        let Some(value) = value else {
            return Ok(Next::Done((RangeCounters::default(), None)));
        };
        self.snapshot = Some(DistributionView::from_bytes(&value)?.head.snapshot);
        Ok(self.accounts_page(None))
    }

    fn read_accounts(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Next<BindingShare>, UsageError> {
        for (key, value) in values {
            let Some(range_id) = key.get(16..).and_then(|id| <[u8; 16]>::try_from(id).ok()) else {
                continue;
            };
            let account = RangeAccount::from_bytes(&value)?;
            self.accounts.push((Ulid::from_bytes(range_id), account));
        }
        if next.is_some() {
            return Ok(self.accounts_page(next));
        }
        let (held, own) = self.charged();
        let keys: BTreeSet<Vec<u8>> = held.into_iter().chain(own).collect();
        self.keys = keys.into_iter().collect();
        let reads = (self.keys.iter())
            .map(|key| (RANGE_COUNTER_KEYSPACE.to_string(), key.clone().into()))
            .collect();
        self.state = ShareState::Counters;
        Ok(Next::Call(batch_read(reads, self.txn_id)))
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

    fn read_counters(&mut self, values: Vec<Option<Value>>) -> Result<BindingShare, UsageError> {
        let (held, own) = self.charged();
        let stored: BTreeMap<_, _> = (std::mem::take(&mut self.keys).into_iter())
            .zip(values)
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

impl Operation for BindingShareOperation {
    type Output = BindingShare;
    type Error = UsageError;

    fn first(&mut self) -> Result<Next<BindingShare>, UsageError> {
        let set = self.binding.set_id.to_bytes().to_vec().into();
        let read = vec![(DISTRIBUTION_KEYSPACE.to_string(), set)];
        Ok(Next::Call(batch_read(read, self.txn_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BindingShare>, UsageError> {
        match self.state {
            ShareState::Set => self.read_set(read_value(result)?),
            ShareState::Accounts => {
                let (rows, next) = page_rows(result)?;
                self.read_accounts(rows, next)
            }
            ShareState::Counters => Ok(Next::Done(self.read_counters(read_values(result)?)?)),
        }
    }
}

type Shares = BTreeMap<GroupId, GroupShare>;

/// Sums each group's replica share over its local distributed buckets, read in `txn_id`. A share
/// that cannot be read fails the whole run, so the publish retries.
#[derive(Debug, PartialEq)]
pub struct ReplicaSharesOperation {
    txn_id: TxnId,
    /// Bound buckets still to read, the next one last.
    pending: Vec<(GroupId, DistributionBinding)>,
    /// The group whose share is being read.
    current: Option<GroupId>,
    shares: Shares,
}

impl ReplicaSharesOperation {
    pub fn new(txn_id: TxnId) -> Self {
        Self {
            txn_id,
            pending: Vec::new(),
            current: None,
            shares: BTreeMap::new(),
        }
    }

    fn read_buckets(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Next<Shares>, UsageError> {
        for (_, value) in values {
            let info = BucketInfo::from_bytes(value.as_ref())?;
            if let Some(binding) = info.distribution {
                self.pending.push((info.group_id, binding));
            }
        }
        if next.is_some() {
            let page = scan(S3_BUCKET_KEYSPACE, None, next, self.txn_id);
            return Ok(Next::Call(page));
        }
        self.pending.reverse();
        Ok(self.next_binding())
    }

    fn next_binding(&mut self) -> Next<Shares> {
        let Some((group_id, binding)) = self.pending.pop() else {
            return Next::Done(std::mem::take(&mut self.shares));
        };
        self.current = Some(group_id);
        Next::Call(IoCall::child(BindingShareOperation::new(
            binding,
            self.txn_id,
        )))
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

impl Operation for ReplicaSharesOperation {
    type Output = Shares;
    type Error = UsageError;

    fn first(&mut self) -> Result<Next<Shares>, UsageError> {
        Ok(Next::Call(scan(S3_BUCKET_KEYSPACE, None, None, self.txn_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Shares>, UsageError> {
        match self.current.take() {
            Some(group_id) => {
                let share = result.child::<BindingShare, UsageError>()??;
                self.add_share(group_id, share)?;
                Ok(self.next_binding())
            }
            None => {
                let (rows, next) = page_rows(result)?;
                self.read_buckets(rows, next)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_record::{binding, record};
    use aruna_core::calls::{ChildOutcome, IoError};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};

    const TXN: TxnId = Ulid::from_bytes([9; 16]);

    fn share() -> BindingShareOperation {
        let (snapshot, _) = record(true);
        BindingShareOperation::new(binding(&snapshot, 1, 1), TXN)
    }

    fn read(values: Vec<Option<Vec<u8>>>) -> IoResult {
        let values = values.into_iter();
        IoResult::Event(Box::new(Event::Storage(StorageEvent::BatchReadResult {
            values: (values.map(|value| (Vec::new().into(), value.map(Into::into)))).collect(),
        })))
    }

    fn page(values: Vec<(Key, Value)>) -> IoResult {
        IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        })))
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
        assert_eq!(operation.first(), Ok(Next::Call(start)));
        let prefix = Some(set_id.to_bytes().to_vec().into());
        let accounts = scan(RANGE_ACCOUNT_KEYSPACE, prefix, None, TXN);
        assert_eq!(
            operation.next(read(vec![Some(bytes)])),
            Ok(Next::Call(accounts))
        );
        assert_eq!(
            operation.next(page(Vec::new())),
            Ok(Next::Call(batch_read(Vec::new(), TXN)))
        );
        let expected = (RangeCounters::default(), Some((set_id, snapshot.number)));
        assert_eq!(operation.next(read(Vec::new())), Ok(Next::Done(expected)));
    }

    // A set this node has no record of shares nothing; a result of another kind fails.
    #[test]
    fn missing_set_empty() {
        let mut operation = share();
        operation.first().unwrap();
        assert_eq!(
            operation.next(read(vec![None])),
            Ok(Next::Done((RangeCounters::default(), None)))
        );
        let mut operation = share();
        operation.first().unwrap();
        assert!(operation.next(IoResult::Done).is_err());
    }

    // A node without bound buckets has no shares.
    #[test]
    fn empty_without_buckets() {
        let mut operation = ReplicaSharesOperation::new(TXN);
        let buckets = scan(S3_BUCKET_KEYSPACE, None, None, TXN);
        assert_eq!(operation.first(), Ok(Next::Call(buckets)));
        assert_eq!(
            operation.next(page(Vec::new())),
            Ok(Next::Done(BTreeMap::new()))
        );
    }

    fn bound_bucket(group_id: GroupId) -> (Key, Value) {
        let (snapshot, _) = record(true);
        let info = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: Some(binding(&snapshot, 1, 1)),
        };
        (b"bucket".to_vec().into(), info.to_bytes().unwrap().into())
    }

    // Each bound bucket's share is read by a child and added to its group; a failed child fails.
    #[test]
    fn sums_bound_buckets() {
        let group_id = Ulid::from_bytes([3; 16]);
        let at_child = || {
            let mut operation = ReplicaSharesOperation::new(TXN);
            operation.first().unwrap();
            let child = operation.next(page(vec![bound_bucket(group_id)]));
            assert!(matches!(child, Ok(Next::Call(IoCall::Child(_)))));
            operation
        };
        let counted = RangeCounters {
            objects: 2,
            logical_bytes: 5,
            ..RangeCounters::default()
        };
        let set_id = Ulid::from_bytes([4; 16]);
        let share: Result<BindingShare, UsageError> = Ok((counted, Some((set_id, 7))));
        let Ok(Next::Done(shares)) = at_child().next(IoResult::Child(ChildOutcome::new(share)))
        else {
            panic!("expected the shares");
        };
        let total = &shares[&group_id];
        assert_eq!((total.replicas.objects, total.replicas.logical_bytes), (2, 5));
        assert_eq!(total.revisions.get(&set_id), Some(&7));

        let failed: Result<BindingShare, UsageError> =
            Err(IoError::Storage(StorageError::Timeout).into());
        assert_eq!(
            at_child().next(IoResult::Child(ChildOutcome::new(failed))),
            Err(StorageError::Timeout.into())
        );
    }
}
