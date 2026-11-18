//! Reads each group's replica share on this node in one read snapshot: the charged rows of every
//! local range account of a bound bucket's set, less those of coordinated ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    DISTRIBUTION_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::values;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::accounting::{
    CounterOverflow, RangeAccount, RangeCounters,
};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, DistributionView,
};
use aruna_core::structs::storage::usage::UsageCounters;
use aruna_core::types::{GroupId, Key, Value};
use ulid::Ulid;

use super::charged_ranges::{UsageError, charged_rows, sum_rows};

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

/// One page of a scan in the operation's snapshot.
fn scan(space: &str, prefix: Option<Key>, after: Option<Key>) -> IoCall {
    IoCall::Page {
        space: space.to_string(),
        prefix: prefix.unwrap_or_else(|| Key::from(Vec::new())),
        after,
        limit: SCAN_PAGE,
    }
}

/// What the pending call of a share read answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShareState {
    Set,
    Accounts,
    Counters,
}

/// Reads one bound bucket's share in the snapshot of its run: zero without a set record here.
#[derive(Debug, PartialEq)]
pub struct BindingShareOperation {
    binding: DistributionBinding,
    state: ShareState,
    snapshot: Option<DistributionSnapshot>,
    accounts: Vec<(Ulid, RangeAccount)>,
    keys: Vec<Vec<u8>>,
}

impl BindingShareOperation {
    pub fn new(binding: DistributionBinding) -> Self {
        Self {
            binding,
            state: ShareState::Set,
            snapshot: None,
            accounts: Vec::new(),
            keys: Vec::new(),
        }
    }

    fn accounts_page(&mut self, after: Option<Key>) -> Next<BindingShare> {
        self.state = ShareState::Accounts;
        let prefix = Some(self.binding.set_id.to_bytes().to_vec().into());
        Next::Call(scan(RANGE_ACCOUNT_KEYSPACE, prefix, after))
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
        Ok(Next::Call(IoCall::Read(reads)))
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
        Ok(Next::Call(IoCall::Read(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BindingShare>, UsageError> {
        match self.state {
            ShareState::Set => {
                let [value] = values::<1>(result)?;
                self.read_set(value)
            }
            ShareState::Accounts => {
                let (rows, next) = result.page()?;
                self.read_accounts(rows, next)
            }
            ShareState::Counters => Ok(Next::Done(self.read_counters(result.values()?)?)),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
}

type Shares = BTreeMap<GroupId, GroupShare>;

/// Sums each group's replica share over its local distributed buckets. Every read runs in the
/// snapshot of the run this operation drives or is folded into; a share that cannot be read fails
/// the whole run, so the publish retries.
#[derive(Debug, PartialEq)]
pub struct ReplicaSharesOperation {
    /// Bound buckets still to read, the next one last.
    pending: Vec<(GroupId, DistributionBinding)>,
    /// The group whose share is being read, in the snapshot of this run.
    current: Option<(GroupId, BindingShareOperation)>,
    shares: Shares,
}

impl ReplicaSharesOperation {
    pub fn new() -> Self {
        Self {
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
            return Ok(Next::Call(scan(S3_BUCKET_KEYSPACE, None, next)));
        }
        self.pending.reverse();
        self.next_binding()
    }

    fn next_binding(&mut self) -> Result<Next<Shares>, UsageError> {
        let Some((group_id, binding)) = self.pending.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.shares)));
        };
        let mut share = BindingShareOperation::new(binding);
        let turn = share.first()?;
        self.current = Some((group_id, share));
        self.share_turn(turn)
    }

    /// Passes on the call of the share being read; its outcome moves on to the next one.
    fn share_turn(&mut self, turn: Next<BindingShare>) -> Result<Next<Shares>, UsageError> {
        match turn {
            Next::Call(call) => Ok(Next::Call(call)),
            Next::Done(share) => {
                let (group_id, _) = self.current.take().ok_or(IoError::Unfinished)?;
                self.add_share(group_id, share)?;
                self.next_binding()
            }
            Next::Wait | Next::Then { .. } => Err(IoError::Unfinished.into()),
        }
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

impl Default for ReplicaSharesOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for ReplicaSharesOperation {
    type Output = Shares;
    type Error = UsageError;

    fn first(&mut self) -> Result<Next<Shares>, UsageError> {
        Ok(Next::Call(scan(S3_BUCKET_KEYSPACE, None, None)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Shares>, UsageError> {
        if let Some((_, share)) = self.current.as_mut() {
            let turn = share.next(result)?;
            return self.share_turn(turn);
        }
        let (rows, next) = result.page()?;
        self.read_buckets(rows, next)
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_record::{binding, record};
    use aruna_core::errors::StorageError;

    fn share() -> BindingShareOperation {
        let (snapshot, _) = record(true);
        BindingShareOperation::new(binding(&snapshot, 1, 1))
    }

    fn read(values: Vec<Option<Vec<u8>>>) -> IoResult {
        IoResult::Values(
            values
                .into_iter()
                .map(|value| value.map(Into::into))
                .collect(),
        )
    }

    fn page(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    // A set without local accounts shares nothing but names its record number.
    #[test]
    fn reads_set_accounts() {
        let (snapshot, bytes) = record(true);
        let set_id = snapshot.set_id();
        let mut operation = share();
        assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
        let start = IoCall::Read(vec![(
            DISTRIBUTION_KEYSPACE.to_string(),
            set_id.to_bytes().to_vec().into(),
        )]);
        assert_eq!(operation.first(), Ok(Next::Call(start)));
        let prefix = Some(set_id.to_bytes().to_vec().into());
        let accounts = scan(RANGE_ACCOUNT_KEYSPACE, prefix, None);
        assert_eq!(
            operation.next(read(vec![Some(bytes)])),
            Ok(Next::Call(accounts))
        );
        assert_eq!(
            operation.next(page(Vec::new())),
            Ok(Next::Call(IoCall::Read(Vec::new())))
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
        let mut operation = ReplicaSharesOperation::new();
        let buckets = scan(S3_BUCKET_KEYSPACE, None, None);
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

    // A bound bucket's share is read in the same run and added to its group.
    #[test]
    fn sums_bound_buckets() {
        let group_id = Ulid::from_bytes([3; 16]);
        let (snapshot, bytes) = record(true);
        let mut operation = ReplicaSharesOperation::new();
        operation.first().unwrap();
        let set = operation.next(page(vec![bound_bucket(group_id)]));
        assert!(matches!(set, Ok(Next::Call(IoCall::Read(_)))));
        let accounts = operation.next(read(vec![Some(bytes)]));
        assert!(matches!(accounts, Ok(Next::Call(IoCall::Page { .. }))));
        operation.next(page(Vec::new())).unwrap();
        let Ok(Next::Done(shares)) = operation.next(read(Vec::new())) else {
            panic!("expected the shares");
        };
        let revisions = &shares[&group_id].revisions;
        assert_eq!(revisions.get(&snapshot.set_id()), Some(&snapshot.number));
    }

    // A failed read of one share fails the run.
    #[test]
    fn failed_share_fails() {
        let group_id = Ulid::from_bytes([3; 16]);
        let mut operation = ReplicaSharesOperation::new();
        operation.first().unwrap();
        operation.next(page(vec![bound_bucket(group_id)])).unwrap();
        assert!(operation.next(IoResult::Done).is_err());
        let error: UsageError = IoError::Storage(StorageError::Timeout).into();
        assert_eq!(error, StorageError::Timeout.into());
    }
}
