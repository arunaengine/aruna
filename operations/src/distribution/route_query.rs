//! Answers the routing questions of a bound bucket from this node's distribution record and
//! fence rows: each question names its rows and is decided from their values.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, ReadRoute, RouteError, WriteRoute,
};
use aruna_core::types::{Key, KeySpace, Value};
use ulid::Ulid;

use crate::distribution::guard::{GuardError, bound_record, read_target, target_for};

/// Decides a routing question from its arguments and the rows read for it.
pub type Decide<A, T> = fn(&A, &[Option<Value>]) -> Result<T, GuardError>;

/// Reads the rows of one routing question, then decides it without further I/O.
pub struct RouteOperation<A, T> {
    pub(super) reads: Vec<(KeySpace, Key)>,
    pub(super) args: A,
    pub(super) decide: Decide<A, T>,
}

impl<A: Debug, T> Debug for RouteOperation<A, T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteOperation")
            .field("reads", &self.reads)
            .field("args", &self.args)
            .finish_non_exhaustive()
    }
}

impl<A: PartialEq, T> PartialEq for RouteOperation<A, T> {
    fn eq(&self, other: &Self) -> bool {
        self.reads == other.reads && self.args == other.args
    }
}

impl<A, T> Operation for RouteOperation<A, T>
where
    A: Send + Debug + PartialEq,
    T: Send + Debug,
{
    type Output = T;
    type Error = GuardError;

    fn first(&mut self) -> Result<Next<T>, GuardError> {
        Ok(Next::Call(IoCall::Read(std::mem::take(&mut self.reads))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<T>, GuardError> {
        let values = result.values()?;
        Ok(Next::Done((self.decide)(&self.args, &values)?))
    }
}

pub(super) fn record_read(set_id: Ulid) -> Vec<(KeySpace, Key)> {
    let key = set_id.to_bytes().to_vec().into();
    vec![(DISTRIBUTION_KEYSPACE.to_string(), key)]
}

pub(super) fn row(values: &[Option<Value>], index: usize) -> Option<&[u8]> {
    values.get(index).and_then(Option::as_deref)
}

/// The record of the bound set among the rows, while the bucket is still its admitted member.
pub(super) fn bound(
    binding: &DistributionBinding,
    values: &[Option<Value>],
) -> Result<DistributionView, GuardError> {
    bound_record(binding, row(values, 0))
}

pub type Bound<A> = (DistributionBinding, A);

pub(super) fn read_route(
    (binding, (key, listing)): &Bound<(String, bool)>,
    rows: &[Option<Value>],
) -> Result<ReadRoute, GuardError> {
    read_target(binding, key, *listing, row(rows, 0))
}

pub(super) fn write_route(
    (binding, (key, completion)): &Bound<(String, bool)>,
    rows: &[Option<Value>],
) -> Result<WriteRoute, GuardError> {
    target_for(binding, key, (row(rows, 0), row(rows, 1)), *completion)
}

pub(super) fn held_view(
    (binding, ()): &Bound<()>,
    rows: &[Option<Value>],
) -> Result<DistributionView, GuardError> {
    bound(binding, rows)
}

pub(super) fn fence_row(_: &(), rows: &[Option<Value>]) -> Result<LocalFence, GuardError> {
    Ok(LocalFence::decode(row(rows, 0))?)
}

pub(super) fn coordinator_of(
    (binding, key): &Bound<String>,
    rows: &[Option<Value>],
) -> Result<Option<(Ulid, NodeId)>, GuardError> {
    let view = bound(binding, rows)?;
    let snapshot = view.snapshot();
    let set_id = binding.set_id;
    let range = snapshot.range_for(key).ok_or(RouteError::NoRange(set_id))?;
    if range.coordinator == binding.member_id {
        LocalFence::decode(row(rows, 1))?.check_read(snapshot, range)?;
        return Ok(None);
    }
    let coordinator = snapshot
        .member(range.coordinator)
        .ok_or(RouteError::NoRange(set_id))?;
    Ok(Some((range.range_id, coordinator.node_id)))
}

pub(super) fn upload_held(
    (binding, (key, tag)): &Bound<(String, UploadTag)>,
    rows: &[Option<Value>],
) -> Result<(), GuardError> {
    let view = bound(binding, rows)?;
    Ok(view
        .snapshot()
        .upload_authority(binding.member_id, key, tag)?)
}

pub(super) fn listed_tags(
    (binding, uploads): &Bound<Vec<(String, Ulid)>>,
    rows: &[Option<Value>],
) -> Result<Vec<UploadTag>, GuardError> {
    let view = bound(binding, rows)?;
    let snapshot = view.snapshot();
    uploads
        .iter()
        .map(|(key, upload_id)| {
            let range = snapshot
                .range_for(key)
                .ok_or(RouteError::NoRange(binding.set_id))?;
            Ok(UploadTag {
                upload_id: *upload_id,
                set_id: binding.set_id,
                range_id: range.range_id,
                epoch: range.epoch,
            })
        })
        .collect()
}

pub(super) fn range_coordinated(
    (binding, (range_id, key)): &Bound<(Ulid, String)>,
    rows: &[Option<Value>],
) -> Result<(), GuardError> {
    let view = bound(binding, rows)?;
    let snapshot = view.snapshot();
    let range = snapshot.resolve(*range_id, key)?;
    if range.coordinator == binding.member_id {
        return Ok(());
    }
    let node_id = snapshot
        .member(range.coordinator)
        .map(|member| member.node_id)
        .ok_or(RouteError::NoRange(binding.set_id))?;
    Err(GuardError::NotCoordinator {
        set_id: binding.set_id,
        range_id: range.range_id,
        node_id,
    })
}

pub(super) fn range_kept(
    (binding, (range_id, key)): &Bound<(Ulid, String)>,
    rows: &[Option<Value>],
) -> Result<(), GuardError> {
    let view = bound(binding, rows)?;
    let snapshot = view.snapshot();
    let range = snapshot.resolve(*range_id, key)?;
    match (snapshot.record_holders(range).iter())
        .any(|member| member.member_id == binding.member_id)
    {
        true => Ok(()),
        false => Err(GuardError::Route(RouteError::StaleRange(range.range_id))),
    }
}

pub type Interval = (String, Option<String>);

pub(super) fn listed_interval(
    (binding, (lower, upper)): &Bound<Interval>,
    rows: &[Option<Value>],
) -> Result<bool, GuardError> {
    let view = bound(binding, rows)?;
    let fence = LocalFence::decode(row(rows, 1))?;
    let interval = (lower.as_str(), upper.as_deref());
    Ok(view
        .snapshot()
        .lists_interval(binding.member_id, interval, &fence))
}

pub(super) fn copied_interval(
    (binding, (lower, upper)): &Bound<Interval>,
    rows: &[Option<Value>],
) -> Result<bool, GuardError> {
    let view = bound(binding, rows)?;
    let interval = (lower.as_str(), upper.as_deref());
    Ok(view.snapshot().copies_interval(binding.member_id, interval))
}

pub(super) fn range_readers(
    (binding, key): &Bound<String>,
    rows: &[Option<Value>],
) -> Result<(Ulid, bool, Vec<NodeId>), GuardError> {
    let view = bound(binding, rows)?;
    let snapshot = view.snapshot();
    let range = snapshot
        .range_for(key)
        .ok_or(RouteError::NoRange(binding.set_id))?;
    let here =
        (snapshot.record_holders(range).iter()).any(|member| member.member_id == binding.member_id);
    let copies = (snapshot.copy_holders(range).into_iter())
        .filter(|member| member.member_id != binding.member_id)
        .map(|member| member.node_id)
        .collect();
    Ok((range.range_id, here, copies))
}

pub(super) fn range_holders(
    (binding, key): &Bound<String>,
    rows: &[Option<Value>],
) -> Result<Vec<NodeId>, GuardError> {
    let view = bound(binding, rows)?;
    let snapshot = view.snapshot();
    let range = snapshot
        .range_for(key)
        .ok_or(RouteError::NoRange(binding.set_id))?;
    Ok((snapshot.record_holders(range).into_iter())
        .map(|member| member.node_id)
        .collect())
}

/// The tag of a new upload of a key this member coordinates.
pub(super) fn new_tag(
    (binding, (key, upload_id)): &Bound<(String, Ulid)>,
    rows: &[Option<Value>],
) -> Result<UploadTag, GuardError> {
    let records = (row(rows, 0), row(rows, 1));
    match target_for(binding, key, records, false)? {
        WriteRoute::Local { range_id, epoch } => Ok(UploadTag {
            upload_id: *upload_id,
            set_id: binding.set_id,
            range_id,
            epoch: epoch.0,
        }),
        WriteRoute::Forward {
            range_id, node_id, ..
        } => Err(GuardError::NotCoordinator {
            set_id: binding.set_id,
            range_id,
            node_id,
        }),
    }
}

pub(super) fn interval(lower: &str, upper: Option<&str>) -> Interval {
    (lower.to_string(), upper.map(str::to_string))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::guard::authority_reads;
    use crate::distribution::routing::read_coordinator;
    use crate::distribution::test_record::{binding, record};
    use aruna_core::calls::IoError;
    use aruna_core::errors::StorageError;

    fn question() -> RouteOperation<DistributionBinding, DistributionView> {
        let (snapshot, _) = record(true);
        let bound_to = binding(&snapshot, 1, 5);
        RouteOperation {
            reads: record_read(bound_to.set_id),
            args: bound_to,
            decide: |binding, rows| bound(binding, rows),
        }
    }

    // A question reads its rows once and is decided from the answer.
    #[test]
    fn decides_from_rows() {
        let (snapshot, bytes) = record(true);
        let mut operation = question();
        let read = IoCall::Read(record_read(snapshot.set_id()));
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let answer = IoResult::Values(vec![Some(bytes.into())]);
        let Ok(Next::Done(view)) = operation.next(answer) else {
            panic!("expected the record");
        };
        assert_eq!(view.snapshot().set_id(), snapshot.set_id());
    }

    // A set without a record here is missing; a failed read is unreadable.
    #[test]
    fn refuses_missing_record() {
        let (snapshot, _) = record(true);
        let mut operation = question();
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Err(GuardError::RecordMissing(snapshot.set_id()))
        );
        let failed = GuardError::from(IoError::Storage(StorageError::Timeout));
        assert!(matches!(failed, GuardError::Unreadable(_)));
    }

    // The coordinator of another member's range is named; this member's own range is not.
    #[test]
    fn finds_read_coordinator() {
        let (snapshot, bytes) = record(true);
        let own = binding(&snapshot, 1, 5);
        let ask = |key: &str| read_coordinator(&own, key);
        let rows = || IoResult::Values(vec![Some(Value::from(bytes.clone())), None]);
        let mut remote = ask("x");
        let reads = IoCall::Read(authority_reads(own.set_id));
        assert_eq!(remote.first(), Ok(Next::Call(reads)));
        let upper = &snapshot.config.ranges[1];
        let node_id = snapshot.member(upper.coordinator).unwrap().node_id;
        let named = Some((upper.range_id, node_id));
        assert_eq!(remote.next(rows()), Ok(Next::Done(named)));
        let mut local = ask("a");
        local.first().unwrap();
        assert_eq!(local.next(rows()), Ok(Next::Done(None)));
    }

    // A set whose record has not arrived yet cannot be routed.
    #[test]
    fn coordinator_needs_record() {
        let (snapshot, _) = record(true);
        let mut read = read_coordinator(&binding(&snapshot, 1, 5), "a");
        read.first().unwrap();
        assert_eq!(
            read.next(IoResult::Values(vec![None, None])),
            Err(GuardError::RecordMissing(snapshot.set_id()))
        );
    }
}
