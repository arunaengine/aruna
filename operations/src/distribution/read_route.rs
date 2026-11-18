//! Finds the coordinator that serves reads of one key of a distributed bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, RouteError,
};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::guard::{GuardError, bound_member};

/// The range id and node of the coordinator of a key's range, or `None` when this member
/// coordinates it and has not fenced it for a transfer.
#[derive(Debug, PartialEq)]
pub struct ReadCoordinatorOperation {
    binding: DistributionBinding,
    key: String,
    view: Option<DistributionView>,
}

type RouteNext = Result<Next<Option<(Ulid, NodeId)>>, GuardError>;

impl ReadCoordinatorOperation {
    pub fn new(binding: DistributionBinding, key: String) -> Self {
        Self {
            binding,
            key,
            view: None,
        }
    }

    fn route(&mut self, row: Option<Value>) -> RouteNext {
        let set_id = self.binding.set_id;
        let view = row
            .map(|value| DistributionView::from_bytes(&value))
            .transpose()?
            .ok_or(GuardError::RecordMissing(set_id))?;
        bound_member(&view, &self.binding)?;
        let snapshot = view.snapshot();
        let range = snapshot
            .range_for(&self.key)
            .ok_or(RouteError::NoRange(set_id))?;
        if range.coordinator != self.binding.member_id {
            let coordinator = snapshot
                .member(range.coordinator)
                .ok_or(RouteError::NoRange(set_id))?;
            return Ok(Next::Done(Some((range.range_id, coordinator.node_id))));
        }
        self.view = Some(view);
        let row = (
            DISTRIBUTION_FENCE_KEYSPACE.to_string(),
            Key::from(LocalFence::key(set_id)),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn fenced(&self, view: &DistributionView, row: Option<Value>) -> RouteNext {
        let set_id = self.binding.set_id;
        let snapshot = view.snapshot();
        let range = snapshot
            .range_for(&self.key)
            .ok_or(RouteError::NoRange(set_id))?;
        LocalFence::decode(row.as_deref())?.check_read(snapshot, range)?;
        Ok(Next::Done(None))
    }
}

impl Operation for ReadCoordinatorOperation {
    type Output = Option<(Ulid, NodeId)>;
    type Error = GuardError;

    fn first(&mut self) -> RouteNext {
        let key = Key::from(self.binding.set_id.to_bytes().to_vec());
        let row = (DISTRIBUTION_KEYSPACE.to_string(), key);
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> RouteNext {
        let [row] = values::<1>(result)?;
        match self.view.take() {
            None => self.route(row),
            Some(view) => self.fenced(&view, row),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::distribution::DistributionRevision;

    // A set whose record has not arrived yet cannot be routed.
    #[test]
    fn missing_record_refused() {
        let set_id = Ulid::from_parts(1, 1);
        let binding = DistributionBinding {
            set_id,
            member_id: Ulid::from_parts(2, 2),
            incarnation: 1,
            revision: DistributionRevision::default(),
        };
        let mut operation = ReadCoordinatorOperation::new(binding, "k".to_string());
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Err(GuardError::RecordMissing(set_id))
        );
    }
}
