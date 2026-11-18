//! Computes the realm-wide data totals a realm publishes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::usage::UsageCounters;
use thiserror::Error;

use crate::node::realm_usage::{RealmUsage, RealmUsageError, RealmUsageOperation, RealmUsageScope};

#[derive(Debug, Error, PartialEq)]
pub enum DataTotalsError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Usage(#[from] RealmUsageError),
    #[error("no realm usage total")]
    Missing,
}

/// The realm's current objects and data bytes, stored and referenced alike, private data
/// included, from the realm usage sum.
#[derive(Debug, PartialEq)]
pub struct DataTotalsOperation {
    node_id: NodeId,
    realm_id: Option<RealmId>,
}

impl DataTotalsOperation {
    /// `realm_id` is the realm this node serves, when it runs a net transport.
    pub fn new(node_id: NodeId, realm_id: Option<RealmId>) -> Self {
        Self { node_id, realm_id }
    }
}

impl Operation for DataTotalsOperation {
    type Output = (u64, u64);
    type Error = DataTotalsError;

    fn first(&mut self) -> Result<Next<(u64, u64)>, DataTotalsError> {
        let usage = RealmUsageOperation::new(self.node_id, self.realm_id, RealmUsageScope::Global);
        Ok(Next::Call(IoCall::child(usage)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<(u64, u64)>, DataTotalsError> {
        let mut totals = result.child::<Vec<RealmUsage>, RealmUsageError>()??;
        let usage = totals.pop().ok_or(DataTotalsError::Missing)?;
        Ok(Next::Done(data_of(&usage.counters)))
    }
}

/// Objects and stored plus referenced bytes; both count each object once.
fn data_of(counters: &UsageCounters) -> (u64, u64) {
    let data_bytes = counters
        .logical_bytes
        .saturating_add(counters.referenced_bytes);
    (counters.objects, data_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[2; 32]).public()
    }

    #[test]
    fn data_counted_once() {
        // Physical copies (`stored_bytes`) never enter the data total.
        let counters = UsageCounters {
            objects: 3,
            stored_bytes: 900,
            logical_bytes: 100,
            referenced_bytes: 20,
            ..UsageCounters::default()
        };
        assert_eq!(data_of(&counters), (3, 120));
    }

    #[test]
    fn empty_sum_unknown() {
        let mut operation = DataTotalsOperation::new(node(), None);
        let usage = RealmUsageOperation::new(node(), None, RealmUsageScope::Global);
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::child(usage))));
        let empty: Result<Vec<RealmUsage>, RealmUsageError> = Ok(Vec::new());
        let answer = operation.next(IoResult::Child(ChildOutcome::new(empty)));
        assert_eq!(answer, Err(DataTotalsError::Missing));
    }
}
