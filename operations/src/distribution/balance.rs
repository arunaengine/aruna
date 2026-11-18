//! Balances a distributed bucket's coordinators: a coordinator on a node clearly fuller than
//! the set's average proposes moving one range to its least filled ready `copy` replica.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fullness is stored bytes relative to capacity; one transfer runs at a time, with a cool-down.

use std::collections::BTreeMap;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionSnapshot, KeyRange, MemberState, Statement, StatementClaim,
    StatementKind, TransferRole,
};
use tracing::{info, warn};
use ulid::Ulid;

use super::submit::{StateOperation, Stated};
use crate::s3::usage::{LoadCountersError, LoadCountersOperation};
use aruna_core::structs::storage::usage::{USAGE_GLOBAL_KEY, UsageCounters};

/// How long a node waits after proposing a transfer before it proposes the next one.
pub(super) const COOL_DOWN: Duration = Duration::from_secs(30 * 60);
/// A node balances only once it holds this many permille more than the set's average.
const MARGIN_PERMILLE: u64 = 1100;

/// One balancing decision: the range this node gives away and the member that takes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Proposal {
    pub range_id: Ulid,
    pub to: Ulid,
}

/// Picks one transfer from the fill of each member node, or none. Pure: `fill` holds the fill
/// in permille of capacity of the nodes this node hears from, itself included.
pub fn propose(
    snapshot: &DistributionSnapshot,
    local: NodeId,
    fill: &BTreeMap<NodeId, u64>,
) -> Option<Proposal> {
    let member = (snapshot.config.members.iter()).find(|member| member.node_id == local)?;
    let open = !snapshot.lifecycle.transfers.is_empty();
    if !snapshot.serving() || snapshot.lifecycle.deletion.is_some() || open {
        return None;
    }
    let counted: Vec<u64> = (snapshot.config.members.iter())
        .filter_map(|member| fill.get(&member.node_id).copied())
        .collect();
    let own = *fill.get(&local)?;
    let average = counted.iter().sum::<u64>() / u64::try_from(counted.len()).ok()?.max(1);
    if own.saturating_mul(1000) <= average.saturating_mul(MARGIN_PERMILLE) {
        return None;
    }
    let coordinated: Vec<&KeyRange> = (snapshot.config.ranges.iter())
        .filter(|range| range.coordinator == member.member_id)
        .collect();
    // Giving away the last range would make the member leave instead of freeing room.
    if coordinated.len() < 2 {
        return None;
    }
    let target = |range: &KeyRange| {
        (range.replicas.iter())
            .filter_map(|replica| snapshot.member(*replica))
            .filter(|m| m.state == MemberState::Ready && m.strategy == DataStrategy::Copy)
            .filter_map(|m| Some((*fill.get(&m.node_id)?, m.member_id)))
            .filter(|(bytes, _)| *bytes < average)
            .min()
    };
    (coordinated.into_iter())
        .filter_map(|range| target(range).map(|(bytes, to)| (bytes, range.range_id, to)))
        .min()
        .map(|(_, range_id, to)| Proposal { range_id, to })
}

/// The fill of a node in permille of its capacity; `None` without a known capacity.
pub fn permille(used: u64, capacity: Option<u64>) -> Option<u64> {
    let capacity = capacity.filter(|capacity| *capacity > 0)?;
    let fill = u128::from(used) * 1000 / u128::from(capacity);
    Some(u64::try_from(fill).unwrap_or(u64::MAX))
}

/// What balancing needs to know about this node's neighbours and its own capacity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BalanceInputs {
    pub(super) fill: BTreeMap<NodeId, u64>,
    pub(super) capacity: Option<u64>,
}

/// The fill of every node this node hears from, and its own capacity.
pub(super) fn balance_inputs(
    fresh: &BTreeMap<NodeId, HeldHeartbeat>,
    capacity: Option<u64>,
) -> BalanceInputs {
    let fill = (fresh.iter())
        .filter_map(|(node_id, held)| {
            let utilization = held.heartbeat.utilization;
            let used = utilization.storage_bytes_used;
            Some((
                *node_id,
                permille(used, utilization.storage_capacity_bytes)?,
            ))
        })
        .collect();
    BalanceInputs { fill, capacity }
}

/// Proposes one balancing transfer of a set to its publisher, when this node is over-full.
/// Outputs whether a transfer was signed, which starts the cool-down.
#[derive(Debug, PartialEq)]
pub(super) struct BalanceOperation {
    ids: (NodeId, RealmId),
    snapshot: DistributionSnapshot,
    inputs: BalanceInputs,
    proposal: Option<Proposal>,
}

impl BalanceOperation {
    pub(super) fn new(
        ids: (NodeId, RealmId),
        snapshot: DistributionSnapshot,
        inputs: BalanceInputs,
    ) -> Self {
        Self {
            ids,
            snapshot,
            inputs,
            proposal: None,
        }
    }

    /// Signs and sends the transfer the fill asks for, if any.
    fn propose(&mut self, used: u64) -> Result<Next<bool>, IoError> {
        let (local, realm_id) = self.ids;
        let Some(own) = permille(used, self.inputs.capacity) else {
            return Ok(Next::Done(false));
        };
        self.inputs.fill.insert(local, own);
        let snapshot = &self.snapshot;
        let Some(proposal) = propose(snapshot, local, &self.inputs.fill) else {
            return Ok(Next::Done(false));
        };
        let Some(member) = (snapshot.config.members.iter()).find(|m| m.node_id == local) else {
            return Ok(Next::Done(false));
        };
        let transfer_id = Ulid::generate();
        let kind = StatementKind::TransferPrepare {
            basis: snapshot.number,
            transfer_id,
            range_id: proposal.range_id,
            to: proposal.to,
            epoch: snapshot.max_epoch() + 1,
            role: TransferRole::Leave,
        };
        let set_id = snapshot.set_id();
        let subject = transfer_id.to_bytes();
        let statement = Statement::lifecycle(realm_id, set_id, member, &subject, kind);
        let claim = StatementClaim {
            signer: local,
            statement,
        };
        let publisher = snapshot.root.publisher;
        self.proposal = Some(proposal);
        let state = StateOperation::new(Some(self.ids), publisher, claim);
        Ok(Next::Call(IoCall::child(state)))
    }
}

impl Operation for BalanceOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let global = LoadCountersOperation::new(USAGE_GLOBAL_KEY.to_vec());
        Ok(Next::Call(IoCall::child(global)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        let Some(proposal) = self.proposal.take() else {
            return match result.child::<UsageCounters, LoadCountersError>()? {
                Ok(usage) => self.propose(usage.stored_bytes),
                Err(_) => Ok(Next::Done(false)),
            };
        };
        let set_id = self.snapshot.set_id();
        let (range_id, to) = (proposal.range_id, proposal.to);
        Ok(Next::Done(match result.child::<Stated, IoError>()?? {
            Stated::Taken => {
                info!(%set_id, %range_id, %to, "Proposed a balancing transfer");
                true
            }
            Stated::Refused(refusal) => {
                warn!(%set_id, %refusal, "The publisher did not take a balancing transfer");
                true
            }
            Stated::Unsigned(reason) => {
                warn!(%reason, "A balancing transfer could not be signed");
                false
            }
        }))
    }
}

#[cfg(test)]
#[path = "balance_tests.rs"]
mod tests;
