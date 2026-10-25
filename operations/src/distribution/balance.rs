//! Balances a distributed bucket's coordinators: a coordinator on a node clearly fuller than
//! the set's average proposes moving one range to its least filled ready `copy` replica.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fullness is stored bytes relative to capacity; one transfer runs at a time, with a cool-down.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::events::StatementSignEvent;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionSnapshot, KeyRange, MemberState, Statement, StatementClaim,
    StatementKind, TransferRole,
};
use tracing::{info, warn};
use ulid::Ulid;

use super::create::submit;
use super::sign_statement;
use crate::driver::DriverContext;
use crate::node::heartbeat::fresh_heartbeats;

/// How long a node waits after proposing a transfer before it proposes the next one.
pub const COOL_DOWN: Duration = Duration::from_secs(30 * 60);
/// A node balances only once it holds this many permille more than the set's average.
const MARGIN_PERMILLE: u64 = 1100;

/// When this node last proposed a transfer.
static LAST: Mutex<Option<Instant>> = Mutex::new(None);

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

/// Proposes one balancing transfer of a set to its publisher, when this node is over-full and
/// no cool-down runs.
pub async fn balance_set(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local: NodeId,
    snapshot: &DistributionSnapshot,
) {
    let cooling = LAST
        .lock()
        .map(|last| last.is_some_and(|at| at.elapsed() < COOL_DOWN))
        .unwrap_or(true);
    if cooling {
        return;
    }
    let mut fill: BTreeMap<NodeId, u64> = fresh_heartbeats(context.net_handle.as_ref())
        .into_iter()
        .filter_map(|(node_id, held)| {
            let utilization = held.heartbeat.utilization;
            let used = utilization.storage_bytes_used;
            Some((node_id, permille(used, utilization.storage_capacity_bytes)?))
        })
        .collect();
    let capacity = crate::effect_adapters::routing::node_routing(context)
        .catalog
        .capacity();
    match crate::node::usage_stats::read_local_global(&context.storage_handle).await {
        Ok(usage) => match permille(usage.stored_bytes, capacity) {
            Some(own) => fill.insert(local, own),
            None => return,
        },
        Err(_) => return,
    };
    let Some(proposal) = propose(snapshot, local, &fill) else {
        return;
    };
    let Some(member) = (snapshot.config.members.iter()).find(|m| m.node_id == local) else {
        return;
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
    let statement = Statement::lifecycle(realm_id, set_id, member, &transfer_id.to_bytes(), kind);
    let claim = StatementClaim {
        signer: local,
        statement,
    };
    let signed = match sign_statement(context, claim) {
        StatementSignEvent::Signed(signed) => *signed,
        StatementSignEvent::Unavailable(reason) => {
            warn!(%reason, "A balancing transfer could not be signed");
            return;
        }
    };
    if let Ok(mut last) = LAST.lock() {
        *last = Some(Instant::now());
    }
    match submit(context, snapshot.root.publisher, local, signed).await {
        Ok(()) => {
            info!(%set_id, range_id = %proposal.range_id, to = %proposal.to, "Proposed a balancing transfer")
        }
        Err(refusal) => {
            warn!(%set_id, %refusal, "The publisher did not take a balancing transfer")
        }
    }
}

#[cfg(test)]
#[path = "balance_tests.rs"]
mod tests;
