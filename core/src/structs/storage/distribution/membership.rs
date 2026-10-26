//! Applies a group admin's `join` and `leave` to a set's record. A joined member starts created,
//! admits itself, catches up from the coordinators and becomes ready.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A leaving member keeps no range and receives no publication.

use ulid::Ulid;

use super::snapshot::{DistributionSnapshot, Tombstone};
use super::{DistributionError, DistributionMember, MemberState, SetPhase};

const JOIN_DOMAIN: &str = "aruna-distribution-join-v1";

/// The intent a joined member's admission names: the set, the digest of the head it joined on
/// and the complete join request, so an admission never moves to another join.
pub fn join_intent(
    set_id: Ulid,
    previous: [u8; 32],
    member: &DistributionMember,
    ranges: &[Ulid],
) -> [u8; 32] {
    let bytes = postcard::to_allocvec(&(JOIN_DOMAIN, set_id, previous, member, ranges));
    *blake3::hash(&bytes.unwrap_or_default()).as_bytes()
}

/// Refuses configuration changes while the set is being deleted.
pub(super) fn changeable(next: &DistributionSnapshot) -> Result<(), DistributionError> {
    match next.lifecycle.deletion {
        Some(_) => Err(DistributionError::Deleting),
        None => Ok(()),
    }
}

/// Adds a created member, as a replica of `ranges`. A leaving member of the same node is
/// replaced and gets a tombstone, so the node can join again.
pub(super) fn join(
    next: &mut DistributionSnapshot,
    member: &DistributionMember,
    ranges: &[Ulid],
) -> Result<(), DistributionError> {
    changeable(next)?;
    if next.phase != SetPhase::Active {
        return Err(DistributionError::InvalidChange(
            "only an active set takes new members",
        ));
    }
    let fresh = member.state == MemberState::Created
        && member.incarnation.is_none()
        && member.intent == [0; 32];
    if !fresh {
        return Err(DistributionError::InvalidCreate);
    }
    let replaced = (next.config.members.iter())
        .position(|known| known.node_id == member.node_id && known.state == MemberState::Leaving);
    if let Some(index) = replaced {
        let left = next.config.members.remove(index);
        if let Some(incarnation) = left.incarnation {
            next.tombstones.push(Tombstone {
                member_id: left.member_id,
                incarnation,
            });
        }
        next.statements
            .retain(|signed| signed.statement.member_id != left.member_id);
    }
    for range_id in ranges {
        let range = (next.config.ranges.iter_mut())
            .find(|range| range.range_id == *range_id)
            .ok_or(DistributionError::InvalidChange(
                "a joined range does not exist",
            ))?;
        if !range.replicas.contains(&member.member_id) {
            range.replicas.push(member.member_id);
        }
    }
    let previous = next.previous.ok_or(DistributionError::UnknownSet)?;
    let mut joined = member.clone();
    joined.intent = join_intent(next.set_id(), previous, member, ranges);
    next.config.members.push(joined);
    Ok(())
}

/// Marks a member leaving: it keeps no range and coordinators stop publishing to it. A
/// coordinator transfers its ranges first.
pub(super) fn leave(
    next: &mut DistributionSnapshot,
    member_id: Ulid,
) -> Result<(), DistributionError> {
    changeable(next)?;
    let member = next
        .member(member_id)
        .ok_or(DistributionError::NoMember(member_id))?;
    if member.state == MemberState::Leaving {
        return Err(DistributionError::InvalidChange(
            "the member is already leaving",
        ));
    }
    if (next.config.ranges.iter()).any(|range| range.coordinator == member_id) {
        return Err(DistributionError::CoordinatorMember(member_id));
    }
    if let Some(open) = (next.lifecycle.transfers.iter())
        .find(|open| open.to == member_id || open.from == member_id)
    {
        return Err(DistributionError::TransferOpen(open.range_id));
    }
    for range in &mut next.config.ranges {
        range.replicas.retain(|replica| *replica != member_id);
    }
    if let Some(member) =
        (next.config.members.iter_mut()).find(|known| known.member_id == member_id)
    {
        member.state = MemberState::Leaving;
    }
    Ok(())
}

impl DistributionSnapshot {
    /// The highest epoch the record names for any range or open transfer; a transfer takes a
    /// fresh epoch above it, so no epoch is ever reused within the set.
    pub fn max_epoch(&self) -> u64 {
        let ranges = self.config.ranges.iter().map(|range| range.epoch);
        let transfers = self.lifecycle.transfers.iter().map(|open| open.epoch);
        ranges.chain(transfers).max().unwrap_or_default()
    }

    /// Whether the record holds work a node does on its own: an open transfer, a deletion, a
    /// leaving member, or a created member that may replace one whose bucket is not released.
    pub fn lifecycle_open(&self) -> bool {
        let members = self.config.members.iter();
        let replacing = !self.tombstones.is_empty()
            && (members.clone()).any(|member| member.state == MemberState::Created);
        !self.lifecycle.transfers.is_empty()
            || self.lifecycle.deletion.is_some()
            || replacing
            || members
                .into_iter()
                .any(|member| member.state == MemberState::Leaving)
    }

    /// Whether the set deleted or released this member incarnation.
    pub fn tombstoned(&self, member_id: Ulid, incarnation: u128) -> bool {
        (self.tombstones.iter())
            .any(|stone| stone.member_id == member_id && stone.incarnation == incarnation)
    }
}

#[cfg(test)]
#[path = "membership_tests.rs"]
mod tests;
