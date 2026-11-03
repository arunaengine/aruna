//! Applies a group admin's `join`, `leave` and `add copy` to a set's record. A joined member
//! starts created, admits itself, catches up from the coordinators and becomes ready.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A leaving member keeps no range and receives no publication.

use ulid::Ulid;

use super::snapshot::{DistributionSnapshot, RangeFill, Tombstone};
use super::{
    DataStrategy, DistributionError, DistributionMember, MemberState, SetPhase, Statement,
    StatementKind,
};
use crate::structs::identity::realm::RealmId;

const JOIN_DOMAIN: &str = "aruna-distribution-join-v1";
const FILLED_DOMAIN: &[u8] = b"aruna-distribution-filled-v1";

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
    next.lifecycle
        .fills
        .retain(|fill| fill.member_id != member_id);
    if let Some(member) =
        (next.config.members.iter_mut()).find(|known| known.member_id == member_id)
    {
        member.state = MemberState::Leaving;
    }
    Ok(())
}

/// Adds a ready `copy` member as a replica of one more range; the range's coordinator fills
/// the copy before reads use it.
pub(super) fn add_copy(
    next: &mut DistributionSnapshot,
    fill: &RangeFill,
) -> Result<(), DistributionError> {
    changeable(next)?;
    let refused = DistributionError::InvalidChange;
    if next.phase != SetPhase::Active {
        return Err(refused("only an active set takes new copies"));
    }
    let member = next
        .member(fill.member_id)
        .ok_or(DistributionError::NoMember(fill.member_id))?;
    if member.state != MemberState::Ready || member.strategy != DataStrategy::Copy {
        return Err(refused(
            "only a ready member with the copy strategy takes a copy",
        ));
    }
    if (next.lifecycle.fills.iter()).any(|open| open.fill_id == fill.fill_id) {
        return Err(refused("the copy was already added"));
    }
    if (next.lifecycle.transfers.iter()).any(|open| open.range_id == fill.range_id) {
        return Err(DistributionError::TransferOpen(fill.range_id));
    }
    let range = (next.config.ranges.iter_mut())
        .find(|range| range.range_id == fill.range_id)
        .ok_or(refused("the range does not exist"))?;
    if range.coordinator == fill.member_id || range.replicas.contains(&fill.member_id) {
        return Err(refused("the member already keeps a copy of the range"));
    }
    range.replicas.push(fill.member_id);
    next.lifecycle.fills.push(RangeFill {
        admitted: false,
        ..fill.clone()
    });
    Ok(())
}

/// Takes the member's statement that its node opened the ranges of its new copies; only then
/// may their coordinators fill them.
pub(super) fn copy_admitted(
    next: &mut DistributionSnapshot,
    statement: &Statement,
) -> Result<(), DistributionError> {
    let invalid = DistributionError::InvalidStatement;
    let StatementKind::CopyAdmitted { fill_ids } = &statement.kind else {
        return Err(invalid("not an admitted copy"));
    };
    let member = next
        .member(statement.member_id)
        .ok_or(DistributionError::NoMember(statement.member_id))?;
    if member.incarnation != Some(statement.incarnation) || member.intent != statement.intent {
        return Err(invalid(
            "the admission does not answer the member's incarnation",
        ));
    }
    let named = |fill: &RangeFill| fill_ids.contains(&fill.fill_id);
    let fills = (next.lifecycle.fills.iter()).filter(|fill| named(fill));
    if fill_ids.is_empty() || fills.clone().count() != fill_ids.len() {
        return Err(invalid("the statement names a copy that is not filling"));
    }
    if fills
        .clone()
        .any(|fill| fill.member_id != statement.member_id)
    {
        return Err(invalid("only the copy's own member admits it"));
    }
    if fills.clone().all(|fill| fill.admitted) {
        return Err(DistributionError::Replayed(statement.statement_id));
    }
    (next.lifecycle.fills.iter_mut())
        .filter(|fill| named(fill))
        .for_each(|fill| fill.admitted = true);
    Ok(())
}

/// Takes the coordinator's statement that the copy holds everything it walked: the member
/// then serves the range like any replica.
pub(super) fn copy_filled(
    next: &mut DistributionSnapshot,
    statement: &Statement,
) -> Result<(), DistributionError> {
    let invalid = DistributionError::InvalidStatement;
    let StatementKind::CopyFilled {
        fill_id,
        range_id,
        epoch,
        member_id,
        ..
    } = &statement.kind
    else {
        return Err(invalid("not a filled copy"));
    };
    let index = (next.lifecycle.fills.iter())
        .position(|fill| fill.fill_id == *fill_id)
        .ok_or(invalid("the copy is not filling"))?;
    let fill = &next.lifecycle.fills[index];
    if !fill.admitted {
        return Err(invalid("the copy's node has not admitted it yet"));
    }
    let range = (next.config.ranges.iter()).find(|range| range.range_id == fill.range_id);
    let current = range
        .is_some_and(|range| range.coordinator == statement.member_id && range.epoch == *epoch);
    if fill.range_id != *range_id || fill.member_id != *member_id || !current {
        return Err(invalid(
            "only the range's coordinator states the copy filled at its epoch",
        ));
    }
    next.lifecycle.fills.remove(index);
    Ok(())
}

impl Statement {
    /// The coordinator's statement that it walked `fill` to its end at `epoch`. Its id is
    /// derived from the fill, so a retry is embedded once.
    pub fn copy_filled(
        realm_id: RealmId,
        set_id: Ulid,
        signer: &DistributionMember,
        fill: &RangeFill,
        (epoch, watermark): (u64, u64),
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(FILLED_DOMAIN);
        hasher.update(&set_id.to_bytes());
        hasher.update(&fill.fill_id.to_bytes());
        let mut id = [0; 16];
        id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Self {
            realm_id,
            set_id,
            statement_id: Ulid::from_bytes(id),
            member_id: signer.member_id,
            incarnation: signer.incarnation.unwrap_or_default(),
            intent: signer.intent,
            kind: StatementKind::CopyFilled {
                fill_id: fill.fill_id,
                range_id: fill.range_id,
                epoch,
                member_id: fill.member_id,
                watermark,
            },
        }
    }
}

impl DistributionSnapshot {
    /// Whether `member_id` keeps a copy of `range_id` that is still filling.
    pub fn filling(&self, range_id: Ulid, member_id: Ulid) -> bool {
        (self.lifecycle.fills.iter())
            .any(|fill| fill.range_id == range_id && fill.member_id == member_id)
    }

    /// Whether a coordinator still walks a range for a member: a catch-up or a filling copy.
    pub fn bootstrap_open(&self) -> bool {
        !self.catching_up().is_empty() || !self.lifecycle.fills.is_empty()
    }

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

#[cfg(test)]
#[path = "copy_tests.rs"]
mod copy_tests;
