//! Applies fenced transfers and deletion to a set's record: `transfer prepare` takes a fresh
//! epoch, the former coordinator's `transfer fenced` and the new one's `transfer complete`
//! activate it; `delete prepare` names the deletion identity, every coordinator's
//! `delete fenced` lets it commit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::membership::changeable;
use super::snapshot::{Deletion, DistributionSnapshot, OpenTransfer, Tombstone};
use super::{
    DataStrategy, DistributionError, DistributionMember, KeyRange, MemberState, MetadataCoverage,
    SetPhase, SignedStatement, Statement, StatementKind, TransferRole,
};
use crate::structs::identity::realm::RealmId;

const DELETION_DOMAIN: &[u8] = b"aruna-distribution-deletion-v1";
const LIFECYCLE_DOMAIN: &[u8] = b"aruna-distribution-lifecycle-v1";
/// Bounds of the descriptors a `transfer fenced` carries.
pub const MAX_WATERMARK_BYTES: usize = 1024;
pub const MAX_CURSOR_BYTES: usize = 4096;

/// What a range's transfer asks for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferPlan {
    pub transfer_id: Ulid,
    pub range_id: Ulid,
    pub to: Ulid,
    /// The least epoch the new coordinator takes; the record raises it above its maximum.
    pub epoch: u64,
    pub role: TransferRole,
}

impl Deletion {
    /// The deletion identity statements bind: the set and every member with its incarnation.
    pub fn identity(&self, set_id: Ulid) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(DELETION_DOMAIN);
        hasher.update(&set_id.to_bytes());
        for (member_id, incarnation) in &self.members {
            hasher.update(&member_id.to_bytes());
            hasher.update(&incarnation.to_be_bytes());
        }
        *hasher.finalize().as_bytes()
    }
}

/// A statement id derived from its signer, kind and subject, so a retry is embedded once.
fn lifecycle_id(set_id: Ulid, signer: Ulid, kind: &str, subject: &[u8]) -> Ulid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(LIFECYCLE_DOMAIN);
    hasher.update(&set_id.to_bytes());
    hasher.update(&signer.to_bytes());
    hasher.update(kind.as_bytes());
    hasher.update(subject);
    let mut id = [0; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ulid::from_bytes(id)
}

impl Statement {
    /// A statement of `signer` about `kind`; its id follows from the signer and the subject.
    pub fn lifecycle(
        realm_id: RealmId,
        set_id: Ulid,
        signer: &DistributionMember,
        subject: &[u8],
        kind: StatementKind,
    ) -> Self {
        Self {
            realm_id,
            set_id,
            statement_id: lifecycle_id(set_id, signer.member_id, kind.name(), subject),
            member_id: signer.member_id,
            incarnation: signer.incarnation.unwrap_or_default(),
            intent: signer.intent,
            kind,
        }
    }
}

impl DistributionSnapshot {
    pub fn open_transfer(&self, transfer_id: Ulid) -> Option<&OpenTransfer> {
        (self.lifecycle.transfers.iter()).find(|open| open.transfer_id == transfer_id)
    }

    /// The members that coordinate a range now.
    pub fn coordinators(&self) -> Vec<Ulid> {
        let mut coordinators: Vec<Ulid> =
            self.config.ranges.iter().map(|r| r.coordinator).collect();
        coordinators.sort();
        coordinators.dedup();
        coordinators
    }

    /// The members a transfer of `range` may go to: its ready `copy` replicas.
    pub fn transfer_targets(&self, range: &KeyRange) -> Vec<&DistributionMember> {
        (range.replicas.iter())
            .filter(|replica| **replica != range.coordinator)
            .filter_map(|replica| self.member(*replica))
            .filter(|m| m.state == MemberState::Ready && m.strategy == DataStrategy::Copy)
            .collect()
    }

    fn statement_digest(&self, digest: &[u8; 32]) -> Option<&SignedStatement> {
        (self.statements.iter()).find(|signed| signed.statement.digest() == *digest)
    }
}

/// Opens a transfer of one range to a ready `copy` replica of it, at a fresh epoch above
/// every epoch the record names. `signer` is the coordinator of an automatic transfer.
pub(super) fn prepare_transfer(
    next: &mut DistributionSnapshot,
    plan: &TransferPlan,
    signer: Option<Ulid>,
) -> Result<(), DistributionError> {
    changeable(next)?;
    let refused = DistributionError::InvalidChange;
    if next.phase != SetPhase::Active {
        return Err(refused("only an active set transfers ranges"));
    }
    let range = (next.config.ranges.iter())
        .find(|range| range.range_id == plan.range_id)
        .ok_or(DistributionError::NoTransfer(plan.transfer_id))?;
    if signer.is_some_and(|signer| signer != range.coordinator) {
        return Err(DistributionError::InvalidStatement(
            "only the range's coordinator proposes its transfer",
        ));
    }
    let open = &next.lifecycle.transfers;
    if open.iter().any(|open| open.range_id == plan.range_id) {
        return Err(DistributionError::TransferOpen(plan.range_id));
    }
    if (open.iter()).any(|open| open.transfer_id == plan.transfer_id || open.to == plan.to) {
        return Err(refused("the member is already receiving a transfer"));
    }
    next.member(plan.to)
        .ok_or(DistributionError::NoMember(plan.to))?;
    if !(next.transfer_targets(range).iter()).any(|target| target.member_id == plan.to) {
        return Err(refused(
            "the new coordinator must be a ready copy replica of the range",
        ));
    }
    let transfer = OpenTransfer {
        transfer_id: plan.transfer_id,
        range_id: plan.range_id,
        from: range.coordinator,
        to: plan.to,
        epoch: plan.epoch.max(next.max_epoch() + 1),
        role: plan.role,
        fenced: None,
        completed: None,
    };
    next.lifecycle.transfers.push(transfer);
    Ok(())
}

/// Takes the former coordinator's statement that it fenced the range with no open upload.
pub(super) fn transfer_fenced(
    next: &mut DistributionSnapshot,
    signed: &SignedStatement,
) -> Result<(), DistributionError> {
    changeable(next)?;
    let invalid = DistributionError::InvalidStatement;
    let StatementKind::TransferFenced {
        transfer_id,
        range_id,
        epoch,
        next_epoch,
        watermark,
        import_cursor,
        refresh_cursor,
        open_uploads,
    } = &signed.statement.kind
    else {
        return Err(invalid("not a transfer fenced statement"));
    };
    let bounded = watermark.len() <= MAX_WATERMARK_BYTES
        && (import_cursor.iter().chain(refresh_cursor)).all(|c| c.len() <= MAX_CURSOR_BYTES);
    if !bounded || *open_uploads != 0 {
        return Err(invalid(
            "a fenced range carries bounded descriptors and no upload",
        ));
    }
    let range_epoch = (next.config.ranges.iter())
        .find(|range| range.range_id == *range_id)
        .map(|range| range.epoch);
    let digest = signed.statement.digest();
    let open = (next.lifecycle.transfers.iter_mut())
        .find(|open| open.transfer_id == *transfer_id && open.range_id == *range_id)
        .ok_or(DistributionError::NoTransfer(*transfer_id))?;
    let matches = open.from == signed.statement.member_id
        && range_epoch == Some(*epoch)
        && open.epoch == *next_epoch
        && open.fenced.is_none();
    if !matches {
        return Err(invalid("the statement does not fence this transfer"));
    }
    open.fenced = Some(digest);
    next.statements.push(signed.clone());
    Ok(())
}

/// Takes the new coordinator's statement that it applied everything up to the fenced
/// watermark, and activates the transfer with both bound statements.
pub(super) fn transfer_complete(
    next: &mut DistributionSnapshot,
    signed: &SignedStatement,
) -> Result<(), DistributionError> {
    changeable(next)?;
    let invalid = DistributionError::InvalidStatement;
    let StatementKind::TransferComplete {
        transfer_id,
        range_id,
        fenced,
        watermark,
    } = &signed.statement.kind
    else {
        return Err(invalid("not a transfer complete statement"));
    };
    let fenced_watermark = match next.statement_digest(fenced).map(|s| &s.statement.kind) {
        Some(StatementKind::TransferFenced { watermark, .. }) => watermark.clone(),
        _ => return Err(invalid("the fenced statement is not in the record")),
    };
    let digest = signed.statement.digest();
    let open = (next.lifecycle.transfers.iter_mut())
        .find(|open| open.transfer_id == *transfer_id && open.range_id == *range_id)
        .ok_or(DistributionError::NoTransfer(*transfer_id))?;
    let matches = open.to == signed.statement.member_id
        && open.fenced == Some(*fenced)
        && open.completed.is_none()
        && fenced_watermark == *watermark;
    if !matches {
        return Err(invalid("the statement does not complete this transfer"));
    }
    open.completed = Some(digest);
    next.statements.push(signed.clone());
    activate_transfer(next, *transfer_id)
}

/// Moves the range to the new coordinator at the transfer's epoch once both bound statements
/// are in the record; the former coordinator stays a replica or leaves the range.
pub(super) fn activate_transfer(
    next: &mut DistributionSnapshot,
    transfer_id: Ulid,
) -> Result<(), DistributionError> {
    changeable(next)?;
    let index = (next.lifecycle.transfers.iter())
        .position(|open| open.transfer_id == transfer_id)
        .ok_or(DistributionError::NoTransfer(transfer_id))?;
    let open = next.lifecycle.transfers[index].clone();
    let (Some(fenced), Some(completed)) = (open.fenced, open.completed) else {
        return Err(DistributionError::InvalidChange(
            "a transfer activates with its fenced and complete statements only",
        ));
    };
    let range = (next.config.ranges.iter_mut())
        .find(|range| range.range_id == open.range_id)
        .ok_or(DistributionError::NoTransfer(transfer_id))?;
    range.coordinator = open.to;
    range.epoch = open.epoch;
    range.replicas.retain(|replica| *replica != open.to);
    if open.role == TransferRole::Replica {
        range.replicas.push(open.from);
    }
    let holds = (next.config.ranges.iter())
        .any(|range| range.coordinator == open.from || range.replicas.contains(&open.from));
    if let Some(former) = (next.config.members.iter_mut()).find(|m| m.member_id == open.from)
        && former.coverage == MetadataCoverage::Assigned
        && !holds
    {
        // A former coordinator that keeps no range leaves the set.
        former.state = MemberState::Leaving;
    }
    next.lifecycle.transfers.remove(index);
    next.statements.retain(|signed| {
        let digest = signed.statement.digest();
        digest != fenced && digest != completed
    });
    Ok(())
}

/// Records the deletion identity; it must name exactly the admitted members.
pub(super) fn prepare_deletion(
    next: &mut DistributionSnapshot,
    members: &[(Ulid, u128)],
) -> Result<(), DistributionError> {
    if next.lifecycle.deletion.is_some() {
        return Err(DistributionError::Deleting);
    }
    let mut admitted: Vec<(Ulid, u128)> = (next.config.members.iter())
        .filter_map(|member| Some((member.member_id, member.incarnation?)))
        .collect();
    admitted.sort();
    let mut named = members.to_vec();
    named.sort();
    if named != admitted {
        return Err(DistributionError::InvalidChange(
            "the deletion identity does not name the admitted members",
        ));
    }
    next.lifecycle.deletion = Some(Deletion {
        members: named,
        fenced: Vec::new(),
        committed: false,
    });
    Ok(())
}

/// Takes a coordinator's statement that it fenced all its ranges for the deletion; the last
/// one commits it.
pub(super) fn deletion_fenced(
    next: &mut DistributionSnapshot,
    signed: &SignedStatement,
) -> Result<(), DistributionError> {
    let invalid = DistributionError::InvalidStatement;
    let StatementKind::DeleteFenced { deletion } = &signed.statement.kind else {
        return Err(invalid("not a delete fenced statement"));
    };
    let set_id = next.set_id();
    let signer = signed.statement.member_id;
    let coordinates = next.config.ranges.iter().any(|r| r.coordinator == signer);
    let current = next
        .lifecycle
        .deletion
        .as_mut()
        .ok_or(DistributionError::NoDeletion)?;
    if current.identity(set_id) != *deletion || current.committed {
        return Err(invalid("the statement does not fence this deletion"));
    }
    if !coordinates {
        return Err(invalid("only coordinators fence for a deletion"));
    }
    if !current.fenced.contains(&signer) {
        current.fenced.push(signer);
    }
    next.statements.push(signed.clone());
    match next.coordinators().iter().all(|id| fenced_by(next, *id)) {
        true => commit_deletion(next),
        false => Ok(()),
    }
}

fn fenced_by(snapshot: &DistributionSnapshot, member_id: Ulid) -> bool {
    (snapshot.lifecycle.deletion.as_ref()).is_some_and(|d| d.fenced.contains(&member_id))
}

/// Commits the deletion once every coordinator fenced: members purge and every member bucket
/// of the identity gets a tombstone.
pub(super) fn commit_deletion(next: &mut DistributionSnapshot) -> Result<(), DistributionError> {
    match &next.lifecycle.deletion {
        None => return Err(DistributionError::NoDeletion),
        Some(deletion) if deletion.committed => {
            return Err(DistributionError::InvalidChange(
                "the deletion is already committed",
            ));
        }
        Some(_) => {}
    }
    if let Some(missing) = (next.coordinators().into_iter()).find(|id| !fenced_by(next, *id)) {
        return Err(DistributionError::NotFenced(missing));
    }
    let deletion = next
        .lifecycle
        .deletion
        .as_mut()
        .ok_or(DistributionError::NoDeletion)?;
    deletion.committed = true;
    let members = deletion.members.clone();
    for (member_id, incarnation) in members {
        if !next.tombstoned(member_id, incarnation) {
            next.tombstones.push(Tombstone {
                member_id,
                incarnation,
            });
        }
    }
    // The fence statements served the commit; the committed deletion stays in the record.
    next.statements
        .retain(|signed| !matches!(signed.statement.kind, StatementKind::DeleteFenced { .. }));
    Ok(())
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
