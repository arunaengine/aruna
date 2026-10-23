//! Defines the changes of a distribution record and applies one change to the record it names.
//! Each change names its set and predecessor revision; a stale predecessor is refused.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::validate::{validate_config, validate_record};
use super::{
    DistributionConfig, DistributionError, DistributionMember, DistributionRecord,
    DistributionRevision, DistributionSettings, MAX_BOUND_BYTES, MAX_MEMBERS, MemberState,
    SetPhase,
};
use crate::NodeId;

/// Fence and transfer descriptors stay bounded; per-object data moves as paginated local data.
pub const MAX_DESCRIPTOR_BYTES: usize = 1024;
const REVISION_DOMAIN: &[u8] = b"aruna-distribution-revision-v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DistributionChange {
    pub set_id: Ulid,
    /// The revision this change builds on; `None` only for `create`.
    pub predecessor: Option<DistributionRevision>,
    pub op: DistributionOp,
}

/// What the former coordinator becomes after a transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum TransferRole {
    Replica,
    Leave,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DistributionOp {
    /// A pending set with its full configuration.
    Create {
        config: DistributionConfig,
    },
    /// A member's own node binds its local bucket incarnation.
    Admit {
        member_id: Ulid,
        incarnation: u128,
    },
    /// Needs the admissions of every coordinator and replica.
    Activate,
    ChangeSettings {
        settings: DistributionSettings,
    },
    Join {
        member: DistributionMember,
    },
    Leave {
        member_id: Ulid,
    },
    Split {
        range_id: Ulid,
        split_key: String,
        lower_id: Ulid,
        upper_id: Ulid,
    },
    TransferPrepare {
        transfer_id: Ulid,
        range_id: Ulid,
        from: Ulid,
        to: Ulid,
        epoch: u64,
        role: TransferRole,
    },
    TransferFenced {
        transfer_id: Ulid,
        range_id: Ulid,
        watermark: Vec<u8>,
        import_cursor: Option<Vec<u8>>,
        refresh_cursor: Option<Vec<u8>>,
        open_uploads: u64,
    },
    TransferComplete {
        transfer_id: Ulid,
        range_id: Ulid,
    },
    TransferActivate {
        transfer_id: Ulid,
        range_id: Ulid,
    },
    /// The deletion identity: every member id with its local bucket incarnation.
    DeletePrepare {
        members: Vec<(Ulid, u128)>,
    },
    DeleteFenced {
        member_id: Ulid,
    },
    DeleteCommit,
}

/// Who must have published a change: checked against the origin or the actor on receive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeAuthority {
    /// Group admin WRITE on the set's owner group (P1).
    GroupAdmin,
    /// The node of this member, as the authenticated origin (K1).
    MemberNode(Ulid),
    /// The node coordinating this range.
    Coordinator(Ulid),
    /// The receiving member of this transfer.
    TransferTarget(Ulid),
}

impl DistributionOp {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Create { .. } => "create",
            Self::Admit { .. } => "admit",
            Self::Activate => "activate",
            Self::ChangeSettings { .. } => "change settings",
            Self::Join { .. } => "join",
            Self::Leave { .. } => "leave",
            Self::Split { .. } => "split",
            Self::TransferPrepare { .. } => "transfer prepare",
            Self::TransferFenced { .. } => "transfer fenced",
            Self::TransferComplete { .. } => "transfer complete",
            Self::TransferActivate { .. } => "transfer activate",
            Self::DeletePrepare { .. } => "delete prepare",
            Self::DeleteFenced { .. } => "delete fenced",
            Self::DeleteCommit => "delete commit",
        }
    }

    pub fn authority(&self) -> ChangeAuthority {
        match self {
            Self::Create { .. }
            | Self::Activate
            | Self::ChangeSettings { .. }
            | Self::Join { .. }
            | Self::Leave { .. }
            | Self::TransferPrepare { .. }
            | Self::TransferActivate { .. }
            | Self::DeletePrepare { .. }
            | Self::DeleteCommit => ChangeAuthority::GroupAdmin,
            Self::Admit { member_id, .. } | Self::DeleteFenced { member_id } => {
                ChangeAuthority::MemberNode(*member_id)
            }
            Self::TransferFenced { range_id, .. } | Self::Split { range_id, .. } => {
                ChangeAuthority::Coordinator(*range_id)
            }
            Self::TransferComplete { transfer_id, .. } => {
                ChangeAuthority::TransferTarget(*transfer_id)
            }
        }
    }

    /// Whether the reducer applies this change yet; later packages fill in the rest.
    pub fn supported(&self) -> bool {
        matches!(
            self,
            Self::Create { .. } | Self::Admit { .. } | Self::Activate | Self::ChangeSettings { .. }
        )
    }
}

impl DistributionChange {
    /// Shape rules that need no record: create carries a fresh configuration, every
    /// other change a predecessor, and descriptors stay bounded.
    pub fn validate_shape(&self) -> Result<(), DistributionError> {
        let malformed = DistributionError::Malformed;
        match (&self.op, &self.predecessor) {
            (DistributionOp::Create { .. }, Some(_)) => {
                return Err(malformed("create has no predecessor"));
            }
            (DistributionOp::Create { .. }, None) => {}
            (_, None) => return Err(malformed("only create may omit the predecessor")),
            (_, Some(_)) => {}
        }
        match &self.op {
            DistributionOp::Create { config } => {
                validate_config(config)?;
                let fresh = config.members.iter().all(|member| {
                    member.state == MemberState::Created && member.incarnation.is_none()
                });
                fresh.then_some(()).ok_or(DistributionError::InvalidCreate)
            }
            DistributionOp::ChangeSettings { settings } if settings.refresh_secs == Some(0) => {
                Err(DistributionError::InvalidRefresh)
            }
            DistributionOp::Split {
                split_key,
                lower_id,
                upper_id,
                ..
            } if split_key.is_empty()
                || split_key.len() > MAX_BOUND_BYTES
                || lower_id == upper_id =>
            {
                Err(malformed("split needs a bounded key and two new range ids"))
            }
            DistributionOp::TransferFenced {
                watermark,
                import_cursor,
                refresh_cursor,
                open_uploads,
                ..
            } => {
                let bounded = [
                    Some(watermark),
                    import_cursor.as_ref(),
                    refresh_cursor.as_ref(),
                ]
                .into_iter()
                .flatten()
                .all(|bytes| bytes.len() <= MAX_DESCRIPTOR_BYTES);
                (bounded && *open_uploads == 0)
                    .then_some(())
                    .ok_or(malformed(
                        "a fence needs bounded descriptors and no open upload",
                    ))
            }
            DistributionOp::DeletePrepare { members } if members.len() > MAX_MEMBERS => {
                Err(malformed("delete prepare names too many members"))
            }
            _ => Ok(()),
        }
    }
}

/// Applies one change published by `origin` to `current`. `link` identifies the publishing
/// event and chains into the new revision's digest. Pure and deterministic.
pub fn apply_change(
    current: Option<&DistributionRecord>,
    change: &DistributionChange,
    origin: NodeId,
    link: &[u8],
) -> Result<DistributionRecord, DistributionError> {
    change.validate_shape()?;
    if let Some(record) = current
        && record.set_id != change.set_id
    {
        return Err(DistributionError::SetMismatch(change.set_id));
    }
    if current.map(|record| record.revision) != change.predecessor {
        return Err(DistributionError::StaleRevision);
    }
    let mut next = match (&change.op, current) {
        (DistributionOp::Create { config }, None) => DistributionRecord {
            set_id: change.set_id,
            revision: DistributionRevision::default(),
            phase: SetPhase::Pending,
            config: config.clone(),
        },
        (_, None) => return Err(DistributionError::StaleRevision),
        (DistributionOp::Create { .. }, Some(_)) => return Err(DistributionError::StaleRevision),
        (op, Some(record)) => {
            let mut next = record.clone();
            apply_op(&mut next, op, origin)?;
            next
        }
    };
    next.revision = next_revision(change.predecessor, link);
    validate_record(&next)?;
    Ok(next)
}

fn apply_op(
    record: &mut DistributionRecord,
    op: &DistributionOp,
    origin: NodeId,
) -> Result<(), DistributionError> {
    match op {
        DistributionOp::Admit {
            member_id,
            incarnation,
        } => {
            if record.phase != SetPhase::Pending {
                return Err(DistributionError::NotPending);
            }
            let member = record
                .config
                .members
                .iter_mut()
                .find(|member| member.member_id == *member_id)
                .ok_or(DistributionError::NoMember(*member_id))?;
            if member.node_id != origin {
                return Err(DistributionError::AdmissionOrigin(*member_id));
            }
            if member
                .incarnation
                .is_some_and(|known| known != *incarnation)
            {
                return Err(DistributionError::IncarnationChanged(*member_id));
            }
            // A new set holds nothing yet, so an admitted member has nothing to catch up.
            member.incarnation = Some(*incarnation);
            member.state = MemberState::Ready;
            Ok(())
        }
        DistributionOp::Activate => {
            if record.phase != SetPhase::Pending {
                return Err(DistributionError::NotPending);
            }
            let missing = record
                .config
                .ranges
                .iter()
                .flat_map(|range| std::iter::once(&range.coordinator).chain(&range.replicas))
                .find(|member_id| {
                    record
                        .member(**member_id)
                        .is_none_or(|member| member.incarnation.is_none())
                });
            if let Some(member_id) = missing {
                return Err(DistributionError::MissingAdmission(*member_id));
            }
            record.phase = SetPhase::Active;
            Ok(())
        }
        DistributionOp::ChangeSettings { settings } => {
            record.config.settings = settings.clone();
            Ok(())
        }
        DistributionOp::Create { .. } => Err(DistributionError::StaleRevision),
        DistributionOp::Join { .. }
        | DistributionOp::Leave { .. }
        | DistributionOp::Split { .. }
        | DistributionOp::TransferPrepare { .. }
        | DistributionOp::TransferFenced { .. }
        | DistributionOp::TransferComplete { .. }
        | DistributionOp::TransferActivate { .. }
        | DistributionOp::DeletePrepare { .. }
        | DistributionOp::DeleteFenced { .. }
        | DistributionOp::DeleteCommit => Err(DistributionError::Unsupported(op.name())),
    }
}

fn next_revision(predecessor: Option<DistributionRevision>, link: &[u8]) -> DistributionRevision {
    let previous = predecessor.unwrap_or_default();
    let mut hasher = blake3::Hasher::new();
    hasher.update(REVISION_DOMAIN);
    hasher.update(&previous.number.to_be_bytes());
    hasher.update(&previous.digest);
    hasher.update(link);
    DistributionRevision {
        number: previous.number.saturating_add(1),
        digest: *hasher.finalize().as_bytes(),
    }
}
