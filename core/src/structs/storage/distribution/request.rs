//! Defines the requests a set's publisher accepts and derives the next snapshot from its head.
//! Admin requests come with the requester's authority; statements carry their signer's.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::snapshot::{DistributionSnapshot, Lifecycle};
use super::validate::validate_config;
use super::{
    DistributionConfig, DistributionError, DistributionMember, DistributionSettings,
    MAX_RECORD_BYTES, MemberState, SetPhase, SetRoot, SignedStatement, StatementKind,
};
use crate::structs::identity::realm::RealmId;

/// What the former coordinator becomes after a transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum TransferRole {
    Replica,
    Leave,
}

const INTENT_DOMAIN: &str = "aruna-distribution-intent-v1";

/// Room the signature and envelope take next to an encoded snapshot.
const SIGNED_OVERHEAD: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DistributionRequest {
    /// A pending set with its full configuration and the preimage of its id.
    Create {
        root: SetRoot,
        config: DistributionConfig,
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
    TransferPrepare {
        transfer_id: Ulid,
        range_id: Ulid,
        to: Ulid,
        epoch: u64,
        role: TransferRole,
    },
    TransferActivate {
        transfer_id: Ulid,
        range_id: Ulid,
    },
    /// The deletion identity: every member id with its local bucket incarnation.
    DeletePrepare {
        members: Vec<(Ulid, u128)>,
    },
    DeleteCommit,
    /// A member's or coordinator's signed statement for the publisher to embed.
    Statement(Box<SignedStatement>),
}

impl DistributionRequest {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Create { .. } => "create",
            Self::Activate => "activate",
            Self::ChangeSettings { .. } => "change settings",
            Self::Join { .. } => "join",
            Self::Leave { .. } => "leave",
            Self::TransferPrepare { .. } => "transfer prepare",
            Self::TransferActivate { .. } => "transfer activate",
            Self::DeletePrepare { .. } => "delete prepare",
            Self::DeleteCommit => "delete commit",
            Self::Statement(signed) => signed.statement.kind.name(),
        }
    }

    /// Admin requests need group admin WRITE on the owner group when the publisher accepts them.
    pub fn admin(&self) -> bool {
        !matches!(self, Self::Statement(_))
    }
}

/// Derives the revision after `head` that accepts `request`. Pure: the publisher commits the
/// result with its signature; nothing else ever produces a revision.
pub fn next_snapshot(
    realm_id: RealmId,
    head: Option<&DistributionSnapshot>,
    request: &DistributionRequest,
) -> Result<DistributionSnapshot, DistributionError> {
    let next = match (head, request) {
        (None, DistributionRequest::Create { root, config }) => created(root, config)?,
        (Some(head), DistributionRequest::Create { .. }) => {
            return Err(DistributionError::SetMismatch(head.set_id()));
        }
        (None, _) => return Err(DistributionError::UnknownSet),
        (Some(head), request) => {
            let mut next = head.clone();
            accept(realm_id, &mut next, request)?;
            next.number = head.number + 1;
            next.previous = Some(head.digest());
            if request.admin() {
                next.admin_number = next.number;
            }
            next
        }
    };
    next.validate(realm_id)?;
    let size = postcard::to_allocvec(&next)
        .map_err(|_| DistributionError::TooLarge)?
        .len();
    if size + SIGNED_OVERHEAD > MAX_RECORD_BYTES {
        return Err(DistributionError::TooLarge);
    }
    Ok(next)
}

fn created(
    root: &SetRoot,
    config: &DistributionConfig,
) -> Result<DistributionSnapshot, DistributionError> {
    if root.owner_group != config.owner_group {
        return Err(DistributionError::ForeignSet(root.set_id()));
    }
    validate_config(config)?;
    let fresh = config.members.iter().all(|member| {
        member.state == MemberState::Created
            && member.incarnation.is_none()
            && member.intent == [0; 32]
    });
    if !fresh {
        return Err(DistributionError::InvalidCreate);
    }
    let intent = create_intent(root, config);
    let mut config = config.clone();
    for member in &mut config.members {
        member.intent = intent;
    }
    Ok(DistributionSnapshot {
        root: root.clone(),
        number: 1,
        previous: None,
        phase: SetPhase::Pending,
        config,
        lifecycle: Lifecycle::default(),
        statements: Vec::new(),
        tombstones: Vec::new(),
        admin_number: 1,
    })
}

/// The digest of a create request's content: the root and the configuration it creates.
/// Two different creates never share it, so an admission cannot move between them.
pub fn create_intent(root: &SetRoot, config: &DistributionConfig) -> [u8; 32] {
    let bytes = postcard::to_allocvec(&(INTENT_DOMAIN, root, config)).unwrap_or_default();
    *blake3::hash(&bytes).as_bytes()
}

fn accept(
    realm_id: RealmId,
    next: &mut DistributionSnapshot,
    request: &DistributionRequest,
) -> Result<(), DistributionError> {
    match request {
        DistributionRequest::Statement(signed) => embed(realm_id, next, signed),
        DistributionRequest::Activate => {
            if next.phase != SetPhase::Pending {
                return Err(DistributionError::NotPending);
            }
            next.phase = SetPhase::Active;
            // Snapshot validation names the first coordinator or replica not yet admitted.
            Ok(())
        }
        DistributionRequest::ChangeSettings { settings } => {
            if settings.refresh_secs == Some(0) {
                return Err(DistributionError::InvalidRefresh);
            }
            next.config.settings = settings.clone();
            Ok(())
        }
        DistributionRequest::Create { .. } => Err(DistributionError::SetMismatch(next.set_id())),
        DistributionRequest::Join { .. }
        | DistributionRequest::Leave { .. }
        | DistributionRequest::TransferPrepare { .. }
        | DistributionRequest::TransferActivate { .. }
        | DistributionRequest::DeletePrepare { .. }
        | DistributionRequest::DeleteCommit => Err(DistributionError::Unsupported(request.name())),
    }
}

/// Embeds a statement once: its own signature, member and intent are checked, and an
/// automatic request older than the last admin revision is dropped.
fn embed(
    realm_id: RealmId,
    next: &mut DistributionSnapshot,
    signed: &SignedStatement,
) -> Result<(), DistributionError> {
    let statement = &signed.statement;
    if next
        .statements
        .iter()
        .any(|known| known.statement.statement_id == statement.statement_id)
    {
        return Err(DistributionError::Replayed(statement.statement_id));
    }
    if statement
        .kind
        .basis()
        .is_some_and(|basis| basis < next.admin_number)
    {
        return Err(DistributionError::StaleRevision);
    }
    next.check_statement(realm_id, signed)?;
    match &statement.kind {
        StatementKind::Admit => {
            // After activation only a member that serves no range may still admit itself.
            let late = next.phase != SetPhase::Pending;
            let serves = next.config.ranges.iter().any(|range| {
                range.coordinator == statement.member_id
                    || range.replicas.contains(&statement.member_id)
            });
            if late && serves {
                return Err(DistributionError::NotPending);
            }
            let member = next
                .config
                .members
                .iter_mut()
                .find(|member| member.member_id == statement.member_id)
                .ok_or(DistributionError::NoMember(statement.member_id))?;
            // A pending set holds nothing yet; a later member catches up first.
            member.incarnation = Some(statement.incarnation);
            member.state = match late {
                true => MemberState::CatchingUp,
                false => MemberState::Ready,
            };
            next.statements.retain(|known| {
                known.statement.member_id != statement.member_id
                    || !matches!(known.statement.kind, StatementKind::Admit)
            });
            next.statements.push(signed.clone());
            Ok(())
        }
        StatementKind::CaughtUp {
            member_id, intent, ..
        } => {
            next.statements.push(signed.clone());
            caught_up(next, statement.member_id, *member_id, *intent)
        }
        kind => Err(DistributionError::Unsupported(kind.name())),
    }
}

/// Takes a coordinator's catch-up statement for `target` and makes the member ready once every
/// coordinator of a range it holds stated it; the statements then leave the record.
fn caught_up(
    next: &mut DistributionSnapshot,
    signer: Ulid,
    target: Ulid,
    intent: [u8; 32],
) -> Result<(), DistributionError> {
    let member = next
        .member(target)
        .ok_or(DistributionError::NoMember(target))?;
    if member.state != MemberState::CatchingUp || member.intent != intent {
        return Err(DistributionError::InvalidStatement(
            "the member is not catching up for this intent",
        ));
    }
    let coordinators: BTreeSet<Ulid> = next
        .held_ranges(target)
        .iter()
        .map(|range| range.coordinator)
        .filter(|coordinator| *coordinator != target)
        .collect();
    if !coordinators.contains(&signer) {
        return Err(DistributionError::InvalidStatement(
            "the signer coordinates no range the member holds",
        ));
    }
    let stated = |signed: &SignedStatement| match signed.statement.kind {
        StatementKind::CaughtUp {
            member_id,
            intent: stated,
            ..
        } => member_id == target && stated == intent,
        _ => false,
    };
    let signers: BTreeSet<Ulid> = (next.statements.iter())
        .filter(|signed| stated(signed))
        .map(|signed| signed.statement.member_id)
        .collect();
    if coordinators.is_subset(&signers) {
        next.statements.retain(|signed| !stated(signed));
        if let Some(member) =
            (next.config.members.iter_mut()).find(|member| member.member_id == target)
        {
            member.state = MemberState::Ready;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;
