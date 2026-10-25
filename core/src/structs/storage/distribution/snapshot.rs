//! Defines the signed full snapshot the publisher issues per revision and how a receiver keeps
//! the highest valid one; two different snapshots of one number are publisher equivocation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::split::{RetiredRange, check_retired};
use super::validate::validate_config;
use super::{
    DistributionConfig, DistributionError, DistributionMember, DistributionRevision, KeyRange,
    MAX_RECORD_BYTES, MemberState, SetPhase, SetRoot, SignedStatement, StatementKind, TransferRole,
};
use crate::structs::identity::realm::RealmId;

const SNAPSHOT_DOMAIN: &str = "aruna-distribution-snapshot-v1";
const DIGEST_DOMAIN: &[u8] = b"aruna-distribution-revision-v2";

/// A transfer between prepare and activate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OpenTransfer {
    pub transfer_id: Ulid,
    pub range_id: Ulid,
    pub from: Ulid,
    pub to: Ulid,
    pub epoch: u64,
    pub role: TransferRole,
    /// Digests of the embedded `transfer fenced` and `transfer complete` statements.
    pub fenced: Option<[u8; 32]>,
    pub completed: Option<[u8; 32]>,
}

/// A deletion between prepare and the last member's purge.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Deletion {
    /// The deletion identity: every member id with its local bucket incarnation.
    pub members: Vec<(Ulid, u128)>,
    pub fenced: Vec<Ulid>,
    pub committed: bool,
}

/// Lifecycle state a snapshot keeps open; a newer snapshot never silently drops a fence.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Lifecycle {
    pub transfers: Vec<OpenTransfer>,
    pub deletion: Option<Deletion>,
}

/// A member bucket the set deleted or released, so a returning member learns of it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Tombstone {
    pub member_id: Ulid,
    pub incarnation: u128,
}

/// One self-contained revision of a set: everything a receiver needs, no history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DistributionSnapshot {
    pub root: SetRoot,
    pub number: u64,
    /// The digest of the previous revision; `None` only for the create revision.
    pub previous: Option<[u8; 32]>,
    pub phase: SetPhase,
    pub config: DistributionConfig,
    pub lifecycle: Lifecycle,
    pub statements: Vec<SignedStatement>,
    pub tombstones: Vec<Tombstone>,
    /// The last revision an admin request produced; older automatic requests are stale.
    pub admin_number: u64,
    /// Ranges splits replaced, so ancestor ids in requests and uploads still resolve.
    pub retired: Vec<RetiredRange>,
}

impl DistributionSnapshot {
    pub fn set_id(&self) -> Ulid {
        self.root.set_id()
    }

    /// The digest over the canonical encoding; the next revision names it as `previous`.
    pub fn digest(&self) -> [u8; 32] {
        let bytes = postcard::to_allocvec(self).unwrap_or_default();
        let mut hasher = blake3::Hasher::new();
        hasher.update(DIGEST_DOMAIN);
        hasher.update(&bytes);
        *hasher.finalize().as_bytes()
    }

    pub fn revision(&self) -> DistributionRevision {
        DistributionRevision {
            number: self.number,
            digest: self.digest(),
        }
    }

    pub fn member(&self, member_id: Ulid) -> Option<&DistributionMember> {
        self.config
            .members
            .iter()
            .find(|member| member.member_id == member_id)
    }

    /// The range holding `key`; ranges are complete, so a valid snapshot always has one.
    pub fn range_for(&self, key: &str) -> Option<&KeyRange> {
        let ranges = &self.config.ranges;
        let index = ranges.partition_point(|range| range.lower.as_str() <= key);
        index
            .checked_sub(1)
            .and_then(|index| ranges.get(index))
            .filter(|range| range.contains(key))
    }

    /// The bucket serves once it is active and every coordinator is ready.
    pub fn serving(&self) -> bool {
        self.phase == SetPhase::Active
            && self.config.ranges.iter().all(|range| {
                self.member(range.coordinator)
                    .is_some_and(|member| member.state == MemberState::Ready)
            })
    }

    pub fn signing_bytes(&self, realm_id: RealmId) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(&(SNAPSHOT_DOMAIN, realm_id, self))
    }

    /// Checks content that needs no signature: configuration, admissions and statements.
    pub fn validate(&self, realm_id: RealmId) -> Result<(), DistributionError> {
        let malformed = DistributionError::Malformed;
        if (self.number == 1) != self.previous.is_none() || self.number == 0 {
            return Err(malformed(
                "only the create revision has no previous revision",
            ));
        }
        validate_config(&self.config)?;
        check_retired(self)?;
        if self.config.owner_group != self.root.owner_group {
            return Err(DistributionError::ForeignSet(self.set_id()));
        }
        if self
            .config
            .members
            .iter()
            .any(|member| member.intent == [0; 32])
        {
            return Err(malformed("a member names no intent"));
        }
        let mut ids = BTreeSet::new();
        for signed in &self.statements {
            if !ids.insert(signed.statement.statement_id) {
                return Err(DistributionError::Replayed(signed.statement.statement_id));
            }
            self.check_statement(realm_id, signed)?;
        }
        for member in &self.config.members {
            self.check_admission(member)?;
        }
        // An active set's coordinators are admitted; a replica only once it joined and admitted.
        if self.phase == SetPhase::Active {
            let missing = (self.config.ranges.iter())
                .flat_map(|range| std::iter::once(&range.coordinator).chain(&range.replicas))
                .find(|member_id| {
                    self.member(**member_id).is_none_or(|member| {
                        member.incarnation.is_none() && member.state != MemberState::Created
                    })
                });
            let unadmitted = (self.config.ranges.iter()).find(|range| {
                self.member(range.coordinator)
                    .is_none_or(|member| member.incarnation.is_none())
            });
            if let Some(member_id) = missing.or(unadmitted.map(|range| &range.coordinator)) {
                return Err(DistributionError::MissingAdmission(*member_id));
            }
        }
        Ok(())
    }

    /// The first coordinator or replica that has not admitted itself, if any.
    pub fn unadmitted(&self) -> Option<Ulid> {
        (self.config.ranges.iter())
            .flat_map(|range| std::iter::once(&range.coordinator).chain(&range.replicas))
            .find(|member_id| {
                self.member(**member_id)
                    .is_none_or(|member| member.incarnation.is_none())
            })
            .copied()
    }

    /// An admitted member needs its own signed admission for its incarnation; a member that
    /// serves or catches up must be admitted, a created one must not be.
    fn check_admission(&self, member: &DistributionMember) -> Result<(), DistributionError> {
        let serving = matches!(
            member.state,
            MemberState::CatchingUp | MemberState::Ready | MemberState::Leaving
        );
        let Some(incarnation) = member.incarnation else {
            return match serving {
                true => Err(DistributionError::MissingAdmission(member.member_id)),
                false => Ok(()),
            };
        };
        if member.state == MemberState::Created {
            return Err(DistributionError::Malformed(
                "a created member has no incarnation yet",
            ));
        }
        // `check_statement` already verified each statement's signer, intent and incarnation.
        let proven = self.statements.iter().any(|signed| {
            let statement = &signed.statement;
            matches!(statement.kind, StatementKind::Admit)
                && statement.member_id == member.member_id
                && statement.incarnation == incarnation
        });
        match proven {
            true => Ok(()),
            false => Err(DistributionError::MissingAdmission(member.member_id)),
        }
    }

    /// A statement holds only with its own signer's signature, from the member's node, for
    /// this realm and set, answering the member's current intent.
    pub fn check_statement(
        &self,
        realm_id: RealmId,
        signed: &SignedStatement,
    ) -> Result<(), DistributionError> {
        let statement = &signed.statement;
        let invalid = DistributionError::InvalidStatement;
        if !signed.verified() {
            return Err(invalid("signature does not verify"));
        }
        if statement.realm_id != realm_id || statement.set_id != self.set_id() {
            return Err(invalid("statement names another realm or set"));
        }
        let member = self
            .member(statement.member_id)
            .ok_or(DistributionError::NoMember(statement.member_id))?;
        if member.node_id != signed.signer {
            return Err(DistributionError::AdmissionOrigin(statement.member_id));
        }
        if matches!(statement.kind, StatementKind::Admit)
            && (statement.intent != member.intent
                || member
                    .incarnation
                    .is_some_and(|known| known != statement.incarnation))
        {
            return Err(invalid("admission does not answer the member's intent"));
        }
        Ok(())
    }
}

/// A snapshot the publisher asks its own node key to sign for one realm.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotClaim {
    pub realm_id: RealmId,
    pub snapshot: DistributionSnapshot,
}

/// A snapshot with the publisher's signature over the realm and its canonical encoding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedSnapshot {
    pub snapshot: DistributionSnapshot,
    pub signature: iroh::Signature,
}

impl SignedSnapshot {
    pub fn sign(
        snapshot: DistributionSnapshot,
        realm_id: RealmId,
        secret: &iroh::SecretKey,
    ) -> Result<Self, DistributionError> {
        let bytes = snapshot
            .signing_bytes(realm_id)
            .map_err(|_| DistributionError::TooLarge)?;
        Ok(Self {
            signature: secret.sign(&bytes),
            snapshot,
        })
    }

    /// Whether the set's bound publisher signed this snapshot for this realm.
    pub fn publisher_signed(&self, realm_id: RealmId) -> bool {
        self.snapshot.signing_bytes(realm_id).is_ok_and(|bytes| {
            self.snapshot
                .root
                .publisher
                .verify(&bytes, &self.signature)
                .is_ok()
        })
    }

    /// Everything a receiver checks before keeping a snapshot: signature, size and content.
    pub fn verify(&self, realm_id: RealmId) -> Result<(), DistributionError> {
        if !self.publisher_signed(realm_id) {
            return Err(DistributionError::InvalidStatement(
                "snapshot is not signed by the set's publisher",
            ));
        }
        let size = postcard::to_allocvec(self)
            .map_err(|_| DistributionError::TooLarge)?
            .len();
        if size > MAX_RECORD_BYTES {
            return Err(DistributionError::TooLarge);
        }
        self.snapshot.validate(realm_id)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::errors::ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::errors::ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// What a receiver holds for one set: the highest valid snapshot and, after publisher
/// equivocation, the conflicting one as evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DistributionView {
    pub head: SignedSnapshot,
    pub conflict: Option<SignedSnapshot>,
}

/// The outcome of offering a verified snapshot to a view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Installed {
    Changed(Box<DistributionView>),
    Unchanged,
}

impl DistributionView {
    /// Configuration changes stop once the publisher signed two revisions of one number.
    pub fn conflicted(&self) -> bool {
        self.conflict.is_some()
    }

    pub fn snapshot(&self) -> &DistributionSnapshot {
        &self.head.snapshot
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::errors::ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::errors::ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Keeps the highest snapshot; older and equal ones are ignored, a different one of the same
/// number, or a successor that names another predecessor, marks the set conflicted with no
/// winner. `incoming` must already be verified.
pub fn install(
    current: Option<&DistributionView>,
    incoming: SignedSnapshot,
) -> Result<Installed, DistributionError> {
    let Some(view) = current else {
        return Ok(Installed::Changed(Box::new(DistributionView {
            head: incoming,
            conflict: None,
        })));
    };
    let head = &view.head.snapshot;
    let next = &incoming.snapshot;
    if next.root != head.root {
        return Err(DistributionError::ForeignSet(next.set_id()));
    }
    if view.conflicted() {
        return Ok(Installed::Unchanged);
    }
    let head_digest = head.digest();
    let forked = match next.number.cmp(&head.number) {
        std::cmp::Ordering::Equal => next.digest() != head_digest,
        std::cmp::Ordering::Greater => {
            next.number == head.number + 1 && next.previous != Some(head_digest)
        }
        std::cmp::Ordering::Less => {
            next.number + 1 == head.number && head.previous != Some(next.digest())
        }
    };
    if forked {
        return Ok(Installed::Changed(Box::new(DistributionView {
            head: view.head.clone(),
            conflict: Some(incoming),
        })));
    }
    if next.number > head.number {
        return Ok(Installed::Changed(Box::new(DistributionView {
            head: incoming,
            conflict: None,
        })));
    }
    Ok(Installed::Unchanged)
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
