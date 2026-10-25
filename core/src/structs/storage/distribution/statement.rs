//! Defines the set root that fixes a distributed bucket's publisher and owner group, and the
//! signed statements member and coordinator nodes send the publisher to embed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::DistributionMember;
use super::request::TransferRole;
use crate::NodeId;
use crate::structs::identity::realm::RealmId;
use crate::types::GroupId;

const SET_DOMAIN: &[u8] = b"aruna-distribution-set-v1";
const STATEMENT_DOMAIN: &str = "aruna-distribution-statement-v1";
const ADMIT_DOMAIN: &[u8] = b"aruna-distribution-admit-v1";
const CAUGHT_DOMAIN: &[u8] = b"aruna-distribution-caught-up-v1";

/// The preimage of a set id: whoever creates the set carries it, every receiver recomputes it.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SetRoot {
    /// The only node that signs revisions of the set.
    pub publisher: NodeId,
    pub owner_group: GroupId,
    pub nonce: [u8; 16],
}

impl SetRoot {
    /// The first 128 bits of BLAKE3 over the domain, publisher, owner group and nonce. Its
    /// ULID time part carries no creation order.
    pub fn set_id(&self) -> Ulid {
        let mut hasher = blake3::Hasher::new();
        hasher.update(SET_DOMAIN);
        hasher.update(self.publisher.as_bytes());
        hasher.update(&self.owner_group.to_bytes());
        hasher.update(&self.nonce);
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Ulid::from_bytes(bytes)
    }
}

/// What a statement asserts; ranges and epochs are part of the signed payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StatementKind {
    /// The member's node bound its local bucket incarnation to the set.
    Admit,
    /// The former coordinator fenced the range; bounded descriptors only.
    TransferFenced {
        transfer_id: Ulid,
        range_id: Ulid,
        epoch: u64,
        next_epoch: u64,
        watermark: Vec<u8>,
        import_cursor: Option<Vec<u8>>,
        refresh_cursor: Option<Vec<u8>>,
        open_uploads: u64,
    },
    /// The new coordinator applied everything up to the fenced watermark.
    TransferComplete {
        transfer_id: Ulid,
        range_id: Ulid,
        /// The digest of the exact `transfer fenced` statement this completes.
        fenced: [u8; 32],
        watermark: Vec<u8>,
    },
    /// A coordinator fenced all its ranges for the named deletion identity.
    DeleteFenced { deletion: [u8; 32] },
    /// An automatic split by the range's coordinator.
    Split {
        /// The revision number the coordinator decided on.
        basis: u64,
        range_id: Ulid,
        epoch: u64,
        split_key: String,
        lower_id: Ulid,
        upper_id: Ulid,
    },
    /// A coordinator delivered every range it coordinates that a catching-up member holds,
    /// everything up to the watermark (milliseconds) its bootstrap started at included.
    CaughtUp {
        member_id: Ulid,
        /// The intent of the catching-up member, so a later join catches up again.
        intent: [u8; 32],
        /// The id and epoch of each range it delivered; a changed range voids the statement.
        ranges: Vec<(Ulid, u64)>,
        watermark: u64,
    },
    /// An automatic balancing transfer proposed by the range's coordinator.
    TransferPrepare {
        /// The revision number the coordinator decided on.
        basis: u64,
        transfer_id: Ulid,
        range_id: Ulid,
        to: Ulid,
        epoch: u64,
        role: TransferRole,
    },
}

impl StatementKind {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Admit => "admit",
            Self::TransferFenced { .. } => "transfer fenced",
            Self::TransferComplete { .. } => "transfer complete",
            Self::DeleteFenced { .. } => "delete fenced",
            Self::Split { .. } => "split",
            Self::TransferPrepare { .. } => "transfer prepare",
            Self::CaughtUp { .. } => "caught up",
        }
    }

    /// The revision an automatic request was decided on; it yields to a newer admin revision.
    pub fn basis(&self) -> Option<u64> {
        match self {
            Self::Split { basis, .. } | Self::TransferPrepare { basis, .. } => Some(*basis),
            _ => None,
        }
    }
}

/// A member's or coordinator's assertion. It names the intent it answers, never the head it
/// is published on, so statements signed against one head do not invalidate each other.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Statement {
    pub realm_id: RealmId,
    pub set_id: Ulid,
    /// The operation identity; the publisher embeds one statement at most once.
    pub statement_id: Ulid,
    pub member_id: Ulid,
    pub incarnation: u128,
    /// The digest of the create or join that added the member; it never names a head.
    pub intent: [u8; 32],
    pub kind: StatementKind,
}

impl Statement {
    /// The admission of one member incarnation for `intent`. Its id is derived from them, so
    /// a retried admission is the same statement and the publisher embeds it once.
    pub fn admit(
        realm_id: RealmId,
        set_id: Ulid,
        member_id: Ulid,
        incarnation: u128,
        intent: [u8; 32],
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(ADMIT_DOMAIN);
        hasher.update(&set_id.to_bytes());
        hasher.update(&member_id.to_bytes());
        hasher.update(&incarnation.to_be_bytes());
        hasher.update(&intent);
        let mut id = [0; 16];
        id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Self {
            realm_id,
            set_id,
            statement_id: Ulid::from_bytes(id),
            member_id,
            incarnation,
            intent,
            kind: StatementKind::Admit,
        }
    }

    /// A coordinator's statement that `target` caught up with `ranges`. Its id is derived from
    /// both members, the target's intent and the ranges, so a retry is embedded once.
    pub fn caught_up(
        realm_id: RealmId,
        set_id: Ulid,
        signer: &DistributionMember,
        target: &DistributionMember,
        ranges: Vec<(Ulid, u64)>,
        watermark: u64,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(CAUGHT_DOMAIN);
        hasher.update(&set_id.to_bytes());
        hasher.update(&signer.member_id.to_bytes());
        hasher.update(&target.member_id.to_bytes());
        hasher.update(&target.intent);
        for (range_id, epoch) in &ranges {
            hasher.update(&range_id.to_bytes());
            hasher.update(&epoch.to_be_bytes());
        }
        let mut id = [0; 16];
        id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Self {
            realm_id,
            set_id,
            statement_id: Ulid::from_bytes(id),
            member_id: signer.member_id,
            incarnation: signer.incarnation.unwrap_or_default(),
            intent: signer.intent,
            kind: StatementKind::CaughtUp {
                member_id: target.member_id,
                intent: target.intent,
                ranges,
                watermark,
            },
        }
    }

    pub fn signing_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(&(STATEMENT_DOMAIN, self))
    }

    pub fn digest(&self) -> [u8; 32] {
        let bytes = self.signing_bytes().unwrap_or_default();
        *blake3::hash(&bytes).as_bytes()
    }

    /// Signs with the member node's key; the publisher only embeds what members signed.
    pub fn sign(self, secret: &iroh::SecretKey) -> SignedStatement {
        let bytes = self.signing_bytes().unwrap_or_default();
        SignedStatement {
            signature: secret.sign(&bytes),
            signer: secret.public(),
            statement: self,
        }
    }
}

/// A statement a member asks its own node key to sign; the adapter refuses another signer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatementClaim {
    pub signer: NodeId,
    pub statement: Statement,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedStatement {
    pub statement: Statement,
    pub signer: NodeId,
    pub signature: iroh::Signature,
}

impl SignedStatement {
    /// Whether the signature is the named signer's over this exact statement.
    pub fn verified(&self) -> bool {
        self.statement
            .signing_bytes()
            .is_ok_and(|bytes| self.signer.verify(&bytes, &self.signature).is_ok())
    }
}

#[cfg(test)]
mod tests {
    use super::{SetRoot, Statement, StatementKind};
    use crate::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn root(seed: u8) -> SetRoot {
        SetRoot {
            publisher: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            owner_group: Ulid::from_bytes([2; 16]),
            nonce: [3; 16],
        }
    }

    #[test]
    fn id_binds_root() {
        let base = root(1);
        assert_eq!(base.set_id(), root(1).set_id());
        assert_ne!(base.set_id(), root(4).set_id());
        let mut owner = base.clone();
        owner.owner_group = Ulid::from_bytes([5; 16]);
        assert_ne!(owner.set_id(), base.set_id());
        let mut nonce = base.clone();
        nonce.nonce[0] = 9;
        assert_ne!(nonce.set_id(), base.set_id());
    }

    #[test]
    fn statements_verify_signer() {
        let secret = iroh::SecretKey::from_bytes(&[6; 32]);
        let statement = Statement {
            realm_id: RealmId::from_bytes([7; 32]),
            set_id: root(1).set_id(),
            statement_id: Ulid::from_bytes([8; 16]),
            member_id: Ulid::from_bytes([9; 16]),
            incarnation: 10,
            intent: [1; 32],
            kind: StatementKind::Admit,
        };
        let signed = statement.clone().sign(&secret);
        assert!(signed.verified());
        let mut forged = signed.clone();
        forged.statement.incarnation = 11;
        assert!(!forged.verified());
        let mut moved = signed.clone();
        moved.signer = iroh::SecretKey::from_bytes(&[12; 32]).public();
        assert!(!moved.verified());
        assert_ne!(forged.statement.digest(), statement.digest());
    }

    #[test]
    fn admissions_keep_identity() {
        let (realm, set) = (RealmId::from_bytes([7; 32]), root(1).set_id());
        let member = Ulid::from_bytes([9; 16]);
        let first = Statement::admit(realm, set, member, 10, [1; 32]);
        assert_eq!(first, Statement::admit(realm, set, member, 10, [1; 32]));
        let other = Statement::admit(realm, set, member, 11, [1; 32]);
        assert_ne!(first.statement_id, other.statement_id);
        let intent = Statement::admit(realm, set, member, 10, [2; 32]);
        assert_ne!(first.statement_id, intent.statement_id);
    }
}
