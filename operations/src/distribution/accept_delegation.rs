//! Accepts a request-bound delegation once: verified, issued by a realm infrastructure node and
//! carrying a nonce no earlier acceptance spent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::distribution::delegation::{
    Delegation, DelegationError, MAX_VALID_SECS, SignedDelegation,
};
use thiserror::Error;

use super::nonce::{NonceError, SpendNonceOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::realm::peer_trust::is_sync_eligible;

#[derive(Debug, Error, PartialEq)]
pub enum DelegateError {
    #[error(transparent)]
    Delegation(#[from] DelegationError),
    #[error("the delegation comes from a node that is not a realm infrastructure node")]
    Issuer,
    #[error("the delegation was already used")]
    Replayed,
    #[error("delegations need the realm transport and configuration")]
    Unavailable,
}

impl From<NonceError> for DelegateError {
    fn from(error: NonceError) -> Self {
        match error {
            NonceError::Replayed => DelegateError::Replayed,
            NonceError::Unavailable => DelegateError::Unavailable,
        }
    }
}

impl From<IoError> for DelegateError {
    fn from(_: IoError) -> Self {
        DelegateError::Unavailable
    }
}

/// Accepts `signed` for the node `node_id` of `realm_id` at `now` (Unix seconds). The caller
/// still checks the request digest and the user's permissions on its own path.
#[derive(Debug, PartialEq)]
pub struct AcceptDelegationOperation {
    signed: SignedDelegation,
    realm_id: RealmId,
    node_id: NodeId,
    now: u64,
    /// The nonce spend runs, after the config read.
    spending: bool,
}

impl AcceptDelegationOperation {
    pub fn new(signed: SignedDelegation, realm_id: RealmId, node_id: NodeId, now: u64) -> Self {
        Self {
            signed,
            realm_id,
            node_id,
            now,
            spending: false,
        }
    }

    /// Spends the nonce once the config names the issuer as an infrastructure node.
    fn spend(
        &mut self,
        config: Result<RealmConfigDocument, GetConfigError>,
    ) -> Result<IoCall, DelegateError> {
        let config = config.map_err(|_| DelegateError::Unavailable)?;
        let delegation = &self.signed.delegation;
        if !is_sync_eligible(&config, delegation.issuer) {
            return Err(DelegateError::Issuer);
        }
        let expires = delegation.issued_at.saturating_add(2 * MAX_VALID_SECS);
        self.spending = true;
        let nonce = SpendNonceOperation::new(delegation.nonce, expires, self.now);
        Ok(IoCall::child(nonce))
    }
}

impl Operation for AcceptDelegationOperation {
    type Output = Delegation;
    type Error = DelegateError;

    fn first(&mut self) -> Result<Next<Delegation>, DelegateError> {
        self.signed.verify(self.realm_id, self.node_id, self.now)?;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Delegation>, DelegateError> {
        if !self.spending {
            let config = result.child::<RealmConfigDocument, GetConfigError>()?;
            return Ok(Next::Call(self.spend(config)?));
        }
        result.child::<(), NonceError>()??;
        Ok(Next::Done(self.signed.delegation.clone()))
    }

    /// No error is a lost conflict: the nonce spend maps its own conflict to a replay.
    fn is_conflict(_error: &DelegateError) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use aruna_core::structs::storage::distribution::delegation::Payload;
    use ulid::Ulid;

    const NOW: u64 = 105;

    fn secret(seed: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[seed; 32])
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([9; 32])
    }

    fn signed() -> SignedDelegation {
        Delegation {
            realm_id: realm(),
            issuer: secret(1).public(),
            target: secret(2).public(),
            set_id: Ulid::from_bytes([3; 16]),
            range_id: Ulid::from_bytes([4; 16]),
            user_id: UserId::local(Ulid::from_bytes([5; 16]), realm()),
            group_id: Ulid::from_bytes([6; 16]),
            credential_id: "AKEY".to_string(),
            restrictions: None,
            credential_expiry: 1_000,
            operation: "PutObject".to_string(),
            bucket: "spire".to_string(),
            key: "a/b".to_string(),
            copy_source: None,
            request_digest: [0; 32],
            payload: Payload::Framed { checksum: None },
            nonce: [7; 16],
            issued_at: 100,
            valid_secs: 10,
        }
        .sign(&secret(1))
    }

    fn operation() -> AcceptDelegationOperation {
        AcceptDelegationOperation::new(signed(), realm(), secret(2).public(), NOW)
    }

    /// The realm config naming the issuer as a node of `kind`.
    fn config(kind: RealmNodeKind) -> IoResult {
        let mut document = RealmConfigDocument::new(realm(), Vec::new(), 3);
        document.ensure_node(secret(1).public(), kind);
        let read: Result<RealmConfigDocument, GetConfigError> = Ok(document);
        IoResult::Child(ChildOutcome::new(read))
    }

    fn spent(result: Result<(), NonceError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    // A verified delegation from an infrastructure node spends its nonce and is accepted.
    #[test]
    fn accepts_once() {
        let mut accept = operation();
        assert!(matches!(accept.first(), Ok(Next::Call(IoCall::Child(_)))));
        assert!(matches!(
            accept.next(config(RealmNodeKind::Server)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(
            accept.next(spent(Ok(()))),
            Ok(Next::Done(signed().delegation))
        );
    }

    // A delegation for another node, from a device or with a spent nonce is refused.
    #[test]
    fn refuses_bad_delegation() {
        let mut accept = AcceptDelegationOperation::new(signed(), realm(), secret(3).public(), NOW);
        assert_eq!(accept.first(), Err(DelegationError::WrongTarget.into()));

        let owner = signed().delegation.user_id;
        let mut accept = operation();
        accept.first().unwrap();
        let device = config(RealmNodeKind::User { owner });
        assert_eq!(accept.next(device), Err(DelegateError::Issuer));

        let mut accept = operation();
        accept.first().unwrap();
        accept.next(config(RealmNodeKind::Server)).unwrap();
        let replayed = spent(Err(NonceError::Replayed));
        assert_eq!(accept.next(replayed), Err(DelegateError::Replayed));
    }

    // A failed config read leaves the check unavailable; a result of another kind fails too.
    #[test]
    fn rejects_unexpected() {
        let mut accept = operation();
        accept.first().unwrap();
        let failed: Result<RealmConfigDocument, GetConfigError> =
            Err(GetConfigError::DocumentNotFound);
        let failed = IoResult::Child(ChildOutcome::new(failed));
        assert_eq!(accept.next(failed), Err(DelegateError::Unavailable));
        let mut accept = operation();
        accept.first().unwrap();
        assert_eq!(accept.next(IoResult::Done), Err(DelegateError::Unavailable));
    }
}
