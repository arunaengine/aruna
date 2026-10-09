//! The short-lived confirmation a realm signs before it links a login of another realm to one of
//! its local accounts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::UserId;
use crate::federation::{FederationError, Signable, Signed};
use crate::structs::identity::realm::RealmId;

pub const LINK_DOMAIN: &str = "aruna-link-confirmation-v1";
/// Longest lifetime of a confirmation, and the oldest login of either side it accepts.
pub const MAX_LINK_SECS: u64 = 300;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkAction {
    Link,
}

/// Signed by `realm_id` after both logins were checked, for one link of `foreign_user` to
/// `local_user`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkConfirmation {
    pub realm_id: RealmId,
    pub action: LinkAction,
    pub local_user: UserId,
    pub foreign_user: UserId,
    /// Issuance of the foreign login's session; a later cutoff of that login voids the link.
    pub foreign_issued_at: u64,
    pub issued_at: u64,
    pub expires_at: u64,
    pub confirmation_id: Ulid,
}

impl Signable for LinkConfirmation {
    const DOMAIN: &'static str = LINK_DOMAIN;
    fn realm_id(&self) -> RealmId {
        self.realm_id
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LinkError {
    #[error(transparent)]
    Signature(#[from] FederationError),
    #[error("the confirmation is for another realm, account or action")]
    Unbound,
    #[error("the confirmation lifetime is invalid or over")]
    BadLifetime,
}

/// Key of the local accounts that claim `alias` in the federation keyspace.
pub fn alias_claims_key(alias: &UserId) -> Vec<u8> {
    [&b"alias/"[..], &alias.to_bytes()].concat()
}

/// The account a linked login is usable for: only an unambiguous single claim.
pub fn alias_owner(claims: &BTreeSet<UserId>) -> Option<UserId> {
    match claims.len() {
        1 => claims.first().copied(),
        _ => None,
    }
}

/// Admits a confirmation at `local` for the caller `local_user`.
pub fn check_confirmation(
    confirmation: &Signed<LinkConfirmation>,
    local: &RealmId,
    local_user: &UserId,
    now: u64,
) -> Result<(), LinkError> {
    confirmation.verify(local)?;
    let payload = &confirmation.payload;
    if payload.action != LinkAction::Link
        || payload.local_user != *local_user
        || local_user.realm_id != *local
        || payload.foreign_user.realm_id == *local
        || payload.foreign_user.is_nil()
    {
        return Err(LinkError::Unbound);
    }
    let lifetime = payload.expires_at.checked_sub(payload.issued_at);
    if !lifetime.is_some_and(|secs| secs > 0 && secs <= MAX_LINK_SECS)
        || now >= payload.expires_at
        || payload.issued_at > now
    {
        return Err(LinkError::BadLifetime);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::auth::NodeCapabilities;
    use ed25519_dalek::SigningKey;

    fn signer(seed: u8) -> (RealmId, NodeCapabilities) {
        let key = SigningKey::from_bytes(&[seed; 32]);
        let realm = RealmId::from_bytes(key.verifying_key().to_bytes());
        (realm, NodeCapabilities::management_node(key).unwrap())
    }

    #[test]
    fn confirmation_bound() {
        // Only this realm's signature for this caller, a foreign login and a short life count.
        let (realm, capabilities) = signer(1);
        let local = UserId::new(Ulid::from_bytes([2; 16]), realm);
        let foreign = UserId::new(Ulid::from_bytes([3; 16]), RealmId::from_bytes([4; 32]));
        let confirmation = LinkConfirmation {
            realm_id: realm,
            action: LinkAction::Link,
            local_user: local,
            foreign_user: foreign,
            foreign_issued_at: 90,
            issued_at: 100,
            expires_at: 100 + MAX_LINK_SECS,
            confirmation_id: Ulid::from_bytes([5; 16]),
        };
        let sign = |payload: LinkConfirmation| Signed::sign(payload, &capabilities).unwrap();
        let signed = sign(confirmation.clone());
        assert_eq!(check_confirmation(&signed, &realm, &local, 101), Ok(()));
        let other = UserId::new(Ulid::from_bytes([6; 16]), realm);
        let unbound = check_confirmation(&signed, &realm, &other, 101);
        assert_eq!(unbound, Err(LinkError::Unbound));
        let late = check_confirmation(&signed, &realm, &local, 100 + MAX_LINK_SECS);
        assert_eq!(late, Err(LinkError::BadLifetime));
        let long = sign(LinkConfirmation {
            expires_at: 101 + MAX_LINK_SECS,
            ..confirmation.clone()
        });
        let checked = check_confirmation(&long, &realm, &local, 101);
        assert_eq!(checked, Err(LinkError::BadLifetime));
        let (other_realm, other_signer) = signer(7);
        let forged = Signed::sign(confirmation, &other_signer).unwrap();
        assert!(check_confirmation(&forged, &realm, &local, 101).is_err());
        assert_ne!(other_realm, realm);
    }

    #[test]
    fn ambiguous_claims_unusable() {
        // A linked login is usable only for exactly one claiming account.
        let realm = RealmId::from_bytes([8; 32]);
        let (first, second) = (
            UserId::new(Ulid::from_bytes([1; 16]), realm),
            UserId::new(Ulid::from_bytes([2; 16]), realm),
        );
        assert_eq!(alias_owner(&BTreeSet::new()), None);
        assert_eq!(alias_owner(&BTreeSet::from([first])), Some(first));
        assert_eq!(alias_owner(&BTreeSet::from([first, second])), None);
    }
}
