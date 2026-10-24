//! Defines the request-bound delegation a node signs when it forwards one S3 request it
//! verified to the coordinator of a distributed bucket's range (D33). It carries the caller's
//! identity and restrictions for exactly one request and is never a credential.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;
use crate::UserId;
use crate::structs::identity::auth::PathRestriction;
use crate::structs::identity::realm::RealmId;
use crate::types::GroupId;

/// The header a forwarded S3 request carries its delegation in.
pub const DELEGATION_HEADER: &str = "x-aruna-delegation";
/// The longest a delegation may be valid.
pub const MAX_VALID_SECS: u64 = 30;
/// Clock difference between nodes a delegation tolerates at its start.
const SKEW_SECS: u64 = 30;
/// The largest encoded delegation a target decodes.
const MAX_ENCODED: usize = 16 * 1024;
const DOMAIN: &str = "aruna-distribution-delegation-v1";
const DIGEST_DOMAIN: &[u8] = b"aruna-distribution-request-v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Delegation {
    pub realm_id: RealmId,
    /// The node that verified the caller's credential and signs this delegation.
    pub issuer: NodeId,
    /// The coordinator node the request is for.
    pub target: NodeId,
    pub set_id: Ulid,
    pub range_id: Ulid,
    pub user_id: UserId,
    /// The group the originating credential is bound to.
    pub group_id: GroupId,
    pub credential_id: String,
    pub restrictions: Option<Vec<PathRestriction>>,
    /// Unix seconds at which the originating credential expires.
    pub credential_expiry: u64,
    /// The S3 operation, such as `PutObject`.
    pub operation: String,
    pub bucket: String,
    pub key: String,
    pub copy_source: Option<String>,
    pub request_digest: [u8; 32],
    pub nonce: [u8; 16],
    /// Unix seconds at which the issuer signed.
    pub issued_at: u64,
    pub valid_secs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedDelegation {
    pub delegation: Delegation,
    pub signature: iroh::Signature,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DelegationError {
    #[error("the delegation does not decode")]
    Encoding,
    #[error("the delegation signature does not verify")]
    Signature,
    #[error("the delegation is for another realm or node")]
    WrongTarget,
    #[error("the delegation is not valid at this time")]
    Expired,
    #[error("the delegation does not match the request")]
    RequestMismatch,
}

impl Delegation {
    pub fn signing_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(&(DOMAIN, self))
    }

    pub fn sign(self, secret: &iroh::SecretKey) -> SignedDelegation {
        let bytes = self.signing_bytes().unwrap_or_default();
        SignedDelegation {
            signature: secret.sign(&bytes),
            delegation: self,
        }
    }
}

impl SignedDelegation {
    /// Checks the issuer's signature, that this node and realm are the target, and that `now`
    /// (Unix seconds) lies inside both the delegation's and the credential's validity.
    pub fn verify(
        &self,
        realm_id: RealmId,
        target: NodeId,
        now: u64,
    ) -> Result<&Delegation, DelegationError> {
        let delegation = &self.delegation;
        let bytes = delegation
            .signing_bytes()
            .map_err(|_| DelegationError::Encoding)?;
        if delegation.issuer.verify(&bytes, &self.signature).is_err() {
            return Err(DelegationError::Signature);
        }
        if delegation.realm_id != realm_id || delegation.target != target {
            return Err(DelegationError::WrongTarget);
        }
        let ends = delegation
            .issued_at
            .saturating_add(delegation.valid_secs)
            .min(delegation.credential_expiry);
        let started = now.saturating_add(SKEW_SECS) >= delegation.issued_at;
        if delegation.valid_secs > MAX_VALID_SECS || !started || now > ends {
            return Err(DelegationError::Expired);
        }
        Ok(delegation)
    }

    pub fn encode(&self) -> Result<String, DelegationError> {
        let bytes = postcard::to_allocvec(self).map_err(|_| DelegationError::Encoding)?;
        Ok(URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn decode(value: &str) -> Result<Self, DelegationError> {
        if value.len() > MAX_ENCODED {
            return Err(DelegationError::Encoding);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| DelegationError::Encoding)?;
        postcard::from_bytes(&bytes).map_err(|_| DelegationError::Encoding)
    }
}

/// Binds a delegation to one request: method, bucket, key, query and the headers that
/// describe its body, given as lowercase name and value pairs in any order.
pub fn request_digest(
    method: &str,
    bucket: &str,
    key: &str,
    query: Option<&str>,
    headers: &[(&str, &str)],
) -> [u8; 32] {
    let mut headers = headers.to_vec();
    headers.sort_unstable();
    let bytes = postcard::to_allocvec(&(method, bucket, key, query, headers)).unwrap_or_default();
    let mut hasher = blake3::Hasher::new();
    hasher.update(DIGEST_DOMAIN);
    hasher.update(&bytes);
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret(seed: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[seed; 32])
    }

    fn delegation() -> Delegation {
        let realm_id = RealmId::from_bytes([9; 32]);
        Delegation {
            realm_id,
            issuer: secret(1).public(),
            target: secret(2).public(),
            set_id: Ulid::from_bytes([3; 16]),
            range_id: Ulid::from_bytes([4; 16]),
            user_id: UserId::local(Ulid::from_bytes([5; 16]), realm_id),
            group_id: Ulid::from_bytes([6; 16]),
            credential_id: "AKEY".to_string(),
            restrictions: None,
            credential_expiry: 1_000,
            operation: "PutObject".to_string(),
            bucket: "spire".to_string(),
            key: "a/b".to_string(),
            copy_source: None,
            request_digest: request_digest("PUT", "spire", "a/b", None, &[]),
            nonce: [7; 16],
            issued_at: 100,
            valid_secs: 10,
        }
    }

    #[test]
    fn delegation_verifies_once_bound() {
        let signed = delegation().sign(&secret(1));
        let (realm, target) = (signed.delegation.realm_id, secret(2).public());
        let decoded = SignedDelegation::decode(&signed.encode().unwrap()).unwrap();
        assert_eq!(decoded.verify(realm, target, 105), Ok(&signed.delegation));
        assert_eq!(
            signed.verify(realm, secret(3).public(), 105),
            Err(DelegationError::WrongTarget)
        );
        assert_eq!(
            signed.verify(realm, target, 111),
            Err(DelegationError::Expired)
        );
        let mut forged = signed.clone();
        forged.delegation.key = "other".to_string();
        assert_eq!(
            forged.verify(realm, target, 105),
            Err(DelegationError::Signature)
        );
        let foreign = delegation().sign(&secret(8));
        assert_eq!(
            foreign.verify(realm, target, 105),
            Err(DelegationError::Signature)
        );
    }

    #[test]
    fn credential_expiry_bounds_validity() {
        let mut long = delegation();
        long.credential_expiry = 103;
        let signed = long.sign(&secret(1));
        let realm = signed.delegation.realm_id;
        assert_eq!(
            signed.verify(realm, secret(2).public(), 104),
            Err(DelegationError::Expired)
        );
        let mut endless = delegation();
        endless.valid_secs = MAX_VALID_SECS + 1;
        let signed = endless.sign(&secret(1));
        assert_eq!(
            signed.verify(realm, secret(2).public(), 101),
            Err(DelegationError::Expired)
        );
    }

    #[test]
    fn digest_binds_request() {
        let base = request_digest("PUT", "b", "k", None, &[("content-length", "3")]);
        let same = request_digest("PUT", "b", "k", None, &[("content-length", "3")]);
        assert_eq!(base, same);
        assert_ne!(
            base,
            request_digest("PUT", "b", "k2", None, &[("content-length", "3")])
        );
        assert_ne!(
            base,
            request_digest("PUT", "b", "k", None, &[("content-length", "4")])
        );
        assert_ne!(
            base,
            request_digest("DELETE", "b", "k", None, &[("content-length", "3")])
        );
    }
}
