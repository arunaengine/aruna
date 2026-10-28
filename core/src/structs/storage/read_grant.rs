//! Read grants: a source node's signed statement that it validated a caller's bearer and lets
//! that caller read one exact scope for one job, and the short-lived request presenting it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A reading node never asserts an identity: the grant carries the one its issuer verified.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::UserId;
use crate::id::NodeId;
use crate::structs::execution::job::SubmissionId;
use crate::structs::identity::auth::PathRestriction;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::replication::VersionedObjectArn;

/// The longest a granted request may be valid.
pub const MAX_REQUEST_SECS: u64 = 30;
/// Clock difference between nodes a request tolerates at its start.
const SKEW_SECS: u64 = 30;
const GRANT_DOMAIN: &str = "aruna-read-grant-v1";
const REQUEST_DOMAIN: &str = "aruna-granted-request-v1";

/// What a grant lets its holder ask for.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GrantScope {
    /// One exact object version.
    Version(VersionedObjectArn),
    /// One bucket on its node, for listings such as release rows.
    Bucket { node_id: NodeId, bucket: String },
}

impl GrantScope {
    pub fn node_id(&self) -> NodeId {
        match self {
            Self::Version(arn) => arn.node_id,
            Self::Bucket { node_id, .. } => *node_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadGrant {
    pub realm_id: RealmId,
    /// The source node that validated the caller's bearer and signs the grant.
    pub issuer: NodeId,
    pub submission_id: SubmissionId,
    pub user_id: UserId,
    /// The caller's restrictions at admission; every granted read applies them.
    pub restrictions: Option<Vec<PathRestriction>>,
    pub scope: GrantScope,
    /// Policy operation names the grant covers, such as `s3.GetObject`.
    pub operations: Vec<String>,
    /// Unix seconds after which the grant is void.
    pub expires_at: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedGrant {
    pub grant: ReadGrant,
    pub signature: iroh::Signature,
}

/// One request a reading node sends under a grant, signed by that node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrantedRequest {
    pub realm_id: RealmId,
    /// The reading node.
    pub issuer: NodeId,
    /// The node that serves the read.
    pub target: NodeId,
    pub operation: String,
    /// Binds the request to one operation, scope and range.
    pub request_digest: [u8; 32],
    pub nonce: [u8; 16],
    /// Unix seconds at which the reading node signed.
    pub issued_at: u64,
    pub valid_secs: u64,
    pub grant: SignedGrant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedRequest {
    pub request: GrantedRequest,
    pub signature: iroh::Signature,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GrantError {
    #[error("the grant or request does not encode")]
    Encoding,
    #[error("the grant or request signature does not verify")]
    Signature,
    #[error("the request is for another realm or node")]
    WrongTarget,
    #[error("the grant or request is not valid at this time")]
    Expired,
    #[error("the grant does not cover the request")]
    NotCovered,
}

impl ReadGrant {
    pub fn signing_bytes(&self) -> Result<Vec<u8>, GrantError> {
        postcard::to_allocvec(&(GRANT_DOMAIN, self)).map_err(|_| GrantError::Encoding)
    }

    pub fn sign(self, secret: &iroh::SecretKey) -> Result<SignedGrant, GrantError> {
        let bytes = self.signing_bytes()?;
        Ok(SignedGrant {
            signature: secret.sign(&bytes),
            grant: self,
        })
    }
}

impl SignedGrant {
    /// Checks the issuer's signature, the realm and that `now` (Unix seconds) is in time.
    pub fn verify(&self, realm_id: RealmId, now: u64) -> Result<&ReadGrant, GrantError> {
        let bytes = self.grant.signing_bytes()?;
        if self.grant.issuer.verify(&bytes, &self.signature).is_err() {
            return Err(GrantError::Signature);
        }
        if self.grant.realm_id != realm_id {
            return Err(GrantError::WrongTarget);
        }
        if now > self.grant.expires_at {
            return Err(GrantError::Expired);
        }
        Ok(&self.grant)
    }
}

impl GrantedRequest {
    pub fn signing_bytes(&self) -> Result<Vec<u8>, GrantError> {
        postcard::to_allocvec(&(REQUEST_DOMAIN, self)).map_err(|_| GrantError::Encoding)
    }

    pub fn sign(self, secret: &iroh::SecretKey) -> Result<SignedRequest, GrantError> {
        let bytes = self.signing_bytes()?;
        Ok(SignedRequest {
            signature: secret.sign(&bytes),
            request: self,
        })
    }
}

impl SignedRequest {
    /// Checks the reading node's signature, that `target` in `realm_id` serves the request,
    /// its short validity, and that a grant `target` itself issued covers its operation.
    pub fn verify(
        &self,
        realm_id: RealmId,
        target: NodeId,
        now: u64,
    ) -> Result<&GrantedRequest, GrantError> {
        let request = &self.request;
        let bytes = request.signing_bytes()?;
        if request.issuer.verify(&bytes, &self.signature).is_err() {
            return Err(GrantError::Signature);
        }
        if request.realm_id != realm_id || request.target != target {
            return Err(GrantError::WrongTarget);
        }
        let started = now.saturating_add(SKEW_SECS) >= request.issued_at;
        let ends = request.issued_at.saturating_add(request.valid_secs);
        if request.valid_secs > MAX_REQUEST_SECS || !started || now > ends {
            return Err(GrantError::Expired);
        }
        let grant = request.grant.verify(realm_id, now)?;
        if grant.issuer != target
            || grant.scope.node_id() != target
            || !grant.operations.contains(&request.operation)
        {
            return Err(GrantError::NotCovered);
        }
        Ok(request)
    }
}

/// The digest a granted request binds: its operation, its scope and the byte range it asks.
pub fn request_digest(
    operation: &str,
    scope: &GrantScope,
    range: Option<(u64, u64)>,
) -> Result<[u8; 32], GrantError> {
    let bytes = postcard::to_allocvec(&(REQUEST_DOMAIN, operation, scope, range))
        .map_err(|_| GrantError::Encoding)?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn secret(seed: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[seed; 32])
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([1; 32])
    }

    fn scope(holder: NodeId) -> GrantScope {
        GrantScope::Version(VersionedObjectArn {
            realm_id: realm(),
            node_id: holder,
            bucket: "bakta".to_string(),
            key: "db/bakta.db".to_string(),
            version: Ulid::from_parts(7, 7),
        })
    }

    fn grant(holder: &iroh::SecretKey, user: UserId) -> SignedGrant {
        ReadGrant {
            realm_id: realm(),
            issuer: holder.public(),
            submission_id: SubmissionId([2; 32]),
            user_id: user,
            restrictions: None,
            scope: scope(holder.public()),
            operations: vec!["s3.GetObject".to_string()],
            expires_at: 1_000,
        }
        .sign(holder)
        .unwrap()
    }

    fn request(reader: &iroh::SecretKey, holder: NodeId, grant: SignedGrant) -> GrantedRequest {
        GrantedRequest {
            realm_id: realm(),
            issuer: reader.public(),
            target: holder,
            operation: "s3.GetObject".to_string(),
            request_digest: request_digest("s3.GetObject", &scope(holder), Some((0, 10))).unwrap(),
            nonce: [3; 16],
            issued_at: 100,
            valid_secs: 15,
            grant,
        }
    }

    #[test]
    fn accepts_signed_request() {
        let (holder, reader) = (secret(4), secret(5));
        let user = UserId::local(Ulid::from_parts(8, 8), realm());
        let signed = request(&reader, holder.public(), grant(&holder, user))
            .sign(&reader)
            .unwrap();
        let accepted = signed.verify(realm(), holder.public(), 105).unwrap();
        assert_eq!(accepted.grant.grant.user_id, user);
    }

    #[test]
    fn refuses_forged_parts() {
        // A changed identity, restriction, operation or target, a late request or a grant from
        // another node never verifies.
        let (holder, reader, other) = (secret(4), secret(5), secret(6));
        let user = UserId::local(Ulid::from_parts(8, 8), realm());
        let valid = grant(&holder, user);
        let mut forged = valid.clone();
        forged.grant.user_id = UserId::local(Ulid::from_parts(9, 9), realm());
        let forged = request(&reader, holder.public(), forged)
            .sign(&reader)
            .unwrap();
        assert_eq!(
            forged.verify(realm(), holder.public(), 105),
            Err(GrantError::Signature)
        );
        let mut widened = valid.clone();
        widened.grant.restrictions = Some(Vec::new());
        let widened = request(&reader, holder.public(), widened)
            .sign(&reader)
            .unwrap();
        assert_eq!(
            widened.verify(realm(), holder.public(), 105),
            Err(GrantError::Signature)
        );
        let mut write = request(&reader, holder.public(), valid.clone());
        write.operation = "s3.PutObject".to_string();
        let write = write.sign(&reader).unwrap();
        assert_eq!(
            write.verify(realm(), holder.public(), 105),
            Err(GrantError::NotCovered)
        );
        let signed = request(&reader, holder.public(), valid.clone())
            .sign(&reader)
            .unwrap();
        assert_eq!(
            signed.verify(realm(), other.public(), 105),
            Err(GrantError::WrongTarget)
        );
        assert_eq!(
            signed.verify(realm(), holder.public(), 200),
            Err(GrantError::Expired)
        );
        let foreign = grant(&other, user);
        let foreign = request(&reader, holder.public(), foreign)
            .sign(&reader)
            .unwrap();
        assert_eq!(
            foreign.verify(realm(), holder.public(), 105),
            Err(GrantError::NotCovered)
        );
        let mut tampered = request(&reader, holder.public(), valid)
            .sign(&reader)
            .unwrap();
        tampered.request.nonce = [9; 16];
        assert_eq!(
            tampered.verify(realm(), holder.public(), 105),
            Err(GrantError::Signature)
        );
    }
}
