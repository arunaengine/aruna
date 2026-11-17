//! Errors of request authorization: an RBAC or policy denial, or a fault while deciding.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::array::TryFromSliceError;

use aruna_core::UserId;
use aruna_core::auth::bearer_token_hash;
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::structs::identity::auth::TokenClaims;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::backend_import::TokenGrant;
use jsonwebtoken::dangerous::insecure_decode;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum PolicyEnforcementError {
    #[error("request denied by policy `{name}`: {reason}")]
    Denied { name: String, reason: String },
    /// Policy state could not be read or compiled; fail closed rather than allow.
    #[error("policy state unavailable: {0}")]
    Unavailable(String),
}

#[derive(Debug, Error)]
pub enum AuthorizeError {
    #[error("permission denied")]
    PermissionDenied,
    #[error(transparent)]
    Policy(#[from] PolicyEnforcementError),
    /// Storage failed while deciding the request: an infrastructure fault that
    /// callers must not present as an authorization verdict.
    #[error(transparent)]
    Storage(StorageError),
    #[error("authorization check failed: {0}")]
    CheckFailed(String),
}

/// A missing or malformed target is an authorization failure, not an internal error, matching
/// the choke points that fed this boundary.
pub(crate) fn check_error(error: AuthorizationError) -> AuthorizeError {
    match error {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::DocNotFound => AuthorizeError::PermissionDenied,
        AuthorizationError::StorageError(error) => AuthorizeError::Storage(error),
        other => AuthorizeError::CheckFailed(other.to_string()),
    }
}

#[derive(Debug, Error)]
pub enum ArunaBearerError {
    #[error("Realm is not trusted")]
    RealmNotTrusted,
    #[error("Token is revoked")]
    TokenRevoked,
    #[error("Invalid issuer key")]
    InvalidIssuerKey,
    #[error("Token expired")]
    Expired,
    #[error("Token lifetime exceeds the revocable maximum")]
    LifetimeTooLong,
    #[error("Revocation state is unavailable")]
    RevocationUnavailable,
    #[error("Invalid server token")]
    InvalidServerToken,
    #[error(transparent)]
    AuthContextConversion(#[from] ConversionError),
    #[error(transparent)]
    PublicKeyError(#[from] ed25519_dalek::ed25519::Error),
    #[error(transparent)]
    FromSliceError(#[from] TryFromSliceError),
    #[error(transparent)]
    PublicConversionError(#[from] ed25519_dalek::pkcs8::spki::Error),
    #[error(transparent)]
    JwtError(#[from] jsonwebtoken::errors::Error),
    #[error(transparent)]
    Base64Error(#[from] base64::DecodeError),
}

/// The non-secret identity of a token the request already validated, for work that runs as its
/// caller later.
pub fn token_grant(token: &str) -> Result<TokenGrant, ArunaBearerError> {
    let claims = insecure_decode::<TokenClaims>(token)?.claims;
    let issuer =
        RealmId::from_base64(&claims.iss).map_err(|_| ArunaBearerError::InvalidIssuerKey)?;
    let via = claims.via.as_deref().map(UserId::from_string).transpose()?;
    Ok(TokenGrant {
        token_hash: bearer_token_hash(token),
        issuer,
        issued_at_secs: claims.iat,
        expires_at_secs: claims.exp,
        via,
    })
}
