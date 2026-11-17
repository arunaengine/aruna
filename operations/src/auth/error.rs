//! Errors of request authorization: an RBAC or policy denial, or a fault while deciding.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::{AuthorizationError, StorageError};
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
