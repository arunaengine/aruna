//! The request-authorization entry point for REST and S3: [`authorize`] applies
//! RBAC and public visibility first, then every applicable deny/require policy.
//! Bulk routes reuse [`PolicyEvaluator`] to read one group's policy state once.

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::request_policy::{
    PolicyEnforcementError, PolicyRequestExtras, enforce_policies, policy_request_with,
};
use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{AuthContext, Permission, RealmId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthorizeError {
    #[error("permission denied")]
    PermissionDenied,
    #[error(transparent)]
    Policy(#[from] PolicyEnforcementError),
    #[error("authorization check failed: {0}")]
    CheckFailed(String),
}

/// Authorizes one action: ordinary RBAC and public visibility first, then the
/// realm and group deny/require policies. Neither layer may grant what the
/// other denied.
pub async fn authorize(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    path: &str,
    permission: &Permission,
    extras: PolicyRequestExtras,
) -> Result<(), AuthorizeError> {
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: path.to_string(),
            required_permission: permission.clone(),
        }),
        context,
    )
    .await
    .map_err(|error| match error {
        // A missing or malformed target is an authorization failure, not an
        // internal error, matching the choke points that fed this boundary.
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::AuthDocNotFound => AuthorizeError::PermissionDenied,
        other => AuthorizeError::CheckFailed(other.to_string()),
    })?;
    if !allowed {
        return Err(AuthorizeError::PermissionDenied);
    }
    enforce_policies(
        context,
        realm_id,
        &policy_request_with(path, permission, Some(&auth.user_id), extras),
    )
    .await?;
    Ok(())
}
