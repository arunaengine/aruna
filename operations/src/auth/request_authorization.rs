//! Entry point that authorizes REST and S3 requests: RBAC first, then policy checks.
//! Bulk routes reuse one evaluator so a group's policy state is read only once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::request_policy::{
    PolicyEnforcementError, PolicyEvaluator, PolicyRequestExtras, enforce_policies,
    policy_request_with,
};
use crate::driver::{DriverContext, drive};
use aruna_core::NodeId;
use aruna_core::errors::{AuthorizationError, StorageError};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::types::GroupId;
use thiserror::Error;

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
fn check_error(error: AuthorizationError) -> AuthorizeError {
    match error {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::DocNotFound => AuthorizeError::PermissionDenied,
        AuthorizationError::StorageError(error) => AuthorizeError::Storage(error),
        other => AuthorizeError::CheckFailed(other.to_string()),
    }
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
    .map_err(check_error)?;
    if !allowed {
        return Err(AuthorizeError::PermissionDenied);
    }
    enforce_policies(
        context,
        realm_id,
        &policy_request_with(path, permission, Some(auth), extras),
    )
    .await?;
    Ok(())
}

/// One caller's rules and policies for a bucket, loaded once to decide many of its keys.
pub struct BucketAccess {
    auth: AuthContext,
    bucket_path: String,
    rules: crate::auth::permission_rules::PermissionRules,
    policies: crate::auth::request_policy::PolicyEvaluator,
}

impl BucketAccess {
    pub async fn load(
        context: &DriverContext,
        auth: &AuthContext,
        (realm_id, group_id, node_id): (RealmId, GroupId, NodeId),
        bucket: &str,
    ) -> Result<Self, AuthorizeError> {
        use crate::auth::permission_rules::{PermissionRulesConfig, PermissionRulesOperation};
        let bucket_path = bucket_permission_path(realm_id, group_id, node_id, bucket);
        let config = PermissionRulesConfig {
            auth_context: auth.clone(),
            path: bucket_path.clone(),
        };
        let rules = drive(PermissionRulesOperation::new(config), context)
            .await
            .map_err(check_error)?;
        let policies = PolicyEvaluator::load(context, realm_id, Some(group_id)).await?;
        Ok(Self {
            auth: auth.clone(),
            bucket_path,
            rules,
            policies,
        })
    }

    /// Whether the caller holds `permission`, by role and policy, on the bucket or one key.
    pub fn allows(&self, key: Option<&str>, permission: &Permission) -> bool {
        let path = match key {
            Some(key) => format!("{}/{key}", self.bucket_path),
            None => self.bucket_path.clone(),
        };
        let extras = PolicyRequestExtras::rest();
        let request = policy_request_with(&path, permission, Some(&self.auth), extras);
        self.rules.allows(&path, permission) && self.policies.evaluate(&request).is_ok()
    }
}
