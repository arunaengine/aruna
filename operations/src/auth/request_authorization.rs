//! Entry point that authorizes REST and S3 requests: RBAC first, then policy checks.
//! Bulk routes reuse one evaluator so a group's policy state is read only once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::enforcement::{PolicyEvaluator, PolicyLoadOperation, enforce_policies};
use crate::auth::error::{AuthorizeError, PolicyEnforcementError, check_error};
use crate::auth::permission_rules::{
    GroupPermissionRules, PermissionRules, PermissionRulesConfig, PermissionRulesOperation,
};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::driver::{DriverContext, drive};
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::operation::Operation;
use aruna_core::permission_path::readable_roots;
use aruna_core::request_policy::PolicyRequest;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::types::GroupId;
use std::collections::{BTreeSet, HashMap};

/// Authorizes one action: ordinary RBAC and public visibility first, then the realm and group
/// deny/require policies. Neither layer may grant what the other denied.
#[derive(Debug, PartialEq)]
pub struct AuthorizeOperation {
    realm_id: RealmId,
    auth: AuthContext,
    path: String,
    permission: Permission,
    extras: PolicyRequestExtras,
    /// Set once the role check passed and the policies are loading.
    checked: bool,
}

impl AuthorizeOperation {
    pub fn new(
        realm_id: RealmId,
        auth: AuthContext,
        path: String,
        permission: Permission,
        extras: PolicyRequestExtras,
    ) -> Self {
        Self {
            realm_id,
            auth,
            path,
            permission,
            extras,
            checked: false,
        }
    }

    fn request(&self) -> PolicyRequest {
        let extras = self.extras.clone();
        policy_request_with(&self.path, &self.permission, Some(&self.auth), extras)
    }
}

impl Operation for AuthorizeOperation {
    type Output = ();
    type Error = AuthorizeError;

    fn first(&mut self) -> Result<Next<()>, AuthorizeError> {
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.auth.clone(),
            path: self.path.clone(),
            required_permission: self.permission.clone(),
        });
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AuthorizeError> {
        if self.checked {
            result
                .child::<PolicyEvaluator, PolicyEnforcementError>()??
                .evaluate(&self.request())?;
            return Ok(Next::Done(()));
        }
        let allowed = result
            .child::<bool, AuthorizationError>()?
            .map_err(check_error)?;
        if !allowed {
            return Err(AuthorizeError::PermissionDenied);
        }
        self.checked = true;
        let load = PolicyLoadOperation::for_request(self.realm_id, &self.request());
        Ok(Next::Call(IoCall::child(load)))
    }

    fn expected_error(error: &AuthorizeError) -> bool {
        matches!(
            error,
            AuthorizeError::PermissionDenied
                | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. })
        )
    }
}

/// Runs [`AuthorizeOperation`] for one request.
pub async fn authorize(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    path: &str,
    permission: &Permission,
    extras: PolicyRequestExtras,
) -> Result<(), AuthorizeError> {
    let operation = AuthorizeOperation::new(
        realm_id,
        auth.clone(),
        path.to_string(),
        permission.clone(),
        extras,
    );
    drive(operation, context).await
}

/// Authorizes `permission` by RBAC on `path` or else on `alternative`, then always enforces the
/// policies of `path`, and of `alternative` when only it granted. A policy denial is final.
pub async fn authorize_either(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    (path, alternative): (&str, &str),
    permission: &Permission,
) -> Result<(), AuthorizeError> {
    let granted = |path: &str| {
        drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: auth.clone(),
                path: path.to_string(),
                required_permission: permission.clone(),
            }),
            context,
        )
    };
    let direct = granted(path).await.map_err(check_error)?;
    if !direct && !granted(alternative).await.map_err(check_error)? {
        return Err(AuthorizeError::PermissionDenied);
    }
    let extras = PolicyRequestExtras::rest();
    let mut checked = vec![path];
    if !direct {
        checked.push(alternative);
    }
    for path in checked {
        let request = policy_request_with(path, permission, Some(auth), extras.clone());
        enforce_policies(context, realm_id, &request).await?;
    }
    Ok(())
}

/// Authorizes reading the metadata document at `path`: a public one needs only the request
/// policies, as every metadata read decides it, a private one also READ by role.
pub async fn authorize_document(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    (path, public): (&str, bool),
    extras: PolicyRequestExtras,
) -> Result<(), AuthorizeError> {
    if !public {
        return authorize(context, realm_id, auth, path, &Permission::READ, extras).await;
    }
    let request = policy_request_with(path, &Permission::READ, Some(auth), extras);
    enforce_policies(context, realm_id, &request).await?;
    Ok(())
}

/// One caller's rules and policies for a bucket, loaded once to decide many of its keys.
pub struct BucketAccess {
    auth: AuthContext,
    bucket_path: String,
    rules: crate::auth::permission_rules::PermissionRules,
    policies: crate::auth::enforcement::PolicyEvaluator,
}

impl BucketAccess {
    /// The access decided by rules and policies an operation has already read.
    pub(crate) fn from_parts(
        auth: AuthContext,
        bucket_path: String,
        rules: PermissionRules,
        policies: PolicyEvaluator,
    ) -> Self {
        Self {
            auth,
            bucket_path,
            rules,
            policies,
        }
    }

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

    /// Whether the caller may read every key that starts with `prefix`: READ by a grant on all
    /// of them, no role deny or path restriction below, and no policy that decides by path.
    pub fn reads_below(&self, prefix: &str) -> bool {
        let key = (!prefix.is_empty()).then_some(prefix);
        let below = format!("{}/{prefix}", self.bucket_path);
        self.allows(key, &Permission::READ)
            && self.rules.reads_below(&below)
            && !self.policies.reads_path()
    }
}

/// The subtrees at or below `root` the caller reaches, decided from the realm
/// and group roles together so a realm deny also applies, and narrowed by the
/// credential's own restrictions. `root` selects the authorization documents.
pub async fn reachable_roots(
    context: &DriverContext,
    auth_context: &AuthContext,
    root: &str,
) -> Result<Vec<String>, AuthorizationError> {
    let rules = permission_rules(context, auth_context, root).await?;
    Ok(readable_roots(
        &rules.direct_patterns(),
        auth_context.path_restrictions.as_deref(),
        root,
    ))
}

pub async fn permission_rules(
    context: &DriverContext,
    auth_context: &AuthContext,
    root: &str,
) -> Result<PermissionRules, AuthorizationError> {
    drive(
        PermissionRulesOperation::new(PermissionRulesConfig {
            auth_context: auth_context.clone(),
            path: root.to_string(),
        }),
        context,
    )
    .await
}

impl GroupPermissionRules {
    /// One rules drive per distinct group. Anonymous callers hold no rules, and
    /// a group whose collection fails keeps none, so its records stay hidden.
    pub async fn collect(
        context: &DriverContext,
        auth_context: Option<&AuthContext>,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Self {
        let Some(auth_context) = auth_context else {
            return Self::default();
        };

        let mut groups = HashMap::new();
        for group_id in group_ids.into_iter().collect::<BTreeSet<_>>() {
            let Ok(rules) = drive(
                PermissionRulesOperation::new(PermissionRulesConfig {
                    auth_context: auth_context.clone(),
                    path: format!("/{}/g/{group_id}", auth_context.realm_id),
                }),
                context,
            )
            .await
            else {
                continue;
            };
            groups.insert(group_id, rules);
        }

        Self::from_groups(Some(auth_context.realm_id), groups)
    }
}
