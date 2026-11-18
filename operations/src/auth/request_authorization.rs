//! Authorizes REST and S3 requests: RBAC first, then policy checks.
//! Bulk routes reuse one evaluator so a group's policy state is read only once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::enforcement::{EnforcePolicyOperation, PolicyEvaluator, PolicyLoadOperation};
use crate::auth::error::{AuthorizeError, PolicyEnforcementError, check_error};
use crate::auth::permission_rules::{
    GroupPermissionRules, PermissionRules, PermissionRulesConfig, PermissionRulesOperation,
};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
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

type AuthorizeTurn = Result<Next<()>, AuthorizeError>;

fn check_call(auth: &AuthContext, path: &str, permission: &Permission) -> IoCall {
    IoCall::child(CheckPermissionsOperation::new(CheckPermissionsConfig {
        auth_context: auth.clone(),
        path: path.to_string(),
        required_permission: permission.clone(),
    }))
}

fn check_answer(result: IoResult) -> Result<bool, AuthorizeError> {
    result
        .child::<bool, AuthorizationError>()?
        .map_err(check_error)
}

fn enforced(result: IoResult) -> Result<(), AuthorizeError> {
    Ok(result.child::<(), PolicyEnforcementError>()??)
}

/// Authorizes one action: ordinary RBAC and public visibility first, then the
/// realm and group deny/require policies. Neither layer may grant what the
/// other denied.
#[derive(Debug, PartialEq)]
pub struct AuthorizeOperation {
    realm_id: RealmId,
    auth: AuthContext,
    path: String,
    permission: Permission,
    request: Option<PolicyRequest>,
}

impl AuthorizeOperation {
    pub fn new(
        realm_id: RealmId,
        auth: &AuthContext,
        path: &str,
        permission: &Permission,
        extras: PolicyRequestExtras,
    ) -> Self {
        Self {
            realm_id,
            auth: auth.clone(),
            path: path.to_string(),
            permission: permission.clone(),
            request: Some(policy_request_with(path, permission, Some(auth), extras)),
        }
    }
}

impl Operation for AuthorizeOperation {
    type Output = ();
    type Error = AuthorizeError;

    fn first(&mut self) -> AuthorizeTurn {
        Ok(Next::Call(check_call(
            &self.auth,
            &self.path,
            &self.permission,
        )))
    }

    fn next(&mut self, result: IoResult) -> AuthorizeTurn {
        let Some(request) = self.request.take() else {
            enforced(result)?;
            return Ok(Next::Done(()));
        };
        if !check_answer(result)? {
            return Err(AuthorizeError::PermissionDenied);
        }
        Ok(Next::Call(IoCall::child(EnforcePolicyOperation::new(
            self.realm_id,
            request,
        ))))
    }
}

/// Authorizes `permission` by RBAC on `path` or else on `alternative`, then always enforces the
/// policies of `path`, and of `alternative` when only it granted. A policy denial is final.
#[derive(Debug, PartialEq)]
pub struct AuthorizeEitherOperation {
    realm_id: RealmId,
    auth: AuthContext,
    path: String,
    alternative: String,
    permission: Permission,
    stage: EitherStage,
    /// Policy requests still to enforce; the next one pops off the end.
    requests: Vec<PolicyRequest>,
}

#[derive(Debug, PartialEq)]
enum EitherStage {
    Direct,
    Alternative,
    Policies,
}

impl AuthorizeEitherOperation {
    pub fn new(
        realm_id: RealmId,
        auth: &AuthContext,
        (path, alternative): (&str, &str),
        permission: &Permission,
    ) -> Self {
        Self {
            realm_id,
            auth: auth.clone(),
            path: path.to_string(),
            alternative: alternative.to_string(),
            permission: permission.clone(),
            stage: EitherStage::Direct,
            requests: Vec::new(),
        }
    }

    fn request(&self, path: &str) -> PolicyRequest {
        let extras = PolicyRequestExtras::rest();
        policy_request_with(path, &self.permission, Some(&self.auth), extras)
    }

    fn enforce(&mut self) -> AuthorizeTurn {
        self.stage = EitherStage::Policies;
        Ok(match self.requests.pop() {
            Some(request) => Next::Call(IoCall::child(EnforcePolicyOperation::new(
                self.realm_id,
                request,
            ))),
            None => Next::Done(()),
        })
    }
}

impl Operation for AuthorizeEitherOperation {
    type Output = ();
    type Error = AuthorizeError;

    fn first(&mut self) -> AuthorizeTurn {
        Ok(Next::Call(check_call(
            &self.auth,
            &self.path,
            &self.permission,
        )))
    }

    fn next(&mut self, result: IoResult) -> AuthorizeTurn {
        match self.stage {
            EitherStage::Direct => {
                self.requests = vec![self.request(&self.path)];
                if check_answer(result)? {
                    return self.enforce();
                }
                self.stage = EitherStage::Alternative;
                Ok(Next::Call(check_call(
                    &self.auth,
                    &self.alternative,
                    &self.permission,
                )))
            }
            EitherStage::Alternative => {
                if !check_answer(result)? {
                    return Err(AuthorizeError::PermissionDenied);
                }
                self.requests.insert(0, self.request(&self.alternative));
                self.enforce()
            }
            EitherStage::Policies => {
                enforced(result)?;
                self.enforce()
            }
        }
    }
}

/// Authorizes reading the metadata document at `path`: a public one needs only the request
/// policies, as every metadata read decides it, a private one also READ by role.
#[derive(Debug, PartialEq)]
pub enum AuthorizeDocumentOperation {
    Public(EnforcePolicyOperation),
    Private(AuthorizeOperation),
}

impl AuthorizeDocumentOperation {
    pub fn new(
        realm_id: RealmId,
        auth: &AuthContext,
        (path, public): (&str, bool),
        extras: PolicyRequestExtras,
    ) -> Self {
        let permission = Permission::READ;
        match public {
            true => Self::Public(EnforcePolicyOperation::new(
                realm_id,
                policy_request_with(path, &permission, Some(auth), extras),
            )),
            false => Self::Private(AuthorizeOperation::new(
                realm_id,
                auth,
                path,
                &permission,
                extras,
            )),
        }
    }
}

impl Operation for AuthorizeDocumentOperation {
    type Output = ();
    type Error = AuthorizeError;

    fn first(&mut self) -> AuthorizeTurn {
        match self {
            Self::Public(enforce) => Ok(enforce.first()?),
            Self::Private(authorize) => authorize.first(),
        }
    }

    fn next(&mut self, result: IoResult) -> AuthorizeTurn {
        match self {
            Self::Public(enforce) => Ok(enforce.next(result)?),
            Self::Private(authorize) => authorize.next(result),
        }
    }
}

/// One caller's rules and policies for a bucket, loaded once to decide many of its keys.
#[derive(Debug, PartialEq)]
pub struct BucketAccess {
    auth: AuthContext,
    bucket_path: String,
    rules: PermissionRules,
    policies: PolicyEvaluator,
}

impl BucketAccess {
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

/// Loads one caller's [`BucketAccess`]: the rules below the bucket, then its policy sets.
#[derive(Debug, PartialEq)]
pub struct BucketAccessOperation {
    auth: AuthContext,
    realm_id: RealmId,
    group_id: GroupId,
    bucket_path: String,
    rules: Option<PermissionRules>,
}

impl BucketAccessOperation {
    pub fn new(
        auth: &AuthContext,
        (realm_id, group_id, node_id): (RealmId, GroupId, NodeId),
        bucket: &str,
    ) -> Self {
        Self {
            auth: auth.clone(),
            realm_id,
            group_id,
            bucket_path: bucket_permission_path(realm_id, group_id, node_id, bucket),
            rules: None,
        }
    }
}

impl Operation for BucketAccessOperation {
    type Output = BucketAccess;
    type Error = AuthorizeError;

    fn first(&mut self) -> Result<Next<BucketAccess>, AuthorizeError> {
        Ok(Next::Call(IoCall::child(rules_operation(
            &self.auth,
            &self.bucket_path,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BucketAccess>, AuthorizeError> {
        let Some(rules) = self.rules.take() else {
            let rules = result
                .child::<PermissionRules, AuthorizationError>()?
                .map_err(check_error)?;
            self.rules = Some(rules);
            return Ok(Next::Call(IoCall::child(PolicyLoadOperation::new(
                self.realm_id,
                Some(self.group_id),
            ))));
        };
        let policies = result.child::<PolicyEvaluator, PolicyEnforcementError>()??;
        Ok(Next::Done(BucketAccess {
            auth: self.auth.clone(),
            bucket_path: self.bucket_path.clone(),
            rules,
            policies,
        }))
    }
}

/// Collects the caller's permission rules below `root`.
pub fn rules_operation(auth_context: &AuthContext, root: &str) -> PermissionRulesOperation {
    PermissionRulesOperation::new(PermissionRulesConfig {
        auth_context: auth_context.clone(),
        path: root.to_string(),
    })
}

/// The subtrees at or below `root` the caller reaches, decided from the realm
/// and group roles together so a realm deny also applies, and narrowed by the
/// credential's own restrictions.
pub fn reachable_roots(
    rules: &PermissionRules,
    auth_context: &AuthContext,
    root: &str,
) -> Vec<String> {
    readable_roots(
        &rules.direct_patterns(),
        auth_context.path_restrictions.as_deref(),
        root,
    )
}

/// One rules collection per distinct group. Anonymous callers hold no rules, and
/// a group whose collection fails keeps none, so its records stay hidden.
#[derive(Debug, PartialEq)]
pub struct GroupRulesOperation {
    auth_context: Option<AuthContext>,
    /// Reversed, so the next group pops off the end.
    remaining: Vec<GroupId>,
    current: Option<GroupId>,
    groups: HashMap<GroupId, PermissionRules>,
}

impl GroupRulesOperation {
    pub fn new(
        auth_context: Option<&AuthContext>,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Self {
        let distinct: BTreeSet<_> = group_ids.into_iter().collect();
        Self {
            auth_context: auth_context.cloned(),
            remaining: distinct.into_iter().rev().collect(),
            current: None,
            groups: HashMap::new(),
        }
    }

    fn ask(&mut self) -> Next<GroupPermissionRules> {
        let Some(auth) = self.auth_context.as_ref() else {
            return Next::Done(GroupPermissionRules::default());
        };
        let Some(group_id) = self.remaining.pop() else {
            let groups = std::mem::take(&mut self.groups);
            return Next::Done(GroupPermissionRules::from_groups(
                Some(auth.realm_id),
                groups,
            ));
        };
        self.current = Some(group_id);
        let root = format!("/{}/g/{group_id}", auth.realm_id);
        Next::Call(IoCall::child(rules_operation(auth, &root)))
    }
}

impl Operation for GroupRulesOperation {
    type Output = GroupPermissionRules;
    type Error = AuthorizationError;

    fn first(&mut self) -> Result<Next<GroupPermissionRules>, AuthorizationError> {
        Ok(self.ask())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<GroupPermissionRules>, AuthorizationError> {
        let rules = result.child::<PermissionRules, AuthorizationError>();
        if let (Some(group_id), Ok(Ok(rules))) = (self.current.take(), rules) {
            self.groups.insert(group_id, rules);
        }
        Ok(self.ask())
    }
}

#[cfg(test)]
#[path = "request_authorization_tests.rs"]
mod operation_tests;
