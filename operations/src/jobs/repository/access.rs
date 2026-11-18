//! Checks a caller's access for repository and harvest work: RBAC first, then the policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::operation::Operation;
use aruna_core::request_policy::PolicyRequest;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::enforcement::{PolicyEvaluator, PolicyLoadOperation};
use crate::auth::error::{AuthorizeError, PolicyEnforcementError, check_error};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};

/// Why an access check did not pass.
#[derive(Debug, Error, PartialEq)]
pub(crate) enum AccessError {
    /// RBAC or a policy refused the caller.
    #[error("{0}")]
    Denied(String),
    /// The check itself could not finish.
    #[error("{0}")]
    Unavailable(String),
}

impl From<IoError> for AccessError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

impl From<AuthorizeError> for AccessError {
    fn from(error: AuthorizeError) -> Self {
        let text = error.to_string();
        match error {
            AuthorizeError::PermissionDenied
            | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }) => Self::Denied(text),
            AuthorizeError::Policy(PolicyEnforcementError::Unavailable(_))
            | AuthorizeError::Storage(_)
            | AuthorizeError::CheckFailed(_) => Self::Unavailable(text),
        }
    }
}

#[derive(Debug, PartialEq)]
enum AccessStep {
    Permission,
    Policies,
}

/// Grants nothing that RBAC or a realm or group policy refuses.
#[derive(Debug, PartialEq)]
pub(crate) struct AccessCheckOperation {
    realm_id: RealmId,
    auth: AuthContext,
    path: String,
    permission: Permission,
    request: PolicyRequest,
    step: AccessStep,
}

impl AccessCheckOperation {
    pub(crate) fn new(
        realm_id: RealmId,
        auth: &AuthContext,
        path: String,
        permission: Permission,
        extras: PolicyRequestExtras,
    ) -> Self {
        let request = policy_request_with(&path, &permission, Some(auth), extras);
        Self {
            realm_id,
            auth: auth.clone(),
            path,
            permission,
            request,
            step: AccessStep::Permission,
        }
    }
}

impl Operation for AccessCheckOperation {
    type Output = ();
    type Error = AccessError;

    fn first(&mut self) -> Result<Next<()>, AccessError> {
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.auth.clone(),
                path: self.path.clone(),
                required_permission: self.permission.clone(),
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AccessError> {
        match self.step {
            AccessStep::Permission => {
                let allowed = result
                    .child::<bool, AuthorizationError>()?
                    .map_err(|error| AccessError::from(check_error(error)))?;
                if !allowed {
                    return Err(AuthorizeError::PermissionDenied.into());
                }
                self.step = AccessStep::Policies;
                Ok(Next::Call(IoCall::child(PolicyLoadOperation::for_request(
                    self.realm_id,
                    &self.request,
                ))))
            }
            AccessStep::Policies => {
                result
                    .child::<PolicyEvaluator, PolicyEnforcementError>()?
                    .and_then(|evaluator| evaluator.evaluate(&self.request))
                    .map_err(|error| AccessError::from(AuthorizeError::from(error)))?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::repository::push::creator_auth;
    use crate::jobs::repository::test_support::{child, link, scope};

    fn check() -> AccessCheckOperation {
        let auth = creator_auth(&scope(), &link());
        AccessCheckOperation::new(
            auth.realm_id,
            &auth,
            "/realm/g/group/meta/**".into(),
            Permission::READ,
            PolicyRequestExtras::default(),
        )
    }

    #[test]
    fn rbac_runs_first() {
        let mut operation = check();
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let denied = child::<bool, AuthorizationError>(Ok(false));
        assert_eq!(
            operation.next(denied),
            Err(AccessError::Denied("permission denied".into()))
        );
    }

    #[test]
    fn policies_follow_rbac() {
        let mut operation = check();
        operation.first().unwrap();
        let allowed = child::<bool, AuthorizationError>(Ok(true));
        assert!(matches!(
            operation.next(allowed),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let unavailable = child::<PolicyEvaluator, PolicyEnforcementError>(Err(
            PolicyEnforcementError::Unavailable("down".into()),
        ));
        assert!(matches!(
            operation.next(unavailable),
            Err(AccessError::Unavailable(_))
        ));
    }

    #[test]
    fn missing_group_denied() {
        let mut operation = check();
        operation.first().unwrap();
        let missing = child::<bool, AuthorizationError>(Err(AuthorizationError::GroupNotFound));
        assert!(matches!(
            operation.next(missing),
            Err(AccessError::Denied(_))
        ));
    }
}
