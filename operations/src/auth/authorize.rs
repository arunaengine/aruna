//! Authorizes one request as an operation: the role check, then the request policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::operation::Operation;
use aruna_core::request_policy::PolicyRequest;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::enforcement::{PolicyEvaluator, PolicyLoadOperation};
use crate::auth::error::{AuthorizeError, PolicyEnforcementError, check_error};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};

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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::errors::StorageError;
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmConfigDocument;
    use ulid::Ulid;

    const PATH: &str = "/documents/one";

    fn realm() -> RealmId {
        RealmId::from_bytes([1; 32])
    }

    fn auth() -> AuthContext {
        AuthContext {
            user_id: UserId::nil(realm()),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        }
    }

    fn operation() -> AuthorizeOperation {
        AuthorizeOperation::new(
            realm(),
            auth(),
            PATH.to_string(),
            Permission::READ,
            PolicyRequestExtras::rest(),
        )
    }

    fn role_answer(answer: Result<bool, AuthorizationError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(answer))
    }

    /// The evaluator a realm with `policies` loads.
    fn evaluator(policies: Vec<RequestPolicy>) -> IoResult {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
        config.request_policies = policies;
        let mut load = PolicyLoadOperation::new(realm(), None);
        load.first().unwrap();
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            user_id: UserId::nil(realm()),
            realm_id: realm(),
        };
        let bytes = config.to_bytes(&actor).unwrap();
        let stored = IoResult::Values(vec![Some(bytes.into())]);
        let Ok(Next::Done(evaluator)) = load.next(stored) else {
            panic!("the policies should load");
        };
        IoResult::Child(ChildOutcome::new(Ok::<_, PolicyEnforcementError>(
            evaluator,
        )))
    }

    /// An operation whose role check passed.
    fn loading() -> AuthorizeOperation {
        let mut authorize = operation();
        authorize.first().unwrap();
        let load = PolicyLoadOperation::for_request(realm(), &authorize.request());
        assert_eq!(
            authorize.next(role_answer(Ok(true))),
            Ok(Next::Call(IoCall::child(load)))
        );
        authorize
    }

    #[test]
    fn checks_role_first() {
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth(),
            path: PATH.to_string(),
            required_permission: Permission::READ,
        });
        assert_eq!(operation().first(), Ok(Next::Call(IoCall::child(check))));
    }

    #[test]
    fn allows_after_policies() {
        let mut authorize = loading();
        assert_eq!(authorize.next(evaluator(Vec::new())), Ok(Next::Done(())));
    }

    #[test]
    fn policy_denies_request() {
        let deny = RequestPolicy {
            policy_id: Ulid::from_bytes([5; 16]),
            name: "deny".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: "true".to_string(),
            enabled: true,
        };
        let denied = loading().next(evaluator(vec![deny])).unwrap_err();
        assert!(matches!(
            denied,
            AuthorizeError::Policy(PolicyEnforcementError::Denied { .. })
        ));
        assert!(AuthorizeOperation::expected_error(&denied));
    }

    #[test]
    fn role_denies_request() {
        let mut authorize = operation();
        authorize.first().unwrap();
        let denied = authorize.next(role_answer(Ok(false))).unwrap_err();
        assert_eq!(denied, AuthorizeError::PermissionDenied);
        assert!(AuthorizeOperation::expected_error(&denied));
    }

    #[test]
    fn keeps_storage_failure() {
        let mut authorize = operation();
        authorize.first().unwrap();
        let failed = AuthorizationError::StorageError(StorageError::Timeout);
        let error = authorize.next(role_answer(Err(failed))).unwrap_err();
        assert_eq!(error, AuthorizeError::Storage(StorageError::Timeout));
        assert!(!AuthorizeOperation::expected_error(&error));
    }
}
