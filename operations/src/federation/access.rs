//! Checks one federation step: the caller's permission on a path, then the request policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::enforcement::{PolicyEnforcementError, PolicyEvaluator, PolicyLoadOperation};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};

/// Whether `auth` holds `permission` on `path` and no policy denies the request. A check that
/// cannot be decided counts as denied, so a federation step fails closed.
#[derive(Debug, PartialEq)]
pub(super) struct AccessCheckOperation {
    realm_id: RealmId,
    auth: AuthContext,
    path: String,
    permission: Permission,
    extras: PolicyRequestExtras,
    loading: bool,
}

impl AccessCheckOperation {
    pub(super) fn new(
        realm_id: RealmId,
        auth: AuthContext,
        (path, permission): (String, Permission),
        extras: PolicyRequestExtras,
    ) -> Self {
        Self {
            realm_id,
            auth,
            path,
            permission,
            extras,
            loading: false,
        }
    }

    fn request(&self) -> aruna_core::request_policy::PolicyRequest {
        let auth = Some(&self.auth);
        policy_request_with(&self.path, &self.permission, auth, self.extras.clone())
    }
}

impl Operation for AccessCheckOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.auth.clone(),
            path: self.path.clone(),
            required_permission: self.permission.clone(),
        });
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        if self.loading {
            let loaded = result.child::<PolicyEvaluator, PolicyEnforcementError>()?;
            let request = self.request();
            let allowed = loaded.and_then(|policies| policies.evaluate(&request));
            return Ok(Next::Done(allowed.is_ok()));
        }
        if !matches!(result.child::<bool, AuthorizationError>()?, Ok(true)) {
            return Ok(Next::Done(false));
        }
        self.loading = true;
        let load = PolicyLoadOperation::for_request(self.realm_id, &self.request());
        Ok(Next::Call(IoCall::child(load)))
    }
}

/// Whether a finished [`AccessCheckOperation`] allowed the step.
pub(super) fn access_granted(result: IoResult) -> bool {
    matches!(result.child::<bool, IoError>(), Ok(Ok(true)))
}

#[cfg(test)]
mod tests {
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use ulid::Ulid;

    use super::*;

    const REALM: RealmId = RealmId([3; 32]);

    fn check() -> AccessCheckOperation {
        let auth = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([1; 16]), REALM),
            realm_id: REALM,
            path_restrictions: None,
            session: None,
        };
        let target = ("/doc".to_string(), Permission::READ);
        AccessCheckOperation::new(REALM, auth, target, PolicyRequestExtras::default())
    }

    fn permission(result: Result<bool, AuthorizationError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    #[test]
    fn missing_permission_denies() {
        let mut operation = check();
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(operation.next(permission(Ok(false))), Ok(Next::Done(false)));
    }

    #[test]
    fn granted_loads_policies() {
        let mut operation = check();
        let request = operation.request();
        let load = IoCall::child(PolicyLoadOperation::for_request(REALM, &request));
        assert_eq!(operation.next(permission(Ok(true))), Ok(Next::Call(load)));
        let unreadable =
            Err::<PolicyEvaluator, _>(PolicyEnforcementError::Unavailable("down".to_string()));
        let answer = operation.next(IoResult::Child(ChildOutcome::new(unreadable)));
        assert_eq!(answer, Ok(Next::Done(false)));
    }

    #[test]
    fn only_yes_grants() {
        let answer = |value| IoResult::Child(ChildOutcome::new(value));
        assert!(access_granted(answer(Ok::<_, IoError>(true))));
        assert!(!access_granted(answer(Ok::<_, IoError>(false))));
        assert!(!access_granted(answer(Err::<bool, _>(IoError::Unfinished))));
    }
}
