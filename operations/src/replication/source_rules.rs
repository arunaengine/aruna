//! Loads the permission rules and policies that decide which source objects a scope may read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::GroupId;

use super::version_replication::{SourceAuthorization, SourceAuthorizationError};
use crate::auth::enforcement::{PolicyEvaluator, PolicyLoadOperation};
use crate::auth::error::PolicyEnforcementError;
use crate::auth::permission_rules::{
    PermissionRules, PermissionRulesConfig, PermissionRulesOperation,
};

impl From<IoError> for SourceAuthorizationError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

type SourceTurn = Result<Next<SourceAuthorization>, SourceAuthorizationError>;

/// Reads the caller's permission rules and the policies of one group's source objects.
#[derive(Debug, PartialEq)]
pub(crate) struct SourceAuthOperation {
    auth_context: AuthContext,
    group_id: GroupId,
    source_node_id: NodeId,
    permissions: Option<PermissionRules>,
}

impl SourceAuthOperation {
    pub(crate) fn new(
        auth_context: AuthContext,
        group_id: GroupId,
        source_node_id: NodeId,
    ) -> Self {
        Self {
            auth_context,
            group_id,
            source_node_id,
            permissions: None,
        }
    }

    fn rules_read(&mut self, rules: Result<PermissionRules, AuthorizationError>) -> SourceTurn {
        self.permissions = Some(rules.map_err(permission_error)?);
        let realm_id = self.auth_context.realm_id;
        let load = PolicyLoadOperation::new(realm_id, Some(self.group_id));
        Ok(Next::Call(IoCall::child(load)))
    }

    fn policies_read(
        &mut self,
        permissions: PermissionRules,
        policies: Result<PolicyEvaluator, PolicyEnforcementError>,
    ) -> SourceTurn {
        let policies =
            policies.map_err(|error| SourceAuthorizationError::Unavailable(error.to_string()))?;
        Ok(Next::Done(SourceAuthorization {
            group_id: self.group_id,
            source_node_id: self.source_node_id,
            auth_context: self.auth_context.clone(),
            permissions,
            policies,
        }))
    }
}

impl Operation for SourceAuthOperation {
    type Output = SourceAuthorization;
    type Error = SourceAuthorizationError;

    fn first(&mut self) -> SourceTurn {
        let auth_context = self.auth_context.clone();
        let path = format!("/{}/g/{}", auth_context.realm_id, self.group_id);
        let rules = PermissionRulesOperation::new(PermissionRulesConfig { auth_context, path });
        Ok(Next::Call(IoCall::child(rules)))
    }

    fn next(&mut self, result: IoResult) -> SourceTurn {
        match self.permissions.take() {
            None => self.rules_read(result.child::<PermissionRules, AuthorizationError>()?),
            Some(permissions) => {
                let policies = result.child::<PolicyEvaluator, PolicyEnforcementError>()?;
                self.policies_read(permissions, policies)
            }
        }
    }
}

fn permission_error(error: AuthorizationError) -> SourceAuthorizationError {
    match error {
        AuthorizationError::DocNotFound
        | AuthorizationError::GroupNotFound
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::InvalidRealmId => SourceAuthorizationError::Denied,
        other => SourceAuthorizationError::Unavailable(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn operation() -> SourceAuthOperation {
        let realm_id = RealmId::from_bytes([1; 32]);
        let auth = AuthContext {
            user_id: UserId::nil(realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        SourceAuthOperation::new(auth, Ulid::from_bytes([3; 16]), node)
    }

    fn rules(result: Result<PermissionRules, AuthorizationError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    #[test]
    fn missing_group_denied() {
        let mut operation = operation();
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let turn = operation.next(rules(Err(AuthorizationError::GroupNotFound)));
        assert!(matches!(turn, Err(SourceAuthorizationError::Denied)));
    }

    #[test]
    fn unreadable_policies_unavailable() {
        let mut operation = operation();
        operation.first().unwrap();
        let turn = operation.next(rules(Ok(PermissionRules::default())));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let failed = PolicyEnforcementError::Unavailable("gone".to_string());
        let failed = Err::<PolicyEvaluator, _>(failed);
        let turn = operation.next(IoResult::Child(ChildOutcome::new(failed)));
        assert!(matches!(
            turn,
            Err(SourceAuthorizationError::Unavailable(_))
        ));
    }
}
