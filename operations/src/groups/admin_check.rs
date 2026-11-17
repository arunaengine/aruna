//! Decides whether a caller administers a group, either as its admin or as a realm groups admin.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::types::GroupId;

use crate::auth::check_permissions::{
    CheckPermissionsConfig, CheckPermissionsOperation, authorization_event,
};
use crate::users::service_account::check::{
    ServiceCheckConfig, ServiceCheckError, ServiceCheckOperation,
};

/// The realm path whose WRITE grant administers every group of the realm.
pub fn realm_groups_path(auth: &AuthContext) -> String {
    format!("/{}/admin/groups", auth.realm_id)
}

/// Group admins pass through the service account check, so service accounts and federated
/// callers never administer a group; everyone else needs WRITE on the realm groups path.
#[derive(Debug, PartialEq)]
pub struct AdminCheckOperation {
    auth_context: AuthContext,
    group_id: GroupId,
    realm_asked: bool,
}

impl AdminCheckOperation {
    pub fn new(auth_context: AuthContext, group_id: GroupId) -> Self {
        Self {
            auth_context,
            group_id,
            realm_asked: false,
        }
    }
}

impl Operation for AdminCheckOperation {
    type Output = bool;
    type Error = AuthorizationError;

    fn first(&mut self) -> Result<Next<bool>, AuthorizationError> {
        if self.auth_context.user_id.realm_id != self.auth_context.realm_id {
            return Ok(Next::Done(false));
        }
        let check = ServiceCheckOperation::new(ServiceCheckConfig {
            auth_context: self.auth_context.clone(),
            group_id: self.group_id,
            target: None,
        });
        Ok(Next::Call(IoCall::Child(boxed_suboperation(
            check,
            |result| authorization_event(ServiceCheckError::allowed(result)),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, AuthorizationError> {
        let answer = match result {
            IoResult::Event(event) => match *event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    Some(allowed)
                }
                _ => None,
            },
            _ => None,
        };
        let Some(allowed) = answer else {
            return Err(AuthorizationError::UnexpectedEvent {
                state: "AdminCheck".to_string(),
                expected: "authorization result",
                got: "another result".to_string(),
            });
        };
        if allowed == Ok(false) && !self.realm_asked {
            self.realm_asked = true;
            let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.auth_context.clone(),
                path: realm_groups_path(&self.auth_context),
                required_permission: Permission::WRITE,
            });
            let child = boxed_suboperation(check, authorization_event);
            return Ok(Next::Call(IoCall::Child(child)));
        }
        allowed.map(Next::Done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn auth(user_realm: u8) -> AuthContext {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        AuthContext {
            user_id: UserId::local(
                Ulid::from_bytes([4u8; 16]),
                RealmId::from_bytes([user_realm; 32]),
            ),
            realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    fn answer(allowed: Result<bool, AuthorizationError>) -> IoResult {
        IoResult::Event(Box::new(authorization_event(allowed)))
    }

    #[test]
    fn foreign_caller_refused() {
        // A caller of another realm never administers a group, and nothing is asked.
        let mut operation = AdminCheckOperation::new(auth(5), Ulid::from_bytes([1u8; 16]));
        assert_eq!(operation.first(), Ok(Next::Done(false)));
    }

    #[test]
    fn realm_admin_fallback() {
        // A caller who is no group admin is asked about the realm groups path once.
        let mut operation = AdminCheckOperation::new(auth(3), Ulid::from_bytes([1u8; 16]));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert!(matches!(
            operation.next(answer(Ok(false))),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(operation.next(answer(Ok(false))), Ok(Next::Done(false)));
        let mut operation = AdminCheckOperation::new(auth(3), Ulid::from_bytes([1u8; 16]));
        operation.first().unwrap();
        assert_eq!(operation.next(answer(Ok(true))), Ok(Next::Done(true)));
    }

    #[test]
    fn check_failures_end() {
        // A failed check or an answer that is not one ends the decision with an error.
        let mut operation = AdminCheckOperation::new(auth(3), Ulid::from_bytes([1u8; 16]));
        operation.first().unwrap();
        let failed = Err(AuthorizationError::DocNotFound);
        assert_eq!(
            operation.next(answer(failed)),
            Err(AuthorizationError::DocNotFound)
        );
        let mut operation = AdminCheckOperation::new(auth(3), Ulid::from_bytes([1u8; 16]));
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(AuthorizationError::UnexpectedEvent { .. })
        ));
    }
}
