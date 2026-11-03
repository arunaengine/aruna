//! Decides whether a caller administers a group, either as its admin or as a realm groups admin.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::Effect;
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::users::service_account::check::{
    ServiceCheckConfig, ServiceCheckError, ServiceCheckOperation,
};

/// The realm path whose WRITE grant administers every group of the realm.
pub fn realm_groups_path(auth: &AuthContext) -> String {
    format!("/{}/admin/groups", auth.realm_id)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AdminCheckState {
    Init,
    GroupAdmin,
    RealmAdmin,
    Finish,
}

/// Group admins pass through the service account check, so service accounts and federated
/// callers never administer a group; everyone else needs WRITE on the realm groups path.
#[derive(Debug, PartialEq)]
pub struct AdminCheckOperation {
    auth_context: AuthContext,
    group_id: GroupId,
    state: AdminCheckState,
    output: Option<Result<bool, AuthorizationError>>,
}

impl AdminCheckOperation {
    pub fn new(auth_context: AuthContext, group_id: GroupId) -> Self {
        Self {
            auth_context,
            group_id,
            state: AdminCheckState::Init,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<bool, AuthorizationError>) -> Effects {
        self.state = AdminCheckState::Finish;
        self.output = Some(result);
        smallvec![]
    }
}

/// Maps any authorization sub-operation onto the shared result event.
pub fn authorization_event(allowed: Result<bool, AuthorizationError>) -> Event {
    Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed })
}

impl Operation for AdminCheckOperation {
    type Output = bool;
    type Error = AuthorizationError;

    fn start(&mut self) -> Effects {
        if self.auth_context.user_id.realm_id != self.auth_context.realm_id {
            return self.finish(Ok(false));
        }
        self.state = AdminCheckState::GroupAdmin;
        smallvec![Effect::SubOperation(boxed_suboperation(
            ServiceCheckOperation::new(ServiceCheckConfig {
                auth_context: self.auth_context.clone(),
                group_id: self.group_id,
                target: None,
            }),
            |result| authorization_event(ServiceCheckError::allowed(result)),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            let error = AuthorizationError::UnexpectedEvent {
                state: format!("{:?}", self.state),
                expected: "authorization result",
                got: format!("{event:?}"),
            };
            return self.finish(Err(error));
        };
        match (&self.state, allowed) {
            (AdminCheckState::GroupAdmin, Ok(false)) => {
                self.state = AdminCheckState::RealmAdmin;
                smallvec![Effect::SubOperation(boxed_suboperation(
                    CheckPermissionsOperation::new(CheckPermissionsConfig {
                        auth_context: self.auth_context.clone(),
                        path: realm_groups_path(&self.auth_context),
                        required_permission: Permission::WRITE,
                    }),
                    authorization_event,
                ))]
            }
            (AdminCheckState::GroupAdmin | AdminCheckState::RealmAdmin, allowed) => {
                self.finish(allowed)
            }
            (AdminCheckState::Init | AdminCheckState::Finish, _) => {
                self.finish(Err(AuthorizationError::NotFinished))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == AdminCheckState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(AuthorizationError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
