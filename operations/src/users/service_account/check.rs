//! Checks that a caller may administer a group's service accounts, and the account it names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::user::User;
use aruna_core::types::{Effects, GroupId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use super::group_admin_path;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

#[derive(Clone, Debug, PartialEq)]
pub struct ServiceCheckConfig {
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    /// A service account that must belong to the group and be active.
    pub target: Option<UserId>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ServiceCheckError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error("caller may not administer this group's service accounts")]
    Refused,
    #[error("no service account of this group has that id")]
    NotFound,
    #[error("the service account is deactivated")]
    Deactivated,
    #[error("service account check did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl ServiceCheckError {
    /// The result a composing operation sees: refusals deny, failures stay failures.
    pub fn allowed(result: Result<Option<User>, Self>) -> Result<bool, AuthorizationError> {
        match result {
            Ok(_) => Ok(true),
            Err(Self::Storage(error)) => Err(error.into()),
            Err(Self::Conversion(error)) => Err(error.into()),
            Err(Self::Authorization(error)) => Err(error),
            Err(_) => Ok(false),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ServiceCheckState {
    Init,
    ReadRecords,
    Authorize { target: Option<User> },
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ServiceCheckOperation {
    config: ServiceCheckConfig,
    state: ServiceCheckState,
    output: Option<Result<Option<User>, ServiceCheckError>>,
}

impl ServiceCheckOperation {
    pub fn new(config: ServiceCheckConfig) -> Self {
        Self {
            config,
            state: ServiceCheckState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ServiceCheckError) -> Effects {
        self.state = ServiceCheckState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.fail(ServiceCheckError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        })
    }

    fn handle_records(&mut self, event: Event) -> Effects {
        let values = match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => values,
            Event::Storage(StorageEvent::Error { error }) => return self.fail(error.into()),
            other => return self.unexpected("batch read result", other),
        };
        let mut users = Vec::with_capacity(values.len());
        for (_, value) in values {
            match value.as_deref().map(User::from_bytes).transpose() {
                Ok(user) => users.push(user),
                Err(error) => return self.fail(error.into()),
            }
        }
        // Service accounts never administer accounts, whatever roles they hold.
        if users
            .first()
            .and_then(Option::as_ref)
            .is_some_and(|caller| caller.service_group().is_some())
        {
            return self.fail(ServiceCheckError::Refused);
        }
        let target = users.into_iter().nth(1).flatten();
        if self.config.target.is_some() {
            let Some(user) = target.as_ref() else {
                return self.fail(ServiceCheckError::NotFound);
            };
            if user.service_group() != Some(self.config.group_id) {
                return self.fail(ServiceCheckError::NotFound);
            }
            if user.is_deactivated() {
                return self.fail(ServiceCheckError::Deactivated);
            }
        }
        self.state = ServiceCheckState::Authorize { target };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: group_admin_path(self.config.auth_context.realm_id, self.config.group_id),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn handle_authorized(&mut self, event: Event, target: Option<User>) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: Ok(true) }) => {
                self.state = ServiceCheckState::Finish;
                self.output = Some(Ok(target));
                smallvec![]
            }
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: Ok(false) }) => {
                self.fail(ServiceCheckError::Refused)
            }
            Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: Err(error),
            }) => self.fail(error.into()),
            other => self.unexpected("authorization result", other),
        }
    }
}

fn user_read(user_id: &UserId) -> (String, ByteView) {
    (
        USER_KEYSPACE.to_string(),
        ByteView::from(user_id.to_bytes()),
    )
}

impl Operation for ServiceCheckOperation {
    type Output = Option<User>;
    type Error = ServiceCheckError;

    fn start(&mut self) -> Effects {
        let mut reads = vec![user_read(&self.config.auth_context.user_id)];
        reads.extend(self.config.target.as_ref().map(user_read));
        self.state = ServiceCheckState::ReadRecords;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match std::mem::replace(&mut self.state, ServiceCheckState::Error) {
            ServiceCheckState::ReadRecords => self.handle_records(event),
            ServiceCheckState::Authorize { target } => self.handle_authorized(event, target),
            ServiceCheckState::Init => self.unexpected("no event before start", event),
            state @ (ServiceCheckState::Finish | ServiceCheckState::Error) => {
                self.state = state;
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ServiceCheckState::Finish | ServiceCheckState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ServiceCheckError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
