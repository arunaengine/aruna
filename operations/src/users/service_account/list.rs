//! Lists the service accounts one group owns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::USER_KEYSPACE;
use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::user::User;
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use thiserror::Error;

use super::check::{ServiceCheckConfig, ServiceCheckError, ServiceCheckOperation};

const PAGE: usize = 256;

#[derive(Debug, Error, PartialEq)]
pub enum ListServiceError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error("caller may not administer this group's service accounts")]
    Unauthorized,
    #[error("service account listing did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum ListServiceState {
    Init,
    Authorize,
    Scan,
    Finish,
    Error,
}

/// Scans the realm's user records, since ownership lives on the account and not on the group.
#[derive(Debug, PartialEq)]
pub struct ListServiceOperation {
    auth_context: AuthContext,
    group_id: GroupId,
    accounts: Vec<User>,
    state: ListServiceState,
    output: Option<Result<Vec<User>, ListServiceError>>,
}

impl ListServiceOperation {
    pub fn new(auth_context: AuthContext, group_id: GroupId) -> Self {
        Self {
            auth_context,
            group_id,
            accounts: Vec::new(),
            state: ListServiceState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ListServiceError) -> Effects {
        self.state = ListServiceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.fail(ListServiceError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        })
    }

    fn scan(&mut self, start: Option<Key>) -> Effects {
        self.state = ListServiceState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: USER_KEYSPACE.to_string(),
            prefix: Some(UserId::storage_prefix(self.auth_context.realm_id)),
            start: start.map(IterStart::After),
            limit: PAGE,
            txn_id: None,
        })]
    }

    fn handle_authorized(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: Ok(true) }) => {
                self.scan(None)
            }
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: Ok(false) }) => {
                self.fail(ListServiceError::Unauthorized)
            }
            Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: Err(error),
            }) => self.fail(error.into()),
            other => self.unexpected("authorization result", other),
        }
    }

    fn handle_page(&mut self, event: Event) -> Effects {
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return self.fail(error.into()),
            other => return self.unexpected("user page", other),
        };
        for (_, value) in values {
            match User::from_bytes(&value) {
                Ok(user) if user.service_group() == Some(self.group_id) => {
                    self.accounts.push(user);
                }
                Ok(_) => {}
                Err(error) => return self.fail(error.into()),
            }
        }
        match next_start_after {
            Some(start) => self.scan(Some(start)),
            None => {
                self.state = ListServiceState::Finish;
                self.output = Some(Ok(std::mem::take(&mut self.accounts)));
                smallvec![]
            }
        }
    }
}

impl Operation for ListServiceOperation {
    type Output = Vec<User>;
    type Error = ListServiceError;

    fn start(&mut self) -> Effects {
        self.state = ListServiceState::Authorize;
        smallvec![Effect::SubOperation(boxed_suboperation(
            ServiceCheckOperation::new(ServiceCheckConfig {
                auth_context: self.auth_context.clone(),
                group_id: self.group_id,
                target: None,
            }),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: ServiceCheckError::allowed(result),
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ListServiceState::Authorize => self.handle_authorized(event),
            ListServiceState::Scan => self.handle_page(event),
            ListServiceState::Init => self.unexpected("no event before start", event),
            ListServiceState::Finish | ListServiceState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListServiceState::Finish | ListServiceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListServiceError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
