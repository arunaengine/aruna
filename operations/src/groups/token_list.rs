//! Lists the tokens this node holds for one group, for the group's administrators.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::Effect;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;

use super::{GroupToken, GroupTokensError, TokenScan};
use crate::groups::admin_check::{AdminCheckOperation, authorization_event};

#[derive(Clone, Debug, PartialEq)]
pub struct GroupTokensConfig {
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    /// Unix seconds that decide expiry and cutoffs.
    pub now: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ListTokensState {
    Init,
    Authorize,
    Scan,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ListTokensOperation {
    config: GroupTokensConfig,
    scan: TokenScan,
    state: ListTokensState,
    output: Option<Result<Vec<GroupToken>, GroupTokensError>>,
}

impl ListTokensOperation {
    pub fn new(config: GroupTokensConfig) -> Self {
        let scan = TokenScan::new(config.auth_context.realm_id, config.group_id, config.now);
        Self {
            config,
            scan,
            state: ListTokensState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GroupTokensError) -> Effects {
        self.state = ListTokensState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

/// The scan starts only after a successful authorization result.
pub(crate) fn authorized(event: Event, state: &str) -> Result<(), GroupTokensError> {
    match event {
        Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: Ok(true) }) => Ok(()),
        Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: Ok(false) }) => {
            Err(GroupTokensError::Refused)
        }
        Event::SubOperation(SubOperationEvent::AuthorizationResult {
            allowed: Err(error),
        }) => Err(error.into()),
        other => Err(GroupTokensError::UnexpectedEvent {
            state: state.to_string(),
            expected: "authorization result",
            got: format!("{other:?}"),
        }),
    }
}

pub(crate) fn admin_check(auth_context: &AuthContext, group_id: GroupId) -> Effects {
    smallvec![Effect::SubOperation(boxed_suboperation(
        AdminCheckOperation::new(auth_context.clone(), group_id),
        authorization_event,
    ))]
}

impl Operation for ListTokensOperation {
    type Output = Vec<GroupToken>;
    type Error = GroupTokensError;

    fn start(&mut self) -> Effects {
        self.state = ListTokensState::Authorize;
        admin_check(&self.config.auth_context, self.config.group_id)
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ListTokensState::Authorize => match authorized(event, "Authorize") {
                Ok(()) => {
                    self.state = ListTokensState::Scan;
                    self.scan.start()
                }
                Err(error) => self.fail(error),
            },
            ListTokensState::Scan => match self.scan.step(event) {
                Ok(Some(effects)) => effects,
                Ok(None) => {
                    self.state = ListTokensState::Finish;
                    self.output = Some(Ok(std::mem::take(&mut self.scan.tokens)));
                    smallvec![]
                }
                Err(error) => self.fail(error),
            },
            ListTokensState::Init => self.fail(GroupTokensError::UnexpectedEvent {
                state: "Init".to_string(),
                expected: "no event before start",
                got: format!("{event:?}"),
            }),
            ListTokensState::Finish | ListTokensState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListTokensState::Finish | ListTokensState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(GroupTokensError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
