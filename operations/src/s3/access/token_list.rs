//! Lists the tokens this node holds for one group, for the group's administrators.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::GroupId;

use super::{GroupToken, GroupTokensError, TokenCursor, TokenScan};
use crate::groups::admin_check::AdminCheckOperation;

#[derive(Clone, Debug, PartialEq)]
pub struct GroupTokensConfig {
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    /// Unix seconds that decide expiry and cutoffs.
    pub now: u64,
    /// Where the previous page stopped; `None` starts at the beginning.
    pub cursor: Option<TokenCursor>,
    /// Tokens per page; one service account's sessions are never split across pages.
    pub limit: usize,
}

/// One page of tokens and the cursor of the next page, absent after the last.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TokenPage {
    pub tokens: Vec<GroupToken>,
    pub next: Option<TokenCursor>,
}

#[derive(Debug, PartialEq)]
pub struct ListTokensOperation {
    config: GroupTokensConfig,
    scan: TokenScan,
    /// Whether the admin check passed and the scan runs.
    scanning: bool,
}

impl ListTokensOperation {
    pub fn new(config: GroupTokensConfig) -> Self {
        let scope = (config.auth_context.realm_id, config.group_id);
        let scan = TokenScan::new(scope, config.now, config.cursor.clone(), config.limit);
        Self {
            config,
            scan,
            scanning: false,
        }
    }
}

/// The admin check of `group_id` as a child call.
pub(crate) fn admin_check(auth_context: &AuthContext, group_id: GroupId) -> IoCall {
    IoCall::child(AdminCheckOperation::new(auth_context.clone(), group_id))
}

/// The scan starts only after a successful admin check.
pub(crate) fn authorized(result: IoResult) -> Result<(), GroupTokensError> {
    match result.child::<bool, AuthorizationError>()? {
        Ok(true) => Ok(()),
        Ok(false) => Err(GroupTokensError::Refused),
        Err(error) => Err(error.into()),
    }
}

impl Operation for ListTokensOperation {
    type Output = TokenPage;
    type Error = GroupTokensError;

    fn first(&mut self) -> Result<Next<TokenPage>, GroupTokensError> {
        Ok(Next::Call(admin_check(
            &self.config.auth_context,
            self.config.group_id,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TokenPage>, GroupTokensError> {
        if !self.scanning {
            authorized(result)?;
            self.scanning = true;
            return Ok(Next::Call(self.scan.start()));
        }
        Ok(match self.scan.step(result)? {
            Some(call) => Next::Call(call),
            None => Next::Done(TokenPage {
                tokens: std::mem::take(&mut self.scan.tokens),
                next: self.scan.next.take(),
            }),
        })
    }
}
