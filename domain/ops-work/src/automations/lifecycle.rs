//! Authorized lifecycle transitions for an automation, synchronizing its service account.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::record::Automation;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_ops_identity::groups::admin_check::AdminCheckOperation;
use ulid::Ulid;

use super::change::{AutoChange, WriteAutoOperation};
use super::identity::SyncIdentityOperation;
use super::{AutoAccessOperation, AutoError, Need};

#[derive(Debug, PartialEq)]
enum Step {
    Access,
    Admin,
    Write,
    Sync,
}

/// Checks caller rights (including human group admin for approval/rejection), writes the
/// automation change atomically, and syncs its service account and role when needed.
#[derive(Debug, PartialEq)]
pub struct ChangeAutoOperation {
    auth: AuthContext,
    id: Ulid,
    expected: Option<u64>,
    change: AutoChange,
    local: NodeId,
    step: Step,
}

impl ChangeAutoOperation {
    pub fn new(
        auth: &AuthContext,
        (id, expected): (Ulid, Option<u64>),
        change: AutoChange,
        local: NodeId,
    ) -> Self {
        Self {
            auth: auth.clone(),
            id,
            expected,
            change,
            local,
            step: Step::Access,
        }
    }

    fn write_call(&mut self) -> Next<Automation> {
        self.step = Step::Write;
        let write = WriteAutoOperation::new(
            (self.id, self.expected),
            self.change.clone(),
            self.auth.user_id,
            self.auth.realm_id,
        );
        Next::Call(IoCall::retry(write, &self.id.to_bytes()))
    }
}

impl Operation for ChangeAutoOperation {
    type Output = Automation;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Automation>, AutoError> {
        let access = AutoAccessOperation::new(&self.auth, self.id, self.local, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Automation>, AutoError> {
        match self.step {
            Step::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                if self.change.needs_admin() {
                    self.step = Step::Admin;
                    let check = AdminCheckOperation::new(self.auth.clone(), auto.group_id);
                    return Ok(Next::Call(IoCall::child(check)));
                }
                Ok(self.write_call())
            }
            Step::Admin => {
                let is_admin = result
                    .child::<bool, AuthorizationError>()?
                    .map_err(|err| AutoError::Storage(err.to_string()))?;
                if !is_admin {
                    return Err(AutoError::Forbidden);
                }
                Ok(self.write_call())
            }
            Step::Write => {
                let auto = result.child::<Automation, AutoError>()??;
                if self.change.moves_identity() && (auto.wants_account() || auto.account.is_some())
                {
                    self.step = Step::Sync;
                    let sync = SyncIdentityOperation::new(&auto, (self.local, self.auth.realm_id));
                    return Ok(Next::Call(IoCall::child(sync)));
                }
                Ok(Next::Done(auto))
            }
            Step::Sync => Ok(Next::Done(result.child::<Automation, AutoError>()??)),
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
