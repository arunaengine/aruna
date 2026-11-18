//! Plans or applies a bucket rename only when the caller may write every old and new key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};

use super::rename::{RenameError, RenameInput, RenameKeysOperation, RenamePlan, renamed_key};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::{BucketAccess, BucketAccessOperation};

type CheckedTurn = Result<Next<RenamePlan>, RenameError>;

/// Plans or applies a rename only with WRITE, by role and policy, on every old and new key.
/// A denial answers `Forbidden` before any key is shown or moved.
#[derive(Debug, PartialEq)]
pub struct AuthorizedRenameOperation {
    auth: AuthContext,
    input: Option<RenameInput>,
    from: String,
    to: String,
    access: Option<BucketAccess>,
    bucket: String,
    scope: (
        aruna_core::structs::identity::realm::RealmId,
        aruna_core::types::GroupId,
        aruna_core::NodeId,
    ),
}

impl AuthorizedRenameOperation {
    pub fn new(auth: &AuthContext, input: RenameInput) -> Self {
        Self {
            auth: auth.clone(),
            from: input.from.clone(),
            to: input.to.clone(),
            bucket: input.bucket.clone(),
            scope: (input.realm_id, input.group_id, input.node_id),
            input: Some(input),
            access: None,
        }
    }

    fn permits(&self, from: &str, to: &str) -> bool {
        let allowed = |key: &str| {
            (self.access.as_ref())
                .is_some_and(|access| access.allows(Some(key), &Permission::WRITE))
        };
        allowed(from) && allowed(to)
    }

    fn access_loaded(&mut self, access: BucketAccess) -> CheckedTurn {
        self.access = Some(access);
        let input = self
            .input
            .take()
            .ok_or(RenameError::Unexpected("Checked"))?;
        let denied = (input.keys.iter().flatten()).any(|key| {
            renamed_key(&self.from, &self.to, key).is_some_and(|to| !self.permits(key, &to))
        });
        if denied {
            return Err(RenameError::Forbidden);
        }
        Ok(Next::Call(IoCall::child(RenameKeysOperation::new(input))))
    }
}

/// A denial by role or policy is a verdict; other failures are not.
fn access_error(error: AuthorizeError) -> RenameError {
    match error {
        AuthorizeError::PermissionDenied | AuthorizeError::Policy(_) => RenameError::Forbidden,
        AuthorizeError::Storage(error) => RenameError::Storage(error),
        AuthorizeError::CheckFailed(message) => RenameError::Authorization(message),
    }
}

impl Operation for AuthorizedRenameOperation {
    type Output = RenamePlan;
    type Error = RenameError;

    fn first(&mut self) -> CheckedTurn {
        let load = BucketAccessOperation::new(&self.auth, self.scope, &self.bucket);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> CheckedTurn {
        if self.access.is_none() {
            let access = result.child::<BucketAccess, AuthorizeError>()?;
            return self.access_loaded(access.map_err(access_error)?);
        }
        let plan = result.child::<RenamePlan, RenameError>()??;
        match plan
            .items
            .iter()
            .all(|item| self.permits(&item.from, &item.to))
        {
            true => Ok(Next::Done(plan)),
            false => Err(RenameError::Forbidden),
        }
    }
}
