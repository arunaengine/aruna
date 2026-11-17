//! Plans or applies a bucket rename only when the caller may write every old and new key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::auth::{AuthContext, Permission};

use super::rename::{RenameError, RenameInput, RenameKeysOperation, RenamePlan, renamed_key};
use crate::auth::request_authorization::{AuthorizeError, BucketAccess};
use crate::driver::{DriverContext, drive};

/// Plans or applies a rename only with WRITE, by role and policy, on every old and new key.
/// A denial answers `Forbidden` before any key is shown or moved.
pub async fn authorized_rename(
    context: &DriverContext,
    auth: &AuthContext,
    input: RenameInput,
) -> Result<RenamePlan, RenameError> {
    let scope = (input.realm_id, input.group_id, input.node_id);
    let access = BucketAccess::load(context, auth, scope, &input.bucket)
        .await
        .map_err(access_error)?;
    let allowed = |key: &str| access.allows(Some(key), &Permission::WRITE);
    let permits = |from: &str, to: &str| allowed(from) && allowed(to);
    let keys = input.keys.clone().unwrap_or_default();
    let moved = |key: &String| renamed_key(&input.from, &input.to, key);
    if !keys
        .iter()
        .all(|key| moved(key).is_none_or(|to| permits(key, &to)))
    {
        return Err(RenameError::Forbidden);
    }
    let plan = drive(RenameKeysOperation::new(input), context).await?;
    match plan.items.iter().all(|item| permits(&item.from, &item.to)) {
        true => Ok(plan),
        false => Err(RenameError::Forbidden),
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
