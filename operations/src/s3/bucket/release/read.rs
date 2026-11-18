//! Reads releases for a caller: loads their access once, then checks memberships or lists the
//! files of one release.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use ulid::Ulid;

use super::access::{LoadAccessOperation, MembersCheckOperation};
use super::view::{FilePage, ReleaseView, ReleaseViewOperation, ViewConfig, ViewError};
use super::{Access, BucketScope, MembersOutput, MembershipCaller, MembershipQuery, ReleaseError};
use crate::driver::{DriverContext, drive};

/// Loads the caller's access once, then runs the membership check.
pub async fn release_members(
    context: &DriverContext,
    scope: BucketScope<'_>,
    caller: MembershipCaller<'_>,
    queries: &[MembershipQuery],
) -> Result<MembersOutput, ReleaseError> {
    drive(MembersCheckOperation::new(scope, caller, queries), context).await
}

/// The caller's rules and policies for the bucket, read once for every key they decide.
pub async fn load_access(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: &AuthContext,
) -> Result<Access, ReleaseError> {
    drive(LoadAccessOperation::new(scope, auth), context).await
}

/// Lists one page of a release's files after checking the caller may read its crate.
pub async fn release_view(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: &AuthContext,
    (key, version_id): (&str, Ulid),
    page: FilePage,
) -> Result<ReleaseView, ViewError> {
    let access = load_access(context, scope, auth)
        .await
        .map_err(|error| ViewError::Unavailable(error.to_string()))?;
    if !access.0.allows(Some(key), &Permission::READ) {
        return Err(ViewError::Forbidden);
    }
    let config = ViewConfig {
        realm_id: scope.realm_id,
        node_id: scope.node_id,
        bucket: scope.bucket.to_string(),
        info: scope.info.clone(),
        auth: auth.clone(),
        key: key.to_string(),
        version_id,
        page,
    };
    drive(ReleaseViewOperation::new(config), context).await
}
