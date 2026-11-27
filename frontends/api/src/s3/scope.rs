//! Resolves the key prefixes a caller may read when a role reaches only part of a bucket.
//! Listing handlers narrow their page to those prefixes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
// Every concrete object path still goes through an ordinary permission check.

use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_ops_identity::auth::request_authorization::rules_operation;
use aruna_runner::driver::DriverContext;
use aruna_runner::driver::drive;
use s3s::{S3Result, s3_error};

pub(super) use aruna_ops_identity::auth::scope::SubpathScope;

/// Resolves the subtrees at or below `root` the caller reaches through its
/// realm and group roles, narrowed by the credential's own restrictions. A
/// missing realm or group document grants nothing instead of failing.
pub(super) async fn resolve_scope(
    context: &DriverContext,
    realm_id: RealmId,
    user_access: &UserAccess,
    root: &str,
) -> S3Result<SubpathScope> {
    let auth_context = AuthContext {
        user_id: user_access.user_identity,
        realm_id,
        path_restrictions: user_access.path_restrictions.clone(),
        session: None,
    };
    let rules = drive(rules_operation(&auth_context, root), context).await;
    let restrictions = user_access.path_restrictions.as_deref();
    SubpathScope::resolve(rules, restrictions, root)
        .map_err(|error| s3_error!(InternalError, "{}", error.to_string()))
}
