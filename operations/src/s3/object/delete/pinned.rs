//! Refuses the permanent deletion of object versions a release of the bucket pins; a delete
//! marker stays allowed, since the pinned version remains readable by its id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Every release counts, also one the caller cannot read, so a refusal names none.

use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::storage_purge::StoragePurgeScope;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::s3::bucket::release::{
    BucketScope, MembersConfig, MembersOutput, MembershipQuery, ReleaseMembersOperation,
    load_access,
};
use crate::s3::bucket::release_view::{FilePage, ReleaseViewOperation, ViewConfig, ViewError};

#[derive(Clone, Debug, Error, PartialEq)]
pub enum PinnedError {
    #[error(
        "version {version_id} of `{key}` is pinned by a release; delete the key for a delete \
         marker instead, or delete the release crate version first"
    )]
    Version { key: String, version_id: Ulid },
    #[error(
        "versions in this scope are pinned by a release; delete the release crate versions \
         first, or delete the keys for delete markers instead"
    )]
    Scope,
    #[error("the releases of the bucket could not all be checked, so nothing was deleted: {0}")]
    Unchecked(String),
}

/// Whether a release pins each queried version, in query order. An answer that may miss a
/// release refuses, since the deletion could break it.
pub fn pinned(
    queries: &[MembershipQuery],
    output: &MembersOutput,
) -> Result<Vec<bool>, PinnedError> {
    if !output.complete || output.answers.len() != queries.len() {
        return Err(PinnedError::Unchecked(
            "a release crate could not be read".to_string(),
        ));
    }
    Ok((output.answers.iter())
        .map(|answer| !answer.releases.is_empty())
        .collect())
}

/// Every release of the bucket that pins `queries`, whatever the caller may read. A
/// distributed bucket deletes no single versions, so only this node's releases count.
async fn retained(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: &AuthContext,
    queries: &[MembershipQuery],
) -> Result<MembersOutput, PinnedError> {
    let unchecked = |error: &dyn std::fmt::Display| PinnedError::Unchecked(error.to_string());
    let access = load_access(context, scope, auth)
        .await
        .map_err(|error| unchecked(&error))?;
    let config = MembersConfig {
        realm_id: scope.realm_id,
        node_id: scope.node_id,
        bucket: scope.bucket.to_string(),
        info: scope.info.clone(),
        auth: auth.clone(),
        access,
        bearer: None,
        queries: queries.to_vec(),
        ranges: None,
        retention: true,
    };
    drive(ReleaseMembersOperation::new(config), context)
        .await
        .map_err(|error| unchecked(&error))
}

/// Whether a release pins each of `queries`.
pub async fn version_pins(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: &AuthContext,
    queries: &[MembershipQuery],
) -> Result<Vec<bool>, PinnedError> {
    if queries.is_empty() || scope.info.distribution.is_some() {
        return Ok(vec![false; queries.len()]);
    }
    let output = retained(context, scope, auth, queries).await?;
    pinned(queries, &output)
}

/// Refuses the permanent deletion of one version a release pins.
pub async fn refuse_pinned(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: &AuthContext,
    (key, version_id): (&str, Ulid),
) -> Result<(), PinnedError> {
    let query = MembershipQuery {
        key: key.to_string(),
        version_id,
    };
    let pins = version_pins(context, scope, auth, std::slice::from_ref(&query)).await?;
    match pins.first() {
        Some(true) => Err(PinnedError::Version {
            key: key.to_string(),
            version_id,
        }),
        _ => Ok(()),
    }
}

/// Whether a release of `prefix` may pin a key the purge `scope` deletes, and the key prefix
/// to look for in it.
fn reach<'a>(scope: &'a StoragePurgeScope, prefix: &str) -> Option<&'a str> {
    let wanted = scope.list_prefix()?;
    (wanted.starts_with(prefix) || prefix.starts_with(wanted)).then_some(wanted)
}

/// Refuses a file or prefix purge while a release pins a version under it. A bucket purge
/// deletes the releases with their files, so it is never refused.
pub async fn refuse_scope(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: &AuthContext,
    purge: &StoragePurgeScope,
) -> Result<(), PinnedError> {
    if purge.is_bucket() || scope.info.distribution.is_some() {
        return Ok(());
    }
    let output = retained(context, scope, auth, &[]).await?;
    if !output.complete {
        return Err(PinnedError::Unchecked(
            "a release crate could not be read".to_string(),
        ));
    }
    let unrestricted = AuthContext {
        path_restrictions: None,
        ..auth.clone()
    };
    for release in &output.releases {
        let Some(wanted) = reach(purge, &release.prefix) else {
            continue;
        };
        let config = ViewConfig {
            realm_id: scope.realm_id,
            node_id: scope.node_id,
            bucket: scope.bucket.to_string(),
            info: scope.info.clone(),
            auth: unrestricted.clone(),
            key: release.key.clone(),
            version_id: release.version_id,
            page: FilePage {
                prefix: wanted.to_string(),
                limit: 1,
                ..FilePage::default()
            },
        };
        let pins = match drive(ReleaseViewOperation::new(config), context).await {
            Ok(view) => {
                (view.files.first()).is_some_and(|file| purge.matches_key(scope.bucket, &file.key))
            }
            Err(ViewError::Missing | ViewError::NoRelease) => false,
            Err(error) => return Err(PinnedError::Unchecked(error.to_string())),
        };
        if pins {
            return Err(PinnedError::Scope);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "pinned_tests.rs"]
mod tests;
