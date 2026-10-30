//! Refuses the permanent deletion of object versions a release of the bucket pins; a delete
//! marker stays allowed, since the pinned version remains readable by its id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::storage_purge::StoragePurgeScope;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::s3::bucket::release::{
    BucketScope, MembersOutput, MembershipCaller, MembershipQuery, release_members,
};
use crate::s3::bucket::release_view::{FilePage, ViewError, release_view};

#[derive(Clone, Debug, Error, PartialEq)]
pub enum PinnedError {
    #[error(
        "version {version_id} of `{key}` is pinned by release {}; delete the key for a delete \
         marker instead, or delete the release crate version first",
        names(.releases)
    )]
    Version {
        key: String,
        version_id: Ulid,
        releases: Vec<String>,
    },
    #[error(
        "versions in this scope are pinned by release {}; delete the release crate versions \
         first, or delete the keys for delete markers instead",
        names(.0)
    )]
    Scope(Vec<String>),
    #[error("the releases of the bucket could not all be checked, so nothing was deleted: {0}")]
    Unchecked(String),
}

fn names(releases: &[String]) -> String {
    let quoted: Vec<String> = releases.iter().map(|label| format!("`{label}`")).collect();
    quoted.join(", ")
}

/// The labels of the releases that pin each queried version, in query order. An answer that
/// may miss a release refuses, since the deletion could break it.
pub fn pinned_labels(
    queries: &[MembershipQuery],
    output: &MembersOutput,
) -> Result<Vec<Vec<String>>, PinnedError> {
    if !output.complete || output.answers.len() != queries.len() {
        return Err(PinnedError::Unchecked(
            "a release crate could not be read".to_string(),
        ));
    }
    Ok((output.answers.iter())
        .map(|answer| {
            let labels = answer.releases.iter().map(|release| release.label.clone());
            labels.collect()
        })
        .collect())
}

/// The labels of the releases that pin each of `queries`, read with the caller's access.
pub async fn version_pins(
    context: &DriverContext,
    scope: BucketScope<'_>,
    caller: MembershipCaller<'_>,
    queries: &[MembershipQuery],
) -> Result<Vec<Vec<String>>, PinnedError> {
    if queries.is_empty() {
        return Ok(Vec::new());
    }
    let output = release_members(context, scope, caller, queries)
        .await
        .map_err(|error| PinnedError::Unchecked(error.to_string()))?;
    pinned_labels(queries, &output)
}

/// Refuses the permanent deletion of one version a release pins.
pub async fn refuse_pinned(
    context: &DriverContext,
    scope: BucketScope<'_>,
    caller: MembershipCaller<'_>,
    (key, version_id): (&str, Ulid),
) -> Result<(), PinnedError> {
    let query = MembershipQuery {
        key: key.to_string(),
        version_id,
    };
    let pins = version_pins(context, scope, caller, std::slice::from_ref(&query)).await?;
    match pins.into_iter().next() {
        Some(releases) if !releases.is_empty() => Err(PinnedError::Version {
            key: key.to_string(),
            version_id,
            releases,
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
    caller: MembershipCaller<'_>,
    purge: &StoragePurgeScope,
) -> Result<(), PinnedError> {
    if purge.is_bucket() {
        return Ok(());
    }
    let output = release_members(context, scope, caller, &[])
        .await
        .map_err(|error| PinnedError::Unchecked(error.to_string()))?;
    if !output.complete {
        return Err(PinnedError::Unchecked(
            "a release crate could not be read".to_string(),
        ));
    }
    let mut pinning = Vec::new();
    for release in &output.releases {
        let Some(wanted) = reach(purge, &release.prefix) else {
            continue;
        };
        let page = FilePage {
            prefix: wanted.to_string(),
            limit: 1,
            ..FilePage::default()
        };
        let found = release_view(
            context,
            scope,
            caller.auth,
            (&release.key, release.version_id),
            page,
        )
        .await;
        let pins = match found {
            Ok(view) => {
                (view.files.first()).is_some_and(|file| purge.matches_key(scope.bucket, &file.key))
            }
            Err(ViewError::Missing | ViewError::NoRelease) => false,
            Err(error) => return Err(PinnedError::Unchecked(error.to_string())),
        };
        if pins && !pinning.contains(&release.release.label) {
            pinning.push(release.release.label.clone());
        }
    }
    match pinning.is_empty() {
        true => Ok(()),
        false => Err(PinnedError::Scope(pinning)),
    }
}

#[cfg(test)]
#[path = "pinned_tests.rs"]
mod tests;
