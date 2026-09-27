//! Edits draft branches and merges branches, as new versions published to every holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::changes::property_conflicts;
use super::project::Projection;
use super::push::{record, unlocked};
use super::snapshot::{execute, linked};
use super::versions::{
    Version, WriteOptions, branch_ref, diff, expect, log, now_ms, open, parse_conflict,
    peeled_tags, plain, protected, resolve, rocrate, version,
};
use super::{GitError, MergeConflict};
use crate::driver::DriverContext;
use aruna_blob::git::GitStore;
use aruna_core::git::{
    GitEffect, GitEvent, GitSnapshot, MergeOutcome, PendingMerge, RefUpdate, ZERO_OID,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use ulid::Ulid;

/// Packs the objects of `commit` that no current ref reaches.
async fn pack(
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    projection: &Projection,
    commit: &str,
) -> Result<Bytes, GitError> {
    let effect = GitEffect::Pack {
        document_id: id,
        include: vec![commit.to_string()],
        exclude: projection.state.refs.values().cloned().collect(),
    };
    match execute(store, effect, auth.user_id).await? {
        GitEvent::Packed(pack) => Ok(pack),
        _ => Err(GitError::Unavailable),
    }
}

/// Replaces a draft branch's metadata with `jsonld` as a new version on that branch.
pub async fn edit(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    branch: &str,
    jsonld: String,
    options: WriteOptions<'_>,
) -> Result<Version, GitError> {
    let WriteOptions { message, expected } = options;
    if protected(branch) {
        return Err(GitError::Refused(format!(
            "{branch} is not a draft; edit main through the metadata document"
        )));
    }
    let name = branch_ref(branch)?;
    let (document, projection, _guard) = open(context, store, auth, id, Permission::WRITE).await?;
    let head = projection
        .state
        .refs
        .get(&name)
        .cloned()
        .ok_or(GitError::NotFound)?;
    expect(Some(&head), expected)?;
    let objects = linked(context, &document, &jsonld, auth.user_id).await;
    let lfs = objects.iter().map(|linked| linked.object.clone()).collect();
    let summary = message.unwrap_or_else(|| "Edit metadata".to_string());
    let effect = GitEffect::Edit {
        head: head.clone(),
        snapshot: GitSnapshot {
            document_id: id,
            event_id: Ulid::nil(),
            occurred_at_ms: now_ms(),
            jsonld,
            objects,
        },
        message: format!("{}\n\nAruna-User: {}\n", plain(&summary), auth.user_id),
    };
    let new = match execute(store, effect, auth.user_id).await? {
        GitEvent::Edited(Ok(new)) => new,
        GitEvent::Edited(Err(error)) => return Err(GitError::Refused(error)),
        _ => return Err(GitError::Unavailable),
    };
    if new != head {
        let paths: Vec<String> = diff(store, auth, id, Some(&head), &new)
            .await?
            .into_iter()
            .map(|change| change.path)
            .collect();
        unlocked(&projection.state, auth, &paths)?;
        let bytes = pack(store, auth, id, &projection, &new).await?;
        let update = RefUpdate {
            name,
            old: head,
            new: new.clone(),
        };
        let made = vec![new.clone()];
        record(
            context,
            auth,
            &document,
            vec![update],
            (bytes, lfs),
            (made, None),
        )
        .await?;
    }
    let info = log(store, auth, id, (&new, None), 0, 1)
        .await?
        .pop()
        .ok_or(GitError::Unavailable)?;
    let tags = peeled_tags(store, auth, id, &projection).await?;
    // The answer shows the state after the edit, which the projection does not know yet.
    let mut after = projection.state.clone();
    after.refs.insert(branch_ref(branch)?, new.clone());
    after.made.insert(new);
    Ok(version(info, &after, &tags))
}

/// The version a merge produced and whether the target simply moved forward.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Merged {
    pub version: String,
    pub fast_forward: bool,
}

/// Merges a branch, tag or version into a branch. Metadata properties both sides changed
/// differently refuse the merge. A merge into main also updates the metadata document.
pub async fn merge(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    source: &str,
    into: &str,
    options: WriteOptions<'_>,
) -> Result<Merged, GitError> {
    let (document, projection, _guard) = open(context, store, auth, id, Permission::WRITE).await?;
    let source = resolve(store, auth, id, source).await?;
    merge_into(
        (context, store, auth, &document, &projection),
        &source,
        into,
        options,
        Vec::new(),
    )
    .await
}

type Scope<'a> = (
    &'a DriverContext,
    &'a GitStore,
    &'a AuthContext,
    &'a MetadataRegistryRecord,
    &'a Projection,
);

async fn merge_into(
    scope: Scope<'_>,
    source: &str,
    into: &str,
    options: WriteOptions<'_>,
    mut updates: Vec<RefUpdate>,
) -> Result<Merged, GitError> {
    let (context, store, auth, document, projection) = scope;
    let WriteOptions { message, expected } = options;
    let id = document.document_id;
    if into == "aruna" {
        return Err(GitError::Refused(
            "aruna only records graph snapshots".into(),
        ));
    }
    let name = branch_ref(into)?;
    let target = projection
        .state
        .refs
        .get(&name)
        .cloned()
        .ok_or(GitError::NotFound)?;
    expect(Some(&target), expected)?;
    let base = GitEffect::MergeBase {
        document_id: id,
        first: target.clone(),
        second: source.to_string(),
    };
    if let GitEvent::Resolved(Some(base)) = execute(store, base, auth.user_id).await?
        && let (Some(base), Some(ours), Some(theirs)) = (
            rocrate(store, auth, id, &base).await,
            rocrate(store, auth, id, &target).await,
            rocrate(store, auth, id, source).await,
        )
    {
        let properties = property_conflicts(&base, &theirs, &ours);
        if !properties.is_empty() {
            return Err(GitError::MergeConflict(Box::new(MergeConflict {
                files: Vec::new(),
                properties,
            })));
        }
    }
    let summary = message.unwrap_or_else(|| format!("Merge into {into}"));
    let effect = GitEffect::Merge {
        document_id: id,
        target: target.clone(),
        source: source.to_string(),
        message: format!("{}\n\nAruna-User: {}\n", plain(&summary), auth.user_id),
    };
    let GitEvent::Merged(outcome) = execute(store, effect, auth.user_id).await? else {
        return Err(GitError::Unavailable);
    };
    let (new, fast_forward) = match outcome {
        MergeOutcome::UpToDate => (target.clone(), false),
        MergeOutcome::FastForward => (source.to_string(), true),
        MergeOutcome::Merged(commit) => (commit, false),
        MergeOutcome::Conflicts(files) => {
            return Err(GitError::MergeConflict(Box::new(MergeConflict {
                files,
                properties: Vec::new(),
            })));
        }
        MergeOutcome::Failed(error) => return Err(GitError::Refused(error)),
    };
    let mut bytes = Bytes::new();
    let made = if fast_forward || new == target {
        Vec::new()
    } else {
        vec![new.clone()]
    };
    // Merged metadata follows the recorded merge, so a failed record changes nothing.
    let merge = (into == "main" && new != target).then(|| PendingMerge {
        user_id: auth.user_id,
        old: target.clone(),
        new: new.clone(),
    });
    let follows = merge.is_some();
    if new != target {
        let paths: Vec<String> = diff(store, auth, id, Some(&target), &new)
            .await?
            .into_iter()
            .map(|change| change.path)
            .collect();
        unlocked(&projection.state, auth, &paths)?;
        bytes = pack(store, auth, id, projection, &new).await?;
        updates.insert(
            0,
            RefUpdate {
                name,
                old: target,
                new: new.clone(),
            },
        );
    }
    if !updates.is_empty() {
        record(
            context,
            auth,
            document,
            updates,
            (bytes, Vec::new()),
            (made, merge),
        )
        .await?;
    }
    if follows && let Err(error) = super::pending::apply(context, store, document).await {
        tracing::warn!(document_id = %id, %error, "Merged metadata applies on the next refresh");
    }
    Ok(Merged {
        version: new,
        fast_forward,
    })
}

/// Merges a kept conflict into its branch and removes it in the same record.
pub async fn resolve_conflict(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    (id, conflict): (Ulid, Ulid),
    expected: Option<&str>,
) -> Result<Merged, GitError> {
    let (document, projection, _guard) = open(context, store, auth, id, Permission::WRITE).await?;
    let (name, kept) = projection
        .state
        .refs
        .iter()
        .find_map(|(name, target)| {
            parse_conflict(name, target)
                .filter(|parsed| parsed.id == conflict)
                .map(|parsed| (name.clone(), parsed))
        })
        .ok_or(GitError::NotFound)?;
    let Some(branch) = kept.branch else {
        return Err(GitError::Refused(
            "a kept tag can only be discarded; tags never move".into(),
        ));
    };
    let discard = RefUpdate {
        name,
        old: kept.version.clone(),
        new: ZERO_OID.into(),
    };
    let options = WriteOptions {
        message: Some(format!("Merge kept conflict {conflict} into {branch}")),
        expected,
    };
    merge_into(
        (context, store, auth, &document, &projection),
        &kept.version,
        &branch,
        options,
        vec![discard],
    )
    .await
}
