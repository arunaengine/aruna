//! Edits draft branches and merges branches, as new versions published to every holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::changes::property_conflicts;
use super::project::Projection;
use super::push::{record, unlocked};
use super::snapshot::{current, execute, linked};
use super::versions::{
    Version, WriteOptions, branch_ref, diff, expect, log, now_ms, open, parse_conflict,
    peeled_tags, protected, resolve, rocrate, version,
};
use super::{GitError, MergeConflict};
use crate::driver::DriverContext;
use crate::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentMutation, UpdateDocumentOperation, update_metadata_document,
};
use aruna_blob::git::GitStore;
use aruna_core::git::{GitEffect, GitEvent, GitSnapshot, MergeOutcome, RefUpdate, ZERO_OID};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
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
        message: format!("{}\n\nAruna-User: {}\n", summary.trim(), auth.user_id),
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
        record(context, auth, &document, vec![update], bytes, lfs).await?;
    }
    let info = log(store, auth, id, (&new, None), 0, 1)
        .await?
        .pop()
        .ok_or(GitError::Unavailable)?;
    let tags = peeled_tags(store, auth, id, &projection).await?;
    Ok(version(info, &projection.state.refs, &tags))
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
        message: format!("{}\n\nAruna-User: {}\n", summary.trim(), auth.user_id),
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
    if new != target {
        let paths: Vec<String> = diff(store, auth, id, Some(&target), &new)
            .await?
            .into_iter()
            .map(|change| change.path)
            .collect();
        unlocked(&projection.state, auth, &paths)?;
        if into == "main" {
            update_metadata(context, store, auth, document, &target, &new).await?;
        }
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
        record(context, auth, document, updates, bytes, Vec::new()).await?;
    }
    Ok(Merged {
        version: new,
        fast_forward,
    })
}

/// Applies the metadata main gains from `old` to `new` to the live document, as a push does.
async fn update_metadata(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    document: &MetadataRegistryRecord,
    old: &str,
    new: &str,
) -> Result<(), GitError> {
    let (_, graph) = current(context, document, None, false)
        .await?
        .ok_or(GitError::Unavailable)?;
    let effect = GitEffect::MergeMetadata {
        document_id: document.document_id,
        old: Some(old.to_string()),
        new: new.to_string(),
        graph,
    };
    let jsonld = match execute(store, effect, auth.user_id).await? {
        GitEvent::MetadataMerged(Ok(Some(jsonld))) => jsonld,
        GitEvent::MetadataMerged(Ok(None)) => return Ok(()),
        GitEvent::MetadataMerged(Err(error)) => return Err(GitError::Refused(error)),
        _ => return Err(GitError::Unavailable),
    };
    let node_id = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
    let operation = UpdateDocumentOperation::new(UpdateDocumentConfig {
        actor: Actor {
            node_id,
            user_id: auth.user_id,
            realm_id: document.realm_id,
        },
        group_id: document.group_id,
        document_id: document.document_id,
        public: document.public,
        mutation: UpdateDocumentMutation::ReplaceRoCrate { jsonld },
        expected_revision: None,
    });
    update_metadata_document(operation, context)
        .await
        .map(|_| ())
        .map_err(|error| GitError::Refused(error.to_string()))
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
