//! Reads a document's ARC history as versions, branches, tags and conflicts, and moves refs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::GitError;
use super::changes::{EntityChange, entity_changes};
use super::project::{Projection, lock};
use super::push::record;
use super::snapshot::{execute, refresh};
use crate::driver::DriverContext;
use aruna_blob::git::GitStore;
use aruna_core::git::{
    CommitInfo, FileChange, GitEffect, GitEvent, RefUpdate, ZERO_OID, valid_ref,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use serde_json::Value;
use std::collections::BTreeMap;
use tokio::sync::OwnedMutexGuard;
use ulid::Ulid;

/// Branches that only the server moves or that hold the live metadata.
const PROTECTED: [&str; 2] = ["main", "aruna"];
const MAX_VERSIONS: usize = 200;
/// Only commits this node made and signed carry trustworthy Aruna trailers.
const SERVICE_EMAIL: &str = "git@aruna.local";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    pub commit: CommitInfo,
    /// The Aruna user who made a server-side edit or merge.
    pub user_id: Option<String>,
    /// The metadata event a server snapshot captures.
    pub metadata_event_id: Option<Ulid>,
    pub branches: Vec<String>,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Comparison {
    /// `None` compares against an empty ARC.
    pub from: Option<String>,
    pub to: String,
    /// `None` when either side has no readable ISA metadata.
    pub entities: Option<Vec<EntityChange>>,
    pub files: Vec<FileChange>,
}

/// A kept branch or tag update; exactly one of `branch` and `tag` is set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    pub id: Ulid,
    pub branch: Option<String>,
    pub tag: Option<String>,
    pub version: String,
}

/// Which versions to list: those of `branch` not reachable from `since`, one page at a time.
#[derive(Clone, Debug, Default)]
pub struct VersionQuery<'a> {
    pub branch: &'a str,
    pub since: Option<&'a str>,
    pub cursor: Option<&'a str>,
    pub limit: usize,
}

/// The commit message of a write and the branch head the caller expects, if any.
#[derive(Clone, Debug, Default)]
pub struct WriteOptions<'a> {
    pub message: Option<String>,
    pub expected: Option<&'a str>,
}

/// A named ref and the commit it finally names; tags are peeled to their commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Named {
    pub name: String,
    pub version: String,
}

pub(super) type Guard = OwnedMutexGuard<()>;

pub(super) async fn open(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    permission: Permission,
) -> Result<(MetadataRegistryRecord, Projection, Guard), GitError> {
    let (document, _) = super::repository(context, auth, id, permission).await?;
    let guard = lock(id).await;
    let projection = refresh(context, store, &document).await?;
    Ok((document, projection, guard))
}

pub(super) async fn resolve(
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    revision: &str,
) -> Result<String, GitError> {
    let effect = GitEffect::Resolve {
        document_id: id,
        revision: revision.to_string(),
    };
    match execute(store, effect, auth.user_id).await? {
        GitEvent::Resolved(Some(commit)) => Ok(commit),
        GitEvent::Resolved(None) => Err(GitError::NotFound),
        _ => Err(GitError::Unavailable),
    }
}

pub(super) async fn log(
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    (revision, exclude): (&str, Option<&str>),
    skip: usize,
    limit: usize,
) -> Result<Vec<CommitInfo>, GitError> {
    let effect = GitEffect::Log {
        document_id: id,
        revision: revision.to_string(),
        exclude: exclude.map(str::to_owned),
        skip,
        limit,
    };
    match execute(store, effect, auth.user_id).await? {
        GitEvent::Log(commits) => Ok(commits),
        _ => Err(GitError::Unavailable),
    }
}

pub(super) async fn diff(
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    from: Option<&str>,
    to: &str,
) -> Result<Vec<FileChange>, GitError> {
    let effect = GitEffect::Diff {
        document_id: id,
        from: from.map(str::to_owned),
        to: to.to_string(),
    };
    match execute(store, effect, auth.user_id).await? {
        GitEvent::Diff(changes) => Ok(changes),
        _ => Err(GitError::Unavailable),
    }
}

/// The ISA-derived RO-Crate of a commit, or `None` when it has no valid ISA metadata.
pub(super) async fn rocrate(
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    commit: &str,
) -> Option<Value> {
    let effect = GitEffect::Export {
        document_id: id,
        revision: commit.to_string(),
    };
    let GitEvent::Exported(bytes) = execute(store, effect, auth.user_id).await.ok()? else {
        return None;
    };
    let mut value: Value = serde_json::from_slice(&bytes).ok()?;
    value.get_mut("rocrate").map(Value::take)
}

pub(super) fn branch_ref(name: &str) -> Result<String, GitError> {
    let full = format!("refs/heads/{name}");
    valid_ref(&full, true)
        .then_some(full)
        .ok_or(GitError::Invalid)
}

pub(super) fn expect(current: Option<&String>, expected: Option<&str>) -> Result<(), GitError> {
    match expected {
        Some(expected) if current.map(String::as_str) != Some(expected) => Err(GitError::Stale),
        _ => Ok(()),
    }
}

pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

fn trailer(message: &str, key: &str) -> Option<String> {
    message
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix(key))
        .map(|value| value.trim().to_string())
}

/// Commits named by each tag, peeled so annotated tags compare with commits.
pub(super) async fn peeled_tags(
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    projection: &Projection,
) -> Result<Vec<Named>, GitError> {
    let mut tags = Vec::new();
    for name in projection.state.refs.keys() {
        if let Some(short) = name.strip_prefix("refs/tags/") {
            let version = resolve(store, auth, id, name).await?;
            tags.push(Named {
                name: short.to_string(),
                version,
            });
        }
    }
    Ok(tags)
}

pub(super) fn version(
    commit: CommitInfo,
    refs: &BTreeMap<String, String>,
    tags: &[Named],
) -> Version {
    let branches = refs
        .iter()
        .filter(|(_, target)| **target == commit.commit)
        .filter_map(|(name, _)| name.strip_prefix("refs/heads/"))
        .map(str::to_owned)
        .collect();
    let tags = tags
        .iter()
        .filter(|tag| tag.version == commit.commit)
        .map(|tag| tag.name.clone())
        .collect();
    // A client can write any trailer; only this node's signed commits are believed.
    let trusted = commit.signed && commit.committer_email == SERVICE_EMAIL;
    let trailer = |key| trusted.then(|| trailer(&commit.message, key)).flatten();
    Version {
        user_id: trailer("Aruna-User:"),
        metadata_event_id: trailer("Aruna-Revision:").and_then(|value| value.parse().ok()),
        commit,
        branches,
        tags,
    }
}

/// Versions of a branch, newest first. The cursor pins the head the first page started
/// from, so later pages stay stable while the branch moves.
pub async fn list(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    query: VersionQuery<'_>,
) -> Result<(Vec<Version>, Option<String>), GitError> {
    let (_, projection, _guard) = open(context, store, auth, id, Permission::READ).await?;
    let (head, skip) = match query.cursor {
        Some(cursor) => {
            let (head, skip) = cursor.split_once('.').ok_or(GitError::Invalid)?;
            let skip = skip.parse::<usize>().map_err(|_| GitError::Invalid)?;
            let head = resolve(store, auth, id, head)
                .await
                .map_err(|_| GitError::Invalid)?;
            (head, skip)
        }
        None => {
            let name = branch_ref(query.branch)?;
            let head = projection
                .state
                .refs
                .get(&name)
                .ok_or(GitError::BranchMissing)?;
            (head.clone(), 0)
        }
    };
    let since = match query.since {
        Some(since) => Some(resolve(store, auth, id, since).await?),
        None => None,
    };
    let limit = query.limit.clamp(1, MAX_VERSIONS);
    let range = (head.as_str(), since.as_deref());
    let mut commits = log(store, auth, id, range, skip, limit + 1).await?;
    let next = (commits.len() > limit).then(|| format!("{head}.{}", skip + limit));
    commits.truncate(limit);
    let tags = peeled_tags(store, auth, id, &projection).await?;
    let versions = commits
        .into_iter()
        .map(|commit| version(commit, &projection.state.refs, &tags))
        .collect();
    Ok((versions, next))
}

/// One version and the files it changed against its first parent.
pub async fn show(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    revision: &str,
) -> Result<(Version, Vec<FileChange>), GitError> {
    let (_, projection, _guard) = open(context, store, auth, id, Permission::READ).await?;
    let commit = resolve(store, auth, id, revision).await?;
    let info = log(store, auth, id, (&commit, None), 0, 1)
        .await?
        .pop()
        .ok_or(GitError::NotFound)?;
    let files = diff(
        store,
        auth,
        id,
        info.parents.first().map(String::as_str),
        &commit,
    )
    .await?;
    let tags = peeled_tags(store, auth, id, &projection).await?;
    Ok((version(info, &projection.state.refs, &tags), files))
}

pub async fn compare(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    from: Option<&str>,
    to: &str,
) -> Result<Comparison, GitError> {
    let (_, _, _guard) = open(context, store, auth, id, Permission::READ).await?;
    let from = match from {
        Some(from) => Some(resolve(store, auth, id, from).await?),
        None => None,
    };
    let to = resolve(store, auth, id, to).await?;
    let files = diff(store, auth, id, from.as_deref(), &to).await?;
    let before = match &from {
        Some(from) => rocrate(store, auth, id, from).await,
        None => Some(serde_json::json!({ "@graph": [] })),
    };
    let entities = match (before, rocrate(store, auth, id, &to).await) {
        (Some(before), Some(after)) => Some(entity_changes(&before, &after)),
        _ => None,
    };
    Ok(Comparison {
        from,
        to,
        entities,
        files,
    })
}

/// Every branch with its head version.
pub async fn branches(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
) -> Result<Vec<Version>, GitError> {
    let (_, projection, _guard) = open(context, store, auth, id, Permission::READ).await?;
    let tags = peeled_tags(store, auth, id, &projection).await?;
    let mut heads = Vec::new();
    for (name, target) in &projection.state.refs {
        if name.starts_with("refs/heads/") {
            let info = log(store, auth, id, (target, None), 0, 1)
                .await?
                .pop()
                .ok_or(GitError::Unavailable)?;
            heads.push(version(info, &projection.state.refs, &tags));
        }
    }
    Ok(heads)
}

/// Every tag with the commit it names.
pub async fn tags(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
) -> Result<Vec<Named>, GitError> {
    let (_, projection, _guard) = open(context, store, auth, id, Permission::READ).await?;
    peeled_tags(store, auth, id, &projection).await
}

pub fn protected(branch: &str) -> bool {
    PROTECTED.contains(&branch)
}

/// Creates (`target` set) or deletes a branch or tag. Creating refuses an existing name.
pub async fn change_ref(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    name: String,
    target: Option<&str>,
    expected: Option<&str>,
) -> Result<Named, GitError> {
    let discard = name.starts_with("refs/conflicts/") && target.is_none();
    if !(valid_ref(&name, false) || discard) {
        return Err(GitError::Invalid);
    }
    let short = name.strip_prefix("refs/heads/").unwrap_or_default();
    if target.is_none() && protected(short) {
        return Err(GitError::Refused(format!("{short} cannot be deleted")));
    }
    let (document, projection, _guard) = open(context, store, auth, id, Permission::WRITE).await?;
    let current = projection.state.refs.get(&name);
    // A tag may be named by the commit it points to as well as by its own object.
    let peeled = match (current, name.starts_with("refs/tags/"), expected) {
        (Some(_), true, Some(_)) => Some(resolve(store, auth, id, &name).await?),
        _ => None,
    };
    if peeled.is_none() || peeled.as_deref() != expected {
        expect(current, expected)?;
    }
    let new = match target {
        Some(_) if current.is_some() => return Err(GitError::Exists),
        Some(target) => resolve(store, auth, id, target).await?,
        None => ZERO_OID.to_string(),
    };
    let old = current.cloned().ok_or(GitError::NotFound);
    let old = if target.is_some() {
        ZERO_OID.to_string()
    } else {
        old?
    };
    let update = RefUpdate {
        name: name.clone(),
        old,
        new: new.clone(),
    };
    record(
        context,
        auth,
        &document,
        vec![update],
        Bytes::new(),
        Vec::new(),
    )
    .await?;
    Ok(Named { name, version: new })
}

pub(super) fn parse_conflict(name: &str, target: &str) -> Option<Conflict> {
    let rest = name.strip_prefix("refs/conflicts/")?;
    let (kind, rest) = rest.split_once('/')?;
    let (short, id) = rest.rsplit_once('/')?;
    let (branch, tag) = match kind {
        "heads" => (Some(short.to_string()), None),
        "tags" => (None, Some(short.to_string())),
        _ => return None,
    };
    Some(Conflict {
        id: id.parse().ok()?,
        branch,
        tag,
        version: target.to_string(),
    })
}

/// Branch and tag updates that lost a race between holders, with the version each kept.
pub async fn conflicts(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
) -> Result<Vec<(Conflict, Version)>, GitError> {
    let (_, projection, _guard) = open(context, store, auth, id, Permission::READ).await?;
    let tags = peeled_tags(store, auth, id, &projection).await?;
    let mut kept = Vec::new();
    for (name, target) in &projection.state.refs {
        if let Some(conflict) = parse_conflict(name, target) {
            let commit = resolve(store, auth, id, target).await?;
            let info = log(store, auth, id, (&commit, None), 0, 1)
                .await?
                .pop()
                .ok_or(GitError::Unavailable)?;
            kept.push((conflict, version(info, &projection.state.refs, &tags)));
        }
    }
    Ok(kept)
}

/// The full ref name of a kept conflict, for discarding it.
pub async fn conflict_ref(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    conflict: Ulid,
) -> Result<String, GitError> {
    let (_, projection, _guard) = open(context, store, auth, id, Permission::READ).await?;
    projection
        .state
        .refs
        .iter()
        .find(|(name, target)| parse_conflict(name, target).is_some_and(|kept| kept.id == conflict))
        .map(|(name, _)| name.clone())
        .ok_or(GitError::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_names() {
        let id = Ulid::from(7);
        let parsed = parse_conflict(&format!("refs/conflicts/heads/draft/x/{id}"), "abc");
        assert_eq!(
            parsed,
            Some(Conflict {
                id,
                branch: Some("draft/x".into()),
                tag: None,
                version: "abc".into(),
            })
        );
        let tag = parse_conflict(&format!("refs/conflicts/tags/v1/{id}"), "abc").expect("tag");
        assert_eq!((tag.branch, tag.tag), (None, Some("v1".into())));
        assert_eq!(parse_conflict("refs/conflicts/tags/v1/x", "abc"), None);
        assert_eq!(parse_conflict("refs/conflicts/notes/x/y", "abc"), None);
    }
}
