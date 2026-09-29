//! Reads a document's ARC history as versions, branches, tags and conflicts, and moves refs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::GitError;
use super::changes::{EntityChange, entity_changes};
use super::project::{Projection, lock};
use super::push::record;
use super::snapshot::{execute, refresh, view};
use super::state::GitState;
use crate::driver::DriverContext;
use aruna_blob::git::GitStore;
use aruna_core::git::{
    CommitInfo, FileChange, GitEffect, GitEvent, RefUpdate, ZERO_OID, refs_clash, valid_ref,
};
use aruna_core::repo_layout::{ARUNA_FILE, entity_path, git_copy, is_file};
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
    /// `None` when either side has no readable metadata.
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
    let reading = permission == Permission::READ;
    let (document, _) = super::repository(context, auth, id, permission).await?;
    let guard = lock(id).await;
    let projection = match reading {
        true => view(context, store, &document).await?.0,
        false => refresh(context, store, &document).await?,
    };
    Ok((document, projection, guard))
}

/// Refreshes under the lock and releases it; the reads that follow use the projection's refs
/// or immutable commits, so they may overlap other requests.
async fn read(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
) -> Result<Projection, GitError> {
    let (_, projection, _) = open(context, store, auth, id, Permission::READ).await?;
    Ok(projection)
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

/// The commit each revision names, in order, read with one Git call.
async fn peel(
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    revisions: Vec<String>,
) -> Result<Vec<String>, GitError> {
    let effect = GitEffect::Peel {
        document_id: id,
        revisions,
    };
    match execute(store, effect, auth.user_id).await? {
        GitEvent::Peeled(commits) => commits
            .into_iter()
            .collect::<Option<_>>()
            .ok_or(GitError::NotFound),
        _ => Err(GitError::Unavailable),
    }
}

/// The commits themselves by id, read with one Git call.
async fn commits(
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    revisions: Vec<String>,
) -> Result<BTreeMap<String, CommitInfo>, GitError> {
    let effect = GitEffect::Commits {
        document_id: id,
        revisions,
    };
    match execute(store, effect, auth.user_id).await? {
        GitEvent::Log(commits) => Ok(commits
            .into_iter()
            .map(|commit| (commit.commit.clone(), commit))
            .collect()),
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

/// The full metadata graph of a commit: `aruna-metadata.json` of an ARC snapshot, otherwise
/// its RO-Crate. Stored entities are named by their repository path, as in plain Git copies,
/// so a version before and after a file was stored compares equal.
pub(super) async fn graph(
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    commit: &str,
) -> Option<Value> {
    let effect = GitEffect::ReadFile {
        document_id: id,
        revision: commit.to_string(),
        path: ARUNA_FILE.to_string(),
    };
    let value = match execute(store, effect, auth.user_id).await.ok()? {
        GitEvent::File(Some(bytes)) => serde_json::from_slice(&bytes).ok()?,
        GitEvent::File(None) => rocrate(store, auth, id, commit).await?,
        _ => return None,
    };
    Some(copied(value))
}

/// Names stored entities by their repository path, as a plain Git copy does.
pub(super) fn copied(mut value: Value) -> Value {
    let paths: BTreeMap<String, String> = value["@graph"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entity| is_file(entity) && entity.get("localPath").is_some())
        .filter_map(|entity| Some((entity["@id"].as_str()?.to_owned(), entity_path(entity)?)))
        .collect();
    git_copy(&mut value, &paths);
    value
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

/// The value of `key` in the message's final trailer block, the lines after its last blank line.
fn trailer(message: &str, key: &str) -> Option<String> {
    let block = message
        .trim_end()
        .rsplit_once("\n\n")
        .map(|(_, block)| block)?;
    block
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .map(|value| value.trim().to_string())
}

/// A caller's commit message without lines that look like Aruna trailers.
pub(super) fn plain(message: &str) -> String {
    message
        .lines()
        .filter(|line| !line.trim_start().to_ascii_lowercase().starts_with("aruna-"))
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

/// Commits named by each tag, peeled so annotated tags compare with commits.
pub(super) async fn peeled_tags(
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    projection: &Projection,
) -> Result<Vec<Named>, GitError> {
    let (names, targets): (Vec<_>, Vec<_>) = projection
        .state
        .refs
        .iter()
        .filter_map(|(name, target)| Some((name.strip_prefix("refs/tags/")?, target.clone())))
        .unzip();
    let versions = peel(store, auth, id, targets).await?;
    Ok(names
        .into_iter()
        .zip(versions)
        .map(|(name, version)| Named {
            name: name.to_string(),
            version,
        })
        .collect())
}

pub(super) fn version(commit: CommitInfo, state: &GitState, tags: &[Named]) -> Version {
    let branches = state
        .refs
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
    // A client can write any trailer; only commits a node recorded as its own are believed.
    let trusted = state.made.contains(&commit.commit);
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
    let projection = read(context, store, auth, id).await?;
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
        .map(|commit| version(commit, &projection.state, &tags))
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
    let projection = read(context, store, auth, id).await?;
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
    Ok((version(info, &projection.state, &tags), files))
}

pub async fn compare(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    from: Option<&str>,
    to: &str,
) -> Result<Comparison, GitError> {
    read(context, store, auth, id).await?;
    let from = match from {
        Some(from) => Some(resolve(store, auth, id, from).await?),
        None => None,
    };
    let to = resolve(store, auth, id, to).await?;
    let files = diff(store, auth, id, from.as_deref(), &to).await?;
    let before = match &from {
        Some(from) => graph(store, auth, id, from).await,
        None => Some(serde_json::json!({ "@graph": [] })),
    };
    let entities = match (before, graph(store, auth, id, &to).await) {
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
) -> Result<Vec<(String, Version)>, GitError> {
    let projection = read(context, store, auth, id).await?;
    let tags = peeled_tags(store, auth, id, &projection).await?;
    let heads: Vec<_> = projection
        .state
        .refs
        .iter()
        .filter_map(|(name, target)| Some((name.strip_prefix("refs/heads/")?, target)))
        .collect();
    let targets = heads.iter().map(|(_, target)| (*target).clone()).collect();
    let infos = commits(store, auth, id, targets).await?;
    heads
        .into_iter()
        .map(|(short, target)| {
            let info = infos.get(target).cloned().ok_or(GitError::Unavailable)?;
            Ok((short.to_string(), version(info, &projection.state, &tags)))
        })
        .collect()
}

/// Every tag with the commit it names.
pub async fn tags(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
) -> Result<Vec<Named>, GitError> {
    let projection = read(context, store, auth, id).await?;
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
    // Git cannot store `a` next to `a/b`.
    if target.is_some()
        && let Some(other) = projection
            .state
            .refs
            .keys()
            .find(|other| refs_clash(other, &name))
    {
        return Err(GitError::Refused(format!("{name} clashes with {other}")));
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
    let nothing = (Bytes::new(), Vec::new());
    record(
        context,
        auth,
        &document,
        vec![update],
        nothing,
        (Vec::new(), None),
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
    let projection = read(context, store, auth, id).await?;
    let tags = peeled_tags(store, auth, id, &projection).await?;
    let kept: Vec<_> = projection
        .state
        .refs
        .iter()
        .filter_map(|(name, target)| parse_conflict(name, target))
        .collect();
    let targets = kept
        .iter()
        .map(|conflict| conflict.version.clone())
        .collect();
    let peeled = peel(store, auth, id, targets).await?;
    let infos = commits(store, auth, id, peeled.clone()).await?;
    kept.into_iter()
        .zip(peeled)
        .map(|(conflict, commit)| {
            let info = infos.get(&commit).cloned().ok_or(GitError::Unavailable)?;
            Ok((conflict, version(info, &projection.state, &tags)))
        })
        .collect()
}

/// The full ref name of a kept conflict, for discarding it.
pub async fn conflict_ref(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    id: Ulid,
    conflict: Ulid,
) -> Result<String, GitError> {
    let projection = read(context, store, auth, id).await?;
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
    fn trailers_need_record() {
        let commit = CommitInfo {
            commit: "a".repeat(40),
            parents: Vec::new(),
            author_name: "Aruna".into(),
            author_email: "git@aruna.local".into(),
            committer_email: "git@aruna.local".into(),
            authored_at_s: 0,
            message: "Edit\n\nAruna-User: someone".into(),
            signed: true,
        };
        let mut state = GitState::default();
        assert_eq!(version(commit.clone(), &state, &[]).user_id, None);
        state.made.insert("a".repeat(40));
        assert_eq!(
            version(commit.clone(), &state, &[]).user_id.as_deref(),
            Some("someone")
        );
        let injected = format!(
            "{}\n\nAruna-User: someone",
            plain("Edit\nAruna-Revision: 01M")
        );
        let commit = CommitInfo {
            message: injected,
            ..commit
        };
        assert_eq!(version(commit, &state, &[]).metadata_event_id, None);
    }

    async fn git(path: &std::path::Path, args: &[&str]) -> String {
        let output = tokio::process::Command::new("git")
            .current_dir(path)
            .args(["-c", "user.name=Ada", "-c", "user.email=ada@example.org"])
            .args(["-c", "commit.gpgsign=false"])
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .await
            .expect("git runs");
        String::from_utf8(output.stdout)
            .expect("utf-8")
            .trim()
            .to_string()
    }

    fn metadata(measured: &[&str]) -> Value {
        serde_json::json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": [
            {"@id": "ro-crate-metadata.json", "@type": "CreativeWork", "about": {"@id": "./"},
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}},
            {"@id": "./", "@type": "Dataset", "name": "Liver", "description": "Liver runs",
                "datePublished": "2026-09-28", "variableMeasured": measured,
                "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}}]})
    }

    /// Commits `file` holding `value` and returns the commit.
    async fn commit(path: &std::path::Path, file: &str, value: &Value) -> String {
        tokio::fs::write(path.join(file), value.to_string())
            .await
            .expect("write");
        git(path, &["add", "-A"]).await;
        git(path, &["commit", "-q", "-m", "Edit"]).await;
        git(path, &["rev-parse", "HEAD"]).await
    }

    #[tokio::test]
    async fn compares_full_graph() {
        // ARC snapshots keep the full graph in `aruna-metadata.json`, plain ones in the crate.
        for file in [ARUNA_FILE, aruna_core::repo_layout::CRATE_FILE] {
            let root = tempfile::tempdir().expect("directory");
            let id = Ulid::from(9);
            let path = root.path().join(format!("{id}.git"));
            tokio::fs::create_dir(&path).await.expect("folder");
            git(&path, &["init", "-q", "--initial-branch=main"]).await;
            let before = commit(&path, file, &metadata(&["depth"])).await;
            let after = commit(&path, file, &metadata(&["depth", "stuff"])).await;
            let store = GitStore::new(root.path().to_path_buf(), "helper".into());
            let auth = super::super::project::author(aruna_core::UserId::nil(
                aruna_core::structs::identity::realm::RealmId([1; 32]),
            ));
            let before = graph(&store, &auth, id, &before).await.expect("before");
            let after = graph(&store, &auth, id, &after).await.expect("after");
            let changes = entity_changes(&before, &after);
            assert_eq!(changes.len(), 1, "{file}");
            assert_eq!(changes[0].id, "./");
            assert_eq!(changes[0].properties[0].name, "variableMeasured");
            assert_eq!(changes[0].properties[0].after.len(), 2);
        }
    }

    #[test]
    fn compares_stored_paths() {
        let stored = serde_json::json!({"@graph": [{"@id": "https://w3id.org/aruna/data/ab",
            "@type": "File", "name": "a.csv", "contentUrl": "s3://b/doc/data/a.csv",
            "localPath": "data/a.csv"}]});
        let plain = serde_json::json!({"@graph": [{"@id": "data/a.csv", "@type": "File",
            "name": "a.csv"}]});
        let mut copied = stored.clone();
        let paths = BTreeMap::from([(
            "https://w3id.org/aruna/data/ab".to_string(),
            "data/a.csv".to_string(),
        )]);
        git_copy(&mut copied, &paths);
        assert!(entity_changes(&plain, &copied).is_empty());
    }

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
