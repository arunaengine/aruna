//! Reads a document's ARC history as versions, branches, tags and conflicts, and moves refs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{
    CommitInfo, FileChange, GitEffect, GitEvent, GitRecord, RefUpdate, ZERO_OID, refs_clash,
    valid_ref,
};
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::repo_layout::{ARUNA_FILE, entity_path, git_copy, is_file};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use serde_json::Value;
use ulid::Ulid;

use super::access::Repository;
use super::calls::{StoreReadOperation, git_call, git_event, store_read};
use super::changes::{EntityChange, entity_changes};
use super::project::Projection;
use super::push::record_made;
use super::snapshot::{read_view, refresh};
use super::state::GitState;
use super::{GitError, GitNode, repository};

type Document = MetadataRegistryRecord;
type Opened = (Document, Projection);

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
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VersionQuery {
    pub branch: String,
    pub since: Option<String>,
    pub cursor: Option<String>,
    pub limit: usize,
}

/// The commit message of a write and the branch head the caller expects, if any.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteOptions {
    pub message: Option<String>,
    pub expected: Option<String>,
}

/// A named ref and the commit it finally names; tags are peeled to their commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Named {
    pub name: String,
    pub version: String,
}

/// Authorizes the caller and refreshes the repository the caller holds.
pub(super) fn open(
    node: &GitNode,
    auth: &AuthContext,
    id: Ulid,
    permission: Permission,
) -> OpenRepositoryOperation {
    OpenRepositoryOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        permission,
        document: None,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct OpenRepositoryOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    permission: Permission,
    document: Option<Document>,
}

impl Operation for OpenRepositoryOperation {
    type Output = Opened;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Opened>, GitError> {
        let permission = self.permission.clone();
        let read = repository(self.node.node(), &self.auth, self.id, permission);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Opened>, GitError> {
        let Some(document) = self.document.take() else {
            let (document, _) = result.child::<Repository, GitError>()??;
            let run = refresh(&self.node, &document);
            self.document = Some(document);
            return Ok(Next::Call(IoCall::child(run)));
        };
        let projection = result.child::<Projection, GitError>()??;
        Ok(Next::Done((document, projection)))
    }
}

/// The current projection for a read; it may overlap other requests, since what follows uses
/// the projection's refs or immutable commits.
fn read(node: &GitNode, auth: &AuthContext, id: Ulid) -> ReadProjectionOperation {
    ReadProjectionOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        viewing: false,
    }
}

#[derive(Debug, PartialEq)]
struct ReadProjectionOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    viewing: bool,
}

impl Operation for ReadProjectionOperation {
    type Output = Projection;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Projection>, GitError> {
        let check = repository(self.node.node(), &self.auth, self.id, Permission::READ);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Projection>, GitError> {
        if self.viewing {
            let (projection, _) = result.child::<(Projection, bool), GitError>()??;
            return Ok(Next::Done(projection));
        }
        self.viewing = true;
        let (document, _) = result.child::<Repository, GitError>()??;
        let view = read_view(&self.node, &document);
        Ok(Next::Call(IoCall::child(view)))
    }
}

pub(super) fn resolve(auth: &AuthContext, id: Ulid, revision: &str) -> StoreReadOperation<String> {
    let effect = GitEffect::Resolve {
        document_id: id,
        revision: revision.to_string(),
    };
    store_read(effect, auth.user_id, |event| match event {
        GitEvent::Resolved(Some(commit)) => Ok(commit),
        GitEvent::Resolved(None) => Err(GitError::NotFound),
        _ => Err(GitError::Unavailable),
    })
}

/// The commit each revision names, in order, read with one Git call.
fn peel(auth: &AuthContext, id: Ulid, revisions: Vec<String>) -> StoreReadOperation<Vec<String>> {
    let effect = GitEffect::Peel {
        document_id: id,
        revisions,
    };
    store_read(effect, auth.user_id, |event| match event {
        GitEvent::Peeled(commits) => commits
            .into_iter()
            .collect::<Option<_>>()
            .ok_or(GitError::NotFound),
        _ => Err(GitError::Unavailable),
    })
}

type CommitMap = BTreeMap<String, CommitInfo>;

/// The commits themselves by id, read with one Git call.
fn commits(auth: &AuthContext, id: Ulid, revisions: Vec<String>) -> StoreReadOperation<CommitMap> {
    let effect = GitEffect::Commits {
        document_id: id,
        revisions,
    };
    store_read(effect, auth.user_id, |event| match event {
        GitEvent::Log(commits) => Ok(commits
            .into_iter()
            .map(|commit| (commit.commit.clone(), commit))
            .collect()),
        _ => Err(GitError::Unavailable),
    })
}

pub(super) fn log(
    auth: &AuthContext,
    id: Ulid,
    (revision, exclude): (&str, Option<&str>),
    skip: usize,
    limit: usize,
) -> StoreReadOperation<Vec<CommitInfo>> {
    let effect = GitEffect::Log {
        document_id: id,
        revision: revision.to_string(),
        exclude: exclude.map(str::to_owned),
        skip,
        limit,
    };
    store_read(effect, auth.user_id, |event| match event {
        GitEvent::Log(commits) => Ok(commits),
        _ => Err(GitError::Unavailable),
    })
}

pub(super) fn diff(
    auth: &AuthContext,
    id: Ulid,
    from: Option<&str>,
    to: &str,
) -> StoreReadOperation<Vec<FileChange>> {
    let effect = GitEffect::Diff {
        document_id: id,
        from: from.map(str::to_owned),
        to: to.to_string(),
    };
    store_read(effect, auth.user_id, |event| match event {
        GitEvent::Diff(changes) => Ok(changes),
        _ => Err(GitError::Unavailable),
    })
}

fn crate_of(bytes: &Bytes) -> Option<Value> {
    let mut value: Value = serde_json::from_slice(bytes).ok()?;
    value.get_mut("rocrate").map(Value::take)
}

/// The ISA-derived RO-Crate of a commit, or `None` when it has no valid ISA metadata.
pub(super) fn rocrate(
    auth: &AuthContext,
    id: Ulid,
    commit: &str,
) -> StoreReadOperation<Option<Value>> {
    let effect = GitEffect::Export {
        document_id: id,
        revision: commit.to_string(),
    };
    store_read(effect, auth.user_id, |event| match event {
        GitEvent::Exported(bytes) => Ok(crate_of(&bytes)),
        _ => Ok(None),
    })
}

/// The full metadata graph of a commit: `aruna-metadata.json` of an ARC snapshot, otherwise
/// its RO-Crate. Stored entities are named by their repository path, as in plain Git copies,
/// so a version before and after a file was stored compares equal.
pub(super) fn graph(auth: &AuthContext, id: Ulid, commit: &str) -> GraphOperation {
    GraphOperation {
        actor: auth.user_id,
        id,
        commit: commit.to_string(),
        exporting: false,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct GraphOperation {
    actor: aruna_core::UserId,
    id: Ulid,
    commit: String,
    exporting: bool,
}

impl Operation for GraphOperation {
    type Output = Option<Value>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Option<Value>>, GitError> {
        let effect = GitEffect::ReadFile {
            document_id: self.id,
            revision: self.commit.clone(),
            path: ARUNA_FILE.to_string(),
        };
        Ok(Next::Call(git_call(effect, self.actor)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Value>>, GitError> {
        let value = match (git_event(result), self.exporting) {
            (Ok(GitEvent::Exported(bytes)), true) => crate_of(&bytes),
            (Ok(GitEvent::File(Some(bytes))), false) => serde_json::from_slice(&bytes).ok(),
            (Ok(GitEvent::File(None)), false) => {
                self.exporting = true;
                let effect = GitEffect::Export {
                    document_id: self.id,
                    revision: self.commit.clone(),
                };
                return Ok(Next::Call(git_call(effect, self.actor)));
            }
            _ => None,
        };
        Ok(Next::Done(value.map(copied)))
    }
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
pub(super) fn peeled_tags(
    auth: &AuthContext,
    id: Ulid,
    projection: &Projection,
) -> PeeledTagsOperation {
    let (names, targets): (Vec<_>, Vec<_>) = (projection.state.refs.iter())
        .filter_map(|(name, target)| {
            Some((name.strip_prefix("refs/tags/")?.to_string(), target.clone()))
        })
        .unzip();
    PeeledTagsOperation {
        names,
        peel: Some(peel(auth, id, targets)),
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct PeeledTagsOperation {
    names: Vec<String>,
    peel: Option<StoreReadOperation<Vec<String>>>,
}

impl Operation for PeeledTagsOperation {
    type Output = Vec<Named>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Vec<Named>>, GitError> {
        let peel = self.peel.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(peel)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Named>>, GitError> {
        let versions = result.child::<Vec<String>, GitError>()??;
        let names = std::mem::take(&mut self.names);
        Ok(Next::Done(
            (names.into_iter().zip(versions))
                .map(|(name, version)| Named { name, version })
                .collect(),
        ))
    }
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

/// What the pending call of listing versions answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListStep {
    Read,
    Cursor,
    Since,
    Log,
    Tags,
}

/// Versions of a branch, newest first. The cursor pins the head the first page started
/// from, so later pages stay stable while the branch moves.
pub fn list(
    node: &GitNode,
    auth: &AuthContext,
    id: Ulid,
    query: VersionQuery,
) -> ListVersionsOperation {
    ListVersionsOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        query,
        step: ListStep::Read,
        projection: None,
        head: String::new(),
        skip: 0,
        since: None,
        commits: Vec::new(),
        next: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct ListVersionsOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    query: VersionQuery,
    step: ListStep,
    projection: Option<Projection>,
    head: String,
    skip: usize,
    since: Option<String>,
    commits: Vec<CommitInfo>,
    next: Option<String>,
}

type ListPage = (Vec<Version>, Option<String>);
type ListTurn = Result<Next<ListPage>, GitError>;

impl ListVersionsOperation {
    fn projection(&self) -> Result<&Projection, IoError> {
        self.projection.as_ref().ok_or(IoError::Unfinished)
    }

    fn limit(&self) -> usize {
        self.query.limit.clamp(1, MAX_VERSIONS)
    }

    fn read_since(&mut self) -> ListTurn {
        let Some(since) = self.query.since.as_deref() else {
            return self.read_log();
        };
        let read = resolve(&self.auth, self.id, since);
        self.step = ListStep::Since;
        Ok(Next::Call(IoCall::child(read)))
    }

    fn read_log(&mut self) -> ListTurn {
        let range = (self.head.as_str(), self.since.as_deref());
        let read = log(&self.auth, self.id, range, self.skip, self.limit() + 1);
        self.step = ListStep::Log;
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for ListVersionsOperation {
    type Output = ListPage;
    type Error = GitError;

    fn first(&mut self) -> ListTurn {
        let run = read(&self.node, &self.auth, self.id);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> ListTurn {
        match self.step {
            ListStep::Read => {
                self.projection = Some(result.child::<Projection, GitError>()??);
                if let Some(cursor) = self.query.cursor.as_deref() {
                    let (head, skip) = cursor.split_once('.').ok_or(GitError::Invalid)?;
                    self.skip = skip.parse::<usize>().map_err(|_| GitError::Invalid)?;
                    let read = resolve(&self.auth, self.id, head);
                    self.step = ListStep::Cursor;
                    return Ok(Next::Call(IoCall::child(read)));
                }
                let name = branch_ref(&self.query.branch)?;
                let refs = &self.projection()?.state.refs;
                self.head = refs.get(&name).cloned().ok_or(GitError::BranchMissing)?;
                self.read_since()
            }
            ListStep::Cursor => {
                let head = result.child::<String, GitError>()?;
                self.head = head.map_err(|_| GitError::Invalid)?;
                self.read_since()
            }
            ListStep::Since => {
                self.since = Some(result.child::<String, GitError>()??);
                self.read_log()
            }
            ListStep::Log => {
                let mut commits = result.child::<Vec<CommitInfo>, GitError>()??;
                let limit = self.limit();
                self.next =
                    (commits.len() > limit).then(|| format!("{}.{}", self.head, self.skip + limit));
                commits.truncate(limit);
                self.commits = commits;
                let tags = peeled_tags(&self.auth, self.id, self.projection()?);
                self.step = ListStep::Tags;
                Ok(Next::Call(IoCall::child(tags)))
            }
            ListStep::Tags => {
                let tags = result.child::<Vec<Named>, GitError>()??;
                let state = &self.projection()?.state;
                let versions = (self.commits.iter().cloned())
                    .map(|commit| version(commit, state, &tags))
                    .collect();
                Ok(Next::Done((versions, self.next.take())))
            }
        }
    }
}

/// What the pending call of showing a version answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShowStep {
    Read,
    Resolve,
    Log,
    Diff,
    Tags,
}

/// One version and the files it changed against its first parent.
pub fn show(node: &GitNode, auth: &AuthContext, id: Ulid, revision: &str) -> ShowVersionOperation {
    ShowVersionOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        revision: revision.to_string(),
        step: ShowStep::Read,
        projection: None,
        commit: String::new(),
        info: None,
        files: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct ShowVersionOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    revision: String,
    step: ShowStep,
    projection: Option<Projection>,
    commit: String,
    info: Option<CommitInfo>,
    files: Vec<FileChange>,
}

type Shown = (Version, Vec<FileChange>);

impl Operation for ShowVersionOperation {
    type Output = Shown;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Shown>, GitError> {
        let run = read(&self.node, &self.auth, self.id);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Shown>, GitError> {
        let (auth, id) = (&self.auth, self.id);
        match self.step {
            ShowStep::Read => {
                self.projection = Some(result.child::<Projection, GitError>()??);
                self.step = ShowStep::Resolve;
                let read = resolve(auth, id, &self.revision);
                Ok(Next::Call(IoCall::child(read)))
            }
            ShowStep::Resolve => {
                self.commit = result.child::<String, GitError>()??;
                self.step = ShowStep::Log;
                let read = log(auth, id, (&self.commit, None), 0, 1);
                Ok(Next::Call(IoCall::child(read)))
            }
            ShowStep::Log => {
                let info = result.child::<Vec<CommitInfo>, GitError>()??.pop();
                let info = info.ok_or(GitError::NotFound)?;
                let parent = info.parents.first().map(String::as_str);
                let read = diff(auth, id, parent, &self.commit);
                self.info = Some(info);
                self.step = ShowStep::Diff;
                Ok(Next::Call(IoCall::child(read)))
            }
            ShowStep::Diff => {
                self.files = result.child::<Vec<FileChange>, GitError>()??;
                let projection = self.projection.as_ref().ok_or(IoError::Unfinished)?;
                self.step = ShowStep::Tags;
                Ok(Next::Call(IoCall::child(peeled_tags(auth, id, projection))))
            }
            ShowStep::Tags => {
                let tags = result.child::<Vec<Named>, GitError>()??;
                let info = self.info.take().ok_or(IoError::Unfinished)?;
                let projection = self.projection.as_ref().ok_or(IoError::Unfinished)?;
                let shown = version(info, &projection.state, &tags);
                Ok(Next::Done((shown, std::mem::take(&mut self.files))))
            }
        }
    }
}

/// What the pending call of comparing versions answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompareStep {
    Read,
    From,
    To,
    Diff,
    Before,
    After,
}

/// The metadata entities and files that differ between two versions.
pub fn compare(
    node: &GitNode,
    auth: &AuthContext,
    id: Ulid,
    from: Option<&str>,
    to: &str,
) -> CompareVersionsOperation {
    CompareVersionsOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        asked_from: from.map(str::to_owned),
        asked_to: to.to_string(),
        step: CompareStep::Read,
        from: None,
        to: String::new(),
        files: Vec::new(),
        before: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct CompareVersionsOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    asked_from: Option<String>,
    asked_to: String,
    step: CompareStep,
    from: Option<String>,
    to: String,
    files: Vec<FileChange>,
    before: Option<Value>,
}

type CompareTurn = Result<Next<Comparison>, GitError>;

impl CompareVersionsOperation {
    fn resolve_to(&mut self) -> CompareTurn {
        self.step = CompareStep::To;
        let read = resolve(&self.auth, self.id, &self.asked_to);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn read_after(&mut self) -> CompareTurn {
        self.step = CompareStep::After;
        let read = graph(&self.auth, self.id, &self.to);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for CompareVersionsOperation {
    type Output = Comparison;
    type Error = GitError;

    fn first(&mut self) -> CompareTurn {
        let run = read(&self.node, &self.auth, self.id);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> CompareTurn {
        match self.step {
            CompareStep::Read => {
                result.child::<Projection, GitError>()??;
                let Some(from) = self.asked_from.as_deref() else {
                    return self.resolve_to();
                };
                let read = resolve(&self.auth, self.id, from);
                self.step = CompareStep::From;
                Ok(Next::Call(IoCall::child(read)))
            }
            CompareStep::From => {
                self.from = Some(result.child::<String, GitError>()??);
                self.resolve_to()
            }
            CompareStep::To => {
                self.to = result.child::<String, GitError>()??;
                self.step = CompareStep::Diff;
                let read = diff(&self.auth, self.id, self.from.as_deref(), &self.to);
                Ok(Next::Call(IoCall::child(read)))
            }
            CompareStep::Diff => {
                self.files = result.child::<Vec<FileChange>, GitError>()??;
                let Some(from) = self.from.as_deref() else {
                    self.before = Some(serde_json::json!({ "@graph": [] }));
                    return self.read_after();
                };
                let read = graph(&self.auth, self.id, from);
                self.step = CompareStep::Before;
                Ok(Next::Call(IoCall::child(read)))
            }
            CompareStep::Before => {
                self.before = result.child::<Option<Value>, GitError>()?.ok().flatten();
                self.read_after()
            }
            CompareStep::After => {
                let after = result.child::<Option<Value>, GitError>()?.ok().flatten();
                let entities = match (self.before.take(), after) {
                    (Some(before), Some(after)) => Some(entity_changes(&before, &after)),
                    _ => None,
                };
                Ok(Next::Done(Comparison {
                    from: self.from.take(),
                    to: std::mem::take(&mut self.to),
                    entities,
                    files: std::mem::take(&mut self.files),
                }))
            }
        }
    }
}

/// What the pending call of a listing with version details answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListingStep {
    Read,
    Tags,
    Peel,
    Commits,
}

/// Every branch with its head version.
pub fn branches(node: &GitNode, auth: &AuthContext, id: Ulid) -> ListBranchesOperation {
    ListBranchesOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        step: ListingStep::Read,
        projection: None,
        tags: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct ListBranchesOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    step: ListingStep,
    projection: Option<Projection>,
    tags: Vec<Named>,
}

type BranchHeads = Vec<(String, Version)>;

impl Operation for ListBranchesOperation {
    type Output = BranchHeads;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<BranchHeads>, GitError> {
        let run = read(&self.node, &self.auth, self.id);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BranchHeads>, GitError> {
        match self.step {
            ListingStep::Read => {
                let projection = result.child::<Projection, GitError>()??;
                let tags = peeled_tags(&self.auth, self.id, &projection);
                self.projection = Some(projection);
                self.step = ListingStep::Tags;
                Ok(Next::Call(IoCall::child(tags)))
            }
            ListingStep::Tags => {
                self.tags = result.child::<Vec<Named>, GitError>()??;
                let projection = self.projection.as_ref().ok_or(IoError::Unfinished)?;
                let targets = (projection.state.refs.iter())
                    .filter(|(name, _)| name.starts_with("refs/heads/"))
                    .map(|(_, target)| target.clone())
                    .collect();
                let read = commits(&self.auth, self.id, targets);
                self.step = ListingStep::Commits;
                Ok(Next::Call(IoCall::child(read)))
            }
            ListingStep::Peel => Err(IoError::Unfinished.into()),
            ListingStep::Commits => {
                let infos = result.child::<CommitMap, GitError>()??;
                let projection = self.projection.as_ref().ok_or(IoError::Unfinished)?;
                let state = &projection.state;
                let heads = (state.refs.iter())
                    .filter_map(|(name, target)| Some((name.strip_prefix("refs/heads/")?, target)))
                    .map(|(short, target)| {
                        let info = infos.get(target).cloned().ok_or(GitError::Unavailable)?;
                        Ok((short.to_string(), version(info, state, &self.tags)))
                    })
                    .collect::<Result<_, GitError>>()?;
                Ok(Next::Done(heads))
            }
        }
    }
}

/// Every tag with the commit it names.
pub fn tags(node: &GitNode, auth: &AuthContext, id: Ulid) -> ListTagsOperation {
    ListTagsOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        peeling: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct ListTagsOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    peeling: bool,
}

impl Operation for ListTagsOperation {
    type Output = Vec<Named>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Vec<Named>>, GitError> {
        let run = read(&self.node, &self.auth, self.id);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Named>>, GitError> {
        if self.peeling {
            return Ok(Next::Done(result.child::<Vec<Named>, GitError>()??));
        }
        self.peeling = true;
        let projection = result.child::<Projection, GitError>()??;
        let tags = peeled_tags(&self.auth, self.id, &projection);
        Ok(Next::Call(IoCall::child(tags)))
    }
}

pub fn protected(branch: &str) -> bool {
    PROTECTED.contains(&branch)
}

/// What the pending call of changing a ref answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RefStep {
    Open,
    Peeled,
    Target,
    Record,
}

/// Creates (`target` set) or deletes a branch or tag. Creating refuses an existing name.
pub fn change_ref(
    node: &GitNode,
    auth: &AuthContext,
    id: Ulid,
    name: String,
    target: Option<&str>,
    expected: Option<&str>,
) -> ChangeRefOperation {
    ChangeRefOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        name,
        target: target.map(str::to_owned),
        expected: expected.map(str::to_owned),
        step: RefStep::Open,
        opened: None,
        new: String::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct ChangeRefOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    name: String,
    target: Option<String>,
    expected: Option<String>,
    step: RefStep,
    opened: Option<Opened>,
    new: String,
}

type RefTurn = Result<Next<Named>, GitError>;

impl ChangeRefOperation {
    fn opened(&self) -> Result<&Opened, IoError> {
        self.opened.as_ref().ok_or(IoError::Unfinished)
    }

    fn current(&self) -> Result<Option<&String>, IoError> {
        Ok(self.opened()?.1.state.refs.get(&self.name))
    }

    /// Checks the expected version; a tag may also be named by the commit it points to.
    fn check_expected(&mut self, peeled: Option<String>) -> RefTurn {
        let expected = self.expected.as_deref();
        if peeled.is_none() || peeled.as_deref() != expected {
            expect(self.current()?, expected)?;
        }
        // Git cannot store `a` next to `a/b`.
        let refs = &self.opened()?.1.state.refs;
        if self.target.is_some()
            && let Some(other) = refs.keys().find(|other| refs_clash(other, &self.name))
        {
            return Err(GitError::Refused(format!(
                "{} clashes with {other}",
                self.name
            )));
        }
        match self.target.as_deref() {
            Some(_) if self.current()?.is_some() => Err(GitError::Exists),
            Some(target) => {
                let read = resolve(&self.auth, self.id, target);
                self.step = RefStep::Target;
                Ok(Next::Call(IoCall::child(read)))
            }
            None => self.record(ZERO_OID.to_string()),
        }
    }

    fn record(&mut self, new: String) -> RefTurn {
        let old = match self.target {
            Some(_) => ZERO_OID.to_string(),
            None => self.current()?.cloned().ok_or(GitError::NotFound)?,
        };
        let update = RefUpdate {
            name: self.name.clone(),
            old,
            new: new.clone(),
        };
        let nothing = (Bytes::new(), Vec::new());
        let record = record_made(
            self.node.node(),
            &self.auth,
            &self.opened()?.0,
            vec![update],
            nothing,
            (Vec::new(), None),
        );
        self.new = new;
        self.step = RefStep::Record;
        Ok(Next::Call(IoCall::child(record)))
    }
}

impl Operation for ChangeRefOperation {
    type Output = Named;
    type Error = GitError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::GitRepository(self.id))
    }

    fn first(&mut self) -> RefTurn {
        let name = &self.name;
        let discard = name.starts_with("refs/conflicts/") && self.target.is_none();
        if !(valid_ref(name, false) || discard) {
            return Err(GitError::Invalid);
        }
        let short = name.strip_prefix("refs/heads/").unwrap_or_default();
        if self.target.is_none() && protected(short) {
            return Err(GitError::Refused(format!("{short} cannot be deleted")));
        }
        let run = open(&self.node, &self.auth, self.id, Permission::WRITE);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> RefTurn {
        match self.step {
            RefStep::Open => {
                self.opened = Some(result.child::<Opened, GitError>()??);
                let tag = self.name.starts_with("refs/tags/");
                if tag && self.expected.is_some() && self.current()?.is_some() {
                    let read = resolve(&self.auth, self.id, &self.name);
                    self.step = RefStep::Peeled;
                    return Ok(Next::Call(IoCall::child(read)));
                }
                self.check_expected(None)
            }
            RefStep::Peeled => {
                let peeled = result.child::<String, GitError>()??;
                self.check_expected(Some(peeled))
            }
            RefStep::Target => {
                let new = result.child::<String, GitError>()??;
                self.record(new)
            }
            RefStep::Record => {
                result.child::<GitRecord, GitError>()??;
                Ok(Next::Done(Named {
                    name: std::mem::take(&mut self.name),
                    version: std::mem::take(&mut self.new),
                }))
            }
        }
    }
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
pub fn conflicts(node: &GitNode, auth: &AuthContext, id: Ulid) -> ListConflictsOperation {
    ListConflictsOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        step: ListingStep::Read,
        projection: None,
        tags: Vec::new(),
        peeled: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct ListConflictsOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    step: ListingStep,
    projection: Option<Projection>,
    tags: Vec<Named>,
    peeled: Vec<String>,
}

type KeptConflicts = Vec<(Conflict, Version)>;

impl ListConflictsOperation {
    fn kept(&self) -> Result<Vec<Conflict>, IoError> {
        let projection = self.projection.as_ref().ok_or(IoError::Unfinished)?;
        Ok((projection.state.refs.iter())
            .filter_map(|(name, target)| parse_conflict(name, target))
            .collect())
    }
}

impl Operation for ListConflictsOperation {
    type Output = KeptConflicts;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<KeptConflicts>, GitError> {
        let run = read(&self.node, &self.auth, self.id);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<KeptConflicts>, GitError> {
        match self.step {
            ListingStep::Read => {
                let projection = result.child::<Projection, GitError>()??;
                let tags = peeled_tags(&self.auth, self.id, &projection);
                self.projection = Some(projection);
                self.step = ListingStep::Tags;
                Ok(Next::Call(IoCall::child(tags)))
            }
            ListingStep::Tags => {
                self.tags = result.child::<Vec<Named>, GitError>()??;
                let targets = self.kept()?.into_iter().map(|kept| kept.version).collect();
                self.step = ListingStep::Peel;
                Ok(Next::Call(IoCall::child(peel(
                    &self.auth, self.id, targets,
                ))))
            }
            ListingStep::Peel => {
                self.peeled = result.child::<Vec<String>, GitError>()??;
                self.step = ListingStep::Commits;
                let read = commits(&self.auth, self.id, self.peeled.clone());
                Ok(Next::Call(IoCall::child(read)))
            }
            ListingStep::Commits => {
                let infos = result.child::<CommitMap, GitError>()??;
                let projection = self.projection.as_ref().ok_or(IoError::Unfinished)?;
                let kept = self.kept()?.into_iter().zip(&self.peeled);
                let found = kept
                    .map(|(conflict, commit)| {
                        let info = infos.get(commit).cloned().ok_or(GitError::Unavailable)?;
                        Ok((conflict, version(info, &projection.state, &self.tags)))
                    })
                    .collect::<Result<_, GitError>>()?;
                Ok(Next::Done(found))
            }
        }
    }
}

/// The full ref name of a kept conflict, for discarding it.
pub fn conflict_ref(
    node: &GitNode,
    auth: &AuthContext,
    id: Ulid,
    conflict: Ulid,
) -> ConflictRefOperation {
    ConflictRefOperation {
        read: Some(read(node, auth, id)),
        conflict,
    }
}

#[derive(Debug, PartialEq)]
pub struct ConflictRefOperation {
    read: Option<ReadProjectionOperation>,
    conflict: Ulid,
}

impl Operation for ConflictRefOperation {
    type Output = String;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<String>, GitError> {
        let read = self.read.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, GitError> {
        let projection = result.child::<Projection, GitError>()??;
        let conflict = self.conflict;
        let name = (projection.state.refs.iter())
            .find(|(name, target)| {
                parse_conflict(name, target).is_some_and(|kept| kept.id == conflict)
            })
            .map(|(name, _)| name.clone());
        Ok(Next::Done(name.ok_or(GitError::NotFound)?))
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{
        self, asks_child, asks_store, auth, child, dataset, git_node, projection, ran,
    };
    use super::*;
    use aruna_blob::git::GitStore;
    use aruna_core::effects::Effect;
    use aruna_core::events::Event;
    use aruna_core::git::{GitAnswer, GitCall};

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

    /// Answers the store calls of a graph read from a real store.
    async fn run_graph(store: &GitStore, mut read: GraphOperation) -> Option<Value> {
        let mut next = read.first().expect("first call");
        loop {
            let call = match next {
                Next::Done(value) => return value,
                Next::Call(IoCall::Effect(effect)) => *effect,
                other => panic!("unexpected step: {other:?}"),
            };
            let Effect::Git(call) = call else {
                panic!("a store call");
            };
            let GitCall::Run { effect, actor } = *call else {
                panic!("a store effect");
            };
            let answer = match store.execute(effect, actor).await {
                Ok(event) => GitAnswer::Ran(event),
                Err(error) => GitAnswer::Failed(error.to_string()),
            };
            let event = Event::Git(Box::new(answer));
            next = read
                .next(IoResult::Event(Box::new(event)))
                .expect("next call");
        }
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
            let before = run_graph(&store, graph(&auth, id, &before)).await;
            let after = run_graph(&store, graph(&auth, id, &after)).await;
            let (before, after) = (before.expect("before"), after.expect("after"));
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

    fn id() -> Ulid {
        dataset().document_id
    }

    fn opened_repo() -> IoResult {
        let repository = aruna_core::git::GitRepository {
            document_id: id(),
            group_id: dataset().group_id,
            bucket: "bucket".to_string(),
        };
        child(Ok::<Repository, GitError>((dataset(), repository)))
    }

    fn projected(refs: &[(&str, &str)]) -> IoResult {
        child(Ok::<_, GitError>(projection(refs)))
    }

    fn no_tags() -> IoResult {
        child(Ok::<_, GitError>(Vec::<Named>::new()))
    }

    fn query(branch: &str) -> VersionQuery {
        VersionQuery {
            branch: branch.to_string(),
            limit: 1,
            ..VersionQuery::default()
        }
    }

    // Opening authorizes first and refreshes the repository the caller holds.
    #[test]
    fn open_refreshes() {
        let mut run = open(&git_node(), &auth(), id(), Permission::WRITE);
        assert!(asks_child(&run.first()));
        assert!(asks_child(&run.next(opened_repo())));
        let known = projection(&[]);
        let answer = run.next(child(Ok::<_, GitError>(known.clone())));
        assert_eq!(answer, Ok(Next::Done((dataset(), known))));
    }

    #[test]
    fn read_views() {
        let mut run = read(&git_node(), &auth(), id());
        assert!(asks_child(&run.first()));
        assert!(asks_child(&run.next(opened_repo())));
        let known = projection(&[]);
        let viewed = child(Ok::<_, GitError>((known.clone(), true)));
        assert_eq!(run.next(viewed), Ok(Next::Done(known)));
    }

    // Tags keep their names, in ref order, with the commits they peel to.
    #[test]
    fn tags_peeled() {
        let known = projection(&[("refs/tags/v1", "tag"), ("refs/heads/main", "a")]);
        let mut run = peeled_tags(&auth(), id(), &known);
        assert!(asks_child(&run.first()));
        let answer = run.next(child(Ok::<_, GitError>(vec!["commit".to_string()])));
        let expected = vec![Named {
            name: "v1".to_string(),
            version: "commit".to_string(),
        }];
        assert_eq!(answer, Ok(Next::Done(expected)));
    }

    #[test]
    fn missing_branch_reported() {
        let mut run = list(&git_node(), &auth(), id(), query("main"));
        assert!(asks_child(&run.first()));
        assert_eq!(run.next(projected(&[])), Err(GitError::BranchMissing));
    }

    #[test]
    fn invalid_cursor_refused() {
        let cursor = VersionQuery {
            cursor: Some("no-separator".to_string()),
            ..query("main")
        };
        let mut run = list(&git_node(), &auth(), id(), cursor);
        run.first().unwrap();
        assert_eq!(run.next(projected(&[])), Err(GitError::Invalid));
    }

    // A cursor whose head is gone is the caller's error too.
    #[test]
    fn unknown_cursor_refused() {
        let cursor = VersionQuery {
            cursor: Some("gone.1".to_string()),
            ..query("main")
        };
        let mut run = list(&git_node(), &auth(), id(), cursor);
        run.first().unwrap();
        assert!(asks_child(&run.next(projected(&[]))));
        let missing = child(Err::<String, _>(GitError::NotFound));
        assert_eq!(run.next(missing), Err(GitError::Invalid));
    }

    // A full page names the next one, pinned to the head it started from.
    #[test]
    fn list_pages() {
        let mut run = list(&git_node(), &auth(), id(), query("main"));
        run.first().unwrap();
        let known = projected(&[("refs/heads/main", "a")]);
        assert!(asks_child(&run.next(known)));
        let logged = child(Ok::<_, GitError>(vec![
            testing::commit("a"),
            testing::commit("b"),
        ]));
        assert!(asks_child(&run.next(logged)));
        let Ok(Next::Done((versions, next))) = run.next(no_tags()) else {
            panic!("expected the page");
        };
        assert_eq!(versions.len(), 1);
        assert_eq!(next.as_deref(), Some("a.1"));
    }

    // A version is compared with its first parent.
    #[test]
    fn show_diffs_parent() {
        let mut run = show(&git_node(), &auth(), id(), "main");
        assert!(asks_child(&run.first()));
        assert!(asks_child(&run.next(projected(&[]))));
        let resolved = child(Ok::<_, GitError>("a".to_string()));
        assert!(asks_child(&run.next(resolved)));
        let empty = child(Ok::<_, GitError>(Vec::<CommitInfo>::new()));
        let mut missing = show(&git_node(), &auth(), id(), "main");
        missing.step = ShowStep::Log;
        assert_eq!(missing.next(empty), Err(GitError::NotFound));
        let logged = child(Ok::<_, GitError>(vec![testing::commit("a")]));
        assert!(asks_child(&run.next(logged)));
        let files = child(Ok::<_, GitError>(Vec::<FileChange>::new()));
        assert!(asks_child(&run.next(files)));
        let Ok(Next::Done((shown, _))) = run.next(no_tags()) else {
            panic!("expected the version");
        };
        assert_eq!(shown.commit.commit, "a");
    }

    // Without a start the comparison runs against an empty ARC; unreadable metadata stays open.
    #[test]
    fn compare_without_start() {
        let mut run = compare(&git_node(), &auth(), id(), None, "main");
        assert!(asks_child(&run.first()));
        assert!(asks_child(&run.next(projected(&[]))));
        let to = child(Ok::<_, GitError>("a".to_string()));
        assert!(asks_child(&run.next(to)));
        let files = child(Ok::<_, GitError>(Vec::<FileChange>::new()));
        assert!(asks_child(&run.next(files)));
        assert_eq!(run.step, CompareStep::After);
        let unread = child(Ok::<Option<Value>, GitError>(None));
        let expected = Comparison {
            from: None,
            to: "a".to_string(),
            entities: None,
            files: Vec::new(),
        };
        assert_eq!(run.next(unread), Ok(Next::Done(expected)));
    }

    #[test]
    fn branches_list_heads() {
        let mut run = branches(&git_node(), &auth(), id());
        assert!(asks_child(&run.first()));
        assert!(asks_child(
            &run.next(projected(&[("refs/heads/main", "a")]))
        ));
        assert!(asks_child(&run.next(no_tags())));
        let infos = CommitMap::from([("a".to_string(), testing::commit("a"))]);
        let Ok(Next::Done(heads)) = run.next(child(Ok::<_, GitError>(infos))) else {
            panic!("expected the branches");
        };
        assert_eq!(heads[0].0, "main");
        assert_eq!(heads[0].1.branches, vec!["main".to_string()]);
    }

    // A head without its commit means the cache is not usable.
    #[test]
    fn missing_head_unavailable() {
        let mut run = branches(&git_node(), &auth(), id());
        run.first().unwrap();
        run.next(projected(&[("refs/heads/main", "a")])).unwrap();
        run.next(no_tags()).unwrap();
        let none = child(Ok::<_, GitError>(CommitMap::new()));
        assert_eq!(run.next(none), Err(GitError::Unavailable));
    }

    #[test]
    fn tags_listed() {
        let mut run = tags(&git_node(), &auth(), id());
        assert!(asks_child(&run.first()));
        assert!(asks_child(&run.next(projected(&[]))));
        assert_eq!(run.next(no_tags()), Ok(Next::Done(Vec::new())));
    }

    fn changing(name: &str, target: Option<&str>, expected: Option<&str>) -> ChangeRefOperation {
        change_ref(
            &git_node(),
            &auth(),
            id(),
            name.to_string(),
            target,
            expected,
        )
    }

    fn opened_with(refs: &[(&str, &str)]) -> IoResult {
        child(Ok::<Opened, GitError>((dataset(), projection(refs))))
    }

    // Names are checked before the repository is touched.
    #[test]
    fn ref_names_checked() {
        assert_eq!(
            changing("bad name", None, None).first(),
            Err(GitError::Invalid)
        );
        let main = changing("refs/heads/main", None, None).first();
        assert!(matches!(main, Err(GitError::Refused(_))));
    }

    #[test]
    fn existing_ref_refused() {
        let name = "refs/heads/draft";
        let mut run = changing(name, Some("main"), None);
        assert_eq!(run.hold_scope(), Some(HoldScope::GitRepository(id())));
        assert!(asks_child(&run.first()));
        assert_eq!(run.next(opened_with(&[(name, "a")])), Err(GitError::Exists));
    }

    // Git cannot keep `a` next to `a/b`.
    #[test]
    fn clashing_ref_refused() {
        let mut run = changing("refs/heads/a/b", Some("main"), None);
        run.first().unwrap();
        let answer = run.next(opened_with(&[("refs/heads/a", "c")]));
        assert!(matches!(answer, Err(GitError::Refused(_))));
    }

    #[test]
    fn moved_ref_refused() {
        let name = "refs/heads/draft";
        let mut run = changing(name, None, Some("other"));
        run.first().unwrap();
        assert_eq!(run.next(opened_with(&[(name, "a")])), Err(GitError::Stale));
    }

    #[test]
    fn missing_ref_reported() {
        let mut run = changing("refs/heads/draft", None, None);
        run.first().unwrap();
        assert_eq!(run.next(opened_with(&[])), Err(GitError::NotFound));
    }

    // A tag may be named by the commit it points to.
    #[test]
    fn tag_matches_commit() {
        let name = "refs/tags/v1";
        let mut run = changing(name, None, Some("commit"));
        run.first().unwrap();
        assert!(asks_child(&run.next(opened_with(&[(name, "tag")]))));
        assert_eq!(run.step, RefStep::Peeled);
        let peeled = child(Ok::<_, GitError>("commit".to_string()));
        assert!(asks_child(&run.next(peeled)));
        assert_eq!(run.step, RefStep::Record);
    }

    // A new ref starts at its resolved target and is answered once recorded.
    #[test]
    fn new_ref_recorded() {
        let name = "refs/heads/draft";
        let mut run = changing(name, Some("main"), None);
        run.first().unwrap();
        assert!(asks_child(&run.next(opened_with(&[]))));
        let target = child(Ok::<_, GitError>("a".to_string()));
        assert!(asks_child(&run.next(target)));
        let change = aruna_core::git::GitChange::Unlock { id: Ulid::from(9) };
        let recorded = child(Ok::<_, GitError>(testing::record(1, change)));
        let expected = Named {
            name: name.to_string(),
            version: "a".to_string(),
        };
        assert_eq!(run.next(recorded), Ok(Next::Done(expected)));
    }

    #[test]
    fn conflicts_list_versions() {
        let kept = Ulid::from(7);
        let name = format!("refs/conflicts/heads/draft/{kept}");
        let mut run = conflicts(&git_node(), &auth(), id());
        assert!(asks_child(&run.first()));
        assert!(asks_child(&run.next(projected(&[(&name, "tag")]))));
        assert!(asks_child(&run.next(no_tags())));
        let peeled = child(Ok::<_, GitError>(vec!["a".to_string()]));
        assert!(asks_child(&run.next(peeled)));
        let infos = CommitMap::from([("a".to_string(), testing::commit("a"))]);
        let Ok(Next::Done(found)) = run.next(child(Ok::<_, GitError>(infos))) else {
            panic!("expected the conflicts");
        };
        assert_eq!(found[0].0.id, kept);
    }

    #[test]
    fn conflict_ref_found() {
        let kept = Ulid::from(7);
        let name = format!("refs/conflicts/tags/v1/{kept}");
        let mut run = conflict_ref(&git_node(), &auth(), id(), kept);
        assert!(asks_child(&run.first()));
        let answer = run.next(projected(&[(&name, "a")]));
        assert_eq!(answer, Ok(Next::Done(name)));
        let mut unknown = conflict_ref(&git_node(), &auth(), id(), kept);
        unknown.first().unwrap();
        assert_eq!(unknown.next(projected(&[])), Err(GitError::NotFound));
    }

    #[test]
    fn graph_prefers_file() {
        let mut read = graph(&auth(), id(), "a");
        assert!(asks_store(&read.first()));
        let file = Bytes::from_static(br#"{"@graph":[]}"#);
        let answer = read.next(ran(GitEvent::File(Some(file))));
        assert_eq!(
            answer,
            Ok(Next::Done(Some(serde_json::json!({"@graph": []}))))
        );
    }

    #[test]
    fn graph_falls_back() {
        let mut read = graph(&auth(), id(), "a");
        read.first().unwrap();
        assert!(asks_store(&read.next(ran(GitEvent::File(None)))));
        let export = Bytes::from_static(br#"{"rocrate":{"@graph":[]}}"#);
        let answer = read.next(ran(GitEvent::Exported(export)));
        assert_eq!(
            answer,
            Ok(Next::Done(Some(serde_json::json!({"@graph": []}))))
        );
    }
}
