//! Commits file contents to a repository's main branch on the node, for callers without a Git
//! client: the node writes the commit and publishes it to every holder like a push.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitEffect, GitEvent, GitNode, GitRecord, RefUpdate, valid_path};
use aruna_core::git_state::Projection;
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::repo_layout::{ARUNA_FILE, CRATE_FILE};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use ulid::Ulid;

use super::GitError;
use super::calls::{git_call, git_event, store_read};
use super::push::{record_made, unlocked};
use super::versions::{open, plain};

const MAIN: &str = "refs/heads/main";

type Opened = (MetadataRegistryRecord, Projection);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitStep {
    Open,
    Commit,
    Pack,
    Record,
}

/// Commits `files` on top of main at `base`, which must still be main's head. The metadata
/// files stay with the metadata document, so a commit cannot change them.
pub fn commit_files(
    node: &GitNode,
    auth: &AuthContext,
    (id, base): (Ulid, String),
    files: Vec<(String, Option<Bytes>)>,
    message: String,
) -> CommitFilesOperation {
    CommitFilesOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        base,
        files,
        message,
        now_ms: 0,
        step: CommitStep::Open,
        opened: None,
        new: String::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct CommitFilesOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    base: String,
    files: Vec<(String, Option<Bytes>)>,
    message: String,
    now_ms: u64,
    step: CommitStep,
    opened: Option<Opened>,
    new: String,
}

impl CommitFilesOperation {
    fn paths(&self) -> Vec<String> {
        self.files.iter().map(|(path, _)| path.clone()).collect()
    }
}

impl Operation for CommitFilesOperation {
    /// The new main commit, or the base when nothing changed.
    type Output = String;
    type Error = GitError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::GitRepository(self.id))
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<String>, GitError> {
        let metadata = |path: &str| path == CRATE_FILE || path == ARUNA_FILE;
        let refused = self
            .files
            .iter()
            .find(|(path, _)| !valid_path(path) || metadata(path));
        if let Some((path, _)) = refused {
            return Err(GitError::Refused(format!(
                "{path} cannot be committed here"
            )));
        }
        if self.files.is_empty() {
            return Err(GitError::Refused("a commit needs at least one file".into()));
        }
        let run = open(&self.node, &self.auth, self.id, Permission::WRITE);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, GitError> {
        let actor = self.auth.user_id;
        match self.step {
            CommitStep::Open => {
                let opened = result.child::<Opened, GitError>()??;
                let head = opened.1.state.refs.get(MAIN).cloned();
                if head.as_deref() != Some(self.base.as_str()) {
                    return Err(GitError::Stale);
                }
                unlocked(&opened.1.state, &self.auth, &self.paths())?;
                self.opened = Some(opened);
                let effect = GitEffect::Commit {
                    document_id: self.id,
                    head: self.base.clone(),
                    files: std::mem::take(&mut self.files),
                    message: format!("{}\n\nAruna-User: {}\n", plain(&self.message), actor),
                    occurred_at_ms: self.now_ms,
                };
                self.step = CommitStep::Commit;
                Ok(Next::Call(git_call(effect, actor)))
            }
            CommitStep::Commit => {
                self.new = match git_event(result)? {
                    GitEvent::Edited(Ok(new)) => new,
                    GitEvent::Edited(Err(error)) => return Err(GitError::Refused(error)),
                    _ => return Err(GitError::Unavailable),
                };
                if self.new == self.base {
                    return Ok(Next::Done(self.new.clone()));
                }
                let projection = &self.opened.as_ref().ok_or(IoError::Unfinished)?.1;
                let effect = GitEffect::Pack {
                    document_id: self.id,
                    include: vec![self.new.clone()],
                    exclude: projection.state.refs.values().cloned().collect(),
                };
                let read = store_read(effect, actor, |event| match event {
                    GitEvent::Packed(pack) => Ok(pack),
                    _ => Err(GitError::Unavailable),
                });
                self.step = CommitStep::Pack;
                Ok(Next::Call(IoCall::child(read)))
            }
            CommitStep::Pack => {
                let pack = result.child::<Bytes, GitError>()??;
                let update = RefUpdate {
                    name: MAIN.to_string(),
                    old: self.base.clone(),
                    new: self.new.clone(),
                };
                let (document, _) = self.opened.as_ref().ok_or(IoError::Unfinished)?;
                let record = record_made(
                    self.node.node(),
                    &self.auth,
                    document,
                    vec![update],
                    (pack, Vec::new()),
                    (vec![self.new.clone()], None),
                );
                self.step = CommitStep::Record;
                Ok(Next::Call(IoCall::child(record)))
            }
            CommitStep::Record => {
                result.child::<GitRecord, GitError>()??;
                Ok(Next::Done(self.new.clone()))
            }
        }
    }
}

/// The files of one commit, with their contents when they were asked for.
#[derive(Clone, Debug, PartialEq)]
pub struct Tree {
    pub commit: String,
    pub files: Vec<(String, Option<Bytes>)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TreeStep {
    Open,
    Resolve,
    List,
    Read,
}

/// Lists the files of `revision` below `root` for a caller with READ, and reads them when
/// `max_bytes` is set; files beyond that many bytes together refuse the read.
pub fn read_tree(
    node: &GitNode,
    auth: &AuthContext,
    (id, revision): (Ulid, &str),
    root: &str,
    max_bytes: Option<u64>,
) -> ReadTreeOperation {
    ReadTreeOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        revision: revision.to_string(),
        root: root.trim_matches('/').to_string(),
        max_bytes,
        step: TreeStep::Open,
        tree: Tree {
            commit: String::new(),
            files: Vec::new(),
        },
        pending: Vec::new(),
        total: 0,
    }
}

#[derive(Debug, PartialEq)]
pub struct ReadTreeOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    revision: String,
    root: String,
    max_bytes: Option<u64>,
    step: TreeStep,
    tree: Tree,
    pending: Vec<String>,
    total: u64,
}

impl ReadTreeOperation {
    fn read_next(&mut self) -> Result<Next<Tree>, GitError> {
        let Some(path) = self.pending.pop() else {
            return Ok(Next::Done(std::mem::replace(
                &mut self.tree,
                Tree {
                    commit: String::new(),
                    files: Vec::new(),
                },
            )));
        };
        let effect = GitEffect::ReadFile {
            document_id: self.id,
            revision: self.tree.commit.clone(),
            path: path.clone(),
        };
        let read = store_read(effect, self.auth.user_id, |event| match event {
            GitEvent::File(Some(bytes)) => Ok(bytes),
            GitEvent::File(None) => Err(GitError::NotFound),
            _ => Err(GitError::Unavailable),
        });
        self.tree.files.push((path, None));
        self.step = TreeStep::Read;
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for ReadTreeOperation {
    type Output = Tree;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Tree>, GitError> {
        let run = open(&self.node, &self.auth, self.id, Permission::READ);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Tree>, GitError> {
        match self.step {
            TreeStep::Open => {
                result.child::<Opened, GitError>()??;
                let read = super::versions::resolve(&self.auth, self.id, &self.revision);
                self.step = TreeStep::Resolve;
                Ok(Next::Call(IoCall::child(read)))
            }
            TreeStep::Resolve => {
                self.tree.commit = result.child::<String, GitError>()??;
                let read = super::versions::diff(&self.auth, self.id, None, &self.tree.commit);
                self.step = TreeStep::List;
                Ok(Next::Call(IoCall::child(read)))
            }
            TreeStep::List => {
                let changes = result.child::<Vec<aruna_core::git::FileChange>, GitError>()??;
                let prefix = format!("{}/", self.root);
                let mut paths: Vec<String> = (changes.into_iter())
                    .map(|change| change.path)
                    .filter_map(|path| match self.root.is_empty() {
                        true => Some(path),
                        false => path.strip_prefix(&prefix).map(str::to_owned),
                    })
                    .collect();
                paths.sort();
                if self.max_bytes.is_none() {
                    self.tree.files = paths.into_iter().map(|path| (path, None)).collect();
                    return self.read_next();
                }
                // Read in path order: the pending list pops from its end.
                paths.reverse();
                self.pending = (paths.into_iter())
                    .map(|path| match self.root.is_empty() {
                        true => path,
                        false => format!("{prefix}{path}"),
                    })
                    .collect();
                self.read_next()
            }
            TreeStep::Read => {
                let bytes = result.child::<Bytes, GitError>()??;
                self.total = self.total.saturating_add(bytes.len() as u64);
                if self.max_bytes.is_some_and(|max| self.total > max) {
                    return Err(GitError::Refused(format!(
                        "the files below {} exceed {} bytes",
                        self.root,
                        self.max_bytes.unwrap_or_default()
                    )));
                }
                let entry = self.tree.files.last_mut().ok_or(IoError::Unfinished)?;
                if let Some(relative) = entry.0.strip_prefix(&format!("{}/", self.root)) {
                    entry.0 = relative.to_string();
                }
                entry.1 = Some(bytes);
                self.read_next()
            }
        }
    }
}
