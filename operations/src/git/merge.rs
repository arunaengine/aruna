//! Edits draft branches and merges branches, as new versions published to every holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{
    CommitInfo, FileChange, GitEffect, GitEvent, GitRecord, GitSnapshot, LinkedObject,
    MergeOutcome, PendingMerge, RefUpdate, StoredObject, ZERO_OID,
};
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use serde_json::Value;
use tracing::warn;
use ulid::Ulid;

use super::calls::{git_call, git_event, store_read};
use super::changes::property_conflicts;
use super::linked::linked;
use super::project::Projection;
use super::push::{record_made, unlocked};
use super::versions::{
    Named, Version, WriteOptions, branch_ref, diff, expect, log, open, parse_conflict, peeled_tags,
    plain, protected, resolve, rocrate, version,
};
use super::{GitError, GitNode, MergeConflict, pending};

type Document = MetadataRegistryRecord;
type Opened = (Document, Projection);

/// Packs the objects of `commit` that no current ref reaches.
fn pack_call(auth: &AuthContext, id: Ulid, projection: &Projection, commit: &str) -> IoCall {
    let effect = GitEffect::Pack {
        document_id: id,
        include: vec![commit.to_string()],
        exclude: projection.state.refs.values().cloned().collect(),
    };
    let read = store_read(effect, auth.user_id, |event| match event {
        GitEvent::Packed(pack) => Ok(pack),
        _ => Err(GitError::Unavailable),
    });
    IoCall::child(read)
}

fn changed_paths(changes: Vec<FileChange>) -> Vec<String> {
    changes.into_iter().map(|change| change.path).collect()
}

/// What the pending call of editing a draft answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EditStep {
    Open,
    Linked,
    Edit,
    Diff,
    Pack,
    Record,
    Log,
    Tags,
}

/// Replaces a draft branch's metadata with `jsonld` as a new version on that branch.
pub fn edit(
    node: &GitNode,
    auth: &AuthContext,
    id: Ulid,
    branch: &str,
    jsonld: String,
    options: WriteOptions,
) -> EditOperation {
    EditOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        branch: branch.to_string(),
        jsonld,
        options,
        now_ms: 0,
        step: EditStep::Open,
        opened: None,
        name: String::new(),
        head: String::new(),
        new: String::new(),
        lfs: Vec::new(),
        info: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct EditOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    branch: String,
    jsonld: String,
    options: WriteOptions,
    now_ms: u64,
    step: EditStep,
    opened: Option<Opened>,
    name: String,
    head: String,
    new: String,
    lfs: Vec<StoredObject>,
    info: Option<CommitInfo>,
}

type EditTurn = Result<Next<Version>, GitError>;

impl EditOperation {
    fn opened(&self) -> Result<&Opened, IoError> {
        self.opened.as_ref().ok_or(IoError::Unfinished)
    }

    fn read_log(&mut self) -> EditTurn {
        self.step = EditStep::Log;
        let read = log(&self.auth, self.id, (&self.new, None), 0, 1);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for EditOperation {
    type Output = Version;
    type Error = GitError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::GitRepository(self.id))
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> EditTurn {
        if protected(&self.branch) {
            return Err(GitError::Refused(format!(
                "{} is not a draft; edit main through the metadata document",
                self.branch
            )));
        }
        self.name = branch_ref(&self.branch)?;
        let run = open(&self.node, &self.auth, self.id, Permission::WRITE);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> EditTurn {
        let (id, actor) = (self.id, self.auth.user_id);
        match self.step {
            EditStep::Open => {
                let opened = result.child::<Opened, GitError>()??;
                let (document, projection) = &opened;
                let head = projection.state.refs.get(&self.name).cloned();
                let head = head.ok_or(GitError::NotFound)?;
                expect(Some(&head), self.options.expected.as_deref())?;
                let find = linked(self.node.ids, document, &self.jsonld, &self.auth);
                self.head = head;
                self.opened = Some(opened);
                self.step = EditStep::Linked;
                Ok(Next::Call(IoCall::child(find)))
            }
            EditStep::Linked => {
                let objects = result.child::<Vec<LinkedObject>, GitError>()??;
                self.lfs = objects.iter().map(|linked| linked.object.clone()).collect();
                let summary =
                    (self.options.message.take()).unwrap_or_else(|| "Edit metadata".to_string());
                let effect = GitEffect::Edit {
                    head: self.head.clone(),
                    snapshot: GitSnapshot {
                        document_id: id,
                        event_id: Ulid::nil(),
                        occurred_at_ms: self.now_ms,
                        jsonld: std::mem::take(&mut self.jsonld),
                        objects,
                        message: None,
                    },
                    message: format!("{}\n\nAruna-User: {}\n", plain(&summary), actor),
                };
                self.step = EditStep::Edit;
                Ok(Next::Call(git_call(effect, actor)))
            }
            EditStep::Edit => {
                self.new = match git_event(result)? {
                    GitEvent::Edited(Ok(new)) => new,
                    GitEvent::Edited(Err(error)) => return Err(GitError::Refused(error)),
                    _ => return Err(GitError::Unavailable),
                };
                if self.new == self.head {
                    return self.read_log();
                }
                self.step = EditStep::Diff;
                let read = diff(&self.auth, id, Some(&self.head), &self.new);
                Ok(Next::Call(IoCall::child(read)))
            }
            EditStep::Diff => {
                let paths = changed_paths(result.child::<Vec<FileChange>, GitError>()??);
                let (_, projection) = self.opened()?;
                unlocked(&projection.state, &self.auth, &paths)?;
                let call = pack_call(&self.auth, id, projection, &self.new);
                self.step = EditStep::Pack;
                Ok(Next::Call(call))
            }
            EditStep::Pack => {
                let bytes = result.child::<Bytes, GitError>()??;
                let update = RefUpdate {
                    name: self.name.clone(),
                    old: self.head.clone(),
                    new: self.new.clone(),
                };
                let lfs = std::mem::take(&mut self.lfs);
                let (document, _) = self.opened()?;
                let record = record_made(
                    self.node.node(),
                    &self.auth,
                    document,
                    vec![update],
                    (bytes, lfs),
                    (vec![self.new.clone()], None),
                );
                self.step = EditStep::Record;
                Ok(Next::Call(IoCall::child(record)))
            }
            EditStep::Record => {
                result.child::<GitRecord, GitError>()??;
                self.read_log()
            }
            EditStep::Log => {
                let info = result.child::<Vec<CommitInfo>, GitError>()??.pop();
                self.info = Some(info.ok_or(GitError::Unavailable)?);
                let (_, projection) = self.opened()?;
                let tags = peeled_tags(&self.auth, id, projection);
                self.step = EditStep::Tags;
                Ok(Next::Call(IoCall::child(tags)))
            }
            EditStep::Tags => {
                let tags = result.child::<Vec<Named>, GitError>()??;
                let info = self.info.take().ok_or(IoError::Unfinished)?;
                // The answer shows the state after the edit, which the projection does not know.
                let mut after = self.opened()?.1.state.clone();
                after.refs.insert(self.name.clone(), self.new.clone());
                after.made.insert(self.new.clone());
                Ok(Next::Done(version(info, &after, &tags)))
            }
        }
    }
}

/// The version a merge produced and whether the target simply moved forward.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Merged {
    pub version: String,
    pub fast_forward: bool,
}

/// What the pending call of a merge answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MergeStep {
    Open,
    Resolve,
    Merge,
}

/// Merges a branch, tag or version into a branch. Metadata properties both sides changed
/// differently refuse the merge. A merge into main also updates the metadata document.
pub fn merge(
    node: &GitNode,
    auth: &AuthContext,
    id: Ulid,
    source: &str,
    into: &str,
    options: WriteOptions,
) -> MergeOperation {
    MergeOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        source: source.to_string(),
        into: into.to_string(),
        options: Some(options),
        step: MergeStep::Open,
        opened: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct MergeOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    source: String,
    into: String,
    options: Option<WriteOptions>,
    step: MergeStep,
    opened: Option<Opened>,
}

type MergeTurn = Result<Next<Merged>, GitError>;

impl Operation for MergeOperation {
    type Output = Merged;
    type Error = GitError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::GitRepository(self.id))
    }

    fn first(&mut self) -> MergeTurn {
        let run = open(&self.node, &self.auth, self.id, Permission::WRITE);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> MergeTurn {
        match self.step {
            MergeStep::Open => {
                self.opened = Some(result.child::<Opened, GitError>()??);
                self.step = MergeStep::Resolve;
                let read = resolve(&self.auth, self.id, &self.source);
                Ok(Next::Call(IoCall::child(read)))
            }
            MergeStep::Resolve => {
                let source = result.child::<String, GitError>()??;
                let (document, projection) = self.opened.take().ok_or(IoError::Unfinished)?;
                let options = self.options.take().ok_or(IoError::Unfinished)?;
                let scope = MergeScope {
                    node: self.node.clone(),
                    auth: self.auth.clone(),
                    document,
                    projection,
                };
                self.step = MergeStep::Merge;
                let run = merge_into(scope, source, &self.into, options, Vec::new());
                Ok(Next::Call(IoCall::child(run)))
            }
            MergeStep::Merge => Ok(Next::Done(result.child::<Merged, GitError>()??)),
        }
    }
}

/// The opened repository a merge works on.
#[derive(Debug, PartialEq)]
struct MergeScope {
    node: GitNode,
    auth: AuthContext,
    document: Document,
    projection: Projection,
}

/// What the pending call of merging into a branch answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntoStep {
    Start,
    Base,
    BaseCrate,
    OursCrate,
    TheirsCrate,
    Merge,
    Diff,
    Pack,
    Record,
    Pending,
}

fn merge_into(
    scope: MergeScope,
    source: String,
    into: &str,
    options: WriteOptions,
    updates: Vec<RefUpdate>,
) -> MergeIntoOperation {
    MergeIntoOperation {
        scope,
        source,
        into: into.to_string(),
        options,
        updates,
        step: IntoStep::Start,
        name: String::new(),
        target: String::new(),
        crates: Vec::new(),
        new: String::new(),
        fast_forward: false,
        bytes: Bytes::new(),
    }
}

#[derive(Debug, PartialEq)]
struct MergeIntoOperation {
    scope: MergeScope,
    source: String,
    into: String,
    options: WriteOptions,
    updates: Vec<RefUpdate>,
    step: IntoStep,
    name: String,
    target: String,
    crates: Vec<Value>,
    new: String,
    fast_forward: bool,
    bytes: Bytes,
}

impl MergeIntoOperation {
    fn id(&self) -> Ulid {
        self.scope.document.document_id
    }

    fn read_crate(&mut self, step: IntoStep, commit: String) -> MergeTurn {
        self.step = step;
        let read = rocrate(&self.scope.auth, self.id(), &commit);
        Ok(Next::Call(IoCall::child(read)))
    }

    /// Asks Git to merge; called once the metadata of both sides raised no conflict.
    fn merge(&mut self) -> MergeTurn {
        let auth = &self.scope.auth;
        let summary =
            (self.options.message.take()).unwrap_or_else(|| format!("Merge into {}", self.into));
        let effect = GitEffect::Merge {
            document_id: self.id(),
            target: self.target.clone(),
            source: self.source.clone(),
            message: format!("{}\n\nAruna-User: {}\n", plain(&summary), auth.user_id),
        };
        let actor = auth.user_id;
        self.step = IntoStep::Merge;
        Ok(Next::Call(git_call(effect, actor)))
    }

    fn check_crates(&mut self) -> MergeTurn {
        let crates: [Value; 3] = std::mem::take(&mut self.crates)
            .try_into()
            .map_err(|_| IoError::Unfinished)?;
        let [base, ours, theirs] = crates;
        let properties = property_conflicts(&base, &theirs, &ours);
        if !properties.is_empty() {
            return Err(GitError::MergeConflict(Box::new(MergeConflict {
                files: Vec::new(),
                properties,
            })));
        }
        self.merge()
    }

    /// A commit without readable metadata cannot conflict, so Git decides alone.
    fn with_crate(&mut self, result: IoResult, next: Option<(IntoStep, String)>) -> MergeTurn {
        let found = result.child::<Option<Value>, GitError>()?.ok().flatten();
        let Some(value) = found else {
            return self.merge();
        };
        self.crates.push(value);
        match next {
            Some((step, commit)) => self.read_crate(step, commit),
            None => self.check_crates(),
        }
    }

    fn record(&mut self) -> MergeTurn {
        let moved = self.new != self.target;
        let made = match self.fast_forward || !moved {
            true => Vec::new(),
            false => vec![self.new.clone()],
        };
        if self.updates.is_empty() {
            return self.finish();
        }
        let scope = &self.scope;
        let merge = self.follows().then(|| PendingMerge {
            user_id: scope.auth.user_id,
            old: self.target.clone(),
            new: self.new.clone(),
        });
        let record = record_made(
            scope.node.node(),
            &scope.auth,
            &scope.document,
            std::mem::take(&mut self.updates),
            (std::mem::take(&mut self.bytes), Vec::new()),
            (made, merge),
        );
        self.step = IntoStep::Record;
        Ok(Next::Call(IoCall::child(record)))
    }

    /// Merged metadata follows the recorded merge, so a failed record changes nothing.
    fn follows(&self) -> bool {
        self.into == "main" && self.new != self.target
    }

    fn finish(&mut self) -> MergeTurn {
        Ok(Next::Done(Merged {
            version: std::mem::take(&mut self.new),
            fast_forward: self.fast_forward,
        }))
    }
}

impl Operation for MergeIntoOperation {
    type Output = Merged;
    type Error = GitError;

    fn first(&mut self) -> MergeTurn {
        if self.into == "aruna" {
            return Err(GitError::Refused(
                "aruna only records graph snapshots".into(),
            ));
        }
        self.name = branch_ref(&self.into)?;
        let refs = &self.scope.projection.state.refs;
        self.target = refs.get(&self.name).cloned().ok_or(GitError::NotFound)?;
        expect(Some(&self.target), self.options.expected.as_deref())?;
        let base = GitEffect::MergeBase {
            document_id: self.id(),
            first: self.target.clone(),
            second: self.source.clone(),
        };
        self.step = IntoStep::Base;
        Ok(Next::Call(git_call(base, self.scope.auth.user_id)))
    }

    fn next(&mut self, result: IoResult) -> MergeTurn {
        let id = self.id();
        match self.step {
            IntoStep::Start => Err(IoError::Unfinished.into()),
            IntoStep::Base => match git_event(result)? {
                GitEvent::Resolved(Some(base)) => self.read_crate(IntoStep::BaseCrate, base),
                _ => self.merge(),
            },
            IntoStep::BaseCrate => {
                let ours = (IntoStep::OursCrate, self.target.clone());
                self.with_crate(result, Some(ours))
            }
            IntoStep::OursCrate => {
                let theirs = (IntoStep::TheirsCrate, self.source.clone());
                self.with_crate(result, Some(theirs))
            }
            IntoStep::TheirsCrate => self.with_crate(result, None),
            IntoStep::Merge => {
                let GitEvent::Merged(outcome) = git_event(result)? else {
                    return Err(GitError::Unavailable);
                };
                (self.new, self.fast_forward) = match outcome {
                    MergeOutcome::UpToDate => (self.target.clone(), false),
                    MergeOutcome::FastForward => (self.source.clone(), true),
                    MergeOutcome::Merged(commit) => (commit, false),
                    MergeOutcome::Conflicts(files) => {
                        return Err(GitError::MergeConflict(Box::new(MergeConflict {
                            files,
                            properties: Vec::new(),
                        })));
                    }
                    MergeOutcome::Failed(error) => return Err(GitError::Refused(error)),
                };
                if self.new == self.target {
                    return self.record();
                }
                self.step = IntoStep::Diff;
                let read = diff(&self.scope.auth, id, Some(&self.target), &self.new);
                Ok(Next::Call(IoCall::child(read)))
            }
            IntoStep::Diff => {
                let paths = changed_paths(result.child::<Vec<FileChange>, GitError>()??);
                let scope = &self.scope;
                unlocked(&scope.projection.state, &scope.auth, &paths)?;
                let call = pack_call(&scope.auth, id, &scope.projection, &self.new);
                self.step = IntoStep::Pack;
                Ok(Next::Call(call))
            }
            IntoStep::Pack => {
                self.bytes = result.child::<Bytes, GitError>()??;
                let update = RefUpdate {
                    name: self.name.clone(),
                    old: self.target.clone(),
                    new: self.new.clone(),
                };
                self.updates.insert(0, update);
                self.record()
            }
            IntoStep::Record => {
                result.child::<GitRecord, GitError>()??;
                if !self.follows() {
                    return self.finish();
                }
                self.step = IntoStep::Pending;
                let apply = pending::apply(&self.scope.node, &self.scope.document);
                Ok(Next::Call(IoCall::child(apply)))
            }
            IntoStep::Pending => {
                if let Err(error) = result.child::<(), GitError>()? {
                    warn!(document_id = %id, %error, "Merged metadata applies on the next refresh");
                }
                self.finish()
            }
        }
    }
}

/// Merges a kept conflict into its branch and removes it in the same record.
pub fn resolve_conflict(
    node: &GitNode,
    auth: &AuthContext,
    (id, conflict): (Ulid, Ulid),
    expected: Option<String>,
) -> ResolveConflictOperation {
    ResolveConflictOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        conflict,
        expected,
        opened: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct ResolveConflictOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    conflict: Ulid,
    expected: Option<String>,
    opened: bool,
}

impl Operation for ResolveConflictOperation {
    type Output = Merged;
    type Error = GitError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::GitRepository(self.id))
    }

    fn first(&mut self) -> MergeTurn {
        let run = open(&self.node, &self.auth, self.id, Permission::WRITE);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> MergeTurn {
        if self.opened {
            return Ok(Next::Done(result.child::<Merged, GitError>()??));
        }
        self.opened = true;
        let (document, projection) = result.child::<Opened, GitError>()??;
        let conflict = self.conflict;
        let (name, kept) = (projection.state.refs.iter())
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
            expected: self.expected.take(),
        };
        let scope = MergeScope {
            node: self.node.clone(),
            auth: self.auth.clone(),
            document,
            projection,
        };
        let run = merge_into(scope, kept.version, &branch, options, vec![discard]);
        Ok(Next::Call(IoCall::child(run)))
    }
}

#[cfg(test)]
#[path = "merge_tests.rs"]
mod tests;
