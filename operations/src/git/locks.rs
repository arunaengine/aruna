//! Git LFS file locks as replicated claims; the earliest claim on a path wins everywhere.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitChange, GitRecord, LfsLock, valid_path};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::access::{Repository, RepositoryOperation};
use super::publish::PublishRecordOperation;
use super::state::reduce;
use super::{GitError, records};
use aruna_core::git_state::Ancestry;

type Document = MetadataRegistryRecord;

#[derive(Debug, PartialEq)]
pub enum LockOutcome {
    Locked(LfsLock),
    /// Another user's claim on the path came first.
    Taken(LfsLock),
}

fn scan_call(id: Ulid) -> IoCall {
    IoCall::child(records::scan(id))
}

fn current(result: IoResult) -> Result<Vec<LfsLock>, GitError> {
    let records = result.child::<Vec<GitRecord>, GitError>()??;
    let (state, _) = reduce(&records, &Ancestry::new());
    Ok(state.locks.into_values().collect())
}

fn publish_call(
    node: Option<NodeId>,
    document: &Document,
    auth: &AuthContext,
    change: GitChange,
) -> IoCall {
    let extras = (None, Vec::new());
    let publish = PublishRecordOperation::new(node, document, auth.user_id, change, extras);
    IoCall::child(publish)
}

fn opening(open: &mut Option<RepositoryOperation>) -> Result<IoCall, IoError> {
    Ok(IoCall::child(open.take().ok_or(IoError::Unfinished)?))
}

/// What the pending call of claiming a path answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateStep {
    Open,
    Current,
    Publish,
    Settled,
}

/// Claims a path. A concurrent claim from another holder may still win once it replicates.
pub fn create(
    node: Option<NodeId>,
    auth: &AuthContext,
    id: Ulid,
    path: String,
) -> CreateLockOperation {
    CreateLockOperation {
        node,
        auth: auth.clone(),
        id,
        path,
        open: Some(RepositoryOperation::new(node, auth, id, Permission::WRITE)),
        step: CreateStep::Open,
        document: None,
        claim: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct CreateLockOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    id: Ulid,
    path: String,
    open: Option<RepositoryOperation>,
    step: CreateStep,
    document: Option<Document>,
    claim: Option<Ulid>,
}

type CreateTurn = Result<Next<LockOutcome>, GitError>;

impl CreateLockOperation {
    fn on_path(&self, locks: Vec<LfsLock>) -> Option<LfsLock> {
        locks.into_iter().find(|lock| lock.path == self.path)
    }
}

impl Operation for CreateLockOperation {
    type Output = LockOutcome;
    type Error = GitError;

    fn first(&mut self) -> CreateTurn {
        if !valid_path(&self.path) {
            return Err(GitError::Invalid);
        }
        Ok(Next::Call(opening(&mut self.open)?))
    }

    fn next(&mut self, result: IoResult) -> CreateTurn {
        match self.step {
            CreateStep::Open => {
                self.document = Some(result.child::<Repository, GitError>()??.0);
                self.step = CreateStep::Current;
                Ok(Next::Call(scan_call(self.id)))
            }
            CreateStep::Current => {
                if let Some(lock) = self.on_path(current(result)?) {
                    return Ok(Next::Done(match lock.user_id == self.auth.user_id {
                        true => LockOutcome::Locked(lock),
                        false => LockOutcome::Taken(lock),
                    }));
                }
                let document = self.document.as_ref().ok_or(IoError::Unfinished)?;
                let claim = Ulid::generate();
                let change = GitChange::Lock {
                    id: claim,
                    path: self.path.clone(),
                };
                let publish = publish_call(self.node, document, &self.auth, change);
                self.claim = Some(claim);
                self.step = CreateStep::Publish;
                Ok(Next::Call(publish))
            }
            CreateStep::Publish => {
                result.child::<GitRecord, GitError>()??;
                self.step = CreateStep::Settled;
                Ok(Next::Call(scan_call(self.id)))
            }
            CreateStep::Settled => {
                let lock = self
                    .on_path(current(result)?)
                    .ok_or(GitError::Unavailable)?;
                Ok(Next::Done(match Some(lock.id) == self.claim {
                    true => LockOutcome::Locked(lock),
                    false => LockOutcome::Taken(lock),
                }))
            }
        }
    }
}

/// The locks of a repository the caller may read.
pub fn list(node: Option<NodeId>, auth: &AuthContext, id: Ulid) -> ListLocksOperation {
    let open = RepositoryOperation::new(node, auth, id, Permission::READ);
    ListLocksOperation {
        id,
        open: Some(open),
        reading: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct ListLocksOperation {
    id: Ulid,
    open: Option<RepositoryOperation>,
    reading: bool,
}

impl Operation for ListLocksOperation {
    type Output = Vec<LfsLock>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Vec<LfsLock>>, GitError> {
        Ok(Next::Call(opening(&mut self.open)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<LfsLock>>, GitError> {
        if self.reading {
            return Ok(Next::Done(current(result)?));
        }
        result.child::<Repository, GitError>()??;
        self.reading = true;
        Ok(Next::Call(scan_call(self.id)))
    }
}

/// What the pending call of releasing a lock answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnlockStep {
    Open,
    Current,
    Publish,
}

/// Releases the caller's lock; WRITE on the document with `force` releases anyone's.
pub fn unlock(
    node: Option<NodeId>,
    auth: &AuthContext,
    id: Ulid,
    lock_id: Ulid,
    force: bool,
) -> UnlockOperation {
    UnlockOperation {
        node,
        auth: auth.clone(),
        id,
        lock_id,
        force,
        open: Some(RepositoryOperation::new(node, auth, id, Permission::WRITE)),
        step: UnlockStep::Open,
        document: None,
        lock: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct UnlockOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    id: Ulid,
    lock_id: Ulid,
    force: bool,
    open: Option<RepositoryOperation>,
    step: UnlockStep,
    document: Option<Document>,
    lock: Option<LfsLock>,
}

impl Operation for UnlockOperation {
    type Output = LfsLock;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<LfsLock>, GitError> {
        Ok(Next::Call(opening(&mut self.open)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LfsLock>, GitError> {
        match self.step {
            UnlockStep::Open => {
                self.document = Some(result.child::<Repository, GitError>()??.0);
                self.step = UnlockStep::Current;
                Ok(Next::Call(scan_call(self.id)))
            }
            UnlockStep::Current => {
                let locks = current(result)?.into_iter();
                let mut held = locks.filter(|lock| lock.id == self.lock_id);
                let lock = held.next().ok_or(GitError::NotFound)?;
                if lock.user_id != self.auth.user_id && !self.force {
                    return Err(GitError::Locked(lock.path));
                }
                let document = self.document.as_ref().ok_or(IoError::Unfinished)?;
                let change = GitChange::Unlock { id: self.lock_id };
                let publish = publish_call(self.node, document, &self.auth, change);
                self.lock = Some(lock);
                self.step = UnlockStep::Publish;
                Ok(Next::Call(publish))
            }
            UnlockStep::Publish => {
                result.child::<GitRecord, GitError>()??;
                Ok(Next::Done(self.lock.take().ok_or(IoError::Unfinished)?))
            }
        }
    }
}

#[cfg(test)]
#[path = "locks_tests.rs"]
mod tests;
