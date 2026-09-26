//! Git LFS file locks as replicated claims; the earliest claim on a path wins everywhere.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::state::{Ancestry, reduce};
use super::{GitError, publish, records};
use crate::driver::DriverContext;
use aruna_core::git::{GitChange, LfsLock, valid_path};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use ulid::Ulid;

pub enum LockOutcome {
    Locked(LfsLock),
    /// Another user's claim on the path came first.
    Taken(LfsLock),
}

async fn current(context: &DriverContext, id: Ulid) -> Result<Vec<LfsLock>, GitError> {
    let (state, _) = reduce(&records::scan(context, id).await?, &Ancestry::new());
    Ok(state.locks.into_values().collect())
}

/// Claims `path`. A concurrent claim from another holder may still win once it replicates.
pub async fn create(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    path: String,
) -> Result<LockOutcome, GitError> {
    if !valid_path(&path) {
        return Err(GitError::Invalid);
    }
    let (document, _) = super::repository(context, auth, id, Permission::WRITE).await?;
    if let Some(lock) = current(context, id)
        .await?
        .into_iter()
        .find(|lock| lock.path == path)
    {
        return Ok(if lock.user_id == auth.user_id {
            LockOutcome::Locked(lock)
        } else {
            LockOutcome::Taken(lock)
        });
    }
    let claim = Ulid::generate();
    let change = GitChange::Lock {
        id: claim,
        path: path.clone(),
    };
    publish::publish(context, &document, auth.user_id, change).await?;
    let lock = current(context, id)
        .await?
        .into_iter()
        .find(|lock| lock.path == path)
        .ok_or(GitError::Unavailable)?;
    Ok(if lock.id == claim {
        LockOutcome::Locked(lock)
    } else {
        LockOutcome::Taken(lock)
    })
}

pub async fn list(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
) -> Result<Vec<LfsLock>, GitError> {
    super::repository(context, auth, id, Permission::READ).await?;
    current(context, id).await
}

/// Releases the caller's lock; WRITE on the document with `force` releases anyone's.
pub async fn unlock(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    lock_id: Ulid,
    force: bool,
) -> Result<LfsLock, GitError> {
    let (document, _) = super::repository(context, auth, id, Permission::WRITE).await?;
    let lock = current(context, id)
        .await?
        .into_iter()
        .find(|lock| lock.id == lock_id)
        .ok_or(GitError::NotFound)?;
    if lock.user_id != auth.user_id && !force {
        return Err(GitError::Locked(lock.path));
    }
    let change = GitChange::Unlock { id: lock_id };
    publish::publish(context, &document, auth.user_id, change).await?;
    Ok(lock)
}
