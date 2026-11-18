//! Builds the Git store calls of the repository operations and reads their answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::git::{GitAnswer, GitCall, GitEffect, GitEvent};
use ulid::Ulid;

use super::GitError;
use crate::driver::DriverContext;

/// This node, which repository operations take from their caller's context.
pub(super) fn node_of(context: &DriverContext) -> Option<NodeId> {
    context.net_handle.as_ref().map(|net| net.node_id())
}

fn store_call(call: GitCall) -> IoCall {
    IoCall::from(Effect::Git(Box::new(call)))
}

/// Runs a repository effect for `actor`.
pub(super) fn git_call(effect: GitEffect, actor: UserId) -> IoCall {
    store_call(GitCall::Run { effect, actor })
}

/// Asks whether the local repository still has its files and settings.
pub(super) fn configured_call(document_id: Ulid) -> IoCall {
    store_call(GitCall::Configured(document_id))
}

/// Asks whether another run holds the document's repository.
pub(super) fn busy_call(document_id: Ulid) -> IoCall {
    store_call(GitCall::Busy(document_id))
}

fn answer(result: IoResult) -> Result<GitAnswer, GitError> {
    match result.event()? {
        Event::Git(answer) => Ok(*answer),
        _ => Err(GitError::Unavailable),
    }
}

/// The event of a [`git_call`].
pub(super) fn git_event(result: IoResult) -> Result<GitEvent, GitError> {
    match answer(result)? {
        GitAnswer::Ran(event) => Ok(event),
        _ => Err(GitError::Unavailable),
    }
}

/// The answer of a [`configured_call`] or a [`busy_call`].
pub(super) fn git_flag(result: IoResult) -> Result<bool, GitError> {
    match answer(result)? {
        GitAnswer::Configured(flag) | GitAnswer::Busy(flag) => Ok(flag),
        _ => Err(GitError::Unavailable),
    }
}
