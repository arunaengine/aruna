//! Builds the Git store calls of the repository operations and reads their answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::git::{GitAnswer, GitCall, GitEffect, GitEvent};
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::operation::{Operation, Runnable, boxed_suboperation};
use bytes::Bytes;
use ulid::Ulid;

use super::GitError;
use super::objects::Blob;

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

/// Reads `blob` to its end, stopping once it holds more than `limit` bytes.
pub(super) fn collect_call(blob: Blob, limit: u64) -> IoCall {
    store_call(GitCall::Collect { blob, limit })
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

/// Whether a [`configured_call`] reached a Git store; a node without one answers `Failed`.
pub(super) fn has_store(result: IoResult) -> Result<bool, GitError> {
    Ok(!matches!(answer(result)?, GitAnswer::Failed(_)))
}

/// The bytes of a [`collect_call`].
pub(super) fn collected(result: IoResult) -> Result<Bytes, GitError> {
    match answer(result)? {
        GitAnswer::Collected(bytes) => Ok(bytes),
        _ => Err(GitError::Unavailable),
    }
}

pub(super) fn metadata_call(effect: MetadataEffect) -> IoCall {
    IoCall::from(Effect::Metadata(effect))
}

pub(super) fn metadata_event(result: IoResult) -> Result<MetadataEvent, GitError> {
    match result.event()? {
        Event::Metadata(event) => Ok(event),
        _ => Err(GitError::Unavailable),
    }
}

/// One group member that runs `operation` as a child.
pub(super) fn child_member<O, K>(operation: O) -> Effect
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: PartialEq + 'static,
    O::Error: PartialEq + 'static,
{
    Effect::SubOperation(boxed_suboperation(operation, |result| {
        Event::Child(ChildOutcome::new(result))
    }))
}

/// Runs `members` at the same time, at most `limit` at once; results keep their order.
pub(super) fn group_call(members: Vec<Effect>, limit: usize, wait: Option<Duration>) -> IoCall {
    IoCall::Group(EffectGroup {
        members,
        limit,
        form: GroupForm::All { deadline: wait },
    })
}

/// The member results of a [`group_call`].
pub(super) fn group_results(result: IoResult) -> Result<Vec<GroupResult>, GitError> {
    match result {
        IoResult::Group(GroupEvent::Finished { results }) => Ok(results),
        _ => Err(GitError::Unavailable),
    }
}

/// The outcome of one [`child_member`]; `None` when it did not finish.
pub(super) fn member_outcome<T: 'static, E: 'static>(member: GroupResult) -> Option<Result<T, E>> {
    let GroupResult::Done(event) = member else {
        return None;
    };
    match *event {
        Event::Child(outcome) => outcome.take(),
        _ => None,
    }
}

/// One store effect whose event `read` turns into the output.
pub(super) fn store_read<T>(
    effect: GitEffect,
    actor: UserId,
    read: fn(GitEvent) -> Result<T, GitError>,
) -> StoreReadOperation<T> {
    StoreReadOperation {
        effect: Some(effect),
        actor,
        read,
    }
}

pub(super) struct StoreReadOperation<T> {
    effect: Option<GitEffect>,
    actor: UserId,
    read: fn(GitEvent) -> Result<T, GitError>,
}

impl<T> std::fmt::Debug for StoreReadOperation<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (formatter.debug_struct("StoreReadOperation"))
            .field("effect", &self.effect)
            .field("actor", &self.actor)
            .finish_non_exhaustive()
    }
}

impl<T> PartialEq for StoreReadOperation<T> {
    fn eq(&self, other: &Self) -> bool {
        self.effect == other.effect && self.actor == other.actor
    }
}

impl<T> Operation for StoreReadOperation<T>
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
{
    type Output = T;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<T>, GitError> {
        let effect = self.effect.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(git_call(effect, self.actor)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<T>, GitError> {
        Ok(Next::Done((self.read)(git_event(result)?)?))
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{answered, auth, ran};
    use super::*;

    fn resolved(event: GitEvent) -> Result<String, GitError> {
        match event {
            GitEvent::Resolved(Some(commit)) => Ok(commit),
            _ => Err(GitError::NotFound),
        }
    }

    // One store effect answers through the given reader; a failed run is no answer.
    #[test]
    fn read_maps_event() {
        let effect = GitEffect::Refs(Ulid::from_bytes([1; 16]));
        let mut read = store_read(effect, auth().user_id, resolved);
        assert!(matches!(read.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let found = ran(GitEvent::Resolved(Some("a".to_string())));
        assert_eq!(read.next(found), Ok(Next::Done("a".to_string())));
        let failed = answered(GitAnswer::Failed("gone".to_string()));
        assert_eq!(read.next(failed), Err(GitError::Unavailable));
    }

    // Only a failed answer means that this node runs no Git store.
    #[test]
    fn store_presence_read() {
        let failed = answered(GitAnswer::Failed("no store".to_string()));
        assert_eq!(has_store(failed), Ok(false));
        assert_eq!(has_store(answered(GitAnswer::Configured(false))), Ok(true));
    }

    // A member that did not finish has no outcome.
    #[test]
    fn unfinished_member_empty() {
        let outcome = member_outcome::<(), GitError>(GroupResult::Unfinished);
        assert_eq!(outcome, None);
    }
}
