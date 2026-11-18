//! Builds the Git store calls of the repository operations and reads their answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::git::{GitAnswer, GitCall, GitEffect, GitEvent, GitRequest, PushProof, Refs};
use aruna_core::git_state::{Ancestry, Projection, ProjectionInputs};
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::operation::{Operation, Runnable, SubOperation, boxed_suboperation};
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

/// `operation` as a child whose outcome comes back in [`Event::Child`].
fn boxed_child<O, K>(operation: O) -> Box<dyn SubOperation>
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: PartialEq + 'static,
    O::Error: PartialEq + 'static,
{
    boxed_suboperation(operation, |result| Event::Child(ChildOutcome::new(result)))
}

/// One group member that runs `operation` as a child.
pub(super) fn child_member<O, K>(operation: O) -> Effect
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: PartialEq + 'static,
    O::Error: PartialEq + 'static,
{
    Effect::SubOperation(boxed_child(operation))
}

/// Runs a receive-pack request under a push key that lasts as long as the run.
pub(super) fn push_call(request: GitRequest, actor: UserId) -> IoCall {
    store_call(GitCall::Push {
        request: Box::new(request),
        actor,
    })
}

pub(super) fn push_key_call(document_id: Ulid, proof: &PushProof) -> IoCall {
    store_call(GitCall::PushKeyValid {
        document_id,
        proof: proof.clone(),
    })
}

pub(super) fn push_key_answer(result: IoResult) -> Result<bool, GitError> {
    match answer(result)? {
        GitAnswer::PushKeyValid(valid) => Ok(valid),
        _ => Err(GitError::Unavailable),
    }
}

pub(super) fn recent_call(document_id: Ulid, inputs: ProjectionInputs) -> IoCall {
    store_call(GitCall::Recent {
        document_id,
        inputs,
    })
}

pub(super) fn last_call(document_id: Ulid) -> IoCall {
    store_call(GitCall::Last(document_id))
}

/// The answer of a [`recent_call`] or a [`last_call`].
pub(super) fn kept_projection(result: IoResult) -> Result<Option<Projection>, GitError> {
    match answer(result)? {
        GitAnswer::Projection(kept) => Ok(kept.map(|projection| *projection)),
        _ => Err(GitError::Unavailable),
    }
}

pub(super) fn remember_call(
    document_id: Ulid,
    inputs: ProjectionInputs,
    projection: &Projection,
) -> IoCall {
    store_call(GitCall::Remember {
        document_id,
        inputs,
        projection: Box::new(projection.clone()),
    })
}

pub(super) fn forget_call(document_id: Ulid) -> IoCall {
    store_call(GitCall::Forget(document_id))
}

/// Notes a stored record or pack; `forget` also drops the remembered projection.
pub(super) fn written_call(document_id: Ulid, forget: bool) -> IoCall {
    store_call(GitCall::RecordStored {
        document_id,
        forget,
    })
}

/// The answer of a call that only updates what the store keeps.
pub(super) fn noted(result: IoResult) -> Result<(), GitError> {
    match answer(result)? {
        GitAnswer::Noted => Ok(()),
        _ => Err(GitError::Unavailable),
    }
}

pub(super) fn ancestry_call(document_id: Ulid) -> IoCall {
    store_call(GitCall::KnownAncestry(document_id))
}

pub(super) fn ancestry_answer(result: IoResult) -> Result<Ancestry, GitError> {
    match answer(result)? {
        GitAnswer::Ancestry(ancestry) => Ok(ancestry),
        _ => Err(GitError::Unavailable),
    }
}

pub(super) fn projected_call(document_id: Ulid, ancestry: Ancestry, refs: Refs) -> IoCall {
    store_call(GitCall::Projected {
        document_id,
        ancestry,
        refs,
    })
}

pub(super) fn served_call(document_id: Ulid) -> IoCall {
    store_call(GitCall::ServedRefs(document_id))
}

pub(super) fn served_answer(result: IoResult) -> Result<Option<Refs>, GitError> {
    match answer(result)? {
        GitAnswer::ServedRefs(refs) => Ok(refs),
        _ => Err(GitError::Unavailable),
    }
}

pub(super) fn writes_call(document_id: Ulid) -> IoCall {
    store_call(GitCall::RecordWrites(document_id))
}

pub(super) fn writes_answer(result: IoResult) -> Result<u64, GitError> {
    match answer(result)? {
        GitAnswer::RecordWrites(writes) => Ok(writes),
        _ => Err(GitError::Unavailable),
    }
}

pub(super) fn sequence_call() -> IoCall {
    store_call(GitCall::NextSequence)
}

pub(super) fn sequence_answer(result: IoResult) -> Result<Ulid, GitError> {
    match answer(result)? {
        GitAnswer::Sequence(sequence) => Ok(sequence),
        _ => Err(GitError::Unavailable),
    }
}

/// Starts `child` beside the caller unless a refresh of the document already runs.
pub(super) fn refresh_once_call(document_id: Ulid, child: Box<dyn SubOperation>) -> IoCall {
    store_call(GitCall::RefreshOnce { document_id, child })
}

/// Runs `operation` under its own stage timers.
pub(super) fn timed_call<O, K>(operation: O) -> IoCall
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: PartialEq + 'static,
    O::Error: PartialEq + 'static,
{
    store_call(GitCall::Timed(boxed_child(operation)))
}

/// The outcome of a [`timed_call`], if the child finished, and its rendered stages.
pub(super) fn timed_outcome<T: 'static, E: 'static>(
    result: IoResult,
) -> Result<(Option<Result<T, E>>, String), GitError> {
    let GitAnswer::Timed { event, stages } = answer(result)? else {
        return Err(GitError::Unavailable);
    };
    let outcome = match *event {
        Event::Child(outcome) => outcome.take(),
        _ => None,
    };
    Ok((outcome, stages))
}

pub(super) fn canonical_call(jsonld: String) -> IoCall {
    store_call(GitCall::Canonicalize(jsonld))
}

/// The digest of a [`canonical_call`]; `None` when the graph cannot be canonicalized.
pub(super) fn canonical_answer(result: IoResult) -> Result<Option<[u8; 32]>, GitError> {
    match answer(result)? {
        GitAnswer::Canonical(digest) => Ok(digest),
        _ => Err(GitError::Unavailable),
    }
}

/// Times request stages from the runner's stamps and reports them when the operation ends.
#[derive(Debug, Default, PartialEq)]
pub(super) struct StageTimes {
    now_ms: u64,
    running: Option<(&'static str, u64)>,
    finished: Vec<(&'static str, u64)>,
}

impl StageTimes {
    pub(super) fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    pub(super) fn now_ms(&self) -> u64 {
        self.now_ms
    }

    pub(super) fn start(&mut self, name: &'static str) {
        self.running = Some((name, self.now_ms));
    }

    /// Ends the running stage; call it first when the stage's answer arrives.
    pub(super) fn stop(&mut self) {
        if let Some((name, started)) = self.running.take() {
            let elapsed = self.now_ms.saturating_sub(started);
            self.finished.push((name, elapsed));
        }
    }

    /// The call that reports the finished stages, if there are any.
    pub(super) fn report(&mut self) -> Vec<IoCall> {
        self.stop();
        let finished = std::mem::take(&mut self.finished);
        match finished.is_empty() {
            true => Vec::new(),
            false => vec![store_call(GitCall::Stages(finished))],
        }
    }

    /// Ends with `output`, then makes `calls` and reports the stages.
    pub(super) fn finish<T>(&mut self, output: T, mut calls: Vec<IoCall>) -> Next<T> {
        calls.extend(self.report());
        match calls.is_empty() {
            true => Next::Done(output),
            false => Next::Then { output, calls },
        }
    }
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
    use std::mem::discriminant;

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

    // A stage runs from its call to its answer on the runner's clock.
    #[test]
    fn stage_uses_stamps() {
        let mut stages = StageTimes::default();
        stages.stamp(100);
        stages.start("git_records");
        stages.stamp(130);
        stages.stop();
        assert_eq!(stages.finished, vec![("git_records", 30)]);
        let Next::Then { output, calls } = stages.finish(1, Vec::new()) else {
            panic!("expected the report");
        };
        assert_eq!(output, 1);
        assert_eq!(calls.len(), 1);
    }

    // Nothing timed reports nothing.
    #[test]
    fn empty_stages_silent() {
        let finish = StageTimes::default().finish(1, Vec::new());
        assert_eq!(discriminant(&finish), discriminant(&Next::Done(1)));
    }
}
