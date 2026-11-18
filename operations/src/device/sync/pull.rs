//! Asks a realm node to pull one device version and polls the pull until it ends.
//! A pull is judged by the bytes it receives, never by how long it takes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::{SyncPull, SyncPullAck, SyncPullStatus, SyncRefusal};
use tracing::warn;

use crate::distribution::forward::MemberRefusal;
use crate::distribution::peer_call::{peer_answer, peer_call};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::transport_message_kind;

/// Delay before the first poll; it doubles up to [`PULL_POLL_AFTER`], so a small file settles
/// within a few quick polls.
const FIRST_POLL_AFTER: Duration = Duration::from_millis(50);

/// Delay between two polls of a running pull.
const PULL_POLL_AFTER: Duration = Duration::from_secs(1);

/// Polls in a row at the full delay a running pull may report no new bytes before it counts as
/// stalled.
const STALL_POLLS: u32 = 10;

/// Times a pull follows a refusal that names the coordinator of its key before it gives up.
const MAX_MOVES: u32 = 2;

/// What a pull asks of the operation that runs it.
#[derive(Debug, PartialEq)]
pub(crate) enum PullStep {
    /// Make this call and hand its answer to [`PullWalk::answered`].
    Ask(IoCall),
    /// The node holds this many bytes now; go on with [`PullWalk::wait`].
    Moved(u64),
    /// Wait this long, then go on with [`PullWalk::poll`].
    Wait(Duration),
    Ended(Result<SyncPullAck, SyncRefusal>),
}

/// One pull at a realm node, from the request to its end. A pull that keeps receiving bytes runs
/// as long as it needs; one that stops, or a node that cannot be reached, ends as `Unavailable`.
#[derive(Debug, PartialEq)]
pub(crate) struct PullWalk {
    node_id: NodeId,
    auth_token: AuthToken,
    pull: SyncPull,
    watch: Progress,
    moves: u32,
    /// The pending call is a request, whose failure ends the pull, and not a poll.
    requesting: bool,
    ask_again: bool,
    delay: Duration,
}

impl PullWalk {
    pub(crate) fn new(node_id: NodeId, auth_token: AuthToken, pull: SyncPull) -> Self {
        Self {
            node_id,
            auth_token,
            pull,
            watch: Progress::default(),
            moves: 0,
            requesting: true,
            ask_again: false,
            delay: Duration::ZERO,
        }
    }

    /// Asks the node for the pull.
    pub(crate) fn request(&mut self) -> PullStep {
        let message = MetadataTransportMessage::ForwardSyncPull {
            auth_token: self.auth_token.clone(),
            pull: Box::new(self.pull.clone()),
        };
        self.requesting = true;
        match peer_call(self.node_id, &message) {
            Ok(call) => PullStep::Ask(call),
            Err(reason) => self.request_failed(&reason),
        }
    }

    fn request_failed(&self, reason: &str) -> PullStep {
        let node = self.node_id;
        warn!(%node, %reason, "Could not ask the realm node for a pull");
        PullStep::Ended(Err(SyncRefusal::Unavailable))
    }

    /// Takes the answer to the pending call. A lost poll is one more poll without new bytes.
    pub(crate) fn answered(&mut self, result: IoResult) -> Result<PullStep, IoError> {
        let status = match peer_answer(result)? {
            Ok(MetadataTransportMessage::ForwardedSyncPull { status }) => Ok(status),
            Ok(other) => Err(transport_message_kind(&other).to_string()),
            Err(error) => Err(error),
        };
        Ok(match (status, self.requesting) {
            (Ok(status), _) => self.status(status),
            (Err(reason), true) => self.request_failed(&reason),
            (Err(_), false) => self.status(self.lost_poll()),
        })
    }

    fn lost_poll(&self) -> SyncPullStatus {
        SyncPullStatus::Pending {
            bytes_done: self.watch.seen,
            total: u64::MAX,
        }
    }

    fn status(&mut self, status: SyncPullStatus) -> PullStep {
        let (bytes_done, total, ask_again) = match status {
            SyncPullStatus::Done(ack) => return PullStep::Ended(Ok(ack)),
            // A distributed bucket's coordinator writes the key; the pull moves there.
            SyncPullStatus::Refused(SyncRefusal::Coordinator(coordinator))
                if self.moves < MAX_MOVES && coordinator != self.node_id =>
            {
                self.moves += 1;
                self.node_id = coordinator;
                self.watch = Progress::default();
                return self.request();
            }
            SyncPullStatus::Refused(refusal) => return PullStep::Ended(Err(refusal)),
            SyncPullStatus::Pending { bytes_done, total } => (bytes_done, total, false),
            // The node lost the pull, as after a restart; it is asked again.
            SyncPullStatus::Unknown => (self.watch.seen, u64::MAX, true),
        };
        let moved = bytes_done != self.watch.seen;
        let Some(delay) = self.watch.next_delay(bytes_done, total) else {
            let node = self.node_id;
            warn!(%node, bytes_done, "A synced-folder pull stopped receiving bytes");
            return PullStep::Ended(Err(SyncRefusal::Unavailable));
        };
        self.delay = delay;
        self.ask_again = ask_again;
        match moved {
            true => PullStep::Moved(bytes_done),
            false => self.wait(),
        }
    }

    /// The wait before the next poll.
    pub(crate) fn wait(&self) -> PullStep {
        PullStep::Wait(self.delay)
    }

    /// Asks how the pull stands, or asks for it again when the node lost it.
    pub(crate) fn poll(&mut self) -> PullStep {
        if self.ask_again {
            return self.request();
        }
        let message = MetadataTransportMessage::ForwardPullStatus {
            auth_token: self.auth_token.clone(),
            pull: Box::new(self.pull.clone()),
        };
        self.requesting = false;
        match peer_call(self.node_id, &message) {
            Ok(call) => PullStep::Ask(call),
            Err(_) => self.status(self.lost_poll()),
        }
    }
}

/// What the pending call of a forwarded pull answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ForwardStep {
    Answer,
    Delay,
}

/// One pull at `node_id` that nobody follows byte by byte.
#[derive(Debug, PartialEq)]
pub(crate) struct ForwardPullOperation {
    walk: PullWalk,
    step: ForwardStep,
}

impl ForwardPullOperation {
    pub(crate) fn new(node_id: NodeId, auth_token: AuthToken, pull: SyncPull) -> Self {
        Self {
            walk: PullWalk::new(node_id, auth_token, pull),
            step: ForwardStep::Answer,
        }
    }

    fn follow(&mut self, step: PullStep) -> Result<Next<SyncPullAck>, MemberRefusal> {
        match step {
            PullStep::Ask(call) => {
                self.step = ForwardStep::Answer;
                Ok(Next::Call(call))
            }
            PullStep::Moved(_) => {
                let wait = self.walk.wait();
                self.follow(wait)
            }
            PullStep::Wait(delay) => {
                self.step = ForwardStep::Delay;
                Ok(Next::Call(IoCall::Delay(delay)))
            }
            PullStep::Ended(end) => Ok(Next::Done(end?)),
        }
    }
}

impl Operation for ForwardPullOperation {
    type Output = SyncPullAck;
    type Error = MemberRefusal;

    fn first(&mut self) -> Result<Next<SyncPullAck>, MemberRefusal> {
        let step = self.walk.request();
        self.follow(step)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncPullAck>, MemberRefusal> {
        let step = match self.step {
            ForwardStep::Answer => self.walk.answered(result)?,
            ForwardStep::Delay => self.walk.poll(),
        };
        self.follow(step)
    }
}

/// What the polls of one pull have seen so far.
#[derive(Debug, PartialEq)]
struct Progress {
    seen: u64,
    stalls: u32,
    wait: Duration,
}

impl Default for Progress {
    fn default() -> Self {
        Self {
            seen: 0,
            stalls: 0,
            // Halved because each answer doubles it before the next poll.
            wait: FIRST_POLL_AFTER / 2,
        }
    }
}

impl Progress {
    /// Takes one answer and sets the next delay; true once the pull has stalled. Every byte in
    /// means the node is committing, which is progress too.
    fn stalled(&mut self, bytes_done: u64, total: u64) -> bool {
        let moved = bytes_done != self.seen || bytes_done >= total;
        let ramping = self.wait < PULL_POLL_AFTER;
        self.seen = bytes_done;
        self.wait = self.wait.saturating_mul(2).min(PULL_POLL_AFTER);
        self.stalls = match (moved, ramping) {
            (true, _) => 0,
            (false, true) => self.stalls,
            (false, false) => self.stalls.saturating_add(1),
        };
        self.stalls >= STALL_POLLS
    }

    /// Takes one answer and returns the delay before the next poll, or `None` once stalled.
    fn next_delay(&mut self, bytes_done: u64, total: u64) -> Option<Duration> {
        (!self.stalled(bytes_done, total)).then_some(self.wait)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A watch past its quick first polls.
    fn settled() -> Progress {
        let mut watch = Progress::default();
        while watch.wait < PULL_POLL_AFTER {
            watch.stalled(0, 100);
        }
        watch
    }

    #[test]
    fn follows_moving_pull() {
        // A slow pull that keeps receiving bytes is polled on for as long as it moves.
        let mut watch = settled();
        for bytes_done in 1..=3 * u64::from(STALL_POLLS) {
            assert!(!watch.stalled(bytes_done, 1_000));
        }
        assert_eq!(watch.wait, PULL_POLL_AFTER);
    }

    #[test]
    fn stops_stalled_pull() {
        // Only polls at the full delay that see no new bytes count toward a stall.
        let mut watch = Progress::default();
        let mut quick = 0;
        while watch.wait < PULL_POLL_AFTER {
            assert!(!watch.stalled(0, 100));
            quick += 1;
        }
        assert!(quick > 0);
        for _ in 1..STALL_POLLS {
            assert!(!watch.stalled(0, 100));
        }
        assert!(watch.stalled(0, 100));
    }

    #[test]
    fn counts_full_polls() {
        // The first poll comes after FIRST_POLL_AFTER; a stall needs STALL_POLLS full delays.
        let mut watch = Progress::default();
        let mut delays = Vec::new();
        while let Some(wait) = watch.next_delay(0, 100) {
            delays.push(wait);
        }
        assert_eq!(delays.first(), Some(&FIRST_POLL_AFTER));
        let full = delays
            .iter()
            .filter(|wait| **wait == PULL_POLL_AFTER)
            .count();
        assert_eq!(full, STALL_POLLS as usize);
    }

    #[test]
    fn waits_for_commit() {
        // A node that has every byte is committing and is never stalled.
        let mut watch = settled();
        for _ in 0..3 * STALL_POLLS {
            assert!(!watch.stalled(100, 100));
        }
    }
}
