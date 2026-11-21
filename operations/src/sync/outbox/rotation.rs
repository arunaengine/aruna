//! Keeps one rotation of the bounded document sync drain and decides when the drain runs again:
//! after a failure on the retry ladder, and after a closed rotation from what it left behind.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::time::Duration;

use aruna_core::id::NodeId;
use irokle::TopicId;
use tracing::info;

use super::defer::SyncWaits;
use crate::queue_backoff::retry_delay_ms;
use crate::sync::shard_placement::DEFER_RETRY_AFTER;

/// Outcome counts accumulated across the invocations of one rotation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RotationTotals {
    pub examined: usize,
    pub deleted: usize,
    pub deferred: usize,
    pub undeliverable: usize,
    pub retry_invocations: usize,
    pub invocations: u32,
}

/// One rotation of the bounded document sync drain: head to observed end across as many bounded
/// invocations as it needs. Ordering blocks and outcome counts carry across them; holdership and
/// publisher authority never do.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OutboxRotation {
    /// Last key observed when this rotation opened.
    pub boundary: Option<Vec<u8>>,
    /// Last key observed in each independently ordered event-kind stream.
    pub stream_boundaries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Where the next invocation resumes; `None` is the head.
    pub cursor: Option<Vec<u8>>,
    /// Topics whose FIFO is blocked for the rest of this rotation.
    pub blocked_topics: HashSet<TopicId>,
    /// Admin origin streams blocked for the rest of this rotation.
    pub blocked_origins: HashSet<NodeId>,
    /// Topics this node can never publish onto, for the rest of this rotation.
    pub undeliverable_topics: HashSet<TopicId>,
    /// Admin origins whose published records must sync again, by the topics they ride.
    pub sync_waits: SyncWaits,
    pub totals: RotationTotals,
    /// Continuations already spent without yielding through the timer.
    pub continuations: u32,
    /// Failed or unfinished drains since the last progress; it survives a closed rotation.
    pub retry_attempts: u32,
}

impl OutboxRotation {
    pub fn admits(&self, key: &[u8]) -> bool {
        if !self.stream_boundaries.is_empty() {
            return self
                .stream_boundaries
                .iter()
                .find(|(prefix, _)| key.starts_with(prefix))
                .is_some_and(|(_, boundary)| key <= boundary.as_slice());
        }
        self.boundary
            .as_deref()
            .is_none_or(|boundary| key <= boundary)
    }

    pub fn at_end(&self, cursor: Option<&[u8]>) -> bool {
        cursor.is_some_and(|cursor| {
            self.boundary
                .as_deref()
                .is_some_and(|boundary| cursor >= boundary)
        })
    }

    /// Ends this rotation and answers its totals; the retry ladder stays.
    pub fn close(&mut self) -> RotationTotals {
        let totals = self.totals;
        let retry_attempts = self.retry_attempts;
        *self = Self {
            retry_attempts,
            ..Self::default()
        };
        totals
    }

    /// The wait before the next drain after a failed or unfinished one, on the retry ladder;
    /// counts the attempt.
    pub fn retry_after(&mut self) -> Duration {
        let after = Duration::from_millis(retry_delay_ms(self.retry_attempts));
        self.retry_attempts = self.retry_attempts.saturating_add(1);
        after
    }

    /// Closes this rotation and decides the next drain from what it left behind: retried pages
    /// run again on the ladder, deferred records after the defer delay, a clean rotation waits.
    /// Deleted records are progress and restart the ladder.
    pub fn close_plan(&mut self) -> (RotationTotals, Option<Duration>) {
        let closed = self.close();
        let pending = closed.retry_invocations > 0 || closed.deferred > 0;
        if closed.deleted > 0 || !pending {
            self.retry_attempts = 0;
        }
        let after = match (closed.retry_invocations, closed.deferred) {
            (0, 0) => None,
            (0, _) => Some(DEFER_RETRY_AFTER),
            _ => Some(self.retry_after()),
        };
        (closed, after)
    }

    /// Closes this rotation as [`Self::close_plan`] does and logs what it examined; answers when
    /// the drain runs again.
    pub fn close_logged(&mut self) -> Option<Duration> {
        let (closed, after) = self.close_plan();
        if closed.examined > 0 {
            info!(
                event = "pipeline.drain.rotation",
                examined = closed.examined,
                deleted = closed.deleted,
                deferred = closed.deferred,
                undeliverable = closed.undeliverable,
                retry_invocations = closed.retry_invocations,
                invocations = closed.invocations,
                "Document sync outbox rotation complete"
            );
        }
        after
    }

    /// After an invocation that must retry: a rotation that reached its end closes, any other
    /// waits on the retry ladder. Either way the continuation streak starts again.
    pub fn finish_retry(&mut self, reached_end: bool) -> Option<Duration> {
        self.continuations = 0;
        match reached_end {
            true => self.close_logged(),
            false => Some(self.retry_after()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn closing(totals: RotationTotals, retry_attempts: u32) -> OutboxRotation {
        OutboxRotation {
            totals,
            retry_attempts,
            ..OutboxRotation::default()
        }
    }

    // Retried pages climb the ladder, deleted records restart it, deferred ones wait for the
    // defer delay and a clean rotation waits for new records.
    #[test]
    fn closed_rotation_plans() {
        let retried = RotationTotals {
            retry_invocations: 1,
            ..RotationTotals::default()
        };
        let mut rotation = closing(retried, 3);
        let after = Some(Duration::from_millis(retry_delay_ms(3)));
        assert_eq!(rotation.close_plan(), (retried, after));
        assert_eq!(rotation.retry_attempts, 4);
        let progressed = RotationTotals {
            deleted: 2,
            ..retried
        };
        let mut rotation = closing(progressed, 3);
        let after = Some(Duration::from_millis(retry_delay_ms(0)));
        assert_eq!(rotation.close_plan(), (progressed, after));
        let deferred = RotationTotals {
            deferred: 2,
            ..RotationTotals::default()
        };
        let mut rotation = closing(deferred, 3);
        assert_eq!(rotation.close_plan().1, Some(DEFER_RETRY_AFTER));
        assert_eq!(rotation.retry_attempts, 3, "no progress keeps the ladder");
        let mut rotation = closing(RotationTotals::default(), 3);
        assert_eq!(rotation.close_plan().1, None);
        assert_eq!(rotation.retry_attempts, 0);
    }

    // A retry that reached the rotation's end closes it, so progress in a clean suffix restarts
    // the ladder; a retry in the middle keeps the cursor and blocks and climbs the ladder.
    #[test]
    fn retry_suffix_closes() {
        let totals = RotationTotals {
            deleted: 1,
            retry_invocations: 1,
            invocations: 2,
            ..RotationTotals::default()
        };
        let mut rotation = OutboxRotation {
            boundary: Some(b"last".to_vec()),
            cursor: Some(b"first".to_vec()),
            continuations: 4,
            ..closing(totals, 0)
        };
        let after = Some(Duration::from_millis(retry_delay_ms(0)));
        assert_eq!(rotation.finish_retry(true), after);
        assert!(rotation.boundary.is_none() && rotation.cursor.is_none());
        assert_eq!((rotation.retry_attempts, rotation.continuations), (1, 0));

        let topic = TopicId::hash(b"midpoint-retry-topic");
        let mut rotation = OutboxRotation {
            boundary: Some(b"last".to_vec()),
            cursor: Some(b"middle".to_vec()),
            ..closing(RotationTotals::default(), 3)
        };
        rotation.blocked_topics.insert(topic);
        let after = Some(Duration::from_millis(retry_delay_ms(3)));
        assert_eq!(rotation.finish_retry(false), after);
        assert_eq!(rotation.cursor.as_deref(), Some(b"middle".as_slice()));
        assert!(rotation.blocked_topics.contains(&topic));
        assert_eq!(rotation.retry_attempts, 4);
    }
}
