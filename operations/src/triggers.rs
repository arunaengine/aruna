//! Names the operation that answers a timer key or a peer request frame.
//! The trigger shells ask this table first and run what it returns; a key or frame it does not
//! name is still handled by the shell's own unconverted code.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::task::TaskKey;
use aruna_core::types::Effects;
use thiserror::Error;

use crate::notifications::outbox::{DELIVERY_RETRY_AFTER, DrainOutboxOperation};
use crate::notifications::prune::{
    NOTIFICATION_RETRY_AFTER, PRUNE_PAGE_SIZE, PruneNotificationsOperation,
};

/// What the timer shell knows when a timer fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerInput {
    pub now_ms: u64,
    /// This node, `None` on a node without a net transport.
    pub local_node: Option<NodeId>,
}

/// A timer run that failed; the shell logs it and runs the timer again after `retry_after`.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("{reason}")]
pub struct TimerFailure {
    pub retry_after: Duration,
    pub reason: String,
}

/// A timer operation decides when its timer runs again.
pub trait TimerRun: Operation + Sized {
    /// The delay before the next run, or `None` when the timer stays off until work arrives.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<Option<Duration>, TimerFailure>;
}

/// The operation of one timer run. Its output is when the timer runs again; the shell persists
/// and arms that timer.
#[derive(Debug, PartialEq)]
pub enum TimerOperation {
    DrainNotificationOutbox(DrainOutboxOperation),
    PruneNotifications(PruneNotificationsOperation),
}

/// The operation that runs when `key` fires, or `None` while the key is not converted.
pub fn timer_operation(key: &TaskKey, input: &TimerInput) -> Option<TimerOperation> {
    match key {
        TaskKey::DrainNotificationOutbox => Some(TimerOperation::DrainNotificationOutbox(
            DrainOutboxOperation::new(input.local_node, input.now_ms),
        )),
        TaskKey::PruneNotifications => Some(TimerOperation::PruneNotifications(
            PruneNotificationsOperation::new(input.now_ms, PRUNE_PAGE_SIZE),
        )),
        _ => None,
    }
}

impl Operation for TimerOperation {
    type Output = Option<Duration>;
    type Error = TimerFailure;

    fn start(&mut self) -> Effects {
        match self {
            TimerOperation::DrainNotificationOutbox(operation) => operation.start(),
            TimerOperation::PruneNotifications(operation) => operation.start(),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match self {
            TimerOperation::DrainNotificationOutbox(operation) => operation.step(event),
            TimerOperation::PruneNotifications(operation) => operation.step(event),
        }
    }

    fn is_complete(&self) -> bool {
        match self {
            TimerOperation::DrainNotificationOutbox(operation) => operation.is_complete(),
            TimerOperation::PruneNotifications(operation) => operation.is_complete(),
        }
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self {
            TimerOperation::DrainNotificationOutbox(operation) => {
                DrainOutboxOperation::rearm(operation.finalize())
            }
            TimerOperation::PruneNotifications(operation) => {
                PruneNotificationsOperation::rearm(operation.finalize())
            }
        }
    }

    fn abort(&mut self) -> Effects {
        match self {
            TimerOperation::DrainNotificationOutbox(operation) => operation.abort(),
            TimerOperation::PruneNotifications(operation) => operation.abort(),
        }
    }
}

impl TimerRun for DrainOutboxOperation {
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<Option<Duration>, TimerFailure> {
        result.map_err(|error| TimerFailure {
            retry_after: DELIVERY_RETRY_AFTER,
            reason: error.to_string(),
        })
    }
}

impl TimerRun for PruneNotificationsOperation {
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<Option<Duration>, TimerFailure> {
        match result {
            Ok(outcome) => Ok(Some(outcome.rearm())),
            Err(error) => Err(TimerFailure {
                retry_after: NOTIFICATION_RETRY_AFTER,
                reason: error.to_string(),
            }),
        }
    }
}

/// One request frame read by the peer shell, with what the shell knows about its stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerInput<'a> {
    pub alpn: Alpn,
    pub peer: NodeId,
    pub local_node: NodeId,
    pub now_ms: u64,
    /// The frame body without its length prefix, at most the protocol's frame bound.
    pub frame: &'a [u8],
}

/// A peer request that ended without an answer; the shell logs it and closes the stream.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct PeerFailure(pub String);

/// A peer operation produces the answer frame body the shell writes back.
pub trait PeerRun: Operation + Sized {
    fn answer(result: Result<Self::Output, Self::Error>) -> Result<Vec<u8>, PeerFailure>;
}

/// The operation that answers one peer request frame.
#[derive(Debug, PartialEq)]
pub enum PeerOperation {}

/// The operation that answers `input`, or `None` while its protocol or request kind is not
/// converted; the shell then answers the frame with its own code.
pub fn peer_operation(input: PeerInput<'_>) -> Option<PeerOperation> {
    match input.alpn {
        Alpn::Dht
        | Alpn::Bao
        | Alpn::DocumentSync
        | Alpn::Metadata
        | Alpn::NativeReference
        | Alpn::Notification
        | Alpn::Shard
        | Alpn::JobControl
        | Alpn::Heartbeat => None,
    }
}

impl Operation for PeerOperation {
    type Output = Vec<u8>;
    type Error = PeerFailure;

    fn start(&mut self) -> Effects {
        match *self {}
    }

    fn step(&mut self, _event: Event) -> Effects {
        match *self {}
    }

    fn is_complete(&self) -> bool {
        match *self {}
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self {}
    }

    fn abort(&mut self) -> Effects {
        match *self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::StorageEvent;
    use aruna_core::operation_check::{check_trace, rejects_finalize};

    const INPUT: TimerInput = TimerInput {
        now_ms: 1_000,
        local_node: None,
    };

    #[test]
    fn unconverted_keys_absent() {
        assert!(timer_operation(&TaskKey::DrainJobQueue, &INPUT).is_none());
        assert!(timer_operation(&TaskKey::PublishWatchInterest, &INPUT).is_none());
    }

    #[test]
    fn prune_key_named() {
        let operation = timer_operation(&TaskKey::PruneNotifications, &INPUT);
        assert_eq!(
            operation,
            Some(TimerOperation::PruneNotifications(
                PruneNotificationsOperation::new(1_000, PRUNE_PAGE_SIZE)
            ))
        );
    }

    #[test]
    fn timer_delegates_steps() {
        let mut operation = timer_operation(&TaskKey::PruneNotifications, &INPUT).unwrap();
        let mut inner = PruneNotificationsOperation::new(1_000, PRUNE_PAGE_SIZE);
        let first = inner.start();
        check_trace(&mut operation, first.as_slice(), Vec::new());
        assert!(matches!(
            first.as_slice(),
            [Effect::Storage(StorageEffect::Iter { .. })]
        ));
    }

    #[test]
    fn timer_failure_rearms() {
        let mut operation = timer_operation(&TaskKey::PruneNotifications, &INPUT).unwrap();
        operation.start();
        operation.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::Timeout,
        }));
        assert!(operation.is_complete());
        let failure = operation.finalize().unwrap_err();
        assert_eq!(failure.retry_after, NOTIFICATION_RETRY_AFTER);
    }

    #[test]
    fn unfinished_timer_fails() {
        let mut operation = timer_operation(&TaskKey::PruneNotifications, &INPUT).unwrap();
        operation.start();
        rejects_finalize(operation);
    }

    #[test]
    fn timer_abort_delegates() {
        let mut operation = timer_operation(&TaskKey::PruneNotifications, &INPUT).unwrap();
        operation.start();
        assert!(operation.abort().is_empty());
    }

    #[test]
    fn unconverted_peers_absent() {
        let local_node = crate::tests::locations::node_id(1);
        for alpn in Alpn::ALL {
            let input = PeerInput {
                alpn,
                peer: local_node,
                local_node,
                now_ms: 1_000,
                frame: &[],
            };
            assert!(peer_operation(input).is_none());
        }
    }
}
