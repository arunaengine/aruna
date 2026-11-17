//! Names the operation that answers a timer key or a peer request frame. The trigger shells ask
//! this table first; a key or frame it does not name stays with the shell's unconverted code.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Display;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::alpn::Alpn;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::operation::{AfterConflict, HoldScope, Operation, TransactionMode};
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::TaskKey;
use thiserror::Error;

use crate::notifications::deliver::{DeliverBatchOperation, DeliverInput};
use crate::notifications::holder::HolderRead;
use crate::notifications::outbox::{DELIVERY_RETRY_AFTER, DrainOutboxOperation};
use crate::notifications::prune::{
    NOTIFICATION_RETRY_AFTER, PRUNE_PAGE_SIZE, PruneNotificationsOperation,
};
use crate::notifications::serve::{InboxRequest, ServeInboxOperation, WatchEventsOperation};
use crate::notifications::watch::digest::WATCH_PUBLISH_DEBOUNCE;
use crate::notifications::watch::interest::PublishInterestOperation;
use crate::notifications::watch::remove::RemoveWatchOperation;

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

/// When a timer runs again after the runner failed its run, for example on a storage error.
const TIMER_RETRY_AFTER: Duration = Duration::from_secs(30);

impl From<IoError> for TimerFailure {
    fn from(error: IoError) -> Self {
        Self {
            retry_after: TIMER_RETRY_AFTER,
            reason: error.to_string(),
        }
    }
}

/// A timer operation decides when its timer runs again.
pub trait TimerRun: Operation + Sized {
    /// The delay before the next run, or `None` when the timer stays off until work arrives.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<Option<Duration>, TimerFailure>;
}

/// The operation of one timer run. Its output is when the timer runs again; the shell persists
/// and arms that timer.
#[derive(Clone, Debug, PartialEq)]
pub enum TimerOperation {
    DrainNotificationOutbox(DrainOutboxOperation),
    PruneNotifications(PruneNotificationsOperation),
    PublishWatchInterest(PublishInterestOperation),
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
        TaskKey::PublishWatchInterest => Some(TimerOperation::PublishWatchInterest(
            PublishInterestOperation::new(input.local_node),
        )),
        _ => None,
    }
}

/// Runs `$body` with the operation of each `TimerOperation` variant bound to `$operation`.
macro_rules! each_timer {
    ($self:expr, $operation:ident => $body:expr) => {
        match $self {
            TimerOperation::DrainNotificationOutbox($operation) => $body,
            TimerOperation::PruneNotifications($operation) => $body,
            TimerOperation::PublishWatchInterest($operation) => $body,
        }
    };
}

/// The next call of a timer operation, or when its timer runs again.
fn timer_turn<O: TimerRun>(
    turn: Result<Next<O::Output>, O::Error>,
) -> Result<Next<Option<Duration>>, TimerFailure> {
    match turn {
        Ok(Next::Call(call)) => Ok(Next::Call(call)),
        Ok(Next::Wait) => Ok(Next::Wait),
        Ok(Next::Done(output)) => O::rearm(Ok(output)).map(Next::Done),
        Ok(Next::Then { output, calls }) => {
            O::rearm(Ok(output)).map(|output| Next::Then { output, calls })
        }
        Err(error) => O::rearm(Err(error)).map(Next::Done),
    }
}

/// The conflict decision of a timer operation; an output it finishes with is rearmed.
fn timer_conflict<O: TimerRun>(operation: &mut O, attempt: u32) -> AfterConflict<Option<Duration>> {
    match operation.on_conflict(attempt) {
        AfterConflict::Rerun => AfterConflict::Rerun,
        AfterConflict::Fail => AfterConflict::Fail,
        AfterConflict::Finish(output) => match O::rearm(Ok(output)) {
            Ok(after) => AfterConflict::Finish(after),
            Err(_) => AfterConflict::Fail,
        },
    }
}

impl Operation for TimerOperation {
    type Output = Option<Duration>;
    type Error = TimerFailure;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        each_timer!(self, operation => timer_first(operation))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        each_timer!(self, operation => timer_next(operation, result))
    }

    fn transaction(&self) -> TransactionMode {
        each_timer!(self, operation => operation.transaction())
    }

    fn abort(&mut self) -> Vec<IoCall> {
        each_timer!(self, operation => operation.abort())
    }

    fn stamp(&mut self, now_ms: u64) {
        each_timer!(self, operation => operation.stamp(now_ms))
    }

    fn after_commit(&mut self) -> Option<Effect> {
        each_timer!(self, operation => operation.after_commit())
    }

    fn hold_scope(&self) -> Option<HoldScope> {
        each_timer!(self, operation => operation.hold_scope())
    }

    fn on_conflict(&mut self, attempt: u32) -> AfterConflict<Self::Output> {
        each_timer!(self, operation => timer_conflict(operation, attempt))
    }
}

fn timer_first<O: TimerRun>(operation: &mut O) -> Result<Next<Option<Duration>>, TimerFailure> {
    timer_turn::<O>(operation.first())
}

fn timer_next<O: TimerRun>(
    operation: &mut O,
    result: IoResult,
) -> Result<Next<Option<Duration>>, TimerFailure> {
    timer_turn::<O>(operation.next(result))
}

impl TimerRun for DrainOutboxOperation {
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<Option<Duration>, TimerFailure> {
        result.map_err(|error| TimerFailure {
            retry_after: DELIVERY_RETRY_AFTER,
            reason: error.to_string(),
        })
    }
}

impl TimerRun for PublishInterestOperation {
    /// A published or skipped run waits for the next change; a failed one runs again soon.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<Option<Duration>, TimerFailure> {
        match result {
            Ok(_) => Ok(None),
            Err(error) => Err(TimerFailure {
                retry_after: WATCH_PUBLISH_DEBOUNCE,
                reason: error.to_string(),
            }),
        }
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
    pub local_realm: RealmId,
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

impl From<IoError> for PeerFailure {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

/// The operation that answers one peer request frame.
#[derive(Clone, Debug, PartialEq)]
pub enum PeerOperation {
    /// A notification batch delivered to the inbox this node holds.
    DeliverNotifications(DeliverBatchOperation),
    /// A watch deleted on the node that holds its owner's inbox.
    RemoveWatch(RemoveWatchOperation),
    /// An inbox read, mark or watch request for a user whose inbox this node holds.
    ServeInbox(ServeInboxOperation),
    /// Watch events expanded into the inboxes this node holds.
    WatchEvents(WatchEventsOperation),
}

/// The operation that answers `input`, or `None` while its protocol or request kind is not
/// converted; the shell then answers the frame with its own code.
pub fn peer_operation(input: PeerInput<'_>) -> Option<PeerOperation> {
    match input.alpn {
        Alpn::Notification => {
            let deliver = DeliverInput {
                peer: input.peer,
                local: input.local_node,
                local_realm: input.local_realm,
                now_ms: input.now_ms,
            };
            match postcard::from_bytes(input.frame) {
                Ok(NotificationTransportMessage::DeliverBatch { records }) => {
                    let operation = DeliverBatchOperation::new(deliver, records);
                    Some(PeerOperation::DeliverNotifications(operation))
                }
                Ok(NotificationTransportMessage::DeleteWatch { owner, watch_id }) => {
                    let operation = RemoveWatchOperation::new(deliver, owner, watch_id);
                    Some(PeerOperation::RemoveWatch(operation))
                }
                Ok(NotificationTransportMessage::DeliverWatchEvents { events }) => {
                    let operation = WatchEventsOperation::new(deliver, events);
                    Some(PeerOperation::WatchEvents(operation))
                }
                Ok(message) => {
                    let (user, request) = inbox_request(message, input.now_ms)?;
                    let operation = ServeInboxOperation::new(deliver, user, request);
                    Some(PeerOperation::ServeInbox(operation))
                }
                _ => None,
            }
        }
        Alpn::Dht
        | Alpn::Bao
        | Alpn::DocumentSync
        | Alpn::Metadata
        | Alpn::NativeReference
        | Alpn::Shard
        | Alpn::JobControl
        | Alpn::Heartbeat => None,
    }
}

/// The answer frame of a notification peer request; an error closes the stream.
fn notification_answer<E: Display>(
    result: Result<NotificationTransportMessage, E>,
) -> Result<Vec<u8>, PeerFailure> {
    let answer = result.map_err(|error| PeerFailure(error.to_string()))?;
    postcard::to_allocvec(&answer).map_err(|error| PeerFailure(error.to_string()))
}

impl PeerRun for DeliverBatchOperation {
    fn answer(result: Result<Self::Output, Self::Error>) -> Result<Vec<u8>, PeerFailure> {
        notification_answer(result)
    }
}

impl PeerRun for RemoveWatchOperation {
    fn answer(result: Result<Self::Output, Self::Error>) -> Result<Vec<u8>, PeerFailure> {
        notification_answer(result)
    }
}

impl PeerRun for ServeInboxOperation {
    fn answer(result: Result<Self::Output, Self::Error>) -> Result<Vec<u8>, PeerFailure> {
        notification_answer(result)
    }
}

impl PeerRun for WatchEventsOperation {
    fn answer(result: Result<Self::Output, Self::Error>) -> Result<Vec<u8>, PeerFailure> {
        notification_answer(result)
    }
}

/// The user and request of an inbox or watch frame, or `None` for another frame.
fn inbox_request(
    message: NotificationTransportMessage,
    now_ms: u64,
) -> Option<(UserId, InboxRequest)> {
    let request = match message {
        NotificationTransportMessage::List {
            recipient,
            cursor,
            limit,
        } => {
            let limit = limit as usize;
            (
                recipient,
                InboxRequest::Read(HolderRead::List { cursor, limit }),
            )
        }
        NotificationTransportMessage::UnreadCount { recipient } => {
            (recipient, InboxRequest::Read(HolderRead::Unread))
        }
        NotificationTransportMessage::MarkRead {
            recipient,
            ids,
            up_to_ms,
        } => {
            let mark = HolderRead::Mark {
                ids,
                up_to_ms,
                now_ms,
            };
            (recipient, InboxRequest::Read(mark))
        }
        NotificationTransportMessage::ListWatches { owner } => (owner, InboxRequest::ListWatches),
        NotificationTransportMessage::CreateWatch {
            owner,
            path_prefix,
            event_mask,
            authorization,
        } => {
            let create = InboxRequest::CreateWatch {
                path_prefix,
                event_mask,
                authorization,
            };
            (owner, create)
        }
        _ => return None,
    };
    Some(request)
}

/// Runs `$body` with the operation of each `PeerOperation` variant bound to `$operation`.
macro_rules! each_peer {
    ($self:expr, $operation:ident => $body:expr) => {
        match $self {
            PeerOperation::DeliverNotifications($operation) => $body,
            PeerOperation::RemoveWatch($operation) => $body,
            PeerOperation::ServeInbox($operation) => $body,
            PeerOperation::WatchEvents($operation) => $body,
        }
    };
}

/// The next call of a peer operation, or its answer frame body.
fn peer_turn<O: PeerRun>(
    turn: Result<Next<O::Output>, O::Error>,
) -> Result<Next<Vec<u8>>, PeerFailure> {
    match turn {
        Ok(Next::Call(call)) => Ok(Next::Call(call)),
        Ok(Next::Wait) => Ok(Next::Wait),
        Ok(Next::Done(output)) => O::answer(Ok(output)).map(Next::Done),
        Ok(Next::Then { output, calls }) => {
            O::answer(Ok(output)).map(|output| Next::Then { output, calls })
        }
        Err(error) => O::answer(Err(error)).map(Next::Done),
    }
}

/// The conflict decision of a peer operation; an output it finishes with is encoded.
fn peer_conflict<O: PeerRun>(operation: &mut O, attempt: u32) -> AfterConflict<Vec<u8>> {
    match operation.on_conflict(attempt) {
        AfterConflict::Rerun => AfterConflict::Rerun,
        AfterConflict::Fail => AfterConflict::Fail,
        AfterConflict::Finish(output) => match O::answer(Ok(output)) {
            Ok(answer) => AfterConflict::Finish(answer),
            Err(_) => AfterConflict::Fail,
        },
    }
}

fn peer_first<O: PeerRun>(operation: &mut O) -> Result<Next<Vec<u8>>, PeerFailure> {
    peer_turn::<O>(operation.first())
}

fn peer_next<O: PeerRun>(
    operation: &mut O,
    result: IoResult,
) -> Result<Next<Vec<u8>>, PeerFailure> {
    peer_turn::<O>(operation.next(result))
}

impl Operation for PeerOperation {
    type Output = Vec<u8>;
    type Error = PeerFailure;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        each_peer!(self, operation => peer_first(operation))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        each_peer!(self, operation => peer_next(operation, result))
    }

    fn transaction(&self) -> TransactionMode {
        each_peer!(self, operation => operation.transaction())
    }

    fn abort(&mut self) -> Vec<IoCall> {
        each_peer!(self, operation => operation.abort())
    }

    fn stamp(&mut self, now_ms: u64) {
        each_peer!(self, operation => operation.stamp(now_ms))
    }

    fn after_commit(&mut self) -> Option<Effect> {
        each_peer!(self, operation => operation.after_commit())
    }

    fn hold_scope(&self) -> Option<HoldScope> {
        each_peer!(self, operation => operation.hold_scope())
    }

    fn on_conflict(&mut self, attempt: u32) -> AfterConflict<Self::Output> {
        each_peer!(self, operation => peer_conflict(operation, attempt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INPUT: TimerInput = TimerInput {
        now_ms: 1_000,
        local_node: None,
    };

    #[test]
    fn unconverted_keys_absent() {
        assert!(timer_operation(&TaskKey::DrainJobQueue, &INPUT).is_none());
        assert!(timer_operation(&TaskKey::PruneJobs, &INPUT).is_none());
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
    fn timer_delegates_calls() {
        let mut operation = timer_operation(&TaskKey::PruneNotifications, &INPUT).unwrap();
        let mut inner = PruneNotificationsOperation::new(1_000, PRUNE_PAGE_SIZE);
        assert_eq!(
            operation.first(),
            timer_turn::<PruneNotificationsOperation>(inner.first())
        );
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::None
        );
        let drain = timer_operation(&TaskKey::DrainNotificationOutbox, &INPUT);
        assert_eq!(
            drain.unwrap().first(),
            Ok(Next::Done(Some(DELIVERY_RETRY_AFTER)))
        );
    }

    /// The table keeps the wrapped operation's cleanup and conflict decisions.
    #[test]
    fn hooks_are_forwarded() {
        let mut operation = timer_operation(&TaskKey::PruneNotifications, &INPUT).unwrap();
        assert!(operation.abort().is_empty());
        assert_eq!(operation.on_conflict(1), AfterConflict::Rerun);
        assert_eq!(operation.on_conflict(9), AfterConflict::Fail);
        let frame = postcard::to_allocvec(&NotificationTransportMessage::DeliverBatch {
            records: Vec::new(),
        })
        .unwrap();
        let mut peer = peer_operation(peer_input(Alpn::Notification, &frame)).unwrap();
        assert_eq!(peer.on_conflict(1), AfterConflict::Rerun);
        assert_eq!(peer.hold_scope(), None);
    }

    #[test]
    fn timer_failure_rearms() {
        let mut operation = timer_operation(&TaskKey::PruneNotifications, &INPUT).unwrap();
        operation.first().unwrap();
        let failure = operation.next(IoResult::Done).unwrap_err();
        assert_eq!(failure.retry_after, NOTIFICATION_RETRY_AFTER);
        let runner = TimerFailure::from(IoError::Unfinished);
        assert_eq!(runner.retry_after, TIMER_RETRY_AFTER);
    }

    fn peer_input(alpn: Alpn, frame: &[u8]) -> PeerInput<'_> {
        let node = crate::tests::locations::node_id(1);
        PeerInput {
            alpn,
            peer: node,
            local_node: node,
            local_realm: RealmId::from_bytes([1; 32]),
            now_ms: 1_000,
            frame,
        }
    }

    #[test]
    fn unconverted_peers_absent() {
        for alpn in Alpn::ALL {
            if alpn != Alpn::Notification {
                assert!(peer_operation(peer_input(alpn, &[])).is_none());
            }
        }
        let ack = NotificationTransportMessage::WatchEventsAck { written: 0 };
        let frame = postcard::to_allocvec(&ack).unwrap();
        assert!(peer_operation(peer_input(Alpn::Notification, &frame)).is_none());
        let list = NotificationTransportMessage::ListWatches {
            owner: crate::tests::notifications::user(1, 2),
        };
        let frame = postcard::to_allocvec(&list).unwrap();
        assert!(matches!(
            peer_operation(peer_input(Alpn::Notification, &frame)),
            Some(PeerOperation::ServeInbox(_))
        ));
        assert!(peer_operation(peer_input(Alpn::Notification, &[0xff])).is_none());
    }

    #[test]
    fn delivery_frame_named() {
        let delivery = NotificationTransportMessage::DeliverBatch {
            records: Vec::new(),
        };
        let frame = postcard::to_allocvec(&delivery).unwrap();
        let mut operation = peer_operation(peer_input(Alpn::Notification, &frame)).unwrap();
        let Ok(Next::Done(answer)) = operation.first() else {
            panic!("an empty batch is answered");
        };
        let decoded: NotificationTransportMessage = postcard::from_bytes(&answer).unwrap();
        assert_eq!(
            decoded,
            NotificationTransportMessage::Reject("empty batch".to_string())
        );
    }
}
