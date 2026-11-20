//! Names the operation that answers a timer key or a peer request frame. The trigger shells ask
//! this table first; a key or frame it does not name stays with the shell's unconverted code.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Display;
use std::time::Duration;

use aruna_core::alpn::Alpn;
use aruna_core::backoff::capped_backoff;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::GitNode;
use aruna_core::id::NodeId;
use aruna_core::node_facts::NodeFacts;
use aruna_core::operation::{AfterConflict, HoldScope, Operation, TransactionMode};
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::execution::notification_watch::WATCH_PUBLISH_DEBOUNCE;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::task::TaskKey;
use aruna_core::types::Key;
use aruna_core::user::id::UserId;
use thiserror::Error;
use tracing::warn;

use crate::blob::cleanup_timer::CleanupTimerOperation;
use crate::blob::hidden::{HIDDEN_SWEEP_AFTER, HIDDEN_SWEEP_RETRY, ProcessHiddenOperation};
use crate::blob::migration::drain::ProcessMigrationsOperation;
use crate::blob::reclaim_sweep::RECLAIM_SWEEP_RETRY;
use crate::connectors::reference_prune::{PRUNE_RETRY, PruneReferencesOperation};
use crate::git::capture_drain::{CAPTURE_RETRY_AFTER, CaptureDrainOperation};
use crate::jobs::{JOB_PRUNE_PAGE, PRUNE_POLL_AFTER, PRUNE_RETRY_AFTER};
use crate::jobs::lifecycle::updates::{SETTLE_RETRY_AFTER, SettleTerminalsOperation};
use crate::jobs::remote_key::{DeliverWakesOperation, Delivery, WAKE_RETRY};
use crate::node::applied_drain::AppliedDrainOperation;
use crate::node::placement_timer::PlacementTimerOperation;
use crate::node::presence_timer::PresenceTimerOperation;
use crate::notifications::deliver::{DeliverBatchOperation, DeliverInput};
use crate::notifications::holder::HolderRead;
use crate::notifications::outbox::{DELIVERY_RETRY_AFTER, DrainOutboxOperation};
use crate::notifications::prune::{
    NOTIFICATION_RETRY_AFTER, PRUNE_PAGE_SIZE, PruneNotificationsOperation,
};
use crate::notifications::serve::{InboxRequest, ServeInboxOperation, WatchEventsOperation};
use crate::notifications::watch::drain::WatchDrainOperation;
use crate::notifications::watch::interest::PublishInterestOperation;
use crate::notifications::watch::remove::RemoveWatchOperation;
use crate::plugins::work::PruneKeepingWork;
use crate::replication::mirror_repair::drain::DrainRepairsOperation;
use crate::replication::mirror_repair::REPAIR_RETRY_AFTER;
use crate::s3::object::metadata::REFRESH_RETRY_AFTER;
use crate::s3::object::metadata_drain::DrainRefreshOperation;

/// What a timer keeps on this node between its runs, in memory only: its attempts since the last
/// clean run and where its work goes on. The shell stores it per key and hands it back unread, so
/// every decision about it stays with the timer's operation; a restart starts it afresh.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimerMemory {
    pub attempts: u32,
    pub cursor: Option<Vec<u8>>,
}

/// Retry steps after which a timer's retry ladder stops doubling.
const LADDER_STEPS: u32 = 7;

impl TimerMemory {
    /// The wait after a failed run: `base` for the first, then doubled per further attempt up to
    /// `max`. The returned memory counts the attempt and keeps the cursor.
    pub fn next_retry(&self, base: Duration, max: Duration) -> (Duration, TimerMemory) {
        let after = capped_backoff(base, max, self.attempts.min(LADDER_STEPS), 0);
        let memory = TimerMemory {
            attempts: self.attempts.saturating_add(1),
            cursor: self.cursor.clone(),
        };
        (after, memory)
    }
}

/// When a timer runs again, `None` while it waits for work, and what it keeps until then.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimerPlan {
    pub after: Option<Duration>,
    pub memory: TimerMemory,
}

impl From<Option<Duration>> for TimerPlan {
    fn from(after: Option<Duration>) -> Self {
        Self {
            after,
            memory: TimerMemory::default(),
        }
    }
}

/// What the timer shell knows when a timer fires.
#[derive(Clone, Debug, PartialEq)]
pub struct TimerInput {
    pub now_ms: u64,
    /// This node, `None` on a node without a net transport.
    pub local_node: Option<NodeId>,
    /// What the timer kept from its last run.
    pub memory: TimerMemory,
    /// What only this node's handles and host know.
    pub facts: NodeFacts,
    /// This node's routing rules and backend catalog.
    pub routing: NodeRouting,
    /// Whether this node has blob storage.
    pub has_blob: bool,
}

impl TimerInput {
    /// A timer firing with nothing kept from an earlier run and no node facts.
    pub fn new(now_ms: u64, local_node: Option<NodeId>) -> Self {
        Self {
            now_ms,
            local_node,
            memory: TimerMemory::default(),
            facts: NodeFacts::default(),
            routing: NodeRouting::default(),
            has_blob: false,
        }
    }
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

/// A timer operation decides when its timer runs again and what it keeps until then.
pub trait TimerRun: Operation + Sized {
    /// The plan of the next run; a failure runs the timer again after its delay, keeping its
    /// memory as it was.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure>;
}

/// The operation of one timer run. Its output is when the timer runs again and what it keeps;
/// the shell persists and arms that timer and stores the memory.
#[derive(Clone, Debug, PartialEq)]
pub enum TimerOperation {
    DrainNotificationOutbox(DrainOutboxOperation),
    PruneNotifications(PruneNotificationsOperation),
    PublishWatchInterest(PublishInterestOperation),
    DrainWatchEvents(WatchDrainOperation),
    DrainAppliedSync(AppliedDrainOperation),
    SyncPlacements(PlacementTimerOperation),
    RealmPresence(PresenceTimerOperation),
    MigrateCompression(ProcessMigrationsOperation),
    SweepHiddenBlobs(ProcessHiddenOperation),
    PruneConnectorReferences(PruneReferencesOperation),
    DeliverKeyWakes(DeliverWakesOperation),
    DrainCleanupQueue(CleanupTimerOperation),
    DrainRefreshQueue(DrainRefreshOperation),
    DrainCaptures(CaptureDrainOperation),
    SettleJobTerminals(SettleTerminalsOperation),
    PruneJobs(PruneKeepingWork),
    DrainMirrorRepair(DrainRepairsOperation),
    /// A timer this node cannot run yet, which only plans its next run.
    Wait(WaitOperation),
}

/// A timer run that only plans the next run, for a node that lacks what the timer needs.
#[derive(Clone, Debug, PartialEq)]
pub struct WaitOperation {
    plan: TimerPlan,
}

impl WaitOperation {
    pub fn after(after: Option<Duration>) -> Self {
        let plan = TimerPlan::from(after);
        Self { plan }
    }
}

impl Operation for WaitOperation {
    type Output = TimerPlan;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<TimerPlan>, IoError> {
        Ok(Next::Done(self.plan.clone()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TimerPlan>, IoError> {
        Err(IoError::Unexpected {
            expected: "no call",
            got: result.label(),
        })
    }
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
        TaskKey::DrainWatchEvents => {
            Some(TimerOperation::DrainWatchEvents(WatchDrainOperation::new()))
        }
        TaskKey::DrainAppliedSync => Some(TimerOperation::DrainAppliedSync(
            AppliedDrainOperation::new(),
        )),
        TaskKey::SyncPlacements { realm_id, node_id } => Some(TimerOperation::SyncPlacements(
            PlacementTimerOperation::new(*realm_id, *node_id, input),
        )),
        TaskKey::RealmPresence { realm_id, node_id } => Some(TimerOperation::RealmPresence(
            PresenceTimerOperation::new(*realm_id, *node_id),
        )),
        TaskKey::MigrateCompression => {
            let after = input.memory.cursor.clone().map(Key::from);
            let routing = input.routing.clone();
            let run = ProcessMigrationsOperation::new(routing, input.facts.serving, after);
            Some(TimerOperation::MigrateCompression(run))
        }
        TaskKey::SweepHiddenBlobs => Some(TimerOperation::SweepHiddenBlobs(
            ProcessHiddenOperation::new(input.now_ms),
        )),
        TaskKey::PruneConnectorReferences => Some(TimerOperation::PruneConnectorReferences(
            PruneReferencesOperation::new(),
        )),
        TaskKey::DeliverKeyWakes => {
            let start = input.memory.cursor.clone().map(Key::from);
            let deliver = DeliverWakesOperation::new(start, input.facts.serving);
            Some(TimerOperation::DeliverKeyWakes(deliver))
        }
        TaskKey::DrainCleanupQueue => Some(TimerOperation::DrainCleanupQueue(
            CleanupTimerOperation::new(input),
        )),
        TaskKey::DrainRefreshQueue => Some(TimerOperation::DrainRefreshQueue(
            DrainRefreshOperation::new(),
        )),
        TaskKey::DrainCaptures => {
            let ids = input.facts.serving;
            let routing = input.routing.clone();
            let node = GitNode { ids, routing };
            Some(TimerOperation::DrainCaptures(CaptureDrainOperation::new(node)))
        }
        TaskKey::SettleJobTerminals => Some(TimerOperation::SettleJobTerminals(
            SettleTerminalsOperation::new(input.facts.serving, input.now_ms),
        )),
        TaskKey::PruneJobs => Some(TimerOperation::PruneJobs(PruneKeepingWork::new(
            JOB_PRUNE_PAGE,
        ))),
        TaskKey::DrainMirrorRepair => Some(match input.local_node {
            Some(node_id) => TimerOperation::DrainMirrorRepair(DrainRepairsOperation::new(node_id)),
            None => TimerOperation::Wait(WaitOperation::after(Some(REPAIR_RETRY_AFTER))),
        }),
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
            TimerOperation::DrainWatchEvents($operation) => $body,
            TimerOperation::DrainAppliedSync($operation) => $body,
            TimerOperation::SyncPlacements($operation) => $body,
            TimerOperation::RealmPresence($operation) => $body,
            TimerOperation::MigrateCompression($operation) => $body,
            TimerOperation::SweepHiddenBlobs($operation) => $body,
            TimerOperation::PruneConnectorReferences($operation) => $body,
            TimerOperation::DeliverKeyWakes($operation) => $body,
            TimerOperation::DrainCleanupQueue($operation) => $body,
            TimerOperation::DrainRefreshQueue($operation) => $body,
            TimerOperation::DrainCaptures($operation) => $body,
            TimerOperation::SettleJobTerminals($operation) => $body,
            TimerOperation::PruneJobs($operation) => $body,
            TimerOperation::DrainMirrorRepair($operation) => $body,
            TimerOperation::Wait($operation) => $body,
        }
    };
}

/// The next call of a timer operation, or when its timer runs again.
fn timer_turn<O: TimerRun>(
    turn: Result<Next<O::Output>, O::Error>,
) -> Result<Next<TimerPlan>, TimerFailure> {
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
fn timer_conflict<O: TimerRun>(operation: &mut O, attempt: u32) -> AfterConflict<TimerPlan> {
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
    type Output = TimerPlan;
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

    fn seed(&mut self, entropy: u128) {
        each_timer!(self, operation => operation.seed(entropy))
    }

    fn hold_scope(&self) -> Option<HoldScope> {
        each_timer!(self, operation => operation.hold_scope())
    }

    fn on_conflict(&mut self, attempt: u32) -> AfterConflict<Self::Output> {
        each_timer!(self, operation => timer_conflict(operation, attempt))
    }
}

fn timer_first<O: TimerRun>(operation: &mut O) -> Result<Next<TimerPlan>, TimerFailure> {
    timer_turn::<O>(operation.first())
}

fn timer_next<O: TimerRun>(
    operation: &mut O,
    result: IoResult,
) -> Result<Next<TimerPlan>, TimerFailure> {
    timer_turn::<O>(operation.next(result))
}

impl TimerRun for DrainOutboxOperation {
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        let after = result.map_err(|error| TimerFailure {
            retry_after: DELIVERY_RETRY_AFTER,
            reason: error.to_string(),
        })?;
        Ok(TimerPlan::from(after))
    }
}

impl TimerRun for PublishInterestOperation {
    /// A published or skipped run waits for the next change; a failed one runs again soon.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        match result {
            Ok(_) => Ok(TimerPlan::default()),
            Err(error) => Err(TimerFailure {
                retry_after: WATCH_PUBLISH_DEBOUNCE,
                reason: error.to_string(),
            }),
        }
    }
}

impl TimerRun for WatchDrainOperation {
    /// A finished run waits for the next queued event; one that kept an unsettled event, or
    /// that failed itself, runs again later.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        Ok(TimerPlan::from(result.map_err(TimerFailure::from)?))
    }
}

impl TimerRun for AppliedDrainOperation {
    /// A finished run waits for the next applied reconcile; one that kept a row after a failed
    /// follow-up, or that failed itself, runs again later.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        Ok(TimerPlan::from(result.map_err(TimerFailure::from)?))
    }
}

impl TimerRun for PlacementTimerOperation {
    /// The operation plans its own next run; a failed run reads as a storage failure.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        result.map_err(TimerFailure::from)
    }
}

impl TimerRun for CleanupTimerOperation {
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        result.map_err(TimerFailure::from)
    }
}

impl TimerRun for PresenceTimerOperation {
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        result.map_err(TimerFailure::from)
    }
}

impl TimerRun for ProcessMigrationsOperation {
    /// The run plans its next start and the queue key it resumes after; a failed run keeps its
    /// cursor and runs again after the reclaim retry.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        let run = result.map_err(|error| TimerFailure {
            retry_after: RECLAIM_SWEEP_RETRY,
            reason: error.to_string(),
        })?;
        let memory = TimerMemory {
            attempts: 0,
            cursor: run.cursor.map(|key| key.to_vec()),
        };
        Ok(TimerPlan {
            after: run.next,
            memory,
        })
    }
}

impl TimerRun for ProcessHiddenOperation {
    /// A sweep that left cleanup pending, or failed, runs again soon; a clean one waits.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        let outcome = result.map_err(|error| TimerFailure {
            retry_after: HIDDEN_SWEEP_RETRY,
            reason: error.to_string(),
        })?;
        let after = match outcome.cleanup_pending {
            true => HIDDEN_SWEEP_RETRY,
            false => HIDDEN_SWEEP_AFTER,
        };
        Ok(TimerPlan::from(Some(after)))
    }
}

impl TimerRun for PruneReferencesOperation {
    /// Queued work runs again at once and a failed run after a short wait; an empty queue waits
    /// for the next queued connector.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        let remaining = result.map_err(|error| TimerFailure {
            retry_after: PRUNE_RETRY,
            reason: error.to_string(),
        })?;
        Ok(TimerPlan::from(remaining.then_some(Duration::ZERO)))
    }
}

impl TimerRun for DeliverWakesOperation {
    /// Owed wakes go on at once with a further page and retry after a finished round; an
    /// unreadable pass starts over later. The kept cursor is where the next pass starts.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        let delivery = result.unwrap_or_else(|_| Delivery::unread());
        let after = match (delivery.owed, &delivery.next) {
            (false, _) => None,
            (true, Some(_)) => Some(Duration::ZERO),
            (true, None) => Some(WAKE_RETRY),
        };
        let memory = TimerMemory {
            attempts: 0,
            cursor: delivery.next.map(|key| key.to_vec()),
        };
        Ok(TimerPlan { after, memory })
    }
}

impl TimerRun for DrainRefreshOperation {
    /// More due refreshes run at once, the next one when it is due; a failed batch retries soon.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        let drained = result.map_err(|error| TimerFailure {
            retry_after: REFRESH_RETRY_AFTER,
            reason: error.to_string(),
        })?;
        let after = match drained.has_more_due {
            true => Some(Duration::ZERO),
            false => drained.next_due_after,
        };
        Ok(TimerPlan::from(after))
    }
}

impl TimerRun for CaptureDrainOperation {
    /// The drain names its own next run; a failed one retries after the capture delay.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        let after = result.map_err(|error| TimerFailure {
            retry_after: CAPTURE_RETRY_AFTER,
            reason: error.to_string(),
        })?;
        Ok(TimerPlan::from(after))
    }
}

impl TimerRun for SettleTerminalsOperation {
    /// A pending or failed settle retries; the reservation rows keep the obligation durable.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        let pending = result.unwrap_or_else(|error| {
            warn!(%error, "Failed to settle terminal job publications");
            true
        });
        Ok(TimerPlan::from(pending.then_some(SETTLE_RETRY_AFTER)))
    }
}

impl TimerRun for PruneKeepingWork {
    /// More due jobs run at once; otherwise the next due job runs at its time, at most after the
    /// poll interval. A failed run retries soon.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        let outcome = result.map_err(|error| TimerFailure {
            retry_after: PRUNE_RETRY_AFTER,
            reason: error.to_string(),
        })?;
        let poll = |due: Duration| due.min(PRUNE_POLL_AFTER);
        let after = match outcome.has_more {
            true => Duration::ZERO,
            false => outcome.next_due_after.map_or(PRUNE_POLL_AFTER, poll),
        };
        Ok(TimerPlan::from(Some(after)))
    }
}

impl TimerRun for DrainRepairsOperation {
    /// More due repairs run at once, the next one when it is due; a failed pass retries later.
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        let drained = result.map_err(|error| TimerFailure {
            retry_after: REPAIR_RETRY_AFTER,
            reason: error.to_string(),
        })?;
        let after = match drained.has_more_due {
            true => Some(Duration::ZERO),
            false => drained.next_due_after,
        };
        Ok(TimerPlan::from(after))
    }
}

impl TimerRun for WaitOperation {
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        result.map_err(TimerFailure::from)
    }
}

impl TimerRun for PruneNotificationsOperation {
    fn rearm(result: Result<Self::Output, Self::Error>) -> Result<TimerPlan, TimerFailure> {
        match result {
            Ok(outcome) => Ok(TimerPlan::from(Some(outcome.rearm()))),
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

    fn seed(&mut self, entropy: u128) {
        each_peer!(self, operation => operation.seed(entropy))
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
    use crate::blob::hidden::HiddenSweepOutcome;
    use crate::replication::mirror_repair::{SyncMirrorError, SyncMirrorResult};
    use crate::jobs::prune::JobPruneOutcome;
    use crate::jobs::lifecycle::LifecycleError;
    use crate::s3::object::metadata::ReferenceRefreshError;
    use crate::s3::object::metadata_drain::ReferenceDrainResult;
    use crate::blob::migration::drain::MigrationRun;
    use crate::blob::migration::shared::MigrationError;
    use crate::connectors::reference_prune::PruneError;

    fn input() -> TimerInput {
        TimerInput::new(1_000, None)
    }

    #[test]
    fn unconverted_keys_absent() {
        assert!(timer_operation(&TaskKey::DrainJobQueue, &input()).is_none());
        assert!(timer_operation(&TaskKey::PruneJobs, &input()).is_none());
    }

    #[test]
    fn migration_resumes_cursor() {
        // The kept queue key is where the next run resumes; a failure keeps it unchanged.
        let run = MigrationRun {
            next: Some(Duration::ZERO),
            cursor: Some(Key::from(b"bucket".to_vec())),
        };
        let plan = ProcessMigrationsOperation::rearm(Ok(run)).unwrap();
        assert_eq!(plan.after, Some(Duration::ZERO));
        assert_eq!(plan.memory.cursor.as_deref(), Some(&b"bucket"[..]));
        let mut input = input();
        input.memory = plan.memory;
        let mut operation = timer_operation(&TaskKey::MigrateCompression, &input).unwrap();
        let Ok(Next::Call(IoCall::Page { after, .. })) = operation.first() else {
            panic!("expected the queue page read");
        };
        assert_eq!(after, Some(Key::from(b"bucket".to_vec())));
        let failed = ProcessMigrationsOperation::rearm(Err(MigrationError::failed("boom")));
        assert_eq!(failed.unwrap_err().retry_after, RECLAIM_SWEEP_RETRY);
    }

    #[test]
    fn hidden_sweep_plans() {
        // Pending cleanup and failures retry soon; a finished cycle waits for the interval.
        let pending = HiddenSweepOutcome {
            cleanup_pending: true,
            ..HiddenSweepOutcome::default()
        };
        let plan = ProcessHiddenOperation::rearm(Ok(pending)).unwrap();
        assert_eq!(plan.after, Some(HIDDEN_SWEEP_RETRY));
        let clean = ProcessHiddenOperation::rearm(Ok(HiddenSweepOutcome::default())).unwrap();
        assert_eq!(clean.after, Some(HIDDEN_SWEEP_AFTER));
        let failed = ProcessHiddenOperation::rearm(Err(MigrationError::failed("boom")));
        assert_eq!(failed.unwrap_err().retry_after, HIDDEN_SWEEP_RETRY);
    }

    #[test]
    fn reference_prune_plans() {
        // Queued work runs again at once; a failure retries after a short wait.
        let more = PruneReferencesOperation::rearm(Ok(true)).unwrap();
        assert_eq!(more.after, Some(Duration::ZERO));
        let idle = PruneReferencesOperation::rearm(Ok(false));
        assert_eq!(idle, Ok(TimerPlan::default()));
        let error = PruneError::Schedule("boom".to_string());
        let failed = PruneReferencesOperation::rearm(Err(error));
        assert_eq!(failed.unwrap_err().retry_after, PRUNE_RETRY);
    }

    #[test]
    fn wake_delivery_plans() {
        // A further page follows at once, a finished round retries later, nothing owed waits.
        let page = Delivery {
            owed: true,
            next: Some(Key::from(b"wake".to_vec())),
            ..Delivery::default()
        };
        let plan = DeliverWakesOperation::rearm(Ok(page)).unwrap();
        assert_eq!(plan.after, Some(Duration::ZERO));
        assert_eq!(plan.memory.cursor.as_deref(), Some(&b"wake"[..]));
        let unread = DeliverWakesOperation::rearm(Err(IoError::Codec("broken".to_string())));
        assert_eq!(unread, Ok(TimerPlan::from(Some(WAKE_RETRY))));
        let idle = DeliverWakesOperation::rearm(Ok(Delivery::default()));
        assert_eq!(idle, Ok(TimerPlan::default()));
    }

    #[test]
    fn refresh_drain_plans() {
        // More due work runs at once, the next due one at its time; a failure retries soon.
        let result = |has_more_due, next_due_after| ReferenceDrainResult {
            processed: 0,
            succeeded: 0,
            failed: 0,
            has_more_due,
            next_due_after,
        };
        let more = DrainRefreshOperation::rearm(Ok(result(true, None))).unwrap();
        assert_eq!(more.after, Some(Duration::ZERO));
        let due = Some(Duration::from_secs(9));
        let later = DrainRefreshOperation::rearm(Ok(result(false, due))).unwrap();
        assert_eq!(later.after, due);
        let error = ReferenceRefreshError::Refresh("boom".to_string());
        let failed = DrainRefreshOperation::rearm(Err(error));
        assert_eq!(failed.unwrap_err().retry_after, REFRESH_RETRY_AFTER);
    }

    #[test]
    fn capture_drain_plans() {
        // The drain's own delay is kept; a failed drain retries after the capture delay.
        let after = Some(Duration::from_secs(5));
        assert_eq!(CaptureDrainOperation::rearm(Ok(after)), Ok(TimerPlan::from(after)));
        let failed = CaptureDrainOperation::rearm(Err(IoError::Unfinished));
        assert_eq!(failed.unwrap_err().retry_after, CAPTURE_RETRY_AFTER);
    }

    #[test]
    fn terminal_settle_plans() {
        // A pending or failed settle retries; a finished one waits for the next obligation.
        let retry = Ok(TimerPlan::from(Some(SETTLE_RETRY_AFTER)));
        assert_eq!(SettleTerminalsOperation::rearm(Ok(true)), retry);
        let error = LifecycleError::Codec("broken".to_string());
        assert_eq!(SettleTerminalsOperation::rearm(Err(error)), retry);
        let settled = SettleTerminalsOperation::rearm(Ok(false));
        assert_eq!(settled, Ok(TimerPlan::default()));
    }

    #[test]
    fn job_prune_plans() {
        // More due jobs run at once, a later one at its time but within the poll interval.
        let outcome = |has_more, next_due_after| JobPruneOutcome {
            has_more,
            next_due_after,
            ..JobPruneOutcome::default()
        };
        let more = PruneKeepingWork::rearm(Ok(outcome(true, None))).unwrap();
        assert_eq!(more.after, Some(Duration::ZERO));
        let idle = PruneKeepingWork::rearm(Ok(outcome(false, None))).unwrap();
        assert_eq!(idle.after, Some(PRUNE_POLL_AFTER));
        let far = Some(PRUNE_POLL_AFTER * 2);
        let capped = PruneKeepingWork::rearm(Ok(outcome(false, far))).unwrap();
        assert_eq!(capped.after, Some(PRUNE_POLL_AFTER));
        let error = crate::jobs::prune::PruneError("boom".to_string());
        let failed = PruneKeepingWork::rearm(Err(error));
        assert_eq!(failed.unwrap_err().retry_after, PRUNE_RETRY_AFTER);
    }

    #[test]
    fn mirror_repair_plans() {
        // Without a network node the timer only waits; more due repairs run at once.
        let mut waiting = timer_operation(&TaskKey::DrainMirrorRepair, &input()).unwrap();
        let retry = TimerPlan::from(Some(REPAIR_RETRY_AFTER));
        assert_eq!(waiting.first(), Ok(Next::Done(retry)));
        let more = SyncMirrorResult {
            has_more_due: true,
            ..SyncMirrorResult::default()
        };
        let plan = DrainRepairsOperation::rearm(Ok(more)).unwrap();
        assert_eq!(plan.after, Some(Duration::ZERO));
        let error = SyncMirrorError::Mirror("boom".to_string());
        let failed = DrainRepairsOperation::rearm(Err(error));
        assert_eq!(failed.unwrap_err().retry_after, REPAIR_RETRY_AFTER);
    }

    #[test]
    fn prune_key_named() {
        let operation = timer_operation(&TaskKey::PruneNotifications, &input());
        assert_eq!(
            operation,
            Some(TimerOperation::PruneNotifications(
                PruneNotificationsOperation::new(1_000, PRUNE_PAGE_SIZE)
            ))
        );
    }

    #[test]
    fn timer_delegates_calls() {
        let mut operation = timer_operation(&TaskKey::PruneNotifications, &input()).unwrap();
        let mut inner = PruneNotificationsOperation::new(1_000, PRUNE_PAGE_SIZE);
        assert_eq!(
            operation.first(),
            timer_turn::<PruneNotificationsOperation>(inner.first())
        );
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::None
        );
        let drain = timer_operation(&TaskKey::DrainNotificationOutbox, &input());
        assert_eq!(
            drain.unwrap().first(),
            Ok(Next::Done(TimerPlan::from(Some(DELIVERY_RETRY_AFTER))))
        );
    }

    /// The table keeps the wrapped operation's cleanup and conflict decisions.
    #[test]
    fn hooks_are_forwarded() {
        let mut operation = timer_operation(&TaskKey::PruneNotifications, &input()).unwrap();
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
        let mut operation = timer_operation(&TaskKey::PruneNotifications, &input()).unwrap();
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
