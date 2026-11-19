//! Runs bounded remote recovery after a start until it converges or is stopped.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::backoff::capped_backoff;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use tracing::{info, warn};

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::node::usage_publish::{PublishUsageOperation, PublishedUsage, UsagePublishError};
use crate::placement::process_placements::{
    PlacementReconcileOutcome, PlacementReconcileStatus, ReconcilePlacementsOperation,
};
use crate::realm::announce_presence::{
    AnnouncePresenceConfig, AnnouncePresenceError, AnnouncePresenceOperation,
};
use crate::shard::recovery::{
    PhaseOutcome, RecoveryBaseline, RecoveryDecision, RecoveryError, RecoveryOutcome, RecoveryPass,
    RecoveryRotation, RecoveryStatus, RecoveryStop, apply_recovery_pass,
};
use crate::shard::restore::{RestorePassOperation, RestoredPass};
use crate::shard::restore_plan::{ShardRestoreCursor, sync_eligible_node};

/// Shortest wait between two bounded recovery passes while work remains local.
pub(crate) const RECOVERY_RETRY_BASE: Duration = Duration::from_millis(250);
/// Longest wait between passes once the remainder is blocked on absent peers.
pub(crate) const RECOVERY_RETRY_MAX: Duration = Duration::from_secs(30);
/// Longest time a waiting recovery takes to notice a stop.
const STOP_CHECK: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RecoveryConfig {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    /// Publish one full node-usage snapshot set on the first pass. Set only for
    /// an initial bootstrap or an actual usage-counter rebuild.
    pub publish_full_usage: bool,
}

fn phase_done() -> PhaseOutcome {
    PhaseOutcome {
        progress: true,
        error: None,
    }
}

fn phase_failed(error: RecoveryError) -> PhaseOutcome {
    PhaseOutcome {
        progress: false,
        error: Some(error),
    }
}

/// Announces this node's realm presence. A user device reads the DHT to locate realm nodes but
/// never announces itself as one; without a presence record nothing routes work to it.
#[derive(Debug, PartialEq)]
pub struct PresencePhaseOperation {
    realm_id: RealmId,
    node_id: NodeId,
    announcing: bool,
}

impl PresencePhaseOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId) -> Self {
        Self {
            realm_id,
            node_id,
            announcing: false,
        }
    }
}

impl Operation for PresencePhaseOperation {
    type Output = PhaseOutcome;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<PhaseOutcome>, IoError> {
        let config = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PhaseOutcome>, IoError> {
        if self.announcing {
            return Ok(Next::Done(
                match result.child::<(), AnnouncePresenceError>()? {
                    Ok(()) => phase_done(),
                    Err(error) => {
                        warn!(error = %error, "Failed to announce realm presence during recovery");
                        phase_failed(RecoveryError::PeerUnavailable)
                    }
                },
            ));
        }
        match result.child::<RealmConfigDocument, GetConfigError>()? {
            Ok(config) if !sync_eligible_node(&config, self.node_id) => {
                return Ok(Next::Done(phase_done()));
            }
            Ok(_) | Err(GetConfigError::DocumentNotFound) => {}
            Err(error) => {
                warn!(realm_id = %self.realm_id, error = %error, "Failed to load realm config for recovery");
            }
        }
        self.announcing = true;
        let announce = AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id: self.realm_id,
            node_id: self.node_id,
            schedule_refresh: true,
        });
        Ok(Next::Call(IoCall::child(announce)))
    }
}

#[derive(Debug, PartialEq)]
enum RecoveryStage {
    Restore,
    Placements,
    Presence,
    Usage,
    Waiting,
}

type RecoveryTurn = Result<Next<()>, IoError>;

/// Repeats bounded recovery passes: restore shard topics, then reconcile placements, announce
/// presence and publish usage, until nothing is left or `stop` is requested. Every peer failure
/// is retryable background state and never a start failure.
#[derive(Debug, PartialEq)]
pub struct RecoveryOperation {
    config: RecoveryConfig,
    serving_realm: Option<RealmId>,
    status: RecoveryStatus,
    stop: RecoveryStop,
    now_ms: u64,
    started_ms: u64,
    stage: RecoveryStage,
    cursor: ShardRestoreCursor,
    publish_usage: bool,
    reconcile_placements: bool,
    announce_presence: bool,
    rotation: RecoveryRotation,
    baseline: Option<RecoveryBaseline>,
    first_rotation: bool,
    passes: u64,
    attempts: u32,
    pass: RecoveryPass,
    waiting: Duration,
}

impl RecoveryOperation {
    /// `serving_realm` is the realm this node serves, when it runs a net transport.
    pub fn new(
        config: RecoveryConfig,
        serving_realm: Option<RealmId>,
        status: RecoveryStatus,
        stop: RecoveryStop,
    ) -> Self {
        Self {
            config,
            serving_realm,
            status,
            stop,
            now_ms: 0,
            started_ms: 0,
            stage: RecoveryStage::Restore,
            cursor: ShardRestoreCursor::default(),
            publish_usage: config.publish_full_usage,
            reconcile_placements: true,
            announce_presence: true,
            rotation: RecoveryRotation::default(),
            baseline: None,
            first_rotation: true,
            passes: 0,
            attempts: 0,
            pass: RecoveryPass::default(),
            waiting: Duration::ZERO,
        }
    }

    fn elapsed_ms(&self) -> u64 {
        self.now_ms.saturating_sub(self.started_ms)
    }

    fn begin_pass(&mut self) -> RecoveryTurn {
        if self.stop.requested() {
            return Ok(Next::Done(()));
        }
        self.status.begin_pass();
        self.pass = RecoveryPass::default();
        self.stage = RecoveryStage::Restore;
        let RecoveryConfig {
            node_id, realm_id, ..
        } = self.config;
        let restore = RestorePassOperation::new(node_id, realm_id, self.cursor, self.stop.clone());
        Ok(Next::Call(IoCall::child(restore)))
    }

    fn restored(&mut self, result: IoResult) -> RecoveryTurn {
        let (restore, cursor) = match result.child::<RestoredPass, IoError>()? {
            Ok(restored) => restored,
            // A stopped pass observed nothing: retry it with backoff.
            Err(error) => {
                warn!(error = %error, "Shard restore pass stopped");
                self.pass.error = Some(RecoveryError::PeerUnavailable);
                return self.finish_pass();
            }
        };
        self.cursor = cursor;
        self.pass.unresolved_topics = restore.unresolved_topics;
        self.pass.unvisited_topics = restore.unvisited_topics;
        self.pass.error = restore.error;
        self.pass.plan_changed = restore.plan_changed;
        self.pass.topics_completed = restore.topics_completed;
        self.pass.enumerated = restore.enumerated;
        self.reconcile_placements |= restore.plan_changed;
        // Convergence needs the cursor to have walked every unit.
        if !restore.wrapped {
            self.pass.more_local_work = true;
            return self.finish_pass();
        }
        // Without an enumerated plan the pass observed nothing; skip the phases.
        if !restore.enumerated {
            return self.finish_pass();
        }
        self.placements_phase()
    }

    fn placements_phase(&mut self) -> RecoveryTurn {
        if self.stop.requested() {
            return Ok(Next::Done(()));
        }
        if !self.reconcile_placements {
            return self.presence_phase();
        }
        self.stage = RecoveryStage::Placements;
        // Placement transitions run on their timer so cluster-wide draining cannot block
        // readiness.
        let reconcile =
            ReconcilePlacementsOperation::new(self.config.realm_id, self.config.node_id, false);
        Ok(Next::Call(IoCall::child(reconcile)))
    }

    fn placements_done(&mut self, result: IoResult) -> RecoveryTurn {
        let placements = (result.child::<PlacementReconcileOutcome, IoError>()?)
            .unwrap_or_else(|_| PlacementReconcileOutcome::storage_failure());
        let outcome = match placements.status {
            PlacementReconcileStatus::Clean => {
                self.reconcile_placements = false;
                phase_done()
            }
            PlacementReconcileStatus::RetryScheduled => {
                phase_failed(RecoveryError::PeerUnavailable)
            }
            PlacementReconcileStatus::StorageFailure => phase_failed(RecoveryError::Storage),
        };
        info!(
            event = "startup.recovery.progress",
            phase = "placements",
            retry_scheduled = placements.retry_scheduled,
            pull_pending = placements.pull_pending,
            "Reconciled shard placements"
        );
        self.pass.merge_phase(outcome);
        self.presence_phase()
    }

    fn presence_phase(&mut self) -> RecoveryTurn {
        if self.stop.requested() {
            return Ok(Next::Done(()));
        }
        if !self.announce_presence {
            return self.usage_phase();
        }
        self.stage = RecoveryStage::Presence;
        let presence = PresencePhaseOperation::new(self.config.realm_id, self.config.node_id);
        Ok(Next::Call(IoCall::child(presence)))
    }

    fn presence_done(&mut self, result: IoResult) -> RecoveryTurn {
        let outcome = (result.child::<PhaseOutcome, IoError>()?)
            .unwrap_or_else(|_| phase_failed(RecoveryError::PeerUnavailable));
        self.announce_presence = outcome.error.is_some();
        self.pass.merge_phase(outcome);
        self.usage_phase()
    }

    fn usage_phase(&mut self) -> RecoveryTurn {
        if self.stop.requested() {
            return Ok(Next::Done(()));
        }
        if !self.publish_usage {
            return self.finish_pass();
        }
        self.stage = RecoveryStage::Usage;
        let keys = (self.config.node_id, self.config.realm_id);
        let publish = PublishUsageOperation::new(keys, self.serving_realm, true, true);
        Ok(Next::Call(IoCall::child(publish)))
    }

    fn usage_done(&mut self, result: IoResult) -> RecoveryTurn {
        let outcome = match result.child::<PublishedUsage, UsagePublishError>()? {
            Ok(_) => {
                self.publish_usage = false;
                phase_done()
            }
            Err(error) => {
                warn!(error = %error, "Failed to publish initial node usage snapshots");
                phase_failed(RecoveryError::Storage)
            }
        };
        self.pass.merge_phase(outcome);
        self.finish_pass()
    }

    fn finish_pass(&mut self) -> RecoveryTurn {
        self.passes = self.passes.saturating_add(1);
        if self.stop.requested() {
            return Ok(Next::Done(()));
        }
        let decision = apply_recovery_pass(
            &mut self.rotation,
            &mut self.baseline,
            &mut self.first_rotation,
            &self.pass,
        );
        match decision {
            RecoveryDecision::ContinueLocal {
                progress,
                topics_remaining,
                error,
            } => {
                match progress {
                    true => self.status.note_progress(topics_remaining),
                    false => self.status.set_remaining(topics_remaining),
                }
                self.status.finish_pass(RecoveryOutcome::Partial, error);
                self.begin_pass()
            }
            RecoveryDecision::Converged => {
                self.status.note_progress(0);
                self.status.finish_pass(RecoveryOutcome::Success, None);
                info!(
                    event = "startup.recovery.complete",
                    elapsed_ms = self.elapsed_ms(),
                    passes = self.passes,
                    "Remote recovery converged"
                );
                Ok(Next::Done(()))
            }
            RecoveryDecision::Retry {
                failure,
                progress,
                enumerated,
            } => {
                let topics_remaining = failure.unresolved_topics.len() as u64;
                if progress {
                    self.status.note_progress(topics_remaining);
                    self.attempts = 0;
                } else if enumerated {
                    self.status.set_remaining(topics_remaining);
                }
                self.status
                    .finish_pass(RecoveryOutcome::Partial, failure.error);
                warn!(
                    event = "startup.recovery.degraded",
                    topics_remaining = failure.unresolved_topics.len(),
                    error_class = failure.error.map(RecoveryError::label).unwrap_or("none"),
                    elapsed_ms = self.elapsed_ms(),
                    "Remote recovery left retryable work behind"
                );
                self.waiting =
                    capped_backoff(RECOVERY_RETRY_BASE, RECOVERY_RETRY_MAX, self.attempts, 0);
                self.attempts = self.attempts.saturating_add(1);
                self.wait()
            }
        }
    }

    /// Waits in short steps, so a stop ends the wait without a timer of its own.
    fn wait(&mut self) -> RecoveryTurn {
        if self.stop.requested() {
            return Ok(Next::Done(()));
        }
        if self.waiting.is_zero() {
            return self.begin_pass();
        }
        let step = self.waiting.min(STOP_CHECK);
        self.waiting -= step;
        self.stage = RecoveryStage::Waiting;
        Ok(Next::Call(IoCall::Delay(step)))
    }
}

impl Operation for RecoveryOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> RecoveryTurn {
        info!(event = "startup.recovery.begin", "Remote recovery started");
        self.started_ms = self.now_ms;
        self.begin_pass()
    }

    fn next(&mut self, result: IoResult) -> RecoveryTurn {
        match self.stage {
            RecoveryStage::Restore => self.restored(result),
            RecoveryStage::Placements => self.placements_done(result),
            RecoveryStage::Presence => self.presence_done(result),
            RecoveryStage::Usage => self.usage_done(result),
            RecoveryStage::Waiting => self.wait(),
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use irokle::TopicId;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::shard::recovery::RecoveryState;
    use crate::shard::restore_plan::ShardRestorePass;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([7; 32])
    }

    fn child<T, E>(outcome: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    fn recovery(stop: RecoveryStop) -> (RecoveryOperation, RecoveryStatus) {
        let config = RecoveryConfig {
            realm_id: realm(),
            node_id: node(),
            publish_full_usage: false,
        };
        let status = RecoveryStatus::new();
        let operation = RecoveryOperation::new(config, Some(realm()), status.clone(), stop);
        (operation, status)
    }

    fn clean_pass() -> ShardRestorePass {
        ShardRestorePass {
            wrapped: true,
            enumerated: true,
            ..ShardRestorePass::default()
        }
    }

    fn blocked_pass() -> ShardRestorePass {
        ShardRestorePass {
            unresolved_topics: [TopicId::from_bytes([3; 32])].into(),
            error: Some(RecoveryError::PeerUnavailable),
            ..clean_pass()
        }
    }

    fn passed(pass: ShardRestorePass) -> IoResult {
        child::<RestoredPass, IoError>(Ok((pass, ShardRestoreCursor::default())))
    }

    fn pass_call() -> IoCall {
        let pass = RestorePassOperation::new(
            node(),
            realm(),
            ShardRestoreCursor::default(),
            RecoveryStop::default(),
        );
        IoCall::child(pass)
    }

    /// A recovery past its placement and presence phases, waiting for the first pass again.
    fn settled() -> (RecoveryOperation, RecoveryStatus) {
        let (mut operation, status) = recovery(RecoveryStop::default());
        operation.reconcile_placements = false;
        operation.announce_presence = false;
        operation.first().unwrap();
        (operation, status)
    }

    #[test]
    fn phases_then_converges() {
        let (mut operation, status) = recovery(RecoveryStop::default());
        assert_eq!(operation.first(), Ok(Next::Call(pass_call())));
        assert_eq!(status.snapshot().state, RecoveryState::Running);
        let reconcile = ReconcilePlacementsOperation::new(realm(), node(), false);
        let turn = operation.next(passed(clean_pass()));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(reconcile))));
        let presence = PresencePhaseOperation::new(realm(), node());
        let clean = PlacementReconcileOutcome::default();
        let turn = operation.next(child::<_, IoError>(Ok(clean)));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(presence))));
        let done = operation.next(child::<_, IoError>(Ok(phase_done())));
        assert_eq!(done, Ok(Next::Done(())));
        assert_eq!(status.snapshot().state, RecoveryState::Converged);
        assert_eq!(status.pass_total(RecoveryOutcome::Success), 1);
    }

    #[test]
    fn full_usage_published() {
        let (mut operation, status) = recovery(RecoveryStop::default());
        operation.publish_usage = true;
        operation.reconcile_placements = false;
        operation.announce_presence = false;
        operation.first().unwrap();
        let turn = operation.next(passed(clean_pass()));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let failed = UsagePublishError::from(IoError::Aborted);
        let turn = operation.next(child::<PublishedUsage, _>(Err(failed)));
        assert_eq!(turn, Ok(Next::Call(IoCall::Delay(RECOVERY_RETRY_BASE))));
        assert!(operation.publish_usage, "a failed publish stays pending");
        assert_eq!(status.snapshot().last_error, Some(RecoveryError::Storage));
    }

    #[test]
    fn unfinished_pass_continues() {
        let (mut operation, status) = settled();
        let partial = ShardRestorePass {
            wrapped: false,
            unvisited_topics: 4,
            ..clean_pass()
        };
        let turn = operation.next(passed(partial));
        assert_eq!(turn, Ok(Next::Call(pass_call())));
        assert_eq!(status.snapshot().topics_remaining, 4);
        assert_eq!(status.pass_total(RecoveryOutcome::Partial), 1);
    }

    #[test]
    fn blocked_pass_retries() {
        let (mut operation, status) = settled();
        let turn = operation.next(passed(blocked_pass()));
        assert_eq!(turn, Ok(Next::Call(IoCall::Delay(RECOVERY_RETRY_BASE))));
        assert_eq!(status.snapshot().state, RecoveryState::Degraded);
        assert_eq!(operation.next(IoResult::Woke), Ok(Next::Call(pass_call())));
    }

    #[test]
    fn long_wait_steps() {
        let (mut operation, _status) = settled();
        operation.attempts = 40;
        let turn = operation.next(passed(blocked_pass()));
        assert_eq!(turn, Ok(Next::Call(IoCall::Delay(STOP_CHECK))));
        assert_eq!(operation.waiting, RECOVERY_RETRY_MAX - STOP_CHECK);
    }

    #[test]
    fn stop_ends_wait() {
        let token = CancellationToken::new();
        let (mut operation, _status) = recovery(RecoveryStop::new(token.clone()));
        operation.reconcile_placements = false;
        operation.announce_presence = false;
        operation.first().unwrap();
        operation.attempts = 40;
        operation.next(passed(blocked_pass())).unwrap();
        token.cancel();
        assert_eq!(operation.next(IoResult::Woke), Ok(Next::Done(())));
    }

    #[test]
    fn stopped_before_pass() {
        let token = CancellationToken::new();
        token.cancel();
        let (mut operation, status) = recovery(RecoveryStop::new(token));
        assert_eq!(operation.first(), Ok(Next::Done(())));
        assert_eq!(status.snapshot().state, RecoveryState::Pending);
    }

    #[test]
    fn failed_pass_retries() {
        let (mut operation, status) = settled();
        let failed = child::<RestoredPass, IoError>(Err(IoError::Aborted));
        let turn = operation.next(failed);
        assert_eq!(turn, Ok(Next::Call(IoCall::Delay(RECOVERY_RETRY_BASE))));
        let error = status.snapshot().last_error;
        assert_eq!(error, Some(RecoveryError::PeerUnavailable));
    }

    #[test]
    fn device_skips_announce() {
        let mut presence = PresencePhaseOperation::new(realm(), node());
        presence.first().unwrap();
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 1);
        let owner = UserId::nil(realm());
        config.ensure_node(node(), RealmNodeKind::User { owner });
        let turn = presence.next(child::<_, GetConfigError>(Ok(config)));
        assert_eq!(turn, Ok(Next::Done(phase_done())));
    }

    #[test]
    fn server_announces_presence() {
        let mut presence = PresencePhaseOperation::new(realm(), node());
        presence.first().unwrap();
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 1);
        config.ensure_node(node(), RealmNodeKind::Server);
        let announce = AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id: realm(),
            node_id: node(),
            schedule_refresh: true,
        });
        let turn = presence.next(child::<_, GetConfigError>(Ok(config)));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(announce))));
        let failed = AnnouncePresenceError::from(IoError::Aborted);
        let turn = presence.next(child::<(), _>(Err(failed)));
        let expected = phase_failed(RecoveryError::PeerUnavailable);
        assert_eq!(turn, Ok(Next::Done(expected)));
    }

    // Backoff climbs from the base to the cap and never past it, so a peer that
    // stays down cannot keep the driver hot.
    #[test]
    fn recovery_backoff_caps() {
        let backoff =
            |attempts| capped_backoff(RECOVERY_RETRY_BASE, RECOVERY_RETRY_MAX, attempts, 0);
        assert_eq!(backoff(0), RECOVERY_RETRY_BASE);
        assert!(backoff(1) > backoff(0));
        assert_eq!(backoff(40), RECOVERY_RETRY_MAX);
    }
}
