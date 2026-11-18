//! Runs the placement transition steps this node owns: freeze, join, pull, verify and sign.
//! A target node never mints a genesis, since a rival genesis would split the topic forever.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::backoff::conflict_backoff;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxRecord, ShardManifest, shard_topic_id};
use aruna_core::effects::ShardNetEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::ShardNetEvent;
use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::placement::fence::{fence_read, stored_generation};
use aruna_core::placement::transition::{
    TransitionRequest, expansion_buckets, holders_in_map, plan_transition,
};
use aruna_core::rows::{PrefixPages, done, read_row, value};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::placement::transition::{
    PlacementTransition, ProofClaim, TransitionLimits, TransitionStatus,
};
use aruna_core::types::Key;
use tracing::{debug, warn};

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::placement::mutation::{KickDrainOperation, PlacementMutationOperation};
use crate::realm::mutate_placement::{
    CONFLICT_ATTEMPTS, MutatePlacementConfig, MutatePlacementError, MutatePlacementOperation,
    RealmPlacementMutation, is_management,
};
use crate::shard::pull::PullTopicsOperation;
use crate::shard::verify::ConvergeOperation;
use crate::shard::{AssembleManifestOperation, ManifestError, shard_answer, shard_call};
use crate::sync::outbox_record::outbox_stream_prefixes;

/// Fence close attempts before a persistent transaction conflict is reported.
const CLOSE_ATTEMPTS: usize = 4;
const OUTBOX_PAGE_SIZE: usize = 256;

fn realm_actor(realm_id: RealmId, node_id: NodeId) -> Actor {
    Actor {
        node_id,
        user_id: UserId::nil(realm_id),
        realm_id,
    }
}

/// One placement mutation of this node's own.
fn submit_call(realm_id: RealmId, local: NodeId, mutation: RealmPlacementMutation) -> IoCall {
    let config = MutatePlacementConfig {
        actor: realm_actor(realm_id, local),
        mutation,
    };
    IoCall::child(PlacementMutationOperation::new(config, None, None))
}

/// Whether a submitted mutation needs another pass.
fn submit_failed(result: IoResult) -> Result<bool, IoError> {
    Ok(
        match result.child::<RealmConfigDocument, MutatePlacementError>()? {
            Ok(_) => false,
            Err(error) => {
                warn!(error = %error, "Placement transition step did not apply");
                true
            }
        },
    )
}

/// An active plan none of whose incomplete buckets can ever apply: every one
/// was derived from an activation epoch the bucket has since left.
fn transition_stale(config: &RealmConfigDocument, transition: &PlacementTransition) -> bool {
    if !matches!(transition.status, TransitionStatus::Active) {
        return false;
    }
    let mut incomplete = 0usize;
    let mut stale = 0usize;
    for bucket in &transition.plan.buckets {
        if transition.completion(bucket.bucket).is_some() {
            continue;
        }
        incomplete += 1;
        if config
            .activation(&transition.plan.strategy_id, bucket.bucket)
            .is_some_and(|activation| activation.activation_epoch != bucket.predecessor_epoch)
        {
            stale += 1;
        }
    }
    incomplete > 0 && stale == incomplete
}

/// One bucket step this node owes: a mutation ready to batch, work still
/// blocked on a precondition, a stall report worth reducing while the retry
/// stays armed, or nothing left to do.
#[derive(Debug, PartialEq)]
enum StepPlan {
    Ready(RealmPlacementMutation),
    Stalled(RealmPlacementMutation),
    Pending,
    Done,
}

/// A bucket step that needs I/O, in the order the pass runs them.
#[derive(Debug, PartialEq)]
enum BucketWork {
    Drain(usize, PlacementRef),
    Barrier(usize, PlacementRef),
    Completion(usize, PlacementRef),
}

#[derive(Debug, PartialEq)]
enum PassStage {
    Activations,
    Expansions,
    Work,
    Submit,
    Kick,
}

type PassTurn = Result<Next<bool>, IoError>;

/// Runs every transition step the local node owns. Answers whether a step is
/// still outstanding, so the caller re-arms the placement retry.
#[derive(Debug, PartialEq)]
pub(crate) struct ProcessTransitionsOperation {
    realm_id: RealmId,
    local: NodeId,
    config: Arc<RealmConfigDocument>,
    now_ms: u64,
    stage: PassStage,
    pending: bool,
    departed: bool,
    /// Batched, to avoid one conflict-prone realm-config transaction per bucket and node.
    steps: Vec<RealmPlacementMutation>,
    /// Reversed, so the next step pops off the end.
    work: Vec<BucketWork>,
}

impl ProcessTransitionsOperation {
    pub(crate) fn new(realm_id: RealmId, local: NodeId, config: Arc<RealmConfigDocument>) -> Self {
        Self {
            realm_id,
            local,
            config,
            now_ms: 0,
            stage: PassStage::Activations,
            pending: false,
            departed: false,
            steps: Vec::new(),
            work: Vec::new(),
        }
    }

    fn plan_work(&mut self) {
        let (config, local) = (self.config.clone(), self.local);
        let mut work = Vec::new();
        for (index, transition) in config.placement_transitions.iter().enumerate() {
            if transition_stale(&config, transition) {
                // A plan whose incomplete buckets all have stale predecessors can
                // never apply; abort it so the strategy's in-flight slot frees.
                if is_management(&config, local) {
                    let id = transition.plan.transition_id;
                    self.steps.push(RealmPlacementMutation::AbortTransition(id));
                }
                continue;
            }
            // One window function decides execution and membership alike: only
            // admitted buckets run, and only their targets join topics.
            let admitted: Vec<u32> = (transition.plan.admitted_buckets(&transition.completed))
                .map(|bucket| bucket.bucket)
                .collect();
            for bucket in &transition.plan.buckets {
                let placement = PlacementRef {
                    strategy_id: transition.plan.strategy_id,
                    shard: bucket.bucket,
                };
                if let Some(completion) = transition.completion(bucket.bucket) {
                    let departing = bucket.old_holders.contains(&local)
                        && !bucket.target_holders.contains(&local);
                    self.departed |= departing;
                    // Past the bucket's grace, a departing holder owes a drain
                    // report before its retention may end.
                    let grace = transition.plan.limits.grace_ms;
                    if departing
                        && self.now_ms >= completion.completed_at_ms.saturating_add(grace)
                        && !transition.drain_reported(bucket.bucket, local)
                    {
                        work.push(BucketWork::Drain(index, placement));
                    }
                    continue;
                }
                // An observed abort stops this node from submitting anything
                // further: the un-cut buckets simply never complete.
                if !matches!(transition.status, TransitionStatus::Active) {
                    continue;
                }
                if !admitted.contains(&bucket.bucket) {
                    self.pending = true;
                    continue;
                }
                if bucket.old_holders.contains(&local) {
                    work.push(BucketWork::Barrier(index, placement));
                }
                if bucket.target_holders.contains(&local) {
                    work.push(BucketWork::Completion(index, placement));
                }
            }
        }
        work.reverse();
        self.work = work;
    }

    fn transition(&self, index: usize) -> Result<PlacementTransition, IoError> {
        (self.config.placement_transitions.get(index).cloned()).ok_or(IoError::Unexpected {
            expected: "a placement transition",
            got: "none",
        })
    }

    fn next_work(&mut self) -> PassTurn {
        self.stage = PassStage::Work;
        let (realm_id, local) = (self.realm_id, self.local);
        match self.work.pop() {
            Some(BucketWork::Drain(index, placement)) => {
                let transition = self.transition(index)?;
                let step = DrainStepOperation::new(realm_id, &transition, &placement, local);
                Ok(Next::Call(IoCall::child(step)))
            }
            Some(BucketWork::Barrier(index, placement)) => {
                let transition = self.transition(index)?;
                let step = BarrierStepOperation::new(realm_id, local, &transition, &placement);
                Ok(Next::Call(IoCall::child(step)))
            }
            Some(BucketWork::Completion(index, placement)) => {
                let transition = self.transition(index)?;
                let step = CompletionStepOperation::new(realm_id, local, &transition, &placement);
                Ok(Next::Call(IoCall::child(step)))
            }
            None => self.submit(),
        }
    }

    fn submit(&mut self) -> PassTurn {
        if self.steps.is_empty() {
            return self.kick();
        }
        self.stage = PassStage::Submit;
        let steps = std::mem::take(&mut self.steps);
        let submit = SubmitStepsOperation::new(realm_actor(self.realm_id, self.local), steps);
        Ok(Next::Call(IoCall::child(submit)))
    }

    /// Flushes before grace ends so writes accepted before cutover reach the topic.
    fn kick(&mut self) -> PassTurn {
        if !self.departed {
            return Ok(Next::Done(self.pending));
        }
        self.stage = PassStage::Kick;
        Ok(Next::Call(IoCall::child(KickDrainOperation::default())))
    }
}

impl Operation for ProcessTransitionsOperation {
    type Output = bool;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PassTurn {
        let run = ActivationsOperation::new(self.realm_id, self.local, self.config.clone());
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> PassTurn {
        match self.stage {
            PassStage::Activations => {
                self.pending = result.child::<bool, IoError>()??;
                self.stage = PassStage::Expansions;
                let run = ExpansionsOperation::new(self.realm_id, self.local, self.config.clone());
                Ok(Next::Call(IoCall::child(run)))
            }
            PassStage::Expansions => {
                self.pending |= result.child::<bool, IoError>()??;
                if self.config.placement_transitions.is_empty() {
                    return Ok(Next::Done(self.pending));
                }
                self.plan_work();
                self.next_work()
            }
            PassStage::Work => {
                match result.child::<StepPlan, IoError>()?? {
                    StepPlan::Ready(mutation) => self.steps.push(mutation),
                    // A stall is diagnostics: reduce it once and keep the retry armed.
                    StepPlan::Stalled(mutation) => {
                        self.steps.push(mutation);
                        self.pending = true;
                    }
                    StepPlan::Pending => self.pending = true,
                    StepPlan::Done => {}
                }
                self.next_work()
            }
            PassStage::Submit => {
                self.pending |= result.child::<bool, IoError>()??;
                self.kick()
            }
            PassStage::Kick => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Failed to scan document sync outbox");
                }
                Ok(Next::Done(self.pending))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum DrainStage {
    Close,
    Topic,
    Blocker,
}

/// Closes the bucket's write fence, then reports it drained once no journalled
/// eviction and no closed-generation record remains. The durable close comes
/// first: an empty scan alone proves nothing about a pre-cutover writer.
#[derive(Debug, PartialEq)]
struct DrainStepOperation {
    realm_id: RealmId,
    transition_id: ulid::Ulid,
    placement: PlacementRef,
    local: NodeId,
    closed: Option<u64>,
    stage: DrainStage,
}

impl DrainStepOperation {
    fn new(
        realm_id: RealmId,
        transition: &PlacementTransition,
        placement: &PlacementRef,
        local: NodeId,
    ) -> Self {
        let bucket = transition.plan.bucket_plan(placement.shard);
        Self {
            realm_id,
            transition_id: transition.plan.transition_id,
            placement: *placement,
            local,
            closed: bucket.map(|bucket| bucket.predecessor_epoch),
            stage: DrainStage::Close,
        }
    }
}

impl Operation for DrainStepOperation {
    type Output = StepPlan;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<StepPlan>, IoError> {
        let Some(closed) = self.closed else {
            return Ok(Next::Done(StepPlan::Done));
        };
        let close = CloseFenceOperation::new(self.realm_id, self.placement, closed);
        Ok(Next::Call(IoCall::child(close)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StepPlan>, IoError> {
        match self.stage {
            DrainStage::Close => {
                if let Err(error) = result.child::<(), IoError>()? {
                    debug!(error = %error, "Transition drain could not close the write fence");
                    return Ok(Next::Done(StepPlan::Pending));
                }
                self.stage = DrainStage::Topic;
                let topic = shard_topic_id(self.realm_id, &self.placement);
                Ok(Next::Call(shard_call(ShardNetEffect::Close(topic))))
            }
            DrainStage::Topic => {
                match shard_answer(result)? {
                    ShardNetEvent::Closed(Ok(true)) => return Ok(Next::Done(StepPlan::Pending)),
                    ShardNetEvent::Closed(Err(error)) => {
                        debug!(error = %error, "Transition drain could not close the shard topic");
                        return Ok(Next::Done(StepPlan::Pending));
                    }
                    _ => {}
                }
                self.stage = DrainStage::Blocker;
                let closed = self.closed.unwrap_or_default();
                let blocker = DrainBlockerOperation::new(&self.placement, closed);
                Ok(Next::Call(IoCall::child(blocker)))
            }
            DrainStage::Blocker => {
                let blocker = match result.child::<Option<DrainBlocker>, IoError>()? {
                    Ok(None) => {
                        return Ok(Next::Done(StepPlan::Ready(
                            RealmPlacementMutation::ReportDrained {
                                transition_id: self.transition_id,
                                bucket: self.placement.shard,
                                reported_by: self.local,
                            },
                        )));
                    }
                    Ok(Some(blocker)) => format!("{blocker:?}"),
                    Err(error) => error.to_string(),
                };
                debug!(
                    %blocker,
                    strategy_id = %self.placement.strategy_id,
                    bucket = self.placement.shard,
                    "Transition drain cannot report the bucket yet"
                );
                Ok(Next::Done(StepPlan::Pending))
            }
        }
    }
}

/// Why a bucket is not drained yet. Named so the order of the checks is
/// testable: an eviction converted between them enqueues replacement rows a
/// finished scan has already passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DrainBlocker {
    Eviction,
    OutboxRow,
}

/// The first reason `placement` cannot be reported drained through `closed`.
/// The eviction journal comes first: a scan that already passed a stream proves
/// nothing about rows the eviction hand-off commits afterwards.
#[derive(Debug, PartialEq)]
struct DrainBlockerOperation {
    placement: PlacementRef,
    closed: u64,
    /// Reversed, so the next stream pops off the end.
    prefixes: Vec<&'static [u8]>,
    pages: Option<PrefixPages>,
}

impl DrainBlockerOperation {
    fn new(placement: &PlacementRef, closed: u64) -> Self {
        let mut prefixes = outbox_stream_prefixes().to_vec();
        prefixes.reverse();
        Self {
            placement: *placement,
            closed,
            prefixes,
            pages: None,
        }
    }

    fn next_stream(&mut self) -> Result<Next<Option<DrainBlocker>>, IoError> {
        let Some(prefix) = self.prefixes.pop() else {
            return Ok(Next::Done(None));
        };
        let pages = PrefixPages::new(SYNC_OUTBOX_KEYSPACE, prefix.to_vec(), OUTBOX_PAGE_SIZE);
        let call = pages.call();
        self.pages = Some(pages);
        Ok(Next::Call(call))
    }
}

impl Operation for DrainBlockerOperation {
    type Output = Option<DrainBlocker>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Option<DrainBlocker>>, IoError> {
        Ok(Next::Call(shard_call(ShardNetEffect::EvictionPending(
            self.placement,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<DrainBlocker>>, IoError> {
        let Some(pages) = self.pages.as_mut() else {
            // No net handle means no eviction source, so nothing can be journalled.
            if matches!(shard_answer(result)?, ShardNetEvent::Flag(true)) {
                return Ok(Next::Done(Some(DrainBlocker::Eviction)));
            }
            return self.next_stream();
        };
        // An unfenced row carries generation zero and always counts.
        let blocked = pages.take(result)?.iter().any(|(_, row)| {
            postcard::from_bytes::<DocumentOutboxRecord>(row).is_ok_and(|record| {
                record.placement == self.placement && record.generation <= self.closed
            })
        });
        if blocked {
            return Ok(Next::Done(Some(DrainBlocker::OutboxRow)));
        }
        if pages.done() {
            return self.next_stream();
        }
        Ok(Next::Call(pages.call()))
    }
}

/// Freezes this old holder's frontier for the bucket, once.
#[derive(Debug, PartialEq)]
struct BarrierStepOperation {
    realm_id: RealmId,
    local: NodeId,
    transition_id: ulid::Ulid,
    placement: PlacementRef,
    reported: bool,
}

impl BarrierStepOperation {
    fn new(
        realm_id: RealmId,
        local: NodeId,
        transition: &PlacementTransition,
        placement: &PlacementRef,
    ) -> Self {
        let reported = transition
            .barriers
            .iter()
            .any(|barrier| barrier.bucket == placement.shard && barrier.reported_by == local);
        Self {
            realm_id,
            local,
            transition_id: transition.plan.transition_id,
            placement: *placement,
            reported,
        }
    }
}

impl Operation for BarrierStepOperation {
    type Output = StepPlan;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<StepPlan>, IoError> {
        if self.reported {
            return Ok(Next::Done(StepPlan::Done));
        }
        let assemble = AssembleManifestOperation::new(self.realm_id, self.placement);
        Ok(Next::Call(IoCall::child(assemble)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StepPlan>, IoError> {
        Ok(Next::Done(
            match result.child::<ShardManifest, ManifestError>()? {
                Ok(manifest) => StepPlan::Ready(RealmPlacementMutation::ReportBarrier {
                    transition_id: self.transition_id,
                    bucket: self.placement.shard,
                    reported_by: self.local,
                    frontier: manifest.cursor,
                }),
                Err(error) => {
                    debug!(error = %error, "Cannot assemble a transition barrier frontier yet");
                    StepPlan::Pending
                }
            },
        ))
    }
}

#[derive(Debug, PartialEq)]
enum CompletionStage {
    Exists,
    Pull,
    Converge,
    Root,
    Activation([u8; 32]),
    Sign,
}

type CompletionTurn = Result<Next<StepPlan>, IoError>;

/// Joins, pulls, verifies, and proves the bucket from the target side.
#[derive(Debug, PartialEq)]
struct CompletionStepOperation {
    realm_id: RealmId,
    local: NodeId,
    transition: PlacementTransition,
    placement: PlacementRef,
    sources: Vec<NodeId>,
    required: irokle::ActorClock,
    stage: CompletionStage,
}

impl CompletionStepOperation {
    fn new(
        realm_id: RealmId,
        local: NodeId,
        transition: &PlacementTransition,
        placement: &PlacementRef,
    ) -> Self {
        Self {
            realm_id,
            local,
            transition: transition.clone(),
            placement: *placement,
            sources: Vec::new(),
            required: irokle::ActorClock::default(),
            stage: CompletionStage::Exists,
        }
    }

    fn topic(&self) -> irokle::TopicId {
        shard_topic_id(self.realm_id, &self.placement)
    }

    fn root_call(&mut self) -> CompletionTurn {
        self.stage = CompletionStage::Root;
        Ok(Next::Call(shard_call(ShardNetEffect::FrontierRoot {
            topic: self.topic(),
            frontier: self.required.clone(),
        })))
    }

    /// Joins the barriers of the old holders; an undecodable one leaves the bucket pending.
    fn join_barriers(&mut self) -> Result<bool, IoError> {
        let shard = self.placement.shard;
        let Some(bucket) = self.transition.plan.bucket_plan(shard) else {
            return Ok(false);
        };
        let barriers = (self.transition.barriers.iter())
            .filter(|barrier| barrier.bucket == shard)
            .filter(|barrier| bucket.old_holders.contains(&barrier.reported_by));
        for barrier in barriers {
            let Ok(frontier) = postcard::from_bytes::<irokle::ActorClock>(&barrier.frontier) else {
                debug!(
                    reported_by = %barrier.reported_by,
                    "Undecodable barrier frontier; leaving the bucket pending"
                );
                return Ok(false);
            };
            self.required.merge(&frontier);
        }
        Ok(true)
    }

    fn verify(&mut self) -> CompletionTurn {
        if !self.join_barriers()? {
            return Ok(Next::Done(StepPlan::Pending));
        }
        // A source-less bucket still binds its fenced frontier and topic genesis,
        // not its moving live fingerprint.
        if self.sources.is_empty() {
            return self.root_call();
        }
        // Proofs dominate all reported fenced frontiers, including unreachable pull sources.
        self.stage = CompletionStage::Converge;
        let converge = ConvergeOperation::new(
            self.realm_id,
            self.placement,
            &self.sources,
            Some(self.required.clone()),
        );
        Ok(Next::Call(IoCall::child(converge)))
    }

    /// Surfaces the bounded failure once per reporter; the reduced record keeps the
    /// report idempotent across retries.
    fn stalled(&self) -> StepPlan {
        let shard = self.placement.shard;
        debug!(
            strategy = %self.placement.strategy_id,
            shard,
            "No old holder served a verifiable shard copy yet"
        );
        let reported = (self.transition.stalls.iter())
            .any(|stall| stall.bucket == shard && stall.reported_by == self.local);
        if reported {
            return StepPlan::Pending;
        }
        StepPlan::Stalled(RealmPlacementMutation::ReportStall {
            transition_id: self.transition.plan.transition_id,
            bucket: shard,
            reported_by: self.local,
            reason: "no old holder served a verifiable copy".to_string(),
        })
    }

    fn sign(&mut self, root: [u8; 32], config: &RealmConfigDocument) -> CompletionTurn {
        let shard = self.placement.shard;
        let transition = &self.transition;
        let activation = config
            .activation(&self.placement.strategy_id, shard)
            .map(|activation| activation.activation_epoch);
        let predecessor =
            (transition.plan.bucket_plan(shard)).map(|bucket| bucket.predecessor_epoch);
        // Never sign a tuple the reducer will ignore: the stored activation must
        // still be the epoch this plan moves from.
        let (Some(activation), true) = (activation, activation == predecessor) else {
            return Ok(Next::Done(StepPlan::Pending));
        };
        let claim = ProofClaim {
            realm_id: self.realm_id,
            transition_id: transition.plan.transition_id,
            strategy_id: transition.plan.strategy_id,
            bucket: shard,
            old_activation_epoch: activation,
            target_map_epoch: transition.plan.target_map_epoch,
            barrier_digest: transition.barrier_digest(shard),
            checkpoint_root: root,
            holder: self.local,
        };
        self.stage = CompletionStage::Sign;
        Ok(Next::Call(shard_call(ShardNetEffect::SignProof(claim))))
    }
}

impl Operation for CompletionStepOperation {
    type Output = StepPlan;
    type Error = IoError;

    fn first(&mut self) -> CompletionTurn {
        let (shard, local) = (self.placement.shard, self.local);
        let proved = self
            .transition
            .proofs_for(shard)
            .any(|proof| proof.holder == local && self.transition.proof_valid(shard, proof));
        let Some(bucket) = self.transition.plan.bucket_plan(shard).filter(|_| !proved) else {
            return Ok(Next::Done(StepPlan::Done));
        };
        // Until every old holder has fenced there is no reference frontier, so a
        // proof would attest to a moving target.
        if !self
            .transition
            .barrier_established(shard, &bucket.old_holders)
        {
            return Ok(Next::Done(StepPlan::Pending));
        }
        self.sources = (bucket.old_holders.iter().copied())
            .filter(|holder| *holder != local)
            .collect();
        if self.sources.is_empty() {
            return self.verify();
        }
        Ok(Next::Call(shard_call(ShardNetEffect::Existing(vec![
            self.topic(),
        ]))))
    }

    fn next(&mut self, result: IoResult) -> CompletionTurn {
        match std::mem::replace(&mut self.stage, CompletionStage::Sign) {
            CompletionStage::Exists => match shard_answer(result)? {
                ShardNetEvent::Existing(topics) if !topics.contains(&self.topic()) => {
                    // Join-only: this adopts an existing genesis and can never mint one.
                    self.stage = CompletionStage::Pull;
                    let pull = PullTopicsOperation::new(vec![self.topic()], self.sources.clone());
                    Ok(Next::Call(IoCall::child(pull)))
                }
                ShardNetEvent::Existing(_) => self.verify(),
                _ => Ok(Next::Done(StepPlan::Pending)),
            },
            CompletionStage::Pull => {
                drop(result.child::<bool, IoError>()?);
                self.verify()
            }
            CompletionStage::Converge => {
                match result.child::<Option<(NodeId, [u8; 32])>, IoError>()?? {
                    Some(_) => self.root_call(),
                    None => Ok(Next::Done(self.stalled())),
                }
            }
            CompletionStage::Root => match shard_answer(result)? {
                ShardNetEvent::Root(Ok(root)) => {
                    self.stage = CompletionStage::Activation(root);
                    let read = GetConfigOperation::new(self.realm_id);
                    Ok(Next::Call(IoCall::child(read)))
                }
                ShardNetEvent::Root(Err(error)) => {
                    debug!(error = %error, "Cannot hash the transition checkpoint frontier");
                    Ok(Next::Done(StepPlan::Pending))
                }
                _ => Ok(Next::Done(StepPlan::Pending)),
            },
            // The activation is read back, so the proof commits to the epoch the
            // reducer will compare it against.
            CompletionStage::Activation(root) => {
                match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => self.sign(root, &config),
                    Err(error) => {
                        warn!(error = %error, "realm config unavailable");
                        Ok(Next::Done(StepPlan::Pending))
                    }
                }
            }
            CompletionStage::Sign => Ok(Next::Done(match shard_answer(result)? {
                ShardNetEvent::Proof(proof) => {
                    StepPlan::Ready(RealmPlacementMutation::SubmitCompletion {
                        transition_id: self.transition.plan.transition_id,
                        strategy_id: self.transition.plan.strategy_id,
                        proof,
                    })
                }
                _ => StepPlan::Pending,
            })),
        }
    }
}

/// Commits one pass's gathered steps in a single transaction; answers whether they failed.
#[derive(Debug, PartialEq)]
struct SubmitStepsOperation {
    actor: Actor,
    steps: Vec<RealmPlacementMutation>,
    attempts: usize,
    waiting: bool,
}

impl SubmitStepsOperation {
    fn new(actor: Actor, steps: Vec<RealmPlacementMutation>) -> Self {
        Self {
            actor,
            steps,
            attempts: 0,
            waiting: false,
        }
    }

    fn submit(&mut self) -> Next<bool> {
        self.waiting = false;
        let batch = MutatePlacementOperation::batch(self.actor.clone(), self.steps.clone());
        Next::Call(IoCall::child(batch))
    }
}

impl Operation for SubmitStepsOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(self.submit())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        if self.waiting {
            return Ok(self.submit());
        }
        match result.child::<RealmConfigDocument, MutatePlacementError>()? {
            Ok(_) => Ok(Next::Done(false)),
            Err(MutatePlacementError::StorageError(StorageError::TransactionConflict))
                if self.attempts < CONFLICT_ATTEMPTS =>
            {
                let wait = conflict_backoff(self.attempts, self.actor.node_id.as_bytes());
                self.attempts += 1;
                self.waiting = true;
                Ok(Next::Call(IoCall::Delay(wait)))
            }
            Err(error) => {
                warn!(error = %error, "Placement transition steps did not apply");
                Ok(Next::Done(true))
            }
        }
    }
}

/// Starts the successor expansion for a strategy whose activations trail the
/// newest map once its active transition is terminal. Only a management node
/// issues the plan; everyone else reports nothing and their timer keeps watching.
#[derive(Debug, PartialEq)]
struct ExpansionsOperation {
    realm_id: RealmId,
    local: NodeId,
    config: Arc<RealmConfigDocument>,
    now_ms: u64,
    pending: bool,
    /// Reversed, so the next start pops off the end.
    starts: Vec<RealmPlacementMutation>,
}

impl ExpansionsOperation {
    fn new(realm_id: RealmId, local: NodeId, config: Arc<RealmConfigDocument>) -> Self {
        Self {
            realm_id,
            local,
            config,
            now_ms: 0,
            pending: false,
            starts: Vec::new(),
        }
    }

    fn plan_starts(&mut self) {
        let (config, local) = (self.config.clone(), self.local);
        let Some(epoch) = config.newest_map_epoch() else {
            return;
        };
        let Some(map) = config.candidate_map(epoch) else {
            return;
        };
        // Management nodes race and the predecessor gate coalesces the winner; a
        // non-management issuer would only ever be rejected by its peers.
        if !is_management(&config, local) {
            return;
        }
        let transitions = &config.placement_transitions;
        for strategy in &config.strategies {
            let strategy_id = strategy.strategy_id;
            let mut own = transitions
                .iter()
                .filter(|t| t.plan.strategy_id == strategy_id);
            // Onboarding owns the first expansion; requiring its record prevents
            // duplicate issuance.
            if own.clone().next().is_none() || own.any(|t| !t.is_terminal()) {
                continue;
            }
            let Ok(buckets) = expansion_buckets(&config, strategy_id, epoch) else {
                continue;
            };
            if buckets.is_empty() {
                continue;
            }
            self.pending = true;
            let placement = PlacementRef {
                strategy_id,
                shard: 0,
            };
            // A map that cannot resolve this strategy's holders cannot plan it; a
            // newer publication resolves that, so just stay pending.
            if holders_in_map(&config, strategy, &placement, map).is_none() {
                continue;
            }
            let request = TransitionRequest {
                transition_id: ulid::Ulid::generate(),
                strategy_id,
                buckets,
                target_map_epoch: epoch,
                // Expansion moves nothing off a holder; every bucket may run.
                limits: TransitionLimits {
                    max_incomplete_buckets: u32::MAX,
                    ..Default::default()
                },
                created_by: local,
                created_at_ms: self.now_ms,
            };
            match plan_transition(&config, request) {
                Ok(plan) => self
                    .starts
                    .push(RealmPlacementMutation::StartTransition(plan)),
                Err(error) => debug!(error = %error, "Successor expansion could not plan yet"),
            }
        }
        self.starts.reverse();
    }

    fn next_start(&mut self) -> Next<bool> {
        match self.starts.pop() {
            Some(start) => Next::Call(submit_call(self.realm_id, self.local, start)),
            None => Next::Done(self.pending),
        }
    }
}

impl Operation for ExpansionsOperation {
    type Output = bool;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        self.plan_starts();
        Ok(self.next_start())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        self.pending |= submit_failed(result)?;
        Ok(self.next_start())
    }
}

/// The activations `config` can issue, whether work stays open, and whether some
/// strategy is missing from the newest map, which only a successor map can freeze.
fn plan_activations(
    config: &RealmConfigDocument,
    local: NodeId,
) -> (Vec<RealmPlacementMutation>, bool, bool) {
    let mut planned = (Vec::new(), false, false);
    let Some((epoch, map)) =
        (config.newest_map_epoch()).and_then(|epoch| Some((epoch, config.candidate_map(epoch)?)))
    else {
        return planned;
    };
    // The activation record is an immutable value, so concurrent management
    // issuers coalesce; a non-management issuer never lands at all.
    if !is_management(config, local) {
        return planned;
    }
    for strategy in &config.strategies {
        let strategy_id = strategy.strategy_id;
        if config.activation(&strategy_id, 0).is_some() {
            continue;
        }
        let placement = PlacementRef {
            strategy_id,
            shard: 0,
        };
        // A map without this strategy's selector cannot activate it; the
        // successor map is what freezes it.
        if holders_in_map(config, strategy, &placement, map).is_none() {
            planned.1 = true;
            planned.2 |= map.selector(&strategy_id).is_none();
            continue;
        }
        planned
            .0
            .push(RealmPlacementMutation::InitializeActivations {
                strategy_id,
                candidate_map_epoch: epoch,
            });
    }
    planned.0.reverse();
    planned
}

#[derive(Debug, PartialEq)]
enum ActivationStage {
    Activate,
    Publish,
    Reread,
    Settle,
}

/// Activates the newest map for a strategy that has none, publishing the
/// successor map first when that map predates the strategy. Only a management
/// node issues either; concurrent duplicates coalesce as immutable values.
#[derive(Debug, PartialEq)]
struct ActivationsOperation {
    realm_id: RealmId,
    local: NodeId,
    config: Arc<RealmConfigDocument>,
    stage: ActivationStage,
    pending: bool,
    unfrozen: bool,
    /// Reversed, so the next activation pops off the end.
    activations: Vec<RealmPlacementMutation>,
}

impl ActivationsOperation {
    fn new(realm_id: RealmId, local: NodeId, config: Arc<RealmConfigDocument>) -> Self {
        Self {
            realm_id,
            local,
            config,
            stage: ActivationStage::Activate,
            pending: false,
            unfrozen: false,
            activations: Vec::new(),
        }
    }

    fn next_activation(&mut self) -> Result<Next<bool>, IoError> {
        if let Some(activation) = self.activations.pop() {
            let submit = submit_call(self.realm_id, self.local, activation);
            return Ok(Next::Call(submit));
        }
        if self.stage == ActivationStage::Settle || !self.unfrozen {
            return Ok(Next::Done(self.pending));
        }
        self.publish()
    }

    /// Freezes a strategy the newest map predates into its successor. The epoch is
    /// derived byte-identically, so a second issuer's copy coalesces rather than
    /// conflicting; admission rejects an occupied epoch.
    fn publish(&mut self) -> Result<Next<bool>, IoError> {
        self.pending = true;
        let publisher = self.local.to_string();
        if map_publisher(&self.config) != Some(publisher.as_str()) {
            return Ok(Next::Done(true));
        }
        let epoch = self.config.newest_map_epoch().unwrap_or_default();
        self.stage = ActivationStage::Publish;
        let map = self.config.freeze_map(epoch + 1);
        let publish = RealmPlacementMutation::PublishCandidateMap(map);
        Ok(Next::Call(submit_call(self.realm_id, self.local, publish)))
    }
}

impl Operation for ActivationsOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let (activations, pending, unfrozen) = plan_activations(&self.config, self.local);
        self.activations = activations;
        self.pending = pending;
        self.unfrozen = unfrozen;
        self.next_activation()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        match self.stage {
            ActivationStage::Activate => {
                self.pending |= submit_failed(result)?;
                self.next_activation()
            }
            ActivationStage::Publish => {
                if submit_failed(result)? {
                    return Ok(Next::Done(true));
                }
                // Activations resolve in this pass: the successor map is already durable.
                self.stage = ActivationStage::Reread;
                let read = GetConfigOperation::new(self.realm_id);
                Ok(Next::Call(IoCall::child(read)))
            }
            ActivationStage::Reread => {
                match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => {
                        self.activations = plan_activations(&config, self.local).0;
                        self.stage = ActivationStage::Settle;
                        self.next_activation()
                    }
                    Err(error) => {
                        warn!(error = %error, "realm config unavailable");
                        Ok(Next::Done(true))
                    }
                }
            }
            ActivationStage::Settle => {
                submit_failed(result)?;
                self.next_activation()
            }
        }
    }
}

/// The realm's candidate-map issuer: the lowest-id Management node. Publishing
/// from every Management node risks divergent values at one epoch, which stays
/// permanently unusable. A removed issuer hands the role to the next node.
fn map_publisher(config: &RealmConfigDocument) -> Option<&str> {
    config
        .nodes
        .iter()
        .filter(|node| {
            matches!(
                node.kind,
                aruna_core::structs::identity::realm::RealmNodeKind::Management
            )
        })
        .map(|node| node.node_id.as_str())
        .min()
}

/// One close attempt in its own write transaction.
#[derive(Debug, PartialEq)]
struct FenceAttemptOperation {
    key_space: String,
    key: Key,
    placement: PlacementRef,
    generation: u64,
    writing: bool,
}

impl Operation for FenceAttemptOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(read_row(&self.key_space, self.key.clone())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.writing {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let current = stored_generation(value(result)?.as_ref());
        if current.is_some_and(|current| current >= self.generation) {
            return Ok(Next::Done(()));
        }
        if current.is_none() {
            // Unparseable bytes admit nothing, so leaving them would brick the
            // bucket for good while this close reported success.
            warn!(placement = ?self.placement, "Rewriting an unparseable placement write fence");
        }
        self.writing = true;
        Ok(Next::Call(IoCall::Write(vec![(
            self.key_space.clone(),
            self.key.clone(),
            self.generation.to_be_bytes().to_vec().into(),
        )])))
    }
}

/// Durably closes `placement` through `generation`, so no later transaction is
/// admitted and every uncommitted one conflicts. Monotone and idempotent; an
/// unparseable stored value is overwritten rather than bricking the bucket.
#[derive(Debug, PartialEq)]
pub struct CloseFenceOperation {
    realm_id: RealmId,
    placement: PlacementRef,
    generation: u64,
    attempts: usize,
}

impl CloseFenceOperation {
    pub fn new(realm_id: RealmId, placement: PlacementRef, generation: u64) -> Self {
        Self {
            realm_id,
            placement,
            generation,
            attempts: 0,
        }
    }

    fn attempt(&mut self) -> Next<()> {
        self.attempts += 1;
        let (key_space, key) = fence_read(&self.realm_id, &self.placement);
        Next::Call(IoCall::child(FenceAttemptOperation {
            key_space,
            key,
            placement: self.placement,
            generation: self.generation,
            writing: false,
        }))
    }
}

impl Operation for CloseFenceOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(self.attempt())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match result.child::<(), IoError>()? {
            Err(IoError::Conflict) if self.attempts < CLOSE_ATTEMPTS => Ok(self.attempt()),
            outcome => outcome.map(Next::Done),
        }
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
    };
    use aruna_core::placement::fence;
    use aruna_core::reducer::{AdminDocumentState, overlay_placement};
    use std::path::Path;

    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use aruna_core::structs::placement::record::PlacementStrategy;
    use aruna_core::structs::placement::transition::BucketCompletion;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use tempfile::tempdir;
    use ulid::Ulid;

    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::realm::load_config::config_unavailable;
    use byteview::ByteView;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn actor(realm_id: RealmId, node_id: NodeId) -> Actor {
        Actor {
            node_id,
            user_id: UserId::nil(realm_id),
            realm_id,
        }
    }

    fn context(root: &str) -> Arc<DriverContext> {
        Arc::new(DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        })
    }

    /// A drain consults the eviction journal through the net handle, so a
    /// context without one never exercises that gate.
    async fn net_context(root: &Path) -> (Arc<DriverContext>, NetHandle) {
        let storage_handle =
            aruna_storage::FjallStorage::open(root.join("storage").to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                sync_storage_path: Some(root.join("document-sync")),
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let context = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        (context, net_handle)
    }

    /// One journalled eviction whose re-emitted row would land on `placement`,
    /// authored locally so the decoder keeps it.
    fn local_eviction(net_handle: &NetHandle, placement: PlacementRef) -> irokle::TopicEviction {
        let event_id = Ulid::from_bytes([44; 16]);
        let event = aruna_core::document::DocumentEvent::Delete {
            event_id,
            target: DocumentTarget::MetadataDocumentLifecycle {
                document_id: event_id,
            },
            change: aruna_core::document::DocumentChange {
                base: None,
                current: aruna_core::document::DocumentSyncRevision {
                    generation: 0,
                    event_id,
                    actor: node(1),
                    updated_at_ms: 1,
                },
                kind: aruna_core::document::DocumentChangeKind::Delete,
                placement,
            },
        };
        let topic_id = irokle::TopicId::from_bytes([44; 32]);
        let author = net_handle.document_sync_node().peer_id();
        irokle::TopicEviction {
            topic_id,
            losing_genesis: irokle::OpId::from_bytes([45; 32]),
            winning_genesis: irokle::OpId::from_bytes([46; 32]),
            evicted: vec![irokle::EvictedOp {
                op_id: irokle::OpId::from_bytes([47; 32]),
                actor_id: irokle::actor_id_for(topic_id, author),
                author,
                actor_seq: 2,
                payload: irokle::TopicPayload::Event(
                    irokle::EventEnvelope::encode_event(&event).expect("the event encodes"),
                ),
            }],
        }
    }

    fn strategy(seed: u8, name: &str) -> PlacementStrategy {
        PlacementStrategy {
            strategy_id: Ulid::from_bytes([seed; 16]),
            name: name.to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 4,
        }
    }

    /// Map epoch one freezes the first strategy only; the second is created
    /// after it, so nothing in the realm carries its frozen selector.
    fn late_strategy_config(realm_id: RealmId) -> (RealmConfigDocument, Ulid) {
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        for seed in [1u8, 2] {
            document.ensure_node(node(seed), RealmNodeKind::Management);
        }
        document.strategies.push(strategy(5, "first"));
        document.default_strategy_id = Some(document.strategies[0].strategy_id);
        document.snapshot_candidate_map();
        let late = strategy(6, "late");
        document.strategies.push(late.clone());
        (document, late.strategy_id)
    }

    async fn store_config(context: &DriverContext, document: &RealmConfigDocument) {
        let target = DocumentTarget::RealmConfig {
            realm_id: document.realm_id,
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: document
                    .to_bytes(&actor(document.realm_id, node(1)))
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn load_config(context: &Arc<DriverContext>, realm_id: RealmId) -> RealmConfigDocument {
        drive(GetConfigOperation::new(realm_id), context)
            .await
            .inspect_err(config_unavailable)
            .ok()
            .expect("the realm config is stored")
    }

    #[tokio::test]
    async fn late_strategy_mapped() {
        // One pass publishes the successor map and activates from it: leaving
        // the activation to the retry interval strands the work for a restart.
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([61; 32]);
        let (document, late) = late_strategy_config(realm_id);
        store_config(&context, &document).await;
        assert!(
            document
                .candidate_map(1)
                .expect("epoch one is usable")
                .selector(&late)
                .is_none()
        );

        let issuer = map_publisher(&document)
            .expect("a management issuer")
            .to_string();
        let publisher = [node(1), node(2)]
            .into_iter()
            .find(|candidate| candidate.to_string() == issuer)
            .expect("the issuer is a configured node");
        assert!(
            drive(
                ProcessTransitionsOperation::new(realm_id, publisher, Arc::new(document.clone())),
                &context
            )
            .await
            .unwrap()
        );

        // One successor map, carrying the missing frozen selector, plus every
        // activation it unblocks.
        let published = load_config(&context, realm_id).await;
        assert_eq!(published.newest_map_epoch(), Some(2));
        assert!(
            published
                .candidate_map(2)
                .expect("the successor is usable")
                .selector(&late)
                .is_some()
        );
        for shard in 0..4 {
            assert_eq!(
                published
                    .activation(&late, shard)
                    .map(|activation| activation.candidate_map_epoch),
                Some(2),
                "the publishing pass must activate from its own successor map"
            );
        }

        // Repeating the pass publishes no further epoch.
        drive(
            ProcessTransitionsOperation::new(realm_id, publisher, Arc::new(published.clone())),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(
            load_config(&context, realm_id).await.newest_map_epoch(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn terminal_starts_successor() {
        // A second join published during an active expansion must wait, then
        // start exactly one successor against the newest map.
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([63; 32]);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(node(1), RealmNodeKind::Management);
        let mut expansion = strategy(7, "everywhere");
        expansion.replica_count = None;
        expansion.shard_count = 1;
        document.strategies.push(expansion.clone());
        document.default_strategy_id = Some(expansion.strategy_id);
        assert_eq!(document.snapshot_candidate_map(), 1);

        document.ensure_node(node(2), RealmNodeKind::Management);
        document.candidate_maps.push(document.freeze_map(2));
        let previous_id = Ulid::from_bytes([8; 16]);
        let previous = plan_transition(
            &document,
            TransitionRequest {
                transition_id: previous_id,
                strategy_id: expansion.strategy_id,
                buckets: Vec::new(),
                target_map_epoch: 2,
                limits: Default::default(),
                created_by: node(1),
                created_at_ms: 1,
            },
        )
        .unwrap();
        document
            .placement_transitions
            .push(PlacementTransition::new(previous));
        document.ensure_node(node(3), RealmNodeKind::Management);
        document.candidate_maps.push(document.freeze_map(3));
        store_config(&context, &document).await;

        let issuer = [node(1), node(2), node(3)]
            .into_iter()
            .find(|candidate| {
                candidate.to_string() == map_publisher(&document).expect("a management issuer")
            })
            .expect("the issuer is a configured node");
        assert!(
            !drive(
                ExpansionsOperation::new(realm_id, issuer, Arc::new(document.clone())),
                &context
            )
            .await
            .unwrap()
        );
        assert_eq!(
            load_config(&context, realm_id)
                .await
                .placement_transitions
                .len(),
            1
        );

        document.placement_transitions[0]
            .completed
            .push(BucketCompletion {
                bucket: 0,
                completed_at_ms: 1,
            });
        let activation = document
            .placement_activations
            .first_mut()
            .expect("the first map activated the bucket");
        activation.activation_epoch = 2;
        activation.candidate_map_epoch = 2;
        activation.transition_id = None;
        store_config(&context, &document).await;

        assert!(
            drive(
                ExpansionsOperation::new(realm_id, issuer, Arc::new(document.clone())),
                &context
            )
            .await
            .unwrap()
        );
        let started = load_config(&context, realm_id).await;
        let successors: Vec<_> = started
            .placement_transitions
            .iter()
            .filter(|transition| transition.plan.transition_id != previous_id)
            .collect();
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].plan.target_map_epoch, 3);
        let bucket = successors[0]
            .plan
            .bucket_plan(0)
            .expect("the successor covers the expanded bucket");
        assert_eq!(bucket.old_holders.len(), 2);
        assert_eq!(bucket.target_holders.len(), 3);
        assert!(bucket.target_holders.contains(&node(2)));
        assert!(bucket.target_holders.contains(&node(3)));

        assert!(
            !drive(
                ExpansionsOperation::new(realm_id, issuer, Arc::new(started.clone())),
                &context
            )
            .await
            .unwrap()
        );
        assert_eq!(
            load_config(&context, realm_id)
                .await
                .placement_transitions
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn one_issuer_publishes() {
        // Every Management node reconciles, but only the deterministic issuer
        // may publish: a rival value at one epoch makes it unusable forever.
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([62; 32]);
        let (document, _late) = late_strategy_config(realm_id);
        store_config(&context, &document).await;

        let issuer = map_publisher(&document)
            .expect("a management issuer")
            .to_string();
        let other = [node(1), node(2)]
            .into_iter()
            .find(|candidate| candidate.to_string() != issuer)
            .expect("two management nodes");

        assert!(
            drive(
                ProcessTransitionsOperation::new(realm_id, other, Arc::new(document.clone())),
                &context
            )
            .await
            .unwrap()
        );
        assert_eq!(
            load_config(&context, realm_id).await.newest_map_epoch(),
            Some(1)
        );
    }

    /// Bucket zero handed from the local node to a peer, cut over and past its
    /// grace, so the departing holder owes a drain report.
    fn departing_config(realm_id: RealmId) -> (RealmConfigDocument, PlacementRef) {
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(node(1), RealmNodeKind::Server);
        document.ensure_node(node(2), RealmNodeKind::Server);
        document.strategies.push(strategy(5, "first"));
        document.default_strategy_id = Some(document.strategies[0].strategy_id);
        document.snapshot_candidate_map();
        let strategy_id = document.strategies[0].strategy_id;
        let placement = PlacementRef {
            strategy_id,
            shard: 0,
        };
        let mut transition =
            PlacementTransition::new(aruna_core::structs::placement::transition::TransitionPlan {
                transition_id: Ulid::from_bytes([8; 16]),
                strategy_id,
                buckets: vec![aruna_core::structs::placement::transition::BucketPlan {
                    bucket: 0,
                    old_holders: vec![node(1)],
                    target_holders: vec![node(2)],
                    predecessor_epoch: 1,
                }],
                target_map_epoch: 1,
                limits: aruna_core::structs::placement::transition::TransitionLimits {
                    max_incomplete_buckets: 1,
                    grace_ms: 0,
                },
                created_by: node(2),
                created_at_ms: 1,
            });
        transition.completed.push(
            aruna_core::structs::placement::transition::BucketCompletion {
                bucket: 0,
                completed_at_ms: 1,
            },
        );
        document.placement_transitions.push(transition);
        (document, placement)
    }

    async fn write_outbox_row(
        context: &DriverContext,
        placement: PlacementRef,
        generation: u64,
        seed: u8,
    ) -> Vec<u8> {
        let change = aruna_core::document::DocumentChange {
            base: None,
            current: aruna_core::document::DocumentSyncRevision {
                generation,
                event_id: Ulid::from_bytes([seed; 16]),
                actor: node(1),
                updated_at_ms: 1,
            },
            kind: aruna_core::document::DocumentChangeKind::Delete,
            placement,
        };
        let record = crate::sync::outbox_record::new_identified_record(
            Ulid::from_bytes([seed; 16]),
            node(1),
            DocumentTarget::MetadataDocumentLifecycle {
                document_id: Ulid::from_bytes([seed; 16]),
            },
            vec![node(2)],
            aruna_core::document::DocumentOutboxEvent::Delete { change },
            placement,
            false,
        )
        .fenced_at(generation);
        let (key_space, key, value) =
            crate::sync::outbox_record::outbox_write_entry(&record).expect("the row encodes");
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key: key.clone(),
                value,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        key.to_vec()
    }

    #[tokio::test]
    async fn drain_fences_first() {
        // The close comes first, a predecessor-generation row blocks the
        // report, and a successor-generation row never does.
        let directory = tempdir().unwrap();
        let (context, _net_handle) = net_context(directory.path()).await;
        let realm_id = RealmId::from_bytes([64; 32]);
        let (document, placement) = departing_config(realm_id);
        let transition = &document.placement_transitions[0];
        let predecessor = write_outbox_row(&context, placement, 1, 21).await;
        write_outbox_row(&context, placement, 2, 22).await;

        let blocked = drive(
            DrainStepOperation::new(realm_id, transition, &placement, node(1)),
            &context,
        )
        .await
        .unwrap();
        assert!(
            matches!(blocked, StepPlan::Pending),
            "an undrained predecessor row blocks the report"
        );
        assert_eq!(closed_generation(&context, realm_id, &placement).await, 1);

        crate::sync::document_outbox::delete_outbox_records(
            &context.storage_handle,
            vec![predecessor],
        )
        .await
        .expect("the row is deleted");
        let reported = drive(
            DrainStepOperation::new(realm_id, transition, &placement, node(1)),
            &context,
        )
        .await
        .unwrap();
        assert!(
            matches!(
                reported,
                StepPlan::Ready(RealmPlacementMutation::ReportDrained { bucket: 0, .. })
            ),
            "a successor-generation row must not block the report"
        );
    }

    #[tokio::test]
    async fn eviction_blocks_first() {
        // The eviction hand-off commits its rows before it releases the entry,
        // so a scan that ran first would pass a stream the hand-off then fills.
        let directory = tempdir().unwrap();
        let (context, net_handle) = net_context(directory.path()).await;
        let realm_id = RealmId::from_bytes([66; 32]);
        let (document, placement) = departing_config(realm_id);
        store_config(&context, &document).await;
        write_outbox_row(&context, placement, 1, 23).await;
        net_handle
            .consume_eviction(local_eviction(&net_handle, placement))
            .await
            .expect("the eviction is journalled");

        assert_eq!(
            drive(DrainBlockerOperation::new(&placement, 1), &context)
                .await
                .unwrap(),
            Some(DrainBlocker::Eviction),
            "the journal must be consulted before the outbox scan"
        );
    }

    #[tokio::test]
    async fn eviction_scopes_bucket() {
        // A journalled eviction for another bucket must not stall this one.
        let directory = tempdir().unwrap();
        let (context, net_handle) = net_context(directory.path()).await;
        let realm_id = RealmId::from_bytes([67; 32]);
        let (document, placement) = departing_config(realm_id);
        store_config(&context, &document).await;
        let transition = &document.placement_transitions[0];
        let elsewhere = PlacementRef {
            strategy_id: placement.strategy_id,
            shard: 2,
        };
        net_handle
            .consume_eviction(local_eviction(&net_handle, elsewhere))
            .await
            .expect("the eviction is journalled");

        assert!(net_handle.eviction_pending(&elsewhere));
        let reported = drive(
            DrainStepOperation::new(realm_id, transition, &placement, node(1)),
            &context,
        )
        .await
        .unwrap();
        assert!(
            matches!(
                reported,
                StepPlan::Ready(RealmPlacementMutation::ReportDrained { bucket: 0, .. })
            ),
            "an unrelated bucket's eviction must not block the report"
        );
    }

    #[tokio::test]
    async fn dead_holder_retained() {
        // Removing a dead holder from the realm must never stand in for its
        // drain report: the record keeps retaining it.
        let realm_id = RealmId::from_bytes([65; 32]);
        let (mut document, _placement) = departing_config(realm_id);
        document
            .nodes
            .retain(|entry| entry.node_id != node(1).to_string());
        document
            .placement_map
            .retain(|entry| entry.node_id != node(1));

        assert!(!document.placement_transitions[0].released(u64::MAX));
        assert!(
            aruna_core::placement::retained_departing_holder(&document, &_placement, node(1)),
            "a removed holder is still a retained publisher until it reports"
        );
    }

    async fn closed_generation(
        context: &Arc<DriverContext>,
        realm_id: RealmId,
        placement: &PlacementRef,
    ) -> u64 {
        let (key_space, key) = fence::fence_read(&realm_id, placement);
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                fence::closed_generation(value.as_ref())
            }
            other => panic!("unexpected fence read: {other:?}"),
        }
    }

    #[test]
    fn concurrent_maps_coalesce() {
        // Two issuers racing at one epoch must reduce to one usable value, so
        // the published map is derived byte-identically from the config.
        let realm_id = RealmId::from_bytes([63; 32]);
        let (document, _late) = late_strategy_config(realm_id);
        let map = document.freeze_map(2);
        assert_eq!(map, document.freeze_map(2));

        let publish = |state: &mut AdminDocumentState, seed: u8, origin: NodeId, map| {
            state
                .apply(&AdminDocumentEvent {
                    event_id: Ulid::from_bytes([seed; 16]),
                    target: AdminDocumentTarget::RealmConfig { realm_id },
                    origin_node_id: origin,
                    origin_seq: 1,
                    observed: AdminDocumentClock::default(),
                    actor: actor(realm_id, origin),
                    op: AdminDocumentOperation::CandidateMapPublished { map },
                })
                .expect("the publication applies");
        };
        let materialize = |state: &AdminDocumentState| {
            let mut materialized = document.clone();
            overlay_placement(&mut materialized, state, 0);
            materialized
        };

        let mut agreed = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id });
        publish(&mut agreed, 70, node(1), map.clone());
        publish(&mut agreed, 71, node(2), map.clone());
        assert_eq!(materialize(&agreed).candidate_map(2), Some(&map));

        // Divergent values at one epoch are exactly what a single issuer avoids.
        let mut divergent = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id });
        let mut rival = map.clone();
        rival.nodes.pop();
        publish(&mut divergent, 72, node(1), map);
        publish(&mut divergent, 73, node(2), rival);
        assert!(materialize(&divergent).candidate_map(2).is_none());
    }

    /// The bucket the fence close tests write to.
    fn fenced_bucket() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([7; 16]),
            shard: 3,
        }
    }

    #[tokio::test]
    async fn close_conflicts_write() {
        // A write that read the open fence and then paused must not commit
        // after the departing holder closed that generation.
        let directory = tempdir().unwrap();
        let storage =
            aruna_storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([9; 32]);
        let placement = fenced_bucket();

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("the write transaction starts");
        };
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage
            .send_storage_effect(StorageEffect::Read {
                key_space: aruna_core::keyspaces::WRITE_FENCE_KEYSPACE.to_string(),
                key: aruna_core::storage_entries::placement_fence_key(&realm_id, &placement),
                txn_id: Some(txn_id),
            })
            .await
        else {
            panic!("the fence read answers");
        };
        assert!(fence::admits(value.as_ref(), 1));

        crate::driver::drive(
            crate::placement::process_transitions::CloseFenceOperation::new(realm_id, placement, 1),
            &crate::driver::DriverContext::for_storage(storage.clone()),
        )
        .await
        .expect("the departing holder closes the fence");

        let write = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE.to_string(),
                key: ByteView::from(b"paused-row".to_vec()),
                value: ByteView::from(vec![1u8]),
                txn_id: Some(txn_id),
            })
            .await;
        assert!(matches!(
            write,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        let commit = storage
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;
        assert!(
            matches!(
                commit,
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict
                })
            ),
            "the paused predecessor write must conflict, got {commit:?}"
        );
    }

    #[tokio::test]
    async fn close_is_monotone() {
        let directory = tempdir().unwrap();
        let storage =
            aruna_storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([10; 32]);
        let placement = fenced_bucket();
        let stored = || async {
            match storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: aruna_core::keyspaces::WRITE_FENCE_KEYSPACE.to_string(),
                    key: aruna_core::storage_entries::placement_fence_key(&realm_id, &placement),
                    txn_id: None,
                })
                .await
            {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    fence::closed_generation(value.as_ref())
                }
                other => panic!("unexpected fence read: {other:?}"),
            }
        };

        crate::driver::drive(
            crate::placement::process_transitions::CloseFenceOperation::new(realm_id, placement, 3),
            &crate::driver::DriverContext::for_storage(storage.clone()),
        )
        .await
        .unwrap();
        assert_eq!(stored().await, 3);
        // A stale re-close never reopens the bucket.
        crate::driver::drive(
            crate::placement::process_transitions::CloseFenceOperation::new(realm_id, placement, 2),
            &crate::driver::DriverContext::for_storage(storage.clone()),
        )
        .await
        .unwrap();
        assert_eq!(stored().await, 3);
        crate::driver::drive(
            crate::placement::process_transitions::CloseFenceOperation::new(realm_id, placement, 4),
            &crate::driver::DriverContext::for_storage(storage.clone()),
        )
        .await
        .unwrap();
        assert_eq!(stored().await, 4);

        let (lower, higher) = tokio::join!(
            async {
                crate::driver::drive(
                    crate::placement::process_transitions::CloseFenceOperation::new(
                        realm_id, placement, 5,
                    ),
                    &crate::driver::DriverContext::for_storage(storage.clone()),
                )
                .await
            },
            async {
                crate::driver::drive(
                    crate::placement::process_transitions::CloseFenceOperation::new(
                        realm_id, placement, 6,
                    ),
                    &crate::driver::DriverContext::for_storage(storage.clone()),
                )
                .await
            },
        );
        lower.unwrap();
        higher.unwrap();
        assert_eq!(stored().await, 6);
    }

    #[tokio::test]
    async fn close_heals_malformed() {
        // An unparseable value admits no generation, so a close that reported
        // success without rewriting it would brick the bucket for good.
        let directory = tempdir().unwrap();
        let storage =
            aruna_storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([12; 32]);
        let placement = fenced_bucket();
        let (key_space, key) = fence_read(&realm_id, &placement);
        let write = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.clone(),
                key: key.clone(),
                value: ByteView::from(vec![9u8; 3]),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            write,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        crate::driver::drive(
            crate::placement::process_transitions::CloseFenceOperation::new(realm_id, placement, 4),
            &crate::driver::DriverContext::for_storage(storage.clone()),
        )
        .await
        .unwrap();

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage
            .send_storage_effect(StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            })
            .await
        else {
            panic!("the healed fence reads back");
        };
        assert_eq!(fence::closed_generation(value.as_ref()), 4);
        assert!(
            fence::admits(value.as_ref(), 5),
            "a successor generation must be writable again"
        );
    }
}
