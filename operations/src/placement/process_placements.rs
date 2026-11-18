//! Reconciles the shard topics this node holds with its co-holders.
//! Rank zero creates the genesis, other holders pull it, then pending records set members.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentTarget, shard_topic_id};
use aruna_core::effects::{ShardMembership, ShardNetEffect};
use aruna_core::events::{Event, GenesisProbe, ShardNetEvent};
use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, SYNC_PLACEMENT_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::placement::{
    bucket_membership, drain_pending, draining_former_holders, next_release_ms,
    resolve_shard_holders, try_resolve_holders,
};
use aruna_core::reducer::decode_reducer_state;
use aruna_core::rows::{done, event, page, values};
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::placement::transition::TransitionStatus;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Key, Value};
use irokle::TopicId;
use tracing::{debug, warn};

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::placement::process_transitions::ProcessTransitionsOperation;
use crate::realm::ensure_config::overlay_reducer_state;
use crate::shard::verify::{VerifiedTopicsOperation, forget_verification};
use crate::shard::{shard_answer, shard_call};
use crate::sync::shard_placement::{
    PLACEMENT_RETRY_AFTER, PULL_RETRY_AFTER, decode_placement, new_placement, placement_prefix,
    schedule_placement_deadline, schedule_retry_after, sort_node_ids, write_placement_effect,
};

const PLACEMENT_PAGE_SIZE: usize = 256;

fn membership_call(
    topics: Vec<TopicId>,
    members: Vec<NodeId>,
    publishers: Vec<NodeId>,
    retained: BTreeSet<NodeId>,
    verified: BTreeSet<TopicId>,
) -> IoCall {
    shard_call(ShardNetEffect::Membership(ShardMembership {
        topics,
        members,
        publishers,
        retained,
        verified,
    }))
}

fn existing_call(topics: Vec<TopicId>) -> IoCall {
    shard_call(ShardNetEffect::Existing(topics))
}

fn existing_answer(result: IoResult) -> Result<BTreeSet<TopicId>, IoError> {
    Ok(match shard_answer(result)? {
        ShardNetEvent::Existing(topics) => topics,
        _ => BTreeSet::new(),
    })
}

fn outcome_answer(result: IoResult) -> Result<Result<(), String>, IoError> {
    Ok(match shard_answer(result)? {
        ShardNetEvent::Outcome(outcome) => outcome,
        other => Err(format!("unexpected shard answer {other:?}")),
    })
}

/// The topics this node may hold or create, and whether a creation was withheld.
#[derive(Debug, PartialEq)]
pub(crate) struct ResolvedTopics {
    pub(crate) to_ensure: Vec<TopicId>,
    pub(crate) withheld: bool,
    pub(crate) unreachable: BTreeSet<NodeId>,
}

#[derive(Debug, PartialEq)]
enum ResolveStage {
    Local,
    Probe,
    Pull,
    Adopted,
}

type ResolveTurn = Result<Next<ResolvedTopics>, IoError>;

/// Splits `topics` into ones this node may safely hold or create, flagging
/// whether creation was withheld. Co-held topics are adopted; only positive
/// absence with `may_mint` allows a genesis, since a fork is a split-brain.
#[derive(Debug, PartialEq)]
pub(crate) struct ResolveTopicsOperation {
    co_members: Vec<NodeId>,
    topics: Vec<TopicId>,
    may_mint: bool,
    unreachable: BTreeSet<NodeId>,
    stage: ResolveStage,
    to_ensure: Vec<TopicId>,
    missing: Vec<TopicId>,
    skipped: Vec<NodeId>,
    adopted: Vec<TopicId>,
    withheld: bool,
}

impl ResolveTopicsOperation {
    pub(crate) fn new(
        co_members: Vec<NodeId>,
        topics: Vec<TopicId>,
        may_mint: bool,
        unreachable: BTreeSet<NodeId>,
    ) -> Self {
        Self {
            co_members,
            topics,
            may_mint,
            unreachable,
            stage: ResolveStage::Local,
            to_ensure: Vec::new(),
            missing: Vec::new(),
            skipped: Vec::new(),
            adopted: Vec::new(),
            withheld: false,
        }
    }

    fn finish(&mut self) -> ResolveTurn {
        Ok(Next::Done(ResolvedTopics {
            to_ensure: std::mem::take(&mut self.to_ensure),
            withheld: self.withheld,
            unreachable: std::mem::take(&mut self.unreachable),
        }))
    }

    fn split_local(&mut self, existing: &BTreeSet<TopicId>) -> ResolveTurn {
        let (known, missing): (Vec<_>, Vec<_>) =
            (self.topics.drain(..)).partition(|topic| existing.contains(topic));
        self.to_ensure = known;
        self.missing = missing;
        if self.missing.is_empty() {
            return self.finish();
        }
        if self.co_members.is_empty() {
            match self.may_mint {
                true => self.to_ensure.append(&mut self.missing),
                false => self.withheld = true,
            }
            return self.finish();
        }
        // Probe an unreachable peer once per pass; recovery is observed on the next pass.
        let unreachable = &self.unreachable;
        let (live, skipped): (Vec<_>, Vec<_>) =
            (self.co_members.iter().copied()).partition(|peer| !unreachable.contains(peer));
        self.skipped = skipped;
        if live.is_empty() {
            return self.decide(GenesisProbe::default());
        }
        self.stage = ResolveStage::Probe;
        Ok(Next::Call(shard_call(ShardNetEffect::Probe {
            topics: self.missing.clone(),
            peers: live,
        })))
    }

    fn decide(&mut self, mut probe: GenesisProbe) -> ResolveTurn {
        // Skipped peers stay in `unreachable` so an all-dead set still withholds:
        // dropping them would read as "no co-holder to consult" and mint a rival.
        probe.unreachable.append(&mut self.skipped);
        self.unreachable.extend(probe.unreachable.iter().copied());
        let mut adopt = Vec::new();
        for topic in std::mem::take(&mut self.missing) {
            if probe.known_co_holder.contains(&topic) {
                adopt.push(topic);
            } else if self.may_mint
                && probe.unreachable.is_empty()
                && !probe.unconfirmed.contains(&topic)
            {
                self.to_ensure.push(topic);
            } else {
                self.withheld = true;
            }
        }
        if !probe.unreachable.is_empty() || !probe.unconfirmed.is_empty() {
            warn!(
                unreachable = ?probe.unreachable,
                unconfirmed = ?probe.unconfirmed,
                "Withholding genesis creation: co-holder unreachable or topic possibly-existing"
            );
        }
        if adopt.is_empty() {
            return self.finish();
        }
        self.adopted = adopt.clone();
        self.stage = ResolveStage::Pull;
        Ok(Next::Call(shard_call(ShardNetEffect::Pull {
            topics: adopt,
            peers: self.co_members.clone(),
        })))
    }
}

impl Operation for ResolveTopicsOperation {
    type Output = ResolvedTopics;
    type Error = IoError;

    fn first(&mut self) -> ResolveTurn {
        Ok(Next::Call(existing_call(self.topics.clone())))
    }

    fn next(&mut self, result: IoResult) -> ResolveTurn {
        match self.stage {
            ResolveStage::Local => match shard_answer(result)? {
                ShardNetEvent::Existing(existing) => self.split_local(&existing),
                _ => {
                    self.withheld = true;
                    self.finish()
                }
            },
            ResolveStage::Probe => match shard_answer(result)? {
                ShardNetEvent::Probed(probe) => self.decide(probe),
                _ => {
                    self.withheld = true;
                    self.finish()
                }
            },
            ResolveStage::Pull => {
                shard_answer(result)?;
                self.stage = ResolveStage::Adopted;
                Ok(Next::Call(existing_call(self.adopted.clone())))
            }
            // Only topics whose genesis landed are kept; a failed adopt must not fall
            // through to a fresh create, so it is retried on the next pass.
            ResolveStage::Adopted => {
                let existing = existing_answer(result)?;
                for topic in std::mem::take(&mut self.adopted) {
                    match existing.contains(&topic) {
                        true => self.to_ensure.push(topic),
                        false => self.withheld = true,
                    }
                }
                self.finish()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum GenesisStage {
    Policy,
    Resolve,
    Ensure(Vec<TopicId>),
    Members,
}

type GenesisTurn = Result<Next<(bool, BTreeSet<NodeId>)>, IoError>;

/// Ensures the topics a rank-zero holder leads, and says whether a genesis was withheld.
#[derive(Debug, PartialEq)]
pub(crate) struct EnsureGenesisOperation {
    local: NodeId,
    co_members: Vec<NodeId>,
    publishers: Vec<NodeId>,
    topics: Vec<TopicId>,
    retained: BTreeSet<NodeId>,
    verified: BTreeSet<TopicId>,
    unreachable: BTreeSet<NodeId>,
    stage: GenesisStage,
    withheld: bool,
}

impl EnsureGenesisOperation {
    pub(crate) fn new(
        local: NodeId,
        co_members: Vec<NodeId>,
        publishers: Vec<NodeId>,
        topics: Vec<TopicId>,
        retained: BTreeSet<NodeId>,
        verified: BTreeSet<TopicId>,
        unreachable: BTreeSet<NodeId>,
    ) -> Self {
        Self {
            local,
            co_members,
            publishers,
            topics,
            retained,
            verified,
            unreachable,
            stage: GenesisStage::Policy,
            withheld: false,
        }
    }

    fn members(&self, topics: Vec<TopicId>) -> IoCall {
        let mut members = self.co_members.clone();
        members.push(self.local);
        sort_node_ids(&mut members);
        membership_call(
            topics,
            members,
            self.publishers.clone(),
            self.retained.clone(),
            self.verified.clone(),
        )
    }

    fn finish(&mut self) -> GenesisTurn {
        let unreachable = std::mem::take(&mut self.unreachable);
        Ok(Next::Done((self.withheld, unreachable)))
    }
}

impl Operation for EnsureGenesisOperation {
    type Output = (bool, BTreeSet<NodeId>);
    type Error = IoError;

    // This first pass installs publisher policy even when a topic still needs
    // to be adopted or created. Exact membership is retried once it exists.
    fn first(&mut self) -> GenesisTurn {
        Ok(Next::Call(self.members(self.topics.clone())))
    }

    fn next(&mut self, result: IoResult) -> GenesisTurn {
        match std::mem::replace(&mut self.stage, GenesisStage::Members) {
            GenesisStage::Policy => {
                shard_answer(result)?;
                self.stage = GenesisStage::Resolve;
                let resolve = ResolveTopicsOperation::new(
                    self.co_members.clone(),
                    std::mem::take(&mut self.topics),
                    true,
                    std::mem::take(&mut self.unreachable),
                );
                Ok(Next::Call(IoCall::child(resolve)))
            }
            GenesisStage::Resolve => {
                let resolved = result.child::<ResolvedTopics, IoError>()??;
                self.withheld = resolved.withheld;
                self.unreachable = resolved.unreachable;
                let topics = resolved.to_ensure;
                if topics.is_empty() {
                    return self.finish();
                }
                // The net layer reads an empty peer list as all realm peers. A topic held
                // here alone stays local, or the reconcile below removes those peers.
                let ensure = match self.co_members.is_empty() {
                    true => ShardNetEffect::EnsureLocal(topics.clone()),
                    false => ShardNetEffect::EnsureShared {
                        topics: topics.clone(),
                        peers: self.co_members.clone(),
                    },
                };
                self.stage = GenesisStage::Ensure(topics);
                Ok(Next::Call(shard_call(ensure)))
            }
            GenesisStage::Ensure(topics) => match outcome_answer(result)? {
                Ok(()) => {
                    self.stage = GenesisStage::Members;
                    Ok(Next::Call(self.members(topics)))
                }
                Err(error) => {
                    warn!(error = %error, "Failed to ensure rank-0 shard topics");
                    self.withheld = true;
                    self.finish()
                }
            },
            GenesisStage::Members => {
                if let Err(error) = outcome_answer(result)? {
                    warn!(error = %error, "Failed to reconcile rank-0 shard membership");
                    self.withheld = true;
                }
                self.finish()
            }
        }
    }
}

/// What reconciling the held topics left open.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct HeldTopicOutcome {
    /// A rank-0 genesis was withheld (co-holder unreachable or refusing).
    withheld: bool,
    /// A held topic could not be pulled from any co-holder yet.
    pull_pending: bool,
}

#[derive(Debug, PartialEq)]
enum MemberStage {
    Policy,
    Local,
    Pull(Vec<TopicId>),
    Pulled(Vec<TopicId>),
    Members,
}

/// Pulls unknown non-leading topics from a co-holder and tops up known ones.
#[derive(Debug, PartialEq)]
struct MemberGroupOperation {
    co_members: Vec<NodeId>,
    members: Vec<NodeId>,
    publishers: Vec<NodeId>,
    topics: Vec<TopicId>,
    retained: BTreeSet<NodeId>,
    verified: BTreeSet<TopicId>,
    stage: MemberStage,
    outcome: HeldTopicOutcome,
}

impl MemberGroupOperation {
    fn membership(&self, topics: Vec<TopicId>) -> IoCall {
        membership_call(
            topics,
            self.members.clone(),
            self.publishers.clone(),
            self.retained.clone(),
            self.verified.clone(),
        )
    }

    fn top_up(&mut self, known: Vec<TopicId>) -> Result<Next<HeldTopicOutcome>, IoError> {
        if known.is_empty() {
            return Ok(Next::Done(self.outcome));
        }
        self.stage = MemberStage::Members;
        Ok(Next::Call(self.membership(known)))
    }
}

impl Operation for MemberGroupOperation {
    type Output = HeldTopicOutcome;
    type Error = IoError;

    // Publisher policy goes in before pulling; membership repeats after a successful pull.
    fn first(&mut self) -> Result<Next<HeldTopicOutcome>, IoError> {
        Ok(Next::Call(self.membership(self.topics.clone())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HeldTopicOutcome>, IoError> {
        match std::mem::replace(&mut self.stage, MemberStage::Members) {
            MemberStage::Policy => {
                shard_answer(result)?;
                self.stage = MemberStage::Local;
                Ok(Next::Call(existing_call(self.topics.clone())))
            }
            MemberStage::Local => {
                let existing = existing_answer(result)?;
                let (known, missing): (Vec<_>, Vec<_>) =
                    (self.topics.drain(..)).partition(|topic| existing.contains(topic));
                if missing.is_empty() {
                    return self.top_up(known);
                }
                debug!(
                    event = "placement.topic.pull",
                    topics = missing.len(),
                    co_members = self.co_members.len(),
                    "Pulling newly held shard topics from co-holders"
                );
                self.topics = missing.clone();
                self.stage = MemberStage::Pull(known);
                Ok(Next::Call(shard_call(ShardNetEffect::Pull {
                    topics: missing,
                    peers: self.co_members.clone(),
                })))
            }
            MemberStage::Pull(known) => {
                shard_answer(result)?;
                self.stage = MemberStage::Pulled(known);
                Ok(Next::Call(existing_call(self.topics.clone())))
            }
            MemberStage::Pulled(mut known) => {
                let existing = existing_answer(result)?;
                for topic in std::mem::take(&mut self.topics) {
                    match existing.contains(&topic) {
                        true => known.push(topic),
                        // No co-holder served a genesis (unreachable, or rank-0 has
                        // not created it yet); retry rather than stay passive.
                        false => self.outcome.pull_pending = true,
                    }
                }
                self.top_up(known)
            }
            MemberStage::Members => {
                if let Err(error) = outcome_answer(result)? {
                    debug!(error = %error, "Could not complete held shard topic membership");
                    self.outcome.withheld = true;
                }
                Ok(Next::Done(self.outcome))
            }
        }
    }
}

type ShardGroup = (Vec<NodeId>, Vec<NodeId>, BTreeSet<NodeId>);
type TopicGroups = BTreeMap<ShardGroup, Vec<TopicId>>;

/// The shard topics this node holds, grouped by who else holds them.
#[derive(Debug, Default, PartialEq)]
struct HeldTopics {
    reopen: Vec<TopicId>,
    leading: TopicGroups,
    joining: TopicGroups,
}

fn held_topics(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    local: NodeId,
    now_ms: u64,
) -> HeldTopics {
    let mut held = HeldTopics::default();
    for strategy in &config.strategies {
        for shard in 0..strategy.shard_count {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                shard,
            };
            let holders = resolve_shard_holders(config, &placement);
            // Admitted targets join; only activated and retained holders may publish.
            let membership = bucket_membership(config, &placement, now_ms);
            if !membership.members.contains(&local) {
                continue;
            }
            let active_target = config.placement_transitions.iter().any(|transition| {
                transition.plan.strategy_id == placement.strategy_id
                    && matches!(transition.status, TransitionStatus::Active)
                    && (transition.plan.bucket_plan(placement.shard)).is_some_and(|bucket| {
                        transition.completion(placement.shard).is_none()
                            && bucket.target_holders.contains(&local)
                    })
            });
            let topic = shard_topic_id(realm_id, &placement);
            if holders.contains(&local) || active_target {
                held.reopen.push(topic);
            }
            let mut co_members: Vec<NodeId> = (membership.members.into_iter())
                .filter(|candidate| *candidate != local)
                .collect();
            sort_node_ids(&mut co_members);
            let mut publishers = membership.publishers;
            sort_node_ids(&mut publishers);
            let retained = draining_former_holders(config, &placement)
                .into_iter()
                .collect();
            // Rank-0 is an activation role: a target that has not cut over yet
            // never creates a genesis.
            let groups = match holders.first() == Some(&local) {
                true => &mut held.leading,
                false => &mut held.joining,
            };
            let group = (co_members, publishers, retained);
            groups.entry(group).or_default().push(topic);
        }
    }
    held
}

#[derive(Debug, PartialEq)]
enum HeldStage {
    Reopen,
    Leading,
    Joining,
}

/// Reconciles every shard topic the local node holds: rank-0 creates the genesis,
/// other holders pull it from a co-holder. Missing topics are adopted before create.
#[derive(Debug, PartialEq)]
struct HeldTopicsOperation {
    realm_id: RealmId,
    local: NodeId,
    config: Arc<RealmConfigDocument>,
    verified: BTreeSet<TopicId>,
    now_ms: u64,
    stage: HeldStage,
    leading: Vec<(ShardGroup, Vec<TopicId>)>,
    joining: Vec<(ShardGroup, Vec<TopicId>)>,
    unreachable: BTreeSet<NodeId>,
    outcome: HeldTopicOutcome,
}

impl HeldTopicsOperation {
    fn new(
        realm_id: RealmId,
        local: NodeId,
        config: Arc<RealmConfigDocument>,
        verified: BTreeSet<TopicId>,
    ) -> Self {
        Self {
            realm_id,
            local,
            config,
            verified,
            now_ms: 0,
            stage: HeldStage::Reopen,
            leading: Vec::new(),
            joining: Vec::new(),
            unreachable: BTreeSet::new(),
            outcome: HeldTopicOutcome::default(),
        }
    }

    fn next_group(&mut self) -> Result<Next<HeldTopicOutcome>, IoError> {
        if let Some(((co_members, publishers, retained), topics)) = self.leading.pop() {
            debug!(
                event = "placement.genesis.ensure",
                topics = topics.len(),
                co_members = co_members.len(),
                "Ensuring rank-0 shard topic geneses"
            );
            self.stage = HeldStage::Leading;
            let genesis = EnsureGenesisOperation::new(
                self.local,
                co_members,
                publishers,
                topics,
                retained,
                self.verified.clone(),
                // Pass-scoped, so a peer that comes back is probed again next reconcile.
                std::mem::take(&mut self.unreachable),
            );
            return Ok(Next::Call(IoCall::child(genesis)));
        }
        while let Some(((co_members, publishers, retained), topics)) = self.joining.pop() {
            if co_members.is_empty() {
                continue;
            }
            let mut members = co_members.clone();
            members.push(self.local);
            sort_node_ids(&mut members);
            self.stage = HeldStage::Joining;
            let group = MemberGroupOperation {
                co_members,
                members,
                publishers,
                topics,
                retained,
                verified: self.verified.clone(),
                stage: MemberStage::Policy,
                outcome: HeldTopicOutcome::default(),
            };
            return Ok(Next::Call(IoCall::child(group)));
        }
        Ok(Next::Done(self.outcome))
    }
}

impl Operation for HeldTopicsOperation {
    type Output = HeldTopicOutcome;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<HeldTopicOutcome>, IoError> {
        let held = held_topics(&self.config, self.realm_id, self.local, self.now_ms);
        self.leading = held.leading.into_iter().rev().collect();
        self.joining = held.joining.into_iter().rev().collect();
        Ok(Next::Call(shard_call(ShardNetEffect::Reopen(held.reopen))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HeldTopicOutcome>, IoError> {
        match self.stage {
            HeldStage::Reopen => {
                if let ShardNetEvent::Reopened { failed } = shard_answer(result)? {
                    self.outcome.pull_pending |= failed > 0;
                }
            }
            HeldStage::Leading => {
                let (withheld, unreachable) =
                    result.child::<(bool, BTreeSet<NodeId>), IoError>()??;
                self.outcome.withheld |= withheld;
                self.unreachable = unreachable;
            }
            HeldStage::Joining => {
                let group = result.child::<HeldTopicOutcome, IoError>()??;
                self.outcome.withheld |= group.withheld;
                self.outcome.pull_pending |= group.pull_pending;
            }
        }
        self.next_group()
    }
}

/// What a reconcile pass decided about follow-up work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PlacementReconcileStatus {
    /// Reconciliation completed without scheduling more work.
    #[default]
    Clean,
    /// A genesis was withheld or a record left incomplete, so the reconciler
    /// scheduled its own [`TaskKey::SyncPlacements`] retry timer.
    RetryScheduled,
    /// Storage could not provide a trustworthy config or placement scan. A
    /// timer consumer must re-arm the consumed timer.
    StorageFailure,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlacementReconcileOutcome {
    /// Compatibility signal for callers that only need to know whether the
    /// reconciler armed its own retry.
    pub retry_scheduled: bool,
    /// The retry includes a held topic that could not be pulled yet.
    pub pull_pending: bool,
    pub status: PlacementReconcileStatus,
}

impl PlacementReconcileOutcome {
    fn clean() -> Self {
        Self::default()
    }

    fn retry_scheduled(pull_pending: bool) -> Self {
        Self {
            retry_scheduled: true,
            pull_pending,
            status: PlacementReconcileStatus::RetryScheduled,
        }
    }

    pub fn storage_failure() -> Self {
        Self {
            retry_scheduled: false,
            pull_pending: false,
            status: PlacementReconcileStatus::StorageFailure,
        }
    }
}

/// Whether the transitions engine may have local steps to run: an unsettled
/// transition, or a strategy whose activations nobody initialized yet.
fn has_transition_work(config: &RealmConfigDocument, now_ms: u64) -> bool {
    config
        .placement_transitions
        .iter()
        .any(|transition| !transition.is_terminal())
        || (config.newest_map_epoch().is_some()
            && config
                .strategies
                .iter()
                .any(|strategy| config.activation(&strategy.strategy_id, 0).is_none()))
        || next_release_ms(config, now_ms).is_some()
        || drain_pending(config, now_ms)
        // A cheap over-approximation of a pending successor expansion: an
        // activation trailing the newest map with no transition in flight.
        || config.newest_map_epoch().is_some_and(|newest| {
            config.placement_activations.iter().any(|activation| {
                activation.candidate_map_epoch != newest
                    && !config.placement_transitions.iter().any(|transition| {
                        transition.plan.strategy_id == activation.strategy_id
                            && !transition.is_terminal()
                    })
            })
        })
}

/// One page of pending placement records.
#[derive(Debug, PartialEq)]
struct PlacementPageOperation {
    realm_id: RealmId,
    after: Option<Key>,
}

impl Operation for PlacementPageOperation {
    type Output = (Vec<(Key, Value)>, Option<Key>);
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        Ok(Next::Call(IoCall::Page {
            space: SYNC_PLACEMENT_KEYSPACE.to_string(),
            prefix: placement_prefix(self.realm_id),
            after: self.after.take(),
            limit: PLACEMENT_PAGE_SIZE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        Ok(Next::Done(page(result)?))
    }
}

#[derive(Debug, PartialEq)]
enum RecordStage {
    Decide,
    Exists,
    Members,
    Finish { retry: bool },
    Forget(PlacementRef),
}

/// Applies one pending placement record; answers whether it needs another pass.
#[derive(Debug, PartialEq)]
struct PlacementRecordOperation {
    realm_id: RealmId,
    local: NodeId,
    config: Arc<RealmConfigDocument>,
    verified: BTreeSet<TopicId>,
    key: Key,
    value: Value,
    now_ms: u64,
    stage: RecordStage,
    record: Option<aruna_core::document::PendingShardPlacement>,
}

impl PlacementRecordOperation {
    fn delete(&self) -> IoCall {
        IoCall::Delete(vec![(
            SYNC_PLACEMENT_KEYSPACE.to_string(),
            self.key.clone(),
        )])
    }

    /// Keeps the record for another pass, with a fresh timestamp.
    fn keep(&mut self) -> Result<Next<bool>, IoError> {
        let Some(record) = self.record.as_ref() else {
            return Ok(Next::Done(true));
        };
        let refreshed = new_placement(
            self.realm_id,
            record.placement,
            self.local,
            record.selected_peers.clone(),
        );
        let Ok(effect) = write_placement_effect(&refreshed) else {
            return Ok(Next::Done(true));
        };
        self.stage = RecordStage::Finish { retry: true };
        Ok(Next::Call(effect.into()))
    }
}

impl Operation for PlacementRecordOperation {
    type Output = bool;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let record = match decode_placement(&self.value) {
            Ok(record) => record,
            Err(error) => {
                warn!(error = %error, "Deleting malformed shard placement record");
                self.stage = RecordStage::Finish { retry: false };
                return Ok(Next::Call(self.delete()));
            }
        };
        if record.realm_id != self.realm_id {
            return Ok(Next::Done(false));
        }
        let placement = record.placement;
        self.record = Some(record);
        // A resolution failure keeps the durable record: deleting it on a
        // missing or conflicted activation would destroy the only retry.
        let holders = match try_resolve_holders(&self.config, &placement) {
            Ok(holders) => holders,
            Err(error) => {
                debug!(error = %error, "Keeping placement record for unresolvable bucket");
                return self.keep();
            }
        };
        if !holders.contains(&self.local) {
            // No longer a holder of this shard. Its verification marker goes too,
            // so a later re-entry verifies again.
            self.stage = RecordStage::Forget(placement);
            return Ok(Next::Call(self.delete()));
        }
        if holders.iter().all(|holder| *holder == self.local) {
            self.stage = RecordStage::Finish { retry: false };
            return Ok(Next::Call(self.delete()));
        }
        self.stage = RecordStage::Exists;
        let topic = shard_topic_id(self.realm_id, &placement);
        Ok(Next::Call(existing_call(vec![topic])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        match std::mem::replace(&mut self.stage, RecordStage::Decide) {
            RecordStage::Exists => {
                let placement = (self.record.as_ref())
                    .map(|record| record.placement)
                    .ok_or(IoError::Unexpected {
                        expected: "a placement record",
                        got: "none",
                    })?;
                let topic = shard_topic_id(self.realm_id, &placement);
                // This loop tops up known topics and keeps records of a missing genesis.
                if !existing_answer(result)?.contains(&topic) {
                    debug!(
                        ?topic,
                        "Shard topic genesis not local yet; keeping placement record"
                    );
                    return self.keep();
                }
                // The topic exists locally, so reconcile its exact canonical holder
                // set without creating a genesis or changing shared/default peers.
                let retained = draining_former_holders(&self.config, &placement)
                    .into_iter()
                    .collect();
                let bucket = bucket_membership(&self.config, &placement, self.now_ms);
                self.stage = RecordStage::Members;
                Ok(Next::Call(membership_call(
                    vec![topic],
                    bucket.members,
                    bucket.publishers,
                    retained,
                    self.verified.clone(),
                )))
            }
            RecordStage::Members => match outcome_answer(result)? {
                // Every co-holder is now a member; the resync loop delivers the events.
                Ok(()) => {
                    self.stage = RecordStage::Finish { retry: false };
                    Ok(Next::Call(self.delete()))
                }
                Err(error) => {
                    debug!(error = %error, "Shard topic membership incomplete; keeping placement record");
                    self.keep()
                }
            },
            RecordStage::Forget(placement) => {
                done(result)?;
                self.stage = RecordStage::Finish { retry: false };
                Ok(Next::Call(forget_verification(self.realm_id, &placement)))
            }
            RecordStage::Finish { retry } => Ok(Next::Done(retry)),
            RecordStage::Decide => Err(IoError::Unexpected {
                expected: "a placement record step",
                got: "none",
            }),
        }
    }
}

/// Drops transitions the local view already considers released, by re-materializing the
/// stored document from the reducer state. A failed run asks the pass to retry.
#[derive(Debug, PartialEq)]
struct PruneReleasedOperation {
    realm_id: RealmId,
    local: NodeId,
    now_ms: u64,
    written: bool,
}

impl Operation for PruneReleasedOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let realm_id = self.realm_id;
        let document = DocumentTarget::RealmConfig { realm_id };
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        Ok(Next::Call(IoCall::Read(vec![
            (
                document.storage_keyspace().to_string(),
                document.storage_key(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&target),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.written {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let [stored, state] = values::<2>(result)?;
        let (Some(stored), Some(state)) = (stored, state) else {
            return Ok(Next::Done(()));
        };
        let (Ok(mut stored), Ok(state)) = (
            RealmConfigDocument::from_bytes(stored.as_ref()),
            decode_reducer_state(state.as_ref()),
        ) else {
            warn!("Undecodable realm config or reducer state; transition release skipped");
            return Ok(Next::Done(()));
        };
        let before = stored.placement_transitions.len();
        overlay_reducer_state(&mut stored, &state, self.now_ms);
        let pruned = before - stored.placement_transitions.len();
        if pruned == 0 {
            return Ok(Next::Done(()));
        }
        let realm_id = self.realm_id;
        let actor = Actor {
            node_id: self.local,
            user_id: aruna_core::UserId::nil(realm_id),
            realm_id,
        };
        let bytes = (stored.to_bytes(&actor)).map_err(|error| IoError::Codec(error.to_string()))?;
        debug!(pruned, "Released placement transitions pruned");
        self.written = true;
        let document = DocumentTarget::RealmConfig { realm_id };
        Ok(Next::Call(IoCall::Write(vec![(
            document.storage_keyspace().to_string(),
            document.storage_key(),
            bytes.into(),
        )])))
    }
}

#[derive(Debug, PartialEq)]
enum ReconcileStage {
    Config,
    Network,
    Verified,
    Held,
    Transitions,
    Armed,
    Pruned,
    Deadline,
    Page,
    Record,
    Retry,
}

type ReconcileTurn = Result<Next<PlacementReconcileOutcome>, IoError>;

/// Reconciles held shard topics with their co-holders, then applies pending placement records;
/// withheld work arms a retry. `run_transitions` is false on request paths: every placement
/// mutation arms a zero-delay timer, so barriers and proofs run in the background.
#[derive(Debug, PartialEq)]
pub struct ReconcilePlacementsOperation {
    realm_id: RealmId,
    local: NodeId,
    run_transitions: bool,
    now_ms: u64,
    stage: ReconcileStage,
    config: Option<Arc<RealmConfigDocument>>,
    verified: BTreeSet<TopicId>,
    held: HeldTopicOutcome,
    retry_needed: bool,
    /// Rows of the current page still to apply.
    rows: Vec<(Key, Value)>,
    after: Option<Key>,
}

impl ReconcilePlacementsOperation {
    pub fn new(realm_id: RealmId, local: NodeId, run_transitions: bool) -> Self {
        Self {
            realm_id,
            local,
            run_transitions,
            now_ms: 0,
            stage: ReconcileStage::Config,
            config: None,
            verified: BTreeSet::new(),
            held: HeldTopicOutcome::default(),
            retry_needed: false,
            rows: Vec::new(),
            after: None,
        }
    }

    fn config(&self) -> Result<Arc<RealmConfigDocument>, IoError> {
        self.config.clone().ok_or(IoError::Unexpected {
            expected: "the realm config",
            got: "none",
        })
    }

    fn transitions(&mut self) -> ReconcileTurn {
        let config = self.config()?;
        if self.run_transitions {
            self.stage = ReconcileStage::Transitions;
            let run = ProcessTransitionsOperation::new(self.realm_id, self.local, config);
            return Ok(Next::Call(IoCall::child(run)));
        }
        if has_transition_work(&config, self.now_ms) {
            // A pure transition target never mutates the config, so nothing else
            // arms its timer; fire the deferred execution now.
            self.stage = ReconcileStage::Armed;
            let arm = schedule_retry_after(self.realm_id, self.local, Duration::ZERO);
            return Ok(Next::Call(arm.into()));
        }
        self.prune()
    }

    /// Prunes locally released records because no later event may rematerialize them.
    fn prune(&mut self) -> ReconcileTurn {
        let config = self.config()?;
        let now_ms = self.now_ms;
        let released = (config.placement_transitions.iter()).any(|t| t.released(now_ms));
        if !released {
            return self.deadline();
        }
        self.stage = ReconcileStage::Pruned;
        Ok(Next::Call(IoCall::child(PruneReleasedOperation {
            realm_id: self.realm_id,
            local: self.local,
            now_ms,
            written: false,
        })))
    }

    /// Arms the earliest grace deadline without postponing an earlier retry.
    fn deadline(&mut self) -> ReconcileTurn {
        let config = self.config()?;
        self.retry_needed |= drain_pending(&config, self.now_ms);
        match next_release_ms(&config, self.now_ms) {
            Some(deadline) => {
                self.stage = ReconcileStage::Deadline;
                let after = Duration::from_millis(deadline.saturating_sub(self.now_ms));
                let arm = schedule_placement_deadline(self.realm_id, self.local, after);
                Ok(Next::Call(arm.into()))
            }
            None => self.next_page(),
        }
    }

    fn next_page(&mut self) -> ReconcileTurn {
        self.stage = ReconcileStage::Page;
        Ok(Next::Call(IoCall::child(PlacementPageOperation {
            realm_id: self.realm_id,
            after: self.after.take(),
        })))
    }

    fn next_row(&mut self) -> ReconcileTurn {
        let Some((key, value)) = self.rows.pop() else {
            return match self.after.is_some() {
                true => self.next_page(),
                false => self.finish(),
            };
        };
        self.stage = ReconcileStage::Record;
        Ok(Next::Call(IoCall::child(PlacementRecordOperation {
            realm_id: self.realm_id,
            local: self.local,
            config: self.config()?,
            verified: self.verified.clone(),
            key,
            value,
            now_ms: 0,
            stage: RecordStage::Decide,
            record: None,
        })))
    }

    /// Pending pulls retry quickly; withheld genesis waits before probing its co-holder again.
    fn finish(&mut self) -> ReconcileTurn {
        if !self.retry_needed {
            return Ok(Next::Done(PlacementReconcileOutcome::clean()));
        }
        let after = match self.held.pull_pending {
            true => PULL_RETRY_AFTER,
            false => PLACEMENT_RETRY_AFTER,
        };
        self.stage = ReconcileStage::Retry;
        let arm = schedule_retry_after(self.realm_id, self.local, after);
        Ok(Next::Call(arm.into()))
    }
}

impl Operation for ReconcilePlacementsOperation {
    type Output = PlacementReconcileOutcome;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ReconcileTurn {
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> ReconcileTurn {
        let realm_id = self.realm_id;
        match self.stage {
            ReconcileStage::Config => {
                match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => {
                        self.config = Some(Arc::new(config));
                        self.stage = ReconcileStage::Network;
                        Ok(Next::Call(existing_call(Vec::new())))
                    }
                    Err(GetConfigError::DocumentNotFound) => {
                        warn!(%realm_id, "Cannot process shard placements without a realm config");
                        Ok(Next::Done(PlacementReconcileOutcome::clean()))
                    }
                    Err(error) => {
                        warn!(%realm_id, error = %error, "Failed to read realm config for shard placements");
                        Ok(Next::Done(PlacementReconcileOutcome::storage_failure()))
                    }
                }
            }
            ReconcileStage::Network => {
                if matches!(shard_answer(result)?, ShardNetEvent::NoNetwork) {
                    return Ok(Next::Done(PlacementReconcileOutcome::clean()));
                }
                // Former-holder history cutoffs are frozen only for durably verified shards.
                self.stage = ReconcileStage::Verified;
                let verified = VerifiedTopicsOperation::new(realm_id);
                Ok(Next::Call(IoCall::child(verified)))
            }
            ReconcileStage::Verified => {
                match result.child::<BTreeSet<TopicId>, IoError>()? {
                    Ok(verified) => self.verified = verified,
                    Err(error) => {
                        warn!(%realm_id, error = %error, "Failed to scan verified shards")
                    }
                }
                // Withheld genesis and pending pulls retry even without a placement record.
                self.stage = ReconcileStage::Held;
                let held = HeldTopicsOperation::new(
                    realm_id,
                    self.local,
                    self.config()?,
                    self.verified.clone(),
                );
                Ok(Next::Call(IoCall::child(held)))
            }
            ReconcileStage::Held => {
                self.held = result.child::<HeldTopicOutcome, IoError>()??;
                self.retry_needed = self.held.withheld || self.held.pull_pending;
                self.transitions()
            }
            ReconcileStage::Transitions => {
                self.retry_needed |= result.child::<bool, IoError>()??;
                self.prune()
            }
            ReconcileStage::Armed => {
                event(result)?;
                self.prune()
            }
            ReconcileStage::Pruned => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Failed to prune released placement transitions");
                    self.retry_needed = true;
                }
                self.deadline()
            }
            ReconcileStage::Deadline => {
                event(result)?;
                self.next_page()
            }
            ReconcileStage::Page => {
                match result.child::<(Vec<(Key, Value)>, Option<Key>), IoError>()? {
                    Ok((mut rows, next)) => {
                        rows.reverse();
                        self.rows = rows;
                        self.after = next;
                        self.next_row()
                    }
                    Err(error) => {
                        warn!(error = %error, "Failed to list pending shard placements");
                        Ok(Next::Done(PlacementReconcileOutcome::storage_failure()))
                    }
                }
            }
            ReconcileStage::Record => {
                match result.child::<bool, IoError>()? {
                    Ok(retry) => self.retry_needed |= retry,
                    Err(error) => {
                        debug!(error = %error, "Placement record step failed");
                        self.retry_needed = true;
                    }
                }
                self.next_row()
            }
            ReconcileStage::Retry => Ok(Next::Done(match event(result)? {
                // Without a task handle nothing can run the retry.
                Event::Task(TaskEvent::Error { key: None, .. }) => {
                    PlacementReconcileOutcome::clean()
                }
                _ => PlacementReconcileOutcome::retry_scheduled(self.held.pull_pending),
            })),
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use aruna_core::structs::placement::record::{PlacementRef, PlacementStrategy};
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn config_with(nodes: &[NodeId], replica: Option<u32>) -> (RealmConfigDocument, PlacementRef) {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([8u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            name: "default".to_string(),
            replica_count: replica,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy.clone()];
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        (
            config,
            PlacementRef {
                strategy_id: strategy.strategy_id,
                shard: 3,
            },
        )
    }

    #[test]
    fn shard_holders_deterministic() {
        let (config, placement) = config_with(&[node(1), node(2), node(3), node(4)], None);
        let first = resolve_shard_holders(&config, &placement);

        let (reversed, _) = config_with(&[node(4), node(3), node(2), node(1)], None);
        let second = resolve_shard_holders(&reversed, &placement);

        assert_eq!(first, second);
        assert_eq!(first.len(), 4);
    }

    #[test]
    fn replica_cap_bounds() {
        let (config, placement) = config_with(&[node(1), node(2), node(3), node(4)], Some(2));
        let holders = resolve_shard_holders(&config, &placement);
        assert_eq!(holders.len(), 2);
    }
}

#[cfg(test)]
mod tests {
    use crate::driver::DriverContext;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use irokle::Storage as _;
    use ulid::Ulid;

    use super::*;

    // A rank-0 group held here alone must keep the realm's default peers out of its
    // topic, so a repeated pass signs no membership change.
    #[tokio::test]
    async fn sole_holder_local() {
        let dir = tempfile::tempdir().expect("temp dir");
        let realm_id = RealmId::from_bytes([61u8; 32]);
        let storage_path = dir.path().join("storage");
        let storage_handle =
            aruna_storage::FjallStorage::open(storage_path.to_str().expect("utf8 path"))
                .expect("storage opens");
        let peer = iroh::SecretKey::from_bytes(&[62u8; 32]).public();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind address"),
                realm_id,
                peer_nodes: vec![peer],
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                sync_storage_path: Some(dir.path().join("document-sync")),
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .expect("net handle");
        let context = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        let local = net.node_id();
        let topic = shard_topic_id(
            realm_id,
            &PlacementRef {
                strategy_id: Ulid::from_bytes([63u8; 16]),
                shard: 0,
            },
        );
        let node = net.document_sync_node();
        let peer_id = irokle::PeerId::from_bytes(*peer.as_bytes());
        let mut clocks = Vec::new();
        for _ in 0..2 {
            let genesis = EnsureGenesisOperation::new(
                local,
                Vec::new(),
                vec![local],
                vec![topic],
                BTreeSet::new(),
                BTreeSet::new(),
                BTreeSet::new(),
            );
            let (withheld, _) = crate::driver::drive(genesis, &context).await.unwrap();
            assert!(!withheld, "a sole holder mints and keeps its topic");
            let state = node
                .storage()
                .topic_state(&topic)
                .expect("topic state reads")
                .expect("topic minted");
            assert!(!state.members.contains(&peer_id), "{:?}", state.members);
            clocks.push(node.storage().actor_clock(&topic).expect("clock reads"));
        }
        assert_eq!(
            clocks[0], clocks[1],
            "a repeated pass signed membership ops"
        );
        net.shutdown().await;
    }
}
