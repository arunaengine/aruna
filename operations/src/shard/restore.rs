//! Restores shard and shared topic subscriptions after a start, in bounded passes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{ShardMembership, ShardNetEffect};
use aruna_core::events::ShardNetEvent;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use irokle::TopicId;
use tracing::{info, warn};

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::placement::process_placements::{
    EnsureGenesisOperation, ResolveTopicsOperation, ResolvedTopics,
};
use crate::shard::pull::PullTopicsOperation;
use crate::shard::recovery::{RecoveryError, RecoveryStop};
use crate::shard::restore_plan::{
    RestoreKind, RestoreRange, RestoreShardSummary, RestoreUnit, RestoreUnitOutcome, ShardGroup,
    ShardRestoreCursor, ShardRestorePass, bind_restore_plan, plan_shard_groups, restore_range,
};
use crate::shard::verify::{
    ShardVerificationSummary, VerifiedTopicsOperation, VerifyHeldOperation,
};
use crate::shard::{shard_answer, shard_call};
use crate::sync::shard_placement::schedule_retry_effect;

fn existing_call(topics: Vec<TopicId>) -> IoCall {
    shard_call(ShardNetEffect::Existing(topics))
}

/// The topics with a local genesis; `None` when this node has no net transport.
fn existing_answer(result: IoResult) -> Result<Option<BTreeSet<TopicId>>, IoError> {
    Ok(match shard_answer(result)? {
        ShardNetEvent::Existing(topics) => Some(topics),
        _ => None,
    })
}

fn outcome_answer(result: IoResult) -> Result<Result<(), String>, IoError> {
    Ok(match shard_answer(result)? {
        ShardNetEvent::Outcome(outcome) => outcome,
        other => Err(format!("unexpected shard answer {other:?}")),
    })
}

fn membership_call(
    topics: Vec<TopicId>,
    (peers, publishers, retained): &ShardGroup,
    node_id: NodeId,
    verified: &BTreeSet<TopicId>,
) -> IoCall {
    let mut members = peers.clone();
    members.push(node_id);
    members.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    shard_call(ShardNetEffect::Membership(ShardMembership {
        topics,
        members,
        publishers: publishers.clone(),
        retained: retained.clone(),
        verified: verified.clone(),
    }))
}

#[derive(Debug, PartialEq)]
enum UnitStage {
    Start,
    Resolve,
    Minted(Vec<TopicId>),
    Ensured(Vec<TopicId>),
    Pulled(Vec<TopicId>),
    Genesis,
    Present,
    JoinPolicy,
    JoinPull,
    JoinPresent,
    JoinMembers(Vec<TopicId>),
}

type UnitTurn = Result<Next<RestoreUnitOutcome>, IoError>;

/// Restores one work unit; every failure leaves its topics unresolved for the next pass.
#[derive(Debug, PartialEq)]
struct RestoreUnitOperation {
    node_id: NodeId,
    unit: RestoreUnit,
    verified: BTreeSet<TopicId>,
    outcome: RestoreUnitOutcome,
    stage: UnitStage,
}

impl RestoreUnitOperation {
    fn new(node_id: NodeId, unit: RestoreUnit, verified: BTreeSet<TopicId>) -> Self {
        Self {
            node_id,
            unit,
            verified,
            outcome: RestoreUnitOutcome::default(),
            stage: UnitStage::Start,
        }
    }

    fn finish(&mut self) -> UnitTurn {
        Ok(Next::Done(std::mem::take(&mut self.outcome)))
    }

    fn membership(&self, topics: Vec<TopicId>) -> IoCall {
        let group = (
            self.unit.peers.clone(),
            self.unit.publishers.clone(),
            self.unit.retained.clone(),
        );
        membership_call(topics, &group, self.node_id, &self.verified)
    }

    fn pull(&self, topics: Vec<TopicId>) -> IoCall {
        IoCall::child(PullTopicsOperation::new(topics, self.unit.peers.clone()))
    }

    fn all_topics(&self) -> Vec<TopicId> {
        self.unit.topics.clone()
    }

    /// Marks the unit's topics outside `kept` as unresolved.
    fn fail_others(&mut self, kept: impl Fn(&TopicId) -> bool) {
        let others: Vec<TopicId> = self.all_topics().into_iter().filter(|t| !kept(t)).collect();
        self.outcome.fail_topics(others);
    }

    /// The unit's topics with a local genesis; the others stay unresolved.
    fn present(&mut self, result: IoResult) -> Result<Vec<TopicId>, IoError> {
        let existing = existing_answer(result)?.unwrap_or_default();
        self.fail_others(|topic| existing.contains(topic));
        let mut present = self.all_topics();
        present.retain(|topic| existing.contains(topic));
        Ok(present)
    }

    fn pulled(result: IoResult) -> Result<bool, IoError> {
        Ok(result.child::<bool, IoError>()?.unwrap_or(false))
    }

    fn resolved(&mut self, result: IoResult) -> UnitTurn {
        // An unreachable or refusing peer may hold the genesis, so minting would fork it.
        // Keep withheld topics unresolved for recovery to retry.
        let (to_ensure, withheld) = match result.child::<ResolvedTopics, IoError>()? {
            Ok(resolved) => (resolved.to_ensure, resolved.withheld),
            Err(_) => (Vec::new(), true),
        };
        if withheld {
            self.fail_others(|topic| to_ensure.contains(topic));
        }
        if to_ensure.is_empty() {
            return self.finish();
        }
        // A unit without peers holds topics this node is alone on: they are minted
        // here and no peer joins their membership.
        if self.unit.peers.is_empty() {
            let ensure = ShardNetEffect::EnsureLocal(to_ensure.clone());
            self.stage = UnitStage::Minted(to_ensure);
            return Ok(Next::Call(shard_call(ensure)));
        }
        let ensure = ShardNetEffect::EnsureShared {
            topics: to_ensure.clone(),
            peers: self.unit.peers.clone(),
        };
        self.stage = UnitStage::Ensured(to_ensure);
        Ok(Next::Call(shard_call(ensure)))
    }
}

impl Operation for RestoreUnitOperation {
    type Output = RestoreUnitOutcome;
    type Error = IoError;

    fn first(&mut self) -> UnitTurn {
        let unit = &self.unit;
        let (stage, call) = match unit.kind {
            RestoreKind::Shared | RestoreKind::SharedJoin => {
                let may_mint = unit.kind == RestoreKind::Shared;
                let resolve = ResolveTopicsOperation::new(
                    unit.peers.clone(),
                    unit.topics.clone(),
                    may_mint,
                    BTreeSet::new(),
                );
                (UnitStage::Resolve, IoCall::child(resolve))
            }
            RestoreKind::Rank0 => {
                let genesis = EnsureGenesisOperation::new(
                    self.node_id,
                    unit.peers.clone(),
                    unit.publishers.clone(),
                    unit.topics.clone(),
                    unit.retained.clone(),
                    self.verified.clone(),
                    BTreeSet::new(),
                );
                (UnitStage::Genesis, IoCall::child(genesis))
            }
            RestoreKind::Join => (UnitStage::JoinPolicy, self.membership(self.all_topics())),
        };
        self.stage = stage;
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> UnitTurn {
        match std::mem::replace(&mut self.stage, UnitStage::Start) {
            UnitStage::Start => Err(IoError::Unfinished),
            UnitStage::Resolve => self.resolved(result),
            UnitStage::Minted(topics) => {
                if let Err(error) = outcome_answer(result)? {
                    warn!(error = %error, "Failed to mint this node's own shared topics");
                    self.outcome.fail_topics(topics);
                }
                self.finish()
            }
            UnitStage::Ensured(topics) => {
                if let Err(error) = outcome_answer(result)? {
                    warn!(error = %error, "Failed to ensure shared realm topics on restart");
                    self.outcome.fail_topics(topics.iter().copied());
                }
                self.stage = UnitStage::Pulled(topics.clone());
                Ok(Next::Call(self.pull(topics)))
            }
            UnitStage::Pulled(topics) => {
                if !Self::pulled(result)? {
                    self.outcome.fail_topics(topics);
                }
                self.finish()
            }
            UnitStage::Genesis => {
                // An unreachable co-holder might hold the genesis; never fork a second one.
                let genesis = result.child::<(bool, BTreeSet<NodeId>), IoError>()?;
                if genesis.map(|(withheld, _)| withheld).unwrap_or(true) {
                    let unavailable = Some(RecoveryError::PeerUnavailable);
                    self.outcome.error = RecoveryError::merge(self.outcome.error, unavailable);
                }
                self.stage = UnitStage::Present;
                Ok(Next::Call(existing_call(self.all_topics())))
            }
            UnitStage::Present => {
                let present = self.present(result)?;
                if present.is_empty() {
                    return self.finish();
                }
                self.stage = UnitStage::Pulled(present.clone());
                Ok(Next::Call(self.pull(present)))
            }
            UnitStage::JoinPolicy => {
                if let Err(error) = outcome_answer(result)? {
                    warn!(error = %error, "Failed to prepare joined shard membership on restart");
                    self.outcome.fail_topics(self.all_topics());
                }
                self.stage = UnitStage::JoinPull;
                Ok(Next::Call(self.pull(self.all_topics())))
            }
            UnitStage::JoinPull => {
                if !Self::pulled(result)? {
                    self.outcome.fail_topics(self.all_topics());
                }
                self.stage = UnitStage::JoinPresent;
                Ok(Next::Call(existing_call(self.all_topics())))
            }
            UnitStage::JoinPresent => {
                let present = self.present(result)?;
                if present.is_empty() {
                    return self.finish();
                }
                self.stage = UnitStage::JoinMembers(present.clone());
                Ok(Next::Call(self.membership(present)))
            }
            UnitStage::JoinMembers(present) => {
                if let Err(error) = outcome_answer(result)? {
                    warn!(error = %error, "Failed to reconcile joined shard membership on restart");
                    self.outcome.fail_topics(present);
                }
                self.finish()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum PassStage {
    Config,
    Network(Box<RealmConfigDocument>),
    Verified,
    Unit,
    Retry,
    Verify,
}

/// One bounded pass and the cursor the next pass resumes from.
pub type RestoredPass = (ShardRestorePass, ShardRestoreCursor);
type PassTurn = Result<Next<RestoredPass>, IoError>;

/// Runs a bounded number of restore work units from `cursor` and wraps at the end, so an
/// unavailable first group cannot starve a later healthy one. A stop lands between units and
/// keeps the cursor on the first unprocessed one.
#[derive(Debug, PartialEq)]
pub struct RestorePassOperation {
    node_id: NodeId,
    realm_id: RealmId,
    cursor: ShardRestoreCursor,
    stop: RecoveryStop,
    now_ms: u64,
    stage: PassStage,
    summary: RestoreShardSummary,
    units: Vec<RestoreUnit>,
    plan_changed: bool,
    verified: BTreeSet<TopicId>,
    range: RestoreRange,
    position: usize,
    topics_completed: usize,
    unresolved: BTreeSet<TopicId>,
    error: Option<RecoveryError>,
}

impl RestorePassOperation {
    pub fn new(
        node_id: NodeId,
        realm_id: RealmId,
        cursor: ShardRestoreCursor,
        stop: RecoveryStop,
    ) -> Self {
        Self {
            node_id,
            realm_id,
            cursor,
            stop,
            now_ms: 0,
            stage: PassStage::Config,
            summary: RestoreShardSummary::default(),
            units: Vec::new(),
            plan_changed: false,
            verified: BTreeSet::new(),
            range: RestoreRange {
                start: 0,
                end: 0,
                wrapped: true,
            },
            position: 0,
            topics_completed: 0,
            unresolved: BTreeSet::new(),
            error: None,
        }
    }

    /// Nothing is sharded here yet: a fresh node, or one without a net transport.
    fn nothing_to_restore(&mut self) -> PassTurn {
        self.cursor = ShardRestoreCursor::default();
        let pass = ShardRestorePass {
            wrapped: true,
            enumerated: true,
            ..ShardRestorePass::default()
        };
        Ok(Next::Done((pass, self.cursor)))
    }

    fn config(&mut self, result: IoResult) -> PassTurn {
        match result.child::<RealmConfigDocument, GetConfigError>()? {
            Ok(config) => {
                self.stage = PassStage::Network(Box::new(config));
                Ok(Next::Call(existing_call(Vec::new())))
            }
            Err(GetConfigError::DocumentNotFound) => self.nothing_to_restore(),
            // Keep the cursor: a failed config read says nothing about the plan, and the
            // next successful load binds it again by hash.
            Err(error) => {
                warn!(realm_id = %self.realm_id, error = %error, "Failed to load realm config for recovery");
                let pass = ShardRestorePass {
                    error: Some(RecoveryError::Storage),
                    wrapped: true,
                    ..ShardRestorePass::default()
                };
                Ok(Next::Done((pass, self.cursor)))
            }
        }
    }

    fn plan(&mut self, config: &RealmConfigDocument) -> PassTurn {
        let plan = plan_shard_groups(config, self.node_id, self.realm_id, self.now_ms);
        self.summary = plan.summary;
        self.units = plan.into_units();
        self.plan_changed = bind_restore_plan(&mut self.cursor, &self.units);
        if self.units.is_empty() {
            self.cursor.next_unit = 0;
            let pass = ShardRestorePass {
                summary: self.summary,
                plan_changed: self.plan_changed,
                wrapped: true,
                enumerated: true,
                ..ShardRestorePass::default()
            };
            return Ok(Next::Done((pass, self.cursor)));
        }
        // Former-holder history cutoffs are frozen only for shards a prior run durably
        // verified; join verification happens after this restore.
        self.stage = PassStage::Verified;
        let verified = VerifiedTopicsOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(verified)))
    }

    fn next_unit(&mut self) -> PassTurn {
        if self.position >= self.range.end {
            return self.finish_batch();
        }
        if self.stop.requested() {
            self.range.end = self.position;
            self.range.wrapped = false;
            return self.finish_batch();
        }
        self.stage = PassStage::Unit;
        let unit = self.units[self.position].clone();
        let restore = RestoreUnitOperation::new(self.node_id, unit, self.verified.clone());
        Ok(Next::Call(IoCall::child(restore)))
    }

    fn unit_done(&mut self, result: IoResult) -> PassTurn {
        let unit = &self.units[self.position];
        let outcome = match result.child::<RestoreUnitOutcome, IoError>()? {
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(error = %error, "Restore work unit stopped");
                let mut failed = RestoreUnitOutcome::default();
                failed.fail_topics(unit.topics.iter().copied());
                failed
            }
        };
        let completed = (unit.topics.len()).saturating_sub(outcome.unresolved_topics.len());
        self.topics_completed = self.topics_completed.saturating_add(completed);
        if completed > 0 {
            info!(
                event = "startup.recovery.progress",
                phase = "shard_restore",
                topics = completed,
                withheld_topics = outcome.unresolved_topics.len(),
                unit = self.position + 1,
                units_total = self.units.len(),
                "Completed recovery topics"
            );
        }
        self.unresolved.extend(outcome.unresolved_topics);
        self.error = RecoveryError::merge(self.error, outcome.error);
        self.position += 1;
        self.next_unit()
    }

    fn finish_batch(&mut self) -> PassTurn {
        self.summary.withheld_topics = self.unresolved.len();
        self.cursor.next_unit = if self.range.wrapped {
            0
        } else {
            self.range.end
        };
        // A withheld genesis has no placement record to trigger another pass. Arm the
        // retry so a returning co-holder unblocks writes without a restart.
        if !self.unresolved.is_empty() {
            self.stage = PassStage::Retry;
            let retry = schedule_retry_effect(self.realm_id, self.node_id);
            return Ok(Next::Call(IoCall::from(retry)));
        }
        self.verify_held()
    }

    fn verify_held(&mut self) -> PassTurn {
        if !self.range.wrapped {
            return self.finish();
        }
        // New-holder verification: reconcile each held shard against a co-holder and
        // persist a marker so a restart resumes only unverified shards.
        self.stage = PassStage::Verify;
        let verify = VerifyHeldOperation::new(self.node_id, self.realm_id);
        Ok(Next::Call(IoCall::child(verify)))
    }

    fn finish(&mut self) -> PassTurn {
        let unvisited_topics = self.units[self.range.end..]
            .iter()
            .map(|unit| unit.topics.len())
            .sum();
        let pass = ShardRestorePass {
            summary: self.summary,
            units_processed: self.range.end - self.range.start,
            topics_completed: self.topics_completed,
            units_total: self.units.len(),
            unvisited_topics,
            unresolved_topics: std::mem::take(&mut self.unresolved),
            error: self.error,
            plan_changed: self.plan_changed,
            wrapped: self.range.wrapped,
            enumerated: true,
        };
        Ok(Next::Done((pass, self.cursor)))
    }
}

impl Operation for RestorePassOperation {
    type Output = RestoredPass;
    type Error = IoError;

    fn first(&mut self) -> PassTurn {
        let config = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> PassTurn {
        match std::mem::replace(&mut self.stage, PassStage::Unit) {
            PassStage::Config => self.config(result),
            PassStage::Network(config) => match shard_answer(result)? {
                ShardNetEvent::NoNetwork => self.nothing_to_restore(),
                _ => self.plan(&config),
            },
            PassStage::Verified => {
                self.verified = (result.child::<BTreeSet<TopicId>, IoError>()?).unwrap_or_default();
                self.range = restore_range(self.cursor, self.units.len());
                self.position = self.range.start;
                self.next_unit()
            }
            PassStage::Unit => self.unit_done(result),
            // Both are best effort: the next pass arms and verifies again.
            PassStage::Retry => self.verify_held(),
            PassStage::Verify => {
                let verified = result.child::<ShardVerificationSummary, IoError>()?;
                if let Err(error) = verified {
                    warn!(error = %error, "Held shard verification stopped");
                }
                self.finish()
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

/// Restarts the node's document-sync subscriptions from held shards plus the fixed shared realm
/// topics, so a restart costs O(held shards), not O(stored documents). Runs bounded passes until
/// the cursor wraps and returns the small topic counts.
#[derive(Debug, PartialEq)]
pub struct RestoreTopicsOperation {
    node_id: NodeId,
    realm_id: RealmId,
    summary: RestoreShardSummary,
}

impl RestoreTopicsOperation {
    pub fn new(node_id: NodeId, realm_id: RealmId) -> Self {
        Self {
            node_id,
            realm_id,
            summary: RestoreShardSummary::default(),
        }
    }

    fn pass(&self, cursor: ShardRestoreCursor) -> Next<RestoreShardSummary> {
        let stop = RecoveryStop::default();
        let pass = RestorePassOperation::new(self.node_id, self.realm_id, cursor, stop);
        Next::Call(IoCall::child(pass))
    }
}

impl Operation for RestoreTopicsOperation {
    type Output = RestoreShardSummary;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<RestoreShardSummary>, IoError> {
        Ok(self.pass(ShardRestoreCursor::default()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RestoreShardSummary>, IoError> {
        let (pass, cursor) = result.child::<RestoredPass, IoError>()??;
        self.summary.held_shards = pass.summary.held_shards;
        self.summary.shard_topics = pass.summary.shard_topics;
        self.summary.shared_topics = pass.summary.shared_topics;
        self.summary.withheld_topics += pass.summary.withheld_topics;
        match pass.wrapped {
            true => Ok(Next::Done(self.summary)),
            false => Ok(self.pass(cursor)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum PolicyStage {
    Network,
    Config,
    Verified,
    Present,
    Members,
}

type PolicyTurn = Result<Next<RestoreShardSummary>, IoError>;

/// Installs publisher and membership policy for the shard topics this node already holds a
/// genesis for, without contacting a peer. Missing geneses and anti-entropy are remote
/// convergence and belong to the recovery run.
#[derive(Debug, PartialEq)]
pub struct PreparePolicyOperation {
    node_id: NodeId,
    realm_id: RealmId,
    now_ms: u64,
    stage: PolicyStage,
    summary: RestoreShardSummary,
    groups: VecDeque<(ShardGroup, Vec<TopicId>)>,
    group: Option<ShardGroup>,
    verified: BTreeSet<TopicId>,
    prepared: usize,
}

impl PreparePolicyOperation {
    pub fn new(node_id: NodeId, realm_id: RealmId) -> Self {
        Self {
            node_id,
            realm_id,
            now_ms: 0,
            stage: PolicyStage::Network,
            summary: RestoreShardSummary::default(),
            groups: VecDeque::new(),
            group: None,
            verified: BTreeSet::new(),
            prepared: 0,
        }
    }

    fn next_group(&mut self) -> PolicyTurn {
        let Some((group, topics)) = self.groups.pop_front() else {
            info!(
                held_shards = self.summary.held_shards,
                prepared_topics = self.prepared,
                "Prepared local shard topic policy"
            );
            return Ok(Next::Done(self.summary));
        };
        self.group = Some(group);
        self.stage = PolicyStage::Present;
        Ok(Next::Call(existing_call(topics)))
    }
}

impl Operation for PreparePolicyOperation {
    type Output = RestoreShardSummary;
    type Error = IoError;

    fn first(&mut self) -> PolicyTurn {
        Ok(Next::Call(existing_call(Vec::new())))
    }

    fn next(&mut self, result: IoResult) -> PolicyTurn {
        match self.stage {
            PolicyStage::Network => {
                if shard_answer(result)? == ShardNetEvent::NoNetwork {
                    return Ok(Next::Done(RestoreShardSummary::default()));
                }
                self.stage = PolicyStage::Config;
                let config = GetConfigOperation::new(self.realm_id);
                Ok(Next::Call(IoCall::child(config)))
            }
            PolicyStage::Config => {
                let config = match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => config,
                    Err(GetConfigError::DocumentNotFound) => {
                        return Ok(Next::Done(RestoreShardSummary::default()));
                    }
                    Err(error) => {
                        warn!(realm_id = %self.realm_id, error = %error, "Failed to load realm config for recovery");
                        return Ok(Next::Done(RestoreShardSummary::default()));
                    }
                };
                let plan = plan_shard_groups(&config, self.node_id, self.realm_id, self.now_ms);
                self.summary = plan.summary;
                self.groups = (plan.rank0_groups.into_iter())
                    .chain(plan.join_groups)
                    .collect();
                self.stage = PolicyStage::Verified;
                let verified = VerifiedTopicsOperation::new(self.realm_id);
                Ok(Next::Call(IoCall::child(verified)))
            }
            PolicyStage::Verified => {
                self.verified = (result.child::<BTreeSet<TopicId>, IoError>()?).unwrap_or_default();
                self.next_group()
            }
            PolicyStage::Present => {
                let present: Vec<TopicId> = (existing_answer(result)?.unwrap_or_default())
                    .into_iter()
                    .collect();
                if present.is_empty() {
                    return self.next_group();
                }
                self.prepared += present.len();
                self.stage = PolicyStage::Members;
                let group = self.group.as_ref().ok_or(IoError::Unfinished)?;
                let members = membership_call(present, group, self.node_id, &self.verified);
                Ok(Next::Call(members))
            }
            PolicyStage::Members => {
                // Local policy is installed before the answer arrives.
                if let Err(error) = outcome_answer(result)? {
                    warn!(error = %error, "Deferring shard membership top-up to remote recovery");
                }
                self.next_group()
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[cfg(test)]
#[path = "restore_tests.rs"]
mod tests;
