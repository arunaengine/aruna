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
