//! Resolves the shard topics a node may hold or create and ensures the genesis of those a
//! rank-zero holder leads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{ShardMembership, ShardNetEffect};
use aruna_core::events::{GenesisProbe, ShardNetEvent};
use aruna_core::operation::Operation;
use irokle::TopicId;
use tracing::warn;

use crate::shard::pull::PullTopicsOperation;
use crate::shard::{shard_answer, shard_call};
use crate::sync::shard_placement::sort_node_ids;

pub(crate) fn membership_call(
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

pub(crate) fn existing_call(topics: Vec<TopicId>) -> IoCall {
    shard_call(ShardNetEffect::Existing(topics))
}

pub(crate) fn existing_answer(result: IoResult) -> Result<BTreeSet<TopicId>, IoError> {
    Ok(match shard_answer(result)? {
        ShardNetEvent::Existing(topics) => topics,
        _ => BTreeSet::new(),
    })
}

pub(crate) fn outcome_answer(result: IoResult) -> Result<Result<(), String>, IoError> {
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
        let pull = PullTopicsOperation::new(adopt, self.co_members.clone());
        Ok(Next::Call(IoCall::child(pull)))
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
                drop(result.child::<bool, IoError>()?);
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
