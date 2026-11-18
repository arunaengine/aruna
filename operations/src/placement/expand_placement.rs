//! Expands placement when a node joins, without taking buckets away from current holders.
//! Only buckets whose target set contains the current set get a transition.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::placement::transition::{TransitionRequest, expansion_buckets, plan_transition};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::transition::{CandidatePlacementMap, TransitionLimits};
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::placement::mutation::PlacementMutationOperation;
use crate::realm::mutate_placement::{
    MutatePlacementConfig, MutatePlacementError, RealmPlacementMutation,
};

/// One placement mutation of this node's own, with the shared conflict re-runs.
pub(crate) fn mutate_call(actor: &Actor, mutation: RealmPlacementMutation) -> IoCall {
    let config = MutatePlacementConfig {
        actor: actor.clone(),
        mutation,
    };
    IoCall::child(PlacementMutationOperation::new(config, None, None))
}

fn mutated(result: IoResult) -> Result<RealmConfigDocument, MutatePlacementError> {
    result.child::<RealmConfigDocument, MutatePlacementError>()?
}

#[derive(Debug, PartialEq)]
enum ActivateStage {
    Read,
    Publish,
    Initialize,
}

type ActivateTurn = Result<Next<RealmConfigDocument>, MutatePlacementError>;

/// Publishes the realm's first candidate map and initializes every strategy's
/// activations through the reducer; this must run before a second node registers,
/// and only reducer-initialized activations advance when a transition completes.
#[derive(Debug, PartialEq)]
pub struct EnsureActivatedOperation {
    actor: Actor,
    stage: ActivateStage,
    epoch: u64,
    config: Option<RealmConfigDocument>,
    /// Strategies still to initialize.
    pending: Vec<Ulid>,
}

impl EnsureActivatedOperation {
    pub fn new(actor: &Actor) -> Self {
        Self {
            actor: actor.clone(),
            stage: ActivateStage::Read,
            epoch: 0,
            config: None,
            pending: Vec::new(),
        }
    }

    /// Bucket zero stands for the strategy: activations are initialized for all
    /// of its buckets in one reduced event.
    fn initialize(&mut self, config: RealmConfigDocument) -> ActivateTurn {
        self.pending = (config.strategies.iter().rev())
            .map(|strategy| strategy.strategy_id)
            .filter(|strategy_id| config.activation(strategy_id, 0).is_none())
            .collect();
        self.config = Some(config);
        self.stage = ActivateStage::Initialize;
        self.next_strategy()
    }

    fn next_strategy(&mut self) -> ActivateTurn {
        match self.pending.pop() {
            Some(strategy_id) => {
                let mutation = RealmPlacementMutation::InitializeActivations {
                    strategy_id,
                    candidate_map_epoch: self.epoch,
                };
                Ok(Next::Call(mutate_call(&self.actor, mutation)))
            }
            None => (self.config.take())
                .map(Next::Done)
                .ok_or(MutatePlacementError::ConfigMissing),
        }
    }
}

impl Operation for EnsureActivatedOperation {
    type Output = RealmConfigDocument;
    type Error = MutatePlacementError;

    fn first(&mut self) -> ActivateTurn {
        let read = GetConfigOperation::new(self.actor.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> ActivateTurn {
        match self.stage {
            ActivateStage::Read => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|_| MutatePlacementError::ConfigMissing)?;
                if let Some(epoch) = config.newest_map_epoch() {
                    self.epoch = epoch;
                    return self.initialize(config);
                }
                let (epoch, map) = next_map(&config);
                self.epoch = epoch;
                self.stage = ActivateStage::Publish;
                let publish = RealmPlacementMutation::PublishCandidateMap(map);
                Ok(Next::Call(mutate_call(&self.actor, publish)))
            }
            ActivateStage::Publish => self.initialize(mutated(result)?),
            ActivateStage::Initialize => {
                self.config = Some(mutated(result)?);
                self.next_strategy()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ExpandStage {
    Activate,
    Publish,
    Start(Ulid),
}

type ExpandTurn = Result<Next<Vec<Ulid>>, MutatePlacementError>;

/// Publishes the current view and starts a transition onto it for every bucket
/// whose holder set only grows. Returns the transitions it started; an empty
/// result means the view already matches the newest map or nothing expands.
#[derive(Debug, PartialEq)]
pub struct ExpandPlacementOperation {
    actor: Actor,
    now_ms: u64,
    stage: ExpandStage,
    epoch: u64,
    config: Option<RealmConfigDocument>,
    /// Strategies still to look at.
    strategies: Vec<Ulid>,
    started: Vec<Ulid>,
}

impl ExpandPlacementOperation {
    pub fn new(actor: &Actor) -> Self {
        Self {
            actor: actor.clone(),
            now_ms: 0,
            stage: ExpandStage::Activate,
            epoch: 0,
            config: None,
            strategies: Vec::new(),
            started: Vec::new(),
        }
    }

    /// Reuses the durable pending view without skipping its unfinished transition work.
    fn publish_view(&mut self, config: RealmConfigDocument) -> ExpandTurn {
        let (next_epoch, map) = next_map(&config);
        let newest = config.newest_map_epoch();
        let reuse = (newest.and_then(|epoch| config.candidate_map(epoch))).is_some_and(|newest| {
            newest.nodes == map.nodes
                && newest.selectors == map.selectors
                && newest.shard_overrides == map.shard_overrides
        });
        if reuse {
            self.epoch = newest.unwrap_or(next_epoch);
            return self.walk(config);
        }
        self.epoch = next_epoch;
        self.stage = ExpandStage::Publish;
        let publish = RealmPlacementMutation::PublishCandidateMap(map);
        Ok(Next::Call(mutate_call(&self.actor, publish)))
    }

    fn walk(&mut self, config: RealmConfigDocument) -> ExpandTurn {
        self.strategies = (config.strategies.iter().rev())
            .map(|strategy| strategy.strategy_id)
            .collect();
        self.config = Some(config);
        self.next_strategy()
    }

    fn next_strategy(&mut self) -> ExpandTurn {
        let config = self.config.as_ref().ok_or(MutatePlacementError::ConfigMissing)?;
        while let Some(strategy_id) = self.strategies.pop() {
            let in_flight = config.placement_transitions.iter().find(|transition| {
                transition.plan.strategy_id == strategy_id && !transition.is_terminal()
            });
            if let Some(transition) = in_flight {
                // The joiner stays out of the holder sets until the successor
                // expansion in `process_transitions` runs for it.
                tracing::debug!(
                    %strategy_id,
                    transition_id = %transition.plan.transition_id,
                    "Expansion deferred behind an in-flight transition"
                );
                continue;
            }
            let buckets = expansion_buckets(config, strategy_id, self.epoch).map_err(invalid)?;
            if buckets.is_empty() {
                continue;
            }
            let transition_id = Ulid::generate();
            let plan = plan_transition(
                config,
                TransitionRequest {
                    transition_id,
                    strategy_id,
                    buckets,
                    target_map_epoch: self.epoch,
                    // Expansion moves nothing off a holder, so every bucket may run
                    // at once: there is no window in which authority is in doubt.
                    limits: TransitionLimits {
                        max_incomplete_buckets: u32::MAX,
                        ..TransitionLimits::default()
                    },
                    created_by: self.actor.node_id,
                    created_at_ms: self.now_ms,
                },
            )
            .map_err(invalid)?;
            self.stage = ExpandStage::Start(transition_id);
            let start = RealmPlacementMutation::StartTransition(plan);
            return Ok(Next::Call(mutate_call(&self.actor, start)));
        }
        Ok(Next::Done(std::mem::take(&mut self.started)))
    }
}

impl Operation for ExpandPlacementOperation {
    type Output = Vec<Ulid>;
    type Error = MutatePlacementError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ExpandTurn {
        let activate = EnsureActivatedOperation::new(&self.actor);
        Ok(Next::Call(IoCall::child(activate)))
    }

    fn next(&mut self, result: IoResult) -> ExpandTurn {
        match self.stage {
            ExpandStage::Activate => self.publish_view(mutated(result)?),
            ExpandStage::Publish => self.walk(mutated(result)?),
            ExpandStage::Start(started) => {
                match mutated(result) {
                    Ok(config) => {
                        self.config = Some(config);
                        self.started.push(started);
                    }
                    // Another driver won the start race; its transition covers the
                    // strategy, so this one follows instead of failing onboarding.
                    Err(MutatePlacementError::TransitionInFlight { transition_id }) => {
                        tracing::debug!(%transition_id, "Expansion start race lost");
                    }
                    Err(error) => return Err(error),
                }
                self.next_strategy()
            }
        }
    }
}

/// The next epoch and the map that freezes the current view into it.
fn next_map(config: &RealmConfigDocument) -> (u64, CandidatePlacementMap) {
    let epoch = config.newest_map_epoch().unwrap_or(0) + 1;
    (epoch, config.freeze_map(epoch))
}

fn invalid(error: aruna_core::placement::transition::TransitionPlanError) -> MutatePlacementError {
    MutatePlacementError::InvalidInput(error.to_string())
}

#[cfg(test)]
#[path = "expand_tests.rs"]
mod tests;
