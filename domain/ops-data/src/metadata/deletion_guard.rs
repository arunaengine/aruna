//! Runs a graph-addressed metadata effect after the deletion check of its graph, under the
//! graph's fence when the effect changes the graph, and prunes the local copies of deleted graphs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::operation::{HoldScope, Operation, boxed_suboperation};
use tracing::{debug_span, field};

use crate::metadata::graph_deleted::{GraphDeletedError, GraphDeletedOperation};
use crate::metadata::registry_cache::cache_answer;
use crate::metadata::span_timing::ReadTiming;

/// The number of graph fence shards; changes to graphs of one shard never overlap.
const FENCE_SHARDS: u64 = 32;

type Turn = Result<Next<MetadataEvent>, MetadataError>;

fn graph_call(effect: MetadataEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Metadata(effect)))
}

// The guard answers as the effect would, so callers read one event either way.
fn answer(result: Result<MetadataEvent, MetadataError>) -> Event {
    Event::Metadata(result.unwrap_or_else(|error| MetadataEvent::Error {
        graph_iri: None,
        error,
    }))
}

/// The effect that runs `effect`: a graph-addressed effect goes through the deletion check of
/// its graph, the checks of unsaved candidates and the node's caches run as they are.
pub fn metadata_effect(effect: MetadataEffect) -> Effect {
    let validates = matches!(
        effect,
        MetadataEffect::ValidateCreateCrate { .. } | MetadataEffect::ValidateRoCrate { .. }
    );
    match effect.graph_iri() {
        Some(graph_iri) if !validates => {
            let guard = DeletionGuardOperation::new(graph_iri, effect);
            Effect::SubOperation(boxed_suboperation(guard, answer))
        }
        _ => Effect::Metadata(effect),
    }
}

/// [`metadata_effect`] as a call.
pub fn metadata_call(effect: MetadataEffect) -> IoCall {
    IoCall::from(metadata_effect(effect))
}

/// The fence shard of `graph_iri`.
pub fn fence_shard(graph_iri: &str) -> u8 {
    let hash = blake3::hash(graph_iri.as_bytes());
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&hash.as_bytes()[..8]);
    (u64::from_be_bytes(prefix) % FENCE_SHARDS) as u8
}

/// Whether `effect` is refused on a deleted graph.
fn rejects_deleted(effect: &MetadataEffect) -> bool {
    matches!(
        effect,
        MetadataEffect::CreateCrate { .. }
            | MetadataEffect::ApplyRoCrate { .. }
            | MetadataEffect::UpsertDataEntity { .. }
            | MetadataEffect::UpsertContextualEntity { .. }
            | MetadataEffect::SetGraphPolicy { .. }
            | MetadataEffect::AddGraphPeer { .. }
            | MetadataEffect::GetGraphPolicy { .. }
            | MetadataEffect::ExportRoCrate { .. }
            | MetadataEffect::ExportRoCrateSummary { .. }
            | MetadataEffect::ExportSummaryAt { .. }
            | MetadataEffect::DescribeTitle { .. }
            | MetadataEffect::ExportRoCratePage { .. }
            | MetadataEffect::PlanBatch { .. }
            | MetadataEffect::MergeBatch { .. }
    )
}

/// The answer `effect` gets on a deleted graph without running, or `None` when it still runs.
fn deleted_answer(effect: &MetadataEffect, graph_iri: &str) -> Option<MetadataEvent> {
    match effect {
        MetadataEffect::ContainsGraph { graph_iri } => Some(MetadataEvent::ContainsGraphResult {
            graph_iri: graph_iri.clone(),
            exists: false,
        }),
        // A deleted graph applies nothing more, so nothing is left to apply.
        MetadataEffect::ContainsDot { graph_iri, .. } => Some(MetadataEvent::ContainsDotResult {
            graph_iri: graph_iri.clone(),
            contains: true,
        }),
        _ if rejects_deleted(effect) => Some(MetadataEvent::Error {
            graph_iri: Some(graph_iri.to_string()),
            error: MetadataError::InvalidInput(format!("metadata graph `{graph_iri}` is deleted")),
        }),
        _ => None,
    }
}

#[derive(Debug, PartialEq)]
enum GuardStage {
    Mark,
    Lifecycle,
    Store(bool),
    Run,
}

/// Checks whether the graph of one effect is deleted, from the deletion cache or else its
/// lifecycle row, then answers as a deleted graph does or runs the effect.
#[derive(Debug, PartialEq)]
pub struct DeletionGuardOperation {
    graph_iri: String,
    effect: Option<MetadataEffect>,
    stage: GuardStage,
    timing: ReadTiming,
}

impl DeletionGuardOperation {
    pub fn new(graph_iri: String, effect: MetadataEffect) -> Self {
        Self {
            graph_iri,
            effect: Some(effect),
            stage: GuardStage::Mark,
            timing: ReadTiming::default(),
        }
    }

    fn decided(&mut self, deleted: bool) -> Turn {
        self.timing.note("deleted", deleted);
        self.timing.finish(None);
        let effect = (self.effect.take())
            .ok_or_else(|| MetadataError::Backend("guarded effect already ran".to_string()))?;
        if deleted && let Some(answer) = deleted_answer(&effect, &self.graph_iri) {
            return Ok(Next::Done(answer));
        }
        self.stage = GuardStage::Run;
        Ok(Next::Call(graph_call(effect)))
    }
}

impl Operation for DeletionGuardOperation {
    type Output = MetadataEvent;
    type Error = MetadataError;

    fn hold_scope(&self) -> Option<HoldScope> {
        let changes = (self.effect.as_ref()).is_some_and(MetadataEffect::mutates_graph);
        changes.then(|| HoldScope::GraphFence(fence_shard(&self.graph_iri)))
    }

    fn stamp(&mut self, now_ms: u64) {
        self.timing.stamp(now_ms);
    }

    fn first(&mut self) -> Turn {
        let kind = self.effect.as_ref().map_or("", MetadataEffect::kind);
        self.timing.open(debug_span!(
            "metadata.graph_lifecycle.read_before_effect",
            effect = kind,
            graph_iri = self.graph_iri.as_str(),
            deleted = field::Empty,
            elapsed_ms = field::Empty,
        ));
        let mark = MetadataEffect::DeletionMark {
            graph_iri: self.graph_iri.clone(),
        };
        Ok(Next::Call(graph_call(mark)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.stage, GuardStage::Run) {
            GuardStage::Mark => match cache_answer(result)? {
                MetadataEvent::DeletionMark { deleted: true } => self.decided(true),
                _ => {
                    self.stage = GuardStage::Lifecycle;
                    let read = GraphDeletedOperation::new(&self.graph_iri);
                    Ok(Next::Call(IoCall::child(read)))
                }
            },
            GuardStage::Lifecycle => match result.child::<bool, GraphDeletedError>()? {
                Ok(deleted) => {
                    self.stage = GuardStage::Store(deleted);
                    let graph_iri = self.graph_iri.clone();
                    Ok(Next::Call(graph_call(MetadataEffect::MarkDeletion {
                        graph_iri,
                        deleted,
                    })))
                }
                Err(error) => {
                    let error = MetadataError::from(error);
                    self.timing.finish(Some(&error));
                    Ok(Next::Done(MetadataEvent::Error {
                        graph_iri: Some(self.graph_iri.clone()),
                        error,
                    }))
                }
            },
            // A mark the cache did not take only leaves the next check to read the row again.
            GuardStage::Store(deleted) => self.decided(deleted),
            GuardStage::Run => match result.event()? {
                Event::Metadata(answer) => Ok(Next::Done(answer)),
                other => Err(MetadataError::Backend(format!(
                    "unexpected metadata event: {}",
                    other.label()
                ))),
            },
        }
    }
}

// The graph store's answer to an effect a prune runs itself; an error answer fails the prune.
fn graph_answer(result: IoResult) -> Result<MetadataEvent, MetadataError> {
    match result.event()? {
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error),
        Event::Metadata(answer) => Ok(answer),
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata event: {}",
            other.label()
        ))),
    }
}

#[derive(Debug, PartialEq)]
enum PruneStage {
    Lifecycle,
    Contains,
    Delete,
    Forget,
}

/// Removes the local copy of one graph whose lifecycle row marks it deleted, under the graph's
/// fence; answers whether the graph was deleted.
#[derive(Debug, PartialEq)]
pub struct PruneGraphOperation {
    graph_iri: String,
    stage: PruneStage,
}

impl PruneGraphOperation {
    pub fn new(graph_iri: String) -> Self {
        Self {
            graph_iri,
            stage: PruneStage::Lifecycle,
        }
    }

    fn graph_effect(&mut self, stage: PruneStage, effect: MetadataEffect) -> PruneTurn {
        self.stage = stage;
        Ok(Next::Call(graph_call(effect)))
    }
}

type PruneTurn = Result<Next<bool>, MetadataError>;

impl Operation for PruneGraphOperation {
    type Output = bool;
    type Error = MetadataError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::GraphFence(fence_shard(&self.graph_iri)))
    }

    fn first(&mut self) -> PruneTurn {
        Ok(Next::Call(IoCall::child(GraphDeletedOperation::new(
            &self.graph_iri,
        ))))
    }

    fn next(&mut self, result: IoResult) -> PruneTurn {
        let graph_iri = self.graph_iri.clone();
        match std::mem::replace(&mut self.stage, PruneStage::Forget) {
            PruneStage::Lifecycle => match result.child::<bool, GraphDeletedError>()?? {
                false => Ok(Next::Done(false)),
                true => self.graph_effect(
                    PruneStage::Contains,
                    MetadataEffect::ContainsGraph { graph_iri },
                ),
            },
            PruneStage::Contains => match graph_answer(result)? {
                MetadataEvent::ContainsGraphResult { exists: true, .. } => self.graph_effect(
                    PruneStage::Delete,
                    MetadataEffect::DeleteGraph { graph_iri },
                ),
                _ => self.graph_effect(
                    PruneStage::Forget,
                    MetadataEffect::ForgetGraph { graph_iri },
                ),
            },
            PruneStage::Delete => {
                graph_answer(result)?;
                self.graph_effect(
                    PruneStage::Forget,
                    MetadataEffect::ForgetGraph { graph_iri },
                )
            }
            PruneStage::Forget => {
                graph_answer(result)?;
                Ok(Next::Done(true))
            }
        }
    }
}

/// Removes the local copy of every stored graph that is deleted, one graph at a time; answers
/// how many were removed.
#[derive(Debug, PartialEq, Default)]
pub struct PruneGraphsOperation {
    remaining: Vec<String>,
    pruned: usize,
    listed: bool,
}

impl PruneGraphsOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn prune_next(&mut self) -> Result<Next<usize>, MetadataError> {
        match self.remaining.pop() {
            Some(graph_iri) => Ok(Next::Call(IoCall::child(PruneGraphOperation::new(
                graph_iri,
            )))),
            None => Ok(Next::Done(self.pruned)),
        }
    }
}

impl Operation for PruneGraphsOperation {
    type Output = usize;
    type Error = MetadataError;

    fn first(&mut self) -> Result<Next<usize>, MetadataError> {
        Ok(Next::Call(graph_call(MetadataEffect::ListGraphs)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, MetadataError> {
        if self.listed {
            if result.child::<bool, MetadataError>()?? {
                self.pruned += 1;
            }
            return self.prune_next();
        }
        match graph_answer(result)? {
            MetadataEvent::GraphListResult { mut graph_iris } => {
                self.listed = true;
                graph_iris.reverse();
                self.remaining = graph_iris;
                self.prune_next()
            }
            other => Err(MetadataError::Backend(format!(
                "unexpected metadata graph list: {}",
                Event::Metadata(other).label()
            ))),
        }
    }
}

#[cfg(test)]
#[path = "deletion_guard_tests.rs"]
mod call_tests;
