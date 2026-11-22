//! Runs one bounded invocation of the document sync outbox rotation from the timer: opens the
//! rotation, drains its pages and decides when the drain runs again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::document_apply::ApplyError;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::task::{TaskEvent, TaskKey};
use tracing::{error, warn};

use crate::node::outbox::drain_page::{DrainPageOperation, DrainedPage, PageEnd};
use crate::sync::document_outbox::OutboxTailsOperation;
use crate::sync::outbox::batches::{DrainInvocation, OutboxLimits};
use crate::sync::outbox::cleanup::Publication;
use crate::sync::outbox::record::OUTBOX_DRAIN_SIZE;
use crate::sync::outbox::rotation::OutboxRotation;
use crate::sync::shard_placement::schedule_retry_after;
use crate::triggers::{TimerInput, TimerMemory, TimerPlan};

/// Outbox records remembered as published; past it they are forgotten and published once
/// more, which only costs log space.
const MAX_PUBLISHED_RECORDS: usize = 65_536;

#[derive(Clone, Copy, Debug, PartialEq)]
enum RunStep {
    Config,
    Tails,
    Page,
    Placements,
}

type RunTurn = Result<Next<TimerPlan>, IoError>;

/// One drain invocation. The rotation and the records this process published live in the
/// timer's memory, so the next invocation goes on where this one stopped.
#[derive(Clone, Debug, PartialEq)]
pub struct RotationRunOperation {
    serving: Option<(NodeId, RealmId)>,
    limits: OutboxLimits,
    rotation: OutboxRotation,
    published: Arc<BTreeMap<Vec<u8>, Publication>>,
    config: Option<Arc<RealmConfigDocument>>,
    invocation: Option<DrainInvocation>,
    before: Option<DrainInvocation>,
    step: RunStep,
    started_ms: u64,
    now_ms: u64,
}

impl RotationRunOperation {
    pub fn new(input: &TimerInput) -> Self {
        Self {
            serving: input.serving,
            limits: input.outbox,
            rotation: input.memory.rotation.clone(),
            published: input.memory.published.clone(),
            config: None,
            invocation: None,
            before: None,
            step: RunStep::Config,
            started_ms: 0,
            now_ms: 0,
        }
    }

    /// The drain runs again after `after` with the rotation and records this run kept.
    fn plan(&mut self, after: Option<Duration>) -> RunTurn {
        let memory = TimerMemory {
            rotation: std::mem::take(&mut self.rotation),
            published: std::mem::take(&mut self.published),
            ..TimerMemory::default()
        };
        Ok(Next::Done(TimerPlan { after, memory }))
    }

    /// Waits on the rotation's retry ladder.
    fn retry(&mut self) -> RunTurn {
        let after = self.rotation.retry_after();
        self.plan(Some(after))
    }

    /// Opens the rotation at the outbox's current tails unless it is open already.
    fn open(&mut self) -> RunTurn {
        if self.rotation.boundary.is_some() {
            self.invocation = Some(DrainInvocation::new(&self.rotation));
            return self.next_page();
        }
        self.step = RunStep::Tails;
        Ok(Next::Call(IoCall::child(OutboxTailsOperation::new())))
    }

    /// Drains the next page while the invocation's bounds allow it.
    fn next_page(&mut self) -> RunTurn {
        let Some(invocation) = self.invocation.take() else {
            return Err(IoError::Unfinished);
        };
        // Read rows bound the invocation, as filtered pages publish nothing.
        let scanned = invocation.scanned;
        if scanned >= self.limits.records || invocation.pages >= self.limits.pages {
            return self.finish(invocation);
        }
        let serving = self.serving.ok_or(IoError::Unfinished)?;
        let limit = OUTBOX_DRAIN_SIZE.min(self.limits.records.saturating_sub(scanned));
        self.before = Some(invocation.clone());
        let page = DrainPageOperation::new(
            serving,
            self.config.clone(),
            self.rotation.clone(),
            invocation,
            limit,
            self.published.clone(),
        );
        self.step = RunStep::Page;
        Ok(Next::Call(IoCall::child(page)))
    }

    /// Takes in a drained page: keeps which records were published and reads on or finishes.
    fn paged(&mut self, page: DrainedPage) -> RunTurn {
        let published = Arc::make_mut(&mut self.published);
        if published.len() + page.published.len() > MAX_PUBLISHED_RECORDS {
            published.clear();
        }
        published.extend(page.published);
        for record_key in &page.deleted {
            published.remove(record_key);
        }
        let mut invocation = page.invocation;
        match page.end {
            PageEnd::Records {
                has_more,
                boundary_reached,
            } if boundary_reached || !has_more => {
                invocation.reached_end = true;
                self.finish(invocation)
            }
            PageEnd::Records { .. } | PageEnd::Skip => {
                self.invocation = Some(invocation);
                self.next_page()
            }
            PageEnd::Stop => self.finish(invocation),
        }
    }

    /// Reports stuck records and, after a realm config change, wakes the placement sync before
    /// the rotation takes the invocation in.
    fn finish(&mut self, invocation: DrainInvocation) -> RunTurn {
        if let Some((oldest_ms, target, topic, placement)) = &invocation.oldest_stuck {
            error!(
                event = "pipeline.drain.stuck",
                count = invocation.stuck,
                oldest_age_ms = self.now_ms.saturating_sub(*oldest_ms),
                representative_target = ?target,
                representative_topic = %topic,
                representative_strategy = %placement.strategy_id,
                representative_shard = placement.shard,
                "Document sync outbox records are stuck: this node holds their buckets but their shard topic geneses have never arrived"
            );
        }
        let wake = (invocation.config_drained)
            .then_some(self.serving)
            .flatten();
        self.invocation = Some(invocation);
        let Some((node_id, realm_id)) = wake else {
            return self.decide();
        };
        self.step = RunStep::Placements;
        let wake = schedule_retry_after(realm_id, node_id, Duration::ZERO);
        Ok(Next::Call(IoCall::from(wake)))
    }

    fn decide(&mut self) -> RunTurn {
        let Some(invocation) = self.invocation.take() else {
            return Err(IoError::Unfinished);
        };
        let elapsed = Duration::from_millis(self.now_ms.saturating_sub(self.started_ms));
        let at = (self.now_ms, elapsed);
        let after = invocation.finish(&mut self.rotation, &self.limits, at);
        self.plan(after)
    }
}

impl Operation for RotationRunOperation {
    type Output = TimerPlan;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> RunTurn {
        self.started_ms = self.now_ms;
        let Some((_, realm_id)) = self.serving else {
            let task_id = TaskKey::DrainSyncOutbox;
            warn!(task_id = ?task_id, "Cannot drain document sync outbox without net handle");
            return self.retry();
        };
        let target = DocumentTarget::RealmConfig { realm_id };
        let row = (target.storage_keyspace().to_string(), target.storage_key());
        self.step = RunStep::Config;
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> RunTurn {
        let task_id = TaskKey::DrainSyncOutbox;
        match self.step {
            RunStep::Config => {
                let value = result
                    .values()
                    .map(|values| values.into_iter().flatten().next());
                let config = match value {
                    Ok(None) => Ok(None),
                    Ok(Some(bytes)) => RealmConfigDocument::from_bytes(&bytes)
                        .map(Some)
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(format!("the realm configuration read answered {error}")),
                };
                match config {
                    Ok(config) => {
                        self.config = config.map(Arc::new);
                        self.open()
                    }
                    Err(error) => {
                        // No row is decided while membership is unknown, so all keep their order.
                        warn!(
                            task_id = ?task_id,
                            %error,
                            "Outbox drain waits for the realm config"
                        );
                        self.retry()
                    }
                }
            }
            RunStep::Tails => match result.child::<Vec<(Vec<u8>, Vec<u8>)>, ApplyError>()? {
                // An empty outbox drops the rotation, and with it the retry ladder.
                Ok(boundaries) if boundaries.is_empty() => {
                    self.rotation = OutboxRotation::default();
                    self.plan(None)
                }
                Ok(boundaries) => {
                    self.rotation.boundary = boundaries.iter().map(|(_, key)| key).max().cloned();
                    self.rotation.stream_boundaries = boundaries;
                    self.open()
                }
                Err(error) => {
                    warn!(
                        task_id = ?task_id,
                        %error,
                        "Failed to open document sync outbox rotation"
                    );
                    self.retry()
                }
            },
            RunStep::Page => match result.child::<DrainedPage, IoError>()? {
                Ok(page) => self.paged(page),
                Err(error) => {
                    warn!(task_id = ?task_id, %error, "Draining an outbox page broke off");
                    let mut invocation = self.before.take().ok_or(IoError::Unfinished)?;
                    invocation.read_failed = true;
                    invocation.outcome.retry_needed = true;
                    self.finish(invocation)
                }
            },
            RunStep::Placements => {
                if let Ok(Event::Task(TaskEvent::Error { key, message })) = result.event() {
                    warn!(
                        task_id = ?key,
                        message = %message,
                        "Failed to schedule shard placement sync after local realm config change"
                    );
                }
                self.decide()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    use crate::queue_backoff::retry_delay_ms;

    fn input(serving: Option<(NodeId, RealmId)>, retry_attempts: u32) -> TimerInput {
        let mut input = TimerInput::new(1_000, None);
        input.serving = serving;
        input.memory.rotation.retry_attempts = retry_attempts;
        input
    }

    fn serving() -> Option<(NodeId, RealmId)> {
        let local = iroh::SecretKey::from_bytes(&[1; 32]).public();
        Some((local, RealmId::from_bytes([3; 32])))
    }

    // A node without a network waits on the retry ladder and counts the attempt.
    #[test]
    fn offline_retries() {
        let mut run = RotationRunOperation::new(&input(None, 2));
        let Ok(Next::Done(plan)) = run.first() else {
            panic!("an offline drain plans its retry");
        };
        assert_eq!(plan.after, Some(Duration::from_millis(retry_delay_ms(2))));
        assert_eq!(plan.memory.rotation.retry_attempts, 3);
    }

    // A realm config that cannot be decoded postpones the drain instead of reading as absent,
    // where the bootstrap rules would let a record publish without verified membership.
    #[test]
    fn unreadable_config_waits() {
        let mut run = RotationRunOperation::new(&input(serving(), 0));
        run.first().unwrap();
        let broken = aruna_core::types::Value::from(vec![0xff]);
        let Ok(Next::Done(plan)) = run.next(IoResult::Values(vec![Some(broken)])) else {
            panic!("an unreadable config ends the run");
        };
        assert_eq!(plan.after, Some(Duration::from_millis(retry_delay_ms(0))));
        assert_eq!(plan.memory.rotation.retry_attempts, 1);
    }

    // An empty outbox drops the rotation and with it the retry ladder, and arms no drain.
    #[test]
    fn empty_resets_backoff() {
        let mut run = RotationRunOperation::new(&input(serving(), 3));
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Read(_)))));
        let tails = run.next(IoResult::Values(vec![None]));
        assert!(matches!(tails, Ok(Next::Call(IoCall::Child(_)))));
        let empty = Ok::<_, ApplyError>(Vec::<(Vec<u8>, Vec<u8>)>::new());
        let Ok(Next::Done(plan)) = run.next(IoResult::Child(ChildOutcome::new(empty))) else {
            panic!("an empty outbox ends the run");
        };
        assert_eq!(plan.after, None);
        assert_eq!(plan.memory.rotation, OutboxRotation::default());
    }

    // Rows read on skipped pages end the invocation, though none of them was admitted.
    #[test]
    fn skipped_rows_count() {
        let mut input = input(serving(), 0);
        input.memory.rotation.boundary = Some(b"z".to_vec());
        input.outbox.records = 4;
        let mut run = RotationRunOperation::new(&input);
        run.first().unwrap();
        let page = run.next(IoResult::Values(vec![None]));
        assert!(matches!(page, Ok(Next::Call(IoCall::Child(_)))));
        let mut invocation = DrainInvocation::new(&input.memory.rotation);
        (invocation.pages, invocation.scanned) = (1, 4);
        invocation.cursor = Some(b"k".to_vec());
        let page = DrainedPage {
            invocation,
            end: PageEnd::Skip,
            published: Vec::new(),
            deleted: Vec::new(),
        };
        let drained = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(page)));
        let Ok(Next::Done(plan)) = run.next(drained) else {
            panic!("the read rows end the invocation");
        };
        assert_eq!(plan.memory.rotation.cursor, Some(b"k".to_vec()));
    }
}
