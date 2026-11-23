//! Decides after a start whether this node caught up with the realm config, and records the time
//! of its last contact with the management nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{CatchUpEffect, Effect, NetEffect};
use aruna_core::events::{CatchUpEvent, Event, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::COMPUTE_DEPARTURE_KEYSPACE;
use aruna_core::node::state::{DISCARD_KEY, LAST_CONTACT_KEY};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use tracing::{debug, warn};

use crate::jobs::fence::{LiftFenceOperation, catch_up_peers};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::held::HeldReservations;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};

/// Spacing of the passes after the node caught up; each one refreshes the last contact.
pub const CATCH_UP_REFRESH: Duration = Duration::from_secs(300);
/// Spacing of the passes while the node has not caught up yet.
pub const CATCH_UP_RETRY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchUpOutcome {
    /// Every counted round target is applied here and the last contact row was refreshed.
    pub caught_up: bool,
    /// Management nodes without a counted round; the next pass asks them again.
    pub unanswered: Vec<NodeId>,
}

#[derive(Clone, Debug, PartialEq)]
enum CatchUpStep {
    /// The marker of an unfinished discard, then the reservations it still holds.
    Marker,
    Held,
    Config,
    Round(usize),
    Applied,
    /// The realm config is read again for management nodes the sync revealed.
    Reload,
    Contact,
    /// Caught up: the start fence is lifted once the executions of a closed interval are marked.
    Lift,
}

type CatchUpNext = Result<Next<CatchUpOutcome>, IoError>;

/// Counts a sync round without `WouldBlock` with every management node that answers, at least one
/// if any exists, then waits until the applied cursor covers each round's target.
#[derive(Clone, Debug, PartialEq)]
pub struct CatchUpOperation {
    local: NodeId,
    realm_id: RealmId,
    now_ms: u64,
    /// The management nodes of the applied realm config, without this node.
    management: Vec<NodeId>,
    /// Each counted round's target with the management node that answered it.
    targets: Vec<(NodeId, irokle::ActorClock)>,
    unanswered: Vec<NodeId>,
    /// An unfinished discard: the contact is refreshed only with the marker's deletion.
    marker: bool,
    step: CatchUpStep,
}

impl CatchUpOperation {
    pub fn new(local: NodeId, realm_id: RealmId) -> Self {
        Self {
            local,
            realm_id,
            now_ms: 0,
            management: Vec::new(),
            targets: Vec::new(),
            unanswered: Vec::new(),
            marker: false,
            step: CatchUpStep::Marker,
        }
    }

    fn topic(&self) -> irokle::TopicId {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        target.sync_topic_id(self.realm_id, &PlacementRef::NIL)
    }

    fn ask(&mut self, step: CatchUpStep, effect: CatchUpEffect) -> CatchUpNext {
        self.step = step;
        let effect = Effect::Net(NetEffect::ConfigCatchUp(Box::new(effect)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn read_config(&mut self, step: CatchUpStep) -> CatchUpNext {
        self.step = step;
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    /// A management node without other management nodes counts through its own config.
    fn configured(&mut self, config: &RealmConfigDocument) -> CatchUpNext {
        self.management = catch_up_peers(config, self.local);
        match self.management.is_empty() {
            true => self.contact(),
            false => self.round(0),
        }
    }

    fn round(&mut self, index: usize) -> CatchUpNext {
        let Some(peer) = self.management.get(index).copied() else {
            return self.applied();
        };
        let topic = self.topic();
        self.ask(
            CatchUpStep::Round(index),
            CatchUpEffect::Round { topic, peer },
        )
    }

    fn answered(&mut self, index: usize, answer: CatchUpEvent) -> CatchUpNext {
        let peer = self
            .management
            .get(index)
            .copied()
            .ok_or(IoError::Unfinished)?;
        match answer {
            CatchUpEvent::Counted { target } => self.targets.push((peer, target)),
            CatchUpEvent::Partial | CatchUpEvent::Failed(_) => self.unanswered.push(peer),
            CatchUpEvent::Applied { .. } => return Err(unexpected("a sync round answer")),
        }
        self.round(index + 1)
    }

    /// Without any counted round the node has not caught up; it never waits on nothing.
    fn applied(&mut self) -> CatchUpNext {
        if self.targets.is_empty() {
            return self.finish(false);
        }
        let topic = self.topic();
        let targets = (self.targets.iter())
            .map(|(_, target)| target.clone())
            .collect();
        self.ask(
            CatchUpStep::Applied,
            CatchUpEffect::Applied { topic, targets },
        )
    }

    /// Asks the management nodes the applied config names that had no round yet. Only rounds
    /// with a current management node count; with none of them the node has not caught up.
    fn reloaded(&mut self, config: &RealmConfigDocument) -> CatchUpNext {
        let current = catch_up_peers(config, self.local);
        self.targets
            .retain(|(node_id, _)| current.contains(node_id));
        let asked = self.management.len();
        for node_id in &current {
            if !self.management.contains(node_id) {
                self.management.push(*node_id);
            }
        }
        if self.management.len() > asked {
            return self.round(asked);
        }
        match self.targets.is_empty() && !current.is_empty() {
            true => self.finish(false),
            false => self.contact(),
        }
    }

    fn contact(&mut self) -> CatchUpNext {
        self.step = CatchUpStep::Contact;
        let refresh = RefreshContactOperation::new(self.now_ms, self.marker);
        Ok(Next::Call(IoCall::child(refresh)))
    }

    fn finish(&mut self, caught_up: bool) -> CatchUpNext {
        let unanswered = std::mem::take(&mut self.unanswered);
        Ok(Next::Done(CatchUpOutcome {
            caught_up,
            unanswered,
        }))
    }
}

fn unexpected(expected: &'static str) -> IoError {
    IoError::Unexpected {
        expected,
        got: "another result",
    }
}

/// The catch-up step answer an effect result carries.
fn catch_up_answer(result: IoResult) -> Result<CatchUpEvent, IoError> {
    match result.event()? {
        Event::Net(NetEvent::ConfigCatchUp(answer)) => Ok(answer),
        _ => Err(unexpected("a realm config catch-up answer")),
    }
}

impl Operation for CatchUpOperation {
    type Output = CatchUpOutcome;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> CatchUpNext {
        let marker = (
            COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            Key::from(DISCARD_KEY),
        );
        Ok(Next::Call(IoCall::Read(vec![marker])))
    }

    fn next(&mut self, result: IoResult) -> CatchUpNext {
        match std::mem::replace(&mut self.step, CatchUpStep::Contact) {
            CatchUpStep::Marker => {
                self.marker = result.value()?.is_some();
                match self.marker {
                    true => {
                        self.step = CatchUpStep::Held;
                        Ok(Next::Call(IoCall::child(HeldReservations::all())))
                    }
                    false => self.read_config(CatchUpStep::Config),
                }
            }
            // While an unfinished discard still holds reservations, nothing is refreshed or lifted.
            CatchUpStep::Held => {
                match result.child::<Vec<JobReservationRecord>, LifecycleError>()? {
                    Ok(held) if held.is_empty() => self.read_config(CatchUpStep::Config),
                    Ok(_) => self.finish(false),
                    Err(error) => {
                        warn!(error = %error, "Discard state was not read; the catch-up waits");
                        self.finish(false)
                    }
                }
            }
            step @ (CatchUpStep::Config | CatchUpStep::Reload) => {
                let config = match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => config,
                    Err(error) => {
                        warn!(error = %error, "Realm config is unreadable; the catch-up waits");
                        return self.finish(false);
                    }
                };
                match step {
                    CatchUpStep::Config => self.configured(&config),
                    _ => self.reloaded(&config),
                }
            }
            CatchUpStep::Round(index) => {
                let answer = catch_up_answer(result)?;
                self.answered(index, answer)
            }
            CatchUpStep::Applied => match catch_up_answer(result)? {
                CatchUpEvent::Applied { covered: true } => self.read_config(CatchUpStep::Reload),
                CatchUpEvent::Applied { covered: false } | CatchUpEvent::Failed(_) => {
                    self.finish(false)
                }
                CatchUpEvent::Counted { .. } | CatchUpEvent::Partial => {
                    Err(unexpected("an applied cursor answer"))
                }
            },
            CatchUpStep::Contact => {
                result.child::<(), IoError>()??;
                if !self.unanswered.is_empty() {
                    debug!(unanswered = ?self.unanswered, "Caught up without every management node");
                }
                self.step = CatchUpStep::Lift;
                let lift = LiftFenceOperation::new(self.local, self.realm_id);
                Ok(Next::Call(IoCall::child(lift)))
            }
            CatchUpStep::Lift => {
                if let Err(error) = result.child::<bool, IoError>()? {
                    warn!(error = %error, "The start fence was not lifted");
                }
                self.finish(true)
            }
        }
    }
}

/// Writes the last contact, and deletes the marker of an unfinished discard in the same
/// transaction.
#[derive(Debug, PartialEq)]
pub struct RefreshContactOperation {
    now_ms: u64,
    marker: bool,
    deleting: bool,
}

impl RefreshContactOperation {
    pub fn new(now_ms: u64, marker: bool) -> Self {
        Self {
            now_ms,
            marker,
            deleting: false,
        }
    }
}

impl Operation for RefreshContactOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let value = Value::from(self.now_ms.to_be_bytes().as_slice());
        let row = (
            COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            Key::from(LAST_CONTACT_KEY),
            value,
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        result.done()?;
        if !self.marker || self.deleting {
            return Ok(Next::Done(()));
        }
        self.deleting = true;
        let marker = (
            COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            Key::from(DISCARD_KEY),
        );
        Ok(Next::Call(IoCall::Delete(vec![marker])))
    }
}

/// When the next pass is due after `outcome`.
pub fn next_pass(outcome: &Result<CatchUpOutcome, IoError>) -> Duration {
    match outcome {
        Ok(outcome) if outcome.caught_up => CATCH_UP_REFRESH,
        Ok(_) => CATCH_UP_RETRY,
        Err(error) => {
            warn!(error = %error, "Realm config catch-up failed");
            CATCH_UP_RETRY
        }
    }
}

/// The timer of the first catch-up pass of a start.
pub fn catch_up_timer() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::ConfigCatchUp,
        after: Duration::ZERO,
    })
}

#[cfg(test)]
#[path = "catch_up_tests.rs"]
mod tests;
